//! Reconcile loop.
//!
//! Turns "something changed" signals into `addrs` writes:
//! - Service changes (from the watchers), foreign writes to owned keys (from
//!   the etcd owned-key watch), leadership changes, the resync tick and config
//!   reloads mark upstreams dirty.
//! - Each dirty upstream is debounced: it fires `debounce` after the last
//!   event, but no later than `debounce_max` after the first pending event.
//! - Firing is level-triggered: the full desired list is computed from the
//!   current snapshots with [`resolve::resolve`], never from event deltas.
//! - Only the leader writes. Followers compute (gauges, warnings) and keep the
//!   desired state, but never call the writer.
//!
//! # Design
//!
//! One task runs one `select!` loop. Per upstream it keeps a small state
//! record with up to two deadlines (debounce fire, write retry). The next
//! wake-up is the minimum over all upstreams; with the tens to hundreds of
//! upstreams this controller is built for, a linear scan per wake-up is
//! cheaper and simpler than a timer wheel. Writes run as tasks in a
//! [`JoinSet`] (at most one in flight per upstream) so a slow etcd never
//! blocks event intake or other upstreams; an upstream whose write is in
//! flight is not fired again until the result is back, and a newer desired
//! state then wins.
//!
//! What counts as "already written": `last_written` is the list this
//! controller last wrote (or saw stored). It is forgotten (set to `None`) on
//! a foreign delete, an owned-key watch resync, the resync tick and on
//! promotion to leader, so the next pass calls the writer, which compares
//! with etcd itself and reports `Unchanged` when nothing needs writing.
//! A foreign `Put` sets it to the stored `addrs`, so a stale admin save is
//! corrected and a save with the right `addrs` causes no write. When any of
//! these happens while a write of that upstream is in flight, the write's
//! result does not overwrite it: the foreign write may be the newer one in
//! etcd, so `last_written` stays unknown and the next pass asks the writer.
//!
//! an empty result keeps the last desired state (no write) and warns once
//! per episode. fallback start and end are logged once each and exported
//! as `pingap_k8s_drain_fallback`. Each distinct resolve warning is logged
//! once until it clears.
//!
//! etcd failures keep the desired state in memory and retry with exponential
//! backoff (500 ms doubling to 30 s, +-20 % jitter); every retry recomputes
//! the desired state, so the latest one wins. Errors are classified best
//! effort by looking for an [`EtcdError`] in the error chain:
//! `Conflict` (CAS retries exhausted in the writer) counts as
//! `result="conflict"` and uses the normal backoff; a malformed stored entry
//! (`Toml`, `MissingTable`, `InvalidAddrs`, `NotUtf8`, `InvalidName`, spec
//! 5.6) is logged as an error once per episode and retried only slowly
//! ([`MALFORMED_RETRY`]) or on the next event or resync, never in a tight
//! loop; anything else uses the normal backoff.
//!
//! Readiness: only a write that failed because etcd could not be reached
//! (transport, timeout, anything unclassified) marks the replica not ready,
//! until that upstream's next write gets an answer from etcd. Malformed
//! entries and CAS conflicts affect metrics and logs only: one broken
//! upstream must not block rollouts of the controller.
//!
//! etcd reachability goes to [`EtcdHealth`]: a write that got any answer
//! from etcd (including a malformed entry, a conflict or permission denied)
//! is reachable, a transport error or timeout is not.
//!
//! Kubernetes Events ([`Reconciler::with_events`]): only the
//! leader posts, with the same once-per-episode dedupe as the logs: a
//! resolve warning when it first appears (on the Service), the drain
//! fallback on enter and exit (on each Service of the upstream), a malformed
//! entry once per episode (on the controller Pod). On promotion the new
//! leader posts the warnings and fallbacks already open, so an episode that
//! started while it was a follower is reported once.
//!
//! Startup: the caller must wait for the watchers' first list before
//! calling [`Reconciler::run`]; `run` reconciles every upstream right away.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::{Future, pending};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::RngExt as _;
use tokio::sync::{Notify, broadcast, mpsc, watch};
use tokio::task::{Id, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, sleep_until};

use crate::config::UpstreamConfig;
use crate::etcd::{self, EtcdError};
use crate::events::{Events, Notice};
use crate::metrics::{EtcdHealth, Metrics, WriteResult};
use crate::resolve::{self, ResolveWarning, ServiceInput};
use crate::server::Readiness;
use crate::status::StatusBoard;
use crate::types::{
    AddrsWriter, ForeignWrite, ServiceKey, SnapshotSource, UpstreamTarget, WriteOutcome,
};

/// First retry delay after a failed write.
pub const RETRY_MIN: Duration = Duration::from_millis(500);
/// Maximum retry delay after repeated failed writes.
pub const RETRY_MAX: Duration = Duration::from_secs(30);
/// Retry delay for a malformed stored entry. An event or resync
/// retries earlier.
pub const MALFORMED_RETRY: Duration = Duration::from_secs(300);
/// Least debounce of a `target = "service"` upstream. Its address (a ClusterIP) changes
/// when a Service is recreated, and kube-proxy routes the new ClusterIP a moment after
/// the Service's EndpointSlice appears. Written sooner, Pingap's first health check
/// connected before that, conntrack kept the unrouted connection for every retry, and
/// the backend stayed unhealthy for a whole check interval (about 10 s of 503s on kind).
pub const SERVICE_SETTLE: Duration = Duration::from_secs(3);

/// Inputs that can be handed to a running [`Reconciler`] from another task.
struct Shared {
    /// The latest upstream set not applied yet, with its generation.
    upstreams: Mutex<Pending>,
    foreign: Mutex<Option<mpsc::Receiver<ForeignWrite>>>,
    notify: Notify,
    /// Generation of the upstream set the loop has applied.
    applied: watch::Sender<u64>,
}

#[derive(Default)]
struct Pending {
    generation: u64,
    upstreams: Option<BTreeMap<String, UpstreamConfig>>,
}

impl Pending {
    fn take(&mut self) -> Option<(u64, BTreeMap<String, UpstreamConfig>)> {
        self.upstreams.take().map(|u| (self.generation, u))
    }
}

/// The reconcile loop. Clone it before calling [`Reconciler::run`] to keep a
/// handle for [`Reconciler::set_upstreams`] (config reload).
pub struct Reconciler<W: AddrsWriter, S: SnapshotSource> {
    writer: Arc<W>,
    source: Arc<S>,
    metrics: Arc<Metrics>,
    readiness: Readiness,
    events: Events,
    shared: Arc<Shared>,
    status: StatusBoard,
}

impl<W: AddrsWriter, S: SnapshotSource> Clone for Reconciler<W, S> {
    fn clone(&self) -> Self {
        Reconciler {
            writer: Arc::clone(&self.writer),
            source: Arc::clone(&self.source),
            metrics: Arc::clone(&self.metrics),
            readiness: self.readiness.clone(),
            events: self.events.clone(),
            shared: Arc::clone(&self.shared),
            status: self.status.clone(),
        }
    }
}

impl<W: AddrsWriter, S: SnapshotSource> Reconciler<W, S> {
    pub fn new(
        writer: Arc<W>,
        source: Arc<S>,
        metrics: Arc<Metrics>,
        readiness: Readiness,
    ) -> Self {
        Reconciler {
            writer,
            source,
            metrics,
            readiness,
            events: Events::disabled(),
            shared: Arc::new(Shared {
                upstreams: Mutex::new(Pending::default()),
                foreign: Mutex::new(None),
                notify: Notify::new(),
                applied: watch::Sender::new(0),
            }),
            status: StatusBoard::new(),
        }
    }

    /// Publishes read-only state into `board` for `/status`.
    /// Call before [`Reconciler::run`]. Does not change reconcile behaviour.
    pub fn with_status(mut self, board: StatusBoard) -> Self {
        self.status = board;
        self
    }

    /// The board this reconciler publishes into.
    pub fn status_board(&self) -> StatusBoard {
        self.status.clone()
    }

    /// Posts Kubernetes Events (and `EtcdUnreachable`/`EtcdRecovered` from
    /// its writes) to `events`. Off by default. Call before [`Reconciler::run`].
    pub fn with_events(mut self, events: Events) -> Self {
        self.events = events;
        self
    }

    /// Replaces the set of owned upstreams. Works before and while `run` is
    /// running (on any clone). New and changed upstreams are reconciled,
    /// removed ones are dropped together with their gauges, unchanged ones
    /// keep their debounce and retry state. The latest call wins.
    ///
    /// Returns the generation of this set, for [`Reconciler::wait_applied`].
    pub fn set_upstreams(&self, upstreams: BTreeMap<String, UpstreamConfig>) -> u64 {
        let generation = {
            let mut pending = lock(&self.shared.upstreams);
            pending.generation += 1;
            pending.upstreams = Some(upstreams);
            pending.generation
        };
        self.shared.notify.notify_one();
        generation
    }

    /// Waits until the running loop has applied the upstream set of
    /// `generation` (or a newer one). After that, no upstream of an older set
    /// is reconciled again, so watchers only those sets used can be stopped
    /// without the loop ever reading a Service that is no longer watched.
    /// Never completes when the loop is not running: bound it with a timeout.
    pub async fn wait_applied(&self, generation: u64) {
        let mut applied = self.shared.applied.subscribe();
        // The sender lives in `shared`, which `self` keeps alive.
        let _ = applied.wait_for(|g| *g >= generation).await;
    }

    /// Replaces the foreign-write receiver of a running loop, for when the
    /// owned-key watch is restarted after a config reload. The new receiver
    /// is treated as a fresh watch: send `ForeignWrite::Resync` on it if
    /// events may have been missed.
    pub fn set_foreign_writes(&self, foreign: mpsc::Receiver<ForeignWrite>) {
        *lock(&self.shared.foreign) = Some(foreign);
        self.shared.notify.notify_one();
    }

    /// Runs until `shutdown` resolves. In-flight writes are aborted on
    /// shutdown.
    pub async fn run(
        self,
        changes: broadcast::Receiver<ServiceKey>,
        foreign: mpsc::Receiver<ForeignWrite>,
        mut leader: watch::Receiver<bool>,
        resync: Duration,
        shutdown: impl Future<Output = ()>,
    ) {
        let is_leader = *leader.borrow_and_update();
        let mut engine = Engine {
            writer: self.writer,
            source: self.source,
            etcd_health: EtcdHealth::new(Arc::clone(&self.metrics), self.events.clone()),
            metrics: self.metrics,
            readiness: self.readiness,
            events: self.events,
            upstreams: BTreeMap::new(),
            by_service: HashMap::new(),
            leader: is_leader,
            inflight: JoinSet::new(),
            inflight_ids: HashMap::new(),
            unreachable: BTreeSet::new(),
            status: self.status,
        };
        engine.metrics.set_leader(is_leader);
        engine.status.set_running(is_leader);
        if let Some((generation, upstreams)) = lock(&self.shared.upstreams).take() {
            engine.apply_upstreams(upstreams, Instant::now());
            self.shared.applied.send_replace(generation);
        }
        // Startup: reconcile everything now (the caller waited for the first list).
        engine.fire_all_now();

        let mut changes = Some(changes);
        let mut foreign = Some(foreign);
        let mut leader_open = true;
        let resync = resync.max(Duration::from_millis(1));
        let mut resync_tick = tokio::time::interval_at(Instant::now() + resync, resync);
        resync_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            let now = Instant::now();
            engine.fire_due(now);
            let next = engine.next_deadline();

            let wake = tokio::select! {
                biased;
                () = &mut shutdown => Wake::Shutdown,
                () = self.shared.notify.notified() => Wake::Reload,
                r = engine.inflight.join_next_with_id(), if !engine.inflight.is_empty() => Wake::WriteDone(r),
                r = leader.changed(), if leader_open => match r {
                    Ok(()) => Wake::Leader(*leader.borrow_and_update()),
                    Err(_) => Wake::LeaderClosed,
                },
                r = recv_changes(&mut changes) => Wake::Change(r),
                r = recv_foreign(&mut foreign) => Wake::Foreign(r),
                _ = resync_tick.tick() => Wake::Resync,
                () = sleep_or_pending(next) => Wake::Timer,
            };

            let now = Instant::now();
            match wake {
                Wake::Shutdown => {
                    tracing::info!("reconciler shutting down");
                    engine.inflight.abort_all();
                    return;
                }
                Wake::Reload => {
                    if let Some(rx) = lock(&self.shared.foreign).take() {
                        foreign = Some(rx);
                    }
                    if let Some((generation, upstreams)) = lock(&self.shared.upstreams).take() {
                        engine.apply_upstreams(upstreams, now);
                        self.shared.applied.send_replace(generation);
                    }
                }
                Wake::WriteDone(Some(r)) => engine.write_done(r, now),
                Wake::WriteDone(None) => {}
                Wake::Leader(is_leader) => engine.set_leader(is_leader, now),
                Wake::LeaderClosed => {
                    tracing::warn!("leader channel closed; keeping the last leadership state");
                    leader_open = false;
                }
                Wake::Change(Ok(key)) => engine.service_changed(&key, now),
                Wake::Change(Err(broadcast::error::RecvError::Lagged(n))) => {
                    tracing::warn!(
                        missed = n,
                        "service change channel lagged; reconciling all upstreams"
                    );
                    engine.mark_all(now, true, false);
                }
                Wake::Change(Err(broadcast::error::RecvError::Closed)) => {
                    tracing::warn!("service change channel closed");
                    changes = None;
                }
                Wake::Foreign(Some(fw)) => engine.foreign_write(fw, now),
                Wake::Foreign(None) => {
                    tracing::debug!("foreign write channel closed");
                    foreign = None;
                }
                Wake::Resync => {
                    tracing::debug!("resync: reconciling all upstreams");
                    engine.mark_all(now, false, true);
                }
                Wake::Timer => {}
            }
        }
    }
}

type WriteMsg = (String, Vec<String>, anyhow::Result<WriteOutcome>);

enum Wake {
    Shutdown,
    Reload,
    WriteDone(Option<Result<(Id, WriteMsg), tokio::task::JoinError>>),
    Leader(bool),
    LeaderClosed,
    Change(Result<ServiceKey, broadcast::error::RecvError>),
    Foreign(Option<ForeignWrite>),
    Resync,
    Timer,
}

async fn recv_changes(
    rx: &mut Option<broadcast::Receiver<ServiceKey>>,
) -> Result<ServiceKey, broadcast::error::RecvError> {
    match rx {
        Some(rx) => rx.recv().await,
        None => pending().await,
    }
}

async fn recv_foreign(rx: &mut Option<mpsc::Receiver<ForeignWrite>>) -> Option<ForeignWrite> {
    match rx {
        Some(rx) => rx.recv().await,
        None => pending().await,
    }
}

async fn sleep_or_pending(deadline: Option<Instant>) {
    match deadline {
        Some(t) => sleep_until(t).await,
        None => pending().await,
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Pending debounce of one upstream.
#[derive(Clone, Copy, Debug)]
struct Debounce {
    first: Instant,
    last: Instant,
}

#[derive(Debug)]
struct UpstreamState {
    config: UpstreamConfig,
    debounce: Option<Debounce>,
    /// First Kubernetes or foreign event not yet reflected in a write, for
    /// `event_to_write_seconds`.
    event_since: Option<Instant>,
    /// Latest non-empty resolved list (an empty result keeps this).
    desired: Option<Vec<String>>,
    /// What is believed to be stored in etcd; `None` = unknown.
    last_written: Option<Vec<String>>,
    retry_at: Option<Instant>,
    backoff: Duration,
    inflight: bool,
    /// What is stored changed (or became unknown) while a write was in
    /// flight, so that write's result no longer says what is stored.
    stored_changed_inflight: bool,
    empty: bool,
    /// The stored entry is malformed; logged once per episode.
    malformed: bool,
    fallback: bool,
    /// Draining endpoints of the last pass (for the fallback Event on promotion).
    fallback_endpoints: usize,
    warnings: BTreeSet<ResolveWarning>,
}

impl UpstreamState {
    fn new(config: UpstreamConfig) -> Self {
        UpstreamState {
            config,
            debounce: None,
            event_since: None,
            desired: None,
            last_written: None,
            retry_at: None,
            backoff: RETRY_MIN,
            inflight: false,
            stored_changed_inflight: false,
            empty: false,
            malformed: false,
            fallback: false,
            fallback_endpoints: 0,
            warnings: BTreeSet::new(),
        }
    }

    fn mark(&mut self, now: Instant, event: bool) {
        match &mut self.debounce {
            Some(d) => d.last = now,
            None => {
                self.debounce = Some(Debounce {
                    first: now,
                    last: now,
                })
            }
        }
        if event && self.event_since.is_none() {
            self.event_since = Some(now);
        }
    }

    /// Records what is now believed to be stored (`None` = unknown). A write
    /// in flight then cannot overwrite it with its own list when it ends: a
    /// foreign write or delete may have landed after it in etcd.
    fn set_stored(&mut self, stored: Option<Vec<String>>) {
        self.last_written = stored;
        if self.inflight {
            self.stored_changed_inflight = true;
        }
    }

    fn deadline(&self) -> Option<Instant> {
        let options = &self.config.options;
        let (debounce, debounce_max) = match options.target {
            UpstreamTarget::Pods => (options.debounce, options.debounce_max),
            UpstreamTarget::Service { .. } => (
                options.debounce.max(SERVICE_SETTLE),
                options.debounce_max.max(SERVICE_SETTLE),
            ),
        };
        let debounce = self
            .debounce
            .map(|d| (d.last + debounce).min(d.first + debounce_max));
        match (debounce, self.retry_at) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}

struct Engine<W: AddrsWriter, S: SnapshotSource> {
    writer: Arc<W>,
    source: Arc<S>,
    metrics: Arc<Metrics>,
    readiness: Readiness,
    events: Events,
    etcd_health: EtcdHealth,
    upstreams: BTreeMap<String, UpstreamState>,
    by_service: HashMap<ServiceKey, Vec<String>>,
    leader: bool,
    inflight: JoinSet<WriteMsg>,
    inflight_ids: HashMap<Id, String>,
    /// Upstreams whose last write attempt failed because etcd could not be
    /// reached (drives readiness). Malformed entries and conflicts are not
    /// in here.
    unreachable: BTreeSet<String>,
    /// Read-only copy of the state for `/status`.
    status: StatusBoard,
}

impl<W: AddrsWriter, S: SnapshotSource> Engine<W, S> {
    /// Copies the state of `name` into the status board.
    fn publish(&self, name: &str) {
        let Some(st) = self.upstreams.get(name) else {
            return;
        };
        let leader = self.leader;
        self.status.update_upstream(name, |u| {
            u.target = match st.config.options.target {
                UpstreamTarget::Pods => "pods",
                UpstreamTarget::Service { .. } => "service",
            }
            .to_string();
            u.services = st
                .config
                .services
                .iter()
                .map(|r| match r.weight {
                    Some(w) => format!("{}.{}:{} {w}", r.key.name, r.key.namespace, r.port),
                    None => format!("{}.{}:{}", r.key.name, r.key.namespace, r.port),
                })
                .collect();
            u.desired = st.desired.clone().unwrap_or_default();
            u.stored = st.last_written.clone();
            u.in_sync =
                leader && !st.inflight && st.desired.is_some() && st.last_written == st.desired;
            u.pending = st.inflight || st.retry_at.is_some() || st.debounce.is_some();
            u.fallback = st.fallback;
            u.no_endpoints = st.empty;
            u.malformed = st.malformed;
            u.warnings = st.warnings.iter().map(ToString::to_string).collect();
        });
    }

    fn publish_all(&self) {
        for name in self.upstreams.keys() {
            self.publish(name);
        }
    }

    fn apply_upstreams(&mut self, new: BTreeMap<String, UpstreamConfig>, now: Instant) {
        let removed: Vec<String> = self
            .upstreams
            .keys()
            .filter(|name| !new.contains_key(*name))
            .cloned()
            .collect();
        for name in removed {
            tracing::info!(upstream = %name, "upstream removed from config; no longer managed");
            self.upstreams.remove(&name);
            self.metrics.remove_upstream(&name);
            self.unreachable.remove(&name);
        }
        for (name, config) in new {
            match self.upstreams.get_mut(&name) {
                Some(st) if st.config == config => {}
                Some(st) => {
                    tracing::info!(upstream = %name, "upstream config changed");
                    st.config = config;
                    st.mark(now, false);
                }
                None => {
                    tracing::info!(upstream = %name, "upstream added");
                    let mut st = UpstreamState::new(config);
                    st.mark(now, false);
                    self.upstreams.insert(name, st);
                }
            }
        }
        self.status
            .retain_upstreams(|name| self.upstreams.contains_key(name));
        self.publish_all();
        self.by_service.clear();
        for (name, st) in &self.upstreams {
            for sref in &st.config.services {
                let names = self.by_service.entry(sref.key.clone()).or_default();
                if !names.contains(name) {
                    names.push(name.clone());
                }
            }
        }
        self.update_readiness();
    }

    /// Makes every upstream due immediately (startup).
    fn fire_all_now(&mut self) {
        let now = Instant::now();
        for st in self.upstreams.values_mut() {
            st.retry_at = Some(now);
        }
    }

    fn mark_all(&mut self, now: Instant, event: bool, forget_written: bool) {
        for st in self.upstreams.values_mut() {
            if forget_written {
                st.set_stored(None);
            }
            st.mark(now, event);
        }
    }

    fn service_changed(&mut self, key: &ServiceKey, now: Instant) {
        if let Some(names) = self.by_service.get(key) {
            for name in names {
                if let Some(st) = self.upstreams.get_mut(name) {
                    st.mark(now, true);
                }
            }
        }
    }

    fn foreign_write(&mut self, fw: ForeignWrite, now: Instant) {
        match fw {
            ForeignWrite::Resync => {
                tracing::info!("owned-key watch resync; reconciling all upstreams");
                self.mark_all(now, false, true);
            }
            ForeignWrite::Put {
                upstream,
                addrs,
                revision,
            } => {
                let Some(st) = self.upstreams.get_mut(&upstream) else {
                    return;
                };
                let correct = match (&addrs, &st.desired) {
                    (Some(stored), Some(desired)) => stored == desired,
                    _ => false,
                };
                self.metrics.record_foreign_write(&upstream, !correct);
                st.set_stored(addrs);
                if correct {
                    tracing::debug!(%upstream, revision, "foreign write kept the desired addrs");
                } else {
                    tracing::warn!(%upstream, revision, "foreign write changed addrs; scheduling a corrective write");
                    st.mark(now, true);
                }
                self.publish(&upstream);
            }
            ForeignWrite::Deleted { upstream, revision } => {
                let Some(st) = self.upstreams.get_mut(&upstream) else {
                    return;
                };
                tracing::warn!(%upstream, revision, "owned upstream key deleted; scheduling recreation");
                self.metrics.record_foreign_write(&upstream, true);
                st.set_stored(None);
                st.mark(now, true);
                self.publish(&upstream);
            }
        }
    }

    fn set_leader(&mut self, leader: bool, now: Instant) {
        if leader == self.leader {
            return;
        }
        self.leader = leader;
        self.metrics.set_leader(leader);
        self.status.set_leader(leader);
        if leader {
            tracing::info!("became leader; reconciling all upstreams");
            self.post_open_episodes();
            // Another replica may have written meanwhile: forget what we
            // believe is stored and let the writer compare.
            self.mark_all(now, false, true);
        } else {
            tracing::info!("lost leadership; writes stop");
            for st in self.upstreams.values_mut() {
                st.retry_at = None;
                st.backoff = RETRY_MIN;
            }
            self.unreachable.clear();
            self.update_readiness();
        }
        self.publish_all();
    }

    /// Events for the warnings and fallbacks already open (on promotion).
    fn post_open_episodes(&self) {
        if !self.events.is_enabled() {
            return;
        }
        for (name, st) in &self.upstreams {
            for w in &st.warnings {
                self.events.post(Notice::resolve_warning(
                    name,
                    w,
                    service_uid(&*self.source, warning_service(w)),
                ));
            }
            if st.fallback {
                self.post_drain_fallback(name, &st.config, true, st.fallback_endpoints);
            }
        }
    }

    /// One drain fallback Event on each Service of the upstream.
    fn post_drain_fallback(
        &self,
        name: &str,
        config: &UpstreamConfig,
        started: bool,
        endpoints: usize,
    ) {
        let keys: BTreeSet<&ServiceKey> = config.services.iter().map(|s| &s.key).collect();
        for key in keys {
            self.events.post(Notice::drain_fallback(
                name,
                key,
                service_uid(&*self.source, key),
                started,
                endpoints,
            ));
        }
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.upstreams
            .values()
            .filter(|st| !st.inflight)
            .filter_map(UpstreamState::deadline)
            .min()
    }

    fn fire_due(&mut self, now: Instant) {
        let due: Vec<String> = self
            .upstreams
            .iter()
            .filter(|(_, st)| !st.inflight && st.deadline().is_some_and(|t| t <= now))
            .map(|(name, _)| name.clone())
            .collect();
        for name in due {
            self.reconcile(&name);
            self.publish(&name);
        }
    }

    /// One level-triggered pass for one upstream.
    fn reconcile(&mut self, name: &str) {
        let Some(st) = self.upstreams.get_mut(name) else {
            return;
        };
        st.debounce = None;
        st.retry_at = None;

        let snapshots: Vec<_> = st
            .config
            .services
            .iter()
            .map(|sref| self.source.snapshot(&sref.key))
            .collect();
        let inputs: Vec<ServiceInput<'_>> = st
            .config
            .services
            .iter()
            .zip(&snapshots)
            .map(|(service_ref, snapshot)| ServiceInput {
                service_ref,
                snapshot,
            })
            .collect();
        let resolved = resolve::resolve(&inputs, &st.config.options);

        self.metrics
            .set_endpoints(name, resolved.ready, resolved.draining);
        self.status.update_upstream(name, |u| {
            u.ready = resolved.ready;
            u.draining = resolved.draining;
        });

        // Each distinct warning once, until it clears.
        let current: BTreeSet<ResolveWarning> = resolved.warnings.iter().cloned().collect();
        for w in current.difference(&st.warnings) {
            tracing::warn!(upstream = %name, "{w}");
            if self.leader && self.events.is_enabled() {
                let service = warning_service(w);
                let uid = st
                    .config
                    .services
                    .iter()
                    .zip(&snapshots)
                    .find(|(sref, _)| &sref.key == service)
                    .and_then(|(_, snap)| snapshot_uid(snap));
                self.events.post(Notice::resolve_warning(name, w, uid));
            }
        }
        for w in st.warnings.difference(&current) {
            tracing::info!(upstream = %name, "resolved: {w}");
        }
        st.warnings = current;

        // D2 fallback, logged on enter and exit.
        let fallback_changed = resolved.fallback != st.fallback;
        if fallback_changed {
            if resolved.fallback {
                tracing::warn!(
                    upstream = %name,
                    draining = resolved.draining,
                    "no ready endpoints; writing draining endpoints until one is ready"
                );
            } else {
                tracing::info!(upstream = %name, ready = resolved.ready, "drain fallback ended");
            }
            st.fallback = resolved.fallback;
        }
        st.fallback_endpoints = resolved.draining;
        self.metrics.set_drain_fallback(name, resolved.fallback);
        if fallback_changed && self.leader && self.events.is_enabled() {
            let endpoints = if resolved.fallback {
                resolved.draining
            } else {
                resolved.ready
            };
            let config = st.config.clone();
            self.post_drain_fallback(name, &config, resolved.fallback, endpoints);
        }
        let Some(st) = self.upstreams.get_mut(name) else {
            return;
        };

        // empty keeps the last addrs.
        if resolved.addrs.is_empty() {
            if !st.empty {
                tracing::warn!(upstream = %name, "no endpoints; keeping the last addrs");
                st.empty = true;
            }
        } else {
            if st.empty {
                tracing::info!(upstream = %name, endpoints = resolved.addrs.len(), "endpoints available again");
                st.empty = false;
            }
            st.desired = Some(resolved.addrs);
        }

        let Some(desired) = st.desired.clone() else {
            st.event_since = None;
            return;
        };
        if !self.leader || st.last_written.as_ref() == Some(&desired) {
            st.event_since = None;
            if self.leader {
                st.backoff = RETRY_MIN;
                if self.unreachable.remove(name) {
                    self.update_readiness();
                }
            }
            return;
        }

        st.inflight = true;
        let writer = Arc::clone(&self.writer);
        let upstream = name.to_string();
        let handle = self.inflight.spawn(async move {
            let result = writer.write_addrs(&upstream, &desired).await;
            (upstream, desired, result)
        });
        self.inflight_ids.insert(handle.id(), name.to_string());
    }

    fn write_done(&mut self, r: Result<(Id, WriteMsg), tokio::task::JoinError>, now: Instant) {
        let (name, addrs, result) = match r {
            Ok((id, (name, addrs, result))) => {
                self.inflight_ids.remove(&id);
                (name, addrs, result)
            }
            Err(e) => {
                let Some(name) = self.inflight_ids.remove(&e.id()) else {
                    return;
                };
                if let Some(st) = self.upstreams.get_mut(&name) {
                    st.inflight = false;
                    st.stored_changed_inflight = false;
                }
                if e.is_cancelled() {
                    return;
                }
                let err = anyhow::anyhow!("write task failed: {e}");
                self.write_failed(&name, &err, now);
                return;
            }
        };
        let Some(st) = self.upstreams.get_mut(&name) else {
            return; // removed while the write was in flight
        };
        st.inflight = false;
        let stored_changed = std::mem::take(&mut st.stored_changed_inflight);
        match result {
            Ok(outcome) => {
                let result = match outcome {
                    WriteOutcome::Unchanged => {
                        tracing::debug!(upstream = %name, "addrs unchanged");
                        WriteResult::Unchanged
                    }
                    WriteOutcome::Updated { revision } => {
                        tracing::info!(upstream = %name, revision, addrs = ?addrs, "addrs written");
                        WriteResult::Ok
                    }
                    WriteOutcome::Created { revision } => {
                        tracing::info!(upstream = %name, revision, addrs = ?addrs, "upstream key missing; created a minimal upstream");
                        WriteResult::Ok
                    }
                };
                self.metrics.record_write(&name, result);
                self.status.record_write(&name, result.as_str(), &addrs);
                if result == WriteResult::Ok
                    && let Some(since) = st.event_since
                {
                    self.metrics.observe_event_to_write(now - since);
                }
                if st.malformed {
                    tracing::info!(upstream = %name, "stored upstream entry is writable again");
                    st.malformed = false;
                }
                st.event_since = None;
                // A foreign write seen while this write was in flight may be
                // newer in etcd: leave it unknown so the pending pass asks
                // the writer, which compares with etcd.
                st.last_written = if stored_changed { None } else { Some(addrs) };
                st.backoff = RETRY_MIN;
                self.etcd_health.report(true);
                self.unreachable.remove(&name);
                self.update_readiness();
                self.publish(&name);
            }
            Err(err) => self.write_failed(&name, &err, now),
        }
    }

    fn write_failed(&mut self, name: &str, err: &anyhow::Error, now: Instant) {
        let kind = classify(err);
        self.metrics.record_write(
            name,
            if kind == FailureKind::Conflict {
                WriteResult::Conflict
            } else {
                WriteResult::Error
            },
        );
        self.status.record_write(
            name,
            match kind {
                FailureKind::Conflict => "conflict",
                FailureKind::Malformed => "malformed",
                FailureKind::Other => "error",
            },
            &[],
        );
        // A malformed entry or a CAS conflict means etcd answered; so does
        // any gRPC status other than a transport one (permission denied...).
        self.etcd_health
            .report(kind != FailureKind::Other || !etcd::is_unreachable(err));
        // A write that finishes after demotion is not retried and does not
        // affect readiness (followers never write).
        if !self.leader {
            return;
        }
        let Some(st) = self.upstreams.get_mut(name) else {
            return;
        };
        // Only a connectivity failure affects readiness; etcd answered a
        // malformed entry or a conflict.
        if kind == FailureKind::Other {
            self.unreachable.insert(name.to_string());
        } else {
            self.unreachable.remove(name);
        }
        let err = format!("{err:#}");
        if kind == FailureKind::Malformed {
            // Retrying fast cannot fix a broken entry: retry slowly, or
            // earlier when the next event or resync fires.
            st.retry_at = Some(now + jitter(MALFORMED_RETRY));
            if !st.malformed {
                st.malformed = true;
                tracing::error!(upstream = %name, error = %err, "stored upstream entry is malformed; not written until it is fixed");
                self.events.post(Notice::malformed_upstream(name, &err));
            }
        } else {
            let delay = jitter(st.backoff);
            st.backoff = (st.backoff * 2).min(RETRY_MAX);
            st.retry_at = Some(now + delay);
            tracing::warn!(upstream = %name, error = %err, retry_in = ?delay, "writing addrs failed; will retry");
        }
        self.update_readiness();
        self.publish(name);
    }

    fn update_readiness(&self) {
        self.readiness
            .set_last_write_ok(self.unreachable.is_empty());
    }
}

/// The Service a resolve warning is about.
fn warning_service(w: &ResolveWarning) -> &ServiceKey {
    match w {
        ResolveWarning::ServiceMissing(key) => key,
        ResolveWarning::PortNotInService { service, .. }
        | ResolveWarning::PortNotInSlices { service, .. } => service,
    }
}

fn snapshot_uid(snapshot: &crate::types::ServiceSnapshot) -> Option<String> {
    snapshot.service.as_ref()?.metadata.uid.clone()
}

/// The UID of the Service as currently watched (`None` when it does not exist).
fn service_uid<S: SnapshotSource>(source: &S, key: &ServiceKey) -> Option<String> {
    snapshot_uid(&source.snapshot(key))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailureKind {
    /// CAS retries exhausted inside the writer.
    Conflict,
    /// The stored entry cannot be edited.
    Malformed,
    /// etcd unreachable, timeouts and anything unclassified.
    Other,
}

/// Best-effort classification: the reconciler stays generic over
/// [`AddrsWriter`], so only errors that carry an [`EtcdError`] are refined.
fn classify(err: &anyhow::Error) -> FailureKind {
    match err.chain().find_map(|e| e.downcast_ref::<EtcdError>()) {
        Some(EtcdError::Conflict { .. }) => FailureKind::Conflict,
        Some(
            EtcdError::Toml(_)
            | EtcdError::MissingTable(_)
            | EtcdError::InvalidAddrs(_)
            | EtcdError::NotUtf8(_)
            | EtcdError::InvalidName(_)
            | EtcdError::EmptyAddrs(_),
        ) => FailureKind::Malformed,
        _ => FailureKind::Other,
    }
}

/// `d` scaled by a random factor in [0.8, 1.2).
fn jitter(d: Duration) -> Duration {
    d.mul_f64(rand::rng().random_range(0.8..1.2))
}

#[cfg(test)]
mod tests {
    use super::*;

    use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
    use k8s_openapi::api::discovery::v1::{
        Endpoint, EndpointConditions, EndpointPort, EndpointSlice,
    };

    use crate::types::{PortRef, ServiceRef, ServiceSnapshot, UpstreamOptions, UpstreamTarget};

    // ---- fakes ----

    #[derive(Default)]
    struct FakeSource {
        snaps: Mutex<HashMap<ServiceKey, ServiceSnapshot>>,
    }

    impl SnapshotSource for FakeSource {
        fn snapshot(&self, key: &ServiceKey) -> ServiceSnapshot {
            lock(&self.snaps).get(key).cloned().unwrap_or_default()
        }
    }

    impl FakeSource {
        /// Service `name` with ready endpoints `ready` and draining `draining`.
        fn set(&self, name: &str, ready: &[&str], draining: &[&str]) {
            let mut endpoints: Vec<Endpoint> = ready.iter().map(|ip| ep(ip, true)).collect();
            endpoints.extend(draining.iter().map(|ip| ep(ip, false)));
            let snap = ServiceSnapshot {
                service: Some(Arc::new(Service {
                    spec: Some(ServiceSpec {
                        ports: Some(vec![ServicePort {
                            name: Some("http".into()),
                            port: 80,
                            ..Default::default()
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                })),
                slices: vec![Arc::new(EndpointSlice {
                    address_type: "IPv4".into(),
                    endpoints,
                    ports: Some(vec![EndpointPort {
                        name: Some("http".into()),
                        port: Some(8080),
                        protocol: Some("TCP".into()),
                        ..Default::default()
                    }]),
                    ..Default::default()
                })],
            };
            lock(&self.snaps).insert(key(name), snap);
        }
    }

    fn ep(ip: &str, ready: bool) -> Endpoint {
        Endpoint {
            addresses: vec![ip.to_string()],
            conditions: Some(EndpointConditions {
                ready: Some(ready),
                serving: Some(true),
                terminating: Some(!ready),
            }),
            ..Default::default()
        }
    }

    /// Records every call; keeps a fake etcd so `Unchanged` is realistic.
    #[derive(Default)]
    struct FakeWriter {
        calls: Mutex<Vec<(String, Vec<String>, Instant)>>,
        stored: Mutex<HashMap<String, Vec<String>>>,
        failures: Mutex<std::collections::VecDeque<anyhow::Error>>,
        /// When set, each write is stored at once but its result is held
        /// back until the gate is notified (a write "in flight").
        gate: Mutex<Option<Arc<Notify>>>,
    }

    impl AddrsWriter for FakeWriter {
        fn write_addrs(
            &self,
            upstream: &str,
            addrs: &[String],
        ) -> impl Future<Output = anyhow::Result<WriteOutcome>> + Send {
            lock(&self.calls).push((upstream.to_string(), addrs.to_vec(), Instant::now()));
            let fail = lock(&self.failures).pop_front();
            let gate = lock(&self.gate).clone();
            let result = if let Some(err) = fail {
                Err(err)
            } else {
                let mut stored = lock(&self.stored);
                match stored.insert(upstream.to_string(), addrs.to_vec()) {
                    None => Ok(WriteOutcome::Created { revision: 1 }),
                    Some(old) if old == addrs => Ok(WriteOutcome::Unchanged),
                    Some(_) => Ok(WriteOutcome::Updated { revision: 2 }),
                }
            };
            async move {
                if let Some(gate) = gate {
                    gate.notified().await;
                }
                result
            }
        }
    }

    impl FakeWriter {
        fn calls(&self) -> Vec<(String, Vec<String>, Instant)> {
            lock(&self.calls).clone()
        }
        fn calls_for(&self, upstream: &str) -> Vec<Vec<String>> {
            self.calls()
                .into_iter()
                .filter(|c| c.0 == upstream)
                .map(|c| c.1)
                .collect()
        }
        fn fail(&self, n: usize, err: impl Fn() -> anyhow::Error) {
            let mut f = lock(&self.failures);
            for _ in 0..n {
                f.push_back(err());
            }
        }
        fn clear(&self) {
            lock(&self.calls).clear();
        }
    }

    // ---- harness ----

    fn key(name: &str) -> ServiceKey {
        ServiceKey {
            namespace: "prod".into(),
            name: name.into(),
        }
    }

    fn upstream(services: &[&str]) -> UpstreamConfig {
        UpstreamConfig {
            services: services
                .iter()
                .map(|s| ServiceRef {
                    key: key(s),
                    port: PortRef::Name("http".into()),
                    weight: None,
                })
                .collect(),
            options: UpstreamOptions {
                include_not_ready: false,
                drain_terminating: true,
                ipv4_only: false,
                debounce: Duration::from_millis(500),
                debounce_max: Duration::from_secs(2),
                target: UpstreamTarget::Pods,
            },
        }
    }

    fn addrs(ips: &[&str]) -> Vec<String> {
        ips.iter().map(|ip| format!("{ip}:8080")).collect()
    }

    const RESYNC: Duration = Duration::from_secs(300);

    struct Harness {
        source: Arc<FakeSource>,
        writer: Arc<FakeWriter>,
        metrics: Arc<Metrics>,
        readiness: Readiness,
        handle: Reconciler<FakeWriter, FakeSource>,
        changes: broadcast::Sender<ServiceKey>,
        foreign: mpsc::Sender<ForeignWrite>,
        leader: watch::Sender<bool>,
        events: Mutex<mpsc::Receiver<Notice>>,
        stop: Option<tokio::sync::oneshot::Sender<()>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Harness {
        fn start(
            upstreams: &[(&str, &[&str])],
            is_leader: bool,
            capacity: usize,
            setup: impl FnOnce(&FakeSource),
        ) -> Self {
            let source = Arc::new(FakeSource::default());
            setup(&source);
            let writer = Arc::new(FakeWriter::default());
            let metrics = Arc::new(Metrics::new());
            let readiness = Readiness::new();
            let (events, events_rx) = Events::channel(64);
            let rec = Reconciler::new(
                Arc::clone(&writer),
                Arc::clone(&source),
                Arc::clone(&metrics),
                readiness.clone(),
            )
            .with_events(events);
            rec.set_upstreams(
                upstreams
                    .iter()
                    .map(|(n, s)| (n.to_string(), upstream(s)))
                    .collect(),
            );
            let handle = rec.clone();
            let (changes, changes_rx) = broadcast::channel(capacity);
            let (foreign, foreign_rx) = mpsc::channel(16);
            let (leader, leader_rx) = watch::channel(is_leader);
            let (stop, stop_rx) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(rec.run(changes_rx, foreign_rx, leader_rx, RESYNC, async {
                let _ = stop_rx.await;
            }));
            Harness {
                source,
                writer,
                metrics,
                readiness,
                handle,
                changes,
                foreign,
                leader,
                events: Mutex::new(events_rx),
                stop: Some(stop),
                task,
            }
        }

        /// The Events posted since the last call: (reason, regarding name).
        fn notices(&self) -> Vec<(&'static str, String)> {
            let mut rx = lock(&self.events);
            let mut out = Vec::new();
            while let Ok(n) = rx.try_recv() {
                let name = match n.regarding {
                    crate::events::Regarding::Service { key, .. } => key.name,
                    crate::events::Regarding::Pod => "<pod>".to_string(),
                };
                out.push((n.reason, name));
            }
            out
        }

        fn change(&self, svc: &str) {
            self.changes.send(key(svc)).unwrap();
        }

        async fn stop(mut self) {
            let _ = self.stop.take().unwrap().send(());
            tokio::time::timeout(Duration::from_secs(1), self.task)
                .await
                .expect("run stops on shutdown")
                .unwrap();
        }
    }

    async fn sleep(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    fn write_count(m: &Metrics, upstream: &str, result: &str) -> u64 {
        m.value(
            "pingap_k8s_writes_total",
            &[("upstream", upstream), ("result", result)],
        )
    }

    // ---- tests ----

    #[tokio::test(start_paused = true)]
    async fn startup_writes_each_upstream_once() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        assert_eq!(h.writer.calls_for("web"), vec![addrs(&["10.0.0.1"])]);
        assert_eq!(write_count(&h.metrics, "web", "ok"), 1);
        assert_eq!(h.metrics.value("pingap_k8s_etcd_up", &[]), 1);
        assert_eq!(h.metrics.value("pingap_k8s_is_leader", &[]), 1);
        sleep(5000).await;
        assert_eq!(h.writer.calls().len(), 1);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn status_board_mirrors_state() {
        let h = Harness::start(&[("web", &["web", "gone"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &["10.0.0.9"])
        });
        sleep(10).await;
        let board = h.handle.status_board();
        let u = board.upstream("web").expect("published");
        assert_eq!(u.target, "pods");
        assert_eq!(u.services, vec!["web.prod:http", "gone.prod:http"]);
        assert_eq!(u.desired, addrs(&["10.0.0.1"]));
        assert_eq!(u.stored, Some(addrs(&["10.0.0.1"])));
        assert!(u.in_sync && !u.pending && !u.fallback);
        assert_eq!((u.ready, u.draining), (1, 1));
        assert_eq!(u.warnings, vec!["service prod/gone not found"]);
        let w = u.last_write.expect("write recorded");
        assert_eq!((w.result.as_str(), w.addrs), ("ok", addrs(&["10.0.0.1"])));

        // A failed write is recorded; demotion clears in_sync.
        h.writer.fail(1, || anyhow::anyhow!("connection refused"));
        h.source.set("web", &["10.0.0.2"], &[]);
        h.change("web");
        sleep(600).await;
        let u = board.upstream("web").unwrap();
        assert_eq!(u.last_write.unwrap().result, "error");
        assert!(!u.in_sync && u.pending);
        h.leader.send(false).unwrap();
        sleep(10).await;
        assert!(!board.upstream("web").unwrap().in_sync);

        h.handle.set_upstreams(BTreeMap::new());
        sleep(10).await;
        assert!(board.upstream("web").is_none());
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn burst_within_debounce_gives_one_write() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        h.writer.clear();

        let start = Instant::now();
        for i in 2..12 {
            h.source
                .set("web", &["10.0.0.1", &format!("10.0.0.{i}")], &[]);
            h.change("web");
            sleep(40).await;
        }
        // last event at ~360 ms; nothing yet at 800 ms
        sleep(400).await;
        assert!(h.writer.calls().is_empty());
        sleep(200).await;
        let calls = h.writer.calls();
        assert_eq!(calls.len(), 1, "{calls:?}");
        let mut want = addrs(&["10.0.0.1", "10.0.0.11"]);
        want.sort();
        assert_eq!(calls[0].1, want);
        let at = calls[0].2 - start;
        assert!(
            at >= Duration::from_millis(850) && at <= Duration::from_millis(900),
            "{at:?}"
        );
        assert_eq!(
            h.metrics
                .value("pingap_k8s_event_to_write_seconds_count", &[]),
            1
        );
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn constant_churn_flushes_within_debounce_max() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        h.writer.clear();

        let start = Instant::now();
        for i in 0..50 {
            h.source.set("web", &[&format!("10.0.1.{i}")], &[]);
            h.change("web");
            sleep(100).await;
        }
        let calls = h.writer.calls();
        assert!(calls.len() >= 2, "{calls:?}");
        let first = calls[0].2 - start;
        assert!(
            first <= Duration::from_millis(2010),
            "first write at {first:?}"
        );
        // Each later write comes at most debounce_max after the first event
        // following the previous write (events arrive every 100 ms).
        for pair in calls.windows(2) {
            let gap = pair[1].2 - pair[0].2;
            assert!(
                gap <= Duration::from_millis(2110),
                "gap {gap:?} in {calls:?}"
            );
        }
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn follower_never_writes_and_writes_on_promotion() {
        let h = Harness::start(&[("web", &["web"])], false, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        h.source.set("web", &["10.0.0.1", "10.0.0.2"], &[]);
        h.change("web");
        sleep(3000).await;
        assert!(h.writer.calls().is_empty());
        // followers still compute
        assert_eq!(
            h.metrics.value(
                "pingap_k8s_endpoints",
                &[("upstream", "web"), ("class", "ready")]
            ),
            2
        );
        assert_eq!(h.metrics.value("pingap_k8s_is_leader", &[]), 0);

        h.leader.send(true).unwrap();
        sleep(3000).await;
        assert_eq!(
            h.writer.calls_for("web"),
            vec![addrs(&["10.0.0.1", "10.0.0.2"])]
        );
        assert_eq!(h.metrics.value("pingap_k8s_is_leader", &[]), 1);

        // demoted again: no more writes
        h.leader.send(false).unwrap();
        sleep(10).await;
        h.source.set("web", &["10.0.0.3"], &[]);
        h.change("web");
        sleep(3000).await;
        assert_eq!(h.writer.calls().len(), 1);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn empty_never_written_and_last_kept() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| s.set("web", &[], &[]));
        sleep(3000).await;
        assert!(h.writer.calls().is_empty());

        h.source.set("web", &["10.0.0.1"], &[]);
        h.change("web");
        sleep(1000).await;
        assert_eq!(h.writer.calls_for("web"), vec![addrs(&["10.0.0.1"])]);

        h.source.set("web", &[], &[]);
        h.change("web");
        sleep(3000).await;
        assert_eq!(h.writer.calls().len(), 1);
        // a resync while empty re-asserts the last addrs, never an empty list
        sleep(RESYNC.as_millis() as u64).await;
        for (_, a, _) in h.writer.calls() {
            assert_eq!(a, addrs(&["10.0.0.1"]));
        }
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn drain_fallback_gauge() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &[], &["10.0.0.9"])
        });
        sleep(10).await;
        let gauge = || {
            h.metrics
                .value("pingap_k8s_drain_fallback", &[("upstream", "web")])
        };
        assert_eq!(gauge(), 1);
        assert_eq!(h.writer.calls_for("web"), vec![addrs(&["10.0.0.9"])]);
        assert_eq!(
            h.metrics.value(
                "pingap_k8s_endpoints",
                &[("upstream", "web"), ("class", "draining")]
            ),
            1
        );

        h.source.set("web", &["10.0.0.1"], &["10.0.0.9"]);
        h.change("web");
        sleep(1000).await;
        assert_eq!(gauge(), 0);
        assert_eq!(h.writer.calls_for("web")[1], addrs(&["10.0.0.1"]));
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn etcd_failure_retries_with_latest_state() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        h.writer.fail(3, || anyhow::anyhow!("etcd unavailable"));
        sleep(10).await;
        assert_eq!(h.writer.calls().len(), 1);
        assert!(!h.readiness_last_write_ok());
        assert_eq!(h.metrics.value("pingap_k8s_etcd_up", &[]), 0);

        // state changes while etcd is down: the latest one wins
        h.source.set("web", &["10.0.0.2"], &[]);
        h.change("web");
        sleep(10_000).await;
        let calls = h.writer.calls_for("web");
        assert_eq!(calls.len(), 4, "{calls:?}");
        assert_eq!(calls.last().unwrap(), &addrs(&["10.0.0.2"]));
        assert_eq!(write_count(&h.metrics, "web", "error"), 3);
        assert_eq!(write_count(&h.metrics, "web", "ok"), 1);
        assert!(h.readiness_last_write_ok());
        assert_eq!(h.metrics.value("pingap_k8s_etcd_up", &[]), 1);

        // backoff: retry delays grow (0.4-0.6 s, then 0.8-1.2 s)
        let t = h.writer.calls();
        let gaps: Vec<Duration> = t.windows(2).map(|w| w[1].2 - w[0].2).collect();
        assert!(
            gaps.iter().all(|g| *g <= Duration::from_secs(3)),
            "{gaps:?}"
        );
        sleep(60_000).await;
        assert_eq!(h.writer.calls().len(), 4);
        h.stop().await;
    }

    impl Harness {
        fn readiness_last_write_ok(&self) -> bool {
            self.readiness.set_watchers_synced(true);
            self.readiness.is_ready()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn foreign_put_stale_is_corrected_and_correct_is_ignored() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1", "10.0.0.2"], &[])
        });
        sleep(10).await;
        h.writer.clear();

        // correct addrs: nothing to do
        h.foreign
            .send(ForeignWrite::Put {
                upstream: "web".into(),
                addrs: Some(addrs(&["10.0.0.1", "10.0.0.2"])),
                revision: 10,
            })
            .await
            .unwrap();
        sleep(3000).await;
        assert!(h.writer.calls().is_empty());
        let fw = |c: &str| {
            h.metrics.value(
                "pingap_k8s_foreign_writes_total",
                &[("upstream", "web"), ("corrected", c)],
            )
        };
        assert_eq!(fw("false"), 1);

        // stale addrs: corrective write through the debounce
        lock(&h.writer.stored).insert("web".into(), addrs(&["10.0.0.1"]));
        let start = Instant::now();
        h.foreign
            .send(ForeignWrite::Put {
                upstream: "web".into(),
                addrs: Some(addrs(&["10.0.0.1"])),
                revision: 11,
            })
            .await
            .unwrap();
        sleep(3000).await;
        let calls = h.writer.calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, addrs(&["10.0.0.1", "10.0.0.2"]));
        assert!(calls[0].2 - start >= Duration::from_millis(500));
        assert_eq!(fw("true"), 1);
        assert_eq!(write_count(&h.metrics, "web", "ok"), 2);

        // unknown upstream is ignored
        h.foreign
            .send(ForeignWrite::Put {
                upstream: "other".into(),
                addrs: None,
                revision: 12,
            })
            .await
            .unwrap();
        sleep(3000).await;
        assert_eq!(h.writer.calls().len(), 1);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn foreign_write_during_inflight_write_is_still_corrected() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        h.writer.clear();

        // Our write of the new list is committed but its result is late.
        let gate = Arc::new(Notify::new());
        *lock(&h.writer.gate) = Some(Arc::clone(&gate));
        h.source.set("web", &["10.0.0.1", "10.0.0.2"], &[]);
        h.change("web");
        sleep(600).await;
        assert_eq!(h.writer.calls().len(), 1);

        // A stale admin save lands in etcd after our write, and its watch
        // event arrives before our write's result.
        lock(&h.writer.stored).insert("web".into(), addrs(&["10.0.0.1"]));
        h.foreign
            .send(ForeignWrite::Put {
                upstream: "web".into(),
                addrs: Some(addrs(&["10.0.0.1"])),
                revision: 20,
            })
            .await
            .unwrap();
        sleep(10).await;
        *lock(&h.writer.gate) = None;
        gate.notify_one();

        sleep(3000).await;
        let calls = h.writer.calls_for("web");
        assert_eq!(calls.len(), 2, "the stale save must be corrected");
        assert_eq!(calls[1], addrs(&["10.0.0.1", "10.0.0.2"]));
        assert_eq!(
            lock(&h.writer.stored).get("web"),
            Some(&addrs(&["10.0.0.1", "10.0.0.2"]))
        );
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn foreign_delete_during_inflight_write_is_recreated() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        h.writer.clear();

        let gate = Arc::new(Notify::new());
        *lock(&h.writer.gate) = Some(Arc::clone(&gate));
        h.source.set("web", &["10.0.0.2"], &[]);
        h.change("web");
        sleep(600).await;

        lock(&h.writer.stored).remove("web");
        h.foreign
            .send(ForeignWrite::Deleted {
                upstream: "web".into(),
                revision: 20,
            })
            .await
            .unwrap();
        sleep(10).await;
        *lock(&h.writer.gate) = None;
        gate.notify_one();

        sleep(3000).await;
        assert_eq!(h.writer.calls_for("web").len(), 2);
        assert_eq!(
            lock(&h.writer.stored).get("web"),
            Some(&addrs(&["10.0.0.2"]))
        );
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn wait_applied_completes_after_the_loop_applied_the_set() {
        let h = Harness::start(&[("a", &["a"])], true, 64, |s| {
            s.set("a", &["10.0.0.1"], &[]);
            s.set("b", &["10.0.0.2"], &[]);
        });
        sleep(10).await;
        let mut next = BTreeMap::new();
        next.insert("b".to_string(), upstream(&["b"]));
        let generation = h.handle.set_upstreams(next.clone());
        tokio::time::timeout(Duration::from_secs(1), h.handle.wait_applied(generation))
            .await
            .expect("applied");
        // A newer set supersedes; waiting for an older generation returns at once.
        let newer = h.handle.set_upstreams(next);
        assert!(newer > generation);
        tokio::time::timeout(Duration::from_secs(1), h.handle.wait_applied(newer))
            .await
            .expect("applied");
        tokio::time::timeout(Duration::from_millis(1), h.handle.wait_applied(generation))
            .await
            .expect("older generation");
        h.stop().await;

        // Not running: the wait does not complete.
        let rec = Reconciler::new(
            Arc::new(FakeWriter::default()),
            Arc::new(FakeSource::default()),
            Arc::new(Metrics::new()),
            Readiness::new(),
        );
        let generation = rec.set_upstreams(BTreeMap::new());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), rec.wait_applied(generation))
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn conflict_is_counted_and_retried() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        h.writer.fail(1, || {
            anyhow::Error::new(EtcdError::Conflict {
                key: "/pingap/upstreams/web.toml".into(),
                attempts: 5,
            })
            .context("writing web")
        });
        sleep(10).await;
        assert_eq!(write_count(&h.metrics, "web", "conflict"), 1);
        assert_eq!(write_count(&h.metrics, "web", "error"), 0);
        assert_eq!(h.metrics.value("pingap_k8s_etcd_up", &[]), 1);
        assert!(h.readiness_last_write_ok());
        sleep(1000).await;
        assert_eq!(h.writer.calls().len(), 2);
        assert_eq!(write_count(&h.metrics, "web", "ok"), 1);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn malformed_entry_is_not_retried_in_a_tight_loop() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        h.writer
            .fail(100, || EtcdError::Toml("expected `=`".into()).into());
        sleep(60_000).await;
        assert_eq!(h.writer.calls().len(), 1);
        assert_eq!(write_count(&h.metrics, "web", "error"), 1);
        // etcd answered: a malformed entry does not fail readiness
        assert!(h.readiness_last_write_ok());
        assert_eq!(h.metrics.value("pingap_k8s_etcd_up", &[]), 1);

        // the next event retries
        h.source.set("web", &["10.0.0.2"], &[]);
        h.change("web");
        sleep(1000).await;
        assert_eq!(h.writer.calls().len(), 2);

        // fixed: the slow retry succeeds
        lock(&h.writer.failures).clear();
        sleep(MALFORMED_RETRY.as_millis() as u64 * 5 / 4).await;
        assert_eq!(
            h.writer.calls_for("web").last().unwrap(),
            &addrs(&["10.0.0.2"])
        );
        assert_eq!(write_count(&h.metrics, "web", "ok"), 1);
        assert!(h.readiness_last_write_ok());
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn only_connectivity_failures_affect_readiness() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        h.writer.fail(1, || anyhow::anyhow!("connection refused"));
        sleep(10).await;
        assert!(!h.readiness_last_write_ok());

        // the retry reaches etcd but finds a malformed entry: ready again
        h.writer
            .fail(100, || EtcdError::MissingTable("web".into()).into());
        sleep(1000).await;
        assert_eq!(h.writer.calls().len(), 2);
        assert_eq!(write_count(&h.metrics, "web", "error"), 2);
        assert!(h.readiness_last_write_ok());
        h.stop().await;
    }

    impl FakeSource {
        /// Renames the Service port of `name` (its slices keep "http").
        fn rename_port(&self, name: &str, port_name: &str) {
            let mut snaps = lock(&self.snaps);
            let snap = snaps.get_mut(&key(name)).unwrap();
            let mut svc = (**snap.service.as_ref().unwrap()).clone();
            svc.metadata.uid = Some(format!("uid-{name}"));
            svc.spec.as_mut().unwrap().ports.as_mut().unwrap()[0].name = Some(port_name.into());
            snap.service = Some(Arc::new(svc));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn leader_posts_resolve_warning_events_once_per_episode() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        assert!(h.notices().is_empty());

        // Port renamed: one Warning on the Service, however often it is reconciled.
        h.source.rename_port("web", "other");
        for _ in 0..3 {
            h.change("web");
            sleep(1000).await;
        }
        assert_eq!(h.notices(), vec![("PortNotInService", "web".to_string())]);
        let posted = {
            h.source.rename_port("web", "other");
            h.change("web");
            sleep(1000).await;
            h.notices()
        };
        assert!(posted.is_empty(), "{posted:?}");

        // Fixed: nothing new; broken again: a new episode, a new Event.
        h.source.set("web", &["10.0.0.1"], &[]);
        h.change("web");
        sleep(1000).await;
        assert!(h.notices().is_empty());
        lock(&h.source.snaps).remove(&key("web"));
        h.change("web");
        sleep(1000).await;
        assert_eq!(h.notices(), vec![("ServiceMissing", "web".to_string())]);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn followers_post_nothing_until_promoted() {
        let h = Harness::start(&[("web", &["web"])], false, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        h.source.rename_port("web", "other");
        h.change("web");
        sleep(1000).await;
        assert!(h.notices().is_empty());

        // The new leader reports the episode already open, once.
        h.leader.send(true).unwrap();
        sleep(3000).await;
        let n = lock(&h.events).try_recv().unwrap();
        assert_eq!(n.reason, "PortNotInService");
        assert_eq!(
            n.regarding,
            crate::events::Regarding::Service {
                key: key("web"),
                uid: Some("uid-web".into())
            }
        );
        assert!(h.notices().is_empty());
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn drain_fallback_and_malformed_events() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        h.source.set("web", &[], &["10.0.0.1"]);
        h.change("web");
        sleep(1000).await;
        assert_eq!(
            h.notices(),
            vec![("DrainFallbackStarted", "web".to_string())]
        );
        h.source.set("web", &["10.0.0.2"], &[]);
        h.change("web");
        sleep(1000).await;
        assert_eq!(h.notices(), vec![("DrainFallbackEnded", "web".to_string())]);

        // A malformed entry: one Event on the Pod per episode.
        h.writer
            .fail(100, || EtcdError::Toml("expected `=`".into()).into());
        for i in 3..6 {
            h.source.set("web", &[&format!("10.0.0.{i}")], &[]);
            h.change("web");
            sleep(1000).await;
        }
        assert_eq!(
            h.notices(),
            vec![("MalformedUpstream", "<pod>".to_string())]
        );
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn etcd_events_on_reachability_changes_only() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        // The first report (up) is not a change.
        assert!(h.notices().is_empty());
        h.writer.fail(3, || anyhow::anyhow!("connection refused"));
        h.source.set("web", &["10.0.0.2"], &[]);
        h.change("web");
        sleep(10_000).await;
        assert_eq!(
            h.notices(),
            vec![
                ("EtcdUnreachable", "<pod>".to_string()),
                ("EtcdRecovered", "<pod>".to_string())
            ]
        );
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn foreign_delete_recreates() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        h.writer.clear();
        lock(&h.writer.stored).remove("web");
        h.foreign
            .send(ForeignWrite::Deleted {
                upstream: "web".into(),
                revision: 20,
            })
            .await
            .unwrap();
        sleep(1000).await;
        assert_eq!(h.writer.calls_for("web"), vec![addrs(&["10.0.0.1"])]);
        assert_eq!(
            lock(&h.writer.stored).get("web"),
            Some(&addrs(&["10.0.0.1"]))
        );
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn foreign_resync_reconciles_all() {
        let h = Harness::start(&[("a", &["a"]), ("b", &["b"])], true, 64, |s| {
            s.set("a", &["10.0.0.1"], &[]);
            s.set("b", &["10.0.0.2"], &[]);
        });
        sleep(10).await;
        h.writer.clear();
        h.foreign.send(ForeignWrite::Resync).await.unwrap();
        sleep(1000).await;
        assert_eq!(h.writer.calls().len(), 2);
        assert_eq!(write_count(&h.metrics, "a", "unchanged"), 1);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn set_upstreams_adds_and_removes() {
        let h = Harness::start(&[("a", &["a"]), ("b", &["b"])], true, 64, |s| {
            s.set("a", &["10.0.0.1"], &[]);
            s.set("b", &["10.0.0.2"], &[]);
            s.set("c", &["10.0.0.3"], &[]);
        });
        sleep(10).await;
        assert_eq!(h.writer.calls().len(), 2);
        h.writer.clear();
        assert!(
            h.metrics
                .encode()
                .contains(r#"pingap_k8s_endpoints{class="ready",upstream="b"}"#)
        );

        let mut next = BTreeMap::new();
        next.insert("a".to_string(), upstream(&["a"]));
        next.insert("c".to_string(), upstream(&["c"]));
        h.handle.set_upstreams(next);
        sleep(1000).await;
        // only the new upstream is written; unchanged `a` is left alone
        assert_eq!(h.writer.calls_for("c"), vec![addrs(&["10.0.0.3"])]);
        assert_eq!(h.writer.calls().len(), 1);
        let text = h.metrics.encode();
        assert!(!text.contains(r#"pingap_k8s_endpoints{class="ready",upstream="b"}"#));
        assert!(!text.contains(r#"pingap_k8s_drain_fallback{upstream="b"}"#));

        // events for the removed upstream's Service no longer cause writes
        h.source.set("b", &["10.0.0.20"], &[]);
        h.change("b");
        sleep(3000).await;
        assert_eq!(h.writer.calls().len(), 1);

        // changed config of an existing upstream is reconciled
        let mut next = BTreeMap::new();
        next.insert("a".to_string(), upstream(&["a", "c"]));
        next.insert("c".to_string(), upstream(&["c"]));
        h.handle.set_upstreams(next);
        sleep(1000).await;
        assert_eq!(
            h.writer.calls_for("a"),
            vec![addrs(&["10.0.0.1", "10.0.0.3"])]
        );
        // `c` is now referenced by both upstreams
        h.writer.clear();
        h.source.set("c", &["10.0.0.4"], &[]);
        h.change("c");
        sleep(1000).await;
        assert_eq!(h.writer.calls().len(), 2);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn set_upstreams_switches_target() {
        let h = Harness::start(&[("web", &["web", "api"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[]);
            s.set("api", &["10.0.0.2"], &[]);
        });
        sleep(10).await;
        assert_eq!(
            h.writer.calls_for("web"),
            vec![addrs(&["10.0.0.1", "10.0.0.2"])]
        );
        h.writer.clear();

        let mut service_mode = upstream(&["web", "api"]);
        service_mode.options.target = UpstreamTarget::Service {
            cluster_domain: "cluster.local".into(),
        };
        h.handle
            .set_upstreams([("web".to_string(), service_mode)].into());
        // Service targets wait SERVICE_SETTLE (3 s) for kube-proxy, not the 500 ms debounce.
        sleep(2500).await;
        assert!(h.writer.calls_for("web").is_empty());
        sleep(1000).await;
        let service_addrs = vec![
            "api.prod.svc.cluster.local:80".to_string(),
            "web.prod.svc.cluster.local:80".to_string(),
        ];
        assert_eq!(h.writer.calls_for("web"), vec![service_addrs.clone()]);
        assert_eq!(
            h.metrics.value(
                "pingap_k8s_endpoints",
                &[("upstream", "web"), ("class", "ready")]
            ),
            2
        );

        // Pod churn does not change the Service addresses: no write.
        h.writer.clear();
        h.source.set("web", &["10.0.0.9"], &["10.0.0.1"]);
        h.change("web");
        sleep(3000).await;
        assert!(h.writer.calls().is_empty());

        // And back to pods.
        h.handle
            .set_upstreams([("web".to_string(), upstream(&["web", "api"]))].into());
        sleep(1000).await;
        assert_eq!(
            h.writer.calls_for("web"),
            vec![addrs(&["10.0.0.2", "10.0.0.9"])]
        );
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn lagged_marks_all_dirty() {
        let h = Harness::start(&[("a", &["a"]), ("b", &["b"])], true, 1, |s| {
            s.set("a", &["10.0.0.1"], &[]);
            s.set("b", &["10.0.0.2"], &[]);
        });
        sleep(10).await;
        h.writer.clear();
        h.source.set("a", &["10.0.0.11"], &[]);
        h.source.set("b", &["10.0.0.22"], &[]);
        // no yield in between: the loop misses events and sees Lagged
        h.change("a");
        h.change("b");
        h.change("unrelated");
        sleep(3000).await;
        assert_eq!(h.writer.calls_for("a"), vec![addrs(&["10.0.0.11"])]);
        assert_eq!(h.writer.calls_for("b"), vec![addrs(&["10.0.0.22"])]);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn resync_tick_corrects_drift() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        h.writer.clear();
        // drift that the owned-key watch missed
        lock(&h.writer.stored).insert("web".into(), addrs(&["10.9.9.9"]));
        sleep(RESYNC.as_millis() as u64 - 1000).await;
        assert!(h.writer.calls().is_empty());
        sleep(2000).await;
        assert_eq!(h.writer.calls_for("web"), vec![addrs(&["10.0.0.1"])]);
        assert_eq!(
            lock(&h.writer.stored).get("web"),
            Some(&addrs(&["10.0.0.1"]))
        );
        assert_eq!(write_count(&h.metrics, "web", "ok"), 2);
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn unchanged_desired_is_not_rewritten() {
        let h = Harness::start(&[("web", &["web"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        h.writer.clear();
        // an event that does not change the resolved list
        h.change("web");
        sleep(3000).await;
        assert!(h.writer.calls().is_empty());
        h.stop().await;
    }

    #[tokio::test(start_paused = true)]
    async fn resolve_warning_does_not_block_other_services() {
        // `missing` has no snapshot: warning, contributes nothing
        let h = Harness::start(&[("web", &["web", "missing"])], true, 64, |s| {
            s.set("web", &["10.0.0.1"], &[])
        });
        sleep(10).await;
        assert_eq!(h.writer.calls_for("web"), vec![addrs(&["10.0.0.1"])]);
        h.stop().await;
    }
}

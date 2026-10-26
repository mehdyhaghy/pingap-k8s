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
//! corrected and a save with the right `addrs` causes no write.
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
//! Startup : the caller must wait for the watchers' first list before
//! calling [`Reconciler::run`]; `run` reconciles every upstream right away.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::{Future, pending};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::Rng;
use tokio::sync::{Notify, broadcast, mpsc, watch};
use tokio::task::{Id, JoinSet};
use tokio::time::{Instant, MissedTickBehavior, sleep_until};

use crate::config::UpstreamConfig;
use crate::etcd::EtcdError;
use crate::metrics::{Metrics, WriteResult};
use crate::resolve::{self, ResolveWarning, ServiceInput};
use crate::server::Readiness;
use crate::types::{AddrsWriter, ForeignWrite, ServiceKey, SnapshotSource, WriteOutcome};

/// First retry delay after a failed write.
pub const RETRY_MIN: Duration = Duration::from_millis(500);
/// Maximum retry delay after repeated failed writes.
pub const RETRY_MAX: Duration = Duration::from_secs(30);
/// Retry delay for a malformed stored entry. An event or resync
/// retries earlier.
pub const MALFORMED_RETRY: Duration = Duration::from_secs(300);

/// Inputs that can be handed to a running [`Reconciler`] from another task.
struct Shared {
    upstreams: Mutex<Option<BTreeMap<String, UpstreamConfig>>>,
    foreign: Mutex<Option<mpsc::Receiver<ForeignWrite>>>,
    notify: Notify,
}

/// The reconcile loop. Clone it before calling [`Reconciler::run`] to keep a
/// handle for [`Reconciler::set_upstreams`] (config reload).
pub struct Reconciler<W: AddrsWriter, S: SnapshotSource> {
    writer: Arc<W>,
    source: Arc<S>,
    metrics: Arc<Metrics>,
    readiness: Readiness,
    shared: Arc<Shared>,
}

impl<W: AddrsWriter, S: SnapshotSource> Clone for Reconciler<W, S> {
    fn clone(&self) -> Self {
        Reconciler {
            writer: Arc::clone(&self.writer),
            source: Arc::clone(&self.source),
            metrics: Arc::clone(&self.metrics),
            readiness: self.readiness.clone(),
            shared: Arc::clone(&self.shared),
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
            shared: Arc::new(Shared {
                upstreams: Mutex::new(None),
                foreign: Mutex::new(None),
                notify: Notify::new(),
            }),
        }
    }

    /// Replaces the set of owned upstreams. Works before and while `run` is
    /// running (on any clone). New and changed upstreams are reconciled,
    /// removed ones are dropped together with their gauges, unchanged ones
    /// keep their debounce and retry state. The latest call wins.
    pub fn set_upstreams(&self, upstreams: BTreeMap<String, UpstreamConfig>) {
        *lock(&self.shared.upstreams) = Some(upstreams);
        self.shared.notify.notify_one();
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
            metrics: self.metrics,
            readiness: self.readiness,
            upstreams: BTreeMap::new(),
            by_service: HashMap::new(),
            leader: is_leader,
            inflight: JoinSet::new(),
            inflight_ids: HashMap::new(),
            failing: BTreeSet::new(),
        };
        engine.metrics.set_leader(is_leader);
        if let Some(upstreams) = lock(&self.shared.upstreams).take() {
            engine.apply_upstreams(upstreams, Instant::now());
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
                    if let Some(upstreams) = lock(&self.shared.upstreams).take() {
                        engine.apply_upstreams(upstreams, now);
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
    empty: bool,
    /// The stored entry is malformed; logged once per episode.
    malformed: bool,
    fallback: bool,
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
            empty: false,
            malformed: false,
            fallback: false,
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

    fn deadline(&self) -> Option<Instant> {
        let debounce = self.debounce.map(|d| {
            (d.last + self.config.options.debounce).min(d.first + self.config.options.debounce_max)
        });
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
    upstreams: BTreeMap<String, UpstreamState>,
    by_service: HashMap<ServiceKey, Vec<String>>,
    leader: bool,
    inflight: JoinSet<WriteMsg>,
    inflight_ids: HashMap<Id, String>,
    /// Upstreams whose last write attempt failed (drives readiness).
    failing: BTreeSet<String>,
}

impl<W: AddrsWriter, S: SnapshotSource> Engine<W, S> {
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
            self.failing.remove(&name);
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
                st.last_written = None;
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
                st.last_written = addrs;
                if correct {
                    tracing::debug!(%upstream, revision, "foreign write kept the desired addrs");
                } else {
                    tracing::warn!(%upstream, revision, "foreign write changed addrs; scheduling a corrective write");
                    st.mark(now, true);
                }
            }
            ForeignWrite::Deleted { upstream, revision } => {
                let Some(st) = self.upstreams.get_mut(&upstream) else {
                    return;
                };
                tracing::warn!(%upstream, revision, "owned upstream key deleted; scheduling recreation");
                self.metrics.record_foreign_write(&upstream, true);
                st.last_written = None;
                st.mark(now, true);
            }
        }
    }

    fn set_leader(&mut self, leader: bool, now: Instant) {
        if leader == self.leader {
            return;
        }
        self.leader = leader;
        self.metrics.set_leader(leader);
        if leader {
            tracing::info!("became leader; reconciling all upstreams");
            // Another replica may have written meanwhile: forget what we
            // believe is stored and let the writer compare.
            self.mark_all(now, false, true);
        } else {
            tracing::info!("lost leadership; writes stop");
            for st in self.upstreams.values_mut() {
                st.retry_at = None;
                st.backoff = RETRY_MIN;
            }
            self.failing.clear();
            self.update_readiness();
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

        // Each distinct warning once, until it clears.
        let current: BTreeSet<ResolveWarning> = resolved.warnings.iter().cloned().collect();
        for w in current.difference(&st.warnings) {
            tracing::warn!(upstream = %name, "{w}");
        }
        for w in st.warnings.difference(&current) {
            tracing::info!(upstream = %name, "resolved: {w}");
        }
        st.warnings = current;

        // D2 fallback, logged on enter and exit.
        if resolved.fallback != st.fallback {
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
        self.metrics.set_drain_fallback(name, resolved.fallback);

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
                if self.failing.remove(name) {
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
                st.last_written = Some(addrs);
                st.backoff = RETRY_MIN;
                self.metrics.set_etcd_up(true);
                self.failing.remove(&name);
                self.update_readiness();
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
        // A malformed entry or a CAS conflict means etcd answered.
        self.metrics.set_etcd_up(kind != FailureKind::Other);
        // A write that finishes after demotion is not retried and does not
        // affect readiness (followers never write).
        if !self.leader {
            return;
        }
        let Some(st) = self.upstreams.get_mut(name) else {
            return;
        };
        self.failing.insert(name.to_string());
        let err = format!("{err:#}");
        if kind == FailureKind::Malformed {
            // Retrying fast cannot fix a broken entry: retry slowly, or
            // earlier when the next event or resync fires.
            st.retry_at = Some(now + jitter(MALFORMED_RETRY));
            if !st.malformed {
                st.malformed = true;
                tracing::error!(upstream = %name, error = %err, "stored upstream entry is malformed; not written until it is fixed");
            }
        } else {
            let delay = jitter(st.backoff);
            st.backoff = (st.backoff * 2).min(RETRY_MAX);
            st.retry_at = Some(now + delay);
            tracing::warn!(upstream = %name, error = %err, retry_in = ?delay, "writing addrs failed; will retry");
        }
        self.update_readiness();
    }

    fn update_readiness(&self) {
        self.readiness.set_last_write_ok(self.failing.is_empty());
    }
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
            | EtcdError::InvalidName(_),
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

    use crate::types::{PortRef, ServiceRef, ServiceSnapshot, UpstreamOptions};

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
    }

    impl AddrsWriter for FakeWriter {
        fn write_addrs(
            &self,
            upstream: &str,
            addrs: &[String],
        ) -> impl Future<Output = anyhow::Result<WriteOutcome>> + Send {
            lock(&self.calls).push((upstream.to_string(), addrs.to_vec(), Instant::now()));
            let fail = lock(&self.failures).pop_front();
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
            async move { result }
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
            let rec = Reconciler::new(
                Arc::clone(&writer),
                Arc::clone(&source),
                Arc::clone(&metrics),
                readiness.clone(),
            );
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
                stop: Some(stop),
                task,
            }
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
        m.writes_total.with_label_values(&[upstream, result]).get()
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
        assert_eq!(h.metrics.etcd_up.get(), 1);
        assert_eq!(h.metrics.is_leader.get(), 1);
        sleep(5000).await;
        assert_eq!(h.writer.calls().len(), 1);
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
        assert_eq!(h.metrics.event_to_write_seconds.get_sample_count(), 1);
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
            h.metrics
                .endpoints
                .with_label_values(&["web", "ready"])
                .get(),
            2
        );
        assert_eq!(h.metrics.is_leader.get(), 0);

        h.leader.send(true).unwrap();
        sleep(3000).await;
        assert_eq!(
            h.writer.calls_for("web"),
            vec![addrs(&["10.0.0.1", "10.0.0.2"])]
        );
        assert_eq!(h.metrics.is_leader.get(), 1);

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
        let gauge = || h.metrics.drain_fallback.with_label_values(&["web"]).get();
        assert_eq!(gauge(), 1);
        assert_eq!(h.writer.calls_for("web"), vec![addrs(&["10.0.0.9"])]);
        assert_eq!(
            h.metrics
                .endpoints
                .with_label_values(&["web", "draining"])
                .get(),
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
        assert_eq!(h.metrics.etcd_up.get(), 0);

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
        assert_eq!(h.metrics.etcd_up.get(), 1);

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
            h.metrics
                .foreign_writes_total
                .with_label_values(&["web", c])
                .get()
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
        assert_eq!(h.metrics.etcd_up.get(), 1);
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
        assert!(!h.readiness_last_write_ok());

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

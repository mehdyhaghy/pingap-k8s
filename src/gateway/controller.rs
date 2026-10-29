//! The Gateway API controller loop: ties the Kubernetes sources, the translation, the
//! endpoint writer and the etcd store together.
//!
//! One task runs one `select!` loop. A pass is triggered by Gateway API object,
//! Namespace, Secret and backend Service changes, etcd changes, settings changes
//! (config reload), the deferred-retry timer and the resync tick, debounced like the
//! endpoint reconciler (500 ms after the last trigger, at most 2 s after the first).
//! Promotion to leader and startup run a pass at once.
//!
//! A pass is level-triggered and does, in order:
//! 1. wait until the Gateway API watches have listed once, translate a first time to
//!    learn what the handled Gateways and routes reference, and point the Secret
//!    watches at the referenced Secrets;
//! 2. wait until every source has listed once: the Secret watches and the backend
//!    Services (the endpoint writer watches those; a [`GeneratedUpstreams`] with the new
//!    Service set returns `applied` once they listed). Nothing is planned, written or
//!    deleted before that;
//! 3. read the stored Pingap entries (their servers and plugins feed the translation)
//!    and translate;
//! 4. hand the generated pods/service upstreams to the endpoint writer and wait for
//!    `applied`: from then on it keeps them current and no longer writes the old ones,
//!    so the plan may delete those;
//! 5. plan. The leader applies the plan; a failed apply (CAS conflict or etcd error)
//!    re-reads and re-plans up to [`APPLY_ATTEMPTS`] times, then the pass is retried
//!    with backoff (0.5 s doubling to 30 s);
//! 6. the leader writes the status of the handled GatewayClasses, Gateways and
//!    HTTPRoutes where it differs from the stored one.
//!
//! Followers run steps 1 to 5 without applying, so their gauges and the endpoint
//! writer's desired state are ready for a promotion; they never write to etcd or to the
//! Kubernetes API. Locations deferred by the plan (upstream not written yet, plugin
//! missing) are retried after 1 s, doubling to 30 s.
//!
//! The pure translation and the plan are reached through [`Logic`]; [`Wired`] is the
//! production implementation.

use std::collections::{BTreeMap, BTreeSet};
use std::future::{Future, pending};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, anyhow, bail};
use k8s_openapi::api::core::v1::Service;
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior, sleep_until};
use tracing::{debug, info, warn};

use crate::config::{Config, UpstreamConfig};
use crate::gateway::model::{
    ApplyOutcome, Desired, GatewaySource, GeneratedUpstreams, ObjectKey, PingapStore, Plan,
    Referenced, SecretSource, ServerInfo, StatusPublisher, Stored, TranslateInput,
    TranslateOptions,
};
use crate::gateway::{plan, render, translate};
use crate::metrics::{GatewaySyncResult, Metrics};
use crate::types::{ServiceKey, SnapshotSource};

/// Quiet period after a trigger before a pass.
pub const DEBOUNCE: Duration = Duration::from_millis(500);
/// A pass runs at most this long after the first pending trigger.
pub const DEBOUNCE_MAX: Duration = Duration::from_secs(2);
/// Retry of deferred locations: first delay, doubling up to the maximum.
pub const DEFERRED_RETRY_MIN: Duration = Duration::from_secs(1);
pub const DEFERRED_RETRY_MAX: Duration = Duration::from_secs(30);
/// Retry after a failed pass: first delay, doubling up to the maximum.
pub const ERROR_RETRY_MIN: Duration = Duration::from_millis(500);
pub const ERROR_RETRY_MAX: Duration = Duration::from_secs(30);
/// Read, plan and apply attempts in one pass before backing off.
pub const APPLY_ATTEMPTS: u32 = 3;
/// How long a pass waits for the endpoint writer to take over generated upstreams.
pub const APPLIED_TIMEOUT: Duration = Duration::from_secs(30);
/// Check again this soon while a source has not listed yet.
const NOT_SYNCED_RETRY: Duration = Duration::from_secs(1);

/// Plugins every Pingap has, besides the ones stored in etcd.
pub const PINGAP_BUILTIN_PLUGINS: &[&str] = crate::gateway::plan::BUILTIN_PLUGINS;

/// The part of the config the controller uses. Everything here is applied on reload.
#[derive(Clone, Debug)]
pub struct ControllerSettings {
    pub translate: TranslateOptions,
}

impl ControllerSettings {
    /// `None` when `[gateway]` is off.
    pub fn from_config(config: &Config) -> Option<Self> {
        let gateway = config.gateway.as_ref()?;
        Some(ControllerSettings {
            translate: TranslateOptions {
                controller_name: gateway.controller_name.clone(),
                external_name: gateway.external_name,
                upstream_defaults: config.defaults.upstream_options(),
                health_check: gateway.health_check.clone(),
                cluster_domain: config.kubernetes.cluster_domain.clone(),
                servers: gateway.servers.clone(),
                addresses: gateway.addresses.clone(),
            },
        })
    }
}

impl PartialEq for ControllerSettings {
    fn eq(&self, other: &Self) -> bool {
        let (a, b) = (&self.translate, &other.translate);
        a.controller_name == b.controller_name
            && a.external_name == b.external_name
            && a.upstream_defaults == b.upstream_defaults
            && a.health_check == b.health_check
            && a.cluster_domain == b.cluster_domain
            && a.servers == b.servers
            && a.addresses == b.addresses
    }
}

/// The pure functions of the other parts, behind a trait so the loop can be tested
/// with fakes.
pub trait Logic: Send + Sync + 'static {
    fn referenced(&self, input: &TranslateInput<'_>) -> Referenced;
    fn translate(&self, input: &TranslateInput<'_>) -> Desired;
    fn plan(&self, desired: &Desired, stored: &Stored, keep_upstreams: &BTreeSet<String>) -> Plan;
}

/// The production [`Logic`]: the translation and planning functions. `prefix` is the
/// etcd prefix of the Pingap config, for the keys of new entries.
#[derive(Clone, Debug, Default)]
pub struct Wired {
    pub prefix: String,
}

impl Logic for Wired {
    fn referenced(&self, input: &TranslateInput<'_>) -> Referenced {
        translate::referenced(input)
    }

    fn translate(&self, input: &TranslateInput<'_>) -> Desired {
        translate::translate(input)
    }

    fn plan(&self, desired: &Desired, stored: &Stored, keep_upstreams: &BTreeSet<String>) -> Plan {
        plan::plan(&plan::PlanInput {
            prefix: &self.prefix,
            desired,
            stored,
            keep_upstreams,
        })
    }
}

/// What the controller reads from and writes to.
pub struct Parts<G, S, V, St, P, L> {
    pub gateways: Arc<G>,
    pub secrets: Arc<S>,
    /// Backend Services, as watched by the endpoint writer.
    pub services: Arc<V>,
    pub store: Arc<St>,
    pub status: Arc<P>,
    pub logic: Arc<L>,
    pub metrics: Arc<Metrics>,
}

/// The channels of a running controller.
pub struct Inputs {
    pub settings: watch::Receiver<ControllerSettings>,
    pub leader: watch::Receiver<bool>,
    /// Backend Service changes (the endpoint writer's watches).
    pub service_changes: broadcast::Receiver<ServiceKey>,
    /// Changes to the Pingap entries in etcd.
    pub etcd_changes: mpsc::Receiver<()>,
    /// To the endpoint writer.
    pub generated: mpsc::Sender<GeneratedUpstreams>,
    /// Set to true after the first complete pass (every source listed once).
    pub synced: watch::Sender<bool>,
    pub resync: Duration,
}

/// Waits until `source` has listed once, at most `timeout`. Returns whether it has.
pub async fn wait_source_synced<G: GatewaySource + ?Sized>(source: &G, timeout: Duration) -> bool {
    let mut changes = source.changes();
    let wait = async {
        while !source.is_synced() {
            // A change, or a periodic look in case the signal came before subscribing.
            let _ = tokio::time::timeout(Duration::from_millis(250), changes.recv()).await;
        }
    };
    tokio::time::timeout(timeout, wait).await.is_ok()
}

/// How a pass ended.
#[derive(Debug, PartialEq, Eq)]
enum PassOutcome {
    /// A source has not listed yet; nothing was planned.
    NotSynced(&'static str),
    Done {
        deferred: bool,
    },
}

/// Pending debounce.
#[derive(Clone, Copy, Debug)]
struct Dirty {
    first: Instant,
    last: Instant,
    immediate: bool,
}

impl Dirty {
    fn deadline(&self) -> Instant {
        if self.immediate {
            self.first
        } else {
            (self.last + DEBOUNCE).min(self.first + DEBOUNCE_MAX)
        }
    }
}

fn mark(dirty: &mut Option<Dirty>, now: Instant) {
    match dirty {
        Some(d) => d.last = now,
        None => {
            *dirty = Some(Dirty {
                first: now,
                last: now,
                immediate: false,
            })
        }
    }
}

fn mark_now(dirty: &mut Option<Dirty>, now: Instant) {
    *dirty = Some(Dirty {
        first: now,
        last: now,
        immediate: true,
    });
}

/// A snapshot of the Gateway API objects, for one pass.
struct Objects {
    classes: Vec<Arc<crate::gateway::api::gatewayclasses::GatewayClass>>,
    gateways: Vec<Arc<crate::gateway::api::gateways::Gateway>>,
    routes: Vec<Arc<crate::gateway::api::httproutes::HttpRoute>>,
    grants: Vec<Arc<crate::gateway::api::referencegrants::ReferenceGrant>>,
    namespaces: BTreeMap<String, Arc<k8s_openapi::api::core::v1::Namespace>>,
}

pub struct GatewayController<G, S, V, St, P, L> {
    parts: Parts<G, S, V, St, P, L>,
    settings: Option<ControllerSettings>,
    leader: bool,
    generated: Option<mpsc::Sender<GeneratedUpstreams>>,
    /// Backend Services the endpoint writer watches for us (confirmed by `applied`).
    services: BTreeSet<ServiceKey>,
    /// Referenced by the handled routes (Service changes outside it are ignored).
    watched: BTreeSet<ServiceKey>,
    /// Generated upstreams the endpoint writer has taken over.
    upstreams: Option<BTreeMap<String, UpstreamConfig>>,
}

impl<G, S, V, St, P, L> GatewayController<G, S, V, St, P, L>
where
    G: GatewaySource,
    S: SecretSource,
    V: SnapshotSource,
    St: PingapStore,
    P: StatusPublisher,
    L: Logic,
{
    pub fn new(parts: Parts<G, S, V, St, P, L>) -> Self {
        GatewayController {
            parts,
            settings: None,
            leader: false,
            generated: None,
            services: BTreeSet::new(),
            watched: BTreeSet::new(),
            upstreams: None,
        }
    }

    /// Runs until `shutdown` resolves.
    pub async fn run(mut self, inputs: Inputs, shutdown: impl Future<Output = ()>) {
        let Inputs {
            mut settings,
            mut leader,
            service_changes,
            etcd_changes,
            generated,
            synced,
            resync,
        } = inputs;
        self.settings = Some(settings.borrow_and_update().clone());
        self.leader = *leader.borrow_and_update();
        self.generated = Some(generated);

        let mut gateway_changes = Some(self.parts.gateways.changes());
        let mut secret_changes = Some(self.parts.secrets.changes());
        let mut service_changes = Some(service_changes);
        let mut etcd_changes = Some(etcd_changes);
        let mut settings_open = true;
        let mut leader_open = true;
        let resync = resync.max(Duration::from_millis(1));
        let mut resync_tick = tokio::time::interval_at(Instant::now() + resync, resync);
        resync_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut shutdown = std::pin::pin!(shutdown);

        let mut dirty = None;
        mark_now(&mut dirty, Instant::now());
        let mut retry_at: Option<Instant> = None;
        let mut deferred_delay = DEFERRED_RETRY_MIN;
        let mut error_delay = ERROR_RETRY_MIN;
        info!(leader = self.leader, "gateway controller started");

        loop {
            let deadline = match (dirty.as_ref().map(Dirty::deadline), retry_at) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            let now_or_later = tokio::select! {
                biased;
                () = &mut shutdown => {
                    info!("gateway controller shutting down");
                    return;
                }
                r = settings.changed(), if settings_open => {
                    match r {
                        Ok(()) => {
                            let new = settings.borrow_and_update().clone();
                            self.apply_settings(new);
                            mark(&mut dirty, Instant::now());
                        }
                        Err(_) => settings_open = false,
                    }
                    false
                }
                r = leader.changed(), if leader_open => {
                    match r {
                        Ok(()) => {
                            let is_leader = *leader.borrow_and_update();
                            if self.set_leader(is_leader) {
                                mark_now(&mut dirty, Instant::now());
                            }
                        }
                        Err(_) => {
                            warn!("leader channel closed; keeping the last leadership state");
                            leader_open = false;
                        }
                    }
                    false
                }
                r = recv_broadcast(&mut gateway_changes) => {
                    if signal_or_close(r, &mut gateway_changes, "Gateway API") {
                        mark(&mut dirty, Instant::now());
                    }
                    false
                }
                r = recv_broadcast(&mut secret_changes) => {
                    if signal_or_close(r, &mut secret_changes, "Secret") {
                        mark(&mut dirty, Instant::now());
                    }
                    false
                }
                r = recv_broadcast(&mut service_changes) => {
                    match r {
                        Ok(key) => if self.watched.contains(&key) {
                            mark(&mut dirty, Instant::now());
                        },
                        Err(broadcast::error::RecvError::Lagged(_)) => mark(&mut dirty, Instant::now()),
                        Err(broadcast::error::RecvError::Closed) => {
                            warn!("Service change channel closed");
                            service_changes = None;
                        }
                    }
                    false
                }
                r = recv_mpsc(&mut etcd_changes) => {
                    match r {
                        Some(()) => mark(&mut dirty, Instant::now()),
                        None => {
                            warn!("etcd change channel closed");
                            etcd_changes = None;
                        }
                    }
                    false
                }
                _ = resync_tick.tick() => {
                    debug!("gateway resync");
                    mark(&mut dirty, Instant::now());
                    false
                }
                () = sleep_or_pending(deadline) => true,
            };
            if !now_or_later {
                continue;
            }

            dirty = None;
            retry_at = None;
            let result = tokio::select! {
                biased;
                () = &mut shutdown => {
                    info!("gateway controller shutting down");
                    return;
                }
                r = self.pass() => r,
            };
            let now = Instant::now();
            match result {
                Ok(PassOutcome::NotSynced(what)) => {
                    debug!(
                        source = what,
                        "gateway sources have not listed yet; not planning"
                    );
                    retry_at = Some(now + NOT_SYNCED_RETRY);
                }
                Ok(PassOutcome::Done { deferred }) => {
                    error_delay = ERROR_RETRY_MIN;
                    synced.send_if_modified(|s| !std::mem::replace(s, true));
                    if deferred {
                        retry_at = Some(now + deferred_delay);
                        deferred_delay = (deferred_delay * 2).min(DEFERRED_RETRY_MAX);
                    } else {
                        deferred_delay = DEFERRED_RETRY_MIN;
                    }
                }
                Err(err) => {
                    warn!(retry_in = ?error_delay, "gateway sync failed: {err:#}");
                    retry_at = Some(now + error_delay);
                    error_delay = (error_delay * 2).min(ERROR_RETRY_MAX);
                }
            }
        }
    }

    fn settings(&self) -> &ControllerSettings {
        self.settings.as_ref().expect("settings are set in run")
    }

    fn apply_settings(&mut self, new: ControllerSettings) {
        if self.settings.as_ref() == Some(&new) {
            return;
        }
        info!("gateway settings changed");
        self.settings = Some(new);
    }

    /// Returns true on promotion.
    fn set_leader(&mut self, leader: bool) -> bool {
        if leader == self.leader {
            return false;
        }
        self.leader = leader;
        if leader {
            info!("became leader; syncing Gateway API objects");
        } else {
            info!("lost leadership; Gateway writes stop");
        }
        leader
    }

    fn objects(&self) -> Objects {
        let g = &self.parts.gateways;
        Objects {
            classes: g.classes(),
            gateways: g.gateways(),
            routes: g.routes(),
            grants: g.grants(),
            namespaces: g.namespaces(),
        }
    }

    async fn pass(&mut self) -> anyhow::Result<PassOutcome> {
        if !self.parts.gateways.is_synced() {
            return Ok(PassOutcome::NotSynced("gateway api"));
        }
        let objects = self.objects();
        let settings = self.settings().clone();
        let options = &settings.translate;
        let no_services = BTreeMap::new();
        let no_secrets = BTreeMap::new();
        let no_plugins = BTreeSet::new();
        let referenced = self.parts.logic.referenced(&TranslateInput {
            classes: &objects.classes,
            gateways: &objects.gateways,
            routes: &objects.routes,
            grants: &objects.grants,
            namespaces: &objects.namespaces,
            services: &no_services,
            secrets: &no_secrets,
            servers: &[],
            plugins: &no_plugins,
            options,
        });
        self.parts.secrets.set_secrets(referenced.secrets.clone());
        self.watched = referenced.services.clone();

        // Backend Services: the endpoint writer starts their watches and returns
        // `applied` once they listed.
        if !referenced.services.is_subset(&self.services) {
            let upstreams = self.upstreams.clone().unwrap_or_default();
            self.hand_over(upstreams, referenced.services.clone())
                .await?;
        }
        if !self.parts.secrets.is_synced() {
            return Ok(PassOutcome::NotSynced("secrets"));
        }

        let mut attempt = 0;
        loop {
            attempt += 1;
            let stored = self
                .parts
                .store
                .read_stored()
                .await
                .context("reading the Pingap entries")?;
            let plugins = plugin_names(&stored);
            let servers = server_infos(&stored);
            let services: BTreeMap<ServiceKey, Arc<Service>> = referenced
                .services
                .iter()
                .filter_map(|key| {
                    let service = self.parts.services.snapshot(key).service?;
                    Some((key.clone(), service))
                })
                .collect();
            let secrets = self.parts.secrets.secrets();
            let desired = self.parts.logic.translate(&TranslateInput {
                classes: &objects.classes,
                gateways: &objects.gateways,
                routes: &objects.routes,
                grants: &objects.grants,
                namespaces: &objects.namespaces,
                services: &services,
                secrets: &secrets,
                servers: &servers,
                plugins: &plugins,
                options,
            });

            if self.upstreams.as_ref() != Some(&desired.upstreams) {
                self.hand_over(desired.upstreams.clone(), referenced.services.clone())
                    .await?;
            }
            let keep: BTreeSet<String> = desired.upstreams.keys().cloned().collect();
            let plan = self.parts.logic.plan(&desired, &stored, &keep);
            let deferred = !plan.deferred.is_empty();

            if !self.leader {
                self.finish(&objects, &desired, &plan).await;
                return Ok(PassOutcome::Done { deferred });
            }
            match self.parts.store.apply(&plan).await {
                Ok(outcome) => {
                    let result = match outcome {
                        ApplyOutcome::Unchanged => GatewaySyncResult::Unchanged,
                        ApplyOutcome::Applied {
                            revision,
                            puts,
                            deletes,
                        } => {
                            info!(
                                revision,
                                puts,
                                deletes,
                                programmed = plan.programmed.len(),
                                deferred = plan.deferred.len(),
                                "Gateway entries written"
                            );
                            GatewaySyncResult::Ok
                        }
                    };
                    self.parts.metrics.record_gateway_sync(result);
                    self.finish(&objects, &desired, &plan).await;
                    return Ok(PassOutcome::Done { deferred });
                }
                Err(err) => {
                    self.parts
                        .metrics
                        .record_gateway_sync(GatewaySyncResult::Error);
                    if attempt >= APPLY_ATTEMPTS {
                        return Err(err
                            .context(format!("applying the Gateway plan failed {attempt} times")));
                    }
                    debug!(
                        attempt,
                        "applying the Gateway plan failed; re-reading: {err:#}"
                    );
                }
            }
        }
    }

    /// Hands generated upstreams and the backend Service set to the endpoint writer
    /// and waits until it has taken them over.
    async fn hand_over(
        &mut self,
        upstreams: BTreeMap<String, UpstreamConfig>,
        services: BTreeSet<ServiceKey>,
    ) -> anyhow::Result<()> {
        let sender = self
            .generated
            .as_ref()
            .context("generated upstream channel not set")?;
        let (applied, rx) = oneshot::channel();
        sender
            .send(GeneratedUpstreams {
                upstreams: upstreams.clone(),
                services: services.clone(),
                applied,
            })
            .await
            .map_err(|_| anyhow!("the endpoint writer is gone"))?;
        match tokio::time::timeout(APPLIED_TIMEOUT, rx).await {
            Ok(Ok(())) => {
                debug!(
                    upstreams = upstreams.len(),
                    services = services.len(),
                    "endpoint writer took over the generated upstreams"
                );
                self.upstreams = Some(upstreams);
                self.services = services;
                Ok(())
            }
            Ok(Err(_)) => bail!("the endpoint writer dropped the generated upstreams"),
            Err(_) => bail!(
                "the endpoint writer has not taken over the generated upstreams within {APPLIED_TIMEOUT:?} (backend Service watches not listed yet?)"
            ),
        }
    }

    /// Gauges for every replica; status from the leader.
    async fn finish(&mut self, objects: &Objects, desired: &Desired, plan: &Plan) {
        self.parts
            .metrics
            .set_gateway_locations(plan.programmed.len(), plan.deferred.len());
        if self.leader {
            self.publish_status(objects, desired).await;
        }
    }

    /// Writes every desired status; the publisher skips those that already match.
    async fn publish_status(&self, objects: &Objects, desired: &Desired) {
        let status = &self.parts.status;
        for class in &objects.classes {
            let Some(name) = class.metadata.name.as_deref() else {
                continue;
            };
            let Some(want) = desired.status.classes.get(name) else {
                continue;
            };
            match status.publish_class(class, want).await {
                Ok(true) => debug!(gatewayclass = name, "GatewayClass status updated"),
                Ok(false) => {}
                Err(err) => {
                    warn!(
                        gatewayclass = name,
                        "updating the GatewayClass status failed: {err:#}"
                    )
                }
            }
        }
        for gateway in &objects.gateways {
            let key = object_key(&gateway.metadata);
            let Some(want) = desired.status.gateways.get(&key) else {
                continue;
            };
            match status.publish_gateway(gateway, want).await {
                Ok(true) => debug!(gateway = %key, "Gateway status updated"),
                Ok(false) => {}
                Err(err) => warn!(gateway = %key, "updating the Gateway status failed: {err:#}"),
            }
        }
        for route in &objects.routes {
            let key = object_key(&route.metadata);
            let Some(want) = desired.status.routes.get(&key) else {
                continue;
            };
            match status.publish_route(route, want).await {
                Ok(true) => debug!(httproute = %key, "HTTPRoute status updated"),
                Ok(false) => {}
                Err(err) => {
                    warn!(httproute = %key, "updating the HTTPRoute status failed: {err:#}")
                }
            }
        }
    }
}

fn object_key(meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta) -> ObjectKey {
    ObjectKey::new(
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    )
}

/// Stored plugin names plus Pingap's built-ins.
fn plugin_names(stored: &Stored) -> BTreeSet<String> {
    stored
        .plugins
        .keys()
        .cloned()
        .chain(PINGAP_BUILTIN_PLUGINS.iter().map(|p| p.to_string()))
        .collect()
}

/// The stored servers a listener can be mapped onto.
fn server_infos(stored: &Stored) -> Vec<ServerInfo> {
    stored
        .servers
        .iter()
        .filter_map(|(name, entry)| {
            let info = render::server_info(&entry.value, name);
            if info.is_none() {
                debug!(
                    server = name,
                    "server has no readable addr; not used for listeners"
                );
            }
            info
        })
        .collect()
}

/// True when the signal means "something changed"; a closed channel is dropped.
fn signal_or_close<T>(
    r: Result<T, broadcast::error::RecvError>,
    rx: &mut Option<broadcast::Receiver<T>>,
    what: &str,
) -> bool {
    match r {
        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => true,
        Err(broadcast::error::RecvError::Closed) => {
            warn!("{what} change channel closed");
            *rx = None;
            false
        }
    }
}

async fn recv_broadcast<T: Clone>(
    rx: &mut Option<broadcast::Receiver<T>>,
) -> Result<T, broadcast::error::RecvError> {
    match rx {
        Some(rx) => rx.recv().await,
        None => pending().await,
    }
}

async fn recv_mpsc<T>(rx: &mut Option<mpsc::Receiver<T>>) -> Option<T> {
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

#[cfg(test)]
mod tests {}

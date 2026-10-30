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
//! 6. the leader writes the status of the handled GatewayClasses, Gateways,
//!    ListenerSets and HTTPRoutes where it differs from the stored one.
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
                groups: gateway.groups.clone(),
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
            && a.groups == b.groups
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
    listener_sets: Vec<Arc<crate::gateway::api::listenersets::ListenerSet>>,
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
            listener_sets: g.listener_sets(),
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
            listener_sets: &objects.listener_sets,
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
                listener_sets: &objects.listener_sets,
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
        for set in &objects.listener_sets {
            let key = object_key(&set.metadata);
            let Some(want) = desired.status.listener_sets.get(&key) else {
                continue;
            };
            match status.publish_listener_set(set, want).await {
                Ok(true) => debug!(listenerset = %key, "ListenerSet status updated"),
                Ok(false) => {}
                Err(err) => {
                    warn!(listenerset = %key, "updating the ListenerSet status failed: {err:#}")
                }
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
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use k8s_openapi::api::core::v1::{Namespace, Secret};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;

    use crate::gateway::api::gatewayclasses::{GatewayClass, GatewayClassSpec, GatewayClassStatus};
    use crate::gateway::api::gateways::{Gateway, GatewayStatus};
    use crate::gateway::api::httproutes::{HttpRoute, HttpRouteStatus};
    use crate::gateway::api::listenersets::{ListenerSet, ListenerSetStatus};
    use crate::gateway::api::referencegrants::ReferenceGrant;
    use crate::gateway::model::{Deferred, Expect, Op, SecretKey, Step, StoredEntry};
    use crate::types::{PortRef, ServiceRef, ServiceSnapshot};

    fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        m.lock().unwrap()
    }

    fn skey(name: &str) -> ServiceKey {
        ServiceKey {
            namespace: "apps".into(),
            name: name.into(),
        }
    }

    fn set(items: &[&str]) -> BTreeSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn class(name: &str) -> Arc<GatewayClass> {
        Arc::new(GatewayClass {
            metadata: ObjectMeta {
                name: Some(name.into()),
                ..Default::default()
            },
            spec: GatewayClassSpec {
                controller_name: "pingap.io/gateway-controller".into(),
                ..Default::default()
            },
            status: None,
        })
    }

    // ---- fakes ----

    struct FakeGateways {
        classes: Mutex<Vec<Arc<GatewayClass>>>,
        synced: AtomicBool,
        tx: broadcast::Sender<()>,
    }

    impl GatewaySource for FakeGateways {
        fn classes(&self) -> Vec<Arc<GatewayClass>> {
            lock(&self.classes).clone()
        }
        fn gateways(&self) -> Vec<Arc<Gateway>> {
            Vec::new()
        }
        fn routes(&self) -> Vec<Arc<HttpRoute>> {
            Vec::new()
        }
        fn listener_sets(&self) -> Vec<Arc<ListenerSet>> {
            Vec::new()
        }
        fn grants(&self) -> Vec<Arc<ReferenceGrant>> {
            Vec::new()
        }
        fn namespaces(&self) -> BTreeMap<String, Arc<Namespace>> {
            BTreeMap::new()
        }
        fn is_synced(&self) -> bool {
            self.synced.load(Ordering::SeqCst)
        }
        fn changes(&self) -> broadcast::Receiver<()> {
            self.tx.subscribe()
        }
    }

    struct FakeSecrets {
        synced: AtomicBool,
        tx: broadcast::Sender<()>,
        wanted: Mutex<Vec<BTreeSet<SecretKey>>>,
    }

    impl SecretSource for FakeSecrets {
        fn set_secrets(&self, keys: BTreeSet<SecretKey>) {
            lock(&self.wanted).push(keys);
        }
        fn secrets(&self) -> BTreeMap<SecretKey, Arc<Secret>> {
            BTreeMap::new()
        }
        fn is_synced(&self) -> bool {
            self.synced.load(Ordering::SeqCst)
        }
        fn changes(&self) -> broadcast::Receiver<()> {
            self.tx.subscribe()
        }
    }

    struct FakeServices;

    impl SnapshotSource for FakeServices {
        fn snapshot(&self, _key: &ServiceKey) -> ServiceSnapshot {
            ServiceSnapshot {
                service: Some(Arc::new(Service::default())),
                slices: Vec::new(),
            }
        }
    }

    #[derive(Default)]
    struct FakeStore {
        reads: AtomicUsize,
        applied: Mutex<Vec<Plan>>,
        /// Applies that fail before one succeeds.
        failures: AtomicUsize,
    }

    impl FakeStore {
        fn applies(&self) -> usize {
            lock(&self.applied).len()
        }
    }

    impl PingapStore for FakeStore {
        async fn read_stored(&self) -> anyhow::Result<Stored> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let mut stored = Stored::default();
            stored.plugins.insert(
                "auth".into(),
                StoredEntry {
                    key: "/p/plugins/auth.toml".into(),
                    value: String::new(),
                    mod_revision: 1,
                },
            );
            stored.servers.insert(
                "http".into(),
                StoredEntry {
                    key: "/p/servers/http.toml".into(),
                    value: "[servers.http]\naddr = \"0.0.0.0:80\"\n".into(),
                    mod_revision: 1,
                },
            );
            Ok(stored)
        }

        async fn apply(&self, plan: &Plan) -> anyhow::Result<ApplyOutcome> {
            let failures = self.failures.load(Ordering::SeqCst);
            if failures > 0 {
                self.failures.store(failures - 1, Ordering::SeqCst);
                bail!("compare failed");
            }
            lock(&self.applied).push(plan.clone());
            Ok(if plan.steps.is_empty() {
                ApplyOutcome::Unchanged
            } else {
                ApplyOutcome::Applied {
                    revision: 2,
                    puts: 1,
                    deletes: 0,
                }
            })
        }
    }

    #[derive(Default)]
    struct FakeStatus {
        classes: Mutex<Vec<String>>,
    }

    impl StatusPublisher for FakeStatus {
        async fn publish_class(
            &self,
            class: &GatewayClass,
            _status: &GatewayClassStatus,
        ) -> anyhow::Result<bool> {
            lock(&self.classes).push(class.metadata.name.clone().unwrap_or_default());
            Ok(true)
        }
        async fn publish_gateway(
            &self,
            _gateway: &Gateway,
            _status: &GatewayStatus,
        ) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn publish_listener_set(
            &self,
            _listener_set: &ListenerSet,
            _status: &ListenerSetStatus,
        ) -> anyhow::Result<bool> {
            Ok(false)
        }
        async fn publish_route(
            &self,
            _route: &HttpRoute,
            _status: &HttpRouteStatus,
        ) -> anyhow::Result<bool> {
            Ok(false)
        }
    }

    #[derive(Default)]
    struct FakeLogic {
        referenced: Mutex<Referenced>,
        desired: Mutex<Desired>,
        plan: Mutex<Plan>,
        translations: AtomicUsize,
        plans: AtomicUsize,
        keeps: Mutex<Vec<BTreeSet<String>>>,
        plugins: Mutex<BTreeSet<String>>,
        servers: Mutex<Vec<ServerInfo>>,
    }

    impl Logic for FakeLogic {
        fn referenced(&self, _input: &TranslateInput<'_>) -> Referenced {
            lock(&self.referenced).clone()
        }
        fn translate(&self, input: &TranslateInput<'_>) -> Desired {
            self.translations.fetch_add(1, Ordering::SeqCst);
            *lock(&self.plugins) = input.plugins.clone();
            *lock(&self.servers) = input.servers.to_vec();
            lock(&self.desired).clone()
        }
        fn plan(
            &self,
            _desired: &Desired,
            _stored: &Stored,
            keep_upstreams: &BTreeSet<String>,
        ) -> Plan {
            self.plans.fetch_add(1, Ordering::SeqCst);
            lock(&self.keeps).push(keep_upstreams.clone());
            lock(&self.plan).clone()
        }
    }

    // ---- harness ----

    fn settings() -> ControllerSettings {
        ControllerSettings {
            translate: TranslateOptions {
                controller_name: "pingap.io/gateway-controller".into(),
                external_name: false,
                upstream_defaults: crate::config::Defaults::default().upstream_options(),
                health_check: None,
                cluster_domain: "cluster.local".into(),
                servers: vec![],
                addresses: vec!["203.0.113.7".into()],
                groups: vec![],
            },
        }
    }

    fn one_step_plan() -> Plan {
        Plan {
            steps: vec![Step {
                what: "locations",
                ops: vec![Op::Put {
                    key: "/p/locations/k8s-apps-web-1.toml".into(),
                    value: String::new(),
                    expect: Expect::Absent,
                }],
            }],
            deferred: BTreeMap::new(),
            programmed: set(&["k8s-apps-web-1"]),
        }
    }

    fn upstream(service: &str) -> UpstreamConfig {
        UpstreamConfig {
            services: vec![ServiceRef {
                key: skey(service),
                port: PortRef::Number(80),
                weight: None,
            }],
            options: crate::config::Defaults::default().upstream_options(),
            health_check: None,
        }
    }

    struct Harness {
        gateways: Arc<FakeGateways>,
        secrets: Arc<FakeSecrets>,
        store: Arc<FakeStore>,
        status: Arc<FakeStatus>,
        logic: Arc<FakeLogic>,
        metrics: Arc<Metrics>,
        leader: watch::Sender<bool>,
        etcd: mpsc::Sender<()>,
        synced: watch::Receiver<bool>,
        /// Present when the test answers the endpoint writer itself.
        generated: Option<mpsc::Receiver<GeneratedUpstreams>>,
        _settings: watch::Sender<ControllerSettings>,
        _services: broadcast::Sender<ServiceKey>,
        _stop: watch::Sender<bool>,
    }

    struct Setup {
        leader: bool,
        gateways_synced: bool,
        secrets_synced: bool,
        auto_apply: bool,
        logic: FakeLogic,
    }

    impl Default for Setup {
        fn default() -> Self {
            let logic = FakeLogic::default();
            *lock(&logic.plan) = one_step_plan();
            lock(&logic.desired)
                .status
                .classes
                .insert("pingap".into(), GatewayClassStatus::default());
            Setup {
                leader: true,
                gateways_synced: true,
                secrets_synced: true,
                auto_apply: true,
                logic,
            }
        }
    }

    fn start(setup: Setup) -> Harness {
        let gateways = Arc::new(FakeGateways {
            classes: Mutex::new(vec![class("pingap"), class("other")]),
            synced: AtomicBool::new(setup.gateways_synced),
            tx: broadcast::channel(16).0,
        });
        let secrets = Arc::new(FakeSecrets {
            synced: AtomicBool::new(setup.secrets_synced),
            tx: broadcast::channel(16).0,
            wanted: Mutex::new(Vec::new()),
        });
        let store = Arc::new(FakeStore::default());
        let status = Arc::new(FakeStatus::default());
        let logic = Arc::new(setup.logic);
        let metrics = Arc::new(Metrics::new());
        let (settings_tx, settings_rx) = watch::channel(settings());
        let (leader_tx, leader_rx) = watch::channel(setup.leader);
        let (etcd_tx, etcd_rx) = mpsc::channel(8);
        let (gen_tx, mut gen_rx) = mpsc::channel::<GeneratedUpstreams>(8);
        let (synced_tx, synced_rx) = watch::channel(false);
        let (svc_tx, svc_rx) = broadcast::channel(16);
        let (stop_tx, mut stop_rx) = watch::channel(false);

        let generated = if setup.auto_apply {
            tokio::spawn(async move {
                while let Some(g) = gen_rx.recv().await {
                    let _ = g.applied.send(());
                }
            });
            None
        } else {
            Some(gen_rx)
        };

        let controller = GatewayController::new(Parts {
            gateways: gateways.clone(),
            secrets: secrets.clone(),
            services: Arc::new(FakeServices),
            store: store.clone(),
            status: status.clone(),
            logic: logic.clone(),
            metrics: metrics.clone(),
        });
        let inputs = Inputs {
            settings: settings_rx,
            leader: leader_rx,
            service_changes: svc_rx,
            etcd_changes: etcd_rx,
            generated: gen_tx,
            synced: synced_tx,
            resync: Duration::from_secs(3600),
        };
        tokio::spawn(controller.run(inputs, async move {
            let _ = stop_rx.wait_for(|s| *s).await;
        }));
        Harness {
            gateways,
            secrets,
            store,
            status,
            logic,
            metrics,
            leader: leader_tx,
            etcd: etcd_tx,
            synced: synced_rx,
            generated,
            _settings: settings_tx,
            _services: svc_tx,
            _stop: stop_tx,
        }
    }

    async fn sleep_ms(ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }

    impl Harness {
        fn plans(&self) -> usize {
            self.logic.plans.load(Ordering::SeqCst)
        }

        async fn poke(&self) {
            self.etcd.send(()).await.unwrap();
            sleep_ms(3_000).await;
        }
    }

    // ---- tests ----

    #[tokio::test(start_paused = true)]
    async fn nothing_is_planned_before_the_gateway_api_listed() {
        let h = start(Setup {
            gateways_synced: false,
            ..Setup::default()
        });
        sleep_ms(5_000).await;
        assert_eq!(h.store.reads.load(Ordering::SeqCst), 0);
        assert_eq!(h.plans(), 0);
        assert!(!*h.synced.borrow());

        h.gateways.synced.store(true, Ordering::SeqCst);
        let _ = h.gateways.tx.send(());
        sleep_ms(3_000).await;
        assert_eq!(h.store.applies(), 1);
        assert!(*h.synced.borrow());
        // Pingap's built-in plugins are known besides the stored ones, and the stored
        // servers reach the translation.
        let plugins = lock(&h.logic.plugins).clone();
        assert!(plugins.contains("auth") && plugins.contains("pingap:ping"));
        let servers = lock(&h.logic.servers).clone();
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "http");
        assert_eq!(servers[0].ports, BTreeSet::from([80]));
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_is_planned_before_secrets_listed() {
        let setup = Setup {
            secrets_synced: false,
            ..Setup::default()
        };
        let secret = SecretKey::new("apps", "tls");
        lock(&setup.logic.referenced).secrets.insert(secret.clone());
        let h = start(setup);
        sleep_ms(5_000).await;
        // The Secret watches follow the referenced Secrets, but nothing is planned.
        assert_eq!(
            lock(&h.secrets.wanted).last(),
            Some(&BTreeSet::from([secret]))
        );
        assert_eq!(h.plans(), 0);

        h.secrets.synced.store(true, Ordering::SeqCst);
        let _ = h.secrets.tx.send(());
        sleep_ms(3_000).await;
        assert_eq!(h.store.applies(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_is_planned_before_backend_services_listed() {
        let setup = Setup {
            auto_apply: false,
            ..Setup::default()
        };
        lock(&setup.logic.referenced).services.insert(skey("api"));
        let mut h = start(setup);
        let mut rx = h.generated.take().unwrap();
        // The first hand-over only asks for the Service watches.
        let first = rx.recv().await.unwrap();
        assert!(first.upstreams.is_empty());
        assert_eq!(first.services, BTreeSet::from([skey("api")]));
        sleep_ms(5_000).await;
        assert_eq!(h.logic.translations.load(Ordering::SeqCst), 0);
        assert_eq!(h.store.applies(), 0);
        first.applied.send(()).unwrap();
        sleep_ms(1_000).await;
        assert!(rx.try_recv().is_err());
        assert_eq!(h.logic.translations.load(Ordering::SeqCst), 1);
        assert_eq!(h.store.applies(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn follower_plans_but_never_writes() {
        let h = start(Setup {
            leader: false,
            ..Setup::default()
        });
        sleep_ms(3_000).await;
        h.poke().await;
        assert!(h.plans() >= 2);
        assert_eq!(h.store.applies(), 0);
        assert!(lock(&h.status.classes).is_empty());
        // Gauges are computed on followers too.
        assert_eq!(h.metrics.value("pingap_k8s_gateway_locations", &[]), 1);
        assert_eq!(h.metrics.value("pingap_k8s_gateway_deferred", &[]), 0);
        assert!(*h.synced.borrow());

        // Promotion: a pass right away, with status for the desired classes only.
        h.leader.send(true).unwrap();
        sleep_ms(100).await;
        assert_eq!(h.store.applies(), 1);
        assert_eq!(*lock(&h.status.classes), vec!["pingap".to_string()]);
        assert_eq!(
            h.metrics
                .value("pingap_k8s_gateway_sync_total", &[("result", "ok")]),
            1
        );

        // Demoted again: no more writes.
        h.leader.send(false).unwrap();
        h.poke().await;
        assert_eq!(h.store.applies(), 1);
        assert_eq!(lock(&h.status.classes).len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_locations_are_retried_with_backoff() {
        let setup = Setup::default();
        lock(&setup.logic.plan).deferred.insert(
            "k8s-apps-web-1".into(),
            Deferred::MissingUpstream("k8s-u".into()),
        );
        let h = start(setup);
        // Passes at 0 s, then retries 1 s, 2 s and 4 s apart: 0, 1, 3, 7.
        sleep_ms(500).await;
        assert_eq!(h.plans(), 1);
        assert_eq!(h.metrics.value("pingap_k8s_gateway_deferred", &[]), 1);
        sleep_ms(1_000).await;
        assert_eq!(h.plans(), 2);
        sleep_ms(2_000).await;
        assert_eq!(h.plans(), 3);
        sleep_ms(2_000).await;
        assert_eq!(h.plans(), 3);
        sleep_ms(2_000).await;
        assert_eq!(h.plans(), 4);

        // Nothing deferred any more: one more timer pass, then none.
        lock(&h.logic.plan).deferred.clear();
        sleep_ms(8_000).await;
        assert_eq!(h.plans(), 5);
        sleep_ms(60_000).await;
        assert_eq!(h.plans(), 5);
        assert_eq!(h.metrics.value("pingap_k8s_gateway_deferred", &[]), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn garbage_collection_waits_for_applied() {
        let setup = Setup {
            auto_apply: false,
            ..Setup::default()
        };
        lock(&setup.logic.desired)
            .upstreams
            .insert("k8s-apps-api-80-1".into(), upstream("api"));
        let mut h = start(setup);
        let mut rx = h.generated.take().unwrap();
        let handed = rx.recv().await.unwrap();
        assert_eq!(
            handed.upstreams.keys().collect::<Vec<_>>(),
            vec!["k8s-apps-api-80-1"]
        );
        // Not taken over yet: no plan, nothing deleted.
        sleep_ms(10_000).await;
        assert_eq!(h.plans(), 0);
        assert_eq!(h.store.applies(), 0);

        handed.applied.send(()).unwrap();
        sleep_ms(100).await;
        assert_eq!(h.store.applies(), 1);
        // The generated upstreams are kept by the plan.
        assert_eq!(
            lock(&h.logic.keeps).last(),
            Some(&set(&["k8s-apps-api-80-1"]))
        );

        // The same upstreams again are not handed over again.
        h.poke().await;
        assert_eq!(h.store.applies(), 2);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn failed_applies_are_replanned_then_backed_off() {
        let h = start(Setup::default());
        h.store
            .failures
            .store(APPLY_ATTEMPTS as usize, Ordering::SeqCst);
        sleep_ms(100).await;
        // Every attempt of the first pass failed: re-read and re-planned each time.
        assert_eq!(h.plans(), APPLY_ATTEMPTS as usize);
        assert_eq!(h.store.applies(), 0);
        assert_eq!(
            h.metrics
                .value("pingap_k8s_gateway_sync_total", &[("result", "error")]),
            u64::from(APPLY_ATTEMPTS)
        );
        // The retry after the backoff succeeds.
        sleep_ms(1_000).await;
        assert_eq!(h.store.applies(), 1);
    }
}

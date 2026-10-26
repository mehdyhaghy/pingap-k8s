//! Kubernetes watches.
//!
//! For every referenced Service there are two watchers, each feeding a
//! [`reflector::Store`]:
//! - EndpointSlices selected by `kubernetes.io/service-name=<svc>`;
//! - the Service object itself, selected by `metadata.name=<svc>` (port table).
//!
//! Both use `any_semantic()` (re-lists served from the apiserver watch cache)
//! with bookmarks on. [`WatchRegistry::set_services`] starts watchers for new
//! Services, stops removed ones and leaves unchanged ones running, so a config
//! reload never re-lists Services it already watches. The caller passes the
//! union of the Services of all upstreams, which is what makes a watcher shared
//! across upstreams.
//!
//! Failures: an error puts the watcher in the "down" state. It logs an error
//! once (a 403 names the missing permission), sets
//! `pingap_k8s_watch_up{k8s_namespace_name,k8s_service_name}` to 0 and counts
//! `pingap_k8s_watch_errors_total{..,error_type}`. The watcher then waits for the
//! next delay of kube's [`DefaultBackoff`] (800 ms to 30 s, jittered, the policy
//! `.default_backoff()` applies) and starts over with a fresh list. The first
//! successful list after that logs one info line and sets `watch_up` back to 1.
//! The store keeps its last contents while the watch is down, so reconcile
//! never sees a partial set (Pingap keeps the last `addrs`).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::runtime::reflector::Store;
use kube::runtime::reflector::store::Writer;
use kube::runtime::watcher::{self, DefaultBackoff, Event};
use kube::{Api, Client, Resource};
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

use crate::metrics::Metrics;
use crate::types::{ServiceKey, ServiceSnapshot, SnapshotSource};

/// Label Kubernetes puts on every EndpointSlice of a Service.
pub const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

/// Capacity of the change broadcast. A receiver that falls further behind
/// gets `RecvError::Lagged` (see [`WatchRegistry::subscribe`]).
const CHANGE_CHANNEL_CAPACITY: usize = 1024;

/// Delay used if the backoff iterator ever ends (it does not with kube's
/// default policy).
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Label selector for the EndpointSlices of Service `name`.
pub fn slice_label_selector(name: &str) -> String {
    format!("{SERVICE_NAME_LABEL}={name}")
}

/// Field selector for the Service object `name`.
pub fn service_field_selector(name: &str) -> String {
    format!("metadata.name={name}")
}

/// Watcher config for the EndpointSlices of Service `name`.
pub fn slice_watcher_config(name: &str) -> watcher::Config {
    watcher::Config::default()
        .labels(&slice_label_selector(name))
        .any_semantic()
}

/// Watcher config for the Service object `name`.
pub fn service_watcher_config(name: &str) -> watcher::Config {
    watcher::Config::default()
        .fields(&service_field_selector(name))
        .any_semantic()
}

/// The two watchers kept per Service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WatchKind {
    EndpointSlices,
    Service,
}

impl WatchKind {
    fn index(self) -> usize {
        match self {
            WatchKind::EndpointSlices => 0,
            WatchKind::Service => 1,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            WatchKind::EndpointSlices => "endpointslices",
            WatchKind::Service => "service",
        }
    }

    /// The RBAC resource (with API group) this watcher needs.
    pub fn rbac_resource(self) -> &'static str {
        match self {
            WatchKind::EndpointSlices => "endpointslices.discovery.k8s.io",
            WatchKind::Service => "services (core API group \"\")",
        }
    }
}

/// Short, bounded `error.type` attribute of `pingap_k8s_watch_errors_total`.
pub fn classify_error(err: &watcher::Error) -> &'static str {
    match err {
        watcher::Error::InitialListFailed(e)
        | watcher::Error::WatchStartFailed(e)
        | watcher::Error::WatchFailed(e) => classify_client_error(e),
        watcher::Error::WatchError(status) => classify_status(status),
        watcher::Error::NoResourceVersion => "other",
    }
}

fn classify_client_error(err: &kube::Error) -> &'static str {
    match err {
        kube::Error::Api(status) => classify_status(status),
        kube::Error::HyperError(_) | kube::Error::Service(_) => "transport",
        _ => "other",
    }
}

fn classify_status(status: &kube::core::Status) -> &'static str {
    if status.is_forbidden() {
        "forbidden"
    } else if status.code == 401 || status.reason == "Unauthorized" {
        "unauthorized"
    } else if status.is_not_found() {
        "not_found"
    } else if status.code == 410 || status.reason == "Expired" || status.reason == "Gone" {
        "gone"
    } else {
        "api"
    }
}

/// `410 Gone` (resourceVersion too old) is routine: the watcher re-lists by
/// itself, so it is not treated as an outage.
fn is_gone(err: &watcher::Error) -> bool {
    classify_error(err) == "gone"
}

/// The log message for a watch that went down. A 403 names the missing
/// permission.
pub fn down_message(key: &ServiceKey, kind: WatchKind, err: &watcher::Error) -> String {
    if classify_error(err) == "forbidden" {
        format!(
            "watch of {} for Service {key} is forbidden: the controller's ServiceAccount is \
             missing get/list/watch on {} in namespace {}; grant it with a Role in that \
             namespace: {err}",
            kind.as_str(),
            kind.rbac_resource(),
            key.namespace,
        )
    } else {
        format!(
            "watch of {} for Service {key} is down, retrying with backoff: {err}",
            kind.as_str()
        )
    }
}

/// Splits the change from `current` to `desired` into (added, removed).
pub fn diff_keys(
    current: &BTreeSet<ServiceKey>,
    desired: &BTreeSet<ServiceKey>,
) -> (Vec<ServiceKey>, Vec<ServiceKey>) {
    let added = desired.difference(current).cloned().collect();
    let removed = current.difference(desired).cloned().collect();
    (added, removed)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Health {
    /// Not listed yet and no error yet.
    #[default]
    Starting,
    Up,
    Down,
}

#[derive(Debug, PartialEq, Eq)]
enum Transition {
    None,
    WentDown,
    Recovered,
}

/// Up/down state of the two watchers of one Service. Pure, so it can be
/// tested without a cluster.
#[derive(Debug, Default)]
struct KeyHealth {
    kinds: [Health; 2],
}

impl KeyHealth {
    fn on_error(&mut self, kind: WatchKind) -> Transition {
        let prev = std::mem::replace(&mut self.kinds[kind.index()], Health::Down);
        if prev == Health::Down {
            Transition::None
        } else {
            Transition::WentDown
        }
    }

    fn on_ok(&mut self, kind: WatchKind) -> Transition {
        let prev = std::mem::replace(&mut self.kinds[kind.index()], Health::Up);
        if prev == Health::Down {
            Transition::Recovered
        } else {
            Transition::None
        }
    }

    fn all_up(&self) -> bool {
        self.kinds.iter().all(|h| *h == Health::Up)
    }
}

struct HealthState {
    health: KeyHealth,
    /// Set when the watcher is removed, so a task that is still running
    /// between `abort()` and its cancellation does not re-create metrics.
    stopped: bool,
}

/// State shared between the registry and the two tasks of one Service.
struct KeyShared {
    key: ServiceKey,
    metrics: Arc<Metrics>,
    changes: broadcast::Sender<ServiceKey>,
    state: Mutex<HealthState>,
    /// Per kind: completed at least one full list.
    listed: [AtomicBool; 2],
}

impl KeyShared {
    fn state(&self) -> MutexGuard<'_, HealthState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn notify(&self) {
        // Err only means there is no receiver right now.
        let _ = self.changes.send(self.key.clone());
    }

    fn on_ok(&self, kind: WatchKind) {
        let mut state = self.state();
        if state.stopped {
            return;
        }
        if state.health.on_ok(kind) == Transition::Recovered {
            info!(service = %self.key, kind = kind.as_str(), "watch recovered");
        }
        let up = state.health.all_up();
        self.metrics
            .set_watch_up(&self.key.namespace, &self.key.name, up);
    }

    fn on_error(&self, kind: WatchKind, err: &watcher::Error) {
        let mut state = self.state();
        if state.stopped {
            return;
        }
        let reason = classify_error(err);
        self.metrics
            .record_watch_error(&self.key.namespace, &self.key.name, reason);
        self.metrics
            .set_watch_up(&self.key.namespace, &self.key.name, false);
        if state.health.on_error(kind) == Transition::WentDown {
            error!(
                service = %self.key,
                kind = kind.as_str(),
                reason,
                "{}",
                down_message(&self.key, kind, err)
            );
        } else {
            debug!(service = %self.key, kind = kind.as_str(), reason, "watch still down: {err}");
        }
    }

    fn stop(&self) {
        let mut state = self.state();
        state.stopped = true;
        self.metrics
            .remove_watch(&self.key.namespace, &self.key.name);
    }
}

/// Aborts the task when dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// The two watchers of one Service.
struct ServiceWatch {
    slices: Store<EndpointSlice>,
    service: Store<Service>,
    shared: Arc<KeyShared>,
    _tasks: [AbortOnDrop; 2],
}

impl ServiceWatch {
    fn start(
        client: &Client,
        key: ServiceKey,
        metrics: Arc<Metrics>,
        changes: broadcast::Sender<ServiceKey>,
    ) -> Self {
        metrics.set_watch_up(&key.namespace, &key.name, false);
        let shared = Arc::new(KeyShared {
            key: key.clone(),
            metrics,
            changes,
            state: Mutex::new(HealthState {
                health: KeyHealth::default(),
                stopped: false,
            }),
            listed: [AtomicBool::new(false), AtomicBool::new(false)],
        });

        let slice_writer = Writer::<EndpointSlice>::default();
        let slices = slice_writer.as_reader();
        let slice_task = tokio::spawn(run_watch(
            Api::namespaced(client.clone(), &key.namespace),
            slice_watcher_config(&key.name),
            slice_writer,
            WatchKind::EndpointSlices,
            shared.clone(),
        ));

        let service_writer = Writer::<Service>::default();
        let service = service_writer.as_reader();
        let service_task = tokio::spawn(run_watch(
            Api::namespaced(client.clone(), &key.namespace),
            service_watcher_config(&key.name),
            service_writer,
            WatchKind::Service,
            shared.clone(),
        ));

        ServiceWatch {
            slices,
            service,
            shared,
            _tasks: [AbortOnDrop(slice_task), AbortOnDrop(service_task)],
        }
    }

    fn is_synced(&self) -> bool {
        self.shared
            .listed
            .iter()
            .all(|listed| listed.load(Ordering::Acquire))
    }
}

/// Runs one watcher forever, feeding `writer` and notifying subscribers.
async fn run_watch<K>(
    api: Api<K>,
    config: watcher::Config,
    mut writer: Writer<K>,
    kind: WatchKind,
    shared: Arc<KeyShared>,
) where
    K: Resource + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Default + Eq + Hash + Clone,
{
    let mut backoff = DefaultBackoff::default();
    loop {
        let mut stream = watcher::watcher(api.clone(), config.clone()).boxed();
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => {
                    writer.apply_watcher_event(&event);
                    match event {
                        Event::InitDone => {
                            shared.listed[kind.index()].store(true, Ordering::Release);
                            shared.on_ok(kind);
                            shared.notify();
                        }
                        Event::Apply(_) | Event::Delete(_) => {
                            shared.on_ok(kind);
                            shared.notify();
                        }
                        // The store only changes on InitDone.
                        Event::Init | Event::InitApply(_) => {}
                    }
                }
                Err(err) if is_gone(&err) => {
                    debug!(service = %shared.key, kind = kind.as_str(), "watch expired, re-listing: {err}");
                }
                Err(err) => {
                    shared.on_error(kind, &err);
                    break;
                }
            }
        }
        // Start over with a fresh list after the backoff delay: a successful
        // list is the signal that the watch has recovered, and it brings the
        // store back in line with whatever changed while it was down.
        drop(stream);
        let delay = backoff.next().unwrap_or(MAX_BACKOFF);
        tokio::time::sleep(delay).await;
    }
}

struct Inner {
    client: Client,
    metrics: Arc<Metrics>,
    changes: broadcast::Sender<ServiceKey>,
    watches: Mutex<BTreeMap<ServiceKey, ServiceWatch>>,
}

impl Inner {
    fn watches(&self) -> MutexGuard<'_, BTreeMap<ServiceKey, ServiceWatch>> {
        self.watches.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Read-only state of the two watchers of one Service.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceWatchState {
    pub key: ServiceKey,
    /// Both watchers completed their first list.
    pub synced: bool,
    /// Both watchers are up (no error since their last successful event).
    pub up: bool,
}

/// Holds the Service and EndpointSlice watchers of every referenced Service.
/// Cheap to clone; the watchers stop when the last clone is dropped.
#[derive(Clone)]
pub struct WatchRegistry {
    inner: Arc<Inner>,
}

impl WatchRegistry {
    pub fn new(client: kube::Client, metrics: Arc<Metrics>) -> Self {
        let (changes, _) = broadcast::channel(CHANGE_CHANNEL_CAPACITY);
        WatchRegistry {
            inner: Arc::new(Inner {
                client,
                metrics,
                changes,
                watches: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// Sets the watched Services to exactly `keys` (the union over all
    /// upstreams): starts watchers for new keys, stops removed ones and keeps
    /// unchanged ones running. Must be called inside a Tokio runtime.
    pub fn set_services(&self, keys: BTreeSet<ServiceKey>) {
        let mut watches = self.inner.watches();
        let current: BTreeSet<ServiceKey> = watches.keys().cloned().collect();
        let (added, removed) = diff_keys(&current, &keys);
        for key in removed {
            if let Some(watch) = watches.remove(&key) {
                watch.shared.stop();
                info!(service = %key, "stopped watching Service");
                // Dropping `watch` aborts its tasks.
            }
        }
        for key in added {
            info!(service = %key, "watching Service");
            let watch = ServiceWatch::start(
                &self.inner.client,
                key.clone(),
                self.inner.metrics.clone(),
                self.inner.changes.clone(),
            );
            watches.insert(key, watch);
        }
    }

    /// The currently watched Services.
    pub fn services(&self) -> BTreeSet<ServiceKey> {
        self.inner.watches().keys().cloned().collect()
    }

    /// Receives a [`ServiceKey`] whenever that Service or any of its
    /// EndpointSlices changed (apply, delete, or a completed (re-)list).
    ///
    /// The channel is bounded. A receiver that falls behind gets
    /// `RecvError::Lagged`: it lost notifications and **must treat every
    /// Service as possibly changed** (reconcile everything). State is always
    /// read from [`SnapshotSource::snapshot`], so notifications only say what
    /// to look at, never what the new state is.
    pub fn subscribe(&self) -> broadcast::Receiver<ServiceKey> {
        self.inner.changes.subscribe()
    }

    /// True when every current watcher (Service and EndpointSlices of every
    /// watched Service) has completed its first list. True for an empty set.
    pub fn is_synced(&self) -> bool {
        self.inner.watches().values().all(ServiceWatch::is_synced)
    }

    /// Watched Services with at least one watcher (Service or
    /// EndpointSlices) that has not completed its first list yet. Empty when
    /// [`is_synced`](Self::is_synced).
    pub fn unsynced(&self) -> BTreeSet<ServiceKey> {
        self.inner
            .watches()
            .iter()
            .filter(|(_, w)| !w.is_synced())
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Per watched Service: whether both watchers have listed once and whether
    /// both are currently up (for the status endpoint, the design notes).
    pub fn watch_states(&self) -> Vec<ServiceWatchState> {
        self.inner
            .watches()
            .iter()
            .map(|(key, w)| ServiceWatchState {
                key: key.clone(),
                synced: w.is_synced(),
                up: w.shared.state().health.all_up(),
            })
            .collect()
    }

    /// Waits until [`is_synced`](Self::is_synced) or `timeout`. Returns
    /// whether all current watchers have listed once. Watchers added by a
    /// concurrent `set_services` are included; removed ones are ignored.
    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        let wait = async {
            loop {
                let pending: Vec<(Store<EndpointSlice>, Store<Service>)> = self
                    .inner
                    .watches()
                    .values()
                    .filter(|w| !w.is_synced())
                    .map(|w| (w.slices.clone(), w.service.clone()))
                    .collect();
                if pending.is_empty() {
                    return;
                }
                for (slices, service) in pending {
                    // Err means the watcher was removed meanwhile; fine.
                    let _ = slices.wait_until_ready().await;
                    let _ = service.wait_until_ready().await;
                }
                // The store is ready right before the `listed` flag is set.
                tokio::task::yield_now().await;
            }
        };
        let _ = tokio::time::timeout(timeout, wait).await;
        self.is_synced()
    }
}

impl SnapshotSource for WatchRegistry {
    fn snapshot(&self, key: &ServiceKey) -> ServiceSnapshot {
        let watches = self.inner.watches();
        let Some(watch) = watches.get(key) else {
            return ServiceSnapshot::default();
        };
        let service = watch
            .service
            .state()
            .into_iter()
            .find(|svc| svc.metadata.name.as_deref() == Some(key.name.as_str()));
        let mut slices = watch.slices.state();
        slices.sort_by(|a, b| a.metadata.name.cmp(&b.metadata.name));
        ServiceSnapshot { service, slices }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::core::Status;

    fn key(ns: &str, name: &str) -> ServiceKey {
        ServiceKey {
            namespace: ns.to_string(),
            name: name.to_string(),
        }
    }

    fn status(code: u16, reason: &str) -> Box<Status> {
        Box::new(Status {
            code,
            reason: reason.to_string(),
            message: format!("{code} {reason}"),
            ..Status::default()
        })
    }

    #[test]
    fn selectors() {
        assert_eq!(
            slice_label_selector("echo"),
            "kubernetes.io/service-name=echo"
        );
        assert_eq!(service_field_selector("echo"), "metadata.name=echo");
        let cfg = slice_watcher_config("echo");
        assert_eq!(
            cfg.label_selector.as_deref(),
            Some("kubernetes.io/service-name=echo")
        );
        assert_eq!(cfg.field_selector, None);
        assert_eq!(cfg.list_semantic, watcher::ListSemantic::Any);
        assert!(cfg.bookmarks);
        let cfg = service_watcher_config("echo");
        assert_eq!(cfg.field_selector.as_deref(), Some("metadata.name=echo"));
        assert_eq!(cfg.label_selector, None);
        assert_eq!(cfg.list_semantic, watcher::ListSemantic::Any);
        assert!(cfg.bookmarks);
    }

    #[test]
    fn classifies_errors() {
        let api = |code, reason| {
            watcher::Error::InitialListFailed(kube::Error::Api(status(code, reason)))
        };
        assert_eq!(classify_error(&api(403, "Forbidden")), "forbidden");
        assert_eq!(classify_error(&api(401, "Unauthorized")), "unauthorized");
        assert_eq!(classify_error(&api(404, "NotFound")), "not_found");
        assert_eq!(classify_error(&api(500, "InternalError")), "api");
        assert_eq!(
            classify_error(&watcher::Error::WatchError(status(410, "Expired"))),
            "gone"
        );
        assert_eq!(
            classify_error(&watcher::Error::WatchStartFailed(kube::Error::Api(status(
                403,
                "Forbidden"
            )))),
            "forbidden"
        );
        assert_eq!(classify_error(&watcher::Error::NoResourceVersion), "other");
        assert!(is_gone(&watcher::Error::WatchError(status(410, "Gone"))));
        assert!(!is_gone(&api(403, "Forbidden")));
    }

    #[test]
    fn forbidden_message_names_permission() {
        let err = watcher::Error::InitialListFailed(kube::Error::Api(status(403, "Forbidden")));
        let msg = down_message(&key("prod", "echo"), WatchKind::EndpointSlices, &err);
        assert!(msg.contains("endpointslices.discovery.k8s.io"), "{msg}");
        assert!(msg.contains("get/list/watch"), "{msg}");
        assert!(msg.contains("namespace prod"), "{msg}");
        let msg = down_message(&key("prod", "echo"), WatchKind::Service, &err);
        assert!(msg.contains("services"), "{msg}");

        let err = watcher::Error::InitialListFailed(kube::Error::Api(status(500, "Boom")));
        let msg = down_message(&key("prod", "echo"), WatchKind::Service, &err);
        assert!(!msg.contains("forbidden"), "{msg}");
    }

    #[test]
    fn diffs_key_sets() {
        let current: BTreeSet<_> = [key("a", "x"), key("a", "y")].into();
        let desired: BTreeSet<_> = [key("a", "y"), key("b", "z")].into();
        let (added, removed) = diff_keys(&current, &desired);
        assert_eq!(added, vec![key("b", "z")]);
        assert_eq!(removed, vec![key("a", "x")]);
        let (added, removed) = diff_keys(&desired, &desired);
        assert!(added.is_empty() && removed.is_empty());
        let (added, removed) = diff_keys(&BTreeSet::new(), &desired);
        assert_eq!(added.len(), 2);
        assert!(removed.is_empty());
    }

    #[test]
    fn health_transitions_log_once() {
        let mut h = KeyHealth::default();
        assert!(!h.all_up());
        // Normal start: no recovery message.
        assert_eq!(h.on_ok(WatchKind::Service), Transition::None);
        assert!(!h.all_up());
        assert_eq!(h.on_ok(WatchKind::EndpointSlices), Transition::None);
        assert!(h.all_up());
        // Goes down once, repeated errors are silent.
        assert_eq!(h.on_error(WatchKind::EndpointSlices), Transition::WentDown);
        assert_eq!(h.on_error(WatchKind::EndpointSlices), Transition::None);
        assert!(!h.all_up());
        assert_eq!(h.on_ok(WatchKind::EndpointSlices), Transition::Recovered);
        assert_eq!(h.on_ok(WatchKind::EndpointSlices), Transition::None);
        assert!(h.all_up());
        // A first list that fails counts as going down.
        let mut h = KeyHealth::default();
        assert_eq!(h.on_error(WatchKind::Service), Transition::WentDown);
    }

    fn unreachable_client() -> Client {
        // Port 1 on loopback: connections are refused immediately.
        let config = kube::Config::new("http://127.0.0.1:1".parse().unwrap());
        Client::try_from(config).unwrap()
    }

    #[tokio::test]
    async fn registry_against_unreachable_apiserver() {
        let metrics = Arc::new(Metrics::new());
        let registry = WatchRegistry::new(unreachable_client(), metrics.clone());
        assert!(registry.is_synced(), "empty set is synced");
        assert!(registry.unsynced().is_empty());
        assert!(registry.wait_ready(Duration::from_millis(10)).await);

        let k = key("prod", "echo");
        registry.set_services([k.clone()].into());
        assert_eq!(registry.services(), [k.clone()].into());
        assert!(!registry.is_synced());
        assert!(!registry.wait_ready(Duration::from_millis(300)).await);
        assert_eq!(registry.unsynced(), [k.clone()].into());

        // Errors are counted and the watch is reported down.
        let errors = || {
            metrics.value(
                "pingap_k8s_watch_errors_total",
                &[
                    ("k8s_namespace_name", "prod"),
                    ("k8s_service_name", "echo"),
                    ("error_type", "transport"),
                ],
            )
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while errors() < 2 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(errors() >= 2, "errors counted for both watchers");
        assert_eq!(
            metrics.value(
                "pingap_k8s_watch_up",
                &[("k8s_namespace_name", "prod"), ("k8s_service_name", "echo")]
            ),
            0
        );

        // Unknown Service: empty snapshot. Known but not listed: empty too.
        let snap = registry.snapshot(&key("prod", "other"));
        assert!(snap.service.is_none() && snap.slices.is_empty());
        let snap = registry.snapshot(&k);
        assert!(snap.service.is_none() && snap.slices.is_empty());

        // Unchanged keys keep their watcher (same shared state).
        let before = Arc::as_ptr(&registry.inner.watches()[&k].shared);
        registry.set_services([k.clone(), key("prod", "two")].into());
        let after = Arc::as_ptr(&registry.inner.watches()[&k].shared);
        assert_eq!(before, after);

        // Removing stops the watchers and drops the watch_up series.
        registry.set_services(BTreeSet::new());
        assert!(registry.services().is_empty());
        assert!(registry.is_synced());
        assert!(registry.unsynced().is_empty());
        assert!(!metrics.encode().contains("pingap_k8s_watch_up{"));
    }
}

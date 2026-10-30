//! Gateway provider: the cluster-wide watches of GatewayClasses, Gateways, HTTPRoutes,
//! ReferenceGrants and Namespaces.
//!
//! Each kind has one watcher feeding a [`reflector::Store`](kube::runtime::reflector::Store).
//! Every object is kept, whatever its class: the controller decides what it handles, so a
//! class change is seen like any other update. `managedFields` and the
//! `last-applied-configuration` annotation are dropped before an object is stored;
//! Namespaces keep only what `allowedRoutes.namespaces.selector` reads (name and labels).
//!
//! The Gateway API CRDs may not be installed. A list that fails with 404 logs one warning
//! for that kind, empties its store and reports it as listed (an empty set), so the
//! controller and the readiness check do not wait for it. The watcher keeps retrying with
//! backoff and picks the kind up once the CRD appears.
//!
//! Other failures follow [`crate::watch`]: an error logs once (a 403 names the missing
//! permission), the watcher waits for kube's [`DefaultBackoff`] delay and starts over with a
//! fresh list. The store keeps its last contents while the watch is down. The runner is
//! shared with the Secret watches ([`super::secrets`]).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::core::v1::Namespace;
use kube::runtime::reflector::Store;
use kube::runtime::reflector::store::Writer;
use kube::runtime::watcher::{self, DefaultBackoff, Event};
use kube::{Api, Client, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::api::gatewayclasses::GatewayClass;
use super::api::gateways::Gateway;
use super::api::httproutes::HttpRoute;
use super::api::referencegrants::ReferenceGrant;
use super::model::GatewaySource;
use crate::watch::classify_error;

/// Capacity of the change broadcasts. A receiver that falls further behind gets
/// `RecvError::Lagged` and must treat everything as changed.
pub(crate) const CHANGE_CHANNEL_CAPACITY: usize = 256;

/// Delay used if the backoff iterator ever ends (it does not with kube's default policy).
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Annotation `kubectl apply` leaves on objects; for a Secret it holds a copy of the key
/// material, so it is never kept in memory.
pub(crate) const LAST_APPLIED_ANNOTATION: &str = "kubectl.kubernetes.io/last-applied-configuration";

/// The Gateway API release whose CRDs the provider expects.
pub const GATEWAY_API_VERSION: &str = "v1.7.0";

/// One watched kind, for logs, states and the 403 message.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WatchedKind {
    GatewayClass,
    Gateway,
    HttpRoute,
    ReferenceGrant,
    Namespace,
}

impl WatchedKind {
    pub const ALL: [WatchedKind; 5] = [
        WatchedKind::GatewayClass,
        WatchedKind::Gateway,
        WatchedKind::HttpRoute,
        WatchedKind::ReferenceGrant,
        WatchedKind::Namespace,
    ];

    /// Plural for logs: "GatewayClasses".
    pub fn as_str(self) -> &'static str {
        match self {
            WatchedKind::GatewayClass => "GatewayClasses",
            WatchedKind::Gateway => "Gateways",
            WatchedKind::HttpRoute => "HTTPRoutes",
            WatchedKind::ReferenceGrant => "ReferenceGrants",
            WatchedKind::Namespace => "Namespaces",
        }
    }

    /// The CRD that defines the kind; `None` for core kinds.
    pub fn crd(self) -> Option<&'static str> {
        match self {
            WatchedKind::GatewayClass => Some("gatewayclasses.gateway.networking.k8s.io"),
            WatchedKind::Gateway => Some("gateways.gateway.networking.k8s.io"),
            WatchedKind::HttpRoute => Some("httproutes.gateway.networking.k8s.io"),
            WatchedKind::ReferenceGrant => Some("referencegrants.gateway.networking.k8s.io"),
            WatchedKind::Namespace => None,
        }
    }
}

/// The permission a watch of `kind` needs, for the 403 message.
pub fn rbac_hint(kind: WatchedKind) -> &'static str {
    match kind {
        WatchedKind::GatewayClass => "get/list/watch on gatewayclasses.gateway.networking.k8s.io",
        WatchedKind::Gateway => "get/list/watch on gateways.gateway.networking.k8s.io",
        WatchedKind::HttpRoute => "get/list/watch on httproutes.gateway.networking.k8s.io",
        WatchedKind::ReferenceGrant => {
            "get/list/watch on referencegrants.gateway.networking.k8s.io"
        }
        WatchedKind::Namespace => "list/watch on namespaces (core API group \"\")",
    }
}

/// Watcher config of the cluster-wide watches (every object of the kind).
pub fn cluster_watcher_config() -> watcher::Config {
    watcher::Config::default().any_semantic()
}

/// Drops the parts of an object the provider never reads.
pub(crate) fn strip_metadata<K: ResourceExt>(obj: &mut K) {
    obj.managed_fields_mut().clear();
    obj.annotations_mut().remove(LAST_APPLIED_ANNOTATION);
}

/// Keeps only the name, labels and resource version of a Namespace: the route namespace
/// selector reads nothing else.
pub(crate) fn strip_namespace(ns: &mut Namespace) {
    ns.metadata.managed_fields = None;
    ns.metadata.annotations = None;
    ns.metadata.owner_references = None;
    ns.metadata.finalizers = None;
    ns.spec = None;
    ns.status = None;
}

/// Where a watch looks, for the 403 message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Scope {
    Cluster,
    Namespace(String),
}

/// The log message for a watch that went down. A 403 names the missing permission.
pub(crate) fn down_message(what: &str, scope: &Scope, rbac: &str, err: &watcher::Error) -> String {
    if classify_error(err) == "forbidden" {
        match scope {
            Scope::Cluster => format!(
                "watch of {what} is forbidden: the controller's ServiceAccount is missing \
                 {rbac} cluster-wide; grant it with a ClusterRole: {err}"
            ),
            Scope::Namespace(ns) => format!(
                "watch of {what} is forbidden: the controller's ServiceAccount is missing \
                 {rbac} in namespace {ns}; grant it with a Role in that namespace: {err}"
            ),
        }
    } else {
        format!("watch of {what} is down, retrying with backoff: {err}")
    }
}

/// The warning for a kind whose CRD is not installed.
pub(crate) fn missing_crd_message(what: &str, crd: &str) -> String {
    format!(
        "Gateway API CRD {crd} not installed: treating {what} as empty and retrying; install \
         the {GATEWAY_API_VERSION} standard-install.yaml \
         (https://github.com/kubernetes-sigs/gateway-api/releases/download/\
         {GATEWAY_API_VERSION}/standard-install.yaml)"
    )
}

/// What a [`ResourceWatch`] watches, for logs and error handling.
#[derive(Clone, Debug)]
pub(crate) struct WatchTarget {
    /// For logs: "HTTPRoutes", "Secret prod/tls".
    pub what: String,
    pub scope: Scope,
    /// The permission it needs, for the 403 message.
    pub rbac: &'static str,
    /// The CRD that defines the kind: a 404 means it is not installed, and the kind is
    /// treated as empty. `None`: a 404 is an error like any other.
    pub crd: Option<&'static str>,
}

/// Aborts the task when dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// State shared between a watcher handle and its task.
struct WatchShared {
    target: WatchTarget,
    changes: broadcast::Sender<()>,
    /// Completed at least one full list (or found the CRD missing).
    listed: AtomicBool,
    down: AtomicBool,
    /// The last list failed with 404: the CRD is not installed.
    missing: AtomicBool,
    /// Set when the watcher is removed, so a task still running between `abort()` and its
    /// cancellation stays silent.
    stopped: AtomicBool,
}

impl WatchShared {
    fn notify(&self) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        // Err only means there is no receiver right now.
        let _ = self.changes.send(());
    }

    fn on_ok(&self) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        if self.missing.swap(false, Ordering::AcqRel) {
            info!(watch = %self.target.what, "Gateway API CRD installed, watching");
        }
        if self.down.swap(false, Ordering::AcqRel) {
            info!(watch = %self.target.what, "watch recovered");
        }
    }

    fn on_error(&self, err: &watcher::Error) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        let reason = classify_error(err);
        if self.down.swap(true, Ordering::AcqRel) {
            debug!(watch = %self.target.what, reason, "watch still down: {err}");
        } else {
            let t = &self.target;
            error!(
                watch = %t.what,
                reason,
                "{}",
                down_message(&t.what, &t.scope, t.rbac, err)
            );
        }
    }

    /// The CRD is not installed: the kind is listed and empty. Returns whether that is new.
    fn on_missing(&self, crd: &str) -> bool {
        self.listed.store(true, Ordering::Release);
        self.down.store(false, Ordering::Release);
        let new = !self.missing.swap(true, Ordering::AcqRel);
        if new && !self.stopped.load(Ordering::Acquire) {
            warn!(
                watch = %self.target.what,
                "{}",
                missing_crd_message(&self.target.what, crd)
            );
        }
        new
    }
}

/// One running watcher with its store. Dropping it stops the watcher.
pub(crate) struct ResourceWatch<K>
where
    K: Resource + 'static,
    K::DynamicType: Eq + Hash + Clone,
{
    store: Store<K>,
    shared: Arc<WatchShared>,
    _task: AbortOnDrop,
}

impl<K> ResourceWatch<K>
where
    K: Resource + ResourceExt + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Default + Eq + Hash + Clone,
{
    /// Starts watching `api` with `config`, storing objects after `strip`. Must be called
    /// inside a Tokio runtime.
    pub(crate) fn start(
        api: Api<K>,
        config: watcher::Config,
        target: WatchTarget,
        strip: fn(&mut K),
        changes: broadcast::Sender<()>,
    ) -> Self {
        let shared = Arc::new(WatchShared {
            target,
            changes,
            listed: AtomicBool::new(false),
            down: AtomicBool::new(false),
            missing: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        });
        let writer = Writer::<K>::default();
        let store = writer.as_reader();
        let task = tokio::spawn(run_watch(api, config, writer, strip, shared.clone()));
        ResourceWatch {
            store,
            shared,
            _task: AbortOnDrop(task),
        }
    }

    /// Listed once, or found its CRD missing.
    pub(crate) fn is_synced(&self) -> bool {
        self.shared.listed.load(Ordering::Acquire)
    }

    /// Whether the watch is currently up (listed and no error since). A missing CRD counts
    /// as up: there is nothing to watch.
    pub(crate) fn is_up(&self) -> bool {
        self.is_synced() && !self.shared.down.load(Ordering::Acquire)
    }

    /// Whether the last list found the CRD missing.
    pub(crate) fn is_missing(&self) -> bool {
        self.shared.missing.load(Ordering::Acquire)
    }

    pub(crate) fn store(&self) -> &Store<K> {
        &self.store
    }

    /// Silences the task before it is dropped (and aborted).
    pub(crate) fn stop(&self) {
        self.shared.stopped.store(true, Ordering::Release);
    }
}

/// Runs one watcher forever, feeding `writer` and notifying subscribers.
async fn run_watch<K>(
    api: Api<K>,
    config: watcher::Config,
    mut writer: Writer<K>,
    strip: fn(&mut K),
    shared: Arc<WatchShared>,
) where
    K: Resource + ResourceExt + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Default + Eq + Hash + Clone,
{
    let mut backoff = DefaultBackoff::default();
    loop {
        let mut stream = watcher::watcher(api.clone(), config.clone()).boxed();
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => {
                    let event = event.modify(strip);
                    writer.apply_watcher_event(&event);
                    match event {
                        Event::InitDone => {
                            shared.listed.store(true, Ordering::Release);
                            shared.on_ok();
                            shared.notify();
                        }
                        Event::Apply(_) | Event::Delete(_) => {
                            shared.on_ok();
                            shared.notify();
                        }
                        // The store only changes on InitDone.
                        Event::Init | Event::InitApply(_) => {}
                    }
                }
                Err(err) if classify_error(&err) == "gone" => {
                    debug!(watch = %shared.target.what, "watch expired, re-listing: {err}");
                }
                Err(err) if classify_error(&err) == "not_found" && shared.target.crd.is_some() => {
                    let crd = shared.target.crd.unwrap_or_default();
                    debug!(watch = %shared.target.what, "CRD {crd} not found: {err}");
                    // An empty list: the store becomes empty and ready.
                    let had_objects = !writer.as_reader().is_empty();
                    writer.apply_watcher_event(&Event::Init);
                    writer.apply_watcher_event(&Event::InitDone);
                    if shared.on_missing(crd) || had_objects {
                        shared.notify();
                    }
                    break;
                }
                Err(err) => {
                    shared.on_error(&err);
                    break;
                }
            }
        }
        // Start over with a fresh list after the backoff delay; the list brings the store
        // back in line with whatever changed while it was down.
        drop(stream);
        let delay = backoff.next().unwrap_or(MAX_BACKOFF);
        tokio::time::sleep(delay).await;
    }
}

/// Waits until every store in `pending` is ready, re-reading the set after each round, or
/// until `timeout`.
pub(crate) async fn wait_stores<K, F>(timeout: Duration, mut pending: F)
where
    K: Resource + Clone + 'static,
    K::DynamicType: Eq + Hash + Clone,
    F: FnMut() -> Vec<Store<K>>,
{
    let wait = async {
        loop {
            let stores = pending();
            if stores.is_empty() {
                return;
            }
            for store in stores {
                // Err means the watcher was removed meanwhile; fine.
                let _ = store.wait_until_ready().await;
            }
            // The store is ready right before the `listed` flag is set.
            tokio::task::yield_now().await;
        }
    };
    let _ = tokio::time::timeout(timeout, wait).await;
}

/// Read-only state of the watcher of one kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KindWatchState {
    pub kind: WatchedKind,
    /// Listed once, or found its CRD missing.
    pub synced: bool,
    pub up: bool,
    /// The CRD is not installed.
    pub crd_missing: bool,
}

struct Inner {
    changes: broadcast::Sender<()>,
    classes: ResourceWatch<GatewayClass>,
    gateways: ResourceWatch<Gateway>,
    routes: ResourceWatch<HttpRoute>,
    grants: ResourceWatch<ReferenceGrant>,
    namespaces: ResourceWatch<Namespace>,
}

impl Inner {
    /// (kind, synced, up, missing) of every watch.
    fn flags(&self) -> [(WatchedKind, bool, bool, bool); 5] {
        fn f<K>(kind: WatchedKind, w: &ResourceWatch<K>) -> (WatchedKind, bool, bool, bool)
        where
            K: Resource + ResourceExt + Clone + DeserializeOwned + Debug + Send + Sync,
            K::DynamicType: Default + Eq + Hash + Clone,
        {
            (kind, w.is_synced(), w.is_up(), w.is_missing())
        }
        [
            f(WatchedKind::GatewayClass, &self.classes),
            f(WatchedKind::Gateway, &self.gateways),
            f(WatchedKind::HttpRoute, &self.routes),
            f(WatchedKind::ReferenceGrant, &self.grants),
            f(WatchedKind::Namespace, &self.namespaces),
        ]
    }
}

/// Starts a cluster-wide watch of `kind`.
fn start_cluster<K>(
    client: &Client,
    kind: WatchedKind,
    strip: fn(&mut K),
    changes: &broadcast::Sender<()>,
) -> ResourceWatch<K>
where
    K: Resource + ResourceExt + Clone + DeserializeOwned + Debug + Send + Sync + 'static,
    K::DynamicType: Default + Eq + Hash + Clone,
{
    ResourceWatch::start(
        Api::all(client.clone()),
        cluster_watcher_config(),
        WatchTarget {
            what: kind.as_str().to_string(),
            scope: Scope::Cluster,
            rbac: rbac_hint(kind),
            crd: kind.crd(),
        },
        strip,
        changes.clone(),
    )
}

/// The Gateway API objects and Namespaces of the cluster. Cheap to clone; the watchers stop
/// when the last clone is dropped.
#[derive(Clone)]
pub struct GatewayWatch {
    inner: Arc<Inner>,
}

impl GatewayWatch {
    /// Starts the five watches. Must be called inside a Tokio runtime.
    pub fn new(client: Client) -> Self {
        let (changes, _) = broadcast::channel(CHANGE_CHANNEL_CAPACITY);
        let inner = Inner {
            classes: start_cluster(&client, WatchedKind::GatewayClass, strip_metadata, &changes),
            gateways: start_cluster(&client, WatchedKind::Gateway, strip_metadata, &changes),
            routes: start_cluster(&client, WatchedKind::HttpRoute, strip_metadata, &changes),
            grants: start_cluster(
                &client,
                WatchedKind::ReferenceGrant,
                strip_metadata,
                &changes,
            ),
            namespaces: start_cluster(&client, WatchedKind::Namespace, strip_namespace, &changes),
            changes,
        };
        info!("watching GatewayClasses, Gateways, HTTPRoutes, ReferenceGrants and Namespaces");
        GatewayWatch {
            inner: Arc::new(inner),
        }
    }

    /// Per kind: listed once, up, and whether its CRD is missing.
    pub fn watch_states(&self) -> Vec<KindWatchState> {
        self.inner
            .flags()
            .into_iter()
            .map(|(kind, synced, up, crd_missing)| KindWatchState {
                kind,
                synced,
                up,
                crd_missing,
            })
            .collect()
    }

    /// The kinds whose CRD is not installed.
    pub fn missing_crds(&self) -> Vec<WatchedKind> {
        self.inner
            .flags()
            .into_iter()
            .filter(|(_, _, _, missing)| *missing)
            .map(|(kind, ..)| kind)
            .collect()
    }

    /// Waits until [`is_synced`](GatewaySource::is_synced) or `timeout`. Returns whether
    /// every watch has listed once (or found its CRD missing).
    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        let mut rx = self.inner.changes.subscribe();
        let wait = async {
            // Subscribed before checking: a list completing in between still wakes us.
            while !GatewaySource::is_synced(self) {
                if let Err(broadcast::error::RecvError::Closed) = rx.recv().await {
                    return;
                }
            }
        };
        let _ = tokio::time::timeout(timeout, wait).await;
        GatewaySource::is_synced(self)
    }
}

impl GatewaySource for GatewayWatch {
    fn classes(&self) -> Vec<Arc<GatewayClass>> {
        self.inner.classes.store().state()
    }

    fn gateways(&self) -> Vec<Arc<Gateway>> {
        self.inner.gateways.store().state()
    }

    fn routes(&self) -> Vec<Arc<HttpRoute>> {
        self.inner.routes.store().state()
    }

    fn grants(&self) -> Vec<Arc<ReferenceGrant>> {
        self.inner.grants.store().state()
    }

    fn namespaces(&self) -> BTreeMap<String, Arc<Namespace>> {
        self.inner
            .namespaces
            .store()
            .state()
            .into_iter()
            .filter_map(|ns| ns.metadata.name.clone().map(|name| (name, ns)))
            .collect()
    }

    /// False until every watch has listed once, so nothing is planned (or deleted) from a
    /// partial view. A kind whose CRD is missing counts as listed and empty.
    fn is_synced(&self) -> bool {
        self.inner.flags().iter().all(|(_, synced, ..)| *synced)
    }

    /// Fires on every apply, delete, completed (re-)list and CRD found missing.
    fn changes(&self) -> broadcast::Receiver<()> {
        self.inner.changes.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::{NamespaceSpec, Secret};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ManagedFieldsEntry, ObjectMeta};
    use kube::core::Status;

    fn unreachable_client() -> Client {
        // Port 1 on loopback: connections are refused immediately.
        let config = kube::Config::new("http://127.0.0.1:1".parse().unwrap());
        Client::try_from(config).unwrap()
    }

    fn api_error(code: u16, reason: &str) -> watcher::Error {
        watcher::Error::InitialListFailed(kube::Error::Api(Box::new(Status {
            code,
            reason: reason.into(),
            message: reason.into(),
            ..Status::default()
        })))
    }

    #[test]
    fn watcher_config() {
        let cfg = cluster_watcher_config();
        assert_eq!(cfg.label_selector, None);
        assert_eq!(cfg.field_selector, None);
        assert_eq!(cfg.list_semantic, watcher::ListSemantic::Any);
        assert!(cfg.bookmarks);
    }

    #[test]
    fn strips_metadata() {
        let mut secret = Secret {
            metadata: ObjectMeta {
                name: Some("tls".into()),
                annotations: Some(
                    [
                        (LAST_APPLIED_ANNOTATION.to_string(), "{\"data\":{}}".into()),
                        ("keep".to_string(), "yes".into()),
                    ]
                    .into(),
                ),
                managed_fields: Some(vec![ManagedFieldsEntry::default()]),
                ..Default::default()
            },
            ..Default::default()
        };
        strip_metadata(&mut secret);
        assert!(
            secret
                .metadata
                .managed_fields
                .unwrap_or_default()
                .is_empty()
        );
        let annotations = secret.metadata.annotations.unwrap();
        assert_eq!(annotations.len(), 1);
        assert_eq!(annotations["keep"], "yes");
    }

    #[test]
    fn namespaces_keep_name_and_labels() {
        let mut ns = Namespace {
            metadata: ObjectMeta {
                name: Some("prod".into()),
                labels: Some([("team".to_string(), "web".to_string())].into()),
                annotations: Some([("note".to_string(), "x".to_string())].into()),
                managed_fields: Some(vec![ManagedFieldsEntry::default()]),
                ..Default::default()
            },
            spec: Some(NamespaceSpec::default()),
            ..Default::default()
        };
        strip_namespace(&mut ns);
        assert_eq!(ns.metadata.name.as_deref(), Some("prod"));
        assert_eq!(ns.metadata.labels.unwrap()["team"], "web");
        assert_eq!(ns.metadata.annotations, None);
        assert_eq!(ns.metadata.managed_fields, None);
        assert_eq!(ns.spec, None);
    }

    #[test]
    fn forbidden_message_names_permission_and_scope() {
        let err = api_error(403, "Forbidden");
        let msg = down_message(
            "HTTPRoutes",
            &Scope::Cluster,
            rbac_hint(WatchedKind::HttpRoute),
            &err,
        );
        assert!(
            msg.contains("httproutes.gateway.networking.k8s.io"),
            "{msg}"
        );
        assert!(msg.contains("ClusterRole"), "{msg}");
        let msg = down_message(
            "Secret prod/tls",
            &Scope::Namespace("prod".into()),
            "get/list/watch on secrets",
            &err,
        );
        assert!(msg.contains("namespace prod"), "{msg}");
        let msg = down_message("HTTPRoutes", &Scope::Cluster, "x", &api_error(500, "Boom"));
        assert!(!msg.contains("forbidden"), "{msg}");
    }

    #[test]
    fn missing_crd_is_a_404() {
        assert_eq!(classify_error(&api_error(404, "NotFound")), "not_found");
        let msg = missing_crd_message("Gateways", WatchedKind::Gateway.crd().unwrap());
        assert!(msg.contains("gateways.gateway.networking.k8s.io"), "{msg}");
        assert!(msg.contains("v1.7.0 standard-install.yaml"), "{msg}");
        assert_eq!(WatchedKind::Namespace.crd(), None);
        assert!(WatchedKind::ALL.iter().all(|k| !rbac_hint(*k).is_empty()));
    }

    #[test]
    fn missing_crd_counts_as_listed_and_warns_once() {
        let (changes, _) = broadcast::channel(4);
        let shared = WatchShared {
            target: WatchTarget {
                what: "Gateways".into(),
                scope: Scope::Cluster,
                rbac: "x",
                crd: WatchedKind::Gateway.crd(),
            },
            changes,
            listed: AtomicBool::new(false),
            down: AtomicBool::new(true),
            missing: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        };
        assert!(shared.on_missing("gateways.gateway.networking.k8s.io"));
        assert!(shared.listed.load(Ordering::Acquire));
        assert!(!shared.down.load(Ordering::Acquire));
        assert!(!shared.on_missing("gateways.gateway.networking.k8s.io"));
        // A successful list clears it.
        shared.on_ok();
        assert!(!shared.missing.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn watch_against_unreachable_apiserver() {
        let watch = GatewayWatch::new(unreachable_client());
        assert!(!GatewaySource::is_synced(&watch));
        assert!(!watch.wait_ready(Duration::from_millis(200)).await);
        assert!(watch.classes().is_empty());
        assert!(watch.gateways().is_empty());
        assert!(watch.routes().is_empty());
        assert!(watch.grants().is_empty());
        assert!(watch.namespaces().is_empty());
        assert!(watch.missing_crds().is_empty());
        let states = watch.watch_states();
        assert_eq!(states.len(), 5);
        assert!(states.iter().all(|s| !s.synced && !s.up && !s.crd_missing));
    }
}

//! Gateway provider: the Secrets named by HTTPS listener `certificateRefs` and Gateway
//! backend client certificates, and the ConfigMaps named by BackendTLSPolicy
//! `caCertificateRefs`.
//!
//! One watcher per referenced object, selected by `metadata.name=<name>`,
//! never a list of every Secret or ConfigMap in a namespace. That keeps other
//! objects out of the controller's memory and works under a Role restricted
//! with `resourceNames` (the apiserver authorizes a list or watch with a
//! `metadata.name` field selector against `resourceNames`).
//! [`SecretRegistry::set_secrets`] and [`SecretRegistry::set_config_maps`]
//! take the union over all handled Gateways and policies, so an object shared
//! by several of them has one watcher; they start watchers for new objects,
//! stop removed ones and keep unchanged ones running.
//!
//! `managedFields` and the `last-applied-configuration` annotation (a second
//! copy of the key) are dropped before an object is stored. Failures are
//! handled like the Gateway API watches ([`super::watch`]).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use k8s_openapi::NamespaceResourceScope;
use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::runtime::watcher;
use kube::{Api, Client, Resource, ResourceExt};
use serde::de::DeserializeOwned;
use tokio::sync::broadcast;
use tracing::info;

use super::model::{ConfigMapKey, ObjectKey, SecretKey, SecretSource};
use super::watch::{
    CHANGE_CHANNEL_CAPACITY, ResourceWatch, Scope, WatchTarget, strip_metadata, wait_stores,
};

/// The permission a Secret watch needs, for the 403 message.
const SECRET_RBAC: &str = "get/list/watch on secrets (core API group \"\"; with resourceNames, \
                           this Secret's name must be listed)";
/// The permission a ConfigMap watch needs, for the 403 message.
const CONFIG_MAP_RBAC: &str = "get/list/watch on configmaps (core API group \"\"; with \
                               resourceNames, this ConfigMap's name must be listed)";

/// Field selector for the object `name`.
pub fn secret_field_selector(name: &str) -> String {
    format!("metadata.name={name}")
}

/// Watcher config for the object `name`.
pub fn secret_watcher_config(name: &str) -> watcher::Config {
    watcher::Config::default()
        .fields(&secret_field_selector(name))
        .any_semantic()
}

/// One watcher per object of one kind, by key.
struct Watches<K>
where
    K: Resource + 'static,
    K::DynamicType: Eq + std::hash::Hash + Clone,
{
    /// For logs: "Secret", "ConfigMap".
    kind: &'static str,
    rbac: &'static str,
    map: Mutex<BTreeMap<ObjectKey, ResourceWatch<K>>>,
}

impl<K> Watches<K>
where
    K: Resource<Scope = NamespaceResourceScope>
        + ResourceExt
        + Clone
        + DeserializeOwned
        + Debug
        + Send
        + Sync
        + 'static,
    K::DynamicType: Default + Eq + std::hash::Hash + Clone,
{
    fn new(kind: &'static str, rbac: &'static str) -> Self {
        Watches {
            kind,
            rbac,
            map: Mutex::new(BTreeMap::new()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<ObjectKey, ResourceWatch<K>>> {
        self.map.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Watches exactly `keys`. Returns whether the set changed.
    fn set(
        &self,
        client: &Client,
        changes: &broadcast::Sender<()>,
        keys: BTreeSet<ObjectKey>,
    ) -> bool {
        let mut watches = self.lock();
        let mut changed = false;
        let removed: Vec<ObjectKey> = watches
            .keys()
            .filter(|k| !keys.contains(*k))
            .cloned()
            .collect();
        for key in removed {
            if let Some(watch) = watches.remove(&key) {
                watch.stop();
                info!(kind = self.kind, object = %key, "stopped watching {}", self.kind);
                changed = true;
            }
        }
        for key in keys {
            if watches.contains_key(&key) {
                continue;
            }
            info!(kind = self.kind, object = %key, "watching {}", self.kind);
            let watch = ResourceWatch::start(
                Api::namespaced(client.clone(), &key.namespace),
                secret_watcher_config(&key.name),
                WatchTarget {
                    what: format!("{} {key}", self.kind),
                    scope: Scope::Namespace(key.namespace.clone()),
                    rbac: self.rbac,
                    crd: None,
                },
                strip_metadata,
                changes.clone(),
            );
            watches.insert(key, watch);
            changed = true;
        }
        changed
    }

    /// The watched objects that exist.
    fn objects(&self) -> BTreeMap<ObjectKey, Arc<K>> {
        self.lock()
            .iter()
            .filter_map(|(key, w)| {
                w.store()
                    .state()
                    .into_iter()
                    .find(|s| s.meta().name.as_deref() == Some(key.name.as_str()))
                    .map(|s| (key.clone(), s))
            })
            .collect()
    }

    fn is_synced(&self) -> bool {
        self.lock().values().all(ResourceWatch::is_synced)
    }

    fn states(&self) -> Vec<SecretWatchState> {
        self.lock()
            .iter()
            .map(|(key, w)| SecretWatchState {
                key: key.clone(),
                synced: w.is_synced(),
                up: w.is_up(),
            })
            .collect()
    }

    fn pending(&self) -> Vec<kube::runtime::reflector::Store<K>> {
        self.lock()
            .values()
            .filter(|w| !w.is_synced())
            .map(|w| w.store().clone())
            .collect()
    }
}

struct Inner {
    client: Client,
    changes: broadcast::Sender<()>,
    secrets: Watches<Secret>,
    config_maps: Watches<ConfigMap>,
}

/// Read-only state of the watcher of one Secret or ConfigMap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretWatchState {
    pub key: SecretKey,
    pub synced: bool,
    pub up: bool,
}

/// Holds one watcher per referenced Secret and ConfigMap. Cheap to clone; the
/// watchers stop when the last clone is dropped.
#[derive(Clone)]
pub struct SecretRegistry {
    inner: Arc<Inner>,
}

impl SecretRegistry {
    pub fn new(client: Client) -> Self {
        let (changes, _) = broadcast::channel(CHANGE_CHANNEL_CAPACITY);
        SecretRegistry {
            inner: Arc::new(Inner {
                client,
                changes,
                secrets: Watches::new("Secret", SECRET_RBAC),
                config_maps: Watches::new("ConfigMap", CONFIG_MAP_RBAC),
            }),
        }
    }

    /// The currently watched Secrets.
    pub fn keys(&self) -> BTreeSet<SecretKey> {
        self.inner.secrets.lock().keys().cloned().collect()
    }

    /// The currently watched ConfigMaps.
    pub fn config_map_keys(&self) -> BTreeSet<ConfigMapKey> {
        self.inner.config_maps.lock().keys().cloned().collect()
    }

    /// Per watched Secret: listed once, and up.
    pub fn watch_states(&self) -> Vec<SecretWatchState> {
        self.inner.secrets.states()
    }

    /// Per watched ConfigMap: listed once, and up.
    pub fn config_map_watch_states(&self) -> Vec<SecretWatchState> {
        self.inner.config_maps.states()
    }

    /// Waits until [`is_synced`](SecretSource::is_synced) or `timeout`.
    /// Returns whether every current watcher has listed once.
    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        let inner = &self.inner;
        let deadline = tokio::time::Instant::now() + timeout;
        wait_stores(timeout, || inner.secrets.pending()).await;
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        wait_stores(left, || inner.config_maps.pending()).await;
        SecretSource::is_synced(self)
    }
}

impl SecretSource for SecretRegistry {
    /// Sets the watched Secrets to exactly `keys` (the union over all handled
    /// Gateways). Must be called inside a Tokio runtime.
    fn set_secrets(&self, keys: BTreeSet<SecretKey>) {
        let inner = &self.inner;
        if inner.secrets.set(&inner.client, &inner.changes, keys) {
            let _ = inner.changes.send(());
        }
    }

    /// The watched Secrets that exist. A Secret not listed yet is absent;
    /// check [`is_synced`](SecretSource::is_synced) before treating that as
    /// missing.
    fn secrets(&self) -> BTreeMap<SecretKey, Arc<Secret>> {
        self.inner.secrets.objects()
    }

    /// Sets the watched ConfigMaps to exactly `keys` (the union over the
    /// BackendTLSPolicies of backend Services). Must be called inside a Tokio
    /// runtime.
    fn set_config_maps(&self, keys: BTreeSet<ConfigMapKey>) {
        let inner = &self.inner;
        if inner.config_maps.set(&inner.client, &inner.changes, keys) {
            let _ = inner.changes.send(());
        }
    }

    /// The watched ConfigMaps that exist, like [`secrets`](SecretSource::secrets).
    fn config_maps(&self) -> BTreeMap<ConfigMapKey, Arc<ConfigMap>> {
        self.inner.config_maps.objects()
    }

    /// True when every current watcher has listed once. True for an empty set.
    fn is_synced(&self) -> bool {
        self.inner.secrets.is_synced() && self.inner.config_maps.is_synced()
    }

    /// Fires on every apply, delete, completed (re-)list and set change.
    fn changes(&self) -> broadcast::Receiver<()> {
        self.inner.changes.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(ns: &str, name: &str) -> SecretKey {
        SecretKey::new(ns, name)
    }

    fn unreachable_client() -> Client {
        // Port 1 on loopback: connections are refused immediately.
        let config = kube::Config::new("http://127.0.0.1:1".parse().unwrap());
        Client::try_from(config).unwrap()
    }

    #[test]
    fn selector() {
        assert_eq!(secret_field_selector("web-tls"), "metadata.name=web-tls");
        let cfg = secret_watcher_config("web-tls");
        assert_eq!(cfg.field_selector.as_deref(), Some("metadata.name=web-tls"));
        assert_eq!(cfg.label_selector, None);
        assert_eq!(cfg.list_semantic, watcher::ListSemantic::Any);
        assert!(cfg.bookmarks);
    }

    #[tokio::test]
    async fn registry_against_unreachable_apiserver() {
        let registry = SecretRegistry::new(unreachable_client());
        assert!(registry.is_synced(), "empty set is synced");
        assert!(registry.wait_ready(Duration::from_millis(10)).await);
        let mut rx = registry.changes();

        let k = key("prod", "web-tls");
        registry.set_secrets([k.clone()].into());
        assert!(rx.try_recv().is_ok());
        assert_eq!(registry.keys(), [k.clone()].into());
        assert!(!registry.is_synced());
        assert!(!registry.wait_ready(Duration::from_millis(200)).await);
        assert!(registry.secrets().is_empty());
        let states = registry.watch_states();
        assert_eq!(states.len(), 1);
        assert!(!states[0].synced && !states[0].up);

        // Unchanged keys keep their watcher and do not notify.
        while rx.try_recv().is_ok() {}
        registry.set_secrets([k.clone(), key("prod", "other")].into());
        assert_eq!(registry.keys().len(), 2);
        assert!(rx.try_recv().is_ok(), "an added key notifies");
        while rx.try_recv().is_ok() {}
        registry.set_secrets([k.clone(), key("prod", "other")].into());
        assert!(rx.try_recv().is_err(), "the same set does not notify");

        registry.set_secrets(BTreeSet::new());
        assert!(registry.keys().is_empty());
        assert!(registry.is_synced());
        assert!(rx.try_recv().is_ok(), "removal notifies");

        // ConfigMaps: their own set, same rules.
        while rx.try_recv().is_ok() {}
        let cm = key("prod", "ca");
        registry.set_config_maps([cm.clone()].into());
        assert!(rx.try_recv().is_ok());
        assert_eq!(registry.config_map_keys(), [cm.clone()].into());
        assert!(registry.keys().is_empty(), "Secrets are not touched");
        assert!(!registry.is_synced());
        assert!(registry.config_maps().is_empty());
        assert_eq!(registry.config_map_watch_states().len(), 1);
        while rx.try_recv().is_ok() {}
        registry.set_config_maps([cm].into());
        assert!(rx.try_recv().is_err(), "the same set does not notify");
        registry.set_config_maps(BTreeSet::new());
        assert!(registry.is_synced());
        assert!(rx.try_recv().is_ok(), "removal notifies");
    }
}

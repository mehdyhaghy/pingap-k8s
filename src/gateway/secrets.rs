//! Gateway provider: the Secrets named by HTTPS listener `certificateRefs`.
//!
//! One watcher per referenced Secret, selected by `metadata.name=<secret>`,
//! never a list of every Secret in a namespace. That keeps other Secrets out
//! of the controller's memory and works under a Role restricted with
//! `resourceNames` (the apiserver authorizes a list or watch with a
//! `metadata.name` field selector against `resourceNames`).
//! [`SecretRegistry::set_secrets`] takes the union over all handled Gateways,
//! so a Secret shared by several listeners has one watcher; it starts watchers
//! for new Secrets, stops removed ones and keeps unchanged ones running.
//!
//! `managedFields` and the `last-applied-configuration` annotation (a second
//! copy of the key) are dropped before a Secret is stored. Failures are
//! handled like the Gateway API watches ([`super::watch`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use k8s_openapi::api::core::v1::Secret;
use kube::runtime::watcher;
use kube::{Api, Client};
use tokio::sync::broadcast;
use tracing::info;

use super::model::{SecretKey, SecretSource};
use super::watch::{
    CHANGE_CHANNEL_CAPACITY, ResourceWatch, Scope, WatchTarget, strip_metadata, wait_stores,
};

/// The permission a Secret watch needs, for the 403 message.
const SECRET_RBAC: &str = "get/list/watch on secrets (core API group \"\"; with resourceNames, \
                           this Secret's name must be listed)";

/// Field selector for the Secret `name`.
pub fn secret_field_selector(name: &str) -> String {
    format!("metadata.name={name}")
}

/// Watcher config for the Secret `name`.
pub fn secret_watcher_config(name: &str) -> watcher::Config {
    watcher::Config::default()
        .fields(&secret_field_selector(name))
        .any_semantic()
}

struct Inner {
    client: Client,
    changes: broadcast::Sender<()>,
    watches: Mutex<BTreeMap<SecretKey, ResourceWatch<Secret>>>,
}

impl Inner {
    fn watches(&self) -> MutexGuard<'_, BTreeMap<SecretKey, ResourceWatch<Secret>>> {
        self.watches.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Read-only state of the watcher of one Secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretWatchState {
    pub key: SecretKey,
    pub synced: bool,
    pub up: bool,
}

/// Holds one watcher per referenced Secret. Cheap to clone; the watchers stop
/// when the last clone is dropped.
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
                watches: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// The currently watched Secrets.
    pub fn keys(&self) -> BTreeSet<SecretKey> {
        self.inner.watches().keys().cloned().collect()
    }

    /// Per watched Secret: listed once, and up.
    pub fn watch_states(&self) -> Vec<SecretWatchState> {
        self.inner
            .watches()
            .iter()
            .map(|(key, w)| SecretWatchState {
                key: key.clone(),
                synced: w.is_synced(),
                up: w.is_up(),
            })
            .collect()
    }

    /// Waits until [`is_synced`](SecretSource::is_synced) or `timeout`.
    /// Returns whether every current watcher has listed once.
    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        wait_stores(timeout, || {
            self.inner
                .watches()
                .values()
                .filter(|w| !w.is_synced())
                .map(|w| w.store().clone())
                .collect()
        })
        .await;
        SecretSource::is_synced(self)
    }
}

impl SecretSource for SecretRegistry {
    /// Sets the watched Secrets to exactly `keys` (the union over all handled
    /// Gateways). Must be called inside a Tokio runtime.
    fn set_secrets(&self, keys: BTreeSet<SecretKey>) {
        let mut watches = self.inner.watches();
        let mut changed = false;
        let removed: Vec<SecretKey> = watches
            .keys()
            .filter(|k| !keys.contains(*k))
            .cloned()
            .collect();
        for key in removed {
            if let Some(watch) = watches.remove(&key) {
                watch.stop();
                info!(secret = %key, "stopped watching Secret");
                changed = true;
            }
        }
        for key in keys {
            if watches.contains_key(&key) {
                continue;
            }
            info!(secret = %key, "watching Secret");
            let watch = ResourceWatch::start(
                Api::namespaced(self.inner.client.clone(), &key.namespace),
                secret_watcher_config(&key.name),
                WatchTarget {
                    what: format!("Secret {key}"),
                    scope: Scope::Namespace(key.namespace.clone()),
                    rbac: SECRET_RBAC,
                    crd: None,
                },
                strip_metadata,
                self.inner.changes.clone(),
            );
            watches.insert(key, watch);
            changed = true;
        }
        drop(watches);
        if changed {
            let _ = self.inner.changes.send(());
        }
    }

    /// The watched Secrets that exist. A Secret not listed yet is absent;
    /// check [`is_synced`](SecretSource::is_synced) before treating that as
    /// missing.
    fn secrets(&self) -> BTreeMap<SecretKey, Arc<Secret>> {
        self.inner
            .watches()
            .iter()
            .filter_map(|(key, w)| {
                w.store()
                    .state()
                    .into_iter()
                    .find(|s| s.metadata.name.as_deref() == Some(key.name.as_str()))
                    .map(|s| (key.clone(), s))
            })
            .collect()
    }

    /// True when every current watcher has listed once. True for an empty set.
    fn is_synced(&self) -> bool {
        self.inner.watches().values().all(ResourceWatch::is_synced)
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
    }
}

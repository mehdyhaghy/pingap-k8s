//! Types shared between modules. Changing anything here affects every module,
//! so keep it small and stable.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;

/// A Service port as written in the controller config: a port name or a
/// Service `port` number (never `targetPort`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PortRef {
    Name(String),
    Number(i32),
}

impl fmt::Display for PortRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PortRef::Name(name) => f.write_str(name),
            PortRef::Number(number) => write!(f, "{number}"),
        }
    }
}

/// Identifies one watched Service.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ServiceKey {
    pub namespace: String,
    pub name: String,
}

impl fmt::Display for ServiceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

/// One `services` entry of an upstream: `<service>[.<namespace>]:<port> [weight]`,
/// with the namespace already defaulted.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceRef {
    pub key: ServiceKey,
    pub port: PortRef,
    /// `None` writes plain `ip:port`; `Some(w)` writes `ip:port w` (w > 0).
    pub weight: Option<u32>,
}

/// Per-upstream options after `[defaults]` and per-upstream overrides are merged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpstreamOptions {
    pub include_not_ready: bool,
    pub drain_terminating: bool,
    pub ipv4_only: bool,
    pub debounce: Duration,
    pub debounce_max: Duration,
}

/// The current view of one Service, as held by the watchers.
#[derive(Clone, Debug, Default)]
pub struct ServiceSnapshot {
    /// `None` when the Service does not exist (or is not listed yet).
    pub service: Option<Arc<Service>>,
    pub slices: Vec<Arc<EndpointSlice>>,
}

/// Where reconcile reads Kubernetes state from. Implemented by
/// `watch::WatchRegistry`, and by in-memory fakes in tests.
pub trait SnapshotSource: Send + Sync + 'static {
    fn snapshot(&self, key: &ServiceKey) -> ServiceSnapshot;
}

/// Result of one write attempt to Pingap's config storage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    /// `addrs` already matched; nothing was written.
    Unchanged,
    /// The key existed and `addrs` was replaced.
    Updated { revision: i64 },
    /// The key did not exist; a minimal upstream was created .
    Created { revision: i64 },
}

/// Writes an upstream's `addrs` into Pingap's config storage.
/// `etcd::EtcdWriter` is the only production implementation.
pub trait AddrsWriter: Send + Sync + 'static {
    /// `addrs` is already sorted and non-empty (D12 is enforced by the caller).
    fn write_addrs(
        &self,
        upstream: &str,
        addrs: &[String],
    ) -> impl Future<Output = anyhow::Result<WriteOutcome>> + Send;
}

/// A change to an owned upstream key that the controller did not make.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForeignWrite {
    /// The key was written by someone else. `addrs` is what is stored now
    /// (`None` if the entry could not be parsed).
    Put {
        upstream: String,
        addrs: Option<Vec<String>>,
        revision: i64,
    },
    Deleted {
        upstream: String,
        revision: i64,
    },
    /// The watch lost history (compaction or reconnect gap): reconcile everything.
    Resync,
}

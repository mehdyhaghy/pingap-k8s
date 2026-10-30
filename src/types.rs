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

/// TLS from Pingap to the backends of an upstream (a Gateway API BackendTLSPolicy):
/// Pingap `sni`, `verify_cert = true`, `ca` and, for a backend that asks for a client
/// certificate, `client_cert` / `client_key`. Written only when the entry is created; the
/// generated name changes with it, so it is never updated in place.
#[derive(Clone, Default, PartialEq, Eq, Hash)]
pub struct UpstreamTls {
    /// SNI and the name the backend certificate must match.
    pub sni: String,
    /// PEM CA bundle; `None`: the system trust store.
    pub ca: Option<String>,
    /// PEM certificate chain and private key presented to the backend.
    pub client_cert: Option<(String, String)>,
}

/// Leaves the PEM text out (the private key must not reach logs).
impl fmt::Debug for UpstreamTls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UpstreamTls")
            .field("sni", &self.sni)
            .field("ca", &self.ca.as_ref().map(|_| "<PEM>"))
            .field("client_cert", &self.client_cert.as_ref().map(|_| "<PEM>"))
            .finish()
    }
}

/// Per-upstream options after `[defaults]` and per-upstream overrides are merged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpstreamOptions {
    pub include_not_ready: bool,
    pub drain_terminating: bool,
    /// Which address families of the endpoints are written.
    pub ip_family: IpFamily,
    /// `ExternalName` Services contribute `<externalName>:<port>` (Pingap DNS discovery).
    pub external_name: bool,
    pub debounce: Duration,
    pub debounce_max: Duration,
    pub target: UpstreamTarget,
    /// Each `services` weight is the weight of the whole Service, split over its
    /// addresses, and the weights are scaled to a small total (Gateway API `backendRefs`
    /// semantics). Off: the weight is written on every address as it is.
    pub spread_weights: bool,
}

/// Address families written for an upstream (`ip_family`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IpFamily {
    /// One address per endpoint: the Service's primary family (`spec.ipFamilies[0]`),
    /// the other family only for endpoints that have no address in the primary one.
    #[default]
    Primary,
    /// IPv4 addresses only.
    Ipv4,
    /// IPv6 addresses only.
    Ipv6,
    /// Every family: a pod of a dual-stack Service is written once per family.
    Both,
}

impl IpFamily {
    /// The config value: `primary`, `ipv4`, `ipv6` or `both`.
    pub fn as_str(self) -> &'static str {
        match self {
            IpFamily::Primary => "primary",
            IpFamily::Ipv4 => "ipv4",
            IpFamily::Ipv6 => "ipv6",
            IpFamily::Both => "both",
        }
    }

    /// Whether addresses of a family (`ipv6` true for IPv6) may be written at all.
    pub fn allows(self, ipv6: bool) -> bool {
        match self {
            IpFamily::Primary | IpFamily::Both => true,
            IpFamily::Ipv4 => !ipv6,
            IpFamily::Ipv6 => ipv6,
        }
    }
}

/// What an upstream's `addrs` point at.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum UpstreamTarget {
    /// Ready pod IPs from EndpointSlices.
    #[default]
    Pods,
    /// One `<ClusterIP>:<service port>` per Service; a headless Service gets
    /// `<service>.<namespace>.svc.<cluster_domain>`, `cluster_domain` from `[kubernetes]`.
    Service { cluster_domain: String },
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
    /// The key did not exist; a minimal upstream was created.
    Created { revision: i64 },
}

/// Writes an upstream's `addrs` into Pingap's config storage.
/// `etcd::EtcdWriter` is the only production implementation.
pub trait AddrsWriter: Send + Sync + 'static {
    /// `addrs` is already sorted and non-empty (an empty result never reaches the writer).
    fn write_addrs(
        &self,
        upstream: &str,
        addrs: &[String],
    ) -> impl Future<Output = anyhow::Result<WriteOutcome>> + Send;

    /// [`AddrsWriter::write_addrs`]; an entry it creates also gets `health_check`
    /// (already validated) and the TLS fields of `tls`. An existing entry's fields other
    /// than `addrs` are left alone.
    fn write_upstream(
        &self,
        upstream: &str,
        addrs: &[String],
        health_check: Option<&str>,
        tls: Option<&UpstreamTls>,
    ) -> impl Future<Output = anyhow::Result<WriteOutcome>> + Send {
        let _ = (health_check, tls);
        self.write_addrs(upstream, addrs)
    }
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

//! Types shared by the parts of the Gateway API provider. Data and traits only.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret, Service};
use tokio::sync::{broadcast, oneshot};

use crate::config::{GatewayGroup, UpstreamConfig};
use crate::gateway::api::backendtlspolicies::{BackendTlsPolicy, BackendTlsPolicyStatus};
use crate::gateway::api::gatewayclasses::{GatewayClass, GatewayClassStatus};
use crate::gateway::api::gateways::{Gateway, GatewayStatus};
use crate::gateway::api::httproutes::{HttpRoute, HttpRouteStatus};
use crate::gateway::api::listenersets::{ListenerSet, ListenerSetStatus};
use crate::gateway::api::referencegrants::ReferenceGrant;
use crate::types::{ServiceKey, UpstreamOptions, UpstreamTls};

/// Every entry the Gateway provider writes has a name starting with this; nothing else
/// is ever changed or deleted by it.
pub const OWNED_PREFIX: &str = "k8s-";
/// Start of the `remark` of generated entries: "pingap-k8s <kind> <ns>/<name>".
pub const REMARK_PREFIX: &str = "pingap-k8s";
/// The API group of every Gateway API resource.
pub const GATEWAY_GROUP: &str = "gateway.networking.k8s.io";

/// Whether an entry name belongs to the Gateway provider.
pub fn is_owned(name: &str) -> bool {
    name.starts_with(OWNED_PREFIX)
}

/// A namespaced object: a Gateway, a ListenerSet, an HTTPRoute or a Secret.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey {
    pub namespace: String,
    pub name: String,
}

impl ObjectKey {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

pub type GatewayKey = ObjectKey;
pub type ListenerSetKey = ObjectKey;
pub type RouteKey = ObjectKey;
pub type SecretKey = ObjectKey;
pub type ConfigMapKey = ObjectKey;
pub type PolicyKey = ObjectKey;

/// What a backend Service becomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendKind {
    /// Pod IPs, kept current by the endpoint writer.
    Pods,
    /// The Service address (`target = "service"`), kept current by the endpoint writer.
    Service,
    /// An `ExternalName` Service: Pingap DNS discovery on `spec.externalName`.
    External,
}

/// A Pingap server the generated locations can be listed in, read from etcd.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerInfo {
    /// The entry name (`[servers.<name>]`).
    pub name: String,
    /// Ports of its `addr` list (`"0.0.0.0:80,[::]:80"` gives `[80]`).
    pub ports: BTreeSet<u16>,
    /// `global_certificates = true`: a TLS listener serving the stored certificates by SNI.
    pub tls: bool,
}

#[derive(Clone, Debug)]
pub struct TranslateOptions {
    /// `spec.controllerName` of the GatewayClasses this controller handles.
    pub controller_name: String,
    /// Allow `ExternalName` backends.
    pub external_name: bool,
    /// Defaults of generated upstreams (target mode included).
    pub upstream_defaults: UpstreamOptions,
    /// Pingap `health_check` of generated upstreams (validated; `None`: Pingap's default
    /// TCP check).
    pub health_check: Option<String>,
    pub cluster_domain: String,
    /// The shared group: Pingap servers the provider may list locations of Gateways
    /// without a group of their own in; empty: every stored server not in a group.
    pub servers: Vec<String>,
    /// The addresses of the shared group, written to `status.addresses` of its
    /// accepted Gateways (IP addresses or host names).
    pub addresses: Vec<String>,
    /// Server groups given to one Gateway each.
    pub groups: Vec<GatewayGroup>,
}

/// A consistent snapshot to translate. Lists are in no particular order; the
/// translation sorts what needs an order.
pub struct TranslateInput<'a> {
    pub classes: &'a [Arc<GatewayClass>],
    pub gateways: &'a [Arc<Gateway>],
    pub routes: &'a [Arc<HttpRoute>],
    pub listener_sets: &'a [Arc<ListenerSet>],
    pub grants: &'a [Arc<ReferenceGrant>],
    /// Every namespace (for `allowedRoutes.namespaces.selector`).
    pub namespaces: &'a BTreeMap<String, Arc<Namespace>>,
    /// Backend Services; absent means missing or not listed yet.
    pub services: &'a BTreeMap<ServiceKey, Arc<Service>>,
    /// Listener certificate and backend client certificate Secrets; absent means missing
    /// or not listed yet.
    pub secrets: &'a BTreeMap<SecretKey, Arc<Secret>>,
    /// Every BackendTLSPolicy.
    pub policies: &'a [Arc<BackendTlsPolicy>],
    /// CA certificate ConfigMaps of the BackendTLSPolicies of backend Services; absent
    /// means missing or not listed yet.
    pub config_maps: &'a BTreeMap<ConfigMapKey, Arc<ConfigMap>>,
    /// The Pingap servers stored in etcd.
    pub servers: &'a [ServerInfo],
    /// Plugin names stored in Pingap plus its built-ins.
    pub plugins: &'a BTreeSet<String>,
    pub options: &'a TranslateOptions,
}

/// What the handled Gateways and routes refer to, so it can be watched.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Referenced {
    pub services: BTreeSet<ServiceKey>,
    pub secrets: BTreeSet<SecretKey>,
    pub config_maps: BTreeSet<ConfigMapKey>,
}

/// A generated Pingap location. One per (server, host, route match): the weight orders
/// it against every other location of the same server, so a location is never shared
/// between servers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocationEntry {
    pub owner: RouteKey,
    /// The Pingap server whose `locations` list it is in.
    pub server: String,
    /// One host (exact or `*.` wildcard); `None` matches every host.
    pub host: Option<String>,
    /// In Pingap syntax: empty (every path), `=exact` or `~regex`. Gateway API prefixes
    /// are segment-aware and become an anchored regex.
    pub path: String,
    /// `name:value` pairs, all of which must match (exact value).
    pub match_headers: Vec<String>,
    /// `name:value` pairs, all of which must match (exact value).
    pub match_query: Vec<String>,
    /// `None`: no upstream. Pingap answers 503 unless a plugin answers first (the mock
    /// plugin that returns 500 for invalid backends, a redirect).
    pub upstream: Option<String>,
    /// Explicit Pingap weight: higher is tried first. Encodes Gateway API precedence.
    pub weight: u16,
    /// Pingap `rewrite`: `"<regex> <replacement>"`.
    pub rewrite: Option<String>,
    /// Pingap `proxy_set_headers` (`name:value`), for the host rewrite.
    pub proxy_set_headers: Vec<String>,
    /// Names in run order; generated plugins are in [`Desired::plugins`].
    pub plugins: Vec<String>,
    /// Retries and timeouts of the rule.
    pub options: LocationOptions,
}

/// Pingap location settings from the `retry` and `timeouts` of a rule. `None` leaves
/// the field out: no retry, and the timeouts of the upstream. Durations are in
/// milliseconds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LocationOptions {
    /// Retries of a connection that could not be established, each to another backend.
    pub max_retries: Option<u8>,
    /// The time from the start of the request after which no retry is made.
    pub max_retry_window: Option<u64>,
    pub connection_timeout: Option<u64>,
    /// Pingap's `read_timeout` is per read: the wait for the response header and for
    /// each piece of the body.
    pub read_timeout: Option<u64>,
    /// Per write, as `read_timeout`.
    pub write_timeout: Option<u64>,
}

/// A generated Pingap plugin: `[plugins.<name>]` with `category` and its fields. Named
/// after its content, so routes that need the same plugin share it.
#[derive(Clone, Debug, PartialEq)]
pub struct PluginEntry {
    pub category: String,
    pub fields: toml::Table,
}

/// An upstream for an `ExternalName` backend, written whole by the Gateway writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalUpstream {
    pub service: ServiceKey,
    /// `host:port`.
    pub addr: String,
    /// Pingap `health_check`, validated.
    pub health_check: Option<String>,
    /// TLS to the backend (BackendTLSPolicy).
    pub tls: Option<UpstreamTls>,
}

/// A certificate from a `kubernetes.io/tls` Secret named by an HTTPS listener.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CertificateEntry {
    pub secret: SecretKey,
    /// Listener hostnames; empty (a listener without hostname): Pingap takes the names
    /// from the certificate and serves it as the default certificate.
    pub domains: Vec<String>,
    pub tls_cert: String,
    pub tls_key: String,
}

/// The desired status of every object the controller handles. Conditions carry the
/// generation they were computed for; `last_transition_time` is a placeholder that the
/// status writer replaces (it keeps the stored time while the condition's status is
/// unchanged).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DesiredStatus {
    /// GatewayClasses with our `controllerName`, by name.
    pub classes: BTreeMap<String, GatewayClassStatus>,
    /// Gateways of those classes.
    pub gateways: BTreeMap<GatewayKey, GatewayStatus>,
    /// ListenerSets whose parent is one of those Gateways.
    pub listener_sets: BTreeMap<ListenerSetKey, ListenerSetStatus>,
    /// Routes with at least one parent of ours: only the `parents` entries with our
    /// `controllerName` are set; entries of other controllers are left as they are.
    pub routes: BTreeMap<RouteKey, HttpRouteStatus>,
    /// BackendTLSPolicies that apply to backends of our routes, or that hold entries of
    /// ours: only the `ancestors` entries with our `controllerName` are set (none: ours
    /// are removed); entries of other controllers are left as they are.
    pub policies: BTreeMap<PolicyKey, BackendTlsPolicyStatus>,
}

/// Everything the handled Gateways and routes want stored.
#[derive(Clone, Debug, Default)]
pub struct Desired {
    pub locations: BTreeMap<String, LocationEntry>,
    /// Server name -> its generated location names, highest weight first.
    pub server_locations: BTreeMap<String, Vec<String>>,
    /// Pods or service target: handed to the endpoint writer, which creates and updates
    /// them.
    pub upstreams: BTreeMap<String, UpstreamConfig>,
    /// `ExternalName` backends: written whole by the Gateway writer.
    pub external: BTreeMap<String, ExternalUpstream>,
    pub certificates: BTreeMap<String, CertificateEntry>,
    pub plugins: BTreeMap<String, PluginEntry>,
    pub status: DesiredStatus,
    pub referenced: Referenced,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredEntry {
    pub key: String,
    pub value: String,
    pub mod_revision: i64,
}

/// The Pingap entries in etcd, read at one revision. Maps are keyed by entry name.
#[derive(Clone, Debug, Default)]
pub struct Stored {
    pub revision: i64,
    pub servers: BTreeMap<String, StoredEntry>,
    pub locations: BTreeMap<String, StoredEntry>,
    pub upstreams: BTreeMap<String, StoredEntry>,
    pub certificates: BTreeMap<String, StoredEntry>,
    pub plugins: BTreeMap<String, StoredEntry>,
}

/// The compare of a write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expect {
    Absent,
    ModRevision(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put {
        key: String,
        value: String,
        expect: Expect,
    },
    Delete {
        key: String,
        expect: Expect,
    },
    RequireExists {
        key: String,
    },
}

/// Ops that do not depend on each other, so a step may be split into several Txns. After
/// every step the stored config is valid for Pingap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub what: &'static str,
    pub ops: Vec<Op>,
}

/// A location that was written without its upstream (Pingap answers 503 until the
/// endpoint writer stores it), or not written at all, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Deferred {
    MissingUpstream(String),
    MissingPlugin(String),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Plan {
    pub steps: Vec<Step>,
    /// Location name -> why it is not complete yet.
    pub deferred: BTreeMap<String, Deferred>,
    /// Owned locations that are live once the plan is applied.
    pub programmed: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApplyOutcome {
    Unchanged,
    Applied {
        revision: i64,
        puts: usize,
        deletes: usize,
    },
}

/// Upstreams generated for route backends, sent to the endpoint writer. `applied` fires
/// once it has taken them over; only then may old generated upstreams be deleted.
#[derive(Debug)]
pub struct GeneratedUpstreams {
    pub upstreams: BTreeMap<String, UpstreamConfig>,
    pub services: BTreeSet<ServiceKey>,
    pub applied: oneshot::Sender<()>,
}

/// The Gateway API objects and namespaces, watched cluster-wide.
pub trait GatewaySource: Send + Sync + 'static {
    fn classes(&self) -> Vec<Arc<GatewayClass>>;
    fn gateways(&self) -> Vec<Arc<Gateway>>;
    fn routes(&self) -> Vec<Arc<HttpRoute>>;
    fn listener_sets(&self) -> Vec<Arc<ListenerSet>>;
    fn grants(&self) -> Vec<Arc<ReferenceGrant>>;
    fn backend_tls_policies(&self) -> Vec<Arc<BackendTlsPolicy>>;
    fn namespaces(&self) -> BTreeMap<String, Arc<Namespace>>;
    /// Every watch listed once.
    fn is_synced(&self) -> bool;
    fn changes(&self) -> broadcast::Receiver<()>;
}

/// The Secrets named by listener `certificateRefs` and Gateway backend client
/// certificates, and the ConfigMaps named by BackendTLSPolicy `caCertificateRefs`,
/// watched one by one.
pub trait SecretSource: Send + Sync + 'static {
    fn set_secrets(&self, keys: BTreeSet<SecretKey>);
    fn secrets(&self) -> BTreeMap<SecretKey, Arc<Secret>>;
    fn set_config_maps(&self, keys: BTreeSet<ConfigMapKey>);
    fn config_maps(&self) -> BTreeMap<ConfigMapKey, Arc<ConfigMap>>;
    /// Every Secret and ConfigMap watch listed once.
    fn is_synced(&self) -> bool;
    fn changes(&self) -> broadcast::Receiver<()>;
}

/// Reads and writes the Pingap entries in etcd.
pub trait PingapStore: Send + Sync + 'static {
    fn read_stored(&self) -> impl Future<Output = anyhow::Result<Stored>> + Send;
    fn apply(&self, plan: &Plan) -> impl Future<Output = anyhow::Result<ApplyOutcome>> + Send;
}

/// Writes the status subresources. Each call returns whether it changed anything.
pub trait StatusPublisher: Send + Sync + 'static {
    fn publish_class(
        &self,
        class: &GatewayClass,
        status: &GatewayClassStatus,
    ) -> impl Future<Output = anyhow::Result<bool>> + Send;
    fn publish_gateway(
        &self,
        gateway: &Gateway,
        status: &GatewayStatus,
    ) -> impl Future<Output = anyhow::Result<bool>> + Send;
    fn publish_listener_set(
        &self,
        listener_set: &ListenerSet,
        status: &ListenerSetStatus,
    ) -> impl Future<Output = anyhow::Result<bool>> + Send;
    /// `status.parents` entries of other controllers are kept.
    fn publish_route(
        &self,
        route: &HttpRoute,
        status: &HttpRouteStatus,
    ) -> impl Future<Output = anyhow::Result<bool>> + Send;
    /// `status.ancestors` entries of other controllers are kept.
    fn publish_backend_tls_policy(
        &self,
        policy: &BackendTlsPolicy,
        status: &BackendTlsPolicyStatus,
    ) -> impl Future<Output = anyhow::Result<bool>> + Send;
}

/// Shared handle: an `Arc` of a source is a source.
impl<T: GatewaySource> GatewaySource for Arc<T> {
    fn classes(&self) -> Vec<Arc<GatewayClass>> {
        (**self).classes()
    }
    fn gateways(&self) -> Vec<Arc<Gateway>> {
        (**self).gateways()
    }
    fn routes(&self) -> Vec<Arc<HttpRoute>> {
        (**self).routes()
    }
    fn listener_sets(&self) -> Vec<Arc<ListenerSet>> {
        (**self).listener_sets()
    }
    fn grants(&self) -> Vec<Arc<ReferenceGrant>> {
        (**self).grants()
    }
    fn backend_tls_policies(&self) -> Vec<Arc<BackendTlsPolicy>> {
        (**self).backend_tls_policies()
    }
    fn namespaces(&self) -> BTreeMap<String, Arc<Namespace>> {
        (**self).namespaces()
    }
    fn is_synced(&self) -> bool {
        (**self).is_synced()
    }
    fn changes(&self) -> broadcast::Receiver<()> {
        (**self).changes()
    }
}

//! Controller config file.
//!
//! The file is TOML. Unknown keys are errors. Durations are humantime strings
//! ("500ms", "5m"). `[defaults]` holds the per-upstream options and the global
//! `resync`; any per-upstream option can be overridden inside its
//! `[upstreams.<name>]` table.
//!
//! Name rules (Kubernetes `apimachinery/pkg/util/validation`):
//! - Service names are DNS-1035 labels: `[a-z]([-a-z0-9]*[a-z0-9])?`, at most 63 chars.
//! - Namespace names are DNS-1123 labels: `[a-z0-9]([-a-z0-9]*[a-z0-9])?`, at most 63
//!   chars. This is what the apiserver enforces for namespaces (a namespace may
//!   start with a digit); it is a superset of DNS-1035, so every name the spec
//!   allows is accepted.
//! - Service port names are DNS-1123 labels too (the apiserver's rule for
//!   `spec.ports[].name`). A port made only of digits is a port number (1-65535).
//! - `[kubernetes] cluster_domain` is a DNS-1123 subdomain: dot-separated
//!   DNS-1123 labels, at most 253 chars, no trailing dot.
//!
//! `[gateway]` (optional) turns on the Gateway API provider; see [`GatewaySettings`].

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::types::{
    IpFamily, PortRef, ServiceKey, ServiceRef, UpstreamOptions, UpstreamTarget, UpstreamTls,
};

/// Maximum length of a DNS label.
const DNS_LABEL_MAX: usize = 63;

/// Maximum length of a DNS-1123 subdomain.
const DNS_SUBDOMAIN_MAX: usize = 253;

/// Default `[kubernetes] cluster_domain`.
pub const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";

/// Default listen address of the metrics/health server: all IPv6 and, on
/// Linux, all IPv4 addresses too. Neither std nor tokio sets `IPV6_V6ONLY`,
/// so an `[::]` socket also accepts IPv4 (as v4-mapped addresses) unless the
/// node sets `net.ipv6.bindv6only=1`. Nodes booted with IPv6 disabled
/// (`ipv6.disable=1`) cannot open it: set `listen = "0.0.0.0:9090"` there.
pub const DEFAULT_LISTEN: &str = "[::]:9090";

/// Default `[gateway] controller_name`: GatewayClasses with this `spec.controllerName`
/// are handled.
pub const DEFAULT_CONTROLLER_NAME: &str = "pingap.io/gateway-controller";
/// Default `[gateway] max_txn_ops` (etcd's own default limit is 128).
pub const DEFAULT_MAX_TXN_OPS: usize = 64;
/// Bounds of `[gateway] max_txn_ops`: a step must fit a put and its compare.
const MAX_TXN_OPS_RANGE: std::ops::RangeInclusive<usize> = 2..=1024;
/// Upstream names starting with this belong to the Gateway API provider.
pub const GATEWAY_OWNED_PREFIX: &str = "k8s-";

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("cannot read config file {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid config TOML: {0}")]
    Toml(#[from] toml::de::Error),
    /// A `services` entry that does not parse (returned by [`parse_service_entry`]).
    #[error("invalid services entry {entry:?}: {reason}")]
    Entry { entry: String, reason: String },
    /// A `services` entry of a given upstream that does not parse.
    #[error("upstream {upstream:?}: invalid services entry {entry:?}: {reason}")]
    UpstreamEntry {
        upstream: String,
        entry: String,
        reason: String,
    },
    /// A problem with one upstream that is not about a single entry.
    #[error("upstream {upstream:?}: {reason}")]
    Upstream { upstream: String, reason: String },
    /// Any other invalid value.
    #[error("invalid config: {0}")]
    Invalid(String),
}

/// The validated controller config.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub etcd: EtcdSection,
    pub kubernetes: KubernetesSection,
    pub defaults: Defaults,
    pub upstreams: BTreeMap<String, UpstreamConfig>,
    pub server: ServerSection,
    /// `[gateway]` with `enabled = true`; `None` when the table is absent or disabled.
    pub gateway: Option<GatewaySettings>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EtcdSection {
    /// Pingap's etcd URL format; parsed by `etcd::parse_etcd_url`.
    pub url: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct KubernetesSection {
    /// Empty: in-cluster, then `$KUBECONFIG`, then `~/.kube/config`.
    pub kubeconfig: String,
    /// Empty: the current context.
    pub context: String,
    /// Empty: the controller pod's own namespace.
    pub default_namespace: String,
    /// Used for `target = "service"` addresses.
    pub cluster_domain: String,
    /// Post Kubernetes Events. Needs `create` and `patch` on
    /// `events.k8s.io` `events` in the Service namespaces and the pod's own.
    pub events: bool,
}

impl Default for KubernetesSection {
    fn default() -> Self {
        Self {
            kubeconfig: String::new(),
            context: String::new(),
            default_namespace: String::new(),
            cluster_domain: DEFAULT_CLUSTER_DOMAIN.to_string(),
            events: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerSection {
    /// Listen address for `/metrics`, `/healthz` and `/readyz`.
    pub listen: SocketAddr,
    /// Listen address of the read-only `/status` endpoint. Empty
    /// (the default) disables it. Must be a loopback address: it is reached
    /// with `kubectl port-forward` only, never through a Service.
    #[serde(deserialize_with = "deserialize_status_listen")]
    pub status_listen: Option<SocketAddr>,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN
                .parse()
                .expect("valid default listen address"),
            status_listen: None,
        }
    }
}

/// `""` is `None`; anything else must be an `ip:port` socket address.
fn deserialize_status_listen<'de, D>(d: D) -> Result<Option<SocketAddr>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    if s.trim().is_empty() {
        return Ok(None);
    }
    s.trim().parse().map(Some).map_err(serde::de::Error::custom)
}

/// `[gateway]`: Gateways of the GatewayClasses with `controller_name`, and the HTTPRoutes
/// attached to them, become Pingap locations, certificates, plugins and upstreams.
/// `enabled`, `controller_name` and `max_txn_ops` need a restart; everything else is
/// applied on reload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GatewaySettings {
    /// `spec.controllerName` of the GatewayClasses handled (`<domain>/<path>`).
    pub controller_name: String,
    /// Pingap servers the generated locations may be listed in; empty: every stored
    /// server whose port and TLS setting fit a listener.
    pub servers: Vec<String>,
    /// Allow `ExternalName` backends.
    pub external_name: bool,
    /// Written to `status.addresses` of accepted Gateways: the addresses clients reach
    /// Pingap on.
    pub addresses: Vec<String>,
    /// Operations per etcd transaction of the Gateway writer.
    pub max_txn_ops: usize,
    /// Pingap `health_check` of generated upstreams; `None`: Pingap's default TCP check.
    pub health_check: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, default)]
struct RawGateway {
    enabled: bool,
    controller_name: String,
    servers: Vec<String>,
    external_name: bool,
    addresses: Vec<String>,
    max_txn_ops: usize,
    health_check: String,
}

impl Default for RawGateway {
    fn default() -> Self {
        Self {
            enabled: false,
            controller_name: DEFAULT_CONTROLLER_NAME.to_string(),
            servers: Vec::new(),
            external_name: false,
            addresses: Vec::new(),
            max_txn_ops: DEFAULT_MAX_TXN_OPS,
            health_check: String::new(),
        }
    }
}

/// `[defaults]` after applying built-in defaults. `resync` is global; the
/// other fields are per-upstream defaults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Defaults {
    pub include_not_ready: bool,
    pub drain_terminating: bool,
    pub debounce: Duration,
    pub debounce_max: Duration,
    pub resync: Duration,
    pub ip_family: IpFamily,
    pub external_name: bool,
    pub target: UpstreamTarget,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            include_not_ready: false,
            drain_terminating: true,
            debounce: Duration::from_millis(500),
            debounce_max: Duration::from_secs(2),
            resync: Duration::from_secs(300),
            ip_family: IpFamily::Primary,
            external_name: false,
            target: UpstreamTarget::Pods,
        }
    }
}

impl Defaults {
    /// The per-upstream part of the defaults.
    pub fn upstream_options(&self) -> UpstreamOptions {
        UpstreamOptions {
            include_not_ready: self.include_not_ready,
            drain_terminating: self.drain_terminating,
            ip_family: self.ip_family,
            external_name: self.external_name,
            debounce: self.debounce,
            debounce_max: self.debounce_max,
            target: self.target.clone(),
            spread_weights: false,
        }
    }
}

/// One owned Pingap upstream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpstreamConfig {
    /// In config order (order matters for dedupe in `resolve`).
    pub services: Vec<ServiceRef>,
    pub options: UpstreamOptions,
    /// Pingap `health_check` written when the controller creates the entry (generated
    /// Gateway upstreams only; the name changes with it, so it is never updated in place).
    pub health_check: Option<String>,
    /// TLS to the backends, written when the controller creates the entry (generated
    /// Gateway upstreams only; like `health_check`, never updated in place).
    pub tls: Option<UpstreamTls>,
}

// ---- raw (serde) layer ----

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    etcd: EtcdSection,
    #[serde(default)]
    kubernetes: KubernetesSection,
    #[serde(default)]
    defaults: RawDefaults,
    #[serde(default)]
    upstreams: BTreeMap<String, RawUpstream>,
    #[serde(default)]
    server: ServerSection,
    gateway: Option<RawGateway>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDefaults {
    include_not_ready: Option<bool>,
    drain_terminating: Option<bool>,
    #[serde(default, with = "humantime_serde")]
    debounce: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    debounce_max: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    resync: Option<Duration>,
    ip_family: Option<String>,
    /// Deprecated alias: `true` is `ip_family = "ipv4"`, `false` is `"primary"`.
    ipv4_only: Option<bool>,
    external_name: Option<bool>,
    target: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawUpstream {
    services: Vec<String>,
    include_not_ready: Option<bool>,
    drain_terminating: Option<bool>,
    #[serde(default, with = "humantime_serde")]
    debounce: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    debounce_max: Option<Duration>,
    ip_family: Option<String>,
    /// Deprecated alias of `ip_family`, see [`RawDefaults`].
    ipv4_only: Option<bool>,
    external_name: Option<bool>,
    target: Option<String>,
}

impl Config {
    /// Parses and validates a config. `pod_namespace` is used when
    /// `kubernetes.default_namespace` is empty.
    pub fn from_toml_str(s: &str, pod_namespace: &str) -> Result<Config, ConfigError> {
        let raw: RawConfig = toml::from_str(s)?;
        if raw.defaults.ipv4_only.is_some() || raw.upstreams.values().any(|u| u.ipv4_only.is_some())
        {
            tracing::warn!(
                "ipv4_only is deprecated: use ip_family = \"ipv4\" (for true) or \"primary\" (for false)"
            );
        }

        if raw.etcd.url.trim().is_empty() {
            return Err(ConfigError::Invalid("etcd.url must not be empty".into()));
        }

        validate_cluster_domain(&raw.kubernetes.cluster_domain).map_err(|reason| {
            ConfigError::Invalid(format!("kubernetes.cluster_domain: {reason}"))
        })?;
        let cluster_domain = raw.kubernetes.cluster_domain.as_str();

        let built = Defaults::default();
        let target = match raw.defaults.target.as_deref() {
            Some(value) => parse_target(value, cluster_domain)
                .map_err(|reason| ConfigError::Invalid(format!("defaults.target: {reason}")))?,
            None => built.target.clone(),
        };
        let ip_family = merge_ip_family(raw.defaults.ip_family.as_deref(), raw.defaults.ipv4_only)
            .map_err(|reason| ConfigError::Invalid(format!("defaults.{reason}")))?
            .unwrap_or(built.ip_family);
        let defaults = Defaults {
            include_not_ready: raw
                .defaults
                .include_not_ready
                .unwrap_or(built.include_not_ready),
            drain_terminating: raw
                .defaults
                .drain_terminating
                .unwrap_or(built.drain_terminating),
            debounce: raw.defaults.debounce.unwrap_or(built.debounce),
            debounce_max: raw.defaults.debounce_max.unwrap_or(built.debounce_max),
            resync: raw.defaults.resync.unwrap_or(built.resync),
            ip_family,
            external_name: raw.defaults.external_name.unwrap_or(built.external_name),
            target,
        };
        if defaults.resync.is_zero() {
            return Err(ConfigError::Invalid(
                "defaults.resync must be greater than 0".into(),
            ));
        }
        check_debounce(&defaults.upstream_options())
            .map_err(|reason| ConfigError::Invalid(format!("defaults: {reason}")))?;

        let default_namespace = if raw.kubernetes.default_namespace.is_empty() {
            pod_namespace
        } else {
            raw.kubernetes.default_namespace.as_str()
        };
        if !raw.kubernetes.default_namespace.is_empty() {
            validate_namespace(&raw.kubernetes.default_namespace).map_err(|reason| {
                ConfigError::Invalid(format!("kubernetes.default_namespace: {reason}"))
            })?;
        }

        if let Some(addr) = raw.server.status_listen
            && !addr.ip().is_loopback()
        {
            return Err(ConfigError::Invalid(format!(
                "server.status_listen {addr} must be a loopback address (127.0.0.1 or [::1]); \
                 the status endpoint is reached with kubectl port-forward only"
            )));
        }

        let gateway = match raw.gateway {
            Some(raw_gateway) => build_gateway(raw_gateway)?,
            None => None,
        };

        let mut upstreams = BTreeMap::new();
        for (name, raw_up) in raw.upstreams {
            if gateway.is_some() && name.starts_with(GATEWAY_OWNED_PREFIX) {
                return Err(ConfigError::Upstream {
                    upstream: name,
                    reason: format!(
                        "names starting with {GATEWAY_OWNED_PREFIX:?} are reserved for the Gateway API provider"
                    ),
                });
            }
            let upstream =
                build_upstream(&name, raw_up, &defaults, default_namespace, cluster_domain)?;
            upstreams.insert(name, upstream);
        }

        Ok(Config {
            etcd: raw.etcd,
            kubernetes: raw.kubernetes,
            defaults,
            upstreams,
            server: raw.server,
            gateway,
        })
    }

    /// Reads and validates the config file at `path`.
    pub fn load(path: &Path, pod_namespace: &str) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Self::from_toml_str(&text, pod_namespace)
    }

    /// Every Service referenced by any upstream.
    pub fn service_keys(&self) -> BTreeSet<ServiceKey> {
        self.upstreams
            .values()
            .flat_map(|u| u.services.iter().map(|s| s.key.clone()))
            .collect()
    }

    /// Every namespace referenced by any upstream.
    pub fn namespaces(&self) -> BTreeSet<String> {
        self.upstreams
            .values()
            .flat_map(|u| u.services.iter().map(|s| s.key.namespace.clone()))
            .collect()
    }
}

fn build_upstream(
    name: &str,
    raw: RawUpstream,
    defaults: &Defaults,
    default_namespace: &str,
    cluster_domain: &str,
) -> Result<UpstreamConfig, ConfigError> {
    let upstream_err = |reason: String| ConfigError::Upstream {
        upstream: name.to_string(),
        reason,
    };

    if !is_valid_upstream_name(name) {
        return Err(upstream_err(
            "name must be non-empty and contain only A-Z, a-z, 0-9, '_' or '-'".into(),
        ));
    }
    if raw.services.is_empty() {
        return Err(upstream_err("services must not be empty".into()));
    }

    let target = match raw.target.as_deref() {
        Some(value) => {
            parse_target(value, cluster_domain).map_err(|r| upstream_err(format!("target: {r}")))?
        }
        None => defaults.target.clone(),
    };
    let ip_family = merge_ip_family(raw.ip_family.as_deref(), raw.ipv4_only)
        .map_err(upstream_err)?
        .unwrap_or(defaults.ip_family);
    let options = UpstreamOptions {
        include_not_ready: raw.include_not_ready.unwrap_or(defaults.include_not_ready),
        drain_terminating: raw.drain_terminating.unwrap_or(defaults.drain_terminating),
        ip_family,
        external_name: raw.external_name.unwrap_or(defaults.external_name),
        debounce: raw.debounce.unwrap_or(defaults.debounce),
        debounce_max: raw.debounce_max.unwrap_or(defaults.debounce_max),
        target,
        spread_weights: false,
    };
    check_debounce(&options).map_err(upstream_err)?;

    let mut services: Vec<ServiceRef> = Vec::with_capacity(raw.services.len());
    for entry in &raw.services {
        let service_ref = parse_service_entry(entry, default_namespace).map_err(|e| match e {
            ConfigError::Entry { entry, reason } => ConfigError::UpstreamEntry {
                upstream: name.to_string(),
                entry,
                reason,
            },
            other => other,
        })?;
        if services
            .iter()
            .any(|s| s.key == service_ref.key && s.port == service_ref.port)
        {
            return Err(ConfigError::UpstreamEntry {
                upstream: name.to_string(),
                entry: entry.clone(),
                reason: format!(
                    "Service {} port {} is listed more than once",
                    service_ref.key, service_ref.port
                ),
            });
        }
        services.push(service_ref);
    }

    Ok(UpstreamConfig {
        services,
        options,
        health_check: None,
        tls: None,
    })
}

/// Validates `[gateway]`. Every field is checked even when it is disabled, so a typo
/// shows before it is turned on. `None` when disabled.
fn build_gateway(raw: RawGateway) -> Result<Option<GatewaySettings>, ConfigError> {
    let fail = |reason: String| ConfigError::Invalid(format!("gateway.{reason}"));

    validate_controller_name(&raw.controller_name)
        .map_err(|r| fail(format!("controller_name: {:?}: {r}", raw.controller_name)))?;
    let mut servers: Vec<String> = Vec::with_capacity(raw.servers.len());
    for server in &raw.servers {
        if !is_valid_upstream_name(server) {
            return Err(fail(format!(
                "servers: {server:?}: a Pingap server name must be non-empty and contain only A-Z, a-z, 0-9, '_' or '-'"
            )));
        }
        if servers.contains(server) {
            return Err(fail(format!(
                "servers: {server:?} is listed more than once"
            )));
        }
        servers.push(server.clone());
    }
    for address in &raw.addresses {
        if address.parse::<std::net::IpAddr>().is_err() {
            validate_cluster_domain(address).map_err(|r| {
                fail(format!(
                    "addresses: {address:?} is neither an IP address nor a host name: {r}"
                ))
            })?;
        }
    }
    if !MAX_TXN_OPS_RANGE.contains(&raw.max_txn_ops) {
        return Err(fail(format!(
            "max_txn_ops must be in {}-{}",
            MAX_TXN_OPS_RANGE.start(),
            MAX_TXN_OPS_RANGE.end()
        )));
    }
    let health_check = raw.health_check.trim();
    if !health_check.is_empty() {
        crate::health_check::validate(health_check)
            .map_err(|r| fail(format!("health_check: {health_check:?}: {r}")))?;
    }
    let health_check = (!health_check.is_empty()).then(|| health_check.to_string());
    if !raw.enabled {
        return Ok(None);
    }
    Ok(Some(GatewaySettings {
        controller_name: raw.controller_name,
        servers,
        external_name: raw.external_name,
        addresses: raw.addresses,
        max_txn_ops: raw.max_txn_ops,
        health_check,
    }))
}

/// A GatewayClass `controllerName`: `<domain>/<path>`, at most 253 characters, the
/// domain a DNS-1123 subdomain and the path made of URL path characters (the CRD's own
/// validation).
fn validate_controller_name(name: &str) -> Result<(), String> {
    if name.len() > 253 {
        return Err("longer than 253 characters".into());
    }
    let (domain, path) = name
        .split_once('/')
        .ok_or_else(|| "must be <domain>/<path>".to_string())?;
    validate_cluster_domain(domain)?;
    let path_char = |c: char| c.is_ascii_alphanumeric() || "/-._~%!$&'()*+,;=:".contains(c);
    if path.is_empty() || !path.chars().all(path_char) {
        return Err("the path after the domain must be non-empty URL path characters".into());
    }
    Ok(())
}

/// `target = "pods" | "service"`.
/// `ip_family` and its deprecated alias `ipv4_only` of one table; `None` when neither is
/// set. Setting both is an error.
fn merge_ip_family(
    ip_family: Option<&str>,
    ipv4_only: Option<bool>,
) -> Result<Option<IpFamily>, String> {
    match (ip_family, ipv4_only) {
        (Some(_), Some(_)) => {
            Err("ipv4_only: set either ip_family or the deprecated ipv4_only, not both".into())
        }
        (Some(value), None) => parse_ip_family(value)
            .map(Some)
            .map_err(|r| format!("ip_family: {r}")),
        (None, Some(true)) => Ok(Some(IpFamily::Ipv4)),
        (None, Some(false)) => Ok(Some(IpFamily::Primary)),
        (None, None) => Ok(None),
    }
}

fn parse_ip_family(value: &str) -> Result<IpFamily, String> {
    [
        IpFamily::Primary,
        IpFamily::Ipv4,
        IpFamily::Ipv6,
        IpFamily::Both,
    ]
    .into_iter()
    .find(|f| f.as_str() == value)
    .ok_or_else(|| {
        format!("unknown value {value:?}; expected \"primary\", \"ipv4\", \"ipv6\" or \"both\"")
    })
}

fn parse_target(value: &str, cluster_domain: &str) -> Result<UpstreamTarget, String> {
    match value {
        "pods" => Ok(UpstreamTarget::Pods),
        "service" => Ok(UpstreamTarget::Service {
            cluster_domain: cluster_domain.to_string(),
        }),
        other => Err(format!(
            "unknown value {other:?}; expected \"pods\" or \"service\""
        )),
    }
}

/// A DNS-1123 subdomain without a trailing dot, e.g. `cluster.local`.
fn validate_cluster_domain(s: &str) -> Result<(), String> {
    if s.is_empty() {
        return Err("must not be empty".into());
    }
    if s.len() > DNS_SUBDOMAIN_MAX {
        return Err(format!(
            "must be a DNS name of at most {DNS_SUBDOMAIN_MAX} characters"
        ));
    }
    for label in s.split('.') {
        check_dns_label(label, false)
            .map_err(|r| format!("{s:?} is not a DNS name (label {label:?}: {r})"))?;
    }
    Ok(())
}

fn check_debounce(opts: &UpstreamOptions) -> Result<(), String> {
    if opts.debounce > opts.debounce_max {
        return Err(format!(
            "debounce ({}) must not be greater than debounce_max ({})",
            humantime::format_duration(opts.debounce),
            humantime::format_duration(opts.debounce_max)
        ));
    }
    Ok(())
}

/// Parses one `services` entry: `<service>[.<namespace>]:<port> [weight]`.
///
/// `default_namespace` is used when the entry has no namespace; it is
/// validated only in that case.
pub fn parse_service_entry(
    entry: &str,
    default_namespace: &str,
) -> Result<ServiceRef, ConfigError> {
    let fail = |reason: String| ConfigError::Entry {
        entry: entry.to_string(),
        reason,
    };

    let mut tokens = entry.split_whitespace();
    let target = tokens.next().ok_or_else(|| fail("entry is empty".into()))?;
    let weight_token = tokens.next();
    if let Some(extra) = tokens.next() {
        return Err(fail(format!(
            "unexpected extra token {extra:?}; expected `<service>[.<namespace>]:<port> [weight]`"
        )));
    }

    let (name_part, port_part) = target
        .rsplit_once(':')
        .ok_or_else(|| fail("missing port; expected `<service>[.<namespace>]:<port>`".into()))?;
    if port_part.is_empty() {
        return Err(fail("port is empty".into()));
    }

    let (service, namespace) = match name_part.split_once('.') {
        Some((svc, ns)) => {
            if ns.contains('.') {
                return Err(fail(format!(
                    "{name_part:?} has more than one '.'; expected `<service>.<namespace>`"
                )));
            }
            validate_namespace(ns).map_err(|r| fail(format!("namespace {ns:?}: {r}")))?;
            (svc, ns)
        }
        None => {
            if default_namespace.is_empty() {
                return Err(fail(
                    "no namespace given and no default namespace is known".into(),
                ));
            }
            validate_namespace(default_namespace)
                .map_err(|r| fail(format!("default namespace {default_namespace:?}: {r}")))?;
            (name_part, default_namespace)
        }
    };
    validate_service_name(service).map_err(|r| fail(format!("service {service:?}: {r}")))?;

    let port = parse_port(port_part).map_err(|r| fail(format!("port {port_part:?}: {r}")))?;

    let weight = match weight_token {
        None => None,
        Some(w) => {
            let value: u32 = w
                .parse()
                .map_err(|_| fail(format!("weight {w:?} must be a positive integer")))?;
            if value == 0 {
                return Err(fail("weight must be greater than 0".into()));
            }
            Some(value)
        }
    };

    Ok(ServiceRef {
        key: ServiceKey {
            namespace: namespace.to_string(),
            name: service.to_string(),
        },
        port,
        weight,
    })
}

fn parse_port(s: &str) -> Result<PortRef, String> {
    if s.bytes().all(|b| b.is_ascii_digit()) {
        let n: u32 = s
            .parse()
            .map_err(|_| "port number must be in 1-65535".to_string())?;
        if !(1..=65535).contains(&n) {
            return Err("port number must be in 1-65535".into());
        }
        return Ok(PortRef::Number(n as i32));
    }
    check_dns_label(s, false)
        .map_err(|r| format!("not a port number, and not a valid port name: {r}"))?;
    Ok(PortRef::Name(s.to_string()))
}

fn validate_service_name(s: &str) -> Result<(), String> {
    check_dns_label(s, true)
}

fn validate_namespace(s: &str) -> Result<(), String> {
    check_dns_label(s, false)
}

/// DNS-1035 (`first_must_be_letter`) or DNS-1123 label.
fn check_dns_label(s: &str, first_must_be_letter: bool) -> Result<(), String> {
    let kind = if first_must_be_letter {
        "DNS-1035"
    } else {
        "DNS-1123"
    };
    if s.is_empty() {
        return Err("must not be empty".into());
    }
    if s.len() > DNS_LABEL_MAX {
        return Err(format!(
            "must be a {kind} label of at most {DNS_LABEL_MAX} characters"
        ));
    }
    let bytes = s.as_bytes();
    let ok_chars = bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-');
    let first = bytes[0];
    let last = bytes[bytes.len() - 1];
    let ok_first = if first_must_be_letter {
        first.is_ascii_lowercase()
    } else {
        first.is_ascii_lowercase() || first.is_ascii_digit()
    };
    let ok_last = last.is_ascii_lowercase() || last.is_ascii_digit();
    if !(ok_chars && ok_first && ok_last) {
        let start = if first_must_be_letter {
            "start with a letter"
        } else {
            "start with a letter or digit"
        };
        return Err(format!(
            "must be a {kind} label: lowercase letters, digits and '-', {start}, end with a letter or digit"
        ));
    }
    Ok(())
}

/// Pingap upstream names: `[A-Za-z0-9_-]+`.
fn is_valid_upstream_name(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    const ETCD: &str = "[etcd]\nurl = \"etcd://127.0.0.1:2379/pingap\"\n";

    fn cfg(rest: &str) -> Result<Config, ConfigError> {
        Config::from_toml_str(&format!("{ETCD}{rest}"), "podns")
    }

    fn err(rest: &str) -> String {
        cfg(rest)
            .expect_err("config should be rejected")
            .to_string()
    }

    fn entry(s: &str) -> Result<ServiceRef, ConfigError> {
        parse_service_entry(s, "def")
    }

    fn sref(ns: &str, name: &str, port: PortRef, weight: Option<u32>) -> ServiceRef {
        ServiceRef {
            key: ServiceKey {
                namespace: ns.into(),
                name: name.into(),
            },
            port,
            weight,
        }
    }

    // ---- services entries ----

    #[test]
    fn entry_named_port_with_namespace() {
        assert_eq!(
            entry("api.prod:http").unwrap(),
            sref("prod", "api", PortRef::Name("http".into()), None)
        );
    }

    #[test]
    fn entry_numeric_port_default_namespace() {
        assert_eq!(
            entry("api:8080").unwrap(),
            sref("def", "api", PortRef::Number(8080), None)
        );
    }

    #[test]
    fn entry_with_weight_and_extra_whitespace() {
        assert_eq!(
            entry("  web-blue.prod:http \t 9 ").unwrap(),
            sref("prod", "web-blue", PortRef::Name("http".into()), Some(9))
        );
    }

    #[test]
    fn entry_port_bounds() {
        assert_eq!(entry("a:1").unwrap().port, PortRef::Number(1));
        assert_eq!(entry("a:65535").unwrap().port, PortRef::Number(65535));
        assert!(entry("a:0").is_err());
        assert!(entry("a:65536").is_err());
        assert!(entry("a:99999999999999999999").is_err());
    }

    #[test]
    fn entry_namespace_may_start_with_digit() {
        assert_eq!(entry("api.1team:80").unwrap().key.namespace, "1team");
    }

    #[test]
    fn entry_port_name_may_start_with_digit() {
        assert_eq!(
            entry("api:8080-tcp").unwrap().port,
            PortRef::Name("8080-tcp".into())
        );
    }

    #[test]
    fn entry_max_weight() {
        assert_eq!(entry("a:80 4294967295").unwrap().weight, Some(u32::MAX));
    }

    #[test]
    fn entry_rejections() {
        let cases = [
            ("", "empty"),
            ("   ", "empty"),
            ("api", "missing port"),
            ("api:", "port is empty"),
            ("api.prod", "missing port"),
            (":80", "service"),
            (".prod:80", "service"),
            ("api.:80", "namespace"),
            ("api.prod.x:80", "more than one"),
            ("Api:80", "DNS-1035"),
            ("1api:80", "DNS-1035"),
            ("api-:80", "DNS-1035"),
            ("-api:80", "DNS-1035"),
            ("a_b:80", "DNS-1035"),
            ("api.Prod:80", "DNS-1123"),
            ("api.-prod:80", "DNS-1123"),
            ("api:HTTP", "port name"),
            ("api:-http", "port name"),
            ("api:http_x", "port name"),
            ("api:80 0", "greater than 0"),
            ("api:80 -1", "positive integer"),
            ("api:80 1.5", "positive integer"),
            ("api:80 x", "positive integer"),
            ("api:80 4294967296", "positive integer"),
            ("api:80 1 2", "extra token"),
            ("api : 80", "extra token"),
        ];
        for (input, needle) in cases {
            let msg = entry(input)
                .expect_err(&format!("{input:?} should be rejected"))
                .to_string();
            assert!(
                msg.contains(needle),
                "{input:?}: error {msg:?} should contain {needle:?}"
            );
            assert!(
                msg.contains(&format!("{input:?}")),
                "{msg:?} names the entry"
            );
        }
    }

    #[test]
    fn entry_label_length() {
        let ok = "a".repeat(63);
        let long = "a".repeat(64);
        assert!(entry(&format!("{ok}.{ok}:80")).is_ok());
        assert!(entry(&format!("{long}:80")).is_err());
        assert!(entry(&format!("api.{long}:80")).is_err());
        assert!(entry(&format!("api:{long}")).is_err());
    }

    #[test]
    fn entry_without_any_namespace() {
        let msg = parse_service_entry("api:80", "").unwrap_err().to_string();
        assert!(msg.contains("no default namespace"), "{msg}");
        // An explicit namespace still works without a default.
        assert!(parse_service_entry("api.prod:80", "").is_ok());
    }

    #[test]
    fn entry_invalid_default_namespace() {
        assert!(parse_service_entry("api:80", "Bad_NS").is_err());
        assert!(parse_service_entry("api.ok:80", "Bad_NS").is_ok());
    }

    // ---- whole config ----

    const CONFIG_EXAMPLE: &str = r#"
[etcd]
url = "etcd://etcd.edge.svc:2379/pingap?ca=/etc/pingap-k8s/etcd/ca.pem"

[kubernetes]
kubeconfig = ""
context = ""
default_namespace = ""

[defaults]
include_not_ready = false
drain_terminating = true
debounce = "500ms"
debounce_max = "2s"
resync = "5m"
ip_family = "primary"
external_name = false

[upstreams.api]
services = ["api.prod:http"]

[upstreams.web]
services = ["web-blue.prod:http 9", "web-green.prod:http 1"]
drain_terminating = true
"#;

    #[test]
    fn config_example_parses() {
        let c = Config::from_toml_str(CONFIG_EXAMPLE, "edge").unwrap();
        assert_eq!(
            c.etcd.url,
            "etcd://etcd.edge.svc:2379/pingap?ca=/etc/pingap-k8s/etcd/ca.pem"
        );
        assert_eq!(c.defaults, Defaults::default());
        assert_eq!(c.server.listen, "[::]:9090".parse().unwrap());
        assert_eq!(c.upstreams.len(), 2);
        let web = &c.upstreams["web"];
        assert_eq!(
            web.services,
            vec![
                sref("prod", "web-blue", PortRef::Name("http".into()), Some(9)),
                sref("prod", "web-green", PortRef::Name("http".into()), Some(1)),
            ]
        );
        assert_eq!(web.options, Defaults::default().upstream_options());
        assert_eq!(c.namespaces(), BTreeSet::from(["prod".to_string()]));
        assert_eq!(c.service_keys().len(), 3);
    }

    #[test]
    fn minimal_config_uses_builtin_defaults() {
        let c = cfg("[upstreams.api]\nservices = [\"api:80\"]\n").unwrap();
        assert_eq!(c.kubernetes, KubernetesSection::default());
        assert_eq!(c.defaults.debounce, Duration::from_millis(500));
        assert_eq!(c.defaults.debounce_max, Duration::from_secs(2));
        assert_eq!(c.defaults.resync, Duration::from_secs(300));
        assert!(c.defaults.drain_terminating);
        assert!(!c.defaults.include_not_ready);
        assert_eq!(c.defaults.ip_family, IpFamily::Primary);
        assert!(!c.defaults.external_name);
        // Pod namespace is the fallback.
        assert_eq!(c.upstreams["api"].services[0].key.namespace, "podns");
    }

    #[test]
    fn no_upstreams_is_allowed() {
        let c = cfg("").unwrap();
        assert!(c.upstreams.is_empty());
        assert!(c.service_keys().is_empty());
    }

    #[test]
    fn default_namespace_overrides_pod_namespace() {
        let c = cfg(
            "[kubernetes]\ndefault_namespace = \"apps\"\n[upstreams.api]\nservices = [\"api:80\", \"db.data:5432\"]\n",
        )
        .unwrap();
        let s = &c.upstreams["api"].services;
        assert_eq!(s[0].key.namespace, "apps");
        assert_eq!(s[1].key.namespace, "data");
        assert_eq!(
            c.namespaces(),
            BTreeSet::from(["apps".to_string(), "data".to_string()])
        );
    }

    #[test]
    fn invalid_default_namespace_rejected() {
        let m = err("[kubernetes]\ndefault_namespace = \"Apps\"\n");
        assert!(m.contains("kubernetes.default_namespace"), "{m}");
    }

    #[test]
    fn missing_namespace_everywhere_rejected() {
        let toml = format!("{ETCD}[upstreams.api]\nservices = [\"api:80\"]\n");
        let m = Config::from_toml_str(&toml, "").unwrap_err().to_string();
        assert!(m.contains("upstream \"api\""), "{m}");
        assert!(m.contains("no default namespace"), "{m}");
    }

    #[test]
    fn overrides_merge_with_defaults() {
        let c = cfg(r#"
[defaults]
include_not_ready = true
debounce = "100ms"
debounce_max = "1s"
resync = "30s"

[upstreams.a]
services = ["a:80"]

[upstreams.b]
services = ["b:80"]
include_not_ready = false
drain_terminating = false
ip_family = "ipv4"
external_name = true
debounce = "1s"
debounce_max = "1m"
"#)
        .unwrap();
        assert_eq!(c.defaults.resync, Duration::from_secs(30));
        assert_eq!(
            c.upstreams["a"].options,
            UpstreamOptions {
                include_not_ready: true,
                drain_terminating: true,
                ip_family: IpFamily::Primary,
                external_name: false,
                debounce: Duration::from_millis(100),
                debounce_max: Duration::from_secs(1),
                target: UpstreamTarget::Pods,
                spread_weights: false,
            }
        );
        assert_eq!(
            c.upstreams["b"].options,
            UpstreamOptions {
                include_not_ready: false,
                drain_terminating: false,
                ip_family: IpFamily::Ipv4,
                external_name: true,
                debounce: Duration::from_secs(1),
                debounce_max: Duration::from_secs(60),
                target: UpstreamTarget::Pods,
                spread_weights: false,
            }
        );
    }

    #[test]
    fn target_defaults_to_pods() {
        let c = cfg("[upstreams.a]\nservices = [\"a:80\"]\n").unwrap();
        assert_eq!(c.defaults.target, UpstreamTarget::Pods);
        assert_eq!(c.upstreams["a"].options.target, UpstreamTarget::Pods);
        assert_eq!(c.kubernetes.cluster_domain, "cluster.local");
    }

    #[test]
    fn target_from_defaults_and_per_upstream() {
        let svc = |d: &str| UpstreamTarget::Service {
            cluster_domain: d.into(),
        };
        let c = cfg(r#"
[defaults]
target = "service"

[upstreams.a]
services = ["a:80"]

[upstreams.b]
services = ["b:80"]
target = "pods"
"#)
        .unwrap();
        assert_eq!(c.defaults.target, svc("cluster.local"));
        assert_eq!(c.upstreams["a"].options.target, svc("cluster.local"));
        assert_eq!(c.upstreams["b"].options.target, UpstreamTarget::Pods);

        let c = cfg(r#"
[kubernetes]
cluster_domain = "k8s.example.internal"

[upstreams.a]
services = ["a:80"]
target = "service"
"#)
        .unwrap();
        assert_eq!(c.defaults.target, UpstreamTarget::Pods);
        assert_eq!(c.upstreams["a"].options.target, svc("k8s.example.internal"));
    }

    #[test]
    fn unknown_target_rejected() {
        let m = err("[defaults]\ntarget = \"Service\"\n");
        assert!(
            m.contains("defaults.target") && m.contains("\"Service\"") && m.contains("\"pods\""),
            "{m}"
        );
        let m = err("[upstreams.a]\nservices = [\"a:80\"]\ntarget = \"endpoints\"\n");
        assert!(
            m.contains("upstream \"a\"") && m.contains("target") && m.contains("\"endpoints\""),
            "{m}"
        );
        assert!(cfg("[defaults]\ntarget = 1\n").is_err());
    }

    #[test]
    fn cluster_domain_validation() {
        for ok in ["cluster.local", "local", "k8s.example-1.internal", "1a.b"] {
            let c = cfg(&format!("[kubernetes]\ncluster_domain = \"{ok}\"\n")).unwrap();
            assert_eq!(c.kubernetes.cluster_domain, ok);
        }
        let long = format!("{}.local", vec!["a".repeat(63); 4].join("."));
        for bad in [
            "",
            "cluster.local.",
            ".cluster.local",
            "cluster..local",
            "Cluster.local",
            "cluster_local",
            "-a.local",
            "a b",
            long.as_str(),
        ] {
            let m = err(&format!("[kubernetes]\ncluster_domain = \"{bad}\"\n"));
            assert!(m.contains("kubernetes.cluster_domain"), "{bad:?}: {m}");
        }
        // Checked even when no upstream uses `service`.
        assert!(cfg("[kubernetes]\ncluster_domain = \"x.\"\n").is_err());
    }

    #[test]
    fn server_listen() {
        let c = cfg("[server]\nlisten = \"127.0.0.1:8081\"\n").unwrap();
        assert_eq!(c.server.listen, "127.0.0.1:8081".parse().unwrap());
        let c = cfg("[server]\nlisten = \"[::]:9090\"\n").unwrap();
        assert_eq!(c.server.listen, "[::]:9090".parse().unwrap());
        assert!(cfg("[server]\nlisten = \"localhost:9090\"\n").is_err());
        assert!(cfg("[server]\nport = 1\n").is_err());
    }

    #[test]
    fn status_listen_off_by_default_and_loopback_only() {
        assert_eq!(cfg("").unwrap().server.status_listen, None);
        assert_eq!(
            cfg("[server]\nstatus_listen = \"\"\n")
                .unwrap()
                .server
                .status_listen,
            None
        );
        for ok in ["127.0.0.1:9091", "[::1]:9091", "127.0.0.2:1"] {
            let c = cfg(&format!("[server]\nstatus_listen = \"{ok}\"\n")).unwrap();
            assert_eq!(c.server.status_listen, Some(ok.parse().unwrap()));
        }
        for bad in [
            "0.0.0.0:9091",
            "[::]:9091",
            "10.0.0.5:9091",
            "[fd00::1]:9091",
        ] {
            let m = err(&format!("[server]\nstatus_listen = \"{bad}\"\n"));
            assert!(m.contains("must be a loopback address"), "{bad}: {m}");
        }
        assert!(cfg("[server]\nstatus_listen = \"localhost:9091\"\n").is_err());
        assert!(cfg("[server]\nstatus_listen = \"9091\"\n").is_err());
    }

    #[test]
    fn duplicate_upstream_table_rejected() {
        let m =
            err("[upstreams.api]\nservices = [\"a:80\"]\n[upstreams.api]\nservices = [\"b:80\"]\n");
        assert!(m.contains("invalid config TOML"), "{m}");
    }

    #[test]
    fn duplicate_service_port_in_upstream_rejected() {
        let m = err("[upstreams.api]\nservices = [\"a.prod:80\", \"a.prod:80 5\"]\n");
        assert!(m.contains("upstream \"api\""), "{m}");
        assert!(m.contains("\"a.prod:80 5\""), "{m}");
        assert!(m.contains("more than once"), "{m}");
        // Defaulted namespace equals the explicit one.
        let m = err("[upstreams.api]\nservices = [\"a:80\", \"a.podns:80\"]\n");
        assert!(m.contains("more than once"), "{m}");
    }

    #[test]
    fn same_service_different_ports_or_upstreams_allowed() {
        let c = cfg(
            "[upstreams.x]\nservices = [\"a:80\", \"a:http\", \"a.other:80\"]\n[upstreams.y]\nservices = [\"a:80\"]\n",
        )
        .unwrap();
        assert_eq!(c.upstreams["x"].services.len(), 3);
        assert_eq!(c.service_keys().len(), 2);
    }

    #[test]
    fn bad_entry_names_upstream_and_entry() {
        let m = err("[upstreams.web]\nservices = [\"ok:80\", \"Bad:80\"]\n");
        assert!(m.contains("upstream \"web\""), "{m}");
        assert!(m.contains("\"Bad:80\""), "{m}");
    }

    #[test]
    fn empty_services_rejected() {
        let m = err("[upstreams.web]\nservices = []\n");
        assert!(
            m.contains("upstream \"web\"") && m.contains("must not be empty"),
            "{m}"
        );
        assert!(cfg("[upstreams.web]\n").is_err());
    }

    #[test]
    fn upstream_names() {
        assert!(cfg("[upstreams.Api_v2-x]\nservices = [\"a:80\"]\n").is_ok());
        for bad in ["\"a.b\"", "\"a b\"", "\"\"", "\"a/b\""] {
            let m = err(&format!("[upstreams.{bad}]\nservices = [\"a:80\"]\n"));
            assert!(m.contains("name must be"), "{bad}: {m}");
        }
    }

    #[test]
    fn debounce_greater_than_max_rejected() {
        let m = err("[defaults]\ndebounce = \"3s\"\n");
        assert!(m.contains("defaults") && m.contains("debounce_max"), "{m}");
        let m = err("[upstreams.a]\nservices = [\"a:80\"]\ndebounce_max = \"100ms\"\n");
        assert!(
            m.contains("upstream \"a\"") && m.contains("debounce_max"),
            "{m}"
        );
        assert!(cfg("[defaults]\ndebounce = \"2s\"\n").is_ok());
        assert!(cfg("[defaults]\ndebounce = \"0s\"\ndebounce_max = \"0s\"\n").is_ok());
    }

    #[test]
    fn resync_rules() {
        assert!(err("[defaults]\nresync = \"0s\"\n").contains("resync"));
        // resync is global: not allowed per upstream.
        assert!(cfg("[upstreams.a]\nservices = [\"a:80\"]\nresync = \"1m\"\n").is_err());
    }

    #[test]
    fn bad_durations_rejected() {
        assert!(cfg("[defaults]\ndebounce = \"fast\"\n").is_err());
        assert!(cfg("[defaults]\ndebounce = 500\n").is_err());
    }

    #[test]
    fn unknown_keys_rejected() {
        for rest in [
            "[defaults]\nfoo = 1\n",
            "[kubernetes]\nnamespace = \"x\"\n",
            "[upstreams.a]\nservices = [\"a:80\"]\nweight = 1\n",
            "[other]\n",
        ] {
            assert!(cfg(rest).is_err(), "{rest}");
        }
        assert!(Config::from_toml_str("[etcd]\nurl = \"x\"\nprefix = \"/p\"\n", "ns").is_err());
    }

    #[test]
    fn etcd_section_required_and_non_empty() {
        assert!(Config::from_toml_str("", "ns").is_err());
        assert!(Config::from_toml_str("[etcd]\n", "ns").is_err());
        let m = Config::from_toml_str("[etcd]\nurl = \" \"\n", "ns")
            .unwrap_err()
            .to_string();
        assert!(m.contains("etcd.url"), "{m}");
    }

    #[test]
    fn wrong_types_rejected() {
        assert!(cfg("[upstreams.a]\nservices = \"a:80\"\n").is_err());
        assert!(cfg("[defaults]\nipv4_only = \"yes\"\n").is_err());
        assert!(cfg("[defaults]\nexternal_name = \"yes\"\n").is_err());
    }

    #[test]
    fn ip_family_values_and_deprecated_alias() {
        let family = |defaults: &str, upstream: &str| {
            cfg(&format!(
                "[defaults]\n{defaults}\n[upstreams.a]\nservices = [\"a:80\"]\n{upstream}\n"
            ))
            .map(|c| (c.defaults.ip_family, c.upstreams["a"].options.ip_family))
        };
        let cases = [
            ("", "", IpFamily::Primary, IpFamily::Primary),
            ("ip_family = \"both\"", "", IpFamily::Both, IpFamily::Both),
            ("ip_family = \"ipv6\"", "", IpFamily::Ipv6, IpFamily::Ipv6),
            (
                "",
                "ip_family = \"ipv4\"",
                IpFamily::Primary,
                IpFamily::Ipv4,
            ),
            // The deprecated alias: true is ipv4, false is the default (primary).
            ("ipv4_only = true", "", IpFamily::Ipv4, IpFamily::Ipv4),
            (
                "ipv4_only = true",
                "ipv4_only = false",
                IpFamily::Ipv4,
                IpFamily::Primary,
            ),
            (
                "ipv4_only = true",
                "ip_family = \"both\"",
                IpFamily::Ipv4,
                IpFamily::Both,
            ),
        ];
        for (defaults, upstream, want_defaults, want_upstream) in cases {
            assert_eq!(
                family(defaults, upstream).unwrap(),
                (want_defaults, want_upstream),
                "{defaults:?} / {upstream:?}"
            );
        }
        let m = err("[defaults]\nip_family = \"dual\"\n");
        assert!(m.contains("defaults.ip_family"), "{m}");
        let m = err("[defaults]\nip_family = \"ipv4\"\nipv4_only = true\n");
        assert!(m.contains("not both"), "{m}");
        let m = family("", "ip_family = \"ipv4\"\nipv4_only = true")
            .unwrap_err()
            .to_string();
        assert!(
            m.contains("upstream \"a\"") && m.contains("not both"),
            "{m}"
        );
    }

    #[test]
    fn events_off_by_default() {
        assert!(
            !cfg("[upstreams.a]\nservices = [\"a:80\"]\n")
                .unwrap()
                .kubernetes
                .events
        );
        let c = cfg("[kubernetes]\nevents = true\n[upstreams.a]\nservices = [\"a:80\"]\n").unwrap();
        assert!(c.kubernetes.events);
        assert!(cfg("[kubernetes]\nevents = \"yes\"\n").is_err());
    }

    // ---- [gateway] ----

    #[test]
    fn gateway_absent_or_disabled_is_none() {
        assert_eq!(cfg("").unwrap().gateway, None);
        assert_eq!(cfg("[gateway]\n").unwrap().gateway, None);
        // Disabled: `k8s-` upstream names stay allowed.
        let c =
            cfg("[gateway]\nenabled = false\n[upstreams.k8s-a]\nservices = [\"a:80\"]\n").unwrap();
        assert!(c.upstreams.contains_key("k8s-a"));
    }

    #[test]
    fn gateway_defaults() {
        let c = cfg("[gateway]\nenabled = true\n").unwrap();
        assert_eq!(
            c.gateway,
            Some(GatewaySettings {
                controller_name: "pingap.io/gateway-controller".into(),
                servers: vec![],
                external_name: false,
                addresses: vec![],
                max_txn_ops: 64,
                health_check: None,
            })
        );
    }

    #[test]
    fn gateway_all_fields() {
        let c = cfg(r#"
[gateway]
enabled = true
controller_name = "edge.example.com/pingap"
servers = ["web_1", "tls"]
external_name = true
addresses = ["203.0.113.7", "2001:db8::1", "lb.example.com"]
max_txn_ops = 128
health_check = "http://k8s/healthz?connection_timeout=1s&check_frequency=5s"
"#)
        .unwrap();
        let g = c.gateway.unwrap();
        assert_eq!(g.controller_name, "edge.example.com/pingap");
        assert!(g.external_name);
        assert_eq!(g.servers, vec!["web_1".to_string(), "tls".to_string()]);
        assert_eq!(g.addresses.len(), 3);
        assert_eq!(g.max_txn_ops, 128);
        assert_eq!(
            g.health_check.as_deref(),
            Some("http://k8s/healthz?connection_timeout=1s&check_frequency=5s")
        );
    }

    #[test]
    fn gateway_rejections() {
        let base = "[gateway]\nenabled = true\n";
        let cases = [
            ("controller_name = \"\"\n", "gateway.controller_name"),
            ("controller_name = \"pingap\"\n", "gateway.controller_name"),
            (
                "controller_name = \"Pingap.io/x\"\n",
                "gateway.controller_name",
            ),
            (
                "controller_name = \"pingap.io/\"\n",
                "gateway.controller_name",
            ),
            (
                "controller_name = \"pingap.io/a b\"\n",
                "gateway.controller_name",
            ),
            ("servers = [\"a/b\"]\n", "gateway.servers"),
            ("servers = [\"\"]\n", "gateway.servers"),
            ("servers = [\"h\", \"h\"]\n", "more than once"),
            (
                "health_check = \"http://k8s/?check_frequency=5\"\n",
                "gateway.health_check",
            ),
            ("health_check = \"udp://k8s\"\n", "gateway.health_check"),
            ("addresses = [\"bad host\"]\n", "gateway.addresses"),
            ("max_txn_ops = 1\n", "max_txn_ops"),
            ("max_txn_ops = 5000\n", "max_txn_ops"),
        ];
        for (rest, needle) in cases {
            let m = err(&format!("{base}{rest}"));
            assert!(m.contains(needle), "{rest:?}: {m}");
        }
        // Checked even when disabled.
        assert!(err("[gateway]\ncontroller_name = \"x\"\n").contains("gateway.controller_name"));
        // Unknown keys and wrong types.
        assert!(cfg("[gateway]\nenabled = true\nfoo = 1\n").is_err());
        assert!(cfg("[gateway]\nenabled = \"yes\"\n").is_err());
        assert!(cfg("[gateway]\nservers = \"a\"\n").is_err());
    }

    #[test]
    fn k8s_upstream_names_reserved_when_gateway_enabled() {
        let m = err("[gateway]\nenabled = true\n[upstreams.k8s-web]\nservices = [\"a:80\"]\n");
        assert!(
            m.contains("upstream \"k8s-web\"") && m.contains("reserved"),
            "{m}"
        );
        assert!(
            cfg("[gateway]\nenabled = true\n[upstreams.k8sweb]\nservices = [\"a:80\"]\n").is_ok()
        );
    }

    /// A directory in the system temp dir, removed on drop (also when the
    /// test panics).
    struct TempDir(PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn load_reads_file_and_reports_missing() {
        let tmp =
            TempDir(std::env::temp_dir().join(format!("pingap-k8s-config-{}", std::process::id())));
        let dir = tmp.0.clone();
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            format!("{ETCD}[upstreams.a]\nservices = [\"a:80\"]\n"),
        )
        .unwrap();
        let c = Config::load(&path, "ns").unwrap();
        assert_eq!(c.upstreams["a"].services[0].key.namespace, "ns");
        std::fs::remove_dir_all(&dir).unwrap();

        let m = Config::load(&dir.join("missing.toml"), "ns")
            .unwrap_err()
            .to_string();
        assert!(m.contains("missing.toml"), "{m}");
    }
}

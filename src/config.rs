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

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::types::{PortRef, ServiceKey, ServiceRef, UpstreamOptions};

/// Maximum length of a DNS label.
const DNS_LABEL_MAX: usize = 63;

/// Default listen address of the metrics/health server: all IPv6 and, on
/// Linux, all IPv4 addresses too. Neither std nor tokio sets `IPV6_V6ONLY`,
/// so an `[::]` socket also accepts IPv4 (as v4-mapped addresses) unless the
/// node sets `net.ipv6.bindv6only=1`. Nodes booted with IPv6 disabled
/// (`ipv6.disable=1`) cannot open it: set `listen = "0.0.0.0:9090"` there.
pub const DEFAULT_LISTEN: &str = "[::]:9090";

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
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EtcdSection {
    /// Pingap's etcd URL format; parsed by `etcd::parse_etcd_url`.
    pub url: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct KubernetesSection {
    /// Empty: in-cluster, then `$KUBECONFIG`, then `~/.kube/config`.
    pub kubeconfig: String,
    /// Empty: the current context.
    pub context: String,
    /// Empty: the controller pod's own namespace.
    pub default_namespace: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ServerSection {
    /// Listen address for `/metrics`, `/healthz` and `/readyz`.
    pub listen: SocketAddr,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            listen: DEFAULT_LISTEN
                .parse()
                .expect("valid default listen address"),
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
    pub ipv4_only: bool,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            include_not_ready: false,
            drain_terminating: true,
            debounce: Duration::from_millis(500),
            debounce_max: Duration::from_secs(2),
            resync: Duration::from_secs(300),
            ipv4_only: false,
        }
    }
}

impl Defaults {
    /// The per-upstream part of the defaults.
    pub fn upstream_options(&self) -> UpstreamOptions {
        UpstreamOptions {
            include_not_ready: self.include_not_ready,
            drain_terminating: self.drain_terminating,
            ipv4_only: self.ipv4_only,
            debounce: self.debounce,
            debounce_max: self.debounce_max,
        }
    }
}

/// One owned Pingap upstream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UpstreamConfig {
    /// In config order (order matters for dedupe in `resolve`).
    pub services: Vec<ServiceRef>,
    pub options: UpstreamOptions,
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
    ipv4_only: Option<bool>,
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
    ipv4_only: Option<bool>,
}

impl Config {
    /// Parses and validates a config. `pod_namespace` is used when
    /// `kubernetes.default_namespace` is empty.
    pub fn from_toml_str(s: &str, pod_namespace: &str) -> Result<Config, ConfigError> {
        let raw: RawConfig = toml::from_str(s)?;

        if raw.etcd.url.trim().is_empty() {
            return Err(ConfigError::Invalid("etcd.url must not be empty".into()));
        }

        let built = Defaults::default();
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
            ipv4_only: raw.defaults.ipv4_only.unwrap_or(built.ipv4_only),
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

        let mut upstreams = BTreeMap::new();
        for (name, raw_up) in raw.upstreams {
            let upstream = build_upstream(&name, raw_up, &defaults, default_namespace)?;
            upstreams.insert(name, upstream);
        }

        Ok(Config {
            etcd: raw.etcd,
            kubernetes: raw.kubernetes,
            defaults,
            upstreams,
            server: raw.server,
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

    let options = UpstreamOptions {
        include_not_ready: raw.include_not_ready.unwrap_or(defaults.include_not_ready),
        drain_terminating: raw.drain_terminating.unwrap_or(defaults.drain_terminating),
        ipv4_only: raw.ipv4_only.unwrap_or(defaults.ipv4_only),
        debounce: raw.debounce.unwrap_or(defaults.debounce),
        debounce_max: raw.debounce_max.unwrap_or(defaults.debounce_max),
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

    Ok(UpstreamConfig { services, options })
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
ipv4_only = false

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
        assert!(!c.defaults.ipv4_only);
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
ipv4_only = true
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
                ipv4_only: false,
                debounce: Duration::from_millis(100),
                debounce_max: Duration::from_secs(1),
            }
        );
        assert_eq!(
            c.upstreams["b"].options,
            UpstreamOptions {
                include_not_ready: false,
                drain_terminating: false,
                ipv4_only: true,
                debounce: Duration::from_secs(1),
                debounce_max: Duration::from_secs(60),
            }
        );
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
    }

    #[test]
    fn load_reads_file_and_reports_missing() {
        let dir = std::env::temp_dir().join(format!("pingap-k8s-config-{}", std::process::id()));
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

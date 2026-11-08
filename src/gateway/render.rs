//! TOML of the entries the Gateway API provider writes, and the reads the planner and
//! the translation need from stored entries.
//!
//! Generated entries (`render_*`) are whole documents holding one `[<category>.<name>]`
//! table, with the field names of Pingap 0.16 (`LocationConf`, `CertificateConf`,
//! `UpstreamConf`, `PluginConf`). Stored entries are compared by value ([`same_entry`]), so a
//! reformatted entry is not rewritten. Servers are never generated: only their
//! `locations` list is edited ([`replace_owned_locations`]), with `toml_edit` so comments
//! and the hand-made part of the list stay as they are.

use std::collections::BTreeSet;
use std::net::IpAddr;

use toml_edit::{Array, DocumentMut, Item, Table, TableLike, Value};

use crate::gateway::model::{
    CertificateEntry, ExternalUpstream, LocationEntry, PluginEntry, REMARK_PREFIX, RouteKey,
    RouteKind, SecretKey, ServerInfo, is_owned,
};
use crate::types::ServiceKey;

#[derive(Debug, thiserror::Error)]
pub enum RenderError {
    #[error("TOML could not be parsed: {0}")]
    Toml(String),
    #[error("no [{category}.{name}] table")]
    MissingTable {
        category: &'static str,
        name: String,
    },
    #[error("servers.{0}.locations is not a list of strings")]
    InvalidLocations(String),
    #[error("upstream {name}: address {addr:?} {reason}")]
    InvalidAddress {
        name: String,
        addr: String,
        reason: String,
    },
}

/// `remark` of the locations of an HTTPRoute: "pingap-k8s httproute <ns>/<name>".
pub fn route_remark(key: &RouteKey) -> String {
    format!("{REMARK_PREFIX} httproute {key}")
}

/// `remark` of the locations of a route of any kind: [`route_remark`] for an HTTPRoute,
/// "pingap-k8s grpcroute <ns>/<name>" for a GRPCRoute.
pub fn kind_route_remark(kind: RouteKind, key: &RouteKey) -> String {
    match kind {
        RouteKind::Http => route_remark(key),
        RouteKind::Grpc => format!("{REMARK_PREFIX} grpcroute {key}"),
    }
}

/// Pingap `alpn` of an upstream that speaks HTTP/2 with prior knowledge: without `sni`
/// (no TLS) Pingap connects in h2c.
pub const H2C_ALPN: &str = "H2";

/// `remark` of the generated plugins: "pingap-k8s plugin" (shared by every route that
/// needs the same plugin, so it names no owner).
pub fn plugin_remark() -> String {
    format!("{REMARK_PREFIX} plugin")
}

/// `remark` of the certificate of a Secret: "pingap-k8s secret <ns>/<name>".
pub fn secret_remark(key: &SecretKey) -> String {
    format!("{REMARK_PREFIX} secret {key}")
}

/// `remark` of the upstream of an `ExternalName` Service: "pingap-k8s service <ns>/<name>".
pub fn service_remark(key: &ServiceKey) -> String {
    format!("{REMARK_PREFIX} service {key}")
}

/// A document holding only `[<category>.<name>]` = `entry`.
fn document(category: &str, name: &str, entry: Table) -> String {
    let mut outer = Table::new();
    outer.set_implicit(true);
    outer.insert(name, Item::Table(entry));
    let mut doc = DocumentMut::new();
    doc.insert(category, Item::Table(outer));
    doc.to_string()
}

fn strings<'a>(values: impl IntoIterator<Item = &'a String>) -> Array {
    values.into_iter().map(String::as_str).collect()
}

/// `[locations.<name>]`. An empty `path`, an absent host and an absent upstream are left
/// out: Pingap then matches every path and host, and answers 503 unless a plugin answers.
pub fn render_location(name: &str, location: &LocationEntry) -> String {
    let mut entry = Table::new();
    if let Some(host) = &location.host {
        entry.insert("host", toml_edit::value(host.as_str()));
    }
    if !location.path.is_empty() {
        entry.insert("path", toml_edit::value(location.path.as_str()));
    }
    if !location.match_headers.is_empty() {
        entry.insert(
            "match_headers",
            toml_edit::value(strings(&location.match_headers)),
        );
    }
    if !location.match_query.is_empty() {
        entry.insert(
            "match_query",
            toml_edit::value(strings(&location.match_query)),
        );
    }
    if let Some(upstream) = &location.upstream {
        entry.insert("upstream", toml_edit::value(upstream.as_str()));
    }
    entry.insert("weight", toml_edit::value(i64::from(location.weight)));
    if let Some(rewrite) = &location.rewrite {
        entry.insert("rewrite", toml_edit::value(rewrite.as_str()));
    }
    if !location.proxy_set_headers.is_empty() {
        entry.insert(
            "proxy_set_headers",
            toml_edit::value(strings(&location.proxy_set_headers)),
        );
    }
    if !location.plugins.is_empty() {
        entry.insert("plugins", toml_edit::value(strings(&location.plugins)));
    }
    let options = &location.options;
    if let Some(retries) = options.max_retries {
        entry.insert("max_retries", toml_edit::value(i64::from(retries)));
    }
    for (key, millis) in [
        ("max_retry_window", options.max_retry_window),
        ("connection_timeout", options.connection_timeout),
        ("read_timeout", options.read_timeout),
        ("write_timeout", options.write_timeout),
    ] {
        if let Some(millis) = millis {
            entry.insert(key, toml_edit::value(format!("{millis}ms")));
        }
    }
    entry.insert(
        "remark",
        toml_edit::value(kind_route_remark(location.owner_kind, &location.owner).as_str()),
    );
    document("locations", name, entry)
}

/// `[plugins.<name>]`: `category`, the plugin's fields and the generated remark.
pub fn render_plugin(name: &str, plugin: &PluginEntry) -> String {
    let mut fields = plugin.fields.clone();
    fields.insert(
        "category".to_string(),
        toml::Value::String(plugin.category.clone()),
    );
    fields.insert("remark".to_string(), toml::Value::String(plugin_remark()));
    let mut outer = toml::Table::new();
    outer.insert(name.to_string(), toml::Value::Table(fields));
    let mut doc = toml::Table::new();
    doc.insert("plugins".to_string(), toml::Value::Table(outer));
    toml::to_string(&doc).expect("a TOML table always serializes")
}

/// What the translation needs to know of a stored server: the ports of its `addr` list
/// (`"0.0.0.0:80,[::]:80"`), whether it serves TLS (`global_certificates`) and HTTP/2
/// (`enabled_h2`). `None` when the entry does not parse or has no usable address.
pub fn server_info(toml: &str, name: &str) -> Option<ServerInfo> {
    let table = entry_table(toml, "servers", name).ok()??;
    let addr = table.get("addr")?.as_str()?;
    let ports: BTreeSet<u16> = addr
        .split(',')
        .filter_map(|a| a.trim().rsplit_once(':'))
        .filter_map(|(_, port)| port.parse::<u16>().ok())
        .filter(|port| *port != 0)
        .collect();
    if ports.is_empty() {
        return None;
    }
    let flag = |key: &str| {
        table
            .get(key)
            .and_then(toml::Value::as_bool)
            .unwrap_or(false)
    };
    Some(ServerInfo {
        name: name.to_string(),
        ports,
        tls: flag("global_certificates"),
        h2: flag("enabled_h2"),
    })
}

/// The `remark` of a stored `[certificates.<name>]`; `None` when it does not parse or
/// has none.
pub fn certificate_remark(toml: &str, name: &str) -> Option<String> {
    entry_string(toml, "certificates", name, "remark")
        .ok()
        .flatten()
}

/// `[certificates.<name>]`; `domains` is a comma-separated string, as Pingap reads it,
/// and always written: without it Pingap would serve the certificate for every name in
/// it, also those that belong to a listener with a hostname (the entry whose name sorts
/// first wins a name two entries claim). An empty string serves it for no name. The
/// default certificate (listeners without hostname) also has `is_default = true`, so it
/// is served for any other or no SNI.
pub fn render_certificate(name: &str, certificate: &CertificateEntry) -> String {
    let mut entry = Table::new();
    entry.insert(
        "domains",
        toml_edit::value(certificate.domains.join(",").as_str()),
    );
    if certificate.is_default {
        entry.insert("is_default", toml_edit::value(true));
    }
    entry.insert("tls_cert", toml_edit::value(certificate.tls_cert.as_str()));
    entry.insert("tls_key", toml_edit::value(certificate.tls_key.as_str()));
    entry.insert(
        "remark",
        toml_edit::value(secret_remark(&certificate.secret).as_str()),
    );
    document("certificates", name, entry)
}

/// `[upstreams.<name>]` for an `ExternalName` backend: `addrs = ["host:port"]` with
/// `discovery = "dns"` and `update_frequency`, and `alpn = "H2"` for an HTTP/2 backend. An
/// IP literal is written as a static address, and only when it passes the same filter as
/// the pod addresses of phase 1 (no loopback, link-local, broadcast, multicast or
/// unspecified address). With TLS (a BackendTLSPolicy): `sni`, `verify_cert = true`,
/// `ca`, `client_cert` and `client_key`.
pub fn render_external_upstream(
    name: &str,
    upstream: &ExternalUpstream,
    update_frequency: &str,
) -> Result<String, RenderError> {
    let addr = &upstream.addr;
    let invalid = |reason: &str| RenderError::InvalidAddress {
        name: name.to_string(),
        addr: addr.clone(),
        reason: reason.to_string(),
    };
    let (host, port) = match addr.strip_prefix('[') {
        Some(rest) => {
            let (host, port) = rest
                .split_once("]:")
                .ok_or_else(|| invalid("is not [ipv6]:port"))?;
            let ip: IpAddr = host.parse().map_err(|_| invalid("is not [ipv6]:port"))?;
            (Some(ip), port)
        }
        None => {
            let (host, port) = addr
                .rsplit_once(':')
                .ok_or_else(|| invalid("is not host:port"))?;
            if host.is_empty() || host.contains(':') || host.contains(char::is_whitespace) {
                return Err(invalid("is not host:port"));
            }
            (host.parse::<IpAddr>().ok(), port)
        }
    };
    if !matches!(port.parse::<u16>(), Ok(p) if p != 0) {
        return Err(invalid("has no valid port"));
    }

    let mut entry = Table::new();
    entry.insert("addrs", toml_edit::value(strings([addr])));
    match host {
        Some(ip) if !is_allowed_ip(&ip) => {
            return Err(invalid(
                "is a loopback, link-local, multicast or unspecified address",
            ));
        }
        Some(_) => {}
        None => {
            entry.insert("discovery", toml_edit::value("dns"));
            entry.insert("update_frequency", toml_edit::value(update_frequency));
        }
    }
    if let Some(health_check) = &upstream.health_check {
        entry.insert("health_check", toml_edit::value(health_check.as_str()));
    }
    if upstream.h2c {
        entry.insert("alpn", toml_edit::value(H2C_ALPN));
    }
    if let Some(tls) = &upstream.tls {
        entry.insert("sni", toml_edit::value(tls.sni.as_str()));
        entry.insert("verify_cert", toml_edit::value(true));
        if let Some(ca) = &tls.ca {
            entry.insert("ca", toml_edit::value(ca.as_str()));
        }
        if let Some((cert, key)) = &tls.client_cert {
            entry.insert("client_cert", toml_edit::value(cert.as_str()));
            entry.insert("client_key", toml_edit::value(key.as_str()));
        }
    }
    entry.insert(
        "remark",
        toml_edit::value(service_remark(&upstream.service).as_str()),
    );
    Ok(document("upstreams", name, entry))
}

/// The address filter of phase 1 (`resolve.rs`): Kubernetes rejects these for pod
/// addresses, and an upstream pointing at one reaches the node itself or cloud metadata.
fn is_allowed_ip(ip: &IpAddr) -> bool {
    let ip = ip.to_canonical();
    let special = match ip {
        IpAddr::V4(v4) => v4.is_link_local() || v4.is_broadcast(),
        IpAddr::V6(v6) => v6.is_unicast_link_local(),
    };
    !(special || ip.is_loopback() || ip.is_unspecified() || ip.is_multicast())
}

/// Whether a stored entry holds the same values as a rendered one (formatting, comments
/// and key order ignored). A stored entry that does not parse is never the same.
pub fn same_entry(stored: &str, rendered: &str) -> bool {
    match (
        toml::from_str::<toml::Table>(stored),
        toml::from_str::<toml::Table>(rendered),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// The `[<category>.<name>]` table of a stored entry; `Ok(None)` when it is absent.
fn entry_table(toml: &str, category: &str, name: &str) -> Result<Option<toml::Table>, RenderError> {
    let mut doc: toml::Table =
        toml::from_str(toml).map_err(|e| RenderError::Toml(e.to_string()))?;
    let Some(toml::Value::Table(mut outer)) = doc.remove(category) else {
        return Ok(None);
    };
    match outer.remove(name) {
        Some(toml::Value::Table(table)) => Ok(Some(table)),
        _ => Ok(None),
    }
}

fn entry_string(
    toml: &str,
    category: &str,
    name: &str,
    field: &str,
) -> Result<Option<String>, RenderError> {
    Ok(entry_table(toml, category, name)?
        .and_then(|t| t.get(field).and_then(|v| v.as_str()).map(str::to_string)))
}

/// `servers.<server>.locations` in order; empty when the key is absent. A missing server
/// table is an error.
pub fn server_locations(toml: &str, server: &str) -> Result<Vec<String>, RenderError> {
    let table = entry_table(toml, "servers", server)?.ok_or_else(|| RenderError::MissingTable {
        category: "servers",
        name: server.to_string(),
    })?;
    match table.get("locations") {
        None => Ok(Vec::new()),
        Some(toml::Value::Array(values)) => values
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| RenderError::InvalidLocations(server.to_string()))
            })
            .collect(),
        Some(_) => Err(RenderError::InvalidLocations(server.to_string())),
    }
}

/// Sets the owned part of `servers.<server>.locations`: hand-made names stay where they
/// are, owned names (`k8s-`) are dropped, and `desired_owned` is appended in its order.
/// Returns the new document, or `None` when the list does not change. Every other byte
/// of the document is kept, including comments and a one-name-per-line layout. An
/// absent `locations` key is added when there is something to list.
pub fn replace_owned_locations(
    toml: &str,
    server: &str,
    desired_owned: &[String],
) -> Result<Option<String>, RenderError> {
    let current = server_locations(toml, server)?;
    let mut wanted: Vec<String> = current.iter().filter(|n| !is_owned(n)).cloned().collect();
    let kept = wanted.len();
    for name in desired_owned {
        if is_owned(name) && !wanted[kept..].contains(name) {
            wanted.push(name.clone());
        }
    }
    if wanted == current {
        return Ok(None);
    }

    let mut doc = toml
        .parse::<DocumentMut>()
        .map_err(|e| RenderError::Toml(e.to_string()))?;
    let table: &mut dyn TableLike = doc
        .get_mut("servers")
        .and_then(Item::as_table_like_mut)
        .and_then(|t| t.get_mut(server))
        .and_then(Item::as_table_like_mut)
        .ok_or_else(|| RenderError::MissingTable {
            category: "servers",
            name: server.to_string(),
        })?;
    let added = &wanted[kept..];
    match table.get_mut("locations") {
        None => {
            table.insert("locations", toml_edit::value(strings(added)));
        }
        Some(item) => {
            let array = item
                .as_array_mut()
                .ok_or_else(|| RenderError::InvalidLocations(server.to_string()))?;
            replace_owned_in_array(array, added);
        }
    }
    Ok(Some(doc.to_string()))
}

/// Drops the owned names of `array` and appends `added`, keeping the hand-made elements
/// with their decor and whether the array is laid out one element per line. A comment
/// stored in front of a dropped element (TOML keeps `"a", # note` as part of the next
/// element) moves to the element that follows it.
fn replace_owned_in_array(array: &mut Array, added: &[String]) {
    let prefix_of = |v: &Value| {
        v.decor()
            .prefix()
            .and_then(|p| p.as_str())
            .unwrap_or("")
            .to_string()
    };
    let line_prefix = array
        .iter()
        .map(prefix_of)
        .find(|p| p.contains('\n'))
        .map(|p| format!("\n{}", p.rsplit('\n').next().unwrap_or("")));
    let Some(line_prefix) = line_prefix else {
        // A single-line array holds no comments: normal formatting loses nothing.
        array.retain(|v| v.as_str().is_none_or(|name| !is_owned(name)));
        for name in added {
            array.push(name.as_str());
        }
        array.fmt();
        return;
    };

    // Comment lines of dropped elements, waiting for the next element.
    let mut pending = String::new();
    let mut kept: Vec<Value> = Vec::new();
    for value in array.iter() {
        let prefix = prefix_of(value);
        if value.as_str().is_some_and(is_owned) {
            if prefix.contains('#') {
                pending.push_str(prefix.rsplit_once('\n').map_or("", |(c, _)| c));
            }
            continue;
        }
        let mut value = value.clone();
        if !pending.is_empty() {
            value
                .decor_mut()
                .set_prefix(format!("{}{prefix}", std::mem::take(&mut pending)));
        }
        kept.push(value);
    }
    for name in added {
        let mut value = Value::from(name.as_str());
        value
            .decor_mut()
            .set_prefix(format!("{}{line_prefix}", std::mem::take(&mut pending)));
        kept.push(value);
    }
    let trailing = array.trailing().as_str().unwrap_or("").to_string();
    array.clear();
    for value in kept {
        array.push_formatted(value);
    }
    let trailing = format!("{pending}{trailing}");
    array.set_trailing(if trailing.contains('\n') {
        trailing
    } else {
        "\n".to_string()
    });
    array.set_trailing_comma(!array.is_empty());
}

/// `locations.<name>.upstream` of a stored location.
pub fn location_upstream(toml: &str, name: &str) -> Result<Option<String>, RenderError> {
    entry_string(toml, "locations", name, "upstream")
}

/// `locations.<name>.plugins` of a stored location; empty when absent or unreadable.
pub fn location_plugins(toml: &str, name: &str) -> Vec<String> {
    entry_table(toml, "locations", name)
        .ok()
        .flatten()
        .and_then(|t| t.get("plugins").and_then(|v| v.as_array()).cloned())
        .map(|values| {
            values
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The HTTPRoute a stored location was generated for, from its `remark`; `None` for a
/// location without a generated remark.
pub fn location_owner(toml: &str, name: &str) -> Option<RouteKey> {
    let remark = entry_string(toml, "locations", name, "remark").ok()??;
    let rest = remark
        .strip_prefix(REMARK_PREFIX)?
        .strip_prefix(" httproute ")?;
    let (namespace, name) = rest.split_once('/')?;
    if namespace.is_empty() || name.is_empty() || name.contains(char::is_whitespace) {
        return None;
    }
    Some(RouteKey::new(namespace, name))
}

/// The upstream a stored plugin names (`plugins.<name>.upstream`, as `traffic_splitting`
/// plugins do). Read for any plugin category, so an upstream a plugin needs is never
/// deleted.
pub fn plugin_upstream(toml: &str, name: &str) -> Result<Option<String>, RenderError> {
    entry_string(toml, "plugins", name, "upstream")
}

/// `upstreams.<name>` without its `addrs`, for telling an addrs-only update apart.
pub(crate) fn upstream_without_addrs(toml: &str, name: &str) -> Option<toml::Table> {
    let mut table = entry_table(toml, "upstreams", name).ok()??;
    table.remove("addrs");
    Some(table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::collections::BTreeMap;
    use std::time::Duration;

    use crate::gateway::model::LocationOptions;

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn owner() -> RouteKey {
        RouteKey::new("shop", "web")
    }

    // Mirrors of the Pingap structs, with their field names.
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct LocationConf {
        host: Option<String>,
        path: Option<String>,
        match_headers: Option<Vec<String>>,
        match_query: Option<Vec<String>>,
        upstream: Option<String>,
        weight: Option<u16>,
        rewrite: Option<String>,
        proxy_set_headers: Option<Vec<String>>,
        plugins: Option<Vec<String>>,
        max_retries: Option<u8>,
        #[serde(default, with = "humantime_serde")]
        max_retry_window: Option<Duration>,
        #[serde(default, with = "humantime_serde")]
        connection_timeout: Option<Duration>,
        #[serde(default, with = "humantime_serde")]
        read_timeout: Option<Duration>,
        #[serde(default, with = "humantime_serde")]
        write_timeout: Option<Duration>,
        remark: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct CertificateConf {
        domains: Option<String>,
        is_default: Option<bool>,
        tls_cert: Option<String>,
        tls_key: Option<String>,
        remark: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct UpstreamConf {
        addrs: Vec<String>,
        discovery: Option<String>,
        update_frequency: Option<String>,
        health_check: Option<String>,
        alpn: Option<String>,
        sni: Option<String>,
        verify_cert: Option<bool>,
        ca: Option<String>,
        client_cert: Option<String>,
        client_key: Option<String>,
        remark: Option<String>,
    }

    fn parse<T: for<'de> Deserialize<'de>>(toml: &str, category: &str) -> BTreeMap<String, T> {
        let mut doc: BTreeMap<String, BTreeMap<String, T>> = toml::from_str(toml).unwrap();
        assert_eq!(doc.len(), 1, "{toml}");
        doc.remove(category).unwrap()
    }

    fn location() -> LocationEntry {
        LocationEntry {
            owner: owner(),
            owner_kind: RouteKind::Http,
            server: "http".into(),
            host: Some("shop.example.com".into()),
            path: "~^/api(/|$)".into(),
            match_headers: s(&["x-env:canary"]),
            match_query: s(&["debug:1"]),
            upstream: Some("k8s-shop-api-80-1234abcd".into()),
            weight: 60000,
            rewrite: Some("^/api/(.*) /$1".into()),
            proxy_set_headers: s(&["host:api.internal"]),
            plugins: s(&["pingap:requestId", "auth"]),
            options: LocationOptions {
                max_retries: Some(3),
                max_retry_window: Some(15_000),
                connection_timeout: Some(500),
                read_timeout: Some(500),
                write_timeout: Some(500),
            },
        }
    }

    #[test]
    fn location_parses_as_pingap_location() {
        let out = render_location("k8s-shop-web-1", &location());
        let mut map: BTreeMap<String, LocationConf> = parse(&out, "locations");
        let conf = map.remove("k8s-shop-web-1").unwrap();
        assert_eq!(conf.host.as_deref(), Some("shop.example.com"));
        assert_eq!(conf.path.as_deref(), Some("~^/api(/|$)"));
        assert_eq!(conf.upstream.as_deref(), Some("k8s-shop-api-80-1234abcd"));
        assert_eq!(conf.weight, Some(60000));
        assert_eq!(conf.match_headers, Some(s(&["x-env:canary"])));
        assert_eq!(conf.match_query, Some(s(&["debug:1"])));
        assert_eq!(conf.rewrite.as_deref(), Some("^/api/(.*) /$1"));
        assert_eq!(conf.proxy_set_headers, Some(s(&["host:api.internal"])));
        assert_eq!(conf.plugins, Some(s(&["pingap:requestId", "auth"])));
        assert_eq!(conf.max_retries, Some(3));
        assert_eq!(conf.max_retry_window, Some(Duration::from_secs(15)));
        assert_eq!(conf.connection_timeout, Some(Duration::from_millis(500)));
        assert_eq!(conf.read_timeout, Some(Duration::from_millis(500)));
        assert_eq!(conf.write_timeout, Some(Duration::from_millis(500)));
        assert_eq!(
            conf.remark.as_deref(),
            Some("pingap-k8s httproute shop/web")
        );
        assert_eq!(location_owner(&out, "k8s-shop-web-1"), Some(owner()));
        assert_eq!(
            location_upstream(&out, "k8s-shop-web-1")
                .unwrap()
                .as_deref(),
            Some("k8s-shop-api-80-1234abcd")
        );
    }

    #[test]
    fn bare_location_leaves_out_host_path_upstream_and_options() {
        let entry = LocationEntry {
            owner: owner(),
            owner_kind: RouteKind::Http,
            server: "http".into(),
            host: None,
            path: String::new(),
            match_headers: vec![],
            match_query: vec![],
            upstream: None,
            weight: 0,
            rewrite: None,
            proxy_set_headers: vec![],
            plugins: vec![],
            options: LocationOptions::default(),
        };
        let out = render_location("k8s-shop-web-default", &entry);
        let conf = parse::<LocationConf>(&out, "locations")
            .remove("k8s-shop-web-default")
            .unwrap();
        assert_eq!(conf.host, None);
        assert_eq!(conf.path, None);
        assert_eq!(conf.upstream, None);
        assert_eq!(conf.match_headers, None);
        assert_eq!(conf.weight, Some(0));
        assert_eq!(conf.plugins, None);
        assert_eq!(conf.rewrite, None);
        assert_eq!(conf.max_retries, None);
        assert_eq!(conf.read_timeout, None);
    }

    #[test]
    fn longest_timeout_parses_as_a_pingap_duration() {
        let mut entry = location();
        let longest = crate::gateway::extended::LONGEST_TIMEOUT_MS;
        entry.options.read_timeout = Some(longest);
        let out = render_location("k8s-a", &entry);
        let conf = parse::<LocationConf>(&out, "locations")
            .remove("k8s-a")
            .unwrap();
        assert_eq!(conf.read_timeout, Some(Duration::from_millis(longest)));
    }

    #[test]
    fn certificate_parses_as_pingap_certificate() {
        let cert = CertificateEntry {
            secret: SecretKey {
                namespace: "shop".into(),
                name: "tls".into(),
            },
            domains: s(&["shop.example.com", "*.shop.example.com"]),
            is_default: false,
            tls_cert: "-----BEGIN CERTIFICATE-----\nAAA\n-----END CERTIFICATE-----\n".into(),
            tls_key: format!(
                "-----BEGIN {k}-----\nBBB\n-----END {k}-----\n",
                k = "PRIVATE KEY"
            ),
        };
        let out = render_certificate("k8s-shop-tls-1", &cert);
        let conf = parse::<CertificateConf>(&out, "certificates")
            .remove("k8s-shop-tls-1")
            .unwrap();
        assert_eq!(
            conf.domains.as_deref(),
            Some("shop.example.com,*.shop.example.com")
        );
        assert_eq!(conf.tls_cert.as_deref(), Some(cert.tls_cert.as_str()));
        assert_eq!(conf.tls_key.as_deref(), Some(cert.tls_key.as_str()));
        assert_eq!(conf.remark.as_deref(), Some("pingap-k8s secret shop/tls"));
        assert_eq!(conf.is_default, None);
    }

    #[test]
    fn default_certificate_has_explicit_domains() {
        let cert = CertificateEntry {
            secret: SecretKey {
                namespace: "shop".into(),
                name: "tls".into(),
            },
            domains: Vec::new(),
            is_default: true,
            tls_cert: "-----BEGIN CERTIFICATE-----\nAAA\n-----END CERTIFICATE-----\n".into(),
            tls_key: format!(
                "-----BEGIN {k}-----\nBBB\n-----END {k}-----\n",
                k = "PRIVATE KEY"
            ),
        };
        let out = render_certificate("k8s-shop-tls-1", &cert);
        let conf = parse::<CertificateConf>(&out, "certificates")
            .remove("k8s-shop-tls-1")
            .unwrap();
        // An empty string, not an absent key: Pingap serves it for no name of its own.
        assert_eq!(conf.domains.as_deref(), Some(""));
        assert_eq!(conf.is_default, Some(true));

        let cert = CertificateEntry {
            domains: s(&["*", "*.org"]),
            ..cert
        };
        let out = render_certificate("k8s-shop-tls-1", &cert);
        let conf = parse::<CertificateConf>(&out, "certificates")
            .remove("k8s-shop-tls-1")
            .unwrap();
        assert_eq!(conf.domains.as_deref(), Some("*,*.org"));
        assert_eq!(conf.is_default, Some(true));
    }

    fn external(addr: &str) -> ExternalUpstream {
        ExternalUpstream {
            service: ServiceKey {
                namespace: "shop".into(),
                name: "legacy".into(),
            },
            addr: addr.into(),
            health_check: None,
            h2c: false,
            tls: None,
        }
    }

    #[test]
    fn external_upstream_uses_dns_discovery() {
        let out =
            render_external_upstream("k8s-x", &external("db.example.com:5432"), "5s").unwrap();
        let conf = parse::<UpstreamConf>(&out, "upstreams")
            .remove("k8s-x")
            .unwrap();
        assert_eq!(conf.addrs, s(&["db.example.com:5432"]));
        assert_eq!(conf.discovery.as_deref(), Some("dns"));
        assert_eq!(conf.update_frequency.as_deref(), Some("5s"));
        assert_eq!(
            conf.remark.as_deref(),
            Some("pingap-k8s service shop/legacy")
        );
        assert_eq!(conf.health_check, None);

        let mut with_check = external("db.example.com:5432");
        with_check.health_check = Some("tcp://db?check_frequency=5s".into());
        let out = render_external_upstream("k8s-x", &with_check, "5s").unwrap();
        let conf = parse::<UpstreamConf>(&out, "upstreams")
            .remove("k8s-x")
            .unwrap();
        assert_eq!(
            conf.health_check.as_deref(),
            Some("tcp://db?check_frequency=5s")
        );
        assert_eq!(conf.alpn, None);

        let mut h2c = external("grpc.example.com:9000");
        h2c.h2c = true;
        let out = render_external_upstream("k8s-x", &h2c, "5s").unwrap();
        let conf = parse::<UpstreamConf>(&out, "upstreams")
            .remove("k8s-x")
            .unwrap();
        assert_eq!(conf.alpn.as_deref(), Some("H2"));
    }

    #[test]
    fn grpc_location_remark_names_the_grpcroute() {
        let mut entry = location();
        entry.owner_kind = RouteKind::Grpc;
        entry.path = "=/pkg.Svc/Get".into();
        let out = render_location("k8s-shop-web-2", &entry);
        let conf = parse::<LocationConf>(&out, "locations")
            .remove("k8s-shop-web-2")
            .unwrap();
        assert_eq!(conf.path.as_deref(), Some("=/pkg.Svc/Get"));
        assert_eq!(
            conf.remark.as_deref(),
            Some("pingap-k8s grpcroute shop/web")
        );
        // Not taken for an HTTPRoute location.
        assert_eq!(location_owner(&out, "k8s-shop-web-2"), None);
    }

    #[test]
    fn external_upstream_with_tls() {
        let mut upstream = external("db.example.com:443");
        upstream.tls = Some(crate::types::UpstreamTls {
            sni: "db.example.com".into(),
            ca: Some("CA PEM".into()),
            client_cert: Some(("CERT PEM".into(), "KEY PEM".into())),
        });
        let out = render_external_upstream("k8s-x", &upstream, "5s").unwrap();
        let conf = parse::<UpstreamConf>(&out, "upstreams")
            .remove("k8s-x")
            .unwrap();
        assert_eq!(conf.sni.as_deref(), Some("db.example.com"));
        assert_eq!(conf.verify_cert, Some(true));
        assert_eq!(conf.ca.as_deref(), Some("CA PEM"));
        assert_eq!(conf.client_cert.as_deref(), Some("CERT PEM"));
        assert_eq!(conf.client_key.as_deref(), Some("KEY PEM"));

        let plain =
            render_external_upstream("k8s-x", &external("db.example.com:443"), "5s").unwrap();
        let conf = parse::<UpstreamConf>(&plain, "upstreams")
            .remove("k8s-x")
            .unwrap();
        assert_eq!((conf.sni, conf.verify_cert, conf.ca), (None, None, None));
    }

    #[test]
    fn external_ip_literal_is_static_and_filtered() {
        for addr in ["10.1.2.3:80", "[2001:db8::1]:443"] {
            let out = render_external_upstream("k8s-x", &external(addr), "5s").unwrap();
            let conf = parse::<UpstreamConf>(&out, "upstreams")
                .remove("k8s-x")
                .unwrap();
            assert_eq!(conf.addrs, s(&[addr]));
            assert_eq!(conf.discovery, None);
            assert_eq!(conf.update_frequency, None);
        }
        for addr in [
            "127.0.0.1:80",
            "169.254.169.254:80",
            "0.0.0.0:80",
            "[::1]:80",
            "[fe80::1]:80",
            "224.0.0.1:80",
            "[::ffff:127.0.0.1]:80",
            "db.example.com",
            "db.example.com:0",
            "db.example.com:http",
            ":80",
            "a b:80",
            "fd00::1:80",
        ] {
            assert!(
                render_external_upstream("k8s-x", &external(addr), "5s").is_err(),
                "{addr}"
            );
        }
    }

    #[test]
    fn same_entry_ignores_formatting() {
        let rendered = render_location("k8s-a", &location());
        let reformatted = format!(
            "# edited\nlocations.k8s-a = {{ weight = {}, upstream = \"k8s-shop-api-80-1234abcd\", \
             remark = 'pingap-k8s httproute shop/web', host = \"shop.example.com\", \
             path = '~^/api(/|$)', rewrite = '^/api/(.*) /$1', match_headers = ['x-env:canary'], \
             match_query = ['debug:1'], proxy_set_headers = ['host:api.internal'], \
             plugins = [\n \"pingap:requestId\",\n \"auth\",\n], max_retries = 3, \
             max_retry_window = '15000ms', connection_timeout = \"500ms\", \
             read_timeout = \"500ms\", write_timeout = \"500ms\" }}\n",
            60000
        );
        assert!(same_entry(&reformatted, &rendered));
        let mut changed = location();
        changed.weight -= 1;
        assert!(!same_entry(&rendered, &render_location("k8s-a", &changed)));
        assert!(!same_entry("not toml [", &rendered));
    }

    const SERVER: &str = r#"# the public server
[servers.http]
addr = "0.0.0.0:80"
locations = [
    "static", # hand-made, first
    "k8s-shop-web-old",
    "admin",
]
global_certificates = true
"#;

    #[test]
    fn replace_keeps_comments_and_hand_made_order() {
        let out = replace_owned_locations(SERVER, "http", &s(&["k8s-b", "k8s-a"]))
            .unwrap()
            .unwrap();
        assert_eq!(
            out,
            SERVER.replace(
                "    \"k8s-shop-web-old\",\n    \"admin\",\n",
                "    \"admin\",\n    \"k8s-b\",\n    \"k8s-a\",\n"
            )
        );
        assert_eq!(
            server_locations(&out, "http").unwrap(),
            s(&["static", "admin", "k8s-b", "k8s-a"])
        );
        // Unchanged: None. Hand-made names are never added from desired_owned.
        assert_eq!(
            replace_owned_locations(&out, "http", &s(&["k8s-b", "k8s-a", "static"])).unwrap(),
            None
        );
        // Stripping all owned names.
        let out = replace_owned_locations(SERVER, "http", &[])
            .unwrap()
            .unwrap();
        assert_eq!(
            server_locations(&out, "http").unwrap(),
            s(&["static", "admin"])
        );
        assert!(out.contains("# hand-made, first"));
        assert!(out.starts_with("# the public server\n"));

        // A comment line in front of a dropped name moves to the next element.
        let doc =
            "[servers.http]\nlocations = [\n    # owned below\n    \"k8s-x\",\n    \"a\",\n]\n";
        let out = replace_owned_locations(doc, "http", &s(&["k8s-y"]))
            .unwrap()
            .unwrap();
        assert_eq!(
            out,
            "[servers.http]\nlocations = [\n    # owned below\n    \"a\",\n    \"k8s-y\",\n]\n"
        );
    }

    #[test]
    fn replace_single_line_and_missing_key() {
        let doc = "[servers.http]\naddr = \"0.0.0.0:80\"\nlocations = [\"k8s-old\", \"a\"]\n";
        let out = replace_owned_locations(doc, "http", &s(&["k8s-new"]))
            .unwrap()
            .unwrap();
        assert_eq!(
            out,
            "[servers.http]\naddr = \"0.0.0.0:80\"\nlocations = [\"a\", \"k8s-new\"]\n"
        );

        let doc = "[servers.http]\naddr = \"0.0.0.0:80\" # keep\n";
        assert_eq!(replace_owned_locations(doc, "http", &[]).unwrap(), None);
        let out = replace_owned_locations(doc, "http", &s(&["k8s-a"]))
            .unwrap()
            .unwrap();
        assert_eq!(server_locations(&out, "http").unwrap(), s(&["k8s-a"]));
        assert!(out.contains("# keep"));
    }

    #[test]
    fn replace_errors() {
        assert!(matches!(
            replace_owned_locations("[servers.other]\n", "http", &[]),
            Err(RenderError::MissingTable { .. })
        ));
        assert!(matches!(
            replace_owned_locations("[servers.http]\nlocations = \"a\"\n", "http", &[]),
            Err(RenderError::InvalidLocations(_))
        ));
        assert!(matches!(
            replace_owned_locations("[servers.http\n", "http", &[]),
            Err(RenderError::Toml(_))
        ));
    }

    #[test]
    fn owner_and_plugin_reads() {
        let hand = "[locations.x]\nupstream = \"u\"\nremark = \"made by hand\"\n";
        assert_eq!(location_owner(hand, "x"), None);
        assert_eq!(location_owner("[locations.x]\n", "x"), None);
        assert_eq!(location_owner("not toml [", "x"), None);
        assert_eq!(
            location_owner(
                "[locations.x]\nremark = \"pingap-k8s httproute a/b\"\n",
                "x"
            ),
            Some(RouteKey::new("a", "b"))
        );
        let plugin = "[plugins.split]\ncategory = \"traffic_splitting\"\nupstream = \"k8s-canary\"\nweight = 10\n";
        assert_eq!(
            plugin_upstream(plugin, "split").unwrap().as_deref(),
            Some("k8s-canary")
        );
        assert_eq!(
            plugin_upstream("[plugins.p]\ncategory = \"stats\"\n", "p").unwrap(),
            None
        );
        assert!(plugin_upstream("x = [", "p").is_err());
    }

    #[test]
    fn plugin_parses_with_category_and_remark() {
        let mut fields = toml::Table::new();
        fields.insert("status".into(), toml::Value::Integer(500));
        fields.insert("percentage".into(), toml::Value::Integer(50));
        let plugin = PluginEntry {
            category: "mock".into(),
            fields,
        };
        let out = render_plugin("k8s-mock-1", &plugin);
        let doc: toml::Table = toml::from_str(&out).unwrap();
        let entry = doc["plugins"]["k8s-mock-1"].as_table().unwrap();
        assert_eq!(entry["category"].as_str(), Some("mock"));
        assert_eq!(entry["status"].as_integer(), Some(500));
        assert_eq!(entry["percentage"].as_integer(), Some(50));
        assert_eq!(entry["remark"].as_str(), Some("pingap-k8s plugin"));
        assert!(same_entry(&out, &render_plugin("k8s-mock-1", &plugin)));
    }

    /// The fields of Pingap's `cors` plugin, with the types it reads them as.
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct CorsConf {
        category: String,
        allow_origins: Vec<String>,
        allow_methods: String,
        allow_headers: Option<String>,
        expose_headers: Option<String>,
        allow_credentials: Option<bool>,
        #[serde(with = "humantime_serde")]
        max_age: Duration,
        remark: String,
    }

    /// The fields of Pingap's `mirror` plugin, with the types it reads them as.
    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct MirrorConf {
        category: String,
        target: String,
        percentage: Option<u8>,
        methods: Vec<String>,
        max_body_size: String,
        remark: String,
    }

    #[test]
    fn cors_and_mirror_plugins_parse_as_pingap_plugins() {
        use crate::gateway::api::httproutes::HttpRouteRulesFiltersCors;
        use crate::gateway::extended::{cors_plugin, mirror_plugin};

        let cors: HttpRouteRulesFiltersCors = serde_json::from_value(serde_json::json!({
            "allowOrigins": ["https://*.bar.com"], "allowMethods": ["GET"],
            "allowHeaders": ["x-a"], "exposeHeaders": ["x-b"], "allowCredentials": true,
            "maxAge": 3600,
        }))
        .unwrap();
        let (name, plugin) = cors_plugin(&cors).unwrap().unwrap();
        let out = render_plugin(&name, &plugin);
        let conf = parse::<CorsConf>(&out, "plugins").remove(&name).unwrap();
        assert_eq!(conf.category, "cors");
        assert_eq!(conf.max_age, Duration::from_secs(3600));
        assert_eq!(conf.allow_methods, "GET");
        assert_eq!(conf.allow_headers.as_deref(), Some("x-a"));
        assert_eq!(conf.expose_headers.as_deref(), Some("x-b"));
        assert_eq!(conf.allow_credentials, Some(true));
        assert_eq!(conf.remark, "pingap-k8s plugin");
        // Pingap compiles a `~` entry as an anchored pattern.
        for origin in &conf.allow_origins {
            let pattern = origin.strip_prefix('~').unwrap();
            regex::Regex::new(&format!("^(?:{pattern})$")).unwrap();
        }

        let (name, plugin) = mirror_plugin("http://v2.ns.svc.cluster.local:8080", 20);
        let out = render_plugin(&name, &plugin);
        let conf = parse::<MirrorConf>(&out, "plugins").remove(&name).unwrap();
        assert_eq!(conf.category, "mirror");
        assert_eq!(conf.percentage, Some(20));
        assert_eq!(conf.max_body_size, "1MiB");
        assert!(conf.methods.contains(&"POST".to_string()));
        assert_eq!(conf.remark, "pingap-k8s plugin");
        // Pingap parses the target as a URL without a query.
        let url = url::Url::parse(&conf.target).unwrap();
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host_str(), Some("v2.ns.svc.cluster.local"));
        assert_eq!(url.port(), Some(8080));
        assert_eq!(url.query(), None);
    }

    #[test]
    fn server_info_reads_ports_and_tls() {
        assert_eq!(
            server_info(SERVER, "http"),
            Some(ServerInfo {
                name: "http".into(),
                ports: BTreeSet::from([80]),
                tls: true,
                h2: false,
            })
        );
        let doc = "[servers.web]\naddr = \"0.0.0.0:8080,[::]:8080, 127.0.0.1:9000\"\n";
        assert_eq!(
            server_info(doc, "web").map(|s| (s.ports, s.tls)),
            Some((BTreeSet::from([8080, 9000]), false))
        );
        assert_eq!(
            server_info(
                "[servers.web]\naddr = \"0.0.0.0:80\"\nenabled_h2 = true\n",
                "web"
            )
            .map(|s| (s.tls, s.h2)),
            Some((false, true))
        );
        assert_eq!(server_info("[servers.web]\n", "web"), None);
        assert_eq!(server_info("[servers.web]\naddr = \"x\"\n", "web"), None);
        assert_eq!(server_info("[servers.web\n", "web"), None);
    }

    #[test]
    fn upstream_without_addrs_drops_only_addrs() {
        let a = "[upstreams.u]\naddrs = [\"1.1.1.1:1\"]\nhealth_check = \"tcp://x\"\n";
        let b = "[upstreams.u]\naddrs = [\"2.2.2.2:2\"]\nhealth_check = \"tcp://x\"\n";
        let c = "[upstreams.u]\naddrs = [\"2.2.2.2:2\"]\nhealth_check = \"tcp://y\"\n";
        assert_eq!(
            upstream_without_addrs(a, "u"),
            upstream_without_addrs(b, "u")
        );
        assert_ne!(
            upstream_without_addrs(a, "u"),
            upstream_without_addrs(c, "u")
        );
    }
}

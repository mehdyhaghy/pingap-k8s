//! Names of the Pingap entries generated for Gateways, HTTPRoutes and GRPCRoutes.
//!
//! Every name is `k8s-` + readable parts (`[a-z0-9-]`, `.` and anything else become `-`,
//! each part capped in length) + a stable 8-hex FNV-1a hash over the full, unambiguous
//! identity. The readable parts are for humans only: two identities that sanitize to the
//! same text (`a-b`/`c` and `a`/`b-c`, `a.b` and `a-b`) still differ in the hash. Owners
//! are found from the `remark`, never parsed back out of a name.

use crate::types::{PortRef, ServiceRef, UpstreamTls};

use super::model::{BackendKind, OWNED_PREFIX, RouteKey, RouteKind, SecretKey};

/// Longest readable part kept in a name. Kubernetes names go up to 253 characters; the
/// hash keeps truncated names unique.
const MAX_PART: usize = 40;

/// The upstream of one Service port, shared by every route that has it as its only
/// backend: `k8s-<ns>-<svc>-<port>[-svc|-ext]-<h8>`. The Pingap `health_check` is part of
/// the hash, so a changed check is a new upstream.
pub fn upstream_name(
    namespace: &str,
    service: &str,
    port: &PortRef,
    kind: BackendKind,
    health_check: Option<&str>,
) -> String {
    let (port_tag, port_text) = port_parts(port);
    let (kind_tag, suffix) = kind_parts(kind);
    let mut identity = vec![
        "upstream", namespace, service, port_tag, &port_text, kind_tag,
    ];
    if let Some(health_check) = health_check {
        identity.extend(["health_check", health_check]);
    }
    let hash = fnv1a(&identity);
    let mut parts = vec![namespace, service, port_text.as_str()];
    parts.extend(suffix);
    build(&parts, &hash)
}

/// The upstream of a weighted set of backends: `k8s-<ns>-<svc>-split[-svc]-<h8>`, named
/// after the first backend and hashed over the whole set (in any order: it is sorted
/// here), the weights and the `health_check`. Rules with the same set share it.
pub fn split_upstream_name(
    backends: &[ServiceRef],
    kind: BackendKind,
    health_check: Option<&str>,
) -> String {
    let mut sorted: Vec<&ServiceRef> = backends.iter().collect();
    sorted.sort_by(|a, b| (&a.key, &a.port, a.weight).cmp(&(&b.key, &b.port, b.weight)));
    let (kind_tag, suffix) = kind_parts(kind);
    let texts: Vec<[String; 5]> = sorted
        .iter()
        .map(|b| {
            let (port_tag, port_text) = port_parts(&b.port);
            [
                b.key.namespace.clone(),
                b.key.name.clone(),
                port_tag.to_string(),
                port_text,
                b.weight.map(|w| w.to_string()).unwrap_or_default(),
            ]
        })
        .collect();
    let mut identity: Vec<&str> = vec!["split", kind_tag];
    for text in &texts {
        identity.extend(text.iter().map(String::as_str));
    }
    if let Some(health_check) = health_check {
        identity.extend(["health_check", health_check]);
    }
    let hash = fnv1a(&identity);
    let (namespace, service) = sorted
        .first()
        .map(|b| (b.key.namespace.as_str(), b.key.name.as_str()))
        .unwrap_or(("", ""));
    let mut parts = vec![namespace, service, "split"];
    parts.extend(suffix);
    build(&parts, &hash)
}

/// The TLS variant of the upstream `name` (from [`upstream_name`] or
/// [`split_upstream_name`]): `<name without its hash>-tls-<h8>`, hashed over `name` and
/// every TLS field (SNI, CA bundle, client certificate and key), so a changed CA or a
/// rotated client certificate is a new upstream. The plain and the TLS upstream of the
/// same backends never share a name.
pub fn tls_upstream_name(name: &str, tls: &UpstreamTls) -> String {
    let (client_cert, client_key) = tls
        .client_cert
        .as_ref()
        .map_or(("", ""), |(c, k)| (c.as_str(), k.as_str()));
    let hash = fnv1a(&[
        "tls",
        name,
        &tls.sni,
        if tls.ca.is_some() { "ca" } else { "system" },
        tls.ca.as_deref().unwrap_or(""),
        if tls.client_cert.is_some() {
            "client"
        } else {
            "anonymous"
        },
        client_cert,
        client_key,
    ]);
    let readable = name
        .rsplit_once('-')
        .map_or(name, |(head, _)| head)
        .strip_prefix(OWNED_PREFIX)
        .unwrap_or(name);
    let mut out = String::from(OWNED_PREFIX);
    out.push_str(readable);
    out.push_str("-tls-");
    out.push_str(&hash);
    out
}

/// The HTTP/2 cleartext variant of a generated upstream name (from [`upstream_name`] or
/// [`split_upstream_name`]): `k8s-<readable>-h2c-<h8>`, hashed over the HTTP/1.1 name, so
/// a Service used both ways gets two entries, each with its own `alpn`.
pub fn h2c_upstream_name(name: &str) -> String {
    let readable = name
        .strip_prefix(OWNED_PREFIX)
        .and_then(|rest| rest.rsplit_once('-'))
        .map_or("x", |(readable, _)| readable);
    format!("{OWNED_PREFIX}{readable}-h2c-{}", fnv1a(&["h2c", name]))
}

/// The location of one HTTPRoute match on one Pingap server and host:
/// `k8s-<ns>-<route>-<h8>`. The hash covers the server, the host and the match as Pingap
/// sees it (path, header and query conditions), not the rule's position, so reordering
/// rules keeps names.
pub fn location_name(
    route: &RouteKey,
    server: &str,
    host: Option<&str>,
    path: &str,
    match_headers: &[String],
    match_query: &[String],
) -> String {
    route_location_name(
        RouteKind::Http,
        route,
        server,
        host,
        path,
        match_headers,
        match_query,
    )
}

/// [`location_name`] for a route of any kind: the kind is part of the hash for
/// GRPCRoutes, so an HTTPRoute and a GRPCRoute with the same name never share a location.
pub fn route_location_name(
    kind: RouteKind,
    route: &RouteKey,
    server: &str,
    host: Option<&str>,
    path: &str,
    match_headers: &[String],
    match_query: &[String],
) -> String {
    let (host_tag, host) = match host {
        Some(host) => ("host", host),
        None => ("any", ""),
    };
    let headers = match_headers.len().to_string();
    let query = match_query.len().to_string();
    let mut identity = vec![
        match kind {
            RouteKind::Http => "location",
            RouteKind::Grpc => "grpc-location",
        },
        &route.namespace,
        &route.name,
        server,
        host_tag,
        host,
        path,
        "headers",
        &headers,
    ];
    identity.extend(match_headers.iter().map(String::as_str));
    identity.extend(["query", &query]);
    identity.extend(match_query.iter().map(String::as_str));
    let hash = fnv1a(&identity);
    build(&[&route.namespace, &route.name], &hash)
}

/// The certificate of one `kubernetes.io/tls` Secret, shared by every listener that
/// names it: `k8s-<ns>-<secret>-<h8>`.
pub fn certificate_name(secret: &SecretKey) -> String {
    let hash = fnv1a(&["certificate", &secret.namespace, &secret.name]);
    build(&[&secret.namespace, &secret.name], &hash)
}

/// A generated plugin, named after its content: `k8s-<category>-<h8>`. `content` is the
/// rendered configuration; identical plugins get the same name and are shared.
pub fn plugin_name(category: &str, content: &str) -> String {
    let hash = fnv1a(&["plugin", category, content]);
    build(&[category], &hash)
}

fn port_parts(port: &PortRef) -> (&'static str, String) {
    match port {
        PortRef::Name(name) => ("name", name.clone()),
        PortRef::Number(number) => ("number", number.to_string()),
    }
}

fn kind_parts(kind: BackendKind) -> (&'static str, Option<&'static str>) {
    match kind {
        BackendKind::Pods => ("pods", None),
        BackendKind::Service => ("service", Some("svc")),
        BackendKind::External => ("external", Some("ext")),
    }
}

fn build(parts: &[&str], hash: &str) -> String {
    let mut name = String::from(OWNED_PREFIX);
    for part in parts {
        name.push_str(&sanitize(part));
        name.push('-');
    }
    name.push_str(hash);
    name
}

/// Lowercase `[a-z0-9-]`, at most [`MAX_PART`] characters, never empty.
fn sanitize(part: &str) -> String {
    let mut out: String = part
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                c
            } else {
                '-'
            }
        })
        .take(MAX_PART)
        .collect();
    if out.is_empty() {
        out.push('x');
    }
    out
}

/// FNV-1a (32 bit) over the parts, each prefixed with its length so that no two part
/// lists encode the same bytes. Rendered as 8 lowercase hex digits.
fn fnv1a(parts: &[&str]) -> String {
    let mut hash: u32 = 0x811c_9dc5;
    let mut feed = |bytes: &[u8]| {
        for b in bytes {
            hash ^= u32::from(*b);
            hash = hash.wrapping_mul(0x0100_0193);
        }
    };
    for part in parts {
        feed(part.len().to_string().as_bytes());
        feed(b":");
        feed(part.as_bytes());
    }
    format!("{hash:08x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ServiceKey;
    use std::collections::BTreeSet;

    fn route(ns: &str, name: &str) -> RouteKey {
        RouteKey::new(ns, name)
    }

    fn sref(ns: &str, name: &str, port: i32, weight: u32) -> ServiceRef {
        ServiceRef {
            key: ServiceKey {
                namespace: ns.into(),
                name: name.into(),
            },
            port: PortRef::Number(port),
            weight: Some(weight),
        }
    }

    fn valid(name: &str) -> bool {
        name.starts_with(OWNED_PREFIX)
            && name.len() <= 128
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    }

    fn loc(key: &RouteKey, host: Option<&str>, path: &str) -> String {
        location_name(key, "http", host, path, &[], &[])
    }

    #[test]
    fn fnv1a_known_values() {
        // No parts: the offset basis.
        assert_eq!(fnv1a(&[]), "811c9dc5");
        assert_eq!(fnv1a(&[""]), fnv1a(&[""]));
        assert_ne!(fnv1a(&[""]), fnv1a(&["", ""]));
        assert_ne!(fnv1a(&["ab", "c"]), fnv1a(&["a", "bc"]));
    }

    #[test]
    fn upstream_names() {
        let port = PortRef::Number(80);
        let pods = upstream_name("default", "web", &port, BackendKind::Pods, None);
        assert!(pods.starts_with("k8s-default-web-80-"), "{pods}");
        assert_eq!(pods.len(), "k8s-default-web-80-".len() + 8);
        let svc = upstream_name("default", "web", &port, BackendKind::Service, None);
        assert!(svc.starts_with("k8s-default-web-80-svc-"), "{svc}");
        let ext = upstream_name(
            "default",
            "web",
            &PortRef::Name("http".into()),
            BackendKind::External,
            None,
        );
        assert!(ext.starts_with("k8s-default-web-http-ext-"), "{ext}");
        for n in [&pods, &svc, &ext] {
            assert!(valid(n), "{n}");
        }
        assert_ne!(pods, svc);
        assert_eq!(
            pods,
            upstream_name("default", "web", &port, BackendKind::Pods, None)
        );
        // The health check is part of the identity.
        let a = upstream_name("default", "web", &port, BackendKind::Pods, Some("tcp://a"));
        let b = upstream_name("default", "web", &port, BackendKind::Pods, Some("tcp://b"));
        assert!(a != pods && a != b);
        // Port names and numbers do not collide.
        assert_ne!(
            upstream_name("ns", "s", &PortRef::Number(80), BackendKind::Pods, None),
            upstream_name(
                "ns",
                "s",
                &PortRef::Name("80".into()),
                BackendKind::Pods,
                None
            )
        );
    }

    #[test]
    fn split_upstream_names() {
        let a = sref("ns", "a", 80, 1);
        let b = sref("ns", "b", 80, 3);
        let n = split_upstream_name(&[a.clone(), b.clone()], BackendKind::Pods, None);
        assert!(n.starts_with("k8s-ns-a-split-"), "{n}");
        assert!(valid(&n), "{n}");
        // Order does not matter; weights, members, kind and check do.
        assert_eq!(
            n,
            split_upstream_name(&[b.clone(), a.clone()], BackendKind::Pods, None)
        );
        assert_ne!(
            n,
            split_upstream_name(
                &[a.clone(), sref("ns", "b", 80, 2)],
                BackendKind::Pods,
                None
            )
        );
        assert_ne!(
            n,
            split_upstream_name(&[a.clone(), b.clone()], BackendKind::Service, None)
        );
        assert_ne!(
            n,
            split_upstream_name(&[a.clone(), b.clone()], BackendKind::Pods, Some("tcp://x"))
        );
        assert_ne!(n, split_upstream_name(&[a], BackendKind::Pods, None));
    }

    #[test]
    fn location_names() {
        let key = route("prod", "shop.v2");
        let a = loc(&key, Some("example.com"), "~^/api(/|$)");
        assert!(a.starts_with("k8s-prod-shop-v2-"), "{a}");
        assert!(valid(&a), "{a}");
        assert_eq!(a, loc(&key, Some("example.com"), "~^/api(/|$)"));
        // Every part of the identity matters.
        assert_ne!(a, loc(&key, None, "~^/api(/|$)"));
        assert_ne!(a, loc(&key, Some(""), "~^/api(/|$)"));
        assert_ne!(a, loc(&key, Some("example.com"), "=/api"));
        assert_ne!(
            a,
            location_name(&key, "https", Some("example.com"), "~^/api(/|$)", &[], &[])
        );
        assert_ne!(
            a,
            loc(&route("prod", "other"), Some("example.com"), "~^/api(/|$)")
        );
        let h = vec!["x:1".to_string()];
        assert_ne!(
            location_name(&key, "http", None, "", &h, &[]),
            location_name(&key, "http", None, "", &[], &h)
        );
        // Host and path cannot trade bytes; neither can namespace and name.
        assert_ne!(loc(&key, Some("ab"), "/c"), loc(&key, Some("a"), "b/c"));
        assert_ne!(
            loc(&route("a-b", "c"), None, ""),
            loc(&route("a", "b-c"), None, "")
        );
        assert_ne!(
            loc(&route("ns", "a.b"), None, ""),
            loc(&route("ns", "a-b"), None, "")
        );
    }

    #[test]
    fn h2c_upstream_and_grpc_location_names() {
        let port = PortRef::Number(8080);
        let h1 = upstream_name("ns", "grpc", &port, BackendKind::Pods, None);
        let h2 = h2c_upstream_name(&h1);
        assert!(h2.starts_with("k8s-ns-grpc-8080-h2c-"), "{h2}");
        assert_eq!(h2.len(), "k8s-ns-grpc-8080-h2c-".len() + 8);
        assert!(valid(&h2), "{h2}");
        assert_ne!(h1, h2);
        assert_eq!(h2, h2c_upstream_name(&h1));
        let svc = upstream_name("ns", "grpc", &port, BackendKind::Service, None);
        assert_ne!(h2c_upstream_name(&svc), h2);
        let split = split_upstream_name(
            &[sref("ns", "a", 80, 1), sref("ns", "b", 80, 1)],
            BackendKind::Pods,
            None,
        );
        assert!(
            h2c_upstream_name(&split).starts_with("k8s-ns-a-split-h2c-"),
            "{split}"
        );

        let key = route("ns", "r");
        let http = route_location_name(RouteKind::Http, &key, "http", None, "=/a/b", &[], &[]);
        assert_eq!(http, location_name(&key, "http", None, "=/a/b", &[], &[]));
        let grpc = route_location_name(RouteKind::Grpc, &key, "http", None, "=/a/b", &[], &[]);
        assert_ne!(http, grpc);
        assert!(grpc.starts_with("k8s-ns-r-"), "{grpc}");
    }

    #[test]
    fn certificate_and_plugin_names() {
        let c = certificate_name(&SecretKey::new("prod", "shop-tls"));
        assert!(c.starts_with("k8s-prod-shop-tls-"), "{c}");
        assert!(valid(&c), "{c}");
        assert_ne!(
            certificate_name(&SecretKey::new("a-b", "c")),
            certificate_name(&SecretKey::new("a", "b-c"))
        );
        let p = plugin_name("request_headers", "set_headers = [\"a:b\"]\n");
        assert!(p.starts_with("k8s-request-headers-"), "{p}");
        assert!(valid(&p), "{p}");
        assert_eq!(
            p,
            plugin_name("request_headers", "set_headers = [\"a:b\"]\n")
        );
        assert_ne!(
            p,
            plugin_name("response_headers", "set_headers = [\"a:b\"]\n")
        );
        assert_ne!(
            p,
            plugin_name("request_headers", "set_headers = [\"a:c\"]\n")
        );
    }

    #[test]
    fn charset_and_length_limits() {
        let long = "a".repeat(253);
        let n = loc(&route(&"n".repeat(63), &long), Some("x.y"), "");
        assert!(valid(&n), "{n}");
        assert!(n.len() <= 4 + 2 * (MAX_PART + 1) + 8, "{n}");
        let u = upstream_name(
            "NS",
            "Web_App",
            &PortRef::Name("HTTP".into()),
            BackendKind::Pods,
            None,
        );
        assert!(u.starts_with("k8s-ns-web-app-http-"), "{u}");
        // Truncated names stay unique.
        let a = loc(&route("ns", &format!("{long}1")), None, "");
        let b = loc(&route("ns", &format!("{long}2")), None, "");
        assert_ne!(a, b);
        assert_eq!(sanitize(""), "x");
    }

    #[test]
    fn tls_upstream_names() {
        let base = upstream_name("ns", "api", &PortRef::Number(443), BackendKind::Pods, None);
        let tls = UpstreamTls {
            sni: "api.example.com".into(),
            ca: Some("CA".into()),
            client_cert: None,
        };
        let name = tls_upstream_name(&base, &tls);
        assert!(name.starts_with("k8s-ns-api-443-tls-"), "{name}");
        assert_eq!(name.len(), "k8s-ns-api-443-tls-".len() + 8);
        assert_ne!(name, base);
        assert_eq!(name, tls_upstream_name(&base, &tls.clone()));
        let other_ca = UpstreamTls {
            ca: Some("CA2".into()),
            ..tls.clone()
        };
        let system = UpstreamTls {
            ca: None,
            ..tls.clone()
        };
        let empty_ca = UpstreamTls {
            ca: Some(String::new()),
            ..tls.clone()
        };
        let other_sni = UpstreamTls {
            sni: "x.example.com".into(),
            ..tls.clone()
        };
        let client = UpstreamTls {
            client_cert: Some(("C".into(), "K".into())),
            ..tls.clone()
        };
        let rotated = UpstreamTls {
            client_cert: Some(("C2".into(), "K2".into())),
            ..tls.clone()
        };
        let names: BTreeSet<String> = [
            &tls, &other_ca, &system, &empty_ca, &other_sni, &client, &rotated,
        ]
        .into_iter()
        .map(|t| tls_upstream_name(&base, t))
        .collect();
        assert_eq!(names.len(), 7);
        let split_base = split_upstream_name(&[], BackendKind::Pods, None);
        assert_ne!(tls_upstream_name(&split_base, &tls), name);
    }
}

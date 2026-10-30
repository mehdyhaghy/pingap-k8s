//! The extended HTTPRoute features that need more than a match or a header plugin:
//! `retry`, `timeouts`, the CORS filter and the RequestMirror filter (pure, no I/O).
//!
//! **Retries.** Pingap retries a request only when the connection to the backend could
//! not be established (each retry to another backend), never on a response status,
//! which is what the Gateway API asks for at least ("SHOULD retry when ... the
//! connection could not be established"). `attempts` becomes the location's
//! `max_retries` (0 disables, the default is 1, at most 255) and a request timeout its
//! `max_retry_window`. `codes` cannot be honored: the rule is dropped. Pingap retries at
//! once, so a route with `backoff` is not accepted (`UnsupportedValue`), as the
//! Gateway API requires of implementations without backoff.
//!
//! **Timeouts.** `request` and `backendRequest` become the location's connection,
//! read and write timeouts (the shorter of the two when both are set). Pingap's read
//! and write timeouts are per I/O operation, not for the whole exchange: a backend that
//! does not start to answer in time gets a 504, one that keeps sending its body slowly
//! does not. A zero duration ("0s") disables a timeout; Pingap has no "no timeout" for
//! a location, so it gets the longest duration the Gateway API can express.
//!
//! **CORS.** The `cors` plugin with `allow_origins` patterns, so the response carries
//! the request's own `Origin` (or none for an origin that is not allowed; a preflight
//! from one gets a 204 without CORS headers). Pingap cannot echo the requested method
//! or headers: with `allowCredentials`, a `*` in `allowMethods` becomes the list of
//! every method, and a `*` in `allowHeaders` or `exposeHeaders` drops the rule.
//!
//! **Mirrors.** One `mirror` plugin per RequestMirror filter, sending a copy of every
//! request (bodies up to 1 MiB) to the Service's cluster DNS name and port, where
//! kube-proxy picks one endpoint. A mirror backend that is not valid is reported in
//! `ResolvedRefs` and left out, as the Gateway API asks.

use std::net::IpAddr;
use std::sync::Arc;

use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;

use super::api::httproutes::{
    HttpRoute, HttpRouteRules, HttpRouteRulesFiltersCors, HttpRouteRulesFiltersRequestMirror,
};
use super::model::{LocationOptions, PluginEntry, RouteKey, TranslateInput};
use super::translate::{
    APP_PROTOCOLS, KIND_HTTP_ROUTE, KIND_SERVICE, granted, plugin, service_port, valid_host,
};
use crate::types::ServiceKey;

/// The longest Gateway API duration ("99999h" four times), for a timeout of zero.
pub const LONGEST_TIMEOUT_MS: u64 = 4 * 99_999 * 3_600_000;

/// Largest request body a mirror copies; a request with a larger one is not mirrored.
pub const MIRROR_MAX_BODY_SIZE: &str = "1MiB";

/// The methods of the Gateway API (`HTTPMethod`), in its order.
const METHODS: [&str; 9] = [
    "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH",
];

/// The methods a mirror copies: every method a proxied request can have.
const MIRROR_METHODS: [&str; 8] = [
    "GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "TRACE", "PATCH",
];

/// A Gateway API duration (GEP-2257: one to four `<1-5 digits><h|m|s|ms>`) in
/// milliseconds.
pub fn parse_duration(value: &str) -> Result<u64, String> {
    let invalid = || format!("duration {value:?} is not a Gateway API duration");
    let mut rest = value;
    let mut total: u64 = 0;
    let mut groups = 0;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if !(1..=5).contains(&digits) {
            return Err(invalid());
        }
        let number: u64 = rest[..digits].parse().map_err(|_| invalid())?;
        rest = &rest[digits..];
        let (unit, len) = if rest.starts_with("ms") {
            (1, 2)
        } else if rest.starts_with('h') {
            (3_600_000, 1)
        } else if rest.starts_with('m') {
            (60_000, 1)
        } else if rest.starts_with('s') {
            (1_000, 1)
        } else {
            return Err(invalid());
        };
        rest = &rest[len..];
        total += number * unit;
        groups += 1;
    }
    if groups == 0 || groups > 4 {
        return Err(invalid());
    }
    Ok(total)
}

/// Why a route is not accepted as a whole: a rule with `retry.backoff`. "Implementations
/// which retry immediately with no backoff ... MUST set the Accepted Condition for the
/// Route to `status: False` with a Reason of `UnsupportedValue` when this field is set."
pub fn route_rejection(route: &HttpRoute) -> Option<String> {
    let rules: Vec<usize> = route
        .spec
        .rules
        .iter()
        .flatten()
        .enumerate()
        .filter(|(_, r)| r.retry.as_ref().is_some_and(|r| r.backoff.is_some()))
        .map(|(i, _)| i)
        .collect();
    if rules.is_empty() {
        return None;
    }
    let list: Vec<String> = rules.iter().map(|i| format!("rule {i}")).collect();
    Some(format!(
        "retry.backoff is not supported (Pingap retries at once): {}",
        list.join(", ")
    ))
}

/// The Pingap location settings of the `retry` and `timeouts` of a rule.
pub fn location_options(rule: &HttpRouteRules) -> Result<LocationOptions, String> {
    let mut out = LocationOptions::default();
    let timeouts = rule.timeouts.as_ref();
    let request = timeouts
        .and_then(|t| t.request.as_deref())
        .map(parse_duration)
        .transpose()?;
    let backend = timeouts
        .and_then(|t| t.backend_request.as_deref())
        .map(parse_duration)
        .transpose()?;
    // The tighter of the two that are set; zero disables one.
    let set: Vec<u64> = [request, backend].into_iter().flatten().collect();
    if !set.is_empty() {
        let timeout = set
            .iter()
            .copied()
            .filter(|t| *t > 0)
            .min()
            .unwrap_or(LONGEST_TIMEOUT_MS);
        out.connection_timeout = Some(timeout);
        out.read_timeout = Some(timeout);
        out.write_timeout = Some(timeout);
    }
    if let Some(retry) = rule.retry.as_ref() {
        if retry.codes.as_ref().is_some_and(|c| !c.is_empty()) {
            return Err(
                "retry.codes is not supported (Pingap retries only connections that fail)".into(),
            );
        }
        let attempts = retry.attempts.unwrap_or(1).clamp(0, i64::from(u8::MAX));
        if attempts > 0 {
            out.max_retries = u8::try_from(attempts).ok();
            out.max_retry_window = request.filter(|r| *r > 0);
        }
    }
    Ok(out)
}

/// The `cors` plugin of a CORS filter; `None` when no origin is allowed (nothing to
/// add to any response).
pub fn cors_plugin(
    cors: &HttpRouteRulesFiltersCors,
) -> Result<Option<(String, PluginEntry)>, String> {
    let credentials = cors.allow_credentials.unwrap_or(false);
    let origins = cors.allow_origins.as_deref().unwrap_or_default();
    if origins.is_empty() {
        return Ok(None);
    }
    let mut patterns: Vec<String> = Vec::new();
    if origins.iter().any(|o| o == "*") {
        patterns.push("~.+".into());
    } else {
        for origin in origins {
            let pattern = origin_pattern(origin)?;
            if !patterns.contains(&pattern) {
                patterns.push(pattern);
            }
        }
    }

    let mut methods: Vec<&str> = Vec::new();
    for method in cors.allow_methods.iter().flatten() {
        let method = method.as_str();
        if method != "*" && !METHODS.contains(&method) {
            return Err(format!("CORS method {method:?} is not supported"));
        }
        if !methods.contains(&method) {
            methods.push(method);
        }
    }
    let allow_methods = if methods.contains(&"*") {
        // With credentials a browser takes `*` for a method named "*".
        if credentials {
            METHODS.join(", ")
        } else {
            "*".to_string()
        }
    } else if methods.is_empty() {
        // Pingap always sends the header: the CORS-safelisted methods, which are allowed
        // without it.
        "GET, HEAD, POST".to_string()
    } else {
        methods.join(", ")
    };

    let header_list = |names: Option<&Vec<String>>, field: &str| -> Result<String, String> {
        let mut out: Vec<String> = Vec::new();
        for name in names.into_iter().flatten() {
            if name == "*" {
                if credentials {
                    return Err(format!(
                        "CORS {field} \"*\" with allowCredentials is not supported"
                    ));
                }
            } else {
                super::translate::check_token(name, "CORS header name")?;
            }
            if !out.iter().any(|n| n.eq_ignore_ascii_case(name)) {
                out.push(name.clone());
            }
        }
        if out.iter().any(|n| n == "*") {
            return Ok("*".into());
        }
        Ok(out.join(", "))
    };
    let allow_headers = header_list(cors.allow_headers.as_ref(), "allowHeaders")?;
    let expose_headers = header_list(cors.expose_headers.as_ref(), "exposeHeaders")?;
    let max_age = cors.max_age.unwrap_or(5);
    if max_age < 1 {
        return Err(format!("CORS maxAge {max_age} is not supported"));
    }

    let mut fields = toml::Table::new();
    if credentials {
        fields.insert("allow_credentials".into(), toml::Value::Boolean(true));
    }
    if !allow_headers.is_empty() {
        fields.insert("allow_headers".into(), toml::Value::String(allow_headers));
    }
    fields.insert("allow_methods".into(), toml::Value::String(allow_methods));
    fields.insert(
        "allow_origins".into(),
        toml::Value::Array(patterns.into_iter().map(toml::Value::String).collect()),
    );
    if !expose_headers.is_empty() {
        fields.insert("expose_headers".into(), toml::Value::String(expose_headers));
    }
    fields.insert("max_age".into(), toml::Value::String(format!("{max_age}s")));
    Ok(Some(plugin("cors", fields)))
}

/// A Gateway API CORS origin (`<scheme>://<host>[:<port>]`, the host possibly `*` or
/// starting with `*.`) as a Pingap `allow_origins` pattern: `~` and a regular
/// expression Pingap matches against the whole `Origin`, ignoring case. `*` takes any
/// number of labels; a missing port and the default port of the scheme are the same.
fn origin_pattern(origin: &str) -> Result<String, String> {
    let invalid = || format!("CORS origin {origin:?} is not supported");
    let (scheme, rest) = origin.split_once("://").ok_or_else(invalid)?;
    let scheme = scheme.to_ascii_lowercase();
    let default_port = match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => return Err(invalid()),
    };
    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) => {
            let port: u16 = port.parse().ok().filter(|p| *p != 0).ok_or_else(invalid)?;
            (host, port)
        }
        None => (rest, default_port),
    };
    let host = host.to_ascii_lowercase();
    let label = "[0-9a-z-]+";
    let labels = format!("{label}(\\.{label})*");
    let host_pattern = if host == "*" {
        labels
    } else {
        let (prefix, exact) = match host.strip_prefix("*.") {
            Some(rest) => (format!("{labels}\\."), rest),
            None => (String::new(), host.as_str()),
        };
        let valid = exact
            .split('.')
            .all(|l| !l.is_empty() && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-'));
        if !valid {
            return Err(invalid());
        }
        format!("{prefix}{}", regex::escape(exact))
    };
    let port_pattern = if port == default_port {
        format!("(:{port})?")
    } else {
        format!(":{port}")
    };
    Ok(format!("~(?i){scheme}://{host_pattern}{port_pattern}"))
}

/// The share of the requests a RequestMirror filter copies, in percent: `percent`, or
/// `fraction` rounded (never 0 or 100 when it is neither), 100 by default.
pub fn mirror_percent(mirror: &HttpRouteRulesFiltersRequestMirror) -> Result<u8, String> {
    if let Some(fraction) = &mirror.fraction {
        let numerator = i64::from(fraction.numerator);
        let denominator = i64::from(fraction.denominator.unwrap_or(100));
        if denominator <= 0 || numerator < 0 || numerator > denominator {
            return Err(format!(
                "mirror fraction {numerator}/{denominator} is not valid"
            ));
        }
        if numerator == 0 || numerator == denominator {
            return Ok(if numerator == 0 { 0 } else { 100 });
        }
        let rounded = (numerator * 100 + denominator / 2) / denominator;
        return Ok(u8::try_from(rounded.clamp(1, 99)).unwrap_or(99));
    }
    match mirror.percent {
        None => Ok(100),
        Some(p) => u8::try_from(p)
            .ok()
            .filter(|p| *p <= 100)
            .ok_or_else(|| format!("mirror percent {p} is not valid")),
    }
}

/// Where the copies of a RequestMirror filter go (`http://host:port`), or the problem
/// for `ResolvedRefs`. A Service is reached by its cluster DNS name and port (a
/// headless one on its numeric target port), an `ExternalName` Service (when enabled)
/// by its external name.
pub fn mirror_target(
    input: &TranslateInput<'_>,
    route: &RouteKey,
    mirror: &HttpRouteRulesFiltersRequestMirror,
) -> Result<String, (&'static str, String)> {
    let b = &mirror.backend_ref;
    let group = b.group.as_deref().unwrap_or("");
    let kind = b.kind.as_deref().unwrap_or(KIND_SERVICE);
    if !group.is_empty() || kind != KIND_SERVICE {
        let shown = if group.is_empty() {
            kind.to_string()
        } else {
            format!("{group}/{kind}")
        };
        return Err((
            "InvalidKind",
            format!("mirror backend {} has the unsupported kind {shown}", b.name),
        ));
    }
    let namespace = b
        .namespace
        .clone()
        .unwrap_or_else(|| route.namespace.clone());
    let key = ServiceKey {
        namespace: namespace.clone(),
        name: b.name.clone(),
    };
    if namespace != route.namespace
        && !granted(
            input.grants,
            &route.namespace,
            KIND_HTTP_ROUTE,
            &namespace,
            KIND_SERVICE,
            &b.name,
        )
    {
        return Err((
            "RefNotPermitted",
            format!("no ReferenceGrant allows a reference to Service {key}"),
        ));
    }
    let Some(port) = b.port else {
        return Err((
            "BackendNotFound",
            format!("mirror backend Service {key} has no port"),
        ));
    };
    let Some(service) = input.services.get(&key).map(Arc::as_ref) else {
        return Err(("BackendNotFound", format!("Service {key} does not exist")));
    };
    let spec = service.spec.as_ref();
    if spec.and_then(|s| s.type_.as_deref()) == Some("ExternalName") {
        if !input.options.external_name {
            return Err((
                "InvalidKind",
                format!("Service {key} is of type ExternalName, which is not enabled"),
            ));
        }
        let host = spec
            .and_then(|s| s.external_name.as_deref())
            .map(|h| h.trim().trim_end_matches('.').to_ascii_lowercase())
            .unwrap_or_default();
        let port = u16::try_from(port).ok().filter(|p| *p != 0);
        let authority = match (host.parse::<IpAddr>(), port) {
            (_, None) => None,
            (Ok(IpAddr::V6(ip)), Some(port)) => Some(format!("[{ip}]:{port}")),
            (Ok(IpAddr::V4(ip)), Some(port)) => Some(format!("{ip}:{port}")),
            (Err(_), Some(port)) => valid_host(&host)
                .ok()
                .filter(|_| !host.starts_with("*."))
                .map(|_| format!("{host}:{port}")),
        };
        return authority.map(|a| format!("http://{a}")).ok_or_else(|| {
            (
                "BackendNotFound",
                format!("ExternalName Service {key} has no usable externalName and port"),
            )
        });
    }
    let Some(service_port) = service_port(service, port) else {
        return Err((
            "BackendNotFound",
            format!("Service {key} has no port {port}"),
        ));
    };
    if let Some(protocol) = service_port.app_protocol.as_deref()
        && !APP_PROTOCOLS.contains(&protocol.to_ascii_lowercase().as_str())
    {
        return Err((
            "UnsupportedProtocol",
            format!("appProtocol {protocol} of Service {key} port {port} is not supported"),
        ));
    }
    // A headless Service's name resolves to the pods, which listen on the target port.
    let headless = spec.and_then(|s| s.cluster_ip.as_deref()) == Some("None");
    let port = if headless {
        match &service_port.target_port {
            None => port,
            Some(IntOrString::Int(p)) => *p,
            Some(IntOrString::String(name)) => {
                return Err((
                    "BackendNotFound",
                    format!(
                        "headless Service {key} port {port} has the named target port {name}, \
                         which a mirror cannot reach"
                    ),
                ));
            }
        }
    } else {
        port
    };
    Ok(format!(
        "http://{}.{}.svc.{}:{port}",
        key.name, key.namespace, input.options.cluster_domain
    ))
}

/// The `mirror` plugin that copies `percent` percent of the requests to `target`.
pub fn mirror_plugin(target: &str, percent: u8) -> (String, PluginEntry) {
    let mut fields = toml::Table::new();
    fields.insert(
        "max_body_size".into(),
        toml::Value::String(MIRROR_MAX_BODY_SIZE.into()),
    );
    fields.insert(
        "methods".into(),
        toml::Value::Array(
            MIRROR_METHODS
                .iter()
                .map(|m| toml::Value::String(m.to_string()))
                .collect(),
        ),
    );
    if percent < 100 {
        fields.insert(
            "percentage".into(),
            toml::Value::Integer(i64::from(percent)),
        );
    }
    fields.insert("target".into(), toml::Value::String(target.to_string()));
    plugin("mirror", fields)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rule(value: serde_json::Value) -> HttpRouteRules {
        serde_json::from_value(value).unwrap()
    }

    fn cors(value: serde_json::Value) -> HttpRouteRulesFiltersCors {
        serde_json::from_value(value).unwrap()
    }

    fn mirror(value: serde_json::Value) -> HttpRouteRulesFiltersRequestMirror {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn durations() {
        for (text, ms) in [
            ("0s", 0),
            ("500ms", 500),
            ("1s", 1000),
            ("1h30m", 5_400_000),
            ("2m1s5ms", 121_005),
            ("99999h", 99_999 * 3_600_000),
            ("1h1m1s1ms", 3_661_001),
        ] {
            assert_eq!(parse_duration(text), Ok(ms), "{text}");
        }
        for text in [
            "",
            "1",
            "s",
            "1.5s",
            "-1s",
            "100000s",
            "1d",
            "1h1m1s1ms1h",
            " 1s",
            "1S",
        ] {
            assert!(parse_duration(text).is_err(), "{text}");
        }
        assert_eq!(
            parse_duration("99999h99999h99999h99999h"),
            Ok(LONGEST_TIMEOUT_MS)
        );
    }

    #[test]
    fn timeouts_become_location_timeouts() {
        let options = |timeouts: serde_json::Value| {
            location_options(&rule(json!({"timeouts": timeouts}))).unwrap()
        };
        let all = |ms: u64| LocationOptions {
            connection_timeout: Some(ms),
            read_timeout: Some(ms),
            write_timeout: Some(ms),
            ..Default::default()
        };
        assert_eq!(options(json!({})), LocationOptions::default());
        assert_eq!(options(json!({"request": "500ms"})), all(500));
        assert_eq!(options(json!({"backendRequest": "2s"})), all(2000));
        assert_eq!(
            options(json!({"request": "10s", "backendRequest": "2s"})),
            all(2000)
        );
        // Zero disables one of them: the other counts, or the longest of all.
        assert_eq!(
            options(json!({"request": "10s", "backendRequest": "0s"})),
            all(10_000)
        );
        assert_eq!(
            options(json!({"request": "0s", "backendRequest": "1s"})),
            all(1000)
        );
        assert_eq!(options(json!({"request": "0s"})), all(LONGEST_TIMEOUT_MS));
        assert_eq!(
            options(json!({"backendRequest": "0ms"})),
            all(LONGEST_TIMEOUT_MS)
        );
        assert!(location_options(&rule(json!({"timeouts": {"request": "1x"}}))).is_err());
    }

    #[test]
    fn retries_become_max_retries() {
        let options = |value: serde_json::Value| location_options(&rule(value));
        let o = options(json!({"retry": {}})).unwrap();
        assert_eq!((o.max_retries, o.max_retry_window), (Some(1), None));
        let o = options(json!({"retry": {"attempts": 3}, "timeouts": {"request": "5s"}})).unwrap();
        assert_eq!((o.max_retries, o.max_retry_window), (Some(3), Some(5000)));
        let o = options(json!({"retry": {"attempts": 3}, "timeouts": {"request": "0s"}})).unwrap();
        assert_eq!((o.max_retries, o.max_retry_window), (Some(3), None));
        let o = options(json!({"retry": {"attempts": 1000}})).unwrap();
        assert_eq!(o.max_retries, Some(255));
        let o = options(json!({"retry": {"attempts": 0}, "timeouts": {"request": "5s"}})).unwrap();
        assert_eq!((o.max_retries, o.max_retry_window), (None, None));
        let o = options(json!({"retry": {"attempts": 2, "codes": []}})).unwrap();
        assert_eq!(o.max_retries, Some(2));
        let e = options(json!({"retry": {"attempts": 2, "codes": [503]}})).unwrap_err();
        assert!(e.contains("retry.codes"), "{e}");
    }

    #[test]
    fn backoff_rejects_the_route() {
        let route = |rules: serde_json::Value| -> HttpRoute {
            serde_json::from_value(json!({
                "apiVersion": "gateway.networking.k8s.io/v1",
                "kind": "HTTPRoute",
                "metadata": {"namespace": "ns", "name": "r"},
                "spec": {"rules": rules},
            }))
            .unwrap()
        };
        assert_eq!(
            route_rejection(&route(json!([{"retry": {"attempts": 2}}]))),
            None
        );
        let message = route_rejection(&route(json!([
            {"retry": {"attempts": 2}},
            {"retry": {"attempts": 2, "backoff": "100ms"}},
        ])))
        .unwrap();
        assert!(
            message.contains("retry.backoff") && message.ends_with("rule 1"),
            "{message}"
        );
    }

    fn origins(fields: &toml::Table) -> Vec<String> {
        fields["allow_origins"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    }

    /// Whether Pingap would let `origin` in with `pattern` (it anchors the pattern).
    fn lets_in(pattern: &str, origin: &str) -> bool {
        let pattern = pattern.strip_prefix('~').unwrap();
        regex::Regex::new(&format!("^(?:{pattern})$"))
            .unwrap()
            .is_match(origin)
    }

    #[test]
    fn cors_origins() {
        let (_, p) = cors_plugin(&cors(json!({
            "allowOrigins": ["https://www.foo.com", "https://*.bar.com", "http://a.test:8080"]
        })))
        .unwrap()
        .unwrap();
        assert_eq!(p.category, "cors");
        let o = origins(&p.fields);
        assert_eq!(o.len(), 3);
        assert!(lets_in(&o[0], "https://www.foo.com"));
        assert!(lets_in(&o[0], "https://WWW.foo.com:443"));
        assert!(!lets_in(&o[0], "http://www.foo.com"));
        assert!(!lets_in(&o[0], "https://www.foo.com:8443"));
        assert!(!lets_in(&o[0], "https://wwwxfoo.com"));
        assert!(!lets_in(&o[0], "https://www.foo.com.evil.net"));
        assert!(lets_in(&o[1], "https://www.bar.com"));
        assert!(lets_in(&o[1], "https://xpto.www.bar.com"));
        assert!(!lets_in(&o[1], "https://bar.com"));
        assert!(!lets_in(&o[1], "https://foobar.com"));
        assert!(!lets_in(&o[1], "https://evil.com/.bar.com"));
        assert!(lets_in(&o[2], "http://a.test:8080"));
        assert!(!lets_in(&o[2], "http://a.test"));

        let (_, p) = cors_plugin(&cors(json!({"allowOrigins": ["https://*"]})))
            .unwrap()
            .unwrap();
        let o = origins(&p.fields);
        assert!(lets_in(&o[0], "https://any.example"));
        assert!(!lets_in(&o[0], "http://any.example"));

        let (_, p) = cors_plugin(&cors(json!({"allowOrigins": ["*"]})))
            .unwrap()
            .unwrap();
        let o = origins(&p.fields);
        assert_eq!(o, vec!["~.+"]);
        assert!(lets_in(&o[0], "https://foobar.com:12345"));

        assert_eq!(cors_plugin(&cors(json!({}))).unwrap(), None);
        for bad in [
            "ftp://a.test",
            "https://a.test:0",
            "https://a.test:x",
            "https://a..b",
            "a.test",
        ] {
            assert!(
                cors_plugin(&cors(json!({"allowOrigins": [bad]}))).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn cors_fields() {
        let (_, p) = cors_plugin(&cors(json!({
            "allowOrigins": ["https://www.foo.com"],
            "allowMethods": ["GET", "OPTIONS"],
            "allowHeaders": ["x-header-1", "x-header-2"],
            "exposeHeaders": ["x-header-3", "x-header-4"],
            "allowCredentials": true,
            "maxAge": 3600,
        })))
        .unwrap()
        .unwrap();
        let f = &p.fields;
        assert_eq!(f["allow_methods"].as_str(), Some("GET, OPTIONS"));
        assert_eq!(f["allow_headers"].as_str(), Some("x-header-1, x-header-2"));
        assert_eq!(f["expose_headers"].as_str(), Some("x-header-3, x-header-4"));
        assert_eq!(f["allow_credentials"].as_bool(), Some(true));
        assert_eq!(f["max_age"].as_str(), Some("3600s"));

        // Defaults: safelisted methods, 5 seconds, no credentials, no header lists.
        let (_, p) = cors_plugin(&cors(json!({"allowOrigins": ["*"]})))
            .unwrap()
            .unwrap();
        let f = &p.fields;
        assert_eq!(f["allow_methods"].as_str(), Some("GET, HEAD, POST"));
        assert_eq!(f["max_age"].as_str(), Some("5s"));
        assert!(!f.contains_key("allow_credentials"));
        assert!(!f.contains_key("allow_headers"));
        assert!(!f.contains_key("expose_headers"));

        // Wildcards without credentials stay wildcards.
        let (_, p) = cors_plugin(&cors(json!({
            "allowOrigins": ["*"], "allowMethods": ["*"], "allowHeaders": ["*"], "exposeHeaders": ["*"],
        })))
        .unwrap()
        .unwrap();
        let f = &p.fields;
        assert_eq!(f["allow_methods"].as_str(), Some("*"));
        assert_eq!(f["allow_headers"].as_str(), Some("*"));
        assert_eq!(f["expose_headers"].as_str(), Some("*"));

        // With credentials: every method; header wildcards cannot be honored.
        let (_, p) = cors_plugin(&cors(json!({
            "allowOrigins": ["*"], "allowMethods": ["*"], "allowCredentials": true,
        })))
        .unwrap()
        .unwrap();
        assert_eq!(
            p.fields["allow_methods"].as_str(),
            Some("GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH")
        );
        for field in ["allowHeaders", "exposeHeaders"] {
            let e = cors_plugin(&cors(json!({
                "allowOrigins": ["*"], field: ["*"], "allowCredentials": true,
            })))
            .unwrap_err();
            assert!(e.contains(field), "{e}");
        }
        assert!(
            cors_plugin(&cors(
                json!({"allowOrigins": ["*"], "allowMethods": ["BREW"]})
            ))
            .is_err()
        );
        assert!(
            cors_plugin(&cors(
                json!({"allowOrigins": ["*"], "allowHeaders": ["a b"]})
            ))
            .is_err()
        );
        assert!(cors_plugin(&cors(json!({"allowOrigins": ["*"], "maxAge": 0}))).is_err());
    }

    #[test]
    fn mirror_percentages() {
        let percent = |v: serde_json::Value| {
            let mut m = json!({"backendRef": {"name": "v2", "port": 80}});
            for (k, v) in v.as_object().unwrap() {
                m[k] = v.clone();
            }
            mirror_percent(&mirror(m))
        };
        assert_eq!(percent(json!({})), Ok(100));
        assert_eq!(percent(json!({"percent": 20})), Ok(20));
        assert_eq!(percent(json!({"percent": 0})), Ok(0));
        assert_eq!(
            percent(json!({"fraction": {"numerator": 25, "denominator": 50}})),
            Ok(50)
        );
        assert_eq!(
            percent(json!({"fraction": {"numerator": 1, "denominator": 3}})),
            Ok(33)
        );
        assert_eq!(
            percent(json!({"fraction": {"numerator": 2, "denominator": 3}})),
            Ok(67)
        );
        assert_eq!(
            percent(json!({"fraction": {"numerator": 1, "denominator": 1000}})),
            Ok(1)
        );
        assert_eq!(
            percent(json!({"fraction": {"numerator": 999, "denominator": 1000}})),
            Ok(99)
        );
        assert_eq!(
            percent(json!({"fraction": {"numerator": 7, "denominator": 7}})),
            Ok(100)
        );
        assert_eq!(percent(json!({"fraction": {"numerator": 30}})), Ok(30));
        assert!(percent(json!({"percent": 101})).is_err());
        assert!(percent(json!({"fraction": {"numerator": 2, "denominator": 1}})).is_err());
        assert!(percent(json!({"fraction": {"numerator": 1, "denominator": 0}})).is_err());
    }

    #[test]
    fn mirror_plugin_fields() {
        let (name, p) = mirror_plugin("http://v2.ns.svc.cluster.local:80", 35);
        assert!(name.starts_with("k8s-mirror-"), "{name}");
        assert_eq!(p.category, "mirror");
        assert_eq!(
            p.fields["target"].as_str(),
            Some("http://v2.ns.svc.cluster.local:80")
        );
        assert_eq!(p.fields["percentage"].as_integer(), Some(35));
        assert_eq!(p.fields["max_body_size"].as_str(), Some("1MiB"));
        assert_eq!(p.fields["methods"].as_array().unwrap().len(), 8);
        let (other, p) = mirror_plugin("http://v2.ns.svc.cluster.local:80", 100);
        assert!(!p.fields.contains_key("percentage"));
        assert_ne!(name, other);
    }
}

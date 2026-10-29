//! Gateways and HTTPRoutes into the Pingap entries they want and the statuses they get
//! (pure, no I/O).
//!
//! **Servers.** Pingap servers cannot be created without a restart, so a listener is
//! programmed on every stored server (limited to `servers` of the options when set)
//! whose ports contain the listener port and whose TLS mode matches its protocol (HTTPS
//! on `global_certificates` servers, HTTP on the others). Gateways of our classes share
//! those servers: they are merged onto them, so listeners of different Gateways with
//! the same port and hostname are all programmed and their routes served side by side
//! (precedence across them as across routes of one Gateway). Inside one Gateway,
//! listeners that are not distinct are all conflicted. HTTPS listeners of different
//! Gateways with the same hostname and different certificates leave the choice of the
//! certificate to Pingap.
//!
//! **Locations.** Every (server, host, route match) becomes one location with one host.
//! Gateway API matching maps onto Pingap's matchers:
//!
//! - `PathPrefix /` -> no path; `PathPrefix /foo` and `/foo/` -> `~^/foo(/|$)` (Pingap's
//!   plain prefix is not segment aware); `Exact /a` -> `=/a`; `RegularExpression re` ->
//!   `~^(?:re)$` (the `regex` crate dialect, anchored here).
//! - Exact header and query matches -> `match_headers` / `match_query`. Regular
//!   expression header and query matches and method matches are not supported.
//!
//! Pingap tries the locations of a server by weight, highest first, and the first full
//! match is final. Every generated location gets an explicit weight from the Gateway
//! API precedence: hostname (longer exact host, then longer host, so exact > longer
//! wildcard > shorter wildcard > any host), then Exact path > longer prefix > regular
//! expression (longer first), then more header matches, then more query matches, then
//! the older route, then `namespace/name`, then rule and match order. The locations of
//! one server get [`WEIGHT_TOP`], `WEIGHT_TOP - 1`, ... in that order, never below
//! [`WEIGHT_FLOOR`]. Pingap's own weights of hand-made locations stay far below (at most
//! 1216 without regular expression hosts), so generated locations are always tried
//! first.
//!
//! **Invalid rules.** A rule that uses something not supported (a filter, a match type,
//! a value Pingap cannot express) is dropped and the route gets `PartiallyInvalid` with
//! a message starting with "Dropped Rule"; when every rule is dropped the route is not
//! accepted (`UnsupportedValue`). Invalid backends get a 500 for their share of the
//! traffic: a location without upstream and a `mock` plugin with status 500 when all
//! of them are invalid, or a `mock` plugin with `percentage` = the invalid share in
//! front of the valid ones (requests it does not pick go on to the upstream).
//!
//! **Filters.** RequestHeaderModifier and ResponseHeaderModifier -> the
//! `request_headers` / `response_headers` plugins (a Host header set or added goes to
//! the location's `proxy_set_headers`, as the plugin refuses Host); RequestRedirect ->
//! the `redirect` plugin; URLRewrite -> the location's `rewrite` and a Host in
//! `proxy_set_headers`. Header values that start with `$` or `:` are refused, because
//! Pingap replaces them with variables (`$NAME` with environment variables of the Pingap
//! process). A redirect keeps the query, except one that needs a whole URL as target
//! (from HTTPS to HTTP, or to a port other than the one Pingap would pick for the
//! target host), where Pingap leaves the query out. A whole URL to the host of the
//! request on an HTTPS listener takes the request authority as the client sent it, so
//! on a listener port other than 443 it may carry that port.
//!
//! Listener isolation is not implemented: a request for `a.example.com` that matches no
//! route of an `a.example.com` listener can match a route of a `*.example.com` listener
//! on the same server.

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;

use k8s_openapi::api::core::v1::Service;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;
use regex::Regex;

use crate::config::UpstreamConfig;
use crate::types::{PortRef, ServiceKey, ServiceRef, UpstreamTarget};

use super::api::gatewayclasses::{
    GatewayClass, GatewayClassStatus, GatewayClassStatusSupportedFeatures,
};
use super::api::gateways::{
    Gateway, GatewayListeners, GatewayListenersAllowedRoutesNamespacesFrom,
    GatewayListenersAllowedRoutesNamespacesSelector, GatewayListenersTlsMode, GatewayStatus,
    GatewayStatusAddresses, GatewayStatusListeners, GatewayStatusListenersSupportedKinds,
};
use super::api::httproutes::{
    HttpRoute, HttpRouteParentRefs, HttpRouteRules, HttpRouteRulesFilters,
    HttpRouteRulesFiltersRequestRedirectPathType, HttpRouteRulesFiltersRequestRedirectScheme,
    HttpRouteRulesFiltersType, HttpRouteRulesFiltersUrlRewritePathType,
    HttpRouteRulesMatchesHeadersType, HttpRouteRulesMatchesPathType,
    HttpRouteRulesMatchesQueryParamsType, HttpRouteStatus, HttpRouteStatusParents,
    HttpRouteStatusParentsParentRef,
};
use super::api::referencegrants::ReferenceGrant;
use super::model::{
    BackendKind, CertificateEntry, Desired, ExternalUpstream, GATEWAY_GROUP, GatewayKey,
    LocationEntry, ObjectKey, PluginEntry, Referenced, RouteKey, SecretKey, ServerInfo,
    TranslateInput,
};
use super::names;
use super::tls;

/// Weight of the first generated location of a server.
pub const WEIGHT_TOP: u16 = 65000;
/// No generated location gets less (more than 62952 locations on one server share it).
pub const WEIGHT_FLOOR: u16 = 2048;

pub const KIND_GATEWAY: &str = "Gateway";
pub const KIND_HTTP_ROUTE: &str = "HTTPRoute";
pub const KIND_SERVICE: &str = "Service";
pub const KIND_SECRET: &str = "Secret";

/// The features of the Gateway API conformance suite this provider implements, sorted
/// as `status.supportedFeatures` must be.
pub const SUPPORTED_FEATURES: [&str; 15] = [
    "Gateway",
    "HTTPRoute",
    "HTTPRoute303RedirectStatusCode",
    "HTTPRoute307RedirectStatusCode",
    "HTTPRoute308RedirectStatusCode",
    "HTTPRouteHostRewrite",
    "HTTPRouteNamedRouteRule",
    "HTTPRouteParentRefPort",
    "HTTPRoutePathRedirect",
    "HTTPRoutePathRewrite",
    "HTTPRoutePortRedirect",
    "HTTPRouteQueryParamMatching",
    "HTTPRouteResponseHeaderModification",
    "HTTPRouteSchemeRedirect",
    "ReferenceGrant",
];

/// Service `appProtocol` values the generated (HTTP/1.1, plain text) upstreams can
/// speak; anything else is `UnsupportedProtocol`.
const APP_PROTOCOLS: [&str; 4] = ["http", "kubernetes.io/http", "ws", "kubernetes.io/ws"];

/// A lowercase DNS name, optionally with a leading `*.` label: labels of 1 to 63
/// `[a-z0-9-]` not starting or ending with `-`, at most 253 characters.
pub fn valid_host(host: &str) -> Result<(), String> {
    let name = host.strip_prefix("*.").unwrap_or(host);
    if name.is_empty() {
        return Err("must not be empty".into());
    }
    if name.len() > 253 {
        return Err("must be at most 253 characters".into());
    }
    for label in name.split('.') {
        if label.is_empty() || label.len() > 63 {
            return Err("every label must be 1 to 63 characters".into());
        }
        if !label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return Err("must contain only a-z, 0-9, '-' and '.' (and a leading '*.')".into());
        }
        if label.starts_with('-') || label.ends_with('-') {
            return Err("labels must not start or end with '-'".into());
        }
    }
    Ok(())
}

/// The Services named by the backendRefs of every route with a parent of ours (allowed
/// or not) and the Secrets named by the HTTPS listeners of our Gateways.
pub fn referenced(input: &TranslateInput<'_>) -> Referenced {
    let mut out = Referenced::default();
    let gateways = our_gateways(input);
    let keys: BTreeSet<GatewayKey> = gateways.iter().map(|g| object_key(&g.metadata)).collect();
    for gateway in &gateways {
        let namespace = gateway.metadata.namespace.clone().unwrap_or_default();
        for listener in &gateway.spec.listeners {
            if listener.protocol != "HTTPS" {
                continue;
            }
            let refs = listener
                .tls
                .as_ref()
                .and_then(|t| t.certificate_refs.as_ref());
            for r in refs.into_iter().flatten() {
                if r.group.as_deref().unwrap_or("").is_empty()
                    && r.kind.as_deref().unwrap_or(KIND_SECRET) == KIND_SECRET
                {
                    out.secrets.insert(SecretKey::new(
                        r.namespace.clone().unwrap_or_else(|| namespace.clone()),
                        r.name.clone(),
                    ));
                }
            }
        }
    }
    for route in input.routes.iter() {
        let namespace = route.metadata.namespace.clone().unwrap_or_default();
        let ours = parent_refs(route)
            .iter()
            .any(|p| gateway_ref_key(p, &namespace).is_some_and(|k| keys.contains(&k)));
        if !ours {
            continue;
        }
        for rule in route.spec.rules.iter().flatten() {
            for b in rule.backend_refs.iter().flatten() {
                if b.group.as_deref().unwrap_or("").is_empty()
                    && b.kind.as_deref().unwrap_or(KIND_SERVICE) == KIND_SERVICE
                {
                    out.services.insert(ServiceKey {
                        namespace: b.namespace.clone().unwrap_or_else(|| namespace.clone()),
                        name: b.name.clone(),
                    });
                }
            }
        }
    }
    out
}

/// Everything the Gateways and routes of our classes want stored, their statuses and
/// what they refer to.
pub fn translate(input: &TranslateInput<'_>) -> Desired {
    let options = input.options;
    let mut desired = Desired {
        referenced: referenced(input),
        ..Default::default()
    };

    // ---- GatewayClasses ----
    for class in input.classes.iter() {
        if class.spec.controller_name != options.controller_name {
            continue;
        }
        let name = class.metadata.name.clone().unwrap_or_default();
        desired.status.classes.insert(name, class_status(class));
    }

    // ---- Gateways and listeners ----
    let mut gateways = our_gateways(input);
    gateways.sort_by_cached_key(|g| age_key(&g.metadata));
    let eligible: Vec<&ServerInfo> = input
        .servers
        .iter()
        .filter(|s| options.servers.is_empty() || options.servers.contains(&s.name))
        .collect();
    for server in &eligible {
        desired
            .server_locations
            .entry(server.name.clone())
            .or_default();
    }

    let mut listeners: Vec<Listener<'_>> = Vec::new();
    let mut gateway_listeners: Vec<Vec<usize>> = Vec::new();
    for gateway in &gateways {
        let mut indices = Vec::new();
        for spec in &gateway.spec.listeners {
            indices.push(listeners.len());
            listeners.push(evaluate_listener(input, gateway, spec, &eligible));
        }
        mark_conflicts(&mut listeners, &indices);
        gateway_listeners.push(indices);
    }

    // ---- Routes ----
    let gateway_index: BTreeMap<GatewayKey, usize> = gateways
        .iter()
        .enumerate()
        .map(|(i, g)| (object_key(&g.metadata), i))
        .collect();
    let mut routes: Vec<&Arc<HttpRoute>> = input.routes.iter().collect();
    routes.sort_by_cached_key(|r| age_key(&r.metadata));

    // Upstream name -> what it is, for every rule that has one.
    let mut upstreams: BTreeMap<String, UpstreamKind> = BTreeMap::new();
    // Server -> location candidates, before weights.
    let mut candidates: BTreeMap<String, Vec<Candidate>> = BTreeMap::new();
    for (age, route) in routes.iter().enumerate() {
        let key = object_key(&route.metadata);
        let generation = route.metadata.generation;
        let mut parents: Vec<(HttpRouteStatusParentsParentRef, Attached)> = Vec::new();
        for parent in parent_refs(route) {
            let Some(gw_key) = gateway_ref_key(parent, &key.namespace) else {
                continue;
            };
            let Some(&gi) = gateway_index.get(&gw_key) else {
                continue;
            };
            let outcome = attach(input, route, parent, &gateway_listeners[gi], &listeners);
            parents.push((status_parent_ref(parent), outcome));
        }
        if parents.is_empty() {
            continue;
        }

        let compiled: Vec<Result<Rule, String>> = route
            .spec
            .rules
            .iter()
            .flatten()
            .enumerate()
            .map(|(i, rule)| compile_rule(input, &key, i, rule))
            .collect();
        let mut issues: Vec<(&'static str, String)> = Vec::new();
        for rule in compiled.iter().flatten() {
            issues.extend(rule.backend_issues.iter().cloned());
        }
        // A rule that is dropped still has its backends reported.
        for (i, rule) in route.spec.rules.iter().flatten().enumerate() {
            if compiled[i].is_err() {
                issues.extend(resolve_backends(input, &key, rule).issues);
            }
        }
        let dropped: Vec<String> = compiled
            .iter()
            .enumerate()
            .filter_map(|(i, r)| r.as_ref().err().map(|e| format!("rule {i}: {e}")))
            .collect();
        let all_dropped = !compiled.is_empty() && dropped.len() == compiled.len();

        let mut status_parents = Vec::new();
        for (parent_ref, outcome) in &parents {
            let accepted = match outcome {
                Err((reason, message)) => cond("Accepted", false, reason, message, generation),
                Ok(_) if all_dropped => cond(
                    "Accepted",
                    false,
                    "UnsupportedValue",
                    &format!("Every rule is invalid: {}", dropped.join("; ")),
                    generation,
                ),
                Ok(_) => cond(
                    "Accepted",
                    true,
                    "Accepted",
                    "Route is accepted",
                    generation,
                ),
            };
            let resolved = match issues.first() {
                None => cond(
                    "ResolvedRefs",
                    true,
                    "ResolvedRefs",
                    "All references are resolved",
                    generation,
                ),
                Some((reason, _)) => cond(
                    "ResolvedRefs",
                    false,
                    reason,
                    &dedup(issues.iter().map(|(_, m)| m.clone())).join("; "),
                    generation,
                ),
            };
            let mut conditions = vec![accepted, resolved];
            if !dropped.is_empty() && !all_dropped && outcome.is_ok() {
                conditions.push(cond(
                    "PartiallyInvalid",
                    true,
                    "UnsupportedValue",
                    &format!("Dropped Rule(s): {}", dropped.join("; ")),
                    generation,
                ));
            }
            status_parents.push(HttpRouteStatusParents {
                conditions,
                controller_name: options.controller_name.clone(),
                parent_ref: parent_ref.clone(),
            });
        }
        desired.status.routes.insert(
            key.clone(),
            HttpRouteStatus {
                parents: status_parents,
            },
        );
        if all_dropped {
            continue;
        }

        let hostnames = route_hostnames(route);
        for (_, outcome) in &parents {
            let Ok(attachments) = outcome else { continue };
            for attachment in attachments {
                listeners[attachment.listener].attached.insert(key.clone());
                let listener = &listeners[attachment.listener];
                if !listener.programmed() {
                    continue;
                }
                let hosts = intersect_hostnames(listener.hostname.as_deref(), &hostnames);
                for server in &listener.servers {
                    for host in &hosts {
                        for rule in compiled.iter().flatten() {
                            for m in &rule.matches {
                                let candidate = build_location(
                                    &key,
                                    age,
                                    rule,
                                    m,
                                    server,
                                    host.as_deref(),
                                    listener,
                                );
                                candidates
                                    .entry(server.clone())
                                    .or_default()
                                    .push(candidate);
                            }
                        }
                    }
                }
            }
        }
        for rule in compiled.iter().flatten() {
            if let Some((name, kind)) = &rule.upstream {
                upstreams
                    .entry(name.clone())
                    .or_insert_with(|| kind.clone());
            }
        }
    }

    // ---- Weights ----
    for (server, mut list) in candidates {
        list.sort_by(|a, b| a.order.cmp(&b.order));
        let mut seen = BTreeSet::new();
        let mut names = Vec::new();
        let mut position: u16 = 0;
        for candidate in list {
            if !seen.insert(candidate.name.clone()) {
                continue;
            }
            let mut entry = candidate.entry;
            entry.weight = WEIGHT_TOP.saturating_sub(position).max(WEIGHT_FLOOR);
            position = position.saturating_add(1);
            for (name, plugin) in candidate.plugins {
                desired.plugins.insert(name, plugin);
            }
            // Only the upstreams a location uses.
            match entry.upstream.as_ref().and_then(|u| upstreams.get(u)) {
                Some(UpstreamKind::Generated(config)) => {
                    let name = entry.upstream.clone().unwrap_or_default();
                    desired.upstreams.insert(name, config.clone());
                }
                Some(UpstreamKind::External(external)) => {
                    let name = entry.upstream.clone().unwrap_or_default();
                    desired.external.insert(name, external.clone());
                }
                None => {}
            }
            names.push(candidate.name.clone());
            desired.locations.insert(candidate.name, entry);
        }
        desired.server_locations.insert(server, names);
    }

    // ---- Gateway and listener statuses, certificates ----
    let mut cert_domains: BTreeMap<SecretKey, (Option<BTreeSet<String>>, String, String)> =
        BTreeMap::new();
    for listener in &listeners {
        if !listener.programmed() {
            continue;
        }
        for (secret, cert, key) in &listener.certs {
            let entry = cert_domains
                .entry(secret.clone())
                .or_insert_with(|| (Some(BTreeSet::new()), cert.clone(), key.clone()));
            match (&listener.hostname, &mut entry.0) {
                (Some(host), Some(domains)) => {
                    domains.insert(host.clone());
                }
                (None, domains) => *domains = None,
                (Some(_), None) => {}
            }
        }
    }
    for (secret, (domains, tls_cert, tls_key)) in cert_domains {
        desired.certificates.insert(
            names::certificate_name(&secret),
            CertificateEntry {
                secret,
                domains: domains.map(|d| d.into_iter().collect()).unwrap_or_default(),
                tls_cert,
                tls_key,
            },
        );
    }
    for (gi, gateway) in gateways.iter().enumerate() {
        let status = gateway_status(input, gateway, &gateway_listeners[gi], &listeners);
        desired
            .status
            .gateways
            .insert(object_key(&gateway.metadata), status);
    }
    desired
}

// ---- classes and Gateways ----

fn class_status(class: &GatewayClass) -> GatewayClassStatus {
    let generation = class.metadata.generation;
    let accepted = if class.spec.parameters_ref.is_some() {
        cond(
            "Accepted",
            false,
            "InvalidParameters",
            "parametersRef is not supported by this controller",
            generation,
        )
    } else {
        cond(
            "Accepted",
            true,
            "Accepted",
            "GatewayClass is accepted",
            generation,
        )
    };
    GatewayClassStatus {
        conditions: Some(vec![
            accepted,
            cond(
                "SupportedVersion",
                true,
                "SupportedVersion",
                "Gateway API v1.6 is supported",
                generation,
            ),
        ]),
        supported_features: Some(
            SUPPORTED_FEATURES
                .iter()
                .map(|f| GatewayClassStatusSupportedFeatures {
                    name: f.to_string(),
                })
                .collect(),
        ),
    }
}

/// The Gateways whose class has our `controllerName` and is accepted.
fn our_gateways<'a>(input: &TranslateInput<'a>) -> Vec<&'a Arc<Gateway>> {
    let classes: BTreeSet<&str> = input
        .classes
        .iter()
        .filter(|c| {
            c.spec.controller_name == input.options.controller_name
                && c.spec.parameters_ref.is_none()
        })
        .filter_map(|c| c.metadata.name.as_deref())
        .collect();
    input
        .gateways
        .iter()
        .filter(|g| classes.contains(g.spec.gateway_class_name.as_str()))
        .collect()
}

/// One listener of one of our Gateways, evaluated.
struct Listener<'a> {
    gateway_key: GatewayKey,
    spec: &'a GatewayListeners,
    /// Lowercase; `None` for any host.
    hostname: Option<String>,
    tls: bool,
    /// HTTP or HTTPS.
    supported_protocol: bool,
    servers: Vec<String>,
    supported_kinds: Vec<GatewayStatusListenersSupportedKinds>,
    /// `Err((reason, message))`: not accepted.
    accepted: Result<(), (&'static str, String)>,
    conflicted: Option<(&'static str, String)>,
    resolved: Result<(), (&'static str, String)>,
    /// The usable certificates: Secret, `tls.crt`, `tls.key`.
    certs: Vec<(SecretKey, String, String)>,
    attached: BTreeSet<RouteKey>,
}

impl Listener<'_> {
    /// Routes may attach to it.
    fn attachable(&self) -> bool {
        self.accepted.is_ok() && self.conflicted.is_none() && !self.supported_kinds.is_empty()
    }

    /// Its routes become locations.
    fn programmed(&self) -> bool {
        self.attachable() && (!self.tls || !self.certs.is_empty())
    }
}

fn evaluate_listener<'a>(
    input: &TranslateInput<'_>,
    gateway: &Gateway,
    spec: &'a GatewayListeners,
    servers: &[&ServerInfo],
) -> Listener<'a> {
    let gateway_key = object_key(&gateway.metadata);
    let hostname = spec
        .hostname
        .as_deref()
        .filter(|h| !h.is_empty())
        .map(str::to_ascii_lowercase);
    let tls = spec.protocol == "HTTPS";
    let supported_protocol = tls || spec.protocol == "HTTP";
    let mut listener = Listener {
        gateway_key: gateway_key.clone(),
        spec,
        hostname,
        tls,
        supported_protocol,
        servers: Vec::new(),
        supported_kinds: Vec::new(),
        accepted: Ok(()),
        conflicted: None,
        resolved: Ok(()),
        certs: Vec::new(),
        attached: BTreeSet::new(),
    };
    if !supported_protocol {
        listener.accepted = Err((
            "UnsupportedProtocol",
            format!("protocol {} is not supported", spec.protocol),
        ));
        return listener;
    }

    // Route kinds.
    let kinds = spec.allowed_routes.as_ref().and_then(|a| a.kinds.as_ref());
    match kinds.filter(|k| !k.is_empty()) {
        None => listener.supported_kinds.push(http_route_kind()),
        Some(kinds) => {
            let mut invalid = Vec::new();
            for kind in kinds {
                let group = kind.group.as_deref().unwrap_or(GATEWAY_GROUP);
                if group == GATEWAY_GROUP && kind.kind == KIND_HTTP_ROUTE {
                    if listener.supported_kinds.is_empty() {
                        listener.supported_kinds.push(http_route_kind());
                    }
                } else {
                    invalid.push(format!("{group}/{}", kind.kind));
                }
            }
            if !invalid.is_empty() {
                listener.resolved = Err((
                    "InvalidRouteKinds",
                    format!("route kinds {} are not supported", invalid.join(", ")),
                ));
            }
        }
    }

    if let Some(host) = &listener.hostname
        && let Err(reason) = valid_host(host)
    {
        listener.accepted = Err(("UnsupportedValue", format!("hostname {host:?} {reason}")));
        return listener;
    }

    let port = u16::try_from(spec.port).ok();
    listener.servers = servers
        .iter()
        .filter(|s| port.is_some_and(|p| s.ports.contains(&p)) && s.tls == tls)
        .map(|s| s.name.clone())
        .collect();
    if listener.servers.is_empty() {
        listener.accepted = Err((
            "PortUnavailable",
            format!(
                "no Pingap {} server listens on port {}",
                if tls { "TLS" } else { "plain HTTP" },
                spec.port
            ),
        ));
        return listener;
    }

    if tls {
        let listener_tls = spec.tls.as_ref();
        if listener_tls.and_then(|t| t.mode.as_ref()) == Some(&GatewayListenersTlsMode::Passthrough)
        {
            listener.accepted = Err((
                "UnsupportedValue",
                "TLS passthrough is not supported".into(),
            ));
            return listener;
        }
        let frontend = gateway.spec.tls.as_ref().and_then(|t| t.frontend.as_ref());
        if frontend.is_some_and(|f| {
            f.default.validation.is_some()
                || f.per_port
                    .iter()
                    .flatten()
                    .any(|p| p.port == spec.port && p.tls.validation.is_some())
        }) {
            listener.accepted = Err((
                "UnsupportedValue",
                "client certificate validation is not supported".into(),
            ));
            return listener;
        }
        let refs = listener_tls.and_then(|t| t.certificate_refs.as_ref());
        let mut problem: Option<(&'static str, String)> = None;
        let mut note = |reason: &'static str, message: String| {
            if problem.is_none() {
                problem = Some((reason, message));
            }
        };
        let refs: Vec<_> = refs.into_iter().flatten().collect();
        if refs.is_empty() {
            note("InvalidCertificateRef", "no certificateRefs".into());
        }
        for r in refs {
            let group = r.group.as_deref().unwrap_or("");
            let kind = r.kind.as_deref().unwrap_or(KIND_SECRET);
            if !group.is_empty() || kind != KIND_SECRET {
                note(
                    "InvalidCertificateRef",
                    format!("certificateRef {group}/{kind} {} is not a Secret", r.name),
                );
                continue;
            }
            let secret = SecretKey::new(
                r.namespace
                    .clone()
                    .unwrap_or_else(|| gateway_key.namespace.clone()),
                r.name.clone(),
            );
            if secret.namespace != gateway_key.namespace
                && !granted(
                    input.grants,
                    &gateway_key.namespace,
                    KIND_GATEWAY,
                    &secret.namespace,
                    KIND_SECRET,
                    &secret.name,
                )
            {
                note(
                    "RefNotPermitted",
                    format!("no ReferenceGrant allows a reference to Secret {secret}"),
                );
                continue;
            }
            let Some(object) = input.secrets.get(&secret) else {
                note(
                    "InvalidCertificateRef",
                    format!("Secret {secret} does not exist"),
                );
                continue;
            };
            match tls::certificate_from_secret(object) {
                Ok((cert, key)) => listener.certs.push((secret, cert, key)),
                Err(reason) => note(
                    "InvalidCertificateRef",
                    format!("Secret {secret}: {reason}"),
                ),
            }
        }
        if let Some(problem) = problem
            && listener.resolved.is_ok()
        {
            listener.resolved = Err(problem);
        }
    }
    listener
}

fn http_route_kind() -> GatewayStatusListenersSupportedKinds {
    GatewayStatusListenersSupportedKinds {
        group: Some(GATEWAY_GROUP.into()),
        kind: KIND_HTTP_ROUTE.into(),
    }
}

/// Marks the listeners `indices` (one Gateway) that are not distinct among themselves.
/// Listeners of different Gateways are never conflicted: Gateways sharing a server are
/// merged onto it (the `GatewaySpec.listeners` "MAY merge separate Gateways" case), and
/// their routes are served side by side.
fn mark_conflicts(listeners: &mut [Listener<'_>], indices: &[usize]) {
    let candidates = |l: &Listener<'_>| l.supported_protocol && l.accepted.is_ok();
    let mut found: Vec<(usize, &'static str, String)> = Vec::new();
    for &i in indices {
        let l = &listeners[i];
        if !candidates(l) {
            continue;
        }
        // Inside the Gateway: every listener of the set is conflicted.
        for &j in indices {
            let o = &listeners[j];
            if i == j || !candidates(o) || o.spec.port != l.spec.port {
                continue;
            }
            if o.spec.protocol != l.spec.protocol {
                found.push((
                    i,
                    "ProtocolConflict",
                    format!(
                        "listener {} uses port {} with protocol {}",
                        o.spec.name, o.spec.port, o.spec.protocol
                    ),
                ));
                break;
            }
            if o.hostname == l.hostname {
                found.push((
                    i,
                    "HostnameConflict",
                    format!("listener {} uses the same port and hostname", o.spec.name),
                ));
                break;
            }
        }
    }
    for (i, reason, message) in found {
        if listeners[i].conflicted.is_none() {
            listeners[i].conflicted = Some((reason, message));
        }
    }
}

fn gateway_status(
    input: &TranslateInput<'_>,
    gateway: &Gateway,
    indices: &[usize],
    listeners: &[Listener<'_>],
) -> GatewayStatus {
    let generation = gateway.metadata.generation;
    let ours: Vec<&Listener<'_>> = indices.iter().map(|&i| &listeners[i]).collect();
    let usable = ours
        .iter()
        .filter(|l| l.accepted.is_ok() && l.conflicted.is_none())
        .count();

    // Requested addresses: only those we already serve on can be honored.
    let mut unsupported_address = None;
    let mut unusable_address = None;
    for address in gateway.spec.addresses.iter().flatten() {
        let kind = address.r#type.as_deref().unwrap_or("IPAddress");
        if kind != "IPAddress" && kind != "Hostname" {
            unsupported_address.get_or_insert(format!("address type {kind} is not supported"));
            continue;
        }
        if let Some(value) = address.value.as_deref().filter(|v| !v.is_empty())
            && !input.options.addresses.iter().any(|a| a == value)
        {
            unusable_address.get_or_insert(format!(
                "address {value} is not one of the addresses this controller serves on"
            ));
        }
    }

    let accepted = if let Some(message) = &unsupported_address {
        cond("Accepted", false, "UnsupportedAddress", message, generation)
    } else if usable == 0 {
        cond(
            "Accepted",
            false,
            "ListenersNotValid",
            "No listener is valid",
            generation,
        )
    } else if usable < ours.len() {
        let invalid: Vec<&str> = ours
            .iter()
            .filter(|l| l.accepted.is_err() || l.conflicted.is_some())
            .map(|l| l.spec.name.as_str())
            .collect();
        cond(
            "Accepted",
            true,
            "ListenersNotValid",
            &format!("Listeners not valid: {}", invalid.join(", ")),
            generation,
        )
    } else {
        cond(
            "Accepted",
            true,
            "Accepted",
            "Gateway is accepted",
            generation,
        )
    };
    let is_accepted = accepted.status == "True";
    let programmed = if !is_accepted {
        cond(
            "Programmed",
            false,
            "Invalid",
            "Gateway is not accepted",
            generation,
        )
    } else if let Some(message) = &unusable_address {
        cond("Programmed", false, "AddressNotUsable", message, generation)
    } else if input.options.addresses.is_empty() {
        cond(
            "Programmed",
            false,
            "AddressNotAssigned",
            "No address is configured for this controller",
            generation,
        )
    } else {
        cond(
            "Programmed",
            true,
            "Programmed",
            "Gateway is programmed",
            generation,
        )
    };

    let listener_statuses = ours
        .iter()
        .map(|l| {
            let mut conditions = Vec::new();
            conditions.push(match &l.accepted {
                Ok(()) => match &l.conflicted {
                    None => cond(
                        "Accepted",
                        true,
                        "Accepted",
                        "Listener is accepted",
                        generation,
                    ),
                    Some((reason, message)) => cond("Accepted", false, reason, message, generation),
                },
                Err((reason, message)) => cond("Accepted", false, reason, message, generation),
            });
            conditions.push(match &l.conflicted {
                None => cond(
                    "Conflicted",
                    false,
                    "NoConflicts",
                    "No conflicts",
                    generation,
                ),
                Some((reason, message)) => cond("Conflicted", true, reason, message, generation),
            });
            conditions.push(match &l.resolved {
                Ok(()) => cond(
                    "ResolvedRefs",
                    true,
                    "ResolvedRefs",
                    "All references are resolved",
                    generation,
                ),
                Err((reason, message)) => cond("ResolvedRefs", false, reason, message, generation),
            });
            conditions.push(if l.programmed() {
                cond(
                    "Programmed",
                    true,
                    "Programmed",
                    "Listener is programmed",
                    generation,
                )
            } else {
                let why = match (&l.accepted, &l.conflicted) {
                    (Err((_, m)), _) => m.clone(),
                    (_, Some((_, m))) => m.clone(),
                    _ if l.supported_kinds.is_empty() => "no supported route kind".into(),
                    _ => "no usable certificate".into(),
                };
                cond("Programmed", false, "Invalid", &why, generation)
            });
            GatewayStatusListeners {
                attached_routes: i32::try_from(l.attached.len()).unwrap_or(i32::MAX),
                conditions,
                name: l.spec.name.clone(),
                supported_kinds: Some(l.supported_kinds.clone()),
            }
        })
        .collect();

    let addresses = is_accepted.then(|| {
        input
            .options
            .addresses
            .iter()
            .map(|a| GatewayStatusAddresses {
                r#type: Some(
                    if a.parse::<IpAddr>().is_ok() {
                        "IPAddress"
                    } else {
                        "Hostname"
                    }
                    .into(),
                ),
                value: a.clone(),
            })
            .collect()
    });
    GatewayStatus {
        addresses,
        attached_listener_sets: None,
        conditions: Some(vec![accepted, programmed]),
        listeners: Some(listener_statuses),
    }
}

// ---- route attachment ----

/// A listener a route parentRef attached to.
struct Attachment {
    listener: usize,
}

/// The listeners a parentRef attached to, or why it attached to none.
type Attached = Result<Vec<Attachment>, (&'static str, String)>;

fn parent_refs(route: &HttpRoute) -> &[HttpRouteParentRefs] {
    route.spec.parent_refs.as_deref().unwrap_or_default()
}

/// The Gateway a parentRef names, when it names one.
fn gateway_ref_key(parent: &HttpRouteParentRefs, route_namespace: &str) -> Option<GatewayKey> {
    let group = parent.group.as_deref().unwrap_or(GATEWAY_GROUP);
    let kind = parent.kind.as_deref().unwrap_or(KIND_GATEWAY);
    (group == GATEWAY_GROUP && kind == KIND_GATEWAY).then(|| {
        ObjectKey::new(
            parent
                .namespace
                .clone()
                .unwrap_or_else(|| route_namespace.to_string()),
            parent.name.clone(),
        )
    })
}

fn status_parent_ref(parent: &HttpRouteParentRefs) -> HttpRouteStatusParentsParentRef {
    HttpRouteStatusParentsParentRef {
        group: parent.group.clone(),
        kind: parent.kind.clone(),
        name: parent.name.clone(),
        namespace: parent.namespace.clone(),
        port: parent.port,
        section_name: parent.section_name.clone(),
    }
}

/// The listeners of one Gateway (`indices`) a parentRef attaches the route to, or the
/// reason it attaches to none.
fn attach(
    input: &TranslateInput<'_>,
    route: &HttpRoute,
    parent: &HttpRouteParentRefs,
    indices: &[usize],
    listeners: &[Listener<'_>],
) -> Attached {
    let namespace = route.metadata.namespace.clone().unwrap_or_default();
    let selected: Vec<usize> = indices
        .iter()
        .copied()
        .filter(|&i| {
            let spec = listeners[i].spec;
            parent.section_name.as_ref().is_none_or(|s| *s == spec.name)
                && parent.port.is_none_or(|p| p == spec.port)
        })
        .collect();
    if selected.is_empty() {
        return Err((
            "NoMatchingParent",
            "no listener matches the sectionName and port of the parentRef".into(),
        ));
    }
    let allowed: Vec<usize> = selected
        .into_iter()
        .filter(|&i| {
            listeners[i].attachable() && namespace_allowed(input, &listeners[i], &namespace)
        })
        .collect();
    if allowed.is_empty() {
        return Err((
            "NotAllowedByListeners",
            "no matching listener allows this route".into(),
        ));
    }
    let hostnames = route_hostnames(route);
    let attached: Vec<Attachment> = allowed
        .into_iter()
        .filter(|&i| !intersect_hostnames(listeners[i].hostname.as_deref(), &hostnames).is_empty())
        .map(|listener| Attachment { listener })
        .collect();
    if attached.is_empty() {
        return Err((
            "NoMatchingListenerHostname",
            "no hostname of the route matches a listener hostname".into(),
        ));
    }
    Ok(attached)
}

fn namespace_allowed(input: &TranslateInput<'_>, listener: &Listener<'_>, namespace: &str) -> bool {
    let namespaces = listener
        .spec
        .allowed_routes
        .as_ref()
        .and_then(|a| a.namespaces.as_ref());
    let from = namespaces
        .and_then(|n| n.from.clone())
        .unwrap_or(GatewayListenersAllowedRoutesNamespacesFrom::Same);
    match from {
        GatewayListenersAllowedRoutesNamespacesFrom::All => true,
        GatewayListenersAllowedRoutesNamespacesFrom::Same => {
            namespace == listener.gateway_key.namespace
        }
        GatewayListenersAllowedRoutesNamespacesFrom::Selector => {
            let Some(selector) = namespaces.and_then(|n| n.selector.as_ref()) else {
                return false;
            };
            let Some(object) = input.namespaces.get(namespace) else {
                return false;
            };
            let empty = BTreeMap::new();
            selector_matches(selector, object.metadata.labels.as_ref().unwrap_or(&empty))
        }
    }
}

/// A Kubernetes label selector: every `matchLabels` entry and every expression holds.
fn selector_matches(
    selector: &GatewayListenersAllowedRoutesNamespacesSelector,
    labels: &BTreeMap<String, String>,
) -> bool {
    let match_labels = selector
        .match_labels
        .iter()
        .flatten()
        .all(|(k, v)| labels.get(k) == Some(v));
    let expressions = selector.match_expressions.iter().flatten().all(|e| {
        let values = e.values.as_deref().unwrap_or_default();
        let value = labels.get(&e.key);
        match e.operator.as_str() {
            "In" => value.is_some_and(|v| values.contains(v)),
            "NotIn" => value.is_none_or(|v| !values.contains(v)),
            "Exists" => value.is_some(),
            "DoesNotExist" => value.is_none(),
            _ => false,
        }
    });
    match_labels && expressions
}

/// `spec.hostnames`, lowercased; invalid ones are left out.
fn route_hostnames(route: &HttpRoute) -> Vec<String> {
    route
        .spec
        .hostnames
        .iter()
        .flatten()
        .map(|h| h.to_ascii_lowercase())
        .filter(|h| valid_host(h).is_ok())
        .collect()
}

/// The hosts a route serves on a listener: `None` is every host. Empty when they do
/// not intersect. A wildcard matches one or more labels, never the bare suffix.
fn intersect_hostnames(listener: Option<&str>, route: &[String]) -> Vec<Option<String>> {
    let Some(listener) = listener else {
        if route.is_empty() {
            return vec![None];
        }
        return dedup(route.iter().cloned()).into_iter().map(Some).collect();
    };
    if route.is_empty() {
        return vec![Some(listener.to_string())];
    }
    let covers = |wildcard: &str, host: &str| {
        wildcard
            .strip_prefix('*')
            .is_some_and(|suffix| host.len() > suffix.len() && host.ends_with(suffix))
    };
    let out = route.iter().filter_map(|h| {
        if h == listener || (listener.starts_with("*.") && covers(listener, h)) {
            Some(h.clone())
        } else if h.starts_with("*.") && covers(h, listener) {
            Some(listener.to_string())
        } else {
            None
        }
    });
    dedup(out).into_iter().map(Some).collect()
}

// ---- rules ----

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum PathKind {
    Regex,
    Prefix,
    Exact,
}

/// One route match in Pingap terms.
#[derive(Clone, Debug)]
struct Match {
    index: usize,
    kind: PathKind,
    /// Exact: the path; Prefix: without trailing `/` (empty for `/`); Regex: the regex.
    value: String,
    pingap_path: String,
    headers: Vec<String>,
    query: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
enum PathModifier {
    Full(String),
    Prefix(String),
}

#[derive(Clone, Debug)]
struct Redirect {
    scheme: Option<&'static str>,
    hostname: Option<String>,
    port: Option<u16>,
    path: Option<PathModifier>,
    status: i64,
}

/// What the backendRefs of a rule resolve to.
#[derive(Default)]
struct Backends {
    /// The upstream and what to register for it.
    upstream: Option<(String, UpstreamKind)>,
    /// Share of the requests that get a 500, in percent.
    invalid_percent: u8,
    issues: Vec<(&'static str, String)>,
}

#[derive(Clone)]
enum UpstreamKind {
    Generated(UpstreamConfig),
    External(ExternalUpstream),
}

/// A rule of a route that is supported, ready to become locations.
struct Rule {
    index: usize,
    matches: Vec<Match>,
    /// Plugins in run order, before the 500 mock and the redirect.
    plugins: Vec<(String, PluginEntry)>,
    host_header: Option<String>,
    rewrite: Option<PathModifier>,
    redirect: Option<Redirect>,
    upstream: Option<(String, UpstreamKind)>,
    invalid_percent: u8,
    backend_issues: Vec<(&'static str, String)>,
}

fn compile_rule(
    input: &TranslateInput<'_>,
    route: &RouteKey,
    index: usize,
    rule: &HttpRouteRules,
) -> Result<Rule, String> {
    let mut matches = Vec::new();
    let specs = rule.matches.as_deref().unwrap_or_default();
    if specs.is_empty() {
        matches.push(Match {
            index: 0,
            kind: PathKind::Prefix,
            value: String::new(),
            pingap_path: String::new(),
            headers: Vec::new(),
            query: Vec::new(),
        });
    }
    for (i, spec) in specs.iter().enumerate() {
        if spec.method.is_some() {
            return Err("method matches are not supported".into());
        }
        let kind = spec
            .path
            .as_ref()
            .and_then(|p| p.r#type.clone())
            .unwrap_or(HttpRouteRulesMatchesPathType::PathPrefix);
        let raw = spec
            .path
            .as_ref()
            .and_then(|p| p.value.clone())
            .unwrap_or_else(|| "/".into());
        let (kind, value, pingap_path) = pingap_path(&kind, &raw)?;

        let mut headers = Vec::new();
        let mut seen = BTreeSet::new();
        for h in spec.headers.iter().flatten() {
            if h.r#type == Some(HttpRouteRulesMatchesHeadersType::RegularExpression) {
                return Err("regular expression header matches are not supported".into());
            }
            let name = h.name.to_ascii_lowercase();
            if !seen.insert(name.clone()) {
                continue;
            }
            check_token(&name, "header name")?;
            check_match_value(&h.value)?;
            headers.push(format!("{name}:{}", h.value));
        }
        let mut query = Vec::new();
        let mut seen = BTreeSet::new();
        for q in spec.query_params.iter().flatten() {
            if q.r#type == Some(HttpRouteRulesMatchesQueryParamsType::RegularExpression) {
                return Err("regular expression query param matches are not supported".into());
            }
            if !seen.insert(q.name.clone()) {
                continue;
            }
            if q.name.is_empty() || q.name.contains(':') || q.name.trim() != q.name {
                return Err(format!("query param name {:?} is not supported", q.name));
            }
            check_match_value(&q.value)?;
            query.push(format!("{}:{}", q.name, q.value));
        }
        matches.push(Match {
            index: i,
            kind,
            value,
            pingap_path,
            headers,
            query,
        });
    }

    let mut plugins = Vec::new();
    let mut host_header = None;
    let mut rewrite = None;
    let mut redirect = None;
    for filter in rule.filters.iter().flatten() {
        apply_filter(
            filter,
            &mut plugins,
            &mut host_header,
            &mut rewrite,
            &mut redirect,
        )?;
    }
    let prefix_modifier = matches!(rewrite, Some(PathModifier::Prefix(_)))
        || matches!(
            redirect.as_ref().and_then(|r: &Redirect| r.path.as_ref()),
            Some(PathModifier::Prefix(_))
        );
    if prefix_modifier && matches.iter().any(|m| m.kind != PathKind::Prefix) {
        return Err("ReplacePrefixMatch needs PathPrefix matches only".into());
    }
    if rule
        .backend_refs
        .iter()
        .flatten()
        .any(|b| b.filters.as_ref().is_some_and(|f| !f.is_empty()))
    {
        return Err("filters on backendRefs are not supported".into());
    }

    let backends = resolve_backends(input, route, rule);
    Ok(Rule {
        index,
        matches,
        plugins,
        host_header,
        rewrite,
        redirect,
        upstream: backends.upstream,
        invalid_percent: backends.invalid_percent,
        backend_issues: backends.issues,
    })
}

/// A Gateway API path match in Pingap syntax: (kind, normalized value, Pingap path).
fn pingap_path(
    kind: &HttpRouteRulesMatchesPathType,
    raw: &str,
) -> Result<(PathKind, String, String), String> {
    let plain = |p: &str| -> Result<(), String> {
        if !p.starts_with('/') {
            return Err(format!("path {p:?} must start with '/'"));
        }
        if p.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(format!("path {p:?} must not contain whitespace"));
        }
        Ok(())
    };
    match kind {
        HttpRouteRulesMatchesPathType::Exact => {
            plain(raw)?;
            Ok((PathKind::Exact, raw.to_string(), format!("={raw}")))
        }
        HttpRouteRulesMatchesPathType::PathPrefix => {
            plain(raw)?;
            let trimmed = raw.trim_end_matches('/');
            let pingap = if trimmed.is_empty() {
                String::new()
            } else {
                format!("~^{}(/|$)", regex::escape(trimmed))
            };
            Ok((PathKind::Prefix, trimmed.to_string(), pingap))
        }
        HttpRouteRulesMatchesPathType::RegularExpression => {
            if raw.is_empty() {
                return Err("a regular expression path must not be empty".into());
            }
            Regex::new(raw).map_err(|e| format!("invalid path regular expression: {e}"))?;
            Ok((PathKind::Regex, raw.to_string(), format!("~^(?:{raw})$")))
        }
    }
}

/// An HTTP token (RFC 9110): a header name.
fn check_token(name: &str, what: &str) -> Result<(), String> {
    let ok = !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b));
    if ok {
        Ok(())
    } else {
        Err(format!("{what} {name:?} is not valid"))
    }
}

/// Pingap trims match values: one with surrounding whitespace could never match.
fn check_match_value(value: &str) -> Result<(), String> {
    if value.is_empty() || value.trim() != value || value.chars().any(|c| c.is_control()) {
        return Err(format!("match value {value:?} is not supported"));
    }
    Ok(())
}

/// A header value Pingap passes on as written: printable, not trimmed away, and not
/// starting with `$` or `:` (those are replaced by Pingap variables, `$NAME` by the
/// environment of the Pingap process).
fn check_header_value(name: &str, value: &str) -> Result<(), String> {
    if value.trim() != value || value.chars().any(|c| c.is_control()) {
        return Err(format!(
            "header {name}: value {value:?} has surrounding whitespace or control characters"
        ));
    }
    if value.starts_with('$') || value.starts_with(':') {
        return Err(format!(
            "header {name}: values starting with '$' or ':' are not supported"
        ));
    }
    Ok(())
}

/// A path for a redirect or rewrite: no whitespace, no `$` (Pingap substitutes it).
fn check_replacement(path: &str, absolute: bool) -> Result<(), String> {
    if absolute && !path.starts_with('/') {
        return Err(format!("replacement path {path:?} must start with '/'"));
    }
    if !path.is_empty() && !path.starts_with('/') {
        return Err(format!("replacement prefix {path:?} must start with '/'"));
    }
    if path
        .chars()
        .any(|c| c.is_whitespace() || c.is_control() || c == '$' || c == '?' || c == '#')
    {
        return Err(format!("replacement path {path:?} is not supported"));
    }
    Ok(())
}

fn apply_filter(
    filter: &HttpRouteRulesFilters,
    plugins: &mut Vec<(String, PluginEntry)>,
    host_header: &mut Option<String>,
    rewrite: &mut Option<PathModifier>,
    redirect: &mut Option<Redirect>,
) -> Result<(), String> {
    match filter.r#type {
        HttpRouteRulesFiltersType::RequestHeaderModifier => {
            let Some(m) = filter.request_header_modifier.as_ref() else {
                return Err("requestHeaderModifier is missing".into());
            };
            let set: Vec<(&str, &str)> = m
                .set
                .iter()
                .flatten()
                .map(|h| (h.name.as_str(), h.value.as_str()))
                .collect();
            let add: Vec<(&str, &str)> = m
                .add
                .iter()
                .flatten()
                .map(|h| (h.name.as_str(), h.value.as_str()))
                .collect();
            let remove: Vec<&str> = m.remove.iter().flatten().map(String::as_str).collect();
            if let Some(plugin) =
                header_plugin("request_headers", &set, &add, &remove, host_header)?
            {
                plugins.push(plugin);
            }
        }
        HttpRouteRulesFiltersType::ResponseHeaderModifier => {
            let Some(m) = filter.response_header_modifier.as_ref() else {
                return Err("responseHeaderModifier is missing".into());
            };
            let set: Vec<(&str, &str)> = m
                .set
                .iter()
                .flatten()
                .map(|h| (h.name.as_str(), h.value.as_str()))
                .collect();
            let add: Vec<(&str, &str)> = m
                .add
                .iter()
                .flatten()
                .map(|h| (h.name.as_str(), h.value.as_str()))
                .collect();
            let remove: Vec<&str> = m.remove.iter().flatten().map(String::as_str).collect();
            if let Some(plugin) = header_plugin("response_headers", &set, &add, &remove, &mut None)?
            {
                plugins.push(plugin);
            }
        }
        HttpRouteRulesFiltersType::RequestRedirect => {
            let Some(r) = filter.request_redirect.as_ref() else {
                return Err("requestRedirect is missing".into());
            };
            let hostname = r.hostname.as_deref().map(str::to_ascii_lowercase);
            if let Some(h) = &hostname {
                valid_host(h).map_err(|e| format!("redirect hostname {h:?} {e}"))?;
                if h.starts_with("*.") {
                    return Err("a redirect hostname must not be a wildcard".into());
                }
            }
            let port = r
                .port
                .map(|p| {
                    u16::try_from(p)
                        .ok()
                        .filter(|p| *p != 0)
                        .ok_or_else(|| format!("redirect port {p} is not valid"))
                })
                .transpose()?;
            let path = match r.path.as_ref() {
                None => None,
                Some(p) => Some(match p.r#type {
                    HttpRouteRulesFiltersRequestRedirectPathType::ReplaceFullPath => {
                        let v = p.replace_full_path.clone().unwrap_or_else(|| "/".into());
                        check_replacement(&v, true)?;
                        PathModifier::Full(v)
                    }
                    HttpRouteRulesFiltersRequestRedirectPathType::ReplacePrefixMatch => {
                        let v = p.replace_prefix_match.clone().unwrap_or_default();
                        check_replacement(&v, false)?;
                        PathModifier::Prefix(v)
                    }
                }),
            };
            let status = r.status_code.unwrap_or(302);
            if ![301, 302, 303, 307, 308].contains(&status) {
                return Err(format!("redirect status {status} is not supported"));
            }
            *redirect = Some(Redirect {
                scheme: r.scheme.as_ref().map(|s| match s {
                    HttpRouteRulesFiltersRequestRedirectScheme::Http => "http",
                    HttpRouteRulesFiltersRequestRedirectScheme::Https => "https",
                }),
                hostname,
                port,
                path,
                status,
            });
        }
        HttpRouteRulesFiltersType::UrlRewrite => {
            let Some(r) = filter.url_rewrite.as_ref() else {
                return Err("urlRewrite is missing".into());
            };
            if let Some(h) = r.hostname.as_deref() {
                let h = h.to_ascii_lowercase();
                valid_host(&h).map_err(|e| format!("rewrite hostname {h:?} {e}"))?;
                if h.starts_with("*.") {
                    return Err("a rewrite hostname must not be a wildcard".into());
                }
                *host_header = Some(h);
            }
            if let Some(p) = r.path.as_ref() {
                *rewrite = Some(match p.r#type {
                    HttpRouteRulesFiltersUrlRewritePathType::ReplaceFullPath => {
                        let v = p.replace_full_path.clone().unwrap_or_else(|| "/".into());
                        check_replacement(&v, true)?;
                        PathModifier::Full(v)
                    }
                    HttpRouteRulesFiltersUrlRewritePathType::ReplacePrefixMatch => {
                        let v = p.replace_prefix_match.clone().unwrap_or_default();
                        check_replacement(&v, false)?;
                        PathModifier::Prefix(v)
                    }
                });
            }
        }
        HttpRouteRulesFiltersType::RequestMirror => {
            return Err("RequestMirror filters are not supported".into());
        }
        HttpRouteRulesFiltersType::ExtensionRef => {
            return Err("ExtensionRef filters are not supported".into());
        }
        HttpRouteRulesFiltersType::Cors => return Err("CORS filters are not supported".into()),
    }
    Ok(())
}

/// A `request_headers` or `response_headers` plugin. For requests, Host goes to
/// `host_header` (set and add both replace it; it cannot be removed).
fn header_plugin(
    category: &str,
    set: &[(&str, &str)],
    add: &[(&str, &str)],
    remove: &[&str],
    host_header: &mut Option<String>,
) -> Result<Option<(String, PluginEntry)>, String> {
    let request = category == "request_headers";
    let mut set_list = Vec::new();
    let mut add_list = Vec::new();
    let mut remove_list = Vec::new();
    for (list, entries) in [(&mut set_list, set), (&mut add_list, add)] {
        for (name, value) in entries {
            check_token(name, "header name")?;
            let lower = name.to_ascii_lowercase();
            if request && lower == "host" {
                valid_host(&value.to_ascii_lowercase())
                    .map_err(|e| format!("Host header value {value:?} {e}"))?;
                *host_header = Some(value.to_ascii_lowercase());
                continue;
            }
            if request && (lower == "content-length" || lower == "transfer-encoding") {
                return Err(format!("the {name} header cannot be modified"));
            }
            check_header_value(name, value)?;
            list.push(toml::Value::String(format!("{name}:{value}")));
        }
    }
    for name in remove {
        check_token(name, "header name")?;
        let lower = name.to_ascii_lowercase();
        if request && ["host", "content-length", "transfer-encoding"].contains(&lower.as_str()) {
            return Err(format!("the {name} header cannot be removed"));
        }
        remove_list.push(toml::Value::String(name.to_string()));
    }
    let mut fields = toml::Table::new();
    if !add_list.is_empty() {
        fields.insert("add_headers".into(), toml::Value::Array(add_list));
    }
    if !remove_list.is_empty() {
        fields.insert("remove_headers".into(), toml::Value::Array(remove_list));
    }
    if !set_list.is_empty() {
        fields.insert("set_headers".into(), toml::Value::Array(set_list));
    }
    if fields.is_empty() {
        return Ok(None);
    }
    Ok(Some(plugin(category, fields)))
}

/// A generated plugin, named after its content.
fn plugin(category: &str, fields: toml::Table) -> (String, PluginEntry) {
    let content = toml::to_string(&fields).unwrap_or_default();
    (
        names::plugin_name(category, &content),
        PluginEntry {
            category: category.to_string(),
            fields,
        },
    )
}

/// The `mock` plugin that answers 500 for `percent` percent of the requests.
fn mock_500(percent: u8) -> (String, PluginEntry) {
    let mut fields = toml::Table::new();
    if percent < 100 {
        fields.insert(
            "percentage".into(),
            toml::Value::Integer(i64::from(percent)),
        );
    }
    fields.insert("status".into(), toml::Value::Integer(500));
    plugin("mock", fields)
}

/// Resolves the backendRefs of a rule into an upstream, the share of the requests that
/// get a 500 and the problems for `ResolvedRefs`.
fn resolve_backends(
    input: &TranslateInput<'_>,
    route: &RouteKey,
    rule: &HttpRouteRules,
) -> Backends {
    let options = input.options;
    let mut out = Backends::default();
    let refs = rule.backend_refs.as_deref().unwrap_or_default();
    // (Service key, port number, weight, external host).
    let mut valid: Vec<(ServiceKey, i32, u32, Option<String>)> = Vec::new();
    let mut total: u64 = 0;
    let mut invalid: u64 = 0;
    for b in refs {
        let weight = u32::try_from(b.weight.unwrap_or(1)).unwrap_or(0);
        total += u64::from(weight);
        let group = b.group.as_deref().unwrap_or("");
        let kind = b.kind.as_deref().unwrap_or(KIND_SERVICE);
        let namespace = b
            .namespace
            .clone()
            .unwrap_or_else(|| route.namespace.clone());
        let key = ServiceKey {
            namespace: namespace.clone(),
            name: b.name.clone(),
        };
        let problem: Option<(&'static str, String)> = (|| {
            if !group.is_empty() || kind != KIND_SERVICE {
                let shown = if group.is_empty() {
                    kind.to_string()
                } else {
                    format!("{group}/{kind}")
                };
                return Some((
                    "InvalidKind",
                    format!("backend {} has the unsupported kind {shown}", b.name),
                ));
            }
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
                return Some((
                    "RefNotPermitted",
                    format!("no ReferenceGrant allows a reference to Service {key}"),
                ));
            }
            let Some(port) = b.port else {
                return Some((
                    "BackendNotFound",
                    format!("backend Service {key} has no port"),
                ));
            };
            let Some(service) = input.services.get(&key) else {
                return Some(("BackendNotFound", format!("Service {key} does not exist")));
            };
            let spec = service.spec.as_ref();
            if spec.and_then(|s| s.type_.as_deref()) == Some("ExternalName") {
                if !options.external_name {
                    return Some((
                        "InvalidKind",
                        format!("Service {key} is of type ExternalName, which is not enabled"),
                    ));
                }
                let host = spec
                    .and_then(|s| s.external_name.as_deref())
                    .map(|h| h.trim().trim_end_matches('.'))
                    .filter(|h| !h.is_empty());
                let Some(host) = host else {
                    return Some((
                        "BackendNotFound",
                        format!("ExternalName Service {key} has no externalName"),
                    ));
                };
                if !(1..=65535).contains(&port) {
                    return Some((
                        "BackendNotFound",
                        format!("port {port} of Service {key} is not valid"),
                    ));
                }
                valid.push((key.clone(), port, weight, Some(host.to_string())));
                return None;
            }
            let Some(service_port) = service_port(service, port) else {
                return Some((
                    "BackendNotFound",
                    format!("Service {key} has no port {port}"),
                ));
            };
            if let Some(protocol) = service_port.app_protocol.as_deref()
                && !APP_PROTOCOLS.contains(&protocol.to_ascii_lowercase().as_str())
            {
                return Some((
                    "UnsupportedProtocol",
                    format!("appProtocol {protocol} of Service {key} port {port} is not supported"),
                ));
            }
            valid.push((key.clone(), port, weight, None));
            None
        })();
        if let Some(problem) = problem {
            invalid += u64::from(weight);
            out.issues.push(problem);
        }
    }
    valid.retain(|v| v.2 > 0);
    // An ExternalName backend is a single address: it cannot share a weighted set.
    if valid.len() > 1 && valid.iter().any(|v| v.3.is_some()) {
        for v in valid.iter().filter(|v| v.3.is_some()) {
            invalid += u64::from(v.2);
            out.issues.push((
                "InvalidKind",
                format!(
                    "ExternalName Service {} can only be the single backend of a rule",
                    v.0
                ),
            ));
        }
        valid.retain(|v| v.3.is_none());
    }
    if valid.is_empty() {
        out.invalid_percent = 100;
        return out;
    }
    out.invalid_percent = if invalid == 0 {
        0
    } else {
        // Rounded, and never all or nothing while both kinds carry traffic.
        u8::try_from((invalid * 100 + total / 2) / total)
            .unwrap_or(100)
            .clamp(1, 99)
    };
    let kind = match options.upstream_defaults.target {
        UpstreamTarget::Pods => BackendKind::Pods,
        UpstreamTarget::Service { .. } => BackendKind::Service,
    };
    let health_check = options.health_check.as_deref();
    if let [(key, port, _, Some(host))] = valid.as_slice() {
        let name = names::upstream_name(
            &key.namespace,
            &key.name,
            &PortRef::Number(*port),
            BackendKind::External,
            health_check,
        );
        let addr = if host.parse::<Ipv6Addr>().is_ok() {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        out.upstream = Some((
            name,
            UpstreamKind::External(ExternalUpstream {
                service: key.clone(),
                addr,
                health_check: health_check.map(String::from),
            }),
        ));
        return out;
    }
    let mut upstream_options = options.upstream_defaults.clone();
    let (name, services) = if let [(key, port, _, _)] = valid.as_slice() {
        upstream_options.spread_weights = false;
        (
            names::upstream_name(
                &key.namespace,
                &key.name,
                &PortRef::Number(*port),
                kind,
                health_check,
            ),
            vec![ServiceRef {
                key: key.clone(),
                port: PortRef::Number(*port),
                weight: None,
            }],
        )
    } else {
        upstream_options.spread_weights = true;
        let services: Vec<ServiceRef> = valid
            .iter()
            .map(|(key, port, weight, _)| ServiceRef {
                key: key.clone(),
                port: PortRef::Number(*port),
                weight: Some(*weight),
            })
            .collect();
        (
            names::split_upstream_name(&services, kind, health_check),
            services,
        )
    };
    out.upstream = Some((
        name,
        UpstreamKind::Generated(UpstreamConfig {
            services,
            options: upstream_options,
            health_check: health_check.map(String::from),
        }),
    ));
    out
}

fn service_port(service: &Service, port: i32) -> Option<&k8s_openapi::api::core::v1::ServicePort> {
    service
        .spec
        .as_ref()?
        .ports
        .as_deref()?
        .iter()
        .find(|p| p.port == port)
}

/// Whether a ReferenceGrant in `to_namespace` lets `from_kind` objects of
/// `from_namespace` refer to the core `to_kind` object `to_name`.
fn granted(
    grants: &[Arc<ReferenceGrant>],
    from_namespace: &str,
    from_kind: &str,
    to_namespace: &str,
    to_kind: &str,
    to_name: &str,
) -> bool {
    grants.iter().any(|g| {
        g.metadata.namespace.as_deref() == Some(to_namespace)
            && g.spec.from.iter().any(|f| {
                f.group == GATEWAY_GROUP && f.kind == from_kind && f.namespace == from_namespace
            })
            && g.spec.to.iter().any(|t| {
                t.group.is_empty()
                    && t.kind == to_kind
                    && t.name
                        .as_deref()
                        .is_none_or(|n| n.is_empty() || n == to_name)
            })
    })
}

// ---- locations ----

/// A location before its weight, with the plugins it uses and its precedence.
struct Candidate {
    name: String,
    entry: LocationEntry,
    plugins: Vec<(String, PluginEntry)>,
    order: Order,
}

/// Sorts highest precedence first.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Order {
    exact_host_len: std::cmp::Reverse<usize>,
    host_len: std::cmp::Reverse<usize>,
    path_kind: std::cmp::Reverse<PathKind>,
    path_len: std::cmp::Reverse<usize>,
    headers: std::cmp::Reverse<usize>,
    query: std::cmp::Reverse<usize>,
    /// Position of the route, oldest first.
    route: usize,
    rule: usize,
    match_index: usize,
}

fn build_location(
    route: &RouteKey,
    age: usize,
    rule: &Rule,
    m: &Match,
    server: &str,
    host: Option<&str>,
    listener: &Listener<'_>,
) -> Candidate {
    let name = names::location_name(route, server, host, &m.pingap_path, &m.headers, &m.query);
    let mut plugins = rule.plugins.clone();
    let mut upstream = None;
    if let Some(redirect) = &rule.redirect {
        plugins.push(redirect_plugin(redirect, listener, host, m));
    } else {
        if rule.invalid_percent > 0 {
            plugins.push(mock_500(rule.invalid_percent));
        }
        if rule.invalid_percent < 100 {
            upstream = rule.upstream.as_ref().map(|(n, _)| n.clone());
        }
    }
    let rewrite = rule.rewrite.as_ref().map(|r| match r {
        PathModifier::Full(path) => format!(".* {path}"),
        PathModifier::Prefix(replacement) => {
            let prefix = regex::escape(&m.value);
            let replacement = replacement.trim_end_matches('/');
            if replacement.is_empty() {
                format!("^{prefix}/? /")
            } else {
                format!("^{prefix} {replacement}")
            }
        }
    });
    let host_len = host.map_or(0, str::len);
    let exact_host_len = host.filter(|h| !h.starts_with("*.")).map_or(0, str::len);
    let path_len = match m.kind {
        PathKind::Prefix if m.value.is_empty() => 1,
        _ => m.value.len(),
    };
    Candidate {
        entry: LocationEntry {
            owner: route.clone(),
            server: server.to_string(),
            host: host.map(String::from),
            path: m.pingap_path.clone(),
            match_headers: m.headers.clone(),
            match_query: m.query.clone(),
            upstream,
            weight: 0,
            rewrite,
            proxy_set_headers: rule
                .host_header
                .iter()
                .map(|h| format!("host:{h}"))
                .collect(),
            plugins: plugins.iter().map(|(n, _)| n.clone()).collect(),
        },
        name,
        plugins,
        order: Order {
            exact_host_len: std::cmp::Reverse(exact_host_len),
            host_len: std::cmp::Reverse(host_len),
            path_kind: std::cmp::Reverse(m.kind),
            path_len: std::cmp::Reverse(path_len),
            headers: std::cmp::Reverse(m.headers.len()),
            query: std::cmp::Reverse(m.query.len()),
            route: age,
            rule: rule.index,
            match_index: m.index,
        },
    }
}

/// The `redirect` plugin of a RequestRedirect, for one location: the target follows
/// the Gateway API rules (the scheme of the listener unless set; the port of the
/// listener unless the scheme or the port is set, the well-known port of the scheme
/// when only the scheme is; no port in the URL for http:80 and https:443; the host of
/// the request unless set). Pingap appends the query to a path target, so a path target
/// is used whenever Pingap's own choice of scheme and authority is the right one, and a
/// whole URL otherwise.
fn redirect_plugin(
    redirect: &Redirect,
    listener: &Listener<'_>,
    location_host: Option<&str>,
    m: &Match,
) -> (String, PluginEntry) {
    let listener_scheme = if listener.tls { "https" } else { "http" };
    let listener_port = u16::try_from(listener.spec.port).unwrap_or(0);
    let scheme = redirect.scheme.unwrap_or(listener_scheme);
    let well_known = if scheme == "https" { 443 } else { 80 };
    let port = redirect.port.unwrap_or(if redirect.scheme.is_some() {
        well_known
    } else {
        listener_port
    });
    let port_suffix = if port == well_known {
        String::new()
    } else {
        format!(":{port}")
    };
    let exact_location_host = location_host.filter(|h| !h.starts_with("*."));
    let target_host = redirect.hostname.as_deref().or(exact_location_host);

    // Pingap keeps the request's authority (scheme unchanged) or takes its host on the
    // well-known port (plain HTTP to HTTPS) when the target host is the request host.
    let implicit_authority = if scheme == listener_scheme {
        port == listener_port
    } else {
        !listener.tls && scheme == "https" && port == 443
    };
    let to_https = !listener.tls && scheme == "https";
    let mut host_field = None;
    let relative = if listener.tls && scheme == "http" {
        false
    } else {
        match target_host {
            // The request host is the target host on every request.
            Some(h) if exact_location_host == Some(h) => implicit_authority,
            Some(h) if implicit_authority || exact_location_host.is_some() => {
                host_field = Some(format!("{h}{port_suffix}"));
                true
            }
            Some(_) => false,
            None => implicit_authority,
        }
    };

    let path_rules: Vec<(String, String)> = match &redirect.path {
        None => vec![("^/(.*)$".into(), "/${1}".into())],
        Some(PathModifier::Full(path)) => vec![("^/".into(), path.clone())],
        Some(PathModifier::Prefix(replacement)) => {
            let prefix = regex::escape(&m.value);
            let replacement = replacement.trim_end_matches('/');
            if replacement.is_empty() {
                vec![
                    (format!("^{prefix}/?$"), "/".into()),
                    (format!("^{prefix}/(.*)$"), "/${1}".into()),
                ]
            } else {
                vec![(format!("^{prefix}(/.*)?$"), format!("{replacement}${{1}}"))]
            }
        }
    };
    let mut fields = toml::Table::new();
    let rules: Vec<toml::Value> = path_rules
        .into_iter()
        .map(|(pattern, target)| {
            let target = if relative {
                target
            } else {
                let authority = target_host.unwrap_or("$host");
                format!("{scheme}://{authority}{port_suffix}{target}")
            };
            toml::Value::String(format!("{pattern} {target}"))
        })
        .collect();
    if let Some(host) = host_field.filter(|_| relative) {
        fields.insert("host".into(), toml::Value::String(host));
    }
    // On a plain listener this also makes `$host` the request host without its port.
    if to_https || (!relative && !listener.tls) {
        fields.insert("http_to_https".into(), toml::Value::Boolean(true));
    }
    fields.insert("rules".into(), toml::Value::Array(rules));
    fields.insert("status".into(), toml::Value::Integer(redirect.status));
    plugin("redirect", fields)
}

// ---- helpers ----

fn cond(type_: &str, ok: bool, reason: &str, message: &str, generation: Option<i64>) -> Condition {
    Condition {
        last_transition_time: Time(Timestamp::UNIX_EPOCH),
        message: message.to_string(),
        observed_generation: generation,
        reason: reason.to_string(),
        status: if ok { "True" } else { "False" }.to_string(),
        type_: type_.to_string(),
    }
}

fn object_key(meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta) -> ObjectKey {
    ObjectKey::new(
        meta.namespace.clone().unwrap_or_default(),
        meta.name.clone().unwrap_or_default(),
    )
}

/// Oldest first; a missing timestamp counts as newest; then namespace/name.
fn age_key(
    meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
) -> (bool, Option<Time>, ObjectKey) {
    (
        meta.creation_timestamp.is_none(),
        meta.creation_timestamp.clone(),
        object_key(meta),
    )
}

/// The distinct items, in their first order.
fn dedup<T: PartialEq>(items: impl IntoIterator<Item = T>) -> Vec<T> {
    let mut out: Vec<T> = Vec::new();
    for item in items {
        if !out.contains(&item) {
            out.push(item);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use k8s_openapi::api::core::v1::{Namespace, Secret};
    use serde_json::{Value, json};

    use super::*;
    use crate::gateway::model::TranslateOptions;
    use crate::gateway::tls::tests::{cert_pem, key_pem, tls_secret};
    use crate::types::UpstreamOptions;

    const CONTROLLER: &str = "pingap.io/gateway-controller";

    fn opts() -> TranslateOptions {
        TranslateOptions {
            controller_name: CONTROLLER.into(),
            external_name: false,
            upstream_defaults: UpstreamOptions {
                include_not_ready: false,
                drain_terminating: true,
                ipv4_only: false,
                debounce: Duration::from_millis(100),
                debounce_max: Duration::from_secs(1),
                target: UpstreamTarget::Pods,
                spread_weights: false,
            },
            health_check: None,
            cluster_domain: "cluster.local".into(),
            servers: Vec::new(),
            addresses: vec!["192.0.2.1".into()],
        }
    }

    fn server(name: &str, ports: &[u16], tls: bool) -> ServerInfo {
        ServerInfo {
            name: name.into(),
            ports: ports.iter().copied().collect(),
            tls,
        }
    }

    fn class(name: &str, controller: &str) -> GatewayClass {
        serde_json::from_value(json!({
            "apiVersion": "gateway.networking.k8s.io/v1",
            "kind": "GatewayClass",
            "metadata": {"name": name, "generation": 2},
            "spec": {"controllerName": controller}
        }))
        .unwrap()
    }

    fn gateway(ns: &str, name: &str, age: i64, listeners: Value) -> Gateway {
        serde_json::from_value(json!({
            "apiVersion": "gateway.networking.k8s.io/v1",
            "kind": "Gateway",
            "metadata": {
                "namespace": ns, "name": name, "generation": 3,
                "creationTimestamp": ts(age),
            },
            "spec": {"gatewayClassName": "pingap", "listeners": listeners}
        }))
        .unwrap()
    }

    fn http_listener(name: &str, port: i32, hostname: Option<&str>) -> Value {
        let mut l = json!({"name": name, "port": port, "protocol": "HTTP"});
        if let Some(h) = hostname {
            l["hostname"] = json!(h);
        }
        l
    }

    fn https_listener(name: &str, hostname: Option<&str>, secret: &str) -> Value {
        let mut l = json!({
            "name": name, "port": 443, "protocol": "HTTPS",
            "tls": {"mode": "Terminate", "certificateRefs": [{"name": secret}]},
        });
        if let Some(h) = hostname {
            l["hostname"] = json!(h);
        }
        l
    }

    fn ts(age: i64) -> String {
        Timestamp::from_second(1_700_000_000 + age)
            .unwrap()
            .to_string()
    }

    /// An HTTPRoute in `ns` with parentRef `gw` (same namespace) and `spec` merged in.
    fn route(ns: &str, name: &str, age: i64, spec: Value) -> HttpRoute {
        let mut full = json!({"parentRefs": [{"name": "gw"}]});
        for (k, v) in spec.as_object().unwrap() {
            full[k] = v.clone();
        }
        serde_json::from_value(json!({
            "apiVersion": "gateway.networking.k8s.io/v1",
            "kind": "HTTPRoute",
            "metadata": {
                "namespace": ns, "name": name, "generation": 5,
                "creationTimestamp": ts(age),
            },
            "spec": full,
        }))
        .unwrap()
    }

    fn backend(name: &str, port: i32) -> Value {
        json!({"name": name, "port": port})
    }

    fn service(ns: &str, name: &str, ports: &[i32]) -> Service {
        serde_json::from_value(json!({
            "metadata": {"namespace": ns, "name": name},
            "spec": {"ports": ports.iter().map(|p| json!({"port": p})).collect::<Vec<_>>()},
        }))
        .unwrap()
    }

    fn grant(
        ns: &str,
        from_kind: &str,
        from_ns: &str,
        to_kind: &str,
        to_name: Option<&str>,
    ) -> ReferenceGrant {
        let mut to = json!({"group": "", "kind": to_kind});
        if let Some(n) = to_name {
            to["name"] = json!(n);
        }
        serde_json::from_value(json!({
            "apiVersion": "gateway.networking.k8s.io/v1",
            "kind": "ReferenceGrant",
            "metadata": {"namespace": ns, "name": "grant"},
            "spec": {
                "from": [{"group": GATEWAY_GROUP, "kind": from_kind, "namespace": from_ns}],
                "to": [to],
            }
        }))
        .unwrap()
    }

    struct World {
        classes: Vec<Arc<GatewayClass>>,
        gateways: Vec<Arc<Gateway>>,
        routes: Vec<Arc<HttpRoute>>,
        grants: Vec<Arc<ReferenceGrant>>,
        namespaces: BTreeMap<String, Arc<Namespace>>,
        services: BTreeMap<ServiceKey, Arc<Service>>,
        secrets: BTreeMap<SecretKey, Arc<Secret>>,
        servers: Vec<ServerInfo>,
        plugins: BTreeSet<String>,
        options: TranslateOptions,
    }

    impl World {
        /// Our class `pingap`, servers `http` (80, 8080) and `https` (443), Services
        /// `ns/web:80`, `ns/api:80`, `ns/v2:80`.
        fn new() -> Self {
            let mut w = World {
                classes: vec![
                    Arc::new(class("pingap", CONTROLLER)),
                    Arc::new(class("other", "x.io/other")),
                ],
                gateways: Vec::new(),
                routes: Vec::new(),
                grants: Vec::new(),
                namespaces: BTreeMap::new(),
                services: BTreeMap::new(),
                secrets: BTreeMap::new(),
                servers: vec![
                    server("http", &[80, 8080], false),
                    server("https", &[443], true),
                ],
                plugins: BTreeSet::new(),
                options: opts(),
            };
            for name in ["web", "api", "v2"] {
                w.service(service("ns", name, &[80]));
            }
            w.namespace("ns", &[("team", "a")]);
            w.namespace("other", &[("team", "b")]);
            w
        }
        fn gw(mut self, g: Gateway) -> Self {
            self.gateways.push(Arc::new(g));
            self
        }
        fn rt(mut self, r: HttpRoute) -> Self {
            self.routes.push(Arc::new(r));
            self
        }
        fn service(&mut self, s: Service) {
            let key = ServiceKey {
                namespace: s.metadata.namespace.clone().unwrap(),
                name: s.metadata.name.clone().unwrap(),
            };
            self.services.insert(key, Arc::new(s));
        }
        fn namespace(&mut self, name: &str, labels: &[(&str, &str)]) {
            let mut l: BTreeMap<String, String> = labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            l.insert("kubernetes.io/metadata.name".into(), name.into());
            let ns: Namespace =
                serde_json::from_value(json!({"metadata": {"name": name, "labels": l}})).unwrap();
            self.namespaces.insert(name.into(), Arc::new(ns));
        }
        fn secret(&mut self, ns: &str, name: &str) {
            self.secrets.insert(
                SecretKey::new(ns, name),
                Arc::new(tls_secret(ns, name, Some(cert_pem()), Some(key_pem()))),
            );
        }
        fn input(&self) -> TranslateInput<'_> {
            TranslateInput {
                classes: &self.classes,
                gateways: &self.gateways,
                routes: &self.routes,
                grants: &self.grants,
                namespaces: &self.namespaces,
                services: &self.services,
                secrets: &self.secrets,
                servers: &self.servers,
                plugins: &self.plugins,
                options: &self.options,
            }
        }
        fn run(&self) -> Desired {
            translate(&self.input())
        }
    }

    /// The usual Gateway `ns/gw` with an HTTP listener `http` on port 80.
    fn basic_gateway() -> Gateway {
        gateway("ns", "gw", 0, json!([http_listener("http", 80, None)]))
    }

    fn rkey(ns: &str, name: &str) -> RouteKey {
        RouteKey::new(ns, name)
    }

    fn condition<'c>(conditions: &'c [Condition], type_: &str) -> &'c Condition {
        conditions
            .iter()
            .find(|c| c.type_ == type_)
            .unwrap_or_else(|| panic!("no {type_} in {conditions:?}"))
    }

    /// (status, reason) of a condition of the first parent of a route.
    fn route_cond(d: &Desired, route: &RouteKey, type_: &str) -> (String, String) {
        let c = condition(&d.status.routes[route].parents[0].conditions, type_);
        (c.status.clone(), c.reason.clone())
    }

    fn listener_status<'d>(d: &'d Desired, gw: &str, listener: &str) -> &'d GatewayStatusListeners {
        d.status.gateways[&ObjectKey::new("ns", gw)]
            .listeners
            .as_ref()
            .unwrap()
            .iter()
            .find(|l| l.name == listener)
            .unwrap()
    }

    fn listener_cond(d: &Desired, gw: &str, listener: &str, type_: &str) -> (String, String) {
        let c = condition(&listener_status(d, gw, listener).conditions, type_);
        (c.status.clone(), c.reason.clone())
    }

    fn gateway_cond(d: &Desired, gw: &str, type_: &str) -> (String, String) {
        let c = condition(
            d.status.gateways[&ObjectKey::new("ns", gw)]
                .conditions
                .as_ref()
                .unwrap(),
            type_,
        );
        (c.status.clone(), c.reason.clone())
    }

    fn tf(status: &str, reason: &str) -> (String, String) {
        (status.to_string(), reason.to_string())
    }

    /// The locations of a server in weight order.
    fn locations<'d>(d: &'d Desired, server: &str) -> Vec<&'d LocationEntry> {
        d.server_locations[server]
            .iter()
            .map(|n| &d.locations[n])
            .collect()
    }

    fn plugins_of<'d>(d: &'d Desired, location: &LocationEntry) -> Vec<&'d PluginEntry> {
        location.plugins.iter().map(|n| &d.plugins[n]).collect()
    }

    fn strings(value: &toml::Value) -> Vec<String> {
        value
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    }

    // ---- classes ----

    #[test]
    fn supported_features_are_sorted_and_known() {
        let mut sorted = SUPPORTED_FEATURES.to_vec();
        sorted.sort();
        assert_eq!(sorted, SUPPORTED_FEATURES.to_vec());
        sorted.dedup();
        assert_eq!(sorted.len(), SUPPORTED_FEATURES.len());
    }

    #[test]
    fn class_status_for_our_classes_only() {
        let mut w = World::new();
        let mut with_params = class("params", CONTROLLER);
        with_params.spec.parameters_ref = Some(Default::default());
        w.classes.push(Arc::new(with_params));
        let d = w.run();
        assert_eq!(d.status.classes.len(), 2, "{:?}", d.status.classes.keys());
        let ours = &d.status.classes["pingap"];
        let conditions = ours.conditions.as_ref().unwrap();
        let accepted = condition(conditions, "Accepted");
        assert_eq!(
            (accepted.status.as_str(), accepted.reason.as_str()),
            ("True", "Accepted")
        );
        assert_eq!(accepted.observed_generation, Some(2));
        assert_eq!(accepted.last_transition_time, Time(Timestamp::UNIX_EPOCH));
        assert_eq!(condition(conditions, "SupportedVersion").status, "True");
        let features: Vec<&str> = ours
            .supported_features
            .as_ref()
            .unwrap()
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(features, SUPPORTED_FEATURES.to_vec());
        let rejected = condition(
            d.status.classes["params"].conditions.as_ref().unwrap(),
            "Accepted",
        );
        assert_eq!(
            (rejected.status.as_str(), rejected.reason.as_str()),
            ("False", "InvalidParameters")
        );
    }

    // ---- attachment ----

    #[test]
    fn simple_route() {
        let w = World::new().gw(basic_gateway()).rt(route(
            "ns",
            "r",
            0,
            json!({"rules": [{"backendRefs": [backend("web", 80)]}]}),
        ));
        let d = w.run();
        let locs = locations(&d, "http");
        assert_eq!(locs.len(), 1);
        let l = locs[0];
        assert_eq!(l.host, None);
        assert_eq!(l.path, "");
        assert_eq!(l.weight, WEIGHT_TOP);
        assert_eq!(l.owner, rkey("ns", "r"));
        let upstream = l.upstream.clone().unwrap();
        assert_eq!(
            upstream,
            names::upstream_name("ns", "web", &PortRef::Number(80), BackendKind::Pods, None)
        );
        let config = &d.upstreams[&upstream];
        assert_eq!(
            config.services,
            vec![ServiceRef {
                key: ServiceKey {
                    namespace: "ns".into(),
                    name: "web".into()
                },
                port: PortRef::Number(80),
                weight: None,
            }]
        );
        assert!(!config.options.spread_weights);
        assert!(l.plugins.is_empty());
        assert!(d.server_locations["https"].is_empty());

        let parent = &d.status.routes[&rkey("ns", "r")].parents[0];
        assert_eq!(parent.controller_name, CONTROLLER);
        assert_eq!(parent.parent_ref.name, "gw");
        assert_eq!(
            route_cond(&d, &rkey("ns", "r"), "Accepted"),
            tf("True", "Accepted")
        );
        assert_eq!(
            route_cond(&d, &rkey("ns", "r"), "ResolvedRefs"),
            tf("True", "ResolvedRefs")
        );
        assert_eq!(
            condition(&parent.conditions, "Accepted").observed_generation,
            Some(5)
        );
        assert!(
            !parent
                .conditions
                .iter()
                .any(|c| c.type_ == "PartiallyInvalid")
        );

        let ls = listener_status(&d, "gw", "http");
        assert_eq!(ls.attached_routes, 1);
        assert_eq!(
            ls.supported_kinds.as_ref().unwrap(),
            &vec![http_route_kind()]
        );
        assert_eq!(
            listener_cond(&d, "gw", "http", "Accepted"),
            tf("True", "Accepted")
        );
        assert_eq!(
            listener_cond(&d, "gw", "http", "Programmed"),
            tf("True", "Programmed")
        );
        assert_eq!(
            listener_cond(&d, "gw", "http", "Conflicted"),
            tf("False", "NoConflicts")
        );
        assert_eq!(
            listener_cond(&d, "gw", "http", "ResolvedRefs"),
            tf("True", "ResolvedRefs")
        );
        assert_eq!(gateway_cond(&d, "gw", "Accepted"), tf("True", "Accepted"));
        assert_eq!(
            gateway_cond(&d, "gw", "Programmed"),
            tf("True", "Programmed")
        );
        let gs = &d.status.gateways[&ObjectKey::new("ns", "gw")];
        assert_eq!(
            gs.addresses.as_ref().unwrap(),
            &vec![GatewayStatusAddresses {
                r#type: Some("IPAddress".into()),
                value: "192.0.2.1".into()
            }]
        );
        assert_eq!(
            d.referenced.services,
            BTreeSet::from([ServiceKey {
                namespace: "ns".into(),
                name: "web".into()
            }])
        );
    }

    #[test]
    fn routes_and_gateways_of_other_controllers_are_left_alone() {
        let mut other = basic_gateway();
        other.spec.gateway_class_name = "other".into();
        other.metadata.name = Some("foreign".into());
        let r = route(
            "ns",
            "r",
            0,
            json!({"parentRefs": [{"name": "foreign"}, {"name": "missing"}],
            "rules": [{"backendRefs": [backend("web", 80)]}]}),
        );
        let d = World::new().gw(other).rt(r).run();
        assert!(d.status.gateways.is_empty());
        assert!(d.status.routes.is_empty());
        assert!(d.locations.is_empty());
        assert!(d.referenced.services.is_empty());
    }

    #[test]
    fn section_name_and_port() {
        let gw = gateway(
            "ns",
            "gw",
            0,
            json!([
                http_listener("a", 80, Some("a.example.com")),
                http_listener("b", 8080, Some("b.example.com")),
            ]),
        );
        let rules = json!([{"backendRefs": [backend("web", 80)]}]);
        let w = World::new()
            .gw(gw)
            .rt(route("ns", "by-name", 0, json!({"parentRefs": [{"name": "gw", "sectionName": "b"}], "rules": rules})))
            .rt(route("ns", "by-port", 1, json!({"parentRefs": [{"name": "gw", "port": 80}], "rules": rules})))
            .rt(route("ns", "both-mismatch", 2, json!({"parentRefs": [{"name": "gw", "sectionName": "a", "port": 8080}], "rules": rules})))
            .rt(route("ns", "no-such", 3, json!({"parentRefs": [{"name": "gw", "sectionName": "c"}], "rules": rules})));
        let d = w.run();
        assert_eq!(
            route_cond(&d, &rkey("ns", "by-name"), "Accepted"),
            tf("True", "Accepted")
        );
        assert_eq!(
            route_cond(&d, &rkey("ns", "by-port"), "Accepted"),
            tf("True", "Accepted")
        );
        assert_eq!(
            route_cond(&d, &rkey("ns", "both-mismatch"), "Accepted"),
            tf("False", "NoMatchingParent")
        );
        assert_eq!(
            route_cond(&d, &rkey("ns", "no-such"), "Accepted"),
            tf("False", "NoMatchingParent")
        );
        assert_eq!(listener_status(&d, "gw", "a").attached_routes, 1);
        assert_eq!(listener_status(&d, "gw", "b").attached_routes, 1);
        let hosts: Vec<(Option<String>, RouteKey)> = locations(&d, "http")
            .iter()
            .map(|l| (l.host.clone(), l.owner.clone()))
            .collect();
        assert_eq!(
            hosts,
            // Same host length: the older route first.
            vec![
                (Some("b.example.com".into()), rkey("ns", "by-name")),
                (Some("a.example.com".into()), rkey("ns", "by-port")),
            ]
        );
    }

    #[test]
    fn allowed_routes_namespaces_and_kinds() {
        let gw = gateway(
            "ns",
            "gw",
            0,
            json!([
                http_listener("same", 80, Some("same.example.com")),
                {"name": "all", "port": 80, "protocol": "HTTP", "hostname": "all.example.com",
                 "allowedRoutes": {"namespaces": {"from": "All"}}},
                {"name": "team-b", "port": 80, "protocol": "HTTP", "hostname": "b.example.com",
                 "allowedRoutes": {"namespaces": {"from": "Selector", "selector": {
                     "matchLabels": {"team": "b"},
                     "matchExpressions": [{"key": "kubernetes.io/metadata.name", "operator": "In", "values": ["other"]},
                                          {"key": "frozen", "operator": "DoesNotExist"}]}}}},
                {"name": "grpc-only", "port": 80, "protocol": "HTTP", "hostname": "g.example.com",
                 "allowedRoutes": {"kinds": [{"kind": "GRPCRoute"}], "namespaces": {"from": "All"}}},
            ]),
        );
        let mut w = World::new().gw(gw);
        w.service(service("other", "web", &[80]));
        let rules = json!([{"backendRefs": [backend("web", 80)]}]);
        let parent =
            |section: &str| json!([{"name": "gw", "namespace": "ns", "sectionName": section}]);
        for section in ["same", "all", "team-b", "grpc-only"] {
            w = w.rt(route(
                "other",
                section,
                0,
                json!({"parentRefs": parent(section), "rules": rules}),
            ));
        }
        let d = w.run();
        assert_eq!(
            route_cond(&d, &rkey("other", "same"), "Accepted"),
            tf("False", "NotAllowedByListeners")
        );
        assert_eq!(
            route_cond(&d, &rkey("other", "all"), "Accepted"),
            tf("True", "Accepted")
        );
        assert_eq!(
            route_cond(&d, &rkey("other", "team-b"), "Accepted"),
            tf("True", "Accepted")
        );
        assert_eq!(
            route_cond(&d, &rkey("other", "grpc-only"), "Accepted"),
            tf("False", "NotAllowedByListeners")
        );
        assert_eq!(
            listener_cond(&d, "gw", "grpc-only", "ResolvedRefs"),
            tf("False", "InvalidRouteKinds")
        );
        assert_eq!(
            listener_status(&d, "gw", "grpc-only").supported_kinds,
            Some(vec![])
        );
        assert_eq!(listener_status(&d, "gw", "same").attached_routes, 0);
        assert_eq!(listener_status(&d, "gw", "all").attached_routes, 1);

        // A namespace label change detaches the route.
        let mut w2 = World::new();
        w2.gateways = w.gateways.clone();
        w2.routes = w.routes.clone();
        w2.service(service("other", "web", &[80]));
        w2.namespace("other", &[("team", "b"), ("frozen", "yes")]);
        let d = w2.run();
        assert_eq!(
            route_cond(&d, &rkey("other", "team-b"), "Accepted"),
            tf("False", "NotAllowedByListeners")
        );
    }

    #[test]
    fn selector_operators() {
        let labels = BTreeMap::from([("a".to_string(), "1".to_string())]);
        let sel = |v: Value| -> GatewayListenersAllowedRoutesNamespacesSelector {
            serde_json::from_value(v).unwrap()
        };
        assert!(selector_matches(&sel(json!({})), &labels));
        assert!(selector_matches(
            &sel(json!({"matchLabels": {"a": "1"}})),
            &labels
        ));
        assert!(!selector_matches(
            &sel(json!({"matchLabels": {"a": "2"}})),
            &labels
        ));
        let expr = |op: &str, values: Value| {
            sel(json!({"matchExpressions": [{"key": "a", "operator": op, "values": values}]}))
        };
        assert!(selector_matches(&expr("In", json!(["1", "2"])), &labels));
        assert!(!selector_matches(&expr("NotIn", json!(["1"])), &labels));
        assert!(selector_matches(&expr("NotIn", json!(["2"])), &labels));
        assert!(selector_matches(&expr("Exists", json!([])), &labels));
        assert!(!selector_matches(&expr("DoesNotExist", json!([])), &labels));
        assert!(!selector_matches(&expr("Bogus", json!([])), &labels));
    }

    #[test]
    fn hostname_intersection() {
        let r = |hosts: &[&str]| hosts.iter().map(|h| h.to_string()).collect::<Vec<_>>();
        let some = |h: &str| Some(h.to_string());
        assert_eq!(intersect_hostnames(None, &[]), vec![None]);
        assert_eq!(
            intersect_hostnames(None, &r(&["a.com", "a.com"])),
            vec![some("a.com")]
        );
        assert_eq!(intersect_hostnames(Some("a.com"), &[]), vec![some("a.com")]);
        assert_eq!(
            intersect_hostnames(Some("a.com"), &r(&["a.com", "b.com"])),
            vec![some("a.com")]
        );
        assert!(intersect_hostnames(Some("a.com"), &r(&["b.com"])).is_empty());
        // Wildcard listener: covers one or more labels, never the bare domain.
        assert_eq!(
            intersect_hostnames(
                Some("*.example.com"),
                &r(&[
                    "x.example.com",
                    "a.b.example.com",
                    "example.com",
                    "*.b.example.com"
                ])
            ),
            vec![
                some("x.example.com"),
                some("a.b.example.com"),
                some("*.b.example.com")
            ]
        );
        // Wildcard route hostname narrowed to the listener.
        assert_eq!(
            intersect_hostnames(Some("foo.example.com"), &r(&["*.example.com"])),
            vec![some("foo.example.com")]
        );
        assert_eq!(
            intersect_hostnames(Some("*.example.com"), &r(&["*.example.com"])),
            vec![some("*.example.com")]
        );
        assert!(intersect_hostnames(Some("*.a.example.com"), &r(&["*.b.example.com"])).is_empty());
        assert!(intersect_hostnames(Some("example.com"), &r(&["*.example.com"])).is_empty());
    }

    #[test]
    fn no_matching_listener_hostname() {
        let gw = gateway(
            "ns",
            "gw",
            0,
            json!([http_listener("http", 80, Some("*.example.com"))]),
        );
        let w = World::new()
            .gw(gw)
            .rt(route("ns", "miss", 0, json!({"hostnames": ["example.org"], "rules": [{"backendRefs": [backend("web", 80)]}]})))
            .rt(route("ns", "hit", 1, json!({"hostnames": ["Foo.Example.com", "example.org"], "rules": [{"backendRefs": [backend("web", 80)]}]})));
        let d = w.run();
        assert_eq!(
            route_cond(&d, &rkey("ns", "miss"), "Accepted"),
            tf("False", "NoMatchingListenerHostname")
        );
        assert_eq!(
            route_cond(&d, &rkey("ns", "hit"), "Accepted"),
            tf("True", "Accepted")
        );
        let locs = locations(&d, "http");
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0].host.as_deref(), Some("foo.example.com"));
        assert_eq!(listener_status(&d, "gw", "http").attached_routes, 1);
    }

    // ---- matching and precedence ----

    #[test]
    fn path_translation() {
        use HttpRouteRulesMatchesPathType as T;
        let p = |t: T, v: &str| pingap_path(&t, v).map(|(_, _, p)| p);
        assert_eq!(p(T::PathPrefix, "/"), Ok(String::new()));
        assert_eq!(p(T::PathPrefix, "/foo"), Ok("~^/foo(/|$)".into()));
        assert_eq!(p(T::PathPrefix, "/foo/"), Ok("~^/foo(/|$)".into()));
        assert_eq!(p(T::PathPrefix, "/a.b+c"), Ok(r"~^/a\.b\+c(/|$)".into()));
        assert_eq!(p(T::Exact, "/x"), Ok("=/x".into()));
        assert_eq!(
            p(T::RegularExpression, "/v[0-9]+/.*"),
            Ok("~^(?:/v[0-9]+/.*)$".into())
        );
        assert!(p(T::RegularExpression, "/v[").is_err());
        assert!(p(T::Exact, "x").is_err());
        assert!(p(T::PathPrefix, "/a b").is_err());
        // The segment-aware prefix regex does what Gateway API says.
        let re = Regex::new("^/foo(/|$)").unwrap();
        for path in ["/foo", "/foo/", "/foo/bar"] {
            assert!(re.is_match(path), "{path}");
        }
        assert!(!re.is_match("/foobar"));
    }

    #[test]
    fn precedence_becomes_weights() {
        let gw = gateway(
            "ns",
            "gw",
            0,
            json!([
                {"name": "http", "port": 80, "protocol": "HTTP", "allowedRoutes": {"namespaces": {"from": "All"}}}
            ]),
        );
        let be = json!([backend("web", 80)]);
        let w = World::new()
            .gw(gw)
            // Newer route, same match as `old`: after it.
            .rt(route("ns", "new", 10, json!({"rules": [{"matches": [{"path": {"type": "PathPrefix", "value": "/api"}}], "backendRefs": be}]})))
            .rt(route("ns", "old", 1, json!({"rules": [
                {"matches": [{"path": {"type": "PathPrefix", "value": "/api"}}], "backendRefs": be},
                {"matches": [{"path": {"type": "PathPrefix", "value": "/api/v1"}}], "backendRefs": be},
                {"matches": [{"path": {"type": "Exact", "value": "/api"}}], "backendRefs": be},
                {"matches": [{"path": {"type": "RegularExpression", "value": "/r.*"}}], "backendRefs": be},
                {"matches": [{"path": {"type": "PathPrefix", "value": "/api"}, "headers": [{"name": "x", "value": "1"}]}], "backendRefs": be},
                {"matches": [{"path": {"type": "PathPrefix", "value": "/api"}, "headers": [{"name": "x", "value": "1"}],
                              "queryParams": [{"name": "q", "value": "2"}]}], "backendRefs": be},
            ]})))
            .rt(route("ns", "hosts", 5, json!({"hostnames": ["a.example.com", "*.example.com", "*.b.example.com"],
                "rules": [{"backendRefs": be}]})));
        let d = w.run();
        let got: Vec<(Option<&str>, &str, usize, usize, &str)> = locations(&d, "http")
            .iter()
            .map(|l| {
                (
                    l.host.as_deref(),
                    l.path.as_str(),
                    l.match_headers.len(),
                    l.match_query.len(),
                    l.owner.name.as_str(),
                )
            })
            .collect();
        assert_eq!(
            got,
            vec![
                (Some("a.example.com"), "", 0, 0, "hosts"),
                (Some("*.b.example.com"), "", 0, 0, "hosts"),
                (Some("*.example.com"), "", 0, 0, "hosts"),
                (None, "=/api", 0, 0, "old"),
                (None, "~^/api/v1(/|$)", 0, 0, "old"),
                (None, "~^/api(/|$)", 1, 1, "old"),
                (None, "~^/api(/|$)", 1, 0, "old"),
                (None, "~^/api(/|$)", 0, 0, "old"),
                (None, "~^/api(/|$)", 0, 0, "new"),
                (None, "~^(?:/r.*)$", 0, 0, "old"),
            ]
        );
        let weights: Vec<u16> = locations(&d, "http").iter().map(|l| l.weight).collect();
        let expected: Vec<u16> = (0..weights.len() as u16).map(|i| WEIGHT_TOP - i).collect();
        assert_eq!(weights, expected);
        assert!(weights.iter().all(|w| *w >= WEIGHT_FLOOR));
    }

    #[test]
    fn header_and_query_matches() {
        let w = World::new().gw(basic_gateway()).rt(route("ns", "r", 0, json!({"rules": [{
            "matches": [{"headers": [{"name": "X-Version", "value": "v2"}, {"name": "x-version", "value": "ignored"},
                                     {"name": "Accept", "value": "a:b"}],
                         "queryParams": [{"name": "debug", "value": "1"}, {"name": "debug", "value": "2"}]}],
            "backendRefs": [backend("web", 80)]}]})));
        let d = w.run();
        let l = locations(&d, "http")[0];
        assert_eq!(l.match_headers, vec!["x-version:v2", "accept:a:b"]);
        assert_eq!(l.match_query, vec!["debug:1"]);
    }

    #[test]
    fn unsupported_matches_drop_the_rule() {
        let be = json!([backend("web", 80)]);
        let w = World::new()
            .gw(basic_gateway())
            .rt(route("ns", "partial", 0, json!({"rules": [
                {"matches": [{"path": {"type": "PathPrefix", "value": "/ok"}}], "backendRefs": be},
                {"matches": [{"method": "GET"}], "backendRefs": be},
                {"matches": [{"headers": [{"type": "RegularExpression", "name": "x", "value": ".*"}]}], "backendRefs": be},
                {"matches": [{"path": {"type": "RegularExpression", "value": "(("}}], "backendRefs": be},
            ]})))
            .rt(route("ns", "none", 1, json!({"rules": [
                {"matches": [{"queryParams": [{"type": "RegularExpression", "name": "q", "value": "x"}]}], "backendRefs": be},
            ]})));
        let d = w.run();
        let partial = rkey("ns", "partial");
        assert_eq!(route_cond(&d, &partial, "Accepted"), tf("True", "Accepted"));
        let pi = condition(
            &d.status.routes[&partial].parents[0].conditions,
            "PartiallyInvalid",
        );
        assert_eq!(
            (pi.status.as_str(), pi.reason.as_str()),
            ("True", "UnsupportedValue")
        );
        assert!(pi.message.starts_with("Dropped Rule"), "{}", pi.message);
        assert!(
            pi.message.contains("rule 1")
                && pi.message.contains("rule 2")
                && pi.message.contains("rule 3")
        );
        assert_eq!(
            route_cond(&d, &rkey("ns", "none"), "Accepted"),
            tf("False", "UnsupportedValue")
        );
        let locs = locations(&d, "http");
        assert_eq!(locs.len(), 1);
        assert_eq!(locs[0].path, "~^/ok(/|$)");
        // Only accepted routes count.
        assert_eq!(listener_status(&d, "gw", "http").attached_routes, 1);
    }

    // ---- backends ----

    #[test]
    fn invalid_backends_get_500() {
        let mut w = World::new().gw(basic_gateway()).rt(route("ns", "r", 0, json!({"rules": [
            {"matches": [{"path": {"value": "/missing"}}], "backendRefs": [backend("gone", 80)]},
            {"matches": [{"path": {"value": "/half"}}], "backendRefs": [backend("web", 80), backend("gone", 80)]},
            {"matches": [{"path": {"value": "/quarter"}}], "backendRefs": [
                {"name": "web", "port": 80, "weight": 3}, {"name": "gone", "port": 80, "weight": 1}]},
            {"matches": [{"path": {"value": "/zero"}}], "backendRefs": [{"name": "web", "port": 80, "weight": 0}]},
            {"matches": [{"path": {"value": "/none"}}]},
            {"matches": [{"path": {"value": "/kind"}}], "backendRefs": [{"group": "example.com", "kind": "Bucket", "name": "b"}]},
            {"matches": [{"path": {"value": "/port"}}], "backendRefs": [backend("web", 81)]},
            {"matches": [{"path": {"value": "/proto"}}], "backendRefs": [backend("h2", 80)]},
        ]})));
        w.service(
            serde_json::from_value(json!({"metadata": {"namespace": "ns", "name": "h2"},
            "spec": {"ports": [{"port": 80, "appProtocol": "kubernetes.io/h2c"}]}}))
            .unwrap(),
        );
        let d = w.run();
        let by_path: BTreeMap<&str, &LocationEntry> = locations(&d, "http")
            .into_iter()
            .map(|l| (l.path.as_str(), l))
            .collect();
        let full_500 = |p: &str| {
            let l = by_path[format!("~^/{p}(/|$)").as_str()];
            assert_eq!(l.upstream, None, "{p}");
            let plugins = plugins_of(&d, l);
            assert_eq!(plugins.len(), 1, "{p}");
            assert_eq!(plugins[0].category, "mock");
            assert_eq!(plugins[0].fields["status"].as_integer(), Some(500));
            assert!(!plugins[0].fields.contains_key("percentage"));
        };
        for p in ["missing", "zero", "none", "kind", "port", "proto"] {
            full_500(p);
        }
        let share = |p: &str, percent: i64| {
            let l = by_path[format!("~^/{p}(/|$)").as_str()];
            assert!(l.upstream.is_some(), "{p}");
            let plugins = plugins_of(&d, l);
            assert_eq!(plugins[0].category, "mock");
            assert_eq!(
                plugins[0].fields["percentage"].as_integer(),
                Some(percent),
                "{p}"
            );
        };
        share("half", 50);
        share("quarter", 25);
        // The 500 mocks are shared between locations.
        assert_eq!(
            d.plugins.values().filter(|p| p.category == "mock").count(),
            3
        );
        let (status, reason) = route_cond(&d, &rkey("ns", "r"), "ResolvedRefs");
        assert_eq!(
            (status.as_str(), reason.as_str()),
            ("False", "BackendNotFound")
        );
        let message = &condition(
            &d.status.routes[&rkey("ns", "r")].parents[0].conditions,
            "ResolvedRefs",
        )
        .message;
        assert!(
            message.contains("ns/gone") && message.contains("Bucket") && message.contains("h2c"),
            "{message}"
        );
        // Invalid backends do not affect acceptance.
        assert_eq!(
            route_cond(&d, &rkey("ns", "r"), "Accepted"),
            tf("True", "Accepted")
        );
    }

    #[test]
    fn weighted_backends_share_one_upstream() {
        let be = json!([{"name": "web", "port": 80, "weight": 90}, {"name": "v2", "port": 80, "weight": 10}]);
        let be_reordered = json!([{"name": "v2", "port": 80, "weight": 10}, {"name": "web", "port": 80, "weight": 90}]);
        let w = World::new()
            .gw(basic_gateway())
            .rt(route("ns", "a", 0, json!({"rules": [{"matches": [{"path": {"value": "/a"}}], "backendRefs": be}]})))
            .rt(route("ns", "b", 1, json!({"rules": [{"matches": [{"path": {"value": "/b"}}], "backendRefs": be_reordered}]})));
        let d = w.run();
        assert_eq!(d.upstreams.len(), 1);
        let (name, config) = d.upstreams.iter().next().unwrap();
        assert!(name.contains("-split-"), "{name}");
        assert!(config.options.spread_weights);
        let weights: BTreeMap<&str, Option<u32>> = config
            .services
            .iter()
            .map(|s| (s.key.name.as_str(), s.weight))
            .collect();
        assert_eq!(
            weights,
            BTreeMap::from([("web", Some(90)), ("v2", Some(10))])
        );
        assert!(
            locations(&d, "http")
                .iter()
                .all(|l| l.upstream.as_ref() == Some(name))
        );
    }

    #[test]
    fn backend_reference_grants() {
        let mut w = World::new().gw(basic_gateway()).rt(route(
            "ns",
            "r",
            0,
            json!({"rules": [
            {"backendRefs": [{"name": "web", "namespace": "other", "port": 80}]}]}),
        ));
        w.service(service("other", "web", &[80]));
        let d = w.run();
        assert_eq!(
            route_cond(&d, &rkey("ns", "r"), "ResolvedRefs"),
            tf("False", "RefNotPermitted")
        );
        assert_eq!(locations(&d, "http")[0].upstream, None);
        assert!(d.referenced.services.contains(&ServiceKey {
            namespace: "other".into(),
            name: "web".into()
        }));

        // A grant for another Service or from another kind does not help.
        w.grants = vec![
            Arc::new(grant("other", "HTTPRoute", "ns", "Service", Some("api"))),
            Arc::new(grant("other", "Gateway", "ns", "Service", None)),
        ];
        let d = w.run();
        assert_eq!(
            route_cond(&d, &rkey("ns", "r"), "ResolvedRefs"),
            tf("False", "RefNotPermitted")
        );

        w.grants.push(Arc::new(grant(
            "other",
            "HTTPRoute",
            "ns",
            "Service",
            Some("web"),
        )));
        let d = w.run();
        assert_eq!(
            route_cond(&d, &rkey("ns", "r"), "ResolvedRefs"),
            tf("True", "ResolvedRefs")
        );
        let upstream = locations(&d, "http")[0].upstream.clone().unwrap();
        assert_eq!(d.upstreams[&upstream].services[0].key.namespace, "other");
    }

    #[test]
    fn external_name_backends() {
        let mut w = World::new().gw(basic_gateway()).rt(route(
            "ns",
            "r",
            0,
            json!({"rules": [
            {"backendRefs": [backend("ext", 443)]}]}),
        ));
        w.service(
            serde_json::from_value(json!({"metadata": {"namespace": "ns", "name": "ext"},
            "spec": {"type": "ExternalName", "externalName": "api.example.net."}}))
            .unwrap(),
        );
        let d = w.run();
        assert_eq!(
            route_cond(&d, &rkey("ns", "r"), "ResolvedRefs"),
            tf("False", "InvalidKind")
        );
        assert!(d.external.is_empty());

        w.options.external_name = true;
        let d = w.run();
        assert_eq!(
            route_cond(&d, &rkey("ns", "r"), "ResolvedRefs"),
            tf("True", "ResolvedRefs")
        );
        let name = locations(&d, "http")[0].upstream.clone().unwrap();
        assert!(
            name.ends_with(&name[name.len() - 8..]) && name.contains("-ext-"),
            "{name}"
        );
        assert_eq!(d.external[&name].addr, "api.example.net:443");
        assert!(d.upstreams.is_empty());
    }

    // ---- TLS ----

    #[test]
    fn https_listener_certificates() {
        let gw = gateway(
            "ns",
            "gw",
            0,
            json!([
                https_listener("a", Some("a.example.com"), "cert"),
                https_listener("b", Some("b.example.com"), "cert"),
                https_listener("missing", Some("m.example.com"), "nope"),
                {"name": "cross", "port": 443, "protocol": "HTTPS", "hostname": "x.example.com",
                 "tls": {"certificateRefs": [{"name": "shared", "namespace": "certs"}]}},
                {"name": "kind", "port": 443, "protocol": "HTTPS", "hostname": "k.example.com",
                 "tls": {"certificateRefs": [{"group": "example.com", "kind": "Vault", "name": "v"}]}},
            ]),
        );
        let mut w = World::new().gw(gw).rt(route(
            "ns",
            "r",
            0,
            json!({
            "parentRefs": [{"name": "gw"}], "rules": [{"backendRefs": [backend("web", 80)]}]}),
        ));
        w.secret("ns", "cert");
        w.secret("certs", "shared");
        let d = w.run();
        assert_eq!(d.certificates.len(), 1);
        let cert = &d.certificates[&names::certificate_name(&SecretKey::new("ns", "cert"))];
        assert_eq!(cert.domains, vec!["a.example.com", "b.example.com"]);
        assert_eq!(cert.tls_cert, cert_pem());
        assert_eq!(
            listener_cond(&d, "gw", "missing", "ResolvedRefs"),
            tf("False", "InvalidCertificateRef")
        );
        assert_eq!(listener_cond(&d, "gw", "missing", "Programmed").0, "False");
        assert_eq!(
            listener_cond(&d, "gw", "cross", "ResolvedRefs"),
            tf("False", "RefNotPermitted")
        );
        assert_eq!(
            listener_cond(&d, "gw", "kind", "ResolvedRefs"),
            tf("False", "InvalidCertificateRef")
        );
        // Routes still attach to listeners with unresolved references.
        assert_eq!(listener_status(&d, "gw", "missing").attached_routes, 1);
        let hosts: Vec<Option<&str>> = locations(&d, "https")
            .iter()
            .map(|l| l.host.as_deref())
            .collect();
        assert_eq!(hosts, vec![Some("a.example.com"), Some("b.example.com")]);
        assert!(d.server_locations["http"].is_empty());
        assert_eq!(
            d.referenced.secrets,
            BTreeSet::from([
                SecretKey::new("ns", "cert"),
                SecretKey::new("ns", "nope"),
                SecretKey::new("certs", "shared")
            ])
        );

        w.grants
            .push(Arc::new(grant("certs", "Gateway", "ns", "Secret", None)));
        let d = w.run();
        assert_eq!(
            listener_cond(&d, "gw", "cross", "ResolvedRefs"),
            tf("True", "ResolvedRefs")
        );
        assert_eq!(d.certificates.len(), 2);
    }

    #[test]
    fn certificate_of_a_listener_without_hostname_has_no_domains() {
        let gw = gateway("ns", "gw", 0, json!([https_listener("a", None, "cert")]));
        let mut w = World::new().gw(gw);
        w.secret("ns", "cert");
        let d = w.run();
        assert_eq!(
            d.certificates.values().next().unwrap().domains,
            Vec::<String>::new()
        );
    }

    // ---- listeners ----

    #[test]
    fn unsupported_listeners() {
        let gw = gateway(
            "ns",
            "gw",
            0,
            json!([
                http_listener("ok", 80, None),
                {"name": "tcp", "port": 9000, "protocol": "TCP"},
                http_listener("noport", 9999, None),
            ]),
        );
        let d = World::new().gw(gw).run();
        assert_eq!(
            listener_cond(&d, "gw", "tcp", "Accepted"),
            tf("False", "UnsupportedProtocol")
        );
        assert_eq!(
            listener_status(&d, "gw", "tcp").supported_kinds,
            Some(vec![])
        );
        assert_eq!(
            listener_cond(&d, "gw", "noport", "Accepted"),
            tf("False", "PortUnavailable")
        );
        assert_eq!(listener_cond(&d, "gw", "noport", "Programmed").0, "False");
        assert_eq!(
            gateway_cond(&d, "gw", "Accepted"),
            tf("True", "ListenersNotValid")
        );

        let only_bad = gateway(
            "ns",
            "gw",
            0,
            json!([{"name": "udp", "port": 53, "protocol": "UDP"}]),
        );
        let d = World::new().gw(only_bad).run();
        assert_eq!(
            gateway_cond(&d, "gw", "Accepted"),
            tf("False", "ListenersNotValid")
        );
        assert_eq!(gateway_cond(&d, "gw", "Programmed").0, "False");
        assert!(
            d.status.gateways[&ObjectKey::new("ns", "gw")]
                .addresses
                .is_none()
        );
    }

    #[test]
    fn servers_option_limits_the_servers() {
        let mut w = World::new().gw(basic_gateway()).rt(route(
            "ns",
            "r",
            0,
            json!({"rules": [{"backendRefs": [backend("web", 80)]}]}),
        ));
        w.servers.push(server("http2", &[80], false));
        let d = w.run();
        assert_eq!(locations(&d, "http").len(), 1);
        assert_eq!(locations(&d, "http2").len(), 1);
        w.options.servers = vec!["http2".into()];
        let d = w.run();
        assert!(!d.server_locations.contains_key("http"));
        assert_eq!(locations(&d, "http2").len(), 1);
    }

    #[test]
    fn listener_conflicts() {
        let mut w = World::new()
            .gw(gateway("ns", "old", 0, json!([http_listener("http", 80, Some("a.example.com"))])))
            .gw(gateway("ns", "new", 5, json!([
                http_listener("same", 80, Some("a.example.com")),
                http_listener("other", 80, Some("b.example.com")),
            ])))
            .gw(gateway("ns", "mixed", 9, json!([
                {"name": "plain", "port": 8000, "protocol": "HTTP"},
                {"name": "tls", "port": 8000, "protocol": "HTTPS", "tls": {"certificateRefs": [{"name": "cert"}]}},
            ])));
        w.servers.push(server("plain8000", &[8000], false));
        w.servers.push(server("tls8000", &[8000], true));
        w.secret("ns", "cert");
        let d = w.run();
        assert_eq!(
            listener_cond(&d, "old", "http", "Conflicted"),
            tf("False", "NoConflicts")
        );
        // Across Gateways: merged, not conflicted.
        for name in ["same", "other"] {
            assert_eq!(
                listener_cond(&d, "new", name, "Conflicted"),
                tf("False", "NoConflicts")
            );
            assert_eq!(
                listener_cond(&d, "new", name, "Programmed"),
                tf("True", "Programmed")
            );
        }
        assert_eq!(gateway_cond(&d, "new", "Accepted"), tf("True", "Accepted"));
        assert_eq!(
            listener_cond(&d, "mixed", "plain", "Conflicted"),
            tf("True", "ProtocolConflict")
        );
        assert_eq!(
            listener_cond(&d, "mixed", "tls", "Conflicted"),
            tf("True", "ProtocolConflict")
        );
        assert_eq!(
            gateway_cond(&d, "mixed", "Accepted"),
            tf("False", "ListenersNotValid")
        );
    }

    #[test]
    fn gateways_share_servers() {
        let w = World::new()
            .gw(gateway("ns", "gw", 0, json!([http_listener("http", 80, Some("a.example.com"))])))
            .gw(gateway("ns", "gw2", 1, json!([http_listener("http", 80, Some("b.example.com"))])))
            .rt(route("ns", "a", 0, json!({"rules": [{"backendRefs": [backend("web", 80)]}]})))
            .rt(route("ns", "b", 1, json!({"parentRefs": [{"name": "gw2"}], "rules": [{"backendRefs": [backend("web", 80)]}]})));
        let d = w.run();
        let hosts: Vec<Option<&str>> = locations(&d, "http")
            .iter()
            .map(|l| l.host.as_deref())
            .collect();
        assert_eq!(hosts, vec![Some("a.example.com"), Some("b.example.com")]);
        // One upstream for the same backend.
        assert_eq!(d.upstreams.len(), 1);
        assert_eq!(
            gateway_cond(&d, "gw2", "Programmed"),
            tf("True", "Programmed")
        );
    }

    #[test]
    fn gateways_with_the_same_listener_are_merged() {
        // The conformance base Gateways: three plain HTTP listeners on port 80 without
        // hostname, each with its routes.
        let w = World::new()
            .gw(gateway("ns", "gw", 0, json!([http_listener("http", 80, None)])))
            .gw(gateway("ns", "gw2", 1, json!([http_listener("http", 80, None)])))
            .rt(route("ns", "a", 0, json!({"rules": [{"matches": [{"path": {"type": "PathPrefix", "value": "/a"}}], "backendRefs": [backend("web", 80)]}]})))
            .rt(route("ns", "b", 1, json!({"parentRefs": [{"name": "gw2"}], "rules": [{"matches": [{"path": {"type": "PathPrefix", "value": "/b"}}], "backendRefs": [backend("web", 80)]}]})));
        let d = w.run();
        for g in ["gw", "gw2"] {
            assert_eq!(gateway_cond(&d, g, "Accepted"), tf("True", "Accepted"));
            assert_eq!(gateway_cond(&d, g, "Programmed"), tf("True", "Programmed"));
            assert_eq!(
                listener_cond(&d, g, "http", "Conflicted"),
                tf("False", "NoConflicts")
            );
        }
        let mut paths: Vec<&str> = locations(&d, "http")
            .iter()
            .map(|l| l.path.as_str())
            .collect();
        paths.sort_unstable();
        assert_eq!(paths, vec!["~^/a(/|$)", "~^/b(/|$)"]);
    }

    #[test]
    fn requested_addresses() {
        let mut g = basic_gateway();
        g.spec.addresses = Some(vec![
            serde_json::from_value(json!({"type": "IPAddress", "value": "192.0.2.1"})).unwrap(),
        ]);
        let d = World::new().gw(g.clone()).run();
        assert_eq!(
            gateway_cond(&d, "gw", "Programmed"),
            tf("True", "Programmed")
        );
        g.spec.addresses = Some(vec![
            serde_json::from_value(json!({"type": "IPAddress", "value": "198.51.100.7"})).unwrap(),
        ]);
        let d = World::new().gw(g.clone()).run();
        assert_eq!(
            gateway_cond(&d, "gw", "Programmed"),
            tf("False", "AddressNotUsable")
        );
        g.spec.addresses = Some(vec![
            serde_json::from_value(json!({"type": "example.com/named", "value": "x"})).unwrap(),
        ]);
        let d = World::new().gw(g).run();
        assert_eq!(
            gateway_cond(&d, "gw", "Accepted"),
            tf("False", "UnsupportedAddress")
        );
        let mut w = World::new().gw(basic_gateway());
        w.options.addresses = vec!["lb.example.com".into()];
        let d = w.run();
        assert_eq!(
            d.status.gateways[&ObjectKey::new("ns", "gw")]
                .addresses
                .as_ref()
                .unwrap()[0]
                .r#type
                .as_deref(),
            Some("Hostname")
        );
        w.options.addresses.clear();
        assert_eq!(
            gateway_cond(&w.run(), "gw", "Programmed"),
            tf("False", "AddressNotAssigned")
        );
    }

    // ---- filters ----

    fn one_rule(filters: Value, matches: Value) -> World {
        World::new().gw(basic_gateway()).rt(route(
            "ns",
            "r",
            0,
            json!({"rules": [{
            "matches": matches, "filters": filters, "backendRefs": [backend("web", 80)]}]}),
        ))
    }

    #[test]
    fn header_modifiers() {
        let d = one_rule(
            json!([
                {"type": "RequestHeaderModifier", "requestHeaderModifier": {
                    "set": [{"name": "X-Set", "value": "1"}, {"name": "Host", "value": "Internal.example.com"}],
                    "add": [{"name": "X-Add", "value": "a b"}], "remove": ["X-Drop"]}},
                {"type": "ResponseHeaderModifier", "responseHeaderModifier": {"set": [{"name": "X-Resp", "value": "2"}]}},
            ]),
            json!([]),
        )
        .run();
        let l = locations(&d, "http")[0];
        assert_eq!(l.proxy_set_headers, vec!["host:internal.example.com"]);
        let plugins = plugins_of(&d, l);
        assert_eq!(plugins.len(), 2);
        assert_eq!(plugins[0].category, "request_headers");
        assert_eq!(strings(&plugins[0].fields["set_headers"]), vec!["X-Set:1"]);
        assert_eq!(
            strings(&plugins[0].fields["add_headers"]),
            vec!["X-Add:a b"]
        );
        assert_eq!(
            strings(&plugins[0].fields["remove_headers"]),
            vec!["X-Drop"]
        );
        assert_eq!(plugins[1].category, "response_headers");
        assert_eq!(strings(&plugins[1].fields["set_headers"]), vec!["X-Resp:2"]);
        assert!(l.upstream.is_some());
    }

    #[test]
    fn unsafe_or_unsupported_filters_drop_the_rule() {
        for filters in [
            json!([{"type": "RequestHeaderModifier", "requestHeaderModifier": {"set": [{"name": "X", "value": "$HOME"}]}}]),
            json!([{"type": "ResponseHeaderModifier", "responseHeaderModifier": {"add": [{"name": "X", "value": ":path"}]}}]),
            json!([{"type": "RequestHeaderModifier", "requestHeaderModifier": {"remove": ["Host"]}}]),
            json!([{"type": "RequestMirror", "requestMirror": {"backendRef": {"name": "web", "port": 80}}}]),
            json!([{"type": "CORS", "cors": {"allowOrigins": ["*"]}}]),
            json!([{"type": "ExtensionRef", "extensionRef": {"group": "x", "kind": "y", "name": "z"}}]),
            json!([{"type": "URLRewrite", "urlRewrite": {"path": {"type": "ReplacePrefixMatch", "replacePrefixMatch": "/x"}}}]),
        ] {
            let matches = json!([{"path": {"type": "Exact", "value": "/e"}}]);
            let d = one_rule(filters.clone(), matches).run();
            assert_eq!(
                route_cond(&d, &rkey("ns", "r"), "Accepted"),
                tf("False", "UnsupportedValue"),
                "{filters}"
            );
            assert!(d.locations.is_empty(), "{filters}");
        }
    }

    #[test]
    fn url_rewrite() {
        let rewrite = |filter: Value, prefix: &str| {
            let d = one_rule(
                json!([{"type": "URLRewrite", "urlRewrite": filter}]),
                json!([{"path": {"type": "PathPrefix", "value": prefix}}]),
            )
            .run();
            let l = locations(&d, "http")[0].clone();
            (l.rewrite, l.proxy_set_headers)
        };
        assert_eq!(
            rewrite(
                json!({"hostname": "Backend.example.com", "path": {"type": "ReplacePrefixMatch", "replacePrefixMatch": "/xyz/"}}),
                "/foo"
            ),
            (
                Some("^/foo /xyz".into()),
                vec!["host:backend.example.com".to_string()]
            )
        );
        assert_eq!(
            rewrite(
                json!({"path": {"type": "ReplacePrefixMatch", "replacePrefixMatch": "/"}}),
                "/foo/"
            ),
            (Some("^/foo/? /".into()), vec![])
        );
        assert_eq!(
            rewrite(
                json!({"path": {"type": "ReplaceFullPath", "replaceFullPath": "/one"}}),
                "/foo"
            ),
            (Some(".* /one".into()), vec![])
        );
        // The prefix rewrite with Pingap's semantics (first match replaced, rest kept)
        // gives the table of the Gateway API spec.
        let apply = |rule: &str, path: &str| {
            let (pattern, replacement) = rule.split_once(' ').unwrap();
            Regex::new(pattern)
                .unwrap()
                .replace(path, replacement)
                .to_string()
        };
        for (path, prefix, replace, want) in [
            ("/foo/bar", "/foo", "/xyz", "/xyz/bar"),
            ("/foo/bar", "/foo", "/xyz/", "/xyz/bar"),
            ("/foo", "/foo", "/xyz", "/xyz"),
            ("/foo/", "/foo", "/xyz", "/xyz/"),
            ("/foo/bar", "/foo", "", "/bar"),
            ("/foo/", "/foo", "", "/"),
            ("/foo", "/foo", "", "/"),
            ("/foo/", "/foo", "/", "/"),
            ("/foo", "/foo", "/", "/"),
        ] {
            let (rule, _) = rewrite(
                json!({"path": {"type": "ReplacePrefixMatch", "replacePrefixMatch": replace}}),
                prefix,
            );
            assert_eq!(
                apply(&rule.unwrap(), path),
                want,
                "{path} {prefix} {replace}"
            );
        }
    }

    /// The redirect plugin fields of the only location of `route_spec` on `server`.
    fn redirect_of(gw: Gateway, filter: Value, hostnames: Value, server: &str) -> toml::Table {
        let mut w = World::new().gw(gw).rt(route(
            "ns",
            "r",
            0,
            json!({"hostnames": hostnames, "rules": [{
            "matches": [{"path": {"type": "PathPrefix", "value": "/old"}}],
            "filters": [{"type": "RequestRedirect", "requestRedirect": filter}]}]}),
        ));
        w.secret("ns", "cert");
        let d = w.run();
        let l = locations(&d, server)[0];
        assert_eq!(l.upstream, None);
        let plugins = plugins_of(&d, l);
        assert_eq!(plugins.len(), 1);
        assert_eq!(plugins[0].category, "redirect");
        plugins[0].fields.clone()
    }

    #[test]
    fn redirects() {
        let http = basic_gateway;
        let https = || {
            gateway(
                "ns",
                "gw",
                0,
                json!([https_listener("https", None, "cert")]),
            )
        };
        let any = json!([]);

        // Hostname only: a path target (keeps the query) with the host.
        let f = redirect_of(
            http(),
            json!({"hostname": "example.org"}),
            any.clone(),
            "http",
        );
        assert_eq!(f["host"].as_str(), Some("example.org"));
        assert_eq!(strings(&f["rules"]), vec!["^/(.*)$ /${1}"]);
        assert_eq!(f["status"].as_integer(), Some(302));
        assert!(!f.contains_key("http_to_https"));

        // To HTTPS on the well-known port, host of the request.
        let f = redirect_of(
            http(),
            json!({"scheme": "https", "statusCode": 301}),
            any.clone(),
            "http",
        );
        assert_eq!(f["http_to_https"].as_bool(), Some(true));
        assert!(!f.contains_key("host"));
        assert_eq!(f["status"].as_integer(), Some(301));

        // Another port of the request host: a whole URL, host without its port.
        let f = redirect_of(http(), json!({"port": 8083}), any.clone(), "http");
        assert_eq!(strings(&f["rules"]), vec!["^/(.*)$ http://$host:8083/${1}"]);
        assert_eq!(f["http_to_https"].as_bool(), Some(true));

        // Hostname and port, default port elided.
        let f = redirect_of(
            http(),
            json!({"hostname": "example.org", "port": 80}),
            any.clone(),
            "http",
        );
        assert_eq!(f["host"].as_str(), Some("example.org"));
        let f = redirect_of(
            http(),
            json!({"hostname": "example.org", "scheme": "https", "port": 8443}),
            any.clone(),
            "http",
        );
        // A request for example.org itself must go to the other port too, and Pingap
        // keeps the request authority for its own host: a whole URL.
        assert_eq!(
            strings(&f["rules"]),
            vec!["^/(.*)$ https://example.org:8443/${1}"]
        );
        assert!(!f.contains_key("host"));

        // Exact location host, other port: Pingap would keep the request port.
        let f = redirect_of(
            http(),
            json!({"port": 8080}),
            json!(["a.example.com"]),
            "http",
        );
        assert_eq!(
            strings(&f["rules"]),
            vec!["^/(.*)$ http://a.example.com:8080/${1}"]
        );

        // HTTPS listener: to plain HTTP needs a whole URL.
        let f = redirect_of(
            https(),
            json!({"scheme": "http", "hostname": "example.org"}),
            any.clone(),
            "https",
        );
        assert_eq!(
            strings(&f["rules"]),
            vec!["^/(.*)$ http://example.org/${1}"]
        );
        assert!(!f.contains_key("http_to_https"));
        let f = redirect_of(
            https(),
            json!({"hostname": "example.org"}),
            any.clone(),
            "https",
        );
        assert_eq!(f["host"].as_str(), Some("example.org"));

        // Paths.
        let f = redirect_of(
            http(),
            json!({"path": {"type": "ReplaceFullPath", "replaceFullPath": "/full"}}),
            any.clone(),
            "http",
        );
        assert_eq!(strings(&f["rules"]), vec!["^/ /full"]);
        let f = redirect_of(
            http(),
            json!({"path": {"type": "ReplacePrefixMatch", "replacePrefixMatch": "/new"}}),
            any.clone(),
            "http",
        );
        assert_eq!(strings(&f["rules"]), vec!["^/old(/.*)?$ /new${1}"]);
        let f = redirect_of(
            http(),
            json!({"path": {"type": "ReplacePrefixMatch", "replacePrefixMatch": ""}}),
            any,
            "http",
        );
        assert_eq!(
            strings(&f["rules"]),
            vec!["^/old/?$ /", "^/old/(.*)$ /${1}"]
        );
    }

    // ---- whole-output properties ----

    fn busy_world() -> World {
        let mut w = World::new()
            .gw(gateway(
                "ns",
                "gw",
                0,
                json!([
                    http_listener("http", 80, None),
                    https_listener("https", Some("*.example.com"), "cert"),
                ]),
            ))
            .gw(gateway(
                "ns",
                "gw2",
                3,
                json!([http_listener("alt", 8080, Some("alt.example.com"))]),
            ));
        w.secret("ns", "cert");
        for (i, name) in ["a", "b", "c"].iter().enumerate() {
            w = w.rt(route("ns", name, i as i64, json!({
                "parentRefs": [{"name": "gw"}, {"name": "gw2"}],
                "hostnames": [format!("{name}.example.com"), "alt.example.com"],
                "rules": [
                    {"matches": [{"path": {"value": format!("/{name}")}}],
                     "filters": [{"type": "RequestHeaderModifier", "requestHeaderModifier": {"set": [{"name": "x", "value": "1"}]}}],
                     "backendRefs": [backend("web", 80), {"name": "api", "port": 80, "weight": 2}]},
                    {"backendRefs": [backend("gone", 80)]},
                ]})));
        }
        w
    }

    fn summary(d: &Desired) -> String {
        format!(
            "{:?}\n{:?}\n{:?}\n{:?}\n{:?}\n{:?}",
            d.server_locations, d.locations, d.upstreams, d.plugins, d.certificates, d.status
        )
    }

    #[test]
    fn deterministic() {
        let w = busy_world();
        let a = w.run();
        assert_eq!(summary(&a), summary(&w.run()));
        let mut shuffled = busy_world();
        shuffled.routes.reverse();
        shuffled.gateways.reverse();
        assert_eq!(summary(&a), summary(&shuffled.run()));
        // Names do not depend on rule positions.
        assert!(a.locations.keys().all(|n| n.starts_with("k8s-ns-")));
        for (server, names) in &a.server_locations {
            let weights: Vec<u16> = names.iter().map(|n| a.locations[n].weight).collect();
            assert!(
                weights.windows(2).all(|w| w[0] > w[1]),
                "{server}: {weights:?}"
            );
        }
        assert_eq!(a.referenced, referenced(&w.input()));
    }
}

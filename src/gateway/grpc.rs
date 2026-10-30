//! GRPCRoutes in the terms of the HTTPRoute translation (pure, no I/O).
//!
//! A gRPC call is an HTTP/2 POST to `/<service>/<method>`, so a GRPCRoute is converted to
//! an HTTPRoute and translated like one, with its backends as HTTP/2 cleartext (h2c)
//! upstreams. Parent references, hostnames, header matches, filters and backendRefs have
//! the same shape in both kinds; method matches become path matches:
//!
//! | GRPCRoute method match            | path match                                    |
//! |-----------------------------------|-----------------------------------------------|
//! | Exact, service and method         | `Exact /<service>/<method>`                   |
//! | Exact, service only               | `PathPrefix /<service>/`                      |
//! | Exact, method only                | `RegularExpression /[^/]+/<method>`           |
//! | RegularExpression `s` and `m`     | `RegularExpression /(?:s)/(?:m)` (a missing one is `[^/]+`) |
//!
//! The HTTPRoute precedence (Exact path, then longer prefix, then regular expression,
//! then more header matches) then orders them as the GRPCRoute precedence wants: more
//! characters of a matching service first, then of a matching method, then more header
//! matches. Regular expressions use the `regex` crate dialect, as for HTTPRoutes.
//!
//! What cannot be converted drops the rule, with the reason in the route status:
//! `RequestMirror` filters (Pingap's `mirror` plugin copies requests with an HTTP/1.1
//! client, which an h2c gRPC backend does not accept) and method
//! values that are not valid gRPC names. Everything else is left to the HTTPRoute
//! translation, which refuses the same things for both kinds (regular expression header
//! matches, `ExtensionRef` filters, filters on backendRefs).

use regex::Regex;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::api::grpcroutes::{
    GrpcRoute, GrpcRouteRules, GrpcRouteRulesFiltersType, GrpcRouteRulesMatches,
    GrpcRouteRulesMatchesHeadersType, GrpcRouteRulesMatchesMethodType, GrpcRouteStatus,
};
use super::api::httproutes::{
    HttpRoute, HttpRouteRules, HttpRouteRulesMatches, HttpRouteRulesMatchesHeaders,
    HttpRouteRulesMatchesHeadersType, HttpRouteRulesMatchesPath, HttpRouteRulesMatchesPathType,
    HttpRouteSpec, HttpRouteStatus,
};

/// A GRPCRoute as an HTTPRoute.
#[derive(Clone, Debug)]
pub struct Converted {
    /// Same metadata, parentRefs and hostnames; one rule per GRPCRoute rule, in order.
    pub route: HttpRoute,
    /// Per rule: why it is dropped. A dropped rule keeps its backendRefs (they are still
    /// reported in `ResolvedRefs`) but nothing else.
    pub rule_errors: Vec<Option<String>>,
}

/// Converts a GRPCRoute; see the module documentation.
pub fn to_http_route(route: &GrpcRoute) -> Converted {
    let mut rules = Vec::new();
    let mut rule_errors = Vec::new();
    for rule in route.spec.rules.iter().flatten() {
        let (converted, error) = match convert_rule(rule) {
            Ok(converted) => (converted, None),
            Err(error) => (
                HttpRouteRules {
                    backend_refs: recast(&rule.backend_refs).ok().flatten(),
                    name: rule.name.clone(),
                    ..Default::default()
                },
                Some(error),
            ),
        };
        rules.push(converted);
        rule_errors.push(error);
    }
    let spec = HttpRouteSpec {
        hostnames: route.spec.hostnames.clone(),
        parent_refs: recast(&route.spec.parent_refs).unwrap_or_default(),
        rules: route.spec.rules.is_some().then_some(rules),
    };
    let mut http = HttpRoute::new(&route.metadata.name.clone().unwrap_or_default(), spec);
    http.metadata = route.metadata.clone();
    Converted {
        route: http,
        rule_errors,
    }
}

/// A route status in the GRPCRoute type (both kinds share `RouteStatus`).
pub fn grpc_status(status: &HttpRouteStatus) -> GrpcRouteStatus {
    recast(status).unwrap_or_default()
}

/// A stored GRPCRoute status in the HTTPRoute type, for the status merge.
pub fn http_status(status: &GrpcRouteStatus) -> HttpRouteStatus {
    recast(status).unwrap_or_default()
}

fn convert_rule(rule: &GrpcRouteRules) -> Result<HttpRouteRules, String> {
    if rule
        .filters
        .iter()
        .flatten()
        .any(|f| f.r#type == GrpcRouteRulesFiltersType::RequestMirror)
    {
        return Err(
            "RequestMirror filters are not supported on GRPCRoutes (Pingap mirrors over \
             HTTP/1.1 only)"
                .into(),
        );
    }
    let matches = match rule.matches.as_deref() {
        None => None,
        Some(list) => Some(
            list.iter()
                .map(convert_match)
                .collect::<Result<Vec<_>, String>>()?,
        ),
    };
    let invalid = |what: &str| format!("{what} could not be converted");
    Ok(HttpRouteRules {
        backend_refs: recast(&rule.backend_refs).map_err(|_| invalid("backendRefs"))?,
        filters: recast(&rule.filters).map_err(|_| invalid("filters"))?,
        matches,
        name: rule.name.clone(),
        ..Default::default()
    })
}

fn convert_match(m: &GrpcRouteRulesMatches) -> Result<HttpRouteRulesMatches, String> {
    let headers = m.headers.as_ref().map(|list| {
        list.iter()
            .map(|h| HttpRouteRulesMatchesHeaders {
                name: h.name.clone(),
                r#type: h.r#type.as_ref().map(|t| match t {
                    GrpcRouteRulesMatchesHeadersType::Exact => {
                        HttpRouteRulesMatchesHeadersType::Exact
                    }
                    GrpcRouteRulesMatchesHeadersType::RegularExpression => {
                        HttpRouteRulesMatchesHeadersType::RegularExpression
                    }
                }),
                value: h.value.clone(),
            })
            .collect()
    });
    let path = match m.method.as_ref() {
        None => None,
        Some(method) => {
            let service = method.service.as_deref().filter(|s| !s.is_empty());
            let name = method.method.as_deref().filter(|s| !s.is_empty());
            let regex = method.r#type == Some(GrpcRouteRulesMatchesMethodType::RegularExpression);
            method_path(service, name, regex)?
        }
    };
    Ok(HttpRouteRulesMatches {
        headers,
        method: None,
        path,
        query_params: None,
    })
}

/// The path match of a method match; `None` matches every path.
fn method_path(
    service: Option<&str>,
    method: Option<&str>,
    regex: bool,
) -> Result<Option<HttpRouteRulesMatchesPath>, String> {
    let path = |kind, value: String| {
        Some(HttpRouteRulesMatchesPath {
            r#type: Some(kind),
            value: Some(value),
        })
    };
    if regex {
        let part = |what: &str, value: Option<&str>| -> Result<String, String> {
            match value {
                None => Ok("[^/]+".into()),
                Some(v) => {
                    Regex::new(v).map_err(|e| format!("invalid {what} regular expression: {e}"))?;
                    Ok(format!("(?:{v})"))
                }
            }
        };
        if service.is_none() && method.is_none() {
            return Ok(None);
        }
        let value = format!("/{}/{}", part("service", service)?, part("method", method)?);
        return Ok(path(
            HttpRouteRulesMatchesPathType::RegularExpression,
            value,
        ));
    }
    if let Some(s) = service {
        check_service(s)?;
    }
    if let Some(m) = method {
        check_method(m)?;
    }
    Ok(match (service, method) {
        (Some(s), Some(m)) => path(HttpRouteRulesMatchesPathType::Exact, format!("/{s}/{m}")),
        (Some(s), None) => path(HttpRouteRulesMatchesPathType::PathPrefix, format!("/{s}/")),
        (None, Some(m)) => path(
            HttpRouteRulesMatchesPathType::RegularExpression,
            format!("/[^/]+/{}", regex::escape(m)),
        ),
        (None, None) => None,
    })
}

/// A gRPC service name as the CRD validates it: dot-separated identifiers.
fn check_service(service: &str) -> Result<(), String> {
    if service.split('.').all(is_identifier) {
        Ok(())
    } else {
        Err(format!("gRPC service {service:?} is not valid"))
    }
}

fn check_method(method: &str) -> Result<(), String> {
    if is_identifier(method) {
        Ok(())
    } else {
        Err(format!("gRPC method {method:?} is not valid"))
    }
}

/// `[A-Za-z_][A-Za-z_0-9]*`
fn is_identifier(part: &str) -> bool {
    let mut chars = part.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// The same JSON in another generated type (the two route kinds share these shapes).
fn recast<T: Serialize, U: DeserializeOwned>(value: &T) -> Result<U, serde_json::Error> {
    serde_json::from_value(serde_json::to_value(value)?)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::gateway::api::httproutes::HttpRouteRulesFiltersType;

    fn grpc_route(spec: serde_json::Value) -> GrpcRoute {
        serde_json::from_value(json!({
            "apiVersion": "gateway.networking.k8s.io/v1",
            "kind": "GRPCRoute",
            "metadata": {"namespace": "ns", "name": "g", "generation": 4},
            "spec": spec,
        }))
        .unwrap()
    }

    fn path_of(m: &HttpRouteRulesMatches) -> (HttpRouteRulesMatchesPathType, String) {
        let p = m.path.clone().unwrap();
        (p.r#type.unwrap(), p.value.unwrap())
    }

    #[test]
    fn keeps_metadata_parents_hostnames_and_backends() {
        let c = to_http_route(&grpc_route(json!({
            "parentRefs": [{"name": "gw", "sectionName": "h", "port": 80}],
            "hostnames": ["grpc.example.com"],
            "rules": [{
                "name": "r1",
                "backendRefs": [{"name": "svc", "port": 8080, "weight": 3, "namespace": "b"}],
            }],
        })));
        assert_eq!(c.route.metadata.name.as_deref(), Some("g"));
        assert_eq!(c.route.metadata.namespace.as_deref(), Some("ns"));
        assert_eq!(c.route.metadata.generation, Some(4));
        let parents = c.route.spec.parent_refs.unwrap();
        assert_eq!(parents[0].name, "gw");
        assert_eq!(parents[0].section_name.as_deref(), Some("h"));
        assert_eq!(parents[0].port, Some(80));
        assert_eq!(
            c.route.spec.hostnames,
            Some(vec!["grpc.example.com".into()])
        );
        let rules = c.route.spec.rules.unwrap();
        assert_eq!(c.rule_errors, vec![None]);
        assert_eq!(rules[0].name.as_deref(), Some("r1"));
        assert_eq!(rules[0].matches, None);
        let b = &rules[0].backend_refs.as_ref().unwrap()[0];
        assert_eq!(
            (b.name.as_str(), b.port, b.weight, b.namespace.as_deref()),
            ("svc", Some(8080), Some(3), Some("b"))
        );
    }

    #[test]
    fn method_matches_become_paths() {
        let c = to_http_route(&grpc_route(json!({
            "rules": [{"matches": [
                {"method": {"service": "pkg.Svc", "method": "Get"}},
                {"method": {"type": "Exact", "service": "pkg.Svc"}},
                {"method": {"method": "Get"}},
                {"method": {"type": "RegularExpression", "service": "pkg\\..*", "method": "Get|Put"}},
                {"method": {"type": "RegularExpression", "method": "Li.t"}},
                {"headers": [{"name": "version", "value": "two"}]},
            ]}],
        })));
        assert_eq!(c.rule_errors, vec![None]);
        let rules = c.route.spec.rules.unwrap();
        let m = rules[0].matches.as_ref().unwrap();
        use HttpRouteRulesMatchesPathType as T;
        assert_eq!(path_of(&m[0]), (T::Exact, "/pkg.Svc/Get".into()));
        assert_eq!(path_of(&m[1]), (T::PathPrefix, "/pkg.Svc/".into()));
        assert_eq!(path_of(&m[2]), (T::RegularExpression, "/[^/]+/Get".into()));
        assert_eq!(
            path_of(&m[3]),
            (T::RegularExpression, "/(?:pkg\\..*)/(?:Get|Put)".into())
        );
        assert_eq!(
            path_of(&m[4]),
            (T::RegularExpression, "/[^/]+/(?:Li.t)".into())
        );
        assert_eq!(m[5].path, None);
        let h = &m[5].headers.as_ref().unwrap()[0];
        assert_eq!((h.name.as_str(), h.value.as_str()), ("version", "two"));
        assert!(
            m.iter()
                .all(|m| m.method.is_none() && m.query_params.is_none())
        );
    }

    #[test]
    fn header_types_and_filters_carry_over() {
        let c = to_http_route(&grpc_route(json!({
            "rules": [{
                "matches": [{"headers": [{"type": "RegularExpression", "name": "a", "value": "b.*"}]}],
                "filters": [
                    {"type": "RequestHeaderModifier", "requestHeaderModifier": {
                        "set": [{"name": "x-set", "value": "1"}], "remove": ["x-rm"]}},
                    {"type": "ResponseHeaderModifier", "responseHeaderModifier": {
                        "add": [{"name": "x-add", "value": "2"}]}},
                ],
            }],
        })));
        assert_eq!(c.rule_errors, vec![None]);
        let rules = c.route.spec.rules.unwrap();
        let h = &rules[0].matches.as_ref().unwrap()[0]
            .headers
            .as_ref()
            .unwrap()[0];
        assert_eq!(
            h.r#type,
            Some(HttpRouteRulesMatchesHeadersType::RegularExpression)
        );
        let filters = rules[0].filters.as_ref().unwrap();
        assert_eq!(
            filters[0].r#type,
            HttpRouteRulesFiltersType::RequestHeaderModifier
        );
        let set = filters[0].request_header_modifier.as_ref().unwrap();
        assert_eq!(set.set.as_ref().unwrap()[0].name, "x-set");
        assert_eq!(set.remove, Some(vec!["x-rm".to_string()]));
        assert_eq!(
            filters[1].r#type,
            HttpRouteRulesFiltersType::ResponseHeaderModifier
        );
    }

    #[test]
    fn unconvertible_rules_keep_only_their_backends() {
        let c = to_http_route(&grpc_route(json!({
            "rules": [
                {
                    "matches": [{"method": {"service": "a/b", "method": "M"}}],
                    "backendRefs": [{"name": "svc", "port": 8080}],
                },
                {
                    "filters": [{"type": "RequestMirror", "requestMirror": {
                        "backendRef": {"name": "m", "port": 80}}}],
                    "backendRefs": [{"name": "svc", "port": 8080}],
                },
                {"matches": [{"method": {"type": "RegularExpression", "method": "("}}]},
                {"matches": [{"method": {"service": "ok.Svc", "method": "1x"}}]},
            ],
        })));
        let errors: Vec<String> = c.rule_errors.iter().map(|e| e.clone().unwrap()).collect();
        assert!(errors[0].contains("service \"a/b\""), "{errors:?}");
        assert!(errors[1].contains("RequestMirror"), "{errors:?}");
        assert!(
            errors[2].contains("method regular expression"),
            "{errors:?}"
        );
        assert!(errors[3].contains("method \"1x\""), "{errors:?}");
        let rules = c.route.spec.rules.unwrap();
        assert_eq!(rules.len(), 4);
        assert_eq!(rules[0].matches, None);
        assert_eq!(rules[1].filters, None);
        assert_eq!(rules[0].backend_refs.as_ref().unwrap()[0].name, "svc");
    }

    #[test]
    fn statuses_convert_both_ways() {
        let status: HttpRouteStatus = serde_json::from_value(json!({
            "parents": [{
                "controllerName": "x.io/c",
                "parentRef": {"name": "gw", "sectionName": "h"},
                "conditions": [{
                    "type": "Accepted", "status": "True", "reason": "Accepted",
                    "message": "", "lastTransitionTime": "2026-01-01T00:00:00Z",
                }],
            }],
        }))
        .unwrap();
        let grpc = grpc_status(&status);
        assert_eq!(grpc.parents[0].controller_name, "x.io/c");
        assert_eq!(
            grpc.parents[0].parent_ref.section_name.as_deref(),
            Some("h")
        );
        assert_eq!(http_status(&grpc), status);
    }
}

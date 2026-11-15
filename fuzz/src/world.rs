//! Gateway API objects built from fuzzer bytes. Values come from small pools (so names
//! and references meet) with arbitrary strings now and then; every object is built as
//! JSON and read with serde, as the Kubernetes watches read them. Objects that do not
//! deserialize are left out.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arbitrary::{Result, Unstructured};
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Secret, Service};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use pingap_k8s::config::GatewayGroup;
use pingap_k8s::gateway::api::backendtlspolicies::BackendTlsPolicy;
use pingap_k8s::gateway::api::gatewayclasses::GatewayClass;
use pingap_k8s::gateway::api::gateways::Gateway;
use pingap_k8s::gateway::api::grpcroutes::GrpcRoute;
use pingap_k8s::gateway::api::httproutes::HttpRoute;
use pingap_k8s::gateway::api::listenersets::ListenerSet;
use pingap_k8s::gateway::api::referencegrants::ReferenceGrant;
use pingap_k8s::gateway::model::{ObjectKey, ServerInfo, TranslateInput, TranslateOptions};
use pingap_k8s::types::{IpFamily, ServiceKey, UpstreamOptions, UpstreamTarget};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{list, maybe, pick, rarely, some, text};

pub const CONTROLLER: &str = "pingap.io/gateway-controller";
const GROUP: &str = "gateway.networking.k8s.io";

const NAMESPACES: &[&str] = &["ns", "other", "third"];
const GATEWAYS: &[&str] = &["gw", "gw2", "gw3"];
const LISTENER_SETS: &[&str] = &["ls", "ls2"];
const ROUTES: &[&str] = &["r1", "r2", "r3", "r4"];
const SERVICES: &[&str] = &["web", "api", "grpc", "ext", "tls", "missing"];
const SECRETS: &[&str] = &["cert", "wild", "bad", "missing"];
const CONFIG_MAPS: &[&str] = &["ca", "bad-ca", "missing"];
const LISTENER_NAMES: &[&str] = &["http", "https", "h2", "tls", "extra"];
const HOSTS: &[&str] = &[
    "a.example.com",
    "b.example.com",
    "*.example.com",
    "example.com",
    "x.a.example.com",
    "*.a.example.com",
    "other.test",
    "A.Example.com",
    "*",
    "",
    "a..example.com",
    "-a.example.com",
    "a.example.com.",
    "1.2.3.4",
    "*.*.example.com",
    "a_b.example.com",
];
const PATHS: &[&str] = &[
    "/", "/a", "/a/", "/a/b", "/b", "", "/a b", "/.*", "/(a|b)", "/[", "//", "/%2F", "/a?x=1",
    "/a#f", "a", "/\u{e9}", "/a\\b", "/$",
];
const HEADER_NAMES: &[&str] = &[
    "x-a",
    "X-A",
    "x-b",
    "host",
    "Content-Type",
    ":authority",
    "bad name",
    "",
    "x:y",
];
const HEADER_VALUES: &[&str] = &["v", "", "a b", "a\nb", "\"q\"", "a:b", "a,b", "*"];
const DURATIONS: &[&str] = &[
    "1s", "10ms", "0s", "1h", "500ms", "1m30s", "bad", "", "2s", "100h",
];
const PORTS: &[i32] = &[80, 443, 8080, 8443, 9000, 0, -1, 65535, 70000];

/// A world to translate, with everything [`TranslateInput`] borrows.
#[derive(Clone)]
pub struct World {
    pub classes: Vec<Arc<GatewayClass>>,
    pub gateways: Vec<Arc<Gateway>>,
    pub routes: Vec<Arc<HttpRoute>>,
    pub grpc_routes: Vec<Arc<GrpcRoute>>,
    pub listener_sets: Vec<Arc<ListenerSet>>,
    pub grants: Vec<Arc<ReferenceGrant>>,
    pub namespaces: BTreeMap<String, Arc<Namespace>>,
    pub services: BTreeMap<ServiceKey, Arc<Service>>,
    pub secrets: BTreeMap<ObjectKey, Arc<Secret>>,
    pub policies: Vec<Arc<BackendTlsPolicy>>,
    pub config_maps: BTreeMap<ObjectKey, Arc<ConfigMap>>,
    /// Hand-made plugins stored in Pingap (built-ins are added by the caller).
    pub plugins: BTreeSet<String>,
    pub options: TranslateOptions,
}

impl World {
    pub fn input<'a>(
        &'a self,
        servers: &'a [ServerInfo],
        plugins: &'a BTreeSet<String>,
    ) -> TranslateInput<'a> {
        TranslateInput {
            classes: &self.classes,
            gateways: &self.gateways,
            routes: &self.routes,
            grpc_routes: &self.grpc_routes,
            listener_sets: &self.listener_sets,
            grants: &self.grants,
            namespaces: &self.namespaces,
            services: &self.services,
            secrets: &self.secrets,
            policies: &self.policies,
            config_maps: &self.config_maps,
            servers,
            plugins,
            options: &self.options,
        }
    }

    /// The same world with every list reversed: the translation must not depend on the
    /// order of the lists.
    pub fn reversed(&self) -> World {
        let mut w = self.clone();
        w.classes.reverse();
        w.gateways.reverse();
        w.routes.reverse();
        w.grpc_routes.reverse();
        w.listener_sets.reverse();
        w.grants.reverse();
        w.policies.reverse();
        w
    }

    /// The world after some objects were deleted (`keep` decides per object).
    pub fn subset(&self, u: &mut Unstructured<'_>) -> Result<World> {
        let mut w = self.clone();
        let keep = |u: &mut Unstructured<'_>| -> Result<bool> { Ok(!rarely(u, 3)?) };
        macro_rules! filter {
            ($field:ident) => {{
                let mut kept = Vec::new();
                for item in w.$field.drain(..) {
                    if keep(u)? {
                        kept.push(item);
                    }
                }
                w.$field = kept;
            }};
        }
        filter!(gateways);
        filter!(routes);
        filter!(grpc_routes);
        filter!(listener_sets);
        filter!(grants);
        filter!(policies);
        let mut services = BTreeMap::new();
        for (k, v) in std::mem::take(&mut w.services) {
            if keep(u)? {
                services.insert(k, v);
            }
        }
        w.services = services;
        Ok(w)
    }
}

fn from_json<T: DeserializeOwned>(value: Value) -> Option<T> {
    serde_json::from_value(value).ok()
}

/// Keeps the first object of every namespace/name, as the API server does.
fn unique<T>(items: Vec<(String, String, T)>) -> Vec<Arc<T>> {
    let mut seen = BTreeSet::new();
    items
        .into_iter()
        .filter(|(ns, name, _)| seen.insert((ns.clone(), name.clone())))
        .map(|(_, _, item)| Arc::new(item))
        .collect()
}

fn metadata(u: &mut Unstructured<'_>, ns: &str, name: &str) -> Result<Value> {
    let age: u16 = u.arbitrary()?;
    let created = k8s_openapi::jiff::Timestamp::from_second(1_700_000_000 + i64::from(age % 64))
        .expect("in range")
        .to_string();
    Ok(json!({
        "namespace": ns,
        "name": name,
        "generation": u.int_in_range(1..=3)?,
        "creationTimestamp": created,
    }))
}

fn insert(object: &mut Value, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        object[key] = value;
    }
}

fn host(u: &mut Unstructured<'_>) -> Result<String> {
    text(u, HOSTS)
}

fn header(u: &mut Unstructured<'_>) -> Result<Value> {
    Ok(json!({"name": text(u, HEADER_NAMES)?, "value": text(u, HEADER_VALUES)?}))
}

fn header_modifier(u: &mut Unstructured<'_>) -> Result<Value> {
    let mut m = json!({});
    insert(&mut m, "set", maybe(u, |u| Ok(json!(list(u, 3, header)?)))?);
    insert(&mut m, "add", maybe(u, |u| Ok(json!(list(u, 3, header)?)))?);
    insert(
        &mut m,
        "remove",
        maybe(u, |u| Ok(json!(list(u, 3, |u| text(u, HEADER_NAMES))?)))?,
    );
    Ok(m)
}

fn path_modifier(u: &mut Unstructured<'_>) -> Result<Value> {
    Ok(if u.arbitrary()? {
        json!({"type": "ReplaceFullPath", "replaceFullPath": text(u, PATHS)?})
    } else {
        json!({"type": "ReplacePrefixMatch", "replacePrefixMatch": text(u, PATHS)?})
    })
}

fn backend_ref(u: &mut Unstructured<'_>) -> Result<Value> {
    let mut b = json!({"name": text(u, SERVICES)?});
    if !rarely(u, 8)? {
        b["port"] = json!(pick(u, PORTS)?);
    }
    insert(
        &mut b,
        "namespace",
        maybe(u, |u| Ok(json!(text(u, NAMESPACES)?)))?,
    );
    insert(
        &mut b,
        "weight",
        maybe(u, |u| Ok(json!(pick(u, &[0, 1, 2, 10, 1000, 1_000_000])?)))?,
    );
    if rarely(u, 10)? {
        b["kind"] = json!(text(u, &["Service", "ConfigMap", "Backend"])?);
        b["group"] = json!(text(u, &["", "core", "x.io"])?);
    }
    Ok(b)
}

fn filter(u: &mut Unstructured<'_>, grpc: bool) -> Result<Value> {
    let kinds: &[&str] = if grpc {
        &[
            "RequestHeaderModifier",
            "ResponseHeaderModifier",
            "RequestMirror",
            "ExtensionRef",
        ]
    } else {
        &[
            "RequestHeaderModifier",
            "ResponseHeaderModifier",
            "RequestMirror",
            "RequestRedirect",
            "URLRewrite",
            "ExtensionRef",
            "CORS",
        ]
    };
    let kind: &str = u.choose(kinds)?;
    let mut f = json!({"type": kind});
    match kind {
        "RequestHeaderModifier" => f["requestHeaderModifier"] = header_modifier(u)?,
        "ResponseHeaderModifier" => f["responseHeaderModifier"] = header_modifier(u)?,
        "RequestMirror" => {
            let mut m = json!({"backendRef": backend_ref(u)?});
            insert(
                &mut m,
                "percent",
                maybe(u, |u| Ok(json!(pick(u, &[0, 1, 50, 100, 101])?)))?,
            );
            if rarely(u, 6)? {
                m["fraction"] = json!({
                    "numerator": pick(u, &[0, 1, 3, 10])?,
                    "denominator": pick(u, &[1, 3, 100, 0])?,
                });
            }
            f["requestMirror"] = m;
        }
        "RequestRedirect" => {
            let mut r = json!({});
            insert(
                &mut r,
                "scheme",
                maybe(u, |u| Ok(json!(pick(u, &["http", "https"])?)))?,
            );
            insert(&mut r, "hostname", maybe(u, |u| Ok(json!(host(u)?)))?);
            insert(&mut r, "path", maybe(u, path_modifier)?);
            insert(&mut r, "port", maybe(u, |u| Ok(json!(pick(u, PORTS)?)))?);
            insert(
                &mut r,
                "statusCode",
                maybe(u, |u| Ok(json!(pick(u, &[301, 302, 303, 307, 308])?)))?,
            );
            f["requestRedirect"] = r;
        }
        "URLRewrite" => {
            let mut r = json!({});
            insert(&mut r, "hostname", maybe(u, |u| Ok(json!(host(u)?)))?);
            insert(&mut r, "path", maybe(u, path_modifier)?);
            f["urlRewrite"] = r;
        }
        "ExtensionRef" => {
            f["extensionRef"] = json!({
                "group": text(u, &["", "pingap.io"])?,
                "kind": text(u, &["Plugin"])?,
                "name": text(u, &["my-plugin", "missing"])?,
            });
        }
        _ => {
            let mut c = json!({});
            let origins = &[
                "https://a.example.com",
                "*",
                "http://*.example.com",
                "bad",
                "",
            ];
            insert(
                &mut c,
                "allowOrigins",
                maybe(u, |u| Ok(json!(list(u, 3, |u| text(u, origins))?)))?,
            );
            insert(
                &mut c,
                "allowMethods",
                maybe(u, |u| {
                    Ok(json!(list(u, 3, |u| text(
                        u,
                        &["GET", "POST", "*", "get"]
                    ))?))
                })?,
            );
            insert(
                &mut c,
                "allowHeaders",
                maybe(u, |u| Ok(json!(list(u, 3, |u| text(u, HEADER_NAMES))?)))?,
            );
            insert(
                &mut c,
                "exposeHeaders",
                maybe(u, |u| Ok(json!(list(u, 3, |u| text(u, HEADER_NAMES))?)))?,
            );
            insert(
                &mut c,
                "maxAge",
                maybe(u, |u| Ok(json!(pick(u, &[0, 5, 86400, -1])?)))?,
            );
            insert(
                &mut c,
                "allowCredentials",
                maybe(u, |u| Ok(json!(u.arbitrary::<bool>()?)))?,
            );
            f["cors"] = c;
        }
    }
    Ok(f)
}

fn parent_ref(u: &mut Unstructured<'_>) -> Result<Value> {
    let mut p = if rarely(u, 4)? {
        json!({"group": GROUP, "kind": "ListenerSet", "name": text(u, LISTENER_SETS)?})
    } else {
        json!({"name": text(u, GATEWAYS)?})
    };
    insert(
        &mut p,
        "namespace",
        maybe(u, |u| Ok(json!(text(u, NAMESPACES)?)))?,
    );
    insert(
        &mut p,
        "sectionName",
        maybe(u, |u| Ok(json!(text(u, LISTENER_NAMES)?)))?,
    );
    if rarely(u, 6)? {
        p["port"] = json!(pick(u, PORTS)?);
    }
    if rarely(u, 12)? {
        p["kind"] = json!(text(u, &["Gateway", "Service", "ListenerSet"])?);
    }
    Ok(p)
}

fn http_match(u: &mut Unstructured<'_>) -> Result<Value> {
    let mut m = json!({});
    insert(
        &mut m,
        "path",
        maybe(u, |u| {
            Ok(json!({
                "type": pick(u, &["Exact", "PathPrefix", "RegularExpression"])?,
                "value": text(u, PATHS)?,
            }))
        })?,
    );
    let typed = |u: &mut Unstructured<'_>| -> Result<Value> {
        let mut h = header(u)?;
        insert(
            &mut h,
            "type",
            maybe(u, |u| Ok(json!(pick(u, &["Exact", "RegularExpression"])?)))?,
        );
        Ok(h)
    };
    insert(
        &mut m,
        "headers",
        maybe(u, |u| Ok(json!(list(u, 3, typed)?)))?,
    );
    insert(
        &mut m,
        "queryParams",
        maybe(u, |u| Ok(json!(list(u, 2, typed)?)))?,
    );
    insert(
        &mut m,
        "method",
        maybe(u, |u| {
            Ok(json!(pick(
                u,
                &["GET", "POST", "HEAD", "PUT", "DELETE", "OPTIONS"]
            )?))
        })?,
    );
    Ok(m)
}

fn grpc_match(u: &mut Unstructured<'_>) -> Result<Value> {
    let mut m = json!({});
    insert(
        &mut m,
        "method",
        maybe(u, |u| {
            let mut method = json!({});
            insert(
                &mut method,
                "type",
                maybe(u, |u| Ok(json!(pick(u, &["Exact", "RegularExpression"])?)))?,
            );
            insert(
                &mut method,
                "service",
                maybe(u, |u| {
                    Ok(json!(text(
                        u,
                        &["a.B", "pkg.v1.Svc", "", ".*", "a b", "a/b"]
                    )?))
                })?,
            );
            insert(
                &mut method,
                "method",
                maybe(u, |u| {
                    Ok(json!(text(u, &["Get", "List", "", ".*", "a/b"])?))
                })?,
            );
            Ok(method)
        })?,
    );
    insert(
        &mut m,
        "headers",
        maybe(u, |u| Ok(json!(list(u, 2, header)?)))?,
    );
    Ok(m)
}

fn rule(u: &mut Unstructured<'_>, grpc: bool) -> Result<Value> {
    let mut r = json!({});
    let matches = if grpc {
        list(u, 3, grpc_match)?
    } else {
        list(u, 3, http_match)?
    };
    if !matches.is_empty() {
        r["matches"] = json!(matches);
    }
    insert(
        &mut r,
        "filters",
        maybe(u, |u| Ok(json!(list(u, 3, |u| filter(u, grpc))?)))?,
    );
    let backend = |u: &mut Unstructured<'_>| -> Result<Value> {
        let mut b = backend_ref(u)?;
        insert(
            &mut b,
            "filters",
            maybe(u, |u| Ok(json!(list(u, 2, |u| filter(u, grpc))?)))?,
        );
        Ok(b)
    };
    if !rarely(u, 6)? {
        r["backendRefs"] = json!(some(u, 3, backend)?);
    }
    insert(
        &mut r,
        "name",
        maybe(u, |u| Ok(json!(text(u, &["rule-a", "rule-b"])?)))?,
    );
    if !grpc {
        insert(
            &mut r,
            "timeouts",
            maybe(u, |u| {
                let mut t = json!({});
                insert(
                    &mut t,
                    "request",
                    maybe(u, |u| Ok(json!(text(u, DURATIONS)?)))?,
                );
                insert(
                    &mut t,
                    "backendRequest",
                    maybe(u, |u| Ok(json!(text(u, DURATIONS)?)))?,
                );
                Ok(t)
            })?,
        );
        insert(
            &mut r,
            "retry",
            maybe(u, |u| {
                let mut t = json!({});
                insert(
                    &mut t,
                    "attempts",
                    maybe(u, |u| Ok(json!(pick(u, &[0, 1, 2, 300, -1])?)))?,
                );
                insert(
                    &mut t,
                    "backoff",
                    maybe(u, |u| Ok(json!(text(u, DURATIONS)?)))?,
                );
                insert(
                    &mut t,
                    "codes",
                    maybe(u, |u| {
                        Ok(json!(list(u, 3, |u| pick(
                            u,
                            &[500, 502, 503, 504, 404, 0]
                        ))?))
                    })?,
                );
                Ok(t)
            })?,
        );
    }
    Ok(r)
}

fn route_value(u: &mut Unstructured<'_>, grpc: bool) -> Result<(String, String, Value)> {
    let ns = pick(u, NAMESPACES)?.to_string();
    let name = pick(u, ROUTES)?.to_string();
    let mut spec = json!({"parentRefs": some(u, 3, parent_ref)?});
    insert(
        &mut spec,
        "hostnames",
        maybe(u, |u| Ok(json!(list(u, 3, host)?)))?,
    );
    spec["rules"] = json!(some(u, 3, |u| rule(u, grpc))?);
    let kind = if grpc { "GRPCRoute" } else { "HTTPRoute" };
    let value = json!({
        "apiVersion": format!("{GROUP}/v1"),
        "kind": kind,
        "metadata": metadata(u, &ns, &name)?,
        "spec": spec,
    });
    Ok((ns, name, value))
}

fn selector(u: &mut Unstructured<'_>) -> Result<Value> {
    let mut s = json!({});
    insert(
        &mut s,
        "matchLabels",
        maybe(u, |u| Ok(json!({"team": text(u, &["a", "b", ""])?})))?,
    );
    insert(
        &mut s,
        "matchExpressions",
        maybe(u, |u| {
            Ok(json!(list(u, 2, |u| {
                Ok(json!({
                    "key": text(u, &["team", "kubernetes.io/metadata.name"])?,
                    "operator": text(u, &["In", "NotIn", "Exists", "DoesNotExist"])?,
                    "values": list(u, 2, |u| text(u, &["a", "b", "ns", "other"]))?,
                }))
            })?))
        })?,
    );
    Ok(s)
}

fn allowed_routes(u: &mut Unstructured<'_>) -> Result<Value> {
    let mut a = json!({});
    insert(
        &mut a,
        "namespaces",
        maybe(u, |u| {
            let mut n = json!({"from": text(u, &["Same", "All", "Selector", "None"])?});
            insert(&mut n, "selector", maybe(u, selector)?);
            Ok(n)
        })?,
    );
    insert(
        &mut a,
        "kinds",
        maybe(u, |u| {
            Ok(json!(list(u, 2, |u| {
                let mut k = json!({"kind": text(u, &["HTTPRoute", "GRPCRoute", "TCPRoute"])?});
                insert(
                    &mut k,
                    "group",
                    maybe(u, |u| Ok(json!(text(u, &[GROUP, "", "x.io"])?)))?,
                );
                Ok(k)
            })?))
        })?,
    );
    Ok(a)
}

fn listener(u: &mut Unstructured<'_>) -> Result<Value> {
    let protocol = text(u, &["HTTP", "HTTPS", "TLS", "TCP", "UDP", "HTTP", "HTTPS"])?;
    let mut l = json!({
        "name": text(u, LISTENER_NAMES)?,
        "port": pick(u, PORTS)?,
        "protocol": protocol,
    });
    insert(&mut l, "hostname", maybe(u, |u| Ok(json!(host(u)?)))?);
    if protocol != "HTTP" || rarely(u, 8)? {
        let mut tls = json!({});
        insert(
            &mut tls,
            "mode",
            maybe(u, |u| Ok(json!(text(u, &["Terminate", "Passthrough"])?)))?,
        );
        let refs = list(u, 2, |u| {
            let mut r = json!({"name": text(u, SECRETS)?});
            insert(
                &mut r,
                "namespace",
                maybe(u, |u| Ok(json!(text(u, NAMESPACES)?)))?,
            );
            if rarely(u, 8)? {
                r["kind"] = json!(text(u, &["Secret", "ConfigMap"])?);
                r["group"] = json!(text(u, &["", "core"])?);
            }
            Ok(r)
        })?;
        tls["certificateRefs"] = json!(refs);
        l["tls"] = tls;
    }
    insert(&mut l, "allowedRoutes", maybe(u, allowed_routes)?);
    Ok(l)
}

fn gateway_value(u: &mut Unstructured<'_>) -> Result<(String, String, Value)> {
    let ns = pick(u, NAMESPACES)?.to_string();
    let name = pick(u, GATEWAYS)?.to_string();
    let mut spec = json!({
        "gatewayClassName": text(u, &["pingap", "pingap", "other", "missing"])?,
        "listeners": some(u, 4, listener)?,
    });
    insert(
        &mut spec,
        "addresses",
        maybe(u, |u| {
            Ok(json!(list(u, 2, |u| {
                let mut a = json!({"value": text(u, &["192.0.2.1", "192.0.2.9", "lb.example.com", "", "::1"])?});
                insert(
                    &mut a,
                    "type",
                    maybe(u, |u| {
                        Ok(json!(text(u, &["IPAddress", "Hostname", "x.io/Named"])?))
                    })?,
                );
                Ok(a)
            })?))
        })?,
    );
    insert(
        &mut spec,
        "allowedListeners",
        maybe(u, |u| {
            let mut n = json!({"from": text(u, &["Same", "All", "Selector", "None"])?});
            insert(&mut n, "selector", maybe(u, selector)?);
            Ok(json!({"namespaces": n}))
        })?,
    );
    if rarely(u, 6)? {
        let mut r = json!({"name": text(u, SECRETS)?});
        insert(
            &mut r,
            "namespace",
            maybe(u, |u| Ok(json!(text(u, NAMESPACES)?)))?,
        );
        spec["tls"] = json!({"backend": {"clientCertificateRef": r}});
    }
    let value = json!({
        "apiVersion": format!("{GROUP}/v1"),
        "kind": "Gateway",
        "metadata": metadata(u, &ns, &name)?,
        "spec": spec,
    });
    Ok((ns, name, value))
}

fn listener_set_value(u: &mut Unstructured<'_>) -> Result<(String, String, Value)> {
    let ns = pick(u, NAMESPACES)?.to_string();
    let name = pick(u, LISTENER_SETS)?.to_string();
    let mut parent = json!({"name": text(u, GATEWAYS)?});
    insert(
        &mut parent,
        "namespace",
        maybe(u, |u| Ok(json!(text(u, NAMESPACES)?)))?,
    );
    if rarely(u, 8)? {
        parent["kind"] = json!(text(u, &["Gateway", "Service"])?);
        parent["group"] = json!(text(u, &[GROUP, ""])?);
    }
    let value = json!({
        "apiVersion": format!("{GROUP}/v1"),
        "kind": "ListenerSet",
        "metadata": metadata(u, &ns, &name)?,
        "spec": {"parentRef": parent, "listeners": some(u, 3, listener)?},
    });
    Ok((ns, name, value))
}

fn grant_value(u: &mut Unstructured<'_>) -> Result<(String, String, Value)> {
    let ns = pick(u, NAMESPACES)?.to_string();
    let name = pick(u, &["g1", "g2"])?.to_string();
    let from = list(u, 2, |u| {
        Ok(json!({
            "group": text(u, &[GROUP, ""])?,
            "kind": text(u, &["HTTPRoute", "GRPCRoute", "Gateway", "ListenerSet"])?,
            "namespace": text(u, NAMESPACES)?,
        }))
    })?;
    let to = list(u, 2, |u| {
        let mut t =
            json!({"group": text(u, &["", "core"])?, "kind": text(u, &["Service", "Secret"])?});
        insert(
            &mut t,
            "name",
            maybe(u, |u| Ok(json!(text(u, &["web", "api", "cert", "wild"])?)))?,
        );
        Ok(t)
    })?;
    let value = json!({
        "apiVersion": format!("{GROUP}/v1"),
        "kind": "ReferenceGrant",
        "metadata": {"namespace": ns, "name": name},
        "spec": {"from": from, "to": to},
    });
    Ok((ns, name, value))
}

fn policy_value(u: &mut Unstructured<'_>) -> Result<(String, String, Value)> {
    let ns = pick(u, NAMESPACES)?.to_string();
    let name = pick(u, &["p1", "p2"])?.to_string();
    let targets = list(u, 2, |u| {
        let mut t = json!({"group": text(u, &["", "core"])?, "kind": text(u, &["Service", "Gateway"])?, "name": text(u, SERVICES)?});
        insert(
            &mut t,
            "sectionName",
            maybe(u, |u| Ok(json!(text(u, &["https", "http", "grpc"])?)))?,
        );
        Ok(t)
    })?;
    let mut validation = json!({"hostname": host(u)?});
    insert(
        &mut validation,
        "caCertificateRefs",
        maybe(u, |u| {
            Ok(json!(list(u, 2, |u| {
                Ok(
                    json!({"group": text(u, &["", "core"])?, "kind": text(u, &["ConfigMap", "Secret"])?, "name": text(u, CONFIG_MAPS)?}),
                )
            })?))
        })?,
    );
    insert(
        &mut validation,
        "wellKnownCACertificates",
        maybe(u, |u| Ok(json!(text(u, &["System", "Other"])?)))?,
    );
    insert(
        &mut validation,
        "subjectAltNames",
        maybe(u, |u| {
            Ok(json!(list(u, 2, |u| {
                Ok(if u.arbitrary()? {
                    json!({"type": "Hostname", "hostname": host(u)?})
                } else {
                    json!({"type": "URI", "uri": text(u, &["spiffe://a/b", "bad uri", ""])?})
                })
            })?))
        })?,
    );
    let value = json!({
        "apiVersion": format!("{GROUP}/v1"),
        "kind": "BackendTLSPolicy",
        "metadata": metadata(u, &ns, &name)?,
        "spec": {"targetRefs": targets, "validation": validation},
    });
    Ok((ns, name, value))
}

fn service_value(u: &mut Unstructured<'_>, ns: &str, name: &str) -> Result<Value> {
    let ports = some(u, 3, |u| {
        let mut p = json!({"port": pick(u, &[80, 443, 8080, 9000, 0])?});
        insert(
            &mut p,
            "name",
            maybe(u, |u| Ok(json!(text(u, &["http", "https", "grpc"])?)))?,
        );
        insert(
            &mut p,
            "appProtocol",
            maybe(u, |u| {
                Ok(json!(text(
                    u,
                    &[
                        "http",
                        "kubernetes.io/h2c",
                        "grpc",
                        "https",
                        "kubernetes.io/wss",
                        "kubernetes.io/ws",
                        "tcp"
                    ],
                )?))
            })?,
        );
        Ok(p)
    })?;
    let mut spec = json!({"ports": ports});
    if rarely(u, 4)? {
        spec["type"] = json!("ExternalName");
        insert(
            &mut spec,
            "externalName",
            maybe(u, |u| {
                Ok(json!(text(
                    u,
                    &[
                        "backend.example.com",
                        "backend.example.com.",
                        "192.0.2.7",
                        "2001:db8::7",
                        ""
                    ]
                )?))
            })?,
        );
    } else if rarely(u, 4)? {
        spec["clusterIP"] = json!("None");
    }
    Ok(json!({"metadata": {"namespace": ns, "name": name}, "spec": spec}))
}

/// Self-signed certificates and their keys, made once: for `*.example.com` and
/// `a.example.com`, and for `example.com`.
fn certificates() -> &'static [(String, String); 2] {
    static CERTS: OnceLock<[(String, String); 2]> = OnceLock::new();
    CERTS.get_or_init(|| {
        let make = |names: &[&str]| {
            let c = rcgen::generate_simple_self_signed(
                names.iter().map(|n| n.to_string()).collect::<Vec<_>>(),
            )
            .expect("self-signed certificate");
            (c.cert.pem(), c.signing_key.serialize_pem())
        };
        [
            make(&["*.example.com", "a.example.com"]),
            make(&["example.com"]),
        ]
    })
}

fn secret(ns: &str, name: &str, kind: u8) -> Secret {
    let [first, second] = certificates();
    let (cert, key) = match kind % 4 {
        0 => (first.0.clone(), first.1.clone()),
        1 => (second.0.clone(), second.1.clone()),
        2 => (first.0.clone(), second.1.clone()),
        _ => ("not a certificate".to_string(), String::new()),
    };
    let mut data = BTreeMap::new();
    data.insert("tls.crt".to_string(), ByteString(cert.into_bytes()));
    data.insert("tls.key".to_string(), ByteString(key.into_bytes()));
    Secret {
        metadata: ObjectMeta {
            namespace: Some(ns.into()),
            name: Some(name.into()),
            ..Default::default()
        },
        type_: Some("kubernetes.io/tls".into()),
        data: Some(data),
        ..Default::default()
    }
}

fn config_map(ns: &str, name: &str, valid: bool) -> ConfigMap {
    let ca = if valid {
        certificates()[0].0.clone()
    } else {
        "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n".to_string()
    };
    ConfigMap {
        metadata: ObjectMeta {
            namespace: Some(ns.into()),
            name: Some(name.into()),
            ..Default::default()
        },
        data: Some(BTreeMap::from([("ca.crt".to_string(), ca)])),
        ..Default::default()
    }
}

fn namespace(name: &str, team: &str) -> Arc<Namespace> {
    Arc::new(
        from_json(json!({"metadata": {"name": name, "labels": {
            "kubernetes.io/metadata.name": name, "team": team,
        }}}))
        .expect("namespace"),
    )
}

fn options(u: &mut Unstructured<'_>) -> Result<TranslateOptions> {
    let target = if rarely(u, 4)? {
        UpstreamTarget::Service {
            cluster_domain: "cluster.local".into(),
        }
    } else {
        UpstreamTarget::Pods
    };
    let mut groups = Vec::new();
    if rarely(u, 4)? {
        groups.push(GatewayGroup {
            name: "own".into(),
            servers: vec!["own".into()],
            addresses: vec!["192.0.2.50".into()],
            ports: BTreeMap::from([(80, 10080)]),
        });
    }
    let servers = if rarely(u, 4)? {
        vec!["http".to_string(), "https".to_string()]
    } else {
        Vec::new()
    };
    Ok(TranslateOptions {
        controller_name: CONTROLLER.into(),
        external_name: u.arbitrary()?,
        upstream_defaults: UpstreamOptions {
            include_not_ready: false,
            drain_terminating: true,
            ip_family: IpFamily::Primary,
            external_name: false,
            debounce: Duration::from_millis(100),
            debounce_max: Duration::from_secs(1),
            target,
            spread_weights: false,
        },
        health_check: if rarely(u, 4)? {
            Some("http://web/ping".into())
        } else {
            None
        },
        cluster_domain: "cluster.local".into(),
        servers,
        addresses: vec!["192.0.2.1".into()],
        groups,
    })
}

/// A world from fuzzer bytes.
pub fn world(u: &mut Unstructured<'_>) -> Result<World> {
    let options = options(u)?;
    let class = |name: &str, controller: &str| -> Arc<GatewayClass> {
        Arc::new(
            from_json(json!({
                "apiVersion": format!("{GROUP}/v1"),
                "kind": "GatewayClass",
                "metadata": {"name": name, "generation": 1},
                "spec": {"controllerName": controller},
            }))
            .expect("class"),
        )
    };
    let classes = vec![class("pingap", CONTROLLER), class("other", "x.io/other")];

    macro_rules! objects {
        ($min:expr, $max:expr, $make:expr) => {{
            let mut items = Vec::new();
            for _ in 0..u.int_in_range($min..=$max)? {
                let (ns, name, value) = $make(&mut *u)?;
                if let Some(item) = from_json(value) {
                    items.push((ns, name, item));
                }
            }
            unique(items)
        }};
    }
    let gateways: Vec<Arc<Gateway>> = objects!(1, 3, gateway_value);
    let listener_sets: Vec<Arc<ListenerSet>> = objects!(0, 2, listener_set_value);
    let routes: Vec<Arc<HttpRoute>> = objects!(1, 4, |u| route_value(u, false));
    let grpc_routes: Vec<Arc<GrpcRoute>> = objects!(0, 2, |u| route_value(u, true));
    let grants: Vec<Arc<ReferenceGrant>> = objects!(0, 2, grant_value);
    let policies: Vec<Arc<BackendTlsPolicy>> = objects!(0, 2, policy_value);

    let mut services = BTreeMap::new();
    for ns in NAMESPACES {
        for name in &SERVICES[..SERVICES.len() - 1] {
            if rarely(u, 5)? {
                continue;
            }
            if let Some(service) = from_json::<Service>(service_value(u, ns, name)?) {
                let key = ServiceKey {
                    namespace: ns.to_string(),
                    name: name.to_string(),
                };
                services.insert(key, Arc::new(service));
            }
        }
    }
    let mut secrets = BTreeMap::new();
    let mut config_maps = BTreeMap::new();
    for ns in NAMESPACES {
        for name in &SECRETS[..SECRETS.len() - 1] {
            let kind = if *name == "bad" { 3 } else { u.arbitrary()? };
            secrets.insert(ObjectKey::new(*ns, *name), Arc::new(secret(ns, name, kind)));
        }
        config_maps.insert(
            ObjectKey::new(*ns, "ca"),
            Arc::new(config_map(ns, "ca", true)),
        );
        config_maps.insert(
            ObjectKey::new(*ns, "bad-ca"),
            Arc::new(config_map(ns, "bad-ca", false)),
        );
    }
    let namespaces = BTreeMap::from([
        ("ns".to_string(), namespace("ns", "a")),
        ("other".to_string(), namespace("other", "b")),
        ("third".to_string(), namespace("third", "a")),
    ]);
    let mut plugins = BTreeSet::new();
    if u.arbitrary()? {
        plugins.insert("my-plugin".to_string());
    }
    Ok(World {
        classes,
        gateways,
        routes,
        grpc_routes,
        listener_sets,
        grants,
        namespaces,
        services,
        secrets,
        policies,
        config_maps,
        plugins,
        options,
    })
}

/// The Pingap servers stored before the first pass, as (name, TOML).
pub fn servers(u: &mut Unstructured<'_>) -> Result<Vec<(String, String)>> {
    let pool: &[(&str, &str, bool, bool)] = &[
        ("http", "0.0.0.0:80,[::]:80", false, false),
        ("alt", "0.0.0.0:8080", false, false),
        ("https", "0.0.0.0:443", true, false),
        ("h2", "0.0.0.0:8443", true, true),
        ("h2c", "0.0.0.0:9000", false, true),
        ("own", "0.0.0.0:10080", false, false),
        ("broken", "nowhere", false, false),
    ];
    let mut out = Vec::new();
    for (name, addr, tls, h2) in pool {
        if rarely(u, 6)? {
            continue;
        }
        let toml = format!(
            "# hand-made\n[servers.{name}]\naddr = \"{addr}\"\nglobal_certificates = {tls}\n\
             enabled_h2 = {h2}\nlocations = [\n    \"manual\", # kept\n]\n"
        );
        out.push((name.to_string(), toml));
    }
    Ok(out)
}

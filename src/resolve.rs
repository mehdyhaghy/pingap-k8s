//! Endpoint resolution: a pure function from the watched
//! Kubernetes state of an upstream's Services to Pingap `addrs`.
//!
//! No I/O happens here. The caller (reconcile) decides what to do with an
//! empty result (keep the last `addrs`) and logs the warnings.
//!
//! Rules implemented:
//! - Port resolution: the configured port (name or Service `port`
//!   number) selects a Service port; that port's `name` (empty for a single
//!   unnamed port) selects the EndpointSlice port with the same name and
//!   protocol TCP (unset protocol means TCP). Nothing is guessed: an
//!   unresolved entry contributes nothing and produces a [`ResolveWarning`].
//! - Classification: see [`classify`]. Only the first address of each
//!   endpoint is used. FQDN slices are skipped, IPv6 slices are skipped with
//!   `ipv4_only`.
//! - Drain: ready endpoints are written; if there are none but some are
//!   draining, the draining ones are written and `fallback` is set. With
//!   `drain_terminating = false`, draining endpoints count as ready.
//!
//! - Target mode: with `target = "service"` the Service and its port
//!   are checked as in 5.2, and each entry contributes one
//!   `<service>.<namespace>.svc.<cluster_domain>:<service port>` address
//!   instead of pod IPs. Slices and readiness are not consulted, `ready`
//!   counts the addresses written, and the drain fallback never applies.
//!
//! Dedupe is by `ip:port` across all slices and all Services of the upstream.
//! When the same `ip:port` is seen more than once, the first occurrence in
//! config order keeps its weight (no warning); if any occurrence is ready, the
//! address counts as ready.

use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, SocketAddr};

use k8s_openapi::api::core::v1::{Service, ServicePort};
use k8s_openapi::api::discovery::v1::{EndpointConditions, EndpointSlice};

use crate::types::{
    PortRef, ServiceKey, ServiceRef, ServiceSnapshot, UpstreamOptions, UpstreamTarget,
};

/// One `services` entry of an upstream together with the current state of
/// its Service.
#[derive(Clone, Copy, Debug)]
pub struct ServiceInput<'a> {
    pub service_ref: &'a ServiceRef,
    pub snapshot: &'a ServiceSnapshot,
}

/// Why a `services` entry contributed nothing. Reconcile logs each distinct
/// warning once.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ResolveWarning {
    /// The Service does not exist (or is not listed yet).
    ServiceMissing(ServiceKey),
    /// No Service port matches the configured name or number.
    PortNotInService { service: ServiceKey, port: PortRef },
    /// The Service port resolved to `port_name` (empty for an unnamed port),
    /// but an EndpointSlice with endpoints has no TCP port of that name.
    PortNotInSlices {
        service: ServiceKey,
        port: PortRef,
        port_name: String,
    },
}

impl fmt::Display for ResolveWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResolveWarning::ServiceMissing(service) => {
                write!(f, "service {service} not found")
            }
            ResolveWarning::PortNotInService { service, port } => {
                write!(f, "service {service} has no port {port}")
            }
            ResolveWarning::PortNotInSlices {
                service,
                port,
                port_name,
            } => write!(
                f,
                "endpoint slices of service {service} have no TCP port named {port_name:?} (configured port {port})"
            ),
        }
    }
}

/// The resolved `addrs` of one upstream.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Resolved {
    /// Sorted by the final string, deduped by `ip:port`, in Pingap form
    /// `ip:port[ weight]` (IPv6 as `[ip]:port`). Empty means "nothing to
    /// write" (, the caller keeps the last `addrs`).
    pub addrs: Vec<String>,
    /// Unique ready endpoints. With `drain_terminating = false` this includes
    /// the terminating-but-serving ones.
    pub ready: usize,
    /// Unique draining endpoints (terminating, serving true or unset) that are
    /// not also ready elsewhere. Always 0 with `drain_terminating = false`.
    pub draining: usize,
    /// True when only draining endpoints were written.
    pub fallback: bool,
    /// Sorted, deduped.
    pub warnings: Vec<ResolveWarning>,
}

/// Endpoint class per the design notes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    Ready,
    Draining,
    Excluded,
}

/// Classifies one endpoint from its conditions.
fn classify(conditions: Option<&EndpointConditions>, include_not_ready: bool) -> Class {
    let (ready, serving, terminating) = match conditions {
        Some(c) => (c.ready, c.serving, c.terminating),
        None => (None, None, None),
    };
    if terminating == Some(true) {
        // Terminating: draining while still serving (unset means serving).
        return if serving == Some(false) {
            Class::Excluded
        } else {
            Class::Draining
        };
    }
    // Not terminating: ready true or unset is ready.
    if ready != Some(false) || include_not_ready {
        Class::Ready
    } else {
        Class::Excluded
    }
}

fn is_tcp(protocol: Option<&str>) -> bool {
    matches!(protocol, None | Some("TCP"))
}

/// Step 1 of 5.2: the Service port selected by `port`. A number matches
/// `port`; a name matches `name`. TCP ports are preferred when a number is
/// shared by several protocols.
fn find_service_port<'s>(service: &'s Service, port: &PortRef) -> Option<&'s ServicePort> {
    let ports = service.spec.as_ref()?.ports.as_deref()?;
    let matches = |p: &&ServicePort| match port {
        PortRef::Name(name) => p.name.as_deref() == Some(name.as_str()),
        PortRef::Number(number) => p.port == *number,
    };
    ports
        .iter()
        .filter(matches)
        .find(|p| is_tcp(p.protocol.as_deref()))
        .or_else(|| ports.iter().find(matches))
}

/// Step 3 of 5.2: the slice's TCP port named `port_name`, if valid.
fn find_slice_port(slice: &EndpointSlice, port_name: &str) -> Option<u16> {
    slice
        .ports
        .as_deref()?
        .iter()
        .find(|p| p.name.as_deref().unwrap_or("") == port_name && is_tcp(p.protocol.as_deref()))
        .and_then(|p| p.port)
        .and_then(|p| u16::try_from(p).ok())
        .filter(|p| *p != 0)
}

struct Entry {
    weight: Option<u32>,
    class: Class,
}

/// Resolves the `addrs` of one upstream from its `services` entries, in
/// config order.
pub fn resolve(inputs: &[ServiceInput<'_>], opts: &UpstreamOptions) -> Resolved {
    if let UpstreamTarget::Service { cluster_domain } = &opts.target {
        return resolve_service_target(inputs, cluster_domain);
    }
    let mut warnings = Vec::new();
    let mut seen: HashMap<SocketAddr, Entry> = HashMap::new();

    for input in inputs {
        let sref = input.service_ref;
        let Some(service) = input.snapshot.service.as_deref() else {
            warnings.push(ResolveWarning::ServiceMissing(sref.key.clone()));
            continue;
        };
        let Some(service_port) = find_service_port(service, &sref.port) else {
            warnings.push(ResolveWarning::PortNotInService {
                service: sref.key.clone(),
                port: sref.port.clone(),
            });
            continue;
        };
        let port_name = service_port.name.as_deref().unwrap_or("");

        let mut port_missing = false;
        for slice in &input.snapshot.slices {
            let ipv6 = match slice.address_type.as_str() {
                "IPv4" => false,
                "IPv6" => true,
                // FQDN (and anything unknown) is not supported.
                _ => continue,
            };
            if ipv6 && opts.ipv4_only {
                continue;
            }
            if slice.endpoints.is_empty() {
                continue;
            }
            let Some(port) = find_slice_port(slice, port_name) else {
                port_missing = true;
                continue;
            };
            for endpoint in &slice.endpoints {
                let mut class = classify(endpoint.conditions.as_ref(), opts.include_not_ready);
                if class == Class::Excluded {
                    continue;
                }
                if class == Class::Draining && !opts.drain_terminating {
                    class = Class::Ready;
                }
                // Only the first address of each endpoint is used.
                let Some(ip) = endpoint
                    .addresses
                    .first()
                    .and_then(|a| a.parse::<IpAddr>().ok())
                else {
                    continue;
                };
                if ip.is_ipv6() != ipv6 {
                    // Address does not match the slice's address type.
                    continue;
                }
                let addr = SocketAddr::new(ip, port);
                seen.entry(addr)
                    .and_modify(|e| {
                        if class == Class::Ready {
                            e.class = Class::Ready;
                        }
                    })
                    .or_insert(Entry {
                        weight: sref.weight,
                        class,
                    });
            }
        }
        if port_missing {
            warnings.push(ResolveWarning::PortNotInSlices {
                service: sref.key.clone(),
                port: sref.port.clone(),
                port_name: port_name.to_string(),
            });
        }
    }

    let ready = seen.values().filter(|e| e.class == Class::Ready).count();
    let draining = seen.len() - ready;
    let (wanted, fallback) = if ready > 0 {
        (Class::Ready, false)
    } else if draining > 0 {
        (Class::Draining, true)
    } else {
        (Class::Ready, false)
    };

    let mut addrs: Vec<String> = seen
        .iter()
        .filter(|(_, e)| e.class == wanted)
        .map(|(addr, e)| match e.weight {
            Some(w) => format!("{addr} {w}"),
            None => addr.to_string(),
        })
        .collect();
    addrs.sort();

    warnings.sort();
    warnings.dedup();

    Resolved {
        addrs,
        ready,
        draining,
        fallback,
        warnings,
    }
}

/// `target = "service"`: one Service DNS name per entry, port is the
/// Service `port`.
///
/// Pingap resolves the name itself: an upstream whose `discovery` is unset
/// and whose `addrs` hold a hostname is treated as `discovery = "dns"`
/// (pingap-config `UpstreamConf::guess_discovery`), and DNS discovery looks
/// the names up again every `update_frequency` (default 60 s). So a Service
/// whose ClusterIP changes (deleted and recreated) is picked up without a
/// controller write. An explicit `discovery = "static"` on the upstream
/// would resolve the name only once, when the upstream is built.
fn resolve_service_target(inputs: &[ServiceInput<'_>], cluster_domain: &str) -> Resolved {
    let mut warnings = Vec::new();
    // address -> weight; the first entry in config order keeps its weight.
    let mut seen: HashMap<String, Option<u32>> = HashMap::new();

    for input in inputs {
        let sref = input.service_ref;
        let Some(service) = input.snapshot.service.as_deref() else {
            warnings.push(ResolveWarning::ServiceMissing(sref.key.clone()));
            continue;
        };
        let Some(port) = find_service_port(service, &sref.port)
            .and_then(|p| u16::try_from(p.port).ok())
            .filter(|p| *p != 0)
        else {
            warnings.push(ResolveWarning::PortNotInService {
                service: sref.key.clone(),
                port: sref.port.clone(),
            });
            continue;
        };
        let addr = format!(
            "{}.{}.svc.{cluster_domain}:{port}",
            sref.key.name, sref.key.namespace
        );
        seen.entry(addr).or_insert(sref.weight);
    }

    let mut addrs: Vec<String> = seen
        .into_iter()
        .map(|(addr, weight)| match weight {
            Some(w) => format!("{addr} {w}"),
            None => addr,
        })
        .collect();
    addrs.sort();
    warnings.sort();
    warnings.dedup();

    Resolved {
        ready: addrs.len(),
        addrs,
        draining: 0,
        fallback: false,
        warnings,
    }
}

#[cfg(test)]
#[allow(clippy::type_complexity)] // table-test tuples
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use k8s_openapi::api::core::v1::ServiceSpec;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointPort};

    // ---- fixture builders ----

    fn opts() -> UpstreamOptions {
        UpstreamOptions {
            include_not_ready: false,
            drain_terminating: true,
            ipv4_only: false,
            debounce: Duration::from_millis(500),
            debounce_max: Duration::from_secs(2),
            target: UpstreamTarget::Pods,
        }
    }

    fn key(name: &str) -> ServiceKey {
        ServiceKey {
            namespace: "prod".into(),
            name: name.into(),
        }
    }

    fn sref(name: &str, port: PortRef, weight: Option<u32>) -> ServiceRef {
        ServiceRef {
            key: key(name),
            port,
            weight,
        }
    }

    fn named(name: &str) -> PortRef {
        PortRef::Name(name.into())
    }

    /// Service with `(name, port, protocol)` ports; empty name = unnamed.
    fn service(ports: &[(&str, i32, &str)]) -> Arc<Service> {
        Arc::new(Service {
            spec: Some(ServiceSpec {
                ports: Some(
                    ports
                        .iter()
                        .map(|(name, port, proto)| ServicePort {
                            name: (!name.is_empty()).then(|| name.to_string()),
                            port: *port,
                            protocol: (!proto.is_empty()).then(|| proto.to_string()),
                            ..Default::default()
                        })
                        .collect(),
                ),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    /// Endpoint conditions as (ready, serving, terminating).
    type Cond = (Option<bool>, Option<bool>, Option<bool>);
    const READY: Cond = (Some(true), Some(true), Some(false));
    const UNKNOWN: Cond = (None, None, None);
    const NOT_READY: Cond = (Some(false), Some(false), Some(false));
    const DRAINING: Cond = (Some(false), Some(true), Some(true));
    const DRAINING_UNSET_SERVING: Cond = (Some(false), None, Some(true));
    const TERMINATED: Cond = (Some(false), Some(false), Some(true));

    fn ep(addr: &str, c: Cond) -> Endpoint {
        Endpoint {
            addresses: vec![addr.to_string()],
            conditions: Some(EndpointConditions {
                ready: c.0,
                serving: c.1,
                terminating: c.2,
            }),
            ..Default::default()
        }
    }

    /// Slice with `(name, port, protocol)` ports; empty name/protocol = unset.
    fn slice(
        address_type: &str,
        ports: &[(&str, i32, &str)],
        endpoints: Vec<Endpoint>,
    ) -> Arc<EndpointSlice> {
        Arc::new(EndpointSlice {
            address_type: address_type.into(),
            endpoints,
            ports: Some(
                ports
                    .iter()
                    .map(|(name, port, proto)| EndpointPort {
                        name: (!name.is_empty()).then(|| name.to_string()),
                        port: Some(*port),
                        protocol: (!proto.is_empty()).then(|| proto.to_string()),
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        })
    }

    fn v4(endpoints: Vec<Endpoint>) -> Arc<EndpointSlice> {
        slice("IPv4", &[("http", 8080, "TCP")], endpoints)
    }

    fn snap(svc: Option<Arc<Service>>, slices: Vec<Arc<EndpointSlice>>) -> ServiceSnapshot {
        ServiceSnapshot {
            service: svc,
            slices,
        }
    }

    fn http_svc() -> Option<Arc<Service>> {
        Some(service(&[("http", 80, "TCP")]))
    }

    fn run1(r: &ServiceRef, s: &ServiceSnapshot, o: &UpstreamOptions) -> Resolved {
        resolve(
            &[ServiceInput {
                service_ref: r,
                snapshot: s,
            }],
            o,
        )
    }

    // ---- classification ----

    #[test]
    fn classification_table() {
        // (name, conditions, include_not_ready, expected class)
        let cases: &[(&str, Option<Cond>, bool, Class)] = &[
            ("ready", Some(READY), false, Class::Ready),
            ("unknown conditions", Some(UNKNOWN), false, Class::Ready),
            ("no conditions object", None, false, Class::Ready),
            (
                "ready unset, not terminating",
                Some((None, Some(false), Some(false))),
                false,
                Class::Ready,
            ),
            ("not ready", Some(NOT_READY), false, Class::Excluded),
            (
                "not ready, include_not_ready",
                Some(NOT_READY),
                true,
                Class::Ready,
            ),
            (
                "terminating + serving",
                Some(DRAINING),
                false,
                Class::Draining,
            ),
            (
                "terminating + serving unset",
                Some(DRAINING_UNSET_SERVING),
                false,
                Class::Draining,
            ),
            (
                "terminating + serving false",
                Some(TERMINATED),
                false,
                Class::Excluded,
            ),
            (
                "terminating + serving false, include_not_ready",
                Some(TERMINATED),
                true,
                Class::Excluded,
            ),
            (
                "terminating + ready true",
                Some((Some(true), Some(true), Some(true))),
                false,
                Class::Draining,
            ),
        ];
        for (name, cond, inr, want) in cases {
            let c = cond.map(|c| EndpointConditions {
                ready: c.0,
                serving: c.1,
                terminating: c.2,
            });
            assert_eq!(classify(c.as_ref(), *inr), *want, "case {name}");
        }
    }

    #[test]
    fn resolve_conditions_table() {
        // (name, endpoints, options tweak, expected addrs, ready, draining, fallback)
        type Tweak = fn(&mut UpstreamOptions);
        let none: Tweak = |_| {};
        let inr: Tweak = |o| o.include_not_ready = true;
        let no_drain: Tweak = |o| o.drain_terminating = false;
        let cases: Vec<(&str, Vec<Endpoint>, Tweak, Vec<&str>, usize, usize, bool)> = vec![
            (
                "ready",
                vec![ep("10.0.0.1", READY)],
                none,
                vec!["10.0.0.1:8080"],
                1,
                0,
                false,
            ),
            (
                "unknown is ready",
                vec![ep("10.0.0.1", UNKNOWN)],
                none,
                vec!["10.0.0.1:8080"],
                1,
                0,
                false,
            ),
            (
                "not ready excluded",
                vec![ep("10.0.0.1", READY), ep("10.0.0.2", NOT_READY)],
                none,
                vec!["10.0.0.1:8080"],
                1,
                0,
                false,
            ),
            (
                "not ready included",
                vec![ep("10.0.0.1", READY), ep("10.0.0.2", NOT_READY)],
                inr,
                vec!["10.0.0.1:8080", "10.0.0.2:8080"],
                2,
                0,
                false,
            ),
            (
                "draining removed while ready exists",
                vec![ep("10.0.0.1", READY), ep("10.0.0.2", DRAINING)],
                none,
                vec!["10.0.0.1:8080"],
                1,
                1,
                false,
            ),
            (
                "serving=false terminating excluded",
                vec![ep("10.0.0.1", READY), ep("10.0.0.2", TERMINATED)],
                none,
                vec!["10.0.0.1:8080"],
                1,
                0,
                false,
            ),
            (
                "all draining fallback",
                vec![
                    ep("10.0.0.2", DRAINING),
                    ep("10.0.0.3", DRAINING_UNSET_SERVING),
                    ep("10.0.0.4", TERMINATED),
                ],
                none,
                vec!["10.0.0.2:8080", "10.0.0.3:8080"],
                0,
                2,
                true,
            ),
            (
                "drain_terminating=false counts draining as ready",
                vec![ep("10.0.0.1", READY), ep("10.0.0.2", DRAINING)],
                no_drain,
                vec!["10.0.0.1:8080", "10.0.0.2:8080"],
                2,
                0,
                false,
            ),
            (
                "drain_terminating=false, only draining: no fallback",
                vec![ep("10.0.0.2", DRAINING)],
                no_drain,
                vec!["10.0.0.2:8080"],
                1,
                0,
                false,
            ),
            ("zero endpoints", vec![], none, vec![], 0, 0, false),
            (
                "only excluded",
                vec![ep("10.0.0.2", NOT_READY), ep("10.0.0.3", TERMINATED)],
                none,
                vec![],
                0,
                0,
                false,
            ),
        ];
        for (name, eps, tweak, addrs, ready, draining, fallback) in cases {
            let mut o = opts();
            tweak(&mut o);
            let r = sref("api", named("http"), None);
            let s = snap(http_svc(), vec![v4(eps)]);
            let got = run1(&r, &s, &o);
            assert_eq!(got.addrs, addrs, "case {name}");
            assert_eq!(got.ready, ready, "case {name} ready");
            assert_eq!(got.draining, draining, "case {name} draining");
            assert_eq!(got.fallback, fallback, "case {name} fallback");
            assert!(got.warnings.is_empty(), "case {name}: {:?}", got.warnings);
        }
    }

    #[test]
    fn only_first_address_used() {
        let mut e = ep("10.0.0.1", READY);
        e.addresses.push("10.0.0.99".into());
        let r = sref("api", named("http"), None);
        let s = snap(http_svc(), vec![v4(vec![e])]);
        assert_eq!(run1(&r, &s, &opts()).addrs, vec!["10.0.0.1:8080"]);
    }

    #[test]
    fn endpoint_without_or_with_bad_address_skipped() {
        let mut empty = ep("10.0.0.1", READY);
        empty.addresses.clear();
        let bad = ep("not-an-ip", READY);
        let r = sref("api", named("http"), None);
        let s = snap(
            http_svc(),
            vec![v4(vec![empty, bad, ep("10.0.0.2", READY)])],
        );
        let got = run1(&r, &s, &opts());
        assert_eq!(got.addrs, vec!["10.0.0.2:8080"]);
        assert_eq!(got.ready, 1);
    }

    // ---- slices: merge, families, FQDN, dedupe ----

    #[test]
    fn slices_table() {
        type Tweak = fn(&mut UpstreamOptions);
        let none: Tweak = |_| {};
        let v4only: Tweak = |o| o.ipv4_only = true;
        let p = &[("http", 8080, "TCP")];
        let cases: Vec<(&str, Vec<Arc<EndpointSlice>>, Tweak, Vec<&str>)> = vec![
            (
                "multiple slices merged",
                vec![
                    v4(vec![ep("10.0.0.1", READY)]),
                    v4(vec![ep("10.0.0.2", READY)]),
                ],
                none,
                vec!["10.0.0.1:8080", "10.0.0.2:8080"],
            ),
            (
                "dual stack merged, IPv6 bracketed",
                vec![
                    v4(vec![ep("10.0.0.1", READY)]),
                    slice("IPv6", p, vec![ep("fd00::1", READY)]),
                ],
                none,
                vec!["10.0.0.1:8080", "[fd00::1]:8080"],
            ),
            (
                "ipv4_only drops IPv6 slices",
                vec![
                    v4(vec![ep("10.0.0.1", READY)]),
                    slice("IPv6", p, vec![ep("fd00::1", READY)]),
                ],
                v4only,
                vec!["10.0.0.1:8080"],
            ),
            (
                "FQDN slices excluded",
                vec![
                    v4(vec![ep("10.0.0.1", READY)]),
                    slice("FQDN", p, vec![ep("db.example.com", READY)]),
                ],
                none,
                vec!["10.0.0.1:8080"],
            ),
            (
                "dedupe across slices",
                vec![
                    v4(vec![ep("10.0.0.1", READY), ep("10.0.0.2", READY)]),
                    v4(vec![ep("10.0.0.1", READY)]),
                ],
                none,
                vec!["10.0.0.1:8080", "10.0.0.2:8080"],
            ),
            (
                "dedupe: ready copy wins over draining copy",
                vec![
                    v4(vec![ep("10.0.0.1", DRAINING)]),
                    v4(vec![ep("10.0.0.1", READY)]),
                ],
                none,
                vec!["10.0.0.1:8080"],
            ),
            (
                "IPv6 address normalized",
                vec![slice("IPv6", p, vec![ep("FD00:0:0::0001", READY)])],
                none,
                vec!["[fd00::1]:8080"],
            ),
            (
                "address family mismatch skipped",
                vec![v4(vec![ep("fd00::1", READY), ep("10.0.0.1", READY)])],
                none,
                vec!["10.0.0.1:8080"],
            ),
            (
                "slice ports can differ per slice",
                vec![
                    v4(vec![ep("10.0.0.1", READY)]),
                    slice("IPv4", &[("http", 9090, "")], vec![ep("10.0.0.2", READY)]),
                ],
                none,
                vec!["10.0.0.1:8080", "10.0.0.2:9090"],
            ),
        ];
        for (name, slices, tweak, addrs) in cases {
            let mut o = opts();
            tweak(&mut o);
            let r = sref("api", named("http"), None);
            let s = snap(http_svc(), slices);
            let got = run1(&r, &s, &o);
            assert_eq!(got.addrs, addrs, "case {name}");
            assert_eq!(got.ready, addrs.len(), "case {name} ready");
            assert!(got.warnings.is_empty(), "case {name}: {:?}", got.warnings);
        }
    }

    #[test]
    fn dedupe_counts_unique_endpoints() {
        let r = sref("api", named("http"), None);
        let s = snap(
            http_svc(),
            vec![
                v4(vec![ep("10.0.0.1", DRAINING), ep("10.0.0.2", DRAINING)]),
                v4(vec![ep("10.0.0.1", DRAINING)]),
            ],
        );
        let got = run1(&r, &s, &opts());
        assert_eq!(got.addrs, vec!["10.0.0.1:8080", "10.0.0.2:8080"]);
        assert_eq!((got.ready, got.draining, got.fallback), (0, 2, true));
    }

    // ---- port resolution ----

    #[test]
    fn port_resolution_table() {
        // (name, service ports, configured port, slice ports, expected addrs, expected warning)
        let k = key("api");
        let cases: Vec<(
            &str,
            Vec<(&str, i32, &str)>,
            PortRef,
            Vec<(&str, i32, &str)>,
            Vec<&str>,
            Option<ResolveWarning>,
        )> = vec![
            (
                "named port",
                vec![("http", 80, "TCP"), ("metrics", 9100, "TCP")],
                named("http"),
                vec![("metrics", 9100, "TCP"), ("http", 8080, "TCP")],
                vec!["10.0.0.1:8080"],
                None,
            ),
            (
                "numeric port maps via the port's name",
                vec![("http", 80, "TCP"), ("metrics", 9100, "TCP")],
                PortRef::Number(9100),
                vec![("http", 8080, "TCP"), ("metrics", 9101, "TCP")],
                vec!["10.0.0.1:9101"],
                None,
            ),
            (
                "numeric port is Service port, not targetPort",
                vec![("http", 80, "TCP")],
                PortRef::Number(8080),
                vec![("http", 8080, "TCP")],
                vec![],
                Some(ResolveWarning::PortNotInService {
                    service: k.clone(),
                    port: PortRef::Number(8080),
                }),
            ),
            (
                "single unnamed port by number",
                vec![("", 80, "")],
                PortRef::Number(80),
                vec![("", 8080, "")],
                vec!["10.0.0.1:8080"],
                None,
            ),
            (
                "protocol unset means TCP",
                vec![("http", 80, "")],
                named("http"),
                vec![("http", 8080, "")],
                vec!["10.0.0.1:8080"],
                None,
            ),
            (
                "unknown port name",
                vec![("http", 80, "TCP")],
                named("grpc"),
                vec![("http", 8080, "TCP")],
                vec![],
                Some(ResolveWarning::PortNotInService {
                    service: k.clone(),
                    port: named("grpc"),
                }),
            ),
            (
                "port missing from slices",
                vec![("http", 80, "TCP")],
                named("http"),
                vec![("web", 8080, "TCP")],
                vec![],
                Some(ResolveWarning::PortNotInSlices {
                    service: k.clone(),
                    port: named("http"),
                    port_name: "http".into(),
                }),
            ),
            (
                "UDP slice port does not match",
                vec![("dns", 53, "UDP")],
                named("dns"),
                vec![("dns", 53, "UDP")],
                vec![],
                Some(ResolveWarning::PortNotInSlices {
                    service: k.clone(),
                    port: named("dns"),
                    port_name: "dns".into(),
                }),
            ),
            (
                "number shared by TCP and UDP prefers TCP",
                vec![("dns-udp", 53, "UDP"), ("dns-tcp", 53, "TCP")],
                PortRef::Number(53),
                vec![("dns-udp", 53, "UDP"), ("dns-tcp", 5353, "TCP")],
                vec!["10.0.0.1:5353"],
                None,
            ),
            (
                "unnamed Service port does not match a named slice port",
                vec![("", 80, "TCP")],
                PortRef::Number(80),
                vec![("http", 8080, "TCP")],
                vec![],
                Some(ResolveWarning::PortNotInSlices {
                    service: k.clone(),
                    port: PortRef::Number(80),
                    port_name: String::new(),
                }),
            ),
        ];
        for (name, svc_ports, port, slice_ports, addrs, warning) in cases {
            let r = sref("api", port, None);
            let s = snap(
                Some(service(&svc_ports)),
                vec![slice("IPv4", &slice_ports, vec![ep("10.0.0.1", READY)])],
            );
            let got = run1(&r, &s, &opts());
            assert_eq!(got.addrs, addrs, "case {name}");
            assert_eq!(
                got.warnings,
                warning.into_iter().collect::<Vec<_>>(),
                "case {name}"
            );
        }
    }

    #[test]
    fn service_missing_warns_and_contributes_nothing() {
        let a = sref("api", named("http"), None);
        let b = sref("gone", named("http"), None);
        let sa = snap(http_svc(), vec![v4(vec![ep("10.0.0.1", READY)])]);
        let sb = snap(None, vec![v4(vec![ep("10.0.0.9", READY)])]);
        let got = resolve(
            &[
                ServiceInput {
                    service_ref: &a,
                    snapshot: &sa,
                },
                ServiceInput {
                    service_ref: &b,
                    snapshot: &sb,
                },
            ],
            &opts(),
        );
        assert_eq!(got.addrs, vec!["10.0.0.1:8080"]);
        assert_eq!(
            got.warnings,
            vec![ResolveWarning::ServiceMissing(key("gone"))]
        );
    }

    #[test]
    fn service_without_spec_or_ports_warns() {
        let r = sref("api", named("http"), None);
        for svc in [Service::default(), (*service(&[])).clone()] {
            let s = snap(Some(Arc::new(svc)), vec![v4(vec![ep("10.0.0.1", READY)])]);
            let got = run1(&r, &s, &opts());
            assert!(got.addrs.is_empty());
            assert_eq!(
                got.warnings,
                vec![ResolveWarning::PortNotInService {
                    service: key("api"),
                    port: named("http"),
                }]
            );
        }
    }

    #[test]
    fn empty_placeholder_slice_does_not_warn() {
        // The EndpointSlice controller keeps an empty slice with no ports
        // for a Service without endpoints: zero endpoints, not a port error.
        let r = sref("api", named("http"), None);
        let placeholder = Arc::new(EndpointSlice {
            address_type: "IPv4".into(),
            ..Default::default()
        });
        let s = snap(http_svc(), vec![placeholder]);
        let got = run1(&r, &s, &opts());
        assert_eq!(got, Resolved::default());
    }

    #[test]
    fn slice_port_nil_or_invalid_does_not_match() {
        let r = sref("api", named("http"), None);
        let mut nil_port = (*v4(vec![ep("10.0.0.1", READY)])).clone();
        nil_port.ports.as_mut().unwrap()[0].port = None;
        let bad_port = slice(
            "IPv4",
            &[("http", 70000, "TCP")],
            vec![ep("10.0.0.2", READY)],
        );
        let s = snap(http_svc(), vec![Arc::new(nil_port), bad_port]);
        let got = run1(&r, &s, &opts());
        assert!(got.addrs.is_empty());
        assert_eq!(got.warnings.len(), 1);
    }

    // ---- weights and ordering ----

    #[test]
    fn weights_and_sort_order() {
        let blue = sref("web-blue", named("http"), Some(9));
        let green = sref("web-green", named("http"), Some(1));
        let sb = snap(
            http_svc(),
            vec![
                v4(vec![ep("10.0.0.9", READY), ep("10.0.0.10", READY)]),
                slice("IPv6", &[("http", 8080, "TCP")], vec![ep("::1", READY)]),
            ],
        );
        let sg = snap(http_svc(), vec![v4(vec![ep("10.0.1.1", READY)])]);
        let inputs = [
            ServiceInput {
                service_ref: &blue,
                snapshot: &sb,
            },
            ServiceInput {
                service_ref: &green,
                snapshot: &sg,
            },
        ];
        let want = vec![
            "10.0.0.10:8080 9",
            "10.0.0.9:8080 9",
            "10.0.1.1:8080 1",
            "[::1]:8080 9",
        ];
        let got = resolve(&inputs, &opts());
        assert_eq!(got.addrs, want);
        assert_eq!(got.ready, 4);

        // Input order of slices and endpoints does not change the output.
        let sb_rev = snap(
            http_svc(),
            vec![
                slice("IPv6", &[("http", 8080, "TCP")], vec![ep("::1", READY)]),
                v4(vec![ep("10.0.0.10", READY), ep("10.0.0.9", READY)]),
            ],
        );
        let inputs_rev = [
            ServiceInput {
                service_ref: &blue,
                snapshot: &sb_rev,
            },
            ServiceInput {
                service_ref: &green,
                snapshot: &sg,
            },
        ];
        assert_eq!(resolve(&inputs_rev, &opts()).addrs, want);
    }

    #[test]
    fn duplicate_across_services_keeps_first_weight() {
        let a = sref("a", named("http"), Some(5));
        let b = sref("b", named("http"), Some(2));
        let s = snap(http_svc(), vec![v4(vec![ep("10.0.0.1", READY)])]);
        let got = resolve(
            &[
                ServiceInput {
                    service_ref: &a,
                    snapshot: &s,
                },
                ServiceInput {
                    service_ref: &b,
                    snapshot: &s,
                },
            ],
            &opts(),
        );
        assert_eq!(got.addrs, vec!["10.0.0.1:8080 5"]);
        assert_eq!(got.ready, 1);
        assert!(got.warnings.is_empty());
    }

    #[test]
    fn fallback_spans_services() {
        // One Service all draining, the other ready: no fallback.
        let a = sref("a", named("http"), None);
        let b = sref("b", named("http"), None);
        let sa = snap(http_svc(), vec![v4(vec![ep("10.0.0.1", DRAINING)])]);
        let sb = snap(http_svc(), vec![v4(vec![ep("10.0.0.2", READY)])]);
        let got = resolve(
            &[
                ServiceInput {
                    service_ref: &a,
                    snapshot: &sa,
                },
                ServiceInput {
                    service_ref: &b,
                    snapshot: &sb,
                },
            ],
            &opts(),
        );
        assert_eq!(got.addrs, vec!["10.0.0.2:8080"]);
        assert!(!got.fallback);
        assert_eq!((got.ready, got.draining), (1, 1));
    }

    #[test]
    fn warnings_display() {
        let w = ResolveWarning::PortNotInSlices {
            service: key("api"),
            port: PortRef::Number(80),
            port_name: String::new(),
        };
        assert!(w.to_string().contains("prod/api"));
        assert_eq!(
            ResolveWarning::ServiceMissing(key("api")).to_string(),
            "service prod/api not found"
        );
    }

    // ---- target = "service" ----

    fn svc_opts(domain: &str) -> UpstreamOptions {
        UpstreamOptions {
            target: UpstreamTarget::Service {
                cluster_domain: domain.into(),
            },
            ..opts()
        }
    }

    #[test]
    fn service_target_table() {
        let web = service(&[("http", 80, "TCP"), ("admin", 9000, "TCP")]);
        let unnamed = service(&[("", 8081, "")]);
        // Slices are ignored: no slice, a not-ready one, a slice port mismatch.
        let not_ready = v4(vec![ep("10.0.0.1", NOT_READY)]);
        let other_port = slice("IPv4", &[("grpc", 1, "TCP")], vec![ep("10.0.0.2", READY)]);
        // (name, entry, snapshot, cluster_domain, addrs, warnings)
        let cases: Vec<(
            &str,
            ServiceRef,
            ServiceSnapshot,
            &str,
            Vec<&str>,
            Vec<ResolveWarning>,
        )> = vec![
            (
                "named port",
                sref("web", named("http"), None),
                snap(Some(web.clone()), vec![]),
                "cluster.local",
                vec!["web.prod.svc.cluster.local:80"],
                vec![],
            ),
            (
                "numeric port is the Service port, not the pod port",
                sref("web", PortRef::Number(9000), None),
                snap(Some(web.clone()), vec![not_ready.clone()]),
                "cluster.local",
                vec!["web.prod.svc.cluster.local:9000"],
                vec![],
            ),
            (
                "single unnamed port by number",
                sref("web", PortRef::Number(8081), None),
                snap(Some(unnamed), vec![other_port]),
                "cluster.local",
                vec!["web.prod.svc.cluster.local:8081"],
                vec![],
            ),
            (
                "weight",
                sref("web", named("http"), Some(7)),
                snap(Some(web.clone()), vec![]),
                "cluster.local",
                vec!["web.prod.svc.cluster.local:80 7"],
                vec![],
            ),
            (
                "custom cluster domain",
                sref("web", named("http"), None),
                snap(Some(web.clone()), vec![]),
                "k8s.example.internal",
                vec!["web.prod.svc.k8s.example.internal:80"],
                vec![],
            ),
            (
                "missing Service",
                sref("web", named("http"), None),
                snap(None, vec![not_ready]),
                "cluster.local",
                vec![],
                vec![ResolveWarning::ServiceMissing(key("web"))],
            ),
            (
                "missing port name",
                sref("web", named("grpc"), None),
                snap(Some(web.clone()), vec![]),
                "cluster.local",
                vec![],
                vec![ResolveWarning::PortNotInService {
                    service: key("web"),
                    port: named("grpc"),
                }],
            ),
            (
                "missing port number (pod port is not matched)",
                sref("web", PortRef::Number(8080), None),
                snap(Some(web), vec![]),
                "cluster.local",
                vec![],
                vec![ResolveWarning::PortNotInService {
                    service: key("web"),
                    port: PortRef::Number(8080),
                }],
            ),
        ];
        for (name, r, s, domain, addrs, warnings) in cases {
            let got = run1(&r, &s, &svc_opts(domain));
            assert_eq!(got.addrs, addrs, "{name}");
            assert_eq!(got.warnings, warnings, "{name}");
            assert_eq!((got.ready, got.draining), (addrs.len(), 0), "{name}");
            assert!(!got.fallback, "{name}");
        }
    }

    #[test]
    fn service_target_multiple_services_sorted_with_weights() {
        let green = sref("web-green", named("http"), Some(1));
        let blue = sref("web-blue", named("http"), Some(9));
        let missing = sref("gone", named("http"), None);
        let blue_again = sref("web-blue", PortRef::Number(80), Some(3));
        let present = snap(http_svc(), vec![]);
        let absent = snap(None, vec![]);
        let got = resolve(
            &[
                ServiceInput {
                    service_ref: &green,
                    snapshot: &present,
                },
                ServiceInput {
                    service_ref: &missing,
                    snapshot: &absent,
                },
                ServiceInput {
                    service_ref: &blue,
                    snapshot: &present,
                },
                // Same address as `blue`: the first entry keeps its weight.
                ServiceInput {
                    service_ref: &blue_again,
                    snapshot: &present,
                },
            ],
            &svc_opts("cluster.local"),
        );
        assert_eq!(
            got.addrs,
            vec![
                "web-blue.prod.svc.cluster.local:80 9",
                "web-green.prod.svc.cluster.local:80 1",
            ]
        );
        assert_eq!(got.ready, 2);
        assert_eq!(
            got.warnings,
            vec![ResolveWarning::ServiceMissing(key("gone"))]
        );
    }

    #[test]
    fn service_target_never_falls_back_to_draining() {
        let r = sref("web", named("http"), None);
        let s = snap(http_svc(), vec![v4(vec![ep("10.0.0.1", DRAINING)])]);
        let pods = run1(&r, &s, &opts());
        assert!(pods.fallback);
        let svc = run1(&r, &s, &svc_opts("cluster.local"));
        assert!(!svc.fallback);
        assert_eq!(svc.addrs, vec!["web.prod.svc.cluster.local:80"]);
        assert_eq!((svc.ready, svc.draining), (1, 0));
    }
}

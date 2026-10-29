//! Endpoint resolution: a pure function from the watched
//! Kubernetes state of an upstream's Services to Pingap `addrs`.
//!
//! No I/O happens here. The caller (reconcile) decides what to do with an
//! empty result (it keeps the last `addrs`) and logs the warnings.
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
//! - Target mode: with `target = "service"` the Service and its port are
//!   checked as for pods, and each entry contributes its ClusterIP (or, when
//!   headless, its DNS name) instead of pod IPs, once a slice has a ready or
//!   serving endpoint on that port (see `resolve_service_target`). `ready`
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
    /// write" (the caller keeps the last `addrs`).
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

/// Endpoint class.
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
    /// Index of the first `services` entry that gave the address.
    source: usize,
}

/// Total of the weights written with `spread_weights`. Pingora's weighted selection keeps
/// one slot per unit of weight, so the total must stay small.
pub const SPREAD_TOTAL: u64 = 1000;

/// `spread_weights`: `(address, source, weight)` with the Service weight of `source`
/// becomes `(address, weight)` where every Service's share of [`SPREAD_TOTAL`] is split
/// evenly over its addresses (at least 1 each). Services with weight 0 never reach here.
fn spread(entries: Vec<(String, usize, u32)>) -> Vec<(String, Option<u32>)> {
    let mut per_source: HashMap<usize, (u64, u64)> = HashMap::new();
    for (_, source, weight) in &entries {
        let e = per_source.entry(*source).or_insert((u64::from(*weight), 0));
        e.1 += 1;
    }
    let total: u64 = per_source.values().map(|(w, _)| *w).sum();
    if total == 0 {
        return entries.into_iter().map(|(a, _, _)| (a, None)).collect();
    }
    entries
        .into_iter()
        .map(|(addr, source, _)| {
            let (weight, count) = per_source[&source];
            let share = (weight * SPREAD_TOTAL + total * count / 2) / (total * count);
            (addr, Some(u32::try_from(share.max(1)).unwrap_or(u32::MAX)))
        })
        .collect()
}

/// Formats `ip:port[ weight]`, spreading the weights first when asked.
fn format_addrs(entries: Vec<(String, usize, Option<u32>)>, spread_weights: bool) -> Vec<String> {
    let weighted: Vec<(String, Option<u32>)> = if spread_weights {
        spread(
            entries
                .into_iter()
                .map(|(a, s, w)| (a, s, w.unwrap_or(1)))
                .collect(),
        )
    } else {
        entries.into_iter().map(|(a, _, w)| (a, w)).collect()
    };
    let mut addrs: Vec<String> = weighted
        .into_iter()
        .map(|(addr, w)| match w {
            Some(w) => format!("{addr} {w}"),
            None => addr,
        })
        .collect();
    addrs.sort();
    addrs
}

/// Resolves the `addrs` of one upstream from its `services` entries, in
/// config order.
pub fn resolve(inputs: &[ServiceInput<'_>], opts: &UpstreamOptions) -> Resolved {
    if let UpstreamTarget::Service { cluster_domain } = &opts.target {
        return resolve_service_target(inputs, cluster_domain, opts);
    }
    let mut warnings = Vec::new();
    let mut seen: HashMap<SocketAddr, Entry> = HashMap::new();

    for (source, input) in inputs.iter().enumerate() {
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
                    .filter(is_pod_address)
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
                        source,
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

    let addrs = format_addrs(
        seen.iter()
            .filter(|(_, e)| e.class == wanted)
            .map(|(addr, e)| (addr.to_string(), e.source, e.weight))
            .collect(),
        opts.spread_weights,
    );

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

/// `target = "service"`: one Service address per entry, port is the Service `port`.
///
/// The address is the Service's ClusterIP (with `ipv4_only`, its first IPv4 one), so a
/// Service deleted and recreated with a new ClusterIP is followed by a write within the
/// debounce, like a pod change. Writing the DNS name instead left it to Pingap's DNS
/// discovery, which keeps an answer for its TTL (30 s with the default CoreDNS config)
/// whatever `update_frequency` says: on kind about 10 s of 503s after a recreate. A
/// Service without a ClusterIP (headless) gets its DNS name, which Pingap resolves itself
/// (an upstream with a hostname in `addrs` and no `discovery` uses DNS discovery).
fn resolve_service_target(
    inputs: &[ServiceInput<'_>],
    cluster_domain: &str,
    opts: &UpstreamOptions,
) -> Resolved {
    let ipv4_only = opts.ipv4_only;
    let mut warnings = Vec::new();
    // address -> (source, weight); the first entry in config order keeps its weight.
    let mut seen: HashMap<String, (usize, Option<u32>)> = HashMap::new();

    for (source, input) in inputs.iter().enumerate() {
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
        let Some(port) = u16::try_from(service_port.port).ok().filter(|p| *p != 0) else {
            warnings.push(ResolveWarning::PortNotInService {
                service: sref.key.clone(),
                port: sref.port.clone(),
            });
            continue;
        };
        let host = match cluster_ip(service, ipv4_only) {
            // The ClusterIP is routed only once the Service has endpoints (kube-proxy
            // programs it from the EndpointSlices): until then it contributes nothing
            // and the stored address is kept. Written earlier, Pingap's first health
            // check of a recreated Service timed out and held it unhealthy for a whole
            // check interval.
            Some(_) if !has_endpoint(input.snapshot, port_name) => continue,
            Some(IpAddr::V4(v4)) => v4.to_string(),
            Some(IpAddr::V6(v6)) => format!("[{v6}]"),
            None => format!(
                "{}.{}.svc.{cluster_domain}",
                sref.key.name, sref.key.namespace
            ),
        };
        seen.entry(format!("{host}:{port}"))
            .or_insert((source, sref.weight));
    }

    let addrs = format_addrs(
        seen.into_iter().map(|(a, (s, w))| (a, s, w)).collect(),
        opts.spread_weights,
    );
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

/// Whether an EndpointSlice of the Service has a ready or still serving endpoint on the
/// Service port named `port_name`.
fn has_endpoint(snapshot: &ServiceSnapshot, port_name: &str) -> bool {
    snapshot.slices.iter().any(|slice| {
        find_slice_port(slice, port_name).is_some()
            && slice
                .endpoints
                .iter()
                .any(|e| classify(e.conditions.as_ref(), false) != Class::Excluded)
    })
}

/// The ClusterIP to write for a Service: the first of `clusterIPs` (the primary family),
/// or with `ipv4_only` the first IPv4 one. `None` for a headless Service (`None`), a
/// Service without one, or no address of the wanted family.
fn cluster_ip(service: &Service, ipv4_only: bool) -> Option<IpAddr> {
    let spec = service.spec.as_ref()?;
    let ips: Vec<IpAddr> = match spec.cluster_ips.as_deref() {
        Some(ips) if !ips.is_empty() => ips.iter().filter_map(|ip| ip.parse().ok()).collect(),
        _ => spec
            .cluster_ip
            .iter()
            .filter_map(|ip| ip.parse().ok())
            .collect(),
    };
    ips.into_iter().find(|ip| !ipv4_only || ip.is_ipv4())
}

/// Kubernetes rejects these for pod and Endpoints addresses; an EndpointSlice
/// written by hand could still carry one and point a public upstream at the
/// node itself (loopback), cloud metadata (link-local) or nowhere.
fn is_pod_address(ip: &IpAddr) -> bool {
    let ip = ip.to_canonical();
    let special = match ip {
        IpAddr::V4(v4) => v4.is_link_local() || v4.is_broadcast(),
        IpAddr::V6(v6) => v6.is_unicast_link_local(),
    };
    !(special || ip.is_loopback() || ip.is_unspecified() || ip.is_multicast())
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
            spread_weights: false,
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
    fn non_pod_addresses_skipped() {
        let r = sref("api", named("http"), None);
        let s = snap(
            http_svc(),
            vec![v4(vec![
                ep("127.0.0.1", READY),
                ep("169.254.169.254", READY),
                ep("0.0.0.0", READY),
                ep("224.0.0.1", READY),
                ep("255.255.255.255", READY),
                ep("10.0.0.2", READY),
            ])],
        );
        assert_eq!(run1(&r, &s, &opts()).addrs, vec!["10.0.0.2:8080"]);

        let v6 = slice(
            "IPv6",
            &[("http", 8080, "TCP")],
            vec![
                ep("::1", READY),
                ep("::", READY),
                ep("fe80::1", READY),
                ep("ff02::1", READY),
                ep("::ffff:127.0.0.1", READY),
                ep("fd00::2", READY),
            ],
        );
        let s = snap(http_svc(), vec![v6]);
        assert_eq!(run1(&r, &s, &opts()).addrs, vec!["[fd00::2]:8080"]);
    }

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
                slice("IPv6", &[("http", 8080, "TCP")], vec![ep("fd00::1", READY)]),
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
            "[fd00::1]:8080 9",
        ];
        let got = resolve(&inputs, &opts());
        assert_eq!(got.addrs, want);
        assert_eq!(got.ready, 4);

        // Input order of slices and endpoints does not change the output.
        let sb_rev = snap(
            http_svc(),
            vec![
                slice("IPv6", &[("http", 8080, "TCP")], vec![ep("fd00::1", READY)]),
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
    fn spread_weights_split_each_service_over_its_addresses() {
        let blue = sref("web-blue", named("http"), Some(9));
        let green = sref("web-green", named("http"), Some(1));
        let sb = snap(
            http_svc(),
            vec![v4(vec![
                ep("10.0.0.9", READY),
                ep("10.0.0.10", READY),
                ep("10.0.0.11", READY),
            ])],
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
        let opts = UpstreamOptions {
            spread_weights: true,
            ..opts()
        };
        // 90 % over three pods, 10 % on one: 300 each and 100.
        assert_eq!(
            resolve(&inputs, &opts).addrs,
            vec![
                "10.0.0.10:8080 300",
                "10.0.0.11:8080 300",
                "10.0.0.9:8080 300",
                "10.0.1.1:8080 100",
            ]
        );

        // Huge Gateway API weights are scaled down; a tiny share still gets 1.
        let big = sref("web-blue", named("http"), Some(1_000_000));
        let tiny = sref("web-green", named("http"), Some(1));
        let inputs = [
            ServiceInput {
                service_ref: &big,
                snapshot: &sb,
            },
            ServiceInput {
                service_ref: &tiny,
                snapshot: &sg,
            },
        ];
        assert_eq!(
            resolve(&inputs, &opts).addrs,
            vec![
                "10.0.0.10:8080 333",
                "10.0.0.11:8080 333",
                "10.0.0.9:8080 333",
                "10.0.1.1:8080 1",
            ]
        );
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

    /// `service(ports)` with `clusterIP` / `clusterIPs` set.
    fn with_cluster_ips(svc: Arc<Service>, ips: &[&str]) -> Arc<Service> {
        let mut svc = (*svc).clone();
        let spec = svc.spec.as_mut().unwrap();
        spec.cluster_ip = ips.first().map(|ip| ip.to_string());
        spec.cluster_ips = Some(ips.iter().map(|ip| ip.to_string()).collect());
        Arc::new(svc)
    }

    #[test]
    fn service_target_writes_the_cluster_ip() {
        let web = service(&[("http", 80, "TCP")]);
        let r = sref("web", named("http"), Some(4));
        let cases: Vec<(&str, Vec<&str>, bool, &str)> = vec![
            ("IPv4", vec!["10.96.0.10"], false, "10.96.0.10:80 4"),
            (
                "IPv6 in brackets",
                vec!["fd00:10:96::a"],
                false,
                "[fd00:10:96::a]:80 4",
            ),
            (
                "dual-stack: the primary family",
                vec!["fd00:10:96::a", "10.96.0.10"],
                false,
                "[fd00:10:96::a]:80 4",
            ),
            (
                "dual-stack with ipv4_only: the IPv4 one",
                vec!["fd00:10:96::a", "10.96.0.10"],
                true,
                "10.96.0.10:80 4",
            ),
            (
                "IPv6 only with ipv4_only: the name",
                vec!["fd00:10:96::a"],
                true,
                "web.prod.svc.cluster.local:80 4",
            ),
            (
                "headless: the name",
                vec!["None"],
                false,
                "web.prod.svc.cluster.local:80 4",
            ),
        ];
        let ready = v4(vec![ep("10.244.0.5", READY)]);
        for (name, ips, ipv4_only, want) in cases {
            let s = snap(
                Some(with_cluster_ips(web.clone(), &ips)),
                vec![ready.clone()],
            );
            let mut o = svc_opts("cluster.local");
            o.ipv4_only = ipv4_only;
            let got = run1(&r, &s, &o);
            assert_eq!(got.addrs, vec![want], "{name}");
            assert!(got.warnings.is_empty(), "{name}");
        }
        // Only `clusterIP` set (older objects): it is used.
        let mut only = (*web).clone();
        only.spec.as_mut().unwrap().cluster_ip = Some("10.96.0.11".into());
        let only = Arc::new(only);
        let got = run1(
            &r,
            &snap(Some(only.clone()), vec![ready.clone()]),
            &svc_opts("cluster.local"),
        );
        assert_eq!(got.addrs, vec!["10.96.0.11:80 4"]);

        // Not routed yet: no slice, only not-ready endpoints, or a slice for another
        // port. A draining (still serving) endpoint counts.
        let cases: Vec<(&str, Vec<Arc<EndpointSlice>>, bool)> = vec![
            ("no slice", vec![], false),
            (
                "not ready",
                vec![v4(vec![ep("10.244.0.5", NOT_READY)])],
                false,
            ),
            (
                "other port",
                vec![slice(
                    "IPv4",
                    &[("grpc", 9, "TCP")],
                    vec![ep("10.244.0.5", READY)],
                )],
                false,
            ),
            ("draining", vec![v4(vec![ep("10.244.0.5", DRAINING)])], true),
            ("ready", vec![ready], true),
        ];
        for (name, slices, routed) in cases {
            let got = run1(
                &r,
                &snap(Some(only.clone()), slices),
                &svc_opts("cluster.local"),
            );
            let want: Vec<&str> = if routed {
                vec!["10.96.0.11:80 4"]
            } else {
                vec![]
            };
            assert_eq!(got.addrs, want, "{name}");
            assert!(got.warnings.is_empty(), "{name}");
        }
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

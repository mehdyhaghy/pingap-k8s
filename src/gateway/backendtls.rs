//! BackendTLSPolicies: which Service ports of route backends Pingap reaches over TLS, with
//! what settings, and the policy statuses (pure, no I/O).
//!
//! **Targets.** A policy applies to the Services of its `targetRefs` (core `Service`, in the
//! policy's namespace); with `sectionName` to the Service port of that name only. For one
//! backend Service port, a policy with the port's name as `sectionName` comes first; a
//! policy without `sectionName` covers the other ports. When several policies select the
//! same target (Service and `sectionName`), the oldest wins (creation timestamp, then
//! `namespace/name`); the others are `Accepted: False` with reason `Conflicted` on the
//! Gateways where that target is used. Other target kinds are ignored.
//!
//! **Validation.** `validation.hostname` is the SNI and the name the backend certificate
//! must match (Pingap `sni`, `verify_cert = true`). `caCertificateRefs` are core
//! ConfigMaps in the policy's namespace with the PEM bundle in `ca.crt` (Pingap `ca`, the
//! certificates of every valid reference); a missing ConfigMap or `ca.crt`, or one that
//! holds no certificate, is `ResolvedRefs: False` with `InvalidCACertificateRef`, any
//! other kind `InvalidKind`; when no reference is valid the policy is `Accepted: False`
//! with `NoValidCACertificate`. `wellKnownCACertificates: System` leaves `ca` out (Pingap
//! then verifies against the system trust store). Pingap verifies the certificate against
//! the SNI only, so a policy with `subjectAltNames` (which must not use the hostname for
//! authentication) is `Accepted: False` with `Invalid`, as is an unknown
//! `wellKnownCACertificates` value or an unusable hostname. Traffic to a backend whose
//! policy is not accepted gets a 500: the connection must fail rather than go out in
//! plain text or unverified.
//!
//! **Status.** One `ancestors` entry per Gateway of ours with an attached route that uses
//! a target of the policy, with `Accepted` and `ResolvedRefs`. Entries of other
//! controllers are kept by the status writer.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use k8s_openapi::api::core::v1::{ConfigMap, ServicePort};

use crate::types::{ServiceKey, UpstreamTls};

use super::api::backendtlspolicies::{
    BackendTlsPolicy, BackendTlsPolicyStatus, BackendTlsPolicyStatusAncestors,
    BackendTlsPolicyStatusAncestorsAncestorRef,
};
use super::model::{ConfigMapKey, GATEWAY_GROUP, GatewayKey, PolicyKey};
use super::tls;
use super::translate::{KIND_GATEWAY, KIND_SERVICE, age_key, cond, object_key, valid_host};

pub const KIND_CONFIG_MAP: &str = "ConfigMap";
/// The ConfigMap key of the CA bundle.
pub const CA_CERT_KEY: &str = "ca.crt";
/// `wellKnownCACertificates` value of the system trust store.
pub const WELL_KNOWN_SYSTEM: &str = "System";
/// Most `status.ancestors` entries the API allows.
pub const MAX_ANCESTORS: usize = 16;

/// What a policy selects: a Service, or one of its ports by name.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Target {
    pub service: ServiceKey,
    /// The Service port name of `sectionName`; `None`: the whole Service.
    pub section: Option<String>,
}

/// How Pingap reaches a backend Service port that a policy selects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PortTls {
    /// The policy's settings (without a client certificate: that is the Gateway's).
    Tls(UpstreamTls),
    /// The policy is not accepted: requests to the backend must fail.
    Invalid(String),
}

/// A policy, checked on its own.
struct Evaluated {
    generation: Option<i64>,
    /// `Err((reason, message))`: `Accepted: False`.
    accepted: Result<UpstreamTls, (&'static str, String)>,
    /// `Err((reason, message))`: `ResolvedRefs: False`.
    resolved: Result<(), (&'static str, String)>,
}

/// Every BackendTLSPolicy, checked, with the policies of each target in precedence order.
pub struct BackendTlsPolicies<'a> {
    objects: BTreeMap<PolicyKey, &'a Arc<BackendTlsPolicy>>,
    evaluated: BTreeMap<PolicyKey, Evaluated>,
    /// Target -> the policies that select it, the one that applies first.
    targets: BTreeMap<Target, Vec<PolicyKey>>,
}

/// The Service targets of a policy (core `Service` refs only).
fn service_targets(policy: &BackendTlsPolicy) -> Vec<Target> {
    let namespace = policy.metadata.namespace.clone().unwrap_or_default();
    policy
        .spec
        .target_refs
        .iter()
        .filter(|t| t.group.is_empty() && t.kind == KIND_SERVICE)
        .map(|t| Target {
            service: ServiceKey {
                namespace: namespace.clone(),
                name: t.name.clone(),
            },
            section: t.section_name.clone().filter(|s| !s.is_empty()),
        })
        .collect()
}

/// The ConfigMaps named by the policies that target one of `services`: what has to be
/// watched.
pub fn referenced_config_maps(
    policies: &[Arc<BackendTlsPolicy>],
    services: &BTreeSet<ServiceKey>,
) -> BTreeSet<ConfigMapKey> {
    let mut out = BTreeSet::new();
    for policy in policies {
        if !service_targets(policy)
            .iter()
            .any(|t| services.contains(&t.service))
        {
            continue;
        }
        let namespace = policy.metadata.namespace.clone().unwrap_or_default();
        let refs = policy.spec.validation.ca_certificate_refs.iter().flatten();
        for r in refs.filter(|r| r.group.is_empty() && r.kind == KIND_CONFIG_MAP) {
            out.insert(ConfigMapKey::new(namespace.clone(), r.name.clone()));
        }
    }
    out
}

/// Checks a policy against the ConfigMaps it names.
fn evaluate(
    policy: &BackendTlsPolicy,
    config_maps: &BTreeMap<ConfigMapKey, Arc<ConfigMap>>,
) -> Evaluated {
    let namespace = policy.metadata.namespace.clone().unwrap_or_default();
    let validation = &policy.spec.validation;
    let mut evaluated = Evaluated {
        generation: policy.metadata.generation,
        accepted: Err(("Invalid", String::new())),
        resolved: Ok(()),
    };

    // CA certificate references.
    let refs = validation
        .ca_certificate_refs
        .as_deref()
        .unwrap_or_default();
    let mut bundle = String::new();
    let mut problems: Vec<(&'static str, String)> = Vec::new();
    for r in refs {
        if !r.group.is_empty() || r.kind != KIND_CONFIG_MAP {
            let shown = if r.group.is_empty() {
                r.kind.clone()
            } else {
                format!("{}/{}", r.group, r.kind)
            };
            problems.push((
                "InvalidKind",
                format!(
                    "caCertificateRef {} has the unsupported kind {shown} (only ConfigMap)",
                    r.name
                ),
            ));
            continue;
        }
        let key = ConfigMapKey::new(namespace.clone(), r.name.clone());
        let Some(object) = config_maps.get(&key) else {
            problems.push((
                "InvalidCACertificateRef",
                format!("ConfigMap {key} does not exist"),
            ));
            continue;
        };
        let Some(text) = object.data.as_ref().and_then(|d| d.get(CA_CERT_KEY)) else {
            problems.push((
                "InvalidCACertificateRef",
                format!("ConfigMap {key} has no {CA_CERT_KEY}"),
            ));
            continue;
        };
        match tls::ca_bundle(text) {
            Ok(pem) => bundle.push_str(&pem),
            Err(reason) => problems.push((
                "InvalidCACertificateRef",
                format!("{CA_CERT_KEY} of ConfigMap {key}: {reason}"),
            )),
        }
    }
    if let Some((reason, _)) = problems.first() {
        let message = problems
            .iter()
            .map(|(_, m)| m.as_str())
            .collect::<Vec<_>>()
            .join("; ");
        evaluated.resolved = Err((reason, message));
    }

    let well_known = validation
        .well_known_ca_certificates
        .as_deref()
        .filter(|w| !w.is_empty());
    let hostname = validation.hostname.as_str();
    let invalid = |message: String| Err(("Invalid", message));
    let host_problem = if hostname.starts_with("*.") {
        Some("must not be a wildcard".to_string())
    } else {
        valid_host(hostname).err()
    };
    evaluated.accepted = if let Some(reason) = host_problem {
        invalid(format!("hostname {hostname:?} {reason}"))
    } else if validation
        .subject_alt_names
        .as_ref()
        .is_some_and(|s| !s.is_empty())
    {
        invalid(
            "subjectAltNames is not supported: Pingap verifies the backend certificate \
             against the hostname only"
                .into(),
        )
    } else if !refs.is_empty() && well_known.is_some() {
        invalid("caCertificateRefs and wellKnownCACertificates are both set".into())
    } else if let Some(name) = well_known {
        if name == WELL_KNOWN_SYSTEM {
            Ok(UpstreamTls {
                sni: hostname.to_string(),
                ca: None,
                client_cert: None,
            })
        } else {
            invalid(format!("wellKnownCACertificates {name:?} is not supported"))
        }
    } else if refs.is_empty() {
        invalid("neither caCertificateRefs nor wellKnownCACertificates is set".into())
    } else if bundle.is_empty() {
        Err((
            "NoValidCACertificate",
            "no caCertificateRef is valid".to_string(),
        ))
    } else {
        Ok(UpstreamTls {
            sni: hostname.to_string(),
            ca: Some(bundle),
            client_cert: None,
        })
    };
    evaluated
}

impl<'a> BackendTlsPolicies<'a> {
    /// Checks every policy; `config_maps` holds the watched CA ConfigMaps.
    pub fn new(
        policies: &'a [Arc<BackendTlsPolicy>],
        config_maps: &BTreeMap<ConfigMapKey, Arc<ConfigMap>>,
    ) -> Self {
        let mut sorted: Vec<&'a Arc<BackendTlsPolicy>> = policies.iter().collect();
        sorted.sort_by_cached_key(|p| age_key(&p.metadata));
        let mut out = BackendTlsPolicies {
            objects: BTreeMap::new(),
            evaluated: BTreeMap::new(),
            targets: BTreeMap::new(),
        };
        for policy in sorted {
            let key = object_key(&policy.metadata);
            for target in service_targets(policy) {
                let list = out.targets.entry(target).or_default();
                if !list.contains(&key) {
                    list.push(key.clone());
                }
            }
            out.evaluated
                .insert(key.clone(), evaluate(policy, config_maps));
            out.objects.insert(key, policy);
        }
        out
    }

    /// The target that decides how Pingap reaches port `port` of `service`, and how. `None`:
    /// no policy selects it (plain text).
    pub fn lookup(&self, service: &ServiceKey, port: &ServicePort) -> Option<(Target, PortTls)> {
        let by_name = port
            .name
            .as_ref()
            .filter(|n| !n.is_empty())
            .map(|n| Target {
                service: service.clone(),
                section: Some(n.clone()),
            });
        let whole = Target {
            service: service.clone(),
            section: None,
        };
        let (target, keys) = by_name
            .and_then(|t| self.targets.get(&t).map(|k| (t, k)))
            .or_else(|| self.targets.get(&whole).map(|k| (whole, k)))?;
        let winner = keys.first()?;
        let tls = match &self.evaluated.get(winner)?.accepted {
            Ok(tls) => PortTls::Tls(tls.clone()),
            Err((_, message)) => PortTls::Invalid(format!(
                "BackendTLSPolicy {winner} is not accepted: {message}"
            )),
        };
        Some((target, tls))
    }

    /// The desired status of every policy used through a Gateway of ours (`uses`: target ->
    /// the Gateways of the attached routes that use it), and an empty one (our entries
    /// removed) for every other policy that holds an entry of `controller`.
    pub fn statuses(
        &self,
        uses: &BTreeMap<Target, BTreeSet<GatewayKey>>,
        controller: &str,
    ) -> BTreeMap<PolicyKey, BackendTlsPolicyStatus> {
        // Policy -> Gateway -> conflicted there.
        let mut ancestors: BTreeMap<&PolicyKey, BTreeMap<&GatewayKey, Option<&PolicyKey>>> =
            BTreeMap::new();
        for (target, gateways) in uses {
            let Some(keys) = self.targets.get(target) else {
                continue;
            };
            let Some(winner) = keys.first() else {
                continue;
            };
            for key in keys {
                let entry = ancestors.entry(key).or_default();
                for gateway in gateways {
                    let conflicted = (key != winner).then_some(winner);
                    let slot = entry.entry(gateway).or_insert(None);
                    if slot.is_none() {
                        *slot = conflicted;
                    }
                }
            }
        }

        let mut out = BTreeMap::new();
        for (key, gateways) in ancestors {
            let Some(evaluated) = self.evaluated.get(key) else {
                continue;
            };
            let generation = evaluated.generation;
            let foreign = self.objects.get(key).map_or(0, |p| {
                p.status.as_ref().map_or(0, |s| {
                    s.ancestors
                        .iter()
                        .filter(|a| a.controller_name != controller)
                        .count()
                })
            });
            let room = MAX_ANCESTORS.saturating_sub(foreign);
            let list = gateways
                .into_iter()
                .take(room)
                .map(|(gateway, conflicted)| {
                    let accepted = match (&evaluated.accepted, conflicted) {
                        (Err((reason, message)), _) => {
                            cond("Accepted", false, reason, message, generation)
                        }
                        (Ok(_), Some(winner)) => cond(
                            "Accepted",
                            false,
                            "Conflicted",
                            &format!(
                                "BackendTLSPolicy {winner} takes precedence for the same target"
                            ),
                            generation,
                        ),
                        (Ok(_), None) => cond(
                            "Accepted",
                            true,
                            "Accepted",
                            "Policy is accepted",
                            generation,
                        ),
                    };
                    let resolved = match &evaluated.resolved {
                        Ok(()) => cond(
                            "ResolvedRefs",
                            true,
                            "ResolvedRefs",
                            "All references are resolved",
                            generation,
                        ),
                        Err((reason, message)) => {
                            cond("ResolvedRefs", false, reason, message, generation)
                        }
                    };
                    BackendTlsPolicyStatusAncestors {
                        ancestor_ref: gateway_ancestor(gateway),
                        conditions: vec![accepted, resolved],
                        controller_name: controller.to_string(),
                    }
                })
                .collect();
            out.insert(key.clone(), BackendTlsPolicyStatus { ancestors: list });
        }
        for (key, policy) in &self.objects {
            let ours = policy
                .status
                .as_ref()
                .is_some_and(|s| s.ancestors.iter().any(|a| a.controller_name == controller));
            if ours && !out.contains_key(key) {
                out.insert((*key).clone(), BackendTlsPolicyStatus::default());
            }
        }
        out
    }
}

fn gateway_ancestor(gateway: &GatewayKey) -> BackendTlsPolicyStatusAncestorsAncestorRef {
    BackendTlsPolicyStatusAncestorsAncestorRef {
        group: Some(GATEWAY_GROUP.into()),
        kind: Some(KIND_GATEWAY.into()),
        name: gateway.name.clone(),
        namespace: Some(gateway.namespace.clone()),
        port: None,
        section_name: None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::gateway::tls::tests::cert_pem;
    use k8s_openapi::jiff::Timestamp;

    const CONTROLLER: &str = "pingap.io/gateway-controller";

    fn ts(age: i64) -> String {
        Timestamp::from_second(1_700_000_000 + age)
            .unwrap()
            .to_string()
    }

    /// A policy in `ns` targeting `targets` (`(service, sectionName)`) with `validation`.
    fn policy(
        ns: &str,
        name: &str,
        age: i64,
        targets: &[(&str, Option<&str>)],
        validation: Value,
    ) -> Arc<BackendTlsPolicy> {
        let refs: Vec<Value> = targets
            .iter()
            .map(|(svc, section)| {
                let mut t = json!({"group": "", "kind": "Service", "name": svc});
                if let Some(s) = section {
                    t["sectionName"] = json!(s);
                }
                t
            })
            .collect();
        Arc::new(
            serde_json::from_value(json!({
                "apiVersion": "gateway.networking.k8s.io/v1",
                "kind": "BackendTLSPolicy",
                "metadata": {
                    "namespace": ns, "name": name, "generation": 4,
                    "creationTimestamp": ts(age),
                },
                "spec": {"targetRefs": refs, "validation": validation},
            }))
            .unwrap(),
        )
    }

    fn ca_validation(host: &str, config_map: &str) -> Value {
        json!({
            "hostname": host,
            "caCertificateRefs": [{"group": "", "kind": "ConfigMap", "name": config_map}],
        })
    }

    fn config_map(ns: &str, name: &str, data: &[(&str, &str)]) -> (ConfigMapKey, Arc<ConfigMap>) {
        let cm: ConfigMap = serde_json::from_value(json!({
            "metadata": {"namespace": ns, "name": name},
            "data": data.iter().map(|(k, v)| (k.to_string(), json!(v))).collect::<serde_json::Map<_, _>>(),
        }))
        .unwrap();
        (ConfigMapKey::new(ns, name), Arc::new(cm))
    }

    fn ca_maps() -> BTreeMap<ConfigMapKey, Arc<ConfigMap>> {
        [
            config_map("ns", "ca", &[(CA_CERT_KEY, cert_pem())]),
            config_map("ns", "empty", &[(CA_CERT_KEY, "")]),
            config_map("ns", "nokey", &[]),
        ]
        .into_iter()
        .collect()
    }

    fn port(name: Option<&str>, number: i32) -> ServicePort {
        ServicePort {
            name: name.map(String::from),
            port: number,
            ..Default::default()
        }
    }

    fn svc(name: &str) -> ServiceKey {
        ServiceKey {
            namespace: "ns".into(),
            name: name.into(),
        }
    }

    fn uses(entries: &[(&str, Option<&str>, &str)]) -> BTreeMap<Target, BTreeSet<GatewayKey>> {
        let mut out: BTreeMap<Target, BTreeSet<GatewayKey>> = BTreeMap::new();
        for (service, section, gw) in entries {
            out.entry(Target {
                service: svc(service),
                section: section.map(String::from),
            })
            .or_default()
            .insert(GatewayKey::new("ns", *gw));
        }
        out
    }

    fn conditions(
        statuses: &BTreeMap<PolicyKey, BackendTlsPolicyStatus>,
        policy: &str,
    ) -> Vec<(String, String, String)> {
        statuses[&PolicyKey::new("ns", policy)].ancestors[0]
            .conditions
            .iter()
            .map(|c| (c.type_.clone(), c.status.clone(), c.reason.clone()))
            .collect()
    }

    fn pair(t: &str, s: &str, r: &str) -> (String, String, String) {
        (t.into(), s.into(), r.into())
    }

    #[test]
    fn valid_policy_gives_tls_and_accepted_status() {
        let policies = vec![policy(
            "ns",
            "p",
            0,
            &[("api", Some("https"))],
            ca_validation("api.example.com", "ca"),
        )];
        let index = BackendTlsPolicies::new(&policies, &ca_maps());
        let (target, tls) = index
            .lookup(&svc("api"), &port(Some("https"), 443))
            .unwrap();
        assert_eq!(target.section.as_deref(), Some("https"));
        assert_eq!(
            tls,
            PortTls::Tls(UpstreamTls {
                sni: "api.example.com".into(),
                ca: Some(cert_pem().to_string()),
                client_cert: None,
            })
        );
        // Another port of the Service, and another Service: no policy.
        assert_eq!(index.lookup(&svc("api"), &port(Some("http"), 80)), None);
        assert_eq!(index.lookup(&svc("web"), &port(Some("https"), 443)), None);

        let statuses = index.statuses(&uses(&[("api", Some("https"), "gw")]), CONTROLLER);
        assert_eq!(
            conditions(&statuses, "p"),
            vec![
                pair("Accepted", "True", "Accepted"),
                pair("ResolvedRefs", "True", "ResolvedRefs")
            ]
        );
        let ancestor = &statuses[&PolicyKey::new("ns", "p")].ancestors[0];
        assert_eq!(ancestor.controller_name, CONTROLLER);
        assert_eq!(ancestor.ancestor_ref.name, "gw");
        assert_eq!(ancestor.ancestor_ref.kind.as_deref(), Some("Gateway"));
        assert_eq!(ancestor.ancestor_ref.group.as_deref(), Some(GATEWAY_GROUP));
        assert_eq!(ancestor.ancestor_ref.namespace.as_deref(), Some("ns"));
        assert!(
            ancestor
                .conditions
                .iter()
                .all(|c| c.observed_generation == Some(4))
        );
    }

    #[test]
    fn invalid_ca_refs() {
        let policies = vec![
            policy(
                "ns",
                "missing",
                0,
                &[("a", None)],
                ca_validation("a.example.com", "nope"),
            ),
            policy(
                "ns",
                "empty",
                0,
                &[("b", None)],
                ca_validation("b.example.com", "empty"),
            ),
            policy(
                "ns",
                "nokey",
                0,
                &[("c", None)],
                ca_validation("c.example.com", "nokey"),
            ),
            policy(
                "ns",
                "kind",
                0,
                &[("d", None)],
                json!({"hostname": "d.example.com",
                       "caCertificateRefs": [{"group": "invalid.io", "kind": "InvalidKind", "name": "x"}]}),
            ),
            policy(
                "ns",
                "partial",
                0,
                &[("e", None)],
                json!({"hostname": "e.example.com", "caCertificateRefs": [
                    {"group": "", "kind": "ConfigMap", "name": "ca"},
                    {"group": "", "kind": "ConfigMap", "name": "nope"},
                ]}),
            ),
        ];
        let index = BackendTlsPolicies::new(&policies, &ca_maps());
        let statuses = index.statuses(
            &uses(&[
                ("a", None, "gw"),
                ("b", None, "gw"),
                ("c", None, "gw"),
                ("d", None, "gw"),
                ("e", None, "gw"),
            ]),
            CONTROLLER,
        );
        for name in ["missing", "empty", "nokey"] {
            assert_eq!(
                conditions(&statuses, name),
                vec![
                    pair("Accepted", "False", "NoValidCACertificate"),
                    pair("ResolvedRefs", "False", "InvalidCACertificateRef")
                ],
                "{name}"
            );
        }
        assert_eq!(
            conditions(&statuses, "kind"),
            vec![
                pair("Accepted", "False", "NoValidCACertificate"),
                pair("ResolvedRefs", "False", "InvalidKind")
            ]
        );
        // One valid reference: accepted with what is valid, ResolvedRefs false.
        assert_eq!(
            conditions(&statuses, "partial"),
            vec![
                pair("Accepted", "True", "Accepted"),
                pair("ResolvedRefs", "False", "InvalidCACertificateRef")
            ]
        );
        assert!(matches!(
            index.lookup(&svc("e"), &port(None, 443)),
            Some((_, PortTls::Tls(UpstreamTls { ca: Some(_), .. })))
        ));
        // Requests to the backends of an invalid policy fail.
        for s in ["a", "b", "c", "d"] {
            assert!(
                matches!(
                    index.lookup(&svc(s), &port(None, 443)),
                    Some((_, PortTls::Invalid(_)))
                ),
                "{s}"
            );
        }
    }

    #[test]
    fn unsupported_validation_is_invalid() {
        let policies = vec![
            policy(
                "ns",
                "system",
                0,
                &[("a", None)],
                json!({"hostname": "a.example.com", "wellKnownCACertificates": "System"}),
            ),
            policy(
                "ns",
                "custom",
                0,
                &[("b", None)],
                json!({"hostname": "b.example.com", "wellKnownCACertificates": "example.com/mine"}),
            ),
            policy(
                "ns",
                "san",
                0,
                &[("c", None)],
                json!({"hostname": "c.example.com",
                       "caCertificateRefs": [{"group": "", "kind": "ConfigMap", "name": "ca"}],
                       "subjectAltNames": [{"type": "Hostname", "hostname": "c.example.com"}]}),
            ),
            policy(
                "ns",
                "host",
                0,
                &[("d", None)],
                ca_validation("*.example.com", "ca"),
            ),
        ];
        let index = BackendTlsPolicies::new(&policies, &ca_maps());
        assert_eq!(
            index.lookup(&svc("a"), &port(None, 443)).unwrap().1,
            PortTls::Tls(UpstreamTls {
                sni: "a.example.com".into(),
                ca: None,
                client_cert: None,
            })
        );
        let statuses = index.statuses(
            &uses(&[
                ("a", None, "gw"),
                ("b", None, "gw"),
                ("c", None, "gw"),
                ("d", None, "gw"),
            ]),
            CONTROLLER,
        );
        assert_eq!(
            conditions(&statuses, "system")[0],
            pair("Accepted", "True", "Accepted")
        );
        for name in ["custom", "san", "host"] {
            assert_eq!(
                conditions(&statuses, name),
                vec![
                    pair("Accepted", "False", "Invalid"),
                    pair("ResolvedRefs", "True", "ResolvedRefs")
                ],
                "{name}"
            );
        }
        assert!(matches!(
            index.lookup(&svc("c"), &port(None, 443)),
            Some((_, PortTls::Invalid(_)))
        ));
    }

    #[test]
    fn conflicts_oldest_wins_and_section_comes_first() {
        let policies = vec![
            policy(
                "ns",
                "newer",
                5,
                &[("a", None)],
                ca_validation("new.example.com", "ca"),
            ),
            policy(
                "ns",
                "older",
                1,
                &[("a", None)],
                ca_validation("old.example.com", "ca"),
            ),
            // Same age: by name.
            policy(
                "ns",
                "s2",
                1,
                &[("b", Some("https"))],
                ca_validation("s2.example.com", "ca"),
            ),
            policy(
                "ns",
                "s1",
                1,
                &[("b", Some("https"))],
                ca_validation("s1.example.com", "ca"),
            ),
            // A port policy and a whole-Service policy do not conflict.
            policy(
                "ns",
                "port",
                9,
                &[("c", Some("https-1"))],
                ca_validation("port.example.com", "ca"),
            ),
            policy(
                "ns",
                "whole",
                0,
                &[("c", None)],
                ca_validation("whole.example.com", "ca"),
            ),
        ];
        let index = BackendTlsPolicies::new(&policies, &ca_maps());
        let sni = |s: &str, p: ServicePort| match index.lookup(&svc(s), &p).unwrap().1 {
            PortTls::Tls(t) => t.sni,
            other => panic!("{other:?}"),
        };
        assert_eq!(sni("a", port(Some("https"), 443)), "old.example.com");
        assert_eq!(sni("b", port(Some("https"), 443)), "s1.example.com");
        assert_eq!(sni("c", port(Some("https-1"), 443)), "port.example.com");
        assert_eq!(sni("c", port(Some("https-2"), 8443)), "whole.example.com");
        // `b` has no whole-Service policy: another port is plain.
        assert_eq!(index.lookup(&svc("b"), &port(Some("http"), 80)), None);

        let statuses = index.statuses(
            &uses(&[
                ("a", None, "gw"),
                ("b", Some("https"), "gw"),
                ("c", Some("https-1"), "gw"),
                ("c", None, "gw"),
            ]),
            CONTROLLER,
        );
        for (name, ok) in [
            ("older", true),
            ("newer", false),
            ("s1", true),
            ("s2", false),
            ("port", true),
            ("whole", true),
        ] {
            let accepted = &conditions(&statuses, name)[0];
            if ok {
                assert_eq!(accepted, &pair("Accepted", "True", "Accepted"), "{name}");
            } else {
                assert_eq!(accepted, &pair("Accepted", "False", "Conflicted"), "{name}");
            }
        }
    }

    #[test]
    fn ancestors_per_gateway_and_stale_entries_cleared() {
        let mut stale = (*policy(
            "ns",
            "stale",
            0,
            &[("x", None)],
            ca_validation("x.example.com", "ca"),
        ))
        .clone();
        stale.status = Some(BackendTlsPolicyStatus {
            ancestors: vec![BackendTlsPolicyStatusAncestors {
                ancestor_ref: gateway_ancestor(&GatewayKey::new("ns", "gw")),
                conditions: Vec::new(),
                controller_name: CONTROLLER.into(),
            }],
        });
        let mut foreign = (*policy(
            "ns",
            "foreign",
            0,
            &[("y", None)],
            ca_validation("y.example.com", "ca"),
        ))
        .clone();
        foreign.status = Some(BackendTlsPolicyStatus {
            ancestors: vec![BackendTlsPolicyStatusAncestors {
                ancestor_ref: gateway_ancestor(&GatewayKey::new("ns", "other")),
                conditions: Vec::new(),
                controller_name: "x.io/other".into(),
            }],
        });
        let policies = vec![
            policy(
                "ns",
                "p",
                0,
                &[("a", None)],
                ca_validation("a.example.com", "ca"),
            ),
            Arc::new(stale),
            Arc::new(foreign),
        ];
        let index = BackendTlsPolicies::new(&policies, &ca_maps());
        let statuses = index.statuses(&uses(&[("a", None, "gw1"), ("a", None, "gw2")]), CONTROLLER);
        let names: Vec<&str> = statuses[&PolicyKey::new("ns", "p")]
            .ancestors
            .iter()
            .map(|a| a.ancestor_ref.name.as_str())
            .collect();
        assert_eq!(names, ["gw1", "gw2"]);
        // Ours removed from a policy no longer used; a policy with only foreign entries
        // is left alone.
        assert_eq!(
            statuses[&PolicyKey::new("ns", "stale")],
            BackendTlsPolicyStatus::default()
        );
        assert!(!statuses.contains_key(&PolicyKey::new("ns", "foreign")));
    }

    #[test]
    fn referenced_config_maps_of_backend_services_only() {
        let policies = vec![
            policy(
                "ns",
                "p",
                0,
                &[("a", None)],
                ca_validation("a.example.com", "ca-a"),
            ),
            policy(
                "ns",
                "q",
                0,
                &[("b", None)],
                ca_validation("b.example.com", "ca-b"),
            ),
            policy(
                "other",
                "r",
                0,
                &[("a", None)],
                json!({"hostname": "a.example.com", "caCertificateRefs": [
                    {"group": "", "kind": "ConfigMap", "name": "ca-r"},
                    {"group": "", "kind": "Secret", "name": "not-watched"},
                ]}),
            ),
        ];
        let services: BTreeSet<ServiceKey> = [
            svc("a"),
            ServiceKey {
                namespace: "other".into(),
                name: "a".into(),
            },
        ]
        .into();
        assert_eq!(
            referenced_config_maps(&policies, &services),
            [
                ConfigMapKey::new("ns", "ca-a"),
                ConfigMapKey::new("other", "ca-r")
            ]
            .into()
        );
    }
}

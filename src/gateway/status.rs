//! Gateway provider: the `status` subresources of GatewayClasses, Gateways, ListenerSets
//! and HTTPRoutes.
//!
//! The desired statuses come from the translation with a placeholder
//! `lastTransitionTime`. Before comparing, every desired condition takes the stored time
//! of the condition of the same `type` when its `status` is unchanged, and the current
//! time otherwise; listener conditions are matched within the listener of the same name,
//! route parent conditions within the entry of the same `parentRef` and `controllerName`.
//!
//! The merged status is compared with the stored one order-insensitively where the API
//! treats a list as a set (conditions by type, listeners by name, route parents by
//! controller and `parentRef`), with the values the API server defaults filled in, and
//! written only when they differ. A write is a JSON merge patch of the `status`
//! subresource carrying the `resourceVersion` it was based on; on a 409 Conflict the
//! object is read again and the merge redone, a few times.
//!
//! GatewayClass, Gateway and ListenerSet status is ours: listeners and addresses are
//! replaced whole; other fields (`supportedFeatures`, `attachedListenerSets`) are only
//! written when the translation sets them. Conditions of a type this controller never
//! sets (on the object or on a listener) belong to someone else and are kept as they
//! are. Route `status.parents` is shared with other controllers: their entries are kept
//! as they are, only ours are replaced or removed.

use std::fmt::Debug;

use anyhow::Context;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;
use kube::api::{Patch, PatchParams};
use kube::{Api, Client, Resource, ResourceExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tracing::debug;

use super::api::gatewayclasses::{GatewayClass, GatewayClassStatus};
use super::api::gateways::{
    Gateway, GatewayStatus, GatewayStatusAddresses, GatewayStatusListeners,
};
use super::api::httproutes::{HttpRoute, HttpRouteStatus, HttpRouteStatusParents};
use super::api::listenersets::{ListenerSet, ListenerSetStatus, ListenerSetStatusListeners};
use super::model::GATEWAY_GROUP;

/// Field manager of the status patches.
pub const FIELD_MANAGER: &str = "pingap-k8s";

/// Writes per status update before giving up on conflicts.
const MAX_ATTEMPTS: usize = 4;

/// API server default of `status.addresses[].type`.
const DEFAULT_ADDRESS_TYPE: &str = "IPAddress";
/// API server default of `parentRef.kind`.
const DEFAULT_PARENT_KIND: &str = "Gateway";

/// The condition types this controller sets, per object: other types are kept.
const CLASS_CONDITIONS: &[&str] = &["Accepted", "SupportedVersion"];
const GATEWAY_CONDITIONS: &[&str] = &["Accepted", "Programmed"];
const LISTENER_SET_CONDITIONS: &[&str] = &["Accepted", "Programmed"];
const LISTENER_CONDITIONS: &[&str] = &["Accepted", "Conflicted", "ResolvedRefs", "Programmed"];

/// The current time, whole seconds (what the API server stores).
pub fn now() -> Time {
    let now = Timestamp::now();
    Time(Timestamp::from_second(now.as_second()).unwrap_or(now))
}

/// `desired` with each `last_transition_time` taken from the `stored` condition of the
/// same type when its status is unchanged, and `now` otherwise.
pub fn merge_conditions(stored: &[Condition], desired: &[Condition], now: &Time) -> Vec<Condition> {
    desired
        .iter()
        .map(|want| {
            let time = stored
                .iter()
                .find(|c| c.type_ == want.type_ && c.status == want.status)
                .map_or_else(|| now.clone(), |c| c.last_transition_time.clone());
            Condition {
                last_transition_time: time,
                ..want.clone()
            }
        })
        .collect()
}

/// [`merge_conditions`] followed by the `stored` conditions of the types this controller
/// does not set (`owned`, or desired), unchanged.
pub fn merge_keeping_foreign(
    stored: &[Condition],
    desired: &[Condition],
    owned: &[&str],
    now: &Time,
) -> Vec<Condition> {
    let mut out = merge_conditions(stored, desired, now);
    out.extend(
        stored
            .iter()
            .filter(|c| {
                !owned.contains(&c.type_.as_str()) && !desired.iter().any(|d| d.type_ == c.type_)
            })
            .cloned(),
    );
    out
}

fn sorted_conditions(conditions: &[Condition]) -> Vec<Condition> {
    let mut list = conditions.to_vec();
    list.sort_by(|a, b| a.type_.cmp(&b.type_));
    list
}

/// Whether two condition lists hold the same conditions, in any order.
pub fn conditions_equal(a: &[Condition], b: &[Condition]) -> bool {
    sorted_conditions(a) == sorted_conditions(b)
}

fn non_empty<T>(list: Option<Vec<T>>) -> Option<Vec<T>> {
    list.filter(|l| !l.is_empty())
}

// GatewayClass

/// The GatewayClass status to store: `desired` with transition times merged.
/// `supportedFeatures` is kept from `stored` unless `desired` sets it.
pub fn merge_class_status(
    stored: Option<&GatewayClassStatus>,
    desired: &GatewayClassStatus,
    now: &Time,
) -> GatewayClassStatus {
    let stored_conditions = stored.and_then(|s| s.conditions.as_deref()).unwrap_or(&[]);
    GatewayClassStatus {
        conditions: desired
            .conditions
            .as_deref()
            .map(|d| merge_keeping_foreign(stored_conditions, d, CLASS_CONDITIONS, now)),
        supported_features: desired
            .supported_features
            .clone()
            .or_else(|| stored.and_then(|s| s.supported_features.clone())),
    }
}

fn canonical_class(status: Option<&GatewayClassStatus>) -> GatewayClassStatus {
    let status = status.cloned().unwrap_or_default();
    let mut features = non_empty(status.supported_features);
    if let Some(f) = features.as_mut() {
        f.sort_by(|a, b| a.name.cmp(&b.name));
    }
    GatewayClassStatus {
        conditions: non_empty(status.conditions).map(|c| sorted_conditions(&c)),
        supported_features: features,
    }
}

/// Whether two GatewayClass statuses are the same (lists compared as sets).
pub fn class_status_equal(a: Option<&GatewayClassStatus>, b: Option<&GatewayClassStatus>) -> bool {
    canonical_class(a) == canonical_class(b)
}

/// The `status` object of the merge patch: conditions are always written (null clears).
pub fn class_patch_body(status: &GatewayClassStatus) -> Value {
    let mut body =
        serde_json::to_value(status).unwrap_or_else(|_| Value::Object(Default::default()));
    set_null_if_absent(&mut body, &["conditions"]);
    body
}

// Gateway

fn normalize_addresses(
    addresses: Option<&Vec<GatewayStatusAddresses>>,
) -> Option<Vec<GatewayStatusAddresses>> {
    non_empty(addresses.cloned()).map(|list| {
        list.into_iter()
            .map(|mut a| {
                a.r#type
                    .get_or_insert_with(|| DEFAULT_ADDRESS_TYPE.to_string());
                a
            })
            .collect()
    })
}

fn normalize_listener(mut listener: GatewayStatusListeners) -> GatewayStatusListeners {
    if let Some(kinds) = listener.supported_kinds.as_mut() {
        for kind in kinds.iter_mut() {
            kind.group.get_or_insert_with(|| GATEWAY_GROUP.to_string());
        }
    }
    listener
}

/// The Gateway status to store: `desired` with transition times merged (listener
/// conditions within the stored listener of the same name), foreign conditions kept and
/// API server defaults filled in. `attachedListenerSets` is kept from `stored` unless
/// `desired` sets it.
pub fn merge_gateway_status(
    stored: Option<&GatewayStatus>,
    desired: &GatewayStatus,
    now: &Time,
) -> GatewayStatus {
    let stored_conditions = stored.and_then(|s| s.conditions.as_deref()).unwrap_or(&[]);
    let stored_listeners = stored.and_then(|s| s.listeners.as_deref()).unwrap_or(&[]);
    let listeners = desired.listeners.as_ref().map(|list| {
        list.iter()
            .map(|want| {
                let old = stored_listeners
                    .iter()
                    .find(|l| l.name == want.name)
                    .map_or(&[][..], |l| l.conditions.as_slice());
                normalize_listener(GatewayStatusListeners {
                    conditions: merge_keeping_foreign(
                        old,
                        &want.conditions,
                        LISTENER_CONDITIONS,
                        now,
                    ),
                    ..want.clone()
                })
            })
            .collect()
    });
    GatewayStatus {
        addresses: normalize_addresses(desired.addresses.as_ref()),
        attached_listener_sets: desired
            .attached_listener_sets
            .or_else(|| stored.and_then(|s| s.attached_listener_sets)),
        conditions: desired
            .conditions
            .as_deref()
            .map(|d| merge_keeping_foreign(stored_conditions, d, GATEWAY_CONDITIONS, now)),
        listeners: non_empty(listeners),
    }
}

fn canonical_gateway(status: Option<&GatewayStatus>) -> GatewayStatus {
    let status = status.cloned().unwrap_or_default();
    let listeners = non_empty(status.listeners).map(|list| {
        let mut list: Vec<GatewayStatusListeners> = list
            .into_iter()
            .map(|l| {
                let mut l = normalize_listener(l);
                l.conditions = sorted_conditions(&l.conditions);
                l.supported_kinds = non_empty(l.supported_kinds);
                l
            })
            .collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
    });
    GatewayStatus {
        addresses: normalize_addresses(status.addresses.as_ref()),
        attached_listener_sets: status.attached_listener_sets,
        conditions: non_empty(status.conditions).map(|c| sorted_conditions(&c)),
        listeners,
    }
}

/// Whether two Gateway statuses are the same (conditions and listeners compared as sets,
/// API server defaults filled in).
pub fn gateway_status_equal(a: Option<&GatewayStatus>, b: Option<&GatewayStatus>) -> bool {
    canonical_gateway(a) == canonical_gateway(b)
}

/// The `status` object of the merge patch: conditions, listeners and addresses are always
/// written (null clears).
pub fn gateway_patch_body(status: &GatewayStatus) -> Value {
    let mut body =
        serde_json::to_value(status).unwrap_or_else(|_| Value::Object(Default::default()));
    set_null_if_absent(&mut body, &["addresses", "conditions", "listeners"]);
    body
}

// ListenerSet

fn normalize_set_listener(mut listener: ListenerSetStatusListeners) -> ListenerSetStatusListeners {
    if let Some(kinds) = listener.supported_kinds.as_mut() {
        for kind in kinds.iter_mut() {
            kind.group.get_or_insert_with(|| GATEWAY_GROUP.to_string());
        }
    }
    listener
}

/// The ListenerSet status to store, like [`merge_gateway_status`].
pub fn merge_listener_set_status(
    stored: Option<&ListenerSetStatus>,
    desired: &ListenerSetStatus,
    now: &Time,
) -> ListenerSetStatus {
    let stored_conditions = stored.and_then(|s| s.conditions.as_deref()).unwrap_or(&[]);
    let stored_listeners = stored.and_then(|s| s.listeners.as_deref()).unwrap_or(&[]);
    let listeners = desired.listeners.as_ref().map(|list| {
        list.iter()
            .map(|want| {
                let old = stored_listeners
                    .iter()
                    .find(|l| l.name == want.name)
                    .map_or(&[][..], |l| l.conditions.as_slice());
                normalize_set_listener(ListenerSetStatusListeners {
                    conditions: merge_keeping_foreign(
                        old,
                        &want.conditions,
                        LISTENER_CONDITIONS,
                        now,
                    ),
                    ..want.clone()
                })
            })
            .collect()
    });
    ListenerSetStatus {
        conditions: desired
            .conditions
            .as_deref()
            .map(|d| merge_keeping_foreign(stored_conditions, d, LISTENER_SET_CONDITIONS, now)),
        listeners: non_empty(listeners),
    }
}

fn canonical_listener_set(status: Option<&ListenerSetStatus>) -> ListenerSetStatus {
    let status = status.cloned().unwrap_or_default();
    let listeners = non_empty(status.listeners).map(|list| {
        let mut list: Vec<ListenerSetStatusListeners> = list
            .into_iter()
            .map(|l| {
                let mut l = normalize_set_listener(l);
                l.conditions = sorted_conditions(&l.conditions);
                l.supported_kinds = non_empty(l.supported_kinds);
                l
            })
            .collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        list
    });
    ListenerSetStatus {
        conditions: non_empty(status.conditions).map(|c| sorted_conditions(&c)),
        listeners,
    }
}

/// Whether two ListenerSet statuses are the same (conditions and listeners compared as
/// sets, API server defaults filled in).
pub fn listener_set_status_equal(
    a: Option<&ListenerSetStatus>,
    b: Option<&ListenerSetStatus>,
) -> bool {
    canonical_listener_set(a) == canonical_listener_set(b)
}

/// The `status` object of the merge patch: conditions and listeners are always written
/// (null clears).
pub fn listener_set_patch_body(status: &ListenerSetStatus) -> Value {
    let mut body =
        serde_json::to_value(status).unwrap_or_else(|_| Value::Object(Default::default()));
    set_null_if_absent(&mut body, &["conditions", "listeners"]);
    body
}

// HTTPRoute

/// Identity of a route parent entry, API server defaults filled in.
type ParentKey = (
    String,
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    Option<i32>,
);

fn parent_key(p: &HttpRouteStatusParents) -> ParentKey {
    let r = &p.parent_ref;
    (
        p.controller_name.clone(),
        r.group.clone().unwrap_or_else(|| GATEWAY_GROUP.to_string()),
        r.kind
            .clone()
            .unwrap_or_else(|| DEFAULT_PARENT_KIND.to_string()),
        r.namespace.clone(),
        r.name.clone(),
        r.section_name.clone(),
        r.port,
    )
}

fn normalize_parent(mut p: HttpRouteStatusParents) -> HttpRouteStatusParents {
    p.parent_ref
        .group
        .get_or_insert_with(|| GATEWAY_GROUP.to_string());
    p.parent_ref
        .kind
        .get_or_insert_with(|| DEFAULT_PARENT_KIND.to_string());
    p
}

/// The route status to store: the `stored` parent entries of other controllers, untouched
/// and in their order, followed by the `desired` entries of `controller` with transition
/// times merged. Stored entries of `controller` that are not desired are dropped.
pub fn splice_route_parents(
    stored: Option<&HttpRouteStatus>,
    desired: &HttpRouteStatus,
    controller: &str,
    now: &Time,
) -> HttpRouteStatus {
    let stored_parents = stored.map_or(&[][..], |s| s.parents.as_slice());
    let mut parents: Vec<HttpRouteStatusParents> = stored_parents
        .iter()
        .filter(|p| p.controller_name != controller)
        .cloned()
        .collect();
    for want in desired
        .parents
        .iter()
        .filter(|p| p.controller_name == controller)
    {
        let key = parent_key(want);
        let old = stored_parents
            .iter()
            .find(|p| parent_key(p) == key)
            .map_or(&[][..], |p| p.conditions.as_slice());
        parents.push(normalize_parent(HttpRouteStatusParents {
            conditions: merge_conditions(old, &want.conditions, now),
            ..want.clone()
        }));
    }
    HttpRouteStatus { parents }
}

fn canonical_route(status: Option<&HttpRouteStatus>) -> Vec<(ParentKey, Vec<Condition>)> {
    let mut list: Vec<(ParentKey, Vec<Condition>)> = status
        .map_or(&[][..], |s| s.parents.as_slice())
        .iter()
        .map(|p| (parent_key(p), sorted_conditions(&p.conditions)))
        .collect();
    list.sort_by(|a, b| a.0.cmp(&b.0));
    list
}

/// Whether two route statuses are the same (parents compared as a set by controller and
/// `parentRef`, API server defaults filled in).
pub fn route_status_equal(a: Option<&HttpRouteStatus>, b: Option<&HttpRouteStatus>) -> bool {
    canonical_route(a) == canonical_route(b)
}

/// The `status` object of the merge patch: the whole `parents` list.
pub fn route_patch_body(status: &HttpRouteStatus) -> Value {
    serde_json::json!({ "parents": status.parents })
}

fn set_null_if_absent(body: &mut Value, keys: &[&str]) {
    if let Value::Object(map) = body {
        for key in keys {
            map.entry(key.to_string()).or_insert(Value::Null);
        }
    }
}

/// The full merge patch: `status` plus the `resourceVersion` it was based on, so a
/// concurrent change makes it fail with 409 instead of being overwritten.
pub fn status_patch(resource_version: Option<&str>, status: Value) -> Value {
    match resource_version {
        Some(rv) => serde_json::json!({
            "metadata": { "resourceVersion": rv },
            "status": status,
        }),
        None => serde_json::json!({ "status": status }),
    }
}

fn is_status(err: &kube::Error, code: u16) -> bool {
    matches!(err, kube::Error::Api(s) if s.code == code)
}

/// Read-modify-write of one status subresource. `plan` returns the `status` patch body for
/// an object, or `None` when its status is already right. On 409 the object is read again
/// and planned anew. Returns whether it wrote; an object that is gone is not an error.
async fn write_status<K, F>(
    api: Api<K>,
    obj: &K,
    what: &str,
    rbac: &str,
    mut plan: F,
) -> anyhow::Result<bool>
where
    K: Resource + Clone + DeserializeOwned + Serialize + Debug,
    F: FnMut(&K) -> Option<Value>,
{
    let name = obj
        .meta()
        .name
        .clone()
        .with_context(|| format!("{what} without a name"))?;
    let params = PatchParams {
        field_manager: Some(FIELD_MANAGER.to_string()),
        ..PatchParams::default()
    };
    let mut fresh: Option<K> = None;
    for attempt in 1..=MAX_ATTEMPTS {
        let base = fresh.as_ref().unwrap_or(obj);
        let Some(body) = plan(base) else {
            return Ok(false);
        };
        let patch = status_patch(base.meta().resource_version.as_deref(), body);
        match api
            .patch_status(&name, &params, &Patch::Merge(&patch))
            .await
        {
            Ok(_) => {
                debug!(object = %what, "status written");
                return Ok(true);
            }
            Err(err) if is_status(&err, 404) => {
                debug!(object = %what, "gone before its status was written");
                return Ok(false);
            }
            Err(err) if is_status(&err, 409) && attempt < MAX_ATTEMPTS => {
                debug!(object = %what, attempt, "status conflict, reading it again");
                match api.get(&name).await {
                    Ok(obj) => fresh = Some(obj),
                    Err(err) if is_status(&err, 404) => return Ok(false),
                    Err(err) => {
                        return Err(err).with_context(|| format!("reading {what} again"));
                    }
                }
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("patching status of {what} (needs {rbac})"));
            }
        }
    }
    unreachable!("the last attempt returns")
}

/// Writes Gateway API status through the Kubernetes API.
#[derive(Clone)]
pub struct KubeStatus {
    client: Client,
    controller_name: String,
}

impl KubeStatus {
    /// `controller_name`: the `controllerName` of our route parent entries.
    pub fn new(client: Client, controller_name: String) -> Self {
        KubeStatus {
            client,
            controller_name,
        }
    }
}

impl super::model::StatusPublisher for KubeStatus {
    async fn publish_class(
        &self,
        class: &GatewayClass,
        status: &GatewayClassStatus,
    ) -> anyhow::Result<bool> {
        let what = format!("GatewayClass {}", class.name_any());
        let now = now();
        write_status(
            Api::all(self.client.clone()),
            class,
            &what,
            "patch on gatewayclasses/status",
            |obj: &GatewayClass| {
                let merged = merge_class_status(obj.status.as_ref(), status, &now);
                (!class_status_equal(obj.status.as_ref(), Some(&merged)))
                    .then(|| class_patch_body(&merged))
            },
        )
        .await
    }

    async fn publish_gateway(
        &self,
        gateway: &Gateway,
        status: &GatewayStatus,
    ) -> anyhow::Result<bool> {
        let namespace = gateway
            .metadata
            .namespace
            .as_deref()
            .context("Gateway without a namespace")?;
        let what = format!("Gateway {namespace}/{}", gateway.name_any());
        let rbac = format!("patch on gateways/status in namespace {namespace}");
        let now = now();
        write_status(
            Api::namespaced(self.client.clone(), namespace),
            gateway,
            &what,
            &rbac,
            |obj: &Gateway| {
                let merged = merge_gateway_status(obj.status.as_ref(), status, &now);
                (!gateway_status_equal(obj.status.as_ref(), Some(&merged)))
                    .then(|| gateway_patch_body(&merged))
            },
        )
        .await
    }

    async fn publish_listener_set(
        &self,
        listener_set: &ListenerSet,
        status: &ListenerSetStatus,
    ) -> anyhow::Result<bool> {
        let namespace = listener_set
            .metadata
            .namespace
            .as_deref()
            .context("ListenerSet without a namespace")?;
        let what = format!("ListenerSet {namespace}/{}", listener_set.name_any());
        let rbac = format!("patch on listenersets/status in namespace {namespace}");
        let now = now();
        write_status(
            Api::namespaced(self.client.clone(), namespace),
            listener_set,
            &what,
            &rbac,
            |obj: &ListenerSet| {
                let merged = merge_listener_set_status(obj.status.as_ref(), status, &now);
                (!listener_set_status_equal(obj.status.as_ref(), Some(&merged)))
                    .then(|| listener_set_patch_body(&merged))
            },
        )
        .await
    }

    async fn publish_route(
        &self,
        route: &HttpRoute,
        status: &HttpRouteStatus,
    ) -> anyhow::Result<bool> {
        let namespace = route
            .metadata
            .namespace
            .as_deref()
            .context("HTTPRoute without a namespace")?;
        let what = format!("HTTPRoute {namespace}/{}", route.name_any());
        let rbac = format!("patch on httproutes/status in namespace {namespace}");
        let now = now();
        let controller = self.controller_name.as_str();
        write_status(
            Api::namespaced(self.client.clone(), namespace),
            route,
            &what,
            &rbac,
            |obj: &HttpRoute| {
                let merged = splice_route_parents(obj.status.as_ref(), status, controller, &now);
                (!route_status_equal(obj.status.as_ref(), Some(&merged)))
                    .then(|| route_patch_body(&merged))
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::api::gatewayclasses::GatewayClassStatusSupportedFeatures;
    use crate::gateway::api::gateways::GatewayStatusListenersSupportedKinds;
    use crate::gateway::api::httproutes::HttpRouteStatusParentsParentRef;
    use crate::gateway::model::StatusPublisher;

    const OURS: &str = "pingap.io/gateway-controller";

    fn at(secs: i64) -> Time {
        Time(Timestamp::from_second(secs).unwrap())
    }

    fn cond(type_: &str, status: &str, time: i64) -> Condition {
        Condition {
            type_: type_.into(),
            status: status.into(),
            reason: type_.into(),
            message: String::new(),
            observed_generation: Some(1),
            last_transition_time: at(time),
        }
    }

    fn listener(name: &str, conditions: Vec<Condition>) -> GatewayStatusListeners {
        GatewayStatusListeners {
            attached_routes: 1,
            conditions,
            name: name.into(),
            supported_kinds: Some(vec![GatewayStatusListenersSupportedKinds {
                group: None,
                kind: "HTTPRoute".into(),
            }]),
        }
    }

    fn parent(
        controller: &str,
        gateway: &str,
        conditions: Vec<Condition>,
    ) -> HttpRouteStatusParents {
        HttpRouteStatusParents {
            conditions,
            controller_name: controller.into(),
            parent_ref: HttpRouteStatusParentsParentRef {
                name: gateway.into(),
                ..Default::default()
            },
        }
    }

    #[test]
    fn merge_keeps_time_of_unchanged_status() {
        let stored = vec![
            cond("Accepted", "True", 100),
            cond("Programmed", "False", 100),
        ];
        let desired = vec![cond("Accepted", "True", 0), cond("Programmed", "True", 0)];
        let merged = merge_conditions(&stored, &desired, &at(500));
        assert_eq!(merged[0].last_transition_time, at(100));
        assert_eq!(merged[1].last_transition_time, at(500));
        assert_eq!(merged[1].status, "True");
        // A new condition takes now.
        let merged = merge_conditions(&[], &desired, &at(500));
        assert!(merged.iter().all(|c| c.last_transition_time == at(500)));
    }

    #[test]
    fn merge_keeps_time_when_only_reason_changes() {
        let stored = vec![cond("Accepted", "True", 100)];
        let mut want = cond("Accepted", "True", 0);
        want.reason = "Other".into();
        want.observed_generation = Some(2);
        let merged = merge_conditions(&stored, &[want], &at(500));
        assert_eq!(merged[0].last_transition_time, at(100));
        assert_eq!(merged[0].reason, "Other");
        assert_eq!(merged[0].observed_generation, Some(2));
    }

    #[test]
    fn conditions_compare_as_a_set() {
        let a = vec![cond("A", "True", 1), cond("B", "True", 1)];
        let b = vec![cond("B", "True", 1), cond("A", "True", 1)];
        assert!(conditions_equal(&a, &b));
        assert!(!conditions_equal(&a, &[cond("A", "True", 1)]));
        assert!(!conditions_equal(
            &a,
            &[cond("A", "True", 1), cond("B", "False", 1)]
        ));
    }

    #[test]
    fn class_unchanged_needs_no_write() {
        let stored = GatewayClassStatus {
            conditions: Some(vec![cond("Accepted", "True", 100)]),
            supported_features: Some(vec![GatewayClassStatusSupportedFeatures {
                name: "Gateway".into(),
            }]),
        };
        let desired = GatewayClassStatus {
            conditions: Some(vec![cond("Accepted", "True", 0)]),
            supported_features: None,
        };
        let merged = merge_class_status(Some(&stored), &desired, &at(500));
        assert!(class_status_equal(Some(&stored), Some(&merged)));
        // Features kept, not dropped.
        assert_eq!(merged.supported_features, stored.supported_features);

        // The API server default (Accepted Unknown) is replaced.
        let defaulted = GatewayClassStatus {
            conditions: Some(vec![cond("Accepted", "Unknown", 0)]),
            supported_features: None,
        };
        let merged = merge_class_status(Some(&defaulted), &desired, &at(500));
        assert!(!class_status_equal(Some(&defaulted), Some(&merged)));
        assert_eq!(
            merged.conditions.as_ref().unwrap()[0].last_transition_time,
            at(500)
        );
        let body = class_patch_body(&merged);
        assert_eq!(body["conditions"][0]["status"], "True");
        assert!(body.get("supportedFeatures").is_none());
    }

    #[test]
    fn gateway_merge_and_compare() {
        let stored = GatewayStatus {
            addresses: Some(vec![GatewayStatusAddresses {
                r#type: Some("IPAddress".into()),
                value: "203.0.113.10".into(),
            }]),
            attached_listener_sets: None,
            conditions: Some(vec![
                cond("Programmed", "True", 100),
                cond("Accepted", "True", 100),
            ]),
            listeners: Some(vec![
                normalize_listener(listener("https", vec![cond("Programmed", "True", 100)])),
                normalize_listener(listener("http", vec![cond("Programmed", "True", 100)])),
            ]),
        };
        let desired = GatewayStatus {
            addresses: Some(vec![GatewayStatusAddresses {
                r#type: None,
                value: "203.0.113.10".into(),
            }]),
            attached_listener_sets: None,
            conditions: Some(vec![
                cond("Accepted", "True", 0),
                cond("Programmed", "True", 0),
            ]),
            listeners: Some(vec![
                listener("http", vec![cond("Programmed", "True", 0)]),
                listener("https", vec![cond("Programmed", "True", 0)]),
            ]),
        };
        let merged = merge_gateway_status(Some(&stored), &desired, &at(500));
        assert!(gateway_status_equal(Some(&stored), Some(&merged)));
        let kinds = merged.listeners.as_ref().unwrap()[0]
            .supported_kinds
            .as_ref()
            .unwrap();
        assert_eq!(kinds[0].group.as_deref(), Some(GATEWAY_GROUP));

        // A listener whose condition flips takes now; the other keeps its time.
        let mut flipped = desired.clone();
        flipped.listeners.as_mut().unwrap()[1].conditions = vec![cond("Programmed", "False", 0)];
        let merged = merge_gateway_status(Some(&stored), &flipped, &at(500));
        assert!(!gateway_status_equal(Some(&stored), Some(&merged)));
        let listeners = merged.listeners.as_ref().unwrap();
        assert_eq!(listeners[0].conditions[0].last_transition_time, at(100));
        assert_eq!(listeners[1].conditions[0].last_transition_time, at(500));

        // No addresses wanted: written as null.
        let mut none = desired.clone();
        none.addresses = Some(vec![]);
        let merged = merge_gateway_status(Some(&stored), &none, &at(500));
        assert!(!gateway_status_equal(Some(&stored), Some(&merged)));
        assert_eq!(gateway_patch_body(&merged)["addresses"], Value::Null);
    }

    #[test]
    fn route_parents_of_other_controllers_are_kept() {
        let stored = HttpRouteStatus {
            parents: vec![
                parent("other.io/ctl", "gw", vec![cond("Accepted", "False", 50)]),
                parent(OURS, "gw", vec![cond("Accepted", "True", 100)]),
                parent(OURS, "old", vec![cond("Accepted", "True", 100)]),
            ],
        };
        let desired = HttpRouteStatus {
            parents: vec![
                parent(OURS, "gw", vec![cond("Accepted", "True", 0)]),
                parent(OURS, "new", vec![cond("Accepted", "True", 0)]),
            ],
        };
        let merged = splice_route_parents(Some(&stored), &desired, OURS, &at(500));
        assert_eq!(merged.parents.len(), 3);
        assert_eq!(merged.parents[0], stored.parents[0], "untouched");
        assert_eq!(merged.parents[1].parent_ref.name, "gw");
        assert_eq!(
            merged.parents[1].conditions[0].last_transition_time,
            at(100)
        );
        assert_eq!(
            merged.parents[1].parent_ref.kind.as_deref(),
            Some("Gateway")
        );
        assert_eq!(merged.parents[2].parent_ref.name, "new");
        assert_eq!(
            merged.parents[2].conditions[0].last_transition_time,
            at(500)
        );
        assert!(!route_status_equal(Some(&stored), Some(&merged)));
        let body = route_patch_body(&merged);
        assert_eq!(body["parents"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn route_unchanged_needs_no_write() {
        // Stored as the API server keeps it: defaults filled in, other order.
        let mut ours = parent(OURS, "gw", vec![cond("Accepted", "True", 100)]);
        ours = normalize_parent(ours);
        let stored = HttpRouteStatus {
            parents: vec![ours, parent("other.io/ctl", "gw", vec![])],
        };
        let desired = HttpRouteStatus {
            parents: vec![parent(OURS, "gw", vec![cond("Accepted", "True", 0)])],
        };
        let merged = splice_route_parents(Some(&stored), &desired, OURS, &at(500));
        assert!(route_status_equal(Some(&stored), Some(&merged)));

        // Nothing of ours wanted any more: our entry goes, the other stays.
        let merged =
            splice_route_parents(Some(&stored), &HttpRouteStatus::default(), OURS, &at(500));
        assert_eq!(merged.parents.len(), 1);
        assert_eq!(merged.parents[0].controller_name, "other.io/ctl");
        assert!(!route_status_equal(Some(&stored), Some(&merged)));

        // A route without status and nothing wanted: no write.
        let merged = splice_route_parents(None, &HttpRouteStatus::default(), OURS, &at(500));
        assert!(route_status_equal(None, Some(&merged)));
    }

    #[test]
    fn patch_carries_resource_version() {
        let patch = status_patch(Some("42"), serde_json::json!({"parents": []}));
        assert_eq!(
            patch,
            serde_json::json!({"metadata": {"resourceVersion": "42"}, "status": {"parents": []}})
        );
        let patch = status_patch(None, serde_json::json!({}));
        assert!(patch.get("metadata").is_none());
    }

    #[tokio::test]
    async fn publish_skips_unchanged_without_calling_the_api() {
        // Port 1 on loopback: any API call would fail.
        let config = kube::Config::new("http://127.0.0.1:1".parse().unwrap());
        let publisher = KubeStatus::new(Client::try_from(config).unwrap(), OURS.into());
        let mut route = HttpRoute::new("web", Default::default());
        route.metadata.namespace = Some("prod".into());
        route.status = Some(HttpRouteStatus {
            parents: vec![parent(OURS, "gw", vec![cond("Accepted", "True", 100)])],
        });
        let same = HttpRouteStatus {
            parents: vec![parent(OURS, "gw", vec![cond("Accepted", "True", 0)])],
        };
        assert!(!publisher.publish_route(&route, &same).await.unwrap());
        // A change reaches the API (and fails here).
        let other = HttpRouteStatus {
            parents: vec![parent(OURS, "gw", vec![cond("Accepted", "False", 0)])],
        };
        let err = publisher.publish_route(&route, &other).await.unwrap_err();
        assert!(format!("{err:#}").contains("httproutes/status"), "{err:#}");
    }

    #[test]
    fn foreign_conditions_are_kept() {
        let mut foreign = cond("SomeoneElse", "True", 42);
        foreign.observed_generation = Some(3);
        foreign.reason = "Seeded".into();
        let stored = GatewayStatus {
            conditions: Some(vec![
                cond("Accepted", "True", 100),
                cond("Programmed", "True", 100),
                foreign.clone(),
            ]),
            listeners: Some(vec![normalize_listener(listener(
                "http",
                vec![cond("Accepted", "True", 100), foreign.clone()],
            ))]),
            ..Default::default()
        };
        let mut desired = GatewayStatus {
            conditions: Some(vec![
                cond("Accepted", "True", 0),
                cond("Programmed", "False", 0),
            ]),
            listeners: Some(vec![listener("http", vec![cond("Accepted", "True", 0)])]),
            ..Default::default()
        };
        for c in desired.conditions.as_mut().unwrap() {
            c.observed_generation = Some(4);
        }
        let merged = merge_gateway_status(Some(&stored), &desired, &at(500));
        let conditions = merged.conditions.as_ref().unwrap();
        assert_eq!(conditions.len(), 3);
        assert!(conditions.contains(&foreign));
        assert!(
            merged.listeners.as_ref().unwrap()[0]
                .conditions
                .contains(&foreign)
        );
        assert!(!gateway_status_equal(Some(&stored), Some(&merged)));
        // Written back whole: the merge patch replaces the list.
        let body = gateway_patch_body(&merged);
        assert_eq!(body["conditions"].as_array().unwrap().len(), 3);

        // A type this controller sets but no longer wants is dropped, not kept.
        let stored = GatewayStatus {
            listeners: Some(vec![normalize_listener(listener(
                "http",
                vec![cond("Conflicted", "True", 100)],
            ))]),
            ..Default::default()
        };
        let merged = merge_gateway_status(Some(&stored), &desired, &at(500));
        assert_eq!(merged.listeners.as_ref().unwrap()[0].conditions.len(), 1);

        // GatewayClass too.
        let stored = GatewayClassStatus {
            conditions: Some(vec![cond("Accepted", "True", 100), foreign.clone()]),
            supported_features: None,
        };
        let want = GatewayClassStatus {
            conditions: Some(vec![cond("Accepted", "True", 0)]),
            supported_features: None,
        };
        let merged = merge_class_status(Some(&stored), &want, &at(500));
        assert!(merged.conditions.unwrap().contains(&foreign));
    }

    #[test]
    fn listener_set_merge_and_compare() {
        let set_listener = |conditions: Vec<Condition>| ListenerSetStatusListeners {
            attached_routes: 2,
            conditions,
            name: "l".into(),
            supported_kinds: Some(vec![
                crate::gateway::api::listenersets::ListenerSetStatusListenersSupportedKinds {
                    group: None,
                    kind: "HTTPRoute".into(),
                },
            ]),
        };
        let foreign = cond("SomeoneElse", "True", 42);
        let stored = ListenerSetStatus {
            conditions: Some(vec![
                cond("Programmed", "True", 100),
                cond("Accepted", "True", 100),
                foreign.clone(),
            ]),
            listeners: Some(vec![normalize_set_listener(set_listener(vec![cond(
                "Accepted", "True", 100,
            )]))]),
        };
        let desired = ListenerSetStatus {
            conditions: Some(vec![
                cond("Accepted", "True", 0),
                cond("Programmed", "True", 0),
            ]),
            listeners: Some(vec![set_listener(vec![cond("Accepted", "True", 0)])]),
        };
        let merged = merge_listener_set_status(Some(&stored), &desired, &at(500));
        assert!(listener_set_status_equal(Some(&stored), Some(&merged)));
        assert!(merged.conditions.as_ref().unwrap().contains(&foreign));
        let kinds = merged.listeners.as_ref().unwrap()[0]
            .supported_kinds
            .as_ref()
            .unwrap();
        assert_eq!(kinds[0].group.as_deref(), Some(GATEWAY_GROUP));

        // Not allowed: the listeners are cleared.
        let rejected = ListenerSetStatus {
            conditions: Some(vec![
                cond("Accepted", "False", 0),
                cond("Programmed", "False", 0),
            ]),
            listeners: None,
        };
        let merged = merge_listener_set_status(Some(&stored), &rejected, &at(500));
        assert!(!listener_set_status_equal(Some(&stored), Some(&merged)));
        assert_eq!(listener_set_patch_body(&merged)["listeners"], Value::Null);
        assert!(
            merged
                .conditions
                .as_ref()
                .unwrap()
                .iter()
                .all(|c| c.type_ == "SomeoneElse" || c.last_transition_time == at(500))
        );
    }
}

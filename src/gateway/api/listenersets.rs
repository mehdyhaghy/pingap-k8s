// Generated from the Gateway API v1.7.0 Standard-channel CRD listenersets.gateway.networking.k8s.io
// by scripts/gen-gateway-api.sh. Do not edit.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "ListenerSet",
    plural = "listenersets"
)]
#[kube(namespaced)]
#[kube(status = "ListenerSetStatus")]
#[kube(schema = "disabled")]
#[kube(derive = "Default")]
#[kube(derive = "PartialEq")]
pub struct ListenerSetSpec {
    pub listeners: Vec<ListenerSetListeners>,
    #[serde(rename = "parentRef")]
    pub parent_ref: ListenerSetParentRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetListeners {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allowedRoutes"
    )]
    pub allowed_routes: Option<ListenerSetListenersAllowedRoutes>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    pub name: String,
    pub port: i32,
    pub protocol: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<ListenerSetListenersTls>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetListenersAllowedRoutes {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<ListenerSetListenersAllowedRoutesKinds>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<ListenerSetListenersAllowedRoutesNamespaces>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetListenersAllowedRoutesKinds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub kind: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetListenersAllowedRoutesNamespaces {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<ListenerSetListenersAllowedRoutesNamespacesFrom>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<ListenerSetListenersAllowedRoutesNamespacesSelector>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ListenerSetListenersAllowedRoutesNamespacesFrom {
    All,
    Selector,
    Same,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetListenersAllowedRoutesNamespacesSelector {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "matchExpressions"
    )]
    pub match_expressions:
        Option<Vec<ListenerSetListenersAllowedRoutesNamespacesSelectorMatchExpressions>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "matchLabels"
    )]
    pub match_labels: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetListenersAllowedRoutesNamespacesSelectorMatchExpressions {
    pub key: String,
    pub operator: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetListenersTls {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "certificateRefs"
    )]
    pub certificate_refs: Option<Vec<ListenerSetListenersTlsCertificateRefs>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ListenerSetListenersTlsMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetListenersTlsCertificateRefs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum ListenerSetListenersTlsMode {
    Terminate,
    Passthrough,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetParentRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listeners: Option<Vec<ListenerSetStatusListeners>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetStatusListeners {
    #[serde(rename = "attachedRoutes")]
    pub attached_routes: i32,
    pub conditions: Vec<Condition>,
    pub name: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "supportedKinds"
    )]
    pub supported_kinds: Option<Vec<ListenerSetStatusListenersSupportedKinds>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListenerSetStatusListenersSupportedKinds {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    pub kind: String,
}

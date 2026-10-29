// Generated from the Gateway API v1.6.3 Standard-channel CRD referencegrants.gateway.networking.k8s.io
// by scripts/gen-gateway-api.sh. Do not edit.

use kube::CustomResource;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "ReferenceGrant",
    plural = "referencegrants"
)]
#[kube(namespaced)]
#[kube(schema = "disabled")]
#[kube(derive = "Default")]
#[kube(derive = "PartialEq")]
pub struct ReferenceGrantSpec {
    pub from: Vec<ReferenceGrantFrom>,
    pub to: Vec<ReferenceGrantTo>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ReferenceGrantFrom {
    pub group: String,
    pub kind: String,
    pub namespace: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ReferenceGrantTo {
    pub group: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

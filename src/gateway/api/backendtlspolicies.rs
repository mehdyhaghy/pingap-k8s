// Generated from the Gateway API v1.7.0 Standard-channel CRD backendtlspolicies.gateway.networking.k8s.io
// by scripts/gen-gateway-api.sh. Do not edit.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "BackendTLSPolicy",
    root = "BackendTlsPolicy",
    plural = "backendtlspolicies"
)]
#[kube(namespaced)]
#[kube(status = "BackendTlsPolicyStatus")]
#[kube(schema = "disabled")]
#[kube(derive = "Default")]
#[kube(derive = "PartialEq")]
pub struct BackendTlsPolicySpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<BTreeMap<String, String>>,
    #[serde(rename = "targetRefs")]
    pub target_refs: Vec<BackendTlsPolicyTargetRefs>,
    pub validation: BackendTlsPolicyValidation,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct BackendTlsPolicyTargetRefs {
    pub group: String,
    pub kind: String,
    pub name: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sectionName"
    )]
    pub section_name: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct BackendTlsPolicyValidation {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "caCertificateRefs"
    )]
    pub ca_certificate_refs: Option<Vec<BackendTlsPolicyValidationCaCertificateRefs>>,
    pub hostname: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "subjectAltNames"
    )]
    pub subject_alt_names: Option<Vec<BackendTlsPolicyValidationSubjectAltNames>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "wellKnownCACertificates"
    )]
    pub well_known_ca_certificates: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct BackendTlsPolicyValidationCaCertificateRefs {
    pub group: String,
    pub kind: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct BackendTlsPolicyValidationSubjectAltNames {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(rename = "type")]
    pub r#type: BackendTlsPolicyValidationSubjectAltNamesType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum BackendTlsPolicyValidationSubjectAltNamesType {
    Hostname,
    #[serde(rename = "URI")]
    Uri,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct BackendTlsPolicyStatus {
    pub ancestors: Vec<BackendTlsPolicyStatusAncestors>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct BackendTlsPolicyStatusAncestors {
    #[serde(rename = "ancestorRef")]
    pub ancestor_ref: BackendTlsPolicyStatusAncestorsAncestorRef,
    pub conditions: Vec<Condition>,
    #[serde(rename = "controllerName")]
    pub controller_name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct BackendTlsPolicyStatusAncestorsAncestorRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "sectionName"
    )]
    pub section_name: Option<String>,
}

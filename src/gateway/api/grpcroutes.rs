// Generated from the Gateway API v1.7.0 Standard-channel CRD grpcroutes.gateway.networking.k8s.io
// by scripts/gen-gateway-api.sh. Do not edit.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "GRPCRoute",
    root = "GrpcRoute",
    plural = "grpcroutes"
)]
#[kube(namespaced)]
#[kube(status = "GrpcRouteStatus")]
#[kube(schema = "disabled")]
#[kube(derive = "Default")]
#[kube(derive = "PartialEq")]
pub struct GrpcRouteSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostnames: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "parentRefs"
    )]
    pub parent_refs: Option<Vec<GrpcRouteParentRefs>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<Vec<GrpcRouteRules>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteParentRefs {
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

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRules {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "backendRefs"
    )]
    pub backend_refs: Option<Vec<GrpcRouteRulesBackendRefs>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<GrpcRouteRulesFilters>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matches: Option<Vec<GrpcRouteRulesMatches>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<GrpcRouteRulesBackendRefsFilters>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFilters {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "extensionRef"
    )]
    pub extension_ref: Option<GrpcRouteRulesBackendRefsFiltersExtensionRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestHeaderModifier"
    )]
    pub request_header_modifier: Option<GrpcRouteRulesBackendRefsFiltersRequestHeaderModifier>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestMirror"
    )]
    pub request_mirror: Option<GrpcRouteRulesBackendRefsFiltersRequestMirror>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "responseHeaderModifier"
    )]
    pub response_header_modifier: Option<GrpcRouteRulesBackendRefsFiltersResponseHeaderModifier>,
    #[serde(rename = "type")]
    pub r#type: GrpcRouteRulesBackendRefsFiltersType,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersExtensionRef {
    pub group: String,
    pub kind: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersRequestHeaderModifier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<GrpcRouteRulesBackendRefsFiltersRequestHeaderModifierAdd>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<Vec<GrpcRouteRulesBackendRefsFiltersRequestHeaderModifierSet>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersRequestHeaderModifierAdd {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersRequestHeaderModifierSet {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersRequestMirror {
    #[serde(rename = "backendRef")]
    pub backend_ref: GrpcRouteRulesBackendRefsFiltersRequestMirrorBackendRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fraction: Option<GrpcRouteRulesBackendRefsFiltersRequestMirrorFraction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersRequestMirrorBackendRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersRequestMirrorFraction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denominator: Option<i32>,
    pub numerator: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersResponseHeaderModifier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<GrpcRouteRulesBackendRefsFiltersResponseHeaderModifierAdd>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<Vec<GrpcRouteRulesBackendRefsFiltersResponseHeaderModifierSet>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersResponseHeaderModifierAdd {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesBackendRefsFiltersResponseHeaderModifierSet {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum GrpcRouteRulesBackendRefsFiltersType {
    ResponseHeaderModifier,
    RequestHeaderModifier,
    RequestMirror,
    ExtensionRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct GrpcRouteRulesFilters {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "extensionRef"
    )]
    pub extension_ref: Option<GrpcRouteRulesFiltersExtensionRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestHeaderModifier"
    )]
    pub request_header_modifier: Option<GrpcRouteRulesFiltersRequestHeaderModifier>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestMirror"
    )]
    pub request_mirror: Option<GrpcRouteRulesFiltersRequestMirror>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "responseHeaderModifier"
    )]
    pub response_header_modifier: Option<GrpcRouteRulesFiltersResponseHeaderModifier>,
    #[serde(rename = "type")]
    pub r#type: GrpcRouteRulesFiltersType,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersExtensionRef {
    pub group: String,
    pub kind: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersRequestHeaderModifier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<GrpcRouteRulesFiltersRequestHeaderModifierAdd>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<Vec<GrpcRouteRulesFiltersRequestHeaderModifierSet>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersRequestHeaderModifierAdd {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersRequestHeaderModifierSet {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersRequestMirror {
    #[serde(rename = "backendRef")]
    pub backend_ref: GrpcRouteRulesFiltersRequestMirrorBackendRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fraction: Option<GrpcRouteRulesFiltersRequestMirrorFraction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersRequestMirrorBackendRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersRequestMirrorFraction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denominator: Option<i32>,
    pub numerator: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersResponseHeaderModifier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<GrpcRouteRulesFiltersResponseHeaderModifierAdd>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<Vec<GrpcRouteRulesFiltersResponseHeaderModifierSet>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersResponseHeaderModifierAdd {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesFiltersResponseHeaderModifierSet {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum GrpcRouteRulesFiltersType {
    ResponseHeaderModifier,
    RequestHeaderModifier,
    RequestMirror,
    ExtensionRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesMatches {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<Vec<GrpcRouteRulesMatchesHeaders>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<GrpcRouteRulesMatchesMethod>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesMatchesHeaders {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub r#type: Option<GrpcRouteRulesMatchesHeadersType>,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum GrpcRouteRulesMatchesHeadersType {
    Exact,
    RegularExpression,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteRulesMatchesMethod {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub r#type: Option<GrpcRouteRulesMatchesMethodType>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum GrpcRouteRulesMatchesMethodType {
    Exact,
    RegularExpression,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteStatus {
    pub parents: Vec<GrpcRouteStatusParents>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteStatusParents {
    pub conditions: Vec<Condition>,
    #[serde(rename = "controllerName")]
    pub controller_name: String,
    #[serde(rename = "parentRef")]
    pub parent_ref: GrpcRouteStatusParentsParentRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct GrpcRouteStatusParentsParentRef {
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

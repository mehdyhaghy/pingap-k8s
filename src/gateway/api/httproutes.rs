// Generated from the Gateway API v1.6.3 Standard-channel CRD httproutes.gateway.networking.k8s.io
// by scripts/gen-gateway-api.sh. Do not edit.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[kube(
    group = "gateway.networking.k8s.io",
    version = "v1",
    kind = "HTTPRoute",
    root = "HttpRoute",
    plural = "httproutes"
)]
#[kube(namespaced)]
#[kube(status = "HttpRouteStatus")]
#[kube(schema = "disabled")]
#[kube(derive = "Default")]
#[kube(derive = "PartialEq")]
pub struct HttpRouteSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostnames: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "parentRefs"
    )]
    pub parent_refs: Option<Vec<HttpRouteParentRefs>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules: Option<Vec<HttpRouteRules>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteParentRefs {
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
pub struct HttpRouteRules {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "backendRefs"
    )]
    pub backend_refs: Option<Vec<HttpRouteRulesBackendRefs>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<HttpRouteRulesFilters>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matches: Option<Vec<HttpRouteRulesMatches>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeouts: Option<HttpRouteRulesTimeouts>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<HttpRouteRulesBackendRefsFilters>>,
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
pub struct HttpRouteRulesBackendRefsFilters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cors: Option<HttpRouteRulesBackendRefsFiltersCors>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "extensionRef"
    )]
    pub extension_ref: Option<HttpRouteRulesBackendRefsFiltersExtensionRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestHeaderModifier"
    )]
    pub request_header_modifier: Option<HttpRouteRulesBackendRefsFiltersRequestHeaderModifier>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestMirror"
    )]
    pub request_mirror: Option<HttpRouteRulesBackendRefsFiltersRequestMirror>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestRedirect"
    )]
    pub request_redirect: Option<HttpRouteRulesBackendRefsFiltersRequestRedirect>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "responseHeaderModifier"
    )]
    pub response_header_modifier: Option<HttpRouteRulesBackendRefsFiltersResponseHeaderModifier>,
    #[serde(rename = "type")]
    pub r#type: HttpRouteRulesBackendRefsFiltersType,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "urlRewrite"
    )]
    pub url_rewrite: Option<HttpRouteRulesBackendRefsFiltersUrlRewrite>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersCors {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allowCredentials"
    )]
    pub allow_credentials: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allowHeaders"
    )]
    pub allow_headers: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allowMethods"
    )]
    pub allow_methods: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allowOrigins"
    )]
    pub allow_origins: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "exposeHeaders"
    )]
    pub expose_headers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "maxAge")]
    pub max_age: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersExtensionRef {
    pub group: String,
    pub kind: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersRequestHeaderModifier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<HttpRouteRulesBackendRefsFiltersRequestHeaderModifierAdd>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<Vec<HttpRouteRulesBackendRefsFiltersRequestHeaderModifierSet>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersRequestHeaderModifierAdd {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersRequestHeaderModifierSet {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersRequestMirror {
    #[serde(rename = "backendRef")]
    pub backend_ref: HttpRouteRulesBackendRefsFiltersRequestMirrorBackendRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fraction: Option<HttpRouteRulesBackendRefsFiltersRequestMirrorFraction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersRequestMirrorBackendRef {
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
pub struct HttpRouteRulesBackendRefsFiltersRequestMirrorFraction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denominator: Option<i32>,
    pub numerator: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersRequestRedirect {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<HttpRouteRulesBackendRefsFiltersRequestRedirectPath>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheme: Option<HttpRouteRulesBackendRefsFiltersRequestRedirectScheme>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "statusCode"
    )]
    pub status_code: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersRequestRedirectPath {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "replaceFullPath"
    )]
    pub replace_full_path: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "replacePrefixMatch"
    )]
    pub replace_prefix_match: Option<String>,
    #[serde(rename = "type")]
    pub r#type: HttpRouteRulesBackendRefsFiltersRequestRedirectPathType,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesBackendRefsFiltersRequestRedirectPathType {
    ReplaceFullPath,
    ReplacePrefixMatch,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesBackendRefsFiltersRequestRedirectScheme {
    #[serde(rename = "http")]
    Http,
    #[serde(rename = "https")]
    Https,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesBackendRefsFiltersRequestRedirectStatusCode {
    #[serde(rename = "301")]
    r#_301,
    #[serde(rename = "302")]
    r#_302,
    #[serde(rename = "303")]
    r#_303,
    #[serde(rename = "307")]
    r#_307,
    #[serde(rename = "308")]
    r#_308,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersResponseHeaderModifier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<HttpRouteRulesBackendRefsFiltersResponseHeaderModifierAdd>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<Vec<HttpRouteRulesBackendRefsFiltersResponseHeaderModifierSet>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersResponseHeaderModifierAdd {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersResponseHeaderModifierSet {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesBackendRefsFiltersType {
    RequestHeaderModifier,
    ResponseHeaderModifier,
    RequestMirror,
    RequestRedirect,
    #[serde(rename = "URLRewrite")]
    UrlRewrite,
    ExtensionRef,
    #[serde(rename = "CORS")]
    Cors,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersUrlRewrite {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<HttpRouteRulesBackendRefsFiltersUrlRewritePath>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HttpRouteRulesBackendRefsFiltersUrlRewritePath {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "replaceFullPath"
    )]
    pub replace_full_path: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "replacePrefixMatch"
    )]
    pub replace_prefix_match: Option<String>,
    #[serde(rename = "type")]
    pub r#type: HttpRouteRulesBackendRefsFiltersUrlRewritePathType,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesBackendRefsFiltersUrlRewritePathType {
    ReplaceFullPath,
    ReplacePrefixMatch,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HttpRouteRulesFilters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cors: Option<HttpRouteRulesFiltersCors>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "extensionRef"
    )]
    pub extension_ref: Option<HttpRouteRulesFiltersExtensionRef>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestHeaderModifier"
    )]
    pub request_header_modifier: Option<HttpRouteRulesFiltersRequestHeaderModifier>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestMirror"
    )]
    pub request_mirror: Option<HttpRouteRulesFiltersRequestMirror>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "requestRedirect"
    )]
    pub request_redirect: Option<HttpRouteRulesFiltersRequestRedirect>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "responseHeaderModifier"
    )]
    pub response_header_modifier: Option<HttpRouteRulesFiltersResponseHeaderModifier>,
    #[serde(rename = "type")]
    pub r#type: HttpRouteRulesFiltersType,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "urlRewrite"
    )]
    pub url_rewrite: Option<HttpRouteRulesFiltersUrlRewrite>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersCors {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allowCredentials"
    )]
    pub allow_credentials: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allowHeaders"
    )]
    pub allow_headers: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allowMethods"
    )]
    pub allow_methods: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "allowOrigins"
    )]
    pub allow_origins: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "exposeHeaders"
    )]
    pub expose_headers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "maxAge")]
    pub max_age: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersExtensionRef {
    pub group: String,
    pub kind: String,
    pub name: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersRequestHeaderModifier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<HttpRouteRulesFiltersRequestHeaderModifierAdd>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<Vec<HttpRouteRulesFiltersRequestHeaderModifierSet>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersRequestHeaderModifierAdd {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersRequestHeaderModifierSet {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersRequestMirror {
    #[serde(rename = "backendRef")]
    pub backend_ref: HttpRouteRulesFiltersRequestMirrorBackendRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fraction: Option<HttpRouteRulesFiltersRequestMirrorFraction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percent: Option<i32>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersRequestMirrorBackendRef {
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
pub struct HttpRouteRulesFiltersRequestMirrorFraction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub denominator: Option<i32>,
    pub numerator: i32,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersRequestRedirect {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<HttpRouteRulesFiltersRequestRedirectPath>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheme: Option<HttpRouteRulesFiltersRequestRedirectScheme>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "statusCode"
    )]
    pub status_code: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HttpRouteRulesFiltersRequestRedirectPath {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "replaceFullPath"
    )]
    pub replace_full_path: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "replacePrefixMatch"
    )]
    pub replace_prefix_match: Option<String>,
    #[serde(rename = "type")]
    pub r#type: HttpRouteRulesFiltersRequestRedirectPathType,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesFiltersRequestRedirectPathType {
    ReplaceFullPath,
    ReplacePrefixMatch,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesFiltersRequestRedirectScheme {
    #[serde(rename = "http")]
    Http,
    #[serde(rename = "https")]
    Https,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesFiltersRequestRedirectStatusCode {
    #[serde(rename = "301")]
    r#_301,
    #[serde(rename = "302")]
    r#_302,
    #[serde(rename = "303")]
    r#_303,
    #[serde(rename = "307")]
    r#_307,
    #[serde(rename = "308")]
    r#_308,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersResponseHeaderModifier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<HttpRouteRulesFiltersResponseHeaderModifierAdd>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<Vec<HttpRouteRulesFiltersResponseHeaderModifierSet>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersResponseHeaderModifierAdd {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersResponseHeaderModifierSet {
    pub name: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesFiltersType {
    RequestHeaderModifier,
    ResponseHeaderModifier,
    RequestMirror,
    RequestRedirect,
    #[serde(rename = "URLRewrite")]
    UrlRewrite,
    ExtensionRef,
    #[serde(rename = "CORS")]
    Cors,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesFiltersUrlRewrite {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<HttpRouteRulesFiltersUrlRewritePath>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HttpRouteRulesFiltersUrlRewritePath {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "replaceFullPath"
    )]
    pub replace_full_path: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "replacePrefixMatch"
    )]
    pub replace_prefix_match: Option<String>,
    #[serde(rename = "type")]
    pub r#type: HttpRouteRulesFiltersUrlRewritePathType,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesFiltersUrlRewritePathType {
    ReplaceFullPath,
    ReplacePrefixMatch,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesMatches {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<Vec<HttpRouteRulesMatchesHeaders>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<HttpRouteRulesMatchesMethod>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<HttpRouteRulesMatchesPath>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "queryParams"
    )]
    pub query_params: Option<Vec<HttpRouteRulesMatchesQueryParams>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesMatchesHeaders {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub r#type: Option<HttpRouteRulesMatchesHeadersType>,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesMatchesHeadersType {
    Exact,
    RegularExpression,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesMatchesMethod {
    #[serde(rename = "GET")]
    Get,
    #[serde(rename = "HEAD")]
    Head,
    #[serde(rename = "POST")]
    Post,
    #[serde(rename = "PUT")]
    Put,
    #[serde(rename = "DELETE")]
    Delete,
    #[serde(rename = "CONNECT")]
    Connect,
    #[serde(rename = "OPTIONS")]
    Options,
    #[serde(rename = "TRACE")]
    Trace,
    #[serde(rename = "PATCH")]
    Patch,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesMatchesPath {
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub r#type: Option<HttpRouteRulesMatchesPathType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesMatchesPathType {
    Exact,
    PathPrefix,
    RegularExpression,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesMatchesQueryParams {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub r#type: Option<HttpRouteRulesMatchesQueryParamsType>,
    pub value: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum HttpRouteRulesMatchesQueryParamsType {
    Exact,
    RegularExpression,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteRulesTimeouts {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "backendRequest"
    )]
    pub backend_request: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteStatus {
    pub parents: Vec<HttpRouteStatusParents>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteStatusParents {
    pub conditions: Vec<Condition>,
    #[serde(rename = "controllerName")]
    pub controller_name: String,
    #[serde(rename = "parentRef")]
    pub parent_ref: HttpRouteStatusParentsParentRef,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct HttpRouteStatusParentsParentRef {
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

// Experimental-channel fields of the Gateway API v1.7.0 CRDs that the provider reads on
// top of the generated Standard-channel types. Written by hand from the
// experimental-install.yaml of that release; scripts/gen-gateway-api.sh adds the
// `session_persistence` field of HTTPRoute and GRPCRoute rules that refers to them.
//
// On a cluster with the Standard-channel CRDs the API server prunes these fields, so
// they are always `None` there.

use serde::{Deserialize, Serialize};

/// `rules[].sessionPersistence` of an HTTPRoute or a GRPCRoute (Extended support).
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct SessionPersistence {
    /// A duration (`^([0-9]{1,5}(h|m|s|ms)){1,4}$`) after which the session ends.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "absoluteTimeout"
    )]
    pub absolute_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cookie: Option<SessionPersistenceCookie>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<SessionPersistenceHeader>,
    /// `Cookie` (the default) or `Header`.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "type")]
    pub r#type: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct SessionPersistenceCookie {
    /// `Session` (the default) or `Permanent`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "lifetimeType"
    )]
    pub lifetime_type: Option<String>,
    /// The cookie name (an HTTP token); the implementation picks one when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The cookie `Path` attribute, `/` when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct SessionPersistenceHeader {
    pub name: String,
}

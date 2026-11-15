//! Gateway API Standard-channel types, generated from the CRDs of one release by
//! `scripts/gen-gateway-api.sh` (see the header of each file for the version), plus the
//! Experimental-channel fields in [`experimental`].

pub mod backendtlspolicies;
pub mod experimental;
pub mod gatewayclasses;
pub mod gateways;
pub mod grpcroutes;
pub mod httproutes;
pub mod listenersets;
pub mod referencegrants;

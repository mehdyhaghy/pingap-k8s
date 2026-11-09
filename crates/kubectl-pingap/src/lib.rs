//! The `/status` document of the pingap-k8s controller, as read by `kubectl pingap`.
//!
//! The controller serves it on a loopback address inside its pod (off by default,
//! chart value `status.enabled`); the plugin reaches it through `kubectl
//! port-forward`. These types are shared by both, so the plugin and the controller
//! agree on the format. The document holds no secrets: the etcd key prefix only,
//! no URLs, credentials, tokens or passwords.

use serde::{Deserialize, Serialize};

/// Version of the JSON document. Bumped on incompatible changes only; new
/// fields may be added within a version.
pub const STATUS_VERSION: u32 = 1;

/// The `/status` document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusDoc {
    pub version: u32,
    pub controller: ControllerInfo,
    /// Unix seconds when this document was built (controller clock).
    pub generated_at_unix: u64,
    /// `starting` (connecting to etcd and Kubernetes), `syncing` (waiting for
    /// the first list of every watcher) or `running`.
    pub phase: String,
    pub is_leader: bool,
    pub ready: bool,
    /// Why `/readyz` fails, when it does.
    pub not_ready_reason: Option<String>,
    pub etcd: EtcdInfo,
    pub watchers: WatchersInfo,
    pub upstreams: Vec<UpstreamStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControllerInfo {
    /// Pod name (or hostname): the leader election identity.
    pub identity: String,
    pub version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EtcdInfo {
    /// Key prefix only. Never the URL, credentials or certificate paths.
    pub prefix: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchersInfo {
    pub synced: bool,
    pub services: Vec<WatchInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchInfo {
    /// `<namespace>/<name>`.
    pub service: String,
    pub synced: bool,
    pub up: bool,
}

/// What the reconciler knows about one owned upstream.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamStatus {
    pub name: String,
    /// `pods` or `service`.
    pub target: String,
    /// The `services` entries as configured: `<service>.<namespace>:<port>[ weight]`.
    pub services: Vec<String>,
    /// Latest non-empty desired `addrs`; empty until the first non-empty result.
    pub desired: Vec<String>,
    /// What the controller believes is stored in etcd; `None` when unknown
    /// (followers, or right after a resync until the writer compared).
    pub stored: Option<Vec<String>>,
    /// Leader only: the stored `addrs` equal the desired ones and no write is
    /// in flight. Always false on followers (they never write).
    pub in_sync: bool,
    /// A debounce, write or retry is pending.
    pub pending: bool,
    pub ready: usize,
    pub draining: usize,
    /// Only draining endpoints are written.
    pub fallback: bool,
    /// The last resolve gave no endpoints; the last `addrs` are kept.
    pub no_endpoints: bool,
    /// The stored entry cannot be edited.
    pub malformed: bool,
    /// Active resolve warnings (missing Service, unknown port, ...).
    pub warnings: Vec<String>,
    pub last_write: Option<LastWrite>,
}

/// The last write attempt of this replica for one upstream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastWrite {
    pub at_unix: u64,
    /// `ok`, `unchanged`, `conflict`, `malformed` or `error`.
    pub result: String,
    pub addrs: Vec<String>,
}

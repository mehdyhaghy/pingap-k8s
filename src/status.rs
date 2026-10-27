//! Read-only `/status` endpoint.
//!
//! Off by default (`[server] status_listen = ""`). When enabled it listens on
//! a loopback address only, on its own port (never on the metrics port), and
//! is reached with `kubectl port-forward` to one controller pod: no Service,
//! no containerPort, no NetworkPolicy rule. Access is therefore gated by the
//! Kubernetes RBAC permission `pods/portforward`.
//!
//! The document is versioned JSON ([`STATUS_VERSION`]) and holds no secrets:
//! the etcd prefix only (never the URL, its credentials or TLS file paths),
//! no tokens, no admin passwords. The reconciler publishes into a
//! [`StatusBoard`] (cheap clones under a short `RwLock`); the HTTP handler
//! only reads it.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::etcd::EtcdTarget;
use crate::server::Readiness;
use crate::watch::{ServiceWatchState, WatchRegistry};

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
    /// Only draining endpoints are written .
    pub fallback: bool,
    /// The last resolve gave no endpoints; the last `addrs` are kept .
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

/// Read-only view of the watchers, for the status document.
pub trait WatchStates: Send + Sync + 'static {
    fn watch_states(&self) -> Vec<ServiceWatchState>;
}

impl WatchStates for WatchRegistry {
    fn watch_states(&self) -> Vec<ServiceWatchState> {
        WatchRegistry::watch_states(self)
    }
}

#[derive(Default)]
struct Board {
    running: bool,
    is_leader: bool,
    watches: Option<Arc<dyn WatchStates>>,
    upstreams: BTreeMap<String, UpstreamStatus>,
}

/// State the reconciler publishes for `/status`. Cheap to clone (one `Arc`).
/// Writers hold the lock only to copy a few small values.
#[derive(Clone, Default)]
pub struct StatusBoard {
    inner: Arc<RwLock<Board>>,
}

impl StatusBoard {
    pub fn new() -> Self {
        Self::default()
    }

    fn read(&self) -> RwLockReadGuard<'_, Board> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> RwLockWriteGuard<'_, Board> {
        self.inner.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// The watchers exist (phase `syncing` until the reconciler runs).
    pub fn set_watches(&self, watches: Arc<dyn WatchStates>) {
        self.write().watches = Some(watches);
    }

    /// The reconcile loop started (phase `running`).
    pub fn set_running(&self, is_leader: bool) {
        let mut board = self.write();
        board.running = true;
        board.is_leader = is_leader;
    }

    pub fn set_leader(&self, is_leader: bool) {
        self.write().is_leader = is_leader;
    }

    /// Drops upstreams that are no longer configured.
    pub fn retain_upstreams(&self, keep: impl Fn(&str) -> bool) {
        self.write().upstreams.retain(|name, _| keep(name));
    }

    /// Creates or edits the entry of `name`.
    pub fn update_upstream(&self, name: &str, f: impl FnOnce(&mut UpstreamStatus)) {
        let mut board = self.write();
        let entry = board
            .upstreams
            .entry(name.to_string())
            .or_insert_with(|| UpstreamStatus {
                name: name.to_string(),
                ..Default::default()
            });
        f(entry);
    }

    /// Records a write attempt of `name` that ended with `result`.
    pub fn record_write(&self, name: &str, result: &str, addrs: &[String]) {
        let last = LastWrite {
            at_unix: unix_now(),
            result: result.to_string(),
            addrs: addrs.to_vec(),
        };
        self.update_upstream(name, |u| u.last_write = Some(last));
    }

    #[cfg(test)]
    pub(crate) fn upstream(&self, name: &str) -> Option<UpstreamStatus> {
        self.read().upstreams.get(name).cloned()
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Everything `/status` reads. Built from the parsed etcd URL, of which only
/// the prefix is kept.
#[derive(Clone)]
pub struct StatusSources {
    identity: String,
    prefix: String,
    board: StatusBoard,
    readiness: Readiness,
}

impl StatusSources {
    pub fn new(
        identity: String,
        target: &EtcdTarget,
        board: StatusBoard,
        readiness: Readiness,
    ) -> Self {
        StatusSources {
            identity,
            prefix: target.prefix.clone(),
            board,
            readiness,
        }
    }

    /// Builds the document from the current state.
    pub fn document(&self) -> StatusDoc {
        let board = self.board.read();
        let (watch_list, phase) = match &board.watches {
            None => (Vec::new(), "starting"),
            Some(w) => (
                w.watch_states(),
                if board.running { "running" } else { "syncing" },
            ),
        };
        let watchers = WatchersInfo {
            synced: board.watches.is_some() && watch_list.iter().all(|w| w.synced),
            services: watch_list
                .into_iter()
                .map(|w| WatchInfo {
                    service: w.key.to_string(),
                    synced: w.synced,
                    up: w.up,
                })
                .collect(),
        };
        let ready = self.readiness.check();
        StatusDoc {
            version: STATUS_VERSION,
            controller: ControllerInfo {
                identity: self.identity.clone(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
            generated_at_unix: unix_now(),
            phase: phase.to_string(),
            is_leader: board.is_leader,
            ready: ready.is_ok(),
            not_ready_reason: ready.err().map(str::to_string),
            etcd: EtcdInfo {
                prefix: self.prefix.clone(),
            },
            watchers,
            upstreams: board.upstreams.values().cloned().collect(),
        }
    }
}

/// Binds the status listener. Refuses any non-loopback address, whatever the
/// config validation let through.
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    anyhow::ensure!(
        addr.ip().is_loopback(),
        "refusing to serve /status on non-loopback address {addr}"
    );
    TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding status server to {addr}"))
}

/// The router: `GET /status` only.
pub fn router(sources: StatusSources) -> Router {
    Router::new()
        .route("/status", get(status_handler))
        .with_state(sources)
}

/// Serves `/status` on a listener from [`bind`] until `shutdown` resolves.
pub async fn serve_listener(
    listener: TcpListener,
    sources: StatusSources,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    if let Ok(local) = listener.local_addr() {
        tracing::info!(addr = %local, "status endpoint listening (loopback only)");
    }
    axum::serve(
        crate::server::GuardedListener::new(listener),
        router(sources),
    )
    .with_graceful_shutdown(shutdown)
    .await
    .context("status server failed")
}

async fn status_handler(State(sources): State<StatusSources>) -> Response {
    let body = serde_json::to_string_pretty(&sources.document())
        .unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"));
    ([(header::CONTENT_TYPE, "application/json")], body).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::etcd::parse_etcd_url;
    use crate::types::ServiceKey;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct FakeWatches(Vec<ServiceWatchState>);

    impl WatchStates for FakeWatches {
        fn watch_states(&self) -> Vec<ServiceWatchState> {
            self.0.clone()
        }
    }

    const SECRET_URL: &str = "etcd://admin-user:hunter2@etcd.edge.svc:2379/pingap?user=etcd-user&password=s3cr3t-pass&ca=/etc/pingap-k8s/etcd/ca.crt&cert=/etc/pingap-k8s/etcd/tls.crt&key=/etc/pingap-k8s/etcd/tls.key&server_name=etcd.internal";

    fn sources() -> (StatusSources, StatusBoard, Readiness) {
        let target = parse_etcd_url(SECRET_URL).unwrap();
        let board = StatusBoard::new();
        let readiness = Readiness::new();
        let s = StatusSources::new(
            "pingap-k8s-abc".into(),
            &target,
            board.clone(),
            readiness.clone(),
        );
        (s, board, readiness)
    }

    fn key(ns: &str, name: &str) -> ServiceKey {
        ServiceKey {
            namespace: ns.into(),
            name: name.into(),
        }
    }

    #[test]
    fn phases_and_shape() {
        let (s, board, readiness) = sources();
        let doc = s.document();
        assert_eq!(doc.version, 1);
        assert_eq!(doc.phase, "starting");
        assert!(!doc.ready);
        assert_eq!(doc.not_ready_reason.as_deref(), Some("watchers not synced"));
        assert!(!doc.watchers.synced);

        board.set_watches(Arc::new(FakeWatches(vec![
            ServiceWatchState {
                key: key("prod", "api"),
                synced: true,
                up: true,
            },
            ServiceWatchState {
                key: key("prod", "web"),
                synced: false,
                up: false,
            },
        ])));
        assert_eq!(s.document().phase, "syncing");
        board.set_running(true);
        readiness.set_watchers_synced(true);
        board.update_upstream("api", |u| {
            u.target = "pods".into();
            u.services = vec!["api.prod:http".into()];
            u.desired = vec!["10.0.0.1:8080".into()];
            u.ready = 1;
        });
        board.record_write("api", "ok", &["10.0.0.1:8080".to_string()]);

        let doc = s.document();
        assert_eq!(doc.phase, "running");
        assert!(doc.is_leader && doc.ready);
        assert_eq!(doc.etcd.prefix, "/pingap");
        assert!(!doc.watchers.synced);
        assert_eq!(doc.watchers.services[1].service, "prod/web");
        assert_eq!(doc.upstreams.len(), 1);
        assert_eq!(doc.upstreams[0].name, "api");
        assert_eq!(doc.upstreams[0].last_write.as_ref().unwrap().result, "ok");

        board.retain_upstreams(|n| n != "api");
        assert!(s.document().upstreams.is_empty());

        // Round trip: the kubectl plugin parses the same types.
        let json = serde_json::to_string(&doc).unwrap();
        let back: StatusDoc = serde_json::from_str(&json).unwrap();
        assert_eq!(back, doc);
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        for field in [
            "version",
            "controller",
            "generated_at_unix",
            "phase",
            "is_leader",
            "ready",
            "etcd",
            "watchers",
            "upstreams",
        ] {
            assert!(v.get(field).is_some(), "missing {field}");
        }
    }

    #[test]
    fn document_holds_no_secrets() {
        let (s, board, _) = sources();
        board.set_watches(Arc::new(FakeWatches(Vec::new())));
        board.set_running(true);
        board.update_upstream("api", |u| u.target = "pods".into());
        let json = serde_json::to_string_pretty(&s.document()).unwrap();
        for secret in [
            "hunter2",
            "admin-user",
            "etcd-user",
            "s3cr3t-pass",
            "password",
            "etcd.edge.svc",
            "2379",
            "/etc/pingap-k8s",
            "ca.crt",
            "tls.crt",
            "tls.key",
            "etcd.internal",
            "token",
        ] {
            assert!(!json.contains(secret), "{secret:?} leaked: {json}");
        }
    }

    #[tokio::test]
    async fn bind_refuses_non_loopback() {
        for bad in ["0.0.0.0:0", "[::]:0", "10.1.2.3:9091"] {
            let err = bind(bad.parse().unwrap()).await.unwrap_err();
            assert!(err.to_string().contains("non-loopback"), "{bad}: {err}");
        }
        let l = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        assert!(l.local_addr().unwrap().ip().is_loopback());
    }

    #[tokio::test]
    async fn serves_status_json_only() {
        let (s, _, _) = sources();
        let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_listener(listener, s, async {
            let _ = stop_rx.await;
        }));
        let get = |path: &'static str| async move {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let req = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            stream.write_all(req.as_bytes()).await.unwrap();
            let mut out = String::new();
            stream.read_to_string(&mut out).await.unwrap();
            out
        };
        let resp = get("/status").await;
        assert!(resp.starts_with("HTTP/1.1 200"), "{resp}");
        assert!(resp.contains("application/json"), "{resp}");
        let body = resp.split_once("\r\n\r\n").unwrap().1;
        let doc: StatusDoc = serde_json::from_str(body).unwrap();
        assert_eq!(doc.controller.identity, "pingap-k8s-abc");
        for path in ["/metrics", "/readyz", "/"] {
            assert!(get(path).await.starts_with("HTTP/1.1 404"), "{path}");
        }
        stop_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
}

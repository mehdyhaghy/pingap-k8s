//! Metrics and health HTTP server.
//!
//! Serves `GET /metrics`, `/healthz` and `/readyz` on the address from the
//! config (`[server] listen`, default `0.0.0.0:9090` inside the pod). The
//! port is meant for in-cluster scraping and kubelet probes only: the chart
//! exposes it through a ClusterIP Service and a NetworkPolicy, never publicly.

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use anyhow::Context;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio::net::TcpListener;

use crate::metrics::Metrics;

const WRITE_NONE: u8 = 0;
const WRITE_OK: u8 = 1;
const WRITE_FAILED: u8 = 2;

#[derive(Debug)]
struct ReadinessInner {
    watchers_synced: AtomicBool,
    last_write: AtomicU8,
}

/// Readiness state shared between the reconciler, the watchers and the HTTP
/// server. Cheap to clone (one `Arc`).
///
/// Ready means: every watcher has listed once, and the last etcd write
/// succeeded or no write has been attempted yet (followers never write).
#[derive(Clone, Debug)]
pub struct Readiness {
    inner: Arc<ReadinessInner>,
}

impl Default for Readiness {
    fn default() -> Self {
        Self::new()
    }
}

impl Readiness {
    /// Not synced, no write attempted: not ready.
    pub fn new() -> Self {
        Readiness {
            inner: Arc::new(ReadinessInner {
                watchers_synced: AtomicBool::new(false),
                last_write: AtomicU8::new(WRITE_NONE),
            }),
        }
    }

    pub fn set_watchers_synced(&self, synced: bool) {
        self.inner.watchers_synced.store(synced, Ordering::Relaxed);
    }

    pub fn set_last_write_ok(&self, ok: bool) {
        let v = if ok { WRITE_OK } else { WRITE_FAILED };
        self.inner.last_write.store(v, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.check().is_ok()
    }

    /// `Err` carries a short reason for `/readyz`.
    pub fn check(&self) -> Result<(), &'static str> {
        if !self.inner.watchers_synced.load(Ordering::Relaxed) {
            return Err("watchers not synced");
        }
        if self.inner.last_write.load(Ordering::Relaxed) == WRITE_FAILED {
            return Err("last etcd write failed");
        }
        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    metrics: Arc<Metrics>,
    readiness: Readiness,
}

/// The router, without a listener. Useful for tests and embedding.
pub fn router(metrics: Arc<Metrics>, readiness: Readiness) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz_handler))
        .route("/readyz", get(readyz_handler))
        .with_state(AppState { metrics, readiness })
}

/// Binds `addr` and serves until `shutdown` resolves.
pub async fn serve(
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    readiness: Readiness,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding metrics server to {addr}"))?;
    serve_listener(listener, metrics, readiness, shutdown).await
}

/// Serves on an already bound listener until `shutdown` resolves.
pub async fn serve_listener(
    listener: TcpListener,
    metrics: Arc<Metrics>,
    readiness: Readiness,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    if let Ok(local) = listener.local_addr() {
        tracing::info!(addr = %local, "metrics server listening");
    }
    axum::serve(listener, router(metrics, readiness))
        .with_graceful_shutdown(shutdown)
        .await
        .context("metrics server failed")
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    (
        [(header::CONTENT_TYPE, prometheus::TEXT_FORMAT)],
        state.metrics.encode(),
    )
        .into_response()
}

async fn healthz_handler() -> &'static str {
    "ok"
}

async fn readyz_handler(State(state): State<AppState>) -> Response {
    match state.readiness.check() {
        Ok(()) => (StatusCode::OK, "ok").into_response(),
        Err(reason) => (StatusCode::SERVICE_UNAVAILABLE, reason).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[test]
    fn readiness_states() {
        let r = Readiness::new();
        assert_eq!(r.check(), Err("watchers not synced"));
        assert!(!r.is_ready());

        r.set_watchers_synced(true);
        assert!(r.is_ready(), "no write attempted yet counts as ready");

        r.set_last_write_ok(false);
        assert_eq!(r.check(), Err("last etcd write failed"));

        r.set_last_write_ok(true);
        assert!(r.is_ready());

        r.set_watchers_synced(false);
        assert!(!r.is_ready());
    }

    #[test]
    fn readiness_clones_share_state() {
        let a = Readiness::new();
        let b = a.clone();
        a.set_watchers_synced(true);
        assert!(b.is_ready());
        b.set_last_write_ok(false);
        assert!(!a.is_ready());
    }

    /// Sends one HTTP/1.1 GET and returns (status, body).
    async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        let status = text
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .expect("status line");
        let body = text
            .split_once("\r\n\r\n")
            .map(|(_, b)| b.to_string())
            .unwrap_or_default();
        (status, body)
    }

    #[tokio::test]
    async fn serves_endpoints_and_shuts_down() {
        let metrics = Arc::new(Metrics::new());
        metrics.set_leader(true);
        let readiness = Readiness::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_listener(
            listener,
            metrics.clone(),
            readiness.clone(),
            async {
                let _ = stop_rx.await;
            },
        ));

        let (status, body) = get(addr, "/healthz").await;
        assert_eq!(status, 200);
        assert_eq!(body, "ok");

        let (status, body) = get(addr, "/readyz").await;
        assert_eq!(status, 503);
        assert_eq!(body, "watchers not synced");

        readiness.set_watchers_synced(true);
        let (status, _) = get(addr, "/readyz").await;
        assert_eq!(status, 200);

        readiness.set_last_write_ok(false);
        let (status, body) = get(addr, "/readyz").await;
        assert_eq!(status, 503);
        assert_eq!(body, "last etcd write failed");

        let (status, body) = get(addr, "/metrics").await;
        assert_eq!(status, 200);
        assert!(body.contains("pingap_k8s_is_leader 1"), "{body}");

        let (status, _) = get(addr, "/nope").await;
        assert_eq!(status, 404);

        stop_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .expect("server stops after shutdown signal")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn serve_reports_bind_error() {
        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = taken.local_addr().unwrap();
        let err = serve(
            addr,
            Arc::new(Metrics::new()),
            Readiness::new(),
            std::future::pending(),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("binding metrics server"));
    }
}

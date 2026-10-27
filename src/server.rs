//! Metrics and health HTTP server.
//!
//! Serves `GET /metrics`, `/healthz` and `/readyz` on the address from the
//! config (`[server] listen`, default `[::]:9090` inside the pod, which
//! also accepts IPv4 on Linux). The
//! port is meant for in-cluster scraping and kubelet probes only: the chart
//! exposes it through a ClusterIP Service and a NetworkPolicy, never publicly.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::task::{Context as TaskContext, Poll};
use std::time::Duration;

use anyhow::Context;
use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, Sleep};

use crate::metrics::Metrics;

/// Open connections served at once; further ones are closed right away.
const MAX_CONNECTIONS: usize = 64;
/// A connection with no bytes read or written for this long is closed
/// (slow or idle clients, including half-sent request headers).
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);
/// No connection lives longer than this, keep-alive or not.
const MAX_CONNECTION_AGE: Duration = Duration::from_secs(60);

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
/// Ready means: every watcher has listed once, and no write has been
/// attempted yet (followers never write) or the last write did not fail
/// because etcd could not be reached. The reconciler reports malformed
/// entries and CAS conflicts as OK here: they only affect metrics and logs.
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

    /// `false` only when the last write failed because etcd could not be
    /// reached.
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
            return Err("etcd unreachable on last write");
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
    axum::serve(GuardedListener::new(listener), router(metrics, readiness))
        .with_graceful_shutdown(shutdown)
        .await
        .context("metrics server failed")
}

/// A TCP listener that bounds what one client can hold: at most
/// [`MAX_CONNECTIONS`] open connections, each closed after [`IDLE_TIMEOUT`]
/// without traffic or [`MAX_CONNECTION_AGE`] in total. axum sets no hyper
/// timeouts, so without this a client trickling request headers keeps its
/// connection (and a file descriptor) forever.
pub(crate) struct GuardedListener {
    inner: TcpListener,
    permits: Arc<Semaphore>,
    idle: Duration,
    max_age: Duration,
}

impl GuardedListener {
    pub(crate) fn new(inner: TcpListener) -> Self {
        Self::with_limits(inner, MAX_CONNECTIONS, IDLE_TIMEOUT, MAX_CONNECTION_AGE)
    }

    fn with_limits(inner: TcpListener, max: usize, idle: Duration, max_age: Duration) -> Self {
        GuardedListener {
            inner,
            permits: Arc::new(Semaphore::new(max)),
            idle,
            max_age,
        }
    }
}

impl axum::serve::Listener for GuardedListener {
    type Io = GuardedIo;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            let (stream, addr) = match self.inner.accept().await {
                Ok(accepted) => accepted,
                Err(err) => {
                    // Mostly EMFILE/ENFILE: back off instead of spinning.
                    tracing::debug!("metrics server accept failed: {err}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            };
            let Ok(permit) = self.permits.clone().try_acquire_owned() else {
                tracing::debug!(%addr, "connection limit reached; closing connection");
                continue;
            };
            let now = Instant::now();
            let io = GuardedIo {
                stream,
                _permit: permit,
                idle: self.idle,
                idle_timer: Box::pin(tokio::time::sleep_until(now + self.idle)),
                age_timer: Box::pin(tokio::time::sleep_until(now + self.max_age)),
            };
            return (io, addr);
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

/// A connection from [`GuardedListener`]; frees its slot when dropped.
pub(crate) struct GuardedIo {
    stream: TcpStream,
    _permit: OwnedSemaphorePermit,
    idle: Duration,
    idle_timer: Pin<Box<Sleep>>,
    age_timer: Pin<Box<Sleep>>,
}

impl GuardedIo {
    /// After an I/O call: progress restarts the idle timer, and a pending call
    /// fails once either timer has fired.
    fn check<T>(
        &mut self,
        cx: &mut TaskContext<'_>,
        poll: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        match poll {
            Poll::Ready(result) => {
                self.idle_timer.as_mut().reset(Instant::now() + self.idle);
                Poll::Ready(result)
            }
            Poll::Pending => {
                if self.age_timer.as_mut().poll(cx).is_ready()
                    || self.idle_timer.as_mut().poll(cx).is_ready()
                {
                    Poll::Ready(Err(io::ErrorKind::TimedOut.into()))
                } else {
                    Poll::Pending
                }
            }
        }
    }
}

impl AsyncRead for GuardedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let poll = Pin::new(&mut self.stream).poll_read(cx, buf);
        self.check(cx, poll)
    }
}

impl AsyncWrite for GuardedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let poll = Pin::new(&mut self.stream).poll_write(cx, buf);
        self.check(cx, poll)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
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
        assert_eq!(r.check(), Err("etcd unreachable on last write"));

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
        assert_eq!(body, "etcd unreachable on last write");

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

    /// The default listener `[::]` is dual-stack: tokio does not set
    /// `IPV6_V6ONLY`, so IPv4 clients reach it too (unless the host sets
    /// `net.ipv6.bindv6only=1`). Skipped where IPv6 is unavailable.
    #[tokio::test]
    async fn unspecified_ipv6_listener_accepts_ipv4() {
        let Ok(listener) = TcpListener::bind("[::]:0").await else {
            eprintln!("IPv6 unavailable, skipping");
            return;
        };
        let port = listener.local_addr().unwrap().port();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_listener(
            listener,
            Arc::new(Metrics::new()),
            Readiness::new(),
            async {
                let _ = stop_rx.await;
            },
        ));
        let (status, body) = get(SocketAddr::from(([127, 0, 0, 1], port)), "/healthz").await;
        assert_eq!(status, 200);
        assert_eq!(body, "ok");
        stop_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
    }

    /// Serves the router on a [`GuardedListener`] with short limits.
    async fn guarded(max: usize, idle: Duration) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let guarded = GuardedListener::with_limits(listener, max, idle, Duration::from_secs(60));
        let app = router(Arc::new(Metrics::new()), Readiness::new());
        let server = tokio::spawn(async move {
            axum::serve(guarded, app).await.unwrap();
        });
        (addr, server)
    }

    #[tokio::test]
    async fn half_sent_request_is_closed_after_idle_timeout() {
        let (addr, server) = guarded(4, Duration::from_millis(200)).await;
        let mut slow = TcpStream::connect(addr).await.unwrap();
        slow.write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n")
            .await
            .unwrap();
        let mut buf = Vec::new();
        let n = tokio::time::timeout(Duration::from_secs(5), slow.read_to_end(&mut buf))
            .await
            .expect("the server closes an idle half-sent request");
        assert!(n.is_err() || buf.is_empty() || !buf.starts_with(b"HTTP/1.1 200"));
        server.abort();
    }

    #[tokio::test]
    async fn connections_over_the_limit_are_closed_and_slots_come_back() {
        let (addr, server) = guarded(2, Duration::from_secs(30)).await;
        let held: Vec<TcpStream> =
            futures::future::join_all((0..2).map(|_| TcpStream::connect(addr)))
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect();
        // Let the server accept both before the third one arrives.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut extra = TcpStream::connect(addr).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), extra.read_to_end(&mut buf))
            .await
            .expect("the connection over the limit is closed");
        assert!(buf.is_empty());

        drop(held);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let (status, body) = get(addr, "/healthz").await;
        assert_eq!((status, body.as_str()), (200, "ok"));
        server.abort();
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

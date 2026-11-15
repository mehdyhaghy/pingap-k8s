//! An etcd connection that stops answering (a hung or partitioned member: the TCP
//! connection stays open, nothing comes back) is dropped and redialed within about the
//! HTTP/2 keep-alive interval plus timeout, so writes resume on a healthy connection.
//! With Pingap's 30 s + 10 s keep-alive, a controller stayed stuck on a paused node's
//! etcd member for up to a minute.
//!
//! A TCP proxy sits between the client and etcd; `freeze` stops forwarding on the
//! connections open at that moment (kept open, never read again) while new connections
//! pass. Skipped unless `PINGAP_K8S_TEST_ETCD` is set (see scripts/test-etcd.sh).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pingap_k8s::etcd::{EtcdWriter, parse_etcd_url};
use pingap_k8s::types::AddrsWriter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Connections with an id below this value are frozen.
struct Proxy {
    addr: std::net::SocketAddr,
    next_id: Arc<AtomicU64>,
    frozen_below: Arc<AtomicU64>,
}

impl Proxy {
    async fn start(upstream: String) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let next_id = Arc::new(AtomicU64::new(0));
        let frozen_below = Arc::new(AtomicU64::new(0));
        let (ids, frozen) = (Arc::clone(&next_id), Arc::clone(&frozen_below));
        tokio::spawn(async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else {
                    return;
                };
                let id = ids.fetch_add(1, Ordering::SeqCst);
                let Ok(outbound) = TcpStream::connect(&upstream).await else {
                    continue;
                };
                let (ri, wi) = inbound.into_split();
                let (ro, wo) = outbound.into_split();
                tokio::spawn(pipe(ri, wo, id, Arc::clone(&frozen)));
                tokio::spawn(pipe(ro, wi, id, Arc::clone(&frozen)));
            }
        });
        Proxy {
            addr,
            next_id,
            frozen_below,
        }
    }

    /// Black-holes every connection open now.
    fn freeze(&self) {
        let n = self.next_id.load(Ordering::SeqCst);
        self.frozen_below.store(n, Ordering::SeqCst);
    }
}

async fn pipe(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    id: u64,
    frozen_below: Arc<AtomicU64>,
) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        if id < frozen_below.load(Ordering::SeqCst) {
            // Keep both halves open and stop reading: the peer sees no reset and no data.
            tokio::time::sleep(Duration::from_secs(3600)).await;
            return;
        }
        let n = match tokio::time::timeout(Duration::from_millis(100), from.read(&mut buf)).await {
            Err(_) => continue,
            Ok(Ok(0)) | Ok(Err(_)) => return,
            Ok(Ok(n)) => n,
        };
        if id < frozen_below.load(Ordering::SeqCst) {
            continue;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

#[tokio::test]
async fn writes_resume_soon_after_the_connection_goes_silent() {
    let Ok(endpoint) = std::env::var("PINGAP_K8S_TEST_ETCD") else {
        eprintln!("PINGAP_K8S_TEST_ETCD not set; skipping");
        return;
    };
    let upstream = endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    let proxy = Proxy::start(upstream).await;
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // A short request timeout, so each attempt on the dead connection fails fast.
    let target = parse_etcd_url(&format!(
        "etcd://{}/pingap-k8s-test-blackhole-{nanos}?timeout=2s",
        proxy.addr
    ))
    .unwrap();
    let w = EtcdWriter::connect(&target).await.unwrap();
    w.write_addrs("api", &["10.0.0.1:80".to_string()])
        .await
        .unwrap();

    proxy.freeze();
    let start = Instant::now();
    let mut n = 1;
    loop {
        n += 1;
        let addrs = vec![format!("10.0.0.{n}:80")];
        if w.write_addrs("api", &addrs).await.is_ok() {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(25),
            "no successful write within 25 s after the etcd connection went silent"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let took = start.elapsed();
    eprintln!("write succeeded {took:?} after the freeze");
    // The frozen connection was dropped and a new one dialed.
    assert!(proxy.next_id.load(Ordering::SeqCst) >= 2);
}

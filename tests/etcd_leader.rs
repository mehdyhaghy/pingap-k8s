//! `run_election` against a real etcd. Skipped unless `PINGAP_K8S_TEST_ETCD` is set to an
//! endpoint such as `http://127.0.0.1:23790` (see scripts/test-etcd.sh).

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etcd_client::{Client, GetOptions};
use pingap_k8s::etcd::{EtcdWriter, parse_etcd_url};
use pingap_k8s::events::Events;
use pingap_k8s::leader::{run_election, run_election_with_health};
use pingap_k8s::metrics::{EtcdHealth, Metrics};
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

/// Short TTL so the tests run quickly (etcd's minimum lease TTL is about 2 s by default).
const TTL: Duration = Duration::from_secs(3);

async fn client(test: &str) -> Option<Client> {
    let Ok(endpoint) = std::env::var("PINGAP_K8S_TEST_ETCD") else {
        eprintln!("PINGAP_K8S_TEST_ETCD not set; skipping {test}");
        return None;
    };
    let host = endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    Some(Client::connect([host], None).await.unwrap())
}

fn election_name(test: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/pingap-k8s-test-{test}-{nanos}-k8s-election")
}

struct Candidate {
    identity: String,
    rx: watch::Receiver<bool>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<()>,
}

impl Candidate {
    async fn start(client: &Client, name: &str, identity: &str) -> Self {
        let (tx, rx) = watch::channel(false);
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(run_election(
            client.clone(),
            name.to_string(),
            TTL,
            identity.to_string(),
            tx,
            async move {
                let _ = stopped.await;
            },
        ));
        Self {
            identity: identity.to_string(),
            rx,
            stop: Some(stop),
            task,
        }
    }

    fn is_leader(&self) -> bool {
        *self.rx.borrow()
    }

    /// Graceful shutdown; returns how long `run_election` took to return.
    async fn shutdown(mut self) -> Duration {
        let started = Instant::now();
        let _ = self.stop.take().unwrap().send(());
        timeout(Duration::from_secs(10), self.task)
            .await
            .expect("run_election did not return after shutdown")
            .unwrap();
        assert!(!*self.rx.borrow(), "still claims leadership after shutdown");
        started.elapsed()
    }

    /// Simulates a crash: no resign, no revoke.
    fn kill(self) {
        self.task.abort();
    }
}

/// Waits until exactly one of `candidates` is leader; returns its index.
async fn wait_single_leader(candidates: &[&Candidate], within: Duration) -> usize {
    let deadline = Instant::now() + within;
    loop {
        let leaders: Vec<usize> = (0..candidates.len())
            .filter(|&i| candidates[i].is_leader())
            .collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(
            Instant::now() < deadline,
            "no single leader within {within:?} (leaders: {leaders:?})"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_value(rx: &watch::Receiver<bool>, value: bool, within: Duration) {
    let mut rx = rx.clone();
    timeout(within, rx.wait_for(|v| *v == value))
        .await
        .unwrap_or_else(|_| panic!("leadership did not become {value} within {within:?}"))
        .unwrap();
}

async fn current_leader_value(client: &Client, name: &str) -> String {
    let resp = client.election_client().leader(name).await.unwrap();
    resp.kv().unwrap().value_str().unwrap().to_string()
}

#[tokio::test]
async fn two_candidates_exactly_one_leader() {
    let Some(client) = client("two-candidates").await else {
        return;
    };
    let name = election_name("two");
    let a = Candidate::start(&client, &name, "pod-a").await;
    let b = Candidate::start(&client, &name, "pod-b").await;

    let leader = wait_single_leader(&[&a, &b], Duration::from_secs(5)).await;
    let expected = [&a, &b][leader].identity.clone();
    assert_eq!(current_leader_value(&client, &name).await, expected);

    // Stays stable over several keep-alive rounds: never two leaders, never zero.
    let until = Instant::now() + TTL * 2;
    while Instant::now() < until {
        assert!(
            a.is_leader() ^ b.is_leader(),
            "a={} b={}",
            a.is_leader(),
            b.is_leader()
        );
        sleep(Duration::from_millis(20)).await;
    }
    assert_eq!([&a, &b][leader].identity, expected);

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn leader_shutdown_hands_over_quickly() {
    let Some(client) = client("shutdown").await else {
        return;
    };
    let name = election_name("shutdown");
    let a = Candidate::start(&client, &name, "pod-a").await;
    let b = Candidate::start(&client, &name, "pod-b").await;
    let leader = wait_single_leader(&[&a, &b], Duration::from_secs(5)).await;
    let (leader, follower) = if leader == 0 { (a, b) } else { (b, a) };

    let started = Instant::now();
    let took = leader.shutdown().await;
    assert!(took < Duration::from_secs(3), "shutdown took {took:?}");
    // Resign + revoke: the follower should not have to wait for the TTL.
    wait_value(&follower.rx, true, TTL).await;
    eprintln!("handover after graceful shutdown: {:?}", started.elapsed());
    assert_eq!(
        current_leader_value(&client, &name).await,
        follower.identity
    );
    follower.shutdown().await;
}

#[tokio::test]
async fn leader_crash_hands_over_after_ttl() {
    let Some(client) = client("crash").await else {
        return;
    };
    let name = election_name("crash");
    let a = Candidate::start(&client, &name, "pod-a").await;
    let b = Candidate::start(&client, &name, "pod-b").await;
    let leader = wait_single_leader(&[&a, &b], Duration::from_secs(5)).await;
    let (leader, follower) = if leader == 0 { (a, b) } else { (b, a) };

    let started = Instant::now();
    leader.kill();
    // The crashed leader's lease expires after the TTL; then the follower's campaign returns.
    wait_value(&follower.rx, true, TTL + Duration::from_secs(3)).await;
    eprintln!("handover after crash: {:?}", started.elapsed());
    assert_eq!(
        current_leader_value(&client, &name).await,
        follower.identity
    );
    follower.shutdown().await;
}

#[tokio::test]
async fn revoked_lease_steps_down_and_reelects() {
    let Some(client) = client("revoke").await else {
        return;
    };
    let name = election_name("revoke");
    let a = Candidate::start(&client, &name, "pod-a").await;
    let b = Candidate::start(&client, &name, "pod-b").await;
    let first = wait_single_leader(&[&a, &b], Duration::from_secs(5)).await;
    let leader_rx = [&a, &b][first].rx.clone();

    let resp = client
        .election_client()
        .leader(name.as_str())
        .await
        .unwrap();
    let lease = resp.kv().unwrap().lease();
    assert_ne!(lease, 0);
    client.lease_client().revoke(lease).await.unwrap();

    // The old leader notices at its next keep-alive (TTL/5) and steps down.
    wait_value(&leader_rx, false, TTL).await;
    // Someone (the follower right away, or the old leader after its backoff) is elected again.
    let second = wait_single_leader(&[&a, &b], TTL * 2).await;
    let resp = client
        .election_client()
        .leader(name.as_str())
        .await
        .unwrap();
    let kv = resp.kv().unwrap();
    assert_ne!(kv.lease(), lease);
    assert_eq!(kv.value_str().unwrap(), [&a, &b][second].identity);

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn deleted_election_key_steps_down_and_reelects() {
    let Some(client) = client("delete-key").await else {
        return;
    };
    let name = election_name("delete");
    let a = Candidate::start(&client, &name, "pod-a").await;
    wait_value(&a.rx, true, Duration::from_secs(5)).await;

    let mut rx = a.rx.clone();
    rx.borrow_and_update();
    let resp = client
        .election_client()
        .leader(name.as_str())
        .await
        .unwrap();
    let key = resp.kv().unwrap().key().to_vec();
    client.kv_client().delete(key, None).await.unwrap();

    timeout(TTL, rx.wait_for(|v| !*v))
        .await
        .expect("did not step down after its key was deleted")
        .unwrap();
    wait_value(&a.rx, true, TTL * 2).await;
    a.shutdown().await;
}

#[tokio::test]
async fn shutdown_while_campaigning_returns() {
    let Some(client) = client("campaigning").await else {
        return;
    };
    let name = election_name("campaigning");
    let a = Candidate::start(&client, &name, "pod-a").await;
    wait_value(&a.rx, true, Duration::from_secs(5)).await;
    let b = Candidate::start(&client, &name, "pod-b").await;
    sleep(Duration::from_millis(300)).await;
    assert!(!b.is_leader());
    let took = b.shutdown().await;
    assert!(took < Duration::from_secs(3), "shutdown took {took:?}");
    // The follower's candidate key is gone (lease revoked), so only one key remains.
    let keys = client
        .kv_client()
        .get(
            format!("{name}/"),
            Some(
                etcd_client::GetOptions::new()
                    .with_prefix()
                    .with_keys_only(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(keys.kvs().len(), 1);
    // A loaded runner can make the leader miss two keep-alives of the short test TTL and step
    // down for a moment; the follower's shutdown must not cost it the leadership for good.
    wait_value(&a.rx, true, TTL * 2).await;
    a.shutdown().await;
}

/// Candidate keys under the election prefix: (key, lease), oldest first.
async fn candidate_keys(client: &Client, name: &str) -> Vec<(String, i64)> {
    let resp = client
        .kv_client()
        .get(
            format!("{name}/"),
            Some(GetOptions::new().with_prefix().with_sort(
                etcd_client::SortTarget::Create,
                etcd_client::SortOrder::Ascend,
            )),
        )
        .await
        .unwrap();
    resp.kvs()
        .iter()
        .map(|kv| (kv.key_str().unwrap().to_string(), kv.lease()))
        .collect()
}

/// A follower whose campaign lasts longer than the etcd URL's request timeout keeps its
/// place (same key, same lease) and is elected once the leader's lease is gone.
#[tokio::test]
async fn long_campaign_outlives_request_timeout() {
    let Some(client) = client("long-campaign").await else {
        return;
    };
    let host = std::env::var("PINGAP_K8S_TEST_ETCD")
        .unwrap()
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    // A 1 s request timeout: the KV client would cancel a 1 s+ Campaign.
    let target = parse_etcd_url(&format!("etcd://{host}/pingap?timeout=1s")).unwrap();
    let election_client = EtcdWriter::connect_election(&target).await.unwrap();

    let name = election_name("long-campaign");
    let leader = Candidate::start(&client, &name, "pod-a").await;
    wait_value(&leader.rx, true, Duration::from_secs(5)).await;
    let follower = Candidate::start(&election_client, &name, "pod-b").await;

    // Wait for the follower's key, then for several request timeouts.
    let deadline = Instant::now() + Duration::from_secs(5);
    let queued = loop {
        let keys = candidate_keys(&client, &name).await;
        if keys.len() == 2 {
            break keys[1].clone();
        }
        assert!(Instant::now() < deadline, "follower never campaigned");
        sleep(Duration::from_millis(20)).await;
    };
    sleep(Duration::from_millis(3500)).await;
    assert!(!follower.is_leader());
    let keys = candidate_keys(&client, &name).await;
    assert_eq!(keys.len(), 2, "{keys:?}");
    assert_eq!(keys[1], queued, "the follower re-campaigned");

    // The leader's lease goes away: the waiting campaign wins with the same key.
    let leader_lease = keys[0].1;
    leader.kill();
    client.lease_client().revoke(leader_lease).await.unwrap();
    wait_value(&follower.rx, true, Duration::from_secs(2)).await;
    let resp = client
        .election_client()
        .leader(name.as_str())
        .await
        .unwrap();
    let kv = resp.kv().unwrap();
    assert_eq!(kv.key_str().unwrap(), queued.0);
    assert_eq!(kv.lease(), queued.1);
    assert_eq!(kv.value_str().unwrap(), "pod-b");
    follower.shutdown().await;
}

/// A TCP proxy to etcd that can be cut: every connection through it is closed
/// and no new one is accepted.
struct Proxy {
    addr: std::net::SocketAddr,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Proxy {
    async fn start(upstream: String) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let tasks = Arc::new(Mutex::new(Vec::new()));
        let accepted = tasks.clone();
        let acceptor = tokio::spawn(async move {
            while let Ok((mut inbound, _)) = listener.accept().await {
                let upstream = upstream.clone();
                let conn = tokio::spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(&upstream).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
                accepted.lock().unwrap().push(conn);
            }
        });
        tasks.lock().unwrap().push(acceptor);
        Self { addr, tasks }
    }

    fn cut(&self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

/// A follower never writes, so only the election tells it whether etcd is
/// reachable: its keep-alives set `etcd_up` to 1 while campaigning and to 0
/// within a few keep-alive intervals once etcd cannot be reached.
#[tokio::test]
async fn follower_reports_etcd_up_from_the_election() {
    let Some(direct) = client("follower_reports_etcd_up_from_the_election").await else {
        return;
    };
    let endpoint = std::env::var("PINGAP_K8S_TEST_ETCD").unwrap();
    let host = endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    let proxy = Proxy::start(host).await;
    let via_proxy = Client::connect([proxy.addr.to_string()], None)
        .await
        .unwrap();

    let name = election_name("etcd-up");
    let leader = Candidate::start(&direct, &name, "leader").await;
    wait_value(&leader.rx, true, Duration::from_secs(10)).await;

    let metrics = Arc::new(Metrics::new());
    let health = EtcdHealth::new(metrics.clone(), Events::disabled());
    let (tx, rx) = watch::channel(false);
    let (stop, stopped) = oneshot::channel::<()>();
    let follower = tokio::spawn(run_election_with_health(
        via_proxy,
        name.clone(),
        TTL,
        "follower".into(),
        tx,
        async move {
            let _ = stopped.await;
        },
        Some(health),
    ));
    let wait_etcd_up = |want: u64, within: Duration| {
        let metrics = metrics.clone();
        async move {
            let deadline = Instant::now() + within;
            while metrics.value("pingap_k8s_etcd_up", &[]) != want {
                assert!(
                    Instant::now() < deadline,
                    "etcd_up did not become {want} within {within:?}"
                );
                sleep(Duration::from_millis(50)).await;
            }
        }
    };
    wait_etcd_up(1, Duration::from_secs(5)).await;
    assert!(!*rx.borrow(), "the follower must not be leader");

    proxy.cut();
    wait_etcd_up(0, Duration::from_secs(10)).await;
    assert!(!*rx.borrow());

    let _ = stop.send(());
    let _ = timeout(Duration::from_secs(10), follower).await;
    leader.shutdown().await;
}

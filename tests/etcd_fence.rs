//! Fencing of etcd writes with the election key, against a real etcd. Skipped unless
//! `PINGAP_K8S_TEST_ETCD` is set to an endpoint such as `http://127.0.0.1:23790` (see
//! scripts/test-etcd.sh).
//!
//! The case that matters: a leader paused after it last checked its lease still believes
//! it leads when it resumes. Its fence still holds the old key, so etcd itself must refuse
//! the write.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use etcd_client::{Client, PutOptions};
use pingap_k8s::etcd::{EtcdWriter, parse_etcd_url, read_addrs};
use pingap_k8s::gateway::model::{Expect, Op, PingapStore, Plan, Step};
use pingap_k8s::gateway::writer::{GatewayWriter, TxnLimits};
use pingap_k8s::leader::{FenceKey, LeaderFence, NotLeader, run_election_fenced};
use pingap_k8s::types::AddrsWriter;
use tokio::sync::{oneshot, watch};
use tokio::time::timeout;

fn host() -> Option<String> {
    let endpoint = std::env::var("PINGAP_K8S_TEST_ETCD").ok()?;
    Some(
        endpoint
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_string(),
    )
}

fn unique(test: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("pingap-k8s-test-{test}-{nanos}")
}

/// A writer under its own prefix with `fence`, and a plain client.
async fn setup(test: &str, fence: &LeaderFence) -> Option<(EtcdWriter, Client, String)> {
    let Some(host) = host() else {
        eprintln!("PINGAP_K8S_TEST_ETCD not set; skipping {test}");
        return None;
    };
    let name = unique(test);
    let target = parse_etcd_url(&format!("etcd://{host}/{name}")).unwrap();
    let writer = EtcdWriter::connect(&target)
        .await
        .unwrap()
        .with_fence(fence.clone());
    let client = Client::connect([host], None).await.unwrap();
    Some((writer, client, name))
}

/// A key held by a fresh lease, like the election server writes it.
async fn election_key(client: &Client, name: &str) -> (FenceKey, i64) {
    let lease = client.lease_client().grant(30, None).await.unwrap().id();
    let key = format!("/{name}-k8s-election/{lease:x}");
    let resp = client
        .kv_client()
        .put(
            key.as_str(),
            "pod-a",
            Some(PutOptions::new().with_lease(lease)),
        )
        .await
        .unwrap();
    let revision = resp.header().unwrap().revision();
    (
        FenceKey {
            key: key.into_bytes(),
            revision,
        },
        lease,
    )
}

fn is_not_leader(err: &anyhow::Error) -> bool {
    err.chain().any(|e| e.is::<NotLeader>())
}

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

async fn stored_addrs(client: &Client, writer: &EtcdWriter, upstream: &str) -> Option<Vec<String>> {
    let resp = client
        .kv_client()
        .get(writer.upstream_key(upstream), None)
        .await
        .unwrap();
    let kv = resp.kvs().first()?;
    read_addrs(std::str::from_utf8(kv.value()).unwrap(), upstream).unwrap()
}

#[tokio::test]
async fn writes_without_the_key_are_refused_before_etcd() {
    let fence = LeaderFence::default();
    let Some((writer, client, _)) = setup("fence-empty", &fence).await else {
        return;
    };
    let err = writer
        .write_addrs("web", &s(&["10.0.0.1:80"]))
        .await
        .unwrap_err();
    assert!(is_not_leader(&err), "{err:#}");
    assert_eq!(stored_addrs(&client, &writer, "web").await, None);
}

#[tokio::test]
async fn a_stale_key_is_refused_by_etcd() {
    let fence = LeaderFence::default();
    let Some((writer, client, name)) = setup("fence-stale", &fence).await else {
        return;
    };
    let (key, lease) = election_key(&client, &name).await;
    fence.set(Some(key.clone()));

    // Leading: creates, then updates.
    writer
        .write_addrs("web", &s(&["10.0.0.1:80"]))
        .await
        .unwrap();
    writer
        .write_addrs("web", &s(&["10.0.0.2:80"]))
        .await
        .unwrap();
    assert_eq!(
        stored_addrs(&client, &writer, "web").await,
        Some(s(&["10.0.0.2:80"]))
    );

    // The lease ends while the process is paused; its fence still holds the key.
    client.lease_client().revoke(lease).await.unwrap();
    for addrs in [&["10.0.0.3:80"][..], &["10.0.0.4:80"]] {
        let err = writer.write_addrs("web", &s(addrs)).await.unwrap_err();
        assert!(is_not_leader(&err), "{err:#}");
    }
    let err = writer
        .write_addrs("new", &s(&["10.0.0.5:80"]))
        .await
        .unwrap_err();
    assert!(is_not_leader(&err), "{err:#}");
    assert_eq!(
        stored_addrs(&client, &writer, "web").await,
        Some(s(&["10.0.0.2:80"]))
    );
    assert_eq!(stored_addrs(&client, &writer, "new").await, None);
}

#[tokio::test]
async fn a_key_of_another_campaign_is_refused() {
    // Same key, other create revision: deleted and written again by a newer leader.
    let fence = LeaderFence::default();
    let Some((writer, client, name)) = setup("fence-recreated", &fence).await else {
        return;
    };
    let (key, _) = election_key(&client, &name).await;
    fence.set(Some(key.clone()));
    client
        .kv_client()
        .delete(key.key.clone(), None)
        .await
        .unwrap();
    client
        .kv_client()
        .put(key.key.clone(), "pod-b", None)
        .await
        .unwrap();
    let err = writer
        .write_addrs("web", &s(&["10.0.0.1:80"]))
        .await
        .unwrap_err();
    assert!(is_not_leader(&err), "{err:#}");
}

#[tokio::test]
async fn a_conflict_is_still_a_conflict_while_leading() {
    // A failed Txn with the key in place is a normal compare-and-swap conflict, which the
    // writer retries; it must not be taken for lost leadership.
    let fence = LeaderFence::default();
    let Some((mut writer, client, name)) = setup("fence-conflict", &fence).await else {
        return;
    };
    let (key, _) = election_key(&client, &name).await;
    fence.set(Some(key));
    writer
        .write_addrs("web", &s(&["10.0.0.1:80"]))
        .await
        .unwrap();
    let other = client.clone();
    let upstream_key = writer.upstream_key("web");
    writer.set_pre_commit_hook(move |attempt| {
        let other = other.clone();
        let upstream_key = upstream_key.clone();
        async move {
            if attempt == 0 {
                let value = "[upstreams.web]\naddrs = [\"10.9.9.9:80\"]\n";
                other
                    .kv_client()
                    .put(upstream_key, value, None)
                    .await
                    .unwrap();
            }
        }
    });
    writer
        .write_addrs("web", &s(&["10.0.0.2:80"]))
        .await
        .unwrap();
    assert_eq!(
        stored_addrs(&client, &writer, "web").await,
        Some(s(&["10.0.0.2:80"]))
    );
}

#[tokio::test]
async fn gateway_plans_are_fenced() {
    let fence = LeaderFence::default();
    let Some((writer, client, name)) = setup("fence-gateway", &fence).await else {
        return;
    };
    let gateway = GatewayWriter::new(&writer, TxnLimits::default());
    let plan = |entry: &str| Plan {
        steps: vec![Step {
            what: "locations",
            ops: vec![Op::Put {
                key: format!("/{name}/locations/{entry}.toml"),
                value: format!("[locations.{entry}]\n"),
                expect: Expect::Absent,
            }],
        }],
        ..Plan::default()
    };

    let err = gateway.apply(&plan("a")).await.unwrap_err();
    assert!(is_not_leader(&err), "{err:#}");

    let (key, lease) = election_key(&client, &name).await;
    fence.set(Some(key));
    gateway.apply(&plan("b")).await.unwrap();

    client.lease_client().revoke(lease).await.unwrap();
    let err = gateway.apply(&plan("c")).await.unwrap_err();
    assert!(is_not_leader(&err), "{err:#}");

    let stored = gateway.read_stored().await.unwrap();
    let names: Vec<&String> = stored.locations.keys().collect();
    assert_eq!(names, ["b"]);
}

#[tokio::test]
async fn the_election_sets_and_clears_the_fence() {
    let Some(host) = host() else {
        eprintln!("PINGAP_K8S_TEST_ETCD not set; skipping the_election_sets_and_clears_the_fence");
        return;
    };
    let client = Client::connect([host], None).await.unwrap();
    let name = format!("/{}-k8s-election", unique("fence-election"));
    let fence = LeaderFence::default();
    let (tx, mut rx) = watch::channel(false);
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(run_election_fenced(
        client.clone(),
        name.clone(),
        Duration::from_secs(3),
        "pod-a".to_string(),
        tx,
        fence.clone(),
        async move {
            let _ = stopped.await;
        },
        None,
    ));

    timeout(Duration::from_secs(10), rx.wait_for(|leader| *leader))
        .await
        .expect("not elected")
        .unwrap();
    let key = fence.get().expect("fence set while leading");
    assert!(key.key.starts_with(name.as_bytes()));
    let resp = client.kv_client().get(key.key.clone(), None).await.unwrap();
    assert_eq!(resp.kvs()[0].create_revision(), key.revision);

    // Deleting the key ends leadership; the fence is cleared before the channel says so.
    client
        .kv_client()
        .delete(key.key.clone(), None)
        .await
        .unwrap();
    timeout(Duration::from_secs(10), rx.wait_for(|leader| !*leader))
        .await
        .expect("still leader")
        .unwrap();
    assert_ne!(fence.get(), Some(key));

    stop.send(()).unwrap();
    timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fence.get(), None);
}

//! `EtcdWriter::watch_owned` after a compaction, against a real etcd. Kept in its own
//! test binary because compaction affects every key in etcd, so it must not run in
//! parallel with the other watch tests. Skipped unless `PINGAP_K8S_TEST_ETCD`
//! is set to an endpoint such as `http://127.0.0.1:23790` (see scripts/test-etcd.sh).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pingap_k8s::etcd::{EtcdWriter, parse_etcd_url};
use pingap_k8s::types::ForeignWrite;
use tokio::sync::mpsc::Receiver;

async fn writer(test: &str) -> Option<EtcdWriter> {
    let Ok(endpoint) = std::env::var("PINGAP_K8S_TEST_ETCD") else {
        eprintln!("PINGAP_K8S_TEST_ETCD not set; skipping {test}");
        return None;
    };
    let host = endpoint
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let target = parse_etcd_url(&format!("etcd://{host}/pingap-k8s-test-{test}-{nanos}")).unwrap();
    Some(EtcdWriter::connect(&target).await.unwrap())
}

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

async fn next(rx: &mut Receiver<ForeignWrite>) -> ForeignWrite {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("no foreign write within 5s")
        .expect("watch channel closed")
}

#[tokio::test]
async fn compacted_revision_requests_resync_then_resumes() {
    let Some(w) = writer("watch-compact").await else {
        return;
    };
    let client = w.client();
    let mut kv = client.kv_client();
    let api = w.upstream_key("api");
    let first = kv
        .put(
            api.as_str(),
            "[upstreams.api]\naddrs = [\"1.1.1.1:1\"]\n",
            None,
        )
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision();
    let last = kv
        .put(
            api.as_str(),
            "[upstreams.api]\naddrs = [\"2.2.2.2:2\"]\n",
            None,
        )
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision();
    kv.compact(last, None).await.unwrap();

    // Resuming from a compacted revision: history is lost, so a full resync is asked for.
    let mut rx = w
        .watch_owned_from(vec!["api".to_string()], first)
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, ForeignWrite::Resync);

    // The watch carries on afterwards.
    let rev = kv
        .put(
            api.as_str(),
            "[upstreams.api]\naddrs = [\"3.3.3.3:3\"]\n",
            None,
        )
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision();
    loop {
        match next(&mut rx).await {
            // Events replayed from the compact revision may come first.
            ForeignWrite::Put { revision, .. } if revision < rev => continue,
            other => {
                assert_eq!(
                    other,
                    ForeignWrite::Put {
                        upstream: "api".to_string(),
                        addrs: Some(s(&["3.3.3.3:3"])),
                        revision: rev,
                    }
                );
                break;
            }
        }
    }
}

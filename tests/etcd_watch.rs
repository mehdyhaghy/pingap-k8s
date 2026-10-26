//! `EtcdWriter::watch_owned` against a real etcd. Skipped unless `PINGAP_K8S_TEST_ETCD`
//! is set to an endpoint such as `http://127.0.0.1:23790` (see scripts/test-etcd.sh).

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pingap_k8s::etcd::{EtcdWriter, parse_etcd_url};
use pingap_k8s::types::{AddrsWriter, ForeignWrite, WriteOutcome};
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
async fn ignores_own_writes_and_reports_foreign_put_and_delete() {
    let Some(w) = writer("watch").await else {
        return;
    };
    let mut rx = w
        .watch_owned(vec!["api".to_string(), "web".to_string()])
        .await
        .unwrap();
    let client = w.client();
    let mut kv = client.kv_client();
    let api = w.upstream_key("api");

    // Own writes (create, then update) are not reported.
    let created = w.write_addrs("api", &s(&["10.0.0.1:80"])).await.unwrap();
    assert!(matches!(created, WriteOutcome::Created { .. }));
    let updated = w
        .write_addrs("api", &s(&["10.0.0.1:80", "10.0.0.2:80"]))
        .await
        .unwrap();
    assert!(matches!(updated, WriteOutcome::Updated { .. }));

    // Keys that are not owned are not reported: another upstream, a neighbour key that
    // shares the name as a prefix, and an owned name under another category.
    kv.put(
        w.upstream_key("other"),
        "[upstreams.other]\naddrs = []\n",
        None,
    )
    .await
    .unwrap();
    kv.put(format!("{api}.bak"), "x", None).await.unwrap();
    kv.put(format!("{}/locations/api.toml", w.prefix()), "x", None)
        .await
        .unwrap();

    // A foreign write (an admin save putting old addrs back) is reported, with addrs.
    let admin = "[upstreams.api]\naddrs = [\"10.0.0.9:80\"]\nhealth_check = \"tcp://x\"\n";
    let rev = kv
        .put(api.as_str(), admin, None)
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision();
    assert_eq!(
        next(&mut rx).await,
        ForeignWrite::Put {
            upstream: "api".to_string(),
            addrs: Some(s(&["10.0.0.9:80"])),
            revision: rev,
        }
    );

    // The controller corrects it; that write is its own and not reported.
    w.write_addrs("api", &s(&["10.0.0.1:80", "10.0.0.2:80"]))
        .await
        .unwrap();

    // An unparsable foreign write is reported with addrs = None.
    let rev = kv
        .put(w.upstream_key("web"), "not toml [", None)
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision();
    assert_eq!(
        next(&mut rx).await,
        ForeignWrite::Put {
            upstream: "web".to_string(),
            addrs: None,
            revision: rev,
        }
    );

    // A delete is reported.
    let rev = kv
        .delete(api.as_str(), None)
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision();
    assert_eq!(
        next(&mut rx).await,
        ForeignWrite::Deleted {
            upstream: "api".to_string(),
            revision: rev,
        }
    );

    // Nothing else is pending.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn concurrent_own_writes_never_reported() {
    let Some(w) = writer("watch-race").await else {
        return;
    };
    let mut rx = w.watch_owned(vec!["api".to_string()]).await.unwrap();
    // Many own writes back to back: the watch may see each event before the writer has
    // returned, and must still recognise it.
    for i in 0..50 {
        let addrs = vec![format!("10.0.0.{}:80", i % 2 + 1)];
        w.write_addrs("api", &addrs).await.unwrap();
    }
    let api = w.upstream_key("api");
    let rev = w
        .client()
        .kv_client()
        .delete(api.as_str(), None)
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision();
    assert_eq!(
        next(&mut rx).await,
        ForeignWrite::Deleted {
            upstream: "api".to_string(),
            revision: rev,
        }
    );
}

#[tokio::test]
async fn watch_task_stops_when_receiver_dropped() {
    let Some(w) = writer("watch-drop").await else {
        return;
    };
    let rx = w.watch_owned(vec!["api".to_string()]).await.unwrap();
    drop(rx);
    // Writing afterwards must not panic or block.
    let outcome = w.write_addrs("api", &s(&["10.0.0.1:80"])).await.unwrap();
    assert!(matches!(outcome, WriteOutcome::Created { .. }));
}

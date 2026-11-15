//! A URL listing several etcd members, some of which do not answer. etcd-client alone
//! sends requests to every listed member, so writes failed at random (and on a fresh
//! install, where the member names resolve only once the pods exist, until a restart).
//! `etcd_members` probes the members and keeps requests on those that answer. Skipped
//! unless `PINGAP_K8S_TEST_ETCD` is set (see scripts/test-etcd.sh).

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use pingap_k8s::etcd::{EtcdWriter, parse_etcd_url};
use pingap_k8s::etcd_members::PROBE_EVERY;
use pingap_k8s::types::AddrsWriter;
use tokio::io::copy_bidirectional;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};

fn etcd_host(test: &str) -> Option<String> {
    let Ok(endpoint) = std::env::var("PINGAP_K8S_TEST_ETCD") else {
        eprintln!("PINGAP_K8S_TEST_ETCD not set; skipping {test}");
        return None;
    };
    Some(
        endpoint
            .trim_start_matches("http://")
            .trim_end_matches('/')
            .to_string(),
    )
}

fn prefix(test: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("pingap-k8s-test-{test}-{nanos}")
}

/// A local port nothing listens on (connections are refused).
async fn closed_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Forwards connections on `listener` to `upstream`.
fn forward(listener: TcpListener, upstream: String) {
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                return;
            };
            let upstream = upstream.clone();
            tokio::spawn(async move {
                if let Ok(mut outbound) = TcpStream::connect(&upstream).await {
                    let _ = copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
}

/// Writes `count` times, one at a time; returns how many failed.
async fn failures(writer: &EtcdWriter, count: usize) -> usize {
    let mut failed = 0;
    for i in 0..count {
        let addrs = vec![format!("10.0.0.{}:80", i % 2 + 1)];
        match timeout(Duration::from_secs(3), writer.write_addrs("web", &addrs)).await {
            Ok(Ok(_)) => {}
            _ => failed += 1,
        }
    }
    failed
}

#[tokio::test]
async fn requests_skip_members_that_do_not_answer() {
    let Some(host) = etcd_host("requests_skip_members_that_do_not_answer") else {
        return;
    };
    // A refused port and a name that never resolves, around the real member.
    let refused = closed_port().await;
    let url = format!(
        "etcd://127.0.0.1:{refused},{host},etcd-member.invalid:2379/{}",
        prefix("members-skip")
    );
    let writer = EtcdWriter::connect(&parse_etcd_url(&url).unwrap())
        .await
        .unwrap();
    // The first probe runs at once; give it time to finish.
    sleep(PROBE_EVERY + Duration::from_secs(1)).await;
    assert_eq!(failures(&writer, 30).await, 0);
}

#[tokio::test]
async fn a_member_that_comes_up_later_is_used() {
    let Some(host) = etcd_host("a_member_that_comes_up_later_is_used") else {
        return;
    };
    // Like a fresh install: no member answers at first; then one does.
    let late = closed_port().await;
    let refused = closed_port().await;
    let url = format!(
        "etcd://127.0.0.1:{late},127.0.0.1:{refused},etcd-member.invalid:2379/{}",
        prefix("members-late")
    );
    let writer = EtcdWriter::connect(&parse_etcd_url(&url).unwrap())
        .await
        .unwrap();
    sleep(PROBE_EVERY + Duration::from_secs(1)).await;
    assert!(failures(&writer, 1).await > 0, "no member answers yet");

    forward(
        TcpListener::bind(("127.0.0.1", late)).await.unwrap(),
        host.clone(),
    );
    let start = Instant::now();
    loop {
        if failures(&writer, 1).await == 0 {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(20),
            "writes did not resume after the member came up"
        );
        sleep(Duration::from_millis(500)).await;
    }
    sleep(PROBE_EVERY + Duration::from_secs(1)).await;
    assert_eq!(failures(&writer, 30).await, 0);
}

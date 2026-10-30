//! The reconciler with the real `EtcdWriter` and owned-key watch:
//! a stale admin save is corrected through the debounce and keeps its other edits, and a
//! deleted key is recreated. Kubernetes is replaced by a fixed snapshot. Skipped unless
//! `PINGAP_K8S_TEST_ETCD` is set (see scripts/test-etcd.sh).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort, EndpointSlice};
use pingap_k8s::config::UpstreamConfig;
use pingap_k8s::etcd::{EtcdWriter, parse_etcd_url, read_addrs};
use pingap_k8s::metrics::Metrics;
use pingap_k8s::reconcile::Reconciler;
use pingap_k8s::server::Readiness;
use pingap_k8s::types::{
    PortRef, ServiceKey, ServiceRef, ServiceSnapshot, SnapshotSource, UpstreamOptions,
    UpstreamTarget,
};
use tokio::sync::{broadcast, oneshot, watch};

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

/// One Service `prod/api` with port `http` -> 8080 and two ready endpoints.
struct Fixed;

impl SnapshotSource for Fixed {
    fn snapshot(&self, _key: &ServiceKey) -> ServiceSnapshot {
        let ready = |ip: &str| Endpoint {
            addresses: vec![ip.to_string()],
            conditions: Some(EndpointConditions {
                ready: Some(true),
                serving: Some(true),
                terminating: Some(false),
            }),
            ..Default::default()
        };
        ServiceSnapshot {
            service: Some(Arc::new(Service {
                spec: Some(ServiceSpec {
                    ports: Some(vec![ServicePort {
                        name: Some("http".into()),
                        port: 80,
                        ..Default::default()
                    }]),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            slices: vec![Arc::new(EndpointSlice {
                address_type: "IPv4".into(),
                endpoints: vec![ready("10.0.0.1"), ready("10.0.0.2")],
                ports: Some(vec![EndpointPort {
                    name: Some("http".into()),
                    port: Some(8080),
                    protocol: Some("TCP".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            })],
        }
    }
}

fn desired() -> Vec<String> {
    vec!["10.0.0.1:8080".to_string(), "10.0.0.2:8080".to_string()]
}

/// The owned upstream `api` -> `prod/api:http`, with the given target.
fn upstreams(target: UpstreamTarget) -> BTreeMap<String, UpstreamConfig> {
    let options = UpstreamOptions {
        include_not_ready: false,
        drain_terminating: true,
        ip_family: pingap_k8s::types::IpFamily::Primary,
        external_name: false,
        debounce: Duration::from_millis(500),
        debounce_max: Duration::from_secs(2),
        target,
        spread_weights: false,
    };
    let mut upstreams = BTreeMap::new();
    upstreams.insert(
        "api".to_string(),
        UpstreamConfig {
            services: vec![ServiceRef {
                key: ServiceKey {
                    namespace: "prod".into(),
                    name: "api".into(),
                },
                port: PortRef::Name("http".into()),
                weight: None,
            }],
            options,
            health_check: None,
            tls: None,
        },
    );
    upstreams
}

async fn stored(w: &EtcdWriter, upstream: &str) -> Option<String> {
    let resp = w
        .client()
        .kv_client()
        .get(w.upstream_key(upstream), None)
        .await
        .unwrap();
    resp.kvs()
        .first()
        .map(|kv| kv.value_str().unwrap().to_string())
}

/// Polls until the stored `addrs` equal `desired()`; returns the stored entry.
async fn wait_desired(w: &EtcdWriter, within: Duration) -> String {
    let start = Instant::now();
    loop {
        if let Some(text) = stored(w, "api").await
            && read_addrs(&text, "api").unwrap() == Some(desired())
        {
            return text;
        }
        assert!(
            start.elapsed() < within,
            "addrs not corrected within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn stale_admin_save_is_corrected_and_deleted_key_recreated() {
    let Some(w) = writer("reconcile").await else {
        return;
    };
    let key = w.upstream_key("api");
    let mut kv = w.client().kv_client();
    kv.put(
        key.as_str(),
        "[upstreams.api]\naddrs = [\"10.0.0.9:80\"]\nhealth_check = \"http://api/ping\"\n",
        None,
    )
    .await
    .unwrap();

    let foreign = w.watch_owned(vec!["api".to_string()]).await.unwrap();
    let metrics = Arc::new(Metrics::new());
    let reconciler = Reconciler::new(
        Arc::new(w.clone()),
        Arc::new(Fixed),
        metrics.clone(),
        Readiness::new(),
    );
    let upstreams = upstreams(UpstreamTarget::Pods);
    reconciler.set_upstreams(upstreams);
    let (_changes_tx, changes) = broadcast::channel(16);
    let (_leader_tx, leader) = watch::channel(true);
    let (stop, stopped) = oneshot::channel::<()>();
    let task =
        tokio::spawn(
            reconciler.run(changes, foreign, leader, Duration::from_secs(300), async {
                let _ = stopped.await;
            }),
        );

    // Startup: only addrs is replaced.
    let text = wait_desired(&w, Duration::from_secs(5)).await;
    assert!(
        text.contains("health_check = \"http://api/ping\""),
        "{text}"
    );

    // An admin save from a form opened before the controller's write: old addrs,
    // and a health-check edit. Corrected within debounce_max + 1 s, edit kept.
    kv.put(
        key.as_str(),
        "[upstreams.api]\naddrs = [\"10.0.0.9:80\"]\nhealth_check = \"http://api/healthz\"\n",
        None,
    )
    .await
    .unwrap();
    let text = wait_desired(&w, Duration::from_secs(3)).await;
    assert!(
        text.contains("health_check = \"http://api/healthz\""),
        "{text}"
    );
    assert_eq!(
        metrics.value(
            "pingap_k8s_foreign_writes_total",
            &[("upstream", "api"), ("corrected", "true")]
        ),
        1
    );

    // A deleted key is recreated with only addrs.
    kv.delete(key.as_str(), None).await.unwrap();
    let text = wait_desired(&w, Duration::from_secs(3)).await;
    assert!(!text.contains("health_check"), "{text}");

    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("reconciler stops")
        .unwrap();
}

/// Polls until the stored `addrs` of `api` equal `want`; returns the stored entry.
async fn wait_addrs(w: &EtcdWriter, want: &[&str], within: Duration) -> String {
    let start = Instant::now();
    loop {
        if let Some(text) = stored(w, "api").await
            && read_addrs(&text, "api")
                .unwrap()
                .is_some_and(|addrs| addrs == want)
        {
            return text;
        }
        assert!(
            start.elapsed() < within,
            "addrs not {want:?} within {within:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `target = "service"` writes the Service DNS name and port, only
/// `addrs` is touched (no `discovery`: Pingap treats hostname addrs as DNS
/// discovery by itself), and switching back to `pods` at runtime works.
#[tokio::test]
async fn service_target_writes_service_address_and_switches_back() {
    let Some(w) = writer("target").await else {
        return;
    };
    let key = w.upstream_key("api");
    let mut kv = w.client().kv_client();
    kv.put(
        key.as_str(),
        "[upstreams.api]\naddrs = [\"10.0.0.9:80\"]\nhealth_check = \"http://api/ping\"\n",
        None,
    )
    .await
    .unwrap();

    let reconciler = Reconciler::new(
        Arc::new(w.clone()),
        Arc::new(Fixed),
        Arc::new(Metrics::new()),
        Readiness::new(),
    );
    reconciler.set_upstreams(upstreams(UpstreamTarget::Service {
        cluster_domain: "cluster.local".into(),
    }));
    let handle = reconciler.clone();
    let (_changes_tx, changes) = broadcast::channel(16);
    let (_foreign_tx, foreign) = tokio::sync::mpsc::channel(16);
    let (_leader_tx, leader) = watch::channel(true);
    let (stop, stopped) = oneshot::channel::<()>();
    let task =
        tokio::spawn(
            reconciler.run(changes, foreign, leader, Duration::from_secs(300), async {
                let _ = stopped.await;
            }),
        );

    let text = wait_addrs(
        &w,
        &["api.prod.svc.cluster.local:80"],
        Duration::from_secs(5),
    )
    .await;
    assert!(
        text.contains("health_check = \"http://api/ping\""),
        "{text}"
    );
    assert!(!text.contains("discovery"), "{text}");

    handle.set_upstreams(upstreams(UpstreamTarget::Pods));
    let text = wait_addrs(
        &w,
        &["10.0.0.1:8080", "10.0.0.2:8080"],
        Duration::from_secs(5),
    )
    .await;
    assert!(
        text.contains("health_check = \"http://api/ping\""),
        "{text}"
    );

    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("reconciler stops")
        .unwrap();
}

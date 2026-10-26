//! Integration test for `watch::WatchRegistry` against a real cluster.
//!
//! Skipped unless `PINGAP_K8S_TEST_KUBECONFIG` points at a kubeconfig (see
//! `scripts/test-kind.sh`, which creates a throwaway kind cluster). The pod
//! image defaults to `registry.k8s.io/pause:3.10`; override it with
//! `PINGAP_K8S_TEST_IMAGE` (kind nodes ship a pause image, so no pull is needed
//! when the tag matches).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec};
use k8s_openapi::api::core::v1::{
    Container, Namespace, PodSpec, PodTemplateSpec, Service, ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta};
use kube::api::{DeleteParams, PostParams};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Api, Client};
use pingap_k8s::metrics::Metrics;
use pingap_k8s::types::{ServiceKey, ServiceSnapshot, SnapshotSource};
use pingap_k8s::watch::WatchRegistry;
use tokio::sync::broadcast;
use tokio::time::Instant;

const SVC: &str = "echo";

fn ready_endpoints(snap: &ServiceSnapshot) -> usize {
    snap.slices
        .iter()
        .flat_map(|s| s.endpoints.iter())
        .filter(|e| e.conditions.as_ref().and_then(|c| c.ready).unwrap_or(false))
        .map(|e| e.addresses.len().min(1))
        .sum()
}

async fn wait_for_ready_count(
    registry: &WatchRegistry,
    key: &ServiceKey,
    want: usize,
    timeout: Duration,
) -> anyhow::Result<ServiceSnapshot> {
    let deadline = Instant::now() + timeout;
    loop {
        let snap = registry.snapshot(key);
        if ready_endpoints(&snap) == want {
            return Ok(snap);
        }
        if Instant::now() > deadline {
            anyhow::bail!(
                "timed out waiting for {want} ready endpoints, have {}",
                ready_endpoints(&snap)
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Drains `rx` until a notification for `key` arrives (a lag also counts:
/// everything may have changed).
async fn wait_for_notification(
    rx: &mut broadcast::Receiver<ServiceKey>,
    key: &ServiceKey,
    timeout: Duration,
) -> anyhow::Result<()> {
    tokio::time::timeout(timeout, async {
        loop {
            match rx.recv().await {
                Ok(k) if &k == key => return Ok(()),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => return Ok(()),
                Err(e) => return Err(anyhow::anyhow!("channel closed: {e}")),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("no change notification for {key} within {timeout:?}"))?
}

fn deployment(image: &str, replicas: i32) -> Deployment {
    let labels: BTreeMap<String, String> = [("app".to_string(), SVC.to_string())].into();
    Deployment {
        metadata: ObjectMeta {
            name: Some(SVC.to_string()),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        name: "pause".to_string(),
                        image: Some(image.to_string()),
                        image_pull_policy: Some("IfNotPresent".to_string()),
                        ..Default::default()
                    }],
                    termination_grace_period_seconds: Some(0),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn service() -> Service {
    Service {
        metadata: ObjectMeta {
            name: Some(SVC.to_string()),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some([("app".to_string(), SVC.to_string())].into()),
            ports: Some(vec![ServicePort {
                name: Some("http".to_string()),
                port: 80,
                target_port: Some(
                    k8s_openapi::apimachinery::pkg::util::intstr::IntOrString::Int(8080),
                ),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

async fn run(client: Client, ns: &str, image: &str) -> anyhow::Result<()> {
    let services: Api<Service> = Api::namespaced(client.clone(), ns);
    let deployments: Api<Deployment> = Api::namespaced(client.clone(), ns);
    services.create(&PostParams::default(), &service()).await?;
    deployments
        .create(&PostParams::default(), &deployment(image, 2))
        .await?;

    let metrics = Arc::new(Metrics::new());
    let registry = WatchRegistry::new(client.clone(), metrics.clone());
    let mut rx = registry.subscribe();
    let key = ServiceKey {
        namespace: ns.to_string(),
        name: SVC.to_string(),
    };
    registry.set_services([key.clone()].into());

    anyhow::ensure!(
        registry.wait_ready(Duration::from_secs(30)).await,
        "watchers did not sync within 30 s"
    );
    anyhow::ensure!(registry.is_synced());
    wait_for_notification(&mut rx, &key, Duration::from_secs(5)).await?;
    anyhow::ensure!(
        metrics.value(
            "pingap_k8s_watch_up",
            &[("k8s_namespace_name", ns), ("k8s_service_name", SVC)]
        ) == 1,
        "watch_up should be 1"
    );

    // Snapshot: the Service with its port table, then 2 ready pods.
    let snap = wait_for_ready_count(&registry, &key, 2, Duration::from_secs(120)).await?;
    let svc = snap.service.as_ref().expect("service in snapshot");
    let ports = svc.spec.as_ref().and_then(|s| s.ports.clone()).unwrap();
    anyhow::ensure!(ports.len() == 1 && ports[0].port == 80);
    anyhow::ensure!(!snap.slices.is_empty());
    for slice in &snap.slices {
        let label = slice
            .metadata
            .labels
            .as_ref()
            .and_then(|l| l.get("kubernetes.io/service-name"));
        anyhow::ensure!(label.map(String::as_str) == Some(SVC), "foreign slice");
        let port = slice.ports.as_ref().and_then(|p| p.first()).cloned();
        anyhow::ensure!(
            port.and_then(|p| p.port) == Some(8080),
            "slice port is the targetPort"
        );
    }
    eprintln!("synced: 2 ready endpoints");

    // Scale to 4: a notification arrives and the snapshot follows.
    while rx.try_recv().is_ok() {}
    let mut deploy = deployments.get(SVC).await?;
    deploy.spec.as_mut().unwrap().replicas = Some(4);
    deployments
        .replace(SVC, &PostParams::default(), &deploy)
        .await?;
    wait_for_notification(&mut rx, &key, Duration::from_secs(60)).await?;
    wait_for_ready_count(&registry, &key, 4, Duration::from_secs(120)).await?;
    eprintln!("scaled up: 4 ready endpoints");

    // Scale down to 1.
    while rx.try_recv().is_ok() {}
    let mut deploy = deployments.get(SVC).await?;
    deploy.spec.as_mut().unwrap().replicas = Some(1);
    deployments
        .replace(SVC, &PostParams::default(), &deploy)
        .await?;
    wait_for_notification(&mut rx, &key, Duration::from_secs(60)).await?;
    wait_for_ready_count(&registry, &key, 1, Duration::from_secs(120)).await?;
    eprintln!("scaled down: 1 ready endpoint");

    // Deleting the Service shows up as `service: None`.
    while rx.try_recv().is_ok() {}
    services.delete(SVC, &DeleteParams::default()).await?;
    wait_for_notification(&mut rx, &key, Duration::from_secs(30)).await?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while registry.snapshot(&key).service.is_some() {
        anyhow::ensure!(Instant::now() < deadline, "Service deletion not observed");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    eprintln!("service deletion observed");

    // Removing the key stops the watchers.
    registry.set_services(Default::default());
    anyhow::ensure!(registry.snapshot(&key).slices.is_empty());
    anyhow::ensure!(!metrics.encode().contains("pingap_k8s_watch_up{"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_registry_on_kind() {
    let Ok(path) = std::env::var("PINGAP_K8S_TEST_KUBECONFIG") else {
        eprintln!("PINGAP_K8S_TEST_KUBECONFIG not set; skipping");
        return;
    };
    let image = std::env::var("PINGAP_K8S_TEST_IMAGE")
        .unwrap_or_else(|_| "registry.k8s.io/pause:3.10".to_string());
    let kubeconfig = Kubeconfig::read_from(&path).expect("read kubeconfig");
    let config = kube::Config::from_custom_kubeconfig(kubeconfig, &KubeConfigOptions::default())
        .await
        .expect("kubeconfig");
    let client = Client::try_from(config).expect("client");

    let ns = format!("pk8s-t07-{:08x}", rand::random::<u32>());
    let namespaces: Api<Namespace> = Api::all(client.clone());
    namespaces
        .create(
            &PostParams::default(),
            &Namespace {
                metadata: ObjectMeta {
                    name: Some(ns.clone()),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("create namespace");

    let result = run(client, &ns, &image).await;
    let _ = namespaces.delete(&ns, &DeleteParams::default()).await;
    result.expect("watch test");
}

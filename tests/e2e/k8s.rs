//! Kubernetes helpers on the kube client: scale, rollout restart, waits, pod listing.

use std::time::Duration;

use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::rbac::v1::Role;
use k8s_openapi::serde_json::json;
use kube::api::{DeleteParams, ListParams, Patch, PatchParams, PostParams};
use kube::{Api, Client};

use crate::harness::wait_for;

const POLL: Duration = Duration::from_millis(250);

/// A pod as the tests see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodInfo {
    pub name: String,
    pub ip: Option<String>,
    /// Ready condition is True.
    pub ready: bool,
    /// Has a deletion timestamp.
    pub terminating: bool,
    pub phase: String,
}

pub async fn scale_deployment(
    client: &Client,
    ns: &str,
    name: &str,
    replicas: i32,
) -> anyhow::Result<()> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), ns);
    let patch = json!({ "spec": { "replicas": replicas } });
    api.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(())
}

pub async fn scale_statefulset(
    client: &Client,
    ns: &str,
    name: &str,
    replicas: i32,
) -> anyhow::Result<()> {
    let api: Api<StatefulSet> = Api::namespaced(client.clone(), ns);
    let patch = json!({ "spec": { "replicas": replicas } });
    api.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// What `kubectl rollout restart` does: bump a pod template annotation.
pub async fn rollout_restart(client: &Client, ns: &str, name: &str) -> anyhow::Result<()> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), ns);
    let now = k8s_openapi::jiff::Timestamp::now().to_string();
    let patch = json!({ "spec": { "template": { "metadata": { "annotations": {
        "kubectl.kubernetes.io/restartedAt": now
    } } } } });
    api.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
        .await?;
    Ok(())
}

/// Waits until the Deployment's latest generation is fully rolled out: every replica
/// updated, ready and available, and no old replica left.
pub async fn wait_deployment_rolled_out(
    client: &Client,
    ns: &str,
    name: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let api: Api<Deployment> = Api::namespaced(client.clone(), ns);
    wait_for(
        &format!("deployment {ns}/{name} rolled out"),
        timeout,
        POLL,
        || async {
            let d = api.get(name).await?;
            let want = d.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
            let gen_ = d.metadata.generation.unwrap_or(0);
            let st = d.status.unwrap_or_default();
            let done = st.observed_generation.unwrap_or(0) >= gen_
                && st.updated_replicas.unwrap_or(0) == want
                && st.replicas.unwrap_or(0) == want
                && st.ready_replicas.unwrap_or(0) == want
                && st.available_replicas.unwrap_or(0) == want;
            Ok(done.then_some(()))
        },
    )
    .await
}

/// Waits until the StatefulSet has `replicas` ready pods at its current revision.
pub async fn wait_statefulset_ready(
    client: &Client,
    ns: &str,
    name: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    let api: Api<StatefulSet> = Api::namespaced(client.clone(), ns);
    wait_for(
        &format!("statefulset {ns}/{name} ready"),
        timeout,
        POLL,
        || async {
            let s = api.get(name).await?;
            let want = s.spec.as_ref().and_then(|s| s.replicas).unwrap_or(1);
            let st = s.status.unwrap_or_default();
            let done = st.observed_generation.unwrap_or(0) >= s.metadata.generation.unwrap_or(0)
                && st.replicas == want
                && st.ready_replicas.unwrap_or(0) == want;
            Ok(done.then_some(()))
        },
    )
    .await
}

/// Pods matching a label selector, sorted by name.
pub async fn pods(client: &Client, ns: &str, selector: &str) -> anyhow::Result<Vec<PodInfo>> {
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    let list = api.list(&ListParams::default().labels(selector)).await?;
    let mut out: Vec<PodInfo> = list
        .items
        .into_iter()
        .map(|p| {
            let status = p.status.unwrap_or_default();
            PodInfo {
                name: p.metadata.name.unwrap_or_default(),
                ip: status.pod_ip.clone(),
                ready: status
                    .conditions
                    .unwrap_or_default()
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True"),
                terminating: p.metadata.deletion_timestamp.is_some(),
                phase: status.phase.unwrap_or_default(),
            }
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// IPs of the ready, non-terminating pods matching `selector`, sorted.
pub async fn ready_pod_ips(
    client: &Client,
    ns: &str,
    selector: &str,
) -> anyhow::Result<Vec<String>> {
    let mut ips: Vec<String> = pods(client, ns, selector)
        .await?
        .into_iter()
        .filter(|p| p.ready && !p.terminating)
        .filter_map(|p| p.ip)
        .collect();
    ips.sort();
    Ok(ips)
}

/// Deletes a pod; `grace` overrides its termination grace period.
pub async fn delete_pod(
    client: &Client,
    ns: &str,
    name: &str,
    grace: Option<u32>,
) -> anyhow::Result<()> {
    let api: Api<Pod> = Api::namespaced(client.clone(), ns);
    let dp = DeleteParams {
        grace_period_seconds: grace,
        ..Default::default()
    };
    api.delete(name, &dp).await?;
    Ok(())
}

/// Waits until no pod matches `selector` (e.g. after scaling etcd to 0).
pub async fn wait_no_pods(
    client: &Client,
    ns: &str,
    selector: &str,
    timeout: Duration,
) -> anyhow::Result<()> {
    wait_for(
        &format!("no pods {selector} in {ns}"),
        timeout,
        POLL,
        || async { Ok(pods(client, ns, selector).await?.is_empty().then_some(())) },
    )
    .await
}

/// Removes a Role and returns it, for [`restore_role`] (e2e test 5).
pub async fn take_role(client: &Client, ns: &str, name: &str) -> anyhow::Result<Role> {
    let api: Api<Role> = Api::namespaced(client.clone(), ns);
    let role = api.get(name).await?;
    api.delete(name, &DeleteParams::default()).await?;
    Ok(role)
}

/// Re-creates a Role removed with [`take_role`].
pub async fn restore_role(client: &Client, ns: &str, mut role: Role) -> anyhow::Result<()> {
    let api: Api<Role> = Api::namespaced(client.clone(), ns);
    role.metadata.resource_version = None;
    role.metadata.uid = None;
    role.metadata.creation_timestamp = None;
    role.metadata.managed_fields = None;
    api.create(&PostParams::default(), &role).await?;
    Ok(())
}

/// Label selector of the controller pods of the chart release.
pub fn controller_selector(release: &str) -> String {
    format!("app.kubernetes.io/name=pingap-k8s,app.kubernetes.io/instance={release}")
}

/// Label selector of the bundled etcd pods.
pub fn etcd_selector(release: &str) -> String {
    format!("app.kubernetes.io/name=etcd,app.kubernetes.io/instance={release}")
}

/// Label selector of the quickstart Pingap pods.
pub fn pingap_selector(release: &str) -> String {
    format!("app.kubernetes.io/name=pingap,app.kubernetes.io/instance={release}")
}

/// Label selector of the echo pods.
pub const ECHO_SELECTOR: &str = "app=echo";

//! The controller pods: per-pod metrics (scraped from the client pod at the pod IP, so a
//! not-ready pod is reachable too and the leader can be told apart), logs, and the
//! controller's config in its ConfigMap.

use std::time::Duration;

use k8s_openapi::api::core::v1::{ConfigMap, Pod};
use k8s_openapi::serde_json::json;
use kube::Api;
use kube::api::{Patch, PatchParams};

use crate::harness::{Env, wait_for};
use crate::k8s::{self, PodInfo};

/// Port of /metrics, /healthz and /readyz (chart `metricsPort`).
pub const METRICS_PORT: u16 = 9090;

pub fn release(env: &Env) -> String {
    env.state.get("E2E_RELEASE").unwrap().to_string()
}

/// Controller pods (including terminating ones), sorted by name.
pub async fn pods(env: &Env) -> anyhow::Result<Vec<PodInfo>> {
    k8s::pods(
        &env.client,
        &env.namespace,
        &k8s::controller_selector(&release(env)),
    )
    .await
}

/// GET http://<ip>:9090<path> from the client pod.
pub async fn get(env: &Env, ip: &str, path: &str) -> anyhow::Result<crate::pingap::HttpResponse> {
    env.pingap
        .get_url(&format!("http://{ip}:{METRICS_PORT}{path}"))
        .await
}

/// The Prometheus text of one controller pod.
pub async fn scrape(env: &Env, ip: &str) -> anyhow::Result<String> {
    let r = get(env, ip, "/metrics").await?;
    if r.status != 200 {
        anyhow::bail!("GET {ip}:{METRICS_PORT}/metrics: status {}", r.status);
    }
    Ok(r.body)
}

/// Sum of every series of `name` whose labels include all of `labels`; `None` when no
/// series matches.
pub fn metric(text: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let mut sum = None;
    for line in text.lines().filter(|l| !l.starts_with('#')) {
        let Some((series, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let (series_name, series_labels) = match series.split_once('{') {
            Some((n, rest)) => (n, rest.trim_end_matches('}')),
            None => (series, ""),
        };
        if series_name != name {
            continue;
        }
        let ok = labels.iter().all(|(k, v)| {
            series_labels
                .split(',')
                .any(|p| p == format!("{k}=\"{v}\""))
        });
        if ok && let Ok(v) = value.parse::<f64>() {
            *sum.get_or_insert(0.0) += v;
        }
    }
    sum
}

/// The pod reporting `pingap_k8s_is_leader 1`, when exactly one does.
pub async fn leader(env: &Env) -> anyhow::Result<Option<PodInfo>> {
    let mut leaders = Vec::new();
    for p in pods(env).await? {
        let Some(ip) = p.ip.clone() else { continue };
        if p.terminating {
            continue;
        }
        if let Ok(text) = scrape(env, &ip).await
            && metric(&text, "pingap_k8s_is_leader", &[]) == Some(1.0)
        {
            leaders.push(p);
        }
    }
    Ok((leaders.len() == 1).then(|| leaders.remove(0)))
}

pub async fn wait_leader(env: &Env, timeout: Duration) -> anyhow::Result<PodInfo> {
    wait_for(
        "exactly one controller leader",
        timeout,
        Duration::from_millis(500),
        || leader(env),
    )
    .await
}

/// Current logs of every controller pod, concatenated.
pub async fn logs(env: &Env) -> anyhow::Result<String> {
    let mut out = String::new();
    for p in pods(env).await? {
        if let Ok(l) = env
            .kubectl
            .logs(&env.namespace, &p.name, Some("controller"), None)
            .await
        {
            out.push_str(&format!("==> {} <==\n{l}", p.name));
        }
    }
    Ok(out)
}

fn config_map_name(env: &Env) -> String {
    format!("{}-config", release(env))
}

/// The controller's config.toml as stored in the chart's ConfigMap.
pub async fn config_toml(env: &Env) -> anyhow::Result<String> {
    let api: Api<ConfigMap> = Api::namespaced(env.client.clone(), &env.namespace);
    let cm = api.get(&config_map_name(env)).await?;
    cm.data
        .and_then(|d| d.get("config.toml").cloned())
        .ok_or_else(|| anyhow::anyhow!("ConfigMap has no config.toml"))
}

/// Replaces the controller's config.toml. The controller reloads it when the kubelet
/// updates the mounted file; the pods are annotated so the kubelet syncs the volume now
/// instead of on its next periodic sync (up to about a minute).
pub async fn set_config_toml(env: &Env, text: &str) -> anyhow::Result<()> {
    let api: Api<ConfigMap> = Api::namespaced(env.client.clone(), &env.namespace);
    let patch = json!({ "data": { "config.toml": text } });
    api.patch(
        &config_map_name(env),
        &PatchParams::default(),
        &Patch::Merge(&patch),
    )
    .await?;
    let pods_api: Api<Pod> = Api::namespaced(env.client.clone(), &env.namespace);
    let now = k8s_openapi::jiff::Timestamp::now().to_string();
    for p in pods(env).await? {
        let patch = json!({ "metadata": { "annotations": { "e2e/config-touched": now } } });
        let _ = pods_api
            .patch(&p.name, &PatchParams::default(), &Patch::Merge(&patch))
            .await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_metrics() {
        let text = "# HELP x\n\
            pingap_k8s_is_leader 1\n\
            pingap_k8s_watch_errors_total{namespace=\"a\",service=\"echo\",reason=\"forbidden\"} 2\n\
            pingap_k8s_watch_errors_total{namespace=\"a\",service=\"echo\",reason=\"other\"} 1\n\
            pingap_k8s_watch_errors_total{namespace=\"a\",service=\"echo-ws\",reason=\"other\"} 5\n";
        assert_eq!(metric(text, "pingap_k8s_is_leader", &[]), Some(1.0));
        assert_eq!(
            metric(
                text,
                "pingap_k8s_watch_errors_total",
                &[("service", "echo")]
            ),
            Some(3.0)
        );
        assert_eq!(metric(text, "pingap_k8s_watch_up", &[]), None);
    }
}

//! Test gating and the environment written by `scripts/e2e-up.sh`.
//!
//! Every test starts with
//! ```ignore
//! let Some(env) = Env::setup("my_test").await else { return };
//! ```
//! Without `PINGAP_K8S_E2E=1` that prints a skip line and the test passes. With it, the
//! state file must exist (`target/e2e/state.env`, or `E2E_STATE`); a missing file fails
//! the test, so a broken `e2e-up.sh` is never reported as green.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::time::Duration;

use kube::Client;
use kube::config::{KubeConfigOptions, Kubeconfig};
use tokio::time::Instant;

use crate::etcd::Etcd;
use crate::kubectl::Kubectl;
use crate::load::Load;
use crate::pingap::Pingap;

pub const ENABLE_VAR: &str = "PINGAP_K8S_E2E";

pub fn enabled() -> bool {
    std::env::var(ENABLE_VAR).is_ok_and(|v| v == "1")
}

/// Raw `KEY=VALUE` pairs of the state file.
#[derive(Debug, Clone)]
pub struct State(BTreeMap<String, String>);

impl State {
    pub fn path() -> PathBuf {
        match std::env::var_os("E2E_STATE") {
            Some(p) => PathBuf::from(p),
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/e2e/state.env"),
        }
    }

    pub fn load() -> anyhow::Result<State> {
        let path = Self::path();
        let text = std::fs::read_to_string(&path).map_err(|e| {
            anyhow::anyhow!(
                "{}: {e}; run scripts/e2e-up.sh first (or scripts/e2e-run.sh)",
                path.display()
            )
        })?;
        Ok(Self::parse(&text))
    }

    pub fn parse(text: &str) -> State {
        State(
            text.lines()
                .map(str::trim)
                .filter(|l| !l.is_empty() && !l.starts_with('#'))
                .filter_map(|l| l.split_once('='))
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .collect(),
        )
    }

    pub fn get(&self, key: &str) -> anyhow::Result<&str> {
        self.0
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| anyhow::anyhow!("state file has no {key}"))
    }
}

/// Everything a test needs: names from the state file, a kube client and the helpers.
#[derive(Clone)]
pub struct Env {
    pub state: State,
    pub client: Client,
    pub kubectl: Kubectl,
    /// Namespace of the chart (controller, etcd, Pingap).
    pub namespace: String,
    /// Namespace of the echo app and the client pods.
    pub app_namespace: String,
    pub controller_deployment: String,
    pub pingap_deployment: String,
    pub etcd_statefulset: String,
    pub echo_deployment: String,
    /// Owned upstream following echo's "http" port.
    pub upstream: String,
    /// Owned upstream following echo's "ws" port.
    pub ws_upstream: String,
    pub etcd: Etcd,
    pub pingap: Pingap,
    pub load: Load,
}

impl Env {
    /// `None` (after printing a skip line) unless `PINGAP_K8S_E2E=1`. Panics when the
    /// harness is enabled but the environment cannot be loaded.
    pub async fn setup(test: &str) -> Option<Env> {
        if !enabled() {
            println!(
                "skip {test}: set {ENABLE_VAR}=1 and run scripts/e2e-up.sh (or use scripts/e2e-run.sh)"
            );
            return None;
        }
        match Self::load().await {
            Ok(env) => Some(env),
            Err(e) => panic!("{test}: e2e environment not usable: {e:#}"),
        }
    }

    pub async fn load() -> anyhow::Result<Env> {
        let state = State::load()?;
        let kubeconfig = PathBuf::from(state.get("E2E_KUBECONFIG")?);
        let kc = Kubeconfig::read_from(&kubeconfig)?;
        let config =
            kube::Config::from_custom_kubeconfig(kc, &KubeConfigOptions::default()).await?;
        let client = Client::try_from(config)?;
        let kubectl = Kubectl::new(kubeconfig);
        let namespace = state.get("E2E_NAMESPACE")?.to_string();
        let app_namespace = state.get("E2E_APP_NAMESPACE")?.to_string();

        let etcd = Etcd::new(
            kubectl.clone(),
            &namespace,
            state.get("E2E_ETCD_POD")?,
            state.get("E2E_ETCD_PREFIX")?,
        );
        let pingap = Pingap {
            kubectl: kubectl.clone(),
            client_namespace: app_namespace.clone(),
            client_pod: state.get("E2E_CLIENT_POD")?.to_string(),
            url: state.get("E2E_PINGAP_URL")?.to_string(),
            admin_url: state.get("E2E_PINGAP_ADMIN_URL")?.to_string(),
            admin_user: state.get("E2E_PINGAP_ADMIN_USER")?.to_string(),
            admin_password: state.get("E2E_PINGAP_ADMIN_PASSWORD")?.to_string(),
            namespace: namespace.clone(),
            service: state.get("E2E_PINGAP_SERVICE")?.to_string(),
            ws_path: state.get("E2E_PINGAP_WS_PATH")?.to_string(),
        };
        let load = Load {
            kubectl: kubectl.clone(),
            namespace: app_namespace.clone(),
            pod: state.get("E2E_LOAD_POD")?.to_string(),
            base_url: pingap.url.clone(),
        };
        Ok(Env {
            client,
            kubectl,
            controller_deployment: state.get("E2E_CONTROLLER_DEPLOYMENT")?.to_string(),
            pingap_deployment: state.get("E2E_PINGAP_DEPLOYMENT")?.to_string(),
            etcd_statefulset: state.get("E2E_ETCD_STATEFULSET")?.to_string(),
            echo_deployment: state.get("E2E_ECHO_DEPLOYMENT")?.to_string(),
            upstream: state.get("E2E_UPSTREAM")?.to_string(),
            ws_upstream: state.get("E2E_WS_UPSTREAM")?.to_string(),
            namespace,
            app_namespace,
            etcd,
            pingap,
            load,
            state,
        })
    }
}

/// Polls `check` every `every` until it returns `Ok(Some(v))` or `timeout` passes. Errors
/// from `check` are retried; the last one is reported on timeout.
pub async fn wait_for<T, F, Fut>(
    what: &str,
    timeout: Duration,
    every: Duration,
    mut check: F,
) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<Option<T>>>,
{
    let deadline = Instant::now() + timeout;
    let mut last = String::from("condition not met");
    loop {
        match check().await {
            Ok(Some(v)) => return Ok(v),
            Ok(None) => {}
            Err(e) => last = format!("{e:#}"),
        }
        if Instant::now() >= deadline {
            anyhow::bail!("timed out after {timeout:?} waiting for {what}: {last}");
        }
        tokio::time::sleep(every).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_state() {
        let s = State::parse("# c\nA=1\n\nB = x=y\nbad line\n");
        assert_eq!(s.get("A").unwrap(), "1");
        assert_eq!(s.get("B").unwrap(), "x=y");
        assert!(s.get("C").is_err());
    }
}

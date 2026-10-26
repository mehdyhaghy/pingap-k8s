//! In-cluster load generator: `fortio load` run with `kubectl exec` in the e2e-load pod
//! (fortio/fortio, pinned in tests/e2e/manifests/app.yaml), targeting Pingap's Service.
//! fortio prints its JSON result on stdout (`-json -`), logs go to stderr.
//!
//! ```ignore
//! let run = env.load.start(&LoadOptions::new(200, Duration::from_secs(60)))?;
//! // ... rollout restart, waits ...
//! let report = run.wait().await?;
//! assert_eq!(report.server_errors(), 0, "{report}");
//! ```

use std::collections::BTreeMap;
use std::fmt;
use std::process::Child;
use std::time::Duration;

use k8s_openapi::serde_json::{self, Value};

use crate::kubectl::{Kubectl, collect};

#[derive(Debug, Clone)]
pub struct Load {
    pub kubectl: Kubectl,
    pub namespace: String,
    pub pod: String,
    /// Pingap's in-cluster URL; [`LoadOptions::path`] is appended.
    pub base_url: String,
}

#[derive(Debug, Clone)]
pub struct LoadOptions {
    /// Target requests per second (all connections together).
    pub qps: u32,
    pub duration: Duration,
    /// Concurrent connections (fortio -c).
    pub connections: u32,
    pub path: String,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Extra fortio flags.
    pub extra_args: Vec<String>,
}

impl LoadOptions {
    pub fn new(qps: u32, duration: Duration) -> Self {
        LoadOptions {
            qps,
            duration,
            connections: 8,
            path: "/hostname".into(),
            timeout: Duration::from_secs(5),
            extra_args: Vec::new(),
        }
    }
}

/// A running load test.
pub struct LoadRun {
    child: Option<Child>,
}

/// fortio's result, reduced to what the tests check. `codes` maps HTTP status to count;
/// fortio reports socket-level failures (connect error, timeout) as code -1.
#[derive(Debug, Clone)]
pub struct LoadReport {
    pub codes: BTreeMap<i64, u64>,
    pub actual_qps: f64,
    pub actual_duration: Duration,
    /// The full fortio JSON, for anything else.
    pub raw: Value,
}

impl LoadReport {
    pub fn total(&self) -> u64 {
        self.codes.values().sum()
    }

    pub fn count(&self, code: i64) -> u64 {
        self.codes.get(&code).copied().unwrap_or(0)
    }

    /// 5xx responses plus socket errors (-1): what "zero 5xx" in e2e test 3 must count.
    pub fn server_errors(&self) -> u64 {
        self.codes
            .iter()
            .filter(|(c, _)| **c < 0 || **c >= 500)
            .map(|(_, n)| n)
            .sum()
    }

    pub fn parse(stdout: &str) -> anyhow::Result<LoadReport> {
        // Only the JSON document: skip anything before the first '{'.
        let start = stdout
            .find('{')
            .ok_or_else(|| anyhow::anyhow!("no JSON in fortio output: {stdout:?}"))?;
        let raw: Value = serde_json::from_str(stdout[start..].trim())?;
        let mut codes = BTreeMap::new();
        if let Some(m) = raw.get("RetCodes").and_then(Value::as_object) {
            for (k, v) in m {
                codes.insert(k.parse::<i64>()?, v.as_u64().unwrap_or(0));
            }
        }
        let actual_qps = raw.get("ActualQPS").and_then(Value::as_f64).unwrap_or(0.0);
        let nanos = raw
            .get("ActualDuration")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        Ok(LoadReport {
            codes,
            actual_qps,
            actual_duration: Duration::from_nanos(nanos),
            raw,
        })
    }
}

impl fmt::Display for LoadReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} requests in {:.1?} ({:.1} qps), codes {:?}",
            self.total(),
            self.actual_duration,
            self.actual_qps,
            self.codes
        )
    }
}

impl Load {
    fn args(&self, o: &LoadOptions) -> Vec<String> {
        let mut a = vec![
            "fortio".to_string(),
            "load".into(),
            "-quiet".into(),
            "-allow-initial-errors".into(),
            "-qps".into(),
            o.qps.to_string(),
            "-c".into(),
            o.connections.to_string(),
            "-t".into(),
            format!("{}ms", o.duration.as_millis()),
            "-timeout".into(),
            format!("{}ms", o.timeout.as_millis()),
            "-json".into(),
            "-".into(),
        ];
        a.extend(o.extra_args.iter().cloned());
        a.push(format!("{}{}", self.base_url, o.path));
        a
    }

    /// Starts a load run in the background.
    pub fn start(&self, o: &LoadOptions) -> anyhow::Result<LoadRun> {
        let child = self
            .kubectl
            .spawn_exec(&self.namespace, &self.pod, None, &self.args(o))?;
        Ok(LoadRun { child: Some(child) })
    }

    /// Runs a load test to completion.
    pub async fn run(&self, o: &LoadOptions) -> anyhow::Result<LoadReport> {
        self.start(o)?.wait().await
    }
}

impl LoadRun {
    /// Waits for fortio to finish and parses its result.
    pub async fn wait(mut self) -> anyhow::Result<LoadReport> {
        let child = self.child.take().expect("load run already collected");
        let out = collect(child).await?;
        if !out.success {
            anyhow::bail!("fortio load failed ({:?}): {}", out.code, out.stderr.trim());
        }
        LoadReport::parse(&out.stdout)
    }
}

impl Drop for LoadRun {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fortio_json() {
        let r = LoadReport::parse(
            r#"{"RunType":"HTTP","ActualQPS":199.5,"ActualDuration":60000000000,"RetCodes":{"200":11960,"503":2,"-1":1}}"#,
        )
        .unwrap();
        assert_eq!(r.total(), 11963);
        assert_eq!(r.count(200), 11960);
        assert_eq!(r.server_errors(), 3);
        assert_eq!(r.actual_duration, Duration::from_secs(60));
    }
}

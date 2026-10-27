//! The `kubectl-pingap` binary end to end, with a fake `kubectl` (a shell script, via
//! `KUBECTL`) and the controller's real `/status` server on 127.0.0.1: pod lookup,
//! port-forward parsing, the HTTP fetch, rendering, and the error paths.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt as _;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use pingap_k8s::etcd::parse_etcd_url;
use pingap_k8s::server::Readiness;
use pingap_k8s::status::{StatusBoard, StatusSources, serve_listener};

/// Answers `get pods`, `get services` and `port-forward` from files in its directory,
/// and appends every argument list to `args.log`.
const FAKE_KUBECTL: &str = r#"#!/bin/sh
dir=$(dirname "$0")
echo "$*" >>"$dir/args.log"
case "$*" in
  *"get pods"*) cat "$dir/pods.json" ;;
  *"get services"*) cat "$dir/services.json" ;;
  *port-forward*)
    echo "Forwarding from 127.0.0.1:$(cat "$dir/forward-port") -> 9091"
    exec sleep 30 ;;
  *) echo "unexpected: $*" >&2; exit 1 ;;
esac
"#;

struct Fake {
    dir: PathBuf,
}

impl Fake {
    fn new(test: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("pingap-k8s-plugin-{test}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        let script = dir.join("kubectl");
        std::fs::write(&script, FAKE_KUBECTL).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        Fake { dir }
    }

    fn write(&self, name: &str, content: &str) {
        std::fs::write(self.dir.join(name), content).unwrap();
    }

    fn args_log(&self) -> String {
        std::fs::read_to_string(self.dir.join("args.log")).unwrap_or_default()
    }

    /// Runs the plugin with `args`; returns (success, stdout, stderr).
    async fn run(&self, args: &[&str]) -> (bool, String, String) {
        let kubectl = self.dir.join("kubectl");
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let out = tokio::task::spawn_blocking(move || {
            Command::new(env!("CARGO_BIN_EXE_kubectl-pingap"))
                .args(&args)
                .env("KUBECTL", &kubectl)
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        (
            out.status.success(),
            String::from_utf8_lossy(&out.stdout).into_owned(),
            String::from_utf8_lossy(&out.stderr).into_owned(),
        )
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn pod(name: &str, phase: &str, status_port: Option<u16>) -> serde_json::Value {
    let annotations = match status_port {
        Some(p) => serde_json::json!({ "pingap-k8s/status-port": p.to_string() }),
        None => serde_json::json!({}),
    };
    serde_json::json!({
        "metadata": { "name": name, "namespace": "ops", "annotations": annotations },
        "status": { "phase": phase }
    })
}

fn pods(items: &[serde_json::Value]) -> String {
    serde_json::json!({ "items": items }).to_string()
}

/// A leader's `/status` server on 127.0.0.1 with one upstream; returns its port.
async fn status_server() -> u16 {
    let target = parse_etcd_url("etcd://etcd:2379/pingap").unwrap();
    let board = StatusBoard::new();
    let readiness = Readiness::new();
    readiness.set_watchers_synced(true);
    board.set_running(true);
    board.update_upstream("api", |u| {
        u.target = "pods".into();
        u.services = vec!["api.prod:http".into()];
        u.desired = vec!["10.0.0.1:8080".into(), "10.0.0.2:8080".into()];
        u.ready = 2;
    });
    board.record_write(
        "api",
        "ok",
        &["10.0.0.1:8080".to_string(), "10.0.0.2:8080".to_string()],
    );
    board.update_upstream("web", |u| {
        u.target = "pods".into();
        u.services = vec!["web.prod:http".into()];
        u.no_endpoints = true;
    });
    let sources = StatusSources::new("pingap-k8s-0".into(), &target, board, readiness);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(serve_listener(listener, sources, std::future::pending()));
    port
}

fn assert_contains(haystack: &str, needle: &str) {
    assert!(haystack.contains(needle), "{needle:?} not in:\n{haystack}");
}

#[tokio::test(flavor = "multi_thread")]
async fn status_and_explain_through_a_port_forward() {
    let fake = Fake::new("status");
    let port = status_server().await;
    fake.write("forward-port", &port.to_string());
    fake.write(
        "pods.json",
        &pods(&[
            pod("pingap-k8s-old", "Succeeded", Some(9091)),
            pod("pingap-k8s-0", "Running", Some(9091)),
        ]),
    );

    let (ok, out, err) = fake.run(&["status", "-n", "ops"]).await;
    assert!(ok, "stdout:\n{out}\nstderr:\n{err}");
    assert_contains(&out, "pingap-k8s-0");
    assert_contains(&out, "api");
    assert_contains(&out, "web");
    assert_contains(&out, "no endpoints");

    let (ok, out, err) = fake.run(&["explain", "web", "-n", "ops"]).await;
    assert!(ok, "stdout:\n{out}\nstderr:\n{err}");
    assert_contains(&out, "web");

    let (ok, _, err) = fake.run(&["explain", "nope", "-n", "ops"]).await;
    assert!(!ok);
    assert_contains(&err, "nope");

    // Only the running pod is forwarded to, on loopback, in the pod's namespace.
    let log = fake.args_log();
    assert_contains(
        &log,
        "--namespace ops get pods -l app.kubernetes.io/name=pingap-k8s",
    );
    assert_contains(
        &log,
        "--namespace ops port-forward --address 127.0.0.1 pod/pingap-k8s-0 :9091",
    );
    assert!(!log.contains("pod/pingap-k8s-old"), "{log}");
}

#[tokio::test(flavor = "multi_thread")]
async fn status_errors_name_the_cause() {
    let fake = Fake::new("errors");
    fake.write("forward-port", "1");

    fake.write("pods.json", &pods(&[]));
    let (ok, _, err) = fake.run(&["status", "-n", "ops"]).await;
    assert!(!ok);
    assert_contains(&err, "no running controller pods");

    // Status endpoint disabled: no annotation and no --port.
    fake.write("pods.json", &pods(&[pod("pingap-k8s-0", "Running", None)]));
    let (ok, _, err) = fake.run(&["status", "-n", "ops"]).await;
    assert!(!ok);
    assert_contains(&err, "status endpoint is disabled");
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_refuses_ambiguous_services() {
    let fake = Fake::new("admin");
    let svc = |ns: &str, name: &str| {
        serde_json::json!({
            "metadata": { "name": name, "namespace": ns },
            "spec": { "ports": [{ "name": "admin", "port": 3018 }] }
        })
    };
    fake.write(
        "services.json",
        &serde_json::json!({ "items": [svc("a", "rel-pingap-admin"), svc("b", "other-admin")] })
            .to_string(),
    );
    let (ok, _, err) = fake.run(&["admin"]).await;
    assert!(!ok);
    assert_contains(&err, "more than one");
    let log = fake.args_log();
    assert_contains(
        &log,
        "app.kubernetes.io/part-of=pingap-k8s,app.kubernetes.io/name=pingap,app.kubernetes.io/component=proxy",
    );
    assert!(!log.contains("port-forward"), "{log}");
}

//! End-to-end tests on kind.
//!
//! Gated on `PINGAP_K8S_E2E=1`; otherwise every test prints a skip line and passes, so a
//! plain `cargo test` stays offline. The environment comes from `scripts/e2e-up.sh`
//! (state in `target/e2e/state.env`); `scripts/e2e-run.sh` does up, test, down:
//!
//! ```text
//! scripts/e2e-run.sh                      # up, cargo test --test e2e, down
//! E2E_KEEP=1 scripts/e2e-run.sh           # keep the cluster afterwards
//! scripts/e2e-up.sh && PINGAP_K8S_E2E=1 cargo test --test e2e -- --test-threads=1
//! ```
//!
//! Tests share one cluster and must run with `--test-threads=1`.
//!
//! Layout of the environment (names in the state file and in [`harness::Env`]):
//! - namespace `pingap-k8s`: chart release `pingap-k8s` (controller Deployment `pingap-k8s`,
//!   etcd StatefulSet `pingap-k8s-etcd`, Pingap Deployment/Service `pingap-k8s-pingap`,
//!   admin Service `pingap-k8s-pingap-admin`), all ClusterIP;
//! - namespace `e2e-app`: Deployment/Service `echo` (3 replicas; port "http" = agnhost
//!   netexec, port "ws" = WebSocket echo), `podinfo` (3 replicas, port "grpc", h2c) and
//!   `httpbin` (2 replicas, port "http", go-httpbin), client pods `e2e-client` (curl) and
//!   `e2e-load` (fortio);
//! - Pingap: listener 8080, location `/ws` -> upstream `echo-ws`, `/sse` -> upstream
//!   `httpbin`, everything else -> upstream `echo`; listener 8081 (h2c, reached by pod IP)
//!   -> upstream `grpc`. The controller owns all four (`echo.e2e-app:http`,
//!   `echo.e2e-app:ws`, `httpbin.e2e-app:http`, `podinfo.e2e-app:grpc`).
//!
//! Helpers: [`k8s`] (scale, rollout, waits, pods, Role), [`etcd`] (etcdctl in the etcd pod),
//! [`pingap`] (curl from the client pod, admin API), [`load`] (fortio), [`ws`] (WebSocket
//! through a port-forward to Pingap), [`harness::wait_for`] (polling), [`controller`]
//! (per-pod metrics, leader, logs, config), [`track`] (timestamped etcd watch, request
//! loop and pod tracker for the timing tests).
//!
//! The end-to-end tests 1–14 are in [`spec`] (`spec::e2e_01_initial_sync` ...
//! `spec::e2e_14_events`); filter with e.g. `scripts/e2e-run.sh e2e_04`. The tests 19–27 of
//! the Gateway API provider are in [`gateway`] (`gateway::e2e_19_gateway_class_status` ...).

// Helpers are shared with the tests in [`spec`]; not every one is used by every test.
#![allow(dead_code)]

mod controller;
mod etcd;
mod gateway;
mod harness;
mod k8s;
mod kubectl;
mod load;
mod pingap;
mod spec;
mod track;
mod ws;

use std::time::Duration;

use harness::{Env, wait_for};

/// The environment is up: kube API, etcd, Pingap and the echo app are ready, etcd is
/// readable with the seeded config, and Pingap answers HTTP through its Service. Does not
/// need a working controller.
#[tokio::test]
async fn e2e_smoke() {
    let Some(env) = Env::setup("e2e_smoke").await else {
        return;
    };
    let t = Duration::from_secs(120);

    k8s::wait_statefulset_ready(&env.client, &env.namespace, &env.etcd_statefulset, t)
        .await
        .expect("etcd ready");
    k8s::wait_deployment_rolled_out(&env.client, &env.namespace, &env.pingap_deployment, t)
        .await
        .expect("Pingap ready");
    k8s::wait_deployment_rolled_out(&env.client, &env.app_namespace, &env.echo_deployment, t)
        .await
        .expect("echo ready");
    let echo_ips = k8s::ready_pod_ips(&env.client, &env.app_namespace, k8s::ECHO_SELECTOR)
        .await
        .unwrap();
    assert_eq!(echo_ips.len(), 3, "echo pods: {echo_ips:?}");

    // etcd: healthy, seeded by the chart's init container.
    env.etcd.health().await.expect("etcd health");
    let keys: Vec<String> = env
        .etcd
        .get_prefix(&format!("{}/", env.etcd.prefix()))
        .await
        .expect("etcd get --prefix")
        .into_iter()
        .map(|kv| kv.key)
        .collect();
    for want in [
        format!("{}/servers/e2e.toml", env.etcd.prefix()),
        env.etcd.upstream_key(&env.upstream),
        env.etcd.upstream_key(&env.ws_upstream),
    ] {
        assert!(keys.contains(&want), "{want} missing from {keys:?}");
    }
    let addrs = env
        .etcd
        .upstream_addrs(&env.upstream)
        .await
        .unwrap()
        .expect("upstream has addrs");
    assert!(!addrs.is_empty());
    println!("upstream {} addrs: {addrs:?}", env.upstream);
    assert!(env.etcd.revision().await.unwrap() > 0);

    // The client pod reaches an echo pod directly.
    let direct = env
        .pingap
        .get_url(&format!("http://{}:8080/hostname", echo_ips[0]))
        .await
        .unwrap();
    assert_eq!(direct.status, 200, "{direct:?}");

    // Pingap answers through its Service. Without a working controller the upstream still
    // holds the seed placeholder, so 5xx is fine here; only "no response" is a failure.
    let r = wait_for(
        "an HTTP response from Pingap",
        t,
        Duration::from_secs(1),
        || async {
            let r = env.pingap.get("/hostname", &[]).await?;
            Ok((r.status != 0).then_some(r))
        },
    )
    .await
    .unwrap();
    let body: String = r.body.trim().chars().take(60).collect();
    println!("Pingap GET /hostname: {} {body:?}", r.status);
}

/// Self-check of the request, load, WebSocket and etcd watch helpers, independent of the
/// controller: scales the controller to 0, writes the echo pod IPs into both upstreams
/// itself, exercises every helper, then restores the placeholder and the controller.
/// Ignored by default (it writes to Pingap's config); run it with
/// `PINGAP_K8S_E2E=1 cargo test --test e2e -- --ignored --test-threads=1 e2e_harness_selfcheck`.
#[tokio::test]
#[ignore]
async fn e2e_harness_selfcheck() {
    let Some(env) = Env::setup("e2e_harness_selfcheck").await else {
        return;
    };
    let t = Duration::from_secs(60);
    let release = env.state.get("E2E_RELEASE").unwrap().to_string();

    let controller_replicas: i32 = env
        .state
        .get("E2E_CONTROLLER_REPLICAS")
        .unwrap()
        .parse()
        .unwrap();
    k8s::scale_deployment(&env.client, &env.namespace, &env.controller_deployment, 0)
        .await
        .unwrap();
    k8s::wait_no_pods(
        &env.client,
        &env.namespace,
        &k8s::controller_selector(&release),
        t,
    )
    .await
    .unwrap();

    let ips = k8s::ready_pod_ips(&env.client, &env.app_namespace, k8s::ECHO_SELECTOR)
        .await
        .unwrap();
    let pods: Vec<String> = k8s::pods(&env.client, &env.app_namespace, k8s::ECHO_SELECTOR)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect();

    // etcd watch + write: set both upstreams to the pod IPs.
    let rev = env.etcd.revision().await.unwrap();
    let watch = env
        .etcd
        .watch(&format!("{}/upstreams/", env.etcd.prefix()), Some(rev + 1))
        .unwrap();
    let mut originals = Vec::new();
    for (name, port) in [(&env.upstream, 8080), (&env.ws_upstream, 8090)] {
        let kv = env
            .etcd
            .upstream(name)
            .await
            .unwrap()
            .expect("upstream key");
        let addrs: Vec<String> = ips.iter().map(|ip| format!("{ip}:{port}")).collect();
        let doc = pingap_k8s::etcd::replace_addrs(&kv.value, name, &addrs)
            .unwrap()
            .unwrap_or(kv.value.clone());
        env.etcd.put(&kv.key, &doc).await.unwrap();
        assert_eq!(env.etcd.upstream_addrs(name).await.unwrap().unwrap(), {
            let mut a = addrs.clone();
            a.sort();
            a
        });
        originals.push(kv);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    let events = watch.stop().await.unwrap();
    println!("etcd watch saw {} event(s)", events.len());
    assert!(events.len() >= 2, "watch events: {events:?}");

    // Pingap picks the change up (autoreload) and spreads requests over all pods.
    wait_for(
        "Pingap serving echo",
        t,
        Duration::from_millis(500),
        || async { Ok(env.pingap.hostname().await.ok()) },
    )
    .await
    .unwrap();
    let sample = env.pingap.sample("/hostname", 60, 2).await.unwrap();
    println!("sample: {sample:?}");
    assert_eq!(sample.count(200), 60, "{sample:?}");
    for p in &pods {
        assert!(
            sample.by_host.contains_key(p),
            "{p} got no traffic: {sample:?}"
        );
    }

    // Load generator.
    let report = env
        .load
        .run(&load::LoadOptions::new(50, Duration::from_secs(5)))
        .await
        .unwrap();
    println!("load: {report}");
    assert!(report.total() >= 200, "{report}");
    assert_eq!(report.server_errors(), 0, "{report}");

    // WebSocket through Pingap.
    let pf = ws::PortForward::start(
        &env.kubectl,
        &env.pingap.namespace,
        &format!("svc/{}", env.pingap.service),
        80,
    )
    .unwrap();
    let mut wsc =
        ws::WsClient::connect(&pf.addr(), &env.pingap.ws_path, Duration::from_secs(5)).unwrap();
    println!("websocket served by {}", wsc.served_by);
    assert!(
        pods.contains(&wsc.served_by),
        "{} not an echo pod",
        wsc.served_by
    );
    wsc.echo("hello", Duration::from_secs(5)).unwrap();
    let err = wsc.recv(Duration::from_millis(300)).unwrap_err();
    assert!(ws::is_timeout(&err), "{err:#}");
    wsc.echo("still there", Duration::from_secs(5)).unwrap();
    wsc.close();
    drop(pf);

    // Admin API answers (any HTTP status: routes depend on the Pingap version).
    let admin = env.pingap.admin("GET", "/", None).await.unwrap();
    println!("admin GET /: {}", admin.status);
    assert_ne!(admin.status, 0);

    // Restore the seed placeholder and the controller.
    for kv in originals {
        env.etcd.put(&kv.key, &kv.value).await.unwrap();
    }
    k8s::scale_deployment(
        &env.client,
        &env.namespace,
        &env.controller_deployment,
        controller_replicas,
    )
    .await
    .unwrap();
}

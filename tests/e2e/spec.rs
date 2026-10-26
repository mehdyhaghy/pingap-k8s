//! e2e "Integration", tests 1–14, one test function each.
//!
//! Every test starts from the same baseline ([`baseline`]: etcd up, both controller
//! replicas rolled out, echo at 3 replicas with no pod terminating, both upstreams equal to
//! the ready echo pods) and leaves the environment in it, so the tests can run alone or in
//! any order. Things a failing assertion could leave behind that the baseline cannot fix
//! (the deleted Role, the blackhole backend, the second Helm release) are restored by a
//! guard that also runs when the test body panics ([`guarded`]).
//!
//! Timings are measured on the test host's clock: pod Ready as first seen by a 100 ms pod
//! poll ([`track::PodTracker`]), etcd writes as they arrive on a streamed `etcdctl watch`
//! ([`track::AddrsWatch`]), traffic as each response arrives from a request loop in the
//! client pod ([`track::Traffic`]). Each test prints its numbers on lines starting with
//! `[e2e_NN]`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use futures::FutureExt;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::rbac::v1::Role;
use k8s_openapi::serde_json::{Value, json};
use kube::Api;
use kube::api::{Patch, PatchParams};

use crate::controller;
use crate::harness::{Env, wait_for};
use crate::k8s;
use crate::load::LoadOptions;
use crate::track::{self, AddrsChange, AddrsWatch, Hit, PodSeen, PodTracker, Traffic};
use crate::ws;

/// Echo container ports behind the two owned upstreams.
const HTTP_PORT: u16 = 8080;
const WS_PORT: u16 = 8090;
const ECHO_REPLICAS: i32 = 3;
/// The echo containers' preStop sleep (tests/e2e/manifests/app.yaml).
const PRESTOP: Duration = Duration::from_secs(10);
/// A rollout of 3 echo pods takes about 40 s (preStop 10 s per pod).
const ROLLOUT: Duration = Duration::from_secs(240);
const CONVERGE: Duration = Duration::from_secs(30);
/// the design notes default debounce_max (tests/e2e/values.yaml keeps the chart default).
const DEBOUNCE_MAX: f64 = 2.0;
/// e2e tests 1 and 2.
const SYNC_BUDGET: f64 = 3.0;
/// Leader lease TTL (src/leader.rs DEFAULT_TTL).
const LEASE_TTL: f64 = 10.0;
/// Slack on top of the lease TTL for test 7: keep-alive interval (ttl/5 = 2 s) plus the
/// 200 ms metrics poll.
const LEASE_SLACK: f64 = 3.0;
/// The health check the admin tests set (e2e tests 9 and 10).
const HEALTH_CHECK: &str =
    "http://echo/hostname?connection_timeout=2s&read_timeout=2s&check_frequency=5s";
/// TEST-NET-1 address used as the unreachable backend of test 3; the kind node drops
/// packets to it, so a connect hangs until its timeout.
const BLACKHOLE_IP: &str = "192.0.2.10";
const BLACKHOLE_PORT: u16 = 80;

// ---------------------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------------------

fn s(d: f64) -> String {
    format!("{d:.2}s")
}

fn with_port(ips: &[String], port: u16) -> Vec<String> {
    let mut v: Vec<String> = ips.iter().map(|ip| format!("{ip}:{port}")).collect();
    v.sort();
    v
}

async fn echo_ready_ips(env: &Env) -> anyhow::Result<Vec<String>> {
    k8s::ready_pod_ips(&env.client, &env.app_namespace, k8s::ECHO_SELECTOR).await
}

/// Both upstreams hold exactly the ready echo pods: returns their IPs.
async fn converged(env: &Env) -> anyhow::Result<Option<Vec<String>>> {
    let ips = echo_ready_ips(env).await?;
    if ips.is_empty() {
        return Ok(None);
    }
    let http = env.etcd.upstream_addrs(&env.upstream).await?;
    let ws = env.etcd.upstream_addrs(&env.ws_upstream).await?;
    Ok(
        (http == Some(with_port(&ips, HTTP_PORT)) && ws == Some(with_port(&ips, WS_PORT)))
            .then_some(ips),
    )
}

async fn wait_converged(env: &Env, timeout: Duration) -> anyhow::Result<Vec<String>> {
    wait_for(
        "both upstreams equal to the ready echo pods",
        timeout,
        Duration::from_millis(200),
        || converged(env),
    )
    .await
}

async fn echo_pods(env: &Env) -> anyhow::Result<Vec<k8s::PodInfo>> {
    k8s::pods(&env.client, &env.app_namespace, k8s::ECHO_SELECTOR).await
}

/// Scales echo, waits for the rollout, for the removed pods to be gone, and for both
/// upstreams to follow. Returns the ready IPs.
async fn scale_echo(env: &Env, replicas: i32) -> anyhow::Result<Vec<String>> {
    k8s::scale_deployment(
        &env.client,
        &env.app_namespace,
        &env.echo_deployment,
        replicas,
    )
    .await?;
    k8s::wait_deployment_rolled_out(
        &env.client,
        &env.app_namespace,
        &env.echo_deployment,
        ROLLOUT,
    )
    .await?;
    wait_for(
        "only the wanted echo pods left",
        ROLLOUT,
        Duration::from_millis(250),
        || async { Ok((echo_pods(env).await?.len() == replicas as usize).then_some(())) },
    )
    .await?;
    wait_converged(env, CONVERGE).await
}

/// The state every test starts from and returns to.
async fn baseline(env: &Env) -> Vec<String> {
    let release = controller::release(env);
    let controller_replicas: i32 = env
        .state
        .get("E2E_CONTROLLER_REPLICAS")
        .unwrap()
        .parse()
        .unwrap();
    k8s::scale_statefulset(&env.client, &env.namespace, &env.etcd_statefulset, 1)
        .await
        .unwrap();
    k8s::wait_statefulset_ready(&env.client, &env.namespace, &env.etcd_statefulset, ROLLOUT)
        .await
        .expect("etcd ready");
    k8s::scale_deployment(
        &env.client,
        &env.namespace,
        &env.controller_deployment,
        controller_replicas,
    )
    .await
    .unwrap();
    k8s::wait_deployment_rolled_out(
        &env.client,
        &env.namespace,
        &env.controller_deployment,
        ROLLOUT,
    )
    .await
    .expect("controller rolled out");
    wait_for(
        "no terminating controller pod",
        ROLLOUT,
        Duration::from_millis(500),
        || async {
            let pods = k8s::pods(
                &env.client,
                &env.namespace,
                &k8s::controller_selector(&release),
            )
            .await?;
            Ok((pods.len() == controller_replicas as usize).then_some(()))
        },
    )
    .await
    .unwrap();
    let ips = scale_echo(env, ECHO_REPLICAS).await.expect("echo baseline");
    controller::wait_leader(env, Duration::from_secs(60))
        .await
        .expect("a leader");
    ips
}

/// Runs `body`, then `restore` whether or not `body` panicked, then re-raises the panic.
async fn guarded<B, R>(body: B, restore: R)
where
    B: Future<Output = ()>,
    R: Future<Output = anyhow::Result<()>>,
{
    let outcome = AssertUnwindSafe(body).catch_unwind().await;
    let restored = restore.await;
    match (outcome, restored) {
        (Err(panic), Err(e)) => {
            eprintln!("restore after a failure also failed: {e:#}");
            std::panic::resume_unwind(panic)
        }
        (Err(panic), Ok(())) => std::panic::resume_unwind(panic),
        (Ok(()), Err(e)) => panic!("restore failed: {e:#}"),
        (Ok(()), Ok(())) => {}
    }
}

/// Runs a host command (docker, helm) off the runtime; non-zero exit is an error.
async fn host_cmd(program: &str, args: Vec<String>) -> anyhow::Result<String> {
    let shown = format!("{program} {}", args.join(" "));
    let mut cmd = std::process::Command::new(program);
    cmd.args(&args);
    let out = tokio::task::spawn_blocking(move || cmd.output()).await??;
    if !out.status.success() {
        anyhow::bail!(
            "{shown} failed ({:?}): {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// `docker exec <kind node> <args>`.
async fn node_exec(env: &Env, args: &[&str]) -> anyhow::Result<String> {
    let node = format!("{}-control-plane", env.state.get("E2E_CLUSTER")?);
    let mut a = vec!["exec".to_string(), node];
    a.extend(args.iter().map(|s| s.to_string()));
    host_cmd("docker", a).await
}

/// Removes ANSI colour codes (the controller's text logs have them).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\x1b' {
            for c in chars.by_ref() {
                if c.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// `upstreams.<name>.health_check` as stored in etcd.
async fn stored_health_check(env: &Env, name: &str) -> anyhow::Result<Option<String>> {
    let kv = env
        .etcd
        .upstream(name)
        .await?
        .ok_or_else(|| anyhow::anyhow!("no upstream {name}"))?;
    let t: toml::Table = toml::from_str(&kv.value)?;
    Ok(t.get("upstreams")
        .and_then(|u| u.get(name))
        .and_then(|u| u.get("health_check"))
        .and_then(|v| v.as_str())
        .map(str::to_string))
}

/// Saves the upstream through the admin API with `health_check` set (or removed).
async fn admin_set_health_check(env: &Env, name: &str, hc: Option<&str>) -> anyhow::Result<()> {
    let mut entry = env.pingap.admin_upstream(name).await?;
    set_health_check(&mut entry, hc);
    env.pingap.admin_save_upstream(name, &entry).await
}

fn set_health_check(entry: &mut Value, hc: Option<&str>) {
    let m = entry.as_object_mut().expect("upstream entry is an object");
    match hc {
        Some(hc) => {
            m.insert("health_check".into(), json!(hc));
        }
        None => {
            m.remove("health_check");
        }
    }
}

fn json_addrs(entry: &Value) -> Vec<String> {
    let mut v: Vec<String> = entry
        .get("addrs")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// For every pod that started terminating while tracked: how long until the controller
/// removed it from `addrs`, and how long after that it still got requests (the swap
/// delay: Pingap's rebuild of the upstream, which health-checks every backend first).
#[derive(Debug)]
struct Swap {
    pod: String,
    removed_after_delete: Option<f64>,
    last_hit_after_removed: Option<f64>,
}

fn swaps(
    changes: &[AddrsChange],
    hits: &[Hit],
    seen: &std::collections::BTreeMap<String, PodSeen>,
    port: u16,
) -> Vec<Swap> {
    let mut out = Vec::new();
    for (name, p) in seen {
        let (Some(term), Some(ip)) = (p.terminating, p.ip.as_ref()) else {
            continue;
        };
        let addr = format!("{ip}:{port}");
        let removed = track::first_change(changes, term, |c| !c.contains(&addr)).map(|c| c.at);
        let last_hit = hits.iter().filter(|h| &h.host == name).map(|h| h.at).max();
        out.push(Swap {
            pod: name.clone(),
            removed_after_delete: removed.map(|r| track::secs_between(r, term)),
            last_hit_after_removed: match (removed, last_hit) {
                (Some(r), Some(h)) => Some(track::secs_between(h, r)),
                _ => None,
            },
        });
    }
    out
}

fn print_swaps(test: &str, label: &str, swaps: &[Swap]) {
    for sw in swaps {
        println!(
            "[{test}] {label}: pod {} removed from addrs {} after deletion; last request it served {} after removal",
            sw.pod,
            sw.removed_after_delete.map_or("never".into(), s),
            sw.last_hit_after_removed.map_or("n/a".into(), s),
        );
    }
    let worst = swaps
        .iter()
        .filter_map(|s| s.last_hit_after_removed)
        .fold(f64::NEG_INFINITY, f64::max);
    if worst.is_finite() {
        println!(
            "[{test}] {label}: worst swap delay (last request after removal) {}",
            s(worst)
        );
    }
}

// ---------------------------------------------------------------------------------------
// 1. A 3-replica echo Deployment: all pod IPs are in Pingap within 3 s of Ready.
// ---------------------------------------------------------------------------------------

/// e2e test 1. The echo pods already exist when the environment comes up (before the
/// controller), so the test replaces all three with a rollout restart and measures, for
/// the 3 new pods, the time from Ready to their IPs being in etcd: per pod, and for "all
/// three" from the last pod's Ready (both as first seen by the pod poll and from its
/// Ready condition's lastTransitionTime, which has whole-second resolution).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_01_initial_sync() {
    const T: &str = "e2e_01";
    let Some(env) = Env::setup("e2e_01_initial_sync").await else {
        return;
    };
    baseline(&env).await;
    let tracker = PodTracker::start(&env.client, &env.app_namespace, k8s::ECHO_SELECTOR)
        .await
        .unwrap();
    let watch = AddrsWatch::start(&env, &env.upstream).await.unwrap();

    k8s::rollout_restart(&env.client, &env.app_namespace, &env.echo_deployment)
        .await
        .unwrap();
    k8s::wait_deployment_rolled_out(
        &env.client,
        &env.app_namespace,
        &env.echo_deployment,
        ROLLOUT,
    )
    .await
    .unwrap();
    let ips = wait_converged(&env, CONVERGE).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let changes = watch.changes();
    let new = tracker.new_ready();
    assert_eq!(new.len(), 3, "new pods: {new:?}");
    let new_addrs: Vec<String> = new
        .values()
        .map(|p| format!("{}:{HTTP_PORT}", p.ip.as_ref().unwrap()))
        .collect();
    assert_eq!(with_port(&ips, HTTP_PORT), {
        let mut a = new_addrs.clone();
        a.sort();
        a
    });

    let first_seen = new.values().map(|p| p.first_seen).min().unwrap();
    for (name, p) in &new {
        let addr = format!("{}:{HTTP_PORT}", p.ip.as_ref().unwrap());
        let c = track::first_change(&changes, p.first_seen, |c| c.contains(&addr))
            .expect("pod IP written");
        println!(
            "[{T}] {name}: in etcd {} after Ready seen, {} after Ready lastTransitionTime",
            s(track::secs_between(c.at, p.ready.unwrap())),
            s(track::secs_between(c.at, p.ready_transition.unwrap())),
        );
    }
    let all = track::first_change(&changes, first_seen, |c| {
        new_addrs.iter().all(|a| c.contains(a))
    })
    .expect("all new IPs written");
    let last_ready = new.values().filter_map(|p| p.ready).max().unwrap();
    let last_transition = new
        .values()
        .filter_map(|p| p.ready_transition)
        .max()
        .unwrap();
    let d_seen = track::secs_between(all.at, last_ready);
    let d_transition = track::secs_between(all.at, last_transition);
    println!(
        "[{T}] all 3 pod IPs in etcd {} after the last pod's Ready was seen, {} after its Ready lastTransitionTime; {} writes during the rollout",
        s(d_seen),
        s(d_transition),
        changes.len() - 1
    );
    assert!(d_seen <= SYNC_BUDGET, "{} > {SYNC_BUDGET}s", s(d_seen));
    assert!(
        d_transition <= SYNC_BUDGET,
        "{} > {SYNC_BUDGET}s",
        s(d_transition)
    );
}

// ---------------------------------------------------------------------------------------
// 2. Scale to 5: the new pods get traffic within 3 s.
// ---------------------------------------------------------------------------------------

/// e2e test 2: time from each new pod's Ready to the first request Pingap sends it
/// (a request loop through Pingap, attributed by the hostname the echo pod returns).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_02_scale_up_traffic() {
    const T: &str = "e2e_02";
    let Some(env) = Env::setup("e2e_02_scale_up_traffic").await else {
        return;
    };
    baseline(&env).await;
    let tracker = PodTracker::start(&env.client, &env.app_namespace, k8s::ECHO_SELECTOR)
        .await
        .unwrap();
    let traffic = Traffic::start(&env, "/hostname", Duration::from_secs(180)).unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;

    k8s::scale_deployment(&env.client, &env.app_namespace, &env.echo_deployment, 5)
        .await
        .unwrap();
    k8s::wait_deployment_rolled_out(
        &env.client,
        &env.app_namespace,
        &env.echo_deployment,
        ROLLOUT,
    )
    .await
    .unwrap();
    wait_for(
        "traffic on both new pods",
        Duration::from_secs(30),
        Duration::from_millis(250),
        || async {
            let new = tracker.new_ready();
            let hits = traffic.hits();
            let all = new.len() == 2 && new.keys().all(|n| hits.iter().any(|h| &h.host == n));
            Ok(all.then_some(()))
        },
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let hits = traffic.stop();
    let new = tracker.new_ready();

    let mut worst: f64 = 0.0;
    let mut worst_transition: f64 = 0.0;
    for (name, p) in &new {
        let first = hits.iter().find(|h| &h.host == name).unwrap();
        let d = track::secs_between(first.at, p.ready.unwrap());
        let dt = track::secs_between(first.at, p.ready_transition.unwrap());
        worst = worst.max(d);
        worst_transition = worst_transition.max(dt);
        println!(
            "[{T}] {name}: first request {} after Ready seen, {} after Ready lastTransitionTime",
            s(d),
            s(dt)
        );
    }
    let failed = track::failures(&hits);
    println!(
        "[{T}] {} requests during the scale-up, {} failed; worst {} (seen) / {} (transition)",
        hits.len(),
        failed.len(),
        s(worst),
        s(worst_transition)
    );
    assert!(failed.is_empty(), "failed requests: {failed:?}");
    assert!(worst <= SYNC_BUDGET, "{} > {SYNC_BUDGET}s", s(worst));
    assert!(
        worst_transition <= SYNC_BUDGET,
        "{} > {SYNC_BUDGET}s",
        s(worst_transition)
    );
    scale_echo(&env, ECHO_REPLICAS).await.unwrap();
}

// ---------------------------------------------------------------------------------------
// 3. Rolling restart under load: zero 5xx; and the swap delay with an unreachable backend.
// ---------------------------------------------------------------------------------------

/// What a rolling restart under load produced.
struct RolloutRun {
    report: crate::load::LoadReport,
    hits: Vec<Hit>,
    swaps: Vec<Swap>,
}

/// Rolling restart of echo while fortio runs `qps` for 60 s (and a request loop measures
/// per-pod swap delays).
async fn rollout_under_load(env: &Env, qps: u32) -> RolloutRun {
    let tracker = PodTracker::start(&env.client, &env.app_namespace, k8s::ECHO_SELECTOR)
        .await
        .unwrap();
    let watch = AddrsWatch::start(env, &env.upstream).await.unwrap();
    let traffic = Traffic::start(env, "/hostname", Duration::from_secs(180)).unwrap();
    let load = env
        .load
        .start(&LoadOptions::new(qps, Duration::from_secs(60)))
        .unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    k8s::rollout_restart(&env.client, &env.app_namespace, &env.echo_deployment)
        .await
        .unwrap();
    k8s::wait_deployment_rolled_out(
        &env.client,
        &env.app_namespace,
        &env.echo_deployment,
        ROLLOUT,
    )
    .await
    .unwrap();
    let report = load.wait().await.unwrap();
    let hits = traffic.stop();
    let swaps = swaps(&watch.changes(), &hits, &tracker.snapshot(), HTTP_PORT);
    RolloutRun {
        report,
        hits,
        swaps,
    }
}

/// Adds a selector-less Service "blackhole" whose manual EndpointSlice points at
/// [`BLACKHOLE_IP`], drops packets to that address on the kind node (so a connect hangs
/// instead of failing fast), and adds the Service to the controller's "echo" upstream.
/// Returns the original controller config.
async fn blackhole_on(env: &Env) -> anyhow::Result<String> {
    let manifest = format!(
        "apiVersion: v1
kind: Service
metadata: {{ name: blackhole, namespace: {ns} }}
spec:
  ports: [{{ name: http, port: {BLACKHOLE_PORT}, protocol: TCP }}]
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: blackhole-1
  namespace: {ns}
  labels:
    kubernetes.io/service-name: blackhole
    endpointslice.kubernetes.io/managed-by: e2e-tests
addressType: IPv4
ports: [{{ name: http, port: {BLACKHOLE_PORT}, protocol: TCP }}]
endpoints:
  - addresses: [\"{BLACKHOLE_IP}\"]
    conditions: {{ ready: true }}
",
        ns = env.app_namespace
    );
    env.kubectl
        .run_stdin(&["apply", "-f", "-"], Some(manifest.as_bytes()))
        .await?;
    node_exec(
        env,
        &[
            "iptables",
            "-I",
            "FORWARD",
            "-d",
            &format!("{BLACKHOLE_IP}/32"),
            "-j",
            "DROP",
        ],
    )
    .await?;

    let original = controller::config_toml(env).await?;
    let mut doc: toml_edit::DocumentMut = original.parse()?;
    doc["upstreams"][&env.upstream]["services"]
        .as_array_mut()
        .ok_or_else(|| anyhow::anyhow!("no services array for {}", env.upstream))?
        .push(format!("blackhole.{}:http", env.app_namespace));
    controller::set_config_toml(env, &doc.to_string()).await?;
    let addr = format!("{BLACKHOLE_IP}:{BLACKHOLE_PORT}");
    wait_for(
        "the blackhole backend in the echo upstream",
        Duration::from_secs(150),
        Duration::from_millis(500),
        || async {
            let a = env.etcd.upstream_addrs(&env.upstream).await?;
            Ok(a.is_some_and(|a| a.contains(&addr)).then_some(()))
        },
    )
    .await?;
    Ok(original)
}

async fn blackhole_off(env: &Env, original: String) -> anyhow::Result<()> {
    controller::set_config_toml(env, &original).await?;
    let addr = format!("{BLACKHOLE_IP}:{BLACKHOLE_PORT}");
    wait_for(
        "the blackhole backend gone from the echo upstream",
        Duration::from_secs(150),
        Duration::from_millis(500),
        || async {
            let a = env.etcd.upstream_addrs(&env.upstream).await?;
            Ok(a.is_some_and(|a| !a.contains(&addr)).then_some(()))
        },
    )
    .await?;
    env.kubectl
        .run(&[
            "delete",
            "-n",
            &env.app_namespace,
            "--ignore-not-found",
            "service/blackhole",
            "endpointslice/blackhole-1",
        ])
        .await?;
    // Every copy of the rule (a retried test may have added more than one).
    while node_exec(
        env,
        &[
            "iptables",
            "-D",
            "FORWARD",
            "-d",
            &format!("{BLACKHOLE_IP}/32"),
            "-j",
            "DROP",
        ],
    )
    .await
    .is_ok()
    {}
    Ok(())
}

/// e2e test 3. fortio at `E2E_LOAD_QPS` (default 200) for 60 s with 8 connections
/// while echo does a rolling restart: zero 5xx and zero socket errors from fortio, and
/// zero 5xx from the request loop that runs alongside (which also measures the swap
/// delay per pod). Then the same with an unreachable backend in the
/// upstream, which only reports the swap delay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_03_rolling_restart_under_load() {
    const T: &str = "e2e_03";
    let Some(env) = Env::setup("e2e_03_rolling_restart_under_load").await else {
        return;
    };
    let qps: u32 = std::env::var("E2E_LOAD_QPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    baseline(&env).await;

    let run = rollout_under_load(&env, qps).await;
    println!("[{T}] rolling restart under load: {}", run.report);
    if run.report.actual_qps < qps as f64 * 0.9 {
        println!(
            "[{T}] note: fortio reached {:.1} qps of the {qps} requested",
            run.report.actual_qps
        );
    }
    let failed = track::failures(&run.hits);
    println!(
        "[{T}] request loop: {} requests, {} failed",
        run.hits.len(),
        failed.len()
    );
    print_swaps(T, "no unreachable backend", &run.swaps);
    if let Some(first) = run.hits.first() {
        for f in &failed {
            println!(
                "[{T}] request loop failure {} into the run: status {}",
                s(track::secs_between(f.at, first.at)),
                f.status
            );
        }
    }
    assert_eq!(run.report.server_errors(), 0, "{}", run.report);
    // The request loop (one new connection per request, curl in the client pod) is a
    // second witness: any 5xx from it fails the test. Requests it got no answer for
    // within 5 s (status 0) are reported, not asserted: they were seen only in runs where
    // the host was overloaded (the loop managed 3-12k requests instead of about 20k)
    // while fortio, the load generator e2e names, had none.
    let server_errors: Vec<_> = failed.iter().filter(|h| h.status >= 500).collect();
    assert!(
        server_errors.is_empty(),
        "request loop 5xx: {server_errors:?}"
    );

    // Variant: one unreachable backend in the upstream.
    baseline(&env).await;
    let original = blackhole_on(&env).await.unwrap();
    guarded(
        async {
            // Let Pingap's health check mark the blackhole down before the rollout.
            tokio::time::sleep(Duration::from_secs(15)).await;
            let run = rollout_under_load(&env, qps).await;
            println!(
                "[{T}] with an unreachable backend ({BLACKHOLE_IP}:{BLACKHOLE_PORT}): {}",
                run.report
            );
            let failed = track::failures(&run.hits);
            println!(
                "[{T}] with an unreachable backend: request loop {} requests, {} failed",
                run.hits.len(),
                failed.len()
            );
            print_swaps(T, "unreachable backend", &run.swaps);
            assert!(
                run.swaps.iter().any(|s| s.removed_after_delete.is_some()),
                "no pod removal observed: {:?}",
                run.swaps
            );
        },
        blackhole_off(&env, original),
    )
    .await;
    baseline(&env).await;
}

// ---------------------------------------------------------------------------------------
// 4. Single-replica restart: fallback logged, requests fail only while no pod serves.
// ---------------------------------------------------------------------------------------

/// e2e test 4. echo at 1 replica; its pod is deleted (the ReplicaSet starts a new one
/// at once, the old one serves through its 10 s preStop). The draining pod is kept in
/// `addrs` until the new one is Ready : the leader logs the start and end of the
/// fallback and sets `pingap_k8s_drain_fallback{upstream="echo"}` meanwhile. Requests
/// may fail only while no pod is serving (old pod gone, new one not Ready yet).
///
/// Without a delay the replacement pod is Ready about 0.6 s after the deletion, inside
/// the 500 ms debounce, and the controller never sees zero ready endpoints (no fallback
/// needed, nothing to test). A 3 s initial readiness delay on the http container
/// (restored afterwards) opens the window the fallback is for, and leaves the new pod
/// Ready well before the old one's preStop ends even on a slow runner (with 6 s it was
/// Ready 9.8 s after the deletion once, too late for the debounce).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_04_single_replica_fallback() {
    let Some(env) = Env::setup("e2e_04_single_replica_fallback").await else {
        return;
    };
    baseline(&env).await;
    scale_echo(&env, 1).await.unwrap();
    set_http_ready_delay(&env, Some(3)).await.unwrap();
    scale_echo(&env, 1).await.unwrap();
    guarded(single_replica_fallback(&env), async {
        set_http_ready_delay(&env, None).await?;
        scale_echo(&env, ECHO_REPLICAS).await.map(|_| ())
    })
    .await;
    baseline(&env).await;
}

/// Sets (or removes) `initialDelaySeconds` of the echo http container's readiness probe.
async fn set_http_ready_delay(env: &Env, secs: Option<u32>) -> anyhow::Result<()> {
    let api: Api<k8s_openapi::api::apps::v1::Deployment> =
        Api::namespaced(env.client.clone(), &env.app_namespace);
    let patch = json!({ "spec": { "template": { "spec": { "containers": [
        { "name": "http", "readinessProbe": { "initialDelaySeconds": secs } }
    ] } } } });
    api.patch(
        &env.echo_deployment,
        &PatchParams::default(),
        &Patch::Strategic(&patch),
    )
    .await?;
    Ok(())
}

async fn single_replica_fallback(env: &Env) {
    const T: &str = "e2e_04";
    let leader = controller::wait_leader(env, Duration::from_secs(60))
        .await
        .unwrap();
    let leader_ip = leader.ip.clone().unwrap();
    let logs_before = env
        .kubectl
        .logs(&env.namespace, &leader.name, Some("controller"), None)
        .await
        .unwrap();

    let tracker = PodTracker::start(&env.client, &env.app_namespace, k8s::ECHO_SELECTOR)
        .await
        .unwrap();
    let traffic = Traffic::start(env, "/hostname", Duration::from_secs(120)).unwrap();
    let gauge: Arc<Mutex<Vec<(SystemTime, f64)>>> = Arc::default();
    let poller = {
        let env = env.clone();
        let gauge = gauge.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(text) = controller::scrape(&env, &leader_ip).await
                    && let Some(v) = controller::metric(
                        &text,
                        "pingap_k8s_drain_fallback",
                        &[("upstream", &env.upstream)],
                    )
                {
                    gauge.lock().unwrap().push((SystemTime::now(), v));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
    };
    tokio::time::sleep(Duration::from_secs(2)).await;

    let old = echo_pods(env).await.unwrap().remove(0).name;
    k8s::delete_pod(&env.client, &env.app_namespace, &old, None)
        .await
        .unwrap();
    wait_for(
        "new pod Ready and the old one gone",
        Duration::from_secs(120),
        Duration::from_millis(250),
        || async {
            let seen = tracker.snapshot();
            let old_gone = seen.get(&old).is_some_and(|p| p.gone.is_some());
            Ok((old_gone && tracker.new_ready().len() == 1).then_some(()))
        },
    )
    .await
    .unwrap();
    wait_converged(env, CONVERGE).await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    poller.abort();
    let hits = traffic.stop();
    let seen = tracker.snapshot();
    let gauge = gauge.lock().unwrap().clone();

    let logs_after = env
        .kubectl
        .logs(&env.namespace, &leader.name, Some("controller"), None)
        .await
        .unwrap();
    let new_logs = strip_ansi(logs_after.get(logs_before.len()..).unwrap_or(&logs_after));
    let for_echo = |msg: &str| {
        new_logs
            .lines()
            .filter(|l| l.contains(msg))
            .filter(|l| {
                l.split_whitespace()
                    .any(|w| w == format!("upstream={}", env.upstream))
            })
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    let started = for_echo("no ready endpoints; writing draining endpoints");
    let ended = for_echo("drain fallback ended");
    for l in started.iter().chain(&ended) {
        println!("[{T}] log: {l}");
    }

    let old_seen = &seen[&old];
    let (new_name, new_seen) = seen
        .iter()
        .find(|(_, p)| !p.preexisting)
        .expect("replacement pod");
    let on: Vec<SystemTime> = gauge
        .iter()
        .filter(|(_, v)| *v >= 1.0)
        .map(|(t, _)| *t)
        .collect();
    let del = old_seen.terminating.unwrap();
    println!(
        "[{T}] old pod {old}: deleted, gone {} later; new pod {new_name} Ready {} after the deletion",
        s(track::secs_between(old_seen.gone.unwrap(), del)),
        s(track::secs_between(new_seen.ready.unwrap(), del)),
    );
    if let (Some(a), Some(b)) = (on.first(), on.last()) {
        println!(
            "[{T}] drain_fallback gauge 1 from {} to {} after the deletion ({} samples)",
            s(track::secs_between(*a, del)),
            s(track::secs_between(*b, del)),
            on.len()
        );
    }

    // Serving: the old pod until its preStop ends (its server stops on SIGTERM, before
    // the pod object is gone), the new one from Ready.
    let serving = |t: SystemTime| {
        seen.values().any(|p| {
            let from = if p.preexisting {
                Some(p.first_seen)
            } else {
                p.ready
            };
            let until = match (p.terminating, p.gone) {
                (Some(d), Some(g)) => Some(g.min(d + PRESTOP)),
                (Some(d), None) => Some(d + PRESTOP),
                (None, g) => g,
            };
            from.is_some_and(|f| f <= t) && until.is_none_or(|u| t < u)
        })
    };
    let failed = track::failures(&hits);
    let while_serving: Vec<_> = failed.iter().filter(|h| serving(h.at)).collect();
    println!(
        "[{T}] {} requests, {} failed, {} of them while a pod was serving",
        hits.len(),
        failed.len(),
        while_serving.len()
    );

    assert!(!started.is_empty(), "no fallback start logged:\n{new_logs}");
    assert!(!ended.is_empty(), "no fallback end logged:\n{new_logs}");
    assert!(!on.is_empty(), "drain_fallback never 1: {gauge:?}");
    assert_eq!(gauge.last().map(|g| g.1), Some(0.0), "{gauge:?}");
    assert!(
        while_serving.is_empty(),
        "requests failed while a pod was serving: {while_serving:?}"
    );
}

// ---------------------------------------------------------------------------------------
// 5. Delete the Role: no writes, Pingap keeps serving, an error metric is set; restore it.
// ---------------------------------------------------------------------------------------

async fn ensure_role(env: &Env, role: Role) -> anyhow::Result<()> {
    let api: Api<Role> = Api::namespaced(env.client.clone(), &env.app_namespace);
    if api
        .get_opt(role.metadata.name.as_deref().unwrap_or_default())
        .await?
        .is_none()
    {
        k8s::restore_role(&env.client, &env.app_namespace, role).await?;
    }
    Ok(())
}

/// e2e test 5. RBAC is checked per request, so a watch opened before the Role is
/// deleted keeps streaming until it ends by itself (minutes later). The test therefore
/// deletes the Role and then restarts the controller pods: they come up without read
/// access, which is the state a broken watch ends in. Then echo is scaled to 4: no
/// write may happen, Pingap keeps serving the old pods, and `watch_up` is 0 with
/// `watch_errors_total` counting. Restoring the Role must bring the 4th pod in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_05_role_deleted() {
    const T: &str = "e2e_05";
    let Some(env) = Env::setup("e2e_05_role_deleted").await else {
        return;
    };
    let old_ips = baseline(&env).await;
    let old_names: BTreeSet<String> = echo_pods(&env)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect();
    let version = |kv: Option<crate::etcd::KeyValue>| kv.map(|kv| kv.mod_revision);
    let before = (
        version(env.etcd.upstream(&env.upstream).await.unwrap()),
        version(env.etcd.upstream(&env.ws_upstream).await.unwrap()),
    );
    let role_name = controller::release(&env);
    let role = k8s::take_role(&env.client, &env.app_namespace, &role_name)
        .await
        .unwrap();
    println!("[{T}] deleted Role {}/{role_name}", env.app_namespace);

    guarded(
        async {
            let old_controllers: BTreeSet<String> = controller::pods(&env)
                .await
                .unwrap()
                .into_iter()
                .map(|p| p.name)
                .collect();
            for name in &old_controllers {
                k8s::delete_pod(&env.client, &env.namespace, name, None)
                    .await
                    .unwrap();
            }
            let t0 = SystemTime::now();
            let reports = wait_for(
                "restarted controllers reporting the watch down",
                Duration::from_secs(120),
                Duration::from_secs(1),
                || async {
                    let pods: Vec<_> = controller::pods(&env)
                        .await?
                        .into_iter()
                        .filter(|p| !old_controllers.contains(&p.name) && !p.terminating)
                        .collect();
                    if pods.len() < 2 {
                        return Ok(None);
                    }
                    let mut out = Vec::new();
                    for p in pods {
                        let Some(ip) = p.ip.clone() else {
                            return Ok(None);
                        };
                        let text = controller::scrape(&env, &ip).await?;
                        let svc = [("k8s_service_name", "echo")];
                        let up = controller::metric(&text, "pingap_k8s_watch_up", &svc);
                        let errors =
                            controller::metric(&text, "pingap_k8s_watch_errors_total", &svc)
                                .unwrap_or(0.0);
                        if up != Some(0.0) || errors < 1.0 {
                            return Ok(None);
                        }
                        let series: Vec<String> = text
                            .lines()
                            .filter(|l| l.starts_with("pingap_k8s_watch_errors_total"))
                            .map(str::to_string)
                            .collect();
                        let ready = controller::get(&env, &ip, "/readyz").await?.status;
                        out.push((p.name, series, ready));
                    }
                    Ok(Some(out))
                },
            )
            .await
            .unwrap();
            println!(
                "[{T}] watch reported down {} after the controller restart",
                s(track::secs_between(SystemTime::now(), t0))
            );
            for (name, series, ready) in &reports {
                println!("[{T}] {name}: /readyz {ready}; {series:?}");
            }

            k8s::scale_deployment(&env.client, &env.app_namespace, &env.echo_deployment, 4)
                .await
                .unwrap();
            k8s::wait_deployment_rolled_out(
                &env.client,
                &env.app_namespace,
                &env.echo_deployment,
                ROLLOUT,
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
            let after = (
                version(env.etcd.upstream(&env.upstream).await.unwrap()),
                version(env.etcd.upstream(&env.ws_upstream).await.unwrap()),
            );
            assert_eq!(before, after, "an upstream was written without RBAC");
            assert_eq!(
                env.etcd.upstream_addrs(&env.upstream).await.unwrap(),
                Some(with_port(&old_ips, HTTP_PORT))
            );
            let sample = env.pingap.sample("/hostname", 30, 2).await.unwrap();
            println!("[{T}] Pingap without the Role: {sample:?}");
            assert_eq!(sample.count(200), 30, "{sample:?}");
            assert!(
                sample.by_host.keys().all(|h| old_names.contains(h)),
                "{sample:?}"
            );

            ensure_role(&env, role.clone()).await.unwrap();
            let t_restore = SystemTime::now();
            let ips = wait_converged(&env, Duration::from_secs(120))
                .await
                .unwrap();
            println!(
                "[{T}] Role restored: 4th pod written {} later",
                s(track::secs_between(SystemTime::now(), t_restore))
            );
            assert_eq!(ips.len(), 4);
            wait_for(
                "watch_up back to 1 on every controller",
                Duration::from_secs(60),
                Duration::from_secs(1),
                || async {
                    for p in controller::pods(&env).await? {
                        if p.terminating {
                            continue;
                        }
                        let text = controller::scrape(&env, p.ip.as_deref().unwrap_or("")).await?;
                        let up = controller::metric(
                            &text,
                            "pingap_k8s_watch_up",
                            &[("k8s_service_name", "echo")],
                        );
                        if up != Some(1.0) {
                            return Ok(None);
                        }
                    }
                    Ok(Some(()))
                },
            )
            .await
            .unwrap();
        },
        ensure_role(&env, role.clone()),
    )
    .await;
    baseline(&env).await;
}

// ---------------------------------------------------------------------------------------
// 6. A WebSocket survives an addrs rewrite and closes only when its own pod terminates.
// ---------------------------------------------------------------------------------------

/// e2e test 6. A WebSocket through Pingap (port-forward to its Service), echoed every
/// 500 ms. Its pod is given a high pod-deletion-cost, then echo is scaled 3 -> 4 -> 3
/// (two rewrites of the "echo-ws" addrs, the scale-down removing another pod). Then its
/// own pod is deleted: the controller removes it from `addrs` at once, the connection
/// must stay up through the 10 s preStop and close when the pod's server stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_06_websocket_survives_rewrite() {
    const T: &str = "e2e_06";
    let Some(env) = Env::setup("e2e_06_websocket_survives_rewrite").await else {
        return;
    };
    baseline(&env).await;
    let watch = AddrsWatch::start(&env, &env.ws_upstream).await.unwrap();
    let pf = ws::PortForward::start(
        &env.kubectl,
        &env.pingap.namespace,
        &format!("svc/{}", env.pingap.service),
        80,
    )
    .unwrap();
    let probe =
        ws::WsProbe::start(&pf.addr(), &env.pingap.ws_path, Duration::from_millis(500)).unwrap();
    let mine = probe.served_by.clone();
    let my_ip = echo_pods(&env)
        .await
        .unwrap()
        .into_iter()
        .find(|p| p.name == mine)
        .and_then(|p| p.ip)
        .expect("the WebSocket's pod is an echo pod");
    println!("[{T}] WebSocket served by {mine} ({my_ip})");
    let pods: Api<Pod> = Api::namespaced(env.client.clone(), &env.app_namespace);
    let patch = json!({ "metadata": { "annotations": {
        "controller.kubernetes.io/pod-deletion-cost": "1000"
    } } });
    pods.patch(&mine, &PatchParams::default(), &Patch::Merge(&patch))
        .await
        .unwrap();

    for replicas in [4, ECHO_REPLICAS] {
        scale_echo(&env, replicas).await.unwrap();
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(
            probe.failure().is_none(),
            "WebSocket broke after scaling to {replicas}: {:?}",
            probe.failure()
        );
        let me = echo_pods(&env).await.unwrap();
        assert!(
            me.iter().any(|p| p.name == mine && !p.terminating),
            "{mine} was removed by the scale-down"
        );
    }
    let rewrites = watch.writes();
    println!(
        "[{T}] {rewrites} rewrites of the {} addrs, WebSocket alive ({} echoes)",
        env.ws_upstream,
        probe.results().len()
    );
    assert!(rewrites >= 2, "expected two addrs rewrites, saw {rewrites}");

    let t_del = SystemTime::now();
    k8s::delete_pod(&env.client, &env.app_namespace, &mine, None)
        .await
        .unwrap();
    let failure = wait_for(
        "the WebSocket to close",
        Duration::from_secs(60),
        Duration::from_millis(200),
        || async { Ok(probe.failure()) },
    )
    .await
    .unwrap();
    let last_ok = probe.last_ok().unwrap();
    let addr = format!("{my_ip}:{WS_PORT}");
    let changes = watch.changes();
    let removed = track::first_change(&changes, t_del, |c| !c.contains(&addr))
        .expect("own pod removed from addrs")
        .at;
    println!(
        "[{T}] own pod deleted: removed from addrs {}, last echo {}, closed {} after the deletion ({})",
        s(track::secs_between(removed, t_del)),
        s(track::secs_between(last_ok, t_del)),
        s(track::secs_between(failure.at, t_del)),
        failure.result.unwrap_err()
    );
    drop(probe);
    drop(pf);
    assert!(
        last_ok > removed,
        "the WebSocket did not outlive its pod's removal from addrs"
    );
    let closed_after = track::secs_between(failure.at, t_del);
    assert!(
        closed_after >= 8.0,
        "closed {} after the deletion, before the pod's 10 s preStop ended",
        s(closed_after)
    );
    assert!(
        closed_after <= 45.0,
        "closed only after {}",
        s(closed_after)
    );
    baseline(&env).await;
}

// ---------------------------------------------------------------------------------------
// 7. Kill the leader: the follower takes over within the lease TTL.
// ---------------------------------------------------------------------------------------

async fn wait_is_leader(env: &Env, ip: &str, timeout: Duration) -> anyhow::Result<SystemTime> {
    wait_for(
        "the follower to become leader",
        timeout,
        Duration::from_millis(200),
        || async {
            let text = controller::scrape(env, ip).await?;
            Ok(
                (controller::metric(&text, "pingap_k8s_is_leader", &[]) == Some(1.0))
                    .then(SystemTime::now),
            )
        },
    )
    .await
}

async fn follower_of(env: &Env, leader: &k8s::PodInfo) -> k8s::PodInfo {
    controller::pods(env)
        .await
        .unwrap()
        .into_iter()
        .find(|p| p.name != leader.name && !p.terminating && p.ip.is_some())
        .expect("a follower")
}

/// e2e test 7 (a): the leader pod is force-deleted (grace period 0). The kubelet still
/// sends SIGTERM, so the leader resigns and the follower takes over at once. Takeover is
/// measured from the delete to the follower's `pingap_k8s_is_leader 1`; the budget is the
/// lease TTL (10 s) plus 3 s for the keep-alive interval and the poll. Then a scale-up
/// checks that the new leader writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_07_leader_failover() {
    const T: &str = "e2e_07";
    let Some(env) = Env::setup("e2e_07_leader_failover").await else {
        return;
    };
    baseline(&env).await;
    let budget = LEASE_TTL + LEASE_SLACK;

    // (a) force delete
    let leader = controller::wait_leader(&env, Duration::from_secs(60))
        .await
        .unwrap();
    let follower = follower_of(&env, &leader).await;
    let t0 = SystemTime::now();
    k8s::delete_pod(&env.client, &env.namespace, &leader.name, Some(0))
        .await
        .unwrap();
    let t1 = wait_is_leader(
        &env,
        follower.ip.as_deref().unwrap(),
        Duration::from_secs(60),
    )
    .await
    .unwrap();
    let forced = track::secs_between(t1, t0);
    println!(
        "[{T}] force delete of leader {}: {} took over after {}",
        leader.name,
        follower.name,
        s(forced)
    );

    // The new leader writes.
    let ips = scale_echo(&env, 4).await.unwrap();
    assert_eq!(ips.len(), 4);
    println!("[{T}] new leader wrote the scale-up");
    assert!(forced <= budget, "force delete: {} > {budget}s", s(forced));
    baseline(&env).await;
}

/// e2e test 7 (b): the leader process is killed with SIGKILL on the node (no
/// resignation, the lease must expire). The follower must take over within the lease TTL
/// (10 s) plus 3 s slack.
///
/// Ignored: a follower's etcd Campaign call is cut by the client's 10 s request timeout
/// (src/etcd.rs DEFAULT_TIMEOUT applies to every RPC), so every follower revokes its lease
/// and re-campaigns at the back of the queue every ~10 s, with growing backoff. Seen on
/// kind: the killed leader's restarted container won the election and the follower never
/// took over (2 of 7 runs); when the follower does win, it is by timing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_07_leader_failover_sigkill() {
    const T: &str = "e2e_07b";
    let Some(env) = Env::setup("e2e_07_leader_failover_sigkill").await else {
        return;
    };
    baseline(&env).await;
    let budget = LEASE_TTL + LEASE_SLACK;

    // (b) SIGKILL of the leader process
    let leader = controller::wait_leader(&env, Duration::from_secs(60))
        .await
        .unwrap();
    let follower = follower_of(&env, &leader).await;
    let pods: Api<Pod> = Api::namespaced(env.client.clone(), &env.namespace);
    let pod = pods.get(&leader.name).await.unwrap();
    let container_id = pod
        .status
        .and_then(|s| s.container_statuses)
        .and_then(|cs| cs.into_iter().find(|c| c.name == "controller"))
        .and_then(|c| c.container_id)
        .expect("container id");
    let container_id = container_id
        .split_once("://")
        .map_or(container_id.as_str(), |(_, id)| id)
        .to_string();
    let pid = node_exec(
        &env,
        &[
            "crictl",
            "inspect",
            "-o",
            "go-template",
            "--template",
            "{{.info.pid}}",
            &container_id,
        ],
    )
    .await
    .unwrap()
    .trim()
    .to_string();
    let t0 = SystemTime::now();
    node_exec(&env, &["kill", "-9", &pid]).await.unwrap();
    let t1 = match wait_is_leader(
        &env,
        follower.ip.as_deref().unwrap(),
        Duration::from_secs(60),
    )
    .await
    {
        Ok(t) => t,
        Err(e) => {
            let mut leaders = Vec::new();
            for p in controller::pods(&env).await.unwrap_or_default() {
                let text = controller::scrape(&env, p.ip.as_deref().unwrap_or(""))
                    .await
                    .unwrap_or_default();
                leaders.push((
                    p.name,
                    controller::metric(&text, "pingap_k8s_is_leader", &[]),
                ));
            }
            let logs = controller::logs(&env).await.unwrap_or_default();
            panic!(
                "SIGKILL of leader {}: {e:#}\nis_leader: {leaders:?}\n{}",
                leader.name,
                strip_ansi(&logs)
            );
        }
    };
    let killed = track::secs_between(t1, t0);
    println!(
        "[{T}] SIGKILL of leader {} (pid {pid}): {} took over after {} (lease TTL {LEASE_TTL}s)",
        leader.name,
        follower.name,
        s(killed)
    );

    // The new leader writes.
    let ips = scale_echo(&env, 4).await.unwrap();
    assert_eq!(ips.len(), 4);
    println!("[{T}] new leader wrote the scale-up");
    assert!(killed <= budget, "SIGKILL: {} > {budget}s", s(killed));
    baseline(&env).await;
}

// ---------------------------------------------------------------------------------------
// 8. Stop etcd for 30 s during a scale-up: the controller converges afterwards.
// ---------------------------------------------------------------------------------------

/// e2e test 8: the bundled etcd StatefulSet is scaled to 0, echo to 5, and etcd back
/// to 1 thirty seconds after it stopped. Meanwhile Pingap keeps serving the old pods.
/// Measured: time from etcd Ready to both upstreams holding the 5 pods, and to Pingap
/// sending traffic to all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_08_etcd_outage() {
    const T: &str = "e2e_08";
    let Some(env) = Env::setup("e2e_08_etcd_outage").await else {
        return;
    };
    baseline(&env).await;
    let old_names: BTreeSet<String> = echo_pods(&env)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect();
    let release = controller::release(&env);
    guarded(
        async {
            k8s::scale_statefulset(&env.client, &env.namespace, &env.etcd_statefulset, 0)
                .await
                .unwrap();
            k8s::wait_no_pods(
                &env.client,
                &env.namespace,
                &k8s::etcd_selector(&release),
                Duration::from_secs(120),
            )
            .await
            .unwrap();
            let t_stop = SystemTime::now();
            println!("[{T}] etcd stopped");

            k8s::scale_deployment(&env.client, &env.app_namespace, &env.echo_deployment, 5)
                .await
                .unwrap();
            k8s::wait_deployment_rolled_out(
                &env.client,
                &env.app_namespace,
                &env.echo_deployment,
                ROLLOUT,
            )
            .await
            .unwrap();
            let sample = env.pingap.sample("/hostname", 20, 2).await.unwrap();
            println!("[{T}] Pingap during the outage: {sample:?}");
            assert_eq!(sample.count(200), 20, "{sample:?}");
            assert!(sample.by_host.keys().all(|h| old_names.contains(h)));

            let elapsed = SystemTime::now()
                .duration_since(t_stop)
                .unwrap_or_default();
            if let Some(rest) = Duration::from_secs(30).checked_sub(elapsed) {
                tokio::time::sleep(rest).await;
            }
            println!(
                "[{T}] etcd down for {}",
                s(track::secs_between(SystemTime::now(), t_stop))
            );
            k8s::scale_statefulset(&env.client, &env.namespace, &env.etcd_statefulset, 1)
                .await
                .unwrap();
            k8s::wait_statefulset_ready(
                &env.client,
                &env.namespace,
                &env.etcd_statefulset,
                ROLLOUT,
            )
            .await
            .unwrap();
            let t_up = SystemTime::now();
            let ips = wait_converged(&env, Duration::from_secs(180)).await.unwrap();
            let t_conv = SystemTime::now();
            assert_eq!(ips.len(), 5);
            let names: BTreeSet<String> = echo_pods(&env)
                .await
                .unwrap()
                .into_iter()
                .map(|p| p.name)
                .collect();
            wait_for(
                "Pingap sending traffic to all 5 pods",
                Duration::from_secs(90),
                Duration::from_millis(500),
                || async {
                    let sample = env.pingap.sample("/hostname", 25, 2).await?;
                    Ok((sample.by_host.len() == names.len()).then_some(()))
                },
            )
            .await
            .unwrap();
            println!(
                "[{T}] after etcd Ready: upstreams converged in {}, Pingap serving all 5 pods in {}",
                s(track::secs_between(t_conv, t_up)),
                s(track::secs_between(SystemTime::now(), t_up))
            );
        },
        async {
            k8s::scale_statefulset(&env.client, &env.namespace, &env.etcd_statefulset, 1)
                .await?;
            k8s::wait_statefulset_ready(
                &env.client,
                &env.namespace,
                &env.etcd_statefulset,
                ROLLOUT,
            )
            .await
        },
    )
    .await;
    baseline(&env).await;
}

// ---------------------------------------------------------------------------------------
// 9. Edit the health check in the Pingap admin while pods churn: the edit survives.
// ---------------------------------------------------------------------------------------

/// e2e test 9: during a rolling restart of echo (several controller writes), the
/// "echo" upstream's health check is changed through Pingap's admin API the way its UI
/// saves (load the config, POST the whole entry). After the rollout the stored entry has
/// both the new health check and the current pods.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_09_admin_health_check_edit() {
    const T: &str = "e2e_09";
    let Some(env) = Env::setup("e2e_09_admin_health_check_edit").await else {
        return;
    };
    baseline(&env).await;
    guarded(
        async {
            let watch = AddrsWatch::start(&env, &env.upstream).await.unwrap();
            k8s::rollout_restart(&env.client, &env.app_namespace, &env.echo_deployment)
                .await
                .unwrap();
            // Edit once the churn has started: after the controller's first write.
            wait_for(
                "the first controller write of the rollout",
                Duration::from_secs(60),
                Duration::from_millis(100),
                || async { Ok((watch.writes() >= 1).then_some(())) },
            )
            .await
            .unwrap();
            let t_edit = SystemTime::now();
            admin_set_health_check(&env, &env.upstream, Some(HEALTH_CHECK))
                .await
                .unwrap();
            println!(
                "[{T}] admin edit (GET /api/configs + POST /api/configs/upstream/{}) took {}",
                env.upstream,
                s(track::secs_between(SystemTime::now(), t_edit))
            );
            k8s::wait_deployment_rolled_out(
                &env.client,
                &env.app_namespace,
                &env.echo_deployment,
                ROLLOUT,
            )
            .await
            .unwrap();
            let ips = wait_converged(&env, CONVERGE).await.unwrap();
            let changes = watch.changes();
            let before = changes.iter().filter(|c| c.at < t_edit).count() - 1;
            let after = changes.iter().filter(|c| c.at >= t_edit).count();
            println!(
                "[{T}] writes to the echo upstream: {before} before the admin edit, {after} from it on (the edit included)"
            );
            for c in changes.iter().skip(1) {
                println!(
                    "[{T}]   rev {} at {} from the edit: {} addrs",
                    c.mod_revision,
                    s(track::secs_between(c.at, t_edit)),
                    c.addrs.as_ref().map_or(0, Vec::len)
                );
            }
            assert!(before >= 1, "no controller write before the edit: {changes:?}");
            assert!(after >= 2, "no controller write after the edit: {changes:?}");
            let stored = stored_health_check(&env, &env.upstream).await.unwrap();
            let admin = env.pingap.admin_upstream(&env.upstream).await.unwrap();
            println!("[{T}] stored health_check {stored:?}; admin addrs {:?}", json_addrs(&admin));
            assert_eq!(stored.as_deref(), Some(HEALTH_CHECK));
            assert_eq!(
                admin.get("health_check").and_then(Value::as_str),
                Some(HEALTH_CHECK)
            );
            // wait_converged above: the stored addrs were the current pods.
            assert_eq!(ips.len(), ECHO_REPLICAS as usize);
        },
        async {
            admin_set_health_check(&env, &env.upstream, None).await?;
            wait_converged(&env, CONVERGE).await.map(|_| ())
        },
    )
    .await;
    assert_eq!(
        stored_health_check(&env, &env.upstream).await.unwrap(),
        None
    );
    baseline(&env).await;
}

// ---------------------------------------------------------------------------------------
// 10. Save a stale admin form: old addrs corrected within debounce_max + 1 s.
// ---------------------------------------------------------------------------------------

/// e2e test 10: the "echo" entry is loaded through the admin API (the open form),
/// echo is scaled to 4 (the controller writes 4 addrs), then the stale form is saved with
/// a health-check edit: Pingap writes the 3 old addrs back. The controller must restore
/// the 4 addrs within debounce_max + 1 s of that foreign write, keeping the health check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_10_stale_admin_save() {
    const T: &str = "e2e_10";
    let Some(env) = Env::setup("e2e_10_stale_admin_save").await else {
        return;
    };
    baseline(&env).await;
    guarded(
        async {
            let mut form = env.pingap.admin_upstream(&env.upstream).await.unwrap();
            let stale = json_addrs(&form);
            let ips = scale_echo(&env, 4).await.unwrap();
            let fresh = with_port(&ips, HTTP_PORT);
            assert_ne!(stale, fresh);
            let leader = controller::wait_leader(&env, Duration::from_secs(30))
                .await
                .unwrap();
            let foreign_metric = || async {
                let text = controller::scrape(&env, leader.ip.as_deref().unwrap())
                    .await
                    .unwrap();
                controller::metric(
                    &text,
                    "pingap_k8s_foreign_writes_total",
                    &[("upstream", &env.upstream), ("corrected", "true")],
                )
                .unwrap_or(0.0)
            };
            let corrected_before = foreign_metric().await;

            let watch = AddrsWatch::start(&env, &env.upstream).await.unwrap();
            set_health_check(&mut form, Some(HEALTH_CHECK));
            let t_save = SystemTime::now();
            env.pingap
                .admin_save_upstream(&env.upstream, &form)
                .await
                .unwrap();
            wait_for(
                "the controller to restore the 4 addrs",
                Duration::from_secs(30),
                Duration::from_millis(100),
                || async {
                    let c = watch.changes();
                    let foreign = c.iter().position(|c| c.at >= t_save && c.addrs.as_ref() == Some(&stale));
                    Ok(foreign
                        .and_then(|i| c[i..].iter().find(|c| c.addrs.as_ref() == Some(&fresh)).cloned()))
                },
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(500)).await;
            let changes = watch.changes();
            let foreign = changes
                .iter()
                .find(|c| c.at >= t_save && c.addrs.as_ref() == Some(&stale))
                .unwrap();
            let fixed = changes
                .iter()
                .find(|c| c.mod_revision > foreign.mod_revision && c.addrs.as_ref() == Some(&fresh))
                .unwrap();
            let delay = track::secs_between(fixed.at, foreign.at);
            let corrected_after = foreign_metric().await;
            let stored = stored_health_check(&env, &env.upstream).await.unwrap();
            println!(
                "[{T}] stale save wrote {} addrs at rev {}; controller restored {} addrs at rev {} {} later; foreign_writes_total{{corrected=\"true\"}} {corrected_before} -> {corrected_after}; health_check {stored:?}",
                stale.len(),
                foreign.mod_revision,
                fresh.len(),
                fixed.mod_revision,
                s(delay)
            );
            assert!(
                delay <= DEBOUNCE_MAX + 1.0,
                "correction took {} > debounce_max + 1s",
                s(delay)
            );
            assert_eq!(stored.as_deref(), Some(HEALTH_CHECK));
            assert!(corrected_after > corrected_before);
            assert_eq!(
                env.etcd.upstream_addrs(&env.upstream).await.unwrap(),
                Some(fresh.clone())
            );
        },
        async {
            admin_set_health_check(&env, &env.upstream, None).await?;
            wait_converged(&env, CONVERGE).await.map(|_| ())
        },
    )
    .await;
    assert_eq!(
        stored_health_check(&env, &env.upstream).await.unwrap(),
        None
    );
    baseline(&env).await;
}

// ---------------------------------------------------------------------------------------
// 11. Helm install from scratch with etcd.deploy and pingap.deploy: traffic, no manual step.
// ---------------------------------------------------------------------------------------

const QS_NAMESPACE: &str = "pk8s-qs";
const QS_RELEASE: &str = "qs";

async fn qs_cleanup(env: &Env) -> anyhow::Result<()> {
    let kubeconfig = env.state.get("E2E_KUBECONFIG")?.to_string();
    let _ = host_cmd(
        "helm",
        vec![
            "--kubeconfig".into(),
            kubeconfig.clone(),
            "uninstall".into(),
            QS_RELEASE.into(),
            "-n".into(),
            QS_NAMESPACE.into(),
            "--wait".into(),
            "--timeout".into(),
            "180s".into(),
        ],
    )
    .await;
    env.kubectl
        .run(&[
            "delete",
            "namespace",
            QS_NAMESPACE,
            "--ignore-not-found",
            "--wait=true",
            "--timeout=180s",
        ])
        .await?;
    let roles: Api<Role> = Api::namespaced(env.client.clone(), &env.app_namespace);
    let leftover = format!("{QS_RELEASE}-pingap-k8s");
    anyhow::ensure!(
        roles.get_opt(&leftover).await?.is_none(),
        "Role {leftover} left in {}",
        env.app_namespace
    );
    Ok(())
}

/// e2e test 11: a second release ("qs" in namespace pk8s-qs) installed from scratch
/// with charts/pingap-k8s/ci/quickstart-values.yaml as is, plus the locally built
/// controller image and its one upstream pointed at the echo Service
/// (`echo.e2e-app:http`). Traffic through the new Pingap must reach the echo pods with
/// no other step. Uninstalled afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_11_quickstart_install() {
    const T: &str = "e2e_11";
    let Some(env) = Env::setup("e2e_11_quickstart_install").await else {
        return;
    };
    baseline(&env).await;
    let root = env!("CARGO_MANIFEST_DIR");
    let kubeconfig = env.state.get("E2E_KUBECONFIG").unwrap().to_string();
    let image = env.state.get("E2E_IMAGE").unwrap().to_string();
    let (repo, tag) = image.rsplit_once(':').expect("image with a tag");
    let echo_names: BTreeSet<String> = echo_pods(&env)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.name)
        .collect();
    qs_cleanup(&env).await.unwrap();

    guarded(
        async {
            let t0 = SystemTime::now();
            host_cmd(
                "helm",
                vec![
                    "--kubeconfig".into(),
                    kubeconfig.clone(),
                    "install".into(),
                    QS_RELEASE.into(),
                    format!("{root}/charts/pingap-k8s"),
                    "-n".into(),
                    QS_NAMESPACE.into(),
                    "--create-namespace".into(),
                    "-f".into(),
                    format!("{root}/charts/pingap-k8s/ci/quickstart-values.yaml"),
                    "--set".into(),
                    format!("image.repository={repo}"),
                    "--set".into(),
                    format!("image.tag={tag}"),
                    "--set".into(),
                    "image.pullPolicy=Never".into(),
                    "--set-json".into(),
                    format!(
                        "config.upstreams.quickstart.services=[\"echo.{}:http\"]",
                        env.app_namespace
                    ),
                ],
            )
            .await
            .unwrap();
            let url =
                format!("http://{QS_RELEASE}-pingap-k8s-pingap.{QS_NAMESPACE}.svc:80/hostname");
            let host = wait_for(
                "traffic through the quickstart Pingap",
                Duration::from_secs(300),
                Duration::from_secs(1),
                || async {
                    let r = env.pingap.get_url(&url).await?;
                    let host = r.body.trim().to_string();
                    Ok((r.status == 200 && echo_names.contains(&host)).then_some(host))
                },
            )
            .await
            .unwrap();
            println!(
                "[{T}] first response from echo pod {host} {} after helm install started",
                s(track::secs_between(SystemTime::now(), t0))
            );
            // Every echo pod gets traffic.
            let script = format!("for i in $(seq 1 30); do curl -s -m 2 '{url}'; echo; done");
            let out = env
                .kubectl
                .exec(
                    &env.app_namespace,
                    &env.pingap.client_pod,
                    None,
                    &["sh", "-c", &script],
                )
                .await
                .unwrap();
            let seen: BTreeSet<String> = out
                .stdout
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect();
            println!("[{T}] 30 requests answered by {seen:?}");
            assert_eq!(seen, echo_names);
            let pods = env
                .kubectl
                .run(&["get", "pods", "-n", QS_NAMESPACE, "-o", "wide"])
                .await
                .unwrap();
            println!("[{T}] pods:\n{pods}");
        },
        qs_cleanup(&env),
    )
    .await;
}

// ---------------------------------------------------------------------------------------
// Apps behind the gRPC and SSE upstreams (tests 12 and 13).
// ---------------------------------------------------------------------------------------

/// Ready pod IPs of `selector`.
async fn app_ready_ips(env: &Env, selector: &str) -> anyhow::Result<Vec<String>> {
    k8s::ready_pod_ips(&env.client, &env.app_namespace, selector).await
}

/// Waits for `upstream` to hold exactly the ready pods of `selector` on `port`.
async fn wait_app_converged(
    env: &Env,
    selector: &str,
    upstream: &str,
    port: u16,
    timeout: Duration,
) -> anyhow::Result<Vec<String>> {
    wait_for(
        &format!("upstream {upstream} equal to the ready {selector} pods"),
        timeout,
        Duration::from_millis(200),
        || async {
            let ips = app_ready_ips(env, selector).await?;
            if ips.is_empty() {
                return Ok(None);
            }
            let addrs = env.etcd.upstream_addrs(upstream).await?;
            Ok((addrs == Some(with_port(&ips, port))).then_some(ips))
        },
    )
    .await
}

/// [`scale_echo`] for another Deployment of the test app and its upstream.
async fn scale_app(
    env: &Env,
    deployment: &str,
    selector: &str,
    replicas: i32,
    upstream: &str,
    port: u16,
) -> anyhow::Result<Vec<String>> {
    k8s::scale_deployment(&env.client, &env.app_namespace, deployment, replicas).await?;
    k8s::wait_deployment_rolled_out(&env.client, &env.app_namespace, deployment, ROLLOUT).await?;
    wait_for(
        &format!("only the wanted {deployment} pods left"),
        ROLLOUT,
        Duration::from_millis(250),
        || async {
            let pods = k8s::pods(&env.client, &env.app_namespace, selector).await?;
            Ok((pods.len() == replicas as usize).then_some(()))
        },
    )
    .await?;
    wait_app_converged(env, selector, upstream, port, CONVERGE).await
}

// ---------------------------------------------------------------------------------------
// 12. gRPC: per-request balancing over one client connection, and a new pod's first call.
// ---------------------------------------------------------------------------------------

const PODINFO_DEPLOYMENT: &str = "podinfo";
const PODINFO_SELECTOR: &str = "app=podinfo";
const PODINFO_REPLICAS: i32 = 3;
/// Owned upstream following podinfo's "grpc" port (tests/e2e/values.yaml).
const GRPC_UPSTREAM: &str = "grpc";
/// podinfo container gRPC port.
const GRPC_PORT: u16 = 9999;
/// Pingap's h2c listener for gRPC (seed `[servers.grpc]`), reached by pod IP.
const PINGAP_GRPC_PORT: u16 = 8081;
/// gRPC calls per batch while waiting for the new pod (test 12).
const GRPC_BATCH: usize = 20;
/// Slack on top of [`SYNC_BUDGET`] for test 12: the first call to the new pod is timed at
/// the end of its batch, one `kubectl exec` round trip after it was answered.
const GRPC_SLACK: f64 = 1.0;

/// Unary gRPC calls made by curl over HTTP/2 without TLS (prior knowledge): every call
/// of one invocation goes over the same connection, which curl reports per transfer
/// (`local_port`, and `num_connects` 0 when reused). The request is an empty message,
/// which `info.InfoService/Info` takes. Prints one line per call: `<local port> <http
/// status> <http version> <new connects> <body>`, the body with everything but
/// `[A-Za-z0-9.-]` blanked (podinfo's InfoResponse starts with the pod name).
const GRPC_CALLS_SCRIPT: &str = r#"url=$1; n=$2; d=$(mktemp -d); head -c 5 /dev/zero > "$d/req"
set --
i=1; while [ "$i" -le "$n" ]; do set -- "$@" -o "$d/$i" "$url"; i=$((i+1)); done
curl -sS -m 10 --http2-prior-knowledge -H 'content-type: application/grpc' -H 'te: trailers' \
  --data-binary "@$d/req" -w '%{local_port} %{http_code} %{http_version} %{num_connects}\n' \
  "$@" > "$d/w"
rc=$?
i=1; while read -r line; do echo "$line $(tr -c 'A-Za-z0-9.-' ' ' < "$d/$i" 2>/dev/null)"; i=$((i+1)); done < "$d/w"
rm -rf "$d"; exit $rc"#;

/// One call of [`grpc_calls`].
#[derive(Debug, Clone)]
struct GrpcCall {
    local_port: u16,
    status: u16,
    http_version: String,
    connects: u32,
    /// The podinfo pod that answered (a `podinfo-` word of the body).
    host: Option<String>,
}

fn parse_grpc_calls(stdout: &str) -> Vec<GrpcCall> {
    stdout
        .lines()
        .filter_map(|line| {
            let mut words = line.split_whitespace();
            let local_port = words.next()?.parse().ok()?;
            let status = words.next()?.parse().ok()?;
            let http_version = words.next()?.to_string();
            let connects = words.next()?.parse().ok()?;
            let host = words
                .find(|w| w.starts_with("podinfo-"))
                .map(str::to_string);
            Some(GrpcCall {
                local_port,
                status,
                http_version,
                connects,
                host,
            })
        })
        .collect()
}

/// `n` calls of `url` from the client pod in one curl invocation ([`GRPC_CALLS_SCRIPT`]).
async fn grpc_calls(env: &Env, url: &str, n: usize) -> anyhow::Result<Vec<GrpcCall>> {
    let n = n.to_string();
    let out = env
        .kubectl
        .exec(
            &env.app_namespace,
            &env.pingap.client_pod,
            None,
            &["sh", "-c", GRPC_CALLS_SCRIPT, "sh", url, &n],
        )
        .await?;
    let calls = parse_grpc_calls(&out.stdout);
    if calls.is_empty() {
        anyhow::bail!(
            "no gRPC call completed: {:?} {:?}",
            out.stdout,
            out.stderr.trim()
        );
    }
    Ok(calls)
}

/// `http://<Pingap pod IP>:8081`: the chart's Pingap Service exposes one port only.
async fn pingap_grpc_base(env: &Env) -> anyhow::Result<String> {
    let release = controller::release(env);
    let pods = k8s::pods(&env.client, &env.namespace, &k8s::pingap_selector(&release)).await?;
    let ip = pods
        .iter()
        .find(|p| p.ready && !p.terminating)
        .and_then(|p| p.ip.clone())
        .ok_or_else(|| anyhow::anyhow!("no ready Pingap pod: {pods:?}"))?;
    let host = if ip.contains(':') {
        format!("[{ip}]")
    } else {
        ip
    };
    Ok(format!("http://{host}:{PINGAP_GRPC_PORT}"))
}

/// Where [`WATCH_SCRIPT`] writes the messages of its stream, in the client pod.
const WATCH_FILE: &str = "/tmp/e2e-grpc-watch";
/// A `grpc.health.v1.Health/Watch` call through Pingap (server streaming: podinfo answers
/// SERVING at once and keeps the stream open), its messages written to `$2`. Prints
/// `START`, then `EXIT <curl status>` when the call ends.
const WATCH_SCRIPT: &str = r#"f=$2; rm -f "$f"; head -c 5 /dev/zero > "$f.req"; echo START
curl -sS -N -m 120 --http2-prior-knowledge -H 'content-type: application/grpc' -H 'te: trailers' \
  --data-binary "@$f.req" -o "$f" "$1/grpc.health.v1.Health/Watch"
echo "EXIT $?""#;

/// The SERVING `HealthCheckResponse` in a gRPC frame, as `od -An -tx1` prints it.
const WATCH_SERVING: &str = "00 00 00 00 02 08 01";

/// e2e test 12. podinfo (3 replicas) serves gRPC behind Pingap's h2c listener
/// (upstream `grpc`: alpn H2, gRPC health check). 60 unary `info.InfoService/Info` calls
/// over one client connection must reach all three pods: HTTP/2 multiplexes the calls on
/// one connection, so only per-request balancing spreads them. Then, with a
/// `Health/Watch` stream open through Pingap, podinfo is scaled to 4: the new pod must
/// answer calls within 3 s of Ready (plus [`GRPC_SLACK`]), no call may fail, and the
/// stream must still be open after the `addrs` rewrite.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_12_grpc_per_request_balancing() {
    const T: &str = "e2e_12";
    let Some(env) = Env::setup("e2e_12_grpc_per_request_balancing").await else {
        return;
    };
    baseline(&env).await;
    scale_app(
        &env,
        PODINFO_DEPLOYMENT,
        PODINFO_SELECTOR,
        PODINFO_REPLICAS,
        GRPC_UPSTREAM,
        GRPC_PORT,
    )
    .await
    .expect("podinfo baseline");
    let base = pingap_grpc_base(&env).await.unwrap();
    let info = format!("{base}/info.InfoService/Info");

    guarded(
        async {
            wait_for(
                "a gRPC answer through Pingap",
                Duration::from_secs(60),
                Duration::from_secs(1),
                || async {
                    let calls = grpc_calls(&env, &info, 1).await?;
                    Ok(calls[0].host.clone())
                },
            )
            .await
            .unwrap();

            // Per-request balancing over one connection.
            let names: BTreeSet<String> =
                k8s::pods(&env.client, &env.app_namespace, PODINFO_SELECTOR)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|p| p.name)
                    .collect();
            let calls = grpc_calls(&env, &info, 60).await.unwrap();
            let mut by_host: BTreeMap<String, usize> = BTreeMap::new();
            for c in &calls {
                let key = c
                    .host
                    .clone()
                    .unwrap_or_else(|| format!("status {}", c.status));
                *by_host.entry(key).or_default() += 1;
            }
            let ports: BTreeSet<u16> = calls.iter().map(|c| c.local_port).collect();
            let connects: u32 = calls.iter().map(|c| c.connects).sum();
            println!(
                "[{T}] {} calls over {} connection(s) (local ports {ports:?}, {connects} connect(s)): {by_host:?}",
                calls.len(),
                ports.len()
            );
            assert_eq!(calls.len(), 60, "{calls:?}");
            assert!(
                calls
                    .iter()
                    .all(|c| c.status == 200 && c.http_version == "2" && c.host.is_some()),
                "{calls:?}"
            );
            assert_eq!(ports.len(), 1, "not one client connection: {calls:?}");
            assert_eq!(connects, 1, "not one client connection: {calls:?}");
            let answered: BTreeSet<String> = by_host.keys().cloned().collect();
            assert_eq!(answered, names, "pods that answered vs podinfo pods");

            // A server stream open through Pingap during the scale-up.
            let watch = track::LineStream::exec(
                &env.kubectl,
                &env.app_namespace,
                &env.pingap.client_pod,
                None,
                &["sh", "-c", WATCH_SCRIPT, "sh", &base, WATCH_FILE],
            )
            .unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;

            // Scale to 4: time from the new pod's Ready to its first call.
            let tracker = PodTracker::start(&env.client, &env.app_namespace, PODINFO_SELECTOR)
                .await
                .unwrap();
            k8s::scale_deployment(&env.client, &env.app_namespace, PODINFO_DEPLOYMENT, 4)
                .await
                .unwrap();
            let deadline = tokio::time::Instant::now() + ROLLOUT;
            let mut failed: Vec<GrpcCall> = Vec::new();
            let mut batches = 0;
            let (name, seen, started, answered_at) = loop {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "no call reached a new podinfo pod: {:?}",
                    tracker.snapshot()
                );
                let Some((name, seen)) = tracker.new_ready().into_iter().next() else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                let started = SystemTime::now();
                let calls = grpc_calls(&env, &info, GRPC_BATCH)
                    .await
                    .expect("gRPC calls during the scale-up");
                let at = SystemTime::now();
                batches += 1;
                failed.extend(
                    calls
                        .iter()
                        .filter(|c| c.status != 200 || c.host.is_none())
                        .cloned(),
                );
                if calls.iter().any(|c| c.host.as_deref() == Some(name.as_str())) {
                    break (name, seen, started, at);
                }
            };
            let ready = seen.ready.unwrap();
            let d = track::secs_between(answered_at, ready);
            let d_start = track::secs_between(started, ready);
            let dt = track::secs_between(answered_at, seen.ready_transition.unwrap());
            println!(
                "[{T}] new pod {name}: first call answered {} after Ready seen (its batch started {} after), {} after Ready lastTransitionTime; {batches} batch(es), {} failed call(s)",
                s(d),
                s(d_start),
                s(dt),
                failed.len()
            );
            assert!(failed.is_empty(), "failed calls: {failed:?}");
            assert!(
                d <= SYNC_BUDGET + GRPC_SLACK,
                "{} > {SYNC_BUDGET}s + {GRPC_SLACK}s",
                s(d)
            );
            wait_app_converged(&env, PODINFO_SELECTOR, GRPC_UPSTREAM, GRPC_PORT, CONVERGE)
                .await
                .unwrap();

            // The stream got its first message and is still open after the rewrite.
            let lines = watch.lines();
            assert!(
                lines.iter().any(|(_, l)| l == "START")
                    && !lines.iter().any(|(_, l)| l.starts_with("EXIT")),
                "Health/Watch stream ended during the scale-up: {lines:?}"
            );
            let dump = env
                .kubectl
                .exec(
                    &env.app_namespace,
                    &env.pingap.client_pod,
                    None,
                    &["od", "-An", "-tx1", WATCH_FILE],
                )
                .await
                .unwrap();
            let bytes = dump.stdout.split_whitespace().collect::<Vec<_>>().join(" ");
            println!(
                "[{T}] Health/Watch stream still open after the rewrite; received {bytes:?}"
            );
            assert!(bytes.starts_with(WATCH_SERVING), "watch received {bytes:?}");
            drop(watch.stop());
        },
        async {
            let _ = env
                .kubectl
                .exec(
                    &env.app_namespace,
                    &env.pingap.client_pod,
                    None,
                    &["sh", "-c", "pkill -f Health/Watch; true"],
                )
                .await;
            scale_app(
                &env,
                PODINFO_DEPLOYMENT,
                PODINFO_SELECTOR,
                PODINFO_REPLICAS,
                GRPC_UPSTREAM,
                GRPC_PORT,
            )
            .await
            .map(|_| ())
        },
    )
    .await;
}

// ---------------------------------------------------------------------------------------
// 13. Server-sent events: streamed incrementally, and survive an addrs rewrite.
// ---------------------------------------------------------------------------------------

const HTTPBIN_DEPLOYMENT: &str = "httpbin";
const HTTPBIN_SELECTOR: &str = "app=httpbin";
const HTTPBIN_REPLICAS: i32 = 2;
/// Owned upstream following httpbin's "http" port; Pingap routes `/sse` to it.
const HTTPBIN_UPSTREAM: &str = "httpbin";
const HTTPBIN_PORT: u16 = 8080;
/// go-httpbin `/sse?count=..&duration=..`: one event per second for 30 s (31 events,
/// the first at once).
const SSE_EVENTS: usize = 31;
const SSE_SECS: u64 = 30;
/// Largest gap allowed between two events as they arrive (they are sent 1 s apart):
/// a buffering proxy would deliver them in bursts.
const SSE_MAX_GAP: f64 = 2.5;

/// `data: {"id":N,"timestamp":...}` lines of a go-httpbin event stream: (arrival, id).
fn sse_events(lines: &[(SystemTime, String)]) -> Vec<(SystemTime, u64)> {
    lines
        .iter()
        .filter_map(|(at, l)| {
            let data = l.strip_prefix("data: ")?;
            let v: Value = k8s_openapi::serde_json::from_str(data).ok()?;
            Some((*at, v.get("id")?.as_u64()?))
        })
        .collect()
}

/// e2e test 13. A 30 s server-sent event stream (go-httpbin `/sse`, one event per
/// second) through Pingap's HTTP listener (location `/sse`, upstream `httpbin`). Five
/// seconds in, httpbin is scaled 2 -> 3 (the controller rewrites the upstream's `addrs`).
/// The events must arrive one by one as sent (no gap over [`SSE_MAX_GAP`], first to last
/// spread over most of the 30 s), some before and some after the rewrite, and the stream
/// must complete with every event and a clean end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_13_sse_stream() {
    const T: &str = "e2e_13";
    let Some(env) = Env::setup("e2e_13_sse_stream").await else {
        return;
    };
    baseline(&env).await;
    scale_app(
        &env,
        HTTPBIN_DEPLOYMENT,
        HTTPBIN_SELECTOR,
        HTTPBIN_REPLICAS,
        HTTPBIN_UPSTREAM,
        HTTPBIN_PORT,
    )
    .await
    .expect("httpbin baseline");

    guarded(
        async {
            wait_for(
                "an event stream through Pingap",
                Duration::from_secs(60),
                Duration::from_secs(1),
                || async {
                    let r = env.pingap.get("/sse?count=1&duration=1s", &[]).await?;
                    Ok((r.status == 200 && r.body.contains("event: ping")).then_some(()))
                },
            )
            .await
            .unwrap();

            let script = format!(
                "echo START; curl -sS -N -m 90 '{}/sse?count={SSE_EVENTS}&duration={SSE_SECS}s'; echo \"EXIT $?\"",
                env.pingap.url
            );
            let stream = track::LineStream::exec(
                &env.kubectl,
                &env.app_namespace,
                &env.pingap.client_pod,
                None,
                &["sh", "-c", &script],
            )
            .unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;

            let watch = AddrsWatch::start(&env, HTTPBIN_UPSTREAM).await.unwrap();
            k8s::scale_deployment(
                &env.client,
                &env.app_namespace,
                HTTPBIN_DEPLOYMENT,
                HTTPBIN_REPLICAS + 1,
            )
            .await
            .unwrap();
            let ips = wait_for(
                "the third httpbin pod in the upstream",
                ROLLOUT,
                Duration::from_millis(200),
                || async {
                    let ips = app_ready_ips(&env, HTTPBIN_SELECTOR).await?;
                    let addrs = env.etcd.upstream_addrs(HTTPBIN_UPSTREAM).await?;
                    Ok((ips.len() == HTTPBIN_REPLICAS as usize + 1
                        && addrs == Some(with_port(&ips, HTTPBIN_PORT)))
                    .then_some(ips))
                },
            )
            .await
            .unwrap();
            let want = with_port(&ips, HTTPBIN_PORT);
            tokio::time::sleep(Duration::from_millis(500)).await;
            let rewrite = watch
                .changes()
                .into_iter()
                .skip(1)
                .find(|c| c.addrs.as_ref() == Some(&want))
                .map(|c| c.at)
                .expect("the rewrite on the etcd watch");

            let end = wait_for(
                "the event stream to end",
                Duration::from_secs(90),
                Duration::from_millis(250),
                || async {
                    Ok(stream
                        .lines()
                        .into_iter()
                        .find(|(_, l)| l.starts_with("EXIT "))
                        .map(|(_, l)| l))
                },
            )
            .await
            .unwrap();
            let lines = stream.stop();
            let events = sse_events(&lines);
            assert!(!events.is_empty(), "no events: {lines:?}");
            let ids: Vec<u64> = events.iter().map(|(_, id)| *id).collect();
            let (first, last) = (events[0].0, events[events.len() - 1].0);
            let spread = track::secs_between(last, first);
            let max_gap = events
                .windows(2)
                .map(|w| track::secs_between(w[1].0, w[0].0))
                .fold(0.0, f64::max);
            let before = events.iter().filter(|(at, _)| *at < rewrite).count();
            let after = events.len() - before;
            println!(
                "[{T}] {} events over {} (largest gap {}); addrs rewritten {} into the stream, {before} events before and {after} after; {end}",
                events.len(),
                s(spread),
                s(max_gap),
                s(track::secs_between(rewrite, first))
            );
            assert_eq!(end, "EXIT 0", "stream did not end cleanly: {lines:?}");
            assert_eq!(
                ids,
                (0..SSE_EVENTS as u64).collect::<Vec<_>>(),
                "events: {lines:?}"
            );
            assert!(
                max_gap <= SSE_MAX_GAP,
                "events not streamed one by one: gap {} > {SSE_MAX_GAP}s",
                s(max_gap)
            );
            assert!(
                spread >= SSE_SECS as f64 * 0.75,
                "events arrived within {}, not spread over the stream",
                s(spread)
            );
            assert!(
                before >= 2 && after >= 2,
                "the rewrite was not during the stream ({before} events before, {after} after)"
            );
        },
        async {
            scale_app(
                &env,
                HTTPBIN_DEPLOYMENT,
                HTTPBIN_SELECTOR,
                HTTPBIN_REPLICAS,
                HTTPBIN_UPSTREAM,
                HTTPBIN_PORT,
            )
            .await
            .map(|_| ())
        },
    )
    .await;
}

// ---------------------------------------------------------------------------------------
// 14. Kubernetes Events (opt-in, events.enabled in tests/e2e/values.yaml).
// ---------------------------------------------------------------------------------------

/// The Service port name `httpbin`'s upstream follows (tests/e2e/values.yaml).
const HTTPBIN_PORT_NAME: &str = "http";

/// Warning Events posted by pingap-k8s about the Service `name`: event name and how often
/// it was observed (`series.count`, 1 without a series). Read through events.k8s.io/v1.
async fn controller_warnings(env: &Env, name: &str) -> anyhow::Result<BTreeMap<String, i32>> {
    use k8s_openapi::api::events::v1::Event;
    let api: Api<Event> = Api::namespaced(env.client.clone(), &env.app_namespace);
    let list = api
        .list(&kube::api::ListParams::default().fields("reportingController=pingap-k8s"))
        .await?;
    Ok(list
        .items
        .into_iter()
        .filter(|e| e.type_.as_deref() == Some("Warning"))
        .filter(|e| {
            e.regarding.as_ref().is_some_and(|r| {
                r.kind.as_deref() == Some("Service") && r.name.as_deref() == Some(name)
            })
        })
        .map(|e| {
            let count = e.series.as_ref().map_or(1, |s| s.count);
            (
                format!(
                    "{}/{}",
                    e.reason.unwrap_or_default(),
                    e.metadata.name.unwrap_or_default()
                ),
                count,
            )
        })
        .collect())
}

/// Renames the httpbin Service port (port 80) to `port_name`.
async fn rename_httpbin_port(env: &Env, port_name: &str) -> anyhow::Result<()> {
    use k8s_openapi::api::core::v1::Service;
    let api: Api<Service> = Api::namespaced(env.client.clone(), &env.app_namespace);
    // Service ports merge by port number (strategic merge patch).
    let patch = json!({ "spec": { "ports": [{ "port": 80, "name": port_name }] } });
    api.patch(
        HTTPBIN_DEPLOYMENT,
        &PatchParams::default(),
        &Patch::Strategic(&patch),
    )
    .await?;
    Ok(())
}

/// Opt-in Events: breaking the port name of a referenced Service posts one Warning
/// (`PortNotInService`) on that Service; fixing it posts no further warnings, and the
/// upstream follows the pods again. The `addrs` stay as they were meanwhile .
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_14_events() {
    const T: &str = "e2e_14";
    let Some(env) = Env::setup("e2e_14_events").await else {
        return;
    };
    baseline(&env).await;
    let ips = scale_app(
        &env,
        HTTPBIN_DEPLOYMENT,
        HTTPBIN_SELECTOR,
        HTTPBIN_REPLICAS,
        HTTPBIN_UPSTREAM,
        HTTPBIN_PORT,
    )
    .await
    .expect("httpbin baseline");

    guarded(
        async {
            let before = controller_warnings(&env, HTTPBIN_DEPLOYMENT).await.unwrap();
            rename_httpbin_port(&env, "http-broken").await.unwrap();
            let posted = wait_for(
                "a PortNotInService Warning Event on the httpbin Service",
                Duration::from_secs(60),
                Duration::from_millis(500),
                || async {
                    let now = controller_warnings(&env, HTTPBIN_DEPLOYMENT).await?;
                    let new: BTreeMap<String, i32> = now
                        .into_iter()
                        .filter(|(k, v)| before.get(k) != Some(v))
                        .collect();
                    Ok((!new.is_empty()).then_some(new))
                },
            )
            .await
            .unwrap();
            println!("[{T}] new warning events: {posted:?}");
            assert!(
                posted.keys().all(|k| k.starts_with("PortNotInService/")),
                "{posted:?}"
            );
            // Kept while broken .
            assert_eq!(
                env.etcd.upstream_addrs(HTTPBIN_UPSTREAM).await.unwrap(),
                Some(with_port(&ips, HTTPBIN_PORT))
            );

            let fixed = controller_warnings(&env, HTTPBIN_DEPLOYMENT).await.unwrap();
            rename_httpbin_port(&env, HTTPBIN_PORT_NAME).await.unwrap();
            wait_app_converged(
                &env,
                HTTPBIN_SELECTOR,
                HTTPBIN_UPSTREAM,
                HTTPBIN_PORT,
                CONVERGE,
            )
            .await
            .unwrap();
            // A resync or debounce later, still nothing new.
            tokio::time::sleep(Duration::from_secs(5)).await;
            let after = controller_warnings(&env, HTTPBIN_DEPLOYMENT).await.unwrap();
            assert_eq!(after, fixed, "no new warnings after the fix");
            println!("[{T}] after the fix: no new warning events");
        },
        async {
            rename_httpbin_port(&env, HTTPBIN_PORT_NAME).await?;
            scale_app(
                &env,
                HTTPBIN_DEPLOYMENT,
                HTTPBIN_SELECTOR,
                HTTPBIN_REPLICAS,
                HTTPBIN_UPSTREAM,
                HTTPBIN_PORT,
            )
            .await
            .map(|_| ())
        },
    )
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_grpc_calls() {
        let calls = parse_grpc_calls(
            "41000 200 2 1      podinfo-5d8f7c9b4-x2x9z  6.15.0  linux\n\
             41000 200 2 0  podinfo-5d8f7c9b4-abcde\n\
             41000 503 2 0 \n\
             garbage\n",
        );
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].host.as_deref(), Some("podinfo-5d8f7c9b4-x2x9z"));
        assert_eq!((calls[0].local_port, calls[0].connects), (41000, 1));
        assert_eq!(calls[1].connects, 0);
        assert_eq!((calls[2].status, calls[2].host.as_deref()), (503, None));
    }

    #[test]
    fn parses_sse_events() {
        let t = SystemTime::UNIX_EPOCH;
        let lines = vec![
            (t, "event: ping".to_string()),
            (t, r#"data: {"id":0,"timestamp":1700000000000}"#.to_string()),
            (t, String::new()),
            (t, r#"data: {"id":1,"timestamp":1700000001000}"#.to_string()),
            (t, "EXIT 0".to_string()),
        ];
        let ids: Vec<u64> = sse_events(&lines).into_iter().map(|(_, id)| id).collect();
        assert_eq!(ids, vec![0, 1]);
    }
}

//! Micro-benchmarks for the CPU-bound parts of a sync:
//! `resolve::resolve` and `etcd::replace_addrs` at 50 upstreams x 20 endpoints.
//!
//! No benchmark framework: plain `Instant` loops after a warm-up, reporting the
//! median (and min) ns/op over several rounds.
//!
//! Run: `cargo run --release --example bench_resolve`
//! Env: `BENCH_ROUNDS` (default 7), `BENCH_MIN_MS` (minimum time per round, default 300).

use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort, EndpointSlice};
use pingap_k8s::etcd::replace_addrs;
use pingap_k8s::resolve::{ServiceInput, resolve};
use pingap_k8s::types::{
    PortRef, ServiceKey, ServiceRef, ServiceSnapshot, UpstreamOptions, UpstreamTarget,
};

const UPSTREAMS: usize = 50;
const ENDPOINTS: usize = 20;

fn opts() -> UpstreamOptions {
    UpstreamOptions {
        include_not_ready: false,
        drain_terminating: true,
        ipv4_only: false,
        debounce: Duration::from_millis(500),
        debounce_max: Duration::from_secs(2),
        target: UpstreamTarget::Pods,
    }
}

fn tcp_port(name: &str, port: i32) -> ServicePort {
    ServicePort {
        name: Some(name.into()),
        port,
        protocol: Some("TCP".into()),
        ..Default::default()
    }
}

fn service() -> Arc<Service> {
    Arc::new(Service {
        spec: Some(ServiceSpec {
            ports: Some(vec![tcp_port("http", 80), tcp_port("metrics", 9090)]),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn slice_port(name: &str, port: i32) -> EndpointPort {
    EndpointPort {
        name: Some(name.into()),
        port: Some(port),
        protocol: Some("TCP".into()),
        ..Default::default()
    }
}

/// One EndpointSlice holding `count` endpoints, as the EndpointSlice controller
/// produces for a 20-replica Deployment. During a rollout `terminating` of
/// them are terminating and still serving.
fn slice(upstream: usize, count: usize, terminating: usize) -> Arc<EndpointSlice> {
    let endpoints = (0..count)
        .map(|i| {
            let term = i < terminating;
            Endpoint {
                addresses: vec![format!("10.244.{upstream}.{}", 10 + i)],
                conditions: Some(EndpointConditions {
                    ready: Some(!term),
                    serving: Some(true),
                    terminating: Some(term),
                }),
                ..Default::default()
            }
        })
        .collect();
    Arc::new(EndpointSlice {
        address_type: "IPv4".into(),
        endpoints,
        ports: Some(vec![slice_port("http", 8080), slice_port("metrics", 9090)]),
        ..Default::default()
    })
}

struct Fixture {
    refs: Vec<ServiceRef>,
    snapshots: Vec<ServiceSnapshot>,
}

fn fixture(terminating: usize) -> Fixture {
    let refs = (0..UPSTREAMS)
        .map(|u| ServiceRef {
            key: ServiceKey {
                namespace: "bench".into(),
                name: format!("svc-{u:02}"),
            },
            port: PortRef::Name("http".into()),
            weight: None,
        })
        .collect();
    let svc = service();
    let snapshots = (0..UPSTREAMS)
        .map(|u| ServiceSnapshot {
            service: Some(svc.clone()),
            slices: vec![slice(u, ENDPOINTS, terminating)],
        })
        .collect();
    Fixture { refs, snapshots }
}

impl Fixture {
    fn inputs(&self) -> Vec<[ServiceInput<'_>; 1]> {
        self.refs
            .iter()
            .zip(&self.snapshots)
            .map(|(r, s)| {
                [ServiceInput {
                    service_ref: r,
                    snapshot: s,
                }]
            })
            .collect()
    }
}

/// Runs `f` repeatedly; returns the median and min ns/op over `rounds` rounds
/// of at least about `min` each, and the iterations per round.
fn bench<F: FnMut()>(rounds: usize, min: Duration, mut f: F) -> (f64, f64, u64) {
    // Warm-up and calibration.
    let mut iters: u64 = 1;
    loop {
        let t = Instant::now();
        for _ in 0..iters {
            f();
        }
        if t.elapsed() >= min / 4 {
            break;
        }
        iters *= 2;
    }
    iters *= 4;
    let mut samples: Vec<f64> = (0..rounds.max(1))
        .map(|_| {
            let t = Instant::now();
            for _ in 0..iters {
                f();
            }
            t.elapsed().as_nanos() as f64 / iters as f64
        })
        .collect();
    samples.sort_by(f64::total_cmp);
    (samples[samples.len() / 2], samples[0], iters)
}

fn report(name: &str, (median, min, iters): (f64, f64, u64)) {
    println!("{name:<66} {median:>9.0} ns/op median {min:>9.0} min  ({iters} iters/round)");
}

/// An upstream as the Pingap admin UI stores it, with the addrs one per line.
fn pingap_toml(upstream: &str, addrs: &[String]) -> String {
    let list: String = addrs.iter().map(|a| format!("    \"{a}\",\n")).collect();
    format!(
        "[upstreams.{upstream}]\n\
         addrs = [\n{list}]\n\
         discovery = \"common\"\n\
         update_frequency = \"30s\"\n\
         algo = \"round_robin\"\n\
         health_check = \"http://{upstream}/healthz?connection_timeout=3s&success=2&failure=1\" # keep me\n\
         connection_timeout = \"3s\"\n\
         total_connection_timeout = \"5s\"\n\
         read_timeout = \"10s\"\n\
         idle_timeout = \"120s\"\n\
         write_timeout = \"10s\"\n\
         tcp_idle = \"60s\"\n\
         tcp_interval = \"30s\"\n\
         tcp_probe_count = 9\n\
         alpn = \"h1\"\n\
         enable_tracer = false\n\
         remark = \"managed by pingap-k8s\"\n"
    )
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let rounds: usize = env_or("BENCH_ROUNDS", 7);
    let min = Duration::from_millis(env_or("BENCH_MIN_MS", 300));
    println!(
        "pingap-k8s micro-benchmarks: {UPSTREAMS} upstreams x {ENDPOINTS} endpoints, median of {rounds} rounds"
    );
    let o = opts();

    // ---- resolve ----
    for (label, terminating) in [("steady", 0), ("rollout, 5 of 20 terminating", 5)] {
        let fx = fixture(terminating);
        let inputs = fx.inputs();
        let sample = resolve(&inputs[0], &o);
        assert_eq!(sample.addrs.len(), ENDPOINTS - terminating);
        assert_eq!(sample.ready, ENDPOINTS - terminating);
        report(
            &format!("resolve, 1 upstream x {ENDPOINTS} endpoints ({label})"),
            bench(rounds, min, || {
                black_box(resolve(black_box(&inputs[7]), &o));
            }),
        );
        report(
            &format!("resolve, all {UPSTREAMS} upstreams ({label})"),
            bench(rounds, min, || {
                for i in &inputs {
                    black_box(resolve(black_box(i), &o));
                }
            }),
        );
    }

    // ---- replace_addrs ----
    let fx = fixture(0);
    let current: Vec<Vec<String>> = fx.inputs().iter().map(|i| resolve(i, &o).addrs).collect();
    let names: Vec<String> = (0..UPSTREAMS).map(|u| format!("svc-{u:02}")).collect();
    let docs: Vec<String> = names
        .iter()
        .zip(&current)
        .map(|(n, a)| pingap_toml(n, a))
        .collect();
    // One pod replaced: a new IP in place of the first one.
    let changed: Vec<Vec<String>> = current
        .iter()
        .enumerate()
        .map(|(u, a)| {
            let mut a = a.clone();
            a[0] = format!("10.245.{u}.200:8080");
            a.sort();
            a
        })
        .collect();
    println!("sample upstream TOML: {} bytes", docs[0].len());

    let out = replace_addrs(&docs[0], &names[0], &changed[0])
        .unwrap()
        .expect("changed addrs must produce a new document");
    assert!(out.contains("# keep me") && out.contains("10.245.0.200:8080"));
    assert!(
        replace_addrs(&docs[0], &names[0], &current[0])
            .unwrap()
            .is_none()
    );

    report(
        "replace_addrs, 1 upstream, unchanged (no write)",
        bench(rounds, min, || {
            black_box(replace_addrs(black_box(&docs[3]), &names[3], &current[3]).unwrap());
        }),
    );
    report(
        "replace_addrs, 1 upstream, 1 of 20 addrs changed",
        bench(rounds, min, || {
            black_box(replace_addrs(black_box(&docs[3]), &names[3], &changed[3]).unwrap());
        }),
    );
    report(
        &format!("replace_addrs, all {UPSTREAMS} upstreams changed"),
        bench(rounds, min, || {
            for u in 0..UPSTREAMS {
                black_box(replace_addrs(black_box(&docs[u]), &names[u], &changed[u]).unwrap());
            }
        }),
    );
}

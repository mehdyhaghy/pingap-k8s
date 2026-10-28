//! Metrics, recorded through the OpenTelemetry metrics API.
//!
//! Every `Metrics` owns its own `SdkMeterProvider` and Prometheus registry
//! (never a process-global one), so tests and multiple instances do not
//! interfere with each other. The Prometheus reader is always attached and
//! served by `server.rs` on the in-cluster metrics port; `telemetry.rs` adds
//! the OTLP reader to the same provider when an endpoint is configured.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, MeterProvider as _};
use opentelemetry_sdk::metrics::{MeterProviderBuilder, SdkMeterProvider};
use prometheus::proto::MetricType;
use prometheus::{Encoder, Registry, TextEncoder};

use crate::events::{Events, Notice};

/// Buckets for `pingap_k8s_event_to_write_seconds`, 50 ms to 30 s. Dense
/// around the default debounce window (500 ms .. 2 s).
pub const EVENT_TO_WRITE_BUCKETS: &[f64] = &[
    0.05, 0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0, 20.0, 30.0,
];

/// `class` label of `pingap_k8s_endpoints`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EndpointClass {
    Ready,
    Draining,
}

impl EndpointClass {
    pub fn as_str(self) -> &'static str {
        match self {
            EndpointClass::Ready => "ready",
            EndpointClass::Draining => "draining",
        }
    }
}

/// `result` label of `pingap_k8s_writes_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteResult {
    Ok,
    Conflict,
    Error,
    Unchanged,
}

impl WriteResult {
    pub fn as_str(self) -> &'static str {
        match self {
            WriteResult::Ok => "ok",
            WriteResult::Conflict => "conflict",
            WriteResult::Error => "error",
            WriteResult::Unchanged => "unchanged",
        }
    }
}

const NAMESPACE: &str = "k8s.namespace.name";
const SERVICE: &str = "k8s.service.name";

/// Current gauge values.
///
/// OpenTelemetry gauges cannot delete a series, so every gauge is an
/// observable gauge whose callback reports this map: removing an entry
/// (`remove_upstream`, `remove_watch`) makes the series disappear from the
/// next collection on both `/metrics` and OTLP.
#[derive(Default)]
struct Gauges {
    endpoints: BTreeMap<(String, &'static str), u64>,
    drain_fallback: BTreeMap<String, u64>,
    watch_up: BTreeMap<(String, String), u64>,
    is_leader: u64,
    /// `None` until the first etcd interaction (reported as 0).
    etcd_up: Option<bool>,
}

pub struct Metrics {
    registry: Registry,
    pub(crate) provider: SdkMeterProvider,
    gauges: Arc<Mutex<Gauges>>,
    writes: Counter<u64>,
    foreign_writes: Counter<u64>,
    watch_errors: Counter<u64>,
    event_to_write: Histogram<f64>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Prometheus reader only, empty resource.
    pub fn new() -> Self {
        Self::build(SdkMeterProvider::builder())
    }

    /// Adds the Prometheus reader to `builder` (which may carry a resource and
    /// other readers), builds the provider and creates the instruments.
    pub fn build(builder: MeterProviderBuilder) -> Self {
        let registry = Registry::new();
        let exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .without_target_info()
            .scope_info_enabled(false)
            .build()
            .expect("prometheus exporter on a fresh registry");
        let provider = builder.with_reader(exporter).build();
        let meter = provider.meter("pingap-k8s");
        let gauges = Arc::new(Mutex::new(Gauges::default()));

        let g = gauges.clone();
        meter
            .u64_observable_gauge("pingap_k8s.endpoints")
            .with_description("Endpoints currently written per upstream, by class.")
            .with_callback(move |o| {
                for ((upstream, class), v) in &lock(&g).endpoints {
                    let attrs = [
                        KeyValue::new("upstream", upstream.clone()),
                        KeyValue::new("class", *class),
                    ];
                    o.observe(*v, &attrs);
                }
            })
            .build();
        let g = gauges.clone();
        meter
            .u64_observable_gauge("pingap_k8s.watch_up")
            .with_description("1 when the Kubernetes watch for a Service is healthy, else 0.")
            .with_callback(move |o| {
                for ((ns, svc), v) in &lock(&g).watch_up {
                    let attrs = [
                        KeyValue::new(NAMESPACE, ns.clone()),
                        KeyValue::new(SERVICE, svc.clone()),
                    ];
                    o.observe(*v, &attrs);
                }
            })
            .build();
        let g = gauges.clone();
        meter
            .u64_observable_gauge("pingap_k8s.drain_fallback")
            .with_description("1 while an upstream is served only by draining endpoints.")
            .with_callback(move |o| {
                for (upstream, v) in &lock(&g).drain_fallback {
                    o.observe(*v, &[KeyValue::new("upstream", upstream.clone())]);
                }
            })
            .build();
        let g = gauges.clone();
        meter
            .u64_observable_gauge("pingap_k8s.is_leader")
            .with_description("1 when this replica holds the leader lease and may write.")
            .with_callback(move |o| o.observe(lock(&g).is_leader, &[]))
            .build();
        let g = gauges.clone();
        meter
            .u64_observable_gauge("pingap_k8s.etcd_up")
            .with_description("1 when the last etcd interaction got an answer, else 0.")
            .with_callback(move |o| o.observe(u64::from(lock(&g).etcd_up == Some(true)), &[]))
            .build();

        Metrics {
            writes: meter
                .u64_counter("pingap_k8s.writes")
                .with_description("Write attempts of upstream addrs to etcd, by result.")
                .build(),
            foreign_writes: meter
                .u64_counter("pingap_k8s.foreign_writes")
                .with_description("Writes to owned upstream keys not made by this controller.")
                .build(),
            watch_errors: meter
                .u64_counter("pingap_k8s.watch_errors")
                .with_description("Kubernetes watch errors per Service, by error type.")
                .build(),
            event_to_write: meter
                .f64_histogram("pingap_k8s.event_to_write")
                .with_unit("s")
                .with_description("Time from the first pending Kubernetes event to the etcd write.")
                .with_boundaries(EVENT_TO_WRITE_BUCKETS.to_vec())
                .build(),
            registry,
            provider,
            gauges,
        }
    }

    /// Prometheus text exposition format of every instrument.
    pub fn encode(&self) -> String {
        let families = self.registry.gather();
        let mut buf = Vec::new();
        if let Err(err) = TextEncoder::new().encode(&families, &mut buf) {
            tracing::error!(error = %err, "encoding metrics failed");
            return String::new();
        }
        String::from_utf8(buf).unwrap_or_default()
    }

    /// Current value of one `/metrics` series with exactly `labels`, 0 when
    /// absent. Histograms are read by `<name>_count`. For tests.
    pub fn value(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        for family in self.registry.gather() {
            let histogram = family.get_field_type() == MetricType::HISTOGRAM;
            let wanted = if histogram {
                name.strip_suffix("_count") == Some(family.name())
            } else {
                name == family.name()
            };
            if !wanted {
                continue;
            }
            for m in family.get_metric() {
                let pairs = m.get_label();
                let matches = pairs.len() == labels.len()
                    && labels
                        .iter()
                        .all(|(k, v)| pairs.iter().any(|p| p.name() == *k && p.value() == *v));
                if matches {
                    return match family.get_field_type() {
                        MetricType::HISTOGRAM => m.get_histogram().get_sample_count(),
                        MetricType::GAUGE => m.get_gauge().get_value() as u64,
                        _ => m.get_counter().get_value() as u64,
                    };
                }
            }
        }
        0
    }

    pub fn record_write(&self, upstream: &str, result: WriteResult) {
        self.writes.add(
            1,
            &[
                KeyValue::new("upstream", upstream.to_string()),
                KeyValue::new("result", result.as_str()),
            ],
        );
    }

    pub fn set_endpoints(&self, upstream: &str, ready: usize, draining: usize) {
        let mut g = lock(&self.gauges);
        for (class, n) in [
            (EndpointClass::Ready, ready),
            (EndpointClass::Draining, draining),
        ] {
            g.endpoints
                .insert((upstream.to_string(), class.as_str()), n as u64);
        }
    }

    pub fn set_drain_fallback(&self, upstream: &str, active: bool) {
        lock(&self.gauges)
            .drain_fallback
            .insert(upstream.to_string(), u64::from(active));
    }

    pub fn record_foreign_write(&self, upstream: &str, corrected: bool) {
        self.foreign_writes.add(
            1,
            &[
                KeyValue::new("upstream", upstream.to_string()),
                KeyValue::new("corrected", corrected),
            ],
        );
    }

    pub fn set_watch_up(&self, namespace: &str, service: &str, up: bool) {
        lock(&self.gauges)
            .watch_up
            .insert((namespace.to_string(), service.to_string()), u64::from(up));
    }

    /// `reason` is the bounded `error.type` attribute.
    pub fn record_watch_error(&self, namespace: &str, service: &str, reason: &str) {
        self.watch_errors.add(
            1,
            &[
                KeyValue::new(NAMESPACE, namespace.to_string()),
                KeyValue::new(SERVICE, service.to_string()),
                KeyValue::new("error.type", reason.to_string()),
            ],
        );
    }

    pub fn observe_event_to_write(&self, elapsed: Duration) {
        self.event_to_write.record(elapsed.as_secs_f64(), &[]);
    }

    pub fn set_leader(&self, leader: bool) {
        lock(&self.gauges).is_leader = u64::from(leader);
    }

    /// Sets `etcd_up` and returns the previous value (`None` before the
    /// first call). Use [`EtcdHealth::report`] rather than this directly.
    pub fn set_etcd_up(&self, up: bool) -> Option<bool> {
        lock(&self.gauges).etcd_up.replace(up)
    }

    /// Drops the per-upstream gauge series of an upstream removed from the
    /// config, so it stops being reported. Counters are kept (they are
    /// cumulative and dropping them would look like a reset).
    pub fn remove_upstream(&self, upstream: &str) {
        let mut g = lock(&self.gauges);
        g.endpoints.retain(|(u, _), _| u != upstream);
        g.drain_fallback.remove(upstream);
    }

    /// Drops the `watch_up` series of a Service that is no longer watched.
    pub fn remove_watch(&self, namespace: &str, service: &str) {
        lock(&self.gauges)
            .watch_up
            .remove(&(namespace.to_string(), service.to_string()));
    }
}

/// The one place that reports etcd reachability (`pingap_k8s_etcd_up`):
/// the gauge holds the result of the most recent etcd interaction
/// of any kind (writes, the owned-key watch, the election's lease grant,
/// keep-alives and campaign), so a follower shows 0 while etcd is down too.
/// `reachable` is true whenever etcd answered, even with an error (a
/// malformed entry, a CAS conflict, permission denied); false for transport
/// errors and timeouts. A change after the first report posts an
/// `EtcdUnreachable` or `EtcdRecovered` Event when Events are on.
///
/// Cheap to clone; every clone built on the same [`Metrics`] shares the state.
#[derive(Clone)]
pub struct EtcdHealth {
    metrics: Arc<Metrics>,
    events: Events,
}

impl EtcdHealth {
    pub fn new(metrics: Arc<Metrics>, events: Events) -> Self {
        Self { metrics, events }
    }

    pub fn report(&self, reachable: bool) {
        match self.metrics.set_etcd_up(reachable) {
            Some(previous) if previous != reachable => {
                if reachable {
                    tracing::info!("etcd is reachable again");
                } else {
                    tracing::warn!("etcd cannot be reached");
                }
                self.events.post(Notice::etcd(reachable));
            }
            _ => {}
        }
    }
}

fn lock(g: &Mutex<Gauges>) -> MutexGuard<'_, Gauges> {
    g.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_everything(m: &Metrics) {
        m.set_endpoints("web", 3, 1);
        m.record_write("web", WriteResult::Ok);
        m.record_write("web", WriteResult::Ok);
        m.record_write("web", WriteResult::Conflict);
        m.record_write("web", WriteResult::Error);
        m.record_write("web", WriteResult::Unchanged);
        m.record_foreign_write("web", false);
        m.set_watch_up("apps", "web", true);
        m.record_watch_error("apps", "web", "forbidden");
        m.observe_event_to_write(Duration::from_millis(700));
        m.set_drain_fallback("web", true);
        m.set_leader(true);
        m.set_etcd_up(false);
    }

    #[test]
    fn encodes_spec_names_and_labels() {
        let m = Metrics::new();
        record_everything(&m);

        let text = m.encode();
        for line in [
            "# TYPE pingap_k8s_endpoints gauge",
            r#"pingap_k8s_endpoints{class="ready",upstream="web"} 3"#,
            r#"pingap_k8s_endpoints{class="draining",upstream="web"} 1"#,
            "# TYPE pingap_k8s_writes_total counter",
            r#"pingap_k8s_writes_total{result="ok",upstream="web"} 2"#,
            r#"pingap_k8s_writes_total{result="conflict",upstream="web"} 1"#,
            r#"pingap_k8s_writes_total{result="error",upstream="web"} 1"#,
            r#"pingap_k8s_writes_total{result="unchanged",upstream="web"} 1"#,
            "# TYPE pingap_k8s_foreign_writes_total counter",
            r#"pingap_k8s_foreign_writes_total{corrected="false",upstream="web"} 1"#,
            "# TYPE pingap_k8s_watch_up gauge",
            r#"pingap_k8s_watch_up{k8s_namespace_name="apps",k8s_service_name="web"} 1"#,
            "# TYPE pingap_k8s_watch_errors_total counter",
            r#"pingap_k8s_watch_errors_total{error_type="forbidden",k8s_namespace_name="apps",k8s_service_name="web"} 1"#,
            "# TYPE pingap_k8s_event_to_write_seconds histogram",
            r#"pingap_k8s_event_to_write_seconds_bucket{le="0.5"} 0"#,
            r#"pingap_k8s_event_to_write_seconds_bucket{le="0.75"} 1"#,
            r#"pingap_k8s_event_to_write_seconds_bucket{le="30"} 1"#,
            "pingap_k8s_event_to_write_seconds_count 1",
            "# TYPE pingap_k8s_drain_fallback gauge",
            r#"pingap_k8s_drain_fallback{upstream="web"} 1"#,
            "# TYPE pingap_k8s_is_leader gauge",
            "pingap_k8s_is_leader 1",
            "# TYPE pingap_k8s_etcd_up gauge",
            "pingap_k8s_etcd_up 0",
        ] {
            assert!(text.contains(line), "missing line {line:?} in:\n{text}");
        }
        // No scope labels, no target_info, nothing beyond the nine families.
        assert!(!text.contains("otel_scope"), "{text}");
        assert!(!text.contains("target_info"), "{text}");
        let families: Vec<&str> = text
            .lines()
            .filter_map(|l| l.strip_prefix("# TYPE "))
            .collect();
        assert_eq!(families.len(), 9, "{families:?}");
    }

    #[test]
    fn value_reads_series() {
        let m = Metrics::new();
        record_everything(&m);
        let w = |r| {
            m.value(
                "pingap_k8s_writes_total",
                &[("upstream", "web"), ("result", r)],
            )
        };
        assert_eq!(w("ok"), 2);
        assert_eq!(w("conflict"), 1);
        assert_eq!(
            m.value(
                "pingap_k8s_endpoints",
                &[("upstream", "web"), ("class", "ready")]
            ),
            3
        );
        assert_eq!(m.value("pingap_k8s_is_leader", &[]), 1);
        assert_eq!(m.value("pingap_k8s_event_to_write_seconds_count", &[]), 1);
        assert_eq!(m.value("pingap_k8s_writes_total", &[("upstream", "x")]), 0);
    }

    #[test]
    fn remove_upstream_drops_gauges_keeps_counters() {
        let m = Metrics::new();
        m.set_endpoints("web", 2, 0);
        m.set_endpoints("api", 1, 0);
        m.set_drain_fallback("web", false);
        m.record_write("web", WriteResult::Ok);
        let before = m.encode();
        assert!(before.contains(r#"pingap_k8s_endpoints{class="ready",upstream="web"} 2"#));
        m.remove_upstream("web");
        m.set_watch_up("apps", "web", true);
        m.set_watch_up("apps", "api", false);
        m.remove_watch("apps", "web");

        let text = m.encode();
        assert!(!text.contains(r#"upstream="web"} 2"#), "{text}");
        assert!(!text.contains("pingap_k8s_drain_fallback{"), "{text}");
        assert!(!text.contains(r#"k8s_service_name="web""#), "{text}");
        assert!(text.contains(r#"pingap_k8s_endpoints{class="ready",upstream="api"} 1"#));
        assert!(text.contains(
            r#"pingap_k8s_watch_up{k8s_namespace_name="apps",k8s_service_name="api"} 0"#
        ));
        assert!(text.contains(r#"pingap_k8s_writes_total{result="ok",upstream="web"} 1"#));

        // A re-added upstream reports again.
        m.set_endpoints("web", 1, 0);
        assert!(
            m.encode()
                .contains(r#"pingap_k8s_endpoints{class="ready",upstream="web"} 1"#)
        );
    }

    #[test]
    fn etcd_health_reports_latest_result_and_posts_changes() {
        let m = Arc::new(Metrics::new());
        let (events, mut rx) = Events::channel(8);
        let a = EtcdHealth::new(m.clone(), events.clone());
        let b = EtcdHealth::new(m.clone(), events);
        // Before the first report: 0, and the first report is not a change.
        assert_eq!(m.value("pingap_k8s_etcd_up", &[]), 0);
        a.report(true);
        assert_eq!(m.value("pingap_k8s_etcd_up", &[]), 1);
        assert!(rx.try_recv().is_err());
        // Any source flips the one gauge; only changes post an Event.
        b.report(false);
        b.report(false);
        assert_eq!(m.value("pingap_k8s_etcd_up", &[]), 0);
        assert_eq!(rx.try_recv().unwrap(), Notice::etcd(false));
        assert!(rx.try_recv().is_err());
        a.report(true);
        assert_eq!(m.value("pingap_k8s_etcd_up", &[]), 1);
        assert_eq!(rx.try_recv().unwrap(), Notice::etcd(true));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn instances_do_not_share_state() {
        let a = Metrics::new();
        let b = Metrics::new();
        a.set_leader(true);
        assert!(a.encode().contains("pingap_k8s_is_leader 1"));
        assert!(b.encode().contains("pingap_k8s_is_leader 0"));
    }

    #[test]
    fn buckets_span_50ms_to_30s() {
        assert_eq!(EVENT_TO_WRITE_BUCKETS.first(), Some(&0.05));
        assert_eq!(EVENT_TO_WRITE_BUCKETS.last(), Some(&30.0));
        assert!(EVENT_TO_WRITE_BUCKETS.windows(2).all(|w| w[0] < w[1]));
    }
}

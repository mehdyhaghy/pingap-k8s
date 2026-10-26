//! Prometheus metrics.
//!
//! `Metrics` owns its own `Registry` (never the process-global default), so
//! tests and multiple instances do not interfere with each other. Served by
//! `server.rs` on the in-cluster metrics port only.

use std::time::Duration;

use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};

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

pub struct Metrics {
    pub registry: Registry,
    /// `pingap_k8s_endpoints{upstream,class}`
    pub endpoints: IntGaugeVec,
    /// `pingap_k8s_writes_total{upstream,result}`
    pub writes_total: IntCounterVec,
    /// `pingap_k8s_foreign_writes_total{upstream,corrected}`
    pub foreign_writes_total: IntCounterVec,
    /// `pingap_k8s_watch_up{namespace,service}`
    pub watch_up: IntGaugeVec,
    /// `pingap_k8s_watch_errors_total{namespace,service,reason}`
    pub watch_errors_total: IntCounterVec,
    /// `pingap_k8s_event_to_write_seconds`
    pub event_to_write_seconds: Histogram,
    /// `pingap_k8s_drain_fallback{upstream}`
    pub drain_fallback: IntGaugeVec,
    /// `pingap_k8s_is_leader`
    pub is_leader: IntGauge,
    /// `pingap_k8s_etcd_up`
    pub etcd_up: IntGauge,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let endpoints = IntGaugeVec::new(
            Opts::new(
                "pingap_k8s_endpoints",
                "Endpoints currently written per upstream, by class.",
            ),
            &["upstream", "class"],
        )
        .expect("valid metric");
        let writes_total = IntCounterVec::new(
            Opts::new(
                "pingap_k8s_writes_total",
                "Write attempts of upstream addrs to etcd, by result.",
            ),
            &["upstream", "result"],
        )
        .expect("valid metric");
        let foreign_writes_total = IntCounterVec::new(
            Opts::new(
                "pingap_k8s_foreign_writes_total",
                "Writes to owned upstream keys not made by this controller.",
            ),
            &["upstream", "corrected"],
        )
        .expect("valid metric");
        let watch_up = IntGaugeVec::new(
            Opts::new(
                "pingap_k8s_watch_up",
                "1 when the Kubernetes watch for a Service is healthy, else 0.",
            ),
            &["namespace", "service"],
        )
        .expect("valid metric");
        let watch_errors_total = IntCounterVec::new(
            Opts::new(
                "pingap_k8s_watch_errors_total",
                "Kubernetes watch errors per Service, by reason.",
            ),
            &["namespace", "service", "reason"],
        )
        .expect("valid metric");
        let event_to_write_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "pingap_k8s_event_to_write_seconds",
                "Time from the first pending Kubernetes event to the etcd write.",
            )
            .buckets(EVENT_TO_WRITE_BUCKETS.to_vec()),
        )
        .expect("valid metric");
        let drain_fallback = IntGaugeVec::new(
            Opts::new(
                "pingap_k8s_drain_fallback",
                "1 while an upstream is served only by draining endpoints.",
            ),
            &["upstream"],
        )
        .expect("valid metric");
        let is_leader = IntGauge::new(
            "pingap_k8s_is_leader",
            "1 when this replica holds the leader lease and may write.",
        )
        .expect("valid metric");
        let etcd_up = IntGauge::new(
            "pingap_k8s_etcd_up",
            "1 when the last etcd operation succeeded, else 0.",
        )
        .expect("valid metric");

        let collectors: [Box<dyn prometheus::core::Collector>; 9] = [
            Box::new(endpoints.clone()),
            Box::new(writes_total.clone()),
            Box::new(foreign_writes_total.clone()),
            Box::new(watch_up.clone()),
            Box::new(watch_errors_total.clone()),
            Box::new(event_to_write_seconds.clone()),
            Box::new(drain_fallback.clone()),
            Box::new(is_leader.clone()),
            Box::new(etcd_up.clone()),
        ];
        for collector in collectors {
            registry
                .register(collector)
                .expect("metric names are unique");
        }

        Metrics {
            registry,
            endpoints,
            writes_total,
            foreign_writes_total,
            watch_up,
            watch_errors_total,
            event_to_write_seconds,
            drain_fallback,
            is_leader,
            etcd_up,
        }
    }

    /// Prometheus text exposition format of everything in this registry.
    pub fn encode(&self) -> String {
        let families = self.registry.gather();
        let mut buf = Vec::new();
        if let Err(err) = TextEncoder::new().encode(&families, &mut buf) {
            tracing::error!(error = %err, "encoding metrics failed");
            return String::new();
        }
        String::from_utf8(buf).unwrap_or_default()
    }

    pub fn record_write(&self, upstream: &str, result: WriteResult) {
        self.writes_total
            .with_label_values(&[upstream, result.as_str()])
            .inc();
    }

    pub fn set_endpoints(&self, upstream: &str, ready: usize, draining: usize) {
        self.endpoints
            .with_label_values(&[upstream, EndpointClass::Ready.as_str()])
            .set(to_i64(ready));
        self.endpoints
            .with_label_values(&[upstream, EndpointClass::Draining.as_str()])
            .set(to_i64(draining));
    }

    pub fn set_drain_fallback(&self, upstream: &str, active: bool) {
        self.drain_fallback
            .with_label_values(&[upstream])
            .set(i64::from(active));
    }

    pub fn record_foreign_write(&self, upstream: &str, corrected: bool) {
        let corrected = if corrected { "true" } else { "false" };
        self.foreign_writes_total
            .with_label_values(&[upstream, corrected])
            .inc();
    }

    pub fn set_watch_up(&self, namespace: &str, service: &str, up: bool) {
        self.watch_up
            .with_label_values(&[namespace, service])
            .set(i64::from(up));
    }

    pub fn record_watch_error(&self, namespace: &str, service: &str, reason: &str) {
        self.watch_errors_total
            .with_label_values(&[namespace, service, reason])
            .inc();
    }

    pub fn observe_event_to_write(&self, elapsed: Duration) {
        self.event_to_write_seconds.observe(elapsed.as_secs_f64());
    }

    pub fn set_leader(&self, leader: bool) {
        self.is_leader.set(i64::from(leader));
    }

    pub fn set_etcd_up(&self, up: bool) {
        self.etcd_up.set(i64::from(up));
    }

    /// Drops the per-upstream gauge series of an upstream removed from the
    /// config, so it stops being reported. Counters are kept (they are
    /// cumulative and dropping them would look like a reset).
    pub fn remove_upstream(&self, upstream: &str) {
        for class in [EndpointClass::Ready, EndpointClass::Draining] {
            let _ = self
                .endpoints
                .remove_label_values(&[upstream, class.as_str()]);
        }
        let _ = self.drain_fallback.remove_label_values(&[upstream]);
    }

    /// Drops the `watch_up` series of a Service that is no longer watched.
    pub fn remove_watch(&self, namespace: &str, service: &str) {
        let _ = self.watch_up.remove_label_values(&[namespace, service]);
    }
}

fn to_i64(n: usize) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_every_metric() {
        let m = Metrics::new();
        m.set_endpoints("web", 3, 1);
        m.record_write("web", WriteResult::Ok);
        m.record_foreign_write("web", true);
        m.set_watch_up("apps", "web", true);
        m.record_watch_error("apps", "web", "forbidden");
        m.observe_event_to_write(Duration::from_millis(700));
        m.set_drain_fallback("web", false);
        m.set_leader(true);
        m.set_etcd_up(true);

        let names: Vec<String> = m
            .registry
            .gather()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        for expected in [
            "pingap_k8s_endpoints",
            "pingap_k8s_writes_total",
            "pingap_k8s_foreign_writes_total",
            "pingap_k8s_watch_up",
            "pingap_k8s_watch_errors_total",
            "pingap_k8s_event_to_write_seconds",
            "pingap_k8s_drain_fallback",
            "pingap_k8s_is_leader",
            "pingap_k8s_etcd_up",
        ] {
            assert!(names.iter().any(|n| n == expected), "missing {expected}");
        }
    }

    #[test]
    fn encodes_text_format_with_spec_labels() {
        let m = Metrics::new();
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
            r#"pingap_k8s_foreign_writes_total{corrected="false",upstream="web"} 1"#,
            r#"pingap_k8s_watch_up{namespace="apps",service="web"} 1"#,
            r#"pingap_k8s_watch_errors_total{namespace="apps",reason="forbidden",service="web"} 1"#,
            "# TYPE pingap_k8s_event_to_write_seconds histogram",
            r#"pingap_k8s_event_to_write_seconds_bucket{le="0.5"} 0"#,
            r#"pingap_k8s_event_to_write_seconds_bucket{le="0.75"} 1"#,
            r#"pingap_k8s_event_to_write_seconds_bucket{le="30"} 1"#,
            "pingap_k8s_event_to_write_seconds_count 1",
            r#"pingap_k8s_drain_fallback{upstream="web"} 1"#,
            "pingap_k8s_is_leader 1",
            "pingap_k8s_etcd_up 0",
        ] {
            assert!(text.contains(line), "missing line {line:?} in:\n{text}");
        }
    }

    #[test]
    fn remove_upstream_drops_gauges_keeps_counters() {
        let m = Metrics::new();
        m.set_endpoints("web", 2, 0);
        m.set_drain_fallback("web", false);
        m.record_write("web", WriteResult::Ok);
        m.remove_upstream("web");
        m.set_watch_up("apps", "web", true);
        m.remove_watch("apps", "web");

        let text = m.encode();
        assert!(!text.contains("pingap_k8s_endpoints{"));
        assert!(!text.contains("pingap_k8s_drain_fallback{"));
        assert!(!text.contains("pingap_k8s_watch_up{"));
        assert!(text.contains(r#"pingap_k8s_writes_total{result="ok",upstream="web"} 1"#));
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

//! OpenTelemetry setup: one `MeterProvider` with the Prometheus
//! reader always on, plus OTLP/gRPC metrics and logs when
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Configuration is the standard
//! `OTEL_*` environment only; the endpoint must be a private or in-cluster
//! address.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use opentelemetry::{Key, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::tonic_types::transport::ClientTlsConfig;
use opentelemetry_otlp::{LogExporter, MetricExporter, WithTonicConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::filter_fn;

use crate::metrics::Metrics;

const DEFAULT_SERVICE_NAME: &str = "pingap-k8s";
/// Upper bound for flushing both providers on exit.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
/// Log targets never sent over OTLP: the SDK and the exporter's own transport
/// would otherwise feed their export logs back into the export.
const OTLP_EXCLUDED_TARGETS: &[&str] = &["opentelemetry", "tonic", "h2", "hyper", "tower"];

pub struct Telemetry {
    pub metrics: Arc<Metrics>,
    logs: Option<SdkLoggerProvider>,
}

/// The OTLP endpoint, when OTLP export is configured and not disabled.
pub fn otlp_endpoint() -> Option<String> {
    let disabled =
        std::env::var("OTEL_SDK_DISABLED").is_ok_and(|v| v.trim().eq_ignore_ascii_case("true"));
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok()?;
    (!disabled && !endpoint.trim().is_empty()).then_some(endpoint)
}

impl Telemetry {
    /// Must run inside the Tokio runtime (the gRPC channel is created here).
    /// `otlp: false` keeps everything local (used by `--check`).
    pub fn init(instance_id: String, otlp: bool) -> anyhow::Result<Self> {
        let resource = resource(instance_id);
        let mut meters = SdkMeterProvider::builder().with_resource(resource.clone());
        let mut logs = None;
        if let Some(endpoint) = otlp.then(otlp_endpoint).flatten() {
            // Plain `http://` unless the endpoint asks for TLS.
            let tls = endpoint
                .starts_with("https://")
                .then(|| ClientTlsConfig::new().with_native_roots());
            let mut metric_exporter = MetricExporter::builder().with_tonic();
            let mut log_exporter = LogExporter::builder().with_tonic();
            if let Some(tls) = tls {
                metric_exporter = metric_exporter.with_tls_config(tls.clone());
                log_exporter = log_exporter.with_tls_config(tls);
            }
            let metric_exporter = metric_exporter.build().context("OTLP metric exporter")?;
            let log_exporter = log_exporter.build().context("OTLP log exporter")?;
            meters = meters.with_reader(PeriodicReader::builder(metric_exporter).build());
            logs = Some(
                SdkLoggerProvider::builder()
                    .with_resource(resource)
                    .with_batch_exporter(log_exporter)
                    .build(),
            );
        }
        Ok(Telemetry {
            metrics: Arc::new(Metrics::build(meters)),
            logs,
        })
    }

    /// The `tracing` layer that sends events as OTLP log records, if enabled.
    pub fn log_layer<S>(&self) -> Option<impl Layer<S> + use<S>>
    where
        S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    {
        let provider = self.logs.as_ref()?;
        Some(
            OpenTelemetryTracingBridge::new(provider).with_filter(filter_fn(|meta| {
                !OTLP_EXCLUDED_TARGETS
                    .iter()
                    .any(|t| meta.target().starts_with(t))
            })),
        )
    }

    /// Flushes and stops both providers, bounded by [`SHUTDOWN_TIMEOUT`].
    pub async fn shutdown(self) {
        let provider = self.metrics.provider.clone();
        let logs = self.logs;
        let flush = tokio::task::spawn_blocking(move || {
            if let Some(logs) = logs {
                let _ = logs.shutdown_with_timeout(SHUTDOWN_TIMEOUT);
            }
            let _ = provider.shutdown_with_timeout(SHUTDOWN_TIMEOUT);
        });
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, flush).await.is_err() {
            tracing::warn!("telemetry flush did not finish within {SHUTDOWN_TIMEOUT:?}");
        }
    }
}

/// `service.name` (default `pingap-k8s`, `OTEL_SERVICE_NAME` wins),
/// `service.version`, `service.instance.id`, plus `OTEL_RESOURCE_ATTRIBUTES`
/// through the SDK's environment detector.
fn resource(instance_id: String) -> Resource {
    let detected = Resource::builder().build();
    let named = detected
        .get(&Key::from_static_str("service.name"))
        .is_some_and(|v| !v.as_str().starts_with("unknown_service"));
    let builder = Resource::builder().with_attributes([
        KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
        KeyValue::new("service.instance.id", instance_id),
    ]);
    if named {
        builder.build()
    } else {
        builder.with_service_name(DEFAULT_SERVICE_NAME).build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attr(r: &Resource, key: &'static str) -> Option<String> {
        r.get(&Key::from_static_str(key)).map(|v| v.to_string())
    }

    #[test]
    fn resource_has_service_identity() {
        let r = resource("pingap-k8s-abc".into());
        let expected_name = std::env::var("OTEL_SERVICE_NAME")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_SERVICE_NAME.into());
        assert_eq!(attr(&r, "service.name"), Some(expected_name));
        assert_eq!(
            attr(&r, "service.version").as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(
            attr(&r, "service.instance.id").as_deref(),
            Some("pingap-k8s-abc")
        );
    }

    #[tokio::test]
    async fn local_only_init_serves_metrics_and_shuts_down() {
        let t = Telemetry::init("x".into(), false).unwrap();
        assert!(t.logs.is_none());
        assert!(
            t.log_layer::<tracing_subscriber::Registry>().is_none(),
            "no OTLP layer without an endpoint"
        );
        t.metrics.set_leader(true);
        assert!(t.metrics.encode().contains("pingap_k8s_is_leader 1"));
        t.shutdown().await;
    }
}

//! OTLP export against an in-process OTLP/gRPC receiver on 127.0.0.1:
//! metrics and log records arrive with the service resource, and the exporter's
//! own transport logs are never exported. The same checks as
//! `scripts/otel-local.sh`, without docker or a cluster.
//!
//! One test per binary: it sets `OTEL_*` variables before any thread starts.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::{
    MetricsService, MetricsServiceServer,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::resource::v1::Resource;
use pingap_k8s::metrics::WriteResult;
use pingap_k8s::telemetry::{Telemetry, otlp_endpoint};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};
use tracing_subscriber::prelude::*;

#[derive(Default)]
struct Received {
    metrics: Vec<ExportMetricsServiceRequest>,
    logs: Vec<ExportLogsServiceRequest>,
}

#[derive(Clone, Default)]
struct Receiver(Arc<Mutex<Received>>);

#[tonic::async_trait]
impl MetricsService for Receiver {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        self.0.lock().unwrap().metrics.push(request.into_inner());
        Ok(Response::new(ExportMetricsServiceResponse::default()))
    }
}

#[tonic::async_trait]
impl LogsService for Receiver {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        self.0.lock().unwrap().logs.push(request.into_inner());
        Ok(Response::new(ExportLogsServiceResponse::default()))
    }
}

fn string(v: &Option<AnyValue>) -> Option<&str> {
    match v.as_ref()?.value.as_ref()? {
        any_value::Value::StringValue(s) => Some(s),
        _ => None,
    }
}

fn attr<'a>(attrs: &'a [KeyValue], key: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| string(&kv.value))
}

fn assert_resource(resource: &Option<Resource>) {
    let attrs = &resource.as_ref().expect("resource").attributes;
    assert_eq!(attr(attrs, "service.name"), Some("pingap-k8s"));
    assert_eq!(
        attr(attrs, "service.version"),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(attr(attrs, "service.instance.id"), Some("otlp-test-pod"));
    assert_eq!(attr(attrs, "k8s.namespace.name"), Some("otlp-test"));
}

#[test]
fn metrics_and_logs_reach_an_otlp_receiver() {
    // No other thread exists yet, so changing the environment is sound.
    unsafe {
        std::env::remove_var("OTEL_SDK_DISABLED");
        std::env::remove_var("OTEL_SERVICE_NAME");
        std::env::set_var("OTEL_RESOURCE_ATTRIBUTES", "k8s.namespace.name=otlp-test");
        std::env::set_var("OTEL_METRIC_EXPORT_INTERVAL", "60000");
    }
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", std_listener.local_addr().unwrap());
    unsafe { std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", &endpoint) };
    assert_eq!(otlp_endpoint().as_deref(), Some(endpoint.as_str()));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let received = Receiver::default();
    runtime.block_on(async {
        std_listener.set_nonblocking(true).unwrap();
        let listener = TcpListener::from_std(std_listener).unwrap();
        let server = tonic::transport::Server::builder()
            .add_service(MetricsServiceServer::new(received.clone()))
            .add_service(LogsServiceServer::new(received.clone()))
            .serve_with_incoming(TcpListenerStream::new(listener));
        tokio::spawn(server);

        let telemetry = Telemetry::init("otlp-test-pod".into(), true).unwrap();
        let subscriber = tracing_subscriber::registry().with(telemetry.log_layer());
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "pingap_k8s::test", "controller log line");
            tracing::info!(target: "tonic::transport", "transport log line");
            tracing::info!(target: "h2::proto", "h2 log line");
        });
        telemetry.metrics.set_leader(true);
        telemetry.metrics.record_write("web", WriteResult::Ok);
        telemetry.metrics.set_endpoints("web", 3, 1);
        // Shutdown flushes both providers: the export happens here.
        tokio::time::timeout(Duration::from_secs(10), telemetry.shutdown())
            .await
            .expect("telemetry shutdown");
    });

    let received = received.0.lock().unwrap();
    assert!(!received.metrics.is_empty(), "no metrics exported");
    let mut names = Vec::new();
    for req in &received.metrics {
        for rm in &req.resource_metrics {
            assert_resource(&rm.resource);
            for sm in &rm.scope_metrics {
                names.extend(sm.metrics.iter().map(|m| m.name.clone()));
            }
        }
    }
    for name in [
        "pingap_k8s.is_leader",
        "pingap_k8s.writes",
        "pingap_k8s.endpoints",
    ] {
        assert!(
            names.iter().any(|n| n == name),
            "{name} missing from {names:?}"
        );
    }

    assert!(!received.logs.is_empty(), "no log records exported");
    let mut bodies = Vec::new();
    for req in &received.logs {
        for rl in &req.resource_logs {
            assert_resource(&rl.resource);
            for sl in &rl.scope_logs {
                bodies.extend(sl.log_records.iter().filter_map(|r| string(&r.body)));
            }
        }
    }
    assert!(bodies.contains(&"controller log line"), "{bodies:?}");
    assert!(
        !bodies
            .iter()
            .any(|b| b.contains("transport") || b.contains("h2 log")),
        "exporter-internal targets were exported: {bodies:?}"
    );
}

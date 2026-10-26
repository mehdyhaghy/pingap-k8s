#!/usr/bin/env bash
# OTLP check: runs scripts/smoke-local.sh with the binary exporting to a local
# OpenTelemetry Collector (docker, published on 127.0.0.1 only, debug exporter)
# and checks that metrics and log records with the expected resource arrive.
#
# Usage: scripts/otel-local.sh
# Env:   OTEL_COLLECTOR_IMAGE (default pinned below), plus smoke-local.sh's env.
set -euo pipefail

IMAGE="${OTEL_COLLECTOR_IMAGE:-otel/opentelemetry-collector:0.163.2}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pingap-k8s-otel.XXXXXX")"
NAME="pingap-k8s-otel-$$"

cleanup() {
  local code=$?
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit $code
}
trap cleanup EXIT INT TERM

cat >"$WORK/collector.yaml" <<'EOF'
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    metrics: { receivers: [otlp], exporters: [debug] }
    logs: { receivers: [otlp], exporters: [debug] }
EOF

port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
echo "== collector $IMAGE on 127.0.0.1:$port"
docker run -d --name "$NAME" -p "127.0.0.1:${port}:4317" \
  -v "$WORK/collector.yaml:/etc/otelcol/config.yaml:ro" "$IMAGE" \
  --config /etc/otelcol/config.yaml >/dev/null
for _ in $(seq 1 100); do
  docker logs "$NAME" 2>&1 | grep -q "Everything is ready" && break
  sleep 0.2
done
docker logs "$NAME" 2>&1 | grep -q "Everything is ready" || { docker logs "$NAME"; exit 1; }

OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:${port}" \
  OTEL_METRIC_EXPORT_INTERVAL=2000 \
  OTEL_RESOURCE_ATTRIBUTES="k8s.namespace.name=otel-local,k8s.pod.name=smoke" \
  "$ROOT/scripts/smoke-local.sh"

docker logs "$NAME" >"$WORK/collector.log" 2>&1
check() {
  grep -qF -- "$1" "$WORK/collector.log" || { echo "FAIL: collector did not receive: $1" >&2; exit 1; }
  echo "PASS: $1"
}
echo; echo "== collector received"
check "service.name: Str(pingap-k8s)"
check "k8s.namespace.name: Str(otel-local)"
check "service.instance.id:"
for m in endpoints writes foreign_writes watch_up event_to_write drain_fallback is_leader etcd_up; do
  check "-> Name: pingap_k8s.$m"
done
check "k8s.service.name: Str("
check "LogRecord #0"
check "Body: Str(connected to etcd)"
check "Body: Str(stopped)"
# The tracing target becomes the log scope; the exporter's own stack must not appear.
if grep -qE "^InstrumentationScope (opentelemetry|tonic|h2|hyper|tower)" "$WORK/collector.log"; then
  echo "FAIL: exporter-internal logs were exported" >&2; exit 1
fi
echo "PASS: no exporter-internal log records"
echo "metric points: $(grep -c 'NumberDataPoints\|HistogramDataPoints' "$WORK/collector.log"), log records: $(grep -c 'LogRecord #' "$WORK/collector.log")"

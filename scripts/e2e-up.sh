#!/usr/bin/env bash
# Brings up the end-to-end environment on kind:
#   - kind cluster $E2E_CLUSTER (node image $KIND_NODE_IMAGE when set);
#   - the controller image built from the Dockerfile and loaded into kind;
#   - namespace e2e-app with the echo, podinfo (gRPC) and httpbin (SSE)
#     Deployments/Services and the client pods (tests/e2e/manifests/app.yaml);
#   - the Helm chart (release pingap-k8s, namespace pingap-k8s) with the bundled
#     etcd and the quickstart Pingap (tests/e2e/values.yaml, a variant of
#     charts/pingap-k8s/ci/quickstart-values.yaml), upstreams "echo" and "echo-ws"
#     following echo.e2e-app, "grpc" following podinfo.e2e-app and "httpbin"
#     following httpbin.e2e-app.
# Then writes target/e2e/state.env (KEY=VALUE, shell sourceable) and
# target/e2e/kubeconfig for the tests (cargo test --test e2e).
#
# etcd, the Pingap admin and the metrics endpoint stay ClusterIP: the tests
# reach them with `kubectl exec` from inside the cluster, never through a
# host port.
#
# Env:
#   E2E_CLUSTER          kind cluster name (default pk8s-e2e)
#   KIND_NODE_IMAGE      kindest/node image (default: kind's default)
#   KIND_CONFIG          kind cluster config file (default: none), e.g. one with
#                        networking.ipFamily: dual for a dual-stack cluster
#   E2E_IMAGE            controller image tag to build and load (default pingap-k8s:e2e)
#   E2E_SKIP_BUILD=1     do not build the image (it must already exist locally)
#   E2E_WAIT_CONTROLLER  1: also wait for the controller Deployment to be Ready
#                        (default 1; 0 skips it, e.g. to debug a broken controller)
#   E2E_TIMEOUT          per-wait timeout (default 300s)
#   E2E_PRELOAD          1 (default): load the third-party images that exist in the
#                        local Docker cache into kind instead of pulling them in the
#                        cluster (slow, and rate-limited on Docker Hub); 0: always pull
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${E2E_CLUSTER:-pk8s-e2e}"
IMAGE="${E2E_IMAGE:-pingap-k8s:e2e}"
TIMEOUT="${E2E_TIMEOUT:-300s}"
WAIT_CONTROLLER="${E2E_WAIT_CONTROLLER:-1}"
STATE_DIR="$ROOT/target/e2e"
KUBECONFIG_FILE="$STATE_DIR/kubeconfig"
STATE_FILE="$STATE_DIR/state.env"

RELEASE=pingap-k8s
NS=pingap-k8s
APP_NS=e2e-app

log() { printf '[e2e-up] %s\n' "$*" >&2; }

for tool in kind docker kubectl helm; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }
done

mkdir -p "$STATE_DIR"
rm -f "$STATE_FILE"
export KUBECONFIG="$KUBECONFIG_FILE"

# --- cluster -----------------------------------------------------------------
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  log "deleting leftover cluster $CLUSTER"
  kind delete cluster --name "$CLUSTER" >/dev/null
fi
create_args=(--name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 180s)
if [[ -n "${KIND_NODE_IMAGE:-}" ]]; then
  create_args+=(--image "$KIND_NODE_IMAGE")
fi
if [[ -n "${KIND_CONFIG:-}" ]]; then
  create_args+=(--config "$KIND_CONFIG")
fi
log "creating kind cluster $CLUSTER ${KIND_NODE_IMAGE:+($KIND_NODE_IMAGE)} ${KIND_CONFIG:+(config $KIND_CONFIG)}"
kind create cluster "${create_args[@]}"

# --- controller image ----------------------------------------------------------
if [[ "${E2E_SKIP_BUILD:-0}" != "1" ]]; then
  log "building $IMAGE"
  docker build -t "$IMAGE" "$ROOT"
fi
log "loading $IMAGE into kind"
kind load docker-image "$IMAGE" --name "$CLUSTER"

# Third-party images of the chart (etcd, Pingap) and of the test app, as pinned in
# charts/pingap-k8s/values.yaml and tests/e2e/manifests/app.yaml. Loaded only when
# already cached locally, and only the node's platform: `kind load` imports with
# --all-platforms, which fails for a multi-platform image pulled for one platform.
# A failed import falls back to the in-cluster pull.
if [[ "${E2E_PRELOAD:-1}" == "1" ]]; then
  node="$CLUSTER-control-plane"
  platform="linux/$(docker version -f '{{.Server.Arch}}')"
  for img in quay.io/coreos/etcd:v3.6.12 vicanso/pingap:0.16.2 \
      registry.k8s.io/e2e-test-images/agnhost:2.66.1 jmalloc/echo-server:v0.3.7 \
      fortio/fortio:1.75.3 ghcr.io/stefanprodan/podinfo:6.15.0 \
      mccutchen/go-httpbin:2.25.0 registry.k8s.io/gateway-api/conformance/echo-basic:v0.1.0; do
    if docker image inspect "$img" >/dev/null 2>&1; then
      log "preloading $img ($platform)"
      docker save "$img" | docker exec -i "$node" \
          ctr -n k8s.io images import --platform "$platform" --digests \
          --snapshotter=overlayfs - >/dev/null 2>&1 \
        || log "preloading $img failed; it will be pulled"
    fi
  done
fi

# --- Gateway API CRDs (before the chart: it creates a GatewayClass) -------------
# The Standard channel of the release the generated types come from
# (scripts/gen-gateway-api.sh), checked against its pinned digest.
GATEWAY_API_VERSION=v1.7.0
GATEWAY_API_SHA256=4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1
gateway_crds="$ROOT/target/e2e/gateway-api-$GATEWAY_API_VERSION-standard-install.yaml"
mkdir -p "$(dirname "$gateway_crds")"
if [[ ! -s "$gateway_crds" ]] || ! echo "$GATEWAY_API_SHA256  $gateway_crds" | shasum -a 256 -c - >/dev/null 2>&1; then
  log "downloading the Gateway API $GATEWAY_API_VERSION CRDs"
  curl -fsSL -o "$gateway_crds.tmp" \
    "https://github.com/kubernetes-sigs/gateway-api/releases/download/$GATEWAY_API_VERSION/standard-install.yaml"
  echo "$GATEWAY_API_SHA256  $gateway_crds.tmp" | shasum -a 256 -c - >/dev/null
  mv "$gateway_crds.tmp" "$gateway_crds"
fi
log "installing the Gateway API $GATEWAY_API_VERSION CRDs"
kubectl apply --server-side -f "$gateway_crds" >/dev/null
kubectl wait --for=condition=Established --timeout=60s \
  crd/gatewayclasses.gateway.networking.k8s.io crd/gateways.gateway.networking.k8s.io \
  crd/httproutes.gateway.networking.k8s.io crd/referencegrants.gateway.networking.k8s.io

# --- test app (before the chart: its RoleBinding goes into e2e-app) -------------
log "applying the test app"
kubectl apply -f "$ROOT/tests/e2e/manifests/app.yaml"

# --- chart --------------------------------------------------------------------
# Pingap admin credentials (pingap.admin.existingSecret in tests/e2e/values.yaml).
ADMIN_USER=admin
ADMIN_PASSWORD=e2e-admin-password
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NS" create secret generic "$RELEASE-pingap-admin-e2e" \
  --from-literal=user="$ADMIN_USER" --from-literal=password="$ADMIN_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

log "installing the chart"
# E2E_PINGAP_TAG overrides the pinned Pingap image tag (CI uses "latest" weekly to
# catch storage-format changes early). It must be a Debian-based tag:
# the seed init container needs a shell.
pingap_tag=()
if [[ -n "${E2E_PINGAP_TAG:-}" ]]; then
  pingap_tag=(--set "pingap.image.tag=$E2E_PINGAP_TAG")
fi
helm upgrade --install "$RELEASE" "$ROOT/charts/pingap-k8s" \
  --namespace "$NS" --create-namespace \
  -f "$ROOT/tests/e2e/values.yaml" \
  --set "image.repository=${IMAGE%:*}" \
  --set "image.tag=${IMAGE##*:}" \
  "${pingap_tag[@]}"

# --- readiness ------------------------------------------------------------------
# On a failed wait, show what the cluster was doing before the script exits.
diagnose() {
  local status=$?
  if [[ $status -ne 0 ]]; then
    log "setup failed ($status); cluster state:"
    kubectl get pods -A -o wide >&2 || true
    kubectl get events -A --sort-by=.lastTimestamp 2>/dev/null | tail -40 >&2 || true
    for ns in "$NS" "$APP_NS"; do
      for pod in $(kubectl -n "$ns" get pods --no-headers 2>/dev/null \
          | awk '$2 !~ /^([0-9]+)\/\1$/ {print $1}'); do
        log "not ready: $ns/$pod"
        kubectl -n "$ns" describe pod "$pod" 2>/dev/null | tail -25 >&2 || true
        kubectl -n "$ns" logs "$pod" --all-containers --tail=30 >&2 2>/dev/null || true
      done
    done
  fi
}
trap diagnose EXIT
log "waiting for etcd"
kubectl -n "$NS" rollout status "statefulset/$RELEASE-etcd" --timeout "$TIMEOUT"
log "waiting for Pingap"
kubectl -n "$NS" rollout status "deployment/$RELEASE-pingap" --timeout "$TIMEOUT"
log "waiting for the test app"
kubectl -n "$APP_NS" rollout status deployment/echo --timeout "$TIMEOUT"
kubectl -n "$APP_NS" rollout status deployment/podinfo --timeout "$TIMEOUT"
kubectl -n "$APP_NS" rollout status deployment/httpbin --timeout "$TIMEOUT"
kubectl -n "$APP_NS" wait --for=condition=Ready pod/e2e-client pod/e2e-load --timeout "$TIMEOUT"
if [[ "$WAIT_CONTROLLER" == "1" ]]; then
  log "waiting for the controller"
  kubectl -n "$NS" rollout status "deployment/$RELEASE" --timeout "$TIMEOUT"
else
  log "not waiting for the controller (E2E_WAIT_CONTROLLER=0)"
fi

trap - EXIT

ETCD_POD="$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=etcd,app.kubernetes.io/instance=$RELEASE \
  -o jsonpath='{.items[0].metadata.name}')"

# --- state file -----------------------------------------------------------------
cat >"$STATE_FILE" <<EOF
# Written by scripts/e2e-up.sh. In-cluster URLs are reached from the client pods.
E2E_CLUSTER=$CLUSTER
E2E_KUBECONFIG=$KUBECONFIG_FILE
E2E_IMAGE=$IMAGE
E2E_NODE_IMAGE=${KIND_NODE_IMAGE:-}
E2E_RELEASE=$RELEASE
E2E_NAMESPACE=$NS
E2E_APP_NAMESPACE=$APP_NS
E2E_CONTROLLER_DEPLOYMENT=$RELEASE
E2E_CONTROLLER_REPLICAS=2
E2E_CONTROLLER_METRICS_URL=http://$RELEASE-metrics.$NS.svc:9090
E2E_ETCD_STATEFULSET=$RELEASE-etcd
E2E_ETCD_POD=$ETCD_POD
E2E_ETCD_PREFIX=/pingap
E2E_PINGAP_DEPLOYMENT=$RELEASE-pingap
E2E_PINGAP_SERVICE=$RELEASE-pingap
E2E_PINGAP_URL=http://$RELEASE-pingap.$NS.svc:80
E2E_PINGAP_ADMIN_URL=http://$RELEASE-pingap-admin.$NS.svc:3018
E2E_PINGAP_ADMIN_USER=$ADMIN_USER
E2E_PINGAP_ADMIN_PASSWORD=$ADMIN_PASSWORD
E2E_PINGAP_WS_PATH=/ws
E2E_UPSTREAM=echo
E2E_WS_UPSTREAM=echo-ws
E2E_ECHO_DEPLOYMENT=echo
E2E_ECHO_SERVICE=echo
E2E_CLIENT_POD=e2e-client
E2E_LOAD_POD=e2e-load
EOF
log "state written to $STATE_FILE"
log "run the tests: PINGAP_K8S_E2E=1 cargo test --test e2e -- --test-threads=1"

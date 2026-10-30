#!/usr/bin/env bash
# Runs the official Gateway API conformance suite (profiles GATEWAY-HTTP and
# GATEWAY-GRPC) against the Gateway API provider, on its own kind cluster:
#   - kind cluster $CONF_CLUSTER (default pk8s-conf, never the e2e cluster);
#   - the controller image built from the Dockerfile and loaded into kind;
#   - the Gateway API Standard CRDs (same release and digest as scripts/e2e-up.sh);
#   - the Helm chart (release pingap-k8s, namespace pingap-k8s) with the bundled
#     etcd and the quickstart Pingap seeded with servers on ports 80, 443
#     (global_certificates), 8080 and 8090 (tests/conformance/values.yaml; the
#     plain ones with enabled_h2, for gRPC and h2c), plus the ClusterIP Service
#     pingap-k8s-gateway with those ports: the address the controller writes to
#     the Gateways' status.addresses;
#   - the suite (sigs.k8s.io/gateway-api/conformance at $GATEWAY_API_VERSION),
#     compiled for the node's platform and run in a pod with cluster-admin
#     rights: the host can not reach pod or ClusterIP addresses on every kind
#     setup, the pod can. The supported features are read from the GatewayClass
#     status.supportedFeatures.
# Writes target/conformance/report.yaml (the conformance report) and
# target/conformance/conformance.log (the test output), then prints a summary.
#
# The cluster is kept for inspection (KUBECONFIG=target/conformance/kubeconfig);
# delete it with `kind delete cluster --name pk8s-conf`, or set CONF_DELETE=1.
#
# Env:
#   CONF_CLUSTER       kind cluster name (default pk8s-conf)
#   KIND_NODE_IMAGE    kindest/node image (default: kind's default)
#   CONF_IMAGE         controller image tag to build and load (default pingap-k8s:conformance)
#   CONF_SKIP_BUILD=1  do not build the controller image (it must exist locally)
#   CONF_REUSE=1       keep an existing cluster: only reload the image, upgrade the
#                      chart, restart the controller and run the suite again
#   CONF_PROFILES      comma-separated conformance profiles (--conformance-profiles,
#                      default GATEWAY-HTTP,GATEWAY-GRPC; GATEWAY-HTTP alone for the
#                      HTTP run only)
#   CONF_RUN_TEST      run this one test only (--run-test), e.g. HTTPRouteSimpleSameNamespace
#   CONF_SKIP_TESTS    comma-separated tests to skip (--skip-tests)
#   CONF_DELETE=1      delete the cluster at the end
#   CONF_TIMEOUT       per-wait timeout (default 300s)
#   CONF_TEST_TIMEOUT  timeout of the whole suite run (default 120m)
#   GATEWAY_API_SRC    an existing checkout of kubernetes-sigs/gateway-api at
#                      $GATEWAY_API_VERSION (default: a shallow clone in
#                      target/conformance/gateway-api)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${CONF_CLUSTER:-pk8s-conf}"
IMAGE="${CONF_IMAGE:-pingap-k8s:conformance}"
TIMEOUT="${CONF_TIMEOUT:-300s}"
TEST_TIMEOUT="${CONF_TEST_TIMEOUT:-120m}"
PROFILES="${CONF_PROFILES:-GATEWAY-HTTP,GATEWAY-GRPC}"
OUT="$ROOT/target/conformance"
KUBECONFIG_FILE="$OUT/kubeconfig"

RELEASE=pingap-k8s
NS=pingap-k8s
RUNNER_NS=gateway-conformance-runner
RUNNER_IMAGE=pingap-k8s-conformance:runner
GATEWAY_CLASS=pingap
# The namespaces of conformance/base/manifests.yaml (tests/conformance/values.yaml
# gateway.namespaces): created before the chart, so its Roles can go there.
CONF_NAMESPACES=(gateway-conformance-infra gateway-conformance-app-backend gateway-conformance-web-backend)

# The release the generated types come from (scripts/gen-gateway-api.sh): the
# Standard CRDs with their pinned digest, and the conformance suite at the
# commit of the tag.
GATEWAY_API_VERSION=v1.7.0
GATEWAY_API_SHA256=4e2602c4cbe1afbc394d86f4ac6522e1a5263b01133bba1e40a6d1a549e753a1
GATEWAY_API_COMMIT=e26d183dbbc1bb782b78addbd26faba03955c152

log() { printf '[conformance] %s\n' "$*" >&2; }

for tool in kind docker kubectl helm go git curl; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }
done

mkdir -p "$OUT"
export KUBECONFIG="$KUBECONFIG_FILE"
VERSION="$(sed -n 's/^version = "\(.*\)"$/\1/p' "$ROOT/Cargo.toml" | head -1)"
ARCH="$(docker version -f '{{.Server.Arch}}')"

# --- suite binary (first: a build error should not leave a half-made cluster) --
SRC="${GATEWAY_API_SRC:-$OUT/gateway-api}"
if [[ -z "${GATEWAY_API_SRC:-}" && ! -d "$SRC/.git" ]]; then
  log "cloning kubernetes-sigs/gateway-api $GATEWAY_API_VERSION"
  git clone --quiet --depth 1 --branch "$GATEWAY_API_VERSION" \
    https://github.com/kubernetes-sigs/gateway-api.git "$SRC"
fi
commit="$(git -C "$SRC" rev-parse HEAD)"
if [[ "$commit" != "$GATEWAY_API_COMMIT" ]]; then
  echo "$SRC is at $commit, expected $GATEWAY_API_VERSION ($GATEWAY_API_COMMIT)" >&2
  exit 1
fi
mkdir -p "$OUT/runner"
log "compiling the conformance suite for linux/$ARCH"
(cd "$SRC/conformance" && CGO_ENABLED=0 GOOS=linux GOARCH="$ARCH" \
  go test -c -o "$OUT/runner/conformance.test" .)
cat >"$OUT/runner/Dockerfile" <<'EOF'
FROM alpine:3.22
COPY conformance.test /usr/local/bin/conformance.test
EOF
docker build --quiet -t "$RUNNER_IMAGE" "$OUT/runner" >/dev/null

# --- cluster -----------------------------------------------------------------
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  if [[ "${CONF_REUSE:-0}" == "1" ]]; then
    log "reusing cluster $CLUSTER"
    kind export kubeconfig --name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" >/dev/null 2>&1
  else
    log "deleting leftover cluster $CLUSTER"
    kind delete cluster --name "$CLUSTER" >/dev/null
  fi
fi
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  create_args=(--name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 180s)
  if [[ -n "${KIND_NODE_IMAGE:-}" ]]; then
    create_args+=(--image "$KIND_NODE_IMAGE")
  fi
  log "creating kind cluster $CLUSTER ${KIND_NODE_IMAGE:+($KIND_NODE_IMAGE)}"
  kind create cluster "${create_args[@]}"
fi

# --- images --------------------------------------------------------------------
if [[ "${CONF_SKIP_BUILD:-0}" != "1" ]]; then
  log "building $IMAGE"
  docker build -t "$IMAGE" "$ROOT"
fi
log "loading $IMAGE and the suite runner into kind"
kind load docker-image "$IMAGE" "$RUNNER_IMAGE" --name "$CLUSTER"

# Third-party images of the chart, loaded when already cached locally (only the
# node's platform; see scripts/e2e-up.sh). The rest is pulled in the cluster.
node="$CLUSTER-control-plane"
for img in quay.io/coreos/etcd:v3.6.12 vicanso/pingap:0.16.1; do
  if docker image inspect "$img" >/dev/null 2>&1; then
    docker save "$img" | docker exec -i "$node" \
        ctr -n k8s.io images import --platform "linux/$ARCH" --digests \
        --snapshotter=overlayfs - >/dev/null 2>&1 \
      || log "preloading $img failed; it will be pulled"
  fi
done

# --- Gateway API CRDs ------------------------------------------------------------
gateway_crds="$OUT/gateway-api-$GATEWAY_API_VERSION-standard-install.yaml"
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
  crd/httproutes.gateway.networking.k8s.io crd/grpcroutes.gateway.networking.k8s.io \
  crd/referencegrants.gateway.networking.k8s.io

# --- chart --------------------------------------------------------------------
for ns in "$NS" "${CONF_NAMESPACES[@]}"; do
  kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done
log "installing the chart"
helm upgrade --install "$RELEASE" "$ROOT/charts/pingap-k8s" \
  --namespace "$NS" \
  -f "$ROOT/tests/conformance/values.yaml" \
  --set "image.repository=${IMAGE%:*}" \
  --set "image.tag=${IMAGE##*:}" >/dev/null

# Every listener port of the seed config, for gateway.addresses.
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: $RELEASE-gateway
  namespace: $NS
spec:
  type: ClusterIP
  selector:
    app.kubernetes.io/name: pingap
    app.kubernetes.io/instance: $RELEASE
    app.kubernetes.io/component: proxy
  ports:
    - { name: http, port: 80, targetPort: 80, protocol: TCP }
    - { name: https, port: 443, targetPort: 443, protocol: TCP }
    - { name: http-8080, port: 8080, targetPort: 8080, protocol: TCP }
    - { name: http-8090, port: 8090, targetPort: 8090, protocol: TCP }
EOF

if [[ "${CONF_REUSE:-0}" == "1" ]]; then
  # Same tag, new image: restart to pick it up.
  kubectl -n "$NS" rollout restart "deployment/$RELEASE" >/dev/null
fi

log "waiting for etcd, Pingap and the controller"
kubectl -n "$NS" rollout status "statefulset/$RELEASE-etcd" --timeout "$TIMEOUT"
kubectl -n "$NS" rollout status "deployment/$RELEASE-pingap" --timeout "$TIMEOUT"
kubectl -n "$NS" rollout status "deployment/$RELEASE" --timeout "$TIMEOUT"
log "waiting for the GatewayClass to be accepted"
kubectl wait --for=condition=Accepted --timeout="$TIMEOUT" "gatewayclass/$GATEWAY_CLASS"

# --- suite --------------------------------------------------------------------
# The suite creates namespaces, Gateways, routes and backends, and reads the
# GatewayClass: cluster-admin, in a namespace of its own.
kubectl delete namespace "$RUNNER_NS" --ignore-not-found --wait=true >/dev/null
kubectl create namespace "$RUNNER_NS" >/dev/null
kubectl -n "$RUNNER_NS" create serviceaccount conformance >/dev/null
kubectl create clusterrolebinding gateway-conformance-runner --clusterrole=cluster-admin \
  --serviceaccount="$RUNNER_NS:conformance" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

suite_args=(
  -test.v -test.timeout="$TEST_TIMEOUT" -test.run '^TestConformance$'
  --gateway-class="$GATEWAY_CLASS"
  --conformance-profiles="$PROFILES"
  --report-output=/results/report.yaml
  --organization=pingap-k8s
  --project=pingap-k8s
  --url=https://github.com/mehdyhaghy/pingap-k8s
  --version="v$VERSION"
  --contact=https://github.com/mehdyhaghy/pingap-k8s/issues
)
if [[ -n "${CONF_RUN_TEST:-}" ]]; then
  suite_args+=(--run-test="$CONF_RUN_TEST")
fi
if [[ -n "${CONF_SKIP_TESTS:-}" ]]; then
  suite_args+=(--skip-tests="$CONF_SKIP_TESTS")
fi
# The pod writes the exit code next to the report, then stays up so both can be
# copied out.
args_json="$(printf '"%s",' "${suite_args[@]}")"
kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: conformance
  namespace: $RUNNER_NS
spec:
  serviceAccountName: conformance
  restartPolicy: Never
  containers:
    - name: suite
      image: $RUNNER_IMAGE
      imagePullPolicy: Never
      command: ["sh", "-c", 'conformance.test "\$@"; echo \$? > /results/exit; sleep 86400', "suite"]
      args: [${args_json%,}]
      volumeMounts:
        - { name: results, mountPath: /results }
  volumes:
    - { name: results, emptyDir: {} }
EOF
kubectl -n "$RUNNER_NS" wait --for=condition=Ready pod/conformance --timeout "$TIMEOUT" >/dev/null
log "running the suite ($PROFILES${CONF_RUN_TEST:+, test $CONF_RUN_TEST}); output in $OUT/conformance.log"
kubectl -n "$RUNNER_NS" logs -f pod/conformance >"$OUT/conformance.log" 2>&1 &
logs_pid=$!
until kubectl -n "$RUNNER_NS" exec conformance -- test -f /results/exit 2>/dev/null; do
  sleep 15
done
sleep 2
kill "$logs_pid" 2>/dev/null || true
kubectl -n "$RUNNER_NS" logs pod/conformance >"$OUT/conformance.log" 2>&1
status="$(kubectl -n "$RUNNER_NS" exec conformance -- cat /results/exit)"
rm -f "$OUT/report.yaml"
kubectl -n "$RUNNER_NS" exec conformance -- cat /results/report.yaml >"$OUT/report.yaml" 2>/dev/null \
  || rm -f "$OUT/report.yaml"
kubectl -n "$NS" logs "deployment/$RELEASE" --tail=-1 >"$OUT/controller.log" 2>&1 || true

# --- summary --------------------------------------------------------------------
log "suite exit code: $status"
echo "--- failed tests"
grep -E '^\s*--- FAIL' "$OUT/conformance.log" || echo "(none)"
if [[ -s "$OUT/report.yaml" ]]; then
  echo "--- report ($OUT/report.yaml)"
  sed -n '/^profiles:/,$p' "$OUT/report.yaml"
fi
if [[ "${CONF_DELETE:-0}" == "1" ]]; then
  log "deleting cluster $CLUSTER"
  kind delete cluster --name "$CLUSTER" >/dev/null
else
  log "cluster $CLUSTER kept: export KUBECONFIG=$KUBECONFIG_FILE"
  log "  delete it with: kind delete cluster --name $CLUSTER"
fi
exit "$status"

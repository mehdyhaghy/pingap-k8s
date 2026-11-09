#!/usr/bin/env bash
# In-cluster check of the kubectl plugin against the chart with status.enabled
# on a throwaway kind cluster: controller (2 replicas), bundled etcd
# and the quickstart Pingap. Checks that the status port is not reachable over
# the pod network, then runs `kubectl pingap status`, `explain` and `admin`,
# and the clear error once status is disabled again. The cluster and temp
# files are always removed on exit.
#
# Usage: make docker && scripts/plugin-kind.sh
# Env:   IMAGE (default pingap-k8s:dev), KIND_CLUSTER (default pk8s-plugin)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${IMAGE:-pingap-k8s:dev}"
CLUSTER="${KIND_CLUSTER:-pk8s-plugin}"
S="$(mktemp -d "${TMPDIR:-/tmp}/pingap-k8s-plugin.XXXXXX")"
KCFG="$S/kubeconfig"
cleanup() {
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  rm -rf "$S"
}
trap cleanup EXIT
for tool in kind docker kubectl helm cargo curl; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }
done
docker image inspect "$IMAGE" >/dev/null || { echo "image $IMAGE not found (make docker)" >&2; exit 1; }
(cd "$ROOT" && cargo build --quiet -p kubectl-pingap)
PLUGIN="$ROOT/target/debug/kubectl-pingap"
cat >"$S/plugin-values.yaml" <<EOF
image:
  repository: ${IMAGE%%:*}
  tag: ${IMAGE##*:}
  pullPolicy: Never
status:
  enabled: true
# The probe pod below runs in the release namespace.
networkPolicy:
  allowSameNamespace: true
etcd:
  deploy: true
pingap:
  deploy: true
  replicas: 1
config:
  defaults:
    debounce: "200ms"
    debounce_max: "1s"
  upstreams:
    quickstart:
      services: ["echo:http"]
    broken:
      services: ["missing:http"]
EOF
k() { kubectl --kubeconfig "$KCFG" "$@"; }
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --kubeconfig "$KCFG" --wait 120s
kind load docker-image "$IMAGE" --name "$CLUSTER"
k create namespace pk8s
PAUSE_IMAGE="$(docker exec "$CLUSTER-control-plane" crictl images | awk '$1 == "registry.k8s.io/pause" { print $1 ":" $2; exit }')"
k -n pk8s apply -f - <<EOF
apiVersion: v1
kind: Service
metadata: { name: echo }
spec:
  selector: { app: echo }
  ports: [{ name: http, port: 80, targetPort: 8080 }]
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: echo }
spec:
  replicas: 2
  selector: { matchLabels: { app: echo } }
  template:
    metadata: { labels: { app: echo } }
    spec:
      terminationGracePeriodSeconds: 1
      containers: [{ name: pause, image: "$PAUSE_IMAGE", imagePullPolicy: IfNotPresent }]
EOF
helm --kubeconfig "$KCFG" install pk "$ROOT/charts/pingap-k8s" -n pk8s -f "$S/plugin-values.yaml" --wait --timeout 300s
k -n pk8s get pods -o wide
echo "== no Service or containerPort for the status port"
if k -n pk8s get svc -o yaml | grep -n 9091; then echo "FAIL: 9091 in a Service"; exit 1; fi
if k -n pk8s get pods -l app.kubernetes.io/component=controller -o yaml | grep -n "containerPort: 9091"; then echo "FAIL"; exit 1; fi
echo "== status pod IP not reachable on 9091 from inside the cluster"
pod_ip=$(k -n pk8s get pods -l app.kubernetes.io/component=controller -o jsonpath='{.items[0].status.podIP}')
k -n pk8s run probe --rm -i --restart=Never --image=busybox:1.37 --command -- sh -c \
  "wget -q -T 3 -O /dev/null http://$pod_ip:9090/healthz && echo metrics-port-reachable; \
   if wget -q -T 3 -O /dev/null http://$pod_ip:9091/status; then echo STATUS-REACHABLE; else echo status-port-unreachable; fi" \
  | tee "$S/probe.out"
grep -q metrics-port-reachable "$S/probe.out"
grep -q status-port-unreachable "$S/probe.out"
echo "== kubectl pingap status"
"$PLUGIN" --kubeconfig "$KCFG" -n pk8s status
echo "== kubectl pingap explain broken"
"$PLUGIN" --kubeconfig "$KCFG" -n pk8s explain broken
echo "== kubectl pingap explain quickstart"
"$PLUGIN" --kubeconfig "$KCFG" -n pk8s explain quickstart
echo "== kubectl pingap explain nope (error expected)"
"$PLUGIN" --kubeconfig "$KCFG" -n pk8s explain nope && exit 1 || true
echo "== kubectl pingap admin (background), curl the UI"
"$PLUGIN" --kubeconfig "$KCFG" -n pk8s admin --local-port 13018 >"$S/admin.out" 2>&1 & apid=$!
for _ in $(seq 1 50); do curl -fsS -o /dev/null http://127.0.0.1:13018/ 2>/dev/null && break; sleep 0.2; done
code=$(curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:13018/)
kill $apid; wait $apid 2>/dev/null || true
cat "$S/admin.out"; echo "admin UI HTTP $code"
grep -q "password: kubectl" "$S/admin.out"
pw=$(k -n pk8s get secret pk-pingap-k8s-pingap-admin -o jsonpath='{.data.password}' | base64 -d)
if grep -qF "$pw" "$S/admin.out"; then echo "FAIL: password printed without --show-password"; exit 1; fi
echo "== status disabled -> clear error"
helm --kubeconfig "$KCFG" upgrade pk "$ROOT/charts/pingap-k8s" -n pk8s -f "$S/plugin-values.yaml" --set status.enabled=false --wait --timeout 300s
k -n pk8s rollout status deploy/pk-pingap-k8s --timeout=120s
sleep 5
if "$PLUGIN" --kubeconfig "$KCFG" -n pk8s status 2>"$S/disabled.err"; then echo "FAIL: expected error"; exit 1; fi
cat "$S/disabled.err"
grep -q "enable status.enabled in the chart" "$S/disabled.err"
echo "== --port with status disabled: port-forward reaches nothing"
if "$PLUGIN" --kubeconfig "$KCFG" -n pk8s status --port 9091 2>"$S/disabled2.err"; then echo "FAIL"; exit 1; fi
cat "$S/disabled2.err"
echo "PLUGIN KIND CHECK PASSED"

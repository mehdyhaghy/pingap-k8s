#!/usr/bin/env bash
# End-to-end smoke test of the pingap-k8s binary on this machine:
# a local etcd (127.0.0.1 only) + a throwaway kind cluster.
#
#  1. 2-replica Deployment behind a Service -> addrs has the 2 pod IPs
#  2. scale to 3 -> addrs updated within a few seconds
#  3. ConfigMap-style `..data` symlink swap adds an upstream -> file-watch reload
#  4. a foreign write to the new upstream is corrected (owned-key watch restarted)
#  5. config edited (in an unwatched subdir) + SIGHUP -> SIGHUP reload
#  6. SIGTERM -> clean exit (code 0)
# With STATUS=1 it also enables the loopback /status endpoint and checks its
# JSON after step 1 (shape, leader, in sync, no etcd address in it).
# The cluster, etcd and temp files are always removed on exit.
#
# Usage: scripts/smoke-local.sh
# Env:   KIND_CLUSTER (default pk8s-t11), KIND_NODE_IMAGE (optional), STATUS=1 (optional)
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-pk8s-t11}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pingap-k8s-smoke.XXXXXX")"
KCFG="$WORK/kubeconfig"
CFG="$WORK/config"
LOG="$WORK/controller.log"
etcd_pid=""
ctrl_pid=""

cleanup() {
  local code=$?
  if [[ -n "$ctrl_pid" ]] && kill -0 "$ctrl_pid" 2>/dev/null; then
    kill -KILL "$ctrl_pid" 2>/dev/null || true
  fi
  if [[ -n "$etcd_pid" ]] && kill -0 "$etcd_pid" 2>/dev/null; then
    kill "$etcd_pid" 2>/dev/null || true
    wait "$etcd_pid" 2>/dev/null || true
  fi
  if [[ $code -ne 0 && -f "$LOG" ]]; then
    echo "---- controller log (last 60 lines) ----" >&2
    tail -n 60 "$LOG" >&2
  fi
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit $code
}
trap cleanup EXIT INT TERM

for tool in kind docker kubectl etcd etcdctl cargo python3 curl; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }
done

step() { echo; echo "== $*"; }
pass() { echo "PASS: $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
now() { python3 -c 'import time; print(time.time())'; }
since() { python3 -c "import sys,time; print(f'{time.time()-float(sys.argv[1]):.2f}')" "$1"; }
free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

# ---- build ---------------------------------------------------------------
step "build"
(cd "$ROOT" && cargo build --quiet)
BIN="$ROOT/target/debug/pingap-k8s"

# ---- etcd ----------------------------------------------------------------
step "start etcd (127.0.0.1 only)"
client_port=$(free_port); peer_port=$(free_port)
while [[ "$peer_port" == "$client_port" ]]; do peer_port=$(free_port); done
case "$(uname -m)" in arm64 | aarch64) export ETCD_UNSUPPORTED_ARCH=arm64 ;; esac
ETCD="http://127.0.0.1:${client_port}"
etcd --name smoke --data-dir "$WORK/etcd" \
  --listen-client-urls "$ETCD" --advertise-client-urls "$ETCD" \
  --listen-peer-urls "http://127.0.0.1:${peer_port}" \
  --initial-advertise-peer-urls "http://127.0.0.1:${peer_port}" \
  --initial-cluster "smoke=http://127.0.0.1:${peer_port}" \
  --log-level error >"$WORK/etcd.log" 2>&1 &
etcd_pid=$!
for _ in $(seq 1 100); do curl -fsS "$ETCD/health" >/dev/null 2>&1 && break; sleep 0.1; done
curl -fsS "$ETCD/health" >/dev/null || { cat "$WORK/etcd.log" >&2; fail "etcd not ready"; }
echo "etcd at $ETCD"
ectl() { etcdctl --endpoints="$ETCD" "$@"; }

# ---- kind ----------------------------------------------------------------
step "create kind cluster $CLUSTER"
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
create_args=(--name "$CLUSTER" --kubeconfig "$KCFG" --wait 120s)
[[ -n "${KIND_NODE_IMAGE:-}" ]] && create_args+=(--image "$KIND_NODE_IMAGE")
kind create cluster "${create_args[@]}"
k() { kubectl --kubeconfig "$KCFG" "$@"; }
PAUSE_IMAGE="$(docker exec "$CLUSTER-control-plane" crictl images \
  | awk '$1 == "registry.k8s.io/pause" { print $1 ":" $2; exit }')"
PAUSE_IMAGE="${PAUSE_IMAGE:-registry.k8s.io/pause:3.10}"

step "deploy Service smoke + 2-replica Deployment ($PAUSE_IMAGE)"
k apply -f - <<EOF
apiVersion: v1
kind: Service
metadata: { name: smoke, namespace: default }
spec:
  selector: { app: smoke }
  ports: [{ name: http, port: 80, targetPort: 8080 }]
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: smoke, namespace: default }
spec:
  replicas: 2
  selector: { matchLabels: { app: smoke } }
  template:
    metadata: { labels: { app: smoke } }
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - { name: pause, image: "$PAUSE_IMAGE", imagePullPolicy: IfNotPresent }
EOF
k rollout status deploy/smoke --timeout=120s

# ---- config (ConfigMap-like layout: config.toml -> ..data/config.toml) ----
METRICS_PORT=$(free_port)
STATUS_PORT=$(free_port)
STATUS_LINE=""
[[ "${STATUS:-0}" == 1 ]] && STATUS_LINE="status_listen = \"127.0.0.1:${STATUS_PORT}\""
write_config() { # $1 = dir, $2 = extra upstream TOML
  mkdir -p "$1"
  cat >"$1/config.toml" <<EOF
[etcd]
url = "etcd://127.0.0.1:${client_port}/pingap"

[kubernetes]
kubeconfig = "$KCFG"

[server]
listen = "127.0.0.1:${METRICS_PORT}"
${STATUS_LINE}

[defaults]
debounce = "200ms"
debounce_max = "1s"

[upstreams.app]
services = ["smoke:http"]
$2
EOF
}
swap_data() { # atomically point ..data at $1, as the kubelet does
  python3 -c 'import os,sys; t=sys.argv[2]+".tmp"; os.symlink(sys.argv[1], t); os.replace(t, sys.argv[2])' "$1" "$CFG/..data"
}
mkdir -p "$CFG"
write_config "$CFG/..v1" ""
ln -s ..v1 "$CFG/..data"
ln -s ..data/config.toml "$CFG/config.toml"

step "--check"
"$BIN" --check --config "$CFG/config.toml"

# ---- helpers -------------------------------------------------------------
pod_ips() { k get pods -l app=smoke --field-selector=status.phase=Running \
  -o jsonpath='{range .items[*]}{.status.podIP}{"\n"}{end}' | sort; }
# addrs of one upstream as sorted lines (ip:port[ weight])
addrs() {
  ectl get --print-value-only "/pingap/upstreams/$1.toml" | python3 -c '
import sys, tomllib
d = tomllib.loads(sys.stdin.read() or "")
for a in sorted(d.get("upstreams", {}).get(sys.argv[1], {}).get("addrs", [])): print(a)' "$1"
}
expected() { # $1 = suffix appended to each pod IP
  pod_ips | sed "s/\$/:8080$1/" | sort
}
wait_addrs() { # $1 upstream, $2 suffix, $3 timeout seconds
  local deadline=$(( $(date +%s) + $3 ))
  while (( $(date +%s) <= deadline )); do
    [[ -n "$(addrs "$1")" && "$(addrs "$1")" == "$(expected "$2")" ]] && return 0
    sleep 0.1
  done
  echo "upstream $1: got [$(addrs "$1" | tr '\n' ' ')] want [$(expected "$2" | tr '\n' ' ')]" >&2
  return 1
}

# ---- run the controller --------------------------------------------------
step "start controller"
POD_NAMESPACE=default POD_NAME=smoke-controller RUST_LOG=info,pingap_k8s=debug \
  "$BIN" --config "$CFG/config.toml" --log-format text >"$LOG" 2>&1 &
ctrl_pid=$!

step "1. initial sync: 2 pod IPs"
t0=$(now)
wait_addrs app "" 60 || fail "initial addrs"
pass "addrs = $(addrs app | tr '\n' ' ')(after $(since "$t0")s from start)"
ectl get /pingap/upstreams/app.toml --print-value-only
curl -fsS "http://127.0.0.1:${METRICS_PORT}/readyz" >/dev/null || fail "/readyz not 200"
curl -fsS "http://127.0.0.1:${METRICS_PORT}/metrics" | grep -q '^pingap_k8s_is_leader 1' || fail "not leader"
pass "/readyz 200, is_leader 1"

if [[ "${STATUS:-0}" == 1 ]]; then
  step "1b. /status (loopback only)"
  curl -fsS "http://127.0.0.1:${STATUS_PORT}/status" >"$WORK/status.json" || fail "/status not 200"
  python3 - "$WORK/status.json" "$client_port" <<'PY' || fail "/status content"
import json, sys
text = open(sys.argv[1]).read()
doc = json.loads(text)
assert doc["version"] == 1 and doc["phase"] == "running", doc
assert doc["is_leader"] and doc["ready"] and doc["watchers"]["synced"], doc
assert doc["etcd"] == {"prefix": "/pingap"}, doc["etcd"]
assert sys.argv[2] not in text, "etcd address leaked"
(app,) = [u for u in doc["upstreams"] if u["name"] == "app"]
assert app["target"] == "pods" and app["services"] == ["smoke.default:http"], app
assert app["ready"] == 2 and app["in_sync"] and len(app["desired"]) == 2, app
assert app["stored"] == app["desired"] and app["last_write"]["result"] == "ok", app
print(json.dumps(app, indent=2))
PY
  code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${METRICS_PORT}/status")
  [[ "$code" == 404 ]] || fail "/status served on the metrics port ($code)"
  pass "/status JSON ok; not served on the metrics port"
fi

step "2. scale to 3"
k scale deploy/smoke --replicas=3 >/dev/null
ready_pods() {
  k get pods -l app=smoke \
    -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
    | grep -c True || true
}
for _ in $(seq 1 1200); do [[ "$(ready_pods)" == 3 ]] && break; sleep 0.1; done
t1=$(now)
[[ "$(ready_pods)" == 3 && $(pod_ips | wc -l | tr -d ' ') == 3 ]] || fail "expected 3 ready pods"
wait_addrs app "" 10 || fail "scale to 3 not reflected"
elapsed=$(since "$t1")
python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) <= 3.5 else 1)" "$elapsed" \
  || fail "update took ${elapsed}s after pods Ready (want ~3s)"
pass "3 addrs ${elapsed}s after the 3rd pod became Ready: $(addrs app | tr '\n' ' ')"

step "3. file-change reload (..data symlink swap adds upstream app2)"
write_config "$CFG/..v2" $'[upstreams.app2]\nservices = ["smoke.default:80 5"]'
swap_data ..v2
wait_addrs app2 " 5" 15 || fail "app2 not created after file change"
grep -q 'config reloaded.*trigger="file change"' "$LOG" || grep -q 'trigger="file change".*config reloaded' "$LOG" \
  || grep 'config reloaded' "$LOG" | grep -q 'file change' || fail "no file-change reload logged"
pass "app2 = $(addrs app2 | tr '\n' ' ')"

step "4. foreign write to app2 is corrected (owned-key watch restarted)"
ectl put /pingap/upstreams/app2.toml $'[upstreams.app2]\naddrs = ["10.255.255.1:1"]\n' >/dev/null
t2=$(now)
sleep 0.2
wait_addrs app2 " 5" 10 || fail "foreign write not corrected"
pass "corrected after $(since "$t2")s"

step "5. SIGHUP reload (edit in unwatched subdir, replace app2 with app3)"
write_config "$CFG/..v2" $'[upstreams.app3]\nservices = ["smoke:http 2"]'
kill -HUP "$ctrl_pid"
wait_addrs app3 " 2" 15 || fail "app3 not created after SIGHUP"
grep 'config reloaded' "$LOG" | grep -q 'SIGHUP' || fail "no SIGHUP reload logged"
pass "app3 = $(addrs app3 | tr '\n' ' ')"

step "6. invalid config is rejected, old config kept"
echo 'not toml [' >"$CFG/..v2/config.toml"
kill -HUP "$ctrl_pid"
sleep 1
grep -q 'config reload failed' "$LOG" || fail "invalid config not reported"
kill -0 "$ctrl_pid" || fail "controller died on invalid config"
pass "invalid config logged, controller still running"

step "7. SIGTERM"
t3=$(now)
kill -TERM "$ctrl_pid"
code=0
wait "$ctrl_pid" || code=$?
ctrl_pid=""
[[ $code -eq 0 ]] || fail "exit code $code"
pass "exited 0 after $(since "$t3")s"
grep -q 'leader election stopped' "$LOG" || fail "election not stopped"

step "controller log (reload / leader / shutdown lines)"
grep -E 'config reloaded|config unchanged|reload failed|owned-key watch|leader|shutting down|stopped|synced' "$LOG" \
  | sed -E 's/\x1b\[[0-9;]*m//g'

echo
echo "SMOKE TEST PASSED"

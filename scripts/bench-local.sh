#!/usr/bin/env bash
# Benchmarks of the pingap-k8s release binary on this machine:
# a local etcd (127.0.0.1 only) + a throwaway kind cluster + optionally the stock
# Pingap image reading the same etcd with --autoreload.
#
# Two phases, each with 50 upstreams (one Service each) and a fresh controller process:
#
#  synthetic  50 selector-less Services, each with one hand-written EndpointSlice of
#             20 endpoints (the full 50 x 20 scale without 1000 pods). A driver
#             patches the slices through `kubectl proxy`:
#               - scale steps (+1 / -1 endpoint on every Service), timed from the
#                 API write to the etcd watch event;
#               - simulated rolling updates (maxSurge 25%, maxUnavailable 25%) of all
#                 50 Services at once, at a fixed step interval.
#  pods       50 Services with real Deployments of BENCH_POD_REPLICAS pause pods:
#               - scale up and down by one replica on every Deployment;
#               - `kubectl rollout restart` of all 50 Deployments at once.
#
# Measured: pingap_k8s_event_to_write_seconds (p50/p95/p99 from bucket deltas),
# writes_total{result="ok"} and etcd revision deltas per rollout, controller RSS
# (idle and peak) and CPU time, and Pingap container CPU (cgroup usage) per write.
# The cluster, etcd, Pingap container and temp files are removed on exit (also on
# failure); raw results are copied to target/bench/<timestamp> (see BENCH_KEEP_RESULTS).
# Results: docs/benchmarks.md.
#
# Usage: scripts/bench-local.sh
# Env:   KIND_CLUSTER        (default pk8s-bench)
#        KIND_NODE_IMAGE     (default kindest/node:v1.35.8)
#        BENCH_PHASES        (default "synthetic pods")
#        BENCH_UPSTREAMS     (default 50)
#        BENCH_ENDPOINTS     (synthetic endpoints per Service, default 20)
#        BENCH_POD_REPLICAS  (pods per Deployment in the pods phase, default 2)
#        BENCH_SYN_STEPS     (rollout step intervals in seconds, default "1.0 0.25")
#        BENCH_SCALE_ROUNDS  (scale up+down rounds per phase, default 3)
#        BENCH_PINGAP        (auto|off, default auto: needs Docker Desktop so the
#                             container reaches the 127.0.0.1-bound etcd)
#        PINGAP_IMAGE        (default vicanso/pingap:0.16.1)
#        BENCH_KEEP_RESULTS  (directory to copy raw results into, default
#                             target/bench/<UTC timestamp>; "" to keep nothing)
#        BENCH_LOAD_MAX      (host 1-minute load average limit, default 20: if it
#                             stays above for 2 minutes the run is aborted)
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-pk8s-bench}"
NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.35.8}"
PHASES="${BENCH_PHASES:-synthetic pods}"
N="${BENCH_UPSTREAMS:-50}"
E="${BENCH_ENDPOINTS:-20}"
R="${BENCH_POD_REPLICAS:-2}"
SYN_STEPS="${BENCH_SYN_STEPS:-1.0 0.25}"
SCALE_ROUNDS="${BENCH_SCALE_ROUNDS:-3}"
PINGAP_MODE="${BENCH_PINGAP:-auto}"
PINGAP_IMAGE="${PINGAP_IMAGE:-vicanso/pingap:0.16.1}"
PINGAP_NAME="$CLUSTER-pingap"
LOAD_MAX="${BENCH_LOAD_MAX:-20}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEEP="${BENCH_KEEP_RESULTS-$ROOT/target/bench/$(date -u +%Y%m%dT%H%M%SZ)}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/pingap-k8s-bench.XXXXXX")"
KCFG="$WORK/kubeconfig"
RES="$WORK/results.txt"
etcd_pid=""
ctrl_pid=""
proxy_pid=""
rss_pid=""
stats_pid=""
guard_pid=""
pingap_on=0

stop_pid() { # $1 pid, $2 signal
  if [[ -n "$1" ]] && kill -0 "$1" 2>/dev/null; then
    kill "-${2:-TERM}" "$1" 2>/dev/null || true
    wait "$1" 2>/dev/null || true
  fi
}

cleanup() {
  local code=$?
  set +e
  stop_pid "$guard_pid"
  stop_pid "$rss_pid"
  stop_pid "$stats_pid"
  stop_pid "$ctrl_pid" KILL
  stop_pid "$proxy_pid"
  docker rm -f "$PINGAP_NAME" >/dev/null 2>&1
  stop_pid "$etcd_pid"
  if [[ $code -ne 0 && -f "$WORK/controller.log" ]]; then
    echo "---- controller log (last 40 lines) ----" >&2
    tail -n 40 "$WORK/controller.log" >&2
  fi
  if [[ -f "$WORK/load-abort" ]]; then
    cat "$WORK/load-abort" >&2
    code=3
  fi
  if [[ -n "$KEEP" ]]; then
    mkdir -p "$KEEP"
    cp "$WORK"/*.txt "$WORK"/*.log "$WORK"/*.json "$WORK"/*.toml "$KEEP"/ 2>/dev/null
    echo "raw results kept in $KEEP" >&2
  fi
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1
  rm -rf "$WORK"
  exit $code
}
trap cleanup EXIT INT TERM

for tool in kind docker kubectl etcd etcdctl cargo python3 curl; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }
done

step() { echo; echo "== $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
out() { echo "$*" | tee -a "$RES"; }
now() { python3 -c 'import time; print(f"{time.time():.3f}")'; }
free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

# ---- python helper (metrics parsing, synthetic driver, etcd watch timing) ----
PY="$WORK/bench.py"
cat >"$PY" <<'PYEOF'
import base64, json, math, re, subprocess, sys, threading, time, tomllib, urllib.request
from concurrent.futures import ThreadPoolExecutor

# Pingap's own listener: with 10.0.0.0/8 routed to its loopback, every backend
# address accepts the TCP health check (see start_pingap).
BACKEND_PORT = 6188

def fetch(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return r.read().decode()

def metrics(url):
    text = fetch(url)
    buckets, writes, count, total = {}, {}, 0, 0.0
    for line in text.splitlines():
        if line.startswith("pingap_k8s_event_to_write_seconds_bucket"):
            le = re.search(r'le="([^"]+)"', line).group(1)
            buckets[le] = float(line.rsplit(" ", 1)[1])
        elif line.startswith("pingap_k8s_event_to_write_seconds_count"):
            count = float(line.rsplit(" ", 1)[1])
        elif line.startswith("pingap_k8s_event_to_write_seconds_sum"):
            total = float(line.rsplit(" ", 1)[1])
        elif line.startswith("pingap_k8s_writes_total"):
            res = re.search(r'result="([^"]+)"', line).group(1)
            writes[res] = writes.get(res, 0) + float(line.rsplit(" ", 1)[1])
    return {"buckets": buckets, "count": count, "sum": total, "writes": writes}

def quantile(buckets, q):
    """Prometheus histogram_quantile: linear interpolation inside the bucket."""
    items = sorted(((math.inf if k == "+Inf" else float(k)), v) for k, v in buckets.items())
    total = items[-1][1]
    if total <= 0:
        return float("nan")
    rank, prev_le, prev_c = q * total, 0.0, 0.0
    for le, c in items:
        if c >= rank:
            if le == math.inf:
                return prev_le
            return prev_le + (le - prev_le) * ((rank - prev_c) / (c - prev_c) if c > prev_c else 0)
        prev_le, prev_c = le, c
    return prev_le

def hist_report(before, after):
    a, b = json.load(open(after)), json.load(open(before))
    delta = {k: a["buckets"][k] - b["buckets"].get(k, 0) for k in a["buckets"]}
    n = a["count"] - b["count"]
    mean = (a["sum"] - b["sum"]) / n if n else float("nan")
    qs = " ".join(f"p{int(q*100)}={quantile(delta, q):.3f}s" for q in (0.5, 0.95, 0.99))
    print(f"samples={int(n)} mean={mean:.3f}s {qs}")

def writes_ok(path):
    return int(json.load(open(path))["writes"].get("ok", 0))

def pct(values, q):
    v = sorted(values)
    if not v:
        return float("nan")
    i = min(len(v) - 1, max(0, math.ceil(q * len(v)) - 1))
    return v[i]

# ---- synthetic EndpointSlices ----
class Api:
    def __init__(self, base):
        self.base = base
    def req(self, method, path, body=None, ctype="application/json"):
        data = json.dumps(body).encode() if body is not None else None
        r = urllib.request.Request(self.base + path, data=data, method=method,
                                   headers={"Content-Type": ctype})
        with urllib.request.urlopen(r, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")

def ep(ip, ready=True, terminating=False):
    return {"addresses": [ip], "conditions": {"ready": ready, "serving": True if ready or terminating else False,
                                              "terminating": terminating}}

class Syn:
    def __init__(self, api, ns, n, e):
        self.api, self.ns, self.n, self.e = api, ns, n, e
        self.gen = [0] * n
        self.eps = [[ep(self.ip(u, 0, i)) for i in range(e)] for u in range(n)]
        self.pool = ThreadPoolExecutor(16)
    def ip(self, u, gen, i):
        return f"10.{100 + gen % 150}.{u}.{i + 1}"
    def name(self, u):
        return f"svc-{u:02d}"
    def setup(self):
        def one(u):
            self.api.req("POST", f"/api/v1/namespaces/{self.ns}/services", {
                "apiVersion": "v1", "kind": "Service", "metadata": {"name": self.name(u)},
                "spec": {"ports": [{"name": "http", "port": 80, "targetPort": BACKEND_PORT}]}})
            self.api.req("POST", f"/apis/discovery.k8s.io/v1/namespaces/{self.ns}/endpointslices", {
                "apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice",
                "metadata": {"name": self.name(u) + "-bench", "labels": {
                    "kubernetes.io/service-name": self.name(u),
                    "endpointslice.kubernetes.io/managed-by": "bench.pingap-k8s"}},
                "addressType": "IPv4",
                "ports": [{"name": "http", "port": BACKEND_PORT, "protocol": "TCP"}],
                "endpoints": self.eps[u]})
        list(self.pool.map(one, range(self.n)))
    def push(self, u):
        self.api.req("PATCH", f"/apis/discovery.k8s.io/v1/namespaces/{self.ns}/endpointslices/{self.name(u)}-bench",
                     {"endpoints": self.eps[u]}, "application/merge-patch+json")
        return time.time()
    def push_all(self):
        return dict(zip(range(self.n), self.pool.map(self.push, range(self.n))))

class EtcdWatch:
    """Timestamps every put under the upstreams prefix (etcdctl watch -w json)."""
    def __init__(self, endpoint, prefix):
        self.events = []  # (time, upstream, addrs count)
        self.lock = threading.Lock()
        self.p = subprocess.Popen(["etcdctl", f"--endpoints={endpoint}", "watch", "--prefix", prefix, "-w", "json"],
                                  stdout=subprocess.PIPE, text=True)
        threading.Thread(target=self.read, daemon=True).start()
        time.sleep(0.5)
    def read(self):
        for line in self.p.stdout:
            t = time.time()
            try:
                msg = json.loads(line)
            except ValueError:
                continue
            for ev in msg.get("Events") or []:
                kv = ev.get("kv", {})
                key = base64.b64decode(kv.get("key", "")).decode()
                name = key.rsplit("/", 1)[-1].removesuffix(".toml")
                try:
                    doc = tomllib.loads(base64.b64decode(kv.get("value", "")).decode())
                    count = len(doc["upstreams"][name]["addrs"])
                except Exception:
                    count = -1
                with self.lock:
                    self.events.append((t, name, count))
    def wait_for(self, since, want, timeout=30):
        """Per upstream: first event after since[u] with want[u] addrs."""
        deadline, got = time.time() + timeout, {}
        while time.time() < deadline and len(got) < len(want):
            with self.lock:
                evs = list(self.events)
            for t, name, count in evs:
                u = int(name.split("-")[1])
                if u in want and u not in got and t >= since[u] and count == want[u]:
                    got[u] = t - since[u]
            time.sleep(0.05)
        return got
    def stop(self):
        self.p.kill()

def syn_cmd(args):
    api = Api(args[0]); ns = args[1]; n = int(args[2]); e = int(args[3]); action = args[4]
    syn = Syn(api, ns, n, e)
    if action == "setup":
        syn.setup()
        return
    etcd, prefix = args[5], args[6]
    if action == "scale":
        rounds = int(args[7])
        watch = EtcdWatch(etcd, prefix)
        lat = []
        for r in range(rounds):
            for delta in (+1, -1):
                for u in range(n):
                    if delta > 0:
                        syn.eps[u].append(ep(f"10.99.{u}.{r + 1}"))
                    else:
                        syn.eps[u].pop()
                want = {u: len(syn.eps[u]) for u in range(n)}
                since = syn.push_all()
                got = watch.wait_for(since, want)
                if len(got) < n:
                    print(f"WARN: {n - len(got)} upstreams not updated in time", file=sys.stderr)
                lat.extend(got.values())
                time.sleep(2.5)
        watch.stop()
        print(f"samples={len(lat)} p50={pct(lat, .5):.3f}s p95={pct(lat, .95):.3f}s "
              f"p99={pct(lat, .99):.3f}s max={max(lat):.3f}s")
    elif action == "rollout":
        step = float(args[7])
        gen = int(args[8])  # the rollout number: endpoints start at generation gen-1
        syn.eps = [[ep(syn.ip(u, gen - 1, i)) for i in range(e)] for u in range(n)]
        syn.gen = [gen] * n
        surge = math.ceil(e * 0.25)
        unavailable = max(1, e * 25 // 100)
        batch = min(surge, unavailable)
        t0 = time.time()
        patches = 0
        old_left = e
        idx = 0
        while old_left > 0:
            k = min(batch, old_left)
            # new pods created (not ready) + old pods terminating (serving)
            for u in range(n):
                old = [x for x in syn.eps[u] if not x["addresses"][0].startswith(f"10.{100 + syn.gen[u] % 150}.")
                       and not x["conditions"]["terminating"]]
                for x in old[:k]:
                    x["conditions"] = {"ready": False, "serving": True, "terminating": True}
                for i in range(k):
                    syn.eps[u].append(ep(syn.ip(u, syn.gen[u], idx + i), ready=False))
            syn.push_all(); patches += n
            time.sleep(step)
            # new pods ready, terminated old pods gone
            for u in range(n):
                syn.eps[u] = [x for x in syn.eps[u] if not x["conditions"]["terminating"]]
                for x in syn.eps[u]:
                    x["conditions"] = {"ready": True, "serving": True, "terminating": False}
            syn.push_all(); patches += n
            time.sleep(step)
            idx += k
            old_left -= k
        print(f"rollout_seconds={time.time() - t0:.2f} slice_patches={patches}")

if __name__ == "__main__":
    cmd, args = sys.argv[1], sys.argv[2:]
    if cmd == "metrics":
        json.dump(metrics(args[0]), open(args[1], "w"))
    elif cmd == "hist":
        hist_report(args[0], args[1])
    elif cmd == "writes":
        print(writes_ok(args[0]))
    elif cmd == "syn":
        syn_cmd(args)
    elif cmd == "rate":  # a b seconds
        print(f"{float(args[0]) / float(args[1]):.1f}")
PYEOF
py() { python3 "$PY" "$@"; }

# ---- build ---------------------------------------------------------------
step "build (release)"
(cd "$ROOT" && nice -n 10 cargo build --release --quiet --bin pingap-k8s)
BIN="$ROOT/target/release/pingap-k8s"

# ---- host load guard -----------------------------------------------------
# Samples the 1-minute load average every 10 s into load.txt. If it stays above
# LOAD_MAX for 2 minutes, the run is aborted (cleanup deletes the cluster).
load1() { sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || awk '{print $1}' /proc/loadavg; }
(
  high=0
  while :; do
    l=$(load1)
    echo "$(date +%s) $l" >>"$WORK/load.txt"
    if awk -v l="$l" -v m="$LOAD_MAX" 'BEGIN { exit !(l > m) }'; then high=$((high + 1)); else high=0; fi
    if (( high >= 12 )); then
      echo "ABORTED: host 1-minute load above $LOAD_MAX for 2 minutes (last $l)" >"$WORK/load-abort"
      kill -TERM $$ 2>/dev/null
      pkill -TERM -P $$ 2>/dev/null
      exit 0
    fi
    sleep 10
  done
) &
guard_pid=$!
load_max() { awk '$2 > m { m = $2 } END { print m + 0 }' "$WORK/load.txt"; }

# ---- etcd ----------------------------------------------------------------
step "start etcd (127.0.0.1 only)"
client_port=$(free_port); peer_port=$(free_port)
while [[ "$peer_port" == "$client_port" ]]; do peer_port=$(free_port); done
case "$(uname -m)" in arm64 | aarch64) export ETCD_UNSUPPORTED_ARCH=arm64 ;; esac
ETCD="http://127.0.0.1:${client_port}"
etcd --name bench --data-dir "$WORK/etcd" \
  --listen-client-urls "$ETCD" --advertise-client-urls "$ETCD" \
  --listen-peer-urls "http://127.0.0.1:${peer_port}" \
  --initial-advertise-peer-urls "http://127.0.0.1:${peer_port}" \
  --initial-cluster "bench=http://127.0.0.1:${peer_port}" \
  --log-level error >"$WORK/etcd.log" 2>&1 &
etcd_pid=$!
for _ in $(seq 1 100); do curl -fsS "$ETCD/health" >/dev/null 2>&1 && break; sleep 0.1; done
curl -fsS "$ETCD/health" >/dev/null || { cat "$WORK/etcd.log" >&2; fail "etcd not ready"; }
echo "etcd at $ETCD"
ectl() { etcdctl --endpoints="$ETCD" "$@"; }
etcd_rev() { ectl endpoint status -w json | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["Status"]["header"]["revision"])'; }

# ---- kind ----------------------------------------------------------------
step "create kind cluster $CLUSTER ($NODE_IMAGE)"
max_pods=$(( N * (R + 1) * 2 + 60 ))
cat >"$WORK/kind.yaml" <<EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
networking:
  podSubnet: "10.244.0.0/16"
kubeadmConfigPatches:
  - |
    kind: ClusterConfiguration
    controllerManager:
      extraArgs:
        node-cidr-mask-size: "20"
  - |
    kind: KubeletConfiguration
    maxPods: ${max_pods}
EOF
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
kind create cluster --name "$CLUSTER" --kubeconfig "$KCFG" --image "$NODE_IMAGE" \
  --config "$WORK/kind.yaml" --wait 180s
k() { kubectl --kubeconfig "$KCFG" "$@"; }
PAUSE_IMAGE="$(docker exec "$CLUSTER-control-plane" crictl images \
  | awk '$1 == "registry.k8s.io/pause" { print $1 ":" $2; exit }')"
PAUSE_IMAGE="${PAUSE_IMAGE:-registry.k8s.io/pause:3.10}"
PROXY_PORT=$(free_port)
k proxy --address=127.0.0.1 --port="$PROXY_PORT" >"$WORK/proxy.log" 2>&1 &
proxy_pid=$!
API="http://127.0.0.1:${PROXY_PORT}"
for _ in $(seq 1 50); do curl -fsS "$API/version" >/dev/null 2>&1 && break; sleep 0.1; done

# ---- Pingap --------------------------------------------------------------
if [[ "$PINGAP_MODE" == auto ]]; then
  if docker info --format '{{.OperatingSystem}}' 2>/dev/null | grep -q 'Docker Desktop'; then
    pingap_on=1
  else
    echo "Pingap skipped: without Docker Desktop a container cannot reach etcd on 127.0.0.1"
  fi
fi
PINGAP_ETCD="etcd://host.docker.internal:${client_port}/pingap"

pingap_cpu_usec() { docker exec "$PINGAP_NAME" sh -c 'grep usage_usec /sys/fs/cgroup/cpu.stat' | awk '{print $2}'; }

start_pingap() { # $1 phase: seeds the etcd prefix with 50 locations/upstreams, then starts Pingap
  [[ $pingap_on == 1 ]] || return 0
  docker rm -f "$PINGAP_NAME" >/dev/null 2>&1 || true
  ectl del --prefix /pingap/ >/dev/null
  local seed="$WORK/seed-$1"
  mkdir -p "$seed"
  {
    printf '[servers.bench]\naddr = "0.0.0.0:6188"\nlocations = [%s]\n\n' \
      "$(for u in $(seq 0 $((N - 1))); do printf '"l-%02d",' "$u"; done | sed 's/,$//')"
    for u in $(seq 0 $((N - 1))); do
      printf '[locations.l-%02d]\nupstream = "svc-%02d"\npath = "/svc-%02d"\n\n' "$u" "$u" "$u"
      printf '[upstreams.svc-%02d]\naddrs = ["127.0.0.1:9"]\n\n' "$u"
    done
  } >"$seed/pingap.toml"
  docker run --rm -v "$seed:/seed:ro" --entrypoint pingap "$PINGAP_IMAGE" \
    --conf /seed/pingap.toml --sync "$PINGAP_ETCD" >"$WORK/pingap-seed-$1.log" 2>&1 \
    || { cat "$WORK/pingap-seed-$1.log" >&2; fail "pingap seed"; }
  docker run -d --name "$PINGAP_NAME" --entrypoint pingap "$PINGAP_IMAGE" \
    --conf "$PINGAP_ETCD" --autoreload >/dev/null
  # Make every backend address answer: route 10.0.0.0/8 (synthetic endpoints and
  # kind pod IPs) to the container's loopback, where Pingap itself listens on
  # 6188. Without this the addresses are blackholed, Pingap's health checks hang
  # on connect timeouts and it applies etcd changes only every few minutes.
  docker run --rm --net "container:$PINGAP_NAME" --cap-add NET_ADMIN --entrypoint ip "$NODE_IMAGE" \
    route add local 10.0.0.0/8 dev lo
  sleep 10 # let the config watch start before the first write
  docker ps --filter "name=$PINGAP_NAME" --format '{{.Status}}' | grep -q Up \
    || { docker logs "$PINGAP_NAME" >&2; fail "pingap not running"; }
}

# docker stats CPU% samples in the background, written to $WORK/stats-<label>.txt
start_stats() {
  [[ $pingap_on == 1 ]] || return 0
  docker stats "$PINGAP_NAME" --format '{{.CPUPerc}}' >"$WORK/stats-$1.txt" 2>/dev/null &
  stats_pid=$!
}
stop_stats() {
  [[ $pingap_on == 1 ]] || return 0
  stop_pid "$stats_pid"; stats_pid=""
  python3 -c '
import re, sys
v = [float(m) for m in re.findall(r"([0-9.]+)%", open(sys.argv[1]).read())]
print(f"peak {max(v):.1f}% of one core, mean {sum(v)/len(v):.1f}% ({len(v)} samples)" if v else "no samples")' "$WORK/stats-$1.txt"
}

# ---- controller ----------------------------------------------------------
METRICS_PORT=$(free_port)
METRICS="http://127.0.0.1:${METRICS_PORT}/metrics"

start_controller() { # $1 phase, $2 namespace
  {
    printf '[etcd]\nurl = "etcd://127.0.0.1:%s/pingap"\n\n' "$client_port"
    printf '[kubernetes]\nkubeconfig = "%s"\n\n' "$KCFG"
    printf '[server]\nlisten = "127.0.0.1:%s"\n\n' "$METRICS_PORT"
    for u in $(seq 0 $((N - 1))); do
      printf '[upstreams.svc-%02d]\nservices = ["svc-%02d.%s:http"]\n\n' "$u" "$u" "$2"
    done
  } >"$WORK/config-$1.toml"
  POD_NAMESPACE=default "$BIN" --check --config "$WORK/config-$1.toml" >/dev/null
  POD_NAMESPACE=default POD_NAME="bench-$1" RUST_LOG=warn \
    "$BIN" --config "$WORK/config-$1.toml" --log-format text >>"$WORK/controller.log" 2>&1 &
  ctrl_pid=$!
  # RSS sampler (KiB)
  (while kill -0 "$ctrl_pid" 2>/dev/null; do ps -o rss= -p "$ctrl_pid" 2>/dev/null; sleep 0.2; done) \
    >"$WORK/rss-$1.txt" &
  rss_pid=$!
}

stop_controller() {
  kill -TERM "$ctrl_pid"; wait "$ctrl_pid" || true; ctrl_pid=""
  stop_pid "$rss_pid"; rss_pid=""
}

rss_now() { ps -o rss= -p "$ctrl_pid" | tr -d ' '; }
cpu_secs() { # controller user+sys CPU seconds
  ps -o time= -p "$ctrl_pid" | python3 -c '
import sys
t = sys.stdin.read().strip()
parts = t.split(":")
s = float(parts[-1]) + 60 * float(parts[-2]) + (3600 * float(parts[-3]) if len(parts) > 2 else 0)
print(f"{s:.2f}")'
}
snap() { py metrics "$METRICS" "$WORK/m-$1.json"; }

# Waits until writes_total{ok} has not changed for $1 seconds; prints the time of the last change.
wait_quiet() {
  local quiet=$1 last cur changed
  last=$(curl -fsS "$METRICS" | awk '/^pingap_k8s_writes_total.*result="ok"/ {s += $2} END {print s+0}')
  changed=$(now)
  while :; do
    sleep 0.25
    cur=$(curl -fsS "$METRICS" | awk '/^pingap_k8s_writes_total.*result="ok"/ {s += $2} END {print s+0}')
    if [[ "$cur" != "$last" ]]; then last=$cur; changed=$(now); fi
    python3 -c "import sys,time; sys.exit(0 if time.time()-float(sys.argv[1]) >= float(sys.argv[2]) else 1)" \
      "$changed" "$quiet" && break
  done
  echo "$changed"
}

wait_all_written() { # every upstream has $1 addrs (or not 127.0.0.1:9 when $1 = any)
  local want=$1 deadline=$(( $(date +%s) + ${2:-120} ))
  while (( $(date +%s) <= deadline )); do
    if ectl get --prefix /pingap/upstreams/ -w json | python3 -c '
import base64, json, sys, tomllib
n, want = int(sys.argv[1]), sys.argv[2]
kvs = json.load(sys.stdin).get("kvs", [])
ok = 0
for kv in kvs:
    key = base64.b64decode(kv["key"]).decode()
    name = key.rsplit("/", 1)[-1].removesuffix(".toml")
    addrs = tomllib.loads(base64.b64decode(kv["value"]).decode())["upstreams"][name].get("addrs", [])
    if (want == "any" and addrs and addrs != ["127.0.0.1:9"]) or (want != "any" and len(addrs) == int(want)):
        ok += 1
sys.exit(0 if ok >= n else 1)' "$N" "$want"; then return 0; fi
    sleep 0.2
  done
  return 1
}

# A rollout measurement: writes, etcd revisions, controller CPU, Pingap CPU.
# $1 label; the rollout command follows.
measure_rollout() {
  local label=$1; shift
  local rev0 rev1 w0 w1 c0 c1 p0 p1 t0 tq dur wdur
  snap "$label-before"
  rev0=$(etcd_rev); c0=$(cpu_secs)
  local tp0 tp1 since_iso
  since_iso=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  [[ $pingap_on == 1 ]] && p0=$(pingap_cpu_usec)
  tp0=$(now)
  start_stats "$label"
  t0=$(now)
  "$@"
  local t_done; t_done=$(now)
  tq=$(wait_quiet 3)
  stop_stats_out=$(stop_stats "$label")
  [[ $pingap_on == 1 ]] && p1=$(pingap_cpu_usec)
  tp1=$(now)
  rev1=$(etcd_rev); c1=$(cpu_secs)
  snap "$label-after"
  w0=$(py writes "$WORK/m-$label-before.json"); w1=$(py writes "$WORK/m-$label-after.json")
  dur=$(python3 -c "print(f'{float(\"$t_done\") - float(\"$t0\"):.2f}')")
  wdur=$(python3 -c "print(f'{max(float(\"$tq\"), float(\"$t_done\")) - float(\"$t0\"):.2f}')")
  local writes=$(( w1 - w0 )) revs=$(( rev1 - rev0 ))
  out "  rollout: ${dur}s until done; writes until the last write: ${wdur}s"
  out "  writes ok: $writes  etcd revisions: $revs  write rate: $(py rate "$writes" "$wdur")/s (over ${wdur}s)"
  out "  event-to-write: $(py hist "$WORK/m-$label-before.json" "$WORK/m-$label-after.json")"
  out "  controller CPU: $(python3 -c "print(f'{float(\"$c1\") - float(\"$c0\"):.2f}')")s  RSS now: $(rss_now) KiB"
  if [[ $pingap_on == 1 ]]; then
    # Pingap logs one 'reload upstream success' per applied batch of changes and
    # one 'starting static discovery' per upstream it rebuilds in that batch.
    local reloads rebuilds
    docker logs --since "$since_iso" "$PINGAP_NAME" >"$WORK/pingap-window-$label.log" 2>&1 || true
    reloads=$(grep -c 'reload upstream success' "$WORK/pingap-window-$label.log" || true)
    rebuilds=$(grep -c 'starting static discovery' "$WORK/pingap-window-$label.log" || true)
    out "  pingap: $(python3 -c "
b = ($p1 - $p0) / 1e6
i = $PINGAP_IDLE_RATE * ($tp1 - $tp0)
n, r, u = max($writes, 1), max($reloads, 1), max($rebuilds, 1)
print(f'cpu {b:.3f}s over {$tp1 - $tp0:.1f}s, {b - i:.3f}s above idle, {1000 * (b - i) / n:.2f} ms per write; '
      f'{$reloads} reloads ({1000 * (b - i) / r:.1f} ms each), {$rebuilds} upstream rebuilds ({1000 * (b - i) / u:.2f} ms each)')")"
    out "  pingap docker stats: $stop_stats_out"
  fi
}

pingap_idle() { # measures Pingap's idle CPU rate (seconds of CPU per second) over $1 s
  [[ $pingap_on == 1 ]] || return 0
  local p0 p1
  p0=$(pingap_cpu_usec); sleep "$1"; p1=$(pingap_cpu_usec)
  PINGAP_IDLE_RATE=$(python3 -c "print(($p1 - $p0) / 1e6 / $1)")
  out "  pingap idle: $(python3 -c "print(f'{100 * $PINGAP_IDLE_RATE:.2f}')")% of one core"
}

phase_common_start() { # $1 phase, $2 namespace, $3 expected addrs per upstream ("any" ok)
  start_pingap "$1"
  local t0; t0=$(now)
  start_controller "$1" "$2"
  wait_all_written "$3" 180 || fail "$1: initial sync incomplete"
  out "  initial sync of $N upstreams: $(python3 -c "import time; print(f'{time.time() - $t0:.2f}')")s after process start"
  wait_quiet 3 >/dev/null
  sleep 5
  out "  controller RSS idle: $(rss_now) KiB"
  pingap_idle 20
}

phase_common_end() { # $1 phase
  out "  controller RSS peak: $(sort -n "$WORK/rss-$1.txt" | tail -1 | tr -d ' ') KiB"
  if [[ $pingap_on == 1 ]]; then
    out "  pingap 'reload upstream success' lines in this phase: $(docker logs "$PINGAP_NAME" 2>&1 | grep -c 'reload upstream success' || true)"
    docker logs "$PINGAP_NAME" >"$WORK/pingap-$1.log" 2>&1 || true
  fi
  stop_controller
}

: >"$RES"
out "machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(uname -sr)"
out "docker: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes, {{.OperatingSystem}} {{.ServerVersion}}')"
out "kubernetes: $(k version -o json | python3 -c 'import json,sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])'), etcd $(etcd --version | awk 'NR==1{print $3}'), pingap image $PINGAP_IMAGE ($([[ $pingap_on == 1 ]] && echo on || echo off))"
out "controller: $("$BIN" --version 2>/dev/null || echo pingap-k8s), release build; defaults debounce 500ms, debounce_max 2s"

# ---- phase: synthetic ------------------------------------------------------
phase_synthetic() {
  local ns=bench-syn
  step "phase synthetic: $N Services x $E endpoints (hand-written EndpointSlices)"
  out ""; out "phase synthetic: $N upstreams x $E endpoints"
  k create namespace "$ns" >/dev/null
  py syn "$API" "$ns" "$N" "$E" setup
  phase_common_start synthetic "$ns" "$E"

  step "synthetic: scale steps (+1/-1 endpoint on all $N Services, $SCALE_ROUNDS rounds)"
  snap syn-scale-before
  out "  scale steps, API write -> etcd event: $(py syn "$API" "$ns" "$N" "$E" scale "$ETCD" /pingap/upstreams/ "$SCALE_ROUNDS")"
  snap syn-scale-after
  out "  scale steps, event-to-write histogram: $(py hist "$WORK/m-syn-scale-before.json" "$WORK/m-syn-scale-after.json")"

  local gen=0
  for s in $SYN_STEPS; do
    gen=$((gen + 1))
    step "synthetic: rolling update of all $N Services, step ${s}s"
    out "  rolling update of all $N (25% surge / 25% unavailable), step ${s}s:"
    measure_rollout "syn-roll-$s" py syn "$API" "$ns" "$N" "$E" rollout "$ETCD" /pingap/upstreams/ "$s" "$gen"
  done
  phase_common_end synthetic
  k delete namespace "$ns" --wait=false >/dev/null
}

# ---- phase: pods -----------------------------------------------------------
pods_diag() { # $1 namespace: state dump when a step does not converge
  echo "---- diagnostics ----" >&2
  k get nodes -o wide >&2 || true
  k -n "$1" get deploy --no-headers | awk '$2 !~ /^([0-9]+)\/\1$/' | head -20 >&2 || true
  k -n "$1" get pods --no-headers | grep -v ' Running ' | head -20 >&2 || true
  k get events -A --sort-by=.lastTimestamp | tail -20 >&2 || true
  ectl get --prefix /pingap/upstreams/ --print-value-only | grep -c . >&2 || true
}

phase_pods() {
  local ns=bench-pods
  step "phase pods: $N Deployments x $R pause pods"
  out ""; out "phase pods: $N upstreams x $R pods (Deployments, pause image $PAUSE_IMAGE)"
  k create namespace "$ns" >/dev/null
  for u in $(seq 0 $((N - 1))); do
    name=$(printf 'svc-%02d' "$u")
    cat <<EOF
apiVersion: v1
kind: Service
metadata: { name: $name, namespace: $ns }
spec:
  selector: { app: $name }
  ports: [{ name: http, port: 80, targetPort: 6188 }]
---
apiVersion: apps/v1
kind: Deployment
metadata: { name: $name, namespace: $ns }
spec:
  replicas: $R
  selector: { matchLabels: { app: $name } }
  template:
    metadata: { labels: { app: $name } }
    spec:
      terminationGracePeriodSeconds: 1
      enableServiceLinks: false
      automountServiceAccountToken: false
      containers:
        - name: pause
          image: "$PAUSE_IMAGE"
          imagePullPolicy: IfNotPresent
          resources:
            requests: { cpu: 1m, memory: 2Mi }
            limits: { memory: 8Mi }
---
EOF
  done >"$WORK/pods.yaml"
  k apply -f "$WORK/pods.yaml" >/dev/null
  local t0; t0=$(now)
  for u in $(seq 0 $((N - 1))); do
    k -n "$ns" rollout status "deploy/$(printf 'svc-%02d' "$u")" --timeout=600s >/dev/null
  done
  out "  $((N * R)) pods ready after $(python3 -c "import time; print(f'{time.time() - $t0:.1f}')")s"
  phase_common_start pods "$ns" "$R"

  step "pods: scale steps ($R -> $((R + 1)) -> $R on all $N Deployments, $SCALE_ROUNDS rounds)"
  snap pods-scale-before
  for _ in $(seq 1 "$SCALE_ROUNDS"); do
    k -n "$ns" scale deploy --all --replicas=$((R + 1)) >/dev/null
    wait_all_written $((R + 1)) 300 || { pods_diag "$ns"; fail "scale up not reflected"; }
    wait_quiet 2 >/dev/null
    k -n "$ns" scale deploy --all --replicas="$R" >/dev/null
    wait_all_written "$R" 300 || { pods_diag "$ns"; fail "scale down not reflected"; }
    wait_quiet 2 >/dev/null
  done
  snap pods-scale-after
  out "  scale steps, event-to-write histogram: $(py hist "$WORK/m-pods-scale-before.json" "$WORK/m-pods-scale-after.json")"

  step "pods: rollout restart of all $N Deployments"
  out "  rollout restart of all $N Deployments ($R replicas, 25% surge / 25% unavailable):"
  rollout_all() {
    k -n "$ns" rollout restart deployment >/dev/null # every Deployment in the namespace
    for u in $(seq 0 $((N - 1))); do
      k -n "$ns" rollout status "deploy/$(printf 'svc-%02d' "$u")" --timeout=900s >/dev/null
    done
  }
  measure_rollout pods-roll rollout_all
  phase_common_end pods
}

for phase in $PHASES; do
  case "$phase" in
    synthetic) phase_synthetic ;;
    pods) phase_pods ;;
    *) fail "unknown phase $phase" ;;
  esac
done

out ""
out "host 1-minute load average: max $(load_max) over the run (sampled every 10 s, limit $LOAD_MAX)"
echo
echo "==== results ===="
cat "$RES"
echo
echo "BENCHMARK DONE"

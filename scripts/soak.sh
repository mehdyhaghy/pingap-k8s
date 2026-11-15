#!/usr/bin/env bash
# Soak test: steady load through Pingap for SOAK_DURATION while a chaos loop injects
# failures, then a report with numbers (target/soak/report.md, raw data next to it).
#
# Environment (scripts/e2e-up.sh with tests/soak/kind.yaml and tests/soak/values.yaml):
#   - kind: 1 control-plane node + 3 workers; the control-plane is never failed and
#     runs the soak client (tests/soak/client.yaml: fortio, load and scrapes);
#   - 2 controller replicas, a 3-member bundled etcd, 3 Pingap replicas;
#   - the e2e echo Deployment (3 pods, 10 s preStop) behind the owned upstream "echo",
#     which has an HTTP health check (README production checklist).
# etcd, the metrics port and the Pingap admin stay ClusterIP: everything is reached
# with `kubectl exec` (soak client, etcd pod) and the API server's node proxy.
#
# Load: fortio from the soak client to Pingap's ClusterIP, SOAK_QPS requests/s over
# SOAK_CONNECTIONS connections, in back-to-back chunks of SOAK_CHUNK seconds (so
# errors and latency get timestamps).
#
# Chaos: after SOAK_WARMUP, one event at a time from SOAK_EVENTS in rotation; after
# each one the script waits until every workload is Ready again, checks that the
# echo addrs in etcd match the ready endpoints (convergence), then sleeps
# SOAK_INTERVAL. No event starts in the last SOAK_COOLDOWN seconds.
#   app-delete     delete one echo pod (and wait until it is gone)
#   app-rollout    kubectl rollout restart deployment/echo
#   app-scale      scale echo 3 -> 6 -> 3
#   leader-delete  delete the controller leader pod (alternately graceful and
#                  --grace-period=0 --force); measures the failover and waits until
#                  the pod is gone
#   etcd-delete    delete one etcd member pod (ordinals in rotation)
#   pingap-delete  delete one Pingap pod
#   node-drain     cordon + drain a worker, uncordon after 20 s
#   node-crash     docker pause the worker running the controller leader (it stops
#                  answering, like a hung or partitioned node) for SOAK_NODE_DOWN
#                  seconds, then unpause; measures the failover by lease expiry
#
# Sampling every SOAK_SAMPLE seconds: controller /metrics (both pods), pod status
# and restarts, container memory and CPU from the kubelet stats summary
# (/api/v1/nodes/<node>/proxy/stats/summary; the controller image is distroless),
# etcd member status (db size, leader, revision) every 4th sample.
#
# Pass/fail (scripts/soak-report.py): exit 1 when
#   - failed requests (non-2xx, socket errors) exceed SOAK_MAX_ERROR_RATIO of all
#     requests (default 0.001), or
#   - failed requests outside node-crash windows exceed SOAK_MAX_PLANNED_ERRORS
#     (default 0: pod deletions, rollouts, scaling, leader and etcd member loss and
#     drains must not fail a request), or
#   - the echo addrs did not converge after an event, a container restarted, or more
#     than 5% of the load chunks could not run.
#
# Usage: scripts/soak.sh
# Env:   SOAK_DURATION      load duration: 60m (default), 10m, 1h30m, 90s ...
#        SOAK_QPS           requests per second (default 200)
#        SOAK_CONNECTIONS   fortio connections (default 8)
#        SOAK_CHUNK         seconds per load chunk (default 30)
#        SOAK_EVENTS        rotation (default: all events above, in that order)
#        SOAK_INTERVAL      quiet seconds after each event (default 60)
#        SOAK_WARMUP        seconds before the first event (default 60)
#        SOAK_COOLDOWN      quiet seconds at the end (default 60)
#        SOAK_NODE_DOWN     seconds a crashed node stays paused (default 90)
#        SOAK_SAMPLE        sampling interval in seconds (default 15)
#        SOAK_MAX_ERROR_RATIO, SOAK_MAX_PLANNED_ERRORS  thresholds (see above)
#        SOAK_OUT           output directory (default target/soak; emptied first)
#        SOAK_REUSE=1       reuse a running cluster set up by an earlier run
#                           (with SOAK_KEEP=1) instead of creating one
#        SOAK_KEEP=1        keep the cluster at the end (scripts/e2e-down.sh with
#                           E2E_CLUSTER=pk8s-soak deletes it)
#        E2E_CLUSTER        kind cluster name (default pk8s-soak)
#        KIND_NODE_IMAGE    default kindest/node:v1.37.0 (pinned digest)
#        E2E_IMAGE          controller image tag (default pingap-k8s:soak)
#        everything else scripts/e2e-up.sh reads (E2E_IMAGE, E2E_SKIP_BUILD, ...)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export E2E_CLUSTER="${E2E_CLUSTER:-pk8s-soak}"
export KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5}"
export KIND_CONFIG="${KIND_CONFIG:-$ROOT/tests/soak/kind.yaml}"
export E2E_EXTRA_VALUES="${E2E_EXTRA_VALUES:-$ROOT/tests/soak/values.yaml}"
# Its own tag, so a parallel e2e run on the same Docker host does not replace the image.
export E2E_IMAGE="${E2E_IMAGE:-pingap-k8s:soak}"
CLUSTER="$E2E_CLUSTER"

DURATION_RAW="${SOAK_DURATION:-60m}"
QPS="${SOAK_QPS:-200}"
CONNECTIONS="${SOAK_CONNECTIONS:-8}"
CHUNK="${SOAK_CHUNK:-30}"
EVENTS="${SOAK_EVENTS:-app-delete app-rollout app-scale leader-delete etcd-delete pingap-delete node-drain node-crash}"
INTERVAL="${SOAK_INTERVAL:-60}"
WARMUP="${SOAK_WARMUP:-60}"
COOLDOWN="${SOAK_COOLDOWN:-60}"
NODE_DOWN="${SOAK_NODE_DOWN:-90}"
SAMPLE="${SOAK_SAMPLE:-15}"
export SOAK_MAX_ERROR_RATIO="${SOAK_MAX_ERROR_RATIO:-0.001}"
export SOAK_MAX_PLANNED_ERRORS="${SOAK_MAX_PLANNED_ERRORS:-0}"
OUT="${SOAK_OUT:-$ROOT/target/soak}"

RELEASE=pingap-k8s
NS=pingap-k8s
APP_NS=e2e-app
CLIENT=soak-client
CTRL_SEL="app.kubernetes.io/name=pingap-k8s,app.kubernetes.io/instance=$RELEASE"
PINGAP_SEL="app.kubernetes.io/name=pingap,app.kubernetes.io/instance=$RELEASE"
ETCD_SEL="app.kubernetes.io/name=etcd,app.kubernetes.io/instance=$RELEASE"
TIMEOUT=300s

log() { printf '[soak %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
now() { perl -MTime::HiRes=time -e 'printf "%.3f\n", time'; }
# 60m, 1h30m, 90s, 45 (seconds) -> seconds.
parse_duration() {
  local s="$1" total=0 n
  [[ "$s" =~ ^[0-9]+$ ]] && { echo "$s"; return; }
  while [[ -n "$s" ]]; do
    [[ "$s" =~ ^([0-9]+)([hms])(.*)$ ]] || { echo "bad duration: $1" >&2; exit 2; }
    n=${BASH_REMATCH[1]}
    case ${BASH_REMATCH[2]} in h) n=$((n * 3600)) ;; m) n=$((n * 60)) ;; esac
    total=$((total + n))
    s=${BASH_REMATCH[3]}
  done
  echo "$total"
}
DURATION=$(parse_duration "$DURATION_RAW")

for tool in kind docker kubectl helm jq python3 perl; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }
done

export KUBECONFIG="$ROOT/target/e2e/kubeconfig"
k() { kubectl --request-timeout=20s "$@"; }

# --- environment ------------------------------------------------------------------
if [[ "${SOAK_REUSE:-0}" == "1" ]] && kind get clusters 2>/dev/null | grep -qx "$CLUSTER" \
    && [[ -s "$ROOT/target/e2e/state.env" ]]; then
  log "reusing cluster $CLUSTER"
else
  "$ROOT/scripts/e2e-up.sh"
fi
# The e2e client pods have no controller, so a drain would refuse to evict them;
# the soak has its own client.
k -n "$APP_NS" delete pod e2e-client e2e-load --ignore-not-found --wait=false >/dev/null
k apply -f "$ROOT/tests/soak/client.yaml" >/dev/null
k -n "$APP_NS" wait --for=condition=Ready "pod/$CLIENT" --timeout "$TIMEOUT" >/dev/null

rm -rf "$OUT"
mkdir -p "$OUT/load" "$OUT/logs"
PINGAP_IP=$(k -n "$NS" get svc "$RELEASE-pingap" -o jsonpath='{.spec.clusterIP}')
PINGAP_URL="http://$RELEASE-pingap.$NS.svc:80/hostname"
mapfile -t WORKERS < <(kind get nodes --name "$CLUSTER" | grep -v control-plane | sort)
[[ ${#WORKERS[@]} -ge 2 ]] || { echo "need at least 2 worker nodes, have ${#WORKERS[@]}" >&2; exit 1; }

PAUSED=""
BG_PIDS=()
cleanup() {
  local status=$?
  for p in "${BG_PIDS[@]}"; do kill "$p" 2>/dev/null || true; done
  if [[ -n "$PAUSED" ]]; then docker unpause "$PAUSED" >/dev/null 2>&1 || true; fi
  for w in "${WORKERS[@]}"; do k uncordon "$w" >/dev/null 2>&1 || true; done
  if [[ "${SOAK_KEEP:-0}" == "1" ]]; then
    log "SOAK_KEEP=1: keeping cluster $CLUSTER (KUBECONFIG=$KUBECONFIG)"
  else
    "$ROOT/scripts/e2e-down.sh" >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

T_START=$(now)
T_END=$(python3 -c "print(f'{$T_START + $DURATION:.3f}')")
before() { python3 -c "import sys; sys.exit(0 if $(now) + $1 < $T_END else 1)"; }

{
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "start=$T_START"
  echo "duration=$DURATION"
  echo "qps=$QPS"
  echo "connections=$CONNECTIONS"
  echo "chunk=$CHUNK"
  echo "events=$EVENTS"
  echo "interval=$INTERVAL"
  echo "node_down=$NODE_DOWN"
  echo "controller_version=$(awk '/^\[workspace\.package\]/{p=1;next} /^\[/{p=0} p && /^version *=/{gsub(/"/,"",$3); print $3; exit}' "$ROOT/Cargo.toml")"
  echo "controller_image=$(k -n "$NS" get deploy "$RELEASE" -o jsonpath='{.spec.template.spec.containers[0].image}')"
  echo "pingap_image=$(k -n "$NS" get deploy "$RELEASE-pingap" -o jsonpath='{.spec.template.spec.containers[0].image}')"
  echo "etcd_image=$(k -n "$NS" get sts "$RELEASE-etcd" -o jsonpath='{.spec.template.spec.containers[0].image}')"
  echo "kubernetes=$(k version -o json | jq -r .serverVersion.gitVersion)"
  echo "node_image=$KIND_NODE_IMAGE"
  echo "workers=${WORKERS[*]}"
  echo "host=$(uname -sm), docker $(docker info -f '{{.NCPU}} CPUs, {{.MemTotal}} bytes')"
} >"$OUT/meta.env"

# --- load -------------------------------------------------------------------------
# Back-to-back fortio chunks; load.tsv: index, start, end, kubectl exit status.
load_loop() {
  local i=0 t0 t1 rc
  while before 0; do
    i=$((i + 1))
    t0=$(now)
    rc=0
    k -n "$APP_NS" exec "$CLIENT" -- fortio load -allow-initial-errors -qps "$QPS" \
        -c "$CONNECTIONS" -t "${CHUNK}s" -timeout 5s -json - -resolve "$PINGAP_IP" \
        "$PINGAP_URL" >"$OUT/load/$(printf %04d $i).json" 2>"$OUT/load/$(printf %04d $i).log" \
      || rc=$?
    t1=$(now)
    printf '%d\t%s\t%s\t%d\n' "$i" "$t0" "$t1" "$rc" >>"$OUT/load.tsv"
    [[ $rc -eq 0 ]] || sleep 1
  done
}

# --- sampling -----------------------------------------------------------------------
scrape_controllers() {
  local ts name ip node
  ts=$(now)
  while read -r name ip node; do
    [[ -n "$ip" ]] || continue
    {
      printf '# soak ts=%s pod=%s node=%s\n' "$ts" "$name" "$node"
      k -n "$APP_NS" exec "$CLIENT" -- fortio curl -timeout 3s "http://$ip:9090/metrics" 2>/dev/null \
        | grep -E '^pingap_k8s_(is_leader|etcd_up|writes_total|foreign_writes_total|event_to_write_seconds|endpoints|watch_errors_total|drain_fallback)' || true
    } >>"$OUT/metrics.txt"
  done < <(k -n "$NS" get pods -l "$CTRL_SEL" -o json 2>/dev/null | jq -r '.items[]
    | select(.metadata.deletionTimestamp == null)
    | "\(.metadata.name) \(.status.podIP // "") \(.spec.nodeName // "")"')
}

sample_pods() {
  local ts
  ts=$(now)
  k get pods -n "$NS" -o json 2>/dev/null | jq -r --arg ts "$ts" '.items[] |
    [$ts, .metadata.namespace, .metadata.name, (.spec.nodeName // ""), .status.phase,
     ((.status.containerStatuses // []) | map(select(.ready)) | length | tostring),
     ((.status.containerStatuses // []) | map(.restartCount) | add // 0 | tostring),
     (if .metadata.deletionTimestamp then "terminating" else "-" end)] | @tsv' >>"$OUT/pods.tsv" || true
  k get pods -n "$APP_NS" -o json 2>/dev/null | jq -r --arg ts "$ts" '.items[] |
    [$ts, .metadata.namespace, .metadata.name, (.spec.nodeName // ""), .status.phase,
     ((.status.containerStatuses // []) | map(select(.ready)) | length | tostring),
     ((.status.containerStatuses // []) | map(.restartCount) | add // 0 | tostring),
     (if .metadata.deletionTimestamp then "terminating" else "-" end)] | @tsv' >>"$OUT/pods.tsv" || true
}

# Container memory and CPU from the kubelet (no metrics-server needed).
sample_usage() {
  local ts n
  ts=$(now)
  for n in "${WORKERS[@]}"; do
    [[ "$n" == "$PAUSED_NOW" ]] && continue
    k get --request-timeout=5s --raw "/api/v1/nodes/$n/proxy/stats/summary" 2>/dev/null \
      | jq -r --arg ts "$ts" --arg node "$n" --arg ns "$NS" '.pods[] | select(.podRef.namespace == $ns or .podRef.namespace == "e2e-app") |
          .podRef as $p | .containers[] |
          [$ts, $node, $p.namespace, $p.name, .name, (.memory.workingSetBytes // 0 | tostring),
           (.memory.rssBytes // 0 | tostring), (.cpu.usageNanoCores // 0 | tostring)] | @tsv' \
      >>"$OUT/usage.tsv" || true
  done
}

etcd_pod() {
  k -n "$NS" get pods -l "$ETCD_SEL" -o json 2>/dev/null | jq -r '[.items[] | select(.metadata.deletionTimestamp == null)
    | select(any(.status.containerStatuses[]?; .ready))] | .[0].metadata.name // empty'
}

sample_etcd() {
  local ts pod
  ts=$(now)
  pod=$(etcd_pod)
  [[ -n "$pod" ]] || return 0
  k -n "$NS" exec "$pod" -c etcd -- etcdctl endpoint status --cluster -w json --command-timeout=3s 2>/dev/null \
    | jq -r --arg ts "$ts" '.[] | [$ts, .Endpoint, (.Status.dbSize | tostring), (.Status.dbSizeInUse | tostring),
        (.Status.leader | tostring), (.Status.header.member_id | tostring), (.Status.raftTerm | tostring),
        (.Status.header.revision | tostring)] | @tsv' >>"$OUT/etcd.tsv" || true
}

PAUSED_NOW=""
sampler_loop() {
  local i=0
  while true; do
    PAUSED_NOW=$(cat "$OUT/.paused" 2>/dev/null || true)
    scrape_controllers
    sample_pods
    sample_usage
    if ((i % 4 == 0)); then sample_etcd; fi
    i=$((i + 1))
    sleep "$SAMPLE"
  done
}

# --- chaos helpers --------------------------------------------------------------------
event_start() { EV_KIND="$1"; EV_TARGET="$2"; EV_T0=$(now); log "event $EV_KIND $EV_TARGET"; }
# event_end <result> <detail>
event_end() {
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$EV_T0" "$(now)" "$EV_KIND" "$EV_TARGET" "$1" "$2" >>"$OUT/events.tsv"
}

leader_pod() {
  local name ip
  while read -r name ip; do
    [[ -n "$ip" ]] || continue
    if k -n "$APP_NS" exec "$CLIENT" -- fortio curl -timeout 2s "http://$ip:9090/metrics" 2>/dev/null \
        | grep -qx 'pingap_k8s_is_leader 1'; then
      echo "$name"
      return
    fi
  done < <(k -n "$NS" get pods -l "$CTRL_SEL" -o json \
    | jq -r '.items[] | select(.metadata.deletionTimestamp == null) | "\(.metadata.name) \(.status.podIP // "")"')
}

wait_settled() {
  k -n "$APP_NS" rollout status deployment/echo --timeout "$TIMEOUT" >/dev/null
  k -n "$NS" rollout status "deployment/$RELEASE" --timeout "$TIMEOUT" >/dev/null
  k -n "$NS" rollout status "deployment/$RELEASE-pingap" --timeout "$TIMEOUT" >/dev/null
  k -n "$NS" rollout status "statefulset/$RELEASE-etcd" --timeout "$TIMEOUT" >/dev/null
  k wait --for=condition=Ready node --all --timeout "$TIMEOUT" >/dev/null
  k -n "$NS" wait --for=condition=Ready pod --all --timeout "$TIMEOUT" >/dev/null
}

# Sorted "ip:port" of the ready echo endpoints, and of the echo addrs in etcd.
ready_endpoints() {
  k -n "$APP_NS" get endpointslices -l kubernetes.io/service-name=echo -o json | jq -r '.items[] |
    (.ports[] | select(.name == "http") | .port) as $port | .endpoints[] |
    select(.conditions.ready == true) | .addresses[] | "\(.):\($port)"' | sort
}
etcd_addrs() {
  local pod
  pod=$(etcd_pod)
  [[ -n "$pod" ]] || return 1
  k -n "$NS" exec "$pod" -c etcd -- etcdctl get /pingap/upstreams/echo.toml --print-value-only \
    | python3 -c 'import sys,tomllib
doc = tomllib.loads(sys.stdin.read())
for a in doc.get("upstreams", {}).get("echo", doc).get("addrs", []): print(a.split()[0])' | sort
}
# Seconds until the etcd addrs equal the ready endpoints (polled), or "timeout".
converge() {
  local t0 deadline e a
  t0=$(now)
  deadline=$(python3 -c "print($t0 + 60)")
  while python3 -c "import sys; sys.exit(0 if $(now) < $deadline else 1)"; do
    e=$(ready_endpoints 2>/dev/null || true)
    a=$(etcd_addrs 2>/dev/null || true)
    if [[ -n "$e" && "$e" == "$a" ]]; then
      python3 -c "print(f'{$(now) - $t0:.1f}')"
      return 0
    fi
    sleep 1
  done
  echo timeout
  {
    echo "== converge timeout at $(now)"
    echo "ready: $e"
    echo "etcd:  $a"
  } >>"$OUT/converge-failures.txt"
  return 1
}

# Waits until pod <name> with uid <uid> is gone (a StatefulSet pod comes back under the
# same name with a new uid).
wait_gone() { # <ns> <name> <uid>
  local u
  for _ in $(seq 1 150); do
    u=$(k -n "$1" get pod "$2" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    [[ "$u" != "$3" ]] && return 0
    sleep 2
  done
  return 1
}
# Waits for a Deployment change to finish: the controller saw the new generation, the
# rollout is complete, and no old pod is left terminating (preStop, shutdown).
wait_rollout() { # <ns> <deployment> <pod selector>
  local gen want
  for _ in $(seq 1 30); do
    gen=$(k -n "$1" get deploy "$2" -o jsonpath='{.metadata.generation} {.status.observedGeneration}')
    [[ "${gen% *}" == "${gen#* }" ]] && break
    sleep 1
  done
  k -n "$1" rollout status "deployment/$2" --timeout "$TIMEOUT" >/dev/null || return 1
  want=$(k -n "$1" get deploy "$2" -o jsonpath='{.spec.replicas}')
  for _ in $(seq 1 90); do
    [[ $(k -n "$1" get pods -l "$3" --no-headers 2>/dev/null | wc -l | tr -d ' ') -le $want ]] && return 0
    sleep 2
  done
  return 1
}
uid_of() { k -n "$1" get pod "$2" -o jsonpath='{.metadata.uid}'; }

random_pod() { # <ns> <selector>
  k -n "$1" get pods -l "$2" -o json | jq -r '[.items[] | select(.metadata.deletionTimestamp == null)
    | select(.status.phase == "Running")] | .[].metadata.name' | python3 -c 'import random,sys; l=sys.stdin.read().split(); print(random.choice(l) if l else "")'
}

pods_on() { # <node>
  k get pods -n "$NS" -o wide --field-selector "spec.nodeName=$1" --no-headers 2>/dev/null | awk '{print $1}' | tr '\n' ' '
  k get pods -n "$APP_NS" -o wide --field-selector "spec.nodeName=$1" --no-headers 2>/dev/null | awk '{print $1}' | tr '\n' ' '
}

N_LEADER=0
N_ETCD=0
N_DRAIN=0
N_CRASH=0
run_event() {
  local kind="$1" pod detail="" t0 old new w uid rc=0
  case "$kind" in
    app-delete)
      pod=$(random_pod "$APP_NS" app=echo)
      event_start "$kind" "$pod"
      uid=$(uid_of "$APP_NS" "$pod")
      k -n "$APP_NS" delete pod "$pod" --wait=false >/dev/null
      wait_gone "$APP_NS" "$pod" "$uid" || rc=1
      ;;
    app-rollout)
      event_start "$kind" deployment/echo
      k -n "$APP_NS" rollout restart deployment/echo >/dev/null
      wait_rollout "$APP_NS" echo app=echo || rc=1
      ;;
    app-scale)
      event_start "$kind" "deployment/echo 3->6->3"
      k -n "$APP_NS" scale deployment/echo --replicas 6 >/dev/null
      wait_rollout "$APP_NS" echo app=echo || rc=1
      sleep 15
      k -n "$APP_NS" scale deployment/echo --replicas 3 >/dev/null
      wait_rollout "$APP_NS" echo app=echo || rc=1
      ;;
    leader-delete)
      old=$(leader_pod)
      if [[ -z "$old" ]]; then
        event_start "$kind" "-"
        event_end fail "no single leader found"
        return
      fi
      N_LEADER=$((N_LEADER + 1))
      local how=graceful flags=()
      # --force --grace-period=0 removes the Pod object at once; the kubelet still
      # stops the container with SIGTERM.
      if ((N_LEADER % 2 == 0)); then how=force; flags=(--grace-period=0 --force); fi
      event_start "$kind" "$old ($how)"
      uid=$(uid_of "$NS" "$old")
      t0=$(now)
      k -n "$NS" delete pod "$old" --wait=false "${flags[@]}" >/dev/null 2>&1
      new=""
      for _ in $(seq 1 120); do
        new=$(leader_pod || true)
        [[ -n "$new" && "$new" != "$old" ]] && break
        new=""
        sleep 0.5
      done
      if [[ -n "$new" ]]; then
        detail="failover_s=$(python3 -c "print(f'{$(now) - $t0:.1f}')") new=$new"
      else
        detail="no new leader within 60s"
        rc=1
      fi
      wait_gone "$NS" "$old" "$uid" || rc=1
      ;;
    etcd-delete)
      pod="$RELEASE-etcd-$((N_ETCD % 3))"
      N_ETCD=$((N_ETCD + 1))
      local status leader_id
      status=$(k -n "$NS" exec "$pod" -c etcd -- etcdctl endpoint status -w json --command-timeout=3s 2>/dev/null || true)
      leader_id=$(jq -r '.[0] | if .Status.leader == .Status.header.member_id then "leader" else "follower" end' <<<"$status" 2>/dev/null || echo "?")
      event_start "$kind" "$pod ($leader_id)"
      uid=$(uid_of "$NS" "$pod")
      k -n "$NS" delete pod "$pod" --wait=false >/dev/null
      wait_gone "$NS" "$pod" "$uid" || rc=1
      ;;
    pingap-delete)
      pod=$(random_pod "$NS" "$PINGAP_SEL")
      event_start "$kind" "$pod"
      uid=$(uid_of "$NS" "$pod")
      k -n "$NS" delete pod "$pod" --wait=false >/dev/null
      wait_gone "$NS" "$pod" "$uid" || rc=1
      ;;
    node-drain)
      w=${WORKERS[$((N_DRAIN % ${#WORKERS[@]}))]}
      N_DRAIN=$((N_DRAIN + 1))
      event_start "$kind" "$w"
      detail="pods: $(pods_on "$w")"
      k drain "$w" --ignore-daemonsets --delete-emptydir-data --timeout 240s >"$OUT/logs/drain-$N_DRAIN.log" 2>&1 || rc=$?
      sleep 20
      k uncordon "$w" >/dev/null
      ;;
    node-crash)
      # The controller leader's node, so the standby has to wait for the lease to
      # expire (a deleted leader resigns at once); the next worker in rotation when
      # the leader is not found.
      old=$(leader_pod || true)
      w=""
      [[ -n "$old" ]] && w=$(k -n "$NS" get pod "$old" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)
      [[ " ${WORKERS[*]} " == *" $w "* ]] || w=${WORKERS[$(((N_CRASH + 1) % ${#WORKERS[@]}))]}
      N_CRASH=$((N_CRASH + 1))
      event_start "$kind" "$w"
      detail="pods: $(pods_on "$w")"
      PAUSED="$w"
      echo "$w" >"$OUT/.paused"
      t0=$(now)
      docker pause "$w" >/dev/null
      new=""
      if [[ -n "$old" ]]; then
        for _ in $(seq 1 60); do
          new=$(leader_pod || true)
          [[ -n "$new" && "$new" != "$old" ]] && break
          new=""
          sleep 0.5
        done
      fi
      [[ -n "$new" ]] && detail="failover_s=$(python3 -c "print(f'{$(now) - $t0:.1f}')") new=$new; $detail"
      python3 -c "import time; time.sleep(max(0, $t0 + $NODE_DOWN - time.time()))"
      docker unpause "$w" >/dev/null
      PAUSED=""
      rm -f "$OUT/.paused"
      ;;
    *)
      echo "unknown event: $kind" >&2
      exit 2
      ;;
  esac
  if ! wait_settled 2>>"$OUT/logs/settle.log"; then
    rc=1
    detail="$detail; not settled within $TIMEOUT"
  fi
  local c
  c=$(converge || true)
  detail="converge_s=$c${detail:+; $detail}"
  [[ "$c" == timeout ]] && rc=1
  event_end "$([[ $rc -eq 0 ]] && echo ok || echo fail)" "$detail"
  log "  $EV_KIND done: $detail"
}

# --- run ----------------------------------------------------------------------------
# Pre-flight: the controller has written the echo addrs and Pingap answers.
pre=$(converge || true)
if [[ "$pre" == timeout ]] || ! k -n "$APP_NS" exec "$CLIENT" -- fortio curl -timeout 5s \
    -resolve "$PINGAP_IP" "$PINGAP_URL" >/dev/null 2>&1; then
  log "pre-flight failed: echo addrs not written or Pingap not answering"
  k -n "$NS" logs -l "$CTRL_SEL" --tail=30 --prefix >&2 || true
  exit 1
fi
rm -f "$OUT/converge-failures.txt"
T_START=$(now)
T_END=$(python3 -c "print(f'{$T_START + $DURATION:.3f}')")
echo "start=$T_START" >>"$OUT/meta.env"
log "load: $QPS qps, $CONNECTIONS connections, $DURATION s through $PINGAP_URL ($PINGAP_IP)"
log "workers: ${WORKERS[*]}; events: $EVENTS"
sampler_loop &
BG_PIDS+=($!)
load_loop &
LOAD_PID=$!
BG_PIDS+=("$LOAD_PID")

sleep "$WARMUP"
read -r -a ROTATION <<<"$EVENTS"
idx=0
while before "$COOLDOWN"; do
  run_event "${ROTATION[$((idx % ${#ROTATION[@]}))]}"
  idx=$((idx + 1))
  before "$((COOLDOWN + INTERVAL))" || break
  sleep "$INTERVAL"
done
log "chaos done ($idx events); waiting for the load to finish"
wait "$LOAD_PID" || true

# --- final state ----------------------------------------------------------------------
final=$(converge || true)
echo "final_converge=$final" >>"$OUT/meta.env"
scrape_controllers
sample_pods
sample_etcd
for p in $(k -n "$NS" get pods -l "$CTRL_SEL" -o jsonpath='{.items[*].metadata.name}'); do
  k -n "$NS" logs "$p" -c controller >"$OUT/logs/$p.log" 2>&1 || true
done
k get events -A --sort-by=.lastTimestamp >"$OUT/logs/events.txt" 2>&1 || true
k get pods -A -o wide >"$OUT/logs/pods.txt" 2>&1 || true

log "writing $OUT/report.md"
python3 "$ROOT/scripts/soak-report.py" "$OUT"

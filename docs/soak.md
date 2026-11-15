# Soak test

`scripts/soak.sh` runs steady load through Pingap for an hour while a chaos loop deletes app
pods, rolls and scales the app, deletes the controller leader, an etcd member and a Pingap pod,
drains a node and freezes a node. It records every failed request, latency, the controller's
metrics, memory and restarts, and writes a report. This page describes the setup, what is
injected, the numbers of one run, and how to run it again.

## Results

One run of 60 minutes on 2026-11-15: controller 0.16.21 (with the etcd keep-alive change below),
Pingap 0.16.2, etcd 3.7.3, Kubernetes 1.37.0 (kindest/node v1.37.0) on kind 0.33.0, Docker
Desktop with 4 CPUs and 16 GB on an arm64 Mac. 37 events in four and a half rotations.
**Result: pass.**

| What | Result |
| --- | --- |
| Requests | 701,470 (194/s over 60.4 min) |
| Failed requests | 106 (0.015 %), all during the 4 node failures; 0 during the other 33 events |
| Latency through Pingap | p50 0.8 ms, p90 1.8 ms, p99 2.9 ms, p99.9 4.9 ms; max 10 s (timeouts to a paused node) |
| Event-to-write (EndpointSlice change to etcd write) | p50 0.68 s, p90 1.39 s, p99 1.90 s (38 writes) |
| Writes | 38 ok, 22 unchanged, 1 error (the etcd member it used was being deleted; retried) |
| Addrs in etcd equal the ready endpoints | within 0.5 s after every event settled, and at the end |
| Leader failover, leader pod deleted | 0.1 to 0.8 s (5 times; the leader resigns) |
| Leader failover, leader's node paused | 12.8 to 16.2 s (4 times; the 10 s lease expires) |
| Leader changes | 13; never two leaders at once in the 211 samples |
| Controller memory | 2.4 MiB at start, 2.9 MiB max (cgroup RSS; 3.7 MiB working set); flat over the hour |
| Controller CPU | 0.5 millicores mean, 1.4 max |
| Pingap memory | 17.6 to 24.6 MiB RSS per pod |
| etcd | db 24 KiB to 68 KiB, revision +70, 5 etcd leader changes |
| Container restarts | 0 |

Per event type:

| Event | Count | Failed requests | Longest event |
| --- | --- | --- | --- |
| `app-delete` | 5 | 0 | 14 s |
| `app-rollout` | 5 | 0 | 16 s |
| `app-scale` | 5 | 0 | 31 s |
| `leader-delete` | 5 | 0 | 5 s |
| `etcd-delete` | 5 (once the etcd leader) | 0 | 17 s |
| `pingap-delete` | 4 | 0 | 49 s |
| `node-drain` | 4 | 0 | 74 s |
| `node-crash` | 4 | 106 (13, 37, 31, 25) | 102 s |

What the numbers say:

- **Planned operations fail no request.** App rollouts, scaling and pod deletions (10 s
  `preStop`), controller leader loss, etcd member loss and node drains: 0 failures in 33
  events. Latency stays flat during them (worst 30 s p99: 5 ms).
- **A hung node costs requests until it is marked `NotReady`.** All 106 failures are client-side
  timeouts (fortio code -1), none a 5xx from Pingap, in the window before Kubernetes marked the
  paused node `NotReady` (about 45 s) and removed its endpoints: requests on connections the
  Pingap Service had sent to the paused node's Pingap pod, and requests a healthy Pingap sent to
  the paused node's echo pod before its health check (every 2 s) took it out. The two node
  failures with two Pingap replicas on the paused node (the spread is a preference, and drains
  had moved them) cost 37 and 31 requests, those with one 13 and 25.
- **The controller is not on the request path** and recovers on its own: with the leader on the
  paused node, the standby took over once the lease expired, and every addrs change after the
  failure was written.
- **No leak in an hour**: controller memory moved from 2.4 to 2.9 MiB, etcd's database stayed
  under 70 KiB with auto-compaction.

### Found by the soak test

Short runs before this one found that a controller connected to an etcd member that stops
answering (a paused node keeps the TCP connection open) was stuck for up to 40 s, the
HTTP/2 keep-alive interval plus timeout it had copied from Pingap (30 s + 10 s). One leader
failover took 64 s that way. The controller now uses 10 s + 5 s, and
`tests/etcd_blackhole.rs` checks that writes resume within 25 s after the connection goes
silent (about 15 s measured). With that, the four node failures above failed over in 13 to 16 s.

The run also showed that the replicas of a Deployment spread with `ScheduleAnyway` do not move
back after a drain, so a later node failure took two of the three Pingap replicas at once; the
[production checklist](../README.md#production-checklist) now says so.

## Setup

A kind cluster with one control-plane node and three workers (`tests/soak/kind.yaml`), set up by
`scripts/e2e-up.sh` with `tests/e2e/values.yaml` and `tests/soak/values.yaml`:

- 2 controller replicas (leader election in etcd), a 3-member bundled etcd with persistent
  volumes, 3 quickstart Pingap replicas (spread over the workers, 15 s `preStop`).
- One owned upstream, `echo`, following the e2e echo Deployment (3 pods, `preStop` sleep 10 s,
  readiness every 2 s), set up as the [production checklist](../README.md#production-checklist)
  says: an HTTP health check (`/hostname`, 1 s connect and read timeouts, every 2 s) and a 1 s
  upstream connect timeout.
- The load client (`tests/soak/client.yaml`, fortio) runs on the control-plane node, which is
  never failed. It sends 200 requests/s over 8 keep-alive connections to Pingap's ClusterIP
  (`GET /hostname`, 5 s timeout), in back-to-back 30 s runs, so every failed request has a
  30 s time window.
- etcd, the metrics port and the Pingap admin stay `ClusterIP`. The script reaches them with
  `kubectl exec` (fortio in the client pod, `etcdctl` in an etcd pod) and reads container
  memory from the kubelet stats summary through the API server
  (`/api/v1/nodes/<node>/proxy/stats/summary`).

## What is injected

After 60 s of warm-up, one event at a time, in this rotation. After each event the script waits
until every workload is Ready again, checks that the `echo` addrs in etcd equal the ready
endpoints, and waits 60 s.

| Event | What it does |
| --- | --- |
| `app-delete` | delete one echo pod |
| `app-rollout` | `kubectl rollout restart deployment/echo` (maxSurge 1, maxUnavailable 0) |
| `app-scale` | scale echo from 3 to 6, then back to 3 |
| `leader-delete` | delete the controller leader pod, alternately graceful and with `--grace-period=0 --force` |
| `etcd-delete` | delete one etcd member pod (ordinals in rotation) |
| `pingap-delete` | delete one Pingap pod |
| `node-drain` | `kubectl drain` a worker (PodDisruptionBudgets apply), uncordon after 20 s |
| `node-crash` | `docker pause` the worker running the controller leader for 90 s, then unpause |

A paused node stops answering without closing connections, like a hung or partitioned machine:
its kubelet stops reporting, so Kubernetes marks the node `NotReady` only after the node monitor
grace period (about 45 s here), and only then are its pods' endpoints marked not ready. Whatever
runs on it (a Pingap pod, an etcd member, the controller leader, app pods) is gone at once for
the clients.

The run fails (exit 1) when:

- failed requests (non-2xx or socket errors) exceed 0.1 % of all requests
  (`SOAK_MAX_ERROR_RATIO`). At 200 requests/s an hour is 720,000 requests, so 720 failures:
  room for the node crashes (see below), but a missing `preStop` or a drain that cuts
  connections goes far over it;
- any request fails outside a node-crash window (`SOAK_MAX_PLANNED_ERRORS`, default 0). Pod
  deletions, rollouts, scaling, controller and etcd failover and drains are all planned
  operations the README says do not interrupt traffic;
- after an event the addrs do not match the ready endpoints within 60 s, a container restarts,
  or more than 5 % of the load runs could not start.

## Caveats

- All four nodes are containers on one machine: a paused node still shares the host's CPU,
  disk and network, and nothing is lost on the wire. Real node and network failures can take
  longer to detect and can lose more requests in flight.
- `docker pause` freezes a node; it does not reboot it. Pods come back as they were, so a
  node failure here never restarts a container.
- Each 30 s load run starts a new `kubectl exec`; the time that takes carries no load (3 % of
  the hour here, hence 194 instead of 200 requests/s). A failed request is placed within its
  30 s run, not to the second.
- Controller metrics are scraped every 15 s; increments in the last interval before a pod is
  deleted are lost, so the write counts are slightly low. Failover times are measured by
  polling the metrics and are accurate to about a second.
- One upstream and 3 to 6 app pods: this is a resilience test, not a scale test. For scale see
  [benchmarks](benchmarks.md).

## Rerun

```sh
scripts/soak.sh                                   # 60 minutes, then deletes the cluster
SOAK_DURATION=10m SOAK_INTERVAL=20 scripts/soak.sh  # a short run
SOAK_KEEP=1 scripts/soak.sh                       # keep the cluster (E2E_CLUSTER=pk8s-soak scripts/e2e-down.sh)
```

It needs kind, Docker, kubectl, Helm, jq, Python 3.11+ and perl, and about 4 CPUs and 6 GB for
Docker. The cluster is `pk8s-soak` and the controller image `pingap-k8s:soak`, so it does not
collide with an e2e run on the same machine. All settings are environment variables, listed at
the top of the script. The report is `target/soak/report.md`, next to the raw data:
`load/*.json` (fortio results per 30 s), `events.tsv`, `metrics.txt` (controller metrics per
sample), `usage.tsv` (memory and CPU), `etcd.tsv`, `pods.tsv` and the controller logs.

On GitHub: Actions > soak > Run workflow, or `gh workflow run soak -f duration=60m`. The report is
the job summary and the `soak-report` artifact.

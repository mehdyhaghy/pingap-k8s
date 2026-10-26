# Benchmarks

the design notes asks for four numbers at 50 upstreams × 20 endpoints: event-to-write latency,
etcd write rate during a rollout, controller memory, and Pingap reload CPU per write. This page
reports what was measured on one laptop, how, and what is estimated rather than measured.

Summary (release build, default `debounce = 500ms`, `debounce_max = 2s`):

| What | Result |
|---|---|
| Event-to-write latency (EndpointSlice change to etcd watch event) | p50 0.520 s, p99 0.536 s; the 500 ms debounce is all but ~20 ms of it |
| Same, during rollouts | p50 0.63 to 0.75 s, p99 0.75 to 1.75 s; up to ~3 s when changes keep coming faster than the debounce (capped by `debounce_max`) |
| etcd write rate, rolling update of all 50 upstreams at once | 47 writes/s peak (synthetic, 20 endpoints each); 3.7 to 5.5 writes/s for a real `kubectl rollout restart`; exactly 1 etcd revision per write |
| Controller memory (RSS) | ~29 MB idle, ~32 MB peak |
| Controller CPU | ~0.5 ms per etcd write (0.20 s for 400 writes) |
| Pingap 0.16.1 CPU per write | 18 to 20 ms (synthetic, 20 endpoints) and 140 to 210 ms (real rollouts); ~0.4 to 0.6 s per Pingap reload, ~7 ms per upstream Pingap rebuilds |
| Pure CPU work per sync (micro-benchmarks) | resolve 2.6 µs per upstream (20 endpoints); `replace_addrs` 10.5 µs per changed upstream |

The controller side is cheap; during a rollout of every Deployment at once, the dominant cost is
Pingap applying the changes (about 70 to 75 % of one core for the length of the rollout).

## Machine

| | |
|---|---|
| Host | Apple M4, 10 cores, 32 GB, macOS 27.0.1 |
| Docker | Docker Desktop 29.9.2, VM with 4 CPUs and 15.8 GiB; about 29 unrelated containers running (mostly idle) |
| Kubernetes | kind 0.33.0, one node `kindest/node:v1.32.2` |
| etcd | 3.7.3, single member on the host, bound to 127.0.0.1 only |
| Pingap | stock image `vicanso/pingap:0.16.1`, `--autoreload` |
| Controller | `pingap-k8s` 0.1.0, release build, rustc 1.99.1, run on the host (not in the cluster) |
| Test pods | `registry.k8s.io/pause:3.10`, requests 1m CPU / 2 Mi memory |

Host 1-minute load average during the reported runs: at most 3.6 (synthetic + 100 pods) and 4.1
(400 pods), sampled every 10 s. The script aborts if it stays above a limit (default 20) for 2
minutes; one earlier run was aborted that way because of an unrelated workload and is not
reported.

## Scale actually used

| Phase | Upstreams | Endpoints per upstream | Pods | Measured |
|---|---|---|---|---|
| synthetic | 50 | 20 (scale steps: 20 ↔ 21) | none | the full 50 × 20 |
| pods, R=2 | 50 | 2 (scale steps: 2 ↔ 3) | 100 | real Deployments |
| pods, R=8 | 50 | 8 (scale steps: 8 ↔ 9) | 400 | real Deployments |

50 × 20 with real pods (1000 pods, ~1250 during a 25 % surge rollout, on one kind node in a
4-CPU VM shared with other containers) was not attempted: an earlier attempt at that size on
this machine, at the same time as another cluster, overloaded Docker. The synthetic phase covers
the full 50 × 20 for everything the controller and Pingap see (EndpointSlices with 20 endpoints,
upstreams with 20 addresses); what it does not cover is the kubelet and EndpointSlice controller
producing those slices from real pods. Numbers for 1000 real pods below are estimates and are
marked as such.

## Method

`scripts/bench-local.sh` (one run is about 6 minutes for the default phases, 4 minutes for the
400-pod phase):

1. Builds the release binary (`nice -n 10`), starts etcd on 127.0.0.1, creates the kind cluster
   `pk8s-bench` (deleted on exit, also on failure), and runs `kubectl proxy` on 127.0.0.1.
2. Seeds the etcd prefix with Pingap's `--sync` (one server, 50 locations, 50 upstreams with a
   placeholder address) and starts stock Pingap with `--conf etcd://host.docker.internal:<port>/pingap
   --autoreload`. Docker Desktop forwards `host.docker.internal` to the host's loopback, so etcd
   never listens on a non-loopback address. Inside the Pingap container, `10.0.0.0/8` is routed to
   its loopback so every backend address (synthetic or pod IP) answers Pingap's TCP health check
   on Pingap's own listener; otherwise health checks hang on unreachable addresses.
3. Starts the controller on the host with 50 `[upstreams.svc-NN]` tables, one Service each, all
   defaults. A fresh controller per phase.
4. **synthetic**: 50 selector-less Services with one hand-written EndpointSlice of 20 endpoints
   each, patched through the API:
   - scale steps: +1 then −1 endpoint on all 50 slices at once, 3 rounds (300 samples). Latency
     is timed from the API write returning to the `etcdctl watch` event that carries the new
     address count, per upstream;
   - simulated rolling updates of all 50 upstreams at once (25 % surge, 25 % unavailable: 4 waves
     of "5 new not-ready + 5 old terminating", then "new ready, old gone"), with 1 s and 0.25 s
     between patches.
5. **pods**: 50 Deployments of R pause pods behind 50 Services; scale every Deployment R → R+1 → R
   (3 rounds for R=2, 2 rounds for R=8), then `kubectl rollout restart` of all 50 Deployments
   at once, waiting for every rollout.
6. For each rollout: `pingap_k8s_writes_total{result="ok"}` delta, etcd revision delta, controller
   CPU time (`ps`), Pingap container CPU from its cgroup `cpu.stat` (`usage_usec`) minus its idle
   rate measured for 20 s before, `docker stats` samples, and Pingap's own log lines
   (`reload upstream success` per applied batch, `starting static discovery` per upstream it
   rebuilds). The window ends 3 s after the last write.
7. Latency quantiles from `pingap_k8s_event_to_write_seconds` are computed from bucket deltas with
   linear interpolation (as `histogram_quantile` does). Controller RSS is sampled with `ps` every
   0.2 s.

Micro-benchmarks: `cargo run --release --example bench_resolve` times `resolve::resolve` and
`etcd::replace_addrs` with plain `Instant` loops (no framework), 7 rounds of at least 300 ms after
a warm-up; the table gives the median of the per-run medians over 3 runs.

## Results

### Event-to-write latency

| Scenario | Samples | Source | mean | p50 | p95 | p99 |
|---|---|---|---|---|---|---|
| synthetic, scale steps (all 50 at once) | 300 | etcd watch | | 0.520 s | 0.532 s | 0.536 s (max 0.539) |
| synthetic, scale steps | 300 | histogram | 0.522 s | 0.625 s | 0.738 s | 0.748 s |
| synthetic, rolling update, 1 s steps | 400 | histogram | 0.518 s | 0.625 s | 0.738 s | 0.748 s |
| synthetic, rolling update, 0.25 s steps | 100 | histogram | 1.267 s | 0.750 s | 2.900 s | 2.980 s |
| pods R=2, scale steps | 300 | histogram | 0.552 s | 0.626 s | 0.739 s | 0.749 s |
| pods R=2, rollout restart of all 50 | 200 | histogram | 0.544 s | 0.625 s | 0.738 s | 0.748 s |
| pods R=8, scale steps | 200 | histogram | 0.578 s | 0.637 s | 0.853 s | 0.971 s |
| pods R=8, rollout restart of all 50 | 301 | histogram | 0.758 s | 0.713 s | 1.406 s | 1.749 s |

- The debounce sets the latency: the first change to an upstream opens a 500 ms window, and the
  write follows ~20 ms after it closes (watch delivery, resolve, read-modify-write, etcd watch).
- The histogram's buckets around the debounce are 0.5 / 0.75 / 1.0 s, so its interpolated p50
  (0.625 s) overstates the precise value: the mean (0.52 s) and the etcd watch timing show that
  nearly all samples sit just above 0.5 s.
- When an upstream keeps changing faster than every 500 ms (0.25 s steps), the window stretches
  until `debounce_max` (2 s) forces a write; latency is measured from the first event of the
  window, so p95 lands in the 2 to 3 s bucket. Fewer writes is the intended trade-off.
- In the real 400-pod rollout, pods of one Deployment become ready at slightly different times,
  which stretches some windows past 500 ms (p95 1.4 s).

### etcd writes during a rollout

| Scenario | Duration | Writes | etcd revisions | Rate | Writes per upstream |
|---|---|---|---|---|---|
| synthetic, rolling update of all 50, 1 s steps | 8.5 s | 400 | 400 | 47.1/s | 8 (one per patch) |
| synthetic, rolling update of all 50, 0.25 s steps | 2.9 s | 100 | 100 | 35.1/s | 2 (debounced) |
| pods R=2, `rollout restart` of all 50 | 36.4 s | 200 | 200 | 5.5/s | 4 |
| pods R=8, `rollout restart` of all 50 | 81.3 s | 301 | 301 | 3.7/s | 6 |

Every write is exactly one etcd revision (no conflicts, no retries, no unchanged writes). The
debounce bounds the rate at about 2 writes/s per upstream however fast the pods churn, so 50
upstreams cannot exceed ~100 writes/s.

### Controller

| Phase | RSS idle | RSS peak | CPU during the rollout |
|---|---|---|---|
| synthetic (50 × 20) | 29.7 MB | 31.8 MB | 0.20 s for 400 writes (1 s steps), 0.09 s for 100 writes (0.25 s steps) |
| pods R=2 | 29.2 MB | 29.5 MB | 0.21 s for 200 writes over 36 s |
| pods R=8 | 28.4 MB | 28.4 MB | 0.45 s for 301 writes over 81 s |

Initial sync of all 50 upstreams: 0.83 to 0.88 s after process start (most of it the
`debounce` window). RSS is macOS RSS of the arm64 binary on the host; inside a Linux container
it will differ somewhat, but the order of magnitude (tens of MB) holds.

### Pingap 0.16.1

| Scenario | Pingap CPU above idle | Writes | Pingap reloads | Upstream rebuilds | per write | per reload | per rebuild | docker stats mean / peak |
|---|---|---|---|---|---|---|---|---|
| synthetic, 1 s steps | 8.18 s over 11.7 s | 400 | 15 | 1150 | 20.5 ms | 545 ms | 7.1 ms | 72 % / 98 % of one core |
| synthetic, 0.25 s steps | 1.80 s over 6.1 s | 100 | 3 | 250 | 18.0 ms | 601 ms | 7.2 ms | 32 % / 89 % |
| pods R=2, rollout restart | 28.2 s over 39.7 s | 200 | 76 | 4000 | 141 ms | 370 ms | 7.0 ms | 70 % / 97 % |
| pods R=8, rollout restart | 62.7 s over 84.5 s | 301 | 153 | 7951 | 208 ms | 410 ms | 7.9 ms | 75 % / 97 % |

Idle (health checks only, 50 upstreams, up to 1000 backends): 0.4 to 0.9 % of one core.

- Pingap batches etcd changes: 400 writes were applied in 15 reloads.
- From its logs, every reload rebuilt at least all 50 upstreams (each with a new static discovery
  and health check), not only the ones that changed: 52 rebuilds per reload on average, and
  52 or 104 for reloads that changed only 2 to 4 upstreams. A rebuild costs about 7 ms of CPU whether the upstream has 2 or 20
  addresses. Pingap's reload CPU therefore grows with the total number of upstreams times the
  number of reloads, and "CPU per write" depends mostly on how many writes fall into one reload.
- The synthetic rollouts change all 50 upstreams together, so writes batch well (~20 ms per
  write). A real `rollout restart` spreads the changes over 40 to 80 s, Pingap reloads every
  ~0.5 s, and each reload rebuilds everything (140 to 210 ms per write). In both cases Pingap used
  about 70 to 75 % of one core for the length of the rollout.

## Micro-benchmarks

`cargo run --release --example bench_resolve`, Apple M4, median of 3 runs (each the median of 7
rounds):

| Operation | ns/op |
|---|---|
| `resolve`, 1 upstream × 20 endpoints | 2 600 |
| `resolve`, all 50 upstreams × 20 endpoints | 154 000 |
| `resolve`, 1 upstream, rollout (5 of 20 terminating) | 2 120 |
| `resolve`, all 50 upstreams, rollout | 124 000 |
| `replace_addrs`, 1 upstream, addrs unchanged (no write) | 6 480 |
| `replace_addrs`, 1 upstream, 1 of 20 addrs changed | 10 550 |
| `replace_addrs`, all 50 upstreams changed | 534 000 |

The sample upstream document is 917 bytes of TOML with a trailing comment that must survive. A
full resync of all 50 upstreams costs under 1 ms of CPU, so the controller's CPU time is spent on
the watch streams, the etcd client and the HTTP stack, not on resolving or editing.

## Estimates for 50 × 20 real pods (not measured)

- **Latency**: the same as the synthetic 50 × 20 run (p50 ~0.52 s), since the controller only
  sees EndpointSlices; during a rollout expect the R=8 shape (p95 ~1.5 s, bounded by
  `debounce_max` plus ~20 ms).
- **etcd writes**: a 20-replica rollout with 25 % surge / 25 % unavailable runs in about 4 waves;
  with 4 writes per upstream at R=2 and 6 at R=8, expect roughly 8 to 12 writes per upstream, i.e.
  400 to 600 writes for 50 Deployments, below the ~100 writes/s debounce ceiling.
- **Controller memory**: the synthetic phase already holds 1000 endpoints (~32 MB peak); 1000
  real pods do not add controller state (it watches EndpointSlices, not pods).
- **Pingap**: around one core busy for the length of the rollout, as measured at 100 and 400 pods,
  because reload cost depends on the upstream count (50) and the reload frequency, not on the
  number of pods.

## Caveats

- One laptop, one run per configuration (the 100-pod and synthetic numbers were repeated in an
  earlier identical run and agreed within a few percent). Docker Desktop's VM also ran ~29
  unrelated, mostly idle containers.
- The controller and etcd run on the host and Pingap in the Docker VM: no network latency of a
  real cluster, single-member etcd without fsync pressure from other clients.
- Pingap's backends all point at its own listener (see Method), so health checks always succeed
  and are cheap; no traffic was proxied during the measurements.
- Pingap CPU is the container's cgroup CPU minus its idle rate, so it includes everything Pingap
  did in the window, not only reloads.
- The reload and rebuild counts come from Pingap's info logs; they describe Pingap 0.16.1 and may
  change in later versions.

## Reproduce

Requirements: kind, Docker Desktop (for `host.docker.internal`; without it the Pingap part is
skipped), kubectl, etcd and etcdctl, python3, curl, a Rust toolchain.

```sh
cargo run --release --example bench_resolve          # micro-benchmarks (no cluster)
scripts/bench-local.sh                               # synthetic 50 x 20 + 50 x 2 pods
BENCH_PHASES=pods BENCH_POD_REPLICAS=8 BENCH_SCALE_ROUNDS=2 scripts/bench-local.sh   # 400 pods
```

Useful variables: `BENCH_UPSTREAMS`, `BENCH_ENDPOINTS`, `BENCH_POD_REPLICAS`, `BENCH_SYN_STEPS`,
`BENCH_SCALE_ROUNDS`, `BENCH_PINGAP=off`, `BENCH_LOAD_MAX` (abort threshold for the host's
1-minute load average, default 20), `KIND_NODE_IMAGE`. Raw results (metrics snapshots, RSS
samples, Pingap logs, load samples) are kept in `target/bench/<timestamp>/`.

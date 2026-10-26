# pingap-k8s

Kubernetes endpoint discovery for [Pingap](https://github.com/vicanso/pingap).

`pingap-k8s` is a small Rust controller that runs in your cluster. It watches Services and
their EndpointSlices and writes the ready pod addresses into the upstreams of Pingap's etcd
config storage. Pingap instances started with `--autoreload` pick up each change and rebuild only
the upstream that changed, without a restart.

```
 Kubernetes API ──watch──▶ pingap-k8s ──put <prefix>/upstreams/api.toml──▶ etcd
                                                                             │
                          Pingap replicas (--autoreload, watch etcd) ◀───────┘
```

- **No change to Pingap.** Use the stock Pingap image. All Kubernetes knowledge lives in the
  controller.
- **Pod IPs, not a virtual IP.** Pingap balances, retries and health-checks the pods themselves, so
  sticky sessions, least connections and per-backend health checks work as intended.
- **Fast and graceful.** New ready pods are in Pingap within about 1 to 3 seconds; terminating
  pods are removed as soon as Kubernetes marks them.
- **Only `addrs` is owned.** Health checks, timeouts, TLS and the balancing algorithm of an
  upstream stay editable in Pingap's admin UI.
- **Least privilege.** Read-only access to Services and EndpointSlices in the namespaces you list.
  No ClusterRole, no writes to the cluster.
- **Fail static.** If the Kubernetes API or the controller is down, Pingap keeps the last written
  addresses and its health checks remove dead pods.

Status: phase 1 (endpoint discovery). Generating Pingap locations from Ingress and Gateway API
resources is planned for later phases.

## Contents

- [How it works](#how-it-works)
- [Install](#install)
  - [Prerequisites](#prerequisites)
  - [Helm](#helm)
  - [Quickstart: everything in one install](#quickstart-everything-in-one-install)
  - [Try it](#try-it)
  - [Plain manifests](#plain-manifests)
  - [etcd client certificates](#etcd-client-certificates)
- [Configuration reference](#configuration-reference)
- [Pingap side](#pingap-side)
- [Upstream target](#upstream-target)
- [Graceful shutdown of application pods (preStop)](#graceful-shutdown-of-application-pods-prestop)
- [Production checklist](#production-checklist)
- [Operating it with kubectl](#operating-it-with-kubectl)
- [Observability](#observability)
  - [Telemetry](#telemetry)
- [Security](#security)
- [Limitations](#limitations)
- [Development](#development)
- [License](#license)

## How it works

1. **Watch.** For every Service referenced in the config, the controller runs one watcher on its
   EndpointSlices (`kubernetes.io/service-name=<svc>`) and one on the Service object (for the
   port table). Watchers are shared between upstreams.
2. **Resolve.** For each upstream, it merges the endpoints of its Services:
   - The configured port is a Service port **name or number** (`port`, not `targetPort`). It is
     mapped to the EndpointSlice port with the same name, so named and remapped ports work. An
     unresolvable port is never guessed: the entry contributes nothing and a warning is logged.
   - Ready endpoints are used. Terminating endpoints are dropped (graceful drain). If *no* endpoint
     is ready but some are still serving while terminating (for example a single-replica
     restart), those are used until a ready one appears.
   - Addresses are sorted and deduplicated, with an optional per-Service weight
     (`ip:port weight`).
3. **Write.** After a short debounce (500 ms after the last event, at most 2 s), the controller
   reads `<prefix>/upstreams/<name>.toml` from etcd, replaces only `addrs`, and writes it back in
   an etcd transaction that fails if the key changed in between (compare-and-swap, retried).
   Nothing is written when the list is unchanged.
4. **Pingap reloads.** Every Pingap instance watching the prefix with `--autoreload` rebuilds that
   one upstream (including a health-check round) and swaps it in. In-flight requests and upgraded
   (WebSocket) connections finish on the backend they started with.
5. **Self-heal.** The controller watches the keys it owns. If a Pingap admin save writes an old
   `addrs` back, it is corrected within the debounce window. A full resync every 5 minutes is a
   safety net.
6. **High availability.** Run 2 replicas. They elect a leader through etcd (no Kubernetes write
   permission needed); only the leader writes, the follower keeps its watches warm.

If an owned upstream does not exist yet in etcd, the controller creates a minimal one (only
`addrs`) and logs it. If an upstream would end up with zero addresses, the last list is kept and
a warning is logged: Pingap rejects an empty list, and its health checks mark the stale pods down.

## Install

### Prerequisites

- Kubernetes 1.32 or newer (CI tests 1.35, the oldest minor still supported upstream, and 1.37).
- An etcd reachable from the cluster, on the private network only. You can reuse the etcd
  Pingap already uses, or let the chart deploy one (`etcd.deploy=true`).
- Pingap running against that etcd with autoreload:

  ```sh
  pingap --conf "etcd://etcd.edge.svc:2379/pingap?ca=...&cert=...&key=..." --autoreload
  ```

  Pingap must be able to reach pod IPs, so in practice it runs inside the cluster. Expose Pingap's
  listeners as usual (a `LoadBalancer` or `NodePort` Service).

The controller image is published for `linux/amd64` and `linux/arm64`.

### Helm

Write a values file with the etcd URL, the client certificate Secret and the upstreams to manage:

```yaml
# values.yaml
etcd:
  url: "etcd://etcd.edge.svc:2379/pingap?ca=/etc/pingap-k8s/etcd/ca.crt&cert=/etc/pingap-k8s/etcd/tls.crt&key=/etc/pingap-k8s/etcd/tls.key"
  tlsSecret: pingap-k8s-etcd        # mounted read-only at /etc/pingap-k8s/etcd

config:
  upstreams:
    api:
      services: ["api.prod:http"]
    web:
      services: ["web-blue.prod:http 9", "web-green.prod:http 1"]
```

Install it from a checkout of this repository:

```sh
kubectl create namespace edge
helm install pingap-k8s ./charts/pingap-k8s --namespace edge -f values.yaml
```

The chart creates:

| Object | Notes |
|---|---|
| ServiceAccount | used only by the controller |
| Role + RoleBinding **per namespace** | `get/list/watch` on `services` and `endpointslices`. The namespaces come from `rbac.namespaces` plus every namespace referenced in `config.upstreams` (`rbac.fromConfig: true`). Never a ClusterRole. |
| ConfigMap | the controller config, rendered as TOML from `config` (or a Secret with `config.useSecret: true`) |
| Deployment | 2 replicas, non-root, read-only root filesystem, all capabilities dropped, probes on `/healthz` and `/readyz` |
| PodDisruptionBudget | `minAvailable: 1` |
| Service (metrics) | always `ClusterIP` |

Config changes are applied without restarting the pods: the controller reloads its config file
when the mounted ConfigMap changes (and on `SIGHUP`). Watches are started or stopped only for the
Services that were added or removed. An invalid new config is rejected and the running one stays.

A few settings are read only at startup: `[etcd]`, `[server]`, `[kubernetes]` `kubeconfig` and
`context`, and `defaults.resync`. A change to them logs "restart required" and keeps the running
value; apply it with `kubectl -n <namespace> rollout restart deployment/pingap-k8s`.

Main chart values:

| Value | Default | Description |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/mehdyhaghy/pingap-k8s` / appVersion | controller image |
| `replicaCount` | `2` | only the leader writes |
| `etcd.url` | `""` | Pingap's etcd URL (see [config](#configuration-reference)). Empty: built for the bundled etcd when `etcd.deploy`, else `etcd://etcd:2379/pingap` |
| `etcd.tlsSecret` | `""` | Secret with the controller's etcd client certificate, mounted at `/etc/pingap-k8s/etcd` |
| `etcd.tlsKeys` | `ca.crt`, `tls.crt`, `tls.key` | file names inside the TLS Secrets |
| `config.kubernetes`, `config.defaults`, `config.upstreams` | see below | rendered into the config file |
| `rbac.namespaces` | `[]` | extra namespaces to grant read access in |
| `rbac.fromConfig` | `true` | also grant access in every namespace referenced by `config.upstreams` |
| `logFormat` / `logLevel` | `text` / `info` | log output (`text` or `json`) and filter |
| `podDisruptionBudget.minAvailable` | `1` | |
| `networkPolicy.enabled` | `false` | NetworkPolicy on the controller pods: only the sources below may reach the metrics port (none when both are empty); probes are not affected |
| `networkPolicy.allowSameNamespace` / `networkPolicy.metricsFrom` | `false` / `[]` | allow pods of the release namespace / extra NetworkPolicy `from` peers (e.g. a monitoring namespace) |
| `etcd.deploy` | `false` | deploy a bundled etcd (below) |
| `pingap.deploy` | `false` | deploy a quickstart Pingap (below) |

See [`charts/pingap-k8s/values.yaml`](charts/pingap-k8s/values.yaml) for every value.

#### Bundled etcd (`etcd.deploy: true`)

For clusters that do not run etcd yet. The chart deploys `quay.io/coreos/etcd` (pinned version)
as a StatefulSet with 1 or 3 members (`etcd.replicas`):

- a `ClusterIP` Service for clients and a headless Service for peers. **Never exposed outside
  the cluster.**
- client and peer TLS with client-certificate authentication. With `etcd.tls.autoGenerate: true`
  (default) the chart generates a CA, the server certificate and client certificates for the
  controller and for Pingap (`pingap-k8s-etcd-client` and `pingap-k8s-etcd-pingap-client` for a
  release named `pingap-k8s`). They are reused on upgrade.
- a NetworkPolicy that admits only the controller and Pingap pods to the client port, and only
  etcd members to the peer port. Pingap pods are matched by `etcd.networkPolicy.pingapPodLabels`
  (default `app.kubernetes.io/name: pingap`, in the release namespace; set
  `etcd.networkPolicy.pingapNamespaceSelector` for another namespace). Label your own Pingap
  pods accordingly. NetworkPolicy needs a CNI plugin that enforces it.
- persistent volumes (`etcd.persistence`), periodic compaction, and a PodDisruptionBudget for 3
  members.

The controller URL is then built automatically:
`etcd://pingap-k8s-etcd.<namespace>.svc:2379/pingap?ca=...&cert=...&key=...` (release
`pingap-k8s`).
Give your own Pingap the same URL and mount the `...-etcd-pingap-client` Secret at
`/etc/pingap-k8s/etcd`.

### Quickstart: everything in one install

`pingap.deploy: true` also deploys the stock Pingap image (`vicanso/pingap`, pinned version),
wired to the bundled etcd, for trying everything out in one install:

```yaml
# quickstart.yaml
etcd:
  deploy: true
pingap:
  deploy: true
  service:
    type: LoadBalancer      # or NodePort; ClusterIP by default
config:
  upstreams:
    quickstart:
      services: ["echo:http"]   # a Service named "echo" with a port named "http"
```

```sh
kubectl create namespace edge
helm install pingap-k8s ./charts/pingap-k8s --namespace edge -f quickstart.yaml
```

- Pingap runs `pingap --conf etcd://... --autoreload` as a non-root user with a read-only root
  filesystem.
- Before Pingap starts, an init container seeds the etcd prefix from `pingap.seed.config` **only if
  the prefix has no server yet**: one listener on `[::]:8080` (IPv4 and IPv6), one location, and
  the upstream
  `quickstart` (with a placeholder address the controller replaces). Servers (listeners) are
  read only at Pingap start, which is why they are seeded first.
- The proxy listener is exposed through the `pingap-k8s-pingap` Service, whose type you choose
  (`pingap.service.type`).
- Pod lifecycle: rolling updates surge one pod and never take one down first (`maxUnavailable: 0`),
  replicas spread over nodes, and a PodDisruptionBudget (`minAvailable: 1`) applies from 2
  replicas. On deletion a `preStop` sleep (15 s) lets endpoints and load balancers catch up, then
  Pingap shuts down gracefully (seeded `[basic] grace_period = "30s"`,
  `graceful_shutdown_timeout = "10s"`) within `terminationGracePeriodSeconds: 60`. Keep that sum
  below the grace period if you change any of them; see `pingap.*` in `values.yaml`.
- The admin UI listens on its own port behind a `ClusterIP`-only Service. Open it with a
  port-forward:

  ```sh
  kubectl -n edge get secret pingap-k8s-pingap-admin -o jsonpath='{.data.password}' | base64 -d; echo
  kubectl -n edge port-forward svc/pingap-k8s-pingap-admin 3018:3018
  # http://localhost:3018, user "admin"
  ```

### Try it

A demo app (`traefik/whoami`, which answers with its pod name and the request headers) behind
the quickstart Pingap, in a few commands. First the app, 3 replicas:

```sh
kubectl create namespace demo
kubectl -n demo create deployment whoami --image=traefik/whoami:v1.12.0 --replicas=3 --port=80
kubectl -n demo expose deployment whoami --port=80
```

Then the chart, with a seed whose location sends everything to the upstream `whoami` and adds
the forwarded headers, and the controller entry that keeps that upstream equal to the app's pods:

```yaml
# try.yaml
etcd:
  deploy: true
pingap:
  deploy: true
  seed:
    config: |
      [basic]
      grace_period = "30s"
      graceful_shutdown_timeout = "10s"

      [servers.quickstart]
      addr = "[::]:8080"
      locations = ["quickstart"]

      [locations.quickstart]
      upstream = "whoami"
      # X-Real-IP, X-Forwarded-For, -Proto, -Host and -Port to the app.
      enable_reverse_proxy_headers = true

      [upstreams.whoami]
      addrs = ["127.0.0.1:9"]   # placeholder, replaced by the controller
config:
  upstreams:
    whoami:
      services: ["whoami.demo:80"]
```

```sh
kubectl create namespace pingap-k8s
helm install pingap-k8s ./charts/pingap-k8s --namespace pingap-k8s -f try.yaml --wait
kubectl -n pingap-k8s port-forward svc/pingap-k8s-pingap 8080:80 &
for i in 1 2 3 4 5 6; do curl -s localhost:8080 | grep -E '^(Hostname|X-Forwarded-For|X-Real-Ip):'; done
```

Each response comes from the next pod (round robin), and the forwarded headers carry the
client address Pingap saw (`127.0.0.1` through a port-forward):

```text
Hostname: whoami-6c9f8d7b5-4kq2x
X-Forwarded-For: 127.0.0.1
X-Real-Ip: 127.0.0.1
Hostname: whoami-6c9f8d7b5-9wz7m
...
```

`kubectl -n demo scale deployment whoami --replicas=5` and the new pod names show up in the
responses within seconds. The seed is written only into an empty etcd prefix: on an existing
install, set "reverse proxy headers" on the location in the admin UI instead.

### Plain manifests

[`deploy/pingap-k8s.yaml`](deploy/pingap-k8s.yaml) is rendered from the chart with the default
values for the namespace `pingap-k8s` (`make manifests` regenerates it). Before applying it:

1. Edit `[etcd] url` and `[upstreams.*]` in the ConfigMap `pingap-k8s-config`.
2. Add a Role + RoleBinding (copy the generated pair) in every namespace your Services live in.
3. For etcd TLS, add a Secret volume mounted at `/etc/pingap-k8s/etcd` to the Deployment, or
   render your own manifests with `helm template ... --set etcd.tlsSecret=pingap-k8s-etcd`.

```sh
kubectl create namespace pingap-k8s
kubectl apply -f deploy/pingap-k8s.yaml
```

### etcd client certificates

The controller connects with the same URL format and TLS parameters as Pingap. Put the client
certificate in a Secret, never in the ConfigMap or on the command line:

```sh
kubectl -n edge create secret generic pingap-k8s-etcd \
  --from-file=ca.crt=ca.pem --from-file=tls.crt=client.pem --from-file=tls.key=client-key.pem
```

and set `etcd.tlsSecret: pingap-k8s-etcd` with `...?ca=/etc/pingap-k8s/etcd/ca.crt&cert=/etc/pingap-k8s/etcd/tls.crt&key=/etc/pingap-k8s/etcd/tls.key`
in the URL.

## Configuration reference

The controller reads one TOML file, given by `--config` or `PINGAP_K8S_CONFIG`. The chart renders
it from `config` into a ConfigMap mounted at `/etc/pingap-k8s/config/config.toml`.

```toml
[etcd]
# Same URL format Pingap uses for --conf:
# etcd://host:port[,host:port]/<prefix>?user=..&password=..&ca=..&cert=..&key=..
url = "etcd://etcd.edge.svc:2379/pingap?ca=/etc/pingap-k8s/etcd/ca.crt&cert=/etc/pingap-k8s/etcd/tls.crt&key=/etc/pingap-k8s/etcd/tls.key"

[kubernetes]
kubeconfig = ""          # empty: in-cluster, then $KUBECONFIG, then ~/.kube/config
context = ""
default_namespace = ""   # empty: the controller pod's own namespace
cluster_domain = "cluster.local"  # for target = "service" addresses

[defaults]
include_not_ready = false
drain_terminating = true
debounce = "500ms"
debounce_max = "2s"
resync = "5m"            # full re-list and re-reconcile, corrects drift
ipv4_only = false
target = "pods"          # or "service", see Upstream target

# One table per Pingap upstream the controller owns.
[upstreams.api]
services = ["api.prod:http"]

[upstreams.web]
services = ["web-blue.prod:http 9", "web-green.prod:http 1"]
drain_terminating = true # any [defaults] key can be overridden per upstream
```

| Key | Default | Meaning |
|---|---|---|
| `etcd.url` | (required) | Pingap's etcd URL. The path is the key prefix, the same one Pingap uses. TLS parameters: `ca`, `cert`, `key`, `server_name`, `tls=true`. Prefer client certificates over `user`/`password`, which would sit in the ConfigMap. |
| `kubernetes.kubeconfig` | `""` | kubeconfig path; empty means in-cluster, then `$KUBECONFIG`, then `~/.kube/config` |
| `kubernetes.context` | `""` | kubeconfig context |
| `kubernetes.default_namespace` | `""` | namespace for `services` entries without one; empty means the controller pod's namespace (`POD_NAMESPACE`) |
| `kubernetes.cluster_domain` | `cluster.local` | cluster DNS domain used in `target = "service"` addresses; applies live |
| `include_not_ready` | `false` | also write endpoints that are not ready (and not terminating) |
| `drain_terminating` | `true` | remove terminating endpoints; `false` treats serving terminating endpoints as ready |
| `debounce` | `500ms` | wait this long after the last event before writing |
| `debounce_max` | `2s` | write at the latest this long after the first pending event |
| `resync` | `5m` | reconcile every upstream at this interval even without events |
| `ipv4_only` | `false` | ignore IPv6 EndpointSlices. On dual-stack clusters each pod has one slice per family, so it is written twice (`10.x:port` and `[fd00::x]:port`); set `true` unless the Pingap pods can reach app pods over IPv6 |
| `target` | `pods` | `pods` writes ready pod IPs, `service` writes the Service DNS name and port ([Upstream target](#upstream-target)); also per upstream |
| `server.listen` | `[::]:9090` | address for `/metrics`, `/healthz` and `/readyz` (restart required). `[::]` also accepts IPv4 on Linux unless the node sets `net.ipv6.bindv6only=1`; on nodes with IPv6 disabled at boot use `0.0.0.0:9090` |
| `upstreams.<name>.services` | (required) | list of `<service>[.<namespace>]:<port> [weight]` |

- `<port>` is a Service port **name** or **number** (the `port` field, not `targetPort`).
- `weight` is a positive integer, written as `ip:port weight` for every endpoint of that Service.
- `<name>` is the Pingap upstream name (`[A-Za-z0-9_-]+`).
- Service and namespace names must be valid DNS-1035 labels, the port is required, weight 0 is
  rejected, and an upstream may be configured only once. An invalid config stops the controller
  with a clear message at startup.
- Endpoint selection: `ready` true or unset counts as ready; terminating but serving endpoints are
  draining (used only when nothing is ready); not serving or not ready endpoints are excluded;
  FQDN slices are ignored; only the first address of each endpoint is used. `ExternalName`
  Services are not supported (use Pingap's `dns` discovery for those).

Command line flags: `--config <path>` (env `PINGAP_K8S_CONFIG`, default
`/etc/pingap-k8s/config/config.toml`), `--log-format text|json` and `--check`. The log filter comes
from `RUST_LOG`.

`--check` validates the config and the etcd URL without connecting anywhere, prints a summary and
exits 0 (valid) or 1 (invalid). Use it in CI on the rendered config:

```sh
helm template pingap-k8s charts/pingap-k8s -f values.yaml --show-only templates/configmap.yaml \
  | yq '.data["config.toml"]' > config.toml
pingap-k8s --check --config config.toml
```

## Pingap side

- Run Pingap with `--conf etcd://...` and `--autoreload`.
- Create each owned upstream once, in the admin UI or by hand, with the fields you want (health
  check, timeouts, algorithm). Leave `discovery` empty (static). Or let the controller create a
  minimal one.
- `addrs` of an owned upstream belongs to the controller: a manual edit is overwritten on the next
  reconcile. Every other field is yours, and an admin save of other fields is never lost.
- Controller writes go straight to etcd, so they do not appear in Pingap's config history. The
  controller's logs and metrics are the audit trail for `addrs`.
- Pingap rewrites the whole entry on an admin save, so TOML comments in an owned upstream are kept
  by the controller but lost on the next admin save.

## Upstream target

Each owned upstream has a `target`, set in `[defaults]` or per upstream. Both modes use standard
Kubernetes addressing only.

| `target` | Written into `addrs` | Who balances | Use when |
|---|---|---|---|
| `pods` (default) | ready pod IPs from EndpointSlices | Pingap, per request | Pingap should reach the pods directly (the usual case) |
| `service` | one `<service>.<namespace>.svc.<cluster_domain>:<port>` per Service | kube-proxy, or whatever layer sits behind the Service | traffic must go through the Service, e.g. for Service-level routing, policy or mTLS applied there |

```toml
[kubernetes]
cluster_domain = "cluster.local"

[upstreams.legacy]
services = ["legacy.prod:http"]
target = "service"
```

- `pods` gives per-pod health checks, drain within about a second, per-request balancing across
  pods (gRPC included) and consistent-hash stickiness. Prefer it unless traffic must pass the
  Service.
- In `service` mode keep the Pingap upstream's `discovery` empty. Pingap then treats the hostname
  in `addrs` as DNS discovery and re-resolves it every `update_frequency` (60 s by default). With
  `discovery = "static"` the name is resolved only once, when the upstream is built.
- The port written is the Service `port`. Several Services in one upstream give one address each,
  with their weights. The Service and port must exist, as in `pods` mode.
- Pingap forwards the client's `Host` header. If a layer behind the Service routes by Service
  name, set `Host` with `proxy_set_headers` on the location, e.g.
  `proxy_set_headers = ["Host:legacy.prod.svc.cluster.local"]`.
- Lost compared with `pods`: Pingap's per-pod health checks (it sees one address), fast drain
  (draining is up to the layer behind the Service), and per-request gRPC spread across pods
  (connections are spread only if that layer balances per request).
- `target` and `cluster_domain` apply live; switching an upstream between modes needs no restart.
- Chart: `config.defaults.target`, `config.upstreams.<name>.target` and
  `config.kubernetes.cluster_domain`. To add the quickstart Pingap pods to another layer that
  selects pods by label or annotation, use `pingap.podLabels` and `pingap.podAnnotations`
  (`podLabels` and `podAnnotations` for the controller).

## Graceful shutdown of application pods (preStop)

Pingap has no draining state for a backend: draining means removing the endpoint from `addrs`.
Before a rebuilt upstream is swapped in, Pingap health-checks all of its remaining backends, so a
terminating pod can still receive new requests for: the debounce, plus the etcd watch and reload,
plus that health-check round (up to the upstream's connect timeout for a slow or dead backend).

Give the application pods behind owned upstreams a `preStop` delay that covers it:

```yaml
spec:
  terminationGracePeriodSeconds: 30
  containers:
    - name: app
      lifecycle:
        preStop:
          sleep: { seconds: 10 }   # >= debounce_max + reload + upstream health-check connect timeout
```

The `sleep` action is GA in Kubernetes 1.32. With the defaults (2 s debounce cap, about 1 s
reload), 10 s covers a health-check connect timeout of up to about 5 s with margin. Size it from
your upstream's connect timeout.

Each write rebuilds the upstream and resets its runtime state (circuit breaker, backend stats,
round-robin position). The debounce keeps the number of writes during a rollout low.

## Production checklist

- [ ] **App pods have a `preStop` delay** sized from the upstream health-check connect timeout
      ([above](#graceful-shutdown-of-application-pods-prestop)).
- [ ] **Every owned upstream has a Pingap health check**, e.g.
      `health_check = "http://api/healthz?connection_timeout=1s&check_frequency=5s"`. Minimal
      upstreams created by the controller have none, and fail-static depends on it: while the
      controller is down, or when a Service has no endpoints, health checks are what take dead pod
      IPs out of rotation.
- [ ] **The health-check connect timeout is short** (about 1 s). Each write is swapped in only
      after a health-check round, so a slow or dead backend delays it by up to that timeout
      (measured 3–5.5 s with a default health check).
- [ ] **Idle timeouts are ordered**:
  - Pingap upstream `idle_timeout` is shorter than the app's HTTP keep-alive timeout, so Pingap
    never reuses a connection the app is closing.
  - Pingora keeps idle client HTTP/1 connections for 60 s. A layer-4 load balancer in front of
    Pingap (Service `LoadBalancer`) needs a longer idle timeout, so Pingap closes idle connections
    first; a layer-7 load balancer that pools connections to Pingap needs a shorter one.
- [ ] **NetworkPolicy allows Pingap pods to app pods** on the target port. Pingap connects to pod
      IPs directly, so policies that only admit traffic "from the Service" do not apply. If
      traffic must pass the Service (Service-level routing, policy or mTLS), use
      [`target = "service"`](#upstream-target) for those upstreams.
- [ ] **Client source IP** is decided:
  - `externalTrafficPolicy: Local` keeps the client address (only nodes running a Pingap pod get
    traffic, so spread the replicas); `Cluster`, the default, shows Pingap a node address;
  - or PROXY protocol from the load balancer: server `proxy_protocol = true`, which needs
    `basic.trusted_proxies`;
  - set location `enable_reverse_proxy_headers = true` to pass `X-Real-IP`, `X-Forwarded-For` and
    `X-Forwarded-Proto` to the apps.
- [ ] **Dual-stack**: `ipv4_only = true` unless the Pingap pods route IPv6 to app pods (each
      dual-stack pod is otherwise written once per family). Listen on `[::]` to accept both.
- [ ] **Bundled etcd maintenance**: periodic auto-compaction is on (1 h). Compaction does not free
      disk space: run `etcdctl defrag` now and then, one member at a time; take regular
      `etcdctl snapshot save` backups; size `--quota-backend-bytes` (via `etcd.extraArgs`) and
      the volume together, and watch for the `NOSPACE` alarm.
- [ ] **Long-lived connections are rebalanced**: Pingap balances each HTTP/2 or gRPC request on
      its own, even when a client sends them all over one connection, but a WebSocket or a
      long-running stream stays on the pod it was opened to, and new pods only get new ones. Cap
      the connection or stream age in the app if load must even out after scaling.
- [ ] **Sticky sessions use consistent hashing**: `algo = "hash:cookie:<name>"` (or `hash:ip`,
      `hash:header:<name>`). Pingora's consistent hash moves only the keys of the pods that
      changed during a rollout; round robin has no affinity.
- [ ] **Quickstart Pingap runs 2 or more replicas** (`pingap.replicas`), so its
      PodDisruptionBudget applies.
- [ ] **Streaming workloads** (server-sent events, long polling, gRPC streams):
  - Pingap's upstream `read_timeout` applies to each read, so a quiet stream is cut after it.
    Set a longer `read_timeout` / `write_timeout` on the location of the streaming paths only,
    and server `downstream_read_timeout` / `downstream_write_timeout` on the client side.
  - Prefer heartbeats from the app (an SSE comment line every 15–30 s, gRPC keepalive pings) to
    raising every timeout: load balancers and NAT drop idle connections anyway.
  - An app rollout ends the streams of each pod as it stops: size the app's
    `terminationGracePeriodSeconds` and make clients reconnect (SSE resumes with
    `Last-Event-ID`).
  - Pingap's own rollout: the quickstart budget (`preStop` 15 s + `grace_period` 30 s +
    `graceful_shutdown_timeout` 10 s) cuts the streams of a terminating Pingap pod after about
    45 s. For longer streams raise seed `grace_period` and `pingap.terminationGracePeriodSeconds`
    together (Pingap rollouts get slower).
  - Do not enable the response cache plugin on streaming routes. The compression plugin already
    skips `text/event-stream`.

## Operating it with kubectl

For release `pingap-k8s` in namespace `pingap-k8s` (as in [`deploy/`](deploy/)); change both to
yours.

Controller pods, logs, readiness and the leader:

```sh
kubectl -n pingap-k8s get pods -l app.kubernetes.io/component=controller -o wide
kubectl -n pingap-k8s logs -l app.kubernetes.io/component=controller --tail=100 -f --prefix
kubectl -n pingap-k8s port-forward deploy/pingap-k8s 9090 &
curl -s localhost:9090/readyz; echo
curl -s localhost:9090/metrics | grep -E '^pingap_k8s_(is_leader|watch_up)'
```

`deploy/pingap-k8s` picks one pod. The leader is the pod with `pingap_k8s_is_leader 1`:

```sh
for p in $(kubectl -n pingap-k8s get pods -l app.kubernetes.io/component=controller -o name); do
  kubectl -n pingap-k8s port-forward "$p" 19090:9090 >/dev/null & pf=$!; sleep 1
  echo "$p $(curl -s localhost:19090/metrics | grep '^pingap_k8s_is_leader')"; kill $pf
done
```

Quickstart Pingap admin UI (ClusterIP only), user `admin`:

```sh
kubectl -n pingap-k8s get secret pingap-k8s-pingap-admin -o jsonpath='{.data.password}' | base64 -d; echo
kubectl -n pingap-k8s port-forward svc/pingap-k8s-pingap-admin 3018:3018   # http://localhost:3018
```

What the controller sees, and what it stored (bundled etcd: its pod already has the `etcdctl`
endpoint and client certificate set):

```sh
kubectl -n <app-namespace> get endpointslices -l kubernetes.io/service-name=<service> -o wide
kubectl -n pingap-k8s exec pingap-k8s-etcd-0 -c etcd -- etcdctl get --prefix /pingap/upstreams/
```

Validate a config before rolling it out (the image runs the controller binary):

```sh
helm template pingap-k8s charts/pingap-k8s -f values.yaml --show-only templates/configmap.yaml \
  | yq '.data["config.toml"]' > config.toml
docker run --rm -v "$PWD/config.toml:/config.toml:ro" ghcr.io/mehdyhaghy/pingap-k8s:0.1.0 \
  --check --config /config.toml
```

RBAC: the controller may read EndpointSlices and Services only in the namespaces it was granted
(`yes` expected for each namespace in `config.upstreams`):

```sh
kubectl auth can-i list endpointslices.discovery.k8s.io -n <app-namespace> \
  --as=system:serviceaccount:pingap-k8s:pingap-k8s
kubectl auth can-i watch services -n <app-namespace> --as=system:serviceaccount:pingap-k8s:pingap-k8s
```

## Observability

The controller serves on port `9090` (cluster-internal only):

- `/metrics` (Prometheus), `/healthz` (process alive), `/readyz` (all watchers synced, and etcd
  was reachable on the last write; followers never write). A malformed upstream entry or a CAS
  conflict does not fail `/readyz`: it shows in `pingap_k8s_writes_total` and the logs, and is
  retried on the next event or resync, or every 5 minutes.

| Metric | Labels |
|---|---|
| `pingap_k8s_endpoints` | `upstream`, `class="ready\|draining"` |
| `pingap_k8s_writes_total` | `upstream`, `result="ok\|conflict\|error\|unchanged"` |
| `pingap_k8s_foreign_writes_total` | `upstream`, `corrected="true\|false"` |
| `pingap_k8s_watch_up` | `k8s_namespace_name`, `k8s_service_name` |
| `pingap_k8s_watch_errors_total` | `k8s_namespace_name`, `k8s_service_name`, `error_type` |
| `pingap_k8s_event_to_write_seconds` | histogram |
| `pingap_k8s_drain_fallback` | `upstream` |
| `pingap_k8s_is_leader`, `pingap_k8s_etcd_up` | none |

The chart's metrics Service is `pingap-k8s-metrics` (`ClusterIP`, release `pingap-k8s`).

### Telemetry

Metrics are recorded once through OpenTelemetry and exposed two ways: `/metrics` (always) and,
when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, OTLP over gRPC (OpenTelemetry names such as
`pingap_k8s.writes` with `k8s.namespace.name`-style attributes). Logs always go to stderr; with an
endpoint the same events are also sent as OTLP log records. There are no traces.

Only the standard environment variables are read: `OTEL_EXPORTER_OTLP_ENDPOINT`,
`OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_TIMEOUT`, `OTEL_METRIC_EXPORT_INTERVAL`,
`OTEL_SDK_DISABLED=true` (turns OTLP export off), `OTEL_SERVICE_NAME` (default `pingap-k8s`) and
`OTEL_RESOURCE_ATTRIBUTES`. The chart sets the endpoint from `otel.endpoint` and always sets
`k8s.namespace.name`, `k8s.pod.name` and `k8s.deployment.name`, plus `otel.resourceAttributes`:

```yaml
otel:
  endpoint: http://otel-collector.observability.svc:4317
  resourceAttributes:
    deployment.environment.name: prod
```

The endpoint must be a collector on the private or cluster network (for example a ClusterIP
Service), never a public address. Use `https://` for TLS (system roots).

## Security

- **Kubernetes access is read-only and namespaced**: `get/list/watch` on `services` and
  `endpointslices`, through one Role per namespace. Never a ClusterRole. API server TLS is always
  verified; there is no insecure option. The ServiceAccount token is re-read on rotation.
- **etcd write access is the main risk**: whoever can write `<prefix>/` can redirect traffic.
  - Keep etcd on the private or cluster network only: a `ClusterIP` Service plus a NetworkPolicy
    that admits only Pingap and pingap-k8s pods. Never give it a public address. The bundled etcd
    does exactly this.
  - Use client-certificate authentication (mTLS). On a shared etcd, give the controller its own
    etcd user with a role that has `readwrite` on exactly two prefixes: `<prefix>/upstreams/`
    (the owned upstreams) and `<prefix>-k8s-election/` (leader election). For prefix `/pingap`:

    ```sh
    etcdctl role add pingap-k8s
    etcdctl role grant-permission pingap-k8s readwrite /pingap/upstreams/ --prefix=true
    etcdctl role grant-permission pingap-k8s readwrite /pingap-k8s-election/ --prefix=true
    etcdctl user add pingap-k8s --no-password   # authenticated by the certificate CN
    etcdctl user grant-role pingap-k8s pingap-k8s
    ```

  - Certificates and keys come from a mounted Secret, never from the ConfigMap or the command line.
- **Only addresses from configured Services are written**: pod IPs from the EndpointSlices of
  explicitly listed Services. A workload cannot inject an arbitrary address.
- **Cluster-internal endpoints**: the metrics/health port, etcd and the quickstart Pingap admin UI
  are `ClusterIP` only. `networkPolicy.enabled: true` also limits who may reach the controller's
  metrics port (`networkPolicy.allowSameNamespace`, `networkPolicy.metricsFrom`); kubelet probes
  keep working. CI fails if the chart renders a `LoadBalancer` or `NodePort` Service other
  than the quickstart proxy listener, or any cluster-wide RBAC.
- **Hardened pods**: non-root (UID 65532), read-only root filesystem, no privilege escalation, all
  capabilities dropped, `RuntimeDefault` seccomp, for the controller, the bundled etcd and the
  quickstart Pingap. The controller image is distroless (no shell).

## Limitations

- Phase 1 manages upstream `addrs` only. Locations, servers and certificates are not generated
  (Ingress and Gateway API support are planned).
- etcd is the only write target. Pingap file or ConfigMap storage is not supported.
- Pingap must be able to reach pod IPs, or with `target = "service"` resolve and reach the
  Service names (normally: run Pingap inside the cluster).
- `ExternalName` Services are not supported.
- When the controller is down, Pingap keeps serving the last addresses, but new pods get no
  traffic until it is back.
- Bundled etcd: the member count (1 or 3) is fixed at install; changing `etcd.replicas` later does
  not reconfigure the cluster. Generated certificates are valid for `etcd.tls.validityDays` (10
  years by default) and are not rotated automatically. No etcd users or roles are configured.
- Quickstart Pingap: the seed runs only on an etcd prefix without servers. A new listener added
  later through the admin UI needs a Pingap restart (Pingap reads servers at start).

## Development

```sh
make fmt           # cargo fmt
make check         # fmt check, clippy -D warnings, tests, helm lint/template, exposure check, manifests check
make manifests     # regenerate deploy/pingap-k8s.yaml from the chart
make docker        # build the image for the local platform
make docker-multiarch PLATFORMS=linux/amd64,linux/arm64
```

Building needs `protoc` and the protobuf well-known types (Debian/Ubuntu:
`protobuf-compiler libprotobuf-dev`), used by `etcd-client`. The Docker build cross-compiles in the
builder stage, so a multi-arch build needs no emulation.

Built with [kube-rs](https://github.com/kube-rs/kube), [k8s-openapi](https://github.com/Arnavion/k8s-openapi),
[etcd-client](https://github.com/etcdv3/etcd-client), [toml_edit](https://github.com/toml-rs/toml),
[tokio](https://tokio.rs), [tracing](https://github.com/tokio-rs/tracing) and
[prometheus](https://github.com/tikv/rust-prometheus). Pingap is built on
[Pingora](https://github.com/cloudflare/pingora).

## License

Apache-2.0, the same license as Pingap. See [LICENSE](LICENSE).

## Releasing

1. Bump the version in three places to the same value (for example `0.2.0`): `version` in
   `Cargo.toml`, and `version` and `appVersion` in `charts/pingap-k8s/Chart.yaml`. Run
   `cargo check` to update `Cargo.lock`, then `make manifests` and `make check`.
2. Commit, merge to `main`, then tag and push the tag:

   ```sh
   git tag -a v0.2.0 -m "v0.2.0"
   git push origin v0.2.0
   ```

The `release` workflow fails if the tag does not match both files. Otherwise it runs the CI checks,
pushes the `linux/amd64` + `linux/arm64` image to `ghcr.io/mehdyhaghy/pingap-k8s:<version>` and
`:latest` (with SBOM and provenance attestations), pushes the chart to
`oci://ghcr.io/mehdyhaghy/charts/pingap-k8s`, and creates a GitHub Release with generated notes,
the chart package and `pingap-k8s.yaml` (plain manifests pinned to the release image). To test the
pipeline without publishing, run the workflow manually with `dry-run` checked (the default).

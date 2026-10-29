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
- [Headers, sticky sessions and auth](#headers-sticky-sessions-and-auth)
  - [Request headers](#request-headers)
  - [Sticky sessions](#sticky-sessions)
  - [Authentication](#authentication)
- [Upgrades and hot reload](#upgrades-and-hot-reload)
- [Production checklist](#production-checklist)
- [Operating it with kubectl](#operating-it-with-kubectl)
  - [kubectl plugin](#kubectl-plugin)
- [Observability](#observability)
  - [Telemetry](#telemetry)
- [Security](#security)
- [Performance](#performance)
- [Limitations](#limitations)
- [Development](#development)
- [License](#license)
- [Releasing](#releasing)

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
| Role + RoleBinding **per namespace** | `get/list/watch` on `services` and `endpointslices`. The namespaces come from `rbac.namespaces` plus every namespace referenced in `config.upstreams` (`rbac.fromConfig: true`). Never a ClusterRole. With `events.enabled`, also `create/patch` on `events.k8s.io` `events` there and in the release namespace. |
| ConfigMap | the controller config, rendered as TOML from `config` (or a Secret with `config.useSecret: true`) |
| Deployment | 2 replicas, non-root, read-only root filesystem, all capabilities dropped, probes on `/healthz` and `/readyz` |
| PodDisruptionBudget | `minAvailable: 1` |
| Service (metrics) | always `ClusterIP` |

Config changes are applied without restarting the pods: the controller reloads its config file
when the mounted ConfigMap changes (and on `SIGHUP`). Watches are started or stopped only for the
Services that were added or removed. An invalid new config is rejected and the running one stays.

A few settings are read only at startup: `[etcd]`, `[server]`, `[kubernetes]` `kubeconfig`,
`context` and `events`, and `defaults.resync`. A change to them logs "restart required" and keeps the running
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
| `events.enabled` | `false` | post Kubernetes Events (see [Observability](#observability)); adds `create/patch` on `events.k8s.io` `events` to the Roles, the controller's only Kubernetes write permission |
| `logFormat` / `logLevel` | `text` / `info` | log output (`text` or `json`) and filter |
| `podDisruptionBudget.minAvailable` | `1` | |
| `networkPolicy.enabled` | `true` | NetworkPolicy on the controller pods: only the sources below may reach the metrics port (none when both are empty); probes and port-forward are not affected |
| `networkPolicy.allowSameNamespace` / `networkPolicy.metricsFrom` | `false` / `[]` | allow pods of the release namespace / extra NetworkPolicy `from` peers (e.g. a monitoring namespace) |
| `etcd.deploy` | `false` | deploy a bundled etcd (below) |
| `etcd.tls.peerAllowedCN` | `""` | CN required of etcd peer certificates; empty: the generated server CN with `etcd.tls.autoGenerate` |
| `pingap.deploy` | `false` | deploy a quickstart Pingap (below) |
| `pingap.networkPolicy.enabled` / `.extraPorts` / `.adminFrom` | `true` / `[]` / `[]` | quickstart Pingap: open the proxy port (and extra listener ports) to all pods, the admin port only to `adminFrom` |

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
- The admin UI listens on its own port (`[::]:3018`, IPv4 and IPv6, so it also works on
  IPv6-only clusters) behind a `ClusterIP`-only Service. Open it with a port-forward:

  ```sh
  kubectl -n edge get secret pingap-k8s-pingap-admin -o jsonpath='{.data.password}' | base64 -d; echo
  kubectl -n edge port-forward svc/pingap-k8s-pingap-admin 3018:3018
  # http://localhost:3018, user "admin"
  ```

- A NetworkPolicy (`pingap.networkPolicy.enabled`, on by default) opens only the proxy port
  (`pingap.httpPort`) to other pods and keeps the admin port closed to them. If your seed config
  adds listeners, list their ports in `pingap.networkPolicy.extraPorts`. To call the admin API
  from other pods, list them in `pingap.networkPolicy.adminFrom`. Port-forward always works.

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
events = false           # post Kubernetes Events (chart: events.enabled)

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
| `kubernetes.cluster_domain` | `cluster.local` | cluster DNS domain of the `target = "service"` address of a headless Service; applies live |
| `kubernetes.events` | `false` | post Kubernetes Events for resolve warnings, drain fallbacks, malformed entries and etcd reachability (needs `create/patch` on `events.k8s.io` `events`); restart required |
| `include_not_ready` | `false` | also write endpoints that are not ready (and not terminating) |
| `drain_terminating` | `true` | remove terminating endpoints; `false` treats serving terminating endpoints as ready |
| `debounce` | `500ms` | wait this long after the last event before writing |
| `debounce_max` | `2s` | write at the latest this long after the first pending event |
| `resync` | `5m` | reconcile every upstream at this interval even without events |
| `ipv4_only` | `false` | ignore IPv6 EndpointSlices. On dual-stack clusters each pod has one slice per family, so it is written twice (`10.x:port` and `[fd00::x]:port`); set `true` unless the Pingap pods can reach app pods over IPv6 |
| `target` | `pods` | `pods` writes ready pod IPs, `service` writes the Service DNS name and port ([Upstream target](#upstream-target)); also per upstream |
| `server.status_listen` | `""` (off) | loopback address of the read-only `/status` endpoint used by the [kubectl plugin](#kubectl-plugin), e.g. `127.0.0.1:9091` (restart required). Non-loopback addresses are rejected. The chart sets it with `status.enabled` |
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

- Tested with Pingap 0.16.1 (the quickstart default) on Kubernetes 1.35 and 1.37, IPv4 and
  dual-stack; a manual CI run with `pingap-latest` checks Pingap `latest`
  (`gh workflow run ci -f pingap-latest=true`).
- The controller version tells you the Pingap version it was tested with: `0.16.1x` means Pingap
  0.16.1 (see [Releasing](#releasing)).
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
| `service` | one `<ClusterIP>:<port>` per Service (`<service>.<namespace>.svc.<cluster_domain>:<port>` for a headless one) | kube-proxy, or whatever layer sits behind the Service | traffic must go through the Service, e.g. for Service-level routing, policy or mTLS applied there |

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
- The address is the Service's ClusterIP (with `ipv4_only`, its IPv4 one), so traffic still
  goes through the Service. A Service deleted and recreated gets a new ClusterIP, and the
  controller writes it within the debounce, as for a pod change; no DNS is involved. Writing
  the Service DNS name instead would leave that to Pingap's DNS discovery, which keeps an answer
  for its DNS TTL (30 s with the default CoreDNS config) whatever `update_frequency` says.
- A headless Service (no ClusterIP) gets its DNS name. Keep that upstream's `discovery` empty, so
  Pingap uses DNS discovery for it; upstreams the controller creates for a name get
  `update_frequency = "5s"`.
- The port written is the Service `port`. Several Services in one upstream give one address each,
  with their weights. The Service and port must exist, as in `pods` mode.
- Pingap forwards the client's `Host` header. If a layer behind the Service routes by Service
  name, set `Host` with `proxy_set_headers` on the location, e.g.
  `proxy_set_headers = ["Host:legacy.prod.svc.cluster.local"]`.
- Lost compared with `pods`: Pingap's per-pod health checks (it sees one address), fast drain
  (draining is up to the layer behind the Service), and per-request gRPC spread across pods
  (connections are spread only if that layer balances per request).
- `target` and `cluster_domain` (used for headless Services) apply live; switching an upstream between modes needs no restart.
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

## Headers, sticky sessions and auth

The controller only writes `addrs`; requests go from the client to Pingap and from Pingap to the
pod (or to the Service in `service` mode). Everything below is Pingap configuration on the
location or upstream, set in the admin UI or in etcd. Phase 1 does not generate it.

### Request headers

- The client's request headers reach the pod as sent, `Host`, cookies and `Authorization`
  included; Pingap drops only hop-by-hop headers such as `Connection`. WebSocket upgrades, gRPC
  (h2c) and server-sent events pass through.
- The pod sees the Pingap pod as the TCP peer. Location `enable_reverse_proxy_headers = true` adds
  `X-Real-IP`, `X-Forwarded-For`, `X-Forwarded-Proto`, `X-Forwarded-Host` and `X-Forwarded-Port`.
  The client address Pingap itself sees depends on how traffic reaches it (see "Client source
  IP" in the [Production checklist](#production-checklist)).
- Add or replace headers with `proxy_set_headers` / `proxy_add_headers` on the location, or the
  `request_headers` plugin. In `service` mode kube-proxy does not touch headers either.

### Sticky sessions

They work in `pods` mode, where Pingap sees every pod:

| Upstream `algo` | Affinity by | Notes |
|---|---|---|
| `sticky:<cookie>` | a cookie Pingap sets itself | The usual choice. The cookie value is derived from the pod address, so every Pingap replica picks the same pod. A client whose pod is gone, unhealthy or just failed is moved and gets a new cookie; a request never fails because of it. |
| `hash:cookie:<name>`, `hash:header:<name>`, `hash:ip`, `hash:url`, `hash:query:<key>` | consistent hash of a request value | The app sets the cookie or header. When pods change, only the clients of those pods move. |

- A terminating pod leaves `addrs` at once, so its clients move to another pod while it drains.
  Affinity is a preference, not a guarantee: keep session state in a shared store if losing it
  matters.
- `hash:ip` needs the real client address (behind a load balancer that hides it, every client
  hashes the same).
- Behind a CDN or another shared cache, `sticky:<cookie>`'s `Set-Cookie` can be cached and given
  to everyone. Use a `hash:` key there, or make the cache leave `Set-Cookie` responses alone.
- In `service` mode Pingap sees one address and cannot pin a client to a pod; the Service's
  `sessionAffinity: ClientIP` would only see the Pingap pods.

### Authentication

Pingap's plugins authenticate on the location, before the request reaches the pod:

| Plugin | What it does |
|---|---|
| `basic_auth` | HTTP basic auth against htpasswd entries (bcrypt and others), with a delay and block after failures |
| `key_auth` | API key in a header or query parameter, optionally hidden from the app |
| `jwt` | JWT bearer tokens: HS256/HS512, RS and ES keys or a JWKS URL, checked claims, claims passed to the app as headers |
| `oidc` | browser login with an OpenID Connect provider (authorization code flow with PKCE), user claims passed to the app as headers |
| `forward_auth` | asks an external auth service for every request; a `2xx` lets it through and selected response headers are copied onto it |
| `hmac_auth` | requests signed with a shared secret (HTTP Signatures) |
| `combined_auth` | per-app secrets combined with an IP allowlist |
| server `tls_client_ca` | client certificates (mutual TLS), required or optional |

Related: IP, geo, referer and user-agent restrictions, URI blocking, CSRF, CORS, and rate and
bandwidth limits.

LDAP or Active Directory, plain OAuth2 providers (for example GitHub) and opaque token
introspection have no plugin. Use `forward_auth` with an auth service running in the cluster:
Authelia, Authentik or Keycloak (LDAP federation), Dex behind oauth2-proxy, or oauth2-proxy alone
for OAuth2 providers.

## Upgrades and hot reload

Traffic is not interrupted by any of these:

- **Upgrading the controller** (`helm upgrade`): the controller is not on the request path, and
  Pingap keeps serving from etcd. The Deployment rolls with `maxSurge: 1`, `maxUnavailable: 0`
  and a PodDisruptionBudget. A stopping leader resigns and revokes its lease, so a standby takes
  over at once and resyncs every upstream; changes during the handover (about a second) are
  written then. With `replicaCount: 1` that gap is the new pod's startup time.
- **Changing the controller config**: reloaded live, see [Helm](#helm).
  The settings read only at startup need a rolling restart, which is the case above.
- **Endpoint changes**: Pingap's `--autoreload` rebuilds only the changed upstream and swaps it
  in; no Pingap restart, in-flight requests and upgraded connections are kept.
- **Upgrading the quickstart Pingap**: rolling update with surge and no unavailable pod, a 15 s
  `preStop`, then pingora's graceful shutdown (in-flight requests finish). Run 2 or more replicas
  for the PodDisruptionBudget. Changing Pingap listeners (`servers`) or `[basic]` needs a Pingap
  restart, by Pingap's design.
- **Application rollouts**: terminating pods leave `addrs` at once and a single draining pod is
  kept until its replacement is Ready; the app pods need a [preStop delay](#graceful-shutdown-of-application-pods-prestop).
  The e2e suite checks no failed requests under load during rollouts and single-replica restarts.

Not covered: while every controller pod is down, new pods get no traffic.

## Production checklist

- [ ] **App pods have a `preStop` delay** sized from the upstream health-check connect timeout
      ([above](#graceful-shutdown-of-application-pods-prestop)).
- [ ] **Every owned upstream has a Pingap health check**, e.g.
      `health_check = "http://api/healthz?connection_timeout=1s&check_frequency=5s"`. Minimal
      upstreams created by the controller have none, and fail-static depends on it: while the
      controller is down, or when a Service has no endpoints, health checks are what take dead pod
      IPs out of rotation. Prefer an HTTP check with a path only this app serves over `tcp://`:
      a pod IP freed during a long controller outage can be given to another app's pod on the
      same port, and a TCP check would still pass.
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
- [ ] **Prometheus may scrape the controller**: the chart's NetworkPolicy closes the metrics port
      by default; list your Prometheus in `networkPolicy.metricsFrom`.
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
- [ ] **Sticky sessions, if needed, use `sticky:<cookie>` or a hash**
      ([Sticky sessions](#sticky-sessions)); round robin has no affinity.
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
docker run --rm -v "$PWD/config.toml:/config.toml:ro" ghcr.io/mehdyhaghy/pingap-k8s:0.16.10 \
  --check --config /config.toml
```

Kubernetes Events (with `events.enabled`):

```sh
kubectl get events -A --field-selector reportingComponent=pingap-k8s
```

RBAC: the controller may read EndpointSlices and Services only in the namespaces it was granted
(`yes` expected for each namespace in `config.upstreams`):

```sh
kubectl auth can-i list endpointslices.discovery.k8s.io -n <app-namespace> \
  --as=system:serviceaccount:pingap-k8s:pingap-k8s
kubectl auth can-i watch services -n <app-namespace> --as=system:serviceaccount:pingap-k8s:pingap-k8s
```

### kubectl plugin

`kubectl pingap` shows what the controller sees and wrote, per upstream, from the leader pod. It
reads the controller's `/status` endpoint, which is off by default. Turn it on in the chart:

```yaml
status:
  enabled: true   # /status on 127.0.0.1:9091 inside the pod; restarts the controller pods
```

Install the plugin from the [release assets](https://github.com/mehdyhaghy/pingap-k8s/releases)
(`kubectl-pingap_<version>_<os>_<arch>.tar.gz` for linux/darwin and amd64/arm64, checked against
`kubectl-pingap_<version>_checksums.txt`): put `kubectl-pingap` on your `PATH`. Or with krew, from
the manifest attached to the release:

```sh
kubectl krew install --manifest-url=https://github.com/mehdyhaghy/pingap-k8s/releases/download/v0.16.10/pingap.yaml
```

```sh
kubectl pingap status -n pingap-k8s          # one row per upstream: target, Services, ready/draining, in sync, last write
kubectl pingap explain web -n pingap-k8s     # warnings, watch state and why an upstream has no endpoints
kubectl pingap admin -n pingap-k8s           # port-forward the quickstart admin UI, print the URL and the password command
```

```text
controller pingap-k8s-6d4f9c7b8-x2kqp (leader) v0.16.10, phase running, ready yes, etcd prefix /pingap, watchers 2/2 synced
UPSTREAM  TARGET   SERVICES                      READY  DRAINING  IN SYNC  LAST WRITE  NOTES
api       pods     api.prod:http                 2      0         yes      ok 12s ago
web       service  web.prod:80 9,gone.prod:80 1  0      0         no       -           no endpoints, 1 warning
```

It runs `kubectl` underneath, so `--context`, `--kubeconfig` and your credentials apply as usual.
`admin` prints the password only with `--show-password`, and only forwards to a Service with all
three labels of the chart's Pingap (`app.kubernetes.io/part-of=pingap-k8s`,
`app.kubernetes.io/name=pingap`, `app.kubernetes.io/component=proxy`); when more than one matches,
it stops and asks for `-n`. Security model:

- `/status` listens on `127.0.0.1` inside the controller pod only, on its own port; no Service or
  container port exposes it, and a non-loopback address is rejected.
- It is reached only with `kubectl port-forward`, so who may use it is decided by RBAC on
  `pods/portforward` in the controller's namespace.
- It is read-only and never contains secrets: no etcd URL or credentials, no certificate paths, no
  tokens, no admin password.

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

`pingap_k8s_etcd_up` follows every etcd call (writes, the owned-key watch, the election's
keep-alives every 2 s), so a follower shows 0 within seconds of etcd going away too.

The chart's metrics Service is `pingap-k8s-metrics` (`ClusterIP`, release `pingap-k8s`). The
chart's NetworkPolicy closes the port to other pods by default; to scrape it, list Prometheus in
`networkPolicy.metricsFrom`, for example:

```yaml
networkPolicy:
  metricsFrom:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: monitoring
```

With `events.enabled: true` the controller also posts Kubernetes Events (`reportingController`
`pingap-k8s`), once per episode, like its warning logs: `ServiceMissing`, `PortNotInService`,
`PortNotInSlices`, `DrainFallbackStarted`/`DrainFallbackEnded` on the Service (by the leader),
`MalformedUpstream` and `EtcdUnreachable`/`EtcdRecovered` on the controller Pod. Repeats update the
existing Event, and each object gets at most 25, then one per 5 minutes.

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
Service), never a public address; a public IP address logs a warning at startup. Use `https://`
for TLS (system roots). Put `OTEL_EXPORTER_OTLP_HEADERS` (for example an auth token) in a Secret
and pass it with `extraEnv` and `valueFrom.secretKeyRef`, not as a plain value.

## Security

Report vulnerabilities privately, as described in [SECURITY.md](SECURITY.md).

- **Kubernetes access is read-only and namespaced**: `get/list/watch` on `services` and
  `endpointslices`, through one Role per namespace. Never a ClusterRole. The only write
  permission is opt-in: `events.enabled` adds `create/patch` on `events.k8s.io` `events` in those
  namespaces and the release namespace. API server TLS is always
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
  - A plaintext etcd connection logs a warning at startup, and `helm install` prints one in its
    notes. A password in the etcd URL is redacted in errors and in the notes.
  - The bundled etcd accepts a peer certificate only with the server certificate's CN
    (`--peer-cert-allowed-cn`), so a client certificate can not join the cluster as a member.
    With your own certificates, set `etcd.tls.peerAllowedCN` to the server certificate's CN.
- **Only addresses from configured Services are written**: pod IPs from the EndpointSlices of
  explicitly listed Services. Loopback, link-local (for example `169.254.169.254`), unspecified,
  broadcast and multicast addresses are skipped, so a hand-written EndpointSlice can not point an
  upstream at the node or at cloud metadata.
- **Cluster-internal endpoints**: the metrics/health port, etcd and the quickstart Pingap admin UI
  are `ClusterIP` only. A NetworkPolicy (`networkPolicy.enabled`, on by default) closes the
  controller's metrics port to other pods; open it to your Prometheus with
  `networkPolicy.metricsFrom` (or `networkPolicy.allowSameNamespace`). Kubelet probes and
  port-forward keep working. CI fails if the chart renders a
  `LoadBalancer` or `NodePort` Service other than the quickstart proxy listener, or any
  cluster-wide RBAC.
- **The quickstart Pingap admin port is closed to other pods** by a NetworkPolicy
  (`pingap.networkPolicy.enabled`, on by default). Only the proxy port (`pingap.httpPort`, plus
  `pingap.networkPolicy.extraPorts` for more listeners) is open. To reach the admin from other
  pods, list them in `pingap.networkPolicy.adminFrom`. `kubectl pingap admin` and
  `kubectl port-forward` keep working: port-forward does not go through NetworkPolicy.
- **Bounded HTTP servers**: the metrics/health and `/status` servers serve at most 64 connections
  at once, and close a connection after 10 s without traffic or 60 s in total. A client that
  trickles request headers can not hold them open.
- **Telemetry stays private**: an `OTEL_EXPORTER_OTLP_ENDPOINT` given as a public IP address logs
  a warning at startup.
- **Hardened pods**: non-root (UID 65532), read-only root filesystem, no privilege escalation, all
  capabilities dropped, `RuntimeDefault` seccomp, for the controller, the bundled etcd and the
  quickstart Pingap. The controller image is distroless (no shell).
- **Signed releases**: the image, the chart and every release file are signed with cosign
  (keyless, Sigstore) by the release workflow, and the base images are pinned by digest. To
  verify:

  ```sh
  ID='https://github.com/mehdyhaghy/pingap-k8s/.github/workflows/release.yml@refs/tags/v0.16.10'
  ISSUER=https://token.actions.githubusercontent.com
  cosign verify --certificate-identity "$ID" --certificate-oidc-issuer "$ISSUER" \
    ghcr.io/mehdyhaghy/pingap-k8s:0.16.10
  cosign verify --certificate-identity "$ID" --certificate-oidc-issuer "$ISSUER" \
    ghcr.io/mehdyhaghy/charts/pingap-k8s:0.16.10
  # A release file, with its .sigstore.json bundle from the same release:
  cosign verify-blob --certificate-identity "$ID" --certificate-oidc-issuer "$ISSUER" \
    --bundle kubectl-pingap_0.16.10_checksums.txt.sigstore.json kubectl-pingap_0.16.10_checksums.txt
  sha256sum -c --ignore-missing kubectl-pingap_0.16.10_checksums.txt
  ```

## Performance

At 50 upstreams × 20 endpoints: event-to-write p50 0.52 s (the 500 ms debounce), ~30 MB controller RSS, one etcd revision per write; see [docs/benchmarks.md](docs/benchmarks.md).

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
make check         # fmt check, clippy -D warnings, tests, helm lint/template, exposure check, version check, manifests check
make manifests     # regenerate deploy/pingap-k8s.yaml from the chart
make docker        # build the image for the local platform
make docker-multiarch PLATFORMS=linux/amd64,linux/arm64
STATUS=1 scripts/smoke-local.sh   # local binary + local etcd + kind, including /status
make docker && scripts/plugin-kind.sh   # kubectl plugin against the chart on kind
```

Tests:

| Command | What it covers | In CI |
| --- | --- | --- |
| `cargo test` | unit tests (including Kubernetes Events against a mock API server); OTLP export to an in-process receiver (`tests/otlp_export.rs`); the plugin against a fake `kubectl` (`tests/kubectl_plugin.rs`) | `rust` |
| `scripts/test-etcd.sh` | `tests/etcd_*.rs` against a throwaway etcd on 127.0.0.1: CAS writes, owned-key watch, compaction, leader election | `integration` |
| `scripts/test-kind.sh` | `tests/watch_kind.rs`: the Service and EndpointSlice watchers on a throwaway kind cluster | `integration` |
| `scripts/e2e-run.sh` | end-to-end tests 1 to 18 with the chart, Pingap and etcd on kind | `e2e` |
| `scripts/otel-local.sh` | the binary exporting to a real OpenTelemetry Collector (docker) | no |
| `cargo audit` | RustSec advisories for `Cargo.lock` | `dependencies` |
| `gitleaks git . && gitleaks dir .` | secrets in the git history and the working tree (`.gitleaks.toml`) | `secrets` |

The `ci` workflow does not run on every push: run `make check` and the scripts above locally
while developing, start it by hand when you want a clean run (`gh workflow run ci`, or Actions >
ci > Run workflow), and the `release` workflow runs all of it before it publishes anything.

Line coverage: `cargo llvm-cov --lib --bins` (add `--test 'etcd_*'` with `PINGAP_K8S_TEST_ETCD`
set to include the etcd tests).

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

Versions follow Pingap: a release is the Pingap version it is tested with plus one more digit,
our own release number for that Pingap version. Releases tested with Pingap 0.16.1 are `0.16.10`,
`0.16.11`, ... up to `0.16.19`; the first release for Pingap 0.16.2 is `0.16.20`. The Pingap
version is `pingap.image.tag` in the chart values (the quickstart default). `make version-check`
(part of `make check` and CI) checks this.

1. Bump the version in three places to the same value (for example `0.16.11`): `version` in
   `Cargo.toml`, and `version` and `appVersion` in `charts/pingap-k8s/Chart.yaml`. For a new
   Pingap version, also set `pingap.image.tag` in `charts/pingap-k8s/values.yaml` and start again
   at digit 0. Run `cargo check` to update `Cargo.lock`, then `make manifests` and `make check`.
2. Commit, merge to `main`, then tag and push the tag:

   ```sh
   git tag -a v0.16.11 -m "v0.16.11"
   git push origin v0.16.11
   ```

The `release` workflow fails if the tag does not match both files or the version does not follow
the Pingap version. Otherwise it runs the whole `ci` workflow (all tests, the e2e suite on every
Kubernetes version, secret scan, advisories) and publishes nothing unless it passes, then
pushes the `linux/amd64` + `linux/arm64` image to `ghcr.io/mehdyhaghy/pingap-k8s:<version>` and
`:latest` (with SBOM and provenance attestations), pushes the chart to
`oci://ghcr.io/mehdyhaghy/charts/pingap-k8s`, and creates a GitHub Release with generated notes,
the chart package and `pingap-k8s.yaml` (plain manifests pinned to the release image). It signs
the image, the chart and the release files with cosign keyless signing (see
[Security](#security)); Sigstore's public transparency log records each signature with the
repository name, workflow path and tag. To test the
pipeline without publishing, run the workflow manually with `dry-run` checked (the default).

# pingap-k8s

Kubernetes endpoint discovery and [Gateway API](https://gateway-api.sigs.k8s.io/) routing for
[Pingap](https://github.com/vicanso/pingap).

`pingap-k8s` is a small Rust controller that runs in your cluster. It watches Services and
their EndpointSlices and writes the ready pod addresses into the upstreams of Pingap's etcd
config storage. Pingap instances started with `--autoreload` pick up each change and rebuild only
the upstream that changed, without a restart. With the Gateway API provider on, it also turns
Gateways, HTTPRoutes and GRPCRoutes into Pingap locations, plugins, certificates and upstreams.

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
  No ClusterRole and no writes to the cluster, unless you turn on the Gateway API provider (it
  reads the Gateway API resources cluster-wide and writes their status).
- **Fail static.** If the Kubernetes API or the controller is down, Pingap keeps the last written
  addresses and its health checks remove dead pods. New pods get no traffic until the controller
  is back.

Status: endpoint discovery, plus the [Gateway API provider](#gateway-api) (Gateway API v1.7.0
Standard channel). The frozen Ingress API will not be supported.

## How it works

1. **Watch.** For every Service referenced in the config, the controller runs one watcher on its
   EndpointSlices (`kubernetes.io/service-name=<svc>`) and one on the Service object (for the
   port table). Watchers are shared between upstreams. Streaming lists (WatchList) are not used
   on purpose: each list is one Service or its few slices, already served from the watch cache.
2. **Resolve.** For each upstream, it merges the ready endpoints of its Services, sorted,
   deduplicated and optionally weighted (`ip:port weight`). The configured Service port (name or
   number) is mapped to the EndpointSlice port of the same name; an unresolvable port is never
   guessed (the entry contributes nothing and a warning is logged). Terminating endpoints are
   dropped, unless *none* is ready (a single-replica restart): then the serving terminating ones
   are kept until a ready one appears.
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

- Kubernetes 1.35 or newer. CI tests 1.35 (the oldest minor still supported upstream) and 1.37,
  IPv4 and dual-stack.
- An etcd reachable from the cluster, on the private network only. You can reuse the etcd
  Pingap already uses, or let the chart deploy one (`etcd.deploy=true`).
- Pingap running against that etcd with autoreload. It must reach pod IPs, so in practice it runs
  inside the cluster, exposed as usual (a `LoadBalancer` or `NodePort` Service):

  ```sh
  pingap --conf "etcd://etcd.edge.svc:2379/pingap?ca=...&cert=...&key=..." --autoreload
  ```

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

The chart validates values against `values.schema.json` (unknown keys are rejected) and creates:

| Object | Notes |
|---|---|
| ServiceAccount | used only by the controller |
| Role + RoleBinding **per namespace** | `get/list/watch` on `services` and `endpointslices`. Namespaces come from `rbac.namespaces`, `gateway.namespaces` (with `gateway.enabled`) and every namespace in `config.upstreams` (`rbac.fromConfig: true`). With `events.enabled`, also `create/patch` on `events.k8s.io` `events` there and in the release namespace. With `gateway.enabled`, also `get/list/watch` on `secrets` (listener certificates, limited by `gateway.secretNames`) and `configmaps` (BackendTLSPolicy CA bundles, limited by `gateway.configMapNames`) in `gateway.namespaces`. |
| ClusterRole + ClusterRoleBinding | only with `gateway.enabled`: `get/list/watch` on `gatewayclasses`, `gateways`, `listenersets`, `httproutes`, `grpcroutes`, `referencegrants` and `backendtlspolicies`; `patch` on the `/status` of all of them but `referencegrants`; `list/watch` on `namespaces`. Nothing else. |
| GatewayClass | only with `gateway.enabled` (`gateway.className`, default `pingap`) |
| ConfigMap | the controller config, rendered as TOML from `config` (or a Secret with `config.useSecret: true`) |
| Deployment | 2 replicas, non-root, read-only root filesystem, all capabilities dropped, probes on `/healthz` and `/readyz` |
| PodDisruptionBudget | `minAvailable: 1` |
| Service (metrics) | always `ClusterIP` |

Config changes are applied without restarting the pods: the controller reloads its config file
when the mounted ConfigMap changes (and on `SIGHUP`). Watches are started or stopped only for the
Services that were added or removed. An invalid new config is rejected and the running one stays.

A few settings are read only at startup: `[etcd]`, `[server]`, `[kubernetes]` `kubeconfig`,
`context` and `events`, `defaults.resync`, and `[gateway]` `enabled`, `controller_name` and
`max_txn_ops`. A change to them logs "restart required" and keeps the running value. The chart
annotates the pod template with a checksum of the matching values (`events.enabled`,
`config.server`, `gateway.enabled`, `gateway.controllerName`, `gateway.maxTxnOps`; `status.*`
too), so `helm upgrade` restarts the controller pods for them. Otherwise apply them with
`kubectl -n <namespace> rollout restart deployment/pingap-k8s`.

Key chart values (see [`values.yaml`](charts/pingap-k8s/values.yaml) for all of them):

| Value | Default | Description |
|---|---|---|
| `etcd.url` | `""` | Pingap's etcd URL. Empty: built for the bundled etcd when `etcd.deploy`, else `etcd://etcd:2379/pingap` |
| `etcd.tlsSecret` | `""` | Secret with the controller's etcd client certificate, mounted at `/etc/pingap-k8s/etcd` |
| `config.defaults`, `config.upstreams` | | rendered into the [config file](#configuration-reference) |
| `rbac.namespaces` / `rbac.fromConfig` | `[]` / `true` | namespaces to grant read access in, plus those in `config.upstreams` |
| `events.enabled` | `false` | post Kubernetes Events ([Observability](#observability)); the controller's only namespaced write permission |
| `networkPolicy.metricsFrom` | `[]` | NetworkPolicy `from` peers allowed to scrape `/metrics` (closed by default) |
| `gateway.enabled` | `false` | [Gateway API provider](#gateway-api) |
| `etcd.deploy` / `pingap.deploy` | `false` / `false` | bundled etcd / quickstart Pingap (below) |

#### Bundled etcd (`etcd.deploy: true`)

For clusters that do not run etcd yet: `quay.io/coreos/etcd` (pinned version) as a StatefulSet
with 1 or 3 members (`etcd.replicas`), persistent volumes, periodic compaction and a
PodDisruptionBudget for 3 members.

- **Never exposed outside the cluster**: a `ClusterIP` Service for clients, a headless one for
  peers, and a NetworkPolicy that admits only the controller and Pingap pods to the client port.
  Pingap pods are matched by `etcd.networkPolicy.pingapPodLabels` (default
  `app.kubernetes.io/name: pingap` in the release namespace; `pingapNamespaceSelector` for
  another). NetworkPolicy needs a CNI plugin that enforces it.
- Client and peer TLS with client-certificate authentication. With `etcd.tls.autoGenerate: true`
  (default) the chart generates a CA, the server certificate and client certificates for the
  controller and Pingap (`pingap-k8s-etcd-client`, `pingap-k8s-etcd-pingap-client` for release
  `pingap-k8s`), reused on upgrade.

The controller URL is then built automatically
(`etcd://pingap-k8s-etcd.<namespace>.svc:2379/pingap?ca=...&cert=...&key=...` for release
`pingap-k8s`). Give your own Pingap the same URL and mount the `...-etcd-pingap-client` Secret at
`/etc/pingap-k8s/etcd`.

### Quickstart: everything in one install

`pingap.deploy: true` also deploys the stock Pingap image (`vicanso/pingap`, pinned version),
wired to the bundled etcd. To try it with a demo app (`traefik/whoami`, which answers with its pod
name and the request headers), first the app:

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
  # service: {type: LoadBalancer}   # to expose Pingap; ClusterIP by default
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

Each response comes from the next pod (round robin). After
`kubectl -n demo scale deployment whoami --replicas=5`, the new pods show up within seconds.

- The init container seeds the etcd prefix from `pingap.seed.config` **only if the prefix has no
  server yet** (the default seed: one listener on `[::]:8080`, IPv4 and IPv6, and the upstream
  `quickstart`). Pingap reads servers and `[basic]` only at start, which is why they are seeded
  first. On an existing install, change them in the admin UI and restart Pingap.
- Pingap runs non-root with a read-only root filesystem. The proxy Service is
  `pingap-k8s-pingap` (`pingap.service.type`; `ipFamilyPolicy` / `ipFamilies` for dual-stack).
- Pod lifecycle: rolling updates with `maxSurge: 1`, `maxUnavailable: 0`, replicas spread over
  nodes, a PodDisruptionBudget from 2 replicas. On deletion a 15 s `preStop` sleep, then a
  graceful shutdown (seeded `grace_period = "30s"`, `graceful_shutdown_timeout = "10s"`) within
  `terminationGracePeriodSeconds: 60`. Keep that sum below the grace period if you change them.
- The admin UI (`[::]:3018`) has a `ClusterIP`-only Service. Open it with `kubectl pingap admin`
  ([kubectl plugin](#kubectl-plugin)) or by hand (user `admin`):

  ```sh
  kubectl -n pingap-k8s get secret pingap-k8s-pingap-admin -o jsonpath='{.data.password}' | base64 -d; echo
  kubectl -n pingap-k8s port-forward svc/pingap-k8s-pingap-admin 3018:3018   # http://localhost:3018
  ```

- A NetworkPolicy (`pingap.networkPolicy.enabled`, on by default) opens only the proxy port
  (`pingap.httpPort`, Gateway group ports and `pingap.networkPolicy.extraPorts`) to other pods,
  and the admin port only to `pingap.networkPolicy.adminFrom`. Port-forward always works.

### Plain manifests

[`deploy/pingap-k8s.yaml`](deploy/pingap-k8s.yaml) is rendered from the chart with the default
values for the namespace `pingap-k8s` (`make manifests` regenerates it). Before applying it, edit
`[etcd] url` and `[upstreams.*]` in the ConfigMap `pingap-k8s-config`, copy the Role +
RoleBinding pair into every namespace your Services live in, and mount the etcd TLS Secret at
`/etc/pingap-k8s/etcd` (or render your own with `helm template`).

### etcd client certificates

The controller connects with the same URL format and TLS parameters as Pingap. Put the client
certificate in a Secret, never in the ConfigMap or on the command line:

```sh
kubectl -n edge create secret generic pingap-k8s-etcd \
  --from-file=ca.crt=ca.pem --from-file=tls.crt=client.pem --from-file=tls.key=client-key.pem
```

Then set `etcd.tlsSecret: pingap-k8s-etcd` and
`...?ca=/etc/pingap-k8s/etcd/ca.crt&cert=/etc/pingap-k8s/etcd/tls.crt&key=/etc/pingap-k8s/etcd/tls.key`
in the URL.

## Configuration reference

The controller reads one TOML file, given by `--config` or `PINGAP_K8S_CONFIG`. Unknown keys are
errors. The chart renders it from `config` into a ConfigMap mounted at
`/etc/pingap-k8s/config/config.toml`.

```toml
[etcd]
# Same URL format Pingap uses for --conf:
# etcd://host:port[,host:port]/<prefix>?user=..&password=..&ca=..&cert=..&key=..
url = "etcd://etcd.edge.svc:2379/pingap?ca=/etc/pingap-k8s/etcd/ca.crt&cert=/etc/pingap-k8s/etcd/tls.crt&key=/etc/pingap-k8s/etcd/tls.key"

[defaults]
include_not_ready = false
drain_terminating = true
debounce = "500ms"
debounce_max = "2s"
resync = "5m"            # global: cannot be set per upstream
ip_family = "primary"
external_name = false
target = "pods"

# One table per Pingap upstream the controller owns.
[upstreams.api]
services = ["api.prod:http"]

[upstreams.web]
services = ["web-blue.prod:http 9", "web-green.prod:http 1"]
drain_terminating = true # every [defaults] key but resync can be set per upstream
```

| Key | Default | Meaning |
|---|---|---|
| `etcd.url` | (required) | Pingap's etcd URL. The path is the key prefix, the same one Pingap uses. TLS parameters: `ca`, `cert`, `key`, `server_name`, `tls=true`. Prefer client certificates over `user`/`password`, which would sit in the ConfigMap. |
| `kubernetes.kubeconfig` / `context` | `""` | kubeconfig path (empty: in-cluster, then `$KUBECONFIG`, then `~/.kube/config`) and context |
| `kubernetes.default_namespace` | `""` | namespace for `services` entries without one; empty: the controller pod's namespace (`POD_NAMESPACE`) |
| `kubernetes.cluster_domain` | `cluster.local` | DNS domain of the `target = "service"` address of a headless Service |
| `kubernetes.events` | `false` | post Kubernetes Events ([Observability](#observability)); needs `create/patch` on `events.k8s.io` `events` |
| `include_not_ready` | `false` | also write endpoints that are not ready (and not terminating) |
| `drain_terminating` | `true` | remove terminating endpoints; `false` treats serving terminating endpoints as ready |
| `debounce` / `debounce_max` | `500ms` / `2s` | wait this long after the last event before writing / write at the latest this long after the first pending event |
| `resync` | `5m` | reconcile every upstream at this interval even without events (global) |
| `ip_family` | `primary` | address families written, see below. `ipv4_only` is a deprecated alias (`true` is `ipv4`, `false` is `primary`); setting both is an error |
| `external_name` | `false` | allow `ExternalName` Services, see below |
| `target` | `pods` | `pods` writes ready pod IPs, `service` the Service's ClusterIP ([Upstream target](#upstream-target)) |
| `upstreams.<name>.services` | (required) | list of `<service>[.<namespace>]:<port> [weight]` |
| `server.listen` | `[::]:9090` | address of `/metrics`, `/healthz` and `/readyz`. `[::]` also accepts IPv4 on Linux unless the node sets `net.ipv6.bindv6only=1`; on nodes with IPv6 disabled at boot use `0.0.0.0:9090` |
| `server.status_listen` | `""` (off) | loopback address of the read-only `/status` endpoint for the [kubectl plugin](#kubectl-plugin), e.g. `127.0.0.1:9091`; non-loopback addresses are rejected. Chart: `status.enabled` |
| `gateway.enabled` | `false` | turn on the [Gateway API provider](#gateway-api). Upstream names starting with `k8s-` are then reserved |
| `gateway.controller_name` | `pingap.io/gateway-controller` | GatewayClasses with this `spec.controllerName` are handled |
| `gateway.servers` | `[]` | shared Pingap servers listeners may be mapped onto; empty: every stored server not in a group |
| `gateway.addresses` | `[]` | IPs or hostnames written to `status.addresses` of Gateways on the shared servers |
| `gateway.groups` | `[]` | [server groups](#server-groups): `[[gateway.groups]]` with `name`, `servers`, `addresses` and `ports = ["<listener>:<server>"]` |
| `gateway.external_name` | `false` | allow `ExternalName` Service backends (Pingap resolves the name) |
| `gateway.health_check` | `""` | Pingap `health_check` of generated upstreams; empty: Pingap's default TCP check |
| `gateway.max_txn_ops` | `64` | operations per etcd transaction of the Gateway writer (2 to 1024; keep it below etcd's `--max-txn-ops`, 128 by default) |

- `<name>` is the Pingap upstream name (`[A-Za-z0-9_-]+`). `<port>` is a Service port **name** or
  **number** (the `port` field, not `targetPort`) and is required. `weight` is a positive integer,
  written as `ip:port weight` for every endpoint of that Service.
- Service names must be DNS-1035 labels, namespace names DNS-1123 labels, and a Service port may
  be listed only once per upstream. An invalid config stops the controller at startup.
- `ready` true or unset counts as ready; FQDN slices are ignored; only the first address of each
  endpoint is used.
- `ip_family`: a dual-stack Service has one EndpointSlice per family, so each pod appears twice.
  `primary` writes one address per pod in the Service's primary family (`spec.ipFamilies[0]`),
  and the other family only for pods without one. `ipv4` and `ipv6` write only that family,
  `both` every address. With `target = "service"`: the ClusterIP of that family, `both` every
  ClusterIP.
- `external_name = true`: an `ExternalName` Service writes `<externalName>:<port>` and Pingap
  resolves the name. Off, it is skipped with an `ExternalNameNotEnabled` warning in `pods` mode,
  and `service` mode writes its cluster DNS name. **Anyone who can create Services in a listed
  namespace can then point the upstream at any host.**
- HTTP/2 to the backends: an upstream gets `alpn = "H2"` (h2c) when every Service port it uses
  has the `appProtocol` `kubernetes.io/h2c` or `grpc`. An `alpn` already set is never changed.

Command line flags: `--config <path>` (env `PINGAP_K8S_CONFIG`, default
`/etc/pingap-k8s/config/config.toml`), `--log-format text|json` and `--check`. The log filter comes
from `RUST_LOG`. `--check` validates the config and the etcd URL without connecting anywhere and
exits 0 (valid) or 1 (invalid). Use it in CI on the rendered config (the image runs the binary):

```sh
helm template pingap-k8s charts/pingap-k8s -f values.yaml --show-only templates/configmap.yaml \
  | yq '.data["config.toml"]' > config.toml
docker run --rm -v "$PWD/config.toml:/config.toml:ro" ghcr.io/mehdyhaghy/pingap-k8s:<version> \
  --check --config /config.toml
```

## Pingap side

- Tested with Pingap 0.16.1 (the quickstart default). The controller version tells you the
  Pingap version it was tested with: `0.16.1x` means Pingap 0.16.1
  (see [CONTRIBUTING.md](CONTRIBUTING.md#releasing)).
- Create each owned upstream once, in the admin UI or by hand, with the fields you want (health
  check, timeouts, algorithm). Leave `discovery` empty (static). Or let the controller create a
  minimal one.
- `addrs` of an owned upstream belongs to the controller: a manual edit is overwritten on the next
  reconcile. Every other field is yours, and an admin save of other fields is never lost.
- Controller writes go straight to etcd, so they do not appear in Pingap's config history. The
  controller's logs and metrics are the audit trail for `addrs`.
- Pingap rewrites the whole entry on an admin save, so TOML comments in an owned upstream are lost
  on the next admin save.

## Upstream target

Each owned upstream has a `target`, set in `[defaults]` or per upstream; both apply live.

| `target` | Written into `addrs` | Who balances | Use when |
|---|---|---|---|
| `pods` (default) | ready pod IPs from EndpointSlices | Pingap, per request | Pingap should reach the pods directly (the usual case) |
| `service` | one `<ClusterIP>:<port>` per Service (`<service>.<namespace>.svc.<cluster_domain>:<port>` for a headless one) | kube-proxy, or whatever layer sits behind the Service | traffic must go through the Service (Service-level routing, policy or mTLS) |

```toml
[upstreams.legacy]
services = ["legacy.prod:http"]
target = "service"
```

- `pods` gives per-pod health checks, drain within about a second, per-request balancing across
  pods (gRPC included) and consistent-hash stickiness. It ignores the Service's
  `trafficDistribution` and `internalTrafficPolicy: Local` (a `TrafficPolicyIgnored` warning);
  use `service` for kube-proxy to apply them.
- `service` writes the ClusterIP, not the DNS name, so a recreated Service is followed within the
  debounce instead of Pingap's DNS TTL (30 s with the default CoreDNS config).
- A headless Service gets its DNS name; keep that upstream's `discovery` empty so Pingap resolves
  it (upstreams the controller creates for a name get `update_frequency = "5s"`).
- Pingap forwards the client's `Host` header. If a layer behind the Service routes by Service
  name, set `proxy_set_headers = ["Host:legacy.prod.svc.cluster.local"]` on the location.
- Lost compared with `pods`: per-pod health checks, fast drain and per-request gRPC spread across
  pods. Sticky sessions do not work either: Pingap sees one address.
- To add the quickstart Pingap pods to a layer that selects pods by label or annotation (a mesh),
  use `pingap.podLabels` and `pingap.podAnnotations`.

## Gateway API

The Gateway API provider (`gateway.enabled: true`, off by default) turns Gateway API v1.7.0
Standard-channel resources into Pingap config in the same etcd prefix: GatewayClass, Gateway,
ListenerSet, HTTPRoute, GRPCRoute, ReferenceGrant and BackendTLSPolicy. Install the CRDs first:

```sh
kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.7.0/standard-install.yaml
```

The chart creates the GatewayClass `pingap` (controllerName `pingap.io/gateway-controller`); list
the namespaces of backends, certificate Secrets and CA ConfigMaps in `gateway.namespaces`. Without the CRDs the controller
logs a warning, keeps syncing the configured upstreams, and starts the provider when the CRDs
appear.

Conformance (v1.7.0): <RESULTS>

`scripts/conformance.sh` runs the official suite on a kind cluster of its own (profiles from
`CONF_PROFILES`, default `GATEWAY-HTTP,GATEWAY-GRPC`) and writes the report to
`target/conformance/report.yaml`.

### How resources map

| Gateway API | Pingap |
|---|---|
| Gateway listener (port, HTTP or HTTPS) | an **existing** Pingap server with that port in `addr` (HTTPS: `global_certificates = true`). The controller never creates servers (a new server needs a Pingap restart): a listener without one gets `Accepted=False` (`PortUnavailable`). |
| HTTPS `certificateRefs` (a `kubernetes.io/tls` Secret) | a certificate with the listener hostname as `domains`, chosen by SNI; a Secret renewal rewrites it |
| HTTPRoute rule match | one location per server, host and match: `Exact` → `=path`, `PathPrefix` → a segment-aware regex (`/foo` matches `/foo` and `/foo/x`, not `/foobar`), `RegularExpression` → an anchored regex, exact header and query matches → `match_headers` / `match_query`. Explicit location `weight`s encode Gateway API precedence. |
| `backendRefs` (Services, weighted) | an upstream kept current by the endpoint writer; each Service gets its share of the weight. Backends that are missing, not permitted or of another kind get a 500 for their share (503 for GRPCRoutes); a new backend without endpoints gets a 503 until it has some. |
| `RequestHeaderModifier`, `ResponseHeaderModifier` | `request_headers` / `response_headers` plugins (Host goes to `proxy_set_headers`) |
| `RequestRedirect`, `URLRewrite` | the `redirect` plugin; the location `rewrite` and `proxy_set_headers` |
| `CORS`, `RequestMirror` | the `cors` plugin; one `mirror` plugin per filter |
| `retry`, `timeouts` | location `max_retries`; connect, read and write timeouts |
| status | `Accepted`, `Programmed`, `ResolvedRefs`, listener and route parent conditions, written by the leader. Conditions of other controllers are kept. |

Details and limits:

- **Timeouts** (`request`, `backendRequest`) apply per read and write, not to the whole
  exchange: a backend that does not start to answer in time gets a 504, one that sends its body
  slowly does not. `0s` becomes the longest Gateway API duration.
- **Retries**: `attempts` retries connection failures only, each to another backend.
  `retry.codes` drops the rule (Pingap never retries on a status); `retry.backoff` makes the route
  `Accepted=False` (`UnsupportedValue`), since Pingap retries at once.
- **Mirrors** go to the Service's cluster DNS name (bodies up to 1 MiB; several per rule;
  `percent` and `fraction` honored).
- **CORS** with `allowCredentials`: a `*` in `allowMethods` becomes the full method list, and a
  `*` in `allowHeaders` or `exposeHeaders` drops the rule. `HTTPRouteCORS` is therefore not
  claimed as a supported feature.
- **h2c backends**: HTTPRoute backends whose Service port has the `appProtocol`
  `kubernetes.io/h2c` (or `grpc`) are reached over HTTP/2 cleartext.
- A rule that uses an unsupported field is dropped and the route reports `PartiallyInvalid`.
  Header values that start with `$` or `:` are refused (Pingap would read them as variables).
- Generated entries are named `k8s-...` and carry a `pingap-k8s` remark. The controller only
  changes or deletes `k8s-` entries; hand-made locations, plugins and upstreams stay, and a
  server's hand-made locations keep their place. Generated locations get weights from 65000 down,
  above the weights Pingap computes for hand-made locations.

### GRPCRoute

- Method matches (`Exact`, `RegularExpression`) become paths `/<service>/<method>`; header
  matches are `Exact` only. Filters: `RequestHeaderModifier`, `ResponseHeaderModifier`.
  Weighted and cross-namespace backends (ReferenceGrant from GRPCRoute) work as for HTTPRoutes.
- Backends are reached over HTTP/2: h2c, or TLS with ALPN `h2` under a BackendTLSPolicy.
- A listener accepts GRPCRoutes only when every Pingap server it maps to sets
  `enabled_h2 = true` (add it yourself; the controller never writes servers).
- An HTTPRoute and a GRPCRoute with intersecting hostnames on one listener are not mixed: the
  older route wins, the newer gets `Accepted=False`.
- Not supported: `RequestMirror` (Pingap mirrors over HTTP/1.1 only) and regex header matches.

### BackendTLSPolicy

- TLS to the backend with SNI = `validation.hostname`, verified against that hostname with the CA
  bundle from a ConfigMap key `ca.crt` or the system roots (`wellKnownCACertificates: System`).
- `subjectAltNames` is not supported (the policy is not accepted). Backends of a policy that is
  not accepted get a 500.
- The `appProtocol` values `https`, `wss` and `kubernetes.io/wss` are accepted on ports a policy
  applies to.
- Gateway `spec.tls.backend.clientCertificateRef` presents that `kubernetes.io/tls` Secret to TLS
  backends (ReferenceGrant for another namespace); Gateways report it in `ResolvedRefs`.

### Server groups

Without groups, all Gateways of the class share the same Pingap servers, certificates and
`gateway.addresses`: listeners with the same port and hostname are merged and their routes served
side by side. A server group gives one Gateway its own Pingap servers and address inside one
Pingap. With `pingap.deploy`, the chart seeds the group servers (`gw-<name>-<port>`) and creates
one Service per group (`<release>-pingap-gw-<name>`, type and annotations from `pingap.service`)
that maps each listener port to its server port. The group's address is that Service's DNS name
unless `addresses` is set.

```yaml
gateway:
  enabled: true
  groups:
    - name: a
      ports:
        - { port: 80, targetPort: 9080, settings: { enabled_h2: true } }
        - { port: 443, targetPort: 9443, tls: true }
```

A Gateway gets a group by, in order: its `spec.addresses` naming the group's addresses; the group
it already has (`status.addresses`); the oldest Gateways take free groups in config order; the
rest share the shared servers (`Programmed=False`, `AddressNotAssigned`, when there is no shared
address). A Gateway that asks for an address the controller does not serve is not programmed
(`AddressNotUsable`).

The seed writes only to an empty etcd prefix: to add a group to an existing install, create its
servers by hand and restart Pingap.

### ListenerSet

Gateways accept ListenerSets through `allowedListeners` (none by default). Their listeners join
the parent Gateway's servers; on conflicts the Gateway's own listeners win, then ListenerSets
oldest first.

### Supported features

`status.supportedFeatures` of the GatewayClass:

`BackendTLSPolicy`, `GRPCRoute`, `GRPCRouteNamedRouteRule`, `GRPCRouteRequestHeaderModifier`,
`Gateway`, `GatewayBackendClientCertificate`, `GatewayStaticAddresses`, `HTTPRoute`,
`HTTPRoute303RedirectStatusCode`, `HTTPRoute307RedirectStatusCode`,
`HTTPRoute308RedirectStatusCode`, `HTTPRouteBackendProtocolH2C`,
`HTTPRouteBackendProtocolWebSocket`, `HTTPRouteBackendTimeout`, `HTTPRouteHostRewrite`,
`HTTPRouteNamedRouteRule`, `HTTPRouteParentRefPort`, `HTTPRoutePathRedirect`,
`HTTPRoutePathRewrite`, `HTTPRoutePortRedirect`, `HTTPRouteQueryParamMatching`,
`HTTPRouteRequestMirror`, `HTTPRouteRequestMultipleMirrors`,
`HTTPRouteRequestPercentageMirror`, `HTTPRouteRequestTimeout`,
`HTTPRouteResponseHeaderModification`, `HTTPRouteRetryConnectionError`,
`HTTPRouteSchemeRedirect`, `ListenerSet`, `ReferenceGrant`.

Not supported: TCPRoute, UDPRoute and TLSRoute (passthrough and terminate; Pingap 0.16.1 is an
HTTP-only L7 proxy, and L4 was declined upstream in
[vicanso/pingap#159](https://github.com/vicanso/pingap/issues/159)), method matching and regex
header or query matches, `HTTPRouteRetryCodes`, retry backoff, BackendTLSPolicy SAN validation,
and session persistence.

## Graceful shutdown of application pods (preStop)

Draining means removing the endpoint from `addrs`, and Pingap health-checks the remaining
backends before it swaps a rebuilt upstream in. So a terminating pod can still get new requests
for the debounce, plus the etcd watch and reload, plus that health-check round (up to the
upstream's connect timeout). Give the app pods behind owned upstreams a `preStop` delay that
covers it:

```yaml
spec:
  terminationGracePeriodSeconds: 30
  containers:
    - name: app
      lifecycle:
        preStop:
          sleep: { seconds: 10 }   # >= debounce_max + reload + upstream health-check connect timeout
```

With the defaults (2 s debounce cap, about 1 s reload), 10 s covers a health-check connect timeout
of up to about 5 s. Each write rebuilds the upstream and resets its runtime state (circuit
breaker, backend stats, round-robin position); the debounce keeps writes during a rollout few.

## Headers, sticky sessions and auth

For configured upstreams the controller only writes `addrs`; headers, sticky sessions and
authentication are Pingap settings on the location or upstream (the Gateway provider generates
header plugins from route filters). Pointers:

- The client's request headers reach the pod as sent; WebSocket, gRPC (h2c) and server-sent
  events pass through. `enable_reverse_proxy_headers = true` on a location adds `X-Real-IP`,
  `X-Forwarded-For`, `-Proto`, `-Host` and `-Port`.
- Sticky sessions (`algo = "sticky:<cookie>"` or `hash:...`) and consistent hashing need
  `target = "pods"`, where Pingap sees every pod. A terminating pod leaves `addrs` at once, so
  affinity is a preference, not a guarantee.
- Authentication plugins (`basic_auth`, `key_auth`, `jwt`, `oidc`, `forward_auth`, `hmac_auth`,
  `combined_auth`) and mutual TLS run in Pingap before the request reaches the pod. For LDAP or
  plain OAuth2, use `forward_auth` with an auth service in the cluster.

See the [Pingap documentation](https://github.com/vicanso/pingap) for the details.

## Upgrades and hot reload

Traffic is not interrupted by any of these:

- **Upgrading the controller**: it is not on the request path, and Pingap keeps serving from
  etcd. A stopping leader resigns and revokes its lease, so a standby takes over at once and
  resyncs every upstream (with `replicaCount: 1`, after the new pod's startup).
- **Changing the controller config**: reloaded live, see [Helm](#helm).
- **Upgrading the quickstart Pingap**: rolling update with surge, a 15 s `preStop`, then
  pingora's graceful shutdown. Changing Pingap `servers` or `[basic]` needs a Pingap restart.
- **Application rollouts**: terminating pods leave `addrs` at once and a single draining pod is
  kept until its replacement is Ready; the app pods need a
  [preStop delay](#graceful-shutdown-of-application-pods-prestop). The e2e suite checks for no
  failed requests under load during rollouts and single-replica restarts.

## Production checklist

- [ ] **App pods have a `preStop` delay** sized from the upstream health-check connect timeout
      ([above](#graceful-shutdown-of-application-pods-prestop)).
- [ ] **Every owned upstream has a Pingap health check**, e.g.
      `health_check = "http://api/healthz?connection_timeout=1s&check_frequency=5s"`. Minimal
      upstreams created by the controller have none, and fail-static depends on it. Prefer an
      HTTP check with a path only this app serves over `tcp://` (a freed pod IP can go to another
      app), and a short connect timeout (about 1 s): each write waits for a health-check round.
- [ ] **NetworkPolicy allows Pingap pods to app pods** on the target port. Policies that only
      admit traffic "from the Service" do not apply to pod IPs; use
      [`target = "service"`](#upstream-target) if traffic must pass the Service.
- [ ] **Prometheus may scrape the controller**: list it in `networkPolicy.metricsFrom`
      ([Observability](#observability)).
- [ ] **Client source IP** is decided: `externalTrafficPolicy: Local` on the Pingap Service
      (spread the replicas), or PROXY protocol (server `proxy_protocol = true` with
      `basic.trusted_proxies`).
- [ ] **Dual-stack**: keep `ip_family = "primary"` (the default) unless the Pingap pods route
      both families to the app pods. Listen on `[::]` to accept both.
- [ ] **Pingap timeouts fit the workload**: upstream `idle_timeout` shorter than the app's
      keep-alive; longer `read_timeout` on streaming locations (SSE, gRPC streams) or app
      heartbeats. WebSockets and long streams stay on their pod after scaling. See the Pingap
      documentation.
- [ ] **Bundled etcd maintenance**: auto-compaction is on (1 h), but compaction does not free
      disk space: run `etcdctl defrag` now and then, one member at a time; take regular
      `etcdctl snapshot save` backups; size `--quota-backend-bytes` (via `etcd.extraArgs`) and
      the volume together, and watch for the `NOSPACE` alarm.
- [ ] **Quickstart Pingap runs 2 or more replicas** (`pingap.replicas`) for its
      PodDisruptionBudget. A terminating Pingap pod cuts streams after about 45 s; raise seed
      `grace_period` and `pingap.terminationGracePeriodSeconds` together for longer ones.

## Operating it with kubectl

For release `pingap-k8s` in namespace `pingap-k8s` (as in [`deploy/`](deploy/)); change both to
yours.

```sh
# Pods, logs, readiness
kubectl -n pingap-k8s get pods -l app.kubernetes.io/component=controller -o wide
kubectl -n pingap-k8s logs -l app.kubernetes.io/component=controller --tail=100 -f --prefix
kubectl -n pingap-k8s port-forward deploy/pingap-k8s 9090 &
curl -s localhost:9090/readyz; echo

# What the controller sees, and what it stored (the bundled etcd pod has etcdctl configured)
kubectl -n <app-namespace> get endpointslices -l kubernetes.io/service-name=<service> -o wide
kubectl -n pingap-k8s exec pingap-k8s-etcd-0 -c etcd -- etcdctl get --prefix /pingap/upstreams/

# Events (with events.enabled)
kubectl get events -A --field-selector reportingComponent=pingap-k8s

# RBAC: "yes" expected for each namespace in config.upstreams
kubectl auth can-i watch endpointslices.discovery.k8s.io -n <app-namespace> \
  --as=system:serviceaccount:pingap-k8s:pingap-k8s
```

### kubectl plugin

`kubectl pingap` shows what the controller sees and wrote, per upstream, from the leader pod. It
reads the controller's `/status` endpoint, which is off by default:

```yaml
status:
  enabled: true   # /status on 127.0.0.1:9091 inside the pod; restarts the controller pods
```

Install the plugin from the [latest release](https://github.com/mehdyhaghy/pingap-k8s/releases/latest)
(`kubectl-pingap_<version>_<os>_<arch>.tar.gz` for linux/darwin and amd64/arm64, checked against
`kubectl-pingap_<version>_checksums.txt`) and put `kubectl-pingap` on your `PATH`, or with krew:

```sh
kubectl krew install --manifest-url=https://github.com/mehdyhaghy/pingap-k8s/releases/latest/download/pingap.yaml
```

```sh
kubectl pingap status -n pingap-k8s          # leader, and one row per upstream: target, Services, ready/draining, in sync, last write
kubectl pingap explain web -n pingap-k8s     # warnings, watch state and why an upstream has no endpoints
kubectl pingap admin -n pingap-k8s           # port-forward the quickstart admin UI, print the URL and the password command
```

```text
controller pingap-k8s-6d4f9c7b8-x2kqp (leader) v<version>, phase running, ready yes, etcd prefix /pingap, watchers 2/2 synced
UPSTREAM  TARGET   SERVICES                      READY  DRAINING  IN SYNC  LAST WRITE  NOTES
api       pods     api.prod:http                 2      0         yes      ok 12s ago
web       service  web.prod:80 9,gone.prod:80 1  0      0         no       -           no endpoints, 1 warning
```

It runs `kubectl` underneath, so `--context`, `--kubeconfig` and your credentials apply.
`admin` prints the password only with `--show-password`, and only forwards to a Service with all
three labels of the chart's Pingap (`app.kubernetes.io/part-of=pingap-k8s`,
`app.kubernetes.io/name=pingap`, `app.kubernetes.io/component=proxy`); when more than one matches,
it stops and asks for `-n`.

`/status` listens on `127.0.0.1` inside the controller pod only and is reached with
`kubectl port-forward`, so RBAC on `pods/portforward` decides who may use it. It is read-only and
holds no secrets (no etcd URL, credentials, certificate paths, tokens or admin password).

## Observability

The controller serves on port `9090` (cluster-internal only): `/metrics` (Prometheus),
`/healthz` (process alive) and `/readyz` (all watchers synced, and etcd was reachable on the last
write; followers never write). A malformed upstream entry or a CAS conflict does not fail
`/readyz`: it shows in `pingap_k8s_writes_total` and the logs, and is retried on the next event or
resync.

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

The chart's metrics Service is `pingap-k8s-metrics` (`ClusterIP`). The chart's NetworkPolicy
closes the port to other pods by default; to scrape it, list Prometheus in
`networkPolicy.metricsFrom` (example in `values.yaml`) or set `networkPolicy.allowSameNamespace`.

With `events.enabled: true` the controller also posts Kubernetes Events (`reportingController`
`pingap-k8s`), once per episode, like its warning logs: `ServiceMissing`, `PortNotInService`,
`PortNotInSlices`, `ExternalNameNotEnabled`, `ExternalNameInvalid`, `TrafficPolicyIgnored` and
`DrainFallbackStarted`/`DrainFallbackEnded` on the Service (by the leader), `MalformedUpstream` and
`EtcdUnreachable`/`EtcdRecovered` on the controller Pod. Repeats update the existing Event, and
each object gets at most 25, then one per 5 minutes.

### Telemetry

Metrics are recorded once through OpenTelemetry and exposed two ways: `/metrics` (always) and,
when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, OTLP over gRPC (OpenTelemetry names such as
`pingap_k8s.writes` with `k8s.namespace.name`-style attributes). Logs always go to stderr; with an
endpoint the same events are also sent as OTLP log records. There are no traces.

Only the standard `OTEL_*` variables are read (`OTEL_EXPORTER_OTLP_ENDPOINT`, `_HEADERS`,
`_TIMEOUT`, `OTEL_METRIC_EXPORT_INTERVAL`, `OTEL_SDK_DISABLED`, `OTEL_SERVICE_NAME`, default
`pingap-k8s`, and `OTEL_RESOURCE_ATTRIBUTES`). The chart sets the endpoint from `otel.endpoint`
(e.g. `http://otel-collector.observability.svc:4317`) and the `k8s.namespace.name`,
`k8s.pod.name` and `k8s.deployment.name` attributes, plus `otel.resourceAttributes`.

The endpoint must be a collector on the private or cluster network, never a public address (a
public IP address logs a warning at startup). Use `https://` for TLS (system roots). Pass
`OTEL_EXPORTER_OTLP_HEADERS` (for example an auth token) from a Secret with `extraEnv` and
`valueFrom.secretKeyRef`.

## Security

Report vulnerabilities privately, as described in [SECURITY.md](SECURITY.md).

- **Kubernetes access is read-only and namespaced** unless you opt in; see the RBAC table under
  [Helm](#helm). API server TLS is always verified; there is no insecure option. The
  ServiceAccount token is re-read on rotation.
- **etcd write access is the main risk**: whoever can write `<prefix>/` can redirect traffic.
  - Keep etcd on the private or cluster network only: a `ClusterIP` Service plus a NetworkPolicy
    that admits only Pingap and pingap-k8s pods. Never give it a public address. The bundled etcd
    does exactly this.
  - Use client-certificate authentication (mTLS). On a shared etcd, give the controller its own
    etcd user with `readwrite` on exactly the prefixes it needs. For prefix `/pingap`:

    ```sh
    etcdctl role add pingap-k8s
    etcdctl role grant-permission pingap-k8s readwrite /pingap/upstreams/ --prefix=true
    etcdctl role grant-permission pingap-k8s readwrite /pingap-k8s-election/ --prefix=true
    # With gateway.enabled, also the entries it generates:
    for c in servers locations plugins certificates; do
      etcdctl role grant-permission pingap-k8s readwrite /pingap/$c/ --prefix=true
    done
    etcdctl user add pingap-k8s --no-password   # authenticated by the certificate CN
    etcdctl user grant-role pingap-k8s pingap-k8s
    ```

  - Certificates and keys come from a mounted Secret. A plaintext etcd connection logs a warning
    (and `helm install` prints one); a password in the etcd URL is redacted in errors and notes.
  - The bundled etcd accepts a peer certificate only with the server certificate's CN
    (`--peer-cert-allowed-cn`), so a client certificate cannot join the cluster as a member.
    With your own certificates, set `etcd.tls.peerAllowedCN` to the server certificate's CN.
- **Only addresses from configured Services are written**. Loopback, link-local (for example
  `169.254.169.254`), unspecified, broadcast and multicast addresses are skipped, so a
  hand-written EndpointSlice cannot point an upstream at the node or at cloud metadata.
  `external_name` lifts this for `ExternalName` Services (see the
  [configuration reference](#configuration-reference)).
- **Cluster-internal endpoints**: the metrics port, etcd and the quickstart Pingap admin UI are
  `ClusterIP` only, and NetworkPolicies close the metrics and admin ports to other pods by default.
  CI fails if the chart renders a `LoadBalancer` or `NodePort` Service that was not asked for
  with `pingap.service.type`, or cluster-wide RBAC other than the gateway ClusterRole (reads and
  status patches only, no Secrets).
- **Bounded HTTP servers**: the metrics/health and `/status` servers serve at most 64 connections
  at once, and close a connection after 10 s without traffic or 60 s in total.
- **Hardened pods**: non-root (UID 65532), read-only root filesystem, no privilege escalation, all
  capabilities dropped, `RuntimeDefault` seccomp, for the controller, the bundled etcd and the
  quickstart Pingap. The controller image is distroless (no shell).
- **Signed releases**: the image, the chart and every release file are signed with cosign
  (keyless, Sigstore) by the release workflow, and the base images are pinned by digest. To
  verify a release:

  ```sh
  VERSION=<version>   # the release to verify, e.g. from the releases page
  ID="https://github.com/mehdyhaghy/pingap-k8s/.github/workflows/release.yml@refs/tags/v$VERSION"
  ISSUER=https://token.actions.githubusercontent.com
  cosign verify --certificate-identity "$ID" --certificate-oidc-issuer "$ISSUER" \
    ghcr.io/mehdyhaghy/pingap-k8s:$VERSION
  cosign verify --certificate-identity "$ID" --certificate-oidc-issuer "$ISSUER" \
    ghcr.io/mehdyhaghy/charts/pingap-k8s:$VERSION
  # A release file, with its .sigstore.json bundle from the same release:
  cosign verify-blob --certificate-identity "$ID" --certificate-oidc-issuer "$ISSUER" \
    --bundle kubectl-pingap_${VERSION}_checksums.txt.sigstore.json kubectl-pingap_${VERSION}_checksums.txt
  sha256sum -c --ignore-missing kubectl-pingap_${VERSION}_checksums.txt
  ```

## Performance

At 50 upstreams × 20 endpoints: event-to-write p50 0.52 s (the 500 ms debounce), ~30 MB
controller RSS, one etcd revision per write. Measured with controller 0.1.0 on Kubernetes 1.32;
see [docs/benchmarks.md](docs/benchmarks.md).

## Limitations

- Gateway API: servers are never generated (listeners need a Pingap server on their port); see
  [Supported features](#supported-features). Without [server groups](#server-groups), all
  Gateways of the class share the same Pingap servers, certificates and addresses.
- etcd is the only write target. Pingap file or ConfigMap storage is not supported.
- Pingap must be able to reach pod IPs, or with `target = "service"` the ClusterIPs (normally:
  run Pingap inside the cluster).
- Bundled etcd: the member count (1 or 3) is fixed at install; changing `etcd.replicas` later does
  not reconfigure the cluster. Generated certificates are valid for `etcd.tls.validityDays` (10
  years by default) and are not rotated automatically. No etcd users or roles are configured.

## Development

```sh
make check        # fmt check, clippy, tests, helm lint/template, exposure, version and manifests checks
```

Building needs `protoc` and the protobuf well-known types. See [CONTRIBUTING.md](CONTRIBUTING.md)
for the test scripts, CI and the release process.

Built with [kube-rs](https://github.com/kube-rs/kube), [k8s-openapi](https://github.com/Arnavion/k8s-openapi),
[etcd-client](https://github.com/etcdv3/etcd-client), [toml_edit](https://github.com/toml-rs/toml),
[tokio](https://tokio.rs), [axum](https://github.com/tokio-rs/axum),
[tracing](https://github.com/tokio-rs/tracing), [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-rust)
and [prometheus](https://github.com/tikv/rust-prometheus). Pingap is built on
[Pingora](https://github.com/cloudflare/pingora).

## License

Apache-2.0, the same license as Pingap. See [LICENSE](LICENSE).

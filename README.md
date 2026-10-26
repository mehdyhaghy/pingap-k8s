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
  - [Plain manifests](#plain-manifests)
  - [etcd client certificates](#etcd-client-certificates)
- [Configuration reference](#configuration-reference)
- [Pingap side](#pingap-side)
- [Graceful shutdown of application pods (preStop)](#graceful-shutdown-of-application-pods-prestop)
- [Observability](#observability)
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

- Kubernetes 1.32 or newer.
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
Services that were added or removed.

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
  the prefix has no server yet**: one listener on `:8080`, one location, and the upstream
  `quickstart` (with a placeholder address the controller replaces). Servers (listeners) are
  read only at Pingap start, which is why they are seeded first.
- The proxy listener is exposed through the `pingap-k8s-pingap` Service, whose type you choose
  (`pingap.service.type`).
- The admin UI listens on its own port behind a `ClusterIP`-only Service. Open it with a
  port-forward:

  ```sh
  kubectl -n edge get secret pingap-k8s-pingap-admin -o jsonpath='{.data.password}' | base64 -d; echo
  kubectl -n edge port-forward svc/pingap-k8s-pingap-admin 3018:3018
  # http://localhost:3018, user "admin"
  ```

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

[defaults]
include_not_ready = false
drain_terminating = true
debounce = "500ms"
debounce_max = "2s"
resync = "5m"            # full re-list and re-reconcile, corrects drift
ipv4_only = false

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
| `include_not_ready` | `false` | also write endpoints that are not ready (and not terminating) |
| `drain_terminating` | `true` | remove terminating endpoints; `false` treats serving terminating endpoints as ready |
| `debounce` | `500ms` | wait this long after the last event before writing |
| `debounce_max` | `2s` | write at the latest this long after the first pending event |
| `resync` | `5m` | reconcile every upstream at this interval even without events |
| `ipv4_only` | `false` | ignore IPv6 EndpointSlices |
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

Command line flags: `--config <path>` (env `PINGAP_K8S_CONFIG`) and `--log-format text|json`.
The log filter comes from `RUST_LOG`.

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

## Observability

The controller serves on port `9090` (cluster-internal only):

- `/metrics` (Prometheus), `/healthz` (process alive), `/readyz` (all watchers synced and the last
  etcd write succeeded).

| Metric | Labels |
|---|---|
| `pingap_k8s_endpoints` | `upstream`, `class="ready\|draining"` |
| `pingap_k8s_writes_total` | `upstream`, `result="ok\|conflict\|error\|unchanged"` |
| `pingap_k8s_foreign_writes_total` | `upstream`, `corrected="true\|false"` |
| `pingap_k8s_watch_up` | `namespace`, `service` |
| `pingap_k8s_watch_errors_total` | `namespace`, `service`, `reason` |
| `pingap_k8s_event_to_write_seconds` | histogram |
| `pingap_k8s_drain_fallback` | `upstream` |

The chart's metrics Service is `pingap-k8s-metrics` (`ClusterIP`, release `pingap-k8s`).

## Security

- **Kubernetes access is read-only and namespaced**: `get/list/watch` on `services` and
  `endpointslices`, through one Role per namespace. Never a ClusterRole. API server TLS is always
  verified; there is no insecure option. The ServiceAccount token is re-read on rotation.
- **etcd write access is the main risk**: whoever can write `<prefix>/` can redirect traffic.
  - Keep etcd on the private or cluster network only: a `ClusterIP` Service plus a NetworkPolicy
    that admits only Pingap and pingap-k8s pods. Never give it a public address. The bundled etcd
    does exactly this.
  - Use client-certificate authentication (mTLS). On a shared etcd, give the controller its own
    etcd user limited to `<prefix>/upstreams/` and the election prefix
    `<prefix>-k8s-election/`, for example:

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
  are `ClusterIP` only. CI fails if the chart renders a `LoadBalancer` or `NodePort` Service other
  than the quickstart proxy listener, or any cluster-wide RBAC.
- **Hardened pods**: non-root (UID 65532), read-only root filesystem, no privilege escalation, all
  capabilities dropped, `RuntimeDefault` seccomp, for the controller, the bundled etcd and the
  quickstart Pingap. The controller image is distroless (no shell).

## Limitations

- Phase 1 manages upstream `addrs` only. Locations, servers and certificates are not generated
  (Ingress and Gateway API support are planned).
- etcd is the only write target. Pingap file or ConfigMap storage is not supported.
- Pingap must be able to reach pod IPs (normally: run Pingap inside the cluster).
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

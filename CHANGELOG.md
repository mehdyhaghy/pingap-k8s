# Changelog

## 0.16.10 (unreleased)

Versions now follow Pingap: the Pingap version the release is tested with plus one digit for our
own releases (Pingap 0.16.1: 0.16.10 to 0.16.19). `make version-check` and the release workflow
enforce it. Kubernetes 1.35 or newer is required (chart `kubeVersion`, k8s-openapi `v1_35`).

Gateway API provider (off by default, `gateway.enabled`), Gateway API v1.7.0 Standard channel:
- GatewayClass, Gateway, ListenerSet, HTTPRoute, GRPCRoute, ReferenceGrant and BackendTLSPolicy
  become Pingap locations, plugins, certificates and upstreams (named `k8s-...`). Listeners map
  onto existing Pingap servers by port and TLS; locations get explicit weights for Gateway API
  precedence, segment-aware prefixes, and exact header and query matches.
- HTTPRoute filters: request and response header modifiers, redirects, URL rewrites, CORS and
  RequestMirror (several per rule, percent or fraction). `retry.attempts` (connection failures
  only; `retry.backoff` makes the route `Accepted=False`, `retry.codes` drops the rule) and
  `timeouts.request` / `backendRequest` (per read and write). Weighted backendRefs; invalid
  backends answer 500, backends without endpoints 503. Backends with the `appProtocol`
  `kubernetes.io/h2c` or `grpc` use h2c.
- GRPCRoute: method matches (Exact, RegularExpression), exact header matches, header modifiers,
  weighted and cross-namespace backends over HTTP/2 (h2c, or TLS with ALPN h2). Listeners take
  GRPCRoutes only on servers with `enabled_h2 = true`; an HTTPRoute and a GRPCRoute with
  intersecting hostnames on one listener are not mixed (the older wins). Invalid gRPC backends
  answer 503.
- BackendTLSPolicy: TLS with SNI and verification against `validation.hostname`, with a CA from a
  ConfigMap (`ca.crt`) or the system roots; `subjectAltNames` is not supported. Gateway
  `spec.tls.backend.clientCertificateRef` presents a client certificate to TLS backends.
- Server groups (`[[gateway.groups]]`, chart `gateway.groups`): a Gateway gets Pingap servers and
  an address of its own; the chart seeds the group servers and creates one Service per group.
  **Behavior change**: a Gateway asking (`spec.addresses`) for an address the controller does not
  serve is not programmed (`AddressNotUsable`).
- ListenerSets attach to Gateways that allow them (`allowedListeners`).
- Status for GatewayClasses (with `supportedFeatures`), Gateways, ListenerSets, routes and
  BackendTLSPolicies, written by the leader; conditions of other controllers are kept.
- Chart: `gateway.*` values, a GatewayClass, and a ClusterRole that only reads the Gateway API
  resources and namespaces and patches their status. Secrets and ConfigMaps stay namespaced
  (`gateway.secretNames`, `gateway.configMapNames`). `make exposure-check` allows only that
  ClusterRole.
- Not supported: TCPRoute, UDPRoute, TLSRoute (Pingap is HTTP-only), retry codes and backoff,
  BackendTLSPolicy SAN validation, session persistence.
- Conformance (v1.7.0): GATEWAY-HTTP core 40/40 and extended 42/42; GATEWAY-GRPC core 17/17 and extended 9/9 (Standard channel, no failures).

Upstreams:
- `ip_family = "primary" | "ipv4" | "ipv6" | "both"` (default `primary`: one address per pod, in
  the Service's primary family). `ipv4_only` is a deprecated alias; setting both is an error.
- `external_name` (default off): `ExternalName` Services write `<externalName>:<port>`. Off, they
  are skipped with an `ExternalNameNotEnabled` warning in `pods` mode.
- Upstreams get `alpn = "H2"` when every Service port has the `appProtocol` `kubernetes.io/h2c`
  or `grpc` (an existing `alpn` is never changed).
- `target = "pods"` warns (`TrafficPolicyIgnored`) about Services with `trafficDistribution` or
  `internalTrafficPolicy: Local`, which it cannot honor.
- `target = "service"` writes the Service's ClusterIP instead of its DNS name (a headless Service
  keeps the name), so a recreated Service is followed within the debounce rather than after
  Pingap's DNS TTL. Upstreams the controller creates with a hostname get
  `update_frequency = "5s"`.
- New Event reasons: `ExternalNameNotEnabled`, `ExternalNameInvalid`, `TrafficPolicyIgnored`.

Chart:
- `values.schema.json`: values are validated and unknown keys are rejected.
- `pingap.service.ipFamilyPolicy` and `ipFamilies`; `gateway.maxTxnOps`, `gateway.configMapNames`,
  `gateway.groups`.
- The controller pods restart on changes to settings read only at startup (`gateway.enabled`,
  `controllerName`, `maxTxnOps`, `events.enabled`, `config.server`).

Security:
- Releases are signed with cosign (keyless, Sigstore): the image, the OCI chart, and each release
  file with a `.sigstore.json` bundle. The README shows how to verify.
- The Dockerfile base images are pinned by digest; Dependabot proposes updates for them and for
  the GitHub Actions.
- The controller NetworkPolicy is now on by default (`networkPolicy.enabled: true`): the metrics
  port is closed to other pods. **To keep scraping `/metrics`, list your Prometheus in
  `networkPolicy.metricsFrom`** (or set `networkPolicy.allowSameNamespace`). Probes and
  port-forward are not affected.
- The quickstart Pingap gets a NetworkPolicy (on by default): only the proxy port is open to other
  pods, and the admin port is closed except to `pingap.networkPolicy.adminFrom`.
- The metrics/health and `/status` servers serve at most 64 connections at once, and close a
  connection after 10 s without traffic or 60 s in total.
- EndpointSlice addresses that are loopback, link-local, unspecified, broadcast or multicast are
  skipped.
- A password in the etcd URL no longer appears in URL errors or in the chart's install notes, and
  a plaintext etcd connection logs a warning.
- The bundled etcd accepts peer certificates only with the server CN (`--peer-cert-allowed-cn`;
  `etcd.tls.peerAllowedCN` for your own certificates).
- An OTLP endpoint given as a public IP address logs a warning.
- `kubectl pingap admin` matches the chart's Pingap Service by all three of its labels, and stops
  when more than one Service matches.

Tests and CI:
- The `ci` workflow is started by hand (`gh workflow run ci`); the `release` workflow runs all of
  it first and publishes nothing unless it passes. The e2e job runs on Kubernetes 1.35 and 1.37.
- New CI jobs: `integration` (etcd and kind watcher tests), `secrets` (gitleaks) and
  `dependencies` (`cargo audit`).
- `scripts/conformance.sh` runs the Gateway API conformance suite (`GATEWAY-HTTP`,
  `GATEWAY-GRPC`) on its own kind cluster.
- `SECURITY.md` (private vulnerability reporting) and `CODEOWNERS`.

## 0.1.0

First release: keeps the `addrs` of Pingap upstreams in etcd in sync with Kubernetes
EndpointSlices.

- Watches the Services and EndpointSlices named in the config and writes ready pod IPs (or, with
  `target = "service"`, the Service DNS name) into Pingap's etcd config. Writes are compare-and-swap
  and only touch `addrs`; every other field of an upstream stays as set in Pingap.
- Graceful drain: terminating endpoints leave `addrs` at once, and a single draining pod is kept
  until a replacement is Ready. An empty result never empties an upstream.
- Per-upstream debounce (500 ms, capped at 2 s), periodic resync, and correction of `addrs` edits
  made outside the controller.
- Leader election in etcd for multiple replicas.
- Metrics (Prometheus `/metrics`, optional OTLP), `/healthz` and `/readyz`, an opt-in localhost-only
  `/status`, and opt-in Kubernetes Events.
- `kubectl pingap` plugin: `status`, `explain`, `admin`.
- Helm chart with an optional bundled etcd (mTLS, NetworkPolicy) and a quickstart Pingap, plus
  plain manifests in `deploy/`.
- Multi-arch image (`linux/amd64`, `linux/arm64`).
- Tested with Pingap 0.16.1 on Kubernetes 1.35 and 1.37 (IPv4 and dual-stack). Benchmarks are in
  [docs/benchmarks.md](docs/benchmarks.md).

# Changelog

## 0.16.20 (unreleased)

First public release. Tested with Pingap 0.16.2 (`pingap.image.tag`) on Kubernetes 1.37, IPv4
and dual-stack. Versions follow Pingap: the Pingap version a release is tested with plus one
digit for our own releases (Pingap 0.16.2: 0.16.20 to 0.16.29). Kubernetes 1.37 or newer is
required.

Endpoint discovery:
- Watches the Services and EndpointSlices named in the config and writes the ready pod addresses
  (or, with `target = "service"`, the ClusterIP; the DNS name for a headless Service) into the
  `addrs` of Pingap upstreams in etcd. Writes are compare-and-swap and only touch `addrs`; every
  other field of an upstream stays as set in Pingap.
- Graceful drain: terminating endpoints leave `addrs` at once, and a single draining pod is kept
  until a replacement is Ready. An empty result never empties an upstream.
- Per-upstream debounce (500 ms, capped at 2 s), periodic resync, and correction of `addrs` edits
  made outside the controller. Leader election in etcd for multiple replicas.
- `ip_family = "primary" | "ipv4" | "ipv6" | "both"`; opt-in `ExternalName` Services
  (`external_name`); `alpn = "H2"` for Services whose ports all have the `appProtocol`
  `kubernetes.io/h2c` or `grpc`. Warnings for Services with `trafficDistribution` or
  `internalTrafficPolicy: Local`, which `target = "pods"` cannot honor.

Gateway API provider (off by default, `gateway.enabled`), Gateway API v1.7.0 Standard channel:
- GatewayClass, Gateway, ListenerSet, HTTPRoute, GRPCRoute, ReferenceGrant and BackendTLSPolicy
  become Pingap locations, plugins, certificates and upstreams (named `k8s-...`). Listeners map
  onto existing Pingap servers by port and TLS; locations get explicit weights for Gateway API
  precedence.
- HTTPRoute: path, exact header and query matches; header modifiers, redirects, URL rewrites,
  CORS and request mirrors; `retry.attempts` (connection failures) and timeouts; weighted and
  cross-namespace backends; h2c and WebSocket backends.
- GRPCRoute: method and exact header matches, header modifiers, weighted backends over HTTP/2.
- BackendTLSPolicy: TLS to backends with SNI and verification against `validation.hostname`, and
  a Gateway client certificate (`spec.tls.backend.clientCertificateRef`).
- Misdirected requests: with HTTP/2 connection reuse a client can send `Host: b.example.com` on a
  connection opened for `a.example.com`. Pingap 0.16.2 keeps the TLS server name (SNI) of the
  connection and, with `strict_sni = true` on the server, answers `421 Misdirected Request` when
  the `Host` belongs to another listener
  ([vicanso/pingap#279](https://github.com/vicanso/pingap/issues/279)). Each Secret and listener
  hostname gets a certificate of its own, and the default certificate (listeners without
  hostname) only the names no listener hostname takes, so every name has one certificate.
- Server groups give a Gateway Pingap servers and an address of its own; listener isolation per
  Gateway and port; status for every handled object, written by the leader.
- Conformance (v1.7.0): GATEWAY-HTTP core 40/40 and extended 47/47, GATEWAY-GRPC core 17/17 and
  extended 9/9, no failures and no skips. The supported features are in the README.
- Not supported: TCPRoute, UDPRoute and TLSRoute (Pingap is an HTTP proxy), retry codes and
  backoff, BackendTLSPolicy SAN validation, filters that differ between weighted backendRefs, and
  session persistence.

Operations:
- Metrics (Prometheus `/metrics`, optional OTLP), `/healthz` and `/readyz`, an opt-in
  localhost-only `/status`, and opt-in Kubernetes Events.
- `kubectl pingap` plugin: `status`, `explain`, `admin` (also as a krew manifest).
- Helm chart with a values schema, an optional bundled etcd (mTLS) and a quickstart Pingap; the
  chart sets `strict_sni = true` on the HTTPS servers of `gateway.groups`. Plain manifests in
  `deploy/`. Multi-arch image (`linux/amd64`, `linux/arm64`).

Security:
- Least privilege: namespaced, read-only access to Services and EndpointSlices; with the Gateway
  API provider, one ClusterRole that only reads the Gateway API resources and patches their
  status. NetworkPolicies on by default for the controller, the bundled etcd and the quickstart
  Pingap (admin port closed).
- Releases are signed with cosign (keyless, Sigstore), with SBOM and provenance attestations; the
  README shows how to verify. Base images and GitHub Actions are pinned by digest.
- CI: tests, e2e on kind, the etcd and watcher integration tests, gitleaks, `cargo audit`, and
  dependency licenses against the CNCF allowlist (`cargo deny`).

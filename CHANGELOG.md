# Changelog

## Unreleased

- Every etcd write of the leader (upstream addrs and Gateway entries) is fenced: its Txn also
  compares the create revision of the leader's election key, so a leader that was paused (a
  frozen node, a long stall) and resumes after its lease ended cannot write once more; etcd
  refuses it and the write is logged as `not_leader`. Found by the soak test.
- Soak test: `scripts/soak.sh` (and the manual `soak` workflow) runs load through Pingap for an
  hour while pods, the controller leader, etcd members, Pingap pods and nodes fail, and writes
  a report; results in [docs/soak.md](docs/soak.md).
- etcd connections use a 10 s HTTP/2 keep-alive with a 5 s timeout (was 30 s + 10 s, as in
  Pingap): a connection to an etcd member that stops answering is dropped and redialed within
  about 15 s instead of 40 s. Found by the soak test, where a paused node stalled leader
  failover for up to a minute.
- Gateway API: generated servers (`[gateway.generate_servers]`, chart
  `gateway.generateServers`, off by default). A listener port of the shared servers without a
  Pingap server gets `k8s-http-<port>` or `k8s-https-<port>`; the listener reports
  `Programmed=False` (`Pending`) until the Pingap pods have restarted onto it. Needs a
  read-only watch of the Pingap pods.
- Gateway API: session persistence (`gateway.session_persistence`, chart
  `gateway.sessionPersistence`, off by default). The Experimental-channel `sessionPersistence`
  of HTTPRoute and GRPCRoute rules with a session cookie becomes a Pingap sticky upstream
  (`algo = "sticky:<cookie>"`). Other session settings drop the rule; so does
  `sessionPersistence` while the option is off (it was ignored before).
- CI: a weekly `upstream-releases` workflow fails when a newer Gateway API or Pingap release is
  out (`scripts/check-upstream-releases.sh`).
- e2e: tests 38 (a generated server, programmed after a Pingap restart) and 39 (session
  persistence with the Experimental-channel HTTPRoute CRD).
- README: TLS certificates from cert-manager for Gateway listeners (a Certificate or the
  Gateway annotation, RBAC, renewal, ACME HTTP-01 through Pingap, key types).
- e2e: tests 36 and 37 with cert-manager v1.21.3 (opt-in, `E2E_CERT_MANAGER=1`): a CA
  issuer and the Gateway annotation, chained certificates, RSA and ECDSA keys, renewal
  served in under a second, and ACME HTTP-01 (Pebble) through a Pingap port 80 listener.
- Fuzz targets (`fuzz/`, cargo-afl with AFL++) for the controller config and etcd URL, the edits of
  stored upstreams and servers, and the Gateway API translation, rendering and planning. The
  `fuzz` workflow runs them weekly.
- Error messages no longer show the etcd password when the URL writes the parameter name
  encoded (`pass%77ord=...`): the name is now decoded before it is redacted, as the parser
  reads it.

## 0.16.21 (2026-11-08)

- kubectl plugin on Windows: `kubectl-pingap_<version>_windows_amd64.zip` in every release and
  in the krew manifest.
- The plugin is its own crate, `kubectl-pingap`, published on crates.io: `cargo install
  kubectl-pingap` builds it without `protoc`, and `cargo binstall kubectl-pingap` downloads the
  release binary. The repository is now a Cargo workspace; the controller is not published.
- `kubectl pingap --help` shows the command as `kubectl pingap`.

## 0.16.20 (2026-11-08)

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

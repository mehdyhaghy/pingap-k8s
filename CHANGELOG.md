# Changelog

## Unreleased

Version 0.16.10. Versions now follow Pingap: the Pingap version the release is tested with plus
one digit for our own releases (Pingap 0.16.1: 0.16.10 to 0.16.19). `make version-check` and the
release workflow enforce it.

Gateway API provider (off by default, `gateway.enabled`):
- Gateway API v1.6.3 Standard channel: GatewayClasses with the controller's `controllerName`,
  their Gateways and the attached HTTPRoutes become Pingap locations, plugins, certificates and
  upstreams (named `k8s-...`). ReferenceGrants allow cross-namespace backends and certificates.
- Listeners map onto existing Pingap servers by port and TLS. Locations get explicit weights for
  Gateway API precedence, segment-aware prefixes, and exact header and query matches. Invalid
  backends answer 500, backends without endpoints 503.
- Filters: request and response header modifiers, redirects (host, scheme, port, path, all
  status codes) and URL rewrites (host and path).
- Weighted backendRefs: each Service gets its share of the weight, split over its addresses, with
  the smallest exact integer weights.
- Status for GatewayClasses (with `supportedFeatures`), Gateways and HTTPRoutes, written by the
  leader; other controllers' route status entries are kept.
- Chart: `gateway.*` values, a GatewayClass, and a ClusterRole that only reads the Gateway API
  resources and namespaces and patches their status. Secrets stay namespaced. `make
  exposure-check` allows only that ClusterRole.

Changes:
- `target = "service"` writes the Service's ClusterIP instead of its DNS name (a headless Service
  keeps the name). A Service deleted and recreated with a new ClusterIP is now followed within the
  debounce. With the name, Pingap kept the old address for its DNS TTL (30 s with the default
  CoreDNS config), whatever `update_frequency` said: on kind about 10 s of 503s after a recreate.
- Upstreams the controller creates with a hostname in `addrs` get `update_frequency = "5s"`.

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
  pods, and the admin port is closed except to `pingap.networkPolicy.adminFrom`. Port-forward
  keeps working. Extra listener ports go in `pingap.networkPolicy.extraPorts`.
- The metrics/health and `/status` servers serve at most 64 connections at once, and close a
  connection after 10 s without traffic or 60 s in total.
- EndpointSlice addresses that are loopback, link-local, unspecified, broadcast or multicast are
  skipped.
- A password in the etcd URL no longer appears in URL errors or in the chart's install notes.
- A plaintext etcd connection logs a warning, and the install notes warn about it too.
- The bundled etcd accepts peer certificates only with the server CN (`--peer-cert-allowed-cn`;
  `etcd.tls.peerAllowedCN` for your own certificates).
- An OTLP endpoint given as a public IP address logs a warning.
- `kubectl pingap admin` matches the chart's Pingap Service by all three of its labels, and stops
  when more than one Service matches.

Tests and CI:
- The `ci` workflow no longer runs on every push, pull request or weekly: it is started by hand
  (`gh workflow run ci`, optionally with `pingap-latest` for the newest Pingap image), and the
  `release` workflow runs all of it first and publishes nothing unless it passes.
- New CI job `secrets` runs gitleaks over the whole git history and the working tree.
- Workflow checkouts no longer keep the GitHub token in `.git/config`
  (`persist-credentials: false`).
- `SECURITY.md` (private vulnerability reporting) and `CODEOWNERS`; more credential, dump and
  snapshot patterns in `.gitignore`, and credentials kept out of the chart package
  (`.helmignore`).
- Kubernetes Events publishing tested against a mock API server: the request paths and bodies,
  series updates, the spam filter and a forbidden namespace.
- e2e test 04 records why a failed request failed (curl exit code, DNS and connect time, address)
  and prints the Pingap log when it fails. That showed its dual-stack flake was a cluster DNS
  lookup lost in kind (a 5 s resolver timeout), never reaching Pingap. The harness now reaches
  Pingap by its ClusterIP (curl `--resolve`, fortio `-resolve`): 15 of 15 local dual-stack runs
  passed, against 5 failures in 30 before.
- OTLP export test against an in-process receiver, and kubectl plugin tests against a fake
  `kubectl`, both in `cargo test`.
- New CI job `integration` runs the etcd integration tests and the kind watcher tests. Before
  this, CI skipped them.
- New CI job `dependencies` runs `cargo audit` (the release binary, checksum-checked, instead of
  compiling it).
- CI checks the `kind` download against its sha256.

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

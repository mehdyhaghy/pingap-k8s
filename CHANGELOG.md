# Changelog

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

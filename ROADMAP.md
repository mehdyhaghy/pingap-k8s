# Roadmap

Where pingap-k8s is going. Dates are targets, not promises; the [CHANGELOG](CHANGELOG.md) says
what has shipped. Ideas and priorities are discussed in issues: open one to propose a change.

## Now (late 2026)

- Submit the Gateway API conformance report so the project is listed as a conformant
  implementation.
- The OpenSSF Best Practices badge.

## Next (first half of 2027)

- A check in the etcd write transaction that the writer still holds the leader key, so a
  leader that was paused cannot write once more after it resumes.
- Header-based session persistence and session timeouts, if Pingap gains them.
- Turn session persistence on by default once `sessionPersistence` reaches the Standard
  channel, and program generated servers without a Pingap restart if Pingap can bind new
  servers live.

## Done

- Soak test with load and pod, leader, etcd and node failures ([docs/soak.md](docs/soak.md)).
- TLS certificates issued by cert-manager for Gateways, documented and tested end to end.
- Pingap servers generated for Gateway listeners (`[gateway.generate_servers]`).
- Cookie session persistence (Experimental-channel `sessionPersistence`).
- Fuzzing of the Gateway API translation and the config parsing (`fuzz/`, weekly).
- Each new Gateway API and Pingap release is flagged by the weekly `upstream-releases`
  workflow and picked up within a month.

## Later

- TLSRoute and TCPRoute if Pingap gains L4 proxying.
- A 1.0 release once the configuration, the chart values and the Gateway API support are
  stable and the project has production adopters.

## Not planned

- The Ingress API: it is frozen, and Gateway API is its successor.
- Storage targets other than etcd.

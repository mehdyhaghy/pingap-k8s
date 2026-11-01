# Roadmap

Where pingap-k8s is going. Dates are targets, not promises; the [CHANGELOG](CHANGELOG.md) says
what has shipped. Ideas and priorities are discussed in issues: open one to propose a change.

## Now (late 2026)

- Follow the next Pingap release: `strict_sni` answers `421 Misdirected Request` when a
  request's `Host` does not match the TLS SNI
  ([vicanso/pingap#279](https://github.com/vicanso/pingap/issues/279)). Turn it on for Gateway
  TLS listeners and claim `GatewayHTTPSListenerDetectMisdirectedRequests`.
- Submit the Gateway API conformance report so the project is listed as a conformant
  implementation.
- A long-running soak test (load plus pod, node, leader and etcd failures) to back the
  production checklist with numbers.
- Document and test TLS certificates issued by cert-manager for Gateways.
- The OpenSSF Best Practices badge.

## Next (first half of 2027)

- Generate the Pingap servers for Gateway listeners, so a listener does not need a server
  configured by hand.
- Session persistence and the remaining Extended HTTPRoute features Pingap can support.
- Fuzzing of the Gateway API translation and the config parsing.
- Track each new Gateway API release within a month.

## Later

- TLSRoute and TCPRoute if Pingap gains L4 proxying.
- A 1.0 release once the configuration, the chart values and the Gateway API support are
  stable and the project has production adopters.

## Not planned

- The Ingress API: it is frozen, and Gateway API is its successor.
- Storage targets other than etcd.

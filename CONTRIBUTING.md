# Contributing

## Development

```sh
make fmt           # cargo fmt
make check         # fmt check, clippy -D warnings, tests, helm lint/template, exposure check, version check, manifests check
make manifests     # regenerate deploy/pingap-k8s.yaml from the chart
make docker        # build the image for the local platform
make docker-multiarch PLATFORMS=linux/amd64,linux/arm64
STATUS=1 scripts/smoke-local.sh          # local binary + local etcd + kind, including /status
make docker && scripts/plugin-kind.sh    # kubectl plugin against the chart on kind
```

Building needs `protoc` and the protobuf well-known types (Debian/Ubuntu:
`protobuf-compiler libprotobuf-dev`), used by `etcd-client`. The Docker build cross-compiles in the
builder stage, so a multi-arch build needs no emulation.

## Tests

A change that adds or changes behavior comes with tests for it: unit tests, plus an e2e test
when it touches what Pingap or the cluster sees. A bug fix comes with a test that fails without
the fix. `make check` must pass before a pull request is merged.

| Command | What it covers | CI job |
| --- | --- | --- |
| `cargo test` | unit tests (including Kubernetes Events against a mock API server), OTLP export to an in-process receiver, the kubectl plugin against a fake `kubectl` | `rust` |
| `scripts/test-etcd.sh` | `tests/etcd_*.rs` against a throwaway etcd on 127.0.0.1: CAS writes, owned-key watch, compaction, leader election, the Gateway writer | `integration` |
| `scripts/test-kind.sh` | `tests/watch_kind.rs`: the Service and EndpointSlice watchers on a throwaway kind cluster | `integration` |
| `scripts/e2e-run.sh` | the end-to-end suite with the chart, Pingap, etcd and the Gateway API CRDs on kind | `e2e` |
| `scripts/conformance.sh` | the Gateway API conformance suite on its own kind cluster (`CONF_PROFILES`, default `GATEWAY-HTTP,GATEWAY-GRPC`); report in `target/conformance/report.yaml` | no |
| `scripts/otel-local.sh` | the binary exporting to a real OpenTelemetry Collector (docker) | no |
| `scripts/bench-local.sh` | the benchmarks of [docs/benchmarks.md](docs/benchmarks.md) | no |
| `cargo audit` | RustSec advisories for `Cargo.lock` | `dependencies` |
| `make deny` (`cargo deny check licenses sources`) | dependency licenses against the CNCF allowlist and crate sources, per `deny.toml` | `dependencies` |
| `gitleaks git . && gitleaks dir .` | secrets in the git history and the working tree (`.gitleaks.toml`) | `secrets` |

The `ci` workflow does not run on every push: run `make check` and the scripts above locally,
and start it by hand for a clean run (`gh workflow run ci`, or Actions > ci > Run workflow; add
`-f pingap-latest=true` to also test Pingap `latest`). The e2e job runs on kindest/node v1.37.0,
IPv4 and dual-stack. The `release` workflow runs all of it before it publishes
anything.

Line coverage: `cargo llvm-cov --lib --bins` (add `--test 'etcd_*'` with `PINGAP_K8S_TEST_ETCD`
set to include the etcd tests).

## Pull requests and sign-off

Open an issue first for larger changes, so the design can be agreed before the work. Keep a
pull request to one topic, with tests (see [Tests](#tests)) and docs for what it changes.
Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md); how decisions are made
is in [GOVERNANCE.md](GOVERNANCE.md).

Every commit must be signed off under the [Developer Certificate of Origin](https://developercertificate.org/)
(DCO): it certifies that you wrote the change or have the right to submit it under the
project's license. Add the line with `git commit -s`:

```
Signed-off-by: Your Name <you@example.com>
```

## Dependencies

- Rust crates are pinned in `Cargo.lock` and built with `--locked`. Add a crate only when the
  standard library or an existing dependency cannot do the job, prefer well-maintained crates,
  and turn off default features you do not use. Licenses must be Apache-2.0 or on the
  [CNCF allowlist](https://github.com/cncf/foundation/blob/main/policies-guidance/allowed-third-party-license-policy.md);
  `deny.toml` enforces it and lists the reviewed exceptions.
- `cargo audit` checks `Cargo.lock` against the RustSec advisories in CI and before every
  release; a vulnerable crate is updated (or replaced) before the next release, and in a patch
  release when the controller is affected.
- Base images are pinned by tag and digest, GitHub Actions by commit SHA. Dependabot proposes
  updates for both weekly; maintainers update the Rust crates by hand (`cargo update`) and
  review the changes.

## Releasing

Versions follow Pingap: a release is the Pingap version it is tested with plus one more digit,
our own release number for that Pingap version. Releases tested with Pingap 0.16.1 are `0.16.10`,
`0.16.11`, ... up to `0.16.19`; the first release for Pingap 0.16.2 is `0.16.20`. The Pingap
version is `pingap.image.tag` in the chart values (the quickstart default). `make version-check`
(part of `make check` and CI) checks this.

1. Bump the version in three places to the same value: `version` in `Cargo.toml`, and `version`
   and `appVersion` in `charts/pingap-k8s/Chart.yaml`. For a new Pingap version, also set
   `pingap.image.tag` in `charts/pingap-k8s/values.yaml` and start again at digit 0. Run
   `cargo check` to update `Cargo.lock`, then `make manifests` and `make check`. Move the
   CHANGELOG `Unreleased` entries under the new version.
2. Commit, merge to `main`, then tag and push the tag:

   ```sh
   git tag -a v<version> -m "v<version>"
   git push origin v<version>
   ```

The `release` workflow fails if the tag does not match both files or the version does not follow
the Pingap version. Otherwise it runs the whole `ci` workflow and publishes nothing unless it
passes. Then it pushes the `linux/amd64` + `linux/arm64` image to
`ghcr.io/mehdyhaghy/pingap-k8s:<version>` and `:latest` (with SBOM and provenance attestations),
pushes the chart to `oci://ghcr.io/mehdyhaghy/charts/pingap-k8s`, and creates a GitHub Release
with generated notes, the chart package, `pingap-k8s.yaml` (plain manifests pinned to the release
image) and the kubectl plugin archives and krew manifest. It signs the image, the chart and the
release files with cosign keyless signing; Sigstore's public transparency log records each
signature with the repository name, workflow path and tag. To test the pipeline without
publishing, run the workflow manually with `dry-run` checked (the default).

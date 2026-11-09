# kubectl-pingap

`kubectl pingap` shows what the [pingap-k8s](https://github.com/mehdyhaghy/pingap-k8s)
controller sees and wrote to [Pingap](https://github.com/vicanso/pingap), per upstream. It
reads the controller's read-only `/status` endpoint through `kubectl port-forward`.

```text
kubectl pingap status -n pingap-k8s      # leader, and one row per upstream
kubectl pingap explain web -n pingap-k8s # warnings, watch state, why an upstream has no endpoints
kubectl pingap admin -n pingap-k8s       # port-forward the quickstart Pingap admin UI
```

The controller serves `/status` only when the chart value `status.enabled` is `true`. Using the
plugin needs RBAC on `pods/portforward` in the controller's namespace. The plugin runs `kubectl`
underneath, so `--context`, `--kubeconfig` and your credentials apply.

## Install

Prebuilt binaries for Linux, macOS (amd64 and arm64) and Windows (amd64) are attached to every
[release](https://github.com/mehdyhaghy/pingap-k8s/releases), with checksums and cosign
signatures. Put `kubectl-pingap` on your `PATH`, or:

```sh
kubectl krew install --manifest-url=https://github.com/mehdyhaghy/pingap-k8s/releases/latest/download/pingap.yaml
cargo binstall kubectl-pingap      # the release binary, with cargo-binstall
cargo install kubectl-pingap       # build from source (Rust 1.89 or newer)
```

The plugin version matches the controller version it ships with.

## License

Apache-2.0, see [LICENSE](LICENSE).

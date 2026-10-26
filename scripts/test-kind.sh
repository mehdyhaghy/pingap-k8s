#!/usr/bin/env bash
# Runs the kind-backed integration tests (tests/watch_kind.rs) against a
# throwaway kind cluster. The cluster is always deleted on exit.
#
# Usage: scripts/test-kind.sh [extra cargo test args]
# Env:   KIND_CLUSTER (default pk8s-t07), KIND_NODE_IMAGE (optional node image)
set -euo pipefail

CLUSTER="${KIND_CLUSTER:-pk8s-t07}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d)"
KUBECONFIG_FILE="$WORKDIR/kubeconfig"

cleanup() {
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT INT TERM

for tool in kind docker cargo; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 1; }
done

# A cluster left over from an interrupted run would make `create` fail.
kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true

create_args=(--name "$CLUSTER" --kubeconfig "$KUBECONFIG_FILE" --wait 120s)
if [[ -n "${KIND_NODE_IMAGE:-}" ]]; then
  create_args+=(--image "$KIND_NODE_IMAGE")
fi
kind create cluster "${create_args[@]}"

# Use the pause image the node already has, so the test pods need no pull.
PAUSE_IMAGE="$(docker exec "$CLUSTER-control-plane" crictl images \
  | awk '$1 == "registry.k8s.io/pause" { print $1 ":" $2; exit }')"
if [[ -z "$PAUSE_IMAGE" ]]; then
  PAUSE_IMAGE="registry.k8s.io/pause:3.10"
fi
echo "test pod image: $PAUSE_IMAGE"

cd "$ROOT"
PINGAP_K8S_TEST_KUBECONFIG="$KUBECONFIG_FILE" \
PINGAP_K8S_TEST_IMAGE="$PAUSE_IMAGE" \
  cargo test --test watch_kind "$@" -- --nocapture

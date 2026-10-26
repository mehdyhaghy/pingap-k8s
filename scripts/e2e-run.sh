#!/usr/bin/env bash
# Full end-to-end run: scripts/e2e-up.sh, the e2e test binary, then
# scripts/e2e-down.sh (always, unless E2E_KEEP=1).
#
# Usage: scripts/e2e-run.sh [extra args for the test binary, e.g. a test name filter]
# Env:   E2E_KEEP=1 keeps the cluster for debugging (tear down with scripts/e2e-down.sh);
#        everything scripts/e2e-up.sh reads (E2E_CLUSTER, KIND_NODE_IMAGE, ...).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cleanup() {
  local status=$?
  if [[ "${E2E_KEEP:-0}" == "1" ]]; then
    echo "[e2e-run] E2E_KEEP=1: keeping cluster ${E2E_CLUSTER:-pk8s-e2e}; state in $ROOT/target/e2e" >&2
    echo "[e2e-run]   export KUBECONFIG=$ROOT/target/e2e/kubeconfig; scripts/e2e-down.sh when done" >&2
  else
    "$ROOT/scripts/e2e-down.sh" || true
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

"$ROOT/scripts/e2e-up.sh"

cd "$ROOT"
PINGAP_K8S_E2E=1 cargo test --test e2e -- --test-threads=1 --nocapture "$@"

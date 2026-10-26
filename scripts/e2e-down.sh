#!/usr/bin/env bash
# Deletes the end-to-end kind cluster and the state written by scripts/e2e-up.sh.
#
# Env: E2E_CLUSTER (default pk8s-e2e)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER="${E2E_CLUSTER:-pk8s-e2e}"

if command -v kind >/dev/null && kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "[e2e-down] deleting kind cluster $CLUSTER" >&2
  kind delete cluster --name "$CLUSTER" >/dev/null
fi
rm -rf "$ROOT/target/e2e"

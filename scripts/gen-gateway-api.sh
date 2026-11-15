#!/usr/bin/env bash
# Regenerates src/gateway/api/*.rs from the Standard-channel CRDs of a Gateway API release.
#
#   scripts/gen-gateway-api.sh [version]     (default: the version in GATEWAY_API_VERSION below)
#
# Needs kopium (cargo install kopium --version 0.24.1 --locked) and yq. The generated files
# are committed; bump GATEWAY_API_VERSION, run this, review the diff and update the
# e2e manifests (tests/e2e) to the same release.
#
# On top of the Standard channel, the rules of HTTPRoutes and GRPCRoutes get the
# Experimental-channel field `sessionPersistence`, typed by the hand-written
# src/gateway/api/experimental.rs (check it against the experimental CRDs of a new
# release).
set -euo pipefail

GATEWAY_API_VERSION="${1:-v1.7.0}"
KOPIUM="${KOPIUM:-kopium}"
RESOURCES=(gatewayclasses gateways httproutes grpcroutes referencegrants backendtlspolicies listenersets)

root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/src/gateway/api"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

curl -fsSL -o "$work/standard-install.yaml" \
  "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/standard-install.yaml"

for r in "${RESOURCES[@]}"; do
  yq "select(.kind == \"CustomResourceDefinition\" and .metadata.name == \"$r.gateway.networking.k8s.io\")" \
    "$work/standard-install.yaml" >"$work/$r.yaml"
  {
    echo "// Generated from the Gateway API ${GATEWAY_API_VERSION} Standard-channel CRD $r.gateway.networking.k8s.io"
    echo "// by scripts/gen-gateway-api.sh. Do not edit."
    echo
    echo "use kube::CustomResource;"
    echo "use serde::{Deserialize, Serialize};"
    "$KOPIUM" --schema=disabled --derive=Default --smart-derive-elision --derive=PartialEq --api-version=v1 \
      --hide-prelude -f "$work/$r.yaml" | sed '1,3d' >"$work/$r.rs"
    if grep -q 'BTreeMap' "$work/$r.rs"; then echo "use std::collections::BTreeMap;"; fi
    if grep -q 'Condition' "$work/$r.rs"; then
      echo "use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;"
    fi
    echo
    cat "$work/$r.rs"
  } >"$out/$r.rs"
done

# The Experimental-channel rule field `sessionPersistence` (see the header).
for pair in httproutes:HttpRouteRules grpcroutes:GrpcRouteRules; do
  file="$out/${pair%%:*}.rs"
  struct="${pair#*:}"
  grep -q "^pub struct $struct {" "$file" || { echo "no struct $struct in $file" >&2; exit 1; }
  awk -v s="pub struct $struct {" '
    { print }
    $0 == s {
      print "    /// Experimental channel: see `super::experimental`."
      print "    #[serde(default, skip_serializing_if = \"Option::is_none\", rename = \"sessionPersistence\")]"
      print "    pub session_persistence: Option<super::experimental::SessionPersistence>,"
    }' "$file" >"$work/patched.rs"
  mv "$work/patched.rs" "$file"
done
rustfmt --edition 2024 "$out"/*.rs
echo "generated ${RESOURCES[*]} from ${GATEWAY_API_VERSION} into $out"

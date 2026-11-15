#!/usr/bin/env bash
# Compares the Gateway API and Pingap versions this repository is built and tested
# against with their latest upstream releases, so a new release is picked up within
# the month (ROADMAP). Exits 1 when either is behind and prints what to do; with
# GITHUB_STEP_SUMMARY set (GitHub Actions), also writes it to the job summary.
#
#   scripts/check-upstream-releases.sh
#
# Needs curl and jq. GITHUB_TOKEN (or GH_TOKEN), when set, is sent to the GitHub API
# to avoid its anonymous rate limit.
set -euo pipefail

cd "$(dirname "$0")/.."

# The Gateway API release of the generated types, the e2e CRDs and the docs.
gateway_api=$(sed -n 's/^pub const GATEWAY_API_VERSION: &str = "\(v[0-9.]*\)";$/\1/p' src/gateway/watch.rs)
# The Pingap version the release is tested with (the quickstart default).
pingap=$(awk '/^[^ #]/{p=($1=="pingap:")} p && $1=="tag:"{gsub(/"/,"",$2); print $2; exit}' charts/pingap-k8s/values.yaml)
[ -n "$gateway_api" ] || { echo "no GATEWAY_API_VERSION in src/gateway/watch.rs" >&2; exit 2; }
[ -n "$pingap" ] || { echo "no pingap.image.tag in charts/pingap-k8s/values.yaml" >&2; exit 2; }

token="${GITHUB_TOKEN:-${GH_TOKEN:-}}"
latest() {
  local args=(-fsSL -H "Accept: application/vnd.github+json")
  [ -n "$token" ] && args+=(-H "Authorization: Bearer $token")
  curl "${args[@]}" "https://api.github.com/repos/$1/releases/latest" | jq -r .tag_name
}
latest_gateway_api=$(latest kubernetes-sigs/gateway-api)
latest_pingap=$(latest vicanso/pingap)
latest_pingap="${latest_pingap#v}"

report=""
note() { report+="$1"$'\n'; }
behind=0
if [ "$latest_gateway_api" != "$gateway_api" ]; then
  behind=1
  note "- Gateway API $latest_gateway_api is out (built on $gateway_api): run scripts/gen-gateway-api.sh $latest_gateway_api, check src/gateway/api/experimental.rs against its experimental-install.yaml, bump GATEWAY_API_VERSION and the CRD digests in scripts/e2e-up.sh and scripts/conformance.sh, rerun the conformance suite and update the docs."
else
  note "- Gateway API $gateway_api is the latest release."
fi
if [ "$latest_pingap" != "$pingap" ]; then
  behind=1
  note "- Pingap $latest_pingap is out (tested with $pingap): set pingap.image.tag, start the release digit at 0 (CONTRIBUTING.md, Releasing) and run the e2e and conformance suites."
else
  note "- Pingap $pingap is the latest release."
fi

printf '%s' "$report"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  { echo "## Upstream releases"; echo; printf '%s' "$report"; } >>"$GITHUB_STEP_SUMMARY"
fi
exit "$behind"

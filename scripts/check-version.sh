#!/usr/bin/env bash
# Checks that the release version follows the Pingap version: the version is the
# Pingap version the release is tested with plus one more digit, our own release
# number for that Pingap version (0 to 9). Pingap 0.16.2 gives 0.16.20, 0.16.21, ...
# The Pingap version is pingap.image.tag in the chart values. Also checks that
# Cargo.toml, Chart.yaml version and Chart.yaml appVersion agree.
# Prints the version on success.
set -euo pipefail

cd "$(dirname "$0")/.."

CHART=charts/pingap-k8s

cargo_version=$(awk '/^\[package\]/{p=1;next} /^\[/{p=0} p && /^version *=/{gsub(/"/,"",$3); print $3; exit}' Cargo.toml)
chart_version=$(sed -n 's/^version: *//p' "$CHART/Chart.yaml" | tr -d "\"' ")
app_version=$(sed -n 's/^appVersion: *//p' "$CHART/Chart.yaml" | tr -d "\"' ")
# The first `tag:` under the top-level `pingap:` key.
pingap_version=$(awk '/^[^ #]/{p=($1=="pingap:")} p && $1=="tag:"{gsub(/"/,"",$2); print $2; exit}' "$CHART/values.yaml")

fail=0
err() { echo "version check: $*" >&2; fail=1; }

[ -n "$cargo_version" ] || err "no version in Cargo.toml [package]"
[ -n "$pingap_version" ] || err "no pingap.image.tag in $CHART/values.yaml"
[ "$chart_version" = "$cargo_version" ] || err "$CHART/Chart.yaml version $chart_version does not match Cargo.toml $cargo_version"
[ "$app_version" = "$cargo_version" ] || err "$CHART/Chart.yaml appVersion $app_version does not match Cargo.toml $cargo_version"

if [ -n "$cargo_version" ] && [ -n "$pingap_version" ]; then
    if ! [[ "$pingap_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        err "pingap.image.tag $pingap_version is not a Pingap release version (x.y.z)"
    elif ! [[ "$cargo_version" =~ ^[0-9]+\.[0-9]+\.[0-9]+[0-9]$ ]]; then
        err "version $cargo_version is not <Pingap version><digit>, for example ${pingap_version}0"
    elif [ "${cargo_version%?}" != "$pingap_version" ]; then
        err "version $cargo_version does not follow Pingap $pingap_version (expected ${pingap_version}0 to ${pingap_version}9)"
    fi
fi

[ "$fail" = 0 ] || exit 1
echo "$cargo_version"

#!/usr/bin/env bash
# Runs the etcd integration tests (tests/etcd_*.rs) against a throwaway local etcd.
# etcd listens on 127.0.0.1 only, on free ports, with a temporary data dir that is
# removed afterwards. Extra arguments are passed to `cargo test`.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v etcd >/dev/null 2>&1; then
    echo "etcd binary not found (macOS: brew install etcd)" >&2
    exit 1
fi

free_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

client_port=$(free_port)
peer_port=$(free_port)
while [ "$peer_port" = "$client_port" ]; do
    peer_port=$(free_port)
done

data_dir=$(mktemp -d "${TMPDIR:-/tmp}/pingap-k8s-etcd.XXXXXX")
etcd_pid=""

cleanup() {
    if [ -n "$etcd_pid" ] && kill -0 "$etcd_pid" 2>/dev/null; then
        kill "$etcd_pid" 2>/dev/null || true
        wait "$etcd_pid" 2>/dev/null || true
    fi
    rm -rf "$data_dir"
}
trap cleanup EXIT INT TERM

case "$(uname -m)" in
    arm64 | aarch64) export ETCD_UNSUPPORTED_ARCH=arm64 ;;
esac

client_url="http://127.0.0.1:${client_port}"
peer_url="http://127.0.0.1:${peer_port}"

etcd \
    --name pingap-k8s-test \
    --data-dir "$data_dir/data" \
    --listen-client-urls "$client_url" \
    --advertise-client-urls "$client_url" \
    --listen-peer-urls "$peer_url" \
    --initial-advertise-peer-urls "$peer_url" \
    --initial-cluster "pingap-k8s-test=${peer_url}" \
    --log-level error \
    >"$data_dir/etcd.log" 2>&1 &
etcd_pid=$!

ready=""
for _ in $(seq 1 100); do
    if curl -fsS "${client_url}/health" >/dev/null 2>&1; then
        ready=1
        break
    fi
    if ! kill -0 "$etcd_pid" 2>/dev/null; then
        break
    fi
    sleep 0.1
done
if [ -z "$ready" ]; then
    echo "etcd did not become ready; log:" >&2
    cat "$data_dir/etcd.log" >&2
    exit 1
fi

echo "etcd ready at ${client_url}"
PINGAP_K8S_TEST_ETCD="$client_url" cargo test --test 'etcd_*' "$@"

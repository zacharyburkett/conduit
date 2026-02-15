#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: capture_loadgen_profile.sh <build-dir> [output-dir]

Runs broker + loadgen profile pass and writes:
- summary.txt (loadgen + broker final summary lines)
- loadgen.log
- broker.log
EOF
}

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
    usage
    exit 1
fi

build_dir="$1"
output_dir="${2:-$build_dir/loadgen-profile}"
broker_bin="$build_dir/conduit_broker"
loadgen_bin="$build_dir/conduit_loadgen"

if [ ! -x "$broker_bin" ]; then
    echo "Broker binary not found: $broker_bin" >&2
    exit 1
fi
if [ ! -x "$loadgen_bin" ]; then
    echo "Loadgen binary not found: $loadgen_bin" >&2
    exit 1
fi

mkdir -p "$output_dir"

tmp_dir="$(mktemp -d "/tmp/conduit-loadgen-profile.XXXXXX")"
socket_path="$tmp_dir/b.sock"
broker_log="$output_dir/broker.log"
loadgen_log="$output_dir/loadgen.log"
summary_file="$output_dir/summary.txt"
broker_pid=""

cleanup() {
    if [ -n "$broker_pid" ]; then
        kill "$broker_pid" 2>/dev/null || true
        wait "$broker_pid" 2>/dev/null || true
    fi
    rm -rf "$tmp_dir"
}
trap cleanup EXIT INT TERM

"$broker_bin" \
    --socket "$socket_path" \
    --run-ms 30000 \
    --metrics-interval-ms 0 \
    >"$broker_log" 2>&1 &
broker_pid="$!"

for _ in $(seq 1 2000); do
    if [ -S "$socket_path" ]; then
        break
    fi
    sleep 0.01
done

if [ ! -S "$socket_path" ]; then
    echo "Broker socket was not created: $socket_path" >&2
    exit 1
fi

"$loadgen_bin" \
    --socket "$socket_path" \
    --events 4000 \
    --requests 800 \
    --payload-size 64 \
    --max-duration-ms 20000 \
    --request-timeout-ns 2000000000 \
    --max-queued-messages 4096 \
    --max-inflight-requests 512 \
    >"$loadgen_log" 2>&1

kill "$broker_pid" 2>/dev/null || true
wait "$broker_pid" 2>/dev/null || true
broker_pid=""

loadgen_summary="$(grep -F "[loadgen] done " "$loadgen_log" | tail -n 1 || true)"
broker_summary="$(grep -F "[broker] final " "$broker_log" | tail -n 1 || true)"

if [ -z "$loadgen_summary" ]; then
    echo "Loadgen summary line missing from $loadgen_log" >&2
    exit 1
fi
if [ -z "$broker_summary" ]; then
    echo "Broker final summary line missing from $broker_log" >&2
    exit 1
fi

{
    echo "$loadgen_summary"
    echo "$broker_summary"
} >"$summary_file"

echo "Loadgen profile summary written to $summary_file"

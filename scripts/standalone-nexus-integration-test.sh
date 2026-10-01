#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
endpoint_name="odin-standalone-${RANDOM}-$(date +%s)"
worker_pid=""
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-standalone-worker.XXXXXX")"
client_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-standalone-client.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  temporal operator nexus endpoint delete \
    --name "$endpoint_name" \
    --command-timeout 5s \
    --output none >/dev/null 2>&1 || true
  rm -f "$worker_log" "$client_log"
}
trap cleanup EXIT INT TERM

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
fi

probe_output="$(TEMPORAL_CORE_STANDALONE_NEXUS_PROBE=1 \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-core-connect.sh")"
echo "$probe_output"
if ! grep -q 'Standalone Nexus operation RPC is recognized' <<<"$probe_output"; then
  echo "The connected Temporal Service does not expose standalone Nexus operation RPCs." >&2
  exit 1
fi

temporal operator nexus endpoint create \
  --name "$endpoint_name" \
  --target-namespace default \
  --target-task-queue odin-hello-nexus \
  --description 'Temporal SDK for Odin standalone Nexus integration' \
  --command-timeout 10s \
  --output none

run_worker() {
  local mode="$1"
  : >"$worker_log"
  if [[ "$mode" == "cancel" ]]; then
    TEMPORAL_NEXUS_STANDALONE_CANCEL=1 \
      TEMPORAL_ODIN_SKIP_BUILD=1 \
      "$repo_root/scripts/run-hello-nexus.sh" >"$worker_log" 2>&1 &
  elif [[ "$mode" == "lifecycle" ]]; then
    TEMPORAL_NEXUS_STANDALONE_LIFECYCLE=1 \
      TEMPORAL_ODIN_SKIP_BUILD=1 \
      "$repo_root/scripts/run-hello-nexus.sh" >"$worker_log" 2>&1 &
  else
    TEMPORAL_ODIN_SKIP_BUILD=1 \
      "$repo_root/scripts/run-hello-nexus.sh" >"$worker_log" 2>&1 &
  fi
  worker_pid=$!
  for _ in {1..30}; do
    if grep -q 'Odin Nexus Worker is polling' "$worker_log" 2>/dev/null; then
      return
    fi
    if ! kill -0 "$worker_pid" 2>/dev/null; then
      echo "Standalone Nexus Worker exited before polling" >&2
      cat "$worker_log" >&2
      exit 1
    fi
    sleep 0.1
  done
  echo "Standalone Nexus Worker did not begin polling" >&2
  exit 1
}

run_worker sync
NEXUS_ENDPOINT="$endpoint_name" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-standalone-nexus-client.sh" >"$client_log"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
cat "$client_log"
grep -q 'returned "Hello Odin" through the Odin Client' "$client_log"
grep -q 'one synchronous Nexus operation completed' "$worker_log"

run_worker cancel
NEXUS_ENDPOINT="$endpoint_name" \
  TEMPORAL_NEXUS_STANDALONE_CANCEL=1 \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-standalone-nexus-client.sh" >"$client_log"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
cat "$client_log"
grep -q 'accepted cancellation through the Odin Client' "$client_log"
grep -q 'one asynchronous Nexus operation was canceled' "$worker_log"

run_worker lifecycle
NEXUS_ENDPOINT="$endpoint_name" \
  TEMPORAL_NEXUS_STANDALONE_LIFECYCLE=1 \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-standalone-nexus-client.sh" >"$client_log"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
cat "$client_log"
grep -q 'was described, listed, counted, terminated, and deleted through the Odin Client' "$client_log"
grep -q 'one asynchronous Nexus operation was started for lifecycle control' "$worker_log"

echo "Standalone Nexus start/poll/result/cancel/describe/list/count/terminate/delete integration passed."

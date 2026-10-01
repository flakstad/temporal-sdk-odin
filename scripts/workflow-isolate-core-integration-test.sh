#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
parent_binary="$repo_root/native/target/release/temporal-odin-workflow-isolate-core"
child_binary="$repo_root/native/target/release/temporal-odin-workflow-isolate"
server_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-isolate-server.XXXXXX")"

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log"
}
trap cleanup EXIT INT TERM

if ! temporal operator cluster health \
  --address localhost:7233 \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  temporal server start-dev --headless --log-level error >"$server_log" 2>&1 &
  server_pid=$!
  for _ in {1..30}; do
    if temporal operator cluster health \
      --address localhost:7233 \
      --client-connect-timeout 1s \
      --command-timeout 1s \
      --output none >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

if ! temporal operator cluster health \
  --address localhost:7233 \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  echo "Temporal CLI dev server did not become ready; see $server_log" >&2
  exit 1
fi

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
elif [[ ! -x "$parent_binary" || ! -x "$child_binary" ]]; then
  echo "Isolation binaries are missing; run ./scripts/build-examples.sh first" >&2
  exit 1
fi

unique_suffix="${RANDOM}-$(date +%s)"
first_mode="${TEMPORAL_ODIN_ISOLATE_FIRST_MODE:-crash}"
cache_mode="${TEMPORAL_ODIN_ISOLATE_CACHE_MODE:-zero}"
output="$(
  TEMPORAL_ODIN_TASK_QUEUE="odin-isolate-$unique_suffix" \
  TEMPORAL_ODIN_WORKFLOW_ID="odin-isolate-$unique_suffix" \
  TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND="$child_binary" \
  TEMPORAL_ODIN_ISOLATE_FIRST_MODE="$first_mode" \
  TEMPORAL_ODIN_ISOLATE_CACHE_MODE="$cache_mode" \
  "$parent_binary" 2>&1
)"
echo "$output"
workflow_log_count="$(grep -c 'isolated-executor workflow_log message="workflow_started"' <<<"$output" || true)"
if [[ "$workflow_log_count" != "2" ]]; then
  echo "Expected exactly two replay-suppressed executor Workflow logs; got $workflow_log_count" >&2
  exit 1
fi
workflow_metric_count="$(grep -c 'isolated-executor workflow_metric name=workflow_started value=1' <<<"$output" || true)"
if [[ "$workflow_metric_count" != "2" ]]; then
  echo "Expected exactly two replay-suppressed executor Workflow metrics; got $workflow_metric_count" >&2
  exit 1
fi
grep -q 'Core registry isolation completed' <<<"$output"
grep -q 'registry_types=2' <<<"$output"
grep -q 'greeting="Hello from an isolated Workflow, Odin"' <<<"$output"
grep -q 'farewell="Goodbye from the shared registry, Odin"' <<<"$output"
grep -q "first_mode=$first_mode" <<<"$output"
grep -q "cache_mode=$cache_mode" <<<"$output"
if [[ "$cache_mode" == "sticky" ]]; then
  grep -q 'generations=3' <<<"$output"
  grep -q 'evictions=3' <<<"$output"
  grep -q 'graceful_shutdowns=3' <<<"$output"
  grep -q 'manifest_handshakes=4' <<<"$output"
  grep -q 'incremental_activations=2' <<<"$output"
  grep -q 'timer_sequences=2' <<<"$output"
else
  grep -q 'generations=5' <<<"$output"
  grep -q 'evictions=5' <<<"$output"
  grep -q 'graceful_shutdowns=5' <<<"$output"
  grep -q 'manifest_handshakes=6' <<<"$output"
  grep -q 'incremental_activations=2' <<<"$output"
  grep -q 'timer_sequences=4' <<<"$output"
fi
grep -q 'manifest_mismatches=0' <<<"$output"
grep -q 'shutdown_timeouts=0' <<<"$output"
grep -q 'crash_loop_rejections=0' <<<"$output"
grep -q 'capacity_rejections=0' <<<"$output"
grep -q 'protocol_failures=0' <<<"$output"
grep -q 'missing_timer_sequences=0' <<<"$output"
if [[ "$first_mode" == "crash" ]]; then
  grep -q 'child_exits=1' <<<"$output"
  grep -q 'activation_timeouts=0' <<<"$output"
elif [[ "$first_mode" == "hang" ]]; then
  grep -q 'child_exits=0' <<<"$output"
  grep -q 'activation_timeouts=1' <<<"$output"
fi

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
parent_binary="$repo_root/native/target/release/temporal-odin-workflow-isolate-core"
child_binary="$repo_root/native/target/release/temporal-odin-workflow-isolate"
server_pid=""
worker_pid=""
workflow_id=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-isolate-nondeterminism-server.XXXXXX")"
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-isolate-nondeterminism-worker.XXXXXX")"
description_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-isolate-nondeterminism-description.XXXXXX")"
history_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-isolate-nondeterminism-history.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  if [[ -n "$workflow_id" ]]; then
    temporal workflow terminate \
      --workflow-id "$workflow_id" \
      --reason "isolated nondeterminism test cleanup" \
      --command-timeout 2s \
      --output none >/dev/null 2>&1 || true
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$worker_log" "$description_file" "$history_file"
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
task_queue="odin-isolate-nondeterminism-$unique_suffix"
workflow_id="odin-isolate-nondeterminism-$unique_suffix"

seed_output="$(
  TEMPORAL_ODIN_ISOLATE_SCENARIO=nondeterminism-seed \
  TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND="$child_binary" \
  TEMPORAL_ODIN_ISOLATE_CACHE_MODE=sticky \
  "$parent_binary" 2>&1
)"
echo "$seed_output"
grep -q 'Nondeterminism seed recorded' <<<"$seed_output"
grep -q 'timer_sequences=1' <<<"$seed_output"

temporal workflow show \
  --workflow-id "$workflow_id" \
  --command-timeout 10s \
  --output json >"$history_file"
grep -q 'EVENT_TYPE_TIMER_STARTED' "$history_file"

TEMPORAL_ODIN_ISOLATE_SCENARIO=nondeterminism-fail \
TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
TEMPORAL_ODIN_WORKFLOW_ID="$workflow_id" \
TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND="$child_binary" \
TEMPORAL_ODIN_ISOLATE_CACHE_MODE=sticky \
"$parent_binary" >"$worker_log" 2>&1 &
worker_pid=$!

for _ in {1..50}; do
  if grep -q 'Nondeterminism replay Worker is polling' "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Nondeterminism replay Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
grep -q 'Nondeterminism replay Worker is polling' "$worker_log"

temporal workflow signal \
  --workflow-id "$workflow_id" \
  --name force-replay \
  --input '"replay"' \
  --command-timeout 10s \
  --output none

for _ in {1..100}; do
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if kill -0 "$worker_pid" 2>/dev/null; then
  echo "Nondeterminism replay Worker did not stop after the terminal failure" >&2
  cat "$worker_log" >&2
  exit 1
fi
if ! wait "$worker_pid"; then
  echo "Nondeterminism replay Worker exited unsuccessfully" >&2
  cat "$worker_log" >&2
  worker_pid=""
  exit 1
fi
worker_pid=""
cat "$worker_log"
grep -q 'Core detected and converted isolated Workflow nondeterminism' "$worker_log"
grep -q 'nondeterminism_evictions=1' "$worker_log"

temporal workflow describe \
  --workflow-id "$workflow_id" \
  --command-timeout 10s \
  --output json >"$description_file"
temporal workflow show \
  --workflow-id "$workflow_id" \
  --command-timeout 10s \
  --output json >"$history_file"
grep -q '"status": "WORKFLOW_EXECUTION_STATUS_FAILED"' "$description_file"
grep -Eq 'Nondeterminism|TMPRL1100' "$history_file"
workflow_id=""

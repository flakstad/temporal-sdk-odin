#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
parent_pid=""
driver_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-activity-isolate-server.XXXXXX")"
parent_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-activity-isolate-parent.XXXXXX")"
driver_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-activity-isolate-driver.XXXXXX")"

cleanup() {
  for process_id in "$driver_pid" "$parent_pid"; do
    if [[ -n "$process_id" ]]; then
      kill "$process_id" 2>/dev/null || true
      wait "$process_id" 2>/dev/null || true
    fi
  done
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$parent_log" "$driver_log"
}
trap cleanup EXIT INT TERM

if ! temporal operator cluster health \
  --address localhost:7233 \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  temporal server start-dev --headless --log-level error >"$server_log" 2>&1 &
  server_pid=$!
  for _ in {1..40}; do
    if temporal operator cluster health \
      --address localhost:7233 \
      --client-connect-timeout 1s \
      --command-timeout 1s \
      --output none >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
fi

if ! temporal operator cluster health \
  --address localhost:7233 \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  echo "Temporal CLI dev server did not become healthy" >&2
  exit 1
fi

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
  cargo build --quiet \
    --manifest-path "$repo_root/native/Cargo.toml" \
    --package activity-test-driver
fi

binary="$repo_root/native/target/release/temporal-odin-activity-isolate"
task_queue="odin-activity-isolate-${RANDOM}-$(date +%s)"
workflow_id="odin-activity-isolate-${RANDOM}-$(date +%s)"

TEMPORAL_ODIN_TASK_QUEUE="$task_queue" "$binary" >"$parent_log" 2>&1 &
parent_pid=$!
for _ in {1..100}; do
  if grep -q "external Activity parent is polling task queue $task_queue" "$parent_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$parent_pid" 2>/dev/null; then
    echo "External Activity parent exited before polling" >&2
    cat "$parent_log" >&2
    exit 1
  fi
  sleep 0.1
done

ACTIVITY_PANIC_RECOVERY=1 \
  ACTIVITY_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_TASK_QUEUE="$task_queue" \
  cargo run --quiet \
    --manifest-path "$repo_root/native/Cargo.toml" \
    --package activity-test-driver >"$driver_log" 2>&1 &
driver_pid=$!

if ! wait "$driver_pid"; then
  driver_pid=""
  echo "Workflow did not recover through the isolated Activity executor" >&2
  cat "$driver_log" >&2
  cat "$parent_log" >&2
  exit 1
fi
driver_pid=""
if ! wait "$parent_pid"; then
  parent_pid=""
  echo "External Activity parent did not shut down cleanly" >&2
  cat "$parent_log" >&2
  exit 1
fi
parent_pid=""

grep -q 'isolated Activity executor forcing panic on attempt 1' "$parent_log"
grep -q 'isolated Activity panic was contained, heartbeat forwarded, and retry completed' "$parent_log"
grep -q 'Workflow survived the Activity process crash: Recovered on Activity attempt 2' "$driver_log"

cat "$parent_log"
cat "$driver_log"
echo "A per-invocation Odin Activity child crashed; the Core-owning parent stayed alive and completed Temporal retry attempt 2."

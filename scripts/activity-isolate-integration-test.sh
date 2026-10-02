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

cancel_task_queue="odin-activity-isolate-cancel-${RANDOM}-$(date +%s)"
cancel_workflow_id="odin-activity-isolate-cancel-${RANDOM}-$(date +%s)"
ACTIVITY_ISOLATE_CANCEL=cooperative \
  TEMPORAL_ODIN_TASK_QUEUE="$cancel_task_queue" \
  "$binary" >>"$parent_log" 2>&1 &
parent_pid=$!
for _ in {1..100}; do
  if grep -q "external Activity parent is polling task queue $cancel_task_queue" "$parent_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$parent_pid" 2>/dev/null; then
    echo "Cancellation Activity parent exited before polling" >&2
    cat "$parent_log" >&2
    exit 1
  fi
  sleep 0.1
done

ACTIVITY_ISOLATED_CANCELLATION=1 \
  ACTIVITY_WORKFLOW_ID="$cancel_workflow_id" \
  TEMPORAL_TASK_QUEUE="$cancel_task_queue" \
  cargo run --quiet \
    --manifest-path "$repo_root/native/Cargo.toml" \
    --package activity-test-driver >>"$driver_log" 2>&1 &
driver_pid=$!

if ! wait "$driver_pid"; then
  driver_pid=""
  echo "Workflow did not observe cooperative isolated Activity cancellation" >&2
  cat "$driver_log" >&2
  cat "$parent_log" >&2
  exit 1
fi
driver_pid=""
if ! wait "$parent_pid"; then
  parent_pid=""
  echo "Cancellation Activity parent did not shut down cleanly" >&2
  cat "$parent_log" >&2
  exit 1
fi
parent_pid=""

grep -q 'isolated Activity cancellation was delivered cooperatively without a forced kill' "$parent_log"
grep -q 'Workflow observed isolated Activity cancellation' "$driver_log"

forced_task_queue="odin-activity-isolate-forced-${RANDOM}-$(date +%s)"
forced_workflow_id="odin-activity-isolate-forced-${RANDOM}-$(date +%s)"
ACTIVITY_ISOLATE_CANCEL=forced \
  TEMPORAL_ODIN_TASK_QUEUE="$forced_task_queue" \
  "$binary" >>"$parent_log" 2>&1 &
parent_pid=$!
for _ in {1..100}; do
  if grep -q "external Activity parent is polling task queue $forced_task_queue" "$parent_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$parent_pid" 2>/dev/null; then
    echo "Forced-cancellation Activity parent exited before polling" >&2
    cat "$parent_log" >&2
    exit 1
  fi
  sleep 0.1
done

ACTIVITY_ISOLATED_CANCELLATION_UNCOOPERATIVE=1 \
  ACTIVITY_WORKFLOW_ID="$forced_workflow_id" \
  TEMPORAL_TASK_QUEUE="$forced_task_queue" \
  cargo run --quiet \
    --manifest-path "$repo_root/native/Cargo.toml" \
    --package activity-test-driver >>"$driver_log" 2>&1 &
driver_pid=$!

if ! wait "$driver_pid"; then
  driver_pid=""
  echo "Workflow did not observe forced isolated Activity cancellation" >&2
  cat "$driver_log" >&2
  cat "$parent_log" >&2
  exit 1
fi
driver_pid=""
if ! wait "$parent_pid"; then
  parent_pid=""
  echo "Forced-cancellation Activity parent did not shut down cleanly" >&2
  cat "$parent_log" >&2
  exit 1
fi
parent_pid=""

grep -q 'uncooperative isolated Activity cancellation reached the bounded forced-kill fallback' "$parent_log"

manifest_task_queue="odin-activity-isolate-manifest-${RANDOM}-$(date +%s)"
manifest_workflow_id="odin-activity-isolate-manifest-${RANDOM}-$(date +%s)"
ACTIVITY_ISOLATE_MULTI=manifest \
  TEMPORAL_ODIN_TASK_QUEUE="$manifest_task_queue" \
  "$binary" >>"$parent_log" 2>&1 &
parent_pid=$!
for _ in {1..100}; do
  if grep -q "external Activity parent is polling task queue $manifest_task_queue" "$parent_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$parent_pid" 2>/dev/null; then
    echo "Manifest Activity parent exited before polling" >&2
    cat "$parent_log" >&2
    exit 1
  fi
  sleep 0.1
done

ACTIVITY_ISOLATED_MANIFEST=1 \
  ACTIVITY_WORKFLOW_ID="$manifest_workflow_id" \
  TEMPORAL_TASK_QUEUE="$manifest_task_queue" \
  cargo run --quiet \
    --manifest-path "$repo_root/native/Cargo.toml" \
    --package activity-test-driver >>"$driver_log" 2>&1 &
driver_pid=$!

if ! wait "$driver_pid"; then
  driver_pid=""
  echo "Workflow did not dispatch both isolated Activity types" >&2
  cat "$driver_log" >&2
  cat "$parent_log" >&2
  exit 1
fi
driver_pid=""
if ! wait "$parent_pid"; then
  parent_pid=""
  echo "Manifest Activity parent did not shut down cleanly" >&2
  cat "$parent_log" >&2
  exit 1
fi
parent_pid=""

grep -q 'isolated Activity host dispatched two Activity types from one explicit manifest' "$parent_log"
grep -q 'Workflow dispatched through the isolated Activity manifest: Recovered on Activity attempt 1; Goodbye Odin' "$driver_log"

cat "$parent_log"
cat "$driver_log"
echo "Per-invocation Activity crash/retry, heartbeat forwarding, cancellation, and multi-type manifest dispatch passed through the Core-owning parent."

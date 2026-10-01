#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
task_queue="odin-standalone-activity-${RANDOM}-$(date +%s)"
worker_pid=""
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-standalone-activity-worker.XXXXXX")"
client_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-standalone-activity-client.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  rm -f "$worker_log" "$client_log"
}
trap cleanup EXIT INT TERM

if ! temporal operator cluster health \
  --address localhost:7233 \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  echo "A compatible Temporal dev server must already be listening on localhost:7233." >&2
  echo "Use Temporal CLI 1.9.1 or newer: temporal server start-dev --headless" >&2
  exit 1
fi

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
fi

TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-activity.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..100}; do
  if grep -q "is polling task queue $task_queue" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Standalone Activity Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
if ! grep -q "is polling task queue $task_queue" "$worker_log" 2>/dev/null; then
  echo "Standalone Activity Worker did not begin polling" >&2
  exit 1
fi

if ! TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-standalone-activity.sh" >"$client_log" 2>&1; then
  cat "$client_log" >&2
  if grep -q 'status=12' "$client_log"; then
    echo "The connected Temporal Service does not implement standalone Activities." >&2
  fi
  exit 1
fi

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
cat "$client_log"
grep -q 'one Activity Task completed and worker shut down cleanly' "$worker_log"
grep -q 'completed, described, listed, counted, and deleted through the Odin Client: Hello Standalone Odin' "$client_log"

for mode in cancel terminate; do
  STANDALONE_ACTIVITY_MODE="$mode" \
    TEMPORAL_ODIN_TASK_QUEUE="$task_queue-no-worker" \
    TEMPORAL_ODIN_SKIP_BUILD=1 \
    "$repo_root/scripts/run-standalone-activity.sh" >"$client_log" 2>&1
  cat "$client_log"
  expected_terminal="terminated"
  if [[ "$mode" == "cancel" ]]; then
    expected_terminal="canceled"
  fi
  grep -q "was $expected_terminal and deleted through the Odin Client" "$client_log"
done

STANDALONE_ACTIVITY_MODE=control \
  TEMPORAL_ODIN_TASK_QUEUE="$task_queue-no-worker" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-standalone-activity.sh" >"$client_log" 2>&1
cat "$client_log"
grep -q 'was paused, reset, unpaused, terminated, and deleted through the Odin Client' "$client_log"

echo "Standalone Activity lifecycle, visibility, pause/unpause/reset integration passed."

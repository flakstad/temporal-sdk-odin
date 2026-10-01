#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
crash_worker_pid=""
recovery_worker_pid=""
workflow_cli_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-crash-server.XXXXXX")"
crash_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-crash-worker.XXXXXX")"
recovery_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-recovery-worker.XXXXXX")"
workflow_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-crash-client.XXXXXX")"

cleanup() {
  for process_id in "$workflow_cli_pid" "$recovery_worker_pid" "$crash_worker_pid"; do
    if [[ -n "$process_id" ]]; then
      kill "$process_id" 2>/dev/null || true
      wait "$process_id" 2>/dev/null || true
    fi
  done
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$crash_log" "$recovery_log" "$workflow_log"
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
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "Temporal dev server exited before becoming healthy" >&2
      cat "$server_log" >&2
      exit 1
    fi
    sleep 0.25
  done
fi

if ! temporal operator cluster health \
  --address localhost:7233 \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  echo "Temporal dev server did not become healthy" >&2
  exit 1
fi

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
fi

task_queue="odin-workflow-crash-${RANDOM}-$(date +%s)"
workflow_id="odin-workflow-crash-${RANDOM}-$(date +%s)"

TEMPORAL_ODIN_SCENARIO=condition \
  TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$crash_log" 2>&1 &
crash_worker_pid=$!

for _ in {1..100}; do
  if grep -q "is polling task queue $task_queue" "$crash_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$crash_worker_pid" 2>/dev/null; then
    echo "Crash-probe Workflow Worker exited before polling" >&2
    cat "$crash_log" >&2
    exit 1
  fi
  sleep 0.1
done

if ! grep -q "is polling task queue $task_queue" "$crash_log" 2>/dev/null; then
  echo "Crash-probe Workflow Worker did not become ready" >&2
  cat "$crash_log" >&2
  exit 1
fi

temporal workflow execute \
  --workflow-id "$workflow_id" \
  --type odin_condition_workflow \
  --task-queue "$task_queue" \
  --input '"durable-order"' \
  --command-timeout 30s \
  --output json >"$workflow_log" 2>&1 &
workflow_cli_pid=$!

for _ in {1..100}; do
  if grep -Eq "replay=false .*waiting_condition=true .*completed=false" "$crash_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$crash_worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Workflow did not durably suspend before the crash probe" >&2
    cat "$crash_log" >&2
    cat "$workflow_log" >&2
    exit 1
  fi
  sleep 0.1
done

if ! grep -Eq "replay=false .*waiting_condition=true .*completed=false" "$crash_log" 2>/dev/null; then
  echo "Workflow suspension was not observed before the crash deadline" >&2
  cat "$crash_log" >&2
  exit 1
fi

kill -KILL "$crash_worker_pid"
set +e
wait "$crash_worker_pid"
crash_status=$?
set -e
crash_worker_pid=""
if [[ "$crash_status" == "0" ]]; then
  echo "Crash-probe Workflow Worker unexpectedly exited successfully" >&2
  exit 1
fi
if ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
  echo "Workflow became terminal when its first Worker process crashed" >&2
  cat "$workflow_log" >&2
  exit 1
fi

TEMPORAL_ODIN_SCENARIO=condition \
  TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$recovery_log" 2>&1 &
recovery_worker_pid=$!

for _ in {1..100}; do
  if grep -q "is polling task queue $task_queue" "$recovery_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$recovery_worker_pid" 2>/dev/null; then
    echo "Replacement Workflow Worker exited before polling" >&2
    cat "$recovery_log" >&2
    exit 1
  fi
  sleep 0.1
done

temporal workflow signal \
  --workflow-id "$workflow_id" \
  --name approve \
  --input '"Supervisor"' \
  --command-timeout 10s \
  --output none

if ! wait "$workflow_cli_pid"; then
  workflow_cli_pid=""
  echo "Workflow did not recover after its Worker process was replaced" >&2
  cat "$workflow_log" >&2
  cat "$recovery_log" >&2
  exit 1
fi
workflow_cli_pid=""
if ! wait "$recovery_worker_pid"; then
  recovery_worker_pid=""
  echo "Replacement Workflow Worker did not shut down cleanly" >&2
  cat "$recovery_log" >&2
  exit 1
fi
recovery_worker_pid=""

grep -q 'Order durable-order approved by Supervisor' "$workflow_log"
grep -Eq "replay=true .*waiting_condition=true .*completed=false" "$recovery_log"
grep -Eq "replay=false .*signal=true .*waiting_condition=false .*completed=true" "$recovery_log"
grep -q "Workflow condition resumed after a Signal and replayed on a fresh stack" "$recovery_log"

cat "$crash_log"
cat "$workflow_log"
cat "$recovery_log"
echo "Temporal reconstructed a suspended Odin Workflow after its Worker process was killed and replaced."

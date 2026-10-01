#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
crash_worker_pid=""
recovery_worker_pid=""
supervisor_pid=""
workflow_cli_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-crash-server.XXXXXX")"
supervisor_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-supervisor.XXXXXX")"
child_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-supervised-child.XXXXXX")"
workflow_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-crash-client.XXXXXX")"

cleanup() {
  if [[ -n "$supervisor_pid" ]] && kill -0 "$supervisor_pid" 2>/dev/null; then
    supervised_children="$(pgrep -P "$supervisor_pid" 2>/dev/null || true)"
    for process_id in $supervised_children; do
      kill "$process_id" 2>/dev/null || true
    done
  fi
  for process_id in "$workflow_cli_pid" "$recovery_worker_pid" "$crash_worker_pid" "$supervisor_pid"; do
    if [[ -n "$process_id" ]]; then
      kill "$process_id" 2>/dev/null || true
      wait "$process_id" 2>/dev/null || true
    fi
  done
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$supervisor_log" "$child_log" "$workflow_log"
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

ready_marker="Workflow Worker is polling task queue $task_queue"
TEMPORAL_SUPERVISOR_CHILD_COMMAND="$repo_root/scripts/run-hello-workflow.sh" \
  TEMPORAL_SUPERVISOR_WORKING_DIR="$repo_root" \
  TEMPORAL_SUPERVISOR_CHILD_LOG="$child_log" \
  TEMPORAL_SUPERVISOR_READY_MARKER="$ready_marker" \
  TEMPORAL_ODIN_SCENARIO=condition \
  TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-worker-supervisor.sh" >"$supervisor_log" 2>&1 &
supervisor_pid=$!

for _ in {1..100}; do
  if grep -Eq 'event=child-ready .*generation=1 ' "$supervisor_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$supervisor_pid" 2>/dev/null; then
    echo "Odin Worker Supervisor exited before its first child became ready" >&2
    cat "$supervisor_log" >&2
    cat "$child_log" >&2
    exit 1
  fi
  sleep 0.1
done

if ! grep -Eq 'event=child-ready .*generation=1 ' "$supervisor_log" 2>/dev/null; then
  echo "Odin Worker Supervisor did not report its first child ready" >&2
  cat "$supervisor_log" >&2
  cat "$child_log" >&2
  exit 1
fi
crash_worker_pid="$(sed -nE 's/.*event=child-ready pid=([0-9]+) generation=1 .*/\1/p' "$supervisor_log" | tail -1)"
if [[ -z "$crash_worker_pid" ]]; then
  echo "Could not identify the first supervised Worker process" >&2
  cat "$supervisor_log" >&2
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
  if grep -Eq "replay=false .*waiting_condition=true .*completed=false" "$child_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$supervisor_pid" 2>/dev/null || ! kill -0 "$crash_worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Workflow did not durably suspend before the crash probe" >&2
    cat "$supervisor_log" >&2
    cat "$child_log" >&2
    cat "$workflow_log" >&2
    exit 1
  fi
  sleep 0.1
done

if ! grep -Eq "replay=false .*waiting_condition=true .*completed=false" "$child_log" 2>/dev/null; then
  echo "Workflow suspension was not observed before the crash deadline" >&2
  cat "$child_log" >&2
  exit 1
fi

kill -KILL "$crash_worker_pid"
crash_worker_pid=""
if ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
  echo "Workflow became terminal when its first Worker process crashed" >&2
  cat "$workflow_log" >&2
  exit 1
fi

for _ in {1..100}; do
  if grep -Eq 'event=child-ready .*generation=2 ' "$supervisor_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$supervisor_pid" 2>/dev/null; then
    echo "Odin Worker Supervisor exited before replacing the crashed Worker" >&2
    cat "$supervisor_log" >&2
    cat "$child_log" >&2
    exit 1
  fi
  sleep 0.1
done
if ! grep -Eq 'event=child-ready .*generation=2 ' "$supervisor_log" 2>/dev/null; then
  echo "Odin Worker Supervisor did not report the replacement Worker ready" >&2
  cat "$supervisor_log" >&2
  cat "$child_log" >&2
  exit 1
fi
recovery_worker_pid="$(sed -nE 's/.*event=child-ready pid=([0-9]+) generation=2 .*/\1/p' "$supervisor_log" | tail -1)"
grep -Eq 'event=child-exited .*generation=1 .*success=false' "$supervisor_log"
grep -Eq 'event=restart-scheduled .*generation=1 ' "$supervisor_log"

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
  cat "$supervisor_log" >&2
  cat "$child_log" >&2
  exit 1
fi
workflow_cli_pid=""
recovery_worker_pid=""
if ! wait "$supervisor_pid"; then
  supervisor_pid=""
  echo "Odin Worker Supervisor did not stop after the replacement exited cleanly" >&2
  cat "$supervisor_log" >&2
  cat "$child_log" >&2
  exit 1
fi
supervisor_pid=""
if ! grep -q 'Worker supervision completed starts=2 ready=2 restarts=1 clean=1 failed=1 forced_kills=0' "$supervisor_log"; then
  echo "Odin Worker Supervisor reported unexpected lifecycle statistics" >&2
  cat "$supervisor_log" >&2
  exit 1
fi

grep -q 'Order durable-order approved by Supervisor' "$workflow_log"
grep -Eq "replay=true .*waiting_condition=true .*completed=false" "$child_log"
grep -Eq "replay=false .*signal=true .*waiting_condition=false .*completed=true" "$child_log"
grep -q "Workflow condition resumed after a Signal and replayed on a fresh stack" "$child_log"

cat "$supervisor_log"
cat "$workflow_log"
cat "$child_log"
echo "The Odin Supervisor restarted a crashed Worker and Temporal replayed its suspended Workflow."

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
crash_worker_pid=""
recovery_worker_pid=""
driver_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-panic-server.XXXXXX")"
crash_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-panic-worker.XXXXXX")"
recovery_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-recovery-worker.XXXXXX")"
driver_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-panic-driver.XXXXXX")"

cleanup() {
  for process_id in "$driver_pid" "$recovery_worker_pid" "$crash_worker_pid"; do
    if [[ -n "$process_id" ]]; then
      kill "$process_id" 2>/dev/null || true
      wait "$process_id" 2>/dev/null || true
    fi
  done
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$crash_log" "$recovery_log" "$driver_log"
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
  cargo build --quiet \
    --manifest-path "$repo_root/native/Cargo.toml" \
    --package activity-test-driver
fi

task_queue="odin-panic-recovery-${RANDOM}-$(date +%s)"
workflow_id="odin-panic-recovery-${RANDOM}-$(date +%s)"

TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-activity.sh" >"$crash_log" 2>&1 &
crash_worker_pid=$!

for _ in {1..100}; do
  if grep -q "is polling task queue $task_queue" "$crash_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$crash_worker_pid" 2>/dev/null; then
    echo "Crash-probe Activity Worker exited before polling" >&2
    cat "$crash_log" >&2
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

crash_observed=false
for _ in {1..100}; do
  if ! kill -0 "$crash_worker_pid" 2>/dev/null; then
    crash_observed=true
    break
  fi
  if ! kill -0 "$driver_pid" 2>/dev/null; then
    echo "Workflow driver exited before the Activity Worker crashed" >&2
    cat "$driver_log" >&2
    cat "$crash_log" >&2
    exit 1
  fi
  sleep 0.1
done

if [[ "$crash_observed" != true ]]; then
  echo "Activity Worker did not crash on the first attempt" >&2
  cat "$crash_log" >&2
  exit 1
fi

set +e
wait "$crash_worker_pid"
crash_status=$?
set -e
crash_worker_pid=""
if [[ "$crash_status" == "0" ]]; then
  echo "Activity panic probe unexpectedly exited successfully" >&2
  cat "$crash_log" >&2
  exit 1
fi
grep -q 'intentional Activity panic on attempt 1' "$crash_log" || {
  echo "Activity Worker crashed without reaching the intentional probe" >&2
  cat "$crash_log" >&2
  exit 1
}

TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-activity.sh" >"$recovery_log" 2>&1 &
recovery_worker_pid=$!

for _ in {1..100}; do
  if grep -q "is polling task queue $task_queue" "$recovery_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$recovery_worker_pid" 2>/dev/null; then
    echo "Recovery Activity Worker exited before polling" >&2
    cat "$recovery_log" >&2
    exit 1
  fi
  sleep 0.1
done

if ! wait "$driver_pid"; then
  driver_pid=""
  echo "Workflow did not recover after the Activity process crash" >&2
  cat "$driver_log" >&2
  cat "$recovery_log" >&2
  exit 1
fi
driver_pid=""
if ! wait "$recovery_worker_pid"; then
  recovery_worker_pid=""
  echo "Replacement Activity Worker did not shut down cleanly" >&2
  cat "$recovery_log" >&2
  exit 1
fi
recovery_worker_pid=""

grep -q 'Workflow survived the Activity process crash: Recovered on Activity attempt 2' "$driver_log"
grep -q 'one Activity Task completed and worker shut down cleanly' "$recovery_log"

cat "$crash_log"
cat "$driver_log"
cat "$recovery_log"
echo "Temporal retried an Activity after the first Odin Activity-only worker process crashed."

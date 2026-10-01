#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
worker_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-callback-server.XXXXXX")"
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-callback-worker.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$worker_log"
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
  echo "Temporal dev server did not become healthy" >&2
  exit 1
fi

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
  cargo build --quiet \
    --manifest-path "$repo_root/native/Cargo.toml" \
    --package activity-test-driver
fi

task_queue="odin-uncooperative-${RANDOM}-$(date +%s)"
workflow_id="odin-uncooperative-${RANDOM}-$(date +%s)"
ACTIVITY_EXPECT_UNCOOPERATIVE=1 \
  TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-activity.sh" >"$worker_log" 2>&1 &
worker_pid=$!

for _ in {1..100}; do
  if grep -q "is polling task queue $task_queue" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Activity Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done

ACTIVITY_INPUT=uncooperative-drain \
  ACTIVITY_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_TASK_QUEUE="$task_queue" \
  cargo run --quiet \
    --manifest-path "$repo_root/native/Cargo.toml" \
    --package activity-test-driver

if ! wait "$worker_pid"; then
  worker_pid=""
  echo "Activity Worker failed during callback-drain observation" >&2
  cat "$worker_log" >&2
  exit 1
fi
worker_pid=""

grep -q "uncooperative callback observed kind=Activity reason=Bounded_Run outstanding=1" "$worker_log"
grep -q "uncooperative Activity drain was observed without unsafe thread termination" "$worker_log"
grep -q "uncooperative callback policy completed its live bounded-drain path" "$worker_log"
cat "$worker_log"
echo "Uncooperative Activity callback was observed and then drained safely."

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
worker_pid=""
server_log="${TMPDIR:-/tmp}/temporal-sdk-odin-dev-server.log"
worker_log="${TMPDIR:-/tmp}/temporal-sdk-odin-activity-worker.log"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
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
  echo "Temporal dev server did not become ready; see $server_log" >&2
  exit 1
fi

"$repo_root/scripts/run-core-connect.sh"

run_activity_case() {
  local expect_failure="$1"
  "$repo_root/scripts/run-hello-activity.sh" >"$worker_log" 2>&1 &
  worker_pid=$!
  for _ in {1..30}; do
    if grep -q "is polling task queue" "$worker_log" 2>/dev/null; then
      break
    fi
    if ! kill -0 "$worker_pid" 2>/dev/null; then
      echo "Odin Activity Worker exited before polling; see $worker_log" >&2
      cat "$worker_log" >&2
      exit 1
    fi
    sleep 1
  done

  if ! grep -q "is polling task queue" "$worker_log" 2>/dev/null; then
    echo "Odin Activity Worker did not become ready; see $worker_log" >&2
    exit 1
  fi

  if [[ "$expect_failure" == "yes" ]]; then
    ACTIVITY_EXPECT_FAILURE=1 cargo run --quiet \
      --manifest-path "$repo_root/native/Cargo.toml" \
      --package activity-test-driver
  else
    cargo run --quiet \
      --manifest-path "$repo_root/native/Cargo.toml" \
      --package activity-test-driver
  fi

  wait "$worker_pid"
  worker_pid=""
  cat "$worker_log"
  grep -q "one Activity Task completed and worker shut down cleanly" "$worker_log"
}

run_activity_case no
run_activity_case yes

"$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

if ! grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
  echo "Odin Workflow Worker did not become ready; see $worker_log" >&2
  exit 1
fi

workflow_id="odin-sdk-workflow-spike-$$-$(date +%s)"
workflow_output="$(temporal workflow execute \
  --workflow-id "$workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"Odin"' \
  --command-timeout 30s \
  --output json)"
echo "$workflow_output"
grep -q '"status": "COMPLETED"' <<<"$workflow_output"
grep -Fq '"result": "Hello Odin \u0026 Hello Temporal"' <<<"$workflow_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "replay=true scheduled=2" "$worker_log"
grep -q "scheduled=2 last_activity=2" "$worker_log"
grep -Eq "resolved=1 waiting=2|resolved=2 waiting=1" "$worker_log"
grep -q "two outstanding Activities completed across evictions before and between resolutions" "$worker_log"

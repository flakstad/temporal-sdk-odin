#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workflow_id="odin-post-reset-${RANDOM}-$(date +%s)"
worker_pid=""
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-post-reset-worker.XXXXXX")"
output_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-post-reset-output.XXXXXX")"
history_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-post-reset-history.XXXXXX")"
describe_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-post-reset-describe.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  temporal workflow terminate \
    --workflow-id "$workflow_id" \
    --reason 'Temporal Odin post-reset integration cleanup' \
    --command-timeout 2s \
    --output none >/dev/null 2>&1 || true
  rm -f "$worker_log" "$output_log" "$history_file" "$describe_file"
}
trap cleanup EXIT INT TERM

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
fi

run_worker() {
  : >"$worker_log"
  TEMPORAL_ODIN_SCENARIO=multiple-workflow-arguments \
    TEMPORAL_ODIN_SKIP_BUILD=1 \
    "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
  worker_pid=$!
  for _ in {1..100}; do
    if grep -q 'Workflow Worker is polling' "$worker_log" 2>/dev/null; then
      return
    fi
    if ! kill -0 "$worker_pid" 2>/dev/null; then
      echo 'Post-reset Workflow Worker exited before polling' >&2
      cat "$worker_log" >&2
      exit 1
    fi
    sleep 0.1
  done
  echo 'Post-reset Workflow Worker did not become ready' >&2
  exit 1
}

run_worker
TEMPORAL_CLIENT_SCENARIO=multiple-workflow-arguments \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$output_log"
grep -q 'result="Odin:42:true"' "$output_log"
wait "$worker_pid"
worker_pid=""

run_worker
reset_output="$(TEMPORAL_CLIENT_ACTION=reset \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_RESET_EVENT_ID=4 \
  TEMPORAL_CLIENT_REASON='Odin SDK post-reset integration' \
  TEMPORAL_CLIENT_RESET_PRIORITY_KEY=2 \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh")"
echo "$reset_output"
grep -q 'post_reset_operations=1' <<<"$reset_output"
reset_run_id="$(sed -n 's/.*new_run_id=\([^ ]*\).*/\1/p' <<<"$reset_output")"
if [[ -z "$reset_run_id" ]]; then
  echo 'Post-reset Client did not return a Run ID' >&2
  exit 1
fi

temporal workflow result \
  --workflow-id "$workflow_id" \
  --run-id "$reset_run_id" \
  --command-timeout 30s \
  --output json >"$output_log"
grep -q 'Odin:42:true' "$output_log"
wait "$worker_pid"
worker_pid=""

temporal workflow show \
  --workflow-id "$workflow_id" \
  --run-id "$reset_run_id" \
  --command-timeout 10s \
  --output json >"$history_file"
grep -q 'EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED' "$history_file"

temporal workflow describe \
  --workflow-id "$workflow_id" \
  --run-id "$reset_run_id" \
  --command-timeout 10s \
  --output json >"$describe_file"
grep -Eq '"priorityKey"[[:space:]]*:[[:space:]]*2' "$describe_file"

if TEMPORAL_CLIENT_ACTION=reset \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_RESET_EVENT_ID=4 \
  TEMPORAL_CLIENT_REASON='Odin SDK post-reset Signal capability probe' \
  TEMPORAL_CLIENT_RESET_SIGNAL_NAME=repair-ready \
  TEMPORAL_CLIENT_RESET_SIGNAL_INPUT=from-reset \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log" 2>&1; then
  echo 'Server unexpectedly accepted the schema-present post-reset Signal operation' >&2
  exit 1
fi
grep -q 'unsupported post reset operation' "$output_log"

cat "$worker_log"
echo "Post-reset priority update reached new run $reset_run_id before its first Workflow Task."
echo 'Server rejected the schema-present post-reset Signal operation as unsupported.'

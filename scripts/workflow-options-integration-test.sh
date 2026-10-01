#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workflow_id="odin-workflow-options-${RANDOM}-$(date +%s)"
worker_pid=""
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-options-worker.XXXXXX")"
output_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-options-output.XXXXXX")"
describe_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-options-describe.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  temporal workflow terminate \
    --workflow-id "$workflow_id" \
    --reason 'Temporal Odin Workflow options integration cleanup' \
    --command-timeout 2s \
    --output none >/dev/null 2>&1 || true
  rm -f "$worker_log" "$output_log" "$describe_file"
}
trap cleanup EXIT INT TERM

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
fi

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..100}; do
  if grep -q 'Workflow Worker is polling' "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo 'Workflow options Worker exited before polling' >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done

temporal workflow start \
  --workflow-id "$workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"Odin"' \
  --command-timeout 10s \
  --output none

for _ in {1..100}; do
  if grep -Eq 'started_timer=true .*waiting_timer=1 waiting_signal=true' "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo 'Workflow options test did not reach the open signal wait' >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
if ! grep -Eq 'started_timer=true .*waiting_timer=1 waiting_signal=true' "$worker_log"; then
  echo 'Workflow options test timed out before the open signal wait' >&2
  exit 1
fi

TEMPORAL_CLIENT_ACTION=update-options \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_PRIORITY_KEY=5 \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log"
cat "$output_log"
grep -q 'Client Workflow options interceptor entered paths=1' "$output_log"
grep -q 'Client Workflow options interceptor exited after Core' "$output_log"
grep -Eq 'priority=5 response_priority=5 update_time=(true|false)' "$output_log"

temporal workflow describe \
  --workflow-id "$workflow_id" \
  --command-timeout 10s \
  --output json >"$describe_file"
grep -Eq '"priorityKey"[[:space:]]*:[[:space:]]*5' "$describe_file"

TEMPORAL_CLIENT_ACTION=signal \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_SIGNAL_NAME=approval \
  TEMPORAL_CLIENT_SIGNAL_INPUT=Approved \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
echo "Workflow execution options priority 5 round-tripped through Core and Server for $workflow_id."

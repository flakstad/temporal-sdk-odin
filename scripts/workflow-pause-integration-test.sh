#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workflow_id="odin-workflow-pause-${RANDOM}-$(date +%s)"
worker_pid=""
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-pause-worker.XXXXXX")"
output_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-pause-output.XXXXXX")"
describe_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-pause-describe.XXXXXX")"
history_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-pause-history.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  temporal workflow terminate \
    --workflow-id "$workflow_id" \
    --reason 'Temporal Odin Workflow pause integration cleanup' \
    --command-timeout 2s \
    --output none >/dev/null 2>&1 || true
  rm -f "$worker_log" "$output_log" "$describe_file" "$history_file"
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
    echo 'Workflow pause Worker exited before polling' >&2
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
    echo 'Workflow pause test did not reach the open signal wait' >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
if ! grep -Eq 'started_timer=true .*waiting_timer=1 waiting_signal=true' "$worker_log"; then
  echo 'Workflow pause test timed out before the open signal wait' >&2
  exit 1
fi

TEMPORAL_CLIENT_ACTION=pause \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_REASON='downstream dependency outage' \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log"
cat "$output_log"
grep -q 'Client Workflow pause interceptor entered kind=Pause' "$output_log"
grep -q 'Client Workflow pause interceptor exited after Core' "$output_log"
grep -q 'operation=pause accepted' "$output_log"

temporal workflow describe \
  --workflow-id "$workflow_id" \
  --command-timeout 10s \
  --output json >"$describe_file"
if ! grep -Eq 'WORKFLOW_EXECUTION_STATUS_PAUSED|"status"[[:space:]]*:[[:space:]]*("Paused"|8)' "$describe_file"; then
  echo 'Describe did not expose the Paused Workflow status' >&2
  cat "$describe_file" >&2
  exit 1
fi

if TEMPORAL_CLIENT_ACTION=query \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_QUERY_NAME=greeting_status \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log" 2>&1; then
  echo 'Paused Workflow unexpectedly accepted a Query' >&2
  exit 1
fi
cat "$output_log"
grep -q 'kind=Workflow_Query_Rejected' "$output_log"

TEMPORAL_CLIENT_ACTION=signal \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_SIGNAL_NAME=approval \
  TEMPORAL_CLIENT_SIGNAL_INPUT=Approved \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log"
sleep 0.25
if grep -q ' signal=true signal_handlers_started=' "$worker_log"; then
  echo 'Paused Workflow processed its accepted Signal before Unpause' >&2
  cat "$worker_log" >&2
  exit 1
fi
kill -0 "$worker_pid"

TEMPORAL_CLIENT_ACTION=unpause \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_REASON='downstream dependency recovered' \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log"
cat "$output_log"
grep -q 'Client Workflow pause interceptor entered kind=Unpause' "$output_log"
grep -q 'Client Workflow pause interceptor exited after Core' "$output_log"
grep -q 'operation=unpause accepted' "$output_log"

wait "$worker_pid"
worker_pid=""
grep -q ' signal=true signal_handlers_started=' "$worker_log"
temporal workflow show \
  --workflow-id "$workflow_id" \
  --command-timeout 10s \
  --output json >"$history_file"
grep -Eq 'EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED|"eventType"[[:space:]]*:[[:space:]]*58' "$history_file"
grep -Eq 'EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED|"eventType"[[:space:]]*:[[:space:]]*59' "$history_file"

cat "$worker_log"
echo "Workflow $workflow_id accepted a Signal while Paused and processed it only after Unpause."

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workflow_id="odin-update-with-start-${RANDOM}-$(date +%s)"
worker_pid=""
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-update-with-start-worker.XXXXXX")"
output_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-update-with-start-output.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  temporal workflow terminate \
    --workflow-id "$workflow_id" \
    --reason 'Temporal Odin Update-With-Start integration cleanup' \
    --command-timeout 2s \
    --output none >/dev/null 2>&1 || true
  rm -f "$worker_log" "$output_log"
}
trap cleanup EXIT INT TERM

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
fi

TEMPORAL_ODIN_SCENARIO=update-with-start \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..100}; do
  if grep -q 'Workflow Worker is polling' "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo 'Update-With-Start Worker exited before polling' >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
if ! grep -q 'Workflow Worker is polling' "$worker_log"; then
  echo 'Update-With-Start Worker did not begin polling' >&2
  exit 1
fi

TEMPORAL_UPDATE_WITH_START_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-update-with-start.sh" >"$output_log"
cat "$output_log"
grep -q "Update-With-Start created then attached workflow_id=$workflow_id" "$output_log"
grep -q 'result="Hello Odin"' "$output_log"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'updates_accepted=1 updates_rejected=0 updates_completed=0' "$worker_log"
grep -q 'updates_accepted=0 updates_rejected=0 updates_completed=1' "$worker_log"
grep -q 'completed=true' "$worker_log"
echo "Update-With-Start create and attach paths completed through Core and Server for $workflow_id."

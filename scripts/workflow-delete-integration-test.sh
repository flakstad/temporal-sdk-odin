#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workflow_id="odin-workflow-delete-${RANDOM}-$(date +%s)"
server_pid=""
worker_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-delete-server.XXXXXX")"
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-delete-worker.XXXXXX")"
output_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-delete-output.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  temporal workflow terminate \
    --workflow-id "$workflow_id" \
    --reason 'Temporal Odin Workflow delete integration cleanup' \
    --command-timeout 2s \
    --output none >/dev/null 2>&1 || true
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$worker_log" "$output_log"
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
  echo "Temporal CLI dev server did not become ready; see $server_log" >&2
  exit 1
fi

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
    echo 'Workflow delete Worker exited before polling' >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done

temporal workflow start \
  --workflow-id "$workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"delete-me"' \
  --command-timeout 10s \
  --output none

for _ in {1..100}; do
  if grep -Eq 'started_timer=true .*waiting_timer=1 waiting_signal=true' "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo 'Workflow delete test did not reach the open signal wait' >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
if ! grep -Eq 'started_timer=true .*waiting_timer=1 waiting_signal=true' "$worker_log"; then
  echo 'Workflow delete test timed out before the open signal wait' >&2
  exit 1
fi

TEMPORAL_CLIENT_ACTION=describe \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log"
cat "$output_log"
run_id="$(sed -n 's/.* run_id=\([^ ]*\) status=.*/\1/p' "$output_log")"
if [[ -z "$run_id" ]]; then
  echo 'Odin Client Describe did not return the Workflow Run ID' >&2
  exit 1
fi

set +e
TEMPORAL_CLIENT_ACTION=delete \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log" 2>&1
unsafe_status=$?
set -e
cat "$output_log"
if [[ "$unsafe_status" == "0" ]]; then
  echo 'Workflow deletion without an exact Run ID unexpectedly succeeded' >&2
  exit 1
fi
grep -q 'kind=Invalid_Argument status=0' "$output_log"
grep -q 'requires an exact Run ID' "$output_log"

TEMPORAL_CLIENT_ACTION=delete \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_RUN_ID="$run_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log"
cat "$output_log"
grep -q "Client delete interceptor entered run_id=\"$run_id\" allow_latest_run=false" "$output_log"
grep -q 'Client delete interceptor exited after Core' "$output_log"
grep -q "Odin client operation=delete accepted workflow_id=$workflow_id" "$output_log"

set +e
TEMPORAL_CLIENT_ACTION=describe \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_RUN_ID="$run_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$output_log" 2>&1
describe_status=$?
set -e
cat "$output_log"
if [[ "$describe_status" == "0" ]]; then
  echo 'Deleted Workflow execution remained describable' >&2
  exit 1
fi
grep -q 'kind=Rpc status=5' "$output_log"

echo "Odin Client deleted exact Workflow execution $workflow_id/$run_id through Core."

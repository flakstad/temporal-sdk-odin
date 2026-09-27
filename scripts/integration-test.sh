#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
worker_pid=""
workflow_cli_pid=""
slow_update_pid=""
unknown_workflow_id=""
server_log="${TMPDIR:-/tmp}/temporal-sdk-odin-dev-server.log"
worker_log="${TMPDIR:-/tmp}/temporal-sdk-odin-activity-worker.log"
workflow_output_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-output.XXXXXX")"
workflow_control_output_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-control.XXXXXX")"
slow_update_output_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-slow-update.XXXXXX")"
unknown_history_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-unknown-history.XXXXXX")"
typed_history_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-typed-history.XXXXXX")"

cleanup() {
  if [[ -n "$unknown_workflow_id" ]]; then
    temporal workflow terminate \
      --workflow-id "$unknown_workflow_id" \
      --reason "Temporal Odin SDK integration cleanup" \
      --command-timeout 2s \
      --output none >/dev/null 2>&1 || true
  fi
  if [[ -n "$workflow_cli_pid" ]]; then
    kill "$workflow_cli_pid" 2>/dev/null || true
    wait "$workflow_cli_pid" 2>/dev/null || true
  fi
  if [[ -n "$slow_update_pid" ]]; then
    kill "$slow_update_pid" 2>/dev/null || true
    wait "$slow_update_pid" 2>/dev/null || true
  fi
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$workflow_output_file" "$workflow_control_output_file" "$slow_update_output_file" "$unknown_history_file" "$typed_history_file"
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

"$repo_root/scripts/build-examples.sh"

TEMPORAL_ODIN_SKIP_BUILD=1 "$repo_root/scripts/run-core-connect.sh"

run_activity_case() {
  local mode="$1"
  if [[ "$mode" == "retry" ]]; then
    ACTIVITY_EXPECT_RETRY=1 \
      TEMPORAL_ODIN_SKIP_BUILD=1 \
      "$repo_root/scripts/run-hello-activity.sh" >"$worker_log" 2>&1 &
  else
    TEMPORAL_ODIN_SKIP_BUILD=1 \
      "$repo_root/scripts/run-hello-activity.sh" >"$worker_log" 2>&1 &
  fi
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

  if [[ "$mode" == "failure" ]]; then
    ACTIVITY_EXPECT_FAILURE=1 cargo run --quiet \
      --manifest-path "$repo_root/native/Cargo.toml" \
      --package activity-test-driver
  elif [[ "$mode" == "typed" ]]; then
    ACTIVITY_TYPED=1 cargo run --quiet \
      --manifest-path "$repo_root/native/Cargo.toml" \
      --package activity-test-driver
  elif [[ "$mode" == "retry" ]]; then
    ACTIVITY_RETRY=1 cargo run --quiet \
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
  if [[ "$mode" == "retry" ]]; then
    grep -q "retryable Activity failed once, then completed and worker shut down cleanly" "$worker_log"
  else
    grep -q "one Activity Task completed and worker shut down cleanly" "$worker_log"
  fi
}

run_activity_case string
run_activity_case failure
run_activity_case typed
run_activity_case retry

ACTIVITY_EXPECT_CONCURRENT=1 \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-activity.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "is polling task queue" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Concurrent Odin Activity Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done
ACTIVITY_CONCURRENT=1 cargo run --quiet \
  --manifest-path "$repo_root/native/Cargo.toml" \
  --package activity-test-driver
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "two Activity callbacks overlapped and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SCENARIO=typed-payload \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Typed Odin Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

typed_workflow_id="odin-sdk-typed-workflow-$$-$(date +%s)"
TEMPORAL_CLIENT_WORKFLOW_ID="$typed_workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$workflow_output_file"
workflow_output="$(<"$workflow_output_file")"
echo "$workflow_output"
grep -q "Odin client completed workflow_id=$typed_workflow_id" <<<"$workflow_output"
grep -q 'message="Hello, Odin!"' <<<"$workflow_output"
grep -q 'activity_id=typed-greeting-options' <<<"$workflow_output"
grep -q 'task_queue=odin-hello-workflow' <<<"$workflow_output"
grep -q 'retry_attempts=3' <<<"$workflow_output"
temporal workflow show \
  --workflow-id "$typed_workflow_id" \
  --command-timeout 10s \
  --output json >"$typed_history_file"
grep -q '"activityId": "typed-greeting-options"' "$typed_history_file"
grep -q '"scheduleToCloseTimeout": "30s"' "$typed_history_file"
grep -q '"scheduleToStartTimeout": "5s"' "$typed_history_file"
grep -q '"startToCloseTimeout": "10s"' "$typed_history_file"
grep -q '"heartbeatTimeout": "2s"' "$typed_history_file"
grep -q '"initialInterval": "0.100s"' "$typed_history_file"
grep -q '"backoffCoefficient": 2' "$typed_history_file"
grep -q '"maximumInterval": "1s"' "$typed_history_file"
grep -q '"maximumAttempts": 3' "$typed_history_file"
grep -q '"InvalidGreeting"' "$typed_history_file"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "typed Workflow and Activity Payloads completed end-to-end" "$worker_log"

TEMPORAL_ODIN_SCENARIO=failure-details \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Failure-details Odin Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

failure_workflow_id="odin-sdk-failure-details-$$-$(date +%s)"
TEMPORAL_CLIENT_SCENARIO=failure-details \
  TEMPORAL_CLIENT_WORKFLOW_ID="$failure_workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$workflow_output_file"
failure_workflow_output="$(<"$workflow_output_file")"
echo "$failure_workflow_output"
grep -q "Odin client completed failure-details workflow_id=$failure_workflow_id" <<<"$failure_workflow_output"
grep -q 'result="Observed structured Activity failure"' <<<"$failure_workflow_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "structured Activity failure details reached Workflow code" "$worker_log"

TEMPORAL_ODIN_SCENARIO=update \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
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

unknown_workflow_id="odin-sdk-unknown-workflow-$$-$(date +%s)"
temporal workflow start \
  --workflow-id "$unknown_workflow_id" \
  --type odin_missing_workflow \
  --task-queue odin-hello-workflow \
  --input '"Odin"' \
  --command-timeout 10s \
  --output none

for _ in {1..30}; do
  temporal workflow show \
    --workflow-id "$unknown_workflow_id" \
    --command-timeout 2s \
    --output json >"$unknown_history_file" 2>/dev/null || true
  if grep -q 'activation_failed=true error="Workflow type odin_missing_workflow not found"' "$worker_log" 2>/dev/null && \
     grep -q 'activation_failed=false .*evicted=true' "$worker_log" 2>/dev/null && \
     grep -q 'EVENT_TYPE_WORKFLOW_TASK_FAILED' "$unknown_history_file" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Unknown Workflow type terminated the Odin worker" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

grep -q 'activation_failed=true error="Workflow type odin_missing_workflow not found"' "$worker_log"
grep -q 'activation_failed=false .*evicted=true' "$worker_log"
grep -q 'EVENT_TYPE_WORKFLOW_TASK_FAILED' "$unknown_history_file"
kill -0 "$worker_pid"
TEMPORAL_CLIENT_ACTION=terminate \
  TEMPORAL_CLIENT_WORKFLOW_ID="$unknown_workflow_id" \
  TEMPORAL_CLIENT_REASON="unknown Workflow type test completed" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh"
unknown_workflow_id=""

workflow_id="odin-sdk-workflow-spike-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"Odin"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file" &
workflow_cli_pid=$!

for _ in {1..30}; do
  if grep -Eq "started_timer=true .*waiting_timer=1 waiting_signal=true" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Workflow did not reach its signal-or-timeout await before signaling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

if ! grep -Eq "started_timer=true .*waiting_timer=1 waiting_signal=true" "$worker_log" 2>/dev/null; then
  echo "Workflow did not expose its signal-or-timeout await before signaling" >&2
  exit 1
fi

if TEMPORAL_CLIENT_ACTION=update \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_UPDATE_NAME=set_greeting_name \
  TEMPORAL_CLIENT_UPDATE_INPUT=reject \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$workflow_control_output_file" 2>&1; then
  echo "Workflow Update validator unexpectedly accepted invalid input" >&2
  exit 1
fi
rejected_update_output="$(<"$workflow_control_output_file")"
echo "$rejected_update_output"
grep -q 'kind=Workflow_Update_Failed' <<<"$rejected_update_output"
grep -q 'greeting name is invalid' <<<"$rejected_update_output"

TEMPORAL_CLIENT_ACTION=update \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_UPDATE_NAME=set_greeting_name \
  TEMPORAL_CLIENT_UPDATE_INPUT=Temporal-slow \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$slow_update_output_file" 2>&1 &
slow_update_pid=$!

for _ in {1..30}; do
  if grep -Eq "replay=false .*scheduled=1 last_activity=1 .*updates_accepted=1 updates_rejected=0 updates_completed=0" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$slow_update_pid" 2>/dev/null; then
    echo "First concurrent Workflow Update did not suspend on its Activity" >&2
    cat "$worker_log" >&2
    cat "$slow_update_output_file" >&2
    exit 1
  fi
  sleep 0.1
done

TEMPORAL_CLIENT_ACTION=update \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_UPDATE_NAME=set_greeting_name \
  TEMPORAL_CLIENT_UPDATE_INPUT=Temporal-fast \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$workflow_control_output_file"
fast_update_output="$(<"$workflow_control_output_file")"
echo "$fast_update_output"
grep -q "Odin client update=set_greeting_name workflow_id=$workflow_id" <<<"$fast_update_output"
grep -q 'previous="Odin" current="Temporal-fast"' <<<"$fast_update_output"

wait "$slow_update_pid"
slow_update_pid=""
slow_update_output="$(<"$slow_update_output_file")"
echo "$slow_update_output"
grep -q "Odin client update=set_greeting_name workflow_id=$workflow_id" <<<"$slow_update_output"
grep -q 'previous="Odin" current="Temporal-slow"' <<<"$slow_update_output"

TEMPORAL_CLIENT_ACTION=query \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_QUERY_NAME=greeting_status \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$workflow_control_output_file"
query_output="$(<"$workflow_control_output_file")"
echo "$query_output"
grep -q "Odin client query=greeting_status workflow_id=$workflow_id" <<<"$query_output"
grep -q 'phase="waiting-for-approval" name="Temporal-slow"' <<<"$query_output"

TEMPORAL_CLIENT_ACTION=signal \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_SIGNAL_NAME=approval \
  TEMPORAL_CLIENT_SIGNAL_INPUT=Approved \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh"

wait "$workflow_cli_pid"
workflow_cli_pid=""
workflow_output="$(<"$workflow_output_file")"
echo "$workflow_output"
grep -q '"status": "COMPLETED"' <<<"$workflow_output"
grep -q '"result": "Hello Approved"' <<<"$workflow_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -Eq "replay=true .*signal=true .*canceled_timer=true .*waiting_activity=3" "$worker_log"
grep -Eq "replay=false .*signal=true .*canceled_timer=true .*waiting_activity=3" "$worker_log"
grep -Eq "replay=(true|false) .*queries=1 query_failures=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*updates_accepted=0 updates_rejected=1 updates_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*scheduled=1 last_activity=1 .*updates_accepted=1 updates_rejected=0 updates_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*scheduled=1 last_activity=2 .*updates_accepted=1 updates_rejected=0 updates_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*resolved=2 .*updates_accepted=0 updates_rejected=0 updates_completed=1 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*resolved=1 .*updates_accepted=0 updates_rejected=0 updates_completed=1 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=true .*scheduled=1 last_activity=1 .*updates_accepted=1 updates_rejected=0 updates_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=true .*scheduled=1 last_activity=2 .*updates_accepted=1 updates_rejected=0 updates_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=true .*resolved=2 .*updates_accepted=0 updates_rejected=0 updates_completed=1 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=true .*resolved=1 .*updates_accepted=0 updates_rejected=0 updates_completed=1 .*waiting_signal=true" "$worker_log"
grep -q "signal won the durable timer race and replay reproduced timer cancellation" "$worker_log"

TEMPORAL_ODIN_SCENARIO=abandon-update \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Update-abandonment Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

abandon_workflow_id="odin-sdk-abandon-update-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$abandon_workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"Odin"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file" &
workflow_cli_pid=$!

for _ in {1..30}; do
  if grep -Eq "started_timer=true .*waiting_timer=1 waiting_signal=true" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Update-abandonment Workflow did not reach its main await" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

TEMPORAL_CLIENT_ACTION=update \
  TEMPORAL_CLIENT_WORKFLOW_ID="$abandon_workflow_id" \
  TEMPORAL_CLIENT_UPDATE_NAME=hold_greeting_name \
  TEMPORAL_CLIENT_UPDATE_INPUT=Never-applied \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$slow_update_output_file" 2>&1 &
slow_update_pid=$!

for _ in {1..30}; do
  if grep -Eq "replay=false .*updates_accepted=1 updates_rejected=0 updates_completed=0 .*waiting_signal=true" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$slow_update_pid" 2>/dev/null; then
    echo "Held Workflow Update was not accepted" >&2
    cat "$worker_log" >&2
    cat "$slow_update_output_file" >&2
    exit 1
  fi
  sleep 0.1
done

TEMPORAL_CLIENT_ACTION=signal \
  TEMPORAL_CLIENT_WORKFLOW_ID="$abandon_workflow_id" \
  TEMPORAL_CLIENT_SIGNAL_NAME=approval \
  TEMPORAL_CLIENT_SIGNAL_INPUT=Approved \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh"

wait "$workflow_cli_pid"
workflow_cli_pid=""
workflow_output="$(<"$workflow_output_file")"
echo "$workflow_output"
grep -q '"status": "COMPLETED"' <<<"$workflow_output"
grep -q '"result": "Hello Approved"' <<<"$workflow_output"

abandoned_update_status=0
wait "$slow_update_pid" || abandoned_update_status=$?
slow_update_pid=""
abandoned_update_output="$(<"$slow_update_output_file")"
echo "$abandoned_update_output"
[[ "$abandoned_update_status" -ne 0 ]]
grep -q 'kind=Workflow_Update_Failed' <<<"$abandoned_update_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -Eq "replay=false .*unfinished_updates=1 update_warnings=1 .*completed=true" "$worker_log"
grep -q "Temporal warning: Workflow .* closed with 1 unfinished Update handler(s); handlers were abandoned" "$worker_log"
grep -q "Workflow close abandoned the unfinished Update handler with a warning" "$worker_log"

TEMPORAL_ODIN_SCENARIO=cancel-update \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Detached-Update Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

cancel_update_workflow_id="odin-sdk-cancel-update-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$cancel_update_workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"cancel-update"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file" 2>&1 &
workflow_cli_pid=$!

for _ in {1..30}; do
  if grep -Eq "started_timer=true .*waiting_timer=1 waiting_signal=true" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Detached-Update Workflow did not reach its main await" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

TEMPORAL_CLIENT_ACTION=update \
  TEMPORAL_CLIENT_WORKFLOW_ID="$cancel_update_workflow_id" \
  TEMPORAL_CLIENT_UPDATE_NAME=set_greeting_name_detached \
  TEMPORAL_CLIENT_UPDATE_INPUT=Temporal-slow \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$slow_update_output_file" 2>&1 &
slow_update_pid=$!

for _ in {1..30}; do
  if grep -Eq "replay=false .*scheduled=1 last_activity=1 .*updates_accepted=1 updates_rejected=0 updates_completed=0" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$slow_update_pid" 2>/dev/null; then
    echo "Detached Workflow Update did not suspend on its Activity" >&2
    cat "$worker_log" >&2
    cat "$slow_update_output_file" >&2
    exit 1
  fi
  sleep 0.1
done

TEMPORAL_CLIENT_ACTION=cancel \
  TEMPORAL_CLIENT_WORKFLOW_ID="$cancel_update_workflow_id" \
  TEMPORAL_CLIENT_REASON="drain detached Update" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh"

wait "$slow_update_pid"
slow_update_pid=""
detached_update_output="$(<"$slow_update_output_file")"
echo "$detached_update_output"
grep -q "Odin client update=set_greeting_name_detached workflow_id=$cancel_update_workflow_id" <<<"$detached_update_output"
grep -q 'previous="cancel-update" current="Temporal-slow"' <<<"$detached_update_output"

cancel_update_cli_status=0
wait "$workflow_cli_pid" || cancel_update_cli_status=$?
workflow_cli_pid=""
workflow_output="$(<"$workflow_output_file")"
echo "$workflow_output"
[[ "$cancel_update_cli_status" -eq 0 || "$cancel_update_cli_status" -eq 1 ]]
grep -q '"status": "CANCELED"' <<<"$workflow_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -Eq "replay=false .*cancel=true canceled_workflow=false .*canceled_timer=true canceled_activity=false .*waiting_handlers=true completed=false" "$worker_log"
grep -Eq "replay=false .*resolved=1 .*updates_accepted=0 updates_rejected=0 updates_completed=1 .*canceled_workflow=true .*completed=true" "$worker_log"
grep -q "detached Workflow Update completed before cancellation acknowledgement" "$worker_log"

run_workflow_cancellation_case() {
  local scenario="$1"
  local input="$2"
  local waiting_pattern="$3"
  local cancellation_pattern="$4"
  local completion_message="$5"

  TEMPORAL_ODIN_SCENARIO="$scenario" \
    TEMPORAL_ODIN_SKIP_BUILD=1 \
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

  workflow_id="odin-sdk-${scenario}-$$-$(date +%s)"
  temporal workflow execute \
    --workflow-id "$workflow_id" \
    --type odin_hello_workflow \
    --task-queue odin-hello-workflow \
    --input "$input" \
    --command-timeout 30s \
    --output json >"$workflow_output_file" 2>&1 &
  workflow_cli_pid=$!

  for _ in {1..30}; do
    if grep -Eq "$waiting_pattern" "$worker_log" 2>/dev/null; then
      break
    fi
    if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
      echo "Workflow did not reach the expected suspension point before cancellation" >&2
      cat "$worker_log" >&2
      cat "$workflow_output_file" >&2
      exit 1
    fi
    sleep 1
  done

  if ! grep -Eq "$waiting_pattern" "$worker_log" 2>/dev/null; then
    echo "Workflow did not expose the expected suspension point before cancellation" >&2
    exit 1
  fi

  TEMPORAL_CLIENT_ACTION=cancel \
    TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
    TEMPORAL_CLIENT_REASON="Odin Client integration cancellation" \
    TEMPORAL_ODIN_SKIP_BUILD=1 \
    "$repo_root/scripts/run-client-control.sh"

  workflow_cli_status=0
  wait "$workflow_cli_pid" || workflow_cli_status=$?
  workflow_cli_pid=""
  workflow_output="$(<"$workflow_output_file")"
  echo "$workflow_output"
  # CLI 1.5.1 exits 1 for the expected canceled terminal status. The JSON
  # assertion is the compatibility boundary rather than that presentation code.
  [[ "$workflow_cli_status" -eq 0 || "$workflow_cli_status" -eq 1 ]]
  grep -q '"status": "CANCELED"' <<<"$workflow_output"

  wait "$worker_pid"
  worker_pid=""
  cat "$worker_log"
  grep -Eq "$cancellation_pattern" "$worker_log"
  grep -q "$completion_message" "$worker_log"
}

run_workflow_cancellation_case \
  cancel-timer \
  '"Odin"' \
  'started_timer=true .*waiting_timer=1 waiting_signal=true' \
  'replay=false .*cancel=true canceled_workflow=true .*canceled_timer=true canceled_activity=false' \
  'Workflow cancellation canceled the pending timer and closed the execution'

run_workflow_cancellation_case \
  cancel-activity \
  '"cancel-activity"' \
  'cancel-activity heartbeat recorded' \
  'replay=false .*cancel=true canceled_workflow=true .*canceled_timer=false canceled_activity=true' \
  'Activity heartbeat delivered cancellation and Odin acknowledged it'

run_workflow_cancellation_case \
  cancel-cleanup \
  '"cancel-cleanup"' \
  'cancel-cleanup heartbeat recorded' \
  'replay=false .*scheduled=1 last_activity=2 .*cancel=true canceled_workflow=false .*canceled_timer=false canceled_activity=true .*waiting_activity=2' \
  'Workflow ran detached cleanup before acknowledging cancellation'

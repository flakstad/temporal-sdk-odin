#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
worker_pid=""
workflow_cli_pid=""
slow_update_pid=""
unknown_workflow_id=""
signal_failure_workflow_id=""
nondeterminism_workflow_id=""
deployment_name=""
deployment_build_id=""
server_log="${TMPDIR:-/tmp}/temporal-sdk-odin-dev-server.log"
worker_log="${TMPDIR:-/tmp}/temporal-sdk-odin-activity-worker.log"
workflow_output_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-output.XXXXXX")"
workflow_control_output_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-workflow-control.XXXXXX")"
slow_update_output_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-slow-update.XXXXXX")"
unknown_history_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-unknown-history.XXXXXX")"
typed_history_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-typed-history.XXXXXX")"
replay_history_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-replay-history.XXXXXX")"

cleanup() {
  if [[ -n "$unknown_workflow_id" ]]; then
    temporal workflow terminate \
      --workflow-id "$unknown_workflow_id" \
      --reason "Temporal Odin SDK integration cleanup" \
      --command-timeout 2s \
      --output none >/dev/null 2>&1 || true
  fi
  if [[ -n "$signal_failure_workflow_id" ]]; then
    temporal workflow terminate \
      --workflow-id "$signal_failure_workflow_id" \
      --reason "Temporal Odin SDK Signal handler failure cleanup" \
      --command-timeout 2s \
      --output none >/dev/null 2>&1 || true
  fi
  if [[ -n "$nondeterminism_workflow_id" ]]; then
    temporal workflow terminate \
      --workflow-id "$nondeterminism_workflow_id" \
      --reason "Temporal Odin SDK nondeterminism test cleanup" \
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
  if [[ -n "$deployment_name" ]]; then
    temporal worker deployment set-current-version \
      --deployment-name "$deployment_name" \
      --unversioned \
      --allow-no-pollers \
      --ignore-missing-task-queues \
      --yes \
      --command-timeout 5s \
      --output none >/dev/null 2>&1 || true
    temporal worker deployment delete-version \
      --deployment-name "$deployment_name" \
      --build-id "$deployment_build_id" \
      --skip-drainage \
      --command-timeout 5s \
      --output none >/dev/null 2>&1 || true
    temporal worker deployment delete \
      --name "$deployment_name" \
      --command-timeout 5s \
      --output none >/dev/null 2>&1 || true
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$workflow_output_file" "$workflow_control_output_file" "$slow_update_output_file" "$unknown_history_file" "$typed_history_file" "$replay_history_file"
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

eager_workflow_id="odin-sdk-eager-$$-$(date +%s)"
TEMPORAL_EAGER_WORKFLOW_ID="$eager_workflow_id" \
  TEMPORAL_REPLAY_HISTORY_OUT="$replay_history_file" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-eager-workflow.sh" >"$workflow_output_file"
eager_output="$(<"$workflow_output_file")"
echo "$eager_output"
grep -q "Eager Workflow start accepted and completed workflow_id=$eager_workflow_id" <<<"$eager_output"
grep -q "Fetched server history and replayed it offline through Core" <<<"$eager_output"
TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-replay-history.sh" \
  "$replay_history_file" >"$workflow_output_file"
replay_output="$(<"$workflow_output_file")"
echo "$replay_output"
grep -q "PASS $replay_history_file" <<<"$replay_output"
temporal workflow show \
  --workflow-id "$eager_workflow_id" \
  --command-timeout 10s \
  --output json >"$typed_history_file"
grep -q '"eagerExecutionAccepted": true' "$typed_history_file"

TEMPORAL_ODIN_SCENARIO=multiple-workflow-arguments \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Repeated-argument Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

repeated_workflow_id="odin-sdk-repeated-workflow-$$-$(date +%s)"
TEMPORAL_CLIENT_SCENARIO=multiple-workflow-arguments \
  TEMPORAL_CLIENT_WORKFLOW_ID="$repeated_workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$workflow_output_file"
repeated_workflow_output="$(<"$workflow_output_file")"
echo "$repeated_workflow_output"
grep -q "Odin client completed repeated-argument workflow_id=$repeated_workflow_id" <<<"$repeated_workflow_output"
grep -q 'result="Odin:42:true"' <<<"$repeated_workflow_output"
grep -q 'Client start interceptor observed three Workflow arguments' <<<"$repeated_workflow_output"
grep -q 'Client result interceptor exited after terminal Workflow result' <<<"$repeated_workflow_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'multiple Workflow arguments completed end-to-end through Core' "$worker_log"

TEMPORAL_ODIN_SCENARIO=multiple-signal-arguments \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Repeated-Signal Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

repeated_signal_workflow_id="odin-sdk-repeated-signal-$$-$(date +%s)"
TEMPORAL_CLIENT_SCENARIO=multiple-signal-arguments \
  TEMPORAL_CLIENT_WORKFLOW_ID="$repeated_signal_workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$workflow_output_file"
repeated_signal_output="$(<"$workflow_output_file")"
echo "$repeated_signal_output"
grep -q "Odin client completed repeated-Signal workflow_id=$repeated_signal_workflow_id" <<<"$repeated_signal_output"
grep -q 'result="Odin:42:true"' <<<"$repeated_signal_output"
grep -q 'Client Signal interceptor observed three Signal arguments' <<<"$repeated_signal_output"
grep -q 'Client result interceptor exited after terminal Workflow result' <<<"$repeated_signal_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'multiple Signal arguments completed end-to-end through Core' "$worker_log"

TEMPORAL_ODIN_SCENARIO=multiple-query-arguments \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Repeated-Query Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

repeated_query_workflow_id="odin-sdk-repeated-query-$$-$(date +%s)"
TEMPORAL_CLIENT_SCENARIO=multiple-query-arguments \
  TEMPORAL_CLIENT_WORKFLOW_ID="$repeated_query_workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$workflow_output_file"
repeated_query_output="$(<"$workflow_output_file")"
echo "$repeated_query_output"
grep -q "Odin client completed repeated-Query workflow_id=$repeated_query_workflow_id" <<<"$repeated_query_output"
grep -q 'query="Odin:42:true/repeated-client"' <<<"$repeated_query_output"
grep -q 'Client Query interceptor observed three Query arguments' <<<"$repeated_query_output"
grep -q 'Client result interceptor exited after terminal Workflow result' <<<"$repeated_query_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'multiple Query arguments completed end-to-end through Core' "$worker_log"

TEMPORAL_ODIN_SCENARIO=multiple-update-arguments \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Repeated-Update Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

repeated_update_workflow_id="odin-sdk-repeated-update-$$-$(date +%s)"
TEMPORAL_CLIENT_SCENARIO=multiple-update-arguments \
  TEMPORAL_CLIENT_WORKFLOW_ID="$repeated_update_workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$workflow_output_file"
repeated_update_output="$(<"$workflow_output_file")"
echo "$repeated_update_output"
grep -q "Odin client completed repeated-Update workflow_id=$repeated_update_workflow_id" <<<"$repeated_update_output"
grep -q 'update="Odin:42:true"' <<<"$repeated_update_output"
grep -q 'Client Update interceptor observed three Update arguments' <<<"$repeated_update_output"
grep -q 'Client result interceptor exited after terminal Workflow result' <<<"$repeated_update_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'multiple Update arguments completed end-to-end through Core' "$worker_log"

TEMPORAL_ODIN_SCENARIO=signal-with-start \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Signal-With-Start Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

signal_with_start_workflow_id="odin-sdk-signal-with-start-$$-$(date +%s)"
TEMPORAL_CLIENT_SCENARIO=signal-with-start \
  TEMPORAL_CLIENT_WORKFLOW_ID="$signal_with_start_workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$workflow_output_file"
signal_with_start_output="$(<"$workflow_output_file")"
echo "$signal_with_start_output"
grep -q "Odin Signal-With-Start created and reused workflow_id=$signal_with_start_workflow_id" <<<"$signal_with_start_output"
grep -q 'result="seed:first/first-client:second/second-client"' <<<"$signal_with_start_output"
[[ "$(grep -c 'Client Signal-With-Start interceptor entered before Core' <<<"$signal_with_start_output")" -eq 2 ]]
[[ "$(grep -c 'Client Signal-With-Start interceptor exited after Core' <<<"$signal_with_start_output")" -eq 2 ]]
grep -q 'Client result interceptor entered follow_runs=true' <<<"$signal_with_start_output"
grep -q 'Client result interceptor exited after terminal Workflow result' <<<"$signal_with_start_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "Signal-With-Start created once, reused the execution, and delivered both Signal headers" "$worker_log"
temporal workflow show \
  --workflow-id "$signal_with_start_workflow_id" \
  --command-timeout 10s \
  --output json >"$typed_history_file"
signal_event_count="$(grep -c 'EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED' "$typed_history_file")"
if [[ "$signal_event_count" -ne 2 ]]; then
  echo "Signal-With-Start history contained $signal_event_count Signal events, expected 2" >&2
  exit 1
fi

TEMPORAL_ODIN_SCENARIO=activity-headers \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Activity-header Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

activity_header_workflow_id="odin-sdk-activity-header-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$activity_header_workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"activity-header"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
activity_header_output="$(<"$workflow_output_file")"
echo "$activity_header_output"
grep -q '"result": "Activity header propagated"' <<<"$activity_header_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "Activity header crossed Workflow command, Core, and Activity Context" "$worker_log"

TEMPORAL_ODIN_SCENARIO=activity-interceptor \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Activity-interceptor Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

activity_interceptor_workflow_id="odin-sdk-activity-interceptor-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$activity_interceptor_workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"activity-interceptor"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
activity_interceptor_output="$(<"$workflow_output_file")"
echo "$activity_interceptor_output"
grep -q '"result": "Activity interceptor executed"' <<<"$activity_interceptor_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "Activity execute interceptor entered before callback" "$worker_log"
grep -q "Activity execute interceptor exited after callback status=Completed" "$worker_log"
grep -q "Activity heartbeat interceptor entered before Core" "$worker_log"
grep -q "Activity heartbeat interceptor exited after Core status=Recorded" "$worker_log"
grep -q "Workflow Activity interceptor entered before command" "$worker_log"
grep -q "Workflow Activity interceptor exited after command" "$worker_log"
grep -q "Workflow execute interceptor entered before Workflow" "$worker_log"
grep -q "Workflow execute interceptor exited after Workflow" "$worker_log"

TEMPORAL_ODIN_SCENARIO=multiple-activity-arguments \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Multi-argument Activity Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

multiple_arguments_workflow_id="odin-sdk-multiple-arguments-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$multiple_arguments_workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"multiple-activity-arguments"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
multiple_arguments_output="$(<"$workflow_output_file")"
echo "$multiple_arguments_output"
grep -q '"result": "Odin:42:true"' <<<"$multiple_arguments_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "multiple Activity arguments completed end-to-end through Core" "$worker_log"

TEMPORAL_ODIN_SCENARIO=signal-headers \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Signal-header Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

signal_header_workflow_id="odin-sdk-signal-header-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$signal_header_workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"signal-header"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file" 2>&1 &
workflow_cli_pid=$!
for _ in {1..30}; do
  if grep -Eq "started_timer=true .*waiting_timer=1 waiting_signal=true" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Signal-header Workflow did not reach its Signal await" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done
TEMPORAL_CLIENT_ACTION=signal \
  TEMPORAL_CLIENT_WORKFLOW_ID="$signal_header_workflow_id" \
  TEMPORAL_CLIENT_SIGNAL_NAME=approval \
  TEMPORAL_CLIENT_SIGNAL_INPUT=Approved \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh"
wait "$workflow_cli_pid"
workflow_cli_pid=""
signal_header_output="$(<"$workflow_output_file")"
echo "$signal_header_output"
grep -q '"result": "Hello Approved"' <<<"$signal_header_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "signal won the durable timer race and replay reproduced timer cancellation" "$worker_log"

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

TEMPORAL_ODIN_SCENARIO=local-activity \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Local Activity Odin Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

local_activity_workflow_id="odin-sdk-local-activity-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$local_activity_workflow_id" \
  --type odin_local_activity_workflow \
  --task-queue odin-hello-workflow \
  --input '"Local"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
local_activity_output="$(<"$workflow_output_file")"
echo "$local_activity_output"
grep -q '"status": "COMPLETED"' <<<"$local_activity_output"
grep -q 'Hello Local' <<<"$local_activity_output"
temporal workflow show \
  --workflow-id "$local_activity_workflow_id" \
  --command-timeout 10s \
  --output json >"$typed_history_file"
grep -q 'core_local_activity' "$typed_history_file"
grep -q '"input"' "$typed_history_file"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -Eq 'replay=false .*local_scheduled=1 .*waiting_activity=1' "$worker_log"
grep -q "Local Activity completed through Core in the Workflow Worker" "$worker_log"

TEMPORAL_ODIN_SCENARIO=local-activity-retry \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Retrying Local Activity Odin Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

local_activity_retry_workflow_id="odin-sdk-local-activity-retry-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$local_activity_retry_workflow_id" \
  --type odin_local_activity_retry_workflow \
  --task-queue odin-hello-workflow \
  --input '"Retry"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
local_activity_retry_output="$(<"$workflow_output_file")"
echo "$local_activity_retry_output"
grep -q '"status": "COMPLETED"' <<<"$local_activity_retry_output"
grep -q 'Hello after Local Activity retry, Retry' <<<"$local_activity_retry_output"
temporal workflow show \
  --workflow-id "$local_activity_retry_workflow_id" \
  --command-timeout 10s \
  --output json >"$typed_history_file"
grep -q 'core_local_activity' "$typed_history_file"
grep -q 'EVENT_TYPE_TIMER_STARTED' "$typed_history_file"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -Eq 'replay=false .*started_timer=true .*waiting_activity=1' "$worker_log"
grep -Eq 'replay=false .*local_scheduled=1 .*last_activity=2 .*waiting_activity=1' "$worker_log"
grep -q "Local Activity timer-backed retry completed through Core and Temporal Server" "$worker_log"

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

TEMPORAL_ODIN_SCENARIO=terminal-failure \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Terminal-failure Odin Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

terminal_failure_workflow_id="odin-sdk-terminal-failure-$$-$(date +%s)"
TEMPORAL_CLIENT_SCENARIO=terminal-failure \
  TEMPORAL_CLIENT_WORKFLOW_ID="$terminal_failure_workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$workflow_output_file"
terminal_failure_output="$(<"$workflow_output_file")"
echo "$terminal_failure_output"
grep -q "Odin client decoded recursive terminal failure workflow_id=$terminal_failure_workflow_id" <<<"$terminal_failure_output"
grep -q "activity_id=terminal-failure-activity application_type=DependencyRejected" <<<"$terminal_failure_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "unhandled Activity failure reached the Client as a recursive Temporal Failure" "$worker_log"

TEMPORAL_ODIN_SCENARIO=activity-cancel-details \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Activity-cancellation-details Odin Workflow Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

cancel_details_workflow_id="odin-sdk-activity-cancel-details-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$cancel_details_workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"activity-cancel-details"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
cancel_details_output="$(<"$workflow_output_file")"
echo "$cancel_details_output"
grep -q '"status": "COMPLETED"' <<<"$cancel_details_output"
grep -q 'Observed Activity cancellation details' <<<"$cancel_details_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "Activity cancellation flags and user details completed end-to-end" "$worker_log"

TEMPORAL_ODIN_SCENARIO=patch \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Patch Odin Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

patch_workflow_id="odin-sdk-patch-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$patch_workflow_id" \
  --type odin_patch_workflow \
  --task-queue odin-hello-workflow \
  --input '"Odin"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
patch_workflow_output="$(<"$workflow_output_file")"
echo "$patch_workflow_output"
grep -q '"status": "COMPLETED"' <<<"$patch_workflow_output"
grep -q 'Patched Odin' <<<"$patch_workflow_output"
temporal workflow show \
  --workflow-id "$patch_workflow_id" \
  --command-timeout 10s \
  --output json >"$typed_history_file"
grep -q 'core_patch' "$typed_history_file"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -Eq 'replay=false .*patches=1 deprecated_patches=0 .*completed=true' "$worker_log"
grep -q "Workflow patch marker completed through Core and Temporal Server" "$worker_log"

TEMPORAL_ODIN_SCENARIO=patch \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Schedule-trigger Odin Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

schedule_id="odin-sdk-schedule-$$-$(date +%s)"
TEMPORAL_SCHEDULE_ID="$schedule_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-schedule-lifecycle.sh" >"$workflow_output_file"
schedule_output="$(<"$workflow_output_file")"
echo "$schedule_output"
grep -q "Odin Schedule lifecycle completed schedule_id=$schedule_id" <<<"$schedule_output"
grep -q 'updated=true' <<<"$schedule_output"
grep -q 'conflict_token_rotated=true' <<<"$schedule_output"
grep -q 'stale_conflict_guarded=true' <<<"$schedule_output"
grep -Eq 'matching_times=[3-9][0-9]*' <<<"$schedule_output"
grep -q 'listed=true' <<<"$schedule_output"
grep -q 'paused_unpaused=true' <<<"$schedule_output"
grep -q 'Patched Scheduled' <<<"$schedule_output"
grep -q 'deleted=true' <<<"$schedule_output"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -Eq 'replay=false .*patches=1 deprecated_patches=0 .*completed=true' "$worker_log"
grep -q "Workflow patch marker completed through Core and Temporal Server" "$worker_log"

if ! temporal operator search-attribute list \
  --command-timeout 10s \
  --output json | grep -q 'OdinSdkKeyword'; then
  temporal operator search-attribute create \
    --name OdinSdkKeyword \
    --type Keyword \
    --command-timeout 10s \
    --output none
fi

TEMPORAL_ODIN_SCENARIO=workflow-properties \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Workflow-properties Odin Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

properties_workflow_id="odin-sdk-workflow-properties-$$-$(date +%s)"
TEMPORAL_CLIENT_SCENARIO=workflow-properties \
  TEMPORAL_CLIENT_WORKFLOW_ID="$properties_workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-start.sh" >"$workflow_output_file"
properties_workflow_output="$(<"$workflow_output_file")"
echo "$properties_workflow_output"
grep -q "Odin client completed workflow-properties workflow_id=$properties_workflow_id" <<<"$properties_workflow_output"
grep -q 'use_existing=true' <<<"$properties_workflow_output"
grep -q 'query_rejected_status=Completed' <<<"$properties_workflow_output"
grep -q 'static_metadata=true' <<<"$properties_workflow_output"
grep -q 'describe_status=Completed' <<<"$properties_workflow_output"
grep -q 'visibility_listed=true' <<<"$properties_workflow_output"
grep -Eq 'visibility_count=[1-9][0-9]*' <<<"$properties_workflow_output"
grep -Eq 'history_events=[1-9][0-9]* history_pages=([2-9]|[1-9][0-9]+)' <<<"$properties_workflow_output"
grep -q 'Workflow properties updated' <<<"$properties_workflow_output"
temporal workflow show \
  --workflow-id "$properties_workflow_id" \
  --command-timeout 10s \
  --output json >"$typed_history_file"
grep -q 'EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED' "$typed_history_file"
grep -q 'EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES' "$typed_history_file"
grep -q 'odin-status' "$typed_history_file"
grep -q 'OdinSdkKeyword' "$typed_history_file"
grep -q 'odin-header' "$typed_history_file"
grep -q 'firstWorkflowTaskBackoff' "$typed_history_file"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -Eq 'replay=false .*memo_upserts=2 search_upserts=1 .*completed=true' "$worker_log"
grep -q "Workflow info, memo, and Search Attributes completed through Core and Temporal Server" "$worker_log"

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
unknown_terminate_output="$(TEMPORAL_CLIENT_ACTION=terminate \
  TEMPORAL_CLIENT_WORKFLOW_ID="$unknown_workflow_id" \
  TEMPORAL_CLIENT_REASON="unknown Workflow type test completed" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh")"
echo "$unknown_terminate_output"
grep -q 'Client terminate interceptor entered reason="unknown Workflow type test completed" has_details=false' <<<"$unknown_terminate_output"
grep -q 'Client terminate interceptor exited after Core' <<<"$unknown_terminate_output"
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
  if grep -Eq "replay=false .*scheduled=1 local_scheduled=0 last_activity=1 .*updates_accepted=1 updates_rejected=0 updates_completed=0" "$worker_log" 2>/dev/null; then
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
grep -q 'caller="odin-client-control"' <<<"$fast_update_output"

wait "$slow_update_pid"
slow_update_pid=""
slow_update_output="$(<"$slow_update_output_file")"
echo "$slow_update_output"
grep -q "Odin client update=set_greeting_name workflow_id=$workflow_id" <<<"$slow_update_output"
grep -q 'previous="Odin" current="Temporal-slow"' <<<"$slow_update_output"
grep -q 'caller="odin-client-control"' <<<"$slow_update_output"

TEMPORAL_CLIENT_ACTION=query \
  TEMPORAL_CLIENT_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_CLIENT_QUERY_NAME=greeting_status \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$workflow_control_output_file"
query_output="$(<"$workflow_control_output_file")"
echo "$query_output"
grep -q "Odin client query=greeting_status workflow_id=$workflow_id" <<<"$query_output"
grep -q 'phase="waiting-for-approval" name="Temporal-slow"' <<<"$query_output"
grep -q 'caller="odin-client-control"' <<<"$query_output"
grep -q "Workflow Query interceptor entered before handler" "$worker_log"
grep -q "Workflow Query interceptor exited after handler" "$worker_log"

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
grep -Eq "replay=false .*scheduled=1 local_scheduled=0 last_activity=1 .*updates_accepted=1 updates_rejected=0 updates_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*scheduled=1 local_scheduled=0 last_activity=2 .*updates_accepted=1 updates_rejected=0 updates_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*resolved=2 .*updates_accepted=0 updates_rejected=0 updates_completed=1 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*resolved=1 .*updates_accepted=0 updates_rejected=0 updates_completed=1 .*waiting_signal=true" "$worker_log"
grep -q "Workflow Update validator interceptor entered" "$worker_log"
grep -q "Workflow Update validator interceptor exited" "$worker_log"
grep -q "Workflow Update handler interceptor entered before handler" "$worker_log"
grep -q "Workflow Update handler interceptor exited after handler" "$worker_log"
grep -Eq "replay=true .*scheduled=1 local_scheduled=0 last_activity=1 .*updates_accepted=1 updates_rejected=0 updates_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=true .*scheduled=1 local_scheduled=0 last_activity=2 .*updates_accepted=1 updates_rejected=0 updates_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=true .*resolved=2 .*updates_accepted=0 updates_rejected=0 updates_completed=1 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=true .*resolved=1 .*updates_accepted=0 updates_rejected=0 updates_completed=1 .*waiting_signal=true" "$worker_log"
grep -q "signal won the durable timer race and replay reproduced timer cancellation" "$worker_log"

TEMPORAL_ODIN_SCENARIO=signal-handler \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Signal-handler Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

signal_handler_workflow_id="odin-sdk-signal-handler-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$signal_handler_workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"late-signal-handler"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file" 2>&1 &
workflow_cli_pid=$!

for _ in {1..30}; do
  if grep -Eq "replay=false .*started_timer=false .*waiting_signal=true" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Signal-handler Workflow did not reach its main await" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

TEMPORAL_CLIENT_ACTION=signal \
  TEMPORAL_CLIENT_WORKFLOW_ID="$signal_handler_workflow_id" \
  TEMPORAL_CLIENT_SIGNAL_NAME=set_greeting_name_async \
  TEMPORAL_CLIENT_SIGNAL_INPUT=Temporal-slow \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh"

for _ in {1..30}; do
  if grep -Eq "replay=false .*signal=true signal_handlers_started=0 signal_handlers_completed=0 .*waiting_signal=true" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Signal sent before handler registration was not buffered" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done

TEMPORAL_CLIENT_ACTION=signal \
  TEMPORAL_CLIENT_WORKFLOW_ID="$signal_handler_workflow_id" \
  TEMPORAL_CLIENT_SIGNAL_NAME=register_signal_handler \
  TEMPORAL_CLIENT_SIGNAL_INPUT=ready \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh"

for _ in {1..60}; do
  if grep -Eq "replay=false .*resolved=1 .*signal_handlers_started=0 signal_handlers_completed=1 .*waiting_signal=true" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Asynchronous Signal handler did not finish its Activity" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done

TEMPORAL_CLIENT_ACTION=query \
  TEMPORAL_CLIENT_WORKFLOW_ID="$signal_handler_workflow_id" \
  TEMPORAL_CLIENT_QUERY_NAME=greeting_status \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh" >"$workflow_control_output_file"
signal_query_output="$(<"$workflow_control_output_file")"
echo "$signal_query_output"
grep -q "Odin client query=greeting_status workflow_id=$signal_handler_workflow_id" <<<"$signal_query_output"
grep -q 'phase="waiting-for-approval" name="Temporal-slow"' <<<"$signal_query_output"
grep -q 'caller="odin-client-control"' <<<"$signal_query_output"

TEMPORAL_CLIENT_ACTION=signal \
  TEMPORAL_CLIENT_WORKFLOW_ID="$signal_handler_workflow_id" \
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
grep -Eq "replay=false .*scheduled=1 local_scheduled=0 last_activity=1 .*signal=true signal_handlers_started=1 signal_handlers_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*resolved=1 .*signal_handlers_started=0 signal_handlers_completed=1 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=true .*signal_handlers_started=1 signal_handlers_completed=0 .*waiting_signal=true" "$worker_log"
grep -Eq "replay=false .*signal=true signal_handlers_started=0 signal_handlers_completed=0 .*waiting_signal=true" "$worker_log"
grep -q "buffered asynchronous Workflow Signal handler registered late, awaited an Activity, and replayed" "$worker_log"
grep -q "Workflow Signal interceptor entered before handler" "$worker_log"
grep -q "Workflow Signal interceptor exited after handler" "$worker_log"

TEMPORAL_ODIN_SCENARIO=signal-handler-failure \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Signal-handler-failure Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

signal_failure_workflow_id="odin-sdk-signal-handler-failure-$$-$(date +%s)"
temporal workflow start \
  --workflow-id "$signal_failure_workflow_id" \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"failing-signal-handler"' \
  --command-timeout 10s \
  --output none

for _ in {1..30}; do
  if grep -Eq "replay=false .*waiting_signal=true" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Failing Signal-handler Workflow did not reach its await" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done

TEMPORAL_CLIENT_ACTION=signal \
  TEMPORAL_CLIENT_WORKFLOW_ID="$signal_failure_workflow_id" \
  TEMPORAL_CLIENT_SIGNAL_NAME=fail_signal \
  TEMPORAL_CLIENT_SIGNAL_INPUT=fail \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh"

for _ in {1..100}; do
  temporal workflow show \
    --workflow-id "$signal_failure_workflow_id" \
    --command-timeout 2s \
    --output json >"$unknown_history_file" 2>/dev/null || true
  signal_handler_failure_attempts="$(grep -c 'activation_failed=true error="intentional Signal handler failure"' "$worker_log" 2>/dev/null || true)"
  if [[ "$signal_handler_failure_attempts" -ge 2 ]] && \
     grep -Eq 'replay=true .*waiting_signal=true' "$worker_log" 2>/dev/null && \
     grep -q 'activation_failed=false .*evicted=true' "$worker_log" 2>/dev/null && \
     grep -q 'EVENT_TYPE_WORKFLOW_TASK_FAILED' "$unknown_history_file" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Signal handler failure terminated the Odin Worker" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done

grep -q 'activation_failed=true error="intentional Signal handler failure"' "$worker_log"
grep -q 'activation_failed=false .*evicted=true' "$worker_log"
grep -q 'EVENT_TYPE_WORKFLOW_TASK_FAILED' "$unknown_history_file"
signal_handler_failure_events="$(grep -c '"eventType": "EVENT_TYPE_WORKFLOW_TASK_FAILED"' "$unknown_history_file" || true)"
if [[ "$signal_handler_failure_events" -ne 1 ]]; then
  echo "Expected Core to report exactly one Workflow Task failure, got $signal_handler_failure_events" >&2
  exit 1
fi
kill -0 "$worker_pid"

signal_failure_terminate_output="$(TEMPORAL_CLIENT_ACTION=terminate \
  TEMPORAL_CLIENT_WORKFLOW_ID="$signal_failure_workflow_id" \
  TEMPORAL_CLIENT_REASON="Signal handler failure test completed" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh")"
echo "$signal_failure_terminate_output"
grep -q 'Client terminate interceptor entered reason="Signal handler failure test completed" has_details=false' <<<"$signal_failure_terminate_output"
grep -q 'Client terminate interceptor exited after Core' <<<"$signal_failure_terminate_output"
signal_failure_workflow_id=""

signal_failure_recovery_workflow_id="odin-sdk-signal-handler-recovery-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$signal_failure_recovery_workflow_id" \
  --type odin_patch_workflow \
  --task-queue odin-hello-workflow \
  --input '"Recovered"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
signal_failure_recovery_output="$(<"$workflow_output_file")"
echo "$signal_failure_recovery_output"
grep -q '"status": "COMPLETED"' <<<"$signal_failure_recovery_output"
grep -q '"result": "Patched Recovered ' <<<"$signal_failure_recovery_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'activation_failed=true error="intentional Signal handler failure"' "$worker_log"
grep -Eq 'replay=true .*waiting_signal=true' "$worker_log"
signal_handler_failure_attempts="$(grep -c 'activation_failed=true error="intentional Signal handler failure"' "$worker_log" || true)"
if [[ "$signal_handler_failure_attempts" -lt 2 ]]; then
  echo "Expected Signal handler failure before and after fresh-stack replay" >&2
  exit 1
fi
grep -q "Signal handler failure triggered Core eviction and the Worker recovered on a fresh Workflow stack" "$worker_log"

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
  if grep -Eq "replay=false .*scheduled=1 local_scheduled=0 last_activity=1 .*updates_accepted=1 updates_rejected=0 updates_completed=0" "$worker_log" 2>/dev/null; then
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

cancel_update_output="$(TEMPORAL_CLIENT_ACTION=cancel \
  TEMPORAL_CLIENT_WORKFLOW_ID="$cancel_update_workflow_id" \
  TEMPORAL_CLIENT_REASON="drain detached Update" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh")"
echo "$cancel_update_output"
grep -q 'Client cancel interceptor entered reason="drain detached Update"' <<<"$cancel_update_output"
grep -q 'Client cancel interceptor exited after Core' <<<"$cancel_update_output"

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
  'replay=false .*scheduled=1 local_scheduled=0 last_activity=2 .*cancel=true canceled_workflow=false .*canceled_timer=false canceled_activity=true .*waiting_activity=2' \
  'Workflow ran detached cleanup before acknowledging cancellation'

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin Child Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

child_parent_id="odin-sdk-child-parent-$$-$(date +%s)"
child_input="Odin-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$child_parent_id" \
  --type odin_parent_workflow \
  --task-queue odin-child-workflow \
  --input "\"$child_input\"" \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
child_output="$(<"$workflow_output_file")"
echo "$child_output"
grep -q '"status": "COMPLETED"' <<<"$child_output"
grep -q "Hello from child, $child_input" <<<"$child_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "Workflow start-Child interceptor entered" "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin repeated-argument Child Workflow Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

repeated_child_parent_id="odin-sdk-repeated-child-parent-$$-$(date +%s)"
repeated_child_input="Odin-repeated-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$repeated_child_parent_id" \
  --type odin_repeated_child_parent_workflow \
  --task-queue odin-child-workflow \
  --input "\"$repeated_child_input\"" \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
repeated_child_output="$(<"$workflow_output_file")"
echo "$repeated_child_output"
grep -q '"status": "COMPLETED"' <<<"$repeated_child_output"
grep -q "Repeated child received $repeated_child_input:42:true" <<<"$repeated_child_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "Workflow start-Child interceptor entered" "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin Child Workflow failure Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

child_failure_parent_id="odin-sdk-child-failure-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$child_failure_parent_id" \
  --type odin_child_failure_parent_workflow \
  --task-queue odin-child-workflow \
  --input '"Odin"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
child_failure_output="$(<"$workflow_output_file")"
echo "$child_failure_output"
grep -q '"status": "COMPLETED"' <<<"$child_failure_output"
grep -q 'Observed structured Child Workflow failure' <<<"$child_failure_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin Child Workflow cancellation Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

child_cancel_suffix="$$-$(date +%s)"
child_cancel_parent_id="odin-sdk-child-cancel-$child_cancel_suffix"
temporal workflow execute \
  --workflow-id "$child_cancel_parent_id" \
  --type odin_parent_workflow \
  --task-queue odin-child-workflow \
  --input "\"cancel-$child_cancel_suffix\"" \
  --command-timeout 30s \
  --output json >"$workflow_output_file" 2>&1 &
workflow_cli_pid=$!

for _ in {1..30}; do
  if grep -q 'start_resolved=1' "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Child Workflow did not start before parent cancellation" >&2
    cat "$worker_log" >&2
    cat "$workflow_output_file" >&2
    exit 1
  fi
  sleep 1
done

TEMPORAL_CLIENT_ACTION=cancel \
  TEMPORAL_CLIENT_WORKFLOW_ID="$child_cancel_parent_id" \
  TEMPORAL_CLIENT_REASON="Odin Child Workflow integration cancellation" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-client-control.sh"

child_cancel_cli_status=0
wait "$workflow_cli_pid" || child_cancel_cli_status=$?
workflow_cli_pid=""
child_cancel_output="$(<"$workflow_output_file")"
echo "$child_cancel_output"
[[ "$child_cancel_cli_status" -eq 0 || "$child_cancel_cli_status" -eq 1 ]]
grep -q '"status": "CANCELED"' <<<"$child_cancel_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'cancel_requested=true canceled_child=1' "$worker_log"
grep -q 'resolved=1 .*canceled_workflow=true' "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin Child Workflow signal Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

child_signal_parent_id="odin-sdk-child-signal-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$child_signal_parent_id" \
  --type odin_child_signal_parent_workflow \
  --task-queue odin-child-workflow \
  --input '"Odin"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
child_signal_output="$(<"$workflow_output_file")"
echo "$child_signal_output"
grep -q '"status": "COMPLETED"' <<<"$child_signal_output"
grep -q 'Odin received approval-from-parent:42:true' <<<"$child_signal_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'signaled=1' "$worker_log"
grep -q 'signal_resolved=1' "$worker_log"
grep -q 'Workflow signal-Child interceptor entered' "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin explicit Child Workflow cancellation Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

child_explicit_cancel_parent_id="odin-sdk-child-explicit-cancel-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$child_explicit_cancel_parent_id" \
  --type odin_child_explicit_cancel_parent_workflow \
  --task-queue odin-child-workflow \
  --input '"cancel-explicit"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
child_explicit_cancel_output="$(<"$workflow_output_file")"
echo "$child_explicit_cancel_output"
grep -q '"status": "COMPLETED"' <<<"$child_explicit_cancel_output"
grep -q 'Observed explicit Child Workflow cancellation' <<<"$child_explicit_cancel_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'cancel_requested=true canceled_child=1' "$worker_log"
grep -q 'resolved=1 .*completed=true' "$worker_log"
grep -q 'Workflow sleep interceptor entered' "$worker_log"
grep -q 'Workflow sleep interceptor exited' "$worker_log"
grep -q 'Workflow cancel-Child interceptor entered' "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin duplicate Child Workflow Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

child_duplicate_parent_id="odin-sdk-child-duplicate-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$child_duplicate_parent_id" \
  --type odin_child_duplicate_parent_workflow \
  --task-queue odin-child-workflow \
  --input '"cancel-duplicate"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
child_duplicate_output="$(<"$workflow_output_file")"
echo "$child_duplicate_output"
grep -q '"status": "COMPLETED"' <<<"$child_duplicate_output"
grep -q 'Observed Child Workflow Already Exists start failure' <<<"$child_duplicate_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'started=1 start_resolved=0' "$worker_log"
grep -q 'start_resolved=2' "$worker_log"
grep -q 'cancel_requested=true canceled_child=1' "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin external Workflow operation Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

external_suffix="$$-$(date +%s)"
external_target_id="odin-sdk-external-target-$external_suffix"
external_caller_id="odin-sdk-external-caller-$external_suffix"
temporal workflow execute \
  --workflow-id "$external_target_id" \
  --type odin_external_target_workflow \
  --task-queue odin-child-workflow \
  --input '"target"' \
  --command-timeout 30s \
  --output json >"$workflow_control_output_file" 2>&1 &
workflow_cli_pid=$!

for _ in {1..30}; do
  if grep -Eq 'history=3 .*completed=false' "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "External target Workflow did not reach its signal await" >&2
    cat "$worker_log" >&2
    cat "$workflow_control_output_file" >&2
    exit 1
  fi
  sleep 1
done

temporal workflow execute \
  --workflow-id "$external_caller_id" \
  --type odin_external_caller_workflow \
  --task-queue odin-child-workflow \
  --input "\"$external_target_id\"" \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
external_caller_output="$(<"$workflow_output_file")"
echo "$external_caller_output"
grep -q '"status": "COMPLETED"' <<<"$external_caller_output"
grep -q 'External Workflow signaled and cancellation requested' <<<"$external_caller_output"

external_target_cli_status=0
wait "$workflow_cli_pid" || external_target_cli_status=$?
workflow_cli_pid=""
external_target_output="$(<"$workflow_control_output_file")"
echo "$external_target_output"
[[ "$external_target_cli_status" -eq 0 || "$external_target_cli_status" -eq 1 ]]
grep -q '"status": "CANCELED"' <<<"$external_target_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'signaled=1' "$worker_log"
grep -q 'signal_resolved=1' "$worker_log"
grep -q 'external_cancel_requested=true' "$worker_log"
grep -q 'external_cancel_resolved=1' "$worker_log"
grep -q 'Workflow signal-external interceptor entered' "$worker_log"
grep -q 'Workflow cancel-external interceptor entered' "$worker_log"
grep -q 'canceled_workflow=true' "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin Continue-As-New Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

continue_workflow_id="odin-sdk-continue-as-new-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$continue_workflow_id" \
  --type odin_continue_as_new_workflow \
  --task-queue odin-child-workflow \
  --input '"first-run"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
continue_output="$(<"$workflow_output_file")"
echo "$continue_output"
grep -q '"status": "COMPLETED"' <<<"$continue_output"
grep -q 'Continue-As-New completed on continued-run' <<<"$continue_output"
temporal workflow describe \
  --workflow-id "$continue_workflow_id" \
  --command-timeout 10s \
  --output json >"$typed_history_file"
grep -q 'odin-continue-memo' "$typed_history_file"
grep -q 'carried' "$typed_history_file"
grep -q 'OdinSdkKeyword' "$typed_history_file"
grep -q 'continue-as-new' "$typed_history_file"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'continued_as_new=true' "$worker_log"
grep -q 'continued_as_new=false completed=true' "$worker_log"
grep -q 'Workflow Continue-As-New interceptor entered' "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Odin repeated Continue-As-New Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

repeated_continue_workflow_id="odin-sdk-repeated-continue-as-new-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$repeated_continue_workflow_id" \
  --type odin_repeated_continue_as_new_workflow \
  --task-queue odin-child-workflow \
  --input '"first-run"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
repeated_continue_output="$(<"$workflow_output_file")"
echo "$repeated_continue_output"
grep -q '"status": "COMPLETED"' <<<"$repeated_continue_output"
grep -q 'Continue-As-New received continued-run:42:true' <<<"$repeated_continue_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q 'continued_as_new=true' "$worker_log"
grep -q 'continued_as_new=false completed=true' "$worker_log"
grep -q 'Workflow Continue-As-New interceptor entered' "$worker_log"
grep -q "Child Workflow completed end-to-end and worker shut down cleanly" "$worker_log"

nondeterminism_task_queue="odin-sdk-nondeterminism-queue-$$-$(date +%s)"
nondeterminism_workflow_id="odin-sdk-nondeterminism-$$-$(date +%s)"
TEMPORAL_ODIN_SCENARIO=nondeterminism-seed \
  TEMPORAL_ODIN_TASK_QUEUE="$nondeterminism_task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Nondeterminism seed Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

temporal workflow start \
  --workflow-id "$nondeterminism_workflow_id" \
  --type odin_nondeterminism_workflow \
  --task-queue "$nondeterminism_task_queue" \
  --input '"Odin"' \
  --command-timeout 10s \
  --output none

for _ in {1..30}; do
  temporal workflow show \
    --workflow-id "$nondeterminism_workflow_id" \
    --command-timeout 2s \
    --output json >"$typed_history_file" 2>/dev/null || true
  if grep -q 'EVENT_TYPE_TIMER_STARTED' "$typed_history_file" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Nondeterminism seed Worker exited before recording its Timer" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
grep -q 'EVENT_TYPE_TIMER_STARTED' "$typed_history_file"
kill "$worker_pid"
wait "$worker_pid" 2>/dev/null || true
worker_pid=""

TEMPORAL_ODIN_SCENARIO=nondeterminism-fail \
  TEMPORAL_ODIN_TASK_QUEUE="$nondeterminism_task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Nondeterminism failure Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done

temporal workflow signal \
  --workflow-id "$nondeterminism_workflow_id" \
  --name force-replay \
  --input '"replay"' \
  --command-timeout 10s \
  --output none

for _ in {1..50}; do
  temporal workflow describe \
    --workflow-id "$nondeterminism_workflow_id" \
    --command-timeout 2s \
    --output json >"$workflow_output_file" 2>/dev/null || true
  temporal workflow show \
    --workflow-id "$nondeterminism_workflow_id" \
    --command-timeout 2s \
    --output json >"$typed_history_file" 2>/dev/null || true
  if grep -q '"status": "WORKFLOW_EXECUTION_STATUS_FAILED"' "$workflow_output_file" 2>/dev/null &&
     grep -Eq 'Nondeterminism|TMPRL1100' "$typed_history_file" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
grep -q '"status": "WORKFLOW_EXECUTION_STATUS_FAILED"' "$workflow_output_file"
grep -Eq 'Nondeterminism|TMPRL1100' "$typed_history_file"
nondeterminism_workflow_id=""

for _ in {1..30}; do
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if kill -0 "$worker_pid" 2>/dev/null; then
  echo "Nondeterminism failure Worker did not finish after terminal Workflow failure" >&2
  cat "$worker_log" >&2
  exit 1
fi
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "Core converted selected Workflow nondeterminism into terminal failure" "$worker_log"

# Keep this scenario last because cleanup is best-effort. Its unique task queue
# also prevents persisted routing state from affecting the normal example queue.
deployment_name="odin-sdk-deployment-$$-$(date +%s)"
deployment_build_id="odin-sdk-build-v1"
deployment_task_queue="odin-sdk-deployment-queue-$$-$(date +%s)"
TEMPORAL_ODIN_SCENARIO=deployment-versioning \
  TEMPORAL_ODIN_DEPLOYMENT_NAME="$deployment_name" \
  TEMPORAL_ODIN_DEPLOYMENT_BUILD_ID="$deployment_build_id" \
  TEMPORAL_ODIN_TASK_QUEUE="$deployment_task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!
for _ in {1..30}; do
  if grep -q "Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Deployment-versioned Odin Workflow Worker exited before polling; see $worker_log" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 1
done
sleep 1

temporal worker deployment set-current-version \
  --deployment-name "$deployment_name" \
  --build-id "$deployment_build_id" \
  --allow-no-pollers \
  --ignore-missing-task-queues \
  --yes \
  --command-timeout 10s \
  --output none

deployment_workflow_id="odin-sdk-deployment-workflow-$$-$(date +%s)"
temporal workflow execute \
  --workflow-id "$deployment_workflow_id" \
  --type odin_patch_workflow \
  --task-queue "$deployment_task_queue" \
  --input '"Versioned"' \
  --command-timeout 30s \
  --output json >"$workflow_output_file"
deployment_workflow_output="$(<"$workflow_output_file")"
echo "$deployment_workflow_output"
grep -q '"status": "COMPLETED"' <<<"$deployment_workflow_output"
grep -q 'Patched Versioned' <<<"$deployment_workflow_output"
temporal workflow describe \
  --workflow-id "$deployment_workflow_id" \
  --command-timeout 10s \
  --output json >"$typed_history_file"
grep -q "$deployment_name" "$typed_history_file"
grep -q "$deployment_build_id" "$typed_history_file"
wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q "Worker Deployment Versioning routed a Workflow through Core and Temporal Server" "$worker_log"

temporal worker deployment set-current-version \
  --deployment-name "$deployment_name" \
  --unversioned \
  --allow-no-pollers \
  --ignore-missing-task-queues \
  --yes \
  --command-timeout 10s \
  --output none >/dev/null 2>&1 || true
temporal worker deployment delete-version \
  --deployment-name "$deployment_name" \
  --build-id "$deployment_build_id" \
  --skip-drainage \
  --command-timeout 10s \
  --output none >/dev/null 2>&1 || true
temporal worker deployment delete \
  --name "$deployment_name" \
  --command-timeout 10s \
  --output none >/dev/null 2>&1 || true
deployment_name=""
deployment_build_id=""

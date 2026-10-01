#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
worker_pid=""
workflow_cli_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-scoped-metrics-server.XXXXXX")"
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-scoped-metrics-worker.XXXXXX")"
workflow_output="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-scoped-metrics-result.XXXXXX")"
metrics_output="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-scoped-metrics-scrape.XXXXXX")"

cleanup() {
  if [[ -n "$workflow_cli_pid" ]]; then
    kill "$workflow_cli_pid" 2>/dev/null || true
    wait "$workflow_cli_pid" 2>/dev/null || true
  fi
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$worker_log" "$workflow_output" "$metrics_output"
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
fi

metrics_port=$((20000 + $$ % 10000))
metrics_address="127.0.0.1:${metrics_port}"
task_queue="odin-scoped-metrics-${RANDOM}-$(date +%s)"
workflow_id="odin-scoped-metrics-${RANDOM}-$(date +%s)"

TEMPORAL_ODIN_PROMETHEUS_BIND_ADDRESS="$metrics_address" \
  TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SCENARIO=scoped-metrics \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-workflow.sh" >"$worker_log" 2>&1 &
worker_pid=$!

for _ in {1..100}; do
  if grep -q "Workflow Worker is polling task queue $task_queue" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Scoped-metrics Worker exited before polling" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done

temporal workflow execute \
  --workflow-id "$workflow_id" \
  --type odin_hello_workflow \
  --task-queue "$task_queue" \
  --input '"scoped-metrics"' \
  --command-timeout 20s \
  --output json >"$workflow_output" 2>&1 &
workflow_cli_pid=$!

# Wait until Core has evicted the first stack, replayed history on a fresh one,
# resolved the Activity, and accepted the durable timer command. Scraping before
# this point would prove emission but not replay suppression.
replay_observed=false
timer_observed=false
for _ in {1..200}; do
  if grep -Eq 'replay=true .*scheduled=1 ' "$worker_log" 2>/dev/null; then
    replay_observed=true
  fi
  if grep -Eq 'replay=false .*resolved=1 .*started_timer=true ' "$worker_log" 2>/dev/null; then
    timer_observed=true
  fi
  if [[ "$replay_observed" == true && "$timer_observed" == true ]]; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null || ! kill -0 "$workflow_cli_pid" 2>/dev/null; then
    echo "Scoped-metrics Workflow did not reach its post-replay timer" >&2
    cat "$worker_log" >&2
    cat "$workflow_output" >&2
    exit 1
  fi
  sleep 0.1
done

if [[ "$replay_observed" != true || "$timer_observed" != true ]]; then
  echo "Scoped-metrics Workflow did not expose replay and timer evidence" >&2
  cat "$worker_log" >&2
  exit 1
fi

curl -fsS "http://${metrics_address}/metrics" >"$metrics_output"

metric_failure() {
  echo "Scoped metric assertion failed: $1" >&2
  cat "$metrics_output" >&2
  cat "$worker_log" >&2
  exit 1
}

workflow_line="$(grep '^odin_scoped_workflow_executions_total{' "$metrics_output" || true)"
activity_line="$(grep '^odin_scoped_activity_executions_total{' "$metrics_output" || true)"
workflow_line_count="$(grep -c '^odin_scoped_workflow_executions_total{' "$metrics_output" || true)"
activity_line_count="$(grep -c '^odin_scoped_activity_executions_total{' "$metrics_output" || true)"
[[ "$workflow_line_count" == "1" ]] || metric_failure "expected one Workflow series"
[[ "$activity_line_count" == "1" ]] || metric_failure "expected one Activity series"
grep -q 'namespace="default"' <<<"$workflow_line" || metric_failure "missing Workflow namespace"
grep -q "task_queue=\"$task_queue\"" <<<"$workflow_line" || metric_failure "missing Workflow task queue"
grep -q 'workflow_type="odin_hello_workflow"' <<<"$workflow_line" || metric_failure "missing Workflow type"
grep -q 'scenario="live"' <<<"$workflow_line" || metric_failure "missing Workflow record attribute"
grep -Eq ' 1(\.0)?$' <<<"$workflow_line" || metric_failure "Workflow replay duplicated the counter"
grep -q 'namespace="default"' <<<"$activity_line" || metric_failure "missing Activity namespace"
grep -q "task_queue=\"$task_queue\"" <<<"$activity_line" || metric_failure "missing Activity task queue"
grep -q 'activity_type="hello"' <<<"$activity_line" || metric_failure "missing Activity type"
grep -q 'scenario="live"' <<<"$activity_line" || metric_failure "missing Activity record attribute"
grep -Eq ' 1(\.0)?$' <<<"$activity_line" || metric_failure "Activity counter value changed"

if ! wait "$workflow_cli_pid"; then
  workflow_cli_pid=""
  echo "Scoped-metrics Workflow command failed" >&2
  cat "$workflow_output" >&2
  cat "$worker_log" >&2
  exit 1
fi
workflow_cli_pid=""
if ! wait "$worker_pid"; then
  worker_pid=""
  echo "Scoped-metrics Worker failed during shutdown" >&2
  cat "$worker_log" >&2
  exit 1
fi
worker_pid=""
grep -q '"result": "Hello scoped-metrics"' "$workflow_output"

echo "$workflow_line"
echo "$activity_line"
echo "Scoped Activity and replay-safe Workflow metrics passed through Core Prometheus export."

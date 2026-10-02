#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="$repo_root/native/target/release/temporal-odin-workflow-in-process-concurrency"
server_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-in-process-concurrency-server.XXXXXX")"

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log"
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
elif [[ ! -x "$binary" ]]; then
  echo "In-process concurrency binary is missing; run ./scripts/build-examples.sh first" >&2
  exit 1
fi

unique_suffix="${RANDOM}-$(date +%s)"

run_probe() {
  local suffix="$1"
  local force_replay="$2"
  local output
  local status

  set +e
  output="$(
    TEMPORAL_ODIN_TASK_QUEUE="odin-in-process-concurrent-$suffix" \
    TEMPORAL_ODIN_WORKFLOW_ID="odin-in-process-concurrent-$suffix" \
    TEMPORAL_ODIN_FORCE_REPLAY="$force_replay" \
    TEMPORAL_ODIN_FORCE_SHUTDOWN=0 \
    TEMPORAL_ODIN_HANDLER_LOAD=0 \
    TEMPORAL_ODIN_MIXED_LOAD=0 \
    "$binary" 2>&1
  )"
  status=$?
  set -e
  echo "$output"
  if [[ "$status" != "0" ]]; then
    return "$status"
  fi

  grep -q 'In-process Workflow concurrency proved' <<<"$output"
  grep -Eq 'executor_threads=4 activation_failures=([1-9][0-9]*) evictions=([4-9]|[1-9][0-9]+) cancellations=1 canceled_timers=1 signals=3 signal_handlers=3 updates=1 replay_activations=([0-9]+) owned_peak=([2-9]|[1-9][0-9]+) queue_peak=([1-9][0-9]*) callback_peak=([2-9]|[1-9][0-9]+) migrations=0 nondeterminism=(true|false)' <<<"$output"
  if [[ "$force_replay" == "1" ]]; then
    grep -Eq 'replay_activations=([1-9][0-9]*)' <<<"$output"
    grep -q 'nondeterminism=true' <<<"$output"
  fi
}

run_shutdown_probe() {
  local suffix="$1"
  local output
  local status

  set +e
  output="$(
    TEMPORAL_ODIN_TASK_QUEUE="odin-in-process-concurrent-$suffix" \
    TEMPORAL_ODIN_WORKFLOW_ID="odin-in-process-concurrent-$suffix" \
    TEMPORAL_ODIN_FORCE_REPLAY=0 \
    TEMPORAL_ODIN_FORCE_SHUTDOWN=1 \
    TEMPORAL_ODIN_HANDLER_LOAD=0 \
    TEMPORAL_ODIN_MIXED_LOAD=0 \
    "$binary" 2>&1
  )"
  status=$?
  set -e
  echo "$output"
  if [[ "$status" != "0" ]]; then
    return "$status"
  fi

  grep -q 'In-process Workflow shutdown proved' <<<"$output"
  grep -Eq 'shards=4 mid_activation_shards=4 stats_shards=4 activations=([4-9]|[1-9][0-9]+) evictions=([0-9]+) cached_shutdown=([1-9][0-9]*) owned_peak=([1-9][0-9]*) queue_peak=([1-9][0-9]*) migrations=0' <<<"$output"
}

run_handler_load_probe() {
  local suffix="$1"
  local output
  local status

  set +e
  output="$(
    TEMPORAL_ODIN_TASK_QUEUE="odin-in-process-concurrent-$suffix" \
    TEMPORAL_ODIN_WORKFLOW_ID="odin-in-process-concurrent-$suffix" \
    TEMPORAL_ODIN_FORCE_REPLAY=0 \
    TEMPORAL_ODIN_FORCE_SHUTDOWN=0 \
    TEMPORAL_ODIN_HANDLER_LOAD=1 \
    TEMPORAL_ODIN_MIXED_LOAD=0 \
    "$binary" 2>&1
  )"
  status=$?
  set -e
  echo "$output"
  if [[ "$status" != "0" ]]; then
    return "$status"
  fi

  grep -q 'In-process Workflow handler load proved' <<<"$output"
  grep -Eq 'handles=([8-9]|[1-9][0-9]+) shards=4 signals=([3-9][0-9]|[1-9][0-9]{2,}) handlers=([3-9][0-9]|[1-9][0-9]{2,}) replay_activations=([8-9]|[1-9][0-9]+) evictions=([8-9]|[1-9][0-9]+) shard_activations_min=([1-9][0-9]*) shard_activations_max=([1-9][0-9]*) owned_peak=([4-9]|[1-9][0-9]+) queue_peak=([1-9][0-9]*) migrations=0' <<<"$output"
}

run_mixed_load_probe() {
  local suffix="$1"
  local output
  local status

  set +e
  output="$(
    TEMPORAL_ODIN_TASK_QUEUE="odin-in-process-concurrent-$suffix" \
    TEMPORAL_ODIN_WORKFLOW_ID="odin-in-process-concurrent-$suffix" \
    TEMPORAL_ODIN_FORCE_REPLAY=0 \
    TEMPORAL_ODIN_FORCE_SHUTDOWN=0 \
    TEMPORAL_ODIN_HANDLER_LOAD=0 \
    TEMPORAL_ODIN_MIXED_LOAD=1 \
    "$binary" 2>&1
  )"
  status=$?
  set -e
  echo "$output"
  if [[ "$status" != "0" ]]; then
    return "$status"
  fi

  grep -q 'In-process mixed Workflow/Activity load proved' <<<"$output"
  grep -Eq 'handles=([8-9]|[1-9][0-9]+) activities=([2-9][0-9]|[1-9][0-9]{2,}) activity_peak=([2-9][0-9]|[1-9][0-9]{2,}) activity_owned_peak=([2-9][0-9]|[1-9][0-9]{2,}) signals=([8-9]|[1-9][0-9]+) signal_handlers=([8-9]|[1-9][0-9]+) updates=([8-9]|[1-9][0-9]+) replay_activations=([8-9]|[1-9][0-9]+) evictions=([8-9]|[1-9][0-9]+) shards=4 shard_activations_min=([1-9][0-9]*) shard_activations_max=([1-9][0-9]*) owned_peak=([4-9]|[1-9][0-9]+) queue_peak=([1-9][0-9]*) migrations=0' <<<"$output"
}

scenario="${TEMPORAL_ODIN_CONCURRENCY_SCENARIO:-all}"
case "$scenario" in
  all)
    run_probe "$unique_suffix" 0
    run_probe "$unique_suffix-replay" 1
    run_shutdown_probe "$unique_suffix-shutdown"
    run_handler_load_probe "$unique_suffix-handler-load"
    run_mixed_load_probe "$unique_suffix-mixed-load"
    ;;
  mixed)
    iterations="${TEMPORAL_ODIN_MIXED_SOAK_ITERATIONS:-1}"
    if [[ ! "$iterations" =~ ^[1-9][0-9]*$ ]]; then
      echo "TEMPORAL_ODIN_MIXED_SOAK_ITERATIONS must be a positive integer" >&2
      exit 1
    fi
    for ((iteration = 1; iteration <= iterations; iteration++)); do
      echo "Mixed Workflow/Activity soak iteration $iteration/$iterations"
      run_mixed_load_probe "$unique_suffix-mixed-soak-$iteration"
    done
    ;;
  *)
    echo "TEMPORAL_ODIN_CONCURRENCY_SCENARIO must be 'all' or 'mixed'" >&2
    exit 1
    ;;
esac

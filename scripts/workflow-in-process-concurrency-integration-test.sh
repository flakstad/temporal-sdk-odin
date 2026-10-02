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
    "$binary" 2>&1
  )"
  status=$?
  set -e
  echo "$output"
  if [[ "$status" != "0" ]]; then
    return "$status"
  fi

  grep -q 'In-process Workflow shutdown proved' <<<"$output"
  grep -Eq 'shards=4 activations=([4-9]|[1-9][0-9]+) evictions=([0-9]+) cached_shutdown=([4-9]|[1-9][0-9]+) owned_peak=([1-9][0-9]*) queue_peak=([1-9][0-9]*) migrations=0' <<<"$output"
}

run_probe "$unique_suffix" 0
run_probe "$unique_suffix-replay" 1
run_shutdown_probe "$unique_suffix-shutdown"

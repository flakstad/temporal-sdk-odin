#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
parent_binary="$repo_root/native/target/release/temporal-odin-workflow-concurrency"
child_binary="$repo_root/native/target/release/temporal-odin-workflow-isolate"
server_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-concurrency-server.XXXXXX")"

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
elif [[ ! -x "$parent_binary" || ! -x "$child_binary" ]]; then
  echo "Concurrency binaries are missing; run ./scripts/build-examples.sh first" >&2
  exit 1
fi

unique_suffix="${RANDOM}-$(date +%s)"
set +e
output="$(
  TEMPORAL_ODIN_TASK_QUEUE="odin-concurrent-$unique_suffix" \
  TEMPORAL_ODIN_WORKFLOW_ID="odin-concurrent-$unique_suffix" \
  TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND="$child_binary" \
  "$parent_binary" 2>&1
)"
status=$?
set -e
echo "$output"
if [[ "$status" != "0" ]]; then
  exit "$status"
fi
grep -q 'Concurrent Workflow execution proved' <<<"$output"
grep -Eq 'executor_threads=4 peak_activations=([2-9]|[1-9][0-9]+)' <<<"$output"

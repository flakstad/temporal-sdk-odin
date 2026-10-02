#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
server_pid=""
server_log="${TMPDIR:-/tmp}/temporal-sdk-odin-history-json-server.log"
history_file="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-history-json.XXXXXX")"
workflow_output="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-history-json-workflow.XXXXXX")"
replay_output="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-history-json-replay.XXXXXX")"

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$history_file" "$workflow_output" "$replay_output"
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

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-0}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
fi

workflow_id="odin-sdk-history-json-$$-$(date +%s)"
TEMPORAL_EAGER_WORKFLOW_ID="$workflow_id" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-eager-workflow.sh" >"$workflow_output"
grep -q "Eager Workflow start accepted and completed workflow_id=$workflow_id" "$workflow_output"

temporal workflow show \
  --workflow-id "$workflow_id" \
  --command-timeout 10s \
  --output json >"$history_file"
grep -q '"events"' "$history_file"

TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-replay-history.sh" "$history_file" >"$replay_output"
cat "$replay_output"
grep -q "PASS $history_file" "$replay_output"
echo "Temporal CLI History JSON converted to protobuf and replayed through SDK Core."

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lib_dir="$repo_root/native/target/release"
worker_binary="$lib_dir/temporal-odin-child-workflow"
explicit_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-child-explicit.XXXXXX")"
duplicate_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-child-duplicate.XXXXXX")"
worker_pid=""
odin_thread_count="${ODIN_THREAD_COUNT:-1}"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  rm -f -- "$explicit_log" "$duplicate_log"
}
trap cleanup EXIT INT TERM

if ! temporal operator cluster health \
  --address localhost:7233 \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  echo "Temporal dev server is not ready; run: temporal server start-dev --headless" >&2
  exit 1
fi

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-core.sh"
  odin build "$repo_root/examples/child_workflow" \
    -thread-count:"$odin_thread_count" \
    -out:"$worker_binary" \
    -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"
elif [[ ! -x "$worker_binary" ]]; then
  echo "Child Workflow binary is missing; run ./scripts/build-examples.sh first" >&2
  exit 1
fi

run_case() {
  local mode="$1"
  local worker_log="$2"
  local workflow_type=""
  local workflow_input=""
  local expected_result=""

  case "$mode" in
    explicit)
      workflow_type="odin_child_explicit_cancel_parent_workflow"
      workflow_input="cancel-explicit"
      expected_result="Observed explicit Child Workflow cancellation"
      ;;
    duplicate)
      workflow_type="odin_child_duplicate_parent_workflow"
      workflow_input="cancel-duplicate"
      expected_result="Observed Child Workflow Already Exists start failure"
      ;;
    *)
      echo "Unknown Child Workflow cancellation case: $mode" >&2
      exit 1
      ;;
  esac

  TEMPORAL_ODIN_SKIP_BUILD=1 \
    "$repo_root/scripts/run-child-workflow.sh" >"$worker_log" 2>&1 &
  worker_pid=$!
  for _ in {1..300}; do
    if grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
      break
    fi
    if ! kill -0 "$worker_pid" 2>/dev/null; then
      cat "$worker_log" >&2
      echo "Child Workflow Worker exited before polling ($mode)" >&2
      wait "$worker_pid" || true
      worker_pid=""
      exit 1
    fi
    sleep 0.05
  done
  if ! grep -q "Child Workflow Worker is polling" "$worker_log" 2>/dev/null; then
    echo "Child Workflow Worker did not become ready ($mode)" >&2
    exit 1
  fi

  local workflow_id="odin-sdk-child-${mode}-$$-$(date +%s)"
  local workflow_output
  workflow_output="$(temporal workflow execute \
    --workflow-id "$workflow_id" \
    --type "$workflow_type" \
    --task-queue odin-child-workflow \
    --input "\"$workflow_input\"" \
    --command-timeout 30s \
    --output json)"
  echo "$workflow_output"
  grep -q '"status": "COMPLETED"' <<<"$workflow_output"
  grep -q "$expected_result" <<<"$workflow_output"

  if ! wait "$worker_pid"; then
    worker_pid=""
    cat "$worker_log" >&2
    exit 1
  fi
  worker_pid=""
  grep -q 'cancel_requested=true canceled_child=1' "$worker_log"
  grep -Eq 'resolved=1 .*completed=true' "$worker_log"
  grep -q 'Child Workflow completed end-to-end and worker shut down cleanly' "$worker_log"
  if [[ "$mode" == "duplicate" ]]; then
    grep -q 'start_resolved=2' "$worker_log"
  fi
  cat "$worker_log"
}

run_case explicit "$explicit_log"
run_case duplicate "$duplicate_log"

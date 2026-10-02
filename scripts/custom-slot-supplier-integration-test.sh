#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lib_dir="$repo_root/native/target/release"
worker_binary="$lib_dir/temporal-odin-hello-activity"
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-sdk-odin-custom-slot.XXXXXX")"
worker_pid=""
odin_thread_count="${ODIN_THREAD_COUNT:-1}"
task_queue="odin-custom-slot-$$-$(date +%s%N)"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  rm -f -- "$worker_log"
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
  odin build "$repo_root/examples/hello_activity" \
    -thread-count:"$odin_thread_count" \
    -out:"$worker_binary" \
    -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"
elif [[ ! -x "$worker_binary" ]]; then
  echo "Activity Worker binary is missing; run ./scripts/build-examples.sh first" >&2
  exit 1
fi

ACTIVITY_CUSTOM_SLOT_SUPPLIER=1 \
TEMPORAL_ODIN_TASK_QUEUE="$task_queue" \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  "$repo_root/scripts/run-hello-activity.sh" >"$worker_log" 2>&1 &
worker_pid=$!

for _ in {1..300}; do
  if grep -q "is polling task queue" "$worker_log" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    cat "$worker_log" >&2
    echo "Custom-slot Activity Worker exited before polling" >&2
    wait "$worker_pid" || true
    worker_pid=""
    exit 1
  fi
  sleep 0.05
done

if ! grep -q "is polling task queue" "$worker_log" 2>/dev/null; then
  echo "Custom-slot Activity Worker did not become ready" >&2
  exit 1
fi

TEMPORAL_TASK_QUEUE="$task_queue" cargo run --quiet \
  --manifest-path "$repo_root/native/Cargo.toml" \
  --package activity-test-driver

if ! wait "$worker_pid"; then
  worker_pid=""
  cat "$worker_log" >&2
  exit 1
fi
worker_pid=""
cat "$worker_log"
grep -Eq \
  "custom Activity slot supplier completed through Core reserve=[0-9]+ cancel=[0-9]+ release=[0-9]+ pending_peak=[0-9]+ capacity=1" \
  "$worker_log"

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lib_dir="$repo_root/native/target/release"
binary="$lib_dir/temporal-odin-worker-replace-client"
secondary_address="localhost:17233"
secondary_pid=""
binary_pid=""
secondary_log="${TMPDIR:-/tmp}/temporal-sdk-odin-replacement-server.log"
odin_thread_count="${ODIN_THREAD_COUNT:-1}"
gate_directory="$(mktemp -d "${TMPDIR:-/tmp}/temporal-sdk-odin-worker-replace.XXXXXX")"
output_file="$gate_directory/output"

cleanup() {
  if [[ -n "$binary_pid" ]]; then
    kill "$binary_pid" 2>/dev/null || true
    wait "$binary_pid" 2>/dev/null || true
  fi
  if [[ -n "$secondary_pid" ]]; then
    kill "$secondary_pid" 2>/dev/null || true
    wait "$secondary_pid" 2>/dev/null || true
  fi
  rm -rf -- "$gate_directory"
}
trap cleanup EXIT INT TERM

if temporal operator cluster health \
  --address "$secondary_address" \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  echo "Replacement test port $secondary_address is already serving Temporal" >&2
  exit 1
fi

temporal server start-dev \
  --headless \
  --log-level error \
  --port 17233 \
  >"$secondary_log" 2>&1 &
secondary_pid=$!

for _ in {1..30}; do
  if temporal operator cluster health \
    --address "$secondary_address" \
    --client-connect-timeout 1s \
    --command-timeout 1s \
    --output none >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! temporal operator cluster health \
  --address "$secondary_address" \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  echo "Replacement Temporal dev server did not become ready; see $secondary_log" >&2
  exit 1
fi

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-core.sh"
  odin build "$repo_root/examples/worker_replace_client" \
    -thread-count:"$odin_thread_count" \
    -out:"$binary" \
    -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"
elif [[ ! -x "$binary" ]]; then
  echo "Example binary is missing; run ./scripts/build-examples.sh first" >&2
  exit 1
fi

TEMPORAL_INITIAL_TARGET="http://$secondary_address" \
TEMPORAL_REPLACEMENT_GATE_DIR="$gate_directory" \
  "$binary" >"$output_file" 2>&1 &
binary_pid=$!

for _ in {1..300}; do
  if [[ -d "$gate_directory/ready" ]]; then
    break
  fi
  if ! kill -0 "$binary_pid" 2>/dev/null; then
    output="$(<"$output_file")"
    echo "$output" >&2
    echo "Worker replacement example exited before its initial poll became active" >&2
    wait "$binary_pid" || true
    binary_pid=""
    exit 1
  fi
  sleep 0.05
done

if [[ ! -d "$gate_directory/ready" ]]; then
  echo "Worker replacement example did not publish readiness" >&2
  exit 1
fi

# Force the outstanding poll to fail before replacement, matching Core's
# documented recovery path rather than waiting for the server long-poll timeout.
sleep 0.25
kill "$secondary_pid"
wait "$secondary_pid" || true
secondary_pid=""
for _ in {1..100}; do
  if ! temporal operator cluster health \
    --address "$secondary_address" \
    --client-connect-timeout 100ms \
    --command-timeout 100ms \
    --output none >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done
sleep 0.25
mkdir "$gate_directory/continue"

if ! wait "$binary_pid"; then
  binary_pid=""
  output="$(<"$output_file")"
  echo "$output" >&2
  exit 1
fi
binary_pid=""
output="$(<"$output_file")"
echo "$output"
grep -Eq '^Worker moved from http://localhost:17233 to http://localhost:7233 and completed workflow_id=odin-worker-replace-[0-9]+\.$' <<<"$output"

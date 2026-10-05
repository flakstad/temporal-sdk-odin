#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="$repo_root/native/target/release/temporal-kvist-replay"
lib_dir="${TEMPORAL_ODIN_NATIVE_LIB_DIR:-$repo_root/native/target/release}"
workflow_id="kvist-replay-${RANDOM}-$(date +%s)"
server_pid=""
first_worker_pid=""
replacement_worker_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-kvist-replay-server.XXXXXX")"
first_worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-kvist-replay-first.XXXXXX")"
replacement_worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-kvist-replay-replacement.XXXXXX")"

run_binary() {
  case "$(uname -s)" in
    Darwin)
      exec env \
        DYLD_LIBRARY_PATH="$lib_dir${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" \
        "$binary" "$@"
      ;;
    Linux)
      exec env \
        LD_LIBRARY_PATH="$lib_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
        "$binary" "$@"
      ;;
    *)
      echo "Unsupported Kvist application host: $(uname -s)" >&2
      return 1
      ;;
  esac
}

cleanup() {
  if [[ -n "$first_worker_pid" ]]; then
    kill "$first_worker_pid" 2>/dev/null || true
    wait "$first_worker_pid" 2>/dev/null || true
  fi
  if [[ -n "$replacement_worker_pid" ]]; then
    kill "$replacement_worker_pid" 2>/dev/null || true
    wait "$replacement_worker_pid" 2>/dev/null || true
  fi
  temporal workflow terminate \
    --workflow-id "$workflow_id" \
    --reason "Kvist replay test cleanup" \
    --command-timeout 2s \
    --output none >/dev/null 2>&1 || true
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$first_worker_log" "$replacement_worker_log"
}
trap cleanup EXIT INT TERM

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-kvist-replay-workflow.sh" >/dev/null
elif [[ ! -x "$binary" ]]; then
  echo "Kvist replay Workflow binary is missing" >&2
  exit 1
fi

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

run_binary worker >"$first_worker_log" 2>&1 &
first_worker_pid=$!
for _ in {1..100}; do
  if grep -q '^worker ready task_queue=kvist-sdk-replay$' "$first_worker_log"; then
    break
  fi
  if ! kill -0 "$first_worker_pid" 2>/dev/null; then
    echo "First Kvist Worker exited before becoming ready" >&2
    cat "$first_worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
grep -q '^worker ready task_queue=kvist-sdk-replay$' "$first_worker_log"

start_output="$(run_binary start "$workflow_id")"
echo "$start_output"
grep -Eq "^started workflow_id=${workflow_id} run_id=[^ ]+$" <<<"$start_output"

for _ in {1..100}; do
  if grep -Eq '^activation replay=false .*started_timer=true .*completed=false ' "$first_worker_log"; then
    break
  fi
  if ! kill -0 "$first_worker_pid" 2>/dev/null; then
    echo "First Kvist Worker exited before the durable timer was pending" >&2
    cat "$first_worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
if ! grep -Eq '^activation replay=false .*started_timer=true .*completed=false ' "$first_worker_log"; then
  echo "The Workflow did not reach its durable timer before the deadline" >&2
  cat "$first_worker_log" >&2
  exit 1
fi
grep -q '^activity type=kvist_replay_greeting attempt=1 input="Temporal"$' "$first_worker_log"

# The first process disappears without running defer or graceful SDK shutdown.
# Temporal Server still owns the Workflow history and pending timer.
kill -KILL "$first_worker_pid"
wait "$first_worker_pid" 2>/dev/null || true
first_worker_pid=""

run_binary worker-once >"$replacement_worker_log" 2>&1 &
replacement_worker_pid=$!
for _ in {1..100}; do
  if grep -q '^worker ready task_queue=kvist-sdk-replay$' "$replacement_worker_log"; then
    break
  fi
  if ! kill -0 "$replacement_worker_pid" 2>/dev/null; then
    echo "Replacement Kvist Worker exited before becoming ready" >&2
    cat "$replacement_worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
grep -q '^worker ready task_queue=kvist-sdk-replay$' "$replacement_worker_log"

result_output="$(run_binary result "$workflow_id")"
echo "$result_output"
grep -Eq "^result workflow_id=${workflow_id} value=\"Hello Temporal after durable replay\"$" \
  <<<"$result_output"

wait "$replacement_worker_pid"
replacement_worker_pid=""

cat "$first_worker_log"
cat "$replacement_worker_log"

grep -Eq '^activation replay=true .*started_timer=true ' "$replacement_worker_log"
grep -Eq '^activation replay=false .*completed=true ' "$replacement_worker_log"
grep -Eq '^worker stopped workflows=1 activities=0 activations=[1-9][0-9]* replay_activations=[1-9][0-9]*$' \
  "$replacement_worker_log"

# A replay reconstructs the Activity command/result from history. It must not
# invoke the side-effecting Activity callback a second time.
activity_count="$(grep -hc '^activity type=kvist_replay_greeting ' \
  "$first_worker_log" "$replacement_worker_log" | awk '{total += $1} END {print total}')"
if [[ "$activity_count" -ne 1 ]]; then
  echo "Expected one Activity execution across both Workers; got $activity_count" >&2
  exit 1
fi

echo "Kvist Workflow survived Worker loss, replayed on a fresh stack, and completed without rerunning its Activity."

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="$repo_root/native/target/release/temporal-kvist-hello-workflow"
lib_dir="${TEMPORAL_ODIN_NATIVE_LIB_DIR:-$repo_root/native/target/release}"
server_pid=""
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-kvist-server.XXXXXX")"

cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log"
}
trap cleanup EXIT INT TERM

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-kvist-hello-workflow.sh" >/dev/null
elif [[ ! -x "$binary" ]]; then
  echo "Kvist Workflow binary is missing" >&2
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

case "$(uname -s)" in
  Darwin)
    output="$(DYLD_LIBRARY_PATH="$lib_dir${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}" "$binary")"
    ;;
  Linux)
    output="$(LD_LIBRARY_PATH="$lib_dir${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" "$binary")"
    ;;
  *)
    echo "Unsupported Kvist application host: $(uname -s)" >&2
    exit 1
    ;;
esac

echo "$output"
grep -q '^Kvist Activity type=kvist_hello_activity attempt=1 input="Temporal"$' <<<"$output"
grep -Eq '^Kvist completed workflow_id=kvist-sdk-hello-[0-9]+ run_id=[^ ]+ activities=1 workflows=1 result="Hello Temporal from Kvist"$' <<<"$output"

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lib_dir="$repo_root/native/target/release"
binary="$lib_dir/temporal-odin-system-info"
odin_thread_count="${ODIN_THREAD_COUNT:-1}"

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-core.sh"
  odin build "$repo_root/examples/system_info" \
    -thread-count:"$odin_thread_count" \
    -out:"$binary" \
    -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"
elif [[ ! -x "$binary" ]]; then
  echo "Example binary is missing; run ./scripts/build-examples.sh first" >&2
  exit 1
fi

output="$($binary)"
echo "$output"
grep -Eq '^Temporal Server [^ ]+ capabilities:' <<<"$output"
grep -q 'schedules=true' <<<"$output"
grep -q 'System Info interceptor surrounded the live Core RPC' <<<"$output"
grep -q '^feature=schedules sdk=true support=supported evidence=get-system-info status=0$' <<<"$output"
grep -q '^feature=nexus sdk=true support=supported evidence=get-system-info status=0$' <<<"$output"
grep -q '^feature=workflow-pause sdk=true support=unknown evidence=none status=0$' <<<"$output"

if grep -q '^Temporal Server 1\.29\.1 capabilities:' <<<"$output"; then
  grep -q '^feature=schedule-count sdk=true support=unsupported evidence=safe-rpc-probe status=12$' <<<"$output"
  grep -q '^feature=standalone-nexus sdk=true support=unsupported evidence=safe-rpc-probe status=12$' <<<"$output"
  grep -q '^feature=standalone-activity sdk=true support=unsupported evidence=safe-rpc-probe status=12$' <<<"$output"
else
  grep -Eq '^feature=schedule-count sdk=true support=(supported|unsupported) evidence=safe-rpc-probe status=(0|12)$' <<<"$output"
  grep -Eq '^feature=standalone-nexus sdk=true support=(supported|unsupported) evidence=safe-rpc-probe status=(0|12)$' <<<"$output"
  grep -Eq '^feature=standalone-activity sdk=true support=(supported|unsupported) evidence=safe-rpc-probe status=(0|12)$' <<<"$output"
fi

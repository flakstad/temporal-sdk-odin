#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lib_dir="$repo_root/native/target/release"
binary="$lib_dir/temporal-odin-config-connect"
odin_thread_count="${ODIN_THREAD_COUNT:-1}"

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-core.sh"
  odin build "$repo_root/examples/config_connect" \
    -thread-count:"$odin_thread_count" \
    -out:"$binary" \
    -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"
elif [[ ! -x "$binary" ]]; then
  echo "Example binary is missing; run ./scripts/build-examples.sh first" >&2
  exit 1
fi

output="$(TEMPORAL_ODIN_CONFIG_PATH="$repo_root/examples/config_connect/temporal.toml" "$binary")"
echo "$output"
grep -Eq '^Core-loaded profile dev connected to Temporal Server [^ ]+ in namespace default\.$' <<<"$output"

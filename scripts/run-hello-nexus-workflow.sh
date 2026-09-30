#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lib_dir="$repo_root/native/target/release"
binary="$lib_dir/temporal-odin-hello-nexus-workflow"
odin_thread_count="${ODIN_THREAD_COUNT:-1}"
if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-core.sh"
  odin build "$repo_root/examples/hello_nexus_workflow" \
    -thread-count:"$odin_thread_count" \
    -out:"$binary" \
    -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"
elif [[ ! -x "$binary" ]]; then
  echo "Example binary is missing; run ./scripts/build-examples.sh first" >&2
  exit 1
fi
exec "$binary"

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-0}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
fi

lib_dir="$repo_root/native/target/release"
exec "$lib_dir/temporal-odin-replay-history" "$@"

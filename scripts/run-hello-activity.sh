#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$repo_root/scripts/build-core.sh"

lib_dir="$repo_root/native/target/release"
binary="$lib_dir/temporal-odin-hello-activity"
odin build "$repo_root/examples/hello_activity" \
  -out:"$binary" \
  -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"
exec "$binary"

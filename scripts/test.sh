#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$repo_root/scripts/build-core.sh"

lib_dir="$repo_root/native/target/release"
odin test "$repo_root/tests" \
  -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"

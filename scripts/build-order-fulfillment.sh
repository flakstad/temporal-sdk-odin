#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="$repo_root/native/target/release"
lib_dir="${TEMPORAL_ODIN_NATIVE_LIB_DIR:-$output_dir}"
binary="$output_dir/temporal-odin-order-fulfillment"
odin_thread_count="${ODIN_THREAD_COUNT:-1}"

if [[ -z "${TEMPORAL_ODIN_NATIVE_LIB_DIR:-}" ]]; then
  "$repo_root/scripts/build-core.sh"
fi

mkdir -p "$output_dir"
odin build "$repo_root/applications/order_fulfillment" \
  -collection:sdk="$repo_root" \
  -thread-count:"$odin_thread_count" \
  -strict-style \
  -vet \
  -warnings-as-errors \
  -out:"$binary" \
  -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"

echo "$binary"

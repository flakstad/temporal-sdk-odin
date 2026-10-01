#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="$repo_root/native/target/release"
lib_dir="${TEMPORAL_ODIN_NATIVE_LIB_DIR:-$output_dir}"
binary="$output_dir/temporal-odin-fiber-guard-test"

if [[ -z "${TEMPORAL_ODIN_NATIVE_LIB_DIR:-}" ]]; then
  "$repo_root/scripts/build-fiber.sh"
fi
if [[ ! -f "$lib_dir/libtemporal_odin_fiber.a" ]]; then
  echo "Native fiber library is missing: $lib_dir/libtemporal_odin_fiber.a" >&2
  exit 1
fi
mkdir -p "$output_dir"
cc -std=c11 -O0 -Wall -Wextra -Werror \
  "$repo_root/native/fiber-bridge/fiber_bridge_test.c" \
  "$lib_dir/libtemporal_odin_fiber.a" \
  -o "$binary"
"$binary"

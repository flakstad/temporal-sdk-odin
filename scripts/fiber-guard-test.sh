#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lib_dir="$repo_root/native/target/release"
binary="$lib_dir/temporal-odin-fiber-guard-test"

"$repo_root/scripts/build-fiber.sh"
cc -std=c11 -O0 -Wall -Wextra -Werror \
  "$repo_root/native/fiber-bridge/fiber_bridge_test.c" \
  "$lib_dir/libtemporal_odin_fiber.a" \
  -o "$binary"
"$binary"

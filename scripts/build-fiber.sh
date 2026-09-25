#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="$repo_root/native/target/release"
object="$output_dir/temporal_odin_fiber.o"
library="$output_dir/libtemporal_odin_fiber.a"

git -C "$repo_root" submodule update --init native/minicoro
mkdir -p "$output_dir"

cc -std=c11 -O2 -DNDEBUG -Wall -Wextra -Werror \
  -I"$repo_root/native/minicoro" \
  -c "$repo_root/native/fiber-bridge/fiber_bridge.c" \
  -o "$object"
ar rcs "$library" "$object"

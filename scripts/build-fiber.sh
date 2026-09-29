#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="$repo_root/native/target/release"
object="$output_dir/temporal_odin_fiber.o"
library="$output_dir/libtemporal_odin_fiber.a"

if git -C "$repo_root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git -C "$repo_root" submodule update --init native/minicoro
elif [[ ! -f "$repo_root/native/minicoro/minicoro.h" ]]; then
  echo "Pinned minicoro source is missing; clone with submodules or unpack a complete source archive" >&2
  exit 1
fi
mkdir -p "$output_dir"

cc -std=c11 -O2 -DNDEBUG -Wall -Wextra -Werror \
  -I"$repo_root/native/minicoro" \
  -c "$repo_root/native/fiber-bridge/fiber_bridge.c" \
  -o "$object"
ar rcs "$library" "$object"

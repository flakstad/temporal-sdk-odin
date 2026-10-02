#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
sdk_core_revision_file="$repo_root/native/sdk-core-revision.txt"

if git -C "$repo_root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git -C "$repo_root" submodule update --init --recursive
  expected_sdk_core_revision="$(tr -d '[:space:]' <"$sdk_core_revision_file")"
  checked_out_sdk_core_revision="$(git -C "$repo_root/native/sdk-core" rev-parse HEAD)"
  indexed_sdk_core_revision="$(git -C "$repo_root" ls-files --stage -- native/sdk-core | cut -d' ' -f2)"
  if [[ "$checked_out_sdk_core_revision" != "$expected_sdk_core_revision" ||
        "$indexed_sdk_core_revision" != "$expected_sdk_core_revision" ]]; then
    echo "native/sdk-core-revision.txt, the checked-out Core, and its gitlink must match" >&2
    exit 1
  fi
elif [[ ! -f "$repo_root/native/sdk-core/crates/sdk-core-c-bridge/Cargo.toml" ||
        ! -f "$repo_root/native/minicoro/minicoro.h" ||
        ! -f "$sdk_core_revision_file" ]]; then
  echo "Pinned native sources are missing; clone with submodules or unpack a complete source archive" >&2
  exit 1
fi
cargo build --manifest-path "$repo_root/native/Cargo.toml" --release
"$repo_root/scripts/build-fiber.sh"

if [[ "$(uname -s)" == "Darwin" ]]; then
  core_library="$repo_root/native/target/release/libtemporalio_sdk_core_c_bridge.dylib"
  history_json_library="$repo_root/native/target/release/libtemporal_odin_history_json.dylib"
  install_name_tool -id "@rpath/libtemporalio_sdk_core_c_bridge.dylib" "$core_library"
  install_name_tool -id "@rpath/libtemporal_odin_history_json.dylib" "$history_json_library"
fi

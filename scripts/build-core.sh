#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

git -C "$repo_root" submodule update --init --recursive
cargo build --manifest-path "$repo_root/native/Cargo.toml" --release
"$repo_root/scripts/build-fiber.sh"

if [[ "$(uname -s)" == "Darwin" ]]; then
  library="$repo_root/native/target/release/libtemporalio_sdk_core_c_bridge.dylib"
  install_name_tool -id "@rpath/libtemporalio_sdk_core_c_bridge.dylib" "$library"
fi

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="${1:-$repo_root/dist}"

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-core.sh"
fi

case "$(uname -s):$(uname -m)" in
  Darwin:arm64)
    target="macos-arm64"
    core_library="libtemporalio_sdk_core_c_bridge.dylib"
    history_json_library="libtemporal_odin_history_json.dylib"
    ;;
  Darwin:x86_64)
    target="macos-x86_64"
    core_library="libtemporalio_sdk_core_c_bridge.dylib"
    history_json_library="libtemporal_odin_history_json.dylib"
    ;;
  Linux:aarch64|Linux:arm64)
    target="linux-arm64"
    core_library="libtemporalio_sdk_core_c_bridge.so"
    history_json_library="libtemporal_odin_history_json.so"
    ;;
  Linux:x86_64)
    target="linux-x86_64"
    core_library="libtemporalio_sdk_core_c_bridge.so"
    history_json_library="libtemporal_odin_history_json.so"
    ;;
  *)
    echo "Unsupported native package target: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

native_dir="$repo_root/native/target/release"
core_path="$native_dir/$core_library"
history_json_path="$native_dir/$history_json_library"
fiber_path="$native_dir/libtemporal_odin_fiber.a"
header_path="$repo_root/native/sdk-core/crates/sdk-core-c-bridge/include/temporal-sdk-core-c-bridge.h"
history_json_header_path="$repo_root/native/history-json-bridge/include/temporal-odin-history-json.h"

for required in "$core_path" "$history_json_path" "$fiber_path" "$header_path" \
  "$history_json_header_path" "$repo_root/LICENSE" \
  "$repo_root/native/sdk-core/LICENSE.txt" "$repo_root/native/minicoro/LICENSE" \
  "$repo_root/docs/native-distribution.md" "$repo_root/native/sdk-core-revision.txt"; do
  if [[ ! -f "$required" ]]; then
    echo "Native package input is missing: $required" >&2
    exit 1
  fi
done

checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

package_version() {
  awk -F '"' '/^version = "/ { print $2; exit }' "$1"
}

sdk_core_commit="$(tr -d '[:space:]' <"$repo_root/native/sdk-core-revision.txt")"
if [[ ! "$sdk_core_commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "native/sdk-core-revision.txt must contain one full lowercase Git commit" >&2
  exit 1
fi
sdk_core_version="$(package_version "$repo_root/native/sdk-core/crates/sdk-core/Cargo.toml")"
c_bridge_version="$(package_version "$repo_root/native/sdk-core/crates/sdk-core-c-bridge/Cargo.toml")"
history_json_version="$(package_version "$repo_root/native/history-json-bridge/Cargo.toml")"
archive_name="temporal-sdk-odin-native-$target.tar.gz"

mkdir -p "$output_dir"
stage_root="$(mktemp -d "${TMPDIR:-/tmp}/temporal-sdk-odin-package.XXXXXX")"
trap 'rm -rf "$stage_root"' EXIT
package_root="$stage_root/temporal-sdk-odin-native-$target"
mkdir -p "$package_root/lib" "$package_root/include" "$package_root/licenses"

cp "$core_path" "$package_root/lib/$core_library"
cp "$history_json_path" "$package_root/lib/$history_json_library"
cp "$fiber_path" "$package_root/lib/libtemporal_odin_fiber.a"
cp "$header_path" "$package_root/include/temporal-sdk-core-c-bridge.h"
cp "$history_json_header_path" "$package_root/include/temporal-odin-history-json.h"
cp "$repo_root/LICENSE" "$package_root/licenses/temporal-sdk-odin-MIT.txt"
cp "$repo_root/native/sdk-core/LICENSE.txt" "$package_root/licenses/temporal-sdk-core-MIT.txt"
cp "$repo_root/native/minicoro/LICENSE" "$package_root/licenses/minicoro-MIT-0.txt"
cp "$repo_root/docs/native-distribution.md" "$package_root/README.md"

odin_version="$(odin version 2>/dev/null || true)"
rust_version="$(rustc --version 2>/dev/null || true)"
{
  printf 'format_version=2\n'
  printf 'target=%s\n' "$target"
  printf 'sdk_core_commit=%s\n' "$sdk_core_commit"
  printf 'sdk_core_version=%s\n' "$sdk_core_version"
  printf 'c_bridge_version=%s\n' "$c_bridge_version"
  printf 'odin_version=%s\n' "$odin_version"
  printf 'rust_version=%s\n' "$rust_version"
  printf 'core_library=%s\n' "$core_library"
  printf 'core_sha256=%s\n' "$(checksum "$package_root/lib/$core_library")"
  printf 'history_json_library=%s\n' "$history_json_library"
  printf 'history_json_version=%s\n' "$history_json_version"
  printf 'history_json_sha256=%s\n' "$(checksum "$package_root/lib/$history_json_library")"
  printf 'fiber_library=libtemporal_odin_fiber.a\n'
  printf 'fiber_sha256=%s\n' "$(checksum "$package_root/lib/libtemporal_odin_fiber.a")"
  printf 'header_sha256=%s\n' "$(checksum "$package_root/include/temporal-sdk-core-c-bridge.h")"
  printf 'history_json_header_sha256=%s\n' "$(checksum "$package_root/include/temporal-odin-history-json.h")"
} >"$package_root/manifest.txt"

archive_path="$output_dir/$archive_name"
tar -C "$stage_root" -czf "$archive_path" "$(basename "$package_root")"
printf '%s  %s\n' "$(checksum "$archive_path")" "$archive_name" >"$archive_path.sha256"
printf '%s\n' "$archive_path"

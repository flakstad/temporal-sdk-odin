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
    portable_rpath='@loader_path/../lib'
    ;;
  Darwin:x86_64)
    target="macos-x86_64"
    core_library="libtemporalio_sdk_core_c_bridge.dylib"
    portable_rpath='@loader_path/../lib'
    ;;
  Linux:aarch64|Linux:arm64)
    target="linux-arm64"
    core_library="libtemporalio_sdk_core_c_bridge.so"
    portable_rpath='$ORIGIN/../lib'
    ;;
  Linux:x86_64)
    target="linux-x86_64"
    core_library="libtemporalio_sdk_core_c_bridge.so"
    portable_rpath='$ORIGIN/../lib'
    ;;
  *)
    echo "Unsupported Workflow isolation package target: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

native_dir="$repo_root/native/target/release"
core_path="$native_dir/$core_library"
for required in "$core_path" "$native_dir/libtemporal_odin_fiber.a" \
  "$repo_root/LICENSE" "$repo_root/native/sdk-core/LICENSE.txt" \
  "$repo_root/native/minicoro/LICENSE" \
  "$repo_root/docs/workflow-isolate-example-package.md" \
  "$repo_root/native/sdk-core-revision.txt"; do
  if [[ ! -f "$required" ]]; then
    echo "Workflow isolation package input is missing: $required" >&2
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

sdk_core_commit="$(tr -d '[:space:]' <"$repo_root/native/sdk-core-revision.txt")"
if [[ ! "$sdk_core_commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "native/sdk-core-revision.txt must contain one full lowercase Git commit" >&2
  exit 1
fi
protocol_version="$(sed -nE 's/^VERSION :: u16\(([0-9]+)\)$/\1/p' \
  "$repo_root/temporal/workflow/isolate/protocol.odin")"
if [[ -z "$protocol_version" ]]; then
  echo "Could not read the Workflow isolation protocol version" >&2
  exit 1
fi

archive_name="temporal-sdk-odin-workflow-isolate-example-$target.tar.gz"
mkdir -p "$output_dir"
stage_root="$(mktemp -d "${TMPDIR:-/tmp}/temporal-odin-isolate-package.XXXXXX")"
trap 'rm -rf "$stage_root"' EXIT
package_root="$stage_root/temporal-sdk-odin-workflow-isolate-example-$target"
mkdir -p "$package_root/bin" "$package_root/lib" "$package_root/licenses"

odin_thread_count="${ODIN_THREAD_COUNT:-1}"
build_bundle_binary() {
  local source_name="$1"
  local binary_name="$2"
  odin build "$repo_root/examples/$source_name" \
    -thread-count:"$odin_thread_count" \
    -out:"$package_root/bin/$binary_name" \
    -extra-linker-flags:"-L${native_dir} -Wl,-rpath,${portable_rpath}"
}

build_bundle_binary workflow_isolate temporal-odin-workflow-isolate
build_bundle_binary workflow_isolate_core temporal-odin-workflow-isolate-core
cp "$core_path" "$package_root/lib/$core_library"
cp "$repo_root/LICENSE" "$package_root/licenses/temporal-sdk-odin-MIT.txt"
cp "$repo_root/native/sdk-core/LICENSE.txt" "$package_root/licenses/temporal-sdk-core-MIT.txt"
cp "$repo_root/native/minicoro/LICENSE" "$package_root/licenses/minicoro-MIT-0.txt"
cp "$repo_root/docs/workflow-isolate-example-package.md" "$package_root/README.md"

{
  printf 'format_version=1\n'
  printf 'target=%s\n' "$target"
  printf 'sdk_core_commit=%s\n' "$sdk_core_commit"
  printf 'isolation_protocol_version=%s\n' "$protocol_version"
  printf 'core_library=%s\n' "$core_library"
  printf 'core_sha256=%s\n' "$(checksum "$package_root/lib/$core_library")"
  printf 'parent_binary=temporal-odin-workflow-isolate-core\n'
  printf 'parent_sha256=%s\n' "$(checksum "$package_root/bin/temporal-odin-workflow-isolate-core")"
  printf 'executor_binary=temporal-odin-workflow-isolate\n'
  printf 'executor_sha256=%s\n' "$(checksum "$package_root/bin/temporal-odin-workflow-isolate")"
} >"$package_root/manifest.txt"

archive_path="$output_dir/$archive_name"
tar -C "$stage_root" -czf "$archive_path" "$(basename "$package_root")"
printf '%s  %s\n' "$(checksum "$archive_path")" "$archive_name" >"$archive_path.sha256"
printf '%s\n' "$archive_path"

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -lt 1 || $# -gt 2 || ( $# -eq 2 && "$2" != "--smoke" ) ]]; then
  echo "usage: $0 ARCHIVE [--smoke]" >&2
  exit 2
fi

archive="$1"
smoke="${2:-}"
if [[ ! -f "$archive" ]]; then
  echo "Native package archive does not exist: $archive" >&2
  exit 1
fi

if tar -tzf "$archive" | awk -F/ '$1 == "" || $1 == ".." { bad = 1 } { for (i = 1; i <= NF; i++) if ($i == "..") bad = 1 } END { exit bad }'; then
  :
else
  echo "Native package contains an unsafe archive path" >&2
  exit 1
fi
if ! tar -tvzf "$archive" | awk '{ type = substr($1, 1, 1); if (type != "-" && type != "d") bad = 1 } END { exit bad }'; then
  echo "Native package contains a link or special archive entry" >&2
  exit 1
fi

stage_root="$(mktemp -d "${TMPDIR:-/tmp}/temporal-sdk-odin-verify.XXXXXX")"
trap 'rm -rf "$stage_root"' EXIT
tar -C "$stage_root" -xzf "$archive"

mapfile_command="mapfile"
if ! command -v "$mapfile_command" >/dev/null 2>&1; then
  package_roots=()
  while IFS= read -r entry; do
    package_roots+=("$entry")
  done < <(find "$stage_root" -mindepth 1 -maxdepth 1 -type d)
else
  mapfile -t package_roots < <(find "$stage_root" -mindepth 1 -maxdepth 1 -type d)
fi
if [[ ${#package_roots[@]} -ne 1 ]]; then
  echo "Native package must contain exactly one root directory" >&2
  exit 1
fi
package_root="${package_roots[0]}"
manifest="$package_root/manifest.txt"
if [[ ! -f "$manifest" ]]; then
  echo "Native package manifest is missing" >&2
  exit 1
fi

manifest_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$manifest"
}

case "$(uname -s):$(uname -m)" in
  Darwin:arm64)
    expected_target="macos-arm64"
    expected_core_library="libtemporalio_sdk_core_c_bridge.dylib"
    ;;
  Darwin:x86_64)
    expected_target="macos-x86_64"
    expected_core_library="libtemporalio_sdk_core_c_bridge.dylib"
    ;;
  Linux:aarch64|Linux:arm64)
    expected_target="linux-arm64"
    expected_core_library="libtemporalio_sdk_core_c_bridge.so"
    ;;
  Linux:x86_64)
    expected_target="linux-x86_64"
    expected_core_library="libtemporalio_sdk_core_c_bridge.so"
    ;;
  *)
    echo "Unsupported native verification host: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

target="$(manifest_value target)"
core_library="$(manifest_value core_library)"
fiber_library="$(manifest_value fiber_library)"
if [[ "$(manifest_value format_version)" != "1" || "$target" != "$expected_target" ]]; then
  echo "Native package target mismatch: expected $expected_target, got $target" >&2
  exit 1
fi
if [[ "$core_library" != "$expected_core_library" ||
      "$fiber_library" != "libtemporal_odin_fiber.a" ]]; then
  echo "Native package library names do not match target $target" >&2
  exit 1
fi
sdk_core_commit="$(manifest_value sdk_core_commit)"
if [[ ! "$sdk_core_commit" =~ ^[0-9a-f]{40}$ ]]; then
  echo "Native package Core revision is not a full lowercase Git commit" >&2
  exit 1
fi
expected_sdk_core_commit="$(tr -d '[:space:]' <"$repo_root/native/sdk-core-revision.txt")"
if [[ "$sdk_core_commit" != "$expected_sdk_core_commit" ]]; then
  echo "Native package Core revision does not match this Odin SDK checkout" >&2
  exit 1
fi

checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

core_path="$package_root/lib/$core_library"
fiber_path="$package_root/lib/$fiber_library"
header_path="$package_root/include/temporal-sdk-core-c-bridge.h"
for required in "$core_path" "$fiber_path" "$header_path" \
  "$package_root/README.md" \
  "$package_root/licenses/temporal-sdk-odin-MIT.txt" \
  "$package_root/licenses/temporal-sdk-core-MIT.txt" \
  "$package_root/licenses/minicoro-MIT-0.txt"; do
  if [[ ! -f "$required" ]]; then
    echo "Native package file is missing: $required" >&2
    exit 1
  fi
done

if [[ "$(checksum "$core_path")" != "$(manifest_value core_sha256)" ||
      "$(checksum "$fiber_path")" != "$(manifest_value fiber_sha256)" ||
      "$(checksum "$header_path")" != "$(manifest_value header_sha256)" ]]; then
  echo "Native package checksum verification failed" >&2
  exit 1
fi

source_header="$repo_root/native/sdk-core/crates/sdk-core-c-bridge/include/temporal-sdk-core-c-bridge.h"
if [[ -f "$source_header" &&
      "$(checksum "$header_path")" != "$(checksum "$source_header")" ]]; then
  echo "Native package C header does not match the pinned source checkout" >&2
  exit 1
fi

if [[ "$smoke" == "--smoke" ]]; then
  odin test "$repo_root/tests" \
    -define:ODIN_TEST_NAMES=tests.runtime_init_and_shutdown \
    -extra-linker-flags:"-L$package_root/lib -Wl,-rpath,$package_root/lib"
fi

echo "Verified native package target=$target core=$(manifest_value sdk_core_version) commit=$sdk_core_commit"

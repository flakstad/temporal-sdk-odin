#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -ne 1 ]]; then
  echo "usage: $0 ARCHIVE" >&2
  exit 2
fi
archive="$1"
if [[ ! -f "$archive" ]]; then
  echo "Workflow isolation package archive does not exist: $archive" >&2
  exit 1
fi

if ! tar -tzf "$archive" | awk -F/ '$1 == "" || $1 == ".." { bad = 1 } { for (i = 1; i <= NF; i++) if ($i == "..") bad = 1 } END { exit bad }'; then
  echo "Workflow isolation package contains an unsafe archive path" >&2
  exit 1
fi
if ! tar -tvzf "$archive" | awk '{ type = substr($1, 1, 1); if (type != "-" && type != "d") bad = 1 } END { exit bad }'; then
  echo "Workflow isolation package contains a link or special archive entry" >&2
  exit 1
fi

case "$(uname -s):$(uname -m)" in
  Darwin:arm64)
    expected_target="macos-arm64"
    expected_core_library="libtemporalio_sdk_core_c_bridge.dylib"
    expected_architecture="arm64"
    expected_rpath='@loader_path/../lib'
    ;;
  Darwin:x86_64)
    expected_target="macos-x86_64"
    expected_core_library="libtemporalio_sdk_core_c_bridge.dylib"
    expected_architecture="x86_64"
    expected_rpath='@loader_path/../lib'
    ;;
  Linux:aarch64|Linux:arm64)
    expected_target="linux-arm64"
    expected_core_library="libtemporalio_sdk_core_c_bridge.so"
    expected_architecture="aarch64"
    expected_rpath='$ORIGIN/../lib'
    ;;
  Linux:x86_64)
    expected_target="linux-x86_64"
    expected_core_library="libtemporalio_sdk_core_c_bridge.so"
    expected_architecture="x86-64"
    expected_rpath='$ORIGIN/../lib'
    ;;
  *)
    echo "Unsupported Workflow isolation verification host: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

stage_root="$(mktemp -d "${TMPDIR:-/tmp}/temporal-odin-isolate-verify.XXXXXX")"
trap 'rm -rf "$stage_root"' EXIT
tar -C "$stage_root" -xzf "$archive"
package_roots=()
while IFS= read -r entry; do
  package_roots+=("$entry")
done < <(find "$stage_root" -mindepth 1 -maxdepth 1 -type d)
if [[ ${#package_roots[@]} -ne 1 ]]; then
  echo "Workflow isolation package must contain exactly one root directory" >&2
  exit 1
fi
relocated_parent="$stage_root/relocated/twice"
mkdir -p "$relocated_parent"
package_root="$relocated_parent/$(basename "${package_roots[0]}")"
mv "${package_roots[0]}" "$package_root"

manifest="$package_root/manifest.txt"
if [[ ! -f "$manifest" ]]; then
  echo "Workflow isolation package manifest is missing" >&2
  exit 1
fi
manifest_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$manifest"
}
checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

if [[ "$(manifest_value format_version)" != "1" ||
      "$(manifest_value target)" != "$expected_target" ||
      "$(manifest_value core_library)" != "$expected_core_library" ]]; then
  echo "Workflow isolation package target or format does not match $expected_target" >&2
  exit 1
fi
expected_commit="$(tr -d '[:space:]' <"$repo_root/native/sdk-core-revision.txt")"
if [[ "$(manifest_value sdk_core_commit)" != "$expected_commit" ]]; then
  echo "Workflow isolation package Core revision does not match this checkout" >&2
  exit 1
fi
expected_protocol="$(sed -nE 's/^VERSION :: u16\(([0-9]+)\)$/\1/p' \
  "$repo_root/temporal/workflow/isolate/protocol.odin")"
if [[ "$(manifest_value isolation_protocol_version)" != "$expected_protocol" ]]; then
  echo "Workflow isolation package protocol version does not match this checkout" >&2
  exit 1
fi

parent_binary="$package_root/bin/$(manifest_value parent_binary)"
executor_binary="$package_root/bin/$(manifest_value executor_binary)"
core_path="$package_root/lib/$expected_core_library"
for required in "$parent_binary" "$executor_binary" "$core_path" \
  "$package_root/README.md" \
  "$package_root/licenses/temporal-sdk-odin-MIT.txt" \
  "$package_root/licenses/temporal-sdk-core-MIT.txt" \
  "$package_root/licenses/minicoro-MIT-0.txt"; do
  if [[ ! -f "$required" ]]; then
    echo "Workflow isolation package file is missing: $required" >&2
    exit 1
  fi
done
if [[ ! -x "$parent_binary" || ! -x "$executor_binary" ]]; then
  echo "Workflow isolation package binaries are not executable" >&2
  exit 1
fi
if [[ "$(checksum "$parent_binary")" != "$(manifest_value parent_sha256)" ||
      "$(checksum "$executor_binary")" != "$(manifest_value executor_sha256)" ||
      "$(checksum "$core_path")" != "$(manifest_value core_sha256)" ]]; then
  echo "Workflow isolation package checksum verification failed" >&2
  exit 1
fi

verify_binary_loader() {
  local binary="$1"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    file "$binary" | grep -q "Mach-O 64-bit executable $expected_architecture"
    rpaths="$(otool -l "$binary" | awk '$1 == "cmd" && $2 == "LC_RPATH" { want = 1; next } want && $1 == "path" { print $2; want = 0 }')"
    grep -Fxq "$expected_rpath" <<<"$rpaths"
    if grep -Eq '^/' <<<"$rpaths"; then
      echo "Workflow isolation binary contains an absolute LC_RPATH: $binary" >&2
      exit 1
    fi
    codesign -v "$binary"
  else
    file "$binary" | grep -qi "$expected_architecture"
    runpaths="$(readelf -d "$binary" | awk '/\((RPATH|RUNPATH)\)/ { line = $0; sub(/^.*\[/, "", line); sub(/\].*$/, "", line); print line }')"
    grep -Fxq "$expected_rpath" <<<"$runpaths"
    if grep -Eq '(^|:)/' <<<"$runpaths"; then
      echo "Workflow isolation binary contains an absolute RPATH/RUNPATH: $binary" >&2
      exit 1
    fi
  fi
}

verify_binary_loader "$parent_binary"
verify_binary_loader "$executor_binary"
if [[ "$(uname -s)" == "Darwin" ]]; then
  otool -L "$parent_binary" | grep -Fq "@rpath/$expected_core_library"
else
  readelf -d "$parent_binary" | grep -Fq "Shared library: [$expected_core_library]"
fi

load_output="$(env -u DYLD_LIBRARY_PATH -u LD_LIBRARY_PATH \
  "$parent_binary" --native-bundle-check)"
grep -q "Workflow isolation application bundle is loadable (protocol=$expected_protocol)" \
  <<<"$load_output"
process_output="$(env -u DYLD_LIBRARY_PATH -u LD_LIBRARY_PATH "$executor_binary")"
grep -q \
  'Workflow process isolation proved two correlated activations, suspension, resumption, and completion' \
  <<<"$process_output"

echo "$load_output"
echo "$process_output"
echo "Verified relocated Workflow isolation application bundle target=$expected_target protocol=$expected_protocol"

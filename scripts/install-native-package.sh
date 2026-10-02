#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 ARCHIVE [INSTALL_ROOT]" >&2
  exit 2
fi

archive="$1"
install_root="${2:-$repo_root/native/prebuilt}"
if [[ ! -f "$archive" ]]; then
  echo "Native package archive does not exist: $archive" >&2
  exit 1
fi

checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

archive_checksum="$(checksum "$archive")"
"$repo_root/scripts/verify-native-package.sh" "$archive" >/dev/null

stage_root="$(mktemp -d "${TMPDIR:-/tmp}/temporal-sdk-odin-install.XXXXXX")"
lock_dir=""
cleanup() {
  if [[ -n "$lock_dir" && -d "$lock_dir" ]]; then
    rmdir "$lock_dir" 2>/dev/null || true
  fi
  rm -rf "$stage_root"
}
trap cleanup EXIT INT TERM

if ! tar -tzf "$archive" | awk -F/ '$1 == "" || $1 == ".." { bad = 1 } { for (i = 1; i <= NF; i++) if ($i == "..") bad = 1 } END { exit bad }'; then
  echo "Native package contains an unsafe archive path" >&2
  exit 1
fi
if ! tar -tvzf "$archive" | awk '{ type = substr($1, 1, 1); if (type != "-" && type != "d") bad = 1 } END { exit bad }'; then
  echo "Native package contains a link or special archive entry" >&2
  exit 1
fi
tar -C "$stage_root" -xzf "$archive"
if [[ "$(checksum "$archive")" != "$archive_checksum" ]]; then
  echo "Native package archive changed during installation" >&2
  exit 1
fi

package_roots=()
while IFS= read -r entry; do
  package_roots+=("$entry")
done < <(find "$stage_root" -mindepth 1 -maxdepth 1 -type d)
if [[ ${#package_roots[@]} -ne 1 ]]; then
  echo "Native package must contain exactly one root directory" >&2
  exit 1
fi
package_root="${package_roots[0]}"
manifest="$package_root/manifest.txt"

manifest_value() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key { sub(/^[^=]*=/, ""); print; exit }' "$manifest"
}

target="$(manifest_value target)"
sdk_core_commit="$(manifest_value sdk_core_commit)"
install_parent="$install_root/$sdk_core_commit"
install_dir="$install_parent/$target"

installed_package_matches() {
  local installed_manifest="$install_dir/manifest.txt"
  [[ -f "$installed_manifest" ]] || return 1
  cmp -s "$manifest" "$installed_manifest" || return 1
  local core_library fiber_library history_json_library
  core_library="$(manifest_value core_library)"
  fiber_library="$(manifest_value fiber_library)"
  history_json_library="$(manifest_value history_json_library)"
  [[ -f "$install_dir/lib/$core_library" &&
     -f "$install_dir/lib/$history_json_library" &&
     -f "$install_dir/lib/$fiber_library" &&
     -f "$install_dir/include/temporal-sdk-core-c-bridge.h" &&
     -f "$install_dir/include/temporal-odin-history-json.h" &&
     -f "$install_dir/README.md" &&
     -f "$install_dir/licenses/temporal-sdk-odin-MIT.txt" &&
     -f "$install_dir/licenses/temporal-sdk-core-MIT.txt" &&
     -f "$install_dir/licenses/minicoro-MIT-0.txt" ]] || return 1
  [[ "$(checksum "$install_dir/lib/$core_library")" == "$(manifest_value core_sha256)" &&
     "$(checksum "$install_dir/lib/$history_json_library")" == "$(manifest_value history_json_sha256)" &&
     "$(checksum "$install_dir/lib/$fiber_library")" == "$(manifest_value fiber_sha256)" &&
     "$(checksum "$install_dir/include/temporal-sdk-core-c-bridge.h")" == "$(manifest_value header_sha256)" &&
     "$(checksum "$install_dir/include/temporal-odin-history-json.h")" == "$(manifest_value history_json_header_sha256)" ]]
}

if [[ -e "$install_dir" ]]; then
  if ! installed_package_matches; then
    echo "Native package destination already exists with different or incomplete contents: $install_dir" >&2
    exit 1
  fi
  printf '%s\n' "$install_dir/lib"
  exit 0
fi

mkdir -p "$install_parent"
lock_dir="$install_parent/.$target.install-lock"
if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "Another native package installation is active for $target" >&2
  exit 1
fi
if [[ -e "$install_dir" ]]; then
  if ! installed_package_matches; then
    echo "Native package destination appeared with incompatible contents: $install_dir" >&2
    exit 1
  fi
else
  mv "$package_root" "$install_dir"
fi
rmdir "$lock_dir"
lock_dir=""
printf '%s\n' "$install_dir/lib"

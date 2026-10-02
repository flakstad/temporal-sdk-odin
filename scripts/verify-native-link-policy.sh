#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
  echo "usage: $0 TARGET CORE_LIBRARY FIBER_LIBRARY [HISTORY_JSON_LIBRARY]" >&2
  exit 2
fi

target="$1"
core_library="$2"
fiber_library="$3"
history_json_library="${4:-}"
required_libraries=("$core_library" "$fiber_library")
if [[ -n "$history_json_library" ]]; then
  required_libraries+=("$history_json_library")
fi
for required in "${required_libraries[@]}"; do
  if [[ ! -f "$required" ]]; then
    echo "Native link-policy input is missing: $required" >&2
    exit 1
  fi
done
core_library="$(cd "$(dirname "$core_library")" && pwd)/$(basename "$core_library")"
fiber_library="$(cd "$(dirname "$fiber_library")" && pwd)/$(basename "$fiber_library")"
if [[ -n "$history_json_library" ]]; then
  history_json_library="$(cd "$(dirname "$history_json_library")" && pwd)/$(basename "$history_json_library")"
fi

command -v ar >/dev/null 2>&1 || {
  echo "ar is required for native package verification" >&2
  exit 1
}

verify_archive_members() {
  local archive="$1"
  local members object_count
  members="$(ar -t "$archive")"
  object_count="$(awk 'NF && $0 !~ /^__\.SYMDEF( SORTED)?$/ { count += 1 } END { print count + 0 }' <<<"$members")"
  if [[ "$object_count" != "1" ]] ||
     ! awk -F/ 'NF != 1 || $1 == "" || $1 == "." || $1 == ".." { bad = 1 } END { exit bad }' <<<"$members"; then
    echo "Native fiber archive must contain one path-free object member" >&2
    exit 1
  fi
}

verify_archive_members "$fiber_library"

case "$target" in
  macos-arm64|macos-x86_64)
    command -v otool >/dev/null 2>&1 || { echo "otool is required for macOS package verification" >&2; exit 1; }
    command -v lipo >/dev/null 2>&1 || { echo "lipo is required for macOS package verification" >&2; exit 1; }
    command -v codesign >/dev/null 2>&1 || { echo "codesign is required for macOS package verification" >&2; exit 1; }
    expected_arch="${target#macos-}"
    if [[ "$(lipo -archs "$core_library")" != "$expected_arch" ||
          "$(lipo -archs "$fiber_library")" != "$expected_arch" ]]; then
      echo "Native package contains the wrong Mach-O architecture for $target" >&2
      exit 1
    fi
    expected_id="@rpath/$(basename "$core_library")"
    install_id="$(otool -D "$core_library" | sed -n '2p')"
    if [[ "$install_id" != "$expected_id" ]]; then
      echo "Core dylib install ID must be $expected_id, got $install_id" >&2
      exit 1
    fi
    if otool -l "$core_library" | grep -q 'cmd LC_RPATH'; then
      echo "Core dylib must not embed an LC_RPATH" >&2
      exit 1
    fi
    while IFS= read -r dependency; do
      [[ -n "$dependency" ]] || continue
      [[ "$dependency" != "$expected_id" ]] || continue
      case "$dependency" in
        /System/Library/*|/usr/lib/*) ;;
        *) echo "Core dylib has a non-system dependency: $dependency" >&2; exit 1 ;;
      esac
    done < <(otool -L "$core_library" | tail -n +2 | awk '{ print $1 }')
    if ! codesign --verify --strict "$core_library" >/dev/null 2>&1; then
      echo "Core dylib has an invalid macOS code signature" >&2
      exit 1
    fi
    if [[ -n "$history_json_library" ]]; then
      if [[ "$(lipo -archs "$history_json_library")" != "$expected_arch" ]]; then
        echo "History JSON dylib contains the wrong architecture for $target" >&2
        exit 1
      fi
      history_json_expected_id="@rpath/$(basename "$history_json_library")"
      history_json_install_id="$(otool -D "$history_json_library" | sed -n '2p')"
      if [[ "$history_json_install_id" != "$history_json_expected_id" ]]; then
        echo "History JSON dylib install ID must be $history_json_expected_id, got $history_json_install_id" >&2
        exit 1
      fi
      if otool -l "$history_json_library" | grep -q 'cmd LC_RPATH'; then
        echo "History JSON dylib must not embed an LC_RPATH" >&2
        exit 1
      fi
      while IFS= read -r dependency; do
        [[ -n "$dependency" ]] || continue
        [[ "$dependency" != "$history_json_expected_id" ]] || continue
        case "$dependency" in
          /System/Library/*|/usr/lib/*) ;;
          *) echo "History JSON dylib has a non-system dependency: $dependency" >&2; exit 1 ;;
        esac
      done < <(otool -L "$history_json_library" | tail -n +2 | awk '{ print $1 }')
      if ! codesign --verify --strict "$history_json_library" >/dev/null 2>&1; then
        echo "History JSON dylib has an invalid macOS code signature" >&2
        exit 1
      fi
    fi
    ;;
  linux-arm64|linux-x86_64)
    command -v readelf >/dev/null 2>&1 || { echo "readelf is required for Linux package verification" >&2; exit 1; }
    expected_machine="AArch64"
    if [[ "$target" == "linux-x86_64" ]]; then
      expected_machine="Advanced Micro Devices X86-64"
    fi
    machine="$(readelf -h "$core_library" | awk -F: '/Machine:/ { sub(/^[[:space:]]+/, "", $2); print $2; exit }')"
    if [[ "$machine" != "$expected_machine" ]]; then
      echo "Core shared library has machine $machine, expected $expected_machine" >&2
      exit 1
    fi
    soname="$(readelf -d "$core_library" | awk -F'[][]' '/\(SONAME\)/ { print $2; exit }')"
    if [[ "$soname" != "$(basename "$core_library")" ]]; then
      echo "Core shared library SONAME mismatch: $soname" >&2
      exit 1
    fi
    if readelf -d "$core_library" | grep -Eq '\((RPATH|RUNPATH)\)'; then
      echo "Core shared library must not embed RPATH or RUNPATH" >&2
      exit 1
    fi
    while IFS= read -r dependency; do
      case "$dependency" in
        libc.so.*|libm.so.*|libdl.so.*|libpthread.so.*|librt.so.*|libgcc_s.so.*|libutil.so.*|libresolv.so.*) ;;
        *) echo "Core shared library has a non-system dependency: $dependency" >&2; exit 1 ;;
      esac
    done < <(readelf -d "$core_library" | awk -F'[][]' '/\(NEEDED\)/ { print $2 }')

    if [[ -n "$history_json_library" ]]; then
      history_json_machine="$(readelf -h "$history_json_library" | awk -F: '/Machine:/ { sub(/^[[:space:]]+/, "", $2); print $2; exit }')"
      if [[ "$history_json_machine" != "$expected_machine" ]]; then
        echo "History JSON shared library has machine $history_json_machine, expected $expected_machine" >&2
        exit 1
      fi
      history_json_soname="$(readelf -d "$history_json_library" | awk -F'[][]' '/\(SONAME\)/ { print $2; exit }')"
      if [[ "$history_json_soname" != "$(basename "$history_json_library")" ]]; then
        echo "History JSON shared library SONAME mismatch: $history_json_soname" >&2
        exit 1
      fi
      if readelf -d "$history_json_library" | grep -Eq '\((RPATH|RUNPATH)\)'; then
        echo "History JSON shared library must not embed RPATH or RUNPATH" >&2
        exit 1
      fi
      while IFS= read -r dependency; do
        case "$dependency" in
          libc.so.*|libm.so.*|libdl.so.*|libpthread.so.*|librt.so.*|libgcc_s.so.*|libutil.so.*|libresolv.so.*) ;;
          *) echo "History JSON shared library has a non-system dependency: $dependency" >&2; exit 1 ;;
        esac
      done < <(readelf -d "$history_json_library" | awk -F'[][]' '/\(NEEDED\)/ { print $2 }')
    fi

    extract_root="$(mktemp -d "${TMPDIR:-/tmp}/temporal-sdk-odin-fiber-elf.XXXXXX")"
    cleanup_extract() { rm -rf "$extract_root"; }
    trap cleanup_extract EXIT INT TERM
    fiber_member="$(ar -t "$fiber_library" | awk '$0 !~ /^__\.SYMDEF( SORTED)?$/ { print; exit }')"
    (cd "$extract_root" && ar -x "$fiber_library" "$fiber_member")
    fiber_machine="$(readelf -h "$extract_root/$fiber_member" | awk -F: '/Machine:/ { sub(/^[[:space:]]+/, "", $2); print $2; exit }')"
    if [[ "$fiber_machine" != "$expected_machine" ]]; then
      echo "Native fiber archive has machine $fiber_machine, expected $expected_machine" >&2
      exit 1
    fi
    ;;
  *)
    echo "Unsupported native link-policy target: $target" >&2
    exit 1
    ;;
esac

echo "Verified native link policy target=$target"

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -ne 1 ]]; then
  echo "usage: $0 ARCHIVE" >&2
  exit 2
fi
archive="$1"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/temporal-sdk-odin-native-package-test.XXXXXX")"
cleanup() {
  rm -rf "$test_root"
}
trap cleanup EXIT INT TERM

lib_dir="$("$repo_root/scripts/install-native-package.sh" "$archive" "$test_root/install")"
second_lib_dir="$("$repo_root/scripts/install-native-package.sh" "$archive" "$test_root/install")"
if [[ "$lib_dir" != "$second_lib_dir" || ! -d "$lib_dir" ]]; then
  echo "Native package installation is not idempotent" >&2
  exit 1
fi

TEMPORAL_ODIN_NATIVE_LIB_DIR="$lib_dir" \
  "$repo_root/scripts/test.sh"

TEMPORAL_ODIN_NATIVE_LIB_DIR="$lib_dir" \
  "$repo_root/scripts/build-examples.sh"

install_dir="${lib_dir%/lib}"
target="$(basename "$install_dir")"
fiber_library="$lib_dir/libtemporal_odin_fiber.a"
case "$target" in
  macos-*)
    core_library="$lib_dir/libtemporalio_sdk_core_c_bridge.dylib"
    invalid_core="$test_root/invalid-install-name.dylib"
    cp "$core_library" "$invalid_core"
    install_name_tool -id /tmp/libtemporalio_sdk_core_c_bridge.dylib "$invalid_core"
    if "$repo_root/scripts/verify-native-link-policy.sh" \
      "$target" "$invalid_core" "$fiber_library" >/dev/null 2>&1; then
      echo "Native link policy accepted an absolute macOS install ID" >&2
      exit 1
    fi
    ;;
  linux-arm64)
    core_library="$lib_dir/libtemporalio_sdk_core_c_bridge.so"
    if "$repo_root/scripts/verify-native-link-policy.sh" \
      linux-x86_64 "$core_library" "$fiber_library" >/dev/null 2>&1; then
      echo "Native link policy accepted the wrong Linux architecture" >&2
      exit 1
    fi
    ;;
  linux-x86_64)
    core_library="$lib_dir/libtemporalio_sdk_core_c_bridge.so"
    if "$repo_root/scripts/verify-native-link-policy.sh" \
      linux-arm64 "$core_library" "$fiber_library" >/dev/null 2>&1; then
      echo "Native link policy accepted the wrong Linux architecture" >&2
      exit 1
    fi
    ;;
esac

mv "$install_dir/README.md" "$install_dir/README.md.missing"
if "$repo_root/scripts/install-native-package.sh" "$archive" "$test_root/install" >/dev/null 2>&1; then
  echo "Native package installer accepted an incomplete existing installation" >&2
  exit 1
fi

mkdir -p "$test_root/incompatible"
tar -C "$test_root/incompatible" -xzf "$archive"
incompatible_manifest="$(find "$test_root/incompatible" -name manifest.txt -type f)"
if [[ -z "$incompatible_manifest" ]]; then
  echo "Native package test could not find the extracted manifest" >&2
  exit 1
fi
sed -i.bak 's/^sdk_core_commit=.*/sdk_core_commit=0000000000000000000000000000000000000000/' "$incompatible_manifest"
rm -f "$incompatible_manifest.bak"
incompatible_archive="$test_root/incompatible.tar.gz"
tar -C "$test_root/incompatible" -czf "$incompatible_archive" .
if "$repo_root/scripts/verify-native-package.sh" "$incompatible_archive" >/dev/null 2>&1; then
  echo "Native package verifier accepted an incompatible Core revision" >&2
  exit 1
fi

mkdir -p "$test_root/linked/package"
ln -s /tmp "$test_root/linked/package/lib"
linked_archive="$test_root/linked.tar.gz"
tar -C "$test_root/linked" -czf "$linked_archive" package
if "$repo_root/scripts/verify-native-package.sh" "$linked_archive" >/dev/null 2>&1; then
  echo "Native package verifier accepted a symbolic-link archive entry" >&2
  exit 1
fi

echo "Installed and consumed a pinned native package from $lib_dir"

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $# -ne 1 ]]; then
  echo "usage: $0 ARCHIVE" >&2
  exit 2
fi
archive="$1"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/temporal-odin-isolate-package-test.XXXXXX")"
trap 'rm -rf "$test_root"' EXIT INT TERM

"$repo_root/scripts/verify-workflow-isolate-example-package.sh" "$archive"

make_manifest_variant() {
  local name="$1"
  local expression="$2"
  local variant_root="$test_root/$name"
  mkdir -p "$variant_root"
  tar -C "$variant_root" -xzf "$archive"
  local package_root
  package_root="$(find "$variant_root" -mindepth 1 -maxdepth 1 -type d)"
  local manifest="$package_root/manifest.txt"
  sed -i.bak "$expression" "$manifest"
  rm -f "$manifest.bak"
  tar -C "$variant_root" -czf "$test_root/$name.tar.gz" "$(basename "$package_root")"
}

make_manifest_variant incompatible-core \
  's/^sdk_core_commit=.*/sdk_core_commit=0000000000000000000000000000000000000000/'
if "$repo_root/scripts/verify-workflow-isolate-example-package.sh" \
  "$test_root/incompatible-core.tar.gz" >/dev/null 2>&1; then
  echo "Workflow isolation verifier accepted an incompatible Core revision" >&2
  exit 1
fi

make_manifest_variant incompatible-protocol \
  's/^isolation_protocol_version=.*/isolation_protocol_version=0/'
if "$repo_root/scripts/verify-workflow-isolate-example-package.sh" \
  "$test_root/incompatible-protocol.tar.gz" >/dev/null 2>&1; then
  echo "Workflow isolation verifier accepted an incompatible protocol version" >&2
  exit 1
fi

mkdir -p "$test_root/linked/package/bin"
ln -s /tmp "$test_root/linked/package/lib"
linked_archive="$test_root/linked.tar.gz"
tar -C "$test_root/linked" -czf "$linked_archive" package
if "$repo_root/scripts/verify-workflow-isolate-example-package.sh" \
  "$linked_archive" >/dev/null 2>&1; then
  echo "Workflow isolation verifier accepted a symbolic-link archive entry" >&2
  exit 1
fi

echo "Verified Workflow isolation package acceptance and rejection paths"

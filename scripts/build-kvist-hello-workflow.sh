#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
kvist_bin="${KVIST_BIN:-$repo_root/../kvist/kvist}"
lib_dir="${TEMPORAL_ODIN_NATIVE_LIB_DIR:-$repo_root/native/target/release}"
source_file="$repo_root/applications/kvist_hello_workflow/main.kvist"
binary="$repo_root/native/target/release/temporal-kvist-hello-workflow"

if [[ "$kvist_bin" == */* ]]; then
  if [[ ! -x "$kvist_bin" ]]; then
    echo "Kvist compiler is missing or not executable: $kvist_bin" >&2
    exit 1
  fi
elif ! command -v "$kvist_bin" >/dev/null 2>&1; then
  echo "Kvist compiler is not on PATH: $kvist_bin" >&2
  exit 1
fi
if [[ -z "${TEMPORAL_ODIN_NATIVE_LIB_DIR:-}" ]]; then
  "$repo_root/scripts/build-core.sh"
fi

mkdir -p "$(dirname "$binary")"
LIBRARY_PATH="$lib_dir${LIBRARY_PATH:+:$LIBRARY_PATH}" \
  "$kvist_bin" build "$source_file" --out "$binary"

echo "$binary"

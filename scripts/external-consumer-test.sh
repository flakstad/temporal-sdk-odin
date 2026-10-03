#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
lib_dir="${TEMPORAL_ODIN_NATIVE_LIB_DIR:-$repo_root/native/target/release}"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/temporal-sdk-odin-consumer.XXXXXX")"
cleanup() {
  rm -rf "$test_root"
}
trap cleanup EXIT INT TERM

if [[ -z "${TEMPORAL_ODIN_NATIVE_LIB_DIR:-}" ]]; then
  "$repo_root/scripts/build-core.sh"
fi

mkdir -p "$test_root/app"
cp "$repo_root/tests_consumer/core_sdk/main.odin" "$test_root/app/main.odin"

odin build "$test_root/app" \
  -out:"$test_root/consumer" \
  -collection:sdk="$repo_root" \
  -vet \
  -strict-style \
  -warnings-as-errors \
  -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"

output="$($test_root/consumer)"
echo "$output"
grep -q '^External Core SDK consumer completed 16 init/double-close cycles$' <<<"$output"

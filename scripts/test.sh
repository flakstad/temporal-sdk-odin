#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$repo_root/scripts/generate-protos.sh" --check
cargo test --quiet \
  --manifest-path "$repo_root/native/Cargo.toml" \
  --package odin-proto-gen
"$repo_root/scripts/build-core.sh"

lib_dir="$repo_root/native/target/release"
odin_thread_count="${ODIN_THREAD_COUNT:-1}"
odin test "$repo_root/tests" \
	-thread-count:"$odin_thread_count" \
	-vet \
	-strict-style \
	-warnings-as-errors \
	-extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"

pbt_root="${PBT_ROOT:-$repo_root/test-deps/pbt}"
if [[ ! -f "$pbt_root/pbt/check.odin" ]]; then
	echo "PBT test dependency is missing; run: git submodule update --init --recursive" >&2
	exit 1
fi

# The pinned PBT repository includes its own tests in the imported collection.
# Keep SDK tests warning-clean above, but do not promote dependency warnings to
# errors in this separately compiled property suite.
odin test "$repo_root/tests_pbt" \
	-thread-count:"$odin_thread_count" \
	-collection:pbt="$pbt_root" \
	-extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"

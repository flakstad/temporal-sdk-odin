#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$repo_root/scripts/generate-protos.sh" --check

# Compile the base Client and optional extensions independently so an accidental
# reverse import cannot collapse the package boundary.
odin check "$repo_root/temporal/client" \
	-no-entry-point \
	-vet \
	-strict-style \
	-warnings-as-errors
odin check "$repo_root/temporal/client/workflow_control" \
	-no-entry-point \
	-vet \
	-strict-style \
	-warnings-as-errors
odin check "$repo_root/temporal/client/schedule" \
	-no-entry-point \
	-vet \
	-strict-style \
	-warnings-as-errors
odin check "$repo_root/temporal/client/standalone_nexus" \
	-no-entry-point \
	-vet \
	-strict-style \
	-warnings-as-errors

cargo test --quiet \
  --manifest-path "$repo_root/native/Cargo.toml" \
  --package odin-proto-gen
cargo test --quiet \
  --manifest-path "$repo_root/native/Cargo.toml" \
  --package temporal-odin-history-json
lib_dir="${TEMPORAL_ODIN_NATIVE_LIB_DIR:-$repo_root/native/target/release}"
if [[ -z "${TEMPORAL_ODIN_NATIVE_LIB_DIR:-}" ]]; then
  "$repo_root/scripts/build-core.sh"
fi
TEMPORAL_ODIN_NATIVE_LIB_DIR="$lib_dir" "$repo_root/scripts/fiber-guard-test.sh"

odin_thread_count="${ODIN_THREAD_COUNT:-1}"
odin test "$repo_root/tests" \
	-thread-count:"$odin_thread_count" \
	-vet \
	-strict-style \
	-warnings-as-errors \
	-extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"

pbt_root="${PBT_ROOT:-$repo_root/test-deps/pbt}"
statecharts_root="${STATECHARTS_ROOT:-$repo_root/test-deps/statecharts}"
if [[ ! -f "$pbt_root/pbt/check.odin" ]]; then
	echo "PBT test dependency is missing; run: git submodule update --init --recursive" >&2
	exit 1
fi
if [[ ! -f "$statecharts_root/statecharts/runtime.odin" ]]; then
	echo "statecharts test dependency is missing; run: git submodule update --init --recursive" >&2
	exit 1
fi

# The pinned PBT repository includes its own tests in the imported collection.
# Keep SDK tests warning-clean above, but do not promote dependency warnings to
# errors in this separately compiled property suite.
odin test "$repo_root/tests_pbt" \
	-thread-count:"$odin_thread_count" \
	-no-threaded-checker \
	-collection:pbt="$pbt_root" \
	-collection:statecharts="$statecharts_root" \
	-extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"

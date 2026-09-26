#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
"$repo_root/scripts/build-core.sh"

lib_dir="$repo_root/native/target/release"
odin_thread_count="${ODIN_THREAD_COUNT:-1}"

build_example() {
  local source_name="$1"
  local binary_name="$2"
  odin build "$repo_root/examples/$source_name" \
    -thread-count:"$odin_thread_count" \
    -out:"$lib_dir/$binary_name" \
    -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"
}

build_example core_connect temporal-odin-core-connect
build_example client_start temporal-odin-client-start
build_example client_control temporal-odin-client-control
build_example hello_activity temporal-odin-hello-activity
build_example hello_workflow temporal-odin-hello-workflow

#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_dir="$repo_root/native/target/release"
lib_dir="${TEMPORAL_ODIN_NATIVE_LIB_DIR:-$output_dir}"
if [[ -z "${TEMPORAL_ODIN_NATIVE_LIB_DIR:-}" ]]; then
  "$repo_root/scripts/build-core.sh"
fi
case "$(uname -s)" in
  Darwin)
    core_library="libtemporalio_sdk_core_c_bridge.dylib"
    history_json_library="libtemporal_odin_history_json.dylib"
    ;;
  Linux)
    core_library="libtemporalio_sdk_core_c_bridge.so"
    history_json_library="libtemporal_odin_history_json.so"
    ;;
  *) echo "Unsupported example-build host: $(uname -s)" >&2; exit 1 ;;
esac
for required in "$lib_dir/$core_library" "$lib_dir/$history_json_library" \
  "$lib_dir/libtemporal_odin_fiber.a"; do
  if [[ ! -f "$required" ]]; then
    echo "Native library is missing: $required" >&2
    exit 1
  fi
done
mkdir -p "$output_dir"
odin_thread_count="${ODIN_THREAD_COUNT:-1}"

build_example() {
  local source_name="$1"
  local binary_name="$2"
  odin build "$repo_root/examples/$source_name" \
    -thread-count:"$odin_thread_count" \
    -out:"$output_dir/$binary_name" \
    -extra-linker-flags:"-L${lib_dir} -Wl,-rpath,${lib_dir}"
}

build_example core_connect temporal-odin-core-connect
build_example system_info temporal-odin-system-info
build_example config_connect temporal-odin-config-connect
build_example worker_replace_client temporal-odin-worker-replace-client
build_example client_start temporal-odin-client-start
build_example client_control temporal-odin-client-control
build_example update_with_start temporal-odin-update-with-start
build_example schedule_lifecycle temporal-odin-schedule-lifecycle
build_example hello_activity temporal-odin-hello-activity
build_example sdk_hello_activity temporal-odin-sdk-hello-activity
build_example standalone_activity temporal-odin-standalone-activity
build_example hello_nexus temporal-odin-hello-nexus
build_example standalone_nexus_client temporal-odin-standalone-nexus-client
build_example hello_nexus_workflow temporal-odin-hello-nexus-workflow
build_example worker_supervisor temporal-odin-worker-supervisor
build_example workflow_isolate temporal-odin-workflow-isolate
build_example workflow_isolate_core temporal-odin-workflow-isolate-core
build_example workflow_concurrency temporal-odin-workflow-concurrency
build_example workflow_in_process_concurrency temporal-odin-workflow-in-process-concurrency
build_example hello_workflow temporal-odin-hello-workflow
build_example eager_workflow temporal-odin-eager-workflow
build_example eager_activity temporal-odin-eager-activity
build_example replay_history temporal-odin-replay-history
build_example child_workflow temporal-odin-child-workflow

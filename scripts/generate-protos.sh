#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mode="${1:-write}"
if [[ "$mode" != "write" && "$mode" != "--check" ]]; then
  echo "usage: $0 [write|--check]" >&2
  exit 2
fi

temp_dir="$(mktemp -d "${TMPDIR:-/tmp}/temporal-sdk-odin-protos.XXXXXX")"
cleanup() {
  rm -rf "$temp_dir"
}
trap cleanup EXIT INT TERM

proto_root="$repo_root/native/sdk-core/crates/protos/protos"
descriptor_set="$temp_dir/activity-descriptors.bin"
generated="$temp_dir/generated.odin"
destination="$repo_root/temporal/core/proto/generated.odin"
protoc_bin="$(command -v protoc)"
protoc_prefix="$(cd "$(dirname "$protoc_bin")/.." && pwd)"
protoc_args=(
  -I "$proto_root/local"
  -I "$proto_root/api_upstream"
)
if [[ -d "$protoc_prefix/include/google/protobuf" ]]; then
  protoc_args+=(-I "$protoc_prefix/include")
fi

"$protoc_bin" \
  "${protoc_args[@]}" \
  --include_imports \
  --descriptor_set_out="$descriptor_set" \
  temporal/sdk/core/activity_task/activity_task.proto \
  temporal/sdk/core/core_interface.proto

cargo run --quiet \
  --manifest-path "$repo_root/native/Cargo.toml" \
  --package odin-proto-gen -- \
  "$descriptor_set" \
  "$generated" \
  coresdk.ActivityHeartbeat \
  coresdk.activity_task.ActivityTask \
  coresdk.ActivityTaskCompletion \
  coresdk.workflow_activation.WorkflowActivation \
  coresdk.workflow_completion.WorkflowActivationCompletion

if [[ "$mode" == "--check" ]]; then
  if ! cmp -s "$generated" "$destination"; then
    echo "generated Odin protobuf code is stale; run ./scripts/generate-protos.sh" >&2
    diff -u "$destination" "$generated" || true
    exit 1
  fi
else
  cp "$generated" "$destination"
fi

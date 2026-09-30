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
  temporal/sdk/core/core_interface.proto \
  temporal/api/workflowservice/v1/request_response.proto

cargo run --quiet \
  --manifest-path "$repo_root/native/Cargo.toml" \
  --package odin-proto-gen -- \
  "$descriptor_set" \
  "$generated" \
  coresdk.ActivityHeartbeat \
  coresdk.activity_task.ActivityTask \
  coresdk.ActivityTaskCompletion \
  coresdk.nexus.NexusTask \
  coresdk.nexus.NexusTaskCompletion \
  coresdk.workflow_activation.WorkflowActivation \
  coresdk.workflow_completion.WorkflowActivationCompletion \
  temporal.api.workflowservice.v1.StartWorkflowExecutionRequest \
  temporal.api.workflowservice.v1.StartWorkflowExecutionResponse \
  temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryRequest \
  temporal.api.workflowservice.v1.GetWorkflowExecutionHistoryResponse \
  temporal.api.workflowservice.v1.DescribeWorkflowExecutionRequest \
  temporal.api.workflowservice.v1.DescribeWorkflowExecutionResponse \
  temporal.api.workflowservice.v1.ListWorkflowExecutionsRequest \
  temporal.api.workflowservice.v1.ListWorkflowExecutionsResponse \
  temporal.api.workflowservice.v1.CountWorkflowExecutionsRequest \
  temporal.api.workflowservice.v1.CountWorkflowExecutionsResponse \
  temporal.api.workflowservice.v1.CreateScheduleRequest \
  temporal.api.workflowservice.v1.CreateScheduleResponse \
  temporal.api.workflowservice.v1.DescribeScheduleRequest \
  temporal.api.workflowservice.v1.DescribeScheduleResponse \
  temporal.api.workflowservice.v1.UpdateScheduleRequest \
  temporal.api.workflowservice.v1.UpdateScheduleResponse \
  temporal.api.workflowservice.v1.PatchScheduleRequest \
  temporal.api.workflowservice.v1.PatchScheduleResponse \
  temporal.api.workflowservice.v1.ListScheduleMatchingTimesRequest \
  temporal.api.workflowservice.v1.ListScheduleMatchingTimesResponse \
  temporal.api.workflowservice.v1.DeleteScheduleRequest \
  temporal.api.workflowservice.v1.DeleteScheduleResponse \
  temporal.api.workflowservice.v1.ListSchedulesRequest \
  temporal.api.workflowservice.v1.ListSchedulesResponse \
  temporal.api.workflowservice.v1.CountSchedulesRequest \
  temporal.api.workflowservice.v1.CountSchedulesResponse \
  temporal.api.workflowservice.v1.SignalWorkflowExecutionRequest \
  temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionRequest \
  temporal.api.workflowservice.v1.SignalWithStartWorkflowExecutionResponse \
  temporal.api.workflowservice.v1.QueryWorkflowRequest \
  temporal.api.workflowservice.v1.QueryWorkflowResponse \
  temporal.api.workflowservice.v1.UpdateWorkflowExecutionRequest \
  temporal.api.workflowservice.v1.UpdateWorkflowExecutionResponse \
  temporal.api.workflowservice.v1.PollWorkflowExecutionUpdateRequest \
  temporal.api.workflowservice.v1.PollWorkflowExecutionUpdateResponse \
  temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatRequest \
  temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatResponse \
  temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatByIdRequest \
  temporal.api.workflowservice.v1.RecordActivityTaskHeartbeatByIdResponse \
  temporal.api.workflowservice.v1.RespondActivityTaskCompletedRequest \
  temporal.api.workflowservice.v1.RespondActivityTaskCompletedByIdRequest \
  temporal.api.workflowservice.v1.RespondActivityTaskFailedRequest \
  temporal.api.workflowservice.v1.RespondActivityTaskFailedByIdRequest \
  temporal.api.workflowservice.v1.RespondActivityTaskCanceledRequest \
  temporal.api.workflowservice.v1.RespondActivityTaskCanceledByIdRequest \
  temporal.api.workflowservice.v1.RequestCancelWorkflowExecutionRequest \
  temporal.api.workflowservice.v1.ResetWorkflowExecutionRequest \
  temporal.api.workflowservice.v1.ResetWorkflowExecutionResponse \
  temporal.api.workflowservice.v1.TerminateWorkflowExecutionRequest

if [[ "$mode" == "--check" ]]; then
  if ! cmp -s "$generated" "$destination"; then
    echo "generated Odin protobuf code is stale; run ./scripts/generate-protos.sh" >&2
    diff -u "$destination" "$generated" || true
    exit 1
  fi
else
  cp "$generated" "$destination"
fi

package workflow_pbt_test

import "core:mem/virtual"
import "core:testing"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"
import workflow_control "../temporal/client/workflow_control"
import proto "../temporal/core/proto"

workflow_pause_request_property :: proc(t: ^pbt.T) -> pbt.Result {
	is_pause := pbt.draw(t, pbt.boolean())
	include_run_id := pbt.draw(t, pbt.boolean())
	reason := pbt.draw(t, pbt.string_ascii(0, 96))
	request_id := pbt.draw(t, pbt.string_ascii(1, 48))
	run_id := include_run_id ? "property-run" : ""
	kind := is_pause ? workflow_control.Workflow_Pause_Mutation_Kind.Pause : .Unpause
	pbt.cover(t, is_pause, 40.0, "pause request")
	pbt.cover(t, !is_pause, 40.0, "unpause request")
	pbt.cover(t, include_run_id, 40.0, "specific Workflow run")
	pbt.cover(t, len(reason) == 0, 1.0, "empty reason")

	encoded, encode_error := workflow_control.encode_workflow_pause_mutation_request(
		"property-namespace",
		"property-identity",
		"property-workflow",
		run_id,
		kind,
		{reason = reason, request_id = request_id},
	)
	defer delete(encoded)
	defer temporal_client.error_free(&encode_error)
	if encode_error.message != nil {
		return pbt.error("generated Workflow pause request did not encode")
	}

	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return pbt.error("could not initialize Workflow pause decode arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	matches := false
	if is_pause {
		request := proto.Temporal_Api_Workflowservice_V1_PauseWorkflowExecutionRequest{}
		decoded := proto.decode_Temporal_Api_Workflowservice_V1_PauseWorkflowExecutionRequest(encoded[:], &request)
		matches = decoded && request.namespace == "property-namespace" &&
		          request.workflow_id == "property-workflow" && request.run_id == run_id &&
		          request.identity == "property-identity" && request.reason == reason &&
		          request.request_id == request_id
	} else {
		request := proto.Temporal_Api_Workflowservice_V1_UnpauseWorkflowExecutionRequest{}
		decoded := proto.decode_Temporal_Api_Workflowservice_V1_UnpauseWorkflowExecutionRequest(encoded[:], &request)
		matches = decoded && request.namespace == "property-namespace" &&
		          request.workflow_id == "property-workflow" && request.run_id == run_id &&
		          request.identity == "property-identity" && request.reason == reason &&
		          request.request_id == request_id
	}
	context.allocator = previous_allocator
	if !matches {
		return pbt.fail("generated Workflow pause mutation changed across protobuf encoding")
	}
	return pbt.pass()
}

@(test)
workflow_pause_requests_preserve_generated_identity_reason_and_idempotence :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow pause requests preserve identity, reason, and idempotence",
		workflow_pause_request_property,
		{num_tests = 500, seed = 0x5041_5553_455f_5251},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

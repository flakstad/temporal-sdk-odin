package workflow_pbt_test

import "core:mem/virtual"
import "core:testing"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"
import proto "../temporal/core/proto"

reset_request_property :: proc(t: ^pbt.T) -> pbt.Result {
	event_id := i64(pbt.draw(t, pbt.int_range(1, 1_000_000)))
	include_run_id := pbt.draw(t, pbt.boolean())
	exclude_signals := pbt.draw(t, pbt.boolean())
	exclude_updates := pbt.draw(t, pbt.boolean())
	exclude_nexus := pbt.draw(t, pbt.boolean())
	pbt.cover(t, !include_run_id, 30.0, "reset current run")
	pbt.cover(t, exclude_signals, 30.0, "exclude Signal reapplication")
	pbt.cover(t, exclude_updates, 30.0, "exclude Update reapplication")
	pbt.cover(t, exclude_nexus, 30.0, "exclude Nexus reapplication")

	exclusions: [dynamic]temporal_client.Reset_Reapply_Exclude_Type
	defer delete(exclusions)
	if exclude_signals do append(&exclusions, temporal_client.Reset_Reapply_Exclude_Type.Signals)
	if exclude_updates do append(&exclusions, temporal_client.Reset_Reapply_Exclude_Type.Updates)
	if exclude_nexus do append(&exclusions, temporal_client.Reset_Reapply_Exclude_Type.Nexus)
	run_id := ""
	if include_run_id {
		run_id = "property-run-id"
	}
	encoded, encode_error := temporal_client.encode_reset_workflow_request(
		"property-namespace",
		"property-identity",
		"property-workflow-id",
		run_id,
		{
			workflow_task_finish_event_id = event_id,
			reason = "property reset",
			request_id = "property-reset-request",
			reapply_exclude_types = exclusions[:],
		},
	)
	defer delete(encoded)
	defer temporal_client.error_free(&encode_error)
	if encode_error.message != nil {
		return pbt.fail("valid generated Workflow reset request did not encode")
	}

	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return pbt.fail("could not allocate Workflow reset decode arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	defer { context.allocator = previous_allocator }
	request := proto.Temporal_Api_Workflowservice_V1_ResetWorkflowExecutionRequest{}
	if !proto.decode_Temporal_Api_Workflowservice_V1_ResetWorkflowExecutionRequest(
		encoded[:],
		&request,
	) {
		return pbt.fail("generated Workflow reset request did not decode")
	}
	if request.workflow_execution == nil ||
	   request.namespace != "property-namespace" ||
	   request.identity != "property-identity" ||
	   request.workflow_execution.workflow_id != "property-workflow-id" ||
	   request.workflow_execution.run_id != run_id ||
	   request.workflow_task_finish_event_id != event_id ||
	   request.reason != "property reset" ||
	   request.request_id != "property-reset-request" {
		return pbt.fail("generated Workflow reset identity or target changed during encoding")
	}
	if len(request.reset_reapply_exclude_types) != len(exclusions) {
		return pbt.fail("generated Workflow reset exclusion count changed during encoding")
	}
	for exclude_type, index in exclusions {
		expected, valid := temporal_client.reset_reapply_exclude_type_to_proto(exclude_type)
		if !valid || request.reset_reapply_exclude_types[index] != expected {
			return pbt.fail("generated Workflow reset exclusion changed during encoding")
		}
	}
	return pbt.pass()
}

@(test)
workflow_reset_requests_preserve_generated_contract :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow reset requests preserve target and reapply exclusions",
		reset_request_property,
		{num_tests = 500, seed = 0x5245_5345_545f_5251},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

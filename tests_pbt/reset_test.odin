package workflow_pbt_test

import "core:fmt"
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
	post_reset_count := pbt.draw(t, pbt.int_range(0, 12))
	pbt.cover(t, !include_run_id, 30.0, "reset current run")
	pbt.cover(t, exclude_signals, 30.0, "exclude Signal reapplication")
	pbt.cover(t, exclude_updates, 30.0, "exclude Update reapplication")
	pbt.cover(t, exclude_nexus, 30.0, "exclude Nexus reapplication")
	pbt.cover(t, post_reset_count == 0, 5.0, "no post-reset operations")
	pbt.cover(t, post_reset_count >= 9, 20.0, "many post-reset operations")

	exclusions: [dynamic]temporal_client.Reset_Reapply_Exclude_Type
	defer delete(exclusions)
	if exclude_signals do append(&exclusions, temporal_client.Reset_Reapply_Exclude_Type.Signals)
	if exclude_updates do append(&exclusions, temporal_client.Reset_Reapply_Exclude_Type.Updates)
	if exclude_nexus do append(&exclusions, temporal_client.Reset_Reapply_Exclude_Type.Nexus)
	run_id := ""
	if include_run_id {
		run_id = "property-run-id"
	}
	post_reset_operations := make([]temporal_client.Post_Reset_Operation, post_reset_count)
	defer delete(post_reset_operations)
	post_reset_signal_kinds := make([]bool, post_reset_count)
	defer delete(post_reset_signal_kinds)
	priorities := make([]temporal_client.Priority, post_reset_count)
	defer delete(priorities)
	priority_path := [?]string{"priority"}
	owned_signal_names: [dynamic]string
	defer {
		for value in owned_signal_names do delete(value)
		delete(owned_signal_names)
	}
	has_signal := false
	has_update := false
	for index in 0..<post_reset_count {
		is_signal := pbt.draw(t, pbt.boolean())
		post_reset_signal_kinds[index] = is_signal
		if is_signal {
			has_signal = true
			name := fmt.aprintf("property-signal-%d", index)
			append(&owned_signal_names, name)
			post_reset_operations[index] = {
				kind = .Signal_Workflow,
				signal_workflow = {signal_name = name},
			}
		} else {
			has_update = true
			priorities[index].priority_key = i32(index + 1)
			post_reset_operations[index] = {
				kind = .Update_Workflow_Options,
				update_workflow_options = {
					update_mask = priority_path[:],
					priority = &priorities[index],
				},
			}
		}
	}
	pbt.cover(t, has_signal && has_update, 30.0, "mixed post-reset operation order")
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
			post_reset_operations = post_reset_operations,
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
	request := proto.Temporal_Api_Workflowservice_V1_ResetWorkflowExecutionRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_ResetWorkflowExecutionRequest(
		encoded[:],
		&request,
	)
	context.allocator = previous_allocator
	if !decoded {
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
	if len(request.post_reset_operations) != post_reset_count {
		return pbt.fail("generated post-reset operation count changed during encoding")
	}
	for operation, index in request.post_reset_operations {
		if post_reset_signal_kinds[index] {
			signal: ^proto.Temporal_Api_Workflow_V1_PostResetOperation_SignalWorkflow
			#partial switch variant in operation.variant {
			case proto.Temporal_Api_Workflow_V1_PostResetOperation_Variant_Signal_Workflow:
				signal = variant.value
			}
			expected_name := fmt.aprintf("property-signal-%d", index)
			matches := signal != nil && signal.signal_name == expected_name
			delete(expected_name)
			if !matches {
				return pbt.fail("generated post-reset Signal order changed during encoding")
			}
		} else {
			update: ^proto.Temporal_Api_Workflow_V1_PostResetOperation_UpdateWorkflowOptions
			#partial switch variant in operation.variant {
			case proto.Temporal_Api_Workflow_V1_PostResetOperation_Variant_Update_Workflow_Options:
				update = variant.value
			}
			if update == nil || update.update_mask == nil ||
			   len(update.update_mask.paths) != 1 || update.update_mask.paths[0] != "priority" ||
			   update.workflow_execution_options == nil ||
			   update.workflow_execution_options.priority == nil ||
			   update.workflow_execution_options.priority.priority_key != i32(index + 1) {
				return pbt.fail("generated post-reset options update order changed during encoding")
			}
		}
	}
	return pbt.pass()
}

@(test)
workflow_reset_requests_preserve_generated_contract :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow reset requests preserve target, exclusions, and post-reset operations",
		reset_request_property,
		{num_tests = 500, seed = 0x5245_5345_545f_5251},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

package client

import "core:mem"
import "core:mem/virtual"
import "core:time"
import temporal_core "../core"
import proto "../core/proto"

Workflow_Fast_Forward_State :: struct {
	id:       string,
	duration: time.Duration,
	present:  bool,
}

Workflow_Time_Skipping_State :: struct {
	enabled:                bool,
	fast_forward:           Workflow_Fast_Forward_State,
	disable_propagation:    bool,
	max_session_skip_count: i32,
	present:                bool,
}

// Workflow_Execution_Options is an owned snapshot returned by Server after an
// update. Call workflow_execution_options_free when it is no longer needed.
Workflow_Execution_Options :: struct {
	versioning_override:     Workflow_Versioning_Override,
	has_versioning_override: bool,
	priority:                Workflow_Priority,
	time_skipping_config:    Workflow_Time_Skipping_State,
	update_time:             Optional_Time,
	allocator:               mem.Allocator,
}

workflow_execution_options_free :: proc(options: ^Workflow_Execution_Options) {
	if options == nil do return
	allocator := options.allocator
	if allocator.procedure != nil {
		delete(options.versioning_override.deployment_name, allocator)
		delete(options.versioning_override.build_id, allocator)
		delete(options.priority.fairness_key, allocator)
		delete(options.time_skipping_config.fast_forward.id, allocator)
	}
	options^ = {}
}

decode_workflow_versioning_override :: proc(
	source: ^proto.Temporal_Api_Workflow_V1_VersioningOverride,
	allocator: mem.Allocator,
) -> (result: Workflow_Versioning_Override, present, ok: bool) {
	if source == nil do return {}, false, true
	present = true
	deployment_name: string
	build_id: string
	#partial switch variant in source.override {
	case proto.Temporal_Api_Workflow_V1_VersioningOverride_Override_Pinned:
		result.kind = .Pinned
		if variant.value == nil do return {}, true, false
		if variant.value.version != nil {
			deployment_name = variant.value.version.deployment_name
			build_id = variant.value.version.build_id
		}
	case proto.Temporal_Api_Workflow_V1_VersioningOverride_Override_Auto_Upgrade:
		result.kind = .Auto_Upgrade
	case proto.Temporal_Api_Workflow_V1_VersioningOverride_Override_One_Time:
		result.kind = .One_Time
		if variant.value == nil || variant.value.target_deployment_version == nil {
			return {}, true, false
		}
		deployment_name = variant.value.target_deployment_version.deployment_name
		build_id = variant.value.target_deployment_version.build_id
	case:
		return {}, true, false
	}
	clone_ok: bool
	result.deployment_name, clone_ok = description_clone_string(deployment_name, allocator)
	if !clone_ok do return {}, true, false
	result.build_id, clone_ok = description_clone_string(build_id, allocator)
	if !clone_ok {
		delete(result.deployment_name, allocator)
		return {}, true, false
	}
	return result, true, true
}

decode_workflow_execution_options_response :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (result: Workflow_Execution_Options, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Workflow options response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionOptionsResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionOptionsResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded || response.workflow_execution_options == nil {
		return {}, error_from_string(.Malformed_Response, "Temporal returned malformed Workflow options")
	}

	result.allocator = allocator
	options := response.workflow_execution_options
	override_ok: bool
	result.versioning_override,
	result.has_versioning_override,
	override_ok = decode_workflow_versioning_override(options.versioning_override, allocator)
	if !override_ok {
		return {}, error_from_string(.Malformed_Response, "could not decode Workflow versioning override")
	}
	priority_ok: bool
	result.priority, priority_ok = description_priority(options.priority, allocator)
	if !priority_ok {
		workflow_execution_options_free(&result)
		return {}, error_from_string(.Malformed_Response, "could not decode Workflow priority")
	}
	if options.time_skipping_config != nil {
		source := options.time_skipping_config
		result.time_skipping_config = {
			enabled = source.enabled,
			disable_propagation = source.disable_propagation,
			max_session_skip_count = source.max_session_skip_count,
			present = true,
		}
		if source.fast_forward_config != nil {
			fast_forward_id: string
			id_ok: bool
			fast_forward_id, id_ok = description_clone_string(
				source.fast_forward_config.id,
				allocator,
			)
			if !id_ok {
				workflow_execution_options_free(&result)
				return {}, error_from_string(.Malformed_Response, "could not decode Workflow fast-forward ID")
			}
			duration := description_duration(source.fast_forward_config.duration)
			result.time_skipping_config.fast_forward = {
				id = fast_forward_id,
				duration = duration.value,
				present = true,
			}
		}
	}
	result.update_time = description_time(response.update_time)
	return result, {}
}

encode_update_workflow_execution_options_request :: proc(
	namespace, identity, workflow_id, run_id: string,
	update: Workflow_Execution_Options_Update,
) -> ([dynamic]u8, Error) {
	if len(namespace) == 0 || len(workflow_id) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Workflow options update identity is incomplete")
	}
	operations := [?]Post_Reset_Operation{{
		kind = .Update_Workflow_Options,
		update_workflow_options = update,
	}}
	storage := post_reset_proto_storage_make(operations[:])
	defer post_reset_proto_storage_free(&storage)
	encode_error := post_reset_operations_encode(operations[:], &storage)
	if encode_error.message != nil do return nil, encode_error
	execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
		workflow_id = workflow_id,
		run_id = run_id,
	}
	request := proto.Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionOptionsRequest{
		namespace = namespace,
		workflow_execution = &execution,
		workflow_execution_options = &storage.workflow_options[0],
		update_mask = &storage.update_masks[0],
		identity = identity,
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionOptionsRequest(&request), {}
}

update_workflow_execution_options_terminal :: proc(
	input: ^Client_Update_Workflow_Execution_Options_Input,
) -> (Workflow_Execution_Options, Error) {
	if input == nil || !workflow_handle_valid(input.handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	request_bytes, request_error := encode_update_workflow_execution_options_request(
		input.handle.client.namespace,
		client_request_identity(input.handle.client),
		input.handle.id,
		input.handle.run_id,
		input.update,
	)
	if request_error.message != nil do return {}, request_error
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		input.handle.client.connection,
		.Workflow,
		"UpdateWorkflowExecutionOptions",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return {}, error_from_core(&rpc_result.error)
	return decode_workflow_execution_options_response(rpc_result.protobuf)
}

update_workflow_execution_options :: proc(
	handle: ^Workflow_Handle,
	update: Workflow_Execution_Options_Update,
) -> (Workflow_Execution_Options, Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	input := Client_Update_Workflow_Execution_Options_Input{handle = handle, update = update}
	state := Client_Interceptor_State{client = handle.client}
	return client_update_workflow_execution_options_chain(&state, &input)
}

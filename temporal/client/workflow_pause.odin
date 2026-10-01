package client

import "core:encoding/uuid"
import temporal_core "../core"
import proto "../core/proto"

Workflow_Pause_Mutation_Kind :: enum {
	Invalid,
	Pause,
	Unpause,
}

Workflow_Pause_Options :: struct {
	reason:     string,
	request_id: string,
}

encode_workflow_pause_mutation_request :: proc(
	namespace, identity, workflow_id, run_id: string,
	kind: Workflow_Pause_Mutation_Kind,
	options: Workflow_Pause_Options,
) -> ([dynamic]u8, Error) {
	if len(namespace) == 0 || len(workflow_id) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Workflow pause identity is incomplete")
	}
	if kind == .Invalid {
		return nil, error_from_string(.Invalid_Argument, "Workflow pause mutation kind is invalid")
	}
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request_id := resolve_request_id(options.request_id, request_id_buffer[:])
	switch kind {
	case .Pause:
		request := proto.Temporal_Api_Workflowservice_V1_PauseWorkflowExecutionRequest{
			namespace = namespace,
			workflow_id = workflow_id,
			run_id = run_id,
			identity = identity,
			reason = options.reason,
			request_id = request_id,
		}
		return proto.encode_Temporal_Api_Workflowservice_V1_PauseWorkflowExecutionRequest(&request), {}
	case .Unpause:
		request := proto.Temporal_Api_Workflowservice_V1_UnpauseWorkflowExecutionRequest{
			namespace = namespace,
			workflow_id = workflow_id,
			run_id = run_id,
			identity = identity,
			reason = options.reason,
			request_id = request_id,
		}
		return proto.encode_Temporal_Api_Workflowservice_V1_UnpauseWorkflowExecutionRequest(&request), {}
	case .Invalid:
		return nil, error_from_string(.Invalid_Argument, "Workflow pause mutation kind is invalid")
	}
	return nil, error_from_string(.Invalid_Argument, "Workflow pause mutation kind is invalid")
}

workflow_pause_mutation_terminal :: proc(
	input: ^Client_Workflow_Pause_Mutation_Input,
) -> Error {
	if input == nil || !workflow_handle_valid(input.handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	request_bytes, request_error := encode_workflow_pause_mutation_request(
		input.handle.client.namespace,
		client_request_identity(input.handle.client),
		input.handle.id,
		input.handle.run_id,
		input.kind,
		input.options,
	)
	if request_error.message != nil do return request_error
	defer delete(request_bytes)
	rpc_name := input.kind == .Pause ? "PauseWorkflowExecution" : "UnpauseWorkflowExecution"
	rpc_result := temporal_core.client_rpc_call(
		input.handle.client.connection,
		.Workflow,
		rpc_name,
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return error_from_core(&rpc_result.error)
	return {}
}

workflow_pause_mutation :: proc(
	handle: ^Workflow_Handle,
	kind: Workflow_Pause_Mutation_Kind,
	options: Workflow_Pause_Options,
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if kind == .Invalid {
		return error_from_string(.Invalid_Argument, "Workflow pause mutation kind is invalid")
	}
	input := Client_Workflow_Pause_Mutation_Input{
		handle = handle,
		kind = kind,
		options = options,
	}
	state := Client_Interceptor_State{client = handle.client}
	return client_workflow_pause_mutation_chain(&state, &input)
}

pause_workflow :: proc(
	handle: ^Workflow_Handle,
	options: Workflow_Pause_Options = {},
) -> Error {
	return workflow_pause_mutation(handle, .Pause, options)
}

unpause_workflow :: proc(
	handle: ^Workflow_Handle,
	options: Workflow_Pause_Options = {},
) -> Error {
	return workflow_pause_mutation(handle, .Unpause, options)
}

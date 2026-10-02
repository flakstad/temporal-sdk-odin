package workflow_control

import allocator_runtime "base:runtime"
import "core:encoding/uuid"
import client ".."
import temporal_core "../../core"
import proto "../../core/proto"

EXTENSION_KEY :: "temporal.client.workflow_control.v1"

Workflow_Pause_Mutation_Kind :: enum {
	Invalid,
	Pause,
	Unpause,
}

Workflow_Pause_Options :: struct {
	reason:     string,
	request_id: string,
}

Client_Workflow_Pause_Mutation_Input :: struct {
	handle:  ^client.Workflow_Handle,
	kind:    Workflow_Pause_Mutation_Kind,
	options: Workflow_Pause_Options,
}

Client_Workflow_Pause_Mutation_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Workflow_Pause_Mutation_Input,
) -> client.Error

Client_Workflow_Pause_Mutation_Next :: struct {
	data:   rawptr,
	invoke: Client_Workflow_Pause_Mutation_Next_Proc,
}

Client_Workflow_Pause_Mutation_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Workflow_Pause_Mutation_Input,
	next: Client_Workflow_Pause_Mutation_Next,
) -> client.Error

Interceptor :: struct {
	user_data:               rawptr,
	workflow_pause_mutation: Client_Workflow_Pause_Mutation_Interceptor_Proc,
}

Interceptor_State :: struct {
	interceptors: [dynamic]Interceptor,
}

Interceptor_Chain_State :: struct {
	state: ^Interceptor_State,
	index: int,
}

interceptor_state_destroy :: proc(data: rawptr) {
	state := cast(^Interceptor_State)data
	if state == nil {
		return
	}
	heap := allocator_runtime.heap_allocator()
	delete(state.interceptors)
	free(state, heap)
}

add_interceptor :: proc(
	temporal_client: ^client.Client,
	interceptor: Interceptor,
) -> client.Error {
	if temporal_client == nil || temporal_client.connection == nil {
		return client.error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if interceptor.workflow_pause_mutation == nil {
		return client.error_from_string(.Invalid_Argument, "Workflow control interceptor is empty")
	}
	state := cast(^Interceptor_State)client.client_extension_state_find(
		temporal_client,
		EXTENSION_KEY,
	)
	if state == nil {
		heap := allocator_runtime.heap_allocator()
		allocation_error: allocator_runtime.Allocator_Error
		state, allocation_error = new(Interceptor_State, heap)
		if allocation_error != nil {
			return client.error_from_string(.Connection, "could not allocate Workflow control interceptor state")
		}
		state.interceptors = make([dynamic]Interceptor, heap)
		registration_error := client.client_extension_state_register(
			temporal_client,
			EXTENSION_KEY,
			state,
			interceptor_state_destroy,
		)
		if registration_error.message != nil {
			interceptor_state_destroy(state)
			return registration_error
		}
	}
	append(&state.interceptors, interceptor)
	return {}
}

encode_workflow_pause_mutation_request :: proc(
	namespace, identity, workflow_id, run_id: string,
	kind: Workflow_Pause_Mutation_Kind,
	options: Workflow_Pause_Options,
) -> ([dynamic]u8, client.Error) {
	if len(namespace) == 0 || len(workflow_id) == 0 {
		return nil, client.error_from_string(.Invalid_Argument, "Workflow pause identity is incomplete")
	}
	if kind == .Invalid {
		return nil, client.error_from_string(.Invalid_Argument, "Workflow pause mutation kind is invalid")
	}
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request_id := client.resolve_request_id(options.request_id, request_id_buffer[:])
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
		return nil, client.error_from_string(.Invalid_Argument, "Workflow pause mutation kind is invalid")
	}
	return nil, client.error_from_string(.Invalid_Argument, "Workflow pause mutation kind is invalid")
}

workflow_pause_mutation_terminal :: proc(
	input: ^Client_Workflow_Pause_Mutation_Input,
) -> client.Error {
	if input == nil || !client.workflow_handle_valid(input.handle) {
		return client.error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	request_bytes, request_error := encode_workflow_pause_mutation_request(
		input.handle.client.namespace,
		client.client_request_identity(input.handle.client),
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
	if rpc_result.error.message != nil do return client.error_from_core(&rpc_result.error)
	return {}
}

client_workflow_pause_mutation_chain :: proc(
	data: rawptr,
	input: ^Client_Workflow_Pause_Mutation_Input,
) -> client.Error {
	state := cast(^Interceptor_Chain_State)data
	if state == nil || state.state == nil || input == nil {
		return client.error_from_string(.Invalid_Argument, "invalid Workflow pause interceptor state")
	}
	for index := state.index; index < len(state.state.interceptors); index += 1 {
		entry := &state.state.interceptors[index]
		if entry.workflow_pause_mutation == nil do continue
		next_state := Interceptor_Chain_State{state = state.state, index = index + 1}
		return entry.workflow_pause_mutation(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_workflow_pause_mutation_chain},
		)
	}
	return workflow_pause_mutation_terminal(input)
}

client_workflow_pause_mutation_next :: proc(
	next: Client_Workflow_Pause_Mutation_Next,
	input: ^Client_Workflow_Pause_Mutation_Input,
) -> client.Error {
	if next.invoke == nil || input == nil {
		return client.error_from_string(.Invalid_Argument, "invalid Workflow pause interceptor continuation")
	}
	return next.invoke(next.data, input)
}

workflow_pause_mutation :: proc(
	handle: ^client.Workflow_Handle,
	kind: Workflow_Pause_Mutation_Kind,
	options: Workflow_Pause_Options,
) -> client.Error {
	if !client.workflow_handle_valid(handle) {
		return client.error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if kind == .Invalid {
		return client.error_from_string(.Invalid_Argument, "Workflow pause mutation kind is invalid")
	}
	input := Client_Workflow_Pause_Mutation_Input{
		handle = handle,
		kind = kind,
		options = options,
	}
	state := cast(^Interceptor_State)client.client_extension_state_find(
		handle.client,
		EXTENSION_KEY,
	)
	if state == nil {
		return workflow_pause_mutation_terminal(&input)
	}
	return client_workflow_pause_mutation_chain(
		&Interceptor_Chain_State{state = state},
		&input,
	)
}

pause_workflow :: proc(
	handle: ^client.Workflow_Handle,
	options: Workflow_Pause_Options = {},
) -> client.Error {
	return workflow_pause_mutation(handle, .Pause, options)
}

unpause_workflow :: proc(
	handle: ^client.Workflow_Handle,
	options: Workflow_Pause_Options = {},
) -> client.Error {
	return workflow_pause_mutation(handle, .Unpause, options)
}

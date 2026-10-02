package standalone_nexus

import allocator_runtime "base:runtime"
import "core:mem"
import client ".."
import codec "../../codec"

EXTENSION_KEY :: "temporal.client.standalone_nexus.v1"

Client_Start_Nexus_Operation_Input :: struct {
	client:  ^Client,
	input:   codec.Payload_View,
	options: Start_Nexus_Operation_Options,
}

Client_Start_Nexus_Operation_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Start_Nexus_Operation_Input,
) -> (Nexus_Operation_Handle, Error)

Client_Start_Nexus_Operation_Next :: struct {
	data:   rawptr,
	invoke: Client_Start_Nexus_Operation_Next_Proc,
}

Client_Start_Nexus_Operation_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Start_Nexus_Operation_Input,
	next: Client_Start_Nexus_Operation_Next,
) -> (Nexus_Operation_Handle, Error)

Client_Poll_Nexus_Operation_Input :: struct {
	handle:     ^Nexus_Operation_Handle,
	wait_stage: Nexus_Operation_Wait_Stage,
	options:    Nexus_Operation_Wait_Options,
}

Client_Poll_Nexus_Operation_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Poll_Nexus_Operation_Input,
) -> (Nexus_Operation_Poll_Result, Error)

Client_Poll_Nexus_Operation_Next :: struct {
	data:   rawptr,
	invoke: Client_Poll_Nexus_Operation_Next_Proc,
}

Client_Poll_Nexus_Operation_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Poll_Nexus_Operation_Input,
	next: Client_Poll_Nexus_Operation_Next,
) -> (Nexus_Operation_Poll_Result, Error)

Client_Describe_Nexus_Operation_Input :: struct {
	handle:    ^Nexus_Operation_Handle,
	options:   Describe_Nexus_Operation_Options,
	allocator: mem.Allocator,
}

Client_Describe_Nexus_Operation_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Describe_Nexus_Operation_Input,
) -> (Nexus_Operation_Description, Error)

Client_Describe_Nexus_Operation_Next :: struct {
	data:   rawptr,
	invoke: Client_Describe_Nexus_Operation_Next_Proc,
}

Client_Describe_Nexus_Operation_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Describe_Nexus_Operation_Input,
	next: Client_Describe_Nexus_Operation_Next,
) -> (Nexus_Operation_Description, Error)

Nexus_Operation_Mutation_Kind :: enum {
	Cancel,
	Terminate,
	Delete,
}

Client_Nexus_Operation_Mutation_Input :: struct {
	handle:     ^Nexus_Operation_Handle,
	kind:       Nexus_Operation_Mutation_Kind,
	reason:     string,
	request_id: string,
}

Client_Nexus_Operation_Mutation_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Nexus_Operation_Mutation_Input,
) -> Error

Client_Nexus_Operation_Mutation_Next :: struct {
	data:   rawptr,
	invoke: Client_Nexus_Operation_Mutation_Next_Proc,
}

Client_Nexus_Operation_Mutation_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Nexus_Operation_Mutation_Input,
	next: Client_Nexus_Operation_Mutation_Next,
) -> Error

Client_List_Nexus_Operations_Input :: struct {
	client:    ^Client,
	options:   List_Nexus_Operations_Options,
	allocator: mem.Allocator,
}

Client_List_Nexus_Operations_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_List_Nexus_Operations_Input,
) -> (Nexus_Operation_List_Page, Error)

Client_List_Nexus_Operations_Next :: struct {
	data:   rawptr,
	invoke: Client_List_Nexus_Operations_Next_Proc,
}

Client_List_Nexus_Operations_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_List_Nexus_Operations_Input,
	next: Client_List_Nexus_Operations_Next,
) -> (Nexus_Operation_List_Page, Error)

Client_Count_Nexus_Operations_Input :: struct {
	client:    ^Client,
	query:     string,
	allocator: mem.Allocator,
}

Client_Count_Nexus_Operations_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Count_Nexus_Operations_Input,
) -> (Nexus_Operation_Count, Error)

Client_Count_Nexus_Operations_Next :: struct {
	data:   rawptr,
	invoke: Client_Count_Nexus_Operations_Next_Proc,
}

Client_Count_Nexus_Operations_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Count_Nexus_Operations_Input,
	next: Client_Count_Nexus_Operations_Next,
) -> (Nexus_Operation_Count, Error)

Interceptor :: struct {
	user_data:                rawptr,
	start_nexus_operation:    Client_Start_Nexus_Operation_Interceptor_Proc,
	poll_nexus_operation:     Client_Poll_Nexus_Operation_Interceptor_Proc,
	describe_nexus_operation: Client_Describe_Nexus_Operation_Interceptor_Proc,
	nexus_operation_mutation: Client_Nexus_Operation_Mutation_Interceptor_Proc,
	list_nexus_operations:    Client_List_Nexus_Operations_Interceptor_Proc,
	count_nexus_operations:   Client_Count_Nexus_Operations_Interceptor_Proc,
}

Interceptor_State :: struct {
	interceptors: [dynamic]Interceptor,
}

Client_Interceptor_State :: struct {
	state: ^Interceptor_State,
	index: int,
}

interceptor_state_destroy :: proc(data: rawptr) {
	state := cast(^Interceptor_State)data
	if state == nil do return
	delete(state.interceptors)
	free(state, allocator_runtime.heap_allocator())
}

add_interceptor :: proc(temporal_client: ^Client, interceptor: Interceptor) -> Error {
	if temporal_client == nil || temporal_client.connection == nil {
		return error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	state := cast(^Interceptor_State)client.client_extension_state_find(temporal_client, EXTENSION_KEY)
	if state == nil {
		heap := allocator_runtime.heap_allocator()
		allocation_error: allocator_runtime.Allocator_Error
		state, allocation_error = new(Interceptor_State, heap)
		if allocation_error != nil {
			return error_from_string(.Connection, "could not allocate standalone Nexus interceptor state")
		}
		state.interceptors = make([dynamic]Interceptor, heap)
		register_error := client.client_extension_state_register(
			temporal_client,
			EXTENSION_KEY,
			state,
			interceptor_state_destroy,
		)
		if register_error.message != nil {
			interceptor_state_destroy(state)
			return register_error
		}
	}
	append(&state.interceptors, interceptor)
	return {}
}

client_interceptor_state :: proc(temporal_client: ^Client) -> Client_Interceptor_State {
	return {state = cast(^Interceptor_State)client.client_extension_state_find(temporal_client, EXTENSION_KEY)}
}

client_start_nexus_operation_next :: proc(
	next: Client_Start_Nexus_Operation_Next,
	input: ^Client_Start_Nexus_Operation_Input,
) -> (Nexus_Operation_Handle, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation start interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_poll_nexus_operation_next :: proc(
	next: Client_Poll_Nexus_Operation_Next,
	input: ^Client_Poll_Nexus_Operation_Input,
) -> (Nexus_Operation_Poll_Result, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation poll interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_describe_nexus_operation_next :: proc(
	next: Client_Describe_Nexus_Operation_Next,
	input: ^Client_Describe_Nexus_Operation_Input,
) -> (Nexus_Operation_Description, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation Describe interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_nexus_operation_mutation_next :: proc(
	next: Client_Nexus_Operation_Mutation_Next,
	input: ^Client_Nexus_Operation_Mutation_Input,
) -> Error {
	if next.invoke == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid Nexus operation mutation interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_list_nexus_operations_next :: proc(
	next: Client_List_Nexus_Operations_Next,
	input: ^Client_List_Nexus_Operations_Input,
) -> (Nexus_Operation_List_Page, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation list interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_count_nexus_operations_next :: proc(
	next: Client_Count_Nexus_Operations_Next,
	input: ^Client_Count_Nexus_Operations_Input,
) -> (Nexus_Operation_Count, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation count interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_start_nexus_operation_chain :: proc(
	data: rawptr,
	input: ^Client_Start_Nexus_Operation_Input,
) -> (Nexus_Operation_Handle, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation start interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.start_nexus_operation == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.start_nexus_operation(
				entry.user_data,
				input,
				{data = &next, invoke = client_start_nexus_operation_chain},
			)
		}
	}
	return start_nexus_operation_payload_terminal(input)
}

client_poll_nexus_operation_chain :: proc(
	data: rawptr,
	input: ^Client_Poll_Nexus_Operation_Input,
) -> (Nexus_Operation_Poll_Result, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation poll interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.poll_nexus_operation == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.poll_nexus_operation(
				entry.user_data,
				input,
				{data = &next, invoke = client_poll_nexus_operation_chain},
			)
		}
	}
	return poll_nexus_operation_terminal(input)
}

client_describe_nexus_operation_chain :: proc(
	data: rawptr,
	input: ^Client_Describe_Nexus_Operation_Input,
) -> (Nexus_Operation_Description, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation Describe interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.describe_nexus_operation == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.describe_nexus_operation(
				entry.user_data,
				input,
				{data = &next, invoke = client_describe_nexus_operation_chain},
			)
		}
	}
	return describe_nexus_operation_terminal(input)
}

client_nexus_operation_mutation_chain :: proc(
	data: rawptr,
	input: ^Client_Nexus_Operation_Mutation_Input,
) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid Nexus operation mutation interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.nexus_operation_mutation == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.nexus_operation_mutation(
				entry.user_data,
				input,
				{data = &next, invoke = client_nexus_operation_mutation_chain},
			)
		}
	}
	return nexus_operation_mutation_terminal(input)
}

client_list_nexus_operations_chain :: proc(
	data: rawptr,
	input: ^Client_List_Nexus_Operations_Input,
) -> (Nexus_Operation_List_Page, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation list interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.list_nexus_operations == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.list_nexus_operations(
				entry.user_data,
				input,
				{data = &next, invoke = client_list_nexus_operations_chain},
			)
		}
	}
	return list_nexus_operations_terminal(input)
}

client_count_nexus_operations_chain :: proc(
	data: rawptr,
	input: ^Client_Count_Nexus_Operations_Input,
) -> (Nexus_Operation_Count, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation count interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.count_nexus_operations == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.count_nexus_operations(
				entry.user_data,
				input,
				{data = &next, invoke = client_count_nexus_operations_chain},
			)
		}
	}
	return count_nexus_operations_terminal(input)
}

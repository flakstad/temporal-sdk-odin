package standalone_activity

import allocator_runtime "base:runtime"
import "core:mem"
import "core:time"
import client ".."
import codec "../../codec"

EXTENSION_KEY :: "temporal.client.standalone_activity.v1"

Client_Start_Standalone_Activity_Input :: struct {
	client:  ^Client,
	input:   codec.Payload_View,
	options: Start_Standalone_Activity_Options,
}
Client_Start_Standalone_Activity_Next_Proc :: #type proc(rawptr, ^Client_Start_Standalone_Activity_Input) -> (Standalone_Activity_Handle, Error)
Client_Start_Standalone_Activity_Next :: struct {data: rawptr, invoke: Client_Start_Standalone_Activity_Next_Proc}
Client_Start_Standalone_Activity_Interceptor_Proc :: #type proc(rawptr, ^Client_Start_Standalone_Activity_Input, Client_Start_Standalone_Activity_Next) -> (Standalone_Activity_Handle, Error)

Client_Poll_Standalone_Activity_Input :: struct {
	handle:  ^Standalone_Activity_Handle,
	options: Standalone_Activity_Wait_Options,
}
Client_Poll_Standalone_Activity_Next_Proc :: #type proc(rawptr, ^Client_Poll_Standalone_Activity_Input) -> (Standalone_Activity_Poll_Result, Error)
Client_Poll_Standalone_Activity_Next :: struct {data: rawptr, invoke: Client_Poll_Standalone_Activity_Next_Proc}
Client_Poll_Standalone_Activity_Interceptor_Proc :: #type proc(rawptr, ^Client_Poll_Standalone_Activity_Input, Client_Poll_Standalone_Activity_Next) -> (Standalone_Activity_Poll_Result, Error)

Client_Describe_Standalone_Activity_Input :: struct {
	handle:    ^Standalone_Activity_Handle,
	options:   Describe_Standalone_Activity_Options,
	allocator: mem.Allocator,
}
Client_Describe_Standalone_Activity_Next_Proc :: #type proc(rawptr, ^Client_Describe_Standalone_Activity_Input) -> (Standalone_Activity_Description, Error)
Client_Describe_Standalone_Activity_Next :: struct {data: rawptr, invoke: Client_Describe_Standalone_Activity_Next_Proc}
Client_Describe_Standalone_Activity_Interceptor_Proc :: #type proc(rawptr, ^Client_Describe_Standalone_Activity_Input, Client_Describe_Standalone_Activity_Next) -> (Standalone_Activity_Description, Error)

Standalone_Activity_Mutation_Kind :: enum {Cancel, Terminate, Delete}
Client_Standalone_Activity_Mutation_Input :: struct {
	handle:     ^Standalone_Activity_Handle,
	kind:       Standalone_Activity_Mutation_Kind,
	reason:     string,
	request_id: string,
}
Client_Standalone_Activity_Mutation_Next_Proc :: #type proc(rawptr, ^Client_Standalone_Activity_Mutation_Input) -> Error
Client_Standalone_Activity_Mutation_Next :: struct {data: rawptr, invoke: Client_Standalone_Activity_Mutation_Next_Proc}
Client_Standalone_Activity_Mutation_Interceptor_Proc :: #type proc(rawptr, ^Client_Standalone_Activity_Mutation_Input, Client_Standalone_Activity_Mutation_Next) -> Error

Standalone_Activity_Control_Kind :: enum {Invalid, Pause, Unpause, Reset}
Standalone_Activity_Control_Options :: struct {
	reason:                   string,
	request_id:               string,
	jitter:                   time.Duration,
	keep_paused:              bool,
	restore_original_options: bool,
	reset_heartbeat:          bool,
}
Client_Standalone_Activity_Control_Input :: struct {
	handle:  ^Standalone_Activity_Handle,
	kind:    Standalone_Activity_Control_Kind,
	options: Standalone_Activity_Control_Options,
}
Client_Standalone_Activity_Control_Next_Proc :: #type proc(rawptr, ^Client_Standalone_Activity_Control_Input) -> Error
Client_Standalone_Activity_Control_Next :: struct {data: rawptr, invoke: Client_Standalone_Activity_Control_Next_Proc}
Client_Standalone_Activity_Control_Interceptor_Proc :: #type proc(rawptr, ^Client_Standalone_Activity_Control_Input, Client_Standalone_Activity_Control_Next) -> Error

Client_Update_Standalone_Activity_Options_Input :: struct {
	handle:    ^Standalone_Activity_Handle,
	update:    Standalone_Activity_Options_Update,
	allocator: mem.Allocator,
}
Client_Update_Standalone_Activity_Options_Next_Proc :: #type proc(rawptr, ^Client_Update_Standalone_Activity_Options_Input) -> (Standalone_Activity_Options, Error)
Client_Update_Standalone_Activity_Options_Next :: struct {data: rawptr, invoke: Client_Update_Standalone_Activity_Options_Next_Proc}
Client_Update_Standalone_Activity_Options_Interceptor_Proc :: #type proc(rawptr, ^Client_Update_Standalone_Activity_Options_Input, Client_Update_Standalone_Activity_Options_Next) -> (Standalone_Activity_Options, Error)

Client_List_Standalone_Activities_Input :: struct {
	client:    ^Client,
	options:   List_Standalone_Activities_Options,
	allocator: mem.Allocator,
}
Client_List_Standalone_Activities_Next_Proc :: #type proc(rawptr, ^Client_List_Standalone_Activities_Input) -> (Standalone_Activity_List_Page, Error)
Client_List_Standalone_Activities_Next :: struct {data: rawptr, invoke: Client_List_Standalone_Activities_Next_Proc}
Client_List_Standalone_Activities_Interceptor_Proc :: #type proc(rawptr, ^Client_List_Standalone_Activities_Input, Client_List_Standalone_Activities_Next) -> (Standalone_Activity_List_Page, Error)

Client_Count_Standalone_Activities_Input :: struct {
	client:    ^Client,
	query:     string,
	allocator: mem.Allocator,
}
Client_Count_Standalone_Activities_Next_Proc :: #type proc(rawptr, ^Client_Count_Standalone_Activities_Input) -> (Standalone_Activity_Count, Error)
Client_Count_Standalone_Activities_Next :: struct {data: rawptr, invoke: Client_Count_Standalone_Activities_Next_Proc}
Client_Count_Standalone_Activities_Interceptor_Proc :: #type proc(rawptr, ^Client_Count_Standalone_Activities_Input, Client_Count_Standalone_Activities_Next) -> (Standalone_Activity_Count, Error)

Interceptor :: struct {
	user_data:                         rawptr,
	start_standalone_activity:         Client_Start_Standalone_Activity_Interceptor_Proc,
	poll_standalone_activity:          Client_Poll_Standalone_Activity_Interceptor_Proc,
	describe_standalone_activity:      Client_Describe_Standalone_Activity_Interceptor_Proc,
	standalone_activity_mutation:      Client_Standalone_Activity_Mutation_Interceptor_Proc,
	standalone_activity_control:       Client_Standalone_Activity_Control_Interceptor_Proc,
	update_standalone_activity_options: Client_Update_Standalone_Activity_Options_Interceptor_Proc,
	list_standalone_activities:        Client_List_Standalone_Activities_Interceptor_Proc,
	count_standalone_activities:       Client_Count_Standalone_Activities_Interceptor_Proc,
}

Interceptor_State :: struct {interceptors: [dynamic]Interceptor}
Client_Interceptor_State :: struct {state: ^Interceptor_State, index: int}

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
			return error_from_string(.Connection, "could not allocate standalone Activity interceptor state")
		}
		state.interceptors = make([dynamic]Interceptor, heap)
		register_error := client.client_extension_state_register(temporal_client, EXTENSION_KEY, state, interceptor_state_destroy)
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

client_start_standalone_activity_next :: proc(next: Client_Start_Standalone_Activity_Next, input: ^Client_Start_Standalone_Activity_Input) -> (Standalone_Activity_Handle, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity start interceptor continuation")
	return next.invoke(next.data, input)
}
client_poll_standalone_activity_next :: proc(next: Client_Poll_Standalone_Activity_Next, input: ^Client_Poll_Standalone_Activity_Input) -> (Standalone_Activity_Poll_Result, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity poll interceptor continuation")
	return next.invoke(next.data, input)
}
client_describe_standalone_activity_next :: proc(next: Client_Describe_Standalone_Activity_Next, input: ^Client_Describe_Standalone_Activity_Input) -> (Standalone_Activity_Description, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity Describe interceptor continuation")
	return next.invoke(next.data, input)
}
client_standalone_activity_mutation_next :: proc(next: Client_Standalone_Activity_Mutation_Next, input: ^Client_Standalone_Activity_Mutation_Input) -> Error {
	if next.invoke == nil || input == nil do return error_from_string(.Invalid_Argument, "invalid standalone Activity mutation interceptor continuation")
	return next.invoke(next.data, input)
}
client_standalone_activity_control_next :: proc(next: Client_Standalone_Activity_Control_Next, input: ^Client_Standalone_Activity_Control_Input) -> Error {
	if next.invoke == nil || input == nil do return error_from_string(.Invalid_Argument, "invalid standalone Activity control interceptor continuation")
	return next.invoke(next.data, input)
}
client_update_standalone_activity_options_next :: proc(next: Client_Update_Standalone_Activity_Options_Next, input: ^Client_Update_Standalone_Activity_Options_Input) -> (Standalone_Activity_Options, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity options interceptor continuation")
	return next.invoke(next.data, input)
}
client_list_standalone_activities_next :: proc(next: Client_List_Standalone_Activities_Next, input: ^Client_List_Standalone_Activities_Input) -> (Standalone_Activity_List_Page, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity list interceptor continuation")
	return next.invoke(next.data, input)
}
client_count_standalone_activities_next :: proc(next: Client_Count_Standalone_Activities_Next, input: ^Client_Count_Standalone_Activities_Input) -> (Standalone_Activity_Count, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity count interceptor continuation")
	return next.invoke(next.data, input)
}

client_start_standalone_activity_chain :: proc(data: rawptr, input: ^Client_Start_Standalone_Activity_Input) -> (Standalone_Activity_Handle, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity start interceptor state")
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.start_standalone_activity == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.start_standalone_activity(entry.user_data, input, {data = &next, invoke = client_start_standalone_activity_chain})
		}
	}
	return start_standalone_activity_payload_terminal(input)
}

client_poll_standalone_activity_chain :: proc(data: rawptr, input: ^Client_Poll_Standalone_Activity_Input) -> (Standalone_Activity_Poll_Result, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity poll interceptor state")
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.poll_standalone_activity == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.poll_standalone_activity(entry.user_data, input, {data = &next, invoke = client_poll_standalone_activity_chain})
		}
	}
	return poll_standalone_activity_terminal(input)
}

client_describe_standalone_activity_chain :: proc(data: rawptr, input: ^Client_Describe_Standalone_Activity_Input) -> (Standalone_Activity_Description, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity Describe interceptor state")
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.describe_standalone_activity == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.describe_standalone_activity(entry.user_data, input, {data = &next, invoke = client_describe_standalone_activity_chain})
		}
	}
	return describe_standalone_activity_terminal(input)
}

client_standalone_activity_mutation_chain :: proc(data: rawptr, input: ^Client_Standalone_Activity_Mutation_Input) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil do return error_from_string(.Invalid_Argument, "invalid standalone Activity mutation interceptor state")
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.standalone_activity_mutation == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.standalone_activity_mutation(entry.user_data, input, {data = &next, invoke = client_standalone_activity_mutation_chain})
		}
	}
	return standalone_activity_mutation_terminal(input)
}

client_standalone_activity_control_chain :: proc(data: rawptr, input: ^Client_Standalone_Activity_Control_Input) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil do return error_from_string(.Invalid_Argument, "invalid standalone Activity control interceptor state")
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.standalone_activity_control == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.standalone_activity_control(entry.user_data, input, {data = &next, invoke = client_standalone_activity_control_chain})
		}
	}
	return standalone_activity_control_terminal(input)
}

client_update_standalone_activity_options_chain :: proc(data: rawptr, input: ^Client_Update_Standalone_Activity_Options_Input) -> (Standalone_Activity_Options, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity options interceptor state")
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.update_standalone_activity_options == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.update_standalone_activity_options(entry.user_data, input, {data = &next, invoke = client_update_standalone_activity_options_chain})
		}
	}
	return update_standalone_activity_options_terminal(input)
}

client_list_standalone_activities_chain :: proc(data: rawptr, input: ^Client_List_Standalone_Activities_Input) -> (Standalone_Activity_List_Page, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity list interceptor state")
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.list_standalone_activities == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.list_standalone_activities(entry.user_data, input, {data = &next, invoke = client_list_standalone_activities_chain})
		}
	}
	return list_standalone_activities_terminal(input)
}

client_count_standalone_activities_chain :: proc(data: rawptr, input: ^Client_Count_Standalone_Activities_Input) -> (Standalone_Activity_Count, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity count interceptor state")
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.count_standalone_activities == nil do continue
			next := Client_Interceptor_State{state = state.state, index = state.index + index + 1}
			return entry.count_standalone_activities(entry.user_data, input, {data = &next, invoke = client_count_standalone_activities_chain})
		}
	}
	return count_standalone_activities_terminal(input)
}

package schedule

import allocator_runtime "base:runtime"
import "core:mem"
import "core:time"
import client ".."
import codec "../../codec"

EXTENSION_KEY :: "temporal.client.schedule.v1"

Client_Describe_Schedule_Input :: struct {
	handle:    ^Schedule_Handle,
	allocator: mem.Allocator,
}
Client_Describe_Schedule_Next_Proc :: #type proc(rawptr, ^Client_Describe_Schedule_Input) -> (Schedule_Description, Error)
Client_Describe_Schedule_Next :: struct {
	data:   rawptr,
	invoke: Client_Describe_Schedule_Next_Proc,
}
Client_Describe_Schedule_Interceptor_Proc :: #type proc(rawptr, ^Client_Describe_Schedule_Input, Client_Describe_Schedule_Next) -> (Schedule_Description, Error)

Client_List_Schedules_Input :: struct {
	client:  ^Client,
	options: List_Schedules_Options,
}
Client_List_Schedules_Next_Proc :: #type proc(rawptr, ^Client_List_Schedules_Input) -> (Schedule_List_Page, Error)
Client_List_Schedules_Next :: struct {
	data:   rawptr,
	invoke: Client_List_Schedules_Next_Proc,
}
Client_List_Schedules_Interceptor_Proc :: #type proc(rawptr, ^Client_List_Schedules_Input, Client_List_Schedules_Next) -> (Schedule_List_Page, Error)

Client_Count_Schedules_Input :: struct {
	client: ^Client,
	query:  string,
}
Client_Count_Schedules_Next_Proc :: #type proc(rawptr, ^Client_Count_Schedules_Input) -> (Schedule_Count, Error)
Client_Count_Schedules_Next :: struct {
	data:   rawptr,
	invoke: Client_Count_Schedules_Next_Proc,
}
Client_Count_Schedules_Interceptor_Proc :: #type proc(rawptr, ^Client_Count_Schedules_Input, Client_Count_Schedules_Next) -> (Schedule_Count, Error)

Client_List_Schedule_Matching_Times_Input :: struct {
	handle: ^Schedule_Handle,
	start_time, end_time: time.Time,
	allocator: mem.Allocator,
}
Client_List_Schedule_Matching_Times_Next_Proc :: #type proc(rawptr, ^Client_List_Schedule_Matching_Times_Input) -> (Schedule_Matching_Times, Error)
Client_List_Schedule_Matching_Times_Next :: struct {
	data:   rawptr,
	invoke: Client_List_Schedule_Matching_Times_Next_Proc,
}
Client_List_Schedule_Matching_Times_Interceptor_Proc :: #type proc(rawptr, ^Client_List_Schedule_Matching_Times_Input, Client_List_Schedule_Matching_Times_Next) -> (Schedule_Matching_Times, Error)

Client_Create_Schedule_Input :: struct {
	client:                     ^Client,
	schedule_id, workflow_type: string,
	input:                      codec.Payload_View,
	options:                    Schedule_Create_Options,
}
Client_Create_Schedule_Next_Proc :: #type proc(rawptr, ^Client_Create_Schedule_Input) -> (Schedule_Handle, Error)
Client_Create_Schedule_Next :: struct {
	data:   rawptr,
	invoke: Client_Create_Schedule_Next_Proc,
}
Client_Create_Schedule_Interceptor_Proc :: #type proc(rawptr, ^Client_Create_Schedule_Input, Client_Create_Schedule_Next) -> (Schedule_Handle, Error)

Schedule_Mutation_Kind :: enum {Update, Backfill, Pause, Unpause, Trigger, Delete}
Client_Schedule_Mutation_Input :: struct {
	handle: ^Schedule_Handle,
	kind: Schedule_Mutation_Kind,
	workflow_type: string,
	input: codec.Payload_View,
	options: Schedule_Create_Options,
	conflict_token: []u8,
	backfills: []Schedule_Backfill,
	note: string,
	overlap_policy: Schedule_Overlap_Policy,
}
Client_Schedule_Mutation_Next_Proc :: #type proc(rawptr, ^Client_Schedule_Mutation_Input) -> Error
Client_Schedule_Mutation_Next :: struct {
	data:   rawptr,
	invoke: Client_Schedule_Mutation_Next_Proc,
}
Client_Schedule_Mutation_Interceptor_Proc :: #type proc(rawptr, ^Client_Schedule_Mutation_Input, Client_Schedule_Mutation_Next) -> Error

Interceptor :: struct {
	user_data: rawptr,
	describe_schedule: Client_Describe_Schedule_Interceptor_Proc,
	list_schedules: Client_List_Schedules_Interceptor_Proc,
	count_schedules: Client_Count_Schedules_Interceptor_Proc,
	list_schedule_matching_times: Client_List_Schedule_Matching_Times_Interceptor_Proc,
	create_schedule: Client_Create_Schedule_Interceptor_Proc,
	schedule_mutation: Client_Schedule_Mutation_Interceptor_Proc,
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
			return error_from_string(.Connection, "could not allocate Schedule interceptor state")
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

client_describe_schedule_next :: proc(next: Client_Describe_Schedule_Next, input: ^Client_Describe_Schedule_Input) -> (Schedule_Description, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule Describe interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_list_schedules_next :: proc(next: Client_List_Schedules_Next, input: ^Client_List_Schedules_Input) -> (Schedule_List_Page, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule list interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_count_schedules_next :: proc(next: Client_Count_Schedules_Next, input: ^Client_Count_Schedules_Input) -> (Schedule_Count, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule count interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_list_schedule_matching_times_next :: proc(next: Client_List_Schedule_Matching_Times_Next, input: ^Client_List_Schedule_Matching_Times_Input) -> (Schedule_Matching_Times, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule matching-times interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_create_schedule_next :: proc(next: Client_Create_Schedule_Next, input: ^Client_Create_Schedule_Input) -> (Schedule_Handle, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule Create interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_schedule_mutation_next :: proc(next: Client_Schedule_Mutation_Next, input: ^Client_Schedule_Mutation_Input) -> Error {
	if next.invoke == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid Schedule mutation interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_describe_schedule_chain :: proc(data: rawptr, input: ^Client_Describe_Schedule_Input) -> (Schedule_Description, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule Describe interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.describe_schedule != nil {
				next := Client_Interceptor_State{
					state = state.state,
					index = state.index + index + 1,
				}
				return entry.describe_schedule(
					entry.user_data,
					input,
					{data = &next, invoke = client_describe_schedule_chain},
				)
			}
		}
	}
	return describe_schedule_terminal(input)
}

client_list_schedules_chain :: proc(data: rawptr, input: ^Client_List_Schedules_Input) -> (Schedule_List_Page, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule list interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.list_schedules != nil {
				next := Client_Interceptor_State{
					state = state.state,
					index = state.index + index + 1,
				}
				return entry.list_schedules(
					entry.user_data,
					input,
					{data = &next, invoke = client_list_schedules_chain},
				)
			}
		}
	}
	return list_schedules_terminal(input)
}

client_count_schedules_chain :: proc(data: rawptr, input: ^Client_Count_Schedules_Input) -> (Schedule_Count, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule count interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.count_schedules != nil {
				next := Client_Interceptor_State{
					state = state.state,
					index = state.index + index + 1,
				}
				return entry.count_schedules(
					entry.user_data,
					input,
					{data = &next, invoke = client_count_schedules_chain},
				)
			}
		}
	}
	return count_schedules_terminal(input)
}

client_list_schedule_matching_times_chain :: proc(data: rawptr, input: ^Client_List_Schedule_Matching_Times_Input) -> (Schedule_Matching_Times, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule matching-times interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.list_schedule_matching_times != nil {
				next := Client_Interceptor_State{
					state = state.state,
					index = state.index + index + 1,
				}
				return entry.list_schedule_matching_times(
					entry.user_data,
					input,
					{data = &next, invoke = client_list_schedule_matching_times_chain},
				)
			}
		}
	}
	return list_schedule_matching_times_terminal(input)
}

client_create_schedule_chain :: proc(data: rawptr, input: ^Client_Create_Schedule_Input) -> (Schedule_Handle, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Schedule Create interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.create_schedule != nil {
				next := Client_Interceptor_State{
					state = state.state,
					index = state.index + index + 1,
				}
				return entry.create_schedule(
					entry.user_data,
					input,
					{data = &next, invoke = client_create_schedule_chain},
				)
			}
		}
	}
	return create_schedule_payload_terminal(input)
}

client_schedule_mutation_chain :: proc(data: rawptr, input: ^Client_Schedule_Mutation_Input) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid Schedule mutation interceptor state")
	}
	if state.state != nil {
		for entry, index in state.state.interceptors[state.index:] {
			if entry.schedule_mutation != nil {
				next := Client_Interceptor_State{
					state = state.state,
					index = state.index + index + 1,
				}
				return entry.schedule_mutation(
					entry.user_data,
					input,
					{data = &next, invoke = client_schedule_mutation_chain},
				)
			}
		}
	}
	return schedule_mutation_terminal(input)
}

package client

import "core:mem"
import "core:time"
import activity_codec "../codec"

// Client outbound interceptors are copied into Client storage at connect time.
// user_data remains application-owned and must outlive the Client. Input,
// headers, and next continuations are scoped to one synchronous SDK call.
Client_Start_Workflow_Input :: struct {
	client:        ^Client,
	workflow_type: string,
	inputs:        []activity_codec.Payload_View,
	options:       Start_Workflow_Options,
	headers:       ^Headers,
}

Client_Start_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Start_Workflow_Input,
) -> (Workflow_Handle, Error)

Client_Start_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Client_Start_Workflow_Next_Proc,
}

Client_Start_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Start_Workflow_Input,
	next: Client_Start_Workflow_Next,
) -> (Workflow_Handle, Error)

Client_Signal_With_Start_Input :: struct {
	client:         ^Client,
	workflow_type:  string,
	workflow_input: activity_codec.Payload_View,
	signal_name:    string,
	signal_input:   activity_codec.Payload_View,
	options:        Signal_With_Start_Options,
	headers:        ^Headers,
}

Client_Signal_With_Start_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Signal_With_Start_Input,
) -> (Workflow_Handle, Error)

Client_Signal_With_Start_Next :: struct {
	data:   rawptr,
	invoke: Client_Signal_With_Start_Next_Proc,
}

Client_Signal_With_Start_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Signal_With_Start_Input,
	next: Client_Signal_With_Start_Next,
) -> (Workflow_Handle, Error)

Client_Signal_Workflow_Input :: struct {
	handle:      ^Workflow_Handle,
	signal_name: string,
	inputs:      []activity_codec.Payload_View,
	options:     Signal_Workflow_Options,
	headers:     ^Headers,
}

Client_Signal_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Signal_Workflow_Input,
) -> Error

Client_Signal_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Client_Signal_Workflow_Next_Proc,
}

Client_Signal_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Signal_Workflow_Input,
	next: Client_Signal_Workflow_Next,
) -> Error

Client_Query_Workflow_Input :: struct {
	handle:     ^Workflow_Handle,
	query_type: string,
	inputs:     []activity_codec.Payload_View,
	options:    Query_Workflow_Options,
	headers:    ^Headers,
}

Client_Query_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Query_Workflow_Input,
) -> (activity_codec.Payload, Error)

Client_Query_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Client_Query_Workflow_Next_Proc,
}

Client_Query_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Query_Workflow_Input,
	next: Client_Query_Workflow_Next,
) -> (activity_codec.Payload, Error)

Client_Get_Result_Input :: struct {
	handle:      ^Workflow_Handle,
	follow_runs: bool,
}

Client_Get_Result_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Get_Result_Input,
) -> (activity_codec.Payload, Error)

Client_Get_Result_Next :: struct {
	data:   rawptr,
	invoke: Client_Get_Result_Next_Proc,
}

Client_Get_Result_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Get_Result_Input,
	next: Client_Get_Result_Next,
) -> (activity_codec.Payload, Error)

Client_Start_Update_Input :: struct {
	handle:      ^Workflow_Handle,
	update_name: string,
	inputs:      []activity_codec.Payload_View,
	options:     Update_Workflow_Options,
	headers:     ^Headers,
}

Client_Start_Update_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Start_Update_Input,
) -> (Update_Handle, Error)

Client_Start_Update_Next :: struct {
	data:   rawptr,
	invoke: Client_Start_Update_Next_Proc,
}

Client_Start_Update_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Start_Update_Input,
	next: Client_Start_Update_Next,
) -> (Update_Handle, Error)

Client_Wait_Update_Input :: struct {
	handle:  ^Update_Handle,
	options: Update_Wait_Options,
}

Client_Wait_Update_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Wait_Update_Input,
) -> (activity_codec.Payload, Error)

Client_Wait_Update_Next :: struct {
	data:   rawptr,
	invoke: Client_Wait_Update_Next_Proc,
}

Client_Wait_Update_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Wait_Update_Input,
	next: Client_Wait_Update_Next,
) -> (activity_codec.Payload, Error)

Client_Cancel_Workflow_Input :: struct {
	handle:     ^Workflow_Handle,
	reason:     string,
	request_id: string,
}

Client_Cancel_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Cancel_Workflow_Input,
) -> Error

Client_Cancel_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Client_Cancel_Workflow_Next_Proc,
}

Client_Cancel_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Cancel_Workflow_Input,
	next: Client_Cancel_Workflow_Next,
) -> Error

Client_Reset_Workflow_Input :: struct {
	handle:  ^Workflow_Handle,
	options: Reset_Workflow_Options,
}

Client_Reset_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Reset_Workflow_Input,
) -> (Workflow_Handle, Error)

Client_Reset_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Client_Reset_Workflow_Next_Proc,
}

Client_Reset_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Reset_Workflow_Input,
	next: Client_Reset_Workflow_Next,
) -> (Workflow_Handle, Error)

Client_Terminate_Workflow_Input :: struct {
	handle:      ^Workflow_Handle,
	reason:      string,
	details:     activity_codec.Payload_View,
	has_details: bool,
}

Client_Terminate_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Terminate_Workflow_Input,
) -> Error

Client_Terminate_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Client_Terminate_Workflow_Next_Proc,
}

Client_Terminate_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Terminate_Workflow_Input,
	next: Client_Terminate_Workflow_Next,
) -> Error

Client_List_Workflows_Input :: struct {
	client:  ^Client,
	options: List_Workflows_Options,
}

Client_List_Workflows_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_List_Workflows_Input,
) -> (Workflow_List_Page, Error)

Client_List_Workflows_Next :: struct {
	data:   rawptr,
	invoke: Client_List_Workflows_Next_Proc,
}

Client_List_Workflows_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_List_Workflows_Input,
	next: Client_List_Workflows_Next,
) -> (Workflow_List_Page, Error)

Client_Count_Workflows_Input :: struct {
	client: ^Client,
	query:  string,
}

Client_Count_Workflows_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Count_Workflows_Input,
) -> (Workflow_Count, Error)

Client_Count_Workflows_Next :: struct {
	data:   rawptr,
	invoke: Client_Count_Workflows_Next_Proc,
}

Client_Count_Workflows_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Count_Workflows_Input,
	next: Client_Count_Workflows_Next,
) -> (Workflow_Count, Error)

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

Client_Describe_Schedule_Input :: struct {
	handle:    ^Schedule_Handle,
	allocator: mem.Allocator,
}

Client_Describe_Schedule_Next_Proc :: #type proc(data: rawptr, input: ^Client_Describe_Schedule_Input) -> (Schedule_Description, Error)
Client_Describe_Schedule_Next :: struct { data: rawptr, invoke: Client_Describe_Schedule_Next_Proc }
Client_Describe_Schedule_Interceptor_Proc :: #type proc(user_data: rawptr, input: ^Client_Describe_Schedule_Input, next: Client_Describe_Schedule_Next) -> (Schedule_Description, Error)

Client_List_Schedules_Input :: struct {
	client:  ^Client,
	options: List_Schedules_Options,
}

Client_List_Schedules_Next_Proc :: #type proc(data: rawptr, input: ^Client_List_Schedules_Input) -> (Schedule_List_Page, Error)
Client_List_Schedules_Next :: struct { data: rawptr, invoke: Client_List_Schedules_Next_Proc }
Client_List_Schedules_Interceptor_Proc :: #type proc(user_data: rawptr, input: ^Client_List_Schedules_Input, next: Client_List_Schedules_Next) -> (Schedule_List_Page, Error)

Client_Count_Schedules_Input :: struct { client: ^Client, query: string }
Client_Count_Schedules_Next_Proc :: #type proc(data: rawptr, input: ^Client_Count_Schedules_Input) -> (Schedule_Count, Error)
Client_Count_Schedules_Next :: struct { data: rawptr, invoke: Client_Count_Schedules_Next_Proc }
Client_Count_Schedules_Interceptor_Proc :: #type proc(user_data: rawptr, input: ^Client_Count_Schedules_Input, next: Client_Count_Schedules_Next) -> (Schedule_Count, Error)

Client_List_Schedule_Matching_Times_Input :: struct {
	handle:     ^Schedule_Handle,
	start_time: time.Time,
	end_time:   time.Time,
	allocator:  mem.Allocator,
}

Client_List_Schedule_Matching_Times_Next_Proc :: #type proc(data: rawptr, input: ^Client_List_Schedule_Matching_Times_Input) -> (Schedule_Matching_Times, Error)
Client_List_Schedule_Matching_Times_Next :: struct { data: rawptr, invoke: Client_List_Schedule_Matching_Times_Next_Proc }
Client_List_Schedule_Matching_Times_Interceptor_Proc :: #type proc(user_data: rawptr, input: ^Client_List_Schedule_Matching_Times_Input, next: Client_List_Schedule_Matching_Times_Next) -> (Schedule_Matching_Times, Error)

Client_Create_Schedule_Input :: struct { client: ^Client, schedule_id: string, workflow_type: string, input: activity_codec.Payload_View, options: Schedule_Create_Options }
Client_Create_Schedule_Next_Proc :: #type proc(data: rawptr, input: ^Client_Create_Schedule_Input) -> (Schedule_Handle, Error)
Client_Create_Schedule_Next :: struct { data: rawptr, invoke: Client_Create_Schedule_Next_Proc }
Client_Create_Schedule_Interceptor_Proc :: #type proc(user_data: rawptr, input: ^Client_Create_Schedule_Input, next: Client_Create_Schedule_Next) -> (Schedule_Handle, Error)

Schedule_Mutation_Kind :: enum { Update, Backfill, Pause, Unpause, Trigger, Delete }
Client_Schedule_Mutation_Input :: struct {
	handle: ^Schedule_Handle,
	kind: Schedule_Mutation_Kind,
	workflow_type: string,
	input: activity_codec.Payload_View,
	options: Schedule_Create_Options,
	conflict_token: []u8,
	backfills: []Schedule_Backfill,
	note: string,
	overlap_policy: Schedule_Overlap_Policy,
}
Client_Schedule_Mutation_Next_Proc :: #type proc(data: rawptr, input: ^Client_Schedule_Mutation_Input) -> Error
Client_Schedule_Mutation_Next :: struct { data: rawptr, invoke: Client_Schedule_Mutation_Next_Proc }
Client_Schedule_Mutation_Interceptor_Proc :: #type proc(user_data: rawptr, input: ^Client_Schedule_Mutation_Input, next: Client_Schedule_Mutation_Next) -> Error

Connection_Mutation_Kind :: enum { RPC_Metadata, API_Key }
Client_Connection_Mutation_Input :: struct {
	client:           ^Client,
	kind:             Connection_Mutation_Kind,
	metadata:         []RPC_Metadata_Entry,
	binary_metadata:  []RPC_Metadata_Entry,
	api_key:          string,
}
Client_Connection_Mutation_Next_Proc :: #type proc(data: rawptr, input: ^Client_Connection_Mutation_Input) -> Error
Client_Connection_Mutation_Next :: struct { data: rawptr, invoke: Client_Connection_Mutation_Next_Proc }
Client_Connection_Mutation_Interceptor_Proc :: #type proc(user_data: rawptr, input: ^Client_Connection_Mutation_Input, next: Client_Connection_Mutation_Next) -> Error

Client_Interceptor :: struct {
	user_data:          rawptr,
	start_workflow:     Client_Start_Workflow_Interceptor_Proc,
	signal_with_start:  Client_Signal_With_Start_Interceptor_Proc,
	signal_workflow:    Client_Signal_Workflow_Interceptor_Proc,
	query_workflow:     Client_Query_Workflow_Interceptor_Proc,
	get_result:         Client_Get_Result_Interceptor_Proc,
	start_update:       Client_Start_Update_Interceptor_Proc,
	wait_update:        Client_Wait_Update_Interceptor_Proc,
	cancel_workflow:    Client_Cancel_Workflow_Interceptor_Proc,
	reset_workflow:     Client_Reset_Workflow_Interceptor_Proc,
	terminate_workflow: Client_Terminate_Workflow_Interceptor_Proc,
	list_workflows:     Client_List_Workflows_Interceptor_Proc,
	count_workflows:    Client_Count_Workflows_Interceptor_Proc,
	list_nexus_operations:  Client_List_Nexus_Operations_Interceptor_Proc,
	count_nexus_operations: Client_Count_Nexus_Operations_Interceptor_Proc,
	describe_schedule:  Client_Describe_Schedule_Interceptor_Proc,
	list_schedules:     Client_List_Schedules_Interceptor_Proc,
	count_schedules:    Client_Count_Schedules_Interceptor_Proc,
	list_schedule_matching_times: Client_List_Schedule_Matching_Times_Interceptor_Proc,
	create_schedule:     Client_Create_Schedule_Interceptor_Proc,
	schedule_mutation:   Client_Schedule_Mutation_Interceptor_Proc,
	connection_mutation: Client_Connection_Mutation_Interceptor_Proc,
}

client_start_workflow_next :: proc(
	next: Client_Start_Workflow_Next,
	input: ^Client_Start_Workflow_Input,
) -> (Workflow_Handle, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid start Workflow interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_signal_with_start_next :: proc(
	next: Client_Signal_With_Start_Next,
	input: ^Client_Signal_With_Start_Input,
) -> (Workflow_Handle, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Signal-With-Start interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_signal_workflow_next :: proc(
	next: Client_Signal_Workflow_Next,
	input: ^Client_Signal_Workflow_Input,
) -> Error {
	if next.invoke == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid Signal interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_query_workflow_next :: proc(
	next: Client_Query_Workflow_Next,
	input: ^Client_Query_Workflow_Input,
) -> (activity_codec.Payload, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Query interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_get_result_next :: proc(
	next: Client_Get_Result_Next,
	input: ^Client_Get_Result_Input,
) -> (activity_codec.Payload, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Workflow result interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_start_update_next :: proc(
	next: Client_Start_Update_Next,
	input: ^Client_Start_Update_Input,
) -> (Update_Handle, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Update interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_wait_update_next :: proc(
	next: Client_Wait_Update_Next,
	input: ^Client_Wait_Update_Input,
) -> (activity_codec.Payload, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Update wait interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_cancel_workflow_next :: proc(
	next: Client_Cancel_Workflow_Next,
	input: ^Client_Cancel_Workflow_Input,
) -> Error {
	if next.invoke == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid cancel Workflow interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_reset_workflow_next :: proc(
	next: Client_Reset_Workflow_Next,
	input: ^Client_Reset_Workflow_Input,
) -> (Workflow_Handle, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid reset Workflow interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_terminate_workflow_next :: proc(
	next: Client_Terminate_Workflow_Next,
	input: ^Client_Terminate_Workflow_Input,
) -> Error {
	if next.invoke == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid terminate Workflow interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_list_workflows_next :: proc(
	next: Client_List_Workflows_Next,
	input: ^Client_List_Workflows_Input,
) -> (Workflow_List_Page, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Workflow list interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_count_workflows_next :: proc(
	next: Client_Count_Workflows_Next,
	input: ^Client_Count_Workflows_Input,
) -> (Workflow_Count, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Workflow count interceptor continuation")
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

client_describe_schedule_next :: proc(next: Client_Describe_Schedule_Next, input: ^Client_Describe_Schedule_Input) -> (Schedule_Description, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule Describe interceptor continuation")
	return next.invoke(next.data, input)
}

client_list_schedules_next :: proc(next: Client_List_Schedules_Next, input: ^Client_List_Schedules_Input) -> (Schedule_List_Page, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule list interceptor continuation")
	return next.invoke(next.data, input)
}

client_count_schedules_next :: proc(next: Client_Count_Schedules_Next, input: ^Client_Count_Schedules_Input) -> (Schedule_Count, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule count interceptor continuation")
	return next.invoke(next.data, input)
}

client_list_schedule_matching_times_next :: proc(next: Client_List_Schedule_Matching_Times_Next, input: ^Client_List_Schedule_Matching_Times_Input) -> (Schedule_Matching_Times, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule matching-times interceptor continuation")
	return next.invoke(next.data, input)
}

client_create_schedule_next :: proc(next: Client_Create_Schedule_Next, input: ^Client_Create_Schedule_Input) -> (Schedule_Handle, Error) {
	if next.invoke == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule Create interceptor continuation")
	return next.invoke(next.data, input)
}

client_schedule_mutation_next :: proc(next: Client_Schedule_Mutation_Next, input: ^Client_Schedule_Mutation_Input) -> Error {
	if next.invoke == nil || input == nil do return error_from_string(.Invalid_Argument, "invalid Schedule mutation interceptor continuation")
	return next.invoke(next.data, input)
}

client_connection_mutation_next :: proc(next: Client_Connection_Mutation_Next, input: ^Client_Connection_Mutation_Input) -> Error {
	if next.invoke == nil || input == nil do return error_from_string(.Invalid_Argument, "invalid connection mutation interceptor continuation")
	return next.invoke(next.data, input)
}

client_interceptor_headers_init :: proc(result: ^Headers, source: ^Headers) -> Error {
	init_error := headers_init(result)
	if init_error.message != nil {
		return init_error
	}
	if source == nil {
		return {}
	}
	if source.allocator.procedure == nil {
		headers_free(result)
		return error_from_string(.Invalid_Argument, "Temporal headers are not initialized")
	}
	for &entry in source.entries {
		add_error := headers_add_payload(result, entry.key, activity_codec.payload_view(&entry.payload))
		if add_error.message != nil {
			headers_free(result)
			return add_error
		}
	}
	return {}
}

client_start_interceptor_headers_init :: proc(
	result: ^Headers,
	metadata: ^Start_Metadata,
) -> Error {
	init_error := headers_init(result)
	if init_error.message != nil {
		return init_error
	}
	if metadata == nil {
		return {}
	}
	if metadata.allocator.procedure == nil {
		headers_free(result)
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	for &entry in metadata.headers {
		add_error := headers_add_payload(result, entry.key, activity_codec.payload_view(&entry.payload))
		if add_error.message != nil {
			headers_free(result)
			return add_error
		}
	}
	return {}
}

Client_Interceptor_State :: struct {
	client: ^Client,
	index:  int,
}

client_start_workflow_chain :: proc(
	data: rawptr,
	input: ^Client_Start_Workflow_Input,
) -> (Workflow_Handle, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid start Workflow interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.start_workflow == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.start_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_start_workflow_chain},
		)
	}
	return start_workflow_payload_terminal(input)
}

client_signal_with_start_chain :: proc(
	data: rawptr,
	input: ^Client_Signal_With_Start_Input,
) -> (Workflow_Handle, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Signal-With-Start interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.signal_with_start == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.signal_with_start(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_signal_with_start_chain},
		)
	}
	return signal_with_start_workflow_payload_terminal(input)
}

client_signal_workflow_chain :: proc(data: rawptr, input: ^Client_Signal_Workflow_Input) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid Signal interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.signal_workflow == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.signal_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_signal_workflow_chain},
		)
	}
	return signal_workflow_payload_terminal(input)
}

client_query_workflow_chain :: proc(
	data: rawptr,
	input: ^Client_Query_Workflow_Input,
) -> (activity_codec.Payload, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Query interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.query_workflow == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.query_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_query_workflow_chain},
		)
	}
	return query_workflow_payload_terminal(input)
}

client_get_result_chain :: proc(
	data: rawptr,
	input: ^Client_Get_Result_Input,
) -> (activity_codec.Payload, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Workflow result interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.get_result == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.get_result(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_get_result_chain},
		)
	}
	return get_result_payload_terminal(input)
}

client_start_update_chain :: proc(
	data: rawptr,
	input: ^Client_Start_Update_Input,
) -> (Update_Handle, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Update interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.start_update == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.start_update(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_start_update_chain},
		)
	}
	return start_update_payload_terminal(input)
}

client_wait_update_chain :: proc(
	data: rawptr,
	input: ^Client_Wait_Update_Input,
) -> (activity_codec.Payload, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Update wait interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.wait_update == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.wait_update(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_wait_update_chain},
		)
	}
	return wait_update_payload_terminal(input)
}

client_cancel_workflow_chain :: proc(
	data: rawptr,
	input: ^Client_Cancel_Workflow_Input,
) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid cancel Workflow interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.cancel_workflow == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.cancel_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_cancel_workflow_chain},
		)
	}
	return cancel_workflow_terminal(input)
}

client_reset_workflow_chain :: proc(
	data: rawptr,
	input: ^Client_Reset_Workflow_Input,
) -> (Workflow_Handle, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid reset Workflow interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.reset_workflow == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.reset_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_reset_workflow_chain},
		)
	}
	return reset_workflow_terminal(input)
}

client_terminate_workflow_chain :: proc(
	data: rawptr,
	input: ^Client_Terminate_Workflow_Input,
) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid terminate Workflow interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.terminate_workflow == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.terminate_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_terminate_workflow_chain},
		)
	}
	return terminate_workflow_terminal(input)
}

client_list_workflows_chain :: proc(
	data: rawptr,
	input: ^Client_List_Workflows_Input,
) -> (Workflow_List_Page, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Workflow list interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.list_workflows == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.list_workflows(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_list_workflows_chain},
		)
	}
	return list_workflows_terminal(input)
}

client_count_workflows_chain :: proc(
	data: rawptr,
	input: ^Client_Count_Workflows_Input,
) -> (Workflow_Count, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Workflow count interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.count_workflows == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.count_workflows(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_count_workflows_chain},
		)
	}
	return count_workflows_terminal(input)
}

client_list_nexus_operations_chain :: proc(
	data: rawptr,
	input: ^Client_List_Nexus_Operations_Input,
) -> (Nexus_Operation_List_Page, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation list interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.list_nexus_operations == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.list_nexus_operations(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_list_nexus_operations_chain},
		)
	}
	return list_nexus_operations_terminal(input)
}

client_count_nexus_operations_chain :: proc(
	data: rawptr,
	input: ^Client_Count_Nexus_Operations_Input,
) -> (Nexus_Operation_Count, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Nexus operation count interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.count_nexus_operations == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.count_nexus_operations(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_count_nexus_operations_chain},
		)
	}
	return count_nexus_operations_terminal(input)
}

client_describe_schedule_chain :: proc(data: rawptr, input: ^Client_Describe_Schedule_Input) -> (Schedule_Description, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule Describe interceptor state")
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.describe_schedule == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.describe_schedule(entry.user_data, input, {data = &next_state, invoke = client_describe_schedule_chain})
	}
	return describe_schedule_terminal(input)
}

client_list_schedules_chain :: proc(data: rawptr, input: ^Client_List_Schedules_Input) -> (Schedule_List_Page, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule list interceptor state")
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.list_schedules == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.list_schedules(entry.user_data, input, {data = &next_state, invoke = client_list_schedules_chain})
	}
	return list_schedules_terminal(input)
}

client_count_schedules_chain :: proc(data: rawptr, input: ^Client_Count_Schedules_Input) -> (Schedule_Count, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule count interceptor state")
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.count_schedules == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.count_schedules(entry.user_data, input, {data = &next_state, invoke = client_count_schedules_chain})
	}
	return count_schedules_terminal(input)
}

client_list_schedule_matching_times_chain :: proc(data: rawptr, input: ^Client_List_Schedule_Matching_Times_Input) -> (Schedule_Matching_Times, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule matching-times interceptor state")
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.list_schedule_matching_times == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.list_schedule_matching_times(entry.user_data, input, {data = &next_state, invoke = client_list_schedule_matching_times_chain})
	}
	return list_schedule_matching_times_terminal(input)
}

client_create_schedule_chain :: proc(data: rawptr, input: ^Client_Create_Schedule_Input) -> (Schedule_Handle, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil do return {}, error_from_string(.Invalid_Argument, "invalid Schedule Create interceptor state")
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.create_schedule == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.create_schedule(entry.user_data, input, {data = &next_state, invoke = client_create_schedule_chain})
	}
	return create_schedule_payload_terminal(input)
}

client_schedule_mutation_chain :: proc(data: rawptr, input: ^Client_Schedule_Mutation_Input) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil do return error_from_string(.Invalid_Argument, "invalid Schedule mutation interceptor state")
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.schedule_mutation == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.schedule_mutation(entry.user_data, input, {data = &next_state, invoke = client_schedule_mutation_chain})
	}
	return schedule_mutation_terminal(input)
}

client_connection_mutation_chain :: proc(data: rawptr, input: ^Client_Connection_Mutation_Input) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil do return error_from_string(.Invalid_Argument, "invalid connection mutation interceptor state")
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.connection_mutation == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.connection_mutation(entry.user_data, input, {data = &next_state, invoke = client_connection_mutation_chain})
	}
	return connection_mutation_terminal(input)
}

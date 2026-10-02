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

Client_Update_With_Start_Input :: struct {
	client:           ^Client,
	workflow_type:    string,
	workflow_inputs:  []activity_codec.Payload_View,
	update_name:      string,
	update_inputs:    []activity_codec.Payload_View,
	options:          Update_With_Start_Options,
	start_headers:    ^Headers,
	update_headers:   ^Headers,
}

Client_Update_With_Start_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Update_With_Start_Input,
) -> (Update_With_Start_Result, Error)

Client_Update_With_Start_Next :: struct {
	data:   rawptr,
	invoke: Client_Update_With_Start_Next_Proc,
}

Client_Update_With_Start_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Update_With_Start_Input,
	next: Client_Update_With_Start_Next,
) -> (Update_With_Start_Result, Error)

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

Client_Update_Workflow_Execution_Options_Input :: struct {
	handle: ^Workflow_Handle,
	update: Workflow_Execution_Options_Update,
}

Client_Update_Workflow_Execution_Options_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Update_Workflow_Execution_Options_Input,
) -> (Workflow_Execution_Options, Error)

Client_Update_Workflow_Execution_Options_Next :: struct {
	data:   rawptr,
	invoke: Client_Update_Workflow_Execution_Options_Next_Proc,
}

Client_Update_Workflow_Execution_Options_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Update_Workflow_Execution_Options_Input,
	next: Client_Update_Workflow_Execution_Options_Next,
) -> (Workflow_Execution_Options, Error)

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

Client_Delete_Workflow_Input :: struct {
	handle:  ^Workflow_Handle,
	options: Delete_Workflow_Options,
}

Client_Delete_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Delete_Workflow_Input,
) -> Error

Client_Delete_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Client_Delete_Workflow_Next_Proc,
}

Client_Delete_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Delete_Workflow_Input,
	next: Client_Delete_Workflow_Next,
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

Client_Start_Standalone_Activity_Input :: struct {
	client:  ^Client,
	input:   activity_codec.Payload_View,
	options: Start_Standalone_Activity_Options,
}

Client_Start_Standalone_Activity_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Start_Standalone_Activity_Input,
) -> (Standalone_Activity_Handle, Error)

Client_Start_Standalone_Activity_Next :: struct {
	data:   rawptr,
	invoke: Client_Start_Standalone_Activity_Next_Proc,
}

Client_Start_Standalone_Activity_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Start_Standalone_Activity_Input,
	next: Client_Start_Standalone_Activity_Next,
) -> (Standalone_Activity_Handle, Error)

Client_Poll_Standalone_Activity_Input :: struct {
	handle:  ^Standalone_Activity_Handle,
	options: Standalone_Activity_Wait_Options,
}

Client_Poll_Standalone_Activity_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Poll_Standalone_Activity_Input,
) -> (Standalone_Activity_Poll_Result, Error)

Client_Poll_Standalone_Activity_Next :: struct {
	data:   rawptr,
	invoke: Client_Poll_Standalone_Activity_Next_Proc,
}

Client_Poll_Standalone_Activity_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Poll_Standalone_Activity_Input,
	next: Client_Poll_Standalone_Activity_Next,
) -> (Standalone_Activity_Poll_Result, Error)

Client_Describe_Standalone_Activity_Input :: struct {
	handle:    ^Standalone_Activity_Handle,
	options:   Describe_Standalone_Activity_Options,
	allocator: mem.Allocator,
}

Client_Describe_Standalone_Activity_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Describe_Standalone_Activity_Input,
) -> (Standalone_Activity_Description, Error)

Client_Describe_Standalone_Activity_Next :: struct {
	data:   rawptr,
	invoke: Client_Describe_Standalone_Activity_Next_Proc,
}

Client_Describe_Standalone_Activity_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Describe_Standalone_Activity_Input,
	next: Client_Describe_Standalone_Activity_Next,
) -> (Standalone_Activity_Description, Error)

Standalone_Activity_Mutation_Kind :: enum {
	Cancel,
	Terminate,
	Delete,
}

Client_Standalone_Activity_Mutation_Input :: struct {
	handle:     ^Standalone_Activity_Handle,
	kind:       Standalone_Activity_Mutation_Kind,
	reason:     string,
	request_id: string,
}

Client_Standalone_Activity_Mutation_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Standalone_Activity_Mutation_Input,
) -> Error

Client_Standalone_Activity_Mutation_Next :: struct {
	data:   rawptr,
	invoke: Client_Standalone_Activity_Mutation_Next_Proc,
}

Client_Standalone_Activity_Mutation_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Standalone_Activity_Mutation_Input,
	next: Client_Standalone_Activity_Mutation_Next,
) -> Error

Standalone_Activity_Control_Kind :: enum {
	Invalid,
	Pause,
	Unpause,
	Reset,
}

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

Client_Standalone_Activity_Control_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Standalone_Activity_Control_Input,
) -> Error

Client_Standalone_Activity_Control_Next :: struct {
	data:   rawptr,
	invoke: Client_Standalone_Activity_Control_Next_Proc,
}

Client_Standalone_Activity_Control_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Standalone_Activity_Control_Input,
	next: Client_Standalone_Activity_Control_Next,
) -> Error

Client_Update_Standalone_Activity_Options_Input :: struct {
	handle:    ^Standalone_Activity_Handle,
	update:    Standalone_Activity_Options_Update,
	allocator: mem.Allocator,
}

Client_Update_Standalone_Activity_Options_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Update_Standalone_Activity_Options_Input,
) -> (Standalone_Activity_Options, Error)

Client_Update_Standalone_Activity_Options_Next :: struct {
	data:   rawptr,
	invoke: Client_Update_Standalone_Activity_Options_Next_Proc,
}

Client_Update_Standalone_Activity_Options_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Update_Standalone_Activity_Options_Input,
	next: Client_Update_Standalone_Activity_Options_Next,
) -> (Standalone_Activity_Options, Error)

Client_List_Standalone_Activities_Input :: struct {
	client:    ^Client,
	options:   List_Standalone_Activities_Options,
	allocator: mem.Allocator,
}

Client_List_Standalone_Activities_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_List_Standalone_Activities_Input,
) -> (Standalone_Activity_List_Page, Error)

Client_List_Standalone_Activities_Next :: struct {
	data:   rawptr,
	invoke: Client_List_Standalone_Activities_Next_Proc,
}

Client_List_Standalone_Activities_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_List_Standalone_Activities_Input,
	next: Client_List_Standalone_Activities_Next,
) -> (Standalone_Activity_List_Page, Error)

Client_Count_Standalone_Activities_Input :: struct {
	client:    ^Client,
	query:     string,
	allocator: mem.Allocator,
}

Client_Count_Standalone_Activities_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Count_Standalone_Activities_Input,
) -> (Standalone_Activity_Count, Error)

Client_Count_Standalone_Activities_Next :: struct {
	data:   rawptr,
	invoke: Client_Count_Standalone_Activities_Next_Proc,
}

Client_Count_Standalone_Activities_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Count_Standalone_Activities_Input,
	next: Client_Count_Standalone_Activities_Next,
) -> (Standalone_Activity_Count, Error)

Client_Get_System_Info_Input :: struct {
	client:    ^Client,
	allocator: mem.Allocator,
}

Client_Get_System_Info_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Client_Get_System_Info_Input,
) -> (System_Info, Error)

Client_Get_System_Info_Next :: struct {
	data:   rawptr,
	invoke: Client_Get_System_Info_Next_Proc,
}

Client_Get_System_Info_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Client_Get_System_Info_Input,
	next: Client_Get_System_Info_Next,
) -> (System_Info, Error)

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
	update_with_start:  Client_Update_With_Start_Interceptor_Proc,
	signal_with_start:  Client_Signal_With_Start_Interceptor_Proc,
	signal_workflow:    Client_Signal_Workflow_Interceptor_Proc,
	query_workflow:     Client_Query_Workflow_Interceptor_Proc,
	get_result:         Client_Get_Result_Interceptor_Proc,
	start_update:       Client_Start_Update_Interceptor_Proc,
	wait_update:        Client_Wait_Update_Interceptor_Proc,
	cancel_workflow:    Client_Cancel_Workflow_Interceptor_Proc,
	reset_workflow:     Client_Reset_Workflow_Interceptor_Proc,
	update_workflow_execution_options: Client_Update_Workflow_Execution_Options_Interceptor_Proc,
	terminate_workflow: Client_Terminate_Workflow_Interceptor_Proc,
	delete_workflow:    Client_Delete_Workflow_Interceptor_Proc,
	list_workflows:     Client_List_Workflows_Interceptor_Proc,
	count_workflows:    Client_Count_Workflows_Interceptor_Proc,
	start_standalone_activity:    Client_Start_Standalone_Activity_Interceptor_Proc,
	poll_standalone_activity:     Client_Poll_Standalone_Activity_Interceptor_Proc,
	describe_standalone_activity: Client_Describe_Standalone_Activity_Interceptor_Proc,
	standalone_activity_mutation: Client_Standalone_Activity_Mutation_Interceptor_Proc,
	standalone_activity_control:  Client_Standalone_Activity_Control_Interceptor_Proc,
	update_standalone_activity_options: Client_Update_Standalone_Activity_Options_Interceptor_Proc,
	list_standalone_activities:   Client_List_Standalone_Activities_Interceptor_Proc,
	count_standalone_activities:  Client_Count_Standalone_Activities_Interceptor_Proc,
	get_system_info:     Client_Get_System_Info_Interceptor_Proc,
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

client_update_with_start_next :: proc(
	next: Client_Update_With_Start_Next,
	input: ^Client_Update_With_Start_Input,
) -> (Update_With_Start_Result, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Update-With-Start interceptor continuation")
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

client_update_workflow_execution_options_next :: proc(
	next: Client_Update_Workflow_Execution_Options_Next,
	input: ^Client_Update_Workflow_Execution_Options_Input,
) -> (Workflow_Execution_Options, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Workflow options interceptor continuation")
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

client_delete_workflow_next :: proc(
	next: Client_Delete_Workflow_Next,
	input: ^Client_Delete_Workflow_Input,
) -> Error {
	if next.invoke == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid delete Workflow interceptor continuation")
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

client_start_standalone_activity_next :: proc(
	next: Client_Start_Standalone_Activity_Next,
	input: ^Client_Start_Standalone_Activity_Input,
) -> (Standalone_Activity_Handle, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity start interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_poll_standalone_activity_next :: proc(
	next: Client_Poll_Standalone_Activity_Next,
	input: ^Client_Poll_Standalone_Activity_Input,
) -> (Standalone_Activity_Poll_Result, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity poll interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_describe_standalone_activity_next :: proc(
	next: Client_Describe_Standalone_Activity_Next,
	input: ^Client_Describe_Standalone_Activity_Input,
) -> (Standalone_Activity_Description, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity Describe interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_standalone_activity_mutation_next :: proc(
	next: Client_Standalone_Activity_Mutation_Next,
	input: ^Client_Standalone_Activity_Mutation_Input,
) -> Error {
	if next.invoke == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid standalone Activity mutation interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_standalone_activity_control_next :: proc(
	next: Client_Standalone_Activity_Control_Next,
	input: ^Client_Standalone_Activity_Control_Input,
) -> Error {
	if next.invoke == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid standalone Activity control interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_update_standalone_activity_options_next :: proc(
	next: Client_Update_Standalone_Activity_Options_Next,
	input: ^Client_Update_Standalone_Activity_Options_Input,
) -> (Standalone_Activity_Options, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity options interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_list_standalone_activities_next :: proc(
	next: Client_List_Standalone_Activities_Next,
	input: ^Client_List_Standalone_Activities_Input,
) -> (Standalone_Activity_List_Page, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity list interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_count_standalone_activities_next :: proc(
	next: Client_Count_Standalone_Activities_Next,
	input: ^Client_Count_Standalone_Activities_Input,
) -> (Standalone_Activity_Count, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity count interceptor continuation")
	}
	return next.invoke(next.data, input)
}

client_get_system_info_next :: proc(
	next: Client_Get_System_Info_Next,
	input: ^Client_Get_System_Info_Input,
) -> (System_Info, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid System Info interceptor continuation")
	}
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

client_update_with_start_chain :: proc(
	data: rawptr,
	input: ^Client_Update_With_Start_Input,
) -> (Update_With_Start_Result, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Update-With-Start interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.update_with_start == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.update_with_start(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_update_with_start_chain},
		)
	}
	return update_with_start_payloads_terminal(input)
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

client_update_workflow_execution_options_chain :: proc(
	data: rawptr,
	input: ^Client_Update_Workflow_Execution_Options_Input,
) -> (Workflow_Execution_Options, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Workflow options interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.update_workflow_execution_options == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.update_workflow_execution_options(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_update_workflow_execution_options_chain},
		)
	}
	return update_workflow_execution_options_terminal(input)
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

client_delete_workflow_chain :: proc(
	data: rawptr,
	input: ^Client_Delete_Workflow_Input,
) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid delete Workflow interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.delete_workflow == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.delete_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_delete_workflow_chain},
		)
	}
	return delete_workflow_terminal(input)
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

client_start_standalone_activity_chain :: proc(
	data: rawptr,
	input: ^Client_Start_Standalone_Activity_Input,
) -> (Standalone_Activity_Handle, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity start interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.start_standalone_activity == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.start_standalone_activity(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_start_standalone_activity_chain},
		)
	}
	return start_standalone_activity_payload_terminal(input)
}

client_poll_standalone_activity_chain :: proc(
	data: rawptr,
	input: ^Client_Poll_Standalone_Activity_Input,
) -> (Standalone_Activity_Poll_Result, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity poll interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.poll_standalone_activity == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.poll_standalone_activity(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_poll_standalone_activity_chain},
		)
	}
	return poll_standalone_activity_terminal(input)
}

client_describe_standalone_activity_chain :: proc(
	data: rawptr,
	input: ^Client_Describe_Standalone_Activity_Input,
) -> (Standalone_Activity_Description, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity Describe interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.describe_standalone_activity == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.describe_standalone_activity(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_describe_standalone_activity_chain},
		)
	}
	return describe_standalone_activity_terminal(input)
}

client_standalone_activity_mutation_chain :: proc(
	data: rawptr,
	input: ^Client_Standalone_Activity_Mutation_Input,
) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid standalone Activity mutation interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.standalone_activity_mutation == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.standalone_activity_mutation(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_standalone_activity_mutation_chain},
		)
	}
	return standalone_activity_mutation_terminal(input)
}

client_standalone_activity_control_chain :: proc(
	data: rawptr,
	input: ^Client_Standalone_Activity_Control_Input,
) -> Error {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return error_from_string(.Invalid_Argument, "invalid standalone Activity control interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.standalone_activity_control == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.standalone_activity_control(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_standalone_activity_control_chain},
		)
	}
	return standalone_activity_control_terminal(input)
}

client_update_standalone_activity_options_chain :: proc(
	data: rawptr,
	input: ^Client_Update_Standalone_Activity_Options_Input,
) -> (Standalone_Activity_Options, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity options interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.update_standalone_activity_options == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.update_standalone_activity_options(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_update_standalone_activity_options_chain},
		)
	}
	return update_standalone_activity_options_terminal(input)
}

client_list_standalone_activities_chain :: proc(
	data: rawptr,
	input: ^Client_List_Standalone_Activities_Input,
) -> (Standalone_Activity_List_Page, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity list interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.list_standalone_activities == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.list_standalone_activities(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_list_standalone_activities_chain},
		)
	}
	return list_standalone_activities_terminal(input)
}

client_count_standalone_activities_chain :: proc(
	data: rawptr,
	input: ^Client_Count_Standalone_Activities_Input,
) -> (Standalone_Activity_Count, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid standalone Activity count interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.count_standalone_activities == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.count_standalone_activities(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_count_standalone_activities_chain},
		)
	}
	return count_standalone_activities_terminal(input)
}

client_get_system_info_chain :: proc(
	data: rawptr,
	input: ^Client_Get_System_Info_Input,
) -> (System_Info, Error) {
	state := cast(^Client_Interceptor_State)data
	if state == nil || state.client == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid System Info interceptor state")
	}
	for index := state.index; index < len(state.client.interceptors); index += 1 {
		entry := &state.client.interceptors[index]
		if entry.get_system_info == nil do continue
		next_state := Client_Interceptor_State{client = state.client, index = index + 1}
		return entry.get_system_info(
			entry.user_data,
			input,
			{data = &next_state, invoke = client_get_system_info_chain},
		)
	}
	return get_system_info_terminal(input)
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

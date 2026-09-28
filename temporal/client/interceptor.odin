package client

import activity_codec "../codec"

// Client outbound interceptors are copied into Client storage at connect time.
// user_data remains application-owned and must outlive the Client. Input,
// headers, and next continuations are scoped to one synchronous SDK call.
Client_Start_Workflow_Input :: struct {
	client:        ^Client,
	workflow_type: string,
	input:         activity_codec.Payload_View,
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

Client_Signal_Workflow_Input :: struct {
	handle:      ^Workflow_Handle,
	signal_name: string,
	input:       activity_codec.Payload_View,
	has_input:   bool,
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
	input:      activity_codec.Payload_View,
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

Client_Start_Update_Input :: struct {
	handle:      ^Workflow_Handle,
	update_name: string,
	input:       activity_codec.Payload_View,
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

Client_Interceptor :: struct {
	user_data:      rawptr,
	start_workflow: Client_Start_Workflow_Interceptor_Proc,
	signal_workflow: Client_Signal_Workflow_Interceptor_Proc,
	query_workflow: Client_Query_Workflow_Interceptor_Proc,
	start_update:   Client_Start_Update_Interceptor_Proc,
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

client_start_update_next :: proc(
	next: Client_Start_Update_Next,
	input: ^Client_Start_Update_Input,
) -> (Update_Handle, Error) {
	if next.invoke == nil || input == nil {
		return {}, error_from_string(.Invalid_Argument, "invalid Update interceptor continuation")
	}
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

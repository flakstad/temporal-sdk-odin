package workflow

import codec "../codec"

// Workflow inbound interceptors execute inside the deterministic Workflow
// boundary, including during replay. Registration order is outermost-first.
// user_data is borrowed for the Worker lifetime. Inputs and next continuations
// are scoped to the intercepted execution and must not be retained.
Inbound_Execute_Workflow_Input :: struct {
	ctx:           ^Fiber_Context,
	workflow_type: string,
	input:         codec.Payload_View,
}

Inbound_Execute_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Inbound_Execute_Workflow_Input,
) -> Payload_Result

Inbound_Execute_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Inbound_Execute_Workflow_Next_Proc,
}

Inbound_Execute_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Inbound_Execute_Workflow_Input,
	next: Inbound_Execute_Workflow_Next,
) -> Payload_Result

Inbound_Handle_Query_Input :: struct {
	workflow_type: string,
	query:         ^Query_Context,
	input:         codec.Payload_View,
}

Inbound_Handle_Query_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Inbound_Handle_Query_Input,
) -> Payload_Result

Inbound_Handle_Query_Next :: struct {
	data:   rawptr,
	invoke: Inbound_Handle_Query_Next_Proc,
}

Inbound_Handle_Query_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Inbound_Handle_Query_Input,
	next: Inbound_Handle_Query_Next,
) -> Payload_Result

Inbound_Interceptor :: struct {
	user_data:        rawptr,
	execute_workflow: Inbound_Execute_Workflow_Interceptor_Proc,
	handle_query:     Inbound_Handle_Query_Interceptor_Proc,
}

inbound_interceptor_has_operation :: proc(interceptor: Inbound_Interceptor) -> bool {
	return interceptor.execute_workflow != nil || interceptor.handle_query != nil
}

inbound_execute_workflow_next :: proc(
	next: Inbound_Execute_Workflow_Next,
	input: ^Inbound_Execute_Workflow_Input,
) -> Payload_Result {
	if next.invoke == nil || input == nil || input.ctx == nil {
		return payload_fail(
			"invalid Workflow execution interceptor continuation",
			"OdinWorkflowInterceptorError",
		)
	}
	return next.invoke(next.data, input)
}

inbound_handle_query_next :: proc(
	next: Inbound_Handle_Query_Next,
	input: ^Inbound_Handle_Query_Input,
) -> Payload_Result {
	if next.invoke == nil || input == nil || input.query == nil {
		return payload_fail(
			"invalid Workflow Query interceptor continuation",
			"OdinWorkflowInterceptorError",
		)
	}
	return next.invoke(next.data, input)
}

Inbound_Execute_Workflow_State :: struct {
	instance: ^Fiber_Instance,
	index:    int,
}

inbound_execute_workflow_chain :: proc(
	data: rawptr,
	input: ^Inbound_Execute_Workflow_Input,
) -> Payload_Result {
	state := cast(^Inbound_Execute_Workflow_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil ||
	   input == nil || input.ctx == nil {
		return payload_fail(
			"invalid Workflow execution interceptor state",
			"OdinWorkflowInterceptorError",
		)
	}
	for index := state.index; index < len(state.instance.runtime.inbound_interceptors); index += 1 {
		entry := &state.instance.runtime.inbound_interceptors[index]
		if entry.execute_workflow == nil do continue
		next_state := Inbound_Execute_Workflow_State{
			instance = state.instance,
			index = index + 1,
		}
		return entry.execute_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = inbound_execute_workflow_chain},
		)
	}
	return state.instance.invoke(
		state.instance.callback_data,
		input.ctx,
		input.input,
	)
}

Inbound_Handle_Query_State :: struct {
	instance: ^Fiber_Instance,
	handler:  ^Fiber_Query_Handler,
	index:    int,
}

inbound_handle_query_chain :: proc(
	data: rawptr,
	input: ^Inbound_Handle_Query_Input,
) -> Payload_Result {
	state := cast(^Inbound_Handle_Query_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil ||
	   state.handler == nil || input == nil || input.query == nil {
		return payload_fail(
			"invalid Workflow Query interceptor state",
			"OdinWorkflowInterceptorError",
		)
	}
	for index := state.index; index < len(state.instance.runtime.inbound_interceptors); index += 1 {
		entry := &state.instance.runtime.inbound_interceptors[index]
		if entry.handle_query == nil do continue
		next_state := Inbound_Handle_Query_State{
			instance = state.instance,
			handler = state.handler,
			index = index + 1,
		}
		return entry.handle_query(
			entry.user_data,
			input,
			{data = &next_state, invoke = inbound_handle_query_chain},
		)
	}
	if state.handler.invoke_with_context != nil {
		return state.handler.invoke_with_context(
			state.handler.callback_data,
			input.query,
			input.input,
		)
	}
	return state.handler.invoke(state.handler.callback_data, input.input)
}

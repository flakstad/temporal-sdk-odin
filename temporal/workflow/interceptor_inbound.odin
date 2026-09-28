package workflow

import codec "../codec"

// Workflow inbound interceptors execute inside the deterministic Workflow
// boundary, including during replay. Registration order is outermost-first.
// user_data is borrowed for the Worker lifetime. Inputs and next continuations
// are scoped to the intercepted execution and must not be retained.
Inbound_Execute_Workflow_Input :: struct {
	ctx:           ^Fiber_Context,
	workflow_type: string,
	inputs:        []codec.Payload_View,
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
	inputs:        []codec.Payload_View,
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

Inbound_Handle_Signal_Input :: struct {
	ctx:           ^Fiber_Context,
	workflow_type: string,
	signal:        ^Signal_Context,
	inputs:        []codec.Payload_View,
}

Inbound_Handle_Signal_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Inbound_Handle_Signal_Input,
) -> Payload_Result

Inbound_Handle_Signal_Next :: struct {
	data:   rawptr,
	invoke: Inbound_Handle_Signal_Next_Proc,
}

Inbound_Handle_Signal_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Inbound_Handle_Signal_Input,
	next: Inbound_Handle_Signal_Next,
) -> Payload_Result

Inbound_Handle_Update_Input :: struct {
	ctx:           ^Fiber_Context,
	workflow_type: string,
	update:        ^Update_Context,
	input:         codec.Payload_View,
}

Inbound_Handle_Update_Validator_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Inbound_Handle_Update_Input,
) -> Update_Validation

Inbound_Handle_Update_Validator_Next :: struct {
	data:   rawptr,
	invoke: Inbound_Handle_Update_Validator_Next_Proc,
}

Inbound_Handle_Update_Validator_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Inbound_Handle_Update_Input,
	next: Inbound_Handle_Update_Validator_Next,
) -> Update_Validation

Inbound_Handle_Update_Handler_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Inbound_Handle_Update_Input,
) -> Payload_Result

Inbound_Handle_Update_Handler_Next :: struct {
	data:   rawptr,
	invoke: Inbound_Handle_Update_Handler_Next_Proc,
}

Inbound_Handle_Update_Handler_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Inbound_Handle_Update_Input,
	next: Inbound_Handle_Update_Handler_Next,
) -> Payload_Result

Inbound_Interceptor :: struct {
	user_data:                rawptr,
	execute_workflow:         Inbound_Execute_Workflow_Interceptor_Proc,
	handle_query:             Inbound_Handle_Query_Interceptor_Proc,
	handle_signal:            Inbound_Handle_Signal_Interceptor_Proc,
	handle_update_validator:  Inbound_Handle_Update_Validator_Interceptor_Proc,
	handle_update_handler:    Inbound_Handle_Update_Handler_Interceptor_Proc,
}

inbound_interceptor_has_operation :: proc(interceptor: Inbound_Interceptor) -> bool {
	return interceptor.execute_workflow != nil || interceptor.handle_query != nil ||
	       interceptor.handle_signal != nil ||
	       interceptor.handle_update_validator != nil ||
	       interceptor.handle_update_handler != nil
}

inbound_handle_signal_next :: proc(
	next: Inbound_Handle_Signal_Next,
	input: ^Inbound_Handle_Signal_Input,
) -> Payload_Result {
	if next.invoke == nil || input == nil || input.signal == nil {
		return payload_fail(
			"invalid Workflow Signal interceptor continuation",
			"OdinWorkflowInterceptorError",
		)
	}
	return next.invoke(next.data, input)
}

inbound_handle_update_validator_next :: proc(
	next: Inbound_Handle_Update_Validator_Next,
	input: ^Inbound_Handle_Update_Input,
) -> Update_Validation {
	if next.invoke == nil || input == nil || input.update == nil {
		return update_reject(
			"invalid Workflow Update validator interceptor continuation",
			"OdinWorkflowInterceptorError",
		)
	}
	return next.invoke(next.data, input)
}

inbound_handle_update_handler_next :: proc(
	next: Inbound_Handle_Update_Handler_Next,
	input: ^Inbound_Handle_Update_Input,
) -> Payload_Result {
	if next.invoke == nil || input == nil || input.update == nil {
		return payload_fail(
			"invalid Workflow Update handler interceptor continuation",
			"OdinWorkflowInterceptorError",
		)
	}
	return next.invoke(next.data, input)
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
		input.inputs,
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
	if state.handler.invoke_payloads_with_context != nil {
		return state.handler.invoke_payloads_with_context(
			state.handler.callback_data,
			input.query,
			input.inputs,
		)
	}
	if state.handler.invoke_payloads != nil {
		return state.handler.invoke_payloads(state.handler.callback_data, input.inputs)
	}
	if len(input.inputs) != 1 {
		return payload_fail(
			"single-Payload Workflow Query handler requires exactly one argument",
			"OdinWorkflowQueryArgumentCountError",
		)
	}
	if state.handler.invoke_with_context != nil {
		return state.handler.invoke_with_context(
			state.handler.callback_data,
			input.query,
			input.inputs[0],
		)
	}
	return state.handler.invoke(state.handler.callback_data, input.inputs[0])
}

Inbound_Handle_Signal_State :: struct {
	instance: ^Fiber_Instance,
	handler:  ^Fiber_Signal_Handler,
	routine:  ^Fiber_Signal_Routine,
	index:    int,
}

inbound_handle_signal_chain :: proc(
	data: rawptr,
	input: ^Inbound_Handle_Signal_Input,
) -> Payload_Result {
	state := cast(^Inbound_Handle_Signal_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil ||
	   state.handler == nil && state.routine == nil || input == nil || input.signal == nil {
		return payload_fail(
			"invalid Workflow Signal interceptor state",
			"OdinWorkflowInterceptorError",
		)
	}
	for index := state.index; index < len(state.instance.runtime.inbound_interceptors); index += 1 {
		entry := &state.instance.runtime.inbound_interceptors[index]
		if entry.handle_signal == nil do continue
		next_state := Inbound_Handle_Signal_State{
			instance = state.instance,
			handler = state.handler,
			routine = state.routine,
			index = index + 1,
		}
		return entry.handle_signal(
			entry.user_data,
			input,
			{data = &next_state, invoke = inbound_handle_signal_chain},
		)
	}
	if state.routine != nil {
		if state.routine.invoke_payloads_with_context != nil {
			return state.routine.invoke_payloads_with_context(
				state.routine.callback_data,
				input.ctx,
				input.signal,
				input.inputs,
			)
		}
		if state.routine.invoke_payloads != nil {
			return state.routine.invoke_payloads(
				state.routine.callback_data,
				input.ctx,
				input.inputs,
			)
		}
		if len(input.inputs) != 1 {
			return payload_fail(
				"single-Payload Workflow Signal routine requires exactly one argument",
				"OdinWorkflowSignalArgumentCountError",
			)
		}
		if state.routine.invoke_with_context != nil {
			return state.routine.invoke_with_context(
				state.routine.callback_data,
				input.ctx,
				input.signal,
				input.inputs[0],
			)
		}
		return state.routine.invoke(
			state.routine.callback_data,
			input.ctx,
			input.inputs[0],
		)
	}
	if state.handler.invoke_payloads_with_context != nil {
		return state.handler.invoke_payloads_with_context(
			state.handler.callback_data,
			input.signal,
			input.inputs,
		)
	}
	if state.handler.invoke_payloads != nil {
		return state.handler.invoke_payloads(state.handler.callback_data, input.inputs)
	}
	if len(input.inputs) != 1 {
		return payload_fail(
			"single-Payload Workflow Signal handler requires exactly one argument",
			"OdinWorkflowSignalArgumentCountError",
		)
	}
	if state.handler.invoke_with_context != nil {
		return state.handler.invoke_with_context(
			state.handler.callback_data,
			input.signal,
			input.inputs[0],
		)
	}
	return state.handler.invoke(state.handler.callback_data, input.inputs[0])
}

Inbound_Handle_Update_Validator_State :: struct {
	instance: ^Fiber_Instance,
	handler:  ^Fiber_Update_Handler,
	index:    int,
}

inbound_handle_update_validator_chain :: proc(
	data: rawptr,
	input: ^Inbound_Handle_Update_Input,
) -> Update_Validation {
	state := cast(^Inbound_Handle_Update_Validator_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil ||
	   state.handler == nil || input == nil || input.update == nil {
		return update_reject(
			"invalid Workflow Update validator interceptor state",
			"OdinWorkflowInterceptorError",
		)
	}
	for index := state.index; index < len(state.instance.runtime.inbound_interceptors); index += 1 {
		entry := &state.instance.runtime.inbound_interceptors[index]
		if entry.handle_update_validator == nil do continue
		next_state := Inbound_Handle_Update_Validator_State{
			instance = state.instance,
			handler = state.handler,
			index = index + 1,
		}
		return entry.handle_update_validator(
			entry.user_data,
			input,
			{data = &next_state, invoke = inbound_handle_update_validator_chain},
		)
	}
	if state.handler.validate_with_context != nil {
		return state.handler.validate_with_context(
			state.handler.callback_data,
			input.update,
			input.input,
		)
	}
	return state.handler.validate(state.handler.callback_data, input.input)
}

Inbound_Handle_Update_Handler_State :: struct {
	instance: ^Fiber_Instance,
	handler:  ^Fiber_Update_Handler,
	routine:  ^Fiber_Update_Routine,
	index:    int,
}

inbound_handle_update_handler_chain :: proc(
	data: rawptr,
	input: ^Inbound_Handle_Update_Input,
) -> Payload_Result {
	state := cast(^Inbound_Handle_Update_Handler_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil ||
	   state.handler == nil && state.routine == nil || input == nil || input.update == nil {
		return payload_fail(
			"invalid Workflow Update handler interceptor state",
			"OdinWorkflowInterceptorError",
		)
	}
	for index := state.index; index < len(state.instance.runtime.inbound_interceptors); index += 1 {
		entry := &state.instance.runtime.inbound_interceptors[index]
		if entry.handle_update_handler == nil do continue
		next_state := Inbound_Handle_Update_Handler_State{
			instance = state.instance,
			handler = state.handler,
			routine = state.routine,
			index = index + 1,
		}
		return entry.handle_update_handler(
			entry.user_data,
			input,
			{data = &next_state, invoke = inbound_handle_update_handler_chain},
		)
	}
	if state.routine != nil {
		if state.routine.invoke_with_context != nil {
			return state.routine.invoke_with_context(
				state.routine.callback_data,
				input.ctx,
				input.update,
				input.input,
			)
		}
		return state.routine.invoke(
			state.routine.callback_data,
			input.ctx,
			input.input,
		)
	}
	if state.handler.invoke_with_context != nil {
		return state.handler.invoke_with_context(
			state.handler.callback_data,
			input.update,
			input.input,
		)
	}
	return state.handler.invoke(state.handler.callback_data, input.input)
}

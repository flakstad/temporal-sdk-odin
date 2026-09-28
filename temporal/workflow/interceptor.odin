package workflow

import codec "../codec"

// Workflow outbound interceptors execute as part of deterministic Workflow
// code, including during replay. Registration order is outermost-first.
// user_data is borrowed for the Worker lifetime. Inputs, Headers, and next
// continuations are call-scoped and must not be retained.
Outbound_Start_Activity_Input :: struct {
	ctx:           ^Fiber_Context,
	activity_type: string,
	input:         codec.Payload_View,
	options:       Activity_Options,
	headers:       ^Headers,
}

Outbound_Start_Activity_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Outbound_Start_Activity_Input,
) -> Activity_Future

Outbound_Start_Activity_Next :: struct {
	data:   rawptr,
	invoke: Outbound_Start_Activity_Next_Proc,
}

Outbound_Start_Activity_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Outbound_Start_Activity_Input,
	next: Outbound_Start_Activity_Next,
) -> Activity_Future

Outbound_Start_Local_Activity_Input :: struct {
	ctx:           ^Fiber_Context,
	activity_type: string,
	input:         codec.Payload_View,
	options:       Local_Activity_Options,
	headers:       ^Headers,
}

Outbound_Start_Local_Activity_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Outbound_Start_Local_Activity_Input,
) -> Activity_Future

Outbound_Start_Local_Activity_Next :: struct {
	data:   rawptr,
	invoke: Outbound_Start_Local_Activity_Next_Proc,
}

Outbound_Start_Local_Activity_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Outbound_Start_Local_Activity_Input,
	next: Outbound_Start_Local_Activity_Next,
) -> Activity_Future

Outbound_Interceptor :: struct {
	user_data:                rawptr,
	start_activity:           Outbound_Start_Activity_Interceptor_Proc,
	start_local_activity:     Outbound_Start_Local_Activity_Interceptor_Proc,
	start_child_workflow:     Outbound_Start_Child_Workflow_Interceptor_Proc,
	signal_child_workflow:    Outbound_Signal_Child_Workflow_Interceptor_Proc,
	signal_external_workflow: Outbound_Signal_External_Workflow_Interceptor_Proc,
	cancel_external_workflow: Outbound_Cancel_External_Workflow_Interceptor_Proc,
	continue_as_new:          Outbound_Continue_As_New_Interceptor_Proc,
	sleep:                    Outbound_Sleep_Interceptor_Proc,
	cancel_child_workflow:    Outbound_Cancel_Child_Workflow_Interceptor_Proc,
}

outbound_interceptor_has_operation :: proc(interceptor: Outbound_Interceptor) -> bool {
	return interceptor.start_activity != nil ||
	       interceptor.start_local_activity != nil ||
	       interceptor.start_child_workflow != nil ||
	       interceptor.signal_child_workflow != nil ||
	       interceptor.signal_external_workflow != nil ||
	       interceptor.cancel_external_workflow != nil ||
	       interceptor.continue_as_new != nil ||
	       interceptor.sleep != nil ||
	       interceptor.cancel_child_workflow != nil
}

workflow_interceptor_failure :: proc(ctx: ^Fiber_Context, message: string) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	ctx.instance.phase = .Failed
	ctx.instance.failure_message = message
	ctx.instance.failure_type = "OdinWorkflowInterceptorError"
}

outbound_start_activity_next :: proc(
	next: Outbound_Start_Activity_Next,
	input: ^Outbound_Start_Activity_Input,
) -> Activity_Future {
	if next.invoke == nil || input == nil || input.ctx == nil {
		if input != nil {
			workflow_interceptor_failure(input.ctx, "invalid Workflow Activity interceptor continuation")
		}
		return {}
	}
	return next.invoke(next.data, input)
}

outbound_start_local_activity_next :: proc(
	next: Outbound_Start_Local_Activity_Next,
	input: ^Outbound_Start_Local_Activity_Input,
) -> Activity_Future {
	if next.invoke == nil || input == nil || input.ctx == nil {
		if input != nil {
			workflow_interceptor_failure(input.ctx, "invalid Workflow Local Activity interceptor continuation")
		}
		return {}
	}
	return next.invoke(next.data, input)
}

Outbound_Interceptor_State :: struct {
	instance: ^Fiber_Instance,
	index:    int,
}

outbound_start_activity_chain :: proc(
	data: rawptr,
	input: ^Outbound_Start_Activity_Input,
) -> Activity_Future {
	state := cast(^Outbound_Interceptor_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil || input == nil {
		if input != nil {
			workflow_interceptor_failure(input.ctx, "invalid Workflow Activity interceptor state")
		}
		return {}
	}
	interceptors := state.instance.runtime.outbound_interceptors[:]
	for index := state.index; index < len(interceptors); index += 1 {
		entry := &interceptors[index]
		if entry.start_activity == nil do continue
		next_state := Outbound_Interceptor_State{instance = state.instance, index = index + 1}
		return entry.start_activity(
			entry.user_data,
			input,
			{data = &next_state, invoke = outbound_start_activity_chain},
		)
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		workflow_interceptor_failure(input.ctx, "Workflow Activity interceptor headers are not initialized")
		return {}
	}
	options := input.options
	options.headers = input.headers
	owned_input, clone_error := codec.payload_clone(
		input.input,
		state.instance.odin_context.allocator,
	)
	if clone_error != .None {
		workflow_interceptor_failure(input.ctx, "could not preserve intercepted Activity input Payload")
		return {}
	}
	return start_activity_owned(input.ctx, input.activity_type, owned_input, options)
}

outbound_start_local_activity_chain :: proc(
	data: rawptr,
	input: ^Outbound_Start_Local_Activity_Input,
) -> Activity_Future {
	state := cast(^Outbound_Interceptor_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil || input == nil {
		if input != nil {
			workflow_interceptor_failure(input.ctx, "invalid Workflow Local Activity interceptor state")
		}
		return {}
	}
	interceptors := state.instance.runtime.outbound_interceptors[:]
	for index := state.index; index < len(interceptors); index += 1 {
		entry := &interceptors[index]
		if entry.start_local_activity == nil do continue
		next_state := Outbound_Interceptor_State{instance = state.instance, index = index + 1}
		return entry.start_local_activity(
			entry.user_data,
			input,
			{data = &next_state, invoke = outbound_start_local_activity_chain},
		)
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		workflow_interceptor_failure(input.ctx, "Workflow Local Activity interceptor headers are not initialized")
		return {}
	}
	options := input.options
	options.headers = input.headers
	owned_input, clone_error := codec.payload_clone(
		input.input,
		state.instance.odin_context.allocator,
	)
	if clone_error != .None {
		workflow_interceptor_failure(input.ctx, "could not preserve intercepted Local Activity input Payload")
		return {}
	}
	return start_local_activity_owned(input.ctx, input.activity_type, owned_input, options)
}

outbound_headers_init :: proc(
	result: ^Headers,
	source: ^Headers,
	instance: ^Fiber_Instance,
) -> bool {
	if result == nil || instance == nil ||
	   !headers_init(result, instance.odin_context.allocator) {
		return false
	}
	if source == nil {
		return true
	}
	if source.allocator.procedure == nil {
		headers_free(result)
		return false
	}
	for &entry in source.entries {
		if !headers_add_payload(result, entry.key, codec.payload_view(&entry.payload)) {
			headers_free(result)
			return false
		}
	}
	return true
}

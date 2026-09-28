package workflow

import "core:time"
import codec "../codec"

Outbound_Sleep_Input :: struct {
	ctx:      ^Fiber_Context,
	duration: time.Duration,
}

Outbound_Sleep_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Outbound_Sleep_Input,
)

Outbound_Sleep_Next :: struct {
	data:   rawptr,
	invoke: Outbound_Sleep_Next_Proc,
}

Outbound_Sleep_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Outbound_Sleep_Input,
	next: Outbound_Sleep_Next,
)

Outbound_Cancel_Child_Workflow_Input :: struct {
	ctx:    ^Fiber_Context,
	future: Child_Workflow_Future,
	reason: string,
}

Outbound_Cancel_Child_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Outbound_Cancel_Child_Workflow_Input,
) -> bool

Outbound_Cancel_Child_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Outbound_Cancel_Child_Workflow_Next_Proc,
}

Outbound_Cancel_Child_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Outbound_Cancel_Child_Workflow_Input,
	next: Outbound_Cancel_Child_Workflow_Next,
) -> bool

Outbound_Start_Child_Workflow_Input :: struct {
	ctx:           ^Fiber_Context,
	workflow_type: string,
	inputs:        []codec.Payload_View,
	options:       Child_Workflow_Options,
	headers:       ^Headers,
}

Outbound_Start_Child_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Outbound_Start_Child_Workflow_Input,
) -> Child_Workflow_Future

Outbound_Start_Child_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Outbound_Start_Child_Workflow_Next_Proc,
}

Outbound_Start_Child_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Outbound_Start_Child_Workflow_Input,
	next: Outbound_Start_Child_Workflow_Next,
) -> Child_Workflow_Future

Outbound_Signal_Child_Workflow_Input :: struct {
	ctx:         ^Fiber_Context,
	future:      Child_Workflow_Future,
	signal_name: string,
	inputs:      []codec.Payload_View,
	options:     Signal_Options,
	headers:     ^Headers,
}

Outbound_Signal_Child_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Outbound_Signal_Child_Workflow_Input,
) -> Child_Workflow_Signal_Result

Outbound_Signal_Child_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Outbound_Signal_Child_Workflow_Next_Proc,
}

Outbound_Signal_Child_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Outbound_Signal_Child_Workflow_Input,
	next: Outbound_Signal_Child_Workflow_Next,
) -> Child_Workflow_Signal_Result

Outbound_Signal_External_Workflow_Input :: struct {
	ctx:         ^Fiber_Context,
	execution:   External_Workflow_Execution,
	signal_name: string,
	inputs:      []codec.Payload_View,
	options:     Signal_Options,
	headers:     ^Headers,
}

Outbound_Signal_External_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Outbound_Signal_External_Workflow_Input,
) -> External_Workflow_Signal_Result

Outbound_Signal_External_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Outbound_Signal_External_Workflow_Next_Proc,
}

Outbound_Signal_External_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Outbound_Signal_External_Workflow_Input,
	next: Outbound_Signal_External_Workflow_Next,
) -> External_Workflow_Signal_Result

Outbound_Cancel_External_Workflow_Input :: struct {
	ctx:       ^Fiber_Context,
	execution: External_Workflow_Execution,
	reason:    string,
}

Outbound_Cancel_External_Workflow_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Outbound_Cancel_External_Workflow_Input,
) -> External_Workflow_Cancel_Result

Outbound_Cancel_External_Workflow_Next :: struct {
	data:   rawptr,
	invoke: Outbound_Cancel_External_Workflow_Next_Proc,
}

Outbound_Cancel_External_Workflow_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Outbound_Cancel_External_Workflow_Input,
	next: Outbound_Cancel_External_Workflow_Next,
) -> External_Workflow_Cancel_Result

Outbound_Continue_As_New_Input :: struct {
	ctx:     ^Fiber_Context,
	input:   codec.Payload_View,
	options: Continue_As_New_Options,
	headers: ^Headers,
}

Outbound_Continue_As_New_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Outbound_Continue_As_New_Input,
) -> !

Outbound_Continue_As_New_Next :: struct {
	data:   rawptr,
	invoke: Outbound_Continue_As_New_Next_Proc,
}

Outbound_Continue_As_New_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Outbound_Continue_As_New_Input,
	next: Outbound_Continue_As_New_Next,
) -> !

outbound_sleep_next :: proc(
	next: Outbound_Sleep_Next,
	input: ^Outbound_Sleep_Input,
) {
	if next.invoke == nil || input == nil || input.ctx == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid Workflow sleep interceptor continuation")
		return
	}
	next.invoke(next.data, input)
}

outbound_cancel_child_workflow_next :: proc(
	next: Outbound_Cancel_Child_Workflow_Next,
	input: ^Outbound_Cancel_Child_Workflow_Input,
) -> bool {
	if next.invoke == nil || input == nil || input.ctx == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid Child Workflow cancellation interceptor continuation")
		return false
	}
	return next.invoke(next.data, input)
}

outbound_start_child_workflow_next :: proc(
	next: Outbound_Start_Child_Workflow_Next,
	input: ^Outbound_Start_Child_Workflow_Input,
) -> Child_Workflow_Future {
	if next.invoke == nil || input == nil || input.ctx == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid Child Workflow interceptor continuation")
		return {}
	}
	return next.invoke(next.data, input)
}

outbound_signal_child_workflow_next :: proc(
	next: Outbound_Signal_Child_Workflow_Next,
	input: ^Outbound_Signal_Child_Workflow_Input,
) -> Child_Workflow_Signal_Result {
	if next.invoke == nil || input == nil || input.ctx == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid Child Workflow Signal interceptor continuation")
		return {status = .Failed, failure_message = "invalid Child Workflow Signal interceptor continuation"}
	}
	return next.invoke(next.data, input)
}

outbound_signal_external_workflow_next :: proc(
	next: Outbound_Signal_External_Workflow_Next,
	input: ^Outbound_Signal_External_Workflow_Input,
) -> External_Workflow_Signal_Result {
	if next.invoke == nil || input == nil || input.ctx == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid external Workflow Signal interceptor continuation")
		return {status = .Failed, failure_message = "invalid external Workflow Signal interceptor continuation"}
	}
	return next.invoke(next.data, input)
}

outbound_cancel_external_workflow_next :: proc(
	next: Outbound_Cancel_External_Workflow_Next,
	input: ^Outbound_Cancel_External_Workflow_Input,
) -> External_Workflow_Cancel_Result {
	if next.invoke == nil || input == nil || input.ctx == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid external Workflow cancellation interceptor continuation")
		return {status = .Failed, failure_message = "invalid external Workflow cancellation interceptor continuation"}
	}
	return next.invoke(next.data, input)
}

outbound_continue_as_new_next :: proc(
	next: Outbound_Continue_As_New_Next,
	input: ^Outbound_Continue_As_New_Input,
) -> ! {
	if next.invoke == nil || input == nil || input.ctx == nil {
		if input == nil || input.ctx == nil {
			panic("invalid Continue-As-New interceptor continuation")
		}
		continue_as_new_stop_with_failure(
			input.ctx,
			"invalid Continue-As-New interceptor continuation",
			"OdinWorkflowInterceptorError",
		)
	}
	next.invoke(next.data, input)
}

outbound_sleep_chain :: proc(
	data: rawptr,
	input: ^Outbound_Sleep_Input,
) {
	state := cast(^Outbound_Interceptor_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil || input == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid Workflow sleep interceptor state")
		return
	}
	for index := state.index; index < len(state.instance.runtime.outbound_interceptors); index += 1 {
		entry := &state.instance.runtime.outbound_interceptors[index]
		if entry.sleep == nil do continue
		next_state := Outbound_Interceptor_State{instance = state.instance, index = index + 1}
		entry.sleep(
			entry.user_data,
			input,
			{data = &next_state, invoke = outbound_sleep_chain},
		)
		return
	}
	sleep_terminal(input.ctx, input.duration)
}

outbound_cancel_child_workflow_chain :: proc(
	data: rawptr,
	input: ^Outbound_Cancel_Child_Workflow_Input,
) -> bool {
	state := cast(^Outbound_Interceptor_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil || input == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid Child Workflow cancellation interceptor state")
		return false
	}
	for index := state.index; index < len(state.instance.runtime.outbound_interceptors); index += 1 {
		entry := &state.instance.runtime.outbound_interceptors[index]
		if entry.cancel_child_workflow == nil do continue
		next_state := Outbound_Interceptor_State{instance = state.instance, index = index + 1}
		return entry.cancel_child_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = outbound_cancel_child_workflow_chain},
		)
	}
	return cancel_child_workflow_terminal(input.ctx, input.future, input.reason)
}

outbound_start_child_workflow_chain :: proc(
	data: rawptr,
	input: ^Outbound_Start_Child_Workflow_Input,
) -> Child_Workflow_Future {
	state := cast(^Outbound_Interceptor_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil || input == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid Child Workflow interceptor state")
		return {}
	}
	for index := state.index; index < len(state.instance.runtime.outbound_interceptors); index += 1 {
		entry := &state.instance.runtime.outbound_interceptors[index]
		if entry.start_child_workflow == nil do continue
		next_state := Outbound_Interceptor_State{instance = state.instance, index = index + 1}
		return entry.start_child_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = outbound_start_child_workflow_chain},
		)
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		workflow_interceptor_failure(input.ctx, "Child Workflow interceptor headers are not initialized")
		return {}
	}
	options := input.options
	options.headers = input.headers
	owned_inputs, inputs_ok := clone_activity_inputs(state.instance, input.inputs)
	if !inputs_ok {
		workflow_interceptor_failure(input.ctx, "could not preserve intercepted Child Workflow input Payloads")
		return {}
	}
	return start_child_workflow_owned(input.ctx, input.workflow_type, owned_inputs, options)
}

outbound_signal_child_workflow_chain :: proc(
	data: rawptr,
	input: ^Outbound_Signal_Child_Workflow_Input,
) -> Child_Workflow_Signal_Result {
	state := cast(^Outbound_Interceptor_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil || input == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid Child Workflow Signal interceptor state")
		return {status = .Failed, failure_message = "invalid Child Workflow Signal interceptor state"}
	}
	for index := state.index; index < len(state.instance.runtime.outbound_interceptors); index += 1 {
		entry := &state.instance.runtime.outbound_interceptors[index]
		if entry.signal_child_workflow == nil do continue
		next_state := Outbound_Interceptor_State{instance = state.instance, index = index + 1}
		return entry.signal_child_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = outbound_signal_child_workflow_chain},
		)
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		return {status = .Failed, failure_message = "Child Workflow Signal interceptor headers are not initialized"}
	}
	options := input.options
	options.headers = input.headers
	owned_inputs, inputs_ok := clone_activity_inputs(state.instance, input.inputs)
	if !inputs_ok {
		return {status = .Failed, failure_message = "could not preserve intercepted Child Workflow Signal Payloads"}
	}
	return signal_child_workflow_payloads_owned(
		input.ctx,
		input.future,
		input.signal_name,
		owned_inputs,
		options,
	)
}

outbound_signal_external_workflow_chain :: proc(
	data: rawptr,
	input: ^Outbound_Signal_External_Workflow_Input,
) -> External_Workflow_Signal_Result {
	state := cast(^Outbound_Interceptor_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil || input == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid external Workflow Signal interceptor state")
		return {status = .Failed, failure_message = "invalid external Workflow Signal interceptor state"}
	}
	for index := state.index; index < len(state.instance.runtime.outbound_interceptors); index += 1 {
		entry := &state.instance.runtime.outbound_interceptors[index]
		if entry.signal_external_workflow == nil do continue
		next_state := Outbound_Interceptor_State{instance = state.instance, index = index + 1}
		return entry.signal_external_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = outbound_signal_external_workflow_chain},
		)
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		return {status = .Failed, failure_message = "external Workflow Signal interceptor headers are not initialized"}
	}
	options := input.options
	options.headers = input.headers
	owned_inputs, inputs_ok := clone_activity_inputs(state.instance, input.inputs)
	if !inputs_ok {
		return {status = .Failed, failure_message = "could not preserve intercepted external Workflow Signal Payloads"}
	}
	return signal_external_workflow_payloads_owned(
		input.ctx,
		input.execution,
		input.signal_name,
		owned_inputs,
		options,
	)
}

outbound_cancel_external_workflow_chain :: proc(
	data: rawptr,
	input: ^Outbound_Cancel_External_Workflow_Input,
) -> External_Workflow_Cancel_Result {
	state := cast(^Outbound_Interceptor_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil || input == nil {
		if input != nil do workflow_interceptor_failure(input.ctx, "invalid external Workflow cancellation interceptor state")
		return {status = .Failed, failure_message = "invalid external Workflow cancellation interceptor state"}
	}
	for index := state.index; index < len(state.instance.runtime.outbound_interceptors); index += 1 {
		entry := &state.instance.runtime.outbound_interceptors[index]
		if entry.cancel_external_workflow == nil do continue
		next_state := Outbound_Interceptor_State{instance = state.instance, index = index + 1}
		return entry.cancel_external_workflow(
			entry.user_data,
			input,
			{data = &next_state, invoke = outbound_cancel_external_workflow_chain},
		)
	}
	return request_cancel_external_workflow_terminal(input.ctx, input.execution, input.reason)
}

outbound_continue_as_new_chain :: proc(
	data: rawptr,
	input: ^Outbound_Continue_As_New_Input,
) -> ! {
	state := cast(^Outbound_Interceptor_State)data
	if state == nil || state.instance == nil || state.instance.runtime == nil || input == nil {
		if input == nil || input.ctx == nil {
			panic("invalid Continue-As-New interceptor state")
		}
		continue_as_new_stop_with_failure(
			input.ctx,
			"invalid Continue-As-New interceptor state",
			"OdinWorkflowInterceptorError",
		)
	}
	for index := state.index; index < len(state.instance.runtime.outbound_interceptors); index += 1 {
		entry := &state.instance.runtime.outbound_interceptors[index]
		if entry.continue_as_new == nil do continue
		next_state := Outbound_Interceptor_State{instance = state.instance, index = index + 1}
		entry.continue_as_new(
			entry.user_data,
			input,
			{data = &next_state, invoke = outbound_continue_as_new_chain},
		)
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		continue_as_new_stop_with_failure(
			input.ctx,
			"Continue-As-New interceptor headers are not initialized",
			"OdinWorkflowInterceptorError",
		)
	}
	options := input.options
	options.headers = input.headers
	owned_input, clone_error := codec.payload_clone(input.input, state.instance.odin_context.allocator)
	if clone_error != .None {
		continue_as_new_stop_with_failure(
			input.ctx,
			"could not preserve intercepted Continue-As-New Payload",
			"OdinContinueAsNewPayloadError",
		)
	}
	continue_as_new_owned(input.ctx, owned_input, options)
}

package workflow_pbt_test

import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import workflow "../temporal/workflow"

Workflow_Interceptor_Property_State :: struct {
	events:      [32]int,
	event_count: int,
}

Workflow_Interceptor_Property_Node :: struct {
	state:         ^Workflow_Interceptor_Property_State,
	id:            int,
	short_circuit: bool,
}

workflow_interceptor_property_activity :: proc(
	user_data: rawptr,
	input: ^workflow.Outbound_Start_Activity_Input,
	next: workflow.Outbound_Start_Activity_Next,
) -> workflow.Activity_Future {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	_ = workflow.headers_add_json(input.headers, "generated-depth", node.id)
	result: workflow.Activity_Future
	if !node.short_circuit {
		result = workflow.outbound_start_activity_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_start_child :: proc(
	user_data: rawptr,
	input: ^workflow.Outbound_Start_Child_Workflow_Input,
	next: workflow.Outbound_Start_Child_Workflow_Next,
) -> workflow.Child_Workflow_Future {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	_ = workflow.headers_add_json(input.headers, "generated-depth", node.id)
	result: workflow.Child_Workflow_Future
	if !node.short_circuit {
		result = workflow.outbound_start_child_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_signal_child :: proc(
	user_data: rawptr,
	input: ^workflow.Outbound_Signal_Child_Workflow_Input,
	next: workflow.Outbound_Signal_Child_Workflow_Next,
) -> workflow.Child_Workflow_Signal_Result {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	_ = workflow.headers_add_json(input.headers, "generated-depth", node.id)
	result := workflow.Child_Workflow_Signal_Result{
		status = .Failed,
		failure_message = "generated short circuit",
	}
	if !node.short_circuit {
		result = workflow.outbound_signal_child_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_signal_external :: proc(
	user_data: rawptr,
	input: ^workflow.Outbound_Signal_External_Workflow_Input,
	next: workflow.Outbound_Signal_External_Workflow_Next,
) -> workflow.External_Workflow_Signal_Result {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	_ = workflow.headers_add_json(input.headers, "generated-depth", node.id)
	result := workflow.External_Workflow_Signal_Result{
		status = .Failed,
		failure_message = "generated short circuit",
	}
	if !node.short_circuit {
		result = workflow.outbound_signal_external_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_cancel_external :: proc(
	user_data: rawptr,
	input: ^workflow.Outbound_Cancel_External_Workflow_Input,
	next: workflow.Outbound_Cancel_External_Workflow_Next,
) -> workflow.External_Workflow_Cancel_Result {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	result := workflow.External_Workflow_Cancel_Result{
		status = .Failed,
		failure_message = "generated short circuit",
	}
	if !node.short_circuit {
		result = workflow.outbound_cancel_external_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_sleep :: proc(
	user_data: rawptr,
	input: ^workflow.Outbound_Sleep_Input,
	next: workflow.Outbound_Sleep_Next,
) {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	input.duration += time.Duration(node.id)
	if !node.short_circuit {
		workflow.outbound_sleep_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
}

workflow_interceptor_property_cancel_child :: proc(
	user_data: rawptr,
	input: ^workflow.Outbound_Cancel_Child_Workflow_Input,
	next: workflow.Outbound_Cancel_Child_Workflow_Next,
) -> bool {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	result := false
	if !node.short_circuit {
		result = workflow.outbound_cancel_child_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_execute :: proc(
	user_data: rawptr,
	input: ^workflow.Inbound_Execute_Workflow_Input,
	next: workflow.Inbound_Execute_Workflow_Next,
) -> workflow.Payload_Result {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	result := workflow.payload_fail("generated short circuit")
	if !node.short_circuit {
		result = workflow.inbound_execute_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_query :: proc(
	user_data: rawptr,
	input: ^workflow.Inbound_Handle_Query_Input,
	next: workflow.Inbound_Handle_Query_Next,
) -> workflow.Payload_Result {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	result := workflow.payload_fail("generated short circuit")
	if !node.short_circuit {
		result = workflow.inbound_handle_query_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_signal :: proc(
	user_data: rawptr,
	input: ^workflow.Inbound_Handle_Signal_Input,
	next: workflow.Inbound_Handle_Signal_Next,
) -> workflow.Payload_Result {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	result := workflow.payload_fail("generated short circuit")
	if !node.short_circuit {
		result = workflow.inbound_handle_signal_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_update_validator :: proc(
	user_data: rawptr,
	input: ^workflow.Inbound_Handle_Update_Input,
	next: workflow.Inbound_Handle_Update_Validator_Next,
) -> workflow.Update_Validation {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	result := workflow.update_reject("generated short circuit")
	if !node.short_circuit {
		result = workflow.inbound_handle_update_validator_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_property_update_handler :: proc(
	user_data: rawptr,
	input: ^workflow.Inbound_Handle_Update_Input,
	next: workflow.Inbound_Handle_Update_Handler_Next,
) -> workflow.Payload_Result {
	node := cast(^Workflow_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	result := workflow.payload_fail("generated short circuit")
	if !node.short_circuit {
		result = workflow.inbound_handle_update_handler_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

workflow_interceptor_chain_property :: proc(t: ^pbt.T) -> pbt.Result {
	depth := pbt.draw(t, pbt.int_range(1, 12))
	short_index := pbt.draw(t, pbt.int_range(0, depth - 1))
	operation := pbt.draw(t, pbt.int_range(0, 11))
	pbt.cover(t, short_index == 0, 5.0, "outermost short circuit")
	pbt.cover(t, short_index == depth - 1, 5.0, "innermost short circuit")
	pbt.cover(t, depth >= 9, 20.0, "deep chain")
	pbt.cover(t, operation == 1, 5.0, "Child Workflow start")
	pbt.cover(t, operation == 2, 5.0, "Child Workflow Signal")
	pbt.cover(t, operation == 3, 5.0, "external Workflow Signal")
	pbt.cover(t, operation == 4, 5.0, "external Workflow cancellation")
	pbt.cover(t, operation == 5, 5.0, "Workflow sleep")
	pbt.cover(t, operation == 6, 5.0, "Child Workflow cancellation")
	pbt.cover(t, operation == 7, 5.0, "Workflow execution")
	pbt.cover(t, operation == 8, 5.0, "Workflow Query")
	pbt.cover(t, operation == 9, 5.0, "Workflow Signal")
	pbt.cover(t, operation == 10, 5.0, "Workflow Update validator")
	pbt.cover(t, operation == 11, 5.0, "Workflow Update handler")

	state := Workflow_Interceptor_Property_State{}
	nodes: [12]Workflow_Interceptor_Property_Node
	runtime := workflow.Fiber_Runtime{
		inbound_interceptors = make([dynamic]workflow.Inbound_Interceptor),
		outbound_interceptors = make([dynamic]workflow.Outbound_Interceptor),
	}
	defer delete(runtime.inbound_interceptors)
	defer delete(runtime.outbound_interceptors)
	instance := workflow.Fiber_Instance{runtime = &runtime}
	ctx := workflow.Fiber_Context{instance = &instance}
	for index in 0..<depth {
		nodes[index] = {
			state = &state,
			id = index + 1,
			short_circuit = index == short_index,
		}
		append(
			&runtime.inbound_interceptors,
			workflow.Inbound_Interceptor{
				user_data = &nodes[index],
				execute_workflow = workflow_interceptor_property_execute,
				handle_query = workflow_interceptor_property_query,
				handle_signal = workflow_interceptor_property_signal,
				handle_update_validator = workflow_interceptor_property_update_validator,
				handle_update_handler = workflow_interceptor_property_update_handler,
			},
		)
		append(
			&runtime.outbound_interceptors,
			workflow.Outbound_Interceptor{
				user_data = &nodes[index],
				start_activity = workflow_interceptor_property_activity,
				start_child_workflow = workflow_interceptor_property_start_child,
				signal_child_workflow = workflow_interceptor_property_signal_child,
				signal_external_workflow = workflow_interceptor_property_signal_external,
				cancel_external_workflow = workflow_interceptor_property_cancel_external,
				sleep = workflow_interceptor_property_sleep,
				cancel_child_workflow = workflow_interceptor_property_cancel_child,
			},
		)
	}

	caller_headers := workflow.Headers{}
	working_headers := workflow.Headers{}
	if !workflow.headers_init(&caller_headers) || !workflow.headers_init(&working_headers) {
		workflow.headers_free(&caller_headers)
		workflow.headers_free(&working_headers)
		return pbt.error("could not initialize generated Workflow headers")
	}
	defer workflow.headers_free(&caller_headers)
	defer workflow.headers_free(&working_headers)
	if !workflow.headers_add_json(&caller_headers, "caller", "property") ||
	   !workflow.headers_add_json(&working_headers, "caller", "property") {
		return pbt.error("could not add generated Workflow header")
	}
	chain := workflow.Outbound_Interceptor_State{instance = &instance}
	switch operation {
	case 0:
		input := workflow.Outbound_Start_Activity_Input{
			ctx = &ctx,
			activity_type = "property-activity",
			input = codec.Payload_View{},
			headers = &working_headers,
		}
		_ = workflow.outbound_start_activity_chain(&chain, &input)
	case 1:
		input := workflow.Outbound_Start_Child_Workflow_Input{
			ctx = &ctx,
			workflow_type = "property-child",
			input = codec.Payload_View{},
			headers = &working_headers,
		}
		_ = workflow.outbound_start_child_workflow_chain(&chain, &input)
	case 2:
		input := workflow.Outbound_Signal_Child_Workflow_Input{
			ctx = &ctx,
			future = {seq = 1},
			signal_name = "property-signal",
			input = codec.Payload_View{},
			headers = &working_headers,
		}
		_ = workflow.outbound_signal_child_workflow_chain(&chain, &input)
	case 3:
		input := workflow.Outbound_Signal_External_Workflow_Input{
			ctx = &ctx,
			execution = {namespace = "default", workflow_id = "property-target"},
			signal_name = "property-signal",
			input = codec.Payload_View{},
			headers = &working_headers,
		}
		_ = workflow.outbound_signal_external_workflow_chain(&chain, &input)
	case 4:
		input := workflow.Outbound_Cancel_External_Workflow_Input{
			ctx = &ctx,
			execution = {namespace = "default", workflow_id = "property-target"},
			reason = "property cancellation",
		}
		_ = workflow.outbound_cancel_external_workflow_chain(&chain, &input)
	case 5:
		input := workflow.Outbound_Sleep_Input{ctx = &ctx, duration = time.Second}
		workflow.outbound_sleep_chain(&chain, &input)
	case 6:
		input := workflow.Outbound_Cancel_Child_Workflow_Input{
			ctx = &ctx,
			future = {seq = 1},
			reason = "property child cancellation",
		}
		_ = workflow.outbound_cancel_child_workflow_chain(&chain, &input)
	case 7:
		inbound_chain := workflow.Inbound_Execute_Workflow_State{instance = &instance}
		input := workflow.Inbound_Execute_Workflow_Input{
			ctx = &ctx,
			workflow_type = "property-workflow",
		}
		_ = workflow.inbound_execute_workflow_chain(&inbound_chain, &input)
	case 8:
		query := workflow.Query_Context{id = "property-query", type = "status"}
		handler := workflow.Fiber_Query_Handler{name = "status"}
		inbound_chain := workflow.Inbound_Handle_Query_State{
			instance = &instance,
			handler = &handler,
		}
		input := workflow.Inbound_Handle_Query_Input{
			workflow_type = "property-workflow",
			query = &query,
		}
		_ = workflow.inbound_handle_query_chain(&inbound_chain, &input)
	case 9:
		signal := workflow.Signal_Context{name = "property-signal"}
		handler := workflow.Fiber_Signal_Handler{name = "property-signal"}
		inbound_chain := workflow.Inbound_Handle_Signal_State{
			instance = &instance,
			handler = &handler,
		}
		input := workflow.Inbound_Handle_Signal_Input{
			workflow_type = "property-workflow",
			signal = &signal,
		}
		_ = workflow.inbound_handle_signal_chain(&inbound_chain, &input)
	case 10:
		update := workflow.Update_Context{id = "property-update", name = "rename"}
		handler := workflow.Fiber_Update_Handler{name = "rename"}
		inbound_chain := workflow.Inbound_Handle_Update_Validator_State{
			instance = &instance,
			handler = &handler,
		}
		input := workflow.Inbound_Handle_Update_Input{
			workflow_type = "property-workflow",
			update = &update,
		}
		_ = workflow.inbound_handle_update_validator_chain(&inbound_chain, &input)
	case 11:
		update := workflow.Update_Context{id = "property-update", name = "rename"}
		handler := workflow.Fiber_Update_Handler{name = "rename"}
		inbound_chain := workflow.Inbound_Handle_Update_Handler_State{
			instance = &instance,
			handler = &handler,
		}
		input := workflow.Inbound_Handle_Update_Input{
			workflow_type = "property-workflow",
			update = &update,
		}
		_ = workflow.inbound_handle_update_handler_chain(&inbound_chain, &input)
	}
	reached := short_index + 1
	if state.event_count != reached * 2 {
		return pbt.fail("Workflow interceptor chain emitted the wrong event count")
	}
	for index in 0..<reached {
		if state.events[index] != index + 1 {
			return pbt.fail("Workflow interceptor entry order changed")
		}
		if state.events[reached + index] != -(reached - index) {
			return pbt.fail("Workflow interceptor unwind order changed")
		}
	}
	if len(caller_headers.entries) != 1 || caller_headers.entries[0].key != "caller" {
		return pbt.fail("Workflow interceptor mutated caller-owned headers")
	}
	expected_working_headers := 2
	if operation >= 4 {
		expected_working_headers = 1
	}
	if len(working_headers.entries) != expected_working_headers {
		return pbt.fail("Workflow interceptor mutations did not share one working header map")
	}
	return pbt.pass()
}

@(test)
workflow_interceptors_preserve_nested_next_and_header_isolation :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow interceptors preserve nested next and header isolation",
		workflow_interceptor_chain_property,
		{num_tests = 500, seed = 0x574f_524b_464c_4f57},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

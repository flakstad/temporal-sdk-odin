package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import activity "../temporal/activity"
import codec "../temporal/codec"
import worker "../temporal/worker"

Activity_Interceptor_Property_State :: struct {
	events:         [32]int,
	event_count:    int,
	terminal_calls: int,
}

Activity_Interceptor_Property_Node :: struct {
	state:         ^Activity_Interceptor_Property_State,
	id:            int,
	short_circuit: bool,
}

activity_interceptor_property_record :: proc(
	state: ^Activity_Interceptor_Property_State,
	event: int,
) {
	if state.event_count < len(state.events) {
		state.events[state.event_count] = event
		state.event_count += 1
	}
}

activity_interceptor_property_execute :: proc(
	user_data: rawptr,
	input: ^worker.Activity_Execute_Input,
	next: worker.Activity_Execute_Next,
) -> activity.Payload_Result {
	node := cast(^Activity_Interceptor_Property_Node)user_data
	activity_interceptor_property_record(node.state, node.id)
	if node.short_circuit {
		return activity.payload_fail(
			"generated interceptor rejection",
			failure_type = "GeneratedActivityInterceptorRejection",
		)
	}
	result := worker.activity_execute_next(next, input)
	activity_interceptor_property_record(node.state, -node.id)
	return result
}

activity_interceptor_property_terminal :: proc(
	callback_data: rawptr,
	_: ^activity.Context,
	inputs: []codec.Payload_View,
) -> activity.Payload_Result {
	state := cast(^Activity_Interceptor_Property_State)callback_data
	activity_interceptor_property_record(state, 1000)
	state.terminal_calls += 1
	if len(inputs) != 1 {
		return activity.payload_fail("generated terminal expected one input")
	}
	payload, clone_error := codec.payload_clone(inputs[0])
	if clone_error != .None {
		return activity.payload_fail("generated terminal clone failed")
	}
	return activity.payload_complete(payload)
}

activity_interceptor_chain_property :: proc(t: ^pbt.T) -> pbt.Result {
	depth := pbt.draw(t, pbt.int_range(1, 12))
	short_choice := pbt.draw(t, pbt.int_range(0, depth))
	short_index := short_choice
	if short_choice == depth {
		short_index = -1
	}
	pbt.cover(t, short_index < 0, 5.0, "terminal reached")
	pbt.cover(t, short_index == 0, 5.0, "outermost short circuit")
	pbt.cover(t, short_index == depth - 1, 5.0, "innermost short circuit")
	pbt.cover(t, depth >= 9, 20.0, "deep chain")

	sdk_worker := worker.Worker{
		activity_interceptors = make([dynamic]worker.Activity_Interceptor),
		lifecycle = .Configuring,
	}
	defer delete(sdk_worker.activity_interceptors)
	state := Activity_Interceptor_Property_State{}
	nodes: [12]Activity_Interceptor_Property_Node
	for index in 0..<depth {
		nodes[index] = {
			state = &state,
			id = index + 1,
			short_circuit = index == short_index,
		}
		registration_error := worker.add_activity_interceptor(
			&sdk_worker,
			{
				user_data = &nodes[index],
				execute = activity_interceptor_property_execute,
			},
		)
		if registration_error.message != nil {
			worker.error_free(&registration_error)
			return pbt.error("generated Activity interceptor registration failed")
		}
		worker.error_free(&registration_error)
	}

	payload, payload_error := codec.encode_json(depth)
	if payload_error != .None {
		return pbt.error("could not encode generated Activity input")
	}
	defer codec.payload_free(&payload)
	ctx := activity.Context{}
	execution := worker.Activity_Execution{
		worker = &sdk_worker,
		callback_data = &state,
		invoke = activity_interceptor_property_terminal,
		ctx = ctx,
	}
	input := worker.Activity_Execute_Input{
		ctx = &execution.ctx,
		inputs = []codec.Payload_View{codec.payload_view(&payload)},
	}
	chain := worker.Activity_Execute_Chain_State{execution = &execution}
	result := worker.activity_execute_chain_invoke(&chain, &input)
	defer codec.payload_free(&result.payload)
	defer activity.application_failure_free(&result.failure)

	reached := depth
	if short_index >= 0 {
		reached = short_index + 1
	}
	expected_count := reached * 2 + 1
	if short_index >= 0 {
		expected_count = reached + short_index
	}
	if state.event_count != expected_count {
		return pbt.fail("Activity interceptor chain emitted the wrong event count")
	}
	for index in 0..<reached {
		if state.events[index] != index + 1 {
			return pbt.fail("Activity interceptor entry order changed")
		}
	}
	if short_index < 0 {
		if result.status != .Completed || state.terminal_calls != 1 ||
		   state.events[depth] != 1000 {
			return pbt.fail("Activity interceptor terminal was not reached exactly once")
		}
		for offset in 0..<depth {
			expected := -(depth - offset)
			if state.events[depth + 1 + offset] != expected {
				return pbt.fail("Activity interceptor exit order changed")
			}
		}
	} else {
		if result.status != .Failed || state.terminal_calls != 0 {
			return pbt.fail("Activity interceptor short circuit reached the terminal")
		}
		for offset in 0..<short_index {
			expected := -(short_index - offset)
			if state.events[reached + offset] != expected {
				return pbt.fail("Activity interceptor short-circuit unwind order changed")
			}
		}
	}
	return pbt.pass()
}

@(test)
activity_interceptors_preserve_nested_next_semantics :: proc(t: ^testing.T) {
	result := pbt.check(
		"Activity interceptors preserve nested next semantics",
		activity_interceptor_chain_property,
		{num_tests = 500, seed = 0x494e_5445_5243_4550},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

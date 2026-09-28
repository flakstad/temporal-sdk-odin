package workflow_pbt_test

import "core:testing"
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

workflow_interceptor_chain_property :: proc(t: ^pbt.T) -> pbt.Result {
	depth := pbt.draw(t, pbt.int_range(1, 12))
	short_index := pbt.draw(t, pbt.int_range(0, depth - 1))
	pbt.cover(t, short_index == 0, 5.0, "outermost short circuit")
	pbt.cover(t, short_index == depth - 1, 5.0, "innermost short circuit")
	pbt.cover(t, depth >= 9, 20.0, "deep chain")

	state := Workflow_Interceptor_Property_State{}
	nodes: [12]Workflow_Interceptor_Property_Node
	runtime := workflow.Fiber_Runtime{
		outbound_interceptors = make([dynamic]workflow.Outbound_Interceptor),
	}
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
			&runtime.outbound_interceptors,
			workflow.Outbound_Interceptor{
				user_data = &nodes[index],
				start_activity = workflow_interceptor_property_activity,
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
	input := workflow.Outbound_Start_Activity_Input{
		ctx = &ctx,
		activity_type = "property-activity",
		input = codec.Payload_View{},
		headers = &working_headers,
	}
	chain := workflow.Outbound_Interceptor_State{instance = &instance}
	result := workflow.outbound_start_activity_chain(&chain, &input)
	if result.seq != 0 {
		return pbt.fail("short-circuited Workflow interceptor returned an Activity future")
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
	if len(working_headers.entries) != 2 {
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


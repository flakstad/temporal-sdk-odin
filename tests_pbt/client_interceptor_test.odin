package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"

Client_Interceptor_Property_State :: struct {
	events:      [32]int,
	event_count: int,
}

Client_Interceptor_Property_Node :: struct {
	state:         ^Client_Interceptor_Property_State,
	id:            int,
	short_circuit: bool,
}

client_interceptor_property_signal :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Signal_Workflow_Input,
	next: temporal_client.Client_Signal_Workflow_Next,
) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	header_error := temporal_client.headers_add_json(input.headers, "generated-depth", node.id)
	if header_error.message != nil {
		return header_error
	}
	result: temporal_client.Error
	if node.short_circuit {
		result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result = temporal_client.client_signal_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

client_interceptor_chain_property :: proc(t: ^pbt.T) -> pbt.Result {
	depth := pbt.draw(t, pbt.int_range(1, 12))
	short_index := pbt.draw(t, pbt.int_range(0, depth - 1))
	pbt.cover(t, short_index == 0, 5.0, "outermost short circuit")
	pbt.cover(t, short_index == depth - 1, 5.0, "innermost short circuit")
	pbt.cover(t, depth >= 9, 20.0, "deep chain")

	state := Client_Interceptor_Property_State{}
	nodes: [12]Client_Interceptor_Property_Node
	client := temporal_client.Client{
		interceptors = make([dynamic]temporal_client.Client_Interceptor),
	}
	defer temporal_client.close(&client)
	for index in 0..<depth {
		nodes[index] = {
			state = &state,
			id = index + 1,
			short_circuit = index == short_index,
		}
		append(
			&client.interceptors,
			temporal_client.Client_Interceptor{
				user_data = &nodes[index],
				signal_workflow = client_interceptor_property_signal,
			},
		)
	}

	headers := temporal_client.Headers{}
	init_error := temporal_client.headers_init(&headers)
	if init_error.message != nil {
		temporal_client.error_free(&init_error)
		return pbt.error("could not initialize generated caller headers")
	}
	temporal_client.error_free(&init_error)
	defer temporal_client.headers_free(&headers)
	caller_error := temporal_client.headers_add_json(&headers, "caller", "property")
	if caller_error.message != nil {
		temporal_client.error_free(&caller_error)
		return pbt.error("could not add generated caller header")
	}
	temporal_client.error_free(&caller_error)
	handle := temporal_client.Workflow_Handle{client = &client, id = "property-workflow"}
	signal_error := temporal_client.signal_workflow_with_options(
		&handle,
		"property-signal",
		{headers = &headers},
	)
	defer temporal_client.error_free(&signal_error)
	if signal_error.kind != .Rpc || string(signal_error.message) != "generated short circuit" {
		return pbt.fail("generated Client interceptor did not short circuit")
	}
	reached := short_index + 1
	if state.event_count != reached * 2 {
		return pbt.fail("Client interceptor chain emitted the wrong event count")
	}
	for index in 0..<reached {
		if state.events[index] != index + 1 {
			return pbt.fail("Client interceptor entry order changed")
		}
		if state.events[reached + index] != -(reached - index) {
			return pbt.fail("Client interceptor unwind order changed")
		}
	}
	if len(headers.entries) != 1 || headers.entries[0].key != "caller" {
		return pbt.fail("Client interceptor mutated caller-owned headers")
	}
	return pbt.pass()
}

@(test)
client_interceptors_preserve_nested_next_and_header_isolation :: proc(t: ^testing.T) {
	result := pbt.check(
		"Client interceptors preserve nested next and header isolation",
		client_interceptor_chain_property,
		{num_tests = 500, seed = 0x434c_4945_4e54_494e},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

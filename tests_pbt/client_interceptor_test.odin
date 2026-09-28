package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"
import temporal_core "../temporal/core"
import raw "../temporal/core/raw"

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

client_interceptor_property_cancel :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Cancel_Workflow_Input,
	next: temporal_client.Client_Cancel_Workflow_Next,
) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	input.reason = "generated cancellation"
	result: temporal_client.Error
	if node.short_circuit {
		result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result = temporal_client.client_cancel_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

client_interceptor_property_terminate :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Terminate_Workflow_Input,
	next: temporal_client.Client_Terminate_Workflow_Next,
) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	input.reason = "generated termination"
	result: temporal_client.Error
	if node.short_circuit {
		result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result = temporal_client.client_terminate_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

client_interceptor_chain_property :: proc(t: ^pbt.T) -> pbt.Result {
	depth := pbt.draw(t, pbt.int_range(1, 12))
	short_index := pbt.draw(t, pbt.int_range(0, depth - 1))
	operation := pbt.draw(t, pbt.int_range(0, 2))
	pbt.cover(t, short_index == 0, 5.0, "outermost short circuit")
	pbt.cover(t, short_index == depth - 1, 5.0, "innermost short circuit")
	pbt.cover(t, depth >= 9, 20.0, "deep chain")
	pbt.cover(t, operation == 0, 20.0, "Workflow Signal")
	pbt.cover(t, operation == 1, 20.0, "Workflow cancellation")
	pbt.cover(t, operation == 2, 20.0, "Workflow termination")

	state := Client_Interceptor_Property_State{}
	nodes: [12]Client_Interceptor_Property_Node
	client := temporal_client.Client{
		interceptors = make([dynamic]temporal_client.Client_Interceptor),
	}
	defer delete(client.interceptors)
	connection := temporal_core.Client{handle = cast(^raw.Connection)(uintptr(1))}
	client.connection = &connection
	for index in 0..<depth {
		nodes[index] = {
			state = &state,
			id = index + 1,
			short_circuit = index == short_index,
		}
		interceptor := temporal_client.Client_Interceptor{user_data = &nodes[index]}
		switch operation {
		case 0:
			interceptor.signal_workflow = client_interceptor_property_signal
		case 1:
			interceptor.cancel_workflow = client_interceptor_property_cancel
		case 2:
			interceptor.terminate_workflow = client_interceptor_property_terminate
		}
		append(&client.interceptors, interceptor)
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
	operation_error: temporal_client.Error
	switch operation {
	case 0:
		operation_error = temporal_client.signal_workflow_with_options(
			&handle,
			"property-signal",
			{headers = &headers},
		)
	case 1:
		operation_error = temporal_client.cancel_workflow(&handle, "caller cancellation")
	case 2:
		operation_error = temporal_client.terminate_workflow(&handle, "caller termination")
	}
	defer temporal_client.error_free(&operation_error)
	if operation_error.kind != .Rpc || string(operation_error.message) != "generated short circuit" {
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

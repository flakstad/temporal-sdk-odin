package workflow_pbt_test

import "core:bytes"
import "core:mem/virtual"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"
import codec "../temporal/codec"
import proto "../temporal/core/proto"

signal_with_start_request_property :: proc(t: ^pbt.T) -> pbt.Result {
	workflow_input_value := pbt.draw(t, pbt.string_ascii(0, 96))
	signal_input_value := pbt.draw(t, pbt.string_ascii(0, 96))
	header_value := pbt.draw(t, pbt.string_ascii(0, 64))
	run_timeout_seconds := pbt.draw(t, pbt.int_range(0, 86_400))
	task_timeout_seconds := pbt.draw(t, pbt.int_range(0, 120))
	reuse_number := pbt.draw(t, pbt.int_range(0, 4))
	conflict_choice := pbt.draw(t, pbt.int_range(0, 2))
	conflict_number := conflict_choice
	if conflict_choice > 0 {
		conflict_number += 1
	}
	if reuse_number == int(temporal_client.Workflow_Id_Reuse_Policy.Terminate_If_Running) {
		conflict_number = 0
	}
	pbt.cover(t, len(workflow_input_value) == 0, 0.2, "empty Workflow input")
	pbt.cover(t, len(signal_input_value) == 0, 0.2, "empty Signal input")
	pbt.cover(t, conflict_number == 0, 20.0, "Service-default conflict policy")
	pbt.cover(
		t,
		conflict_number == int(temporal_client.Workflow_Id_Conflict_Policy.Terminate_Existing),
		20.0,
		"terminate-existing conflict policy",
	)

	workflow_input := codec.payload_create(transmute([]u8)workflow_input_value)
	defer codec.payload_free(&workflow_input)
	signal_input := codec.payload_create(transmute([]u8)signal_input_value)
	defer codec.payload_free(&signal_input)
	headers := temporal_client.Headers{}
	header_init_error := temporal_client.headers_init(&headers)
	defer temporal_client.error_free(&header_init_error)
	if header_init_error.message != nil {
		return pbt.error("could not initialize generated Signal headers")
	}
	defer temporal_client.headers_free(&headers)
	header_error := temporal_client.headers_add_json(&headers, "property-header", header_value)
	defer temporal_client.error_free(&header_error)
	if header_error.message != nil {
		return pbt.error("could not encode generated Signal header")
	}

	encoded, encode_error := temporal_client.encode_signal_with_start_workflow_request(
		"property-namespace",
		"property_workflow",
		codec.payload_view(&workflow_input),
		"property-signal",
		codec.payload_view(&signal_input),
		{
			start_options = {
				id = "property-workflow-id",
				task_queue = "property-queue",
				request_id = "property-request-id",
				workflow_run_timeout = time.Duration(run_timeout_seconds) * time.Second,
				workflow_task_timeout = time.Duration(task_timeout_seconds) * time.Second,
				id_reuse_policy = temporal_client.Workflow_Id_Reuse_Policy(reuse_number),
				id_conflict_policy = temporal_client.Workflow_Id_Conflict_Policy(conflict_number),
			},
			signal_headers = &headers,
		},
	)
	defer delete(encoded)
	defer temporal_client.error_free(&encode_error)
	if encode_error.message != nil {
		return pbt.fail("valid generated Signal-With-Start request was rejected")
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize Signal-With-Start property arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	request := proto.Temporal_Api_Workflowservice_V1_SignalWithStartWorkflowExecutionRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_SignalWithStartWorkflowExecutionRequest(
		encoded[:],
		&request,
	)
	context.allocator = previous_allocator
	if !decoded || request.input == nil || len(request.input.payloads) != 1 ||
	   request.signal_input == nil || len(request.signal_input.payloads) != 1 ||
	   request.header == nil || len(request.header.fields) != 1 {
		return pbt.fail("generated Signal-With-Start request did not decode")
	}
	if request.namespace != "property-namespace" ||
	   request.workflow_id != "property-workflow-id" ||
	   request.workflow_type.name != "property_workflow" ||
	   request.task_queue.name != "property-queue" ||
	   request.request_id != "property-request-id" ||
	   request.signal_name != "property-signal" ||
	   !bytes.equal(request.input.payloads[0].data, transmute([]u8)workflow_input_value) ||
	   !bytes.equal(request.signal_input.payloads[0].data, transmute([]u8)signal_input_value) ||
	   int(request.workflow_id_reuse_policy) != reuse_number ||
	   int(request.workflow_id_conflict_policy) != conflict_number {
		return pbt.fail("generated Signal-With-Start scalar fields changed during round trip")
	}
	if (run_timeout_seconds == 0) != (request.workflow_run_timeout == nil) ||
	   request.workflow_run_timeout != nil && request.workflow_run_timeout.seconds != i64(run_timeout_seconds) ||
	   (task_timeout_seconds == 0) != (request.workflow_task_timeout == nil) ||
	   request.workflow_task_timeout != nil && request.workflow_task_timeout.seconds != i64(task_timeout_seconds) {
		return pbt.fail("generated Signal-With-Start timeouts changed during round trip")
	}
	owned_header, clone_error := proto.clone_payload(request.header.fields[0].value)
	defer codec.payload_free(&owned_header)
	if clone_error != .None {
		return pbt.fail("generated Signal header could not be preserved")
	}
	decoded_header: string
	defer delete(decoded_header)
	if codec.decode_json(codec.payload_view(&owned_header), &decoded_header) != .None ||
	   decoded_header != header_value {
		return pbt.fail("generated Signal header changed during round trip")
	}
	return pbt.pass()
}

@(test)
signal_with_start_requests_preserve_generated_contract :: proc(t: ^testing.T) {
	result := pbt.check(
		"Signal-With-Start requests preserve start, signal, and header fields",
		signal_with_start_request_property,
		{num_tests = 500, seed = 0x5349_474e_414c_5354},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

signal_request_property :: proc(t: ^pbt.T) -> pbt.Result {
	argument_count := pbt.draw(t, pbt.int_range(0, 12))
	header_value := pbt.draw(t, pbt.string_ascii(0, 64))
	run_id := pbt.draw(t, pbt.string_ascii(0, 48))
	pbt.cover(t, argument_count == 0, 2.0, "zero Signal arguments")
	pbt.cover(t, argument_count == 1, 2.0, "one Signal argument")
	pbt.cover(t, argument_count >= 8, 20.0, "many Signal arguments")
	pbt.cover(t, len(header_value) == 0, 0.5, "empty Signal header value")
	pbt.cover(t, len(run_id) == 0, 0.5, "current-run Signal target")

	input_values: [12]string
	inputs: [12]codec.Payload
	input_views: [12]codec.Payload_View
	external_sizes: [12]i64
	defer {
		for index in 0..<argument_count {
			codec.payload_free(&inputs[index])
		}
	}
	for index in 0..<argument_count {
		input_values[index] = pbt.draw(t, pbt.string_ascii(0, 96))
		external_sizes[index] = i64(pbt.draw(t, pbt.int_range(0, 1_000_000)))
		inputs[index] = codec.payload_create(transmute([]u8)input_values[index])
		if codec.payload_add_metadata(
			&inputs[index],
			"argument",
			transmute([]u8)input_values[index],
		) != .None || codec.payload_add_external(&inputs[index], external_sizes[index]) != .None {
			return pbt.error("could not decorate generated Signal argument")
		}
		input_views[index] = codec.payload_view(&inputs[index])
	}
	headers := temporal_client.Headers{}
	init_error := temporal_client.headers_init(&headers)
	defer temporal_client.error_free(&init_error)
	if init_error.message != nil {
		return pbt.error("could not initialize generated Signal headers")
	}
	defer temporal_client.headers_free(&headers)
	header_error := temporal_client.headers_add_json(&headers, "property-header", header_value)
	defer temporal_client.error_free(&header_error)
	if header_error.message != nil {
		return pbt.error("could not encode generated Signal header")
	}

	encoded, encode_error := temporal_client.encode_signal_workflow_request_payloads(
		"property-namespace",
		"property-workflow-id",
		run_id,
		"property-signal",
		input_views[:argument_count],
		{request_id = "property-request-id", headers = &headers},
	)
	defer delete(encoded)
	defer temporal_client.error_free(&encode_error)
	if encode_error.message != nil {
		return pbt.fail("valid generated Signal request was rejected")
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize Signal property arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	request := proto.Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest(
		encoded[:],
		&request,
	)
	context.allocator = previous_allocator
	actual_argument_count := 0
	if request.input != nil {
		actual_argument_count = len(request.input.payloads)
	}
	if !decoded || request.workflow_execution == nil ||
	   actual_argument_count != argument_count || request.header == nil ||
	   len(request.header.fields) != 1 || request.header.fields[0].value == nil {
		return pbt.fail("generated Signal request did not decode")
	}
	if request.namespace != "property-namespace" ||
	   request.workflow_execution.workflow_id != "property-workflow-id" ||
	   request.workflow_execution.run_id != run_id ||
	   request.signal_name != "property-signal" ||
	   request.request_id != "property-request-id" ||
	   request.header.fields[0].key != "property-header" {
		return pbt.fail("generated Signal scalar fields changed during round trip")
	}
	for index in 0..<argument_count {
		argument := &request.input.payloads[index]
		if !bytes.equal(argument.data, transmute([]u8)input_values[index]) ||
		   len(argument.external_payloads) != 1 ||
		   argument.external_payloads[0].size_bytes != external_sizes[index] {
			return pbt.fail("generated Signal argument order or external details changed")
		}
		metadata_value: []u8
		for entry in argument.metadata {
			if entry.key == "argument" {
				metadata_value = entry.value
				break
			}
		}
		if !bytes.equal(metadata_value, transmute([]u8)input_values[index]) {
			return pbt.fail("generated Signal argument metadata changed")
		}
	}
	owned_header, clone_error := proto.clone_payload(request.header.fields[0].value)
	defer codec.payload_free(&owned_header)
	if clone_error != .None {
		return pbt.fail("generated Signal header could not be preserved")
	}
	decoded_header: string
	defer delete(decoded_header)
	if codec.decode_json(codec.payload_view(&owned_header), &decoded_header) != .None ||
	   decoded_header != header_value {
		return pbt.fail("generated Signal header changed during round trip")
	}
	reencoded, reencode_error := temporal_client.encode_signal_workflow_request_payloads(
		"property-namespace",
		"property-workflow-id",
		run_id,
		"property-signal",
		input_views[:argument_count],
		{request_id = "property-request-id", headers = &headers},
	)
	defer delete(reencoded)
	defer temporal_client.error_free(&reencode_error)
	if reencode_error.message != nil || !bytes.equal(encoded[:], reencoded[:]) {
		return pbt.fail("generated Signal request bytes were not deterministic")
	}
	return pbt.pass()
}

@(test)
signal_requests_preserve_generated_headers :: proc(t: ^testing.T) {
	result := pbt.check(
		"Signal requests preserve ordered arguments, details, and headers",
		signal_request_property,
		{num_tests = 500, seed = 0x5349_474e_414c_4844},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

update_request_property :: proc(t: ^pbt.T) -> pbt.Result {
	header_value := pbt.draw(t, pbt.string_ascii(0, 64))
	run_id := pbt.draw(t, pbt.string_ascii(0, 48))
	argument_count := pbt.draw(t, pbt.int_range(0, 12))
	pbt.cover(t, len(header_value) == 0, 0.5, "empty Update header value")
	pbt.cover(t, len(run_id) == 0, 0.5, "current-run Update target")
	pbt.cover(t, argument_count == 0, 2.0, "zero Update arguments")
	pbt.cover(t, argument_count == 1, 2.0, "one Update argument")
	pbt.cover(t, argument_count >= 8, 20.0, "many Update arguments")

	headers := temporal_client.Headers{}
	init_error := temporal_client.headers_init(&headers)
	defer temporal_client.error_free(&init_error)
	if init_error.message != nil {
		return pbt.error("could not initialize generated Update headers")
	}
	defer temporal_client.headers_free(&headers)
	header_error := temporal_client.headers_add_json(&headers, "property-header", header_value)
	defer temporal_client.error_free(&header_error)
	if header_error.message != nil {
		return pbt.error("could not encode generated Update header")
	}
	input_values: [12]string
	inputs: [12]codec.Payload
	input_views: [12]codec.Payload_View
	external_sizes: [12]i64
	defer {
		for index in 0..<argument_count {
			codec.payload_free(&inputs[index])
		}
	}
	for index in 0..<argument_count {
		input_values[index] = pbt.draw(t, pbt.string_ascii(0, 96))
		external_sizes[index] = i64(pbt.draw(t, pbt.int_range(0, 1_000_000)))
		inputs[index] = codec.payload_create(transmute([]u8)input_values[index])
		if codec.payload_add_metadata(
			&inputs[index],
			"argument",
			transmute([]u8)input_values[index],
		) != .None || codec.payload_add_external(&inputs[index], external_sizes[index]) != .None {
			return pbt.error("could not decorate generated Update argument")
		}
		input_views[index] = codec.payload_view(&inputs[index])
	}

	encoded, encode_error := temporal_client.encode_update_workflow_request_payloads(
		"property-namespace",
		"property-workflow-id",
		run_id,
		"property-first-run-id",
		"property-update",
		input_views[:argument_count],
		{update_id = "property-update-id", headers = &headers},
		"property-request-id",
	)
	defer delete(encoded)
	defer temporal_client.error_free(&encode_error)
	if encode_error.message != nil {
		return pbt.fail("valid generated Update request was rejected")
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize Update property arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	request := proto.Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionRequest(
		encoded[:],
		&request,
	)
	context.allocator = previous_allocator
	if !decoded || request.workflow_execution == nil || request.request == nil ||
	   request.request.meta == nil || request.request.input == nil ||
	   request.request.input.args == nil ||
	   len(request.request.input.args.payloads) != argument_count ||
	   request.request.input.header == nil || len(request.request.input.header.fields) != 1 ||
	   request.request.input.header.fields[0].value == nil {
		return pbt.fail("generated Update request did not decode")
	}
	if request.namespace != "property-namespace" ||
	   request.workflow_execution.workflow_id != "property-workflow-id" ||
	   request.workflow_execution.run_id != run_id ||
	   request.first_execution_run_id != "property-first-run-id" ||
	   request.request.meta.update_id != "property-update-id" ||
	   request.request.request_id != "property-request-id" ||
	   request.request.input.name != "property-update" ||
	   request.request.input.header.fields[0].key != "property-header" {
		return pbt.fail("generated Update fields changed during round trip")
	}
	for index in 0..<argument_count {
		argument := &request.request.input.args.payloads[index]
		if !bytes.equal(argument.data, transmute([]u8)input_values[index]) ||
		   len(argument.external_payloads) != 1 ||
		   argument.external_payloads[0].size_bytes != external_sizes[index] {
			return pbt.fail("generated Update argument order or external details changed")
		}
		metadata_value: []u8
		for entry in argument.metadata {
			if entry.key == "argument" {
				metadata_value = entry.value
				break
			}
		}
		if !bytes.equal(metadata_value, transmute([]u8)input_values[index]) {
			return pbt.fail("generated Update argument metadata changed")
		}
	}
	owned_header, clone_error := proto.clone_payload(
		request.request.input.header.fields[0].value,
	)
	defer codec.payload_free(&owned_header)
	if clone_error != .None {
		return pbt.fail("generated Update header could not be preserved")
	}
	decoded_header: string
	defer delete(decoded_header)
	if codec.decode_json(codec.payload_view(&owned_header), &decoded_header) != .None ||
	   decoded_header != header_value {
		return pbt.fail("generated Update header changed during round trip")
	}
	reencoded, reencode_error := temporal_client.encode_update_workflow_request_payloads(
		"property-namespace",
		"property-workflow-id",
		run_id,
		"property-first-run-id",
		"property-update",
		input_views[:argument_count],
		{update_id = "property-update-id", headers = &headers},
		"property-request-id",
	)
	defer delete(reencoded)
	defer temporal_client.error_free(&reencode_error)
	if reencode_error.message != nil || !bytes.equal(encoded[:], reencoded[:]) {
		return pbt.fail("generated Update request bytes were not deterministic")
	}
	return pbt.pass()
}

@(test)
update_requests_preserve_generated_headers :: proc(t: ^testing.T) {
	result := pbt.check(
		"Update requests preserve ordered arguments, details, identity, and headers",
		update_request_property,
		{num_tests = 500, seed = 0x5550_4441_5445_4844},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

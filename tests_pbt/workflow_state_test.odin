package workflow_pbt_test

import "core:bytes"
import "core:mem/virtual"
import "core:strings"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import activity "../temporal/activity"
import temporal_client "../temporal/client"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import worker "../temporal/worker"
import workflow "../temporal/workflow"

Activity_Heartbeat_Capture :: struct {
	calls: int,
}

capture_activity_heartbeat :: proc(
	user_data: rawptr,
	details: codec.Payload_View,
) -> activity.Heartbeat_Status {
	_ = details
	capture := cast(^Activity_Heartbeat_Capture)user_data
	capture.calls += 1
	return .Recorded
}

Generated_Payload_Value :: struct {
	name:    string,
	count:   int,
	enabled: bool,
}

json_payload_round_trip_property :: proc(t: ^pbt.T) -> pbt.Result {
	expected := Generated_Payload_Value{
		name = pbt.draw(t, pbt.string_ascii(0, 32)),
		count = pbt.draw(t, pbt.int_range(-100000, 100000)),
		enabled = pbt.draw(t, pbt.boolean()),
	}
	external_size := i64(pbt.draw(t, pbt.int_range(0, 1000000)))
	pbt.cover(t, len(expected.name) == 0, 2.0, "empty string")
	pbt.cover(t, len(expected.name) >= 24, 10.0, "long string")
	pbt.cover(t, expected.count < 0, 30.0, "negative integer")

	payload, encode_error := codec.encode_json(expected)
	if encode_error != .None {
		return pbt.fail("generated Odin value could not be encoded")
	}
	defer codec.payload_free(&payload)
	metadata_error := codec.payload_add_metadata(
		&payload,
		"odin-sdk-pbt",
		transmute([]u8)expected.name,
	)
	if metadata_error != .None {
		return pbt.error("could not add generated payload metadata")
	}
	if codec.payload_add_external(&payload, external_size) != .None {
		return pbt.error("could not add generated external payload details")
	}

	cloned, clone_error := codec.payload_clone(codec.payload_view(&payload))
	if clone_error != .None {
		return pbt.error("could not clone generated payload")
	}
	defer codec.payload_free(&cloned)
	codec.payload_free(&payload)

	metadata, found := codec.metadata_get(codec.payload_view(&cloned), "odin-sdk-pbt")
	if !found || string(metadata) != expected.name {
		return pbt.fail("payload clone did not preserve generated metadata")
	}
	if len(cloned.external_payloads) != 1 || cloned.external_payloads[0].size_bytes != external_size {
		return pbt.fail("payload clone did not preserve external payload details")
	}
	actual: Generated_Payload_Value
	decode_error := codec.decode_json(codec.payload_view(&cloned), &actual)
	if decode_error != .None {
		return pbt.fail("cloned generated payload could not be decoded")
	}
	defer delete(actual.name)
	if actual.name != expected.name || actual.count != expected.count || actual.enabled != expected.enabled {
		return pbt.fail("generated payload changed during JSON round trip")
	}
	return pbt.pass()
}

@(test)
json_payloads_preserve_generated_values_and_metadata :: proc(t: ^testing.T) {
	result := pbt.check(
		"JSON payloads preserve generated Odin values and metadata",
		json_payload_round_trip_property,
		{num_tests = 500, seed = 0x5041_594c_4f41_4453},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

client_close_result_property :: proc(t: ^pbt.T) -> pbt.Result {
	expected := Generated_Payload_Value{
		name = pbt.draw(t, pbt.string_ascii(0, 32)),
		count = pbt.draw(t, pbt.int_range(-100000, 100000)),
		enabled = pbt.draw(t, pbt.boolean()),
	}
	pbt.cover(t, len(expected.name) == 0, 2.0, "empty result string")
	pbt.cover(t, expected.enabled, 30.0, "enabled result")

	owned_payload, encode_error := codec.encode_json(expected)
	if encode_error != .None {
		return pbt.error("could not encode generated Client result")
	}
	defer codec.payload_free(&owned_payload)
	proto_payload := proto.encode_payload(codec.payload_view(&owned_payload))
	defer delete(proto_payload.metadata)
	defer delete(proto_payload.external_payloads)
	payloads := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(payloads.payloads)
	append(&payloads.payloads, proto_payload)
	completed := proto.Temporal_Api_History_V1_WorkflowExecutionCompletedEventAttributes{
		result = &payloads,
	}
	event := proto.Temporal_Api_History_V1_HistoryEvent{
		event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		attributes = proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Completed_Event_Attributes{
			value = &completed,
		},
	}
	history := proto.Temporal_Api_History_V1_History{}
	defer delete(history.events)
	append(&history.events, event)
	response := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse{
		history = &history,
	}
	encoded := proto.encode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse(&response)
	page, page_error := temporal_client.decode_result_page(encoded[:])
	delete(encoded)
	defer temporal_client.result_page_free(&page)
	defer temporal_client.error_free(&page_error)
	if page_error.message != nil || page.kind != .Completed || !page.has_payload {
		return pbt.fail("generated close event did not produce a Client result")
	}

	actual: Generated_Payload_Value
	decode_error := codec.decode_json(codec.payload_view(&page.payload), &actual)
	if decode_error != .None {
		return pbt.fail("generated Client result could not be decoded")
	}
	defer delete(actual.name)
	if actual.name != expected.name || actual.count != expected.count || actual.enabled != expected.enabled {
		return pbt.fail("Client close-event decoding changed the generated result")
	}
	return pbt.pass()
}

@(test)
client_close_events_preserve_generated_results :: proc(t: ^testing.T) {
	result := pbt.check(
		"Client close events preserve generated results",
		client_close_result_property,
		{num_tests = 500, seed = 0x434c_4945_4e54_5253},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

activity_failure_wire_property :: proc(t: ^pbt.T) -> pbt.Result {
	failure_type := pbt.draw(t, pbt.string_ascii(1, 32))
	non_retryable := pbt.draw(t, pbt.boolean())
	has_retry_delay := pbt.draw(t, pbt.boolean())
	delay_milliseconds := 0
	if has_retry_delay {
		delay_milliseconds = pbt.draw(t, pbt.int_range(1, 60_000))
	}
	detail_value := pbt.draw(t, pbt.int_range(-100_000, 100_000))
	pbt.cover(t, non_retryable, 30.0, "non-retryable")
	pbt.cover(t, !non_retryable, 30.0, "retryable")
	pbt.cover(t, delay_milliseconds == 0, 30.0, "default retry delay")
	pbt.cover(t, delay_milliseconds > 0, 30.0, "explicit retry delay")

	details, details_error := codec.encode_json(detail_value)
	if details_error != .None {
		return pbt.error("could not encode generated failure details")
	}
	defer codec.payload_free(&details)
	delay := time.Duration(delay_milliseconds) * time.Millisecond
	completion := proto.encode_activity_failure(
		transmute([]u8)string("generated-token"),
		"generated failure",
		failure_type,
		non_retryable,
		codec.payload_view(&details),
		true,
		delay,
	)
	defer delete(completion)

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize failure decode arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	decoded := proto.Coresdk_ActivityTaskCompletion{}
	decoded_ok := proto.decode_Coresdk_ActivityTaskCompletion(completion[:], &decoded)
	context.allocator = previous_allocator
	if !decoded_ok || decoded.result == nil {
		return pbt.fail("generated Activity failure did not decode")
	}

	found := false
	valid := false
	#partial switch status in decoded.result.status {
	case proto.Coresdk_Activity_Result_ActivityExecutionResult_Status_Failed:
		if status.value == nil || status.value.failure == nil {
			break
		}
		#partial switch failure_info in status.value.failure.failure_info {
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
			if failure_info.value == nil {
				break
			}
			found = true
			info := failure_info.value
			valid = info.type == failure_type &&
				info.non_retryable == non_retryable &&
				info.details != nil &&
				len(info.details.payloads) == 1 &&
				bytes.equal(info.details.payloads[0].data, details.data)
			if delay_milliseconds == 0 {
				valid = valid && info.next_retry_delay == nil
			} else {
				valid = valid && info.next_retry_delay != nil
				if info.next_retry_delay != nil {
					actual_nanoseconds := info.next_retry_delay.seconds * i64(time.Second) +
						i64(info.next_retry_delay.nanos)
					valid = valid && actual_nanoseconds == i64(delay)
				}
			}
			operation := workflow.Fiber_Activity_Operation{}
			context.allocator = virtual.arena_allocator(&decode_arena)
			preserved := workflow.preserve_activity_failure(&operation, status.value.failure)
			context.allocator = previous_allocator
			preserved_detail := 0
			detail_found, detail_error := workflow.activity_failure_detail_json(
				&operation.failure,
				0,
				&preserved_detail,
			)
			valid = valid && preserved && operation.failure.application.present &&
				operation.failure.application.message == "generated failure" &&
				operation.failure.application.type == failure_type &&
				operation.failure.application.non_retryable == non_retryable &&
				operation.failure.application.has_next_retry_delay == has_retry_delay &&
				operation.failure.application.next_retry_delay == delay &&
				detail_found && detail_error == .None && preserved_detail == detail_value
		}
	}
	if !found || !valid {
		return pbt.fail("ApplicationFailure fields changed across protobuf encoding")
	}
	return pbt.pass()
}

@(test)
activity_failures_preserve_generated_retry_contracts :: proc(t: ^testing.T) {
	result := pbt.check(
		"Activity failures preserve generated retry contracts",
		activity_failure_wire_property,
		{num_tests = 500, seed = 0x5245_5452_595f_4641},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

query_response_wire_property :: proc(t: ^pbt.T) -> pbt.Result {
	query_id := pbt.draw(t, pbt.string_ascii(1, 32))
	succeed := pbt.draw(t, pbt.boolean())
	expected := Generated_Payload_Value{
		name = pbt.draw(t, pbt.string_ascii(0, 24)),
		count = pbt.draw(t, pbt.int_range(-10_000, 10_000)),
		enabled = pbt.draw(t, pbt.boolean()),
	}
	failure_type := pbt.draw(t, pbt.string_ascii(1, 24))
	pbt.cover(t, succeed, 30.0, "successful query")
	pbt.cover(t, !succeed, 30.0, "failed query")

	payload: codec.Payload
	has_payload := false
	defer {
		if has_payload {
			codec.payload_free(&payload)
		}
	}
	result: workflow.Payload_Result
	if succeed {
		encode_error: codec.Error
		payload, encode_error = codec.encode_json(expected)
		if encode_error != .None {
			return pbt.error("could not encode generated query result")
		}
		has_payload = true
		result = workflow.payload_complete(payload)
	} else {
		result = workflow.payload_fail("generated query failure", failure_type)
	}

	command_arena: virtual.Arena
	if virtual.arena_init_growing(&command_arena) != nil {
		return pbt.error("could not initialize query command arena")
	}
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&command_arena)
	commands := make([dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand)
	response := workflow.Pending_Query_Response{query_id = query_id, result = result}
	workflow.append_query_response_command(&commands, &response)
	context.allocator = previous_allocator
	success := proto.Coresdk_Workflow_Completion_Success{commands = commands}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{
		run_id = "generated-query-run",
		status = proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful{
			value = &success,
		},
	}
	encoded := proto.encode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(&completion)
	virtual.arena_destroy(&command_arena)
	defer delete(encoded)

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize query response decode arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	context.allocator = virtual.arena_allocator(&decode_arena)
	decoded := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded_ok := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
		encoded[:],
		&decoded,
	)
	context.allocator = previous_allocator
	if !decoded_ok {
		return pbt.fail("generated query response did not decode")
	}

	found := false
	valid := false
	#partial switch status in decoded.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil || len(status.value.commands) != 1 {
			break
		}
		#partial switch variant in status.value.commands[0].variant {
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Respond_To_Query:
			if variant.value == nil || variant.value.query_id != query_id {
				break
			}
			found = true
			#partial switch query_variant in variant.value.variant {
			case proto.Coresdk_Workflow_Commands_QueryResult_Variant_Succeeded:
				if !succeed || query_variant.value == nil || query_variant.value.response == nil {
					break
				}
				owned, clone_error := proto.clone_payload(query_variant.value.response)
				if clone_error != .None {
					break
				}
				actual: Generated_Payload_Value
				decode_error := codec.decode_json(codec.payload_view(&owned), &actual)
				valid = decode_error == .None && actual.name == expected.name &&
					actual.count == expected.count && actual.enabled == expected.enabled
				delete(actual.name)
				codec.payload_free(&owned)
			case proto.Coresdk_Workflow_Commands_QueryResult_Variant_Failed:
				if succeed || query_variant.value == nil {
					break
				}
				valid = query_variant.value.message == "generated query failure"
				#partial switch failure_info in query_variant.value.failure_info {
				case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
					valid = valid && failure_info.value != nil &&
						failure_info.value.type == failure_type && failure_info.value.non_retryable
				}
			}
		}
	}
	if !found || !valid {
		return pbt.fail("generated Workflow query response changed across protobuf encoding")
	}
	return pbt.pass()
}

@(test)
query_responses_preserve_generated_payloads_and_failures :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow query responses preserve generated payloads and failures",
		query_response_wire_property,
		{num_tests = 500, seed = 0x5155_4552_595f_5253},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

update_response_wire_property :: proc(t: ^pbt.T) -> pbt.Result {
	protocol_instance_id := pbt.draw(t, pbt.string_ascii(1, 32))
	mode := pbt.draw(t, pbt.int_range(0, 2))
	expected := Generated_Payload_Value{
		name = pbt.draw(t, pbt.string_ascii(0, 24)),
		count = pbt.draw(t, pbt.int_range(-10_000, 10_000)),
		enabled = pbt.draw(t, pbt.boolean()),
	}
	failure_type := pbt.draw(t, pbt.string_ascii(1, 24))
	pbt.cover(t, mode == 0, 20.0, "accepted and completed")
	pbt.cover(t, mode == 1, 20.0, "validator rejected")
	pbt.cover(t, mode == 2, 20.0, "accepted then handler rejected")

	payload: codec.Payload
	has_payload := false
	defer {
		if has_payload {
			codec.payload_free(&payload)
		}
	}
	if mode == 0 {
		encode_error: codec.Error
		payload, encode_error = codec.encode_json(expected)
		if encode_error != .None {
			return pbt.error("could not encode generated Update result")
		}
		has_payload = true
	}

	command_arena: virtual.Arena
	if virtual.arena_init_growing(&command_arena) != nil {
		return pbt.error("could not initialize Update command arena")
	}
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&command_arena)
	commands := make([dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand)
	if mode != 1 {
		accepted := workflow.Pending_Update_Response{
			protocol_instance_id = protocol_instance_id,
			kind = .Accepted,
		}
		workflow.append_update_response_command(&commands, &accepted)
	}
	if mode == 0 {
		completed := workflow.Pending_Update_Response{
			protocol_instance_id = protocol_instance_id,
			kind = .Completed,
			result = workflow.payload_complete(payload),
		}
		workflow.append_update_response_command(&commands, &completed)
	} else {
		rejected := workflow.Pending_Update_Response{
			protocol_instance_id = protocol_instance_id,
			kind = .Rejected,
			result = workflow.payload_fail("generated Update rejection", failure_type),
		}
		workflow.append_update_response_command(&commands, &rejected)
	}
	context.allocator = previous_allocator
	success := proto.Coresdk_Workflow_Completion_Success{commands = commands}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{
		run_id = "generated-update-run",
		status = proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful{
			value = &success,
		},
	}
	encoded := proto.encode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(&completion)
	virtual.arena_destroy(&command_arena)
	defer delete(encoded)

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize Update decode arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	context.allocator = virtual.arena_allocator(&decode_arena)
	decoded := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded_ok := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
		encoded[:],
		&decoded,
	)
	context.allocator = previous_allocator
	if !decoded_ok {
		return pbt.fail("generated Update response did not decode")
	}

	accepted_count := 0
	completed_count := 0
	rejected_count := 0
	valid := true
	#partial switch status in decoded.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil || len(status.value.commands) != (mode == 1 ? 1 : 2) {
			valid = false
			break
		}
		for command, index in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Update_Response:
				if variant.value == nil || variant.value.protocol_instance_id != protocol_instance_id {
					valid = false
					continue
				}
				#partial switch response in variant.value.response {
				case proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Accepted:
					accepted_count += 1
					valid = valid && index == 0 && mode != 1
				case proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Completed:
					completed_count += 1
					if mode != 0 || index != 1 || response.value == nil {
						valid = false
						continue
					}
					owned, clone_error := proto.clone_payload(response.value)
					if clone_error != .None {
						valid = false
						continue
					}
					actual: Generated_Payload_Value
					decode_error := codec.decode_json(codec.payload_view(&owned), &actual)
					valid = valid && decode_error == .None && actual.name == expected.name &&
						actual.count == expected.count && actual.enabled == expected.enabled
					delete(actual.name)
					codec.payload_free(&owned)
				case proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Rejected:
					rejected_count += 1
					valid = valid && mode != 0 && response.value != nil &&
						response.value.message == "generated Update rejection"
					if response.value != nil {
						#partial switch failure_info in response.value.failure_info {
						case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
							valid = valid && failure_info.value != nil &&
								failure_info.value.type == failure_type && failure_info.value.non_retryable
						}
					}
				}
			}
		}
	}
	valid = valid && accepted_count == (mode == 1 ? 0 : 1) &&
		completed_count == (mode == 0 ? 1 : 0) && rejected_count == (mode == 0 ? 0 : 1)
	if !valid {
		return pbt.fail("generated Workflow Update responses changed across protobuf encoding")
	}
	return pbt.pass()
}

@(test)
update_responses_preserve_protocol_order_and_results :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow Update responses preserve protocol order and results",
		update_response_wire_property,
		{num_tests = 500, seed = 0x5550_4441_5445_5253},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

concurrent_update_completion_order_property :: proc(t: ^pbt.T) -> pbt.Result {
	second_finishes_first := pbt.draw(t, pbt.boolean())
	first_value := pbt.draw(t, pbt.int_range(-100_000, 100_000))
	second_value := pbt.draw(t, pbt.int_range(-100_000, 100_000))
	pbt.cover(t, second_finishes_first, 40.0, "second Update finishes first")
	pbt.cover(t, !second_finishes_first, 40.0, "first Update finishes first")

	first_payload, first_error := codec.encode_json(first_value)
	second_payload, second_error := codec.encode_json(second_value)
	if first_error != .None || second_error != .None {
		codec.payload_free(&first_payload)
		codec.payload_free(&second_payload)
		return pbt.error("could not encode concurrent Update results")
	}
	defer codec.payload_free(&first_payload)
	defer codec.payload_free(&second_payload)

	command_arena: virtual.Arena
	if virtual.arena_init_growing(&command_arena) != nil {
		return pbt.error("could not initialize concurrent Update command arena")
	}
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&command_arena)
	commands := make([dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand)
	first_accepted := workflow.Pending_Update_Response{
		protocol_instance_id = "concurrent-update-first",
		kind = .Accepted,
	}
	second_accepted := workflow.Pending_Update_Response{
		protocol_instance_id = "concurrent-update-second",
		kind = .Accepted,
	}
	first_completed := workflow.Pending_Update_Response{
		protocol_instance_id = "concurrent-update-first",
		kind = .Completed,
		result = workflow.payload_complete(first_payload),
	}
	second_completed := workflow.Pending_Update_Response{
		protocol_instance_id = "concurrent-update-second",
		kind = .Completed,
		result = workflow.payload_complete(second_payload),
	}
	workflow.append_update_response_command(&commands, &first_accepted)
	workflow.append_update_response_command(&commands, &second_accepted)
	if second_finishes_first {
		workflow.append_update_response_command(&commands, &second_completed)
		workflow.append_update_response_command(&commands, &first_completed)
	} else {
		workflow.append_update_response_command(&commands, &first_completed)
		workflow.append_update_response_command(&commands, &second_completed)
	}
	context.allocator = previous_allocator
	success := proto.Coresdk_Workflow_Completion_Success{commands = commands}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{
		run_id = "generated-concurrent-update-run",
		status = proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful{
			value = &success,
		},
	}
	encoded := proto.encode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(&completion)
	virtual.arena_destroy(&command_arena)
	defer delete(encoded)

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize concurrent Update decode arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	context.allocator = virtual.arena_allocator(&decode_arena)
	decoded := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded_ok := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
		encoded[:],
		&decoded,
	)
	context.allocator = previous_allocator
	if !decoded_ok {
		return pbt.fail("concurrent Update responses did not decode")
	}

	valid := true
	#partial switch status in decoded.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil || len(status.value.commands) != 4 {
			return pbt.fail("concurrent Update response command count changed")
		}
		for command, index in status.value.commands {
			expected_id := "concurrent-update-first"
			expected_value := first_value
			if index == 1 || (index == 2 && second_finishes_first) ||
			   (index == 3 && !second_finishes_first) {
				expected_id = "concurrent-update-second"
				expected_value = second_value
			}
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Update_Response:
				if variant.value == nil || variant.value.protocol_instance_id != expected_id {
					valid = false
					continue
				}
				if index < 2 {
					#partial switch response in variant.value.response {
					case proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Accepted:
						// google.protobuf.Empty may decode as a nil zero-size pointer.
						_ = response.value
					case:
						valid = false
					}
				} else {
					#partial switch response in variant.value.response {
					case proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Completed:
						if response.value == nil {
							valid = false
							continue
						}
						owned, clone_error := proto.clone_payload(response.value)
						if clone_error != .None {
							valid = false
							continue
						}
						actual := 0
						decode_error := codec.decode_json(codec.payload_view(&owned), &actual)
						valid = valid && decode_error == .None && actual == expected_value
						codec.payload_free(&owned)
					case:
						valid = false
					}
				}
			case:
				valid = false
			}
		}
	}
	if !valid {
		return pbt.fail("concurrent Update protocol identity or completion order changed")
	}
	return pbt.pass()
}

@(test)
concurrent_update_responses_preserve_resolution_order :: proc(t: ^testing.T) {
	result := pbt.check(
		"Concurrent Workflow Updates preserve protocol identity and resolution order",
		concurrent_update_completion_order_property,
		{num_tests = 500, seed = 0x5550_4441_5445_324f},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

activity_deadline_property :: proc(t: ^pbt.T) -> pbt.Result {
	scheduled_seconds := i64(pbt.draw(t, pbt.int_range(1, 1_000_000)))
	started_offset := i64(pbt.draw(t, pbt.int_range(0, 2_000)))
	schedule_timeout_seconds := i64(pbt.draw(t, pbt.int_range(1, 20_000)))
	start_timeout_seconds := i64(pbt.draw(t, pbt.int_range(1, 20_000)))
	has_schedule_deadline := pbt.draw(t, pbt.boolean())
	has_attempt_deadline := pbt.draw(t, pbt.boolean())
	if !has_schedule_deadline && !has_attempt_deadline {
		has_attempt_deadline = true
	}
	scheduled := time.unix(scheduled_seconds, 0)
	started := time.unix(scheduled_seconds + started_offset, 0)
	schedule_timeout := time.Duration(schedule_timeout_seconds) * time.Second
	start_timeout := time.Duration(start_timeout_seconds) * time.Second
	schedule_deadline_ns := time.to_unix_nanoseconds(time.time_add(scheduled, schedule_timeout))
	attempt_deadline_ns := time.to_unix_nanoseconds(time.time_add(started, start_timeout))
	pbt.cover(
		t,
		has_schedule_deadline && has_attempt_deadline && schedule_deadline_ns < attempt_deadline_ns,
		10.0,
		"Schedule-to-Close wins",
	)
	pbt.cover(
		t,
		has_schedule_deadline && has_attempt_deadline && attempt_deadline_ns <= schedule_deadline_ns,
		10.0,
		"Start-to-Close wins",
	)
	pbt.cover(t, has_schedule_deadline && !has_attempt_deadline, 10.0, "only Schedule-to-Close")
	pbt.cover(t, !has_schedule_deadline && has_attempt_deadline, 10.0, "only Start-to-Close")

	actual, present := worker.activity_deadline(
		{value = scheduled, present = has_schedule_deadline},
		{value = started, present = has_attempt_deadline},
		{value = schedule_timeout, present = has_schedule_deadline},
		{value = start_timeout, present = has_attempt_deadline},
	)
	if !present {
		return pbt.fail("a generated Activity deadline unexpectedly disappeared")
	}
	expected_ns := attempt_deadline_ns
	if has_schedule_deadline && (!has_attempt_deadline || schedule_deadline_ns < attempt_deadline_ns) {
		expected_ns = schedule_deadline_ns
	}
	if time.to_unix_nanoseconds(actual) != expected_ns {
		return pbt.fail("Activity deadline was not the earliest applicable timeout")
	}
	return pbt.pass()
}

@(test)
activity_deadline_is_earliest_applicable_timeout :: proc(t: ^testing.T) {
	result := pbt.check(
		"Activity deadline is the earliest applicable timeout",
		activity_deadline_property,
		{num_tests = 500, seed = 0x4143_5449_5649_5459},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Generated_Workflow_Output :: struct {
	name:    string,
	count:   int,
	enabled: bool,
}

generated_typed_workflow :: proc(
	_: ^workflow.Fiber_Context,
	input: Generated_Payload_Value,
) -> Generated_Workflow_Output {
	return {
		name = input.name,
		count = input.count + 1,
		enabled = !input.enabled,
	}
}

clone_pbt_workflow_result :: proc(data: []u8) -> (payload: codec.Payload, found: bool) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion)
	context.allocator = previous_allocator
	if !decoded {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return
		}
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution:
				if variant.value != nil && variant.value.result != nil {
					clone_error: codec.Error
					payload, clone_error = proto.clone_payload(variant.value.result)
					found = clone_error == .None
					return
				}
			}
		}
	}
	return
}

typed_workflow_payload_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	input_value := Generated_Payload_Value{
		name = pbt.draw(t, pbt.string_ascii(0, 32)),
		count = pbt.draw(t, pbt.int_range(-100_000, 100_000)),
		enabled = pbt.draw(t, pbt.boolean()),
	}
	owned_queue, queue_error := strings.clone("pbt-queue")
	if queue_error != nil {
		return pbt.error("could not allocate generated Workflow queue")
	}
	sdk_worker := worker.Worker{
		task_queue = owned_queue,
		workflow_registrations = make([dynamic]worker.Workflow_Registration),
		lifecycle = .Configuring,
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}
	register_error := worker.register_json_workflow(
		&sdk_worker,
		"generated_typed_workflow",
		generated_typed_workflow,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register generated typed Workflow")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input, encode_error := codec.encode_json(input_value)
	if encode_error != .None {
		return pbt.error("could not encode generated Workflow input")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_typed_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_activation(runtime, &activation)
	defer destroy_process_result(&initial)
	if initial.error.message != nil || !initial.completed {
		return pbt.fail("generated typed Workflow did not complete")
	}
	result_payload, result_found := clone_pbt_workflow_result(initial.completion[:])
	if !result_found {
		return pbt.fail("generated typed Workflow completion has no Payload")
	}
	defer codec.payload_free(&result_payload)
	actual: Generated_Workflow_Output
	if codec.decode_json(codec.payload_view(&result_payload), &actual) != .None {
		return pbt.fail("generated typed Workflow result did not decode")
	}
	defer delete(actual.name)
	if actual.name != input_value.name || actual.count != input_value.count + 1 || actual.enabled == input_value.enabled {
		return pbt.fail("generated typed Workflow changed its structured value")
	}

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_activation(runtime, &activation)
	if evicted.error.message != nil || !evicted.evicted {
		destroy_process_result(&evicted)
		return pbt.fail("could not evict generated typed Workflow")
	}
	destroy_process_result(&evicted)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed := process_activation(runtime, &activation)
	defer destroy_process_result(&replayed)
	if replayed.error.message != nil || !bytes.equal(initial.completion[:], replayed.completion[:]) {
		return pbt.fail("generated typed Workflow replay changed completion bytes")
	}
	return pbt.pass()
}

@(test)
typed_workflow_payloads_replay_for_generated_structs :: proc(t: ^testing.T) {
	result := pbt.check(
		"typed Workflow Payloads replay for generated structs",
		typed_workflow_payload_replay_property,
		{num_tests = 500, seed = 0x5459_5045_445f_5746},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Generated_Activity_Options_Input :: struct {
	activity_id:              string,
	task_queue:               string,
	schedule_to_close_ms:     int,
	schedule_to_start_ms:     int,
	start_to_close_ms:        int,
	heartbeat_ms:             int,
	retry_initial_ms:         int,
	retry_maximum_ms:         int,
	retry_maximum_attempts:   int,
	cancellation_type:        int,
	disable_eager_execution:  bool,
}

generated_activity_options_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Generated_Activity_Options_Input,
) -> string {
	cancellation_type := workflow.Activity_Cancellation_Type.Try_Cancel
	switch input.cancellation_type {
	case 1:
		cancellation_type = .Wait_Cancellation_Completed
	case 2:
		cancellation_type = .Abandon
	}
	non_retryable_types := [?]string{"GeneratedPermanentFailure"}
	retry_policy := workflow.Retry_Policy{
		initial_interval = time.Duration(input.retry_initial_ms) * time.Millisecond,
		backoff_coefficient = 1.5,
		maximum_interval = time.Duration(input.retry_maximum_ms) * time.Millisecond,
		maximum_attempts = i32(input.retry_maximum_attempts),
		non_retryable_error_types = non_retryable_types[:],
	}
	return workflow.execute_activity_json_with_options(
		ctx,
		"generated_options_activity",
		input.activity_id,
		string,
		{
			activity_id = input.activity_id,
			task_queue = input.task_queue,
			schedule_to_close_timeout = time.Duration(input.schedule_to_close_ms) * time.Millisecond,
			schedule_to_start_timeout = time.Duration(input.schedule_to_start_ms) * time.Millisecond,
			start_to_close_timeout = time.Duration(input.start_to_close_ms) * time.Millisecond,
			heartbeat_timeout = time.Duration(input.heartbeat_ms) * time.Millisecond,
			retry_policy = &retry_policy,
			cancellation_type = cancellation_type,
			disable_eager_execution = input.disable_eager_execution,
		},
	)
}

pbt_proto_duration :: proc(value: ^proto.Google_Protobuf_Duration) -> time.Duration {
	if value == nil {
		return 0
	}
	return time.Duration(value.seconds * i64(time.Second) + i64(value.nanos))
}

generated_activity_options_match :: proc(
	data: []u8,
	expected: ^Generated_Activity_Options_Input,
) -> bool {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return false
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion)
	context.allocator = previous_allocator
	if !decoded {
		return false
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil || len(status.value.commands) != 1 {
			return false
		}
		#partial switch variant in status.value.commands[0].variant {
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity:
			if variant.value == nil || variant.value.retry_policy == nil {
				return false
			}
			schedule := variant.value
			policy := schedule.retry_policy
			expected_cancellation := proto.Coresdk_Workflow_Commands_ActivityCancellationType.TRY_CANCEL
			switch expected.cancellation_type {
			case 1:
				expected_cancellation = .WAIT_CANCELLATION_COMPLETED
			case 2:
				expected_cancellation = .ABANDON
			}
			return schedule.activity_id == expected.activity_id &&
				schedule.activity_type == "generated_options_activity" &&
				schedule.task_queue == expected.task_queue &&
				pbt_proto_duration(schedule.schedule_to_close_timeout) == time.Duration(expected.schedule_to_close_ms) * time.Millisecond &&
				pbt_proto_duration(schedule.schedule_to_start_timeout) == time.Duration(expected.schedule_to_start_ms) * time.Millisecond &&
				pbt_proto_duration(schedule.start_to_close_timeout) == time.Duration(expected.start_to_close_ms) * time.Millisecond &&
				pbt_proto_duration(schedule.heartbeat_timeout) == time.Duration(expected.heartbeat_ms) * time.Millisecond &&
				pbt_proto_duration(policy.initial_interval) == time.Duration(expected.retry_initial_ms) * time.Millisecond &&
				policy.backoff_coefficient == 1.5 &&
				pbt_proto_duration(policy.maximum_interval) == time.Duration(expected.retry_maximum_ms) * time.Millisecond &&
				policy.maximum_attempts == i32(expected.retry_maximum_attempts) &&
				len(policy.non_retryable_error_types) == 1 &&
				policy.non_retryable_error_types[0] == "GeneratedPermanentFailure" &&
				schedule.cancellation_type == expected_cancellation &&
				schedule.do_not_eagerly_execute == expected.disable_eager_execution
		}
	}
	return false
}

activity_options_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	schedule_to_close_ms := pbt.draw(t, pbt.int_range(100, 120_000))
	start_to_close_ms := pbt.draw(t, pbt.int_range(1, schedule_to_close_ms))
	schedule_to_start_ms := pbt.draw(t, pbt.int_range(1, schedule_to_close_ms))
	heartbeat_ms := pbt.draw(t, pbt.int_range(1, start_to_close_ms))
	retry_initial_ms := pbt.draw(t, pbt.int_range(1, 10_000))
	retry_maximum_ms := pbt.draw(t, pbt.int_range(retry_initial_ms, 120_000))
	input_value := Generated_Activity_Options_Input{
		activity_id = pbt.draw(t, pbt.string_ascii(1, 32)),
		task_queue = pbt.draw(t, pbt.string_ascii(1, 32)),
		schedule_to_close_ms = schedule_to_close_ms,
		schedule_to_start_ms = schedule_to_start_ms,
		start_to_close_ms = start_to_close_ms,
		heartbeat_ms = heartbeat_ms,
		retry_initial_ms = retry_initial_ms,
		retry_maximum_ms = retry_maximum_ms,
		retry_maximum_attempts = pbt.draw(t, pbt.int_range(0, 20)),
		cancellation_type = pbt.draw(t, pbt.int_range(0, 2)),
		disable_eager_execution = pbt.draw(t, pbt.boolean()),
	}
	pbt.cover(t, input_value.cancellation_type == 0, 20.0, "try cancel")
	pbt.cover(t, input_value.cancellation_type == 1, 20.0, "wait for cancellation")
	pbt.cover(t, input_value.cancellation_type == 2, 20.0, "abandon")
	pbt.cover(t, input_value.retry_maximum_attempts == 0, 2.0, "unlimited attempts")
	pbt.cover(t, input_value.disable_eager_execution, 30.0, "eager disabled")

	owned_queue, queue_error := strings.clone("pbt-workflow-queue")
	if queue_error != nil {
		return pbt.error("could not allocate Activity options Workflow queue")
	}
	sdk_worker := worker.Worker{
		task_queue = owned_queue,
		workflow_registrations = make([dynamic]worker.Workflow_Registration),
		lifecycle = .Configuring,
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}
	register_error := worker.register_json_workflow(
		&sdk_worker,
		"generated_activity_options_workflow",
		generated_activity_options_workflow,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register Activity options Workflow")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input, encode_error := codec.encode_json(input_value)
	if encode_error != .None {
		return pbt.error("could not encode generated Activity options")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_activity_options_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-options-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_activation(runtime, &activation)
	defer destroy_process_result(&initial)
	if initial.error.message != nil || !initial.scheduled_activity {
		return pbt.fail("generated Activity options did not schedule an Activity")
	}
	if !generated_activity_options_match(initial.completion[:], &input_value) {
		return pbt.fail("generated Activity options changed across command encoding")
	}

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_activation(runtime, &activation)
	if evicted.error.message != nil || !evicted.evicted {
		destroy_process_result(&evicted)
		return pbt.fail("could not evict generated Activity options Workflow")
	}
	destroy_process_result(&evicted)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed := process_activation(runtime, &activation)
	defer destroy_process_result(&replayed)
	if replayed.error.message != nil || !bytes.equal(initial.completion[:], replayed.completion[:]) {
		return pbt.fail("generated Activity options replay changed command bytes")
	}
	return pbt.pass()
}

@(test)
activity_options_replay_for_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Activity options survive generated command encoding and replay",
		activity_options_replay_property,
		{num_tests = 500, seed = 0x4143_545f_4f50_5453},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Generated_Child_Options_Input :: struct {
	workflow_id:          string,
	task_queue:           string,
	execution_timeout_ms: int,
	run_timeout_ms:       int,
	task_timeout_ms:      int,
	retry_initial_ms:     int,
	retry_maximum_ms:     int,
	retry_attempts:       int,
	cancellation_type:    int,
	parent_close_policy:  int,
}

generated_child_options_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Generated_Child_Options_Input,
) -> string {
	cancellation_type := workflow.Child_Workflow_Cancellation_Type.Wait_Cancellation_Completed
	switch input.cancellation_type {
	case 1:
		cancellation_type = .Abandon
	case 2:
		cancellation_type = .Try_Cancel
	case 3:
		cancellation_type = .Wait_Cancellation_Requested
	}
	parent_close_policy := workflow.Parent_Close_Policy.Unspecified
	switch input.parent_close_policy {
	case 1:
		parent_close_policy = .Terminate
	case 2:
		parent_close_policy = .Abandon
	case 3:
		parent_close_policy = .Request_Cancel
	}
	non_retryable_types := [?]string{"GeneratedChildPermanentFailure"}
	retry_policy := workflow.Retry_Policy{
		initial_interval = time.Duration(input.retry_initial_ms) * time.Millisecond,
		backoff_coefficient = 2,
		maximum_interval = time.Duration(input.retry_maximum_ms) * time.Millisecond,
		maximum_attempts = i32(input.retry_attempts),
		non_retryable_error_types = non_retryable_types[:],
	}
	future := workflow.start_child_workflow_json(
		ctx,
		"generated_child_workflow",
		input.workflow_id,
		{
			workflow_id = input.workflow_id,
			task_queue = input.task_queue,
			workflow_execution_timeout = time.Duration(input.execution_timeout_ms) * time.Millisecond,
			workflow_run_timeout = time.Duration(input.run_timeout_ms) * time.Millisecond,
			workflow_task_timeout = time.Duration(input.task_timeout_ms) * time.Millisecond,
			retry_policy = &retry_policy,
			cancellation_type = cancellation_type,
			parent_close_policy = parent_close_policy,
		},
	)
	_ = workflow.await_child_workflow_start(ctx, future)
	return "started"
}

generated_child_options_match :: proc(
	data: []u8,
	expected: ^Generated_Child_Options_Input,
) -> bool {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return false
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion)
	context.allocator = previous_allocator
	if !decoded {
		return false
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil || len(status.value.commands) != 1 {
			return false
		}
		#partial switch variant in status.value.commands[0].variant {
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Child_Workflow_Execution:
			if variant.value == nil || variant.value.retry_policy == nil {
				return false
			}
			start := variant.value
			cancellation_type := proto.Coresdk_Child_Workflow_ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED
			switch expected.cancellation_type {
			case 1:
				cancellation_type = .ABANDON
			case 2:
				cancellation_type = .TRY_CANCEL
			case 3:
				cancellation_type = .WAIT_CANCELLATION_REQUESTED
			}
			parent_close_policy := proto.Coresdk_Child_Workflow_ParentClosePolicy.PARENT_CLOSE_POLICY_UNSPECIFIED
			switch expected.parent_close_policy {
			case 1:
				parent_close_policy = .PARENT_CLOSE_POLICY_TERMINATE
			case 2:
				parent_close_policy = .PARENT_CLOSE_POLICY_ABANDON
			case 3:
				parent_close_policy = .PARENT_CLOSE_POLICY_REQUEST_CANCEL
			}
			policy := start.retry_policy
			return start.seq == 1 && start.workflow_id == expected.workflow_id &&
				start.workflow_type == "generated_child_workflow" &&
				start.task_queue == expected.task_queue && len(start.input) == 1 &&
				pbt_proto_duration(start.workflow_execution_timeout) == time.Duration(expected.execution_timeout_ms) * time.Millisecond &&
				pbt_proto_duration(start.workflow_run_timeout) == time.Duration(expected.run_timeout_ms) * time.Millisecond &&
				pbt_proto_duration(start.workflow_task_timeout) == time.Duration(expected.task_timeout_ms) * time.Millisecond &&
				pbt_proto_duration(policy.initial_interval) == time.Duration(expected.retry_initial_ms) * time.Millisecond &&
				policy.backoff_coefficient == 2 &&
				pbt_proto_duration(policy.maximum_interval) == time.Duration(expected.retry_maximum_ms) * time.Millisecond &&
				policy.maximum_attempts == i32(expected.retry_attempts) &&
				len(policy.non_retryable_error_types) == 1 &&
				policy.non_retryable_error_types[0] == "GeneratedChildPermanentFailure" &&
				start.cancellation_type == cancellation_type &&
				start.parent_close_policy == parent_close_policy
		}
	}
	return false
}

child_options_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	execution_timeout_ms := pbt.draw(t, pbt.int_range(1000, 120_000))
	run_timeout_ms := pbt.draw(t, pbt.int_range(1, execution_timeout_ms))
	task_timeout_ms := pbt.draw(t, pbt.int_range(1, run_timeout_ms))
	retry_initial_ms := pbt.draw(t, pbt.int_range(1, 10_000))
	retry_maximum_ms := pbt.draw(t, pbt.int_range(retry_initial_ms, 120_000))
	input_value := Generated_Child_Options_Input{
		workflow_id = pbt.draw(t, pbt.string_ascii(1, 32)),
		task_queue = pbt.draw(t, pbt.string_ascii(1, 32)),
		execution_timeout_ms = execution_timeout_ms,
		run_timeout_ms = run_timeout_ms,
		task_timeout_ms = task_timeout_ms,
		retry_initial_ms = retry_initial_ms,
		retry_maximum_ms = retry_maximum_ms,
		retry_attempts = pbt.draw(t, pbt.int_range(0, 20)),
		cancellation_type = pbt.draw(t, pbt.int_range(0, 3)),
		parent_close_policy = pbt.draw(t, pbt.int_range(0, 3)),
	}
	pbt.cover(t, input_value.cancellation_type == 0, 15.0, "wait cancellation completed")
	pbt.cover(t, input_value.cancellation_type == 1, 15.0, "abandon child")
	pbt.cover(t, input_value.cancellation_type == 2, 15.0, "try cancel child")
	pbt.cover(t, input_value.cancellation_type == 3, 15.0, "wait cancellation requested")
	pbt.cover(t, input_value.parent_close_policy == 0, 15.0, "server parent-close default")
	pbt.cover(t, input_value.parent_close_policy == 3, 15.0, "request cancel on parent close")

	owned_queue, queue_error := strings.clone("pbt-child-workflow-queue")
	if queue_error != nil {
		return pbt.error("could not allocate Child Workflow options queue")
	}
	sdk_worker := worker.Worker{
		task_queue = owned_queue,
		workflow_registrations = make([dynamic]worker.Workflow_Registration),
		lifecycle = .Configuring,
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}
	register_error := worker.register_json_workflow(
		&sdk_worker,
		"generated_child_options_workflow",
		generated_child_options_workflow,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register Child Workflow options Workflow")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input, encode_error := codec.encode_json(input_value)
	if encode_error != .None {
		return pbt.error("could not encode generated Child Workflow options")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_child_options_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-child-options-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_activation(runtime, &activation)
	defer destroy_process_result(&initial)
	if initial.error.message != nil || !initial.started_child_workflow ||
	   !generated_child_options_match(initial.completion[:], &input_value) {
		return pbt.fail("generated Child Workflow options changed across command encoding")
	}

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_activation(runtime, &activation)
	if evicted.error.message != nil || !evicted.evicted {
		destroy_process_result(&evicted)
		return pbt.fail("could not evict generated Child Workflow options Workflow")
	}
	destroy_process_result(&evicted)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed := process_activation(runtime, &activation)
	defer destroy_process_result(&replayed)
	if replayed.error.message != nil || !bytes.equal(initial.completion[:], replayed.completion[:]) {
		return pbt.fail("generated Child Workflow options replay changed command bytes")
	}
	return pbt.pass()
}

@(test)
child_workflow_options_replay_for_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Child Workflow options survive generated command encoding and replay",
		child_options_replay_property,
		{num_tests = 500, seed = 0x4348_494c_445f_4f50},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

race_workflow :: proc(ctx: ^workflow.Fiber_Context, input: string) -> string {
	_ = input
	result := workflow.await_signal_or_timeout(ctx, "approval", 30 * time.Second)
	if result.status == .Timed_Out {
		return "timeout"
	}
	return result.value
}

process_activation :: proc(
	runtime: ^workflow.Fiber_Runtime,
	activation: ^proto.Coresdk_Workflow_Activation_WorkflowActivation,
) -> workflow.Process_Result {
	encoded := proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(activation)
	defer delete(encoded)
	return workflow.process_fiber_activation(runtime, encoded[:])
}

destroy_process_result :: proc(result: ^workflow.Process_Result) {
	delete(result.completion)
	workflow.error_free(&result.error)
	result^ = {}
}

append_race_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	event_case: int,
	signal: ^proto.Coresdk_Workflow_Activation_SignalWorkflow,
	timer: ^proto.Coresdk_Workflow_Activation_FireTimer,
	cancel: ^proto.Coresdk_Workflow_Activation_CancelWorkflow,
) {
	signal_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
			value = signal,
		},
	}
	timer_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{
			value = timer,
		},
	}
	cancel_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
			value = cancel,
		},
	}
	switch event_case {
	case 0:
		append(jobs, signal_job)
	case 1:
		append(jobs, timer_job)
	case 2:
		append(jobs, cancel_job)
	case 3:
		append(jobs, signal_job, timer_job)
	case 4:
		append(jobs, signal_job, cancel_job)
	case 5:
		append(jobs, timer_job, cancel_job)
	case 6:
		append(jobs, cancel_job, timer_job)
	case 7:
		append(jobs, signal_job, cancel_job, timer_job)
	case 8:
		append(jobs, signal_job, timer_job, cancel_job)
	}
}

race_case_has_signal :: proc(event_case: int) -> bool {
	return event_case == 0 || event_case == 3 || event_case == 4 || event_case == 7 || event_case == 8
}

race_case_has_timer :: proc(event_case: int) -> bool {
	return event_case == 1 || event_case == 3 || event_case == 5 || event_case == 6 || event_case == 7 || event_case == 8
}

race_case_has_cancel :: proc(event_case: int) -> bool {
	return event_case == 2 || event_case == 4 || event_case == 5 || event_case == 6 || event_case == 7 || event_case == 8
}

workflow_race_replays_deterministically :: proc(t: ^pbt.T) -> pbt.Result {
	event_case := pbt.draw(t, pbt.int_range(0, 8))
	case_names := [?]string{
		"signal",
		"timer",
		"cancel",
		"signal+timer",
		"signal+cancel",
		"timer+cancel",
		"cancel+timer",
		"signal+cancel+timer",
		"signal+timer+cancel",
	}
	for case_name, index in case_names {
		pbt.cover(t, event_case == index, 5.0, case_name)
	}

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_race_workflow",
		"test-queue",
		race_workflow,
	)
	if init_error.message != nil {
		workflow.error_free(&init_error)
		return pbt.error("could not initialize Workflow runtime")
	}
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"input"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_race_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-race-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_activation(&runtime, &activation)
	defer destroy_process_result(&initial)
	if initial.error.message != nil || !initial.started_timer {
		return pbt.fail("initial activation did not start the race timer")
	}

	signal_payload := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"approved"`)}
	defer delete(signal_payload.metadata)
	append(
		&signal_payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "approval"}
	defer delete(signal.input)
	append(&signal.input, signal_payload)
	timer := proto.Coresdk_Workflow_Activation_FireTimer{seq = 1}
	cancel := proto.Coresdk_Workflow_Activation_CancelWorkflow{reason = "generated cancellation"}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append_race_jobs(&activation.jobs, event_case, &signal, &timer, &cancel)
	decisive := process_activation(&runtime, &activation)
	defer destroy_process_result(&decisive)
	if decisive.error.message != nil || !decisive.completed {
		return pbt.fail("generated decisive activation did not complete")
	}

	has_signal := race_case_has_signal(event_case)
	has_timer := race_case_has_timer(event_case)
	has_cancel := race_case_has_cancel(event_case)
	if decisive.received_signal != has_signal || decisive.received_cancellation != has_cancel {
		return pbt.fail("runtime did not report the generated activation jobs")
	}
	if decisive.canceled_workflow != has_cancel {
		return pbt.fail("Workflow cancellation did not match the generated model")
	}
	expected_timer_cancel := !has_timer && (has_signal || has_cancel)
	if decisive.canceled_timer != expected_timer_cancel {
		return pbt.fail("timer cancellation did not match the generated winner")
	}
	decisive_completion := bytes.clone(decisive.completion[:])
	defer delete(decisive_completion)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_activation(&runtime, &activation)
	defer destroy_process_result(&evicted)
	if evicted.error.message != nil || !evicted.evicted {
		return pbt.fail("generated Workflow could not be evicted")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replay_initial := process_activation(&runtime, &activation)
	defer destroy_process_result(&replay_initial)
	if replay_initial.error.message != nil || !replay_initial.is_replaying {
		return pbt.fail("generated Workflow could not initialize during replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append_race_jobs(&activation.jobs, event_case, &signal, &timer, &cancel)
	replayed := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed)
	if replayed.error.message != nil || !replayed.is_replaying {
		return pbt.fail("generated decisive activation failed during replay")
	}
	if !bytes.equal(decisive_completion, replayed.completion[:]) {
		return pbt.fail("generated activation produced different completion bytes on replay")
	}
	return pbt.pass()
}

@(test)
workflow_event_races_are_deterministic :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow event races replay deterministically",
		workflow_race_replays_deterministically,
		{num_tests = 500, seed = 0x5445_4d50_4f52_414c},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

activity_token_routing_preserves_cancellation :: proc(t: ^pbt.T) -> pbt.Result {
	count := pbt.draw(t, pbt.int_range(1, 10))
	target := pbt.draw(t, pbt.int_range(0, count - 1))
	reason_value := pbt.draw(t, pbt.int_range(1, 6))
	heartbeats_before := pbt.draw(t, pbt.int_range(0, 8))
	pbt.cover(t, target == 0, 10.0, "first task token")
	pbt.cover(t, target == count - 1, 10.0, "last task token")
	pbt.cover(t, count >= 8, 15.0, "large in-flight set")

	tokens: [10][2]u8
	executions: [10]worker.Activity_Execution
	routes: [10]^worker.Activity_Execution
	captures: [10]Activity_Heartbeat_Capture
	for index in 0 ..< count {
		tokens[index] = {u8(index), u8(index + 0xa5)}
		executions[index].task_token = tokens[index][:]
		activity.context_init(
			&executions[index].ctx,
			"generated-activity",
			u32(index + 1),
			&captures[index],
			capture_activity_heartbeat,
		)
		routes[index] = &executions[index]
	}

	for _ in 0 ..< heartbeats_before {
		if activity.heartbeat(&executions[target].ctx, "progress") != .Recorded {
			return pbt.fail("heartbeat failed before generated cancellation")
		}
	}
	found, ok := worker.find_activity_execution(routes[:count], tokens[target][:])
	if !ok || found != &executions[target] {
		return pbt.fail("task token routed to the wrong Activity execution")
	}
	reason := activity.Cancel_Reason(reason_value)
	activity.context_request_cancellation(&found.ctx, reason)

	for &execution, index in executions[:count] {
		if index == target {
			if !activity.is_cancellation_requested(&execution.ctx) ||
			   activity.cancellation_reason(&execution.ctx) != reason {
				return pbt.fail("target Activity lost its cancellation state")
			}
		} else if activity.is_cancellation_requested(&execution.ctx) {
			return pbt.fail("cancellation leaked to another task token")
		}
	}
	if activity.heartbeat(&found.ctx, "after cancellation") != .Cancellation_Requested {
		return pbt.fail("heartbeat did not expose generated cancellation")
	}
	if captures[target].calls != heartbeats_before {
		return pbt.fail("heartbeat transport ran after cancellation was visible")
	}
	return pbt.pass()
}

@(test)
activity_cancel_routing_is_token_local :: proc(t: ^testing.T) {
	result := pbt.check(
		"Activity cancellation remains local to its task token",
		activity_token_routing_preserves_cancellation,
		{num_tests = 500, seed = 0x4143_5449_5649_5459},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

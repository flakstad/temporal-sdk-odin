package workflow_pbt_test

import "core:bytes"
import "core:fmt"
import "core:mem/virtual"
import "core:strings"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import activity "../temporal/activity"
import temporal_client "../temporal/client"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import temporal_failure "../temporal/failure"
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

generated_failure_kind :: proc(value: int) -> temporal_failure.Kind {
	switch value {
	case 0: return .Application
	case 1: return .Timeout
	case 2: return .Canceled
	case 3: return .Terminated
	case 4: return .Server
	case 5: return .Reset_Workflow
	case 6: return .Activity
	case 7: return .Child_Workflow
	case 8: return .Nexus_Operation
	case 9: return .Nexus_Handler
	}
	return .Unknown
}

recursive_failure_round_trip_property :: proc(t: ^pbt.T) -> pbt.Result {
	depth := pbt.draw(t, pbt.int_range(1, 32))
	message := pbt.draw(t, pbt.string_ascii(0, 40))
	pbt.cover(t, depth == 1, 2.0, "single failure")
	pbt.cover(t, depth >= 24, 20.0, "deep failure chain")
	pbt.cover(t, len(message) == 0, 1.0, "empty message")

	failures: [32]proto.Temporal_Api_Failure_V1_Failure
	application: [32]proto.Temporal_Api_Failure_V1_ApplicationFailureInfo
	timeout: [32]proto.Temporal_Api_Failure_V1_TimeoutFailureInfo
	canceled: [32]proto.Temporal_Api_Failure_V1_CanceledFailureInfo
	terminated: [32]proto.Temporal_Api_Failure_V1_TerminatedFailureInfo
	server: [32]proto.Temporal_Api_Failure_V1_ServerFailureInfo
	reset: [32]proto.Temporal_Api_Failure_V1_ResetWorkflowFailureInfo
	activity: [32]proto.Temporal_Api_Failure_V1_ActivityFailureInfo
	child: [32]proto.Temporal_Api_Failure_V1_ChildWorkflowExecutionFailureInfo
	nexus_operation: [32]proto.Temporal_Api_Failure_V1_NexusOperationFailureInfo
	nexus_handler: [32]proto.Temporal_Api_Failure_V1_NexusHandlerFailureInfo
	kinds: [32]int

	for index in 0..<depth {
		kind := pbt.draw(t, pbt.int_range(0, 9))
		kinds[index] = kind
		failures[index].message = message
		switch kind {
		case 0:
			application[index].type = "GeneratedApplication"
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{value = &application[index]}
		case 1:
			timeout[index].timeout_type = .TIMEOUT_TYPE_START_TO_CLOSE
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Timeout_Failure_Info{value = &timeout[index]}
		case 2:
			canceled[index].identity = "generated"
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info{value = &canceled[index]}
		case 3:
			terminated[index].identity = "generated"
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Terminated_Failure_Info{value = &terminated[index]}
		case 4:
			server[index].non_retryable = index % 2 == 0
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Server_Failure_Info{value = &server[index]}
		case 5:
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Reset_Workflow_Failure_Info{value = &reset[index]}
		case 6:
			activity[index].activity_id = "generated-activity"
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Activity_Failure_Info{value = &activity[index]}
		case 7:
			child[index].namespace = "default"
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Child_Workflow_Execution_Failure_Info{value = &child[index]}
		case 8:
			nexus_operation[index].operation = "generated-operation"
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Nexus_Operation_Execution_Failure_Info{value = &nexus_operation[index]}
		case 9:
			nexus_handler[index].type = "GeneratedHandler"
			failures[index].failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Nexus_Handler_Failure_Info{value = &nexus_handler[index]}
		}
		if index + 1 < depth {
			failures[index].cause = &failures[index + 1]
		}
	}

	encoded := proto.encode_Temporal_Api_Failure_V1_Failure(&failures[0])
	decoded, decode_error := temporal_failure.decode(encoded[:])
	delete(encoded)
	if decode_error != .None {
		return pbt.fail("generated Temporal Failure could not be decoded")
	}
	defer temporal_failure.failure_free(&decoded)
	current := &decoded
	for index in 0..<depth {
		if current == nil {
			return pbt.fail("decoded Temporal Failure chain was truncated")
		}
		if current.kind != generated_failure_kind(kinds[index]) || current.message != message {
			return pbt.fail("decoded Temporal Failure node changed")
		}
		current = current.cause
	}
	if current != nil {
		return pbt.fail("decoded Temporal Failure chain gained nodes")
	}
	return pbt.pass()
}

@(test)
recursive_temporal_failures_preserve_generated_chains :: proc(t: ^testing.T) {
	result := pbt.check(
		"recursive Temporal Failures preserve generated chains",
		recursive_failure_round_trip_property,
		{num_tests = 500, seed = 0x4641_494c_5552_4553},
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

generated_search_attribute_type :: proc(value: int) -> codec.Search_Attribute_Type {
	switch value {
	case 0: return .Bool
	case 1: return .Int
	case 2: return .Double
	case 3: return .Keyword
	case 4: return .Text
	case 5: return .Datetime
	case 6: return .Keyword_List
	}
	return .Keyword
}

generated_id_reuse_policy :: proc(value: int) -> temporal_client.Workflow_Id_Reuse_Policy {
	switch value {
	case 0: return .Unspecified
	case 1: return .Allow_Duplicate
	case 2: return .Allow_Duplicate_Failed_Only
	case 3: return .Reject_Duplicate
	case 4: return .Terminate_If_Running
	}
	return .Unspecified
}

generated_id_conflict_policy :: proc(value: int) -> temporal_client.Workflow_Id_Conflict_Policy {
	switch value {
	case 0: return .Unspecified
	case 1: return .Fail
	case 2: return .Use_Existing
	case 3: return .Terminate_Existing
	}
	return .Unspecified
}

client_start_metadata_wire_property :: proc(t: ^pbt.T) -> pbt.Result {
	header_key := pbt.draw(t, pbt.string_ascii(1, 24))
	memo_key := pbt.draw(t, pbt.string_ascii(1, 24))
	search_key := pbt.draw(t, pbt.string_ascii(1, 24))
	header_value := pbt.draw(t, pbt.string_ascii(0, 48))
	memo_value := pbt.draw(t, pbt.string_ascii(0, 48))
	search_value := pbt.draw(t, pbt.string_ascii(0, 48))
	static_summary := pbt.draw(t, pbt.string_ascii(0, 80))
	static_details := pbt.draw(t, pbt.string_ascii(0, 256))
	type_case := pbt.draw(t, pbt.int_range(0, 6))
	attribute_type := generated_search_attribute_type(type_case)
	reuse_case := pbt.draw(t, pbt.int_range(0, 4))
	conflict_case := pbt.draw(t, pbt.int_range(0, 3))
	if reuse_case == 4 {
		conflict_case = 0
	}
	id_reuse_policy := generated_id_reuse_policy(reuse_case)
	id_conflict_policy := generated_id_conflict_policy(conflict_case)
	start_delay_ms := pbt.draw(t, pbt.int_range(1, 10_000))
	retry_initial_ms := pbt.draw(t, pbt.int_range(1, 10_000))
	retry_maximum_ms := retry_initial_ms + pbt.draw(t, pbt.int_range(0, 100_000))
	retry_coefficient := f64(pbt.draw(t, pbt.int_range(1, 10)))
	retry_attempts := i32(pbt.draw(t, pbt.int_range(0, 20)))
	retry_error_type := pbt.draw(t, pbt.string_ascii(1, 24))
	priority_key := i32(pbt.draw(t, pbt.int_range(0, 5)))
	fairness_key := pbt.draw(t, pbt.string_ascii(0, 64))
	fairness_weight := f32(pbt.draw(t, pbt.int_range(0, 100_000))) / 100.0
	enable_eager_start := pbt.draw(t, pbt.boolean())
	pbt.cover(t, type_case == 0, 5.0, "Bool Search Attribute metadata")
	pbt.cover(t, type_case == 6, 5.0, "KeywordList Search Attribute metadata")
	pbt.cover(t, len(search_value) == 0, 2.0, "empty Search Attribute Payload")
	pbt.cover(t, reuse_case == 4, 5.0, "deprecated terminate-if-running reuse policy")
	pbt.cover(t, conflict_case == 3, 5.0, "terminate-existing conflict policy")
	pbt.cover(t, len(fairness_key) == 64, 1.0, "maximum-length fairness key")
	pbt.cover(t, enable_eager_start, 30.0, "eager Workflow start requested")

	metadata := temporal_client.Start_Metadata{}
	init_error := temporal_client.start_metadata_init(&metadata)
	if init_error.message != nil {
		temporal_client.error_free(&init_error)
		return pbt.error("could not initialize generated Client start metadata")
	}
	temporal_client.error_free(&init_error)
	defer temporal_client.start_metadata_free(&metadata)

	header := codec.payload_create(transmute([]u8)header_value)
	memo := codec.payload_create(transmute([]u8)memo_value)
	search := codec.payload_create(transmute([]u8)search_value)
	if codec.payload_add_metadata(&header, "encoding", transmute([]u8)string("binary/test")) != .None ||
	   codec.payload_add_metadata(&memo, "encoding", transmute([]u8)string("binary/test")) != .None ||
	   codec.payload_add_metadata(&search, "encoding", transmute([]u8)string("binary/test")) != .None {
		codec.payload_free(&header)
		codec.payload_free(&memo)
		codec.payload_free(&search)
		return pbt.error("could not prepare generated Client start Payloads")
	}
	header_error := temporal_client.start_metadata_add_header_payload(
		&metadata,
		header_key,
		codec.payload_view(&header),
	)
	memo_error := temporal_client.start_metadata_add_memo_payload(
		&metadata,
		memo_key,
		codec.payload_view(&memo),
	)
	search_error := temporal_client.start_metadata_add_search_attribute_payload(
		&metadata,
		search_key,
		attribute_type,
		codec.payload_view(&search),
	)
	summary_error := temporal_client.start_metadata_set_static_summary(&metadata, static_summary)
	details_error := temporal_client.start_metadata_set_static_details(&metadata, static_details)
	codec.payload_free(&header)
	codec.payload_free(&memo)
	codec.payload_free(&search)
	defer temporal_client.error_free(&header_error)
	defer temporal_client.error_free(&memo_error)
	defer temporal_client.error_free(&search_error)
	defer temporal_client.error_free(&summary_error)
	defer temporal_client.error_free(&details_error)
	if header_error.message != nil || memo_error.message != nil || search_error.message != nil ||
	   summary_error.message != nil || details_error.message != nil {
		return pbt.error("could not add generated Client start metadata")
	}

	input := codec.payload_create(transmute([]u8)string("input"))
	defer codec.payload_free(&input)
	retry_error_types := [?]string{retry_error_type}
	retry_policy := temporal_client.Retry_Policy{
		initial_interval = time.Duration(retry_initial_ms) * time.Millisecond,
		backoff_coefficient = retry_coefficient,
		maximum_interval = time.Duration(retry_maximum_ms) * time.Millisecond,
		maximum_attempts = retry_attempts,
		non_retryable_error_types = retry_error_types[:],
	}
	priority := temporal_client.Priority{
		priority_key = priority_key,
		fairness_key = fairness_key,
		fairness_weight = fairness_weight,
	}
	request_bytes, request_error := temporal_client.encode_start_workflow_request(
		"generated-namespace",
		"generated-workflow",
		codec.payload_view(&input),
		{
			id = "generated-workflow-id",
			task_queue = "generated-queue",
			request_id = "generated-request-id",
			id_reuse_policy = id_reuse_policy,
			id_conflict_policy = id_conflict_policy,
			retry_policy = &retry_policy,
			start_delay = time.Duration(start_delay_ms) * time.Millisecond,
			priority = &priority,
			metadata = &metadata,
			enable_eager_workflow_start = enable_eager_start,
		},
	)
	defer delete(request_bytes)
	defer temporal_client.error_free(&request_error)
	if request_error.message != nil {
		return pbt.fail("generated Client start request did not encode")
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize generated Client request arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	request := proto.Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest(
		request_bytes[:],
		&request,
	)
	context.allocator = previous_allocator
	if !decoded || request.header == nil || request.memo == nil ||
	   request.search_attributes == nil || request.user_metadata == nil ||
	   len(request.header.fields) != 1 ||
	   len(request.memo.fields) != 1 || len(request.search_attributes.indexed_fields) != 1 ||
	   request.retry_policy == nil || request.workflow_start_delay == nil || request.priority == nil {
		return pbt.fail("generated Client start metadata did not decode")
	}
	if request.request_eager_execution != enable_eager_start ||
	   i32(request.workflow_id_reuse_policy) != i32(reuse_case) ||
	   i32(request.workflow_id_conflict_policy) != i32(conflict_case) ||
	   pbt_proto_duration(request.workflow_start_delay) !=
	   time.Duration(start_delay_ms) * time.Millisecond ||
	   pbt_proto_duration(request.retry_policy.initial_interval) !=
	   time.Duration(retry_initial_ms) * time.Millisecond ||
	   request.retry_policy.backoff_coefficient != retry_coefficient ||
	   pbt_proto_duration(request.retry_policy.maximum_interval) !=
	   time.Duration(retry_maximum_ms) * time.Millisecond ||
	   request.retry_policy.maximum_attempts != retry_attempts ||
	   len(request.retry_policy.non_retryable_error_types) != 1 ||
	   request.retry_policy.non_retryable_error_types[0] != retry_error_type ||
	   request.priority.priority_key != priority_key ||
	   request.priority.fairness_key != fairness_key ||
	   request.priority.fairness_weight != fairness_weight {
		return pbt.fail("generated Client start options changed across protobuf encoding")
	}
	header_field := request.header.fields[0]
	memo_field := request.memo.fields[0]
	search_field := request.search_attributes.indexed_fields[0]
	if header_field.value == nil || memo_field.value == nil || search_field.value == nil ||
	   header_field.key != header_key || memo_field.key != memo_key || search_field.key != search_key ||
	   string(header_field.value.data) != header_value || string(memo_field.value.data) != memo_value ||
	   string(search_field.value.data) != search_value {
		return pbt.fail("generated Client start metadata changed across protobuf encoding")
	}
	type_name := ""
	for entry in search_field.value.metadata {
		if entry.key == "type" {
			type_name = string(entry.value)
		}
	}
	if type_name != codec.search_attribute_type_name(attribute_type) {
		return pbt.fail("generated Search Attribute type metadata changed")
	}
	if request.user_metadata.summary == nil || request.user_metadata.details == nil {
		return pbt.fail("generated Workflow user metadata was omitted")
	}
	owned_summary, summary_clone_error := proto.clone_payload(request.user_metadata.summary)
	owned_details, details_clone_error := proto.clone_payload(request.user_metadata.details)
	defer codec.payload_free(&owned_summary)
	defer codec.payload_free(&owned_details)
	if summary_clone_error != .None || details_clone_error != .None {
		return pbt.error("could not preserve generated Workflow user metadata")
	}
	decoded_summary: string
	decoded_details: string
	defer delete(decoded_summary)
	defer delete(decoded_details)
	if codec.decode_json(codec.payload_view(&owned_summary), &decoded_summary) != .None ||
	   codec.decode_json(codec.payload_view(&owned_details), &decoded_details) != .None ||
	   decoded_summary != static_summary || decoded_details != static_details {
		return pbt.fail("generated Workflow user metadata changed across protobuf encoding")
	}
	return pbt.pass()
}

@(test)
client_start_metadata_preserves_generated_payload_maps :: proc(t: ^testing.T) {
	result := pbt.check(
		"Client start metadata preserves generated Payload maps",
		client_start_metadata_wire_property,
		{num_tests = 500, seed = 0x5354_4152_544d_4554},
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
			failure_bytes := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
			preserved := workflow.preserve_activity_failure(&operation, failure_bytes[:])
			context.allocator = previous_allocator
			preserved_detail := 0
			detail_found, detail_error := workflow.activity_failure_detail_json(
				&operation.failure,
				0,
				&preserved_detail,
			)
			failure_tree := workflow.activity_failure_tree(&operation.failure)
			application_node := temporal_failure.find_kind(failure_tree, .Application)
			valid = valid && preserved && operation.failure.application.present &&
				failure_tree != nil && application_node != nil &&
				application_node.application.type == failure_type &&
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

client_query_header_wire_property :: proc(t: ^pbt.T) -> pbt.Result {
	header_key := pbt.draw(t, pbt.string_ascii(1, 24))
	header_value := pbt.draw(t, pbt.string_ascii(0, 96))
	query_type := pbt.draw(t, pbt.string_ascii(1, 32))
	reject_case := pbt.draw(t, pbt.int_range(0, 3))
	argument_count := pbt.draw(t, pbt.int_range(0, 12))
	pbt.cover(t, len(header_value) == 0, 1.0, "empty query header Payload")
	pbt.cover(t, reject_case == 2, 20.0, "reject query when not open")
	pbt.cover(t, argument_count == 0, 2.0, "zero Query arguments")
	pbt.cover(t, argument_count == 1, 2.0, "one Query argument")
	pbt.cover(t, argument_count >= 8, 20.0, "many Query arguments")

	headers := temporal_client.Headers{}
	init_error := temporal_client.headers_init(&headers)
	if init_error.message != nil {
		temporal_client.error_free(&init_error)
		return pbt.error("could not initialize generated query headers")
	}
	temporal_client.error_free(&init_error)
	defer temporal_client.headers_free(&headers)
	header := codec.payload_create(transmute([]u8)header_value)
	if codec.payload_add_metadata(
		&header,
		"encoding",
		transmute([]u8)string("binary/query-test"),
	) != .None {
		codec.payload_free(&header)
		return pbt.error("could not add generated query header metadata")
	}
	header_error := temporal_client.headers_add_payload(
		&headers,
		header_key,
		codec.payload_view(&header),
	)
	codec.payload_free(&header)
	defer temporal_client.error_free(&header_error)
	if header_error.message != nil {
		return pbt.error("could not preserve generated query header")
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
			return pbt.error("could not decorate generated Query argument")
		}
		input_views[index] = codec.payload_view(&inputs[index])
	}
	request_bytes, request_error := temporal_client.encode_query_workflow_request_payloads(
		"generated-namespace",
		"generated-workflow-id",
		"generated-run-id",
		query_type,
		input_views[:argument_count],
		{
			reject_condition = temporal_client.Query_Reject_Condition(reject_case),
			headers = &headers,
		},
	)
	defer delete(request_bytes)
	defer temporal_client.error_free(&request_error)
	if request_error.message != nil {
		return pbt.fail("generated query request did not encode")
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize generated query request arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	request := proto.Temporal_Api_Workflowservice_V1_QueryWorkflowRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_QueryWorkflowRequest(
		request_bytes[:],
		&request,
	)
	context.allocator = previous_allocator
	actual_argument_count := 0
	if request.query != nil && request.query.query_args != nil {
		actual_argument_count = len(request.query.query_args.payloads)
	}
	if !decoded || request.query == nil || request.query.header == nil ||
	   actual_argument_count != argument_count || len(request.query.header.fields) != 1 ||
	   request.query.header.fields[0].value == nil {
		return pbt.fail("generated query header did not decode")
	}
	field := request.query.header.fields[0]
	if request.query.query_type != query_type || field.key != header_key ||
	   string(field.value.data) != header_value ||
	   i32(request.query_reject_condition) != i32(reject_case) {
		return pbt.fail("generated query options changed across protobuf encoding")
	}
	encoding := ""
	for entry in field.value.metadata {
		if entry.key == "encoding" {
			encoding = string(entry.value)
		}
	}
	if encoding != "binary/query-test" {
		return pbt.fail("generated query header metadata changed across protobuf encoding")
	}
	for index in 0..<argument_count {
		argument := &request.query.query_args.payloads[index]
		if !bytes.equal(argument.data, transmute([]u8)input_values[index]) ||
		   len(argument.external_payloads) != 1 ||
		   argument.external_payloads[0].size_bytes != external_sizes[index] {
			return pbt.fail("generated Query argument order or external details changed")
		}
		metadata_value: []u8
		for entry in argument.metadata {
			if entry.key == "argument" {
				metadata_value = entry.value
				break
			}
		}
		if !bytes.equal(metadata_value, transmute([]u8)input_values[index]) {
			return pbt.fail("generated Query argument metadata changed")
		}
	}
	reencoded, reencode_error := temporal_client.encode_query_workflow_request_payloads(
		"generated-namespace",
		"generated-workflow-id",
		"generated-run-id",
		query_type,
		input_views[:argument_count],
		{
			reject_condition = temporal_client.Query_Reject_Condition(reject_case),
			headers = &headers,
		},
	)
	defer delete(reencoded)
	defer temporal_client.error_free(&reencode_error)
	if reencode_error.message != nil || !bytes.equal(request_bytes[:], reencoded[:]) {
		return pbt.fail("generated Query request bytes were not deterministic")
	}
	return pbt.pass()
}

@(test)
client_query_headers_preserve_generated_payloads :: proc(t: ^testing.T) {
	result := pbt.check(
		"Client Queries preserve ordered arguments, details, and headers",
		client_query_header_wire_property,
		{num_tests = 500, seed = 0x5155_4552_595f_4844},
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

Generated_Workflow_Properties_Input :: struct {
	memo_key:       string,
	memo_value:     string,
	search_key:     string,
	search_value:   string,
	search_is_text: bool,
	remove_memo:    bool,
	remove_search:  bool,
}

generated_workflow_properties :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Generated_Workflow_Properties_Input,
) -> bool {
	metadata, metadata_found := workflow.info(ctx)
	if !metadata_found || metadata.namespace != "generated-namespace" ||
	   metadata.task_queue != "generated-properties-queue" ||
	   metadata.workflow_id != "generated-properties-id" ||
	   metadata.run_id != "generated-properties-run" ||
	   metadata.workflow_type != "generated_workflow_properties" {
		return false
	}
	if !workflow.upsert_memo_json(ctx, input.memo_key, input.memo_value) {
		return false
	}
	attribute_type := workflow.Search_Attribute_Type.Keyword
	if input.search_is_text {
		attribute_type = .Text
	}
	if !workflow.upsert_search_attribute_json(
		ctx,
		input.search_key,
		attribute_type,
		input.search_value,
	) {
		return false
	}
	if input.remove_memo && !workflow.remove_memo(ctx, input.memo_key) {
		return false
	}
	if input.remove_search && !workflow.remove_search_attribute(ctx, input.search_key) {
		return false
	}
	memo_value: string
	memo_found, memo_error := workflow.memo_json(ctx, input.memo_key, &memo_value)
	search_value: string
	search_found, search_error := workflow.search_attribute_json(ctx, input.search_key, &search_value)
	return memo_error == .None && search_error == .None &&
	       memo_found == !input.remove_memo && search_found == !input.remove_search &&
	       (input.remove_memo || memo_value == input.memo_value) &&
	       (input.remove_search || search_value == input.search_value)
}

workflow_properties_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	input_value := Generated_Workflow_Properties_Input{
		memo_key = pbt.draw(t, pbt.string_ascii(1, 24)),
		memo_value = pbt.draw(t, pbt.string_ascii(0, 48)),
		search_key = pbt.draw(t, pbt.string_ascii(1, 24)),
		search_value = pbt.draw(t, pbt.string_ascii(0, 48)),
		search_is_text = pbt.draw(t, pbt.boolean()),
		remove_memo = pbt.draw(t, pbt.boolean()),
		remove_search = pbt.draw(t, pbt.boolean()),
	}
	pbt.cover(t, input_value.search_is_text, 30.0, "Text Search Attribute")
	pbt.cover(t, !input_value.search_is_text, 30.0, "Keyword Search Attribute")
	pbt.cover(t, input_value.remove_memo, 30.0, "memo removal")
	pbt.cover(t, input_value.remove_search, 30.0, "Search Attribute removal")
	pbt.cover(
		t,
		input_value.remove_memo && input_value.remove_search,
		15.0,
		"both properties removed",
	)

	owned_queue, queue_error := strings.clone("generated-properties-queue")
	if queue_error != nil {
		return pbt.error("could not allocate generated Workflow properties queue")
	}
	owned_namespace, namespace_error := strings.clone("generated-namespace")
	if namespace_error != nil {
		delete(owned_queue)
		return pbt.error("could not allocate generated Workflow properties namespace")
	}
	sdk_worker := worker.Worker{
		namespace = owned_namespace,
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
		"generated_workflow_properties",
		generated_workflow_properties,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register generated Workflow properties runtime")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input, encode_error := codec.encode_json(input_value)
	if encode_error != .None {
		return pbt.error("could not encode generated Workflow properties")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_workflow_properties",
		workflow_id = "generated-properties-id",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-properties-run",
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
	expected_memo_commands := 1
	if input_value.remove_memo {
		expected_memo_commands += 1
	}
	expected_search_commands := 1
	if input_value.remove_search {
		expected_search_commands += 1
	}
	if initial.error.message != nil || !initial.completed ||
	   initial.memo_upserts != expected_memo_commands ||
	   initial.search_attribute_upserts != expected_search_commands {
		return pbt.fail("generated Workflow property commands were incomplete")
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
		return pbt.fail("could not evict generated Workflow properties run")
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
		return pbt.fail("generated Workflow property replay changed command bytes")
	}
	return pbt.pass()
}

@(test)
workflow_properties_replay_for_generated_mutations :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow properties replay for generated mutations",
		workflow_properties_replay_property,
		{num_tests = 500, seed = 0x5052_4f50_4552_5459},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Generated_Activity_Options_Input :: struct {
	activity_id:              string,
	task_queue:               string,
	header_value:             string,
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
	headers := workflow.Headers{}
	if !workflow.headers_init(&headers) {
		return ""
	}
	defer workflow.headers_free(&headers)
	if !workflow.headers_add_json(&headers, "property-header", input.header_value) {
		return ""
	}
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
			headers = &headers,
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
			if len(schedule.headers) != 1 || schedule.headers[0].key != "property-header" ||
			   schedule.headers[0].value == nil {
				return false
			}
			owned_header, clone_error := proto.clone_payload(schedule.headers[0].value)
			if clone_error != .None {
				return false
			}
			defer codec.payload_free(&owned_header)
			decoded_header: string
			if codec.decode_json(codec.payload_view(&owned_header), &decoded_header) != .None {
				return false
			}
			defer delete(decoded_header)
			policy := schedule.retry_policy
			expected_cancellation := proto.Coresdk_Workflow_Commands_ActivityCancellationType.TRY_CANCEL
			switch expected.cancellation_type {
			case 1:
				expected_cancellation = .WAIT_CANCELLATION_COMPLETED
			case 2:
				expected_cancellation = .ABANDON
			}
			return decoded_header == expected.header_value &&
				schedule.activity_id == expected.activity_id &&
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
		header_value = pbt.draw(t, pbt.string_ascii(0, 64)),
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

Generated_Local_Activity_Options_Input :: struct {
	activity_id:                string,
	header_value:               string,
	schedule_to_close_ms:       int,
	schedule_to_start_ms:       int,
	start_to_close_ms:          int,
	retry_initial_ms:           int,
	retry_maximum_ms:           int,
	retry_maximum_attempts:     int,
	local_retry_threshold_ms:   int,
	cancellation_type:          int,
	include_arguments_in_marker: bool,
}

generated_local_activity_options_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Generated_Local_Activity_Options_Input,
) -> string {
	headers := workflow.Headers{}
	if !workflow.headers_init(&headers) {
		return ""
	}
	defer workflow.headers_free(&headers)
	if !workflow.headers_add_json(&headers, "property-header", input.header_value) {
		return ""
	}
	cancellation_type := workflow.Local_Activity_Cancellation_Type.Wait_Cancellation_Completed
	switch input.cancellation_type {
	case 1:
		cancellation_type = .Try_Cancel
	case 2:
		cancellation_type = .Abandon
	}
	retry_policy := workflow.Retry_Policy{
		initial_interval = time.Duration(input.retry_initial_ms) * time.Millisecond,
		backoff_coefficient = 1.5,
		maximum_interval = time.Duration(input.retry_maximum_ms) * time.Millisecond,
		maximum_attempts = i32(input.retry_maximum_attempts),
	}
	return workflow.execute_local_activity_json_with_options(
		ctx,
		"generated_local_activity",
		input.activity_id,
		string,
		{
			activity_id = input.activity_id,
			headers = &headers,
			schedule_to_close_timeout = time.Duration(input.schedule_to_close_ms) * time.Millisecond,
			schedule_to_start_timeout = time.Duration(input.schedule_to_start_ms) * time.Millisecond,
			start_to_close_timeout = time.Duration(input.start_to_close_ms) * time.Millisecond,
			retry_policy = &retry_policy,
			local_retry_threshold = time.Duration(input.local_retry_threshold_ms) * time.Millisecond,
			cancellation_type = cancellation_type,
			include_arguments_in_marker = input.include_arguments_in_marker,
		},
	)
}

generated_local_activity_options_match :: proc(
	data: []u8,
	expected: ^Generated_Local_Activity_Options_Input,
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
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Local_Activity:
			if variant.value == nil || variant.value.retry_policy == nil ||
			   len(variant.value.arguments) != 1 {
				return false
			}
			schedule := variant.value
			if len(schedule.headers) != 1 || schedule.headers[0].key != "property-header" ||
			   schedule.headers[0].value == nil {
				return false
			}
			owned_header, clone_error := proto.clone_payload(schedule.headers[0].value)
			if clone_error != .None {
				return false
			}
			defer codec.payload_free(&owned_header)
			decoded_header: string
			if codec.decode_json(codec.payload_view(&owned_header), &decoded_header) != .None {
				return false
			}
			defer delete(decoded_header)
			expected_cancellation := proto.Coresdk_Workflow_Commands_ActivityCancellationType.WAIT_CANCELLATION_COMPLETED
			switch expected.cancellation_type {
			case 1:
				expected_cancellation = .TRY_CANCEL
			case 2:
				expected_cancellation = .ABANDON
			}
			return decoded_header == expected.header_value &&
				schedule.seq == 1 && schedule.activity_id == expected.activity_id &&
				schedule.activity_type == "generated_local_activity" && schedule.attempt == 1 &&
				pbt_proto_duration(schedule.schedule_to_close_timeout) == time.Duration(expected.schedule_to_close_ms) * time.Millisecond &&
				pbt_proto_duration(schedule.schedule_to_start_timeout) == time.Duration(expected.schedule_to_start_ms) * time.Millisecond &&
				pbt_proto_duration(schedule.start_to_close_timeout) == time.Duration(expected.start_to_close_ms) * time.Millisecond &&
				pbt_proto_duration(schedule.retry_policy.initial_interval) == time.Duration(expected.retry_initial_ms) * time.Millisecond &&
				pbt_proto_duration(schedule.retry_policy.maximum_interval) == time.Duration(expected.retry_maximum_ms) * time.Millisecond &&
				schedule.retry_policy.backoff_coefficient == 1.5 &&
				schedule.retry_policy.maximum_attempts == i32(expected.retry_maximum_attempts) &&
				pbt_proto_duration(schedule.local_retry_threshold) == time.Duration(expected.local_retry_threshold_ms) * time.Millisecond &&
				schedule.cancellation_type == expected_cancellation &&
				schedule.include_arguments_in_marker == expected.include_arguments_in_marker
		}
	}
	return false
}

local_activity_options_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	schedule_to_close_ms := pbt.draw(t, pbt.int_range(100, 120_000))
	input_value := Generated_Local_Activity_Options_Input{
		activity_id = pbt.draw(t, pbt.string_ascii(1, 32)),
		header_value = pbt.draw(t, pbt.string_ascii(0, 64)),
		schedule_to_close_ms = schedule_to_close_ms,
		schedule_to_start_ms = pbt.draw(t, pbt.int_range(1, schedule_to_close_ms)),
		start_to_close_ms = pbt.draw(t, pbt.int_range(1, schedule_to_close_ms)),
		retry_initial_ms = pbt.draw(t, pbt.int_range(1, 10_000)),
		local_retry_threshold_ms = pbt.draw(t, pbt.int_range(1, 120_000)),
		retry_maximum_attempts = pbt.draw(t, pbt.int_range(0, 20)),
		cancellation_type = pbt.draw(t, pbt.int_range(0, 2)),
		include_arguments_in_marker = pbt.draw(t, pbt.boolean()),
	}
	input_value.retry_maximum_ms = pbt.draw(
		t,
		pbt.int_range(input_value.retry_initial_ms, 120_000),
	)
	pbt.cover(t, input_value.cancellation_type == 0, 20.0, "wait for cancellation")
	pbt.cover(t, input_value.cancellation_type == 1, 20.0, "try cancel")
	pbt.cover(t, input_value.cancellation_type == 2, 20.0, "abandon")
	pbt.cover(t, input_value.include_arguments_in_marker, 30.0, "arguments in marker")
	pbt.cover(t, input_value.retry_maximum_attempts == 0, 2.0, "unlimited retries")
	pbt.cover(t, input_value.retry_maximum_attempts > 1, 80.0, "multiple retry attempts")

	owned_queue, queue_error := strings.clone("pbt-local-activity-queue")
	if queue_error != nil {
		return pbt.error("could not allocate Local Activity Workflow queue")
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
		"generated_local_activity_options_workflow",
		generated_local_activity_options_workflow,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register Local Activity options Workflow")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input, encode_error := codec.encode_json(input_value)
	if encode_error != .None {
		return pbt.error("could not encode generated Local Activity options")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_local_activity_options_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-local-activity-run",
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
	if initial.error.message != nil || !initial.scheduled_local_activity ||
	   initial.scheduled_activity {
		return pbt.fail("generated Local Activity options did not schedule a Local Activity")
	}
	if !generated_local_activity_options_match(initial.completion[:], &input_value) {
		return pbt.fail("generated Local Activity options changed across command encoding")
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
		return pbt.fail("could not evict generated Local Activity Workflow")
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
	if replayed.error.message != nil ||
	   !bytes.equal(initial.completion[:], replayed.completion[:]) {
		return pbt.fail("generated Local Activity replay changed command bytes")
	}
	return pbt.pass()
}

@(test)
local_activity_options_replay_for_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Local Activity options survive generated command encoding and replay",
		local_activity_options_replay_property,
		{num_tests = 500, seed = 0x4c4f_4341_4c5f_4143},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

generated_local_activity_backoff_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> string {
	retry_policy := workflow.Retry_Policy{
		initial_interval = time.Millisecond,
		backoff_coefficient = 2,
		maximum_interval = time.Minute,
		maximum_attempts = 0,
	}
	return workflow.execute_local_activity_with_options(
		ctx,
		"generated-backoff-local",
		input,
		{
			schedule_to_close_timeout = 10 * time.Minute,
			start_to_close_timeout = time.Minute,
			retry_policy = &retry_policy,
			local_retry_threshold = time.Millisecond,
		},
	)
}

generated_local_activity_backoff_timer_matches :: proc(
	data: []u8,
	expected: time.Duration,
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
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Timer:
			return variant.value != nil && variant.value.seq == 1 &&
				pbt_proto_duration(variant.value.start_to_fire_timeout) == expected
		}
	}
	return false
}

generated_local_activity_retry_matches :: proc(
	data: []u8,
	attempt: u32,
	original_seconds: i64,
	original_nanos: i32,
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
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Local_Activity:
			schedule := variant.value
			return schedule != nil && schedule.seq == 2 && schedule.attempt == attempt &&
				schedule.original_schedule_time != nil &&
				schedule.original_schedule_time.seconds == original_seconds &&
				schedule.original_schedule_time.nanos == original_nanos
		}
	}
	return false
}

local_activity_backoff_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	backoff_ms := pbt.draw(t, pbt.int_range(1, 120_000))
	attempt := u32(pbt.draw(t, pbt.int_range(2, 20)))
	original_seconds := i64(pbt.draw(t, pbt.int_range(1_600_000_000, 1_800_000_000)))
	original_nanos := i32(pbt.draw(t, pbt.int_range(0, 999_999_999)))
	pbt.cover(t, backoff_ms < 12_000, 5.0, "short backoff")
	pbt.cover(t, backoff_ms > 60_000, 40.0, "long backoff")
	pbt.cover(t, attempt > 10, 40.0, "late retry attempt")

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"generated_local_activity_backoff_workflow",
		"pbt-local-backoff-queue",
		generated_local_activity_backoff_workflow,
	)
	if init_error.message != nil {
		workflow.error_free(&init_error)
		return pbt.error("could not initialize generated Local Activity backoff Workflow")
	}
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, encode_error := codec.encode_json("generated")
	if encode_error != .None {
		return pbt.error("could not encode generated Local Activity backoff input")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_local_activity_backoff_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-local-backoff-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(&activation.jobs, proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{value = &initialize},
	})
	initial := process_activation(&runtime, &activation)
	defer destroy_process_result(&initial)
	if initial.error.message != nil || !initial.scheduled_local_activity || initial.activity_seq != 1 {
		return pbt.fail("generated Local Activity initial command was invalid")
	}
	initial_bytes := bytes.clone(initial.completion[:])
	defer delete(initial_bytes)

	duration := proto.Google_Protobuf_Duration{
		seconds = i64(backoff_ms / 1000),
		nanos = i32(backoff_ms % 1000) * 1_000_000,
	}
	timestamp := proto.Google_Protobuf_Timestamp{
		seconds = original_seconds,
		nanos = original_nanos,
	}
	backoff := proto.Coresdk_Activity_Result_DoBackoff{
		attempt = attempt,
		backoff_duration = &duration,
		original_schedule_time = &timestamp,
	}
	resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Backoff{value = &backoff},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &resolution,
		is_local = true,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(&activation.jobs, proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{value = &resolve},
	})
	backing_off := process_activation(&runtime, &activation)
	defer destroy_process_result(&backing_off)
	expected_duration := time.Duration(backoff_ms) * time.Millisecond
	if backing_off.error.message != nil || !backing_off.started_timer ||
	   !generated_local_activity_backoff_timer_matches(backing_off.completion[:], expected_duration) {
		return pbt.fail("generated Local Activity backoff did not emit the expected timer")
	}
	backoff_bytes := bytes.clone(backing_off.completion[:])
	defer delete(backoff_bytes)

	fire := proto.Coresdk_Workflow_Activation_FireTimer{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 11
	append(&activation.jobs, proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{value = &fire},
	})
	retried := process_activation(&runtime, &activation)
	defer destroy_process_result(&retried)
	if retried.error.message != nil || !retried.scheduled_local_activity ||
	   retried.activity_seq != 2 || retried.waiting_activity_seq != 1 ||
	   !generated_local_activity_retry_matches(
		   retried.completion[:],
		   attempt,
		   original_seconds,
		   original_nanos,
	   ) {
		return pbt.fail("generated Local Activity retry command lost backoff metadata")
	}
	retry_bytes := bytes.clone(retried.completion[:])
	defer delete(retry_bytes)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(&activation.jobs, proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{value = &eviction},
	})
	evicted := process_activation(&runtime, &activation)
	if evicted.error.message != nil || !evicted.evicted {
		destroy_process_result(&evicted)
		return pbt.fail("could not evict generated Local Activity backoff Workflow")
	}
	destroy_process_result(&evicted)

	activation.is_replaying = true
	activation.history_length = 3
	delete(activation.jobs)
	activation.jobs = nil
	append(&activation.jobs, proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{value = &initialize},
	})
	replay_initial := process_activation(&runtime, &activation)
	defer destroy_process_result(&replay_initial)
	if replay_initial.error.message != nil || !bytes.equal(initial_bytes, replay_initial.completion[:]) {
		return pbt.fail("generated Local Activity initial replay changed command bytes")
	}

	activation.history_length = 7
	delete(activation.jobs)
	activation.jobs = nil
	append(&activation.jobs, proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{value = &resolve},
	})
	replay_backoff := process_activation(&runtime, &activation)
	defer destroy_process_result(&replay_backoff)
	if replay_backoff.error.message != nil || !bytes.equal(backoff_bytes, replay_backoff.completion[:]) {
		return pbt.fail("generated Local Activity backoff replay changed timer bytes")
	}

	activation.history_length = 11
	delete(activation.jobs)
	activation.jobs = nil
	append(&activation.jobs, proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{value = &fire},
	})
	replay_retry := process_activation(&runtime, &activation)
	defer destroy_process_result(&replay_retry)
	if replay_retry.error.message != nil || !bytes.equal(retry_bytes, replay_retry.completion[:]) {
		return pbt.fail("generated Local Activity retry replay changed command bytes")
	}
	return pbt.pass()
}

@(test)
local_activity_backoff_retries_replay_for_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Local Activity timer-backed retries preserve metadata and replay bytes",
		local_activity_backoff_replay_property,
		{num_tests = 500, seed = 0x4c41_5f42_4143_4b4f},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Generated_Child_Options_Input :: struct {
	workflow_id:          string,
	task_queue:           string,
	header_value:         string,
	argument_count:       int,
	argument_prefix:      string,
	execution_timeout_ms: int,
	run_timeout_ms:       int,
	task_timeout_ms:      int,
	retry_initial_ms:     int,
	retry_maximum_ms:     int,
	retry_attempts:       int,
	cancellation_type:    int,
	parent_close_policy:  int,
	signal_name:          string,
	signal_value:         string,
}

generated_child_options_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Generated_Child_Options_Input,
) -> string {
	headers := workflow.Headers{}
	if !workflow.headers_init(&headers) {
		return ""
	}
	defer workflow.headers_free(&headers)
	if !workflow.headers_add_json(&headers, "property-header", input.header_value) {
		return ""
	}
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
	arguments := make([dynamic]codec.Payload)
	defer {
		for &argument in arguments {
			codec.payload_free(&argument)
		}
		delete(arguments)
	}
	argument_views := make([dynamic]codec.Payload_View)
	defer delete(argument_views)
	for index in 0..<input.argument_count {
		value := fmt.aprintf("%s:%d", input.argument_prefix, index)
		argument, encode_error := codec.encode_json(value)
		delete(value)
		if encode_error != .None {
			return ""
		}
		ordinal := fmt.aprintf("%d", index)
		metadata_error := codec.payload_add_metadata(
			&argument,
			"generated-ordinal",
			transmute([]u8)ordinal,
		)
		delete(ordinal)
		if metadata_error != .None ||
		   codec.payload_add_external(&argument, i64(index * 101)) != .None {
			codec.payload_free(&argument)
			return ""
		}
		append(&arguments, argument)
	}
	for &argument in arguments {
		append(&argument_views, codec.payload_view(&argument))
	}
	future := workflow.start_child_workflow_payloads(
		ctx,
		"generated_child_workflow",
		argument_views[:],
		{
			workflow_id = input.workflow_id,
			task_queue = input.task_queue,
			headers = &headers,
			workflow_execution_timeout = time.Duration(input.execution_timeout_ms) * time.Millisecond,
			workflow_run_timeout = time.Duration(input.run_timeout_ms) * time.Millisecond,
			workflow_task_timeout = time.Duration(input.task_timeout_ms) * time.Millisecond,
			retry_policy = &retry_policy,
			cancellation_type = cancellation_type,
			parent_close_policy = parent_close_policy,
		},
	)
	start := workflow.await_child_workflow_start(ctx, future)
	if start.status != .Succeeded {
		return "start failed"
	}
	result := workflow.signal_child_workflow_payloads_with_options(
		ctx,
		future,
		input.signal_name,
		argument_views[:],
		{headers = &headers},
	)
	if result.status != .Succeeded {
		return "signal failed"
	}
	return "signaled"
}

generated_workflow_arguments_match :: proc(
	arguments: []proto.Temporal_Api_Common_V1_Payload,
	argument_count: int,
	argument_prefix: string,
) -> bool {
	if len(arguments) != argument_count {
		return false
	}
	for &argument, index in arguments {
		value := fmt.aprintf("%s:%d", argument_prefix, index)
		expected_payload, encode_error := codec.encode_json(value)
		delete(value)
		if encode_error != .None {
			return false
		}
		ordinal := fmt.aprintf("%d", index)
		metadata_matches := false
		for metadata in argument.metadata {
			if metadata.key == "generated-ordinal" {
				metadata_matches = string(metadata.value) == ordinal
				break
			}
		}
		delete(ordinal)
		argument_matches := bytes.equal(argument.data, expected_payload.data) &&
		                    metadata_matches && len(argument.external_payloads) == 1 &&
		                    argument.external_payloads[0].size_bytes == i64(index * 101)
		codec.payload_free(&expected_payload)
		if !argument_matches {
			return false
		}
	}
	return true
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
			if len(start.headers) != 1 || start.headers[0].key != "property-header" ||
			   start.headers[0].value == nil {
				return false
			}
			owned_header, clone_error := proto.clone_payload(start.headers[0].value)
			if clone_error != .None {
				return false
			}
			defer codec.payload_free(&owned_header)
			decoded_header: string
			if codec.decode_json(codec.payload_view(&owned_header), &decoded_header) != .None {
				return false
			}
			defer delete(decoded_header)
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
			if !generated_workflow_arguments_match(
				start.input[:],
				expected.argument_count,
				expected.argument_prefix,
			) {
				return false
			}
			return decoded_header == expected.header_value &&
				start.seq == 1 && start.workflow_id == expected.workflow_id &&
				start.workflow_type == "generated_child_workflow" &&
				start.task_queue == expected.task_queue &&
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

generated_child_signal_match :: proc(
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
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Signal_External_Workflow_Execution:
			if variant.value == nil || variant.value.seq != 1 ||
			   variant.value.signal_name != expected.signal_name ||
			   len(variant.value.headers) != 1 ||
			   variant.value.headers[0].key != "property-header" ||
			   variant.value.headers[0].value == nil {
				return false
			}
			owned_header, clone_error := proto.clone_payload(variant.value.headers[0].value)
			if clone_error != .None {
				return false
			}
			defer codec.payload_free(&owned_header)
			decoded_header: string
			if codec.decode_json(codec.payload_view(&owned_header), &decoded_header) != .None ||
			   decoded_header != expected.header_value {
				delete(decoded_header)
				return false
			}
			delete(decoded_header)
			#partial switch target in variant.value.target {
			case proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution_Target_Child_Workflow_Id:
				if target.value != expected.workflow_id {
					return false
				}
			case:
				return false
			}
			return generated_workflow_arguments_match(
				variant.value.args[:],
				expected.argument_count,
				expected.argument_prefix,
			)
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
		header_value = pbt.draw(t, pbt.string_ascii(0, 64)),
		argument_count = pbt.draw(t, pbt.int_range(0, 12)),
		argument_prefix = pbt.draw(t, pbt.string_ascii(0, 24)),
		execution_timeout_ms = execution_timeout_ms,
		run_timeout_ms = run_timeout_ms,
		task_timeout_ms = task_timeout_ms,
		retry_initial_ms = retry_initial_ms,
		retry_maximum_ms = retry_maximum_ms,
		retry_attempts = pbt.draw(t, pbt.int_range(0, 20)),
		cancellation_type = pbt.draw(t, pbt.int_range(0, 3)),
		parent_close_policy = pbt.draw(t, pbt.int_range(0, 3)),
		signal_name = pbt.draw(t, pbt.string_ascii(1, 32)),
		signal_value = pbt.draw(t, pbt.string_ascii(0, 64)),
	}
	pbt.cover(t, input_value.argument_count == 0, 3.0, "zero Child Workflow arguments")
	pbt.cover(t, input_value.argument_count == 1, 3.0, "one Child Workflow argument")
	pbt.cover(t, input_value.argument_count >= 8, 25.0, "many Child Workflow arguments")
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

	start_success := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartSuccess{
		run_id = "generated-child-run",
	}
	resolve_start := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded{
			value = &start_success,
		},
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	signaled := process_activation(runtime, &activation)
	defer destroy_process_result(&signaled)
	if signaled.error.message != nil || !signaled.signaled_child_workflow ||
	   !generated_child_signal_match(signaled.completion[:], &input_value) {
		return pbt.fail("generated Child Workflow signal changed across command encoding")
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
	activation.history_length = 3
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

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	replayed_signal := process_activation(runtime, &activation)
	defer destroy_process_result(&replayed_signal)
	if replayed_signal.error.message != nil ||
	   !bytes.equal(signaled.completion[:], replayed_signal.completion[:]) {
		return pbt.fail("generated Child Workflow signal replay changed command bytes")
	}
	return pbt.pass()
}

@(test)
child_workflow_options_replay_for_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Child Workflow options and signals survive generated command encoding and replay",
		child_options_replay_property,
		{num_tests = 500, seed = 0x4348_494c_445f_4f50},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Generated_External_Workflow_Input :: struct {
	namespace:   string,
	workflow_id: string,
	run_id:      string,
	signal_name: string,
	signal_value: string,
	argument_count: int,
	header_value: string,
	cancel_reason: string,
}

generated_external_workflow_operations :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Generated_External_Workflow_Input,
) -> string {
	headers := workflow.Headers{}
	if !workflow.headers_init(&headers) {
		return ""
	}
	defer workflow.headers_free(&headers)
	if !workflow.headers_add_json(&headers, "property-header", input.header_value) {
		return ""
	}
	execution := workflow.External_Workflow_Execution{
		namespace = input.namespace,
		workflow_id = input.workflow_id,
		run_id = input.run_id,
	}
	arguments := make([dynamic]codec.Payload)
	defer {
		for &argument in arguments {
			codec.payload_free(&argument)
		}
		delete(arguments)
	}
	argument_views := make([dynamic]codec.Payload_View)
	defer delete(argument_views)
	for index in 0..<input.argument_count {
		value := fmt.aprintf("%s:%d", input.signal_value, index)
		argument, encode_error := codec.encode_json(value)
		delete(value)
		if encode_error != .None {
			return ""
		}
		ordinal := fmt.aprintf("%d", index)
		metadata_error := codec.payload_add_metadata(
			&argument,
			"generated-ordinal",
			transmute([]u8)ordinal,
		)
		delete(ordinal)
		if metadata_error != .None ||
		   codec.payload_add_external(&argument, i64(index * 101)) != .None {
			codec.payload_free(&argument)
			return ""
		}
		append(&arguments, argument)
	}
	for &argument in arguments {
		append(&argument_views, codec.payload_view(&argument))
	}
	signal := workflow.signal_external_workflow_payloads_with_options(
		ctx,
		execution,
		input.signal_name,
		argument_views[:],
		{headers = &headers},
	)
	if signal.status != .Succeeded {
		return "signal failed"
	}
	cancel := workflow.request_cancel_external_workflow(ctx, execution, input.cancel_reason)
	if cancel.status != .Succeeded {
		return "cancel failed"
	}
	return "done"
}

generated_external_signal_match :: proc(
	data: []u8,
	expected: ^Generated_External_Workflow_Input,
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
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Signal_External_Workflow_Execution:
			if variant.value == nil || variant.value.seq != 1 ||
			   variant.value.signal_name != expected.signal_name ||
			   len(variant.value.headers) != 1 ||
			   variant.value.headers[0].key != "property-header" ||
			   variant.value.headers[0].value == nil {
				return false
			}
			owned_header, clone_error := proto.clone_payload(variant.value.headers[0].value)
			if clone_error != .None {
				return false
			}
			defer codec.payload_free(&owned_header)
			decoded_header: string
			if codec.decode_json(codec.payload_view(&owned_header), &decoded_header) != .None ||
			   decoded_header != expected.header_value {
				delete(decoded_header)
				return false
			}
			delete(decoded_header)
			#partial switch target in variant.value.target {
			case proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution_Target_Workflow_Execution:
				if target.value == nil || target.value.namespace != expected.namespace ||
				   target.value.workflow_id != expected.workflow_id || target.value.run_id != expected.run_id {
					return false
				}
			case:
				return false
			}
			return generated_workflow_arguments_match(
				variant.value.args[:],
				expected.argument_count,
				expected.signal_value,
			)
		}
	}
	return false
}

generated_external_cancel_match :: proc(
	data: []u8,
	expected: ^Generated_External_Workflow_Input,
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
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Request_Cancel_External_Workflow_Execution:
			request := variant.value
			return request != nil && request.seq == 1 && request.reason == expected.cancel_reason &&
				request.workflow_execution != nil &&
				request.workflow_execution.namespace == expected.namespace &&
				request.workflow_execution.workflow_id == expected.workflow_id &&
				request.workflow_execution.run_id == expected.run_id
		}
	}
	return false
}

external_workflow_operations_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	input_value := Generated_External_Workflow_Input{
		namespace = pbt.draw(t, pbt.string_ascii(1, 24)),
		workflow_id = pbt.draw(t, pbt.string_ascii(1, 48)),
		run_id = pbt.draw(t, pbt.string_ascii(0, 48)),
		signal_name = pbt.draw(t, pbt.string_ascii(1, 32)),
		signal_value = pbt.draw(t, pbt.string_ascii(0, 64)),
		argument_count = pbt.draw(t, pbt.int_range(0, 12)),
		header_value = pbt.draw(t, pbt.string_ascii(0, 64)),
		cancel_reason = pbt.draw(t, pbt.string_ascii(0, 64)),
	}
	pbt.cover(t, input_value.argument_count == 0, 3.0, "zero external Signal arguments")
	pbt.cover(t, input_value.argument_count == 1, 3.0, "one external Signal argument")
	pbt.cover(t, input_value.argument_count >= 8, 25.0, "many external Signal arguments")
	pbt.cover(t, len(input_value.run_id) == 0, 0.2, "current run target")
	pbt.cover(t, len(input_value.run_id) > 32, 15.0, "specific long run id")
	pbt.cover(t, len(input_value.cancel_reason) == 0, 0.2, "empty cancellation reason")

	owned_queue, queue_error := strings.clone("pbt-external-workflow-queue")
	if queue_error != nil {
		return pbt.error("could not allocate external Workflow queue")
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
		"generated_external_workflow_operations",
		generated_external_workflow_operations,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register external Workflow operation Workflow")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input, encode_error := codec.encode_json(input_value)
	if encode_error != .None {
		return pbt.error("could not encode generated external Workflow input")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_external_workflow_operations",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-external-workflow-run",
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
	if initial.error.message != nil || !initial.signaled_external_workflow ||
	   initial.signaled_child_workflow ||
	   !generated_external_signal_match(initial.completion[:], &input_value) {
		return pbt.fail("generated external Workflow signal changed across command encoding")
	}

	resolve_signal := proto.Coresdk_Workflow_Activation_ResolveSignalExternalWorkflow{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Signal_External_Workflow{
				value = &resolve_signal,
			},
		},
	)
	canceled := process_activation(runtime, &activation)
	defer destroy_process_result(&canceled)
	if canceled.error.message != nil || !canceled.requested_external_workflow_cancellation ||
	   !generated_external_cancel_match(canceled.completion[:], &input_value) {
		return pbt.fail("generated external Workflow cancellation changed across command encoding")
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
		return pbt.fail("could not evict generated external Workflow operation Workflow")
	}
	destroy_process_result(&evicted)

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
	replayed_signal := process_activation(runtime, &activation)
	defer destroy_process_result(&replayed_signal)
	if replayed_signal.error.message != nil ||
	   !bytes.equal(initial.completion[:], replayed_signal.completion[:]) {
		return pbt.fail("generated external Workflow signal replay changed command bytes")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Signal_External_Workflow{
				value = &resolve_signal,
			},
		},
	)
	replayed_cancel := process_activation(runtime, &activation)
	defer destroy_process_result(&replayed_cancel)
	if replayed_cancel.error.message != nil ||
	   !bytes.equal(canceled.completion[:], replayed_cancel.completion[:]) {
		return pbt.fail("generated external Workflow cancellation replay changed command bytes")
	}
	return pbt.pass()
}

@(test)
external_workflow_operations_replay_for_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"External Workflow signals and cancellations survive generated encoding and replay",
		external_workflow_operations_replay_property,
		{num_tests = 500, seed = 0x4558_5445_524e_414c},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

generated_activity_argument_matches :: proc(
	argument: ^proto.Temporal_Api_Common_V1_Payload,
	expected: ^codec.Payload,
	index: int,
) -> bool {
	if argument == nil || expected == nil || !bytes.equal(argument.data, expected.data) ||
	   len(argument.external_payloads) != 1 ||
	   argument.external_payloads[0].size_bytes != i64(index * 101) {
		return false
	}
	expected_ordinal := fmt.aprintf("%d", index)
	defer delete(expected_ordinal)
	for metadata in argument.metadata {
		if metadata.key == "generated-ordinal" {
			return string(metadata.value) == expected_ordinal
		}
	}
	return false
}

activity_argument_command_property :: proc(t: ^pbt.T) -> pbt.Result {
	argument_count := pbt.draw(t, pbt.int_range(0, 12))
	is_local := pbt.draw(t, pbt.boolean())
	prefix := pbt.draw(t, pbt.string_ascii(0, 24))
	pbt.cover(t, argument_count == 0, 3.0, "zero arguments")
	pbt.cover(t, argument_count == 1, 3.0, "one argument")
	pbt.cover(t, argument_count >= 8, 25.0, "many arguments")
	pbt.cover(t, is_local, 30.0, "Local Activity")

	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return pbt.error("could not initialize Activity argument arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	defer {context.allocator = previous_allocator}

	operation := workflow.Fiber_Activity_Operation{
		command_seq = 17,
		activity_id = "generated-activity-id",
		activity_type = "generated-activity",
		is_local = is_local,
		attempt = 1,
		options = {
			start_to_close_timeout = time.Second,
		},
		local_options = {
			start_to_close_timeout = time.Second,
		},
		inputs = make([dynamic]codec.Payload),
	}
	for index in 0..<argument_count {
		value := fmt.aprintf("%s:%d", prefix, index)
		payload, encode_error := codec.encode_json(value)
		if encode_error != .None {
			return pbt.error("could not encode generated Activity argument")
		}
		ordinal := fmt.aprintf("%d", index)
		if codec.payload_add_metadata(
			&payload,
			"generated-ordinal",
			transmute([]u8)ordinal,
		) != .None || codec.payload_add_external(&payload, i64(index * 101)) != .None {
			return pbt.error("could not decorate generated Activity argument")
		}
		append(&operation.inputs, payload)
	}

	first_result := workflow.Process_Result{}
	first_commands := make([dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand)
	workflow.append_fiber_schedule_command(
		&first_result,
		&first_commands,
		&operation,
		"generated-default-queue",
	)
	if len(first_commands) != 1 {
		return pbt.fail("Activity arguments did not produce exactly one command")
	}

	arguments_valid := false
	#partial switch variant in first_commands[0].variant {
	case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity:
		if !is_local && variant.value != nil &&
		   len(variant.value.arguments) == argument_count {
			arguments_valid = true
			for &argument, index in variant.value.arguments {
				if !generated_activity_argument_matches(
					&argument,
					&operation.inputs[index],
					index,
				) {
					arguments_valid = false
					break
				}
			}
		}
	case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Local_Activity:
		if is_local && variant.value != nil &&
		   len(variant.value.arguments) == argument_count {
			arguments_valid = true
			for &argument, index in variant.value.arguments {
				if !generated_activity_argument_matches(
					&argument,
					&operation.inputs[index],
					index,
				) {
					arguments_valid = false
					break
				}
			}
		}
	}
	if !arguments_valid {
		return pbt.fail("Activity argument order or payload details changed in command encoding")
	}

	first_bytes := proto.encode_Coresdk_Workflow_Commands_WorkflowCommand(&first_commands[0])
	second_result := workflow.Process_Result{}
	second_commands := make([dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand)
	workflow.append_fiber_schedule_command(
		&second_result,
		&second_commands,
		&operation,
		"generated-default-queue",
	)
	if len(second_commands) != 1 {
		return pbt.fail("repeated Activity scheduling did not produce a command")
	}
	second_bytes := proto.encode_Coresdk_Workflow_Commands_WorkflowCommand(&second_commands[0])
	if !bytes.equal(first_bytes[:], second_bytes[:]) {
		return pbt.fail("repeated Activity argument command encoding changed bytes")
	}
	return pbt.pass()
}

@(test)
activity_argument_lists_preserve_generated_payloads_and_order :: proc(t: ^testing.T) {
	result := pbt.check(
		"Activity argument lists preserve payloads, order, and deterministic command bytes",
		activity_argument_command_property,
		{num_tests = 500, seed = 0x4152_4755_4d45_4e54},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

workflow_start_argument_property :: proc(t: ^pbt.T) -> pbt.Result {
	argument_count := pbt.draw(t, pbt.int_range(0, 12))
	prefix := pbt.draw(t, pbt.string_ascii(0, 24))
	pbt.cover(t, argument_count == 0, 3.0, "zero arguments")
	pbt.cover(t, argument_count == 1, 3.0, "one argument")
	pbt.cover(t, argument_count >= 8, 25.0, "many arguments")

	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return pbt.error("could not initialize Workflow start argument arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	defer {context.allocator = previous_allocator}

	payloads := make([]codec.Payload, argument_count)
	views := make([]codec.Payload_View, argument_count)
	for index in 0..<argument_count {
		value := fmt.aprintf("%s:%d", prefix, index)
		payload, encode_error := codec.encode_json(value)
		if encode_error != .None {
			return pbt.error("could not encode generated Workflow start argument")
		}
		ordinal := fmt.aprintf("%d", index)
		if codec.payload_add_metadata(
			&payload,
			"generated-ordinal",
			transmute([]u8)ordinal,
		) != .None || codec.payload_add_external(&payload, i64(index * 101)) != .None {
			return pbt.error("could not decorate generated Workflow start argument")
		}
		payloads[index] = payload
		views[index] = codec.payload_view(&payloads[index])
	}
	options := temporal_client.Start_Workflow_Options{
		id = "generated-workflow-id",
		task_queue = "generated-workflow-queue",
		request_id = "generated-workflow-request",
	}
	first_bytes, first_error := temporal_client.encode_start_workflow_request_payloads(
		"default",
		"generated_workflow",
		views,
		options,
	)
	if first_error.message != nil {
		temporal_client.error_free(&first_error)
		return pbt.fail("generated Workflow start arguments did not encode")
	}
	request := proto.Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest{}
	if !proto.decode_Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest(
		first_bytes[:],
		&request,
	) || request.input == nil || len(request.input.payloads) != argument_count {
		return pbt.fail("generated Workflow start request changed argument count")
	}
	for &argument, index in request.input.payloads {
		if !generated_activity_argument_matches(&argument, &payloads[index], index) {
			return pbt.fail("Workflow start argument order or payload details changed")
		}
	}
	second_bytes, second_error := temporal_client.encode_start_workflow_request_payloads(
		"default",
		"generated_workflow",
		views,
		options,
	)
	if second_error.message != nil {
		temporal_client.error_free(&second_error)
		return pbt.fail("repeated Workflow start arguments did not encode")
	}
	if !bytes.equal(first_bytes[:], second_bytes[:]) {
		return pbt.fail("repeated Workflow start argument encoding changed bytes")
	}
	return pbt.pass()
}

@(test)
workflow_start_argument_lists_preserve_generated_payloads_and_order :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow start argument lists preserve payloads, order, and deterministic bytes",
		workflow_start_argument_property,
		{num_tests = 500, seed = 0x5746_5f41_5247_554d},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Generated_Continue_As_New_Input :: struct {
	value:               string,
	argument_count:      int,
	workflow_type:       string,
	task_queue:          string,
	run_timeout_ms:      int,
	task_timeout_ms:     int,
	retry_initial_ms:    int,
	retry_maximum_ms:    int,
	retry_attempts:      int,
	backoff_ms:          int,
}

generated_continue_as_new_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Generated_Continue_As_New_Input,
) -> string {
	retry_policy := workflow.Retry_Policy{
		initial_interval = time.Duration(input.retry_initial_ms) * time.Millisecond,
		backoff_coefficient = 2,
		maximum_interval = time.Duration(input.retry_maximum_ms) * time.Millisecond,
		maximum_attempts = i32(input.retry_attempts),
	}
	arguments := make([dynamic]codec.Payload)
	for index in 0..<input.argument_count {
		value := fmt.aprintf("%s:%d", input.value, index)
		argument, encode_error := codec.encode_json(value)
		delete(value)
		if encode_error != .None {
			return ""
		}
		ordinal := fmt.aprintf("%d", index)
		metadata_error := codec.payload_add_metadata(
			&argument,
			"generated-ordinal",
			transmute([]u8)ordinal,
		)
		delete(ordinal)
		if metadata_error != .None ||
		   codec.payload_add_external(&argument, i64(index * 101)) != .None {
			codec.payload_free(&argument)
			return ""
		}
		append(&arguments, argument)
	}
	argument_views := make([dynamic]codec.Payload_View)
	for &argument in arguments {
		append(&argument_views, codec.payload_view(&argument))
	}
	workflow.continue_as_new_payloads(
		ctx,
		argument_views[:],
		{
			workflow_type = input.workflow_type,
			task_queue = input.task_queue,
			workflow_run_timeout = time.Duration(input.run_timeout_ms) * time.Millisecond,
			workflow_task_timeout = time.Duration(input.task_timeout_ms) * time.Millisecond,
			retry_policy = &retry_policy,
			backoff_start_interval = time.Duration(input.backoff_ms) * time.Millisecond,
		},
	)
}

generated_continue_as_new_match :: proc(
	data: []u8,
	expected: ^Generated_Continue_As_New_Input,
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
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Continue_As_New_Workflow_Execution:
			command := variant.value
			if command == nil || command.retry_policy == nil {
				return false
			}
			expected_type := expected.workflow_type
			if len(expected_type) == 0 {
				expected_type = "generated_continue_as_new_workflow"
			}
			expected_queue := expected.task_queue
			if len(expected_queue) == 0 {
				expected_queue = "pbt-continue-as-new-queue"
			}
			if !generated_workflow_arguments_match(
				command.arguments[:],
				expected.argument_count,
				expected.value,
			) {
				return false
			}
			return command.workflow_type == expected_type && command.task_queue == expected_queue &&
				pbt_proto_duration(command.workflow_run_timeout) ==
					time.Duration(expected.run_timeout_ms) * time.Millisecond &&
				pbt_proto_duration(command.workflow_task_timeout) ==
					time.Duration(expected.task_timeout_ms) * time.Millisecond &&
				pbt_proto_duration(command.backoff_start_interval) ==
					time.Duration(expected.backoff_ms) * time.Millisecond &&
				pbt_proto_duration(command.retry_policy.initial_interval) ==
					time.Duration(expected.retry_initial_ms) * time.Millisecond &&
				pbt_proto_duration(command.retry_policy.maximum_interval) ==
					time.Duration(expected.retry_maximum_ms) * time.Millisecond &&
				command.retry_policy.backoff_coefficient == 2 &&
				command.retry_policy.maximum_attempts == i32(expected.retry_attempts)
		}
	}
	return false
}

continue_as_new_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	run_timeout_ms := pbt.draw(t, pbt.int_range(1, 120_000))
	task_timeout_ms := pbt.draw(t, pbt.int_range(1, run_timeout_ms))
	retry_initial_ms := pbt.draw(t, pbt.int_range(1, 10_000))
	retry_maximum_ms := pbt.draw(t, pbt.int_range(retry_initial_ms, 120_000))
	input_value := Generated_Continue_As_New_Input{
		value = pbt.draw(t, pbt.string_ascii(0, 64)),
		argument_count = pbt.draw(t, pbt.int_range(0, 12)),
		workflow_type = pbt.draw(t, pbt.string_ascii(0, 32)),
		task_queue = pbt.draw(t, pbt.string_ascii(0, 32)),
		run_timeout_ms = run_timeout_ms,
		task_timeout_ms = task_timeout_ms,
		retry_initial_ms = retry_initial_ms,
		retry_maximum_ms = retry_maximum_ms,
		retry_attempts = pbt.draw(t, pbt.int_range(0, 20)),
		backoff_ms = pbt.draw(t, pbt.int_range(0, 30_000)),
	}
	pbt.cover(t, input_value.argument_count == 0, 3.0, "zero Continue-As-New arguments")
	pbt.cover(t, input_value.argument_count == 1, 3.0, "one Continue-As-New argument")
	pbt.cover(t, input_value.argument_count >= 8, 25.0, "many Continue-As-New arguments")
	pbt.cover(t, len(input_value.workflow_type) == 0, 0.2, "reuse Workflow type")
	pbt.cover(t, len(input_value.task_queue) == 0, 0.2, "reuse task queue")

	owned_queue, queue_error := strings.clone("pbt-continue-as-new-queue")
	if queue_error != nil {
		return pbt.error("could not allocate Continue-As-New queue")
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
		"generated_continue_as_new_workflow",
		generated_continue_as_new_workflow,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register Continue-As-New Workflow")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input, encode_error := codec.encode_json(input_value)
	if encode_error != .None {
		return pbt.error("could not encode generated Continue-As-New input")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_continue_as_new_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-continue-as-new-run",
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
	if initial.error.message != nil || !initial.continued_as_new ||
	   !generated_continue_as_new_match(initial.completion[:], &input_value) {
		return pbt.fail("generated Continue-As-New options changed across command encoding")
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
		return pbt.fail("could not evict generated Continue-As-New Workflow")
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
	if replayed.error.message != nil || !replayed.continued_as_new ||
	   !bytes.equal(initial.completion[:], replayed.completion[:]) {
		return pbt.fail("generated Continue-As-New replay changed command bytes")
	}
	return pbt.pass()
}

@(test)
continue_as_new_replay_for_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Continue-As-New options survive generated command encoding and replay",
		continue_as_new_replay_property,
		{num_tests = 500, seed = 0x434f_4e54_494e_5545},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Generated_Patch_Input :: struct {
	patch_id:   string,
	deprecated: bool,
}

Generated_Patch_Output :: struct {
	first:  bool,
	second: bool,
}

generated_patch_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Generated_Patch_Input,
) -> Generated_Patch_Output {
	if input.deprecated {
		return {
			first = workflow.deprecate_patch(ctx, input.patch_id),
			second = workflow.deprecate_patch(ctx, input.patch_id),
		}
	}
	return {
		first = workflow.patched(ctx, input.patch_id),
		second = workflow.patched(ctx, input.patch_id),
	}
}

generated_patch_completion_matches :: proc(
	data: []u8,
	input: ^Generated_Patch_Input,
	expected_present: bool,
) -> bool {
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return false
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	defer {context.allocator = previous_allocator}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	if !proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion) {
		return false
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil || len(status.value.commands) != (expected_present ? 2 : 1) {
			return false
		}
		marker_count := 0
		completed := false
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Set_Patch_Marker:
				if variant.value == nil || variant.value.patch_id != input.patch_id ||
				   variant.value.deprecated != input.deprecated {
					return false
				}
				marker_count += 1
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution:
				if variant.value == nil || variant.value.result == nil {
					return false
				}
				owned, clone_error := proto.clone_payload(variant.value.result)
				if clone_error != .None {
					return false
				}
				output: Generated_Patch_Output
				decode_error := codec.decode_json(codec.payload_view(&owned), &output)
				codec.payload_free(&owned)
				if decode_error != .None ||
				   output.first != expected_present || output.second != expected_present {
					return false
				}
				completed = true
			}
		}
		return completed && marker_count == (expected_present ? 1 : 0)
	}
	return false
}

workflow_patch_truth_table_property :: proc(t: ^pbt.T) -> pbt.Result {
	replaying := pbt.draw(t, pbt.boolean())
	notified := pbt.draw(t, pbt.boolean())
	deprecated := pbt.draw(t, pbt.boolean())
	input_value := Generated_Patch_Input{
		patch_id = pbt.draw(t, pbt.string_ascii(1, 64)),
		deprecated = deprecated,
	}
	pbt.cover(t, !replaying, 20.0, "fresh history")
	pbt.cover(t, replaying && !notified, 15.0, "replay without marker")
	pbt.cover(t, replaying && notified, 15.0, "replay with marker")
	pbt.cover(t, deprecated, 30.0, "deprecated marker")

	owned_queue, queue_error := strings.clone("pbt-patch-queue")
	if queue_error != nil {
		return pbt.error("could not allocate patch queue")
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
		"generated_patch_workflow",
		generated_patch_workflow,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register generated patch Workflow")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input, encode_error := codec.encode_json(input_value)
	if encode_error != .None {
		return pbt.error("could not encode generated patch input")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_patch_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	notification := proto.Coresdk_Workflow_Activation_NotifyHasPatch{
		patch_id = input_value.patch_id,
	}
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-patch-run",
		is_replaying = replaying,
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
	if notified {
		append(
			&activation.jobs,
			proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
				variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Notify_Has_Patch{
					value = &notification,
				},
			},
		)
	}
	processed := process_activation(runtime, &activation)
	defer destroy_process_result(&processed)
	expected_present := !replaying || notified
	if processed.error.message != nil ||
	   processed.patch_markers_emitted != (expected_present ? 1 : 0) ||
	   processed.deprecated_patch_markers_emitted !=
		(expected_present && deprecated ? 1 : 0) ||
	   !generated_patch_completion_matches(
		processed.completion[:],
		&input_value,
		expected_present,
	) {
		return pbt.fail("generated patch case violated Core's patch truth table")
	}
	return pbt.pass()
}

@(test)
workflow_patch_truth_table_for_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow patches follow replay/notification/deprecation truth table",
		workflow_patch_truth_table_property,
		{num_tests = 500, seed = 0x5041_5443_485f_4f44},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Generated_Determinism_Output :: struct {
	time_nanoseconds: i64,
	history_length:   u32,
	first:            i64,
	second:           i64,
	fraction:         f64,
	uuid:             string,
}

generated_determinism_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> Generated_Determinism_Output {
	_ = input
	activation_time, present := workflow.now(ctx)
	result := Generated_Determinism_Output{}
	result.history_length = workflow.history_length(ctx)
	result.first = i64(workflow.random_u64(ctx) & 0x7fff_ffff_ffff_ffff)
	result.second = i64(workflow.random_u64(ctx) & 0x7fff_ffff_ffff_ffff)
	result.fraction = workflow.random_f64(ctx)
	result.uuid = workflow.uuid_v4(ctx)
	if present {
		result.time_nanoseconds = time.to_unix_nanoseconds(activation_time)
	}
	return result
}

splitmix64_next :: proc(state: ^u64) -> u64 {
	state^ += 0x9e3779b97f4a7c15
	value := state^
	value = (value ~ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ~ (value >> 27)) * 0x94d049bb133111eb
	return value ~ (value >> 31)
}

decode_determinism_completion :: proc(
	data: []u8,
) -> (output: Generated_Determinism_Output, ok: bool) {
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return {}, false
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	defer {context.allocator = previous_allocator}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	if !proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion) {
		return {}, false
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil || len(status.value.commands) != 1 {
			return {}, false
		}
		#partial switch variant in status.value.commands[0].variant {
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution:
			if variant.value == nil || variant.value.result == nil {
				return {}, false
			}
			owned, clone_error := proto.clone_payload(variant.value.result)
			if clone_error != .None {
				return {}, false
			}
			defer codec.payload_free(&owned)
			if codec.decode_json(codec.payload_view(&owned), &output) != .None {
				return {}, false
			}
			owned_uuid, allocation_error := strings.clone(output.uuid, previous_allocator)
			if allocation_error != nil {
				return {}, false
			}
			output.uuid = owned_uuid
			return output, true
		}
	}
	return {}, false
}

workflow_deterministic_primitives_property :: proc(t: ^pbt.T) -> pbt.Result {
	initial_seed := u64(pbt.draw(t, pbt.int_range(0, 2_000_000_000)))
	updated_seed := u64(pbt.draw(t, pbt.int_range(0, 2_000_000_000)))
	has_update := pbt.draw(t, pbt.boolean())
	seconds := i64(pbt.draw(t, pbt.int_range(1_000_000_000, 2_000_000_000)))
	nanos := i32(pbt.draw(t, pbt.int_range(0, 999_999_999)))
	history := u32(pbt.draw(t, pbt.int_range(1, 1_000_000)))
	pbt.cover(t, has_update, 30.0, "Core seed update")
	pbt.cover(t, !has_update, 30.0, "initial seed")

	owned_queue, queue_error := strings.clone("pbt-determinism-queue")
	if queue_error != nil {
		return pbt.error("could not allocate determinism queue")
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
		"generated_determinism_workflow",
		generated_determinism_workflow,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register generated determinism Workflow")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input, encode_error := codec.encode_json("input")
	if encode_error != .None {
		return pbt.error("could not encode determinism input")
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_determinism_workflow",
		randomness_seed = initial_seed,
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	seed_update := proto.Coresdk_Workflow_Activation_UpdateRandomSeed{
		randomness_seed = updated_seed,
	}
	timestamp := proto.Google_Protobuf_Timestamp{seconds = seconds, nanos = nanos}
	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-determinism-run",
		history_length = history,
		timestamp = &timestamp,
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
	if has_update {
		append(
			&activation.jobs,
			proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
				variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Update_Random_Seed{
					value = &seed_update,
				},
			},
		)
	}
	fresh := process_activation(runtime, &activation)
	defer destroy_process_result(&fresh)
	if fresh.error.message != nil || !fresh.completed {
		return pbt.fail("generated deterministic Workflow did not complete")
	}
	output, decoded := decode_determinism_completion(fresh.completion[:])
	defer delete(output.uuid)
	effective_seed := has_update ? updated_seed : initial_seed
	expected_state := effective_seed
	expected_first := i64(splitmix64_next(&expected_state) & 0x7fff_ffff_ffff_ffff)
	expected_second := i64(splitmix64_next(&expected_state) & 0x7fff_ffff_ffff_ffff)
	expected_fraction := f64(splitmix64_next(&expected_state) >> 11) *
		(1.0 / 9007199254740992.0)
	fraction_difference := output.fraction - expected_fraction
	if fraction_difference < 0 {
		fraction_difference = -fraction_difference
	}
	if !decoded || output.time_nanoseconds != seconds * i64(time.Second) + i64(nanos) ||
	   output.history_length != history || output.first != expected_first ||
	   output.second != expected_second || fraction_difference > 1e-15 ||
	   len(output.uuid) != 36 || output.uuid[14] != '4' {
		return pbt.fail(fmt.tprintf(
			"deterministic mismatch decoded=%v time=%d/%d history=%d/%d first=%d/%d second=%d/%d fraction=%v/%v uuid=%q",
			decoded,
			output.time_nanoseconds,
			seconds * i64(time.Second) + i64(nanos),
			output.history_length,
			history,
			output.first,
			expected_first,
			output.second,
			expected_second,
			output.fraction,
			expected_fraction,
			output.uuid,
		))
	}

	completion_bytes := bytes.clone(fresh.completion[:])
	defer delete(completion_bytes)
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
		return pbt.fail("could not evict generated deterministic Workflow")
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
	if has_update {
		append(
			&activation.jobs,
			proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
				variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Update_Random_Seed{
					value = &seed_update,
				},
			},
		)
	}
	replayed := process_activation(runtime, &activation)
	defer destroy_process_result(&replayed)
	if replayed.error.message != nil || !bytes.equal(completion_bytes, replayed.completion[:]) {
		return pbt.fail("generated deterministic primitives changed on fresh-stack replay")
	}
	return pbt.pass()
}

@(test)
workflow_deterministic_primitives_for_generated_activations :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow time and random primitives replay deterministically",
		workflow_deterministic_primitives_property,
		{num_tests = 500, seed = 0x5449_4d45_524e_4721},
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
	details := activity.Cancellation_Details{
		is_not_found = pbt.draw(t, pbt.boolean()),
		is_cancelled = pbt.draw(t, pbt.boolean()),
		is_paused = pbt.draw(t, pbt.boolean()),
		is_timed_out = pbt.draw(t, pbt.boolean()),
		is_worker_shutdown = pbt.draw(t, pbt.boolean()),
		is_reset = pbt.draw(t, pbt.boolean()),
	}
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
	activity.context_request_cancellation(&found.ctx, reason, details)

	for &execution, index in executions[:count] {
		if index == target {
			if !activity.is_cancellation_requested(&execution.ctx) ||
			   activity.cancellation_reason(&execution.ctx) != reason {
				return pbt.fail("target Activity lost its cancellation state")
			}
			actual_details, present := activity.cancellation_details(&execution.ctx)
			if !present || actual_details != details {
				return pbt.fail("target Activity lost generated cancellation detail flags")
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

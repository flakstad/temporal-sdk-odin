package workflow_pbt_test

import "core:bytes"
import "core:mem/virtual"
import "core:testing"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import proto "../temporal/core/proto"

nexus_task_and_completion_property :: proc(t: ^pbt.T) -> pbt.Result {
	task_token := pbt.draw(t, pbt.byte_array(1, 64))
	payload_data := pbt.draw(t, pbt.byte_array(0, 256))
	service := pbt.draw(t, pbt.string_ascii(1, 48))
	operation := pbt.draw(t, pbt.string_ascii(1, 48))
	request_id := pbt.draw(t, pbt.string_ascii(1, 48))
	header_value := pbt.draw(t, pbt.string_ascii(0, 96))
	callback_value := pbt.draw(t, pbt.string_ascii(0, 96))
	pbt.cover(t, len(task_token) >= 48, 15.0, "long opaque Nexus task token")
	pbt.cover(t, len(payload_data) == 0, 0.2, "empty Nexus payload")
	pbt.cover(t, len(payload_data) >= 192, 15.0, "large Nexus payload")
	has_zero := false
	for value in task_token {
		if value == 0 {
			has_zero = true
			break
		}
	}
	pbt.cover(t, has_zero, 1.0, "embedded zero Nexus task token byte")

	payload := proto.Temporal_Api_Common_V1_Payload{data = payload_data}
	append(
		&payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("binary/test"),
		},
	)
	defer delete(payload.metadata)
	start := proto.Temporal_Api_Nexus_V1_StartOperationRequest{
		service = service,
		operation = operation,
		request_id = request_id,
		payload = &payload,
	}
	append(
		&start.callback_header,
		proto.Temporal_Api_Nexus_V1_StartOperationRequest_CallbackHeaderEntry{
			key = "callback-key",
			value = callback_value,
		},
	)
	defer delete(start.callback_header)
	request := proto.Temporal_Api_Nexus_V1_Request{
		endpoint = "property-endpoint",
		variant = proto.Temporal_Api_Nexus_V1_Request_Variant_Start_Operation{value = &start},
	}
	append(
		&request.header,
		proto.Temporal_Api_Nexus_V1_Request_HeaderEntry{
			key = "property-key",
			value = header_value,
		},
	)
	defer delete(request.header)
	poll := proto.Temporal_Api_Workflowservice_V1_PollNexusTaskQueueResponse{
		task_token = task_token,
		request = &request,
	}
	exact := proto.Coresdk_Nexus_NexusTask{
		endpoint = "core-property-endpoint",
		variant = proto.Coresdk_Nexus_NexusTask_Variant_Task{value = &poll},
	}
	encoded := proto.encode_Coresdk_Nexus_NexusTask(&exact)
	defer delete(encoded)

	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return pbt.fail("could not initialize Nexus property decode arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	task, decode_error := proto.decode_nexus_task(encoded[:])
	context.allocator = previous_allocator
	if decode_error != .None || task.kind != .Start_Operation {
		return pbt.fail("generated Nexus start task did not decode")
	}
	if !bytes.equal(task.task_token, task_token) ||
	   task.service != service ||
	   task.operation != operation ||
	   task.request_id != request_id ||
	   task.endpoint != "core-property-endpoint" {
		return pbt.fail("generated Nexus routing fields changed during decode")
	}
	if task.input.encoding != "binary/test" || !bytes.equal(task.input.data, payload_data) {
		return pbt.fail("generated Nexus input Payload changed during decode")
	}
	if len(task.headers) != 1 || task.headers[0].value != header_value ||
	   len(task.callback_headers) != 1 || task.callback_headers[0].value != callback_value {
		return pbt.fail("generated Nexus headers changed during decode")
	}

	result_payload := codec.payload_create(payload_data)
	defer codec.payload_free(&result_payload)
	if codec.payload_add_metadata(
		&result_payload,
		"encoding",
		transmute([]u8)string("binary/test"),
	) != .None {
		return pbt.fail("could not allocate generated Nexus result metadata")
	}
	completion_bytes := proto.encode_nexus_sync_success(
		task_token,
		codec.payload_view(&result_payload),
	)
	defer delete(completion_bytes)
	context.allocator = virtual.arena_allocator(&arena)
	completion := proto.Coresdk_Nexus_NexusTaskCompletion{}
	completion_ok := proto.decode_Coresdk_Nexus_NexusTaskCompletion(
		completion_bytes[:],
		&completion,
	)
	context.allocator = previous_allocator
	if !completion_ok || !bytes.equal(completion.task_token, task_token) {
		return pbt.fail("generated Nexus completion did not preserve its task token")
	}
	completed, completed_ok := completion.status.(proto.Coresdk_Nexus_NexusTaskCompletion_Status_Completed)
	if !completed_ok || completed.value == nil {
		return pbt.fail("generated Nexus completion did not contain a completed response")
	}
	response, response_ok := completed.value.variant.(proto.Temporal_Api_Nexus_V1_Response_Variant_Start_Operation)
	if !response_ok || response.value == nil {
		return pbt.fail("generated Nexus completion lost its start response")
	}
	sync_result, sync_ok := response.value.variant.(proto.Temporal_Api_Nexus_V1_StartOperationResponse_Variant_Sync_Success)
	if !sync_ok || sync_result.value == nil || sync_result.value.payload == nil ||
	   !bytes.equal(sync_result.value.payload.data, payload_data) {
		return pbt.fail("generated Nexus completion Payload changed during encoding")
	}
	return pbt.pass()
}

@(test)
nexus_tasks_and_completions_preserve_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Nexus tasks and completions preserve generated values",
		nexus_task_and_completion_property,
		{num_tests = 500, seed = 0x4e45_5855_535f_4142},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

nexus_workflow_command_property :: proc(t: ^pbt.T) -> pbt.Result {
	seq := u32(pbt.draw(t, pbt.int_range(1, 1_000_000)))
	endpoint := pbt.draw(t, pbt.string_ascii(1, 64))
	service := pbt.draw(t, pbt.string_ascii(1, 64))
	operation := pbt.draw(t, pbt.string_ascii(1, 64))
	payload_data := pbt.draw(t, pbt.byte_array(0, 512))
	header_key := pbt.draw(t, pbt.string_ascii(1, 48))
	header_value := pbt.draw(t, pbt.string_ascii(0, 128))
	cancellation_index := pbt.draw(t, pbt.int_range(0, 3))
	cancellation_type := proto.Coresdk_Nexus_NexusOperationCancellationType(cancellation_index)
	close_seconds := i64(pbt.draw(t, pbt.int_range(1, 86_400)))
	start_seconds := i64(pbt.draw(t, pbt.int_range(0, 3_600)))
	run_seconds := i64(pbt.draw(t, pbt.int_range(0, 3_600)))
	pbt.cover(t, len(payload_data) == 0, 0.1, "empty Workflow Nexus input")
	pbt.cover(t, len(payload_data) >= 400, 15.0, "large Workflow Nexus input")
	pbt.cover(t, cancellation_index == 0, 20.0, "wait cancellation completed")
	pbt.cover(t, cancellation_index == 3, 20.0, "wait cancellation requested")

	payload := proto.Temporal_Api_Common_V1_Payload{data = payload_data}
	close_timeout := proto.Google_Protobuf_Duration{seconds = close_seconds}
	start_timeout := proto.Google_Protobuf_Duration{seconds = start_seconds}
	run_timeout := proto.Google_Protobuf_Duration{seconds = run_seconds}
	schedule := proto.Coresdk_Workflow_Commands_ScheduleNexusOperation{
		seq = seq,
		endpoint = endpoint,
		service = service,
		operation = operation,
		input = &payload,
		schedule_to_close_timeout = &close_timeout,
		schedule_to_start_timeout = &start_timeout,
		start_to_close_timeout = &run_timeout,
		cancellation_type = cancellation_type,
	}
	append(
		&schedule.nexus_header,
		proto.Coresdk_Workflow_Commands_ScheduleNexusOperation_NexusHeaderEntry{
			key = header_key,
			value = header_value,
		},
	)
	defer delete(schedule.nexus_header)
	command := proto.Coresdk_Workflow_Commands_WorkflowCommand{
		variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Nexus_Operation{
			value = &schedule,
		},
	}
	encoded := proto.encode_Coresdk_Workflow_Commands_WorkflowCommand(&command)
	defer delete(encoded)

	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return pbt.error("could not initialize Workflow Nexus command decode arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	decoded := proto.Coresdk_Workflow_Commands_WorkflowCommand{}
	decoded_ok := proto.decode_Coresdk_Workflow_Commands_WorkflowCommand(encoded[:], &decoded)
	context.allocator = previous_allocator
	if !decoded_ok {
		return pbt.fail("generated Workflow Nexus command did not decode")
	}
	variant, variant_ok := decoded.variant.(proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Nexus_Operation)
	if !variant_ok || variant.value == nil {
		return pbt.fail("generated Workflow Nexus command lost its variant")
	}
	actual := variant.value
	if actual.seq != seq || actual.endpoint != endpoint || actual.service != service ||
	   actual.operation != operation || actual.input == nil ||
	   !bytes.equal(actual.input.data, payload_data) ||
	   actual.cancellation_type != cancellation_type {
		return pbt.fail("generated Workflow Nexus command changed routing or payload fields")
	}
	if len(actual.nexus_header) != 1 || actual.nexus_header[0].key != header_key ||
	   actual.nexus_header[0].value != header_value {
		return pbt.fail("generated Workflow Nexus command changed header fields")
	}
	if actual.schedule_to_close_timeout == nil ||
	   actual.schedule_to_close_timeout.seconds != close_seconds ||
	   actual.schedule_to_start_timeout == nil ||
	   actual.schedule_to_start_timeout.seconds != start_seconds ||
	   actual.start_to_close_timeout == nil ||
	   actual.start_to_close_timeout.seconds != run_seconds {
		return pbt.fail("generated Workflow Nexus command changed timeout fields")
	}
	return pbt.pass()
}

@(test)
workflow_nexus_commands_preserve_generated_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow Nexus commands preserve generated values",
		nexus_workflow_command_property,
		{num_tests = 500, seed = 0x4e45_5855_535f_5746},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

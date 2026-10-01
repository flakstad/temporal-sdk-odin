package workflow_pbt_test

import "core:bytes"
import "core:mem/virtual"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import workflow "../temporal/workflow"

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

nexus_cancellation_policy_property :: proc(t: ^pbt.T) -> pbt.Result {
	policy_index := pbt.draw(t, pbt.int_range(0, 3))
	policy := workflow.Nexus_Operation_Cancellation_Type(policy_index)
	pbt.cover(t, policy == .Wait_Cancellation_Completed, 20.0, "wait cancellation completed")
	pbt.cover(t, policy == .Abandon, 20.0, "abandon")
	pbt.cover(t, policy == .Try_Cancel, 20.0, "try cancel")
	pbt.cover(t, policy == .Wait_Cancellation_Requested, 20.0, "wait cancellation requested")

	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return pbt.error("could not initialize Nexus cancellation policy arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	defer {context.allocator = previous_allocator}

	instance := workflow.Fiber_Instance{phase = .Ready}
	instance.nexus_operations = make([dynamic]workflow.Fiber_Nexus_Operation)
	append(
		&instance.nexus_operations,
		workflow.Fiber_Nexus_Operation{
			seq = 1,
			options = {cancellation_type = policy},
			start_state = .Succeeded,
			state = .Commanded,
		},
	)
	ctx := workflow.Fiber_Context{instance = &instance}
	if !workflow.cancel_nexus_operation(&ctx, {seq = 1}) {
		return pbt.fail("generated Nexus policy rejected cancellation")
	}

	commands := make([dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand)
	processed := workflow.Process_Result{}
	workflow.append_fiber_nexus_operation_cancellations(&processed, &commands, &instance)
	operation := &instance.nexus_operations[0]
	if policy == .Abandon {
		if len(commands) != 0 || operation.state != .Canceled ||
		   operation.cancellation_command_emitted ||
		   !workflow.nexus_operation_ready_for_waiter(&instance, operation, 0) {
			return pbt.fail("Abandon emitted cancellation or remained suspended")
		}
	} else {
		if len(commands) != 1 || !operation.cancellation_command_emitted ||
		   !processed.requested_nexus_operation_cancellation {
			return pbt.fail("canceling Nexus policy did not emit exactly one command")
		}
		if policy == .Try_Cancel {
			if operation.state != .Canceled ||
			   !workflow.nexus_operation_ready_for_waiter(&instance, operation, 0) {
				return pbt.fail("TryCancel did not resume immediately")
			}
		} else if operation.state != .Cancel_Requested ||
		          workflow.nexus_operation_ready_for_waiter(&instance, operation, 0) {
			return pbt.fail("waiting Nexus policy resumed before Core resolution")
		}
	}
	command_count := len(commands)
	workflow.append_fiber_nexus_operation_cancellations(&processed, &commands, &instance)
	if len(commands) != command_count {
		return pbt.fail("Nexus cancellation command was emitted more than once")
	}
	return pbt.pass()
}

@(test)
workflow_nexus_cancellation_policies_preserve_distinct_transitions :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow Nexus cancellation policies preserve distinct transitions",
		nexus_cancellation_policy_property,
		{num_tests = 240, seed = 0x4e45_5855_535f_4341},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

nexus_terminal_resolution_property :: proc(t: ^pbt.T) -> pbt.Result {
	terminal_index := pbt.draw(t, pbt.int_range(0, 3))
	terminal_states := [?]workflow.Nexus_Operation_State{
		.Resolved,
		.Failed,
		.Canceled,
		.Timed_Out,
	}
	terminal_state := terminal_states[terminal_index]
	for index in 0..<len(terminal_states) {
		pbt.cover(t, terminal_index == index, 20.0, "terminal Nexus state")
	}
	late_data := pbt.draw(t, pbt.byte_array(0, 128))

	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return pbt.error("could not initialize terminal Nexus resolution arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	defer {context.allocator = previous_allocator}

	instance := workflow.Fiber_Instance{}
	instance.nexus_operations = make([dynamic]workflow.Fiber_Nexus_Operation)
	append(
		&instance.nexus_operations,
		workflow.Fiber_Nexus_Operation{
			seq = 1,
			start_state = .Succeeded,
			state = terminal_state,
			failure_message = "original terminal outcome",
		},
	)
	late_payload := proto.Temporal_Api_Common_V1_Payload{data = late_data}
	late_result := proto.Coresdk_Nexus_NexusOperationResult{
		status = proto.Coresdk_Nexus_NexusOperationResult_Status_Completed{
			value = &late_payload,
		},
	}
	resolution := proto.Coresdk_Workflow_Activation_ResolveNexusOperation{
		seq = 1,
		result = &late_result,
	}

	for _ in 0..<2 {
		processed := workflow.Process_Result{}
		resolution_error := workflow.apply_nexus_operation_resolution(
			&instance,
			&resolution,
			&processed,
		)
		if len(resolution_error) > 0 || processed.resolved_nexus_operation_seq != 1 {
			return pbt.fail("duplicate terminal Nexus resolution was not acknowledged")
		}
		operation := &instance.nexus_operations[0]
		if operation.state != terminal_state ||
		   operation.failure_message != "original terminal outcome" ||
		   len(operation.result.data) != 0 {
			return pbt.fail("late Nexus result replaced a terminal outcome")
		}
		if terminal_state == .Canceled && !processed.resolved_nexus_operation_canceled {
			return pbt.fail("terminal Nexus cancellation lost its acknowledgement")
		}
	}
	return pbt.pass()
}

@(test)
workflow_nexus_terminal_results_are_absorbing :: proc(t: ^testing.T) {
	result := pbt.check(
		"terminal Nexus results absorb duplicate or conflicting delivery",
		nexus_terminal_resolution_property,
		{num_tests = 240, seed = 0x4e45_5855_535f_4455},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

nexus_selector_property_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	order: string,
) -> string {
	cancellation_token := workflow.cancellation_detached(ctx)
	if cancellation_token.id == 0 {
		return "invalid-token"
	}
	options := workflow.Nexus_Operation_Options{
		schedule_to_close_timeout = time.Minute,
		cancellation_token = cancellation_token,
	}
	first := workflow.start_nexus_operation(
		ctx,
		"pbt-endpoint",
		"SelectorService",
		"first",
		"input",
		options,
	)
	second := workflow.start_nexus_operation(
		ctx,
		"pbt-endpoint",
		"SelectorService",
		"second",
		"input",
		options,
	)
	cases := [2]workflow.Selector_Case{
		workflow.selector_nexus_operation(first),
		workflow.selector_nexus_operation(second),
	}
	if order == "reverse" {
		cases = {
			workflow.selector_nexus_operation(second),
			workflow.selector_nexus_operation(first),
		}
	}
	selected := workflow.selector_select(ctx, cases[:])
	if selected.status != .Selected || selected.index != 0 ||
	   selected.kind != .Nexus_Operation {
		return "wrong-selection"
	}
	join_cases := [2]workflow.Selector_Case{
		workflow.selector_nexus_operation(first),
		workflow.selector_nexus_operation(second),
	}
	if workflow.join_all(ctx, join_cases[:]) != .Ready {
		return "wrong-join"
	}
	if order == "reverse" {
		return workflow.await_nexus_operation(ctx, second)
	}
	return workflow.await_nexus_operation(ctx, first)
}

append_nexus_selector_property_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	permutation: int,
	first_start: ^proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart,
	first_result: ^proto.Coresdk_Workflow_Activation_ResolveNexusOperation,
	second_start: ^proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart,
	second_result: ^proto.Coresdk_Workflow_Activation_ResolveNexusOperation,
) {
	available := [4]proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Nexus_Operation_Start{
				value = first_start,
			},
		},
		{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Nexus_Operation{
				value = first_result,
			},
		},
		{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Nexus_Operation_Start{
				value = second_start,
			},
		},
		{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Nexus_Operation{
				value = second_result,
			},
		},
	}
	orders := [6][4]int{
		{0, 1, 2, 3},
		{0, 2, 1, 3},
		{0, 2, 3, 1},
		{2, 3, 0, 1},
		{2, 0, 3, 1},
		{2, 0, 1, 3},
	}
	for index in orders[permutation] {
		append(jobs, available[index])
	}
}

nexus_selector_activation_order_property :: proc(t: ^pbt.T) -> pbt.Result {
	reverse_cases := pbt.draw(t, pbt.boolean())
	job_order := pbt.draw(t, pbt.int_range(0, 5))
	pbt.cover(t, reverse_cases, 40.0, "reversed Nexus selector declarations")
	for index in 0..<6 {
		pbt.cover(t, job_order == index, 10.0, "Nexus activation job interleaving")
	}

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_nexus_selector_workflow",
		"test-queue",
		nexus_selector_property_workflow,
	)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	if init_error.message != nil {
		return pbt.error("could not initialize Nexus selector runtime")
	}

	order := "forward"
	if reverse_cases do order = "reverse"
	input, input_error := codec.encode_json(order)
	defer codec.payload_free(&input)
	if input_error != .None {
		return pbt.error("could not encode Nexus selector order")
	}
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_nexus_selector_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-nexus-selector-run",
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
	if initial.error.message != nil || initial.scheduled_nexus_operation_count != 2 ||
	   !initial.waiting_selector {
		return pbt.fail("generated Nexus selector Workflow did not initialize")
	}
	initial_completion := bytes.clone(initial.completion[:])
	defer delete(initial_completion)

	first_payload, first_payload_error := codec.encode_json("first")
	second_payload, second_payload_error := codec.encode_json("second")
	defer codec.payload_free(&first_payload)
	defer codec.payload_free(&second_payload)
	if first_payload_error != .None || second_payload_error != .None {
		return pbt.error("could not encode Nexus selector results")
	}
	first_proto := proto.encode_payload(codec.payload_view(&first_payload))
	second_proto := proto.encode_payload(codec.payload_view(&second_payload))
	defer delete(first_proto.metadata)
	defer delete(first_proto.external_payloads)
	defer delete(second_proto.metadata)
	defer delete(second_proto.external_payloads)
	first_start := proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart_Status_Started_Sync{value = true},
	}
	second_start := proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart{
		seq = 2,
		status = proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart_Status_Started_Sync{value = true},
	}
	first_value := proto.Coresdk_Nexus_NexusOperationResult{
		status = proto.Coresdk_Nexus_NexusOperationResult_Status_Completed{value = &first_proto},
	}
	second_value := proto.Coresdk_Nexus_NexusOperationResult{
		status = proto.Coresdk_Nexus_NexusOperationResult_Status_Completed{value = &second_proto},
	}
	first_result := proto.Coresdk_Workflow_Activation_ResolveNexusOperation{seq = 1, result = &first_value}
	second_result := proto.Coresdk_Workflow_Activation_ResolveNexusOperation{seq = 2, result = &second_value}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 12
	append_nexus_selector_property_jobs(
		&activation.jobs,
		job_order,
		&first_start,
		&first_result,
		&second_start,
		&second_result,
	)
	terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&terminal)
	if terminal.error.message != nil || !terminal.completed || terminal.waiting_selector {
		return pbt.fail("generated Nexus selector activation did not complete")
	}
	result_payload, result_found := clone_pbt_workflow_result(terminal.completion[:])
	defer codec.payload_free(&result_payload)
	if !result_found {
		return pbt.fail("Nexus selector completion had no result")
	}
	result: string
	if codec.decode_json(codec.payload_view(&result_payload), &result) != .None {
		return pbt.fail("Nexus selector completion result did not decode")
	}
	defer delete(result)
	expected := "first"
	if reverse_cases do expected = "second"
	if result != expected {
		return pbt.fail("Nexus selector did not choose declaration order")
	}
	terminal_completion := bytes.clone(terminal.completion[:])
	defer delete(terminal_completion)

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
		return pbt.fail("generated Nexus selector Workflow did not evict")
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
	replayed_initial := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_initial)
	if replayed_initial.error.message != nil ||
	   !bytes.equal(initial_completion, replayed_initial.completion[:]) {
		return pbt.fail("Nexus selector initialization changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 12
	append_nexus_selector_property_jobs(
		&activation.jobs,
		job_order,
		&first_start,
		&first_result,
		&second_start,
		&second_result,
	)
	replayed_terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_terminal)
	if replayed_terminal.error.message != nil ||
	   !bytes.equal(terminal_completion, replayed_terminal.completion[:]) {
		return pbt.fail("Nexus selector completion changed on replay")
	}
	return pbt.pass()
}

@(test)
workflow_nexus_selector_is_biased_across_activation_orders :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow Nexus selector is declaration-biased across activation orders",
		nexus_selector_activation_order_property,
		{num_tests = 180, seed = 0x4e45_5855_535f_5345},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

package workflow_pbt_test

import "core:bytes"
import "core:mem/virtual"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import activity "../temporal/activity"
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

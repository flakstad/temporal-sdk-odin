package workflow_pbt_test

import "core:bytes"
import "core:mem/virtual"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import workflow "../temporal/workflow"

Generated_Signal_Cancellation_State :: struct {
	token:     workflow.Cancellation_Token,
	requested: bool,
}

generated_cancel_signal_token :: proc(
	_: ^workflow.Fiber_Context,
	state: ^Generated_Signal_Cancellation_State,
	_: string,
) {
	state.requested = workflow.cancellation_request(
		state.token,
		"generated Signal cancellation",
	)
}

generated_child_signal_start_wait_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> string {
	state := Generated_Signal_Cancellation_State{
		token = workflow.cancellation_detached(ctx),
	}
	if state.token.id == 0 ||
	   !workflow.set_signal_routine_json(
		ctx,
		"cancel-generated-signal",
		&state,
		generated_cancel_signal_token,
	   ) {
		return "registration-failed"
	}
	child := workflow.start_child_workflow_json(
		ctx,
		"generated_signal_target",
		input,
		{
			workflow_id = "generated-signal-target-id",
			workflow_execution_timeout = time.Hour,
		},
	)
	result := workflow.signal_child_workflow_json_with_options(
		ctx,
		child,
		"generated-approval",
		input,
		{cancellation_token = state.token},
	)
	if !state.requested || result.status != .Canceled ||
	   result.failure_message != "generated Signal cancellation" {
		return "cancellation-failed"
	}
	_ = workflow.await_signal(ctx, "finish-generated-signal")
	return "done"
}

generated_completion_has_command_count :: proc(data: []u8, expected: int) -> bool {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return false
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
		data,
		&completion,
	)
	context.allocator = previous_allocator
	if !decoded {
		return false
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		return status.value != nil && len(status.value.commands) == expected
	}
	return false
}

child_signal_start_wait_cancellation_property :: proc(t: ^pbt.T) -> pbt.Result {
	resolve_start := pbt.draw(t, pbt.boolean())
	cancel_first := pbt.draw(t, pbt.boolean())
	pbt.cover(t, resolve_start, 40.0, "Child start resolves in cancellation activation")
	pbt.cover(t, !resolve_start, 40.0, "Child start remains pending")
	pbt.cover(t, resolve_start && cancel_first, 15.0, "cancel job before Child start")
	pbt.cover(t, resolve_start && !cancel_first, 15.0, "Child start before cancel job")

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"generated_child_signal_start_wait_workflow",
		"generated-signal-queue",
		generated_child_signal_start_wait_workflow,
	)
	if init_error.message != nil {
		workflow.error_free(&init_error)
		return pbt.error("could not initialize generated Signal cancellation Workflow")
	}
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("generated")
	if input_error != .None {
		return pbt.error("could not encode generated Signal cancellation input")
	}
	defer codec.payload_free(&input)
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_child_signal_start_wait_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "generated-child-signal-start-wait-run",
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
	if initial.error.message != nil || !initial.started_child_workflow ||
	   initial.waiting_child_workflow_start_seq != 1 ||
	   !generated_completion_has_command_count(initial.completion[:], 1) {
		return pbt.fail("generated Child Signal did not wait for Child start")
	}
	initial_bytes := bytes.clone(initial.completion[:])
	defer delete(initial_bytes)

	signal_input, signal_error := codec.encode_json("cancel")
	if signal_error != .None {
		return pbt.error("could not encode generated cancellation Signal")
	}
	defer codec.payload_free(&signal_input)
	signal_input_proto := proto.encode_payload(codec.payload_view(&signal_input))
	defer delete(signal_input_proto.metadata)
	defer delete(signal_input_proto.external_payloads)
	cancel_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "cancel-generated-signal",
	}
	defer delete(cancel_signal.input)
	append(&cancel_signal.input, signal_input_proto)
	cancel_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
			value = &cancel_signal,
		},
	}
	start_success := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartSuccess{
		run_id = "generated-child-run",
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded{
			value = &start_success,
		},
	}
	resolve_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
			value = &resolve,
		},
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	if resolve_start && !cancel_first {
		append(&activation.jobs, resolve_job)
	}
	append(&activation.jobs, cancel_job)
	if resolve_start && cancel_first {
		append(&activation.jobs, resolve_job)
	}
	canceled := process_activation(&runtime, &activation)
	defer destroy_process_result(&canceled)
	if canceled.error.message != nil || !canceled.received_signal ||
	   canceled.signaled_child_workflow || !canceled.waiting_signal ||
	   !generated_completion_has_command_count(canceled.completion[:], 0) {
		return pbt.fail("generated Signal token did not cancel the Child start prerequisite")
	}
	cancel_bytes := bytes.clone(canceled.completion[:])
	defer delete(cancel_bytes)

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
	if evicted.error.message != nil || !evicted.evicted {
		destroy_process_result(&evicted)
		return pbt.fail("could not evict generated Signal cancellation Workflow")
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
	replayed_initial := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_initial)
	if replayed_initial.error.message != nil ||
	   !bytes.equal(initial_bytes, replayed_initial.completion[:]) {
		return pbt.fail("generated Signal cancellation initialization changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	if resolve_start && !cancel_first {
		append(&activation.jobs, resolve_job)
	}
	append(&activation.jobs, cancel_job)
	if resolve_start && cancel_first {
		append(&activation.jobs, resolve_job)
	}
	replayed_cancel := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_cancel)
	if replayed_cancel.error.message != nil ||
	   !bytes.equal(cancel_bytes, replayed_cancel.completion[:]) {
		return pbt.fail("generated Signal cancellation changed on replay")
	}
	return pbt.pass()
}

@(test)
child_signal_start_wait_cancellation_is_order_independent :: proc(t: ^testing.T) {
	result := pbt.check(
		"Child Signal start-wait cancellation is deterministic across Core job order",
		child_signal_start_wait_cancellation_property,
		{num_tests = 240, seed = 0x5349_474e_4341_4e43},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

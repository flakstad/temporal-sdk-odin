package workflow_pbt_test

import "core:bytes"
import "core:fmt"
import "core:testing"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import workflow "../temporal/workflow"

Condition_Property_State :: struct {
	value:          int,
	signal_resumed: bool,
	update_resumed: bool,
}

condition_property_ready :: proc(state: ^Condition_Property_State) -> bool {
	return state.value >= 3
}

condition_property_signal_waiter :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Condition_Property_State,
	_: string,
) {
	state.signal_resumed = workflow.await_condition(ctx, state, condition_property_ready)
}

condition_property_update_waiter :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Condition_Property_State,
	_: string,
) -> string {
	state.update_resumed = workflow.await_condition(ctx, state, condition_property_ready)
	if state.update_resumed {
		return "ready"
	}
	return "canceled"
}

condition_property_increment :: proc(state: ^Condition_Property_State, amount: int) {
	state.value += amount
}

condition_property_workflow :: proc(ctx: ^workflow.Fiber_Context, _: string) -> string {
	state := Condition_Property_State{}
	if !workflow.set_signal_routine_json(
		ctx,
		"condition_waiter",
		&state,
		condition_property_signal_waiter,
	) || !workflow.set_update_routine_json(
		ctx,
		"condition_update",
		&state,
		condition_property_update_waiter,
	) || !workflow.set_signal_handler_json(
		ctx,
		"condition_increment",
		&state,
		condition_property_increment,
	) {
		return "registration-failed"
	}
	_ = workflow.await_signal(ctx, "finish")
	workflow.await_all_handlers_finished(ctx)
	return fmt.aprintf(
		"%d:%t:%t",
		state.value,
		state.signal_resumed,
		state.update_resumed,
	)
}

Condition_Partition :: struct {
	values: [3]int,
	count:  int,
}

condition_partitions := [4]Condition_Partition{
	{values = {3, 0, 0}, count = 1},
	{values = {1, 2, 0}, count = 2},
	{values = {2, 1, 0}, count = 2},
	{values = {1, 1, 1}, count = 3},
}

process_condition_increment :: proc(
	runtime: ^workflow.Fiber_Runtime,
	activation: ^proto.Coresdk_Workflow_Activation_WorkflowActivation,
	amount: int,
) -> workflow.Process_Result {
	payload, payload_error := codec.encode_json(amount)
	defer codec.payload_free(&payload)
	if payload_error != .None {
		return {error = workflow.error_from_string("could not encode condition increment")}
	}
	proto_payload := proto.encode_payload(codec.payload_view(&payload))
	defer delete(proto_payload.metadata)
	defer delete(proto_payload.external_payloads)
	signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "condition_increment",
	}
	defer delete(signal.input)
	append(&signal.input, proto_payload)
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &signal,
			},
		},
	)
	return process_activation(runtime, activation)
}

append_condition_waiter_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	signal: ^proto.Coresdk_Workflow_Activation_SignalWorkflow,
	update: ^proto.Coresdk_Workflow_Activation_DoUpdate,
	update_first: bool,
) {
	signal_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
			value = signal,
		},
	}
	update_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
			value = update,
		},
	}
	if update_first {
		append(jobs, update_job, signal_job)
	} else {
		append(jobs, signal_job, update_job)
	}
}

condition_waiter_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	partition_index := pbt.draw(t, pbt.int_range(0, 3))
	update_first_value := pbt.draw(t, pbt.int_range(0, 1))
	update_first := update_first_value == 1
	partition_labels := [?]string{"3", "1+2", "2+1", "1+1+1"}
	for candidate in 0..<4 {
		pbt.cover(t, partition_index == candidate, 15.0, partition_labels[candidate])
	}
	pbt.cover(t, update_first, 35.0, "update-first")
	pbt.cover(t, !update_first, 35.0, "signal-first")

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_workflow_condition",
		"test-queue",
		condition_property_workflow,
	)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	if init_error.message != nil {
		return pbt.error("could not initialize condition Workflow runtime")
	}

	input, input_error := codec.encode_json("start")
	defer codec.payload_free(&input)
	if input_error != .None {
		return pbt.error("could not encode condition Workflow input")
	}
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_workflow_condition",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-workflow-condition-run",
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
	if initial.error.message != nil || !initial.waiting_signal {
		destroy_process_result(&initial)
		return pbt.fail("condition Workflow did not initialize")
	}
	initial_completion := bytes.clone(initial.completion[:])
	destroy_process_result(&initial)
	defer delete(initial_completion)

	waiter_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "condition_waiter",
	}
	defer delete(waiter_signal.input)
	append(&waiter_signal.input, proto_input)
	waiter_update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "condition-update",
		protocol_instance_id = "condition-update-protocol",
		name = "condition_update",
		run_validator = false,
	}
	defer delete(waiter_update.input)
	append(&waiter_update.input, proto_input)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append_condition_waiter_jobs(
		&activation.jobs,
		&waiter_signal,
		&waiter_update,
		update_first,
	)
	waiters := process_activation(&runtime, &activation)
	if waiters.error.message != nil || waiters.signal_handlers_started != 1 ||
	   waiters.signal_handlers_completed != 0 || waiters.updates_accepted != 1 ||
	   waiters.updates_completed != 0 {
		destroy_process_result(&waiters)
		return pbt.fail("condition routines did not suspend")
	}
	waiters_completion := bytes.clone(waiters.completion[:])
	destroy_process_result(&waiters)
	defer delete(waiters_completion)

	partition := condition_partitions[partition_index]
	increment_completions := make([dynamic][]u8)
	defer {
		for completion in increment_completions {
			delete(completion)
		}
		delete(increment_completions)
	}
	for index in 0..<partition.count {
		activation.history_length += 3
		increment := process_condition_increment(
			&runtime,
			&activation,
			partition.values[index],
		)
		if increment.error.message != nil {
			destroy_process_result(&increment)
			return pbt.fail("condition increment failed")
		}
		if index < partition.count - 1 &&
		   (increment.signal_handlers_completed != 1 || increment.updates_completed != 0) {
			destroy_process_result(&increment)
			return pbt.fail("condition routines resumed before predicate became true")
		}
		if index == partition.count - 1 &&
		   (increment.signal_handlers_completed != 2 || increment.updates_completed != 1) {
			destroy_process_result(&increment)
			return pbt.fail("condition routines did not resume at the threshold")
		}
		append(&increment_completions, bytes.clone(increment.completion[:]))
		destroy_process_result(&increment)
	}

	finish := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "finish"}
	defer delete(finish.input)
	append(&finish.input, proto_input)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length += 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish,
			},
		},
	)
	terminal := process_activation(&runtime, &activation)
	if terminal.error.message != nil || !terminal.completed {
		destroy_process_result(&terminal)
		return pbt.fail("condition Workflow did not complete")
	}
	terminal_payload, terminal_found := clone_pbt_workflow_result(terminal.completion[:])
	actual: string
	decode_error := codec.decode_json(codec.payload_view(&terminal_payload), &actual)
	codec.payload_free(&terminal_payload)
	if !terminal_found || decode_error != .None || actual != "3:true:true" {
		delete(actual)
		destroy_process_result(&terminal)
		return pbt.fail("condition Workflow state was incorrect")
	}
	delete(actual)
	terminal_completion := bytes.clone(terminal.completion[:])
	destroy_process_result(&terminal)
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
	if evicted.error.message != nil || !evicted.evicted {
		destroy_process_result(&evicted)
		return pbt.fail("condition Workflow did not evict")
	}
	destroy_process_result(&evicted)

	activation.is_replaying = true
	activation.history_length = 3
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed := process_activation(&runtime, &activation)
	if replayed.error.message != nil || !bytes.equal(initial_completion, replayed.completion[:]) {
		destroy_process_result(&replayed)
		return pbt.fail("condition initialization changed on replay")
	}
	destroy_process_result(&replayed)

	activation.history_length = 7
	delete(activation.jobs)
	activation.jobs = nil
	append_condition_waiter_jobs(
		&activation.jobs,
		&waiter_signal,
		&waiter_update,
		update_first,
	)
	replayed = process_activation(&runtime, &activation)
	if replayed.error.message != nil || !bytes.equal(waiters_completion, replayed.completion[:]) {
		destroy_process_result(&replayed)
		return pbt.fail("condition waiters changed on replay")
	}
	destroy_process_result(&replayed)

	for index in 0..<partition.count {
		activation.history_length += 3
		replayed = process_condition_increment(
			&runtime,
			&activation,
			partition.values[index],
		)
		if replayed.error.message != nil ||
		   !bytes.equal(increment_completions[index], replayed.completion[:]) {
			destroy_process_result(&replayed)
			return pbt.fail("condition threshold progression changed on replay")
		}
		destroy_process_result(&replayed)
	}

	activation.history_length += 3
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish,
			},
		},
	)
	replayed = process_activation(&runtime, &activation)
	if replayed.error.message != nil || !bytes.equal(terminal_completion, replayed.completion[:]) {
		destroy_process_result(&replayed)
		return pbt.fail("condition terminal completion changed on replay")
	}
	destroy_process_result(&replayed)
	return pbt.pass()
}

@(test)
workflow_condition_waiters_resume_at_threshold_and_replay :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow condition waiters resume at threshold and replay",
		condition_waiter_replay_property,
		{num_tests = 160, seed = 0x434f_4e44_4954_494f},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

package workflow_pbt_test

import "core:bytes"
import "core:fmt"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import workflow "../temporal/workflow"

Mutex_Property_State :: struct {
	mutex: workflow.Mutex,
	order: [3]int,
	count: int,
}

mutex_property_owner :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Mutex_Property_State,
	_: string,
) {
	if !workflow.mutex_lock(ctx, &state.mutex) {
		return
	}
	defer workflow.mutex_unlock(ctx, &state.mutex)
	workflow.sleep(ctx, time.Second)
}

mutex_property_update :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Mutex_Property_State,
	value: int,
) -> int {
	if !workflow.mutex_lock(ctx, &state.mutex) {
		return -1
	}
	defer workflow.mutex_unlock(ctx, &state.mutex)
	state.order[state.count] = value
	state.count += 1
	return value
}

mutex_property_workflow :: proc(ctx: ^workflow.Fiber_Context, _: string) -> string {
	state := Mutex_Property_State{}
	if !workflow.set_signal_routine_json(
		ctx,
		"owner",
		&state,
		mutex_property_owner,
	) || !workflow.set_update_routine_json(
		ctx,
		"waiter",
		&state,
		mutex_property_update,
	) {
		return "registration-failed"
	}
	_ = workflow.await_signal(ctx, "finish")
	workflow.await_all_handlers_finished(ctx)
	if state.count != 3 || workflow.mutex_is_locked(&state.mutex) {
		return "mutex-state-failed"
	}
	return fmt.aprintf("%d%d%d", state.order[0], state.order[1], state.order[2])
}

mutex_permutations := [6][3]int{
	{1, 2, 3},
	{1, 3, 2},
	{2, 1, 3},
	{2, 3, 1},
	{3, 1, 2},
	{3, 2, 1},
}

append_mutex_update_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	updates: ^[3]proto.Coresdk_Workflow_Activation_DoUpdate,
	permutation: int,
) {
	for value in mutex_permutations[permutation] {
		append(
			jobs,
			proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
				variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
					value = &updates[value - 1],
				},
			},
		)
	}
}

mutex_fifo_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	permutation := pbt.draw(t, pbt.int_range(0, 5))
	labels := [?]string{"123", "132", "213", "231", "312", "321"}
	for candidate in 0..<6 {
		pbt.cover(t, permutation == candidate, 10.0, labels[candidate])
	}

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_workflow_mutex",
		"test-queue",
		mutex_property_workflow,
	)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	if init_error.message != nil {
		return pbt.error("could not initialize mutex Workflow runtime")
	}

	workflow_input, workflow_input_error := codec.encode_json("start")
	defer codec.payload_free(&workflow_input)
	if workflow_input_error != .None {
		return pbt.error("could not encode mutex Workflow input")
	}
	workflow_input_proto := proto.encode_payload(codec.payload_view(&workflow_input))
	defer delete(workflow_input_proto.metadata)
	defer delete(workflow_input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_workflow_mutex",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, workflow_input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-workflow-mutex-run",
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
	if initial.error.message != nil || !initial.waiting_signal {
		return pbt.fail("mutex Workflow did not initialize")
	}
	initial_completion := bytes.clone(initial.completion[:])
	defer delete(initial_completion)

	owner_input, owner_input_error := codec.encode_json("owner")
	defer codec.payload_free(&owner_input)
	if owner_input_error != .None {
		return pbt.error("could not encode mutex owner input")
	}
	owner_input_proto := proto.encode_payload(codec.payload_view(&owner_input))
	defer delete(owner_input_proto.metadata)
	defer delete(owner_input_proto.external_payloads)
	owner := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "owner"}
	defer delete(owner.input)
	append(&owner.input, owner_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &owner,
			},
		},
	)
	owner_started := process_activation(&runtime, &activation)
	defer destroy_process_result(&owner_started)
	if owner_started.error.message != nil || !owner_started.started_timer ||
	   owner_started.signal_handlers_started != 1 || owner_started.signal_handlers_completed != 0 {
		return pbt.fail("mutex owner did not suspend while holding the lock")
	}
	owner_completion := bytes.clone(owner_started.completion[:])
	defer delete(owner_completion)

	update_payloads: [3]codec.Payload
	update_protos: [3]proto.Temporal_Api_Common_V1_Payload
	updates: [3]proto.Coresdk_Workflow_Activation_DoUpdate
	defer {
		for index in 0..<3 {
			codec.payload_free(&update_payloads[index])
			delete(update_protos[index].metadata)
			delete(update_protos[index].external_payloads)
			delete(updates[index].input)
		}
	}
	for index in 0..<3 {
		payload_error: codec.Error
		update_payloads[index], payload_error = codec.encode_json(index + 1)
		if payload_error != .None {
			return pbt.error("could not encode mutex waiter input")
		}
		update_protos[index] = proto.encode_payload(codec.payload_view(&update_payloads[index]))
		updates[index] = {
			id = labels[index],
			protocol_instance_id = labels[index],
			name = "waiter",
			run_validator = false,
		}
		append(&updates[index].input, update_protos[index])
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append_mutex_update_jobs(&activation.jobs, &updates, permutation)
	waiters_started := process_activation(&runtime, &activation)
	defer destroy_process_result(&waiters_started)
	if waiters_started.error.message != nil || waiters_started.updates_accepted != 3 ||
	   waiters_started.updates_completed != 0 {
		return pbt.fail("generated mutex waiters did not queue")
	}
	waiters_completion := bytes.clone(waiters_started.completion[:])
	defer delete(waiters_completion)

	fire_timer := proto.Coresdk_Workflow_Activation_FireTimer{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 12
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{
				value = &fire_timer,
			},
		},
	)
	handoff := process_activation(&runtime, &activation)
	defer destroy_process_result(&handoff)
	if handoff.error.message != nil || handoff.signal_handlers_completed != 1 ||
	   handoff.updates_completed != 3 {
		return pbt.fail("mutex did not drain FIFO waiters after owner release")
	}
	handoff_completion := bytes.clone(handoff.completion[:])
	defer delete(handoff_completion)

	finish := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "finish"}
	defer delete(finish.input)
	append(&finish.input, workflow_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 15
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish,
			},
		},
	)
	terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&terminal)
	if terminal.error.message != nil || !terminal.completed {
		return pbt.fail("mutex Workflow did not complete")
	}
	terminal_payload, terminal_found := clone_pbt_workflow_result(terminal.completion[:])
	defer codec.payload_free(&terminal_payload)
	actual_order: string
	defer delete(actual_order)
	if !terminal_found || codec.decode_json(codec.payload_view(&terminal_payload), &actual_order) != .None {
		return pbt.fail("could not decode mutex Workflow order")
	}
	if actual_order != labels[permutation] {
		return pbt.fail("mutex acquisition order was not FIFO")
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
		return pbt.fail("mutex Workflow did not evict")
	}

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
	replayed_initial := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_initial)
	if replayed_initial.error.message != nil ||
	   !bytes.equal(initial_completion, replayed_initial.completion[:]) {
		return pbt.fail("mutex initialization changed on replay")
	}

	activation.history_length = 5
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &owner,
			},
		},
	)
	replayed_owner := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_owner)
	if replayed_owner.error.message != nil ||
	   !bytes.equal(owner_completion, replayed_owner.completion[:]) {
		return pbt.fail("mutex owner changed on replay")
	}

	activation.history_length = 9
	delete(activation.jobs)
	activation.jobs = nil
	append_mutex_update_jobs(&activation.jobs, &updates, permutation)
	replayed_waiters := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_waiters)
	if replayed_waiters.error.message != nil ||
	   !bytes.equal(waiters_completion, replayed_waiters.completion[:]) {
		return pbt.fail("mutex waiter queue changed on replay")
	}

	activation.history_length = 12
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{
				value = &fire_timer,
			},
		},
	)
	replayed_handoff := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_handoff)
	if replayed_handoff.error.message != nil ||
	   !bytes.equal(handoff_completion, replayed_handoff.completion[:]) {
		return pbt.fail("mutex FIFO handoff changed on replay")
	}

	activation.history_length = 15
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
	replayed_terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_terminal)
	if replayed_terminal.error.message != nil ||
	   !bytes.equal(terminal_completion, replayed_terminal.completion[:]) {
		return pbt.fail("mutex terminal completion changed on replay")
	}
	return pbt.pass()
}

@(test)
workflow_mutex_preserves_fifo_order_and_replay :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow mutex preserves FIFO order and replay",
		mutex_fifo_replay_property,
		{num_tests = 120, seed = 0x4d55_5445_585f_4649},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

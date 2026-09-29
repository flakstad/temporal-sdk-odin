package workflow_pbt_test

import "core:bytes"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import workflow "../temporal/workflow"

Cancellation_Scheduler_State :: struct {
	token:           workflow.Cancellation_Token,
	signal_canceled: bool,
	update_canceled: bool,
}

cancellation_scheduler_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	_: string,
) -> string {
	state := Cancellation_Scheduler_State{
		token = workflow.cancellation_child(workflow.cancellation_root(ctx)),
	}
	if !workflow.set_signal_routine_json(
		ctx,
		"start-waiter",
		&state,
		proc(
			routine_ctx: ^workflow.Fiber_Context,
			state: ^Cancellation_Scheduler_State,
			_: string,
		) {
			scope := workflow.cancellation_scope(routine_ctx, state.token)
			defer workflow.cancellation_scope_end(&scope)
			_ = workflow.await_signal(routine_ctx, "never")
			state.signal_canceled = workflow.cancellation_requested(state.token)
		},
	) ||
	   !workflow.set_update_routine_json(
		ctx,
		"start-timer",
		&state,
		proc(
			routine_ctx: ^workflow.Fiber_Context,
			state: ^Cancellation_Scheduler_State,
			_: string,
		) -> string {
			scope := workflow.cancellation_scope(routine_ctx, state.token)
			defer workflow.cancellation_scope_end(&scope)
			workflow.sleep(routine_ctx, time.Hour)
			state.update_canceled = workflow.cancellation_requested(state.token)
			return "canceled"
		},
	) {
		return "registration failed"
	}
	_ = workflow.await_signal(ctx, "cancel")
	if !workflow.cancellation_request(state.token, "generated cancellation") {
		return "cancellation failed"
	}
	workflow.await_all_handlers_finished(ctx)
	if !state.signal_canceled || !state.update_canceled {
		return "handler drain failed"
	}
	return "ok"
}

append_cancellation_scheduler_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	permutation: int,
	start: ^proto.Coresdk_Workflow_Activation_SignalWorkflow,
	update: ^proto.Coresdk_Workflow_Activation_DoUpdate,
	cancel: ^proto.Coresdk_Workflow_Activation_SignalWorkflow,
) {
	start_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
			value = start,
		},
	}
	update_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
			value = update,
		},
	}
	cancel_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
			value = cancel,
		},
	}
	switch permutation {
	case 0: append(jobs, start_job, update_job, cancel_job)
	case 1: append(jobs, start_job, cancel_job, update_job)
	case 2: append(jobs, update_job, start_job, cancel_job)
	case 3: append(jobs, update_job, cancel_job, start_job)
	case 4: append(jobs, cancel_job, start_job, update_job)
	case 5: append(jobs, cancel_job, update_job, start_job)
	}
}

cancellation_scheduler_permutation_property :: proc(t: ^pbt.T) -> pbt.Result {
	permutation := pbt.draw(t, pbt.int_range(0, 5))
	permutation_labels := [?]string{
		"signal-update-cancel",
		"signal-cancel-update",
		"update-signal-cancel",
		"update-cancel-signal",
		"cancel-signal-update",
		"cancel-update-signal",
	}
	for candidate in 0..<6 {
		pbt.cover(t, permutation == candidate, 10.0, permutation_labels[candidate])
	}

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_cancellation_scheduler_workflow",
		"test-queue",
		cancellation_scheduler_workflow,
	)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	if init_error.message != nil {
		return pbt.error("could not initialize cancellation scheduler runtime")
	}

	input, input_error := codec.encode_json("Odin")
	defer codec.payload_free(&input)
	if input_error != .None {
		return pbt.error("could not encode Workflow input")
	}
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_cancellation_scheduler_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-cancellation-scheduler-run",
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
		return pbt.fail("generated cancellation Workflow did not initialize")
	}
	initial_completion := bytes.clone(initial.completion[:])
	defer delete(initial_completion)

	job_input, job_input_error := codec.encode_json("start")
	defer codec.payload_free(&job_input)
	if job_input_error != .None {
		return pbt.error("could not encode generated handler input")
	}
	job_input_proto := proto.encode_payload(codec.payload_view(&job_input))
	defer delete(job_input_proto.metadata)
	defer delete(job_input_proto.external_payloads)
	start := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "start-waiter"}
	defer delete(start.input)
	append(&start.input, job_input_proto)
	update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "generated-update",
		protocol_instance_id = "generated-update-protocol",
		name = "start-timer",
	}
	defer delete(update.input)
	append(&update.input, job_input_proto)
	cancel := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "cancel"}
	defer delete(cancel.input)
	append(&cancel.input, job_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append_cancellation_scheduler_jobs(
		&activation.jobs,
		permutation,
		&start,
		&update,
		&cancel,
	)
	terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&terminal)
	if terminal.error.message != nil || !terminal.completed || terminal.started_timer ||
	   terminal.signal_handlers_started != 1 || terminal.signal_handlers_completed != 1 ||
	   terminal.updates_accepted != 1 || terminal.updates_completed != 1 {
		return pbt.fail("generated job order did not drain to a stable completion")
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
		return pbt.fail("generated cancellation Workflow did not evict")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 3
	activation.is_replaying = true
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
		return pbt.fail("generated cancellation initialization changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append_cancellation_scheduler_jobs(
		&activation.jobs,
		permutation,
		&start,
		&update,
		&cancel,
	)
	replayed_terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_terminal)
	if replayed_terminal.error.message != nil ||
	   !bytes.equal(terminal_completion, replayed_terminal.completion[:]) {
		return pbt.fail("generated cancellation completion changed on replay")
	}
	return pbt.pass()
}

@(test)
workflow_cancellation_scheduler_drains_generated_job_orders :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow cancellation scheduler drains generated job orders",
		cancellation_scheduler_permutation_property,
		{num_tests = 120, seed = 0x4341_4e43_454c_4650},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

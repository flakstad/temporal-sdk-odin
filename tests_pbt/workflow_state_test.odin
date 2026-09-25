package workflow_pbt_test

import "core:bytes"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import proto "../temporal/core/proto"
import workflow "../temporal/workflow"

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

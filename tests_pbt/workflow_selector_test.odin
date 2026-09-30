package workflow_pbt_test

import "core:bytes"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import workflow "../temporal/workflow"

selector_property_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	order: string,
) -> string {
	first := workflow.start_activity(ctx, "first", "input")
	second := workflow.start_activity(ctx, "second", "input")
	timer := workflow.start_timer(ctx, time.Hour)
	cases: [3]workflow.Selector_Case
	switch order {
	case "0":
		cases = {workflow.selector_activity(first), workflow.selector_activity(second), workflow.selector_timer(timer)}
	case "1":
		cases = {workflow.selector_activity(first), workflow.selector_timer(timer), workflow.selector_activity(second)}
	case "2":
		cases = {workflow.selector_activity(second), workflow.selector_activity(first), workflow.selector_timer(timer)}
	case "3":
		cases = {workflow.selector_activity(second), workflow.selector_timer(timer), workflow.selector_activity(first)}
	case "4":
		cases = {workflow.selector_timer(timer), workflow.selector_activity(first), workflow.selector_activity(second)}
	case:
		cases = {workflow.selector_timer(timer), workflow.selector_activity(second), workflow.selector_activity(first)}
	}
	selected := workflow.selector_select(ctx, cases[:])
	if selected.status != .Selected || selected.index != 0 {
		return "wrong-index"
	}
	switch order {
	case "0", "1":
		if selected.kind != .Activity do return "wrong-kind"
		return workflow.await_activity(ctx, first)
	case "2", "3":
		if selected.kind != .Activity do return "wrong-kind"
		return workflow.await_activity(ctx, second)
	case:
		if selected.kind != .Timer || workflow.await_timer(ctx, timer) != .Fired {
			return "wrong-kind"
		}
		return "timer"
	}
}

append_selector_property_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	permutation: int,
	first: ^proto.Coresdk_Workflow_Activation_ResolveActivity,
	second: ^proto.Coresdk_Workflow_Activation_ResolveActivity,
	timer: ^proto.Coresdk_Workflow_Activation_FireTimer,
) {
	first_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{value = first},
	}
	second_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{value = second},
	}
	timer_job := proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{value = timer},
	}
	switch permutation {
	case 0: append(jobs, first_job, second_job, timer_job)
	case 1: append(jobs, first_job, timer_job, second_job)
	case 2: append(jobs, second_job, first_job, timer_job)
	case 3: append(jobs, second_job, timer_job, first_job)
	case 4: append(jobs, timer_job, first_job, second_job)
	case 5: append(jobs, timer_job, second_job, first_job)
	}
}

selector_order_property :: proc(t: ^pbt.T) -> pbt.Result {
	case_order := pbt.draw(t, pbt.int_range(0, 5))
	job_order := pbt.draw(t, pbt.int_range(0, 5))
	labels := [?]string{"a-b-t", "a-t-b", "b-a-t", "b-t-a", "t-a-b", "t-b-a"}
	job_labels := [?]string{
		"a-b-t jobs",
		"a-t-b jobs",
		"b-a-t jobs",
		"b-t-a jobs",
		"t-a-b jobs",
		"t-b-a jobs",
	}
	for label, index in labels {
		pbt.cover(t, case_order == index, 10.0, label)
		pbt.cover(t, job_order == index, 10.0, job_labels[index])
	}

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_selector_workflow",
		"test-queue",
		selector_property_workflow,
	)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	if init_error.message != nil {
		return pbt.error("could not initialize selector runtime")
	}

	// The workflow switches on a compact stable digit rather than a display label.
	order_digits := [?]string{"0", "1", "2", "3", "4", "5"}
	input, input_error := codec.encode_json(order_digits[case_order])
	defer codec.payload_free(&input)
	if input_error != .None {
		return pbt.error("could not encode selector order")
	}
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{workflow_type = "pbt_selector_workflow"}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-selector-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{value = &initialize},
		},
	)
	initial := process_activation(&runtime, &activation)
	defer destroy_process_result(&initial)
	if initial.error.message != nil || initial.scheduled_activity_count != 2 ||
	   !initial.started_timer || !initial.waiting_selector {
		return pbt.fail("generated selector Workflow did not initialize")
	}
	initial_completion := bytes.clone(initial.completion[:])
	defer delete(initial_completion)

	first_payload, first_payload_error := codec.encode_json("first")
	second_payload, second_payload_error := codec.encode_json("second")
	defer codec.payload_free(&first_payload)
	defer codec.payload_free(&second_payload)
	if first_payload_error != .None || second_payload_error != .None {
		return pbt.error("could not encode selector Activity results")
	}
	first_proto := proto.encode_payload(codec.payload_view(&first_payload))
	second_proto := proto.encode_payload(codec.payload_view(&second_payload))
	defer delete(first_proto.metadata)
	defer delete(first_proto.external_payloads)
	defer delete(second_proto.metadata)
	defer delete(second_proto.external_payloads)
	first_success := proto.Coresdk_Activity_Result_Success{result = &first_proto}
	second_success := proto.Coresdk_Activity_Result_Success{result = &second_proto}
	first_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{value = &first_success},
	}
	second_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{value = &second_success},
	}
	resolve_first := proto.Coresdk_Workflow_Activation_ResolveActivity{seq = 1, result = &first_resolution}
	resolve_second := proto.Coresdk_Workflow_Activation_ResolveActivity{seq = 2, result = &second_resolution}
	fire_timer := proto.Coresdk_Workflow_Activation_FireTimer{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 12
	append_selector_property_jobs(
		&activation.jobs,
		job_order,
		&resolve_first,
		&resolve_second,
		&fire_timer,
	)
	terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&terminal)
	if terminal.error.message != nil || !terminal.completed || terminal.waiting_selector {
		return pbt.fail("generated selector activation did not complete")
	}
	result_payload, found := clone_pbt_workflow_result(terminal.completion[:])
	defer codec.payload_free(&result_payload)
	if !found {
		return pbt.fail("selector completion had no result")
	}
	result: string
	if codec.decode_json(codec.payload_view(&result_payload), &result) != .None {
		return pbt.fail("selector completion result did not decode")
	}
	defer delete(result)
	expected := "timer"
	if case_order < 2 {
		expected = "first"
	} else if case_order < 4 {
		expected = "second"
	}
	if result != expected {
		return pbt.fail("selector did not choose the first ready declaration")
	}
	terminal_completion := bytes.clone(terminal.completion[:])
	defer delete(terminal_completion)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{value = &eviction},
		},
	)
	evicted := process_activation(&runtime, &activation)
	defer destroy_process_result(&evicted)
	if evicted.error.message != nil || !evicted.evicted {
		return pbt.fail("generated selector Workflow did not evict")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{value = &initialize},
		},
	)
	replayed_initial := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_initial)
	if replayed_initial.error.message != nil ||
	   !bytes.equal(initial_completion, replayed_initial.completion[:]) {
		return pbt.fail("selector initialization changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 12
	append_selector_property_jobs(
		&activation.jobs,
		job_order,
		&resolve_first,
		&resolve_second,
		&fire_timer,
	)
	replayed_terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_terminal)
	if replayed_terminal.error.message != nil ||
	   !bytes.equal(terminal_completion, replayed_terminal.completion[:]) {
		return pbt.fail("selector completion changed on replay")
	}
	return pbt.pass()
}

@(test)
workflow_selector_is_biased_across_generated_job_orders :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow selector is declaration-biased across generated Core job orders",
		selector_order_property,
		{num_tests = 180, seed = 0x5345_4c45_4354_4f52},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

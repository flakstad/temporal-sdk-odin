package workflow_pbt_test

import "core:bytes"
import "core:fmt"
import "core:mem/virtual"
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
	timer := workflow.start_timer_with_options(
		ctx,
		{duration = time.Hour, summary = order},
	)
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

selector_timer_summary_matches :: proc(
	data: []u8,
	expected: codec.Payload_View,
) -> bool {
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
		if status.value == nil {
			return false
		}
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Timer:
				if variant.value == nil || command.user_metadata == nil ||
				   command.user_metadata.summary == nil {
					return false
				}
				summary := command.user_metadata.summary
				if !bytes.equal(summary.data, expected.data) ||
				   len(summary.metadata) != len(expected.metadata) {
					return false
				}
				for entry, index in summary.metadata {
					if entry.key != expected.metadata[index].key ||
					   !bytes.equal(entry.value, expected.metadata[index].value) {
						return false
					}
				}
				return true
			}
		}
	}
	return false
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
	if !selector_timer_summary_matches(initial.completion[:], codec.payload_view(&input)) {
		return pbt.fail("generated selector Timer summary did not reach command metadata")
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

join_property_workflow :: proc(ctx: ^workflow.Fiber_Context, _: string) -> string {
	first := workflow.start_activity(ctx, "first", "input")
	second := workflow.start_activity(ctx, "second", "input")
	timer := workflow.start_timer(ctx, time.Hour)
	cases := [?]workflow.Selector_Case{
		workflow.selector_activity(first),
		workflow.selector_activity(second),
		workflow.selector_timer(timer),
	}
	if workflow.join_all(ctx, cases[:]) != .Ready {
		return "canceled"
	}
	first_result := workflow.await_activity(ctx, first)
	second_result := workflow.await_activity(ctx, second)
	if workflow.await_timer(ctx, timer) != .Fired {
		return "timer-not-fired"
	}
	if first_result == "first" && second_result == "second" {
		return "ordered"
	}
	return "misordered"
}

append_join_property_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	permutation, begin, end: int,
	first: ^proto.Coresdk_Workflow_Activation_ResolveActivity,
	second: ^proto.Coresdk_Workflow_Activation_ResolveActivity,
	timer: ^proto.Coresdk_Workflow_Activation_FireTimer,
) {
	orders := [6][3]int{
		{0, 1, 2},
		{0, 2, 1},
		{1, 0, 2},
		{1, 2, 0},
		{2, 0, 1},
		{2, 1, 0},
	}
	for position in begin..<end {
		switch orders[permutation][position] {
		case 0:
			append(
				jobs,
				proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
					variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{value = first},
				},
			)
		case 1:
			append(
				jobs,
				proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
					variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{value = second},
				},
			)
		case 2:
			append(
				jobs,
				proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
					variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{value = timer},
				},
			)
		}
	}
}

join_order_property :: proc(t: ^pbt.T) -> pbt.Result {
	job_order := pbt.draw(t, pbt.int_range(0, 5))
	split := pbt.draw(t, pbt.int_range(1, 2))
	job_labels := [?]string{
		"a-b-t join jobs",
		"a-t-b join jobs",
		"b-a-t join jobs",
		"b-t-a join jobs",
		"t-a-b join jobs",
		"t-b-a join jobs",
	}
	for label, index in job_labels {
		pbt.cover(t, job_order == index, 10.0, label)
	}
	pbt.cover(t, split == 1, 40.0, "one early join result")
	pbt.cover(t, split == 2, 40.0, "two early join results")

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_join_workflow",
		"test-queue",
		join_property_workflow,
	)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	if init_error.message != nil {
		return pbt.error("could not initialize join runtime")
	}

	input, input_error := codec.encode_json("input")
	defer codec.payload_free(&input)
	if input_error != .None {
		return pbt.error("could not encode join input")
	}
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_join_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-join-run",
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
	   !initial.started_timer || !initial.waiting_join {
		return pbt.fail("generated join Workflow did not initialize")
	}
	initial_completion := bytes.clone(initial.completion[:])
	defer delete(initial_completion)

	first_payload, first_payload_error := codec.encode_json("first")
	second_payload, second_payload_error := codec.encode_json("second")
	defer codec.payload_free(&first_payload)
	defer codec.payload_free(&second_payload)
	if first_payload_error != .None || second_payload_error != .None {
		return pbt.error("could not encode join Activity results")
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
	activation.history_length = 8
	append_join_property_jobs(
		&activation.jobs,
		job_order,
		0,
		split,
		&resolve_first,
		&resolve_second,
		&fire_timer,
	)
	partial := process_activation(&runtime, &activation)
	defer destroy_process_result(&partial)
	if partial.error.message != nil || partial.completed || !partial.waiting_join {
		return pbt.fail("generated partial join resumed too early")
	}
	partial_completion := bytes.clone(partial.completion[:])
	defer delete(partial_completion)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append_join_property_jobs(
		&activation.jobs,
		job_order,
		split,
		3,
		&resolve_first,
		&resolve_second,
		&fire_timer,
	)
	terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&terminal)
	if terminal.error.message != nil || !terminal.completed || terminal.waiting_join {
		return pbt.fail("generated complete join did not resume")
	}
	result_payload, found := clone_pbt_workflow_result(terminal.completion[:])
	defer codec.payload_free(&result_payload)
	if !found {
		return pbt.fail("join completion had no result")
	}
	result: string
	if codec.decode_json(codec.payload_view(&result_payload), &result) != .None {
		return pbt.fail("join completion result did not decode")
	}
	defer delete(result)
	if result != "ordered" {
		return pbt.fail("join results did not preserve declaration order")
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
		return pbt.fail("generated join Workflow did not evict")
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
		return pbt.fail("join initialization changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append_join_property_jobs(
		&activation.jobs,
		job_order,
		0,
		split,
		&resolve_first,
		&resolve_second,
		&fire_timer,
	)
	replayed_partial := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_partial)
	if replayed_partial.error.message != nil ||
	   !bytes.equal(partial_completion, replayed_partial.completion[:]) {
		return pbt.fail("partial join changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append_join_property_jobs(
		&activation.jobs,
		job_order,
		split,
		3,
		&resolve_first,
		&resolve_second,
		&fire_timer,
	)
	replayed_terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_terminal)
	if replayed_terminal.error.message != nil ||
	   !bytes.equal(terminal_completion, replayed_terminal.completion[:]) {
		return pbt.fail("terminal join changed on replay")
	}
	return pbt.pass()
}

@(test)
workflow_join_all_waits_across_generated_resolution_orders :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow join waits across generated Core resolution orders",
		join_order_property,
		{num_tests = 180, seed = 0x4a4f_494e_5f41_4c4c},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

signal_join_property_workflow :: proc(ctx: ^workflow.Fiber_Context, flags: string) -> string {
	reverse_cases := len(flags) > 0 && flags[0] == '1'
	cases := [2]workflow.Selector_Case{
		workflow.selector_signal("alpha"),
		workflow.selector_signal("beta"),
	}
	if reverse_cases {
		cases = {
			workflow.selector_signal("beta"),
			workflow.selector_signal("alpha"),
		}
	}
	results: [2]workflow.Join_Case_Result
	if workflow.join_all_into(ctx, cases[:], results[:]) != .Ready {
		return "canceled"
	}
	values: [2]string
	for &result, index in results {
		if !result.present || result.kind != .Signal || len(result.signal.values) != 1 ||
		   result.signal.metadata.name != cases[index].signal_name {
			return "wrong-result"
		}
		if codec.decode_json(result.signal.values[0], &values[index]) != .None {
			return "decode-failed"
		}
	}
	return fmt.aprintf(
		"%s=%s/%s=%s",
		results[0].signal.metadata.name,
		values[0],
		results[1].signal.metadata.name,
		values[1],
	)
}

append_signal_join_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	reverse_delivery: bool,
	begin, end: int,
	alpha, beta: ^proto.Coresdk_Workflow_Activation_SignalWorkflow,
) {
	ordered := [2]^proto.Coresdk_Workflow_Activation_SignalWorkflow{alpha, beta}
	if reverse_delivery {
		ordered = {beta, alpha}
	}
	for index in begin..<end {
		append(
			jobs,
			proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
				variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
					value = ordered[index],
				},
			},
		)
	}
}

signal_join_property :: proc(t: ^pbt.T) -> pbt.Result {
	reverse_cases := pbt.draw(t, pbt.boolean())
	reverse_delivery := pbt.draw(t, pbt.boolean())
	split_activations := pbt.draw(t, pbt.boolean())
	pbt.cover(t, reverse_cases, 35.0, "reversed Signal join cases")
	pbt.cover(t, reverse_delivery, 35.0, "reversed Signal delivery")
	pbt.cover(t, split_activations, 35.0, "split Signal activations")

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_signal_join_workflow",
		"test-queue",
		signal_join_property_workflow,
	)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	if init_error.message != nil {
		return pbt.error("could not initialize Signal join runtime")
	}

	flags := reverse_cases ? "1" : "0"
	input, input_error := codec.encode_json(flags)
	defer codec.payload_free(&input)
	if input_error != .None {
		return pbt.error("could not encode Signal join input")
	}
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_signal_join_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-signal-join-run",
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
	if initial.error.message != nil || !initial.waiting_join || initial.completed {
		return pbt.fail("generated Signal join Workflow did not initialize")
	}
	initial_completion := bytes.clone(initial.completion[:])
	defer delete(initial_completion)

	alpha_payload, alpha_error := codec.encode_json("A")
	beta_payload, beta_error := codec.encode_json("B")
	defer codec.payload_free(&alpha_payload)
	defer codec.payload_free(&beta_payload)
	if alpha_error != .None || beta_error != .None {
		return pbt.error("could not encode Signal join values")
	}
	alpha_proto := proto.encode_payload(codec.payload_view(&alpha_payload))
	beta_proto := proto.encode_payload(codec.payload_view(&beta_payload))
	defer delete(alpha_proto.metadata)
	defer delete(alpha_proto.external_payloads)
	defer delete(beta_proto.metadata)
	defer delete(beta_proto.external_payloads)
	alpha_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "alpha",
		identity = "alpha-client",
		originating_event_id = 41,
	}
	beta_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "beta",
		identity = "beta-client",
		originating_event_id = 43,
	}
	defer delete(alpha_signal.input)
	defer delete(beta_signal.input)
	append(&alpha_signal.input, alpha_proto)
	append(&beta_signal.input, beta_proto)

	first_end := split_activations ? 1 : 2
	partial_completion: []u8
	defer delete(partial_completion)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 6
	append_signal_join_jobs(
		&activation.jobs,
		reverse_delivery,
		0,
		first_end,
		&alpha_signal,
		&beta_signal,
	)
	first_delivery := process_activation(&runtime, &activation)
	defer destroy_process_result(&first_delivery)
	if first_delivery.error.message != nil {
		return pbt.fail("generated Signal join delivery failed")
	}
	if split_activations {
		if first_delivery.completed || !first_delivery.waiting_join {
			return pbt.fail("Signal join resumed after only one unique Signal")
		}
		partial_completion = bytes.clone(first_delivery.completion[:])
	} else if !first_delivery.completed || first_delivery.waiting_join {
		return pbt.fail("Signal join did not resume after both Signals")
	}

	terminal: workflow.Process_Result
	if split_activations {
		delete(activation.jobs)
		activation.jobs = nil
		activation.history_length = 8
		append_signal_join_jobs(
			&activation.jobs,
			reverse_delivery,
			1,
			2,
			&alpha_signal,
			&beta_signal,
		)
		terminal = process_activation(&runtime, &activation)
		if terminal.error.message != nil || !terminal.completed || terminal.waiting_join {
			return pbt.fail("split Signal join did not complete")
		}
	} else {
		terminal = first_delivery
	}
	defer {
		if split_activations {
			destroy_process_result(&terminal)
		}
	}
	terminal_completion := bytes.clone(terminal.completion[:])
	defer delete(terminal_completion)
	result_payload, found := clone_pbt_workflow_result(terminal.completion[:])
	defer codec.payload_free(&result_payload)
	if !found {
		return pbt.fail("Signal join completion had no result")
	}
	result: string
	if codec.decode_json(codec.payload_view(&result_payload), &result) != .None {
		return pbt.fail("Signal join completion result did not decode")
	}
	defer delete(result)
	expected := reverse_cases ? "beta=B/alpha=A" : "alpha=A/beta=B"
	if result != expected {
		return pbt.fail("Signal join results did not follow case slots")
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
	evicted := process_activation(&runtime, &activation)
	defer destroy_process_result(&evicted)
	if evicted.error.message != nil || !evicted.evicted {
		return pbt.fail("Signal join Workflow did not evict")
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
		return pbt.fail("Signal join initialization changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 6
	append_signal_join_jobs(
		&activation.jobs,
		reverse_delivery,
		0,
		first_end,
		&alpha_signal,
		&beta_signal,
	)
	replayed_first := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_first)
	if replayed_first.error.message != nil {
		return pbt.fail("Signal join first replay delivery failed")
	}
	if split_activations {
		if !bytes.equal(partial_completion, replayed_first.completion[:]) {
			return pbt.fail("Signal join partial completion changed on replay")
		}
		delete(activation.jobs)
		activation.jobs = nil
		activation.history_length = 8
		append_signal_join_jobs(
			&activation.jobs,
			reverse_delivery,
			1,
			2,
			&alpha_signal,
			&beta_signal,
		)
		replayed_terminal := process_activation(&runtime, &activation)
		defer destroy_process_result(&replayed_terminal)
		if replayed_terminal.error.message != nil ||
		   !bytes.equal(terminal_completion, replayed_terminal.completion[:]) {
			return pbt.fail("Signal join terminal completion changed on replay")
		}
	} else if !bytes.equal(terminal_completion, replayed_first.completion[:]) {
		return pbt.fail("same-activation Signal join changed on replay")
	}
	return pbt.pass()
}

@(test)
workflow_signal_join_is_deterministic_across_case_and_delivery_orders :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow Signal join is deterministic across case and delivery orders",
		signal_join_property,
		{num_tests = 180, seed = 0x5349_474e_414c_4a4f},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

mixed_join_property_workflow :: proc(ctx: ^workflow.Fiber_Context, flags: string) -> string {
	reverse_cases := len(flags) > 0 && flags[0] == '1'
	activity_future := workflow.start_activity(ctx, "mixed", "input")
	cases := [2]workflow.Selector_Case{
		workflow.selector_activity(activity_future),
		workflow.selector_signal("approval"),
	}
	if reverse_cases {
		cases = {
			workflow.selector_signal("approval"),
			workflow.selector_activity(activity_future),
		}
	}
	results: [2]workflow.Join_Case_Result
	if workflow.join_all_into(ctx, cases[:], results[:]) != .Ready {
		return "canceled"
	}
	signal_index := reverse_cases ? 0 : 1
	activity_index := reverse_cases ? 1 : 0
	if !results[signal_index].present || !results[activity_index].present ||
	   results[signal_index].kind != .Signal ||
	   results[activity_index].kind != .Activity ||
	   len(results[signal_index].signal.values) != 1 ||
	   len(results[activity_index].signal.values) != 0 {
		return "wrong-result-slots"
	}
	signal_value: string
	if codec.decode_json(
		results[signal_index].signal.values[0],
		&signal_value,
	) != .None {
		return "signal-decode-failed"
	}
	activity_value := workflow.await_activity(ctx, activity_future)
	return fmt.aprintf("%s/%s", activity_value, signal_value)
}

append_mixed_join_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	signal_first: bool,
	begin, end: int,
	activity: ^proto.Coresdk_Workflow_Activation_ResolveActivity,
	signal: ^proto.Coresdk_Workflow_Activation_SignalWorkflow,
) {
	for position in begin..<end {
		use_signal := signal_first ? position == 0 : position == 1
		if use_signal {
			append(
				jobs,
				proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
					variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
						value = signal,
					},
				},
			)
		} else {
			append(
				jobs,
				proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
					variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
						value = activity,
					},
				},
			)
		}
	}
}

mixed_join_property :: proc(t: ^pbt.T) -> pbt.Result {
	reverse_cases := pbt.draw(t, pbt.boolean())
	signal_first := pbt.draw(t, pbt.boolean())
	split_activations := pbt.draw(t, pbt.boolean())
	pbt.cover(t, reverse_cases, 35.0, "reversed mixed join cases")
	pbt.cover(t, signal_first, 35.0, "Signal before Activity resolution")
	pbt.cover(t, split_activations, 35.0, "split mixed join activations")

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_mixed_join_workflow",
		"test-queue",
		mixed_join_property_workflow,
	)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	if init_error.message != nil {
		return pbt.error("could not initialize mixed join runtime")
	}

	flags := reverse_cases ? "1" : "0"
	input, input_error := codec.encode_json(flags)
	defer codec.payload_free(&input)
	if input_error != .None {
		return pbt.error("could not encode mixed join input")
	}
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_mixed_join_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-mixed-join-run",
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
	if initial.error.message != nil || initial.scheduled_activity_count != 1 ||
	   !initial.waiting_join || initial.completed {
		return pbt.fail("generated mixed join Workflow did not initialize")
	}
	initial_completion := bytes.clone(initial.completion[:])
	defer delete(initial_completion)

	activity_payload, activity_error := codec.encode_json("activity")
	signal_payload, signal_error := codec.encode_json("signal")
	defer codec.payload_free(&activity_payload)
	defer codec.payload_free(&signal_payload)
	if activity_error != .None || signal_error != .None {
		return pbt.error("could not encode mixed join values")
	}
	activity_proto := proto.encode_payload(codec.payload_view(&activity_payload))
	signal_proto := proto.encode_payload(codec.payload_view(&signal_payload))
	defer delete(activity_proto.metadata)
	defer delete(activity_proto.external_payloads)
	defer delete(signal_proto.metadata)
	defer delete(signal_proto.external_payloads)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &activity_proto}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve_activity := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "approval",
		identity = "mixed-join-client",
		originating_event_id = 47,
	}
	defer delete(signal.input)
	append(&signal.input, signal_proto)

	first_end := split_activations ? 1 : 2
	partial_completion: []u8
	defer delete(partial_completion)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append_mixed_join_jobs(
		&activation.jobs,
		signal_first,
		0,
		first_end,
		&resolve_activity,
		&signal,
	)
	first_delivery := process_activation(&runtime, &activation)
	defer destroy_process_result(&first_delivery)
	if first_delivery.error.message != nil {
		return pbt.fail("generated mixed join delivery failed")
	}
	if split_activations {
		if first_delivery.completed || !first_delivery.waiting_join {
			return pbt.fail("mixed join resumed after only one case")
		}
		partial_completion = bytes.clone(first_delivery.completion[:])
	} else if !first_delivery.completed || first_delivery.waiting_join {
		return pbt.fail("mixed join did not resume after both cases")
	}

	terminal: workflow.Process_Result
	if split_activations {
		delete(activation.jobs)
		activation.jobs = nil
		activation.history_length = 11
		append_mixed_join_jobs(
			&activation.jobs,
			signal_first,
			1,
			2,
			&resolve_activity,
			&signal,
		)
		terminal = process_activation(&runtime, &activation)
		if terminal.error.message != nil || !terminal.completed || terminal.waiting_join {
			return pbt.fail("split mixed join did not complete")
		}
	} else {
		terminal = first_delivery
	}
	defer {
		if split_activations {
			destroy_process_result(&terminal)
		}
	}
	terminal_completion := bytes.clone(terminal.completion[:])
	defer delete(terminal_completion)
	result_payload, found := clone_pbt_workflow_result(terminal.completion[:])
	defer codec.payload_free(&result_payload)
	if !found {
		return pbt.fail("mixed join completion had no result")
	}
	result: string
	if codec.decode_json(codec.payload_view(&result_payload), &result) != .None {
		return pbt.fail("mixed join completion result did not decode")
	}
	defer delete(result)
	if result != "activity/signal" {
		return pbt.fail("mixed join results did not preserve typed slots")
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
	evicted := process_activation(&runtime, &activation)
	defer destroy_process_result(&evicted)
	if evicted.error.message != nil || !evicted.evicted {
		return pbt.fail("mixed join Workflow did not evict")
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
		return pbt.fail("mixed join initialization changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append_mixed_join_jobs(
		&activation.jobs,
		signal_first,
		0,
		first_end,
		&resolve_activity,
		&signal,
	)
	replayed_first := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_first)
	if replayed_first.error.message != nil {
		return pbt.fail("mixed join first replay delivery failed")
	}
	if split_activations {
		if !bytes.equal(partial_completion, replayed_first.completion[:]) {
			return pbt.fail("mixed join partial completion changed on replay")
		}
		delete(activation.jobs)
		activation.jobs = nil
		activation.history_length = 11
		append_mixed_join_jobs(
			&activation.jobs,
			signal_first,
			1,
			2,
			&resolve_activity,
			&signal,
		)
		replayed_terminal := process_activation(&runtime, &activation)
		defer destroy_process_result(&replayed_terminal)
		if replayed_terminal.error.message != nil ||
		   !bytes.equal(terminal_completion, replayed_terminal.completion[:]) {
			return pbt.fail("mixed join terminal completion changed on replay")
		}
	} else if !bytes.equal(terminal_completion, replayed_first.completion[:]) {
		return pbt.fail("same-activation mixed join changed on replay")
	}
	return pbt.pass()
}

@(test)
workflow_mixed_join_is_deterministic_across_case_and_delivery_orders :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow mixed join is deterministic across case and delivery orders",
		mixed_join_property,
		{num_tests = 180, seed = 0x4d49_5845_445f_4a4f},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Child_Composition_Input :: struct {
	start_selector_reverse:      bool,
	start_join_order:            int,
	completion_selector_reverse: bool,
	completion_join_order:       int,
	start_job_order:             int,
	completion_job_reverse:      bool,
	cancel_target:               int,
}

child_composition_order :: proc(index: int) -> [3]int {
	switch index {
	case 0: return {0, 1, 2}
	case 1: return {0, 2, 1}
	case 2: return {1, 0, 2}
	case 3: return {1, 2, 0}
	case 4: return {2, 0, 1}
	case:   return {2, 1, 0}
	}
}

child_composition_decode_input :: proc(encoded: string) -> (Child_Composition_Input, bool) {
	if len(encoded) != 7 || encoded[0] > '1' || encoded[1] > '5' ||
	   encoded[2] > '1' || encoded[3] > '5' || encoded[4] > '5' ||
	   encoded[5] > '1' || encoded[6] > '2' {
		return {}, false
	}
	for value in encoded {
		if value < '0' {
			return {}, false
		}
	}
	return {
		start_selector_reverse = encoded[0] == '1',
		start_join_order = int(encoded[1] - '0'),
		completion_selector_reverse = encoded[2] == '1',
		completion_join_order = int(encoded[3] - '0'),
		start_job_order = int(encoded[4] - '0'),
		completion_job_reverse = encoded[5] == '1',
		cancel_target = int(encoded[6] - '0'),
	}, true
}

child_composition_property_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	encoded_input: string,
) -> string {
	input, input_valid := child_composition_decode_input(encoded_input)
	if !input_valid {
		return "invalid-input"
	}
	child_ids := [?]string{"pbt-child-a", "pbt-child-b", "pbt-child-c"}
	children: [3]workflow.Child_Workflow_Future
	for child_id, index in child_ids {
		children[index] = workflow.start_child_workflow_json(
			ctx,
			"pbt_child_workflow",
			child_id,
			{
				workflow_id = child_id,
				cancellation_type = .Wait_Cancellation_Completed,
			},
		)
	}

	start_selector_cases := [2]workflow.Selector_Case{
		workflow.selector_child_workflow_start(children[0]),
		workflow.selector_child_workflow_start(children[1]),
	}
	if input.start_selector_reverse {
		start_selector_cases = {
			workflow.selector_child_workflow_start(children[1]),
			workflow.selector_child_workflow_start(children[0]),
		}
	}
	selected_start := workflow.selector_select(ctx, start_selector_cases[:])
	if selected_start.status != .Selected || selected_start.index != 0 ||
	   selected_start.kind != .Child_Workflow_Start {
		return "start-selector-mismatch"
	}
	selected_start_result := workflow.await_child_workflow_start(
		ctx,
		start_selector_cases[0].child_workflow,
	)
	if selected_start_result.status != .Succeeded {
		return "selected-start-failed"
	}

	start_order := child_composition_order(input.start_join_order)
	start_join_cases: [3]workflow.Selector_Case
	for child_index, case_index in start_order {
		start_join_cases[case_index] = workflow.selector_child_workflow_start(
			children[child_index],
		)
	}
	if workflow.join_all(ctx, start_join_cases[:]) != .Ready {
		return "start-join-canceled"
	}

	if input.cancel_target < 0 || input.cancel_target >= len(children) ||
	   !workflow.cancel_child_workflow(
		ctx,
		children[input.cancel_target],
		"generated Child composition cancellation",
	) {
		return "child-cancel-failed"
	}

	completion_selector_cases := [2]workflow.Selector_Case{
		workflow.selector_child_workflow(children[0]),
		workflow.selector_child_workflow(children[1]),
	}
	if input.completion_selector_reverse {
		completion_selector_cases = {
			workflow.selector_child_workflow(children[1]),
			workflow.selector_child_workflow(children[0]),
		}
	}
	selected_completion := workflow.selector_select(ctx, completion_selector_cases[:])
	if selected_completion.status != .Selected || selected_completion.index != 0 ||
	   selected_completion.kind != .Child_Workflow {
		return "completion-selector-mismatch"
	}
	selected_index := 0
	if input.completion_selector_reverse {
		selected_index = 1
	}
	selected_result := workflow.await_child_workflow_result(
		ctx,
		completion_selector_cases[0].child_workflow,
	)
	if selected_index == input.cancel_target {
		if selected_result.status != .Canceled {
			return "selected-child-not-canceled"
		}
	} else if selected_result.status != .Succeeded ||
	          selected_result.value != "completed" {
		return "selected-child-not-completed"
	}

	completion_order := child_composition_order(input.completion_join_order)
	completion_join_cases: [3]workflow.Selector_Case
	for child_index, case_index in completion_order {
		completion_join_cases[case_index] = workflow.selector_child_workflow(
			children[child_index],
		)
	}
	if workflow.join_all(ctx, completion_join_cases[:]) != .Ready {
		return "completion-join-canceled"
	}
	for child, index in children {
		result := workflow.await_child_workflow_result(ctx, child)
		if index == input.cancel_target {
			if result.status != .Canceled {
				return "joined-child-not-canceled"
			}
		} else if result.status != .Succeeded || result.value != "completed" {
			return "joined-child-not-completed"
		}
	}
	return "composed"
}

append_child_composition_start_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	permutation: int,
	resolutions: ^[3]proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart,
) {
	order := child_composition_order(permutation)
	for resolution_index in order {
		append(
			jobs,
			proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
				variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
					value = &resolutions[resolution_index],
				},
			},
		)
	}
}

append_child_composition_result_job :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	child_index, cancel_target: int,
	completed: ^[3]proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecution,
	canceled: ^[3]proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecution,
) {
	resolution := &completed[child_index]
	if child_index == cancel_target {
		resolution = &canceled[child_index]
	}
	append(
		jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution{
				value = resolution,
			},
		},
	)
}

child_composition_property :: proc(t: ^pbt.T) -> pbt.Result {
	input := Child_Composition_Input{
		start_selector_reverse = pbt.draw(t, pbt.boolean()),
		start_join_order = pbt.draw(t, pbt.int_range(0, 5)),
		completion_selector_reverse = pbt.draw(t, pbt.boolean()),
		completion_join_order = pbt.draw(t, pbt.int_range(0, 5)),
		start_job_order = pbt.draw(t, pbt.int_range(0, 5)),
		completion_job_reverse = pbt.draw(t, pbt.boolean()),
		cancel_target = pbt.draw(t, pbt.int_range(0, 2)),
	}
	pbt.cover(t, input.start_selector_reverse, 35.0, "reversed Child start selector")
	pbt.cover(t, input.completion_selector_reverse, 35.0, "reversed Child completion selector")
	pbt.cover(t, input.completion_job_reverse, 35.0, "reversed Child completion jobs")
	cancel_labels := [?]string{"cancel Child A", "cancel Child B", "cancel Child C"}
	for label, index in cancel_labels {
		pbt.cover(t, input.cancel_target == index, 20.0, label)
	}
	start_join_labels := [?]string{
		"start join a-b-c", "start join a-c-b", "start join b-a-c",
		"start join b-c-a", "start join c-a-b", "start join c-b-a",
	}
	completion_join_labels := [?]string{
		"completion join a-b-c", "completion join a-c-b", "completion join b-a-c",
		"completion join b-c-a", "completion join c-a-b", "completion join c-b-a",
	}
	start_job_labels := [?]string{
		"start jobs a-b-c", "start jobs a-c-b", "start jobs b-a-c",
		"start jobs b-c-a", "start jobs c-a-b", "start jobs c-b-a",
	}
	for label, index in start_join_labels {
		pbt.cover(t, input.start_join_order == index, 8.0, label)
		pbt.cover(t, input.completion_join_order == index, 8.0, completion_join_labels[index])
		pbt.cover(t, input.start_job_order == index, 8.0, start_job_labels[index])
	}

	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"pbt_child_composition_workflow",
		"test-queue",
		child_composition_property_workflow,
	)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	if init_error.message != nil {
		return pbt.error("could not initialize Child composition runtime")
	}

	encoded_input := [7]u8{
		'0' + u8(input.start_selector_reverse),
		'0' + u8(input.start_join_order),
		'0' + u8(input.completion_selector_reverse),
		'0' + u8(input.completion_join_order),
		'0' + u8(input.start_job_order),
		'0' + u8(input.completion_job_reverse),
		'0' + u8(input.cancel_target),
	}
	input_payload, input_error := codec.encode_json(string(encoded_input[:]))
	defer codec.payload_free(&input_payload)
	if input_error != .None {
		return pbt.error("could not encode Child composition input")
	}
	input_proto := proto.encode_payload(codec.payload_view(&input_payload))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_child_composition_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-child-composition-run",
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
	if initial.error.message != nil || initial.started_child_workflow_count != 3 ||
	   !initial.waiting_selector {
		return pbt.fail("generated Child composition did not initialize")
	}
	initial_completion := bytes.clone(initial.completion[:])
	defer delete(initial_completion)

	start_successes := [3]proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartSuccess{
		{run_id = "pbt-child-a-run"},
		{run_id = "pbt-child-b-run"},
		{run_id = "pbt-child-c-run"},
	}
	start_resolutions: [3]proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart
	for index in 0..<3 {
		start_resolutions[index] = {
			seq = u32(index + 1),
			status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded{
				value = &start_successes[index],
			},
		}
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 12
	append_child_composition_start_jobs(
		&activation.jobs,
		input.start_job_order,
		&start_resolutions,
	)
	started := process_activation(&runtime, &activation)
	defer destroy_process_result(&started)
	if started.error.message != nil || !started.requested_child_workflow_cancellation ||
	   started.canceled_child_workflow_seq != u32(input.cancel_target + 1) ||
	   !started.waiting_selector {
		return pbt.fail("generated Child starts did not select, join, and cancel")
	}
	started_completion := bytes.clone(started.completion[:])
	defer delete(started_completion)

	completed_payload, completed_error := codec.encode_json("completed")
	defer codec.payload_free(&completed_payload)
	if completed_error != .None {
		return pbt.error("could not encode generated Child completion")
	}
	completed_proto := proto.encode_payload(codec.payload_view(&completed_payload))
	defer delete(completed_proto.metadata)
	defer delete(completed_proto.external_payloads)
	child_success := proto.Coresdk_Child_Workflow_Success{result = &completed_proto}
	completed_result := proto.Coresdk_Child_Workflow_ChildWorkflowResult{
		status = proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Completed{
			value = &child_success,
		},
	}
	canceled_info := proto.Temporal_Api_Failure_V1_CanceledFailureInfo{}
	canceled_failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "generated Child canceled",
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info{
			value = &canceled_info,
		},
	}
	child_cancellation := proto.Coresdk_Child_Workflow_Cancellation{failure = &canceled_failure}
	canceled_result := proto.Coresdk_Child_Workflow_ChildWorkflowResult{
		status = proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Cancelled{
			value = &child_cancellation,
		},
	}
	completed_resolutions: [3]proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecution
	canceled_resolutions: [3]proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecution
	for index in 0..<3 {
		completed_resolutions[index] = {seq = u32(index + 1), result = &completed_result}
		canceled_resolutions[index] = {seq = u32(index + 1), result = &canceled_result}
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 20
	first_child, second_child := 0, 1
	if input.completion_job_reverse {
		first_child, second_child = 1, 0
	}
	append_child_composition_result_job(
		&activation.jobs,
		first_child,
		input.cancel_target,
		&completed_resolutions,
		&canceled_resolutions,
	)
	append_child_composition_result_job(
		&activation.jobs,
		second_child,
		input.cancel_target,
		&completed_resolutions,
		&canceled_resolutions,
	)
	partial := process_activation(&runtime, &activation)
	defer destroy_process_result(&partial)
	if partial.error.message != nil || partial.completed || !partial.waiting_join {
		return pbt.fail("generated Child completion join resumed too early")
	}
	partial_completion := bytes.clone(partial.completion[:])
	defer delete(partial_completion)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 24
	append_child_composition_result_job(
		&activation.jobs,
		2,
		input.cancel_target,
		&completed_resolutions,
		&canceled_resolutions,
	)
	terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&terminal)
	if terminal.error.message != nil || !terminal.completed || terminal.waiting_join {
		return pbt.fail("generated Child completion join did not finish")
	}
	terminal_payload, terminal_found := clone_pbt_workflow_result(terminal.completion[:])
	defer codec.payload_free(&terminal_payload)
	if !terminal_found {
		return pbt.fail("generated Child composition had no result")
	}
	terminal_value: string
	if codec.decode_json(codec.payload_view(&terminal_payload), &terminal_value) != .None {
		return pbt.fail("generated Child composition result did not decode")
	}
	defer delete(terminal_value)
	if terminal_value != "composed" {
		return pbt.fail("generated Child composition returned the wrong result")
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
		return pbt.fail("generated Child composition did not evict")
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
		return pbt.fail("Child composition initialization changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 12
	append_child_composition_start_jobs(
		&activation.jobs,
		input.start_job_order,
		&start_resolutions,
	)
	replayed_started := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_started)
	if replayed_started.error.message != nil ||
	   !bytes.equal(started_completion, replayed_started.completion[:]) {
		return pbt.fail("Child start selection, join, or cancellation changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 20
	append_child_composition_result_job(
		&activation.jobs,
		first_child,
		input.cancel_target,
		&completed_resolutions,
		&canceled_resolutions,
	)
	append_child_composition_result_job(
		&activation.jobs,
		second_child,
		input.cancel_target,
		&completed_resolutions,
		&canceled_resolutions,
	)
	replayed_partial := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_partial)
	if replayed_partial.error.message != nil ||
	   !bytes.equal(partial_completion, replayed_partial.completion[:]) {
		return pbt.fail("Child completion selection or partial join changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 24
	append_child_composition_result_job(
		&activation.jobs,
		2,
		input.cancel_target,
		&completed_resolutions,
		&canceled_resolutions,
	)
	replayed_terminal := process_activation(&runtime, &activation)
	defer destroy_process_result(&replayed_terminal)
	if replayed_terminal.error.message != nil ||
	   !bytes.equal(terminal_completion, replayed_terminal.completion[:]) {
		return pbt.fail("Child completion join changed on replay")
	}
	return pbt.pass()
}

@(test)
workflow_child_composition_survives_generated_orders_cancellation_and_replay :: proc(
	t: ^testing.T,
) {
	result := pbt.check(
		"Child Workflow selectors and joins survive generated orders, cancellation, and replay",
		child_composition_property,
		{num_tests = 300, seed = 0x4348_494c_445f_434f},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

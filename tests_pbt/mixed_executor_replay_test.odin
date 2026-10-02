package workflow_pbt_test

import "core:bytes"
import "core:fmt"
import "core:testing"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import workflow "../temporal/workflow"

Mixed_Replay_State :: struct {
	sum:         int,
	signal_done: bool,
	update_done: bool,
}

mixed_replay_signal :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Mixed_Replay_State,
	value: int,
) {
	state.sum += workflow.execute_activity_json(ctx, "mixed", value, int)
	state.signal_done = true
}

mixed_replay_update :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Mixed_Replay_State,
	value: int,
) -> int {
	result := workflow.execute_activity_json(ctx, "mixed", value, int)
	state.sum += result
	state.update_done = true
	return result
}

mixed_replay_ready :: proc(state: ^Mixed_Replay_State) -> bool {
	return state.signal_done && state.update_done
}

mixed_replay_workflow :: proc(ctx: ^workflow.Fiber_Context, _: string) -> string {
	state := Mixed_Replay_State{}
	if !workflow.set_signal_routine_json(
		ctx,
		"mixed_signal",
		&state,
		mixed_replay_signal,
	) || !workflow.set_update_routine_json(
		ctx,
		"mixed_update",
		&state,
		mixed_replay_update,
	) {
		return "registration-failed"
	}
	state.sum += workflow.execute_activity_json(ctx, "mixed", 1, int)
	if !workflow.await_condition(ctx, &state, mixed_replay_ready) {
		return "canceled"
	}
	workflow.await_all_handlers_finished(ctx)
	return fmt.aprintf("sum:%d", state.sum)
}

Mixed_Replay_Config :: struct {
	resolution_order: int,
	partition:        int,
	update_first:     bool,
}

mixed_replay_orders := [6][3]int{
	{0, 1, 2},
	{0, 2, 1},
	{1, 0, 2},
	{1, 2, 0},
	{2, 0, 1},
	{2, 1, 0},
}

mixed_replay_chunk_count :: proc(partition: int) -> int {
	switch partition {
	case 0: return 1
	case 1, 2: return 2
	case: return 3
	}
}

mixed_replay_chunk_bounds :: proc(partition, chunk: int) -> (begin, end: int) {
	switch partition {
	case 0:
		return 0, 3
	case 1:
		if chunk == 0 do return 0, 1
		return 1, 3
	case 2:
		if chunk == 0 do return 0, 2
		return 2, 3
	case:
		return chunk, chunk + 1
	}
}

mixed_replay_append_handler_jobs :: proc(
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

mixed_replay_append_resolution_jobs :: proc(
	jobs: ^[dynamic]proto.Coresdk_Workflow_Activation_WorkflowActivationJob,
	resolutions: ^[3]proto.Coresdk_Workflow_Activation_ResolveActivity,
	config: Mixed_Replay_Config,
	chunk: int,
) {
	begin, end := mixed_replay_chunk_bounds(config.partition, chunk)
	order := mixed_replay_orders[config.resolution_order]
	for position in begin ..< end {
		resolution := &resolutions[order[position]]
		append(
			jobs,
			proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
				variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
					value = resolution,
				},
			},
		)
	}
}

mixed_replay_process_step :: proc(
	runtime: ^workflow.Fiber_Runtime,
	activation: ^proto.Coresdk_Workflow_Activation_WorkflowActivation,
	initialize: ^proto.Coresdk_Workflow_Activation_InitializeWorkflow,
	signal: ^proto.Coresdk_Workflow_Activation_SignalWorkflow,
	update: ^proto.Coresdk_Workflow_Activation_DoUpdate,
	resolutions: ^[3]proto.Coresdk_Workflow_Activation_ResolveActivity,
	config: Mixed_Replay_Config,
	step: int,
) -> workflow.Process_Result {
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = u32(3 + step * 3)
	if step == 0 {
		append(
			&activation.jobs,
			proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
				variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
					value = initialize,
				},
			},
		)
	} else if step == 1 {
		mixed_replay_append_handler_jobs(
			&activation.jobs,
			signal,
			update,
			config.update_first,
		)
	} else {
		mixed_replay_append_resolution_jobs(
			&activation.jobs,
			resolutions,
			config,
			step - 2,
		)
	}
	return process_activation(runtime, activation)
}

mixed_replay_evict :: proc(
	runtime: ^workflow.Fiber_Runtime,
	activation: ^proto.Coresdk_Workflow_Activation_WorkflowActivation,
) -> bool {
	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	result := process_activation(runtime, activation)
	ok := result.error.message == nil && result.evicted
	destroy_process_result(&result)
	return ok
}

mixed_replay_result_matches :: proc(data: []u8) -> bool {
	payload, found := clone_pbt_workflow_result(data)
	defer codec.payload_free(&payload)
	if !found {
		return false
	}
	value: string
	if codec.decode_json(codec.payload_view(&payload), &value) != .None {
		return false
	}
	defer delete(value)
	return value == "sum:6"
}

mixed_executor_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	config := Mixed_Replay_Config{
		resolution_order = pbt.draw(t, pbt.int_range(0, 5)),
		partition = pbt.draw(t, pbt.int_range(0, 3)),
		update_first = pbt.draw(t, pbt.boolean()),
	}
	chunk_count := mixed_replay_chunk_count(config.partition)
	checkpoint := pbt.draw(t, pbt.int_range(0, chunk_count - 1))
	order_labels := [?]string{"1-2-3", "1-3-2", "2-1-3", "2-3-1", "3-1-2", "3-2-1"}
	partition_labels := [?]string{"3", "1+2", "2+1", "1+1+1"}
	for index in 0 ..< 6 {
		pbt.cover(t, config.resolution_order == index, 8.0, order_labels[index])
	}
	for index in 0 ..< 4 {
		pbt.cover(t, config.partition == index, 15.0, partition_labels[index])
	}
	pbt.cover(t, config.update_first, 35.0, "Update before Signal")
	pbt.cover(t, !config.update_first, 35.0, "Signal before Update")
	pbt.cover(t, checkpoint == 0, 25.0, "evict before resolutions")
	pbt.cover(t, checkpoint > 0, 20.0, "evict after partial resolution")

	oracle := workflow.Fiber_Runtime{}
	oracle_error := workflow.fiber_runtime_init(
		&oracle,
		"pbt_mixed_executor_replay",
		"test-queue",
		mixed_replay_workflow,
	)
	defer workflow.error_free(&oracle_error)
	defer workflow.fiber_runtime_close(&oracle)
	if oracle_error.message != nil {
		return pbt.error("could not initialize mixed replay oracle")
	}
	subject := workflow.Fiber_Runtime{}
	subject_error := workflow.fiber_runtime_init(
		&subject,
		"pbt_mixed_executor_replay",
		"test-queue",
		mixed_replay_workflow,
	)
	defer workflow.error_free(&subject_error)
	defer workflow.fiber_runtime_close(&subject)
	if subject_error.message != nil {
		return pbt.error("could not initialize mixed replay subject")
	}

	input, input_error := codec.encode_json("start")
	defer codec.payload_free(&input)
	if input_error != .None {
		return pbt.error("could not encode mixed replay input")
	}
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "pbt_mixed_executor_replay",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	handler_input, handler_input_error := codec.encode_json(1)
	defer codec.payload_free(&handler_input)
	if handler_input_error != .None {
		return pbt.error("could not encode mixed replay handler input")
	}
	handler_input_proto := proto.encode_payload(codec.payload_view(&handler_input))
	defer delete(handler_input_proto.metadata)
	defer delete(handler_input_proto.external_payloads)
	signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "mixed_signal",
	}
	defer delete(signal.input)
	append(&signal.input, handler_input_proto)
	update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "mixed-update",
		protocol_instance_id = "mixed-update-protocol",
		name = "mixed_update",
	}
	defer delete(update.input)
	append(&update.input, handler_input_proto)

	result_payload, result_error := codec.encode_json(2)
	defer codec.payload_free(&result_payload)
	if result_error != .None {
		return pbt.error("could not encode mixed replay Activity result")
	}
	result_proto := proto.encode_payload(codec.payload_view(&result_payload))
	defer delete(result_proto.metadata)
	defer delete(result_proto.external_payloads)
	success := proto.Coresdk_Activity_Result_Success{result = &result_proto}
	resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &success,
		},
	}
	resolutions := [3]proto.Coresdk_Workflow_Activation_ResolveActivity{
		{seq = 1, result = &resolution},
		{seq = 2, result = &resolution},
		{seq = 3, result = &resolution},
	}
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-mixed-executor-replay-run",
	}
	defer delete(activation.jobs)

	step_count := 2 + chunk_count
	expected := make([dynamic][]u8, 0, step_count)
	defer {
		for completion in expected {
			delete(completion)
		}
		delete(expected)
	}
	scheduled_activities := 0
	signal_handlers_started := 0
	signal_handlers_completed := 0
	updates_accepted := 0
	updates_completed := 0
	for step in 0 ..< step_count {
		result := mixed_replay_process_step(
			&oracle,
			&activation,
			&initialize,
			&signal,
			&update,
			&resolutions,
			config,
			step,
		)
		if result.error.message != nil || result.completion == nil ||
		   (step < step_count - 1 && result.completed) ||
		   (step == step_count - 1 && !result.completed) {
			destroy_process_result(&result)
			return pbt.fail("uninterrupted mixed replay oracle diverged from its lifecycle")
		}
		scheduled_activities += result.scheduled_activity_count
		signal_handlers_started += result.signal_handlers_started
		signal_handlers_completed += result.signal_handlers_completed
		updates_accepted += result.updates_accepted
		updates_completed += result.updates_completed
		if step == step_count - 1 && !mixed_replay_result_matches(result.completion[:]) {
			destroy_process_result(&result)
			return pbt.fail("uninterrupted mixed replay oracle returned the wrong result")
		}
		append(&expected, bytes.clone(result.completion[:]))
		destroy_process_result(&result)
	}
	if scheduled_activities != 3 || signal_handlers_started != 1 ||
	   signal_handlers_completed != 1 || updates_accepted != 1 ||
	   updates_completed != 1 {
		return pbt.fail("uninterrupted mixed replay oracle produced incorrect handler counts")
	}

	checkpoint_step := 1 + checkpoint
	for step in 0 ..= checkpoint_step {
		result := mixed_replay_process_step(
			&subject,
			&activation,
			&initialize,
			&signal,
			&update,
			&resolutions,
			config,
			step,
		)
		matches := result.error.message == nil &&
			bytes.equal(expected[step], result.completion[:])
		destroy_process_result(&result)
		if !matches {
			return pbt.fail("mixed replay subject changed before eviction")
		}
	}
	if !mixed_replay_evict(&subject, &activation) {
		return pbt.fail("mixed replay subject did not evict at the generated checkpoint")
	}

	activation.is_replaying = true
	for step in 0 ..= checkpoint_step {
		result := mixed_replay_process_step(
			&subject,
			&activation,
			&initialize,
			&signal,
			&update,
			&resolutions,
			config,
			step,
		)
		matches := result.error.message == nil &&
			bytes.equal(expected[step], result.completion[:])
		destroy_process_result(&result)
		if !matches {
			return pbt.fail("fresh stack did not reproduce the generated mixed history")
		}
	}

	activation.is_replaying = false
	for step in checkpoint_step + 1 ..< step_count {
		result := mixed_replay_process_step(
			&subject,
			&activation,
			&initialize,
			&signal,
			&update,
			&resolutions,
			config,
			step,
		)
		matches := result.error.message == nil &&
			bytes.equal(expected[step], result.completion[:])
		if step == step_count - 1 {
			matches = matches && result.completed &&
				mixed_replay_result_matches(result.completion[:])
		}
		destroy_process_result(&result)
		if !matches {
			return pbt.fail("mixed replay subject diverged after catching up to history")
		}
	}
	return pbt.pass()
}

@(test)
mixed_workflow_activity_handlers_survive_generated_midflight_replay :: proc(t: ^testing.T) {
	result := pbt.check(
		"mixed Workflow Activity handlers survive generated mid-flight replay",
		mixed_executor_replay_property,
		{num_tests = 500, seed = 0x4d49_5845_4452_504c},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

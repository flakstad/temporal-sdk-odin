package main

import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"
import nexus "../../temporal/nexus"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-hello-nexus-workflow"
WORKFLOW_TYPE :: "odin_hello_nexus_workflow"
NEXUS_SERVICE :: "odin-greeting-service"
NEXUS_OPERATION :: "hello"

Start_State :: struct {
	client:      ^temporal_client.Client,
	endpoint:    string,
	workflow_id: string,
	handle:      temporal_client.Workflow_Handle,
	error:       temporal_client.Error,
}

hello :: proc(ctx: ^nexus.Context, name: string) -> string {
	operation_info := nexus.info(ctx)
	if operation_info.service != NEXUS_SERVICE ||
	   operation_info.operation != NEXUS_OPERATION ||
	   len(operation_info.request_id) == 0 ||
	   len(operation_info.task_token) == 0 {
		return "invalid Nexus operation context"
	}
	return fmt.aprintf("Hello %s", name)
}

hello_nexus_workflow :: proc(ctx: ^workflow.Fiber_Context, endpoint: string) -> string {
	headers := [?]workflow.Nexus_Header{
		{key = "x-odin-sdk", value = "hello-nexus-workflow"},
	}
	return workflow.execute_nexus_operation_json(
		ctx,
		endpoint,
		NEXUS_SERVICE,
		NEXUS_OPERATION,
		"Odin",
		string,
		{
			schedule_to_close_timeout = 20 * time.Second,
			start_to_close_timeout = 10 * time.Second,
			cancellation_type = .Wait_Cancellation_Completed,
			headers = headers[:],
		},
	)
}

start_when_worker_ready :: proc(user_data: rawptr) -> bool {
	state := cast(^Start_State)user_data
	if state == nil || state.client == nil || len(state.endpoint) == 0 {
		return false
	}
	state.handle, state.error = temporal_client.start_workflow_json(
		state.client,
		WORKFLOW_TYPE,
		state.endpoint,
		{
			id = state.workflow_id,
			task_queue = TASK_QUEUE,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	return state.error.message == nil
}

run :: proc() -> int {
	endpoint_buffer: [256]u8
	endpoint := os.get_env(endpoint_buffer[:], "NEXUS_ENDPOINT")
	if len(endpoint) == 0 {
		fmt.eprintln("NEXUS_ENDPOINT must name a Temporal Nexus endpoint")
		return 1
	}

	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		fmt.eprintf("Temporal Client connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	options := worker.options_default(TASK_QUEUE)
	options.identity_override = "temporal-sdk-odin-hello-nexus-workflow"
	options.max_concurrent_nexus_tasks = 2
	options.nexus_task_poller_behavior = worker.poller_simple_maximum(1)
	sdk_worker, create_error := worker.create_with_options(
		temporal_client.core_client(&client),
		options,
	)
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		return 1
	}
	defer {
		close_error := worker.close(&sdk_worker)
		if close_error.message != nil {
			fmt.eprintf("Worker close failed: %s\n", string(close_error.message))
			worker.error_free(&close_error)
		}
	}

	workflow_error := worker.register_workflow(
		&sdk_worker,
		WORKFLOW_TYPE,
		hello_nexus_workflow,
	)
	if workflow_error.message != nil {
		fmt.eprintf("Workflow registration failed: %s\n", string(workflow_error.message))
		worker.error_free(&workflow_error)
		return 1
	}
	nexus_error := worker.register_nexus_operation(
		&sdk_worker,
		NEXUS_SERVICE,
		NEXUS_OPERATION,
		hello,
	)
	if nexus_error.message != nil {
		fmt.eprintf("Nexus registration failed: %s\n", string(nexus_error.message))
		worker.error_free(&nexus_error)
		return 1
	}

	id_buffer: [256]u8
	workflow_id := os.get_env(id_buffer[:], "TEMPORAL_NEXUS_WORKFLOW_ID")
	owned_workflow_id := ""
	if len(workflow_id) == 0 {
		owned_workflow_id = fmt.aprintf("odin-nexus-workflow-%d", time.now()._nsec)
		defer delete(owned_workflow_id)
		workflow_id = owned_workflow_id
	}
	state := Start_State{
		client = &client,
		endpoint = endpoint,
		workflow_id = workflow_id,
	}
	stats, run_error := worker.run(
		&sdk_worker,
		{
			max_nexus_tasks = 1,
			max_workflow_executions = 1,
			on_ready = start_when_worker_ready,
			on_ready_data = &state,
		},
	)
	if run_error.message != nil {
		fmt.eprintf("Worker failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		if state.error.message != nil {
			fmt.eprintf("Workflow start failed: %s\n", string(state.error.message))
			temporal_client.error_free(&state.error)
		}
		return 1
	}
	defer temporal_client.workflow_handle_free(&state.handle)
	if stats.workflow_executions_completed != 1 || stats.nexus_tasks_completed != 1 {
		fmt.eprintf(
			"Unexpected Worker stats: workflows=%d Nexus=%d\n",
			stats.workflow_executions_completed,
			stats.nexus_tasks_completed,
		)
		return 1
	}

	result: string
	result_error := temporal_client.get_result_json(&state.handle, &result)
	defer delete(result)
	if result_error.message != nil {
		fmt.eprintf("Workflow result failed: %s\n", string(result_error.message))
		temporal_client.error_free(&result_error)
		return 1
	}
	if result != "Hello Odin" {
		fmt.eprintf("Unexpected Workflow result: %q\n", result)
		return 1
	}

	history, history_error := temporal_client.fetch_history_protobuf(&state.handle, 1)
	if history_error.message != nil {
		fmt.eprintf("Workflow history fetch failed: %s\n", string(history_error.message))
		temporal_client.error_free(&history_error)
		return 1
	}
	defer delete(history)
	core_client := temporal_client.core_client(&client)
	replayer, replayer_error := worker.create_replayer(core_client.runtime)
	if replayer_error.message != nil {
		fmt.eprintf("Offline replayer creation failed: %s\n", string(replayer_error.message))
		worker.error_free(&replayer_error)
		return 1
	}
	defer {
		close_error := worker.close(&replayer)
		if close_error.message != nil {
			worker.error_free(&close_error)
		}
	}
	replay_register_error := worker.register_workflow(
		&replayer,
		WORKFLOW_TYPE,
		hello_nexus_workflow,
	)
	if replay_register_error.message != nil {
		fmt.eprintf("Replay registration failed: %s\n", string(replay_register_error.message))
		worker.error_free(&replay_register_error)
		return 1
	}
	replay_result, replay_error := worker.replay_history(&replayer, state.handle.id, history)
	defer worker.replay_result_free(&replay_result)
	if replay_error.message != nil {
		fmt.eprintf("Offline replay failed: %s\n", string(replay_error.message))
		worker.error_free(&replay_error)
		return 1
	}
	if replay_result.failure != .None {
		fmt.eprintf("Offline replay rejected Nexus history: %s\n", string(replay_result.message))
		return 1
	}

	fmt.printf(
		"Odin Workflow called Odin Nexus operation through Temporal: %s (workflow_id=%s)\n",
		result,
		state.handle.id,
	)
	fmt.printf(
		"Nexus Workflow history replayed offline through Core activations=%d\n",
		replay_result.activations_processed,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

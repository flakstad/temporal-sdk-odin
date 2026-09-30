package main

import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"
import temporal_failure "../../temporal/failure"
import nexus "../../temporal/nexus"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-hello-nexus-workflow"
WORKFLOW_TYPE :: "odin_hello_nexus_workflow"
NEXUS_SERVICE :: "odin-greeting-service"
NEXUS_OPERATION :: "hello"
ASYNC_COMPLETE_WORKFLOW_TYPE :: "odin_nexus_async_complete_workflow"
ASYNC_WAIT_WORKFLOW_TYPE :: "odin_nexus_async_wait_workflow"

nexus_client: ^temporal_client.Client

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

fail :: proc(_: ^nexus.Context, name: string) -> nexus.Result(string) {
	return nexus.result_handler_error(
		string,
		fmt.aprintf("cannot greet %s", name),
		"BAD_REQUEST",
		.Non_Retryable,
	)
}

slow :: proc(ctx: ^nexus.Context, _: string) -> string {
	for !nexus.is_cancellation_requested(ctx) {
		time.sleep(10 * time.Millisecond)
	}
	if nexus.cancellation_reason(ctx) == .Timed_Out {
		return "handler observed its request deadline"
	}
	return "handler observed Worker shutdown"
}

async_complete_workflow :: proc(_: ^workflow.Fiber_Context, name: string) -> string {
	return fmt.aprintf("Hello async %s", name)
}

async_wait_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	workflow.sleep(ctx, time.Hour)
	return fmt.aprintf("Finished waiting for %s", name)
}

start_async_completer :: proc(
	ctx: ^nexus.Context,
	name, workflow_type: string,
) -> nexus.Result(string) {
	operation_info := nexus.info(ctx)
	if nexus_client == nil || len(operation_info.callback_url) == 0 ||
	   len(operation_info.callback_headers) == 0 || len(operation_info.request_id) == 0 {
		return nexus.result_handler_error(
			string,
			"Temporal did not provide complete Nexus callback information",
			"INTERNAL",
			.Non_Retryable,
		)
	}
	callback_headers := make(
		[]temporal_client.Nexus_Callback_Header,
		len(operation_info.callback_headers),
	)
	for header, index in operation_info.callback_headers {
		callback_headers[index] = {key = header.key, value = header.value}
	}
	callbacks := [?]temporal_client.Nexus_Completion_Callback{{
		url = operation_info.callback_url,
		headers = callback_headers,
	}}
	workflow_id := fmt.aprintf("odin-nexus-completer-%s", operation_info.request_id)
	defer delete(workflow_id)
	handle, start_error := temporal_client.start_workflow_json(
		nexus_client,
		workflow_type,
		name,
		{
			id = workflow_id,
			task_queue = TASK_QUEUE,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
			nexus_completion_callbacks = callbacks[:],
		},
	)
	defer temporal_client.workflow_handle_free(&handle)
	if start_error.message != nil {
		message := fmt.aprintf("could not start Nexus completer Workflow: %s", string(start_error.message))
		temporal_client.error_free(&start_error)
		return nexus.result_handler_error(string, message, "INTERNAL", .Retryable)
	}
	return nexus.result_async_copy(string, handle.id)
}

async_complete :: proc(ctx: ^nexus.Context, name: string) -> nexus.Result(string) {
	return start_async_completer(ctx, name, ASYNC_COMPLETE_WORKFLOW_TYPE)
}

async_wait :: proc(ctx: ^nexus.Context, name: string) -> nexus.Result(string) {
	return start_async_completer(ctx, name, ASYNC_WAIT_WORKFLOW_TYPE)
}

cancel_async_wait :: proc(
	ctx: ^nexus.Context,
	operation_token: string,
) -> nexus.Cancel_Result {
	operation_info := nexus.info(ctx)
	if len(operation_token) == 0 || operation_info.operation_token != operation_token ||
	   nexus_client == nil {
		return nexus.cancel_handler_error(
			"unexpected Nexus operation token",
			"BAD_REQUEST",
			.Non_Retryable,
		)
	}
	handle, handle_error := temporal_client.get_workflow_handle(nexus_client, operation_token)
	if handle_error.message != nil {
		message := fmt.aprintf("could not attach to Nexus completer Workflow: %s", string(handle_error.message))
		temporal_client.error_free(&handle_error)
		return nexus.cancel_handler_error(message, "INTERNAL", .Retryable)
	}
	defer temporal_client.workflow_handle_free(&handle)
	cancel_error := temporal_client.cancel_workflow(&handle, "Nexus operation cancellation requested")
	if cancel_error.message != nil {
		message := fmt.aprintf("could not cancel Nexus completer Workflow: %s", string(cancel_error.message))
		temporal_client.error_free(&cancel_error)
		return nexus.cancel_handler_error(message, "INTERNAL", .Retryable)
	}
	return nexus.cancel_complete()
}

hello_nexus_workflow :: proc(ctx: ^workflow.Fiber_Context, endpoint: string) -> string {
	headers := [?]workflow.Nexus_Header{
		{key = "x-odin-sdk", value = "hello-nexus-workflow"},
	}
	failure_result := workflow.execute_nexus_operation_json_result(
		ctx,
		endpoint,
		NEXUS_SERVICE,
		"fail",
		"Odin",
		string,
		{
			schedule_to_close_timeout = 20 * time.Second,
			start_to_close_timeout = 10 * time.Second,
		},
	)
	if failure_result.status != .Failed || failure_result.failure == nil ||
	   temporal_failure.find_kind(failure_result.failure, .Nexus_Operation) == nil ||
	   temporal_failure.find_kind(failure_result.failure, .Nexus_Handler) == nil {
		return "Nexus handler failure was not preserved"
	}

	timeout_result := workflow.execute_nexus_operation_json_result(
		ctx,
		endpoint,
		NEXUS_SERVICE,
		"slow",
		"Odin",
		string,
		{
			schedule_to_close_timeout = time.Second,
			start_to_close_timeout = 5 * time.Second,
		},
	)
	if timeout_result.status != .Timed_Out || timeout_result.failure == nil ||
	   temporal_failure.find_kind(timeout_result.failure, .Timeout) == nil {
		return "Nexus timeout was not preserved"
	}

	async_completed := workflow.execute_nexus_operation_json_result(
		ctx,
		endpoint,
		NEXUS_SERVICE,
		"async-complete",
		"Odin",
		string,
		{
			schedule_to_close_timeout = 20 * time.Second,
			cancellation_type = .Wait_Cancellation_Completed,
		},
	)
	if async_completed.status != .Succeeded || async_completed.value != "Hello async Odin" {
		return "Nexus async callback completion failed"
	}

	async_future := workflow.start_nexus_operation_json(
		ctx,
		endpoint,
		NEXUS_SERVICE,
		"async-wait",
		"Odin",
		{
			schedule_to_close_timeout = 20 * time.Second,
			cancellation_type = .Wait_Cancellation_Requested,
		},
	)
	// Yield a durable activation so Core records the async operation token before
	// the Workflow asks the Service to dispatch CancelOperation.
	workflow.sleep(ctx, 100 * time.Millisecond)
	if !workflow.cancel_nexus_operation(ctx, async_future) {
		return "Nexus async operation cancellation was not requested"
	}
	async_result := workflow.await_nexus_operation_result(ctx, async_future)
	if async_result.status != .Canceled {
		return "Nexus async operation did not acknowledge cancellation"
	}

	options := workflow.Nexus_Operation_Options{
		schedule_to_close_timeout = 20 * time.Second,
		start_to_close_timeout = 10 * time.Second,
		cancellation_type = .Wait_Cancellation_Completed,
		headers = headers[:],
	}
	first := workflow.start_nexus_operation_json(
		ctx,
		endpoint,
		NEXUS_SERVICE,
		NEXUS_OPERATION,
		"Odin",
		options,
	)
	second := workflow.start_nexus_operation_json(
		ctx,
		endpoint,
		NEXUS_SERVICE,
		NEXUS_OPERATION,
		"Odin",
		options,
	)
	selector_cases := [?]workflow.Selector_Case{
		workflow.selector_nexus_operation(second),
		workflow.selector_nexus_operation(first),
	}
	selected := workflow.selector_select(ctx, selector_cases[:])
	if selected.status != .Selected || selected.kind != .Nexus_Operation {
		return "Nexus selector did not resolve an operation"
	}
	join_cases := [?]workflow.Selector_Case{
		workflow.selector_nexus_operation(first),
		workflow.selector_nexus_operation(second),
	}
	if workflow.join_all(ctx, join_cases[:]) != .Ready {
		return "Nexus join did not resolve both operations"
	}
	first_result := workflow.await_nexus_operation(ctx, first)
	second_result := workflow.await_nexus_operation(ctx, second)
	if first_result != "Hello Odin" || second_result != "Hello Odin" {
		return "Nexus selector/join returned unexpected results"
	}
	return first_result
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
	async_complete_workflow_error := worker.register_workflow(
		&sdk_worker,
		ASYNC_COMPLETE_WORKFLOW_TYPE,
		async_complete_workflow,
	)
	if async_complete_workflow_error.message != nil {
		fmt.eprintf("Async completer Workflow registration failed: %s\n", string(async_complete_workflow_error.message))
		worker.error_free(&async_complete_workflow_error)
		return 1
	}
	async_wait_workflow_error := worker.register_workflow(
		&sdk_worker,
		ASYNC_WAIT_WORKFLOW_TYPE,
		async_wait_workflow,
	)
	if async_wait_workflow_error.message != nil {
		fmt.eprintf("Async wait Workflow registration failed: %s\n", string(async_wait_workflow_error.message))
		worker.error_free(&async_wait_workflow_error)
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
	failure_registration_error := worker.register_json_nexus_operation_result(
		&sdk_worker,
		NEXUS_SERVICE,
		"fail",
		fail,
	)
	if failure_registration_error.message != nil {
		fmt.eprintf(
			"Nexus failure registration failed: %s\n",
			string(failure_registration_error.message),
		)
		worker.error_free(&failure_registration_error)
		return 1
	}
	slow_registration_error := worker.register_nexus_operation(
		&sdk_worker,
		NEXUS_SERVICE,
		"slow",
		slow,
	)
	if slow_registration_error.message != nil {
		fmt.eprintf(
			"Nexus timeout registration failed: %s\n",
			string(slow_registration_error.message),
		)
		worker.error_free(&slow_registration_error)
		return 1
	}
	async_registration_error := worker.register_json_nexus_operation_result(
		&sdk_worker,
		NEXUS_SERVICE,
		"async-wait",
		async_wait,
	)
	if async_registration_error.message != nil {
		fmt.eprintf(
			"Nexus async registration failed: %s\n",
			string(async_registration_error.message),
		)
		worker.error_free(&async_registration_error)
		return 1
	}
	async_complete_registration_error := worker.register_json_nexus_operation_result(
		&sdk_worker,
		NEXUS_SERVICE,
		"async-complete",
		async_complete,
	)
	if async_complete_registration_error.message != nil {
		fmt.eprintf(
			"Nexus async completion registration failed: %s\n",
			string(async_complete_registration_error.message),
		)
		worker.error_free(&async_complete_registration_error)
		return 1
	}
	cancel_registration_error := worker.register_nexus_cancel_handler(
		&sdk_worker,
		NEXUS_SERVICE,
		"async-wait",
		cancel_async_wait,
	)
	if cancel_registration_error.message != nil {
		fmt.eprintf(
			"Nexus cancel registration failed: %s\n",
			string(cancel_registration_error.message),
		)
		worker.error_free(&cancel_registration_error)
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
	nexus_client = &client
	defer {nexus_client = nil}
	stats, run_error := worker.run(
		&sdk_worker,
		{
			max_workflow_executions = 3,
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
	if stats.workflow_executions_completed != 3 || stats.nexus_tasks_completed != 7 ||
	   stats.nexus_cancellations_received != 1 {
		fmt.eprintf(
			"Unexpected Worker stats: workflows=%d Nexus=%d cancellations=%d\n",
			stats.workflow_executions_completed,
			stats.nexus_tasks_completed,
			stats.nexus_cancellations_received,
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
	fmt.println("Nexus handler failure and schedule-to-close timeout retained structured Failure trees")
	fmt.println("Nexus selector and join composed two concurrent operations")
	fmt.println("Nexus async start token reached the registered cancel handler")
	fmt.println("Nexus async result completed through a Temporal Workflow callback")
	fmt.printf(
		"Nexus Workflow history replayed offline through Core activations=%d\n",
		replay_result.activations_processed,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

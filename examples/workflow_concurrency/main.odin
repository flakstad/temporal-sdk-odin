package main

import "core:fmt"
import "core:os"
import "core:thread"
import "core:time"
import temporal_client "../../temporal/client"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"
import workflow_isolate "../../temporal/workflow/isolate"
import workflow_app "../workflow_isolate_app"

SHARD_COUNT :: 16
EXECUTOR_THREADS :: 4

Concurrency_State :: struct {
	client:       ^temporal_client.Client,
	sdk_worker:   ^worker.Worker,
	task_queue:   string,
	id_prefix:    string,
	handles:      [dynamic]temporal_client.Workflow_Handle,
	watcher:      ^thread.Thread,
	start_error:  temporal_client.Error,
	result_error: temporal_client.Error,
	results_ok:   bool,
}

wait_for_results :: proc(state: ^Concurrency_State) {
	state.results_ok = true
	for &handle in state.handles {
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		if result_error.message != nil {
			state.result_error = result_error
			state.results_ok = false
			break
		}
		expected := fmt.aprintf("Hello from an isolated Workflow, %s", handle.id)
		if result != expected {
			state.results_ok = false
		}
		delete(expected)
		delete(result)
		if !state.results_ok {
			break
		}
	}
	worker.request_shutdown(state.sdk_worker)
}

start_concurrent_workflows :: proc(user_data: rawptr) -> bool {
	state := cast(^Concurrency_State)user_data
	if state == nil || state.client == nil || state.sdk_worker == nil {
		return false
	}
	first_shard := -1
	found_distinct_shard := false
	for candidate in 0 ..< 64 {
		workflow_id := fmt.aprintf("%s-%d", state.id_prefix, candidate)
		handle, start_error := temporal_client.start_workflow_json(
			state.client,
			workflow_app.GREETING_WORKFLOW_TYPE,
			workflow_id,
			{
				id = workflow_id,
				task_queue = state.task_queue,
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
		)
		delete(workflow_id)
		if start_error.message != nil {
			state.start_error = start_error
			return false
		}
		append(&state.handles, handle)
		shard, shard_ok := workflow_isolate.workflow_shard_index(handle.run_id, SHARD_COUNT)
		if !shard_ok {
			return false
		}
		if first_shard < 0 {
			first_shard = shard
		} else if shard != first_shard {
			found_distinct_shard = true
			break
		}
	}
	if !found_distinct_shard {
		return false
	}
	state.watcher = thread.create_and_start_with_poly_data(state, wait_for_results)
	return state.watcher != nil
}

run :: proc() -> int {
	child_buffer: [1024]u8
	child_command := os.get_env(child_buffer[:], "TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND")
	if len(child_command) == 0 {
		fmt.eprintln("TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND is required")
		return 1
	}
	task_queue_buffer: [256]u8
	task_queue := os.get_env(task_queue_buffer[:], "TEMPORAL_ODIN_TASK_QUEUE")
	if len(task_queue) == 0 {
		task_queue = "odin-workflow-concurrency"
	}
	id_buffer: [256]u8
	id_prefix := os.get_env(id_buffer[:], "TEMPORAL_ODIN_WORKFLOW_ID")
	owned_id_prefix := ""
	if len(id_prefix) == 0 {
		owned_id_prefix = fmt.aprintf("odin-concurrent-%d", time.now()._nsec)
		defer delete(owned_id_prefix)
		id_prefix = owned_id_prefix
	}

	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		fmt.eprintf("Concurrent Workflow Client connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	registry := workflow.Registry{}
	registry_error := workflow.registry_init(&registry)
	if registry_error.message != nil {
		workflow.error_free(&registry_error)
		return 1
	}
	defer workflow.registry_close(&registry)
	registry_error = workflow_app.register(&registry)
	if registry_error.message != nil {
		workflow.error_free(&registry_error)
		return 1
	}

	worker_options := worker.options_default(task_queue)
	worker_options.max_concurrent_workflow_tasks = EXECUTOR_THREADS
	worker_options.max_workflow_task_slots = EXECUTOR_THREADS
	worker_options.max_cached_workflows = 16
	worker_options.workflow_task_poller_behavior = worker.poller_simple_maximum(EXECUTOR_THREADS)
	sdk_worker, create_error := worker.create_with_options(
		temporal_client.core_client(&client),
		worker_options,
	)
	if create_error.message != nil {
		fmt.eprintf("Concurrent Workflow Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		return 1
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}
	register_error := worker.register_external_workflow_registry(&sdk_worker, &registry)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return 1
	}

	child := [?]string{child_command, "--child-slow", task_queue}
	executor_manager := workflow_isolate.Concurrent_Manager{}
	manager_error := workflow_isolate.concurrent_manager_init(&executor_manager, {
		manager = {
			command = child[:],
			expected_registry = &registry,
			maximum_executors = 4,
			activation_timeout = 5 * time.Second,
			shutdown_timeout = 2 * time.Second,
		},
		shard_count = SHARD_COUNT,
	})
	if manager_error.message != nil {
		workflow_isolate.manager_error_free(&manager_error)
		return 1
	}
	defer workflow_isolate.concurrent_manager_close(&executor_manager)
	verify_error := workflow_isolate.concurrent_manager_verify_host(&executor_manager)
	if verify_error.message != nil {
		workflow_isolate.manager_error_free(&verify_error)
		return 1
	}

	state := Concurrency_State{
		client = &client,
		sdk_worker = &sdk_worker,
		task_queue = task_queue,
		id_prefix = id_prefix,
	}
	run_stats, run_error := worker.run(&sdk_worker, {
		workflow_activation_processor = workflow_isolate.concurrent_manager_process_activation,
		workflow_activation_processor_data = &executor_manager,
		on_ready = start_concurrent_workflows,
		on_ready_data = &state,
	})
	if state.watcher != nil {
		thread.join(state.watcher)
		thread.destroy(state.watcher)
	}
	defer {
		for &handle in state.handles {
			temporal_client.workflow_handle_free(&handle)
		}
		delete(state.handles)
	}
	if state.start_error.message != nil {
		fmt.eprintf("Concurrent Workflow start failed: %s\n", string(state.start_error.message))
		temporal_client.error_free(&state.start_error)
		worker.error_free(&run_error)
		return 1
	}
	if state.result_error.message != nil {
		fmt.eprintf("Concurrent Workflow result failed: %s\n", string(state.result_error.message))
		temporal_client.error_free(&state.result_error)
		worker.error_free(&run_error)
		return 1
	}
	if run_error.message != nil {
		fmt.eprintf("Concurrent Workflow Worker failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		return 1
	}
	shutdown_error := workflow_isolate.concurrent_manager_shutdown(&executor_manager)
	if shutdown_error.message != nil {
		workflow_isolate.manager_error_free(&shutdown_error)
		return 1
	}
	peak := workflow_isolate.concurrent_manager_peak_activations(&executor_manager)
	if !state.results_ok || len(state.handles) < 2 ||
	   run_stats.workflow_executions_completed != len(state.handles) || peak < 2 ||
	   workflow_isolate.concurrent_manager_route_count(&executor_manager) != 0 {
		fmt.eprintf(
			"Concurrent Workflow proof failed handles=%d completed=%d peak=%d routes=%d\n",
			len(state.handles),
			run_stats.workflow_executions_completed,
			peak,
			workflow_isolate.concurrent_manager_route_count(&executor_manager),
		)
		return 1
	}
	fmt.printf(
		"Concurrent Workflow execution proved handles=%d executor_threads=%d peak_activations=%d\n",
		len(state.handles),
		EXECUTOR_THREADS,
		peak,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

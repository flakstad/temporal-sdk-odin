package main

import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"
import workflow_isolate "../../temporal/workflow/isolate"

WORKFLOW_TYPE :: "odin_isolated_workflow_spike"
DEFAULT_TASK_QUEUE :: "odin-isolated-workflow-core"

// Core requires a registered Workflow type before polling. Online activations
// are deliberately routed to the external manager, so this procedure must
// never execute in the parent process.
parent_registration_only :: proc(_: ^workflow.Fiber_Context, _: string) -> string {
	return "parent Workflow dispatcher was unexpectedly invoked"
}

Start_State :: struct {
	client:      ^temporal_client.Client,
	workflow_id: string,
	task_queue:  string,
	handle:      temporal_client.Workflow_Handle,
	error:       temporal_client.Error,
}

start_when_ready :: proc(user_data: rawptr) -> bool {
	state := cast(^Start_State)user_data
	state.handle, state.error = temporal_client.start_workflow_json(
		state.client,
		WORKFLOW_TYPE,
		"Odin",
		{
			id = state.workflow_id,
			task_queue = state.task_queue,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	return state.error.message == nil
}

run :: proc() -> int {
	child_buffer: [1024]u8
	child_command := os.get_env(child_buffer[:], "TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND")
	if len(child_command) == 0 {
		fmt.eprintln("TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND must name the workflow_isolate executable")
		return 1
	}
	task_queue_buffer: [256]u8
	task_queue := os.get_env(task_queue_buffer[:], "TEMPORAL_ODIN_TASK_QUEUE")
	if len(task_queue) == 0 {
		task_queue = DEFAULT_TASK_QUEUE
	}
	id_buffer: [256]u8
	workflow_id := os.get_env(id_buffer[:], "TEMPORAL_ODIN_WORKFLOW_ID")
	owned_workflow_id := ""
	if len(workflow_id) == 0 {
		owned_workflow_id = fmt.aprintf("odin-isolate-%d", time.now()._nsec)
		defer delete(owned_workflow_id)
		workflow_id = owned_workflow_id
	}

	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	worker_options := worker.options_default(task_queue)
	worker_options.max_cached_workflows = 0
	worker_options.workflow_task_poller_behavior = worker.poller_simple_maximum(1)
	sdk_worker, create_error := worker.create_with_options(
		temporal_client.core_client(&client),
		worker_options,
	)
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		return 1
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}
	register_error := worker.register_workflow(&sdk_worker, WORKFLOW_TYPE, parent_registration_only)
	if register_error.message != nil {
		fmt.eprintf("Workflow registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}

	child := [?]string{child_command, "--child", task_queue}
	first_mode_buffer: [128]u8
	first_mode := os.get_env(
		first_mode_buffer[:],
		"TEMPORAL_ODIN_ISOLATE_FIRST_MODE",
	)
	if len(first_mode) == 0 {
		first_mode = "crash"
	}
	first_argument := "--child-crash"
	expected_child_exits := 1
	expected_timeouts := 0
	expected_activation_failures := 1
	activation_timeout := 5 * time.Second
	switch first_mode {
	case "crash":
	case "hang":
		first_argument = "--child-hang"
		expected_child_exits = 0
		expected_timeouts = 1
		activation_timeout = 250 * time.Millisecond
	case "normal":
		first_argument = "--child"
		expected_child_exits = 0
		expected_activation_failures = 0
	case:
		fmt.eprintf("Unsupported TEMPORAL_ODIN_ISOLATE_FIRST_MODE: %s\n", first_mode)
		return 1
	}
	first_child := [?]string{child_command, first_argument, task_queue}
	executor_manager := workflow_isolate.Manager{}
	manager_error := workflow_isolate.manager_init(&executor_manager, {
		command = child[:],
		first_command = first_child[:],
		maximum_executors = 1,
		activation_timeout = activation_timeout,
	})
	if manager_error.message != nil {
		fmt.eprintf("Workflow executor manager initialization failed: %s\n", string(manager_error.message))
		workflow_isolate.manager_error_free(&manager_error)
		return 1
	}
	defer workflow_isolate.manager_close(&executor_manager)

	start := Start_State{
		client = &client,
		workflow_id = workflow_id,
		task_queue = task_queue,
	}
	run_stats, run_error := worker.run(&sdk_worker, {
		max_workflow_executions = 1,
		workflow_activation_processor = workflow_isolate.manager_process_activation,
		workflow_activation_processor_data = &executor_manager,
		on_ready = start_when_ready,
		on_ready_data = &start,
	})
	if start.error.message != nil {
		fmt.eprintf("Workflow start failed: %s\n", string(start.error.message))
		temporal_client.error_free(&start.error)
		worker.error_free(&run_error)
		return 1
	}
	defer temporal_client.workflow_handle_free(&start.handle)
	if run_error.message != nil {
		fmt.eprintf("Worker run failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		return 1
	}

	result: string
	result_error := temporal_client.get_result_json(&start.handle, &result)
	defer delete(result)
	if result_error.message != nil {
		fmt.eprintf("Workflow result failed: %s\n", string(result_error.message))
		temporal_client.error_free(&result_error)
		return 1
	}
	manager_stats := workflow_isolate.manager_stats(&executor_manager)
	if result != "Hello from an isolated Workflow, Odin" ||
	   run_stats.workflow_executions_completed != 1 ||
	   run_stats.workflow_activation_failures != expected_activation_failures ||
	   manager_stats.child_exits != expected_child_exits ||
	   manager_stats.activation_timeouts != expected_timeouts ||
	   manager_stats.generations_started < 2 ||
	   workflow_isolate.manager_route_count(&executor_manager) != 0 {
		fmt.eprintf(
			"Unexpected isolation result=%q completed=%d failures=%d exits=%d timeouts=%d generations=%d evictions=%d routes=%d\n",
			result,
			run_stats.workflow_executions_completed,
			run_stats.workflow_activation_failures,
			manager_stats.child_exits,
			manager_stats.activation_timeouts,
			manager_stats.generations_started,
			manager_stats.evictions,
			workflow_isolate.manager_route_count(&executor_manager),
		)
		return 1
	}
	fmt.printf(
		"Core process isolation completed workflow_id=%s result=%q first_mode=%s generations=%d child_exits=%d activation_timeouts=%d evictions=%d replay_activations=%d graceful_shutdowns=%d aborted_children=%d protocol_failures=%d\n",
		workflow_id,
		result,
		first_mode,
		manager_stats.generations_started,
		manager_stats.child_exits,
		manager_stats.activation_timeouts,
		manager_stats.evictions,
		run_stats.workflow_replay_activations,
		manager_stats.graceful_shutdowns,
		manager_stats.aborted_children,
		manager_stats.protocol_failures,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

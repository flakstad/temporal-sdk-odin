package main

import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"
import workflow_isolate "../../temporal/workflow/isolate"
import workflow_app "../workflow_isolate_app"

Start_State :: struct {
	client:       ^temporal_client.Client,
	greeting_id:  string,
	farewell_id:  string,
	task_queue:   string,
	greeting:     temporal_client.Workflow_Handle,
	farewell:     temporal_client.Workflow_Handle,
	greeting_error: temporal_client.Error,
	farewell_error: temporal_client.Error,
}

Observation_Stats :: struct {
	timer_commands_with_sequence: int,
	timer_commands_without_sequence: int,
	diagnostic_logs: int,
	diagnostic_metrics: int,
	malformed_diagnostics: int,
}

observations: Observation_Stats

observe_activation :: proc(processed: ^workflow.Process_Result) {
	if processed == nil || !processed.started_timer {
		return
	}
	if processed.timer_seq == 0 {
		observations.timer_commands_without_sequence += 1
		return
	}
	observations.timer_commands_with_sequence += 1
}

observe_diagnostic :: proc(_: rawptr, event: workflow_isolate.Diagnostic_Event_View) {
	if event.generation == 0 || len(event.run_id) == 0 {
		observations.malformed_diagnostics += 1
		return
	}
	switch event.kind {
	case .Log:
		if string(event.payload) == "workflow_started" {
			observations.diagnostic_logs += 1
		} else {
			observations.malformed_diagnostics += 1
		}
	case .Metric:
		if string(event.payload) == "workflow_started" {
			observations.diagnostic_metrics += 1
		} else {
			observations.malformed_diagnostics += 1
		}
	case .Application:
		observations.malformed_diagnostics += 1
	}
}

start_when_ready :: proc(user_data: rawptr) -> bool {
	state := cast(^Start_State)user_data
	common_options := temporal_client.Start_Workflow_Options{
		task_queue = state.task_queue,
		workflow_execution_timeout = time.Minute,
		workflow_run_timeout = time.Minute,
		workflow_task_timeout = 10 * time.Second,
	}
	greeting_options := common_options
	greeting_options.id = state.greeting_id
	state.greeting, state.greeting_error = temporal_client.start_workflow_json(
		state.client,
		workflow_app.GREETING_WORKFLOW_TYPE,
		"Odin",
		greeting_options,
	)
	if state.greeting_error.message != nil {
		return false
	}
	farewell_options := common_options
	farewell_options.id = state.farewell_id
	state.farewell, state.farewell_error = temporal_client.start_workflow_json(
		state.client,
		workflow_app.FAREWELL_WORKFLOW_TYPE,
		"Odin",
		farewell_options,
	)
	return state.farewell_error.message == nil
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
		task_queue = workflow_app.DEFAULT_TASK_QUEUE
	}
	id_buffer: [256]u8
	greeting_id := os.get_env(id_buffer[:], "TEMPORAL_ODIN_WORKFLOW_ID")
	owned_greeting_id := ""
	if len(greeting_id) == 0 {
		owned_greeting_id = fmt.aprintf("odin-isolate-%d", time.now()._nsec)
		defer delete(owned_greeting_id)
		greeting_id = owned_greeting_id
	}
	farewell_id := fmt.aprintf("%s-farewell", greeting_id)
	defer delete(farewell_id)

	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	registry := workflow.Registry{}
	registry_init_error := workflow.registry_init(&registry)
	if registry_init_error.message != nil {
		workflow.error_free(&registry_init_error)
		return 1
	}
	defer workflow.registry_close(&registry)
	registry_error := workflow_app.register(&registry)
	if registry_error.message != nil {
		fmt.eprintf("Workflow registry creation failed: %s\n", string(registry_error.message))
		workflow.error_free(&registry_error)
		return 1
	}

	worker_options := worker.options_default(task_queue)
	cache_mode_buffer: [64]u8
	cache_mode := os.get_env(
		cache_mode_buffer[:],
		"TEMPORAL_ODIN_ISOLATE_CACHE_MODE",
	)
	if len(cache_mode) == 0 {
		cache_mode = "zero"
	}
	switch cache_mode {
	case "zero":
		worker_options.max_cached_workflows = 0
	case "sticky":
		worker_options.max_cached_workflows = 100
	case:
		fmt.eprintf("Unsupported TEMPORAL_ODIN_ISOLATE_CACHE_MODE: %s\n", cache_mode)
		return 1
	}
	worker_options.workflow_task_poller_behavior = worker.poller_simple_maximum(2)
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
	register_error := worker.register_external_workflow_registry(&sdk_worker, &registry)
	if register_error.message != nil {
		fmt.eprintf("External Workflow registry failed: %s\n", string(register_error.message))
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
	expected_protocol_failures := 0
	expected_activation_failures := 1
	activation_timeout := 5 * time.Second
	switch first_mode {
	case "crash":
	case "hang":
		first_argument = "--child-hang"
		expected_child_exits = 0
		expected_timeouts = 1
		activation_timeout = 250 * time.Millisecond
	case "late":
		first_argument = "--child-late"
		expected_child_exits = 0
		expected_timeouts = 1
		activation_timeout = 250 * time.Millisecond
	case "stale":
		first_argument = "--child-stale"
		expected_child_exits = 0
		expected_protocol_failures = 1
	case "malformed-metadata":
		first_argument = "--child-malformed-metadata"
		expected_child_exits = 0
		expected_protocol_failures = 1
	case "malformed-completion":
		first_argument = "--child-malformed-completion"
		expected_child_exits = 0
		expected_protocol_failures = 1
	case "metadata-mismatch":
		first_argument = "--child-metadata-mismatch"
		expected_child_exits = 0
		expected_protocol_failures = 1
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
		expected_registry = &registry,
		maximum_executors = 2,
		activation_timeout = activation_timeout,
		shutdown_timeout = 2 * time.Second,
		crash_loop = {
			maximum_consecutive_failures = 5,
			cooldown = 5 * time.Second,
		},
		diagnostic_observer = observe_diagnostic,
	})
	if manager_error.message != nil {
		fmt.eprintf("Workflow executor manager initialization failed: %s\n", string(manager_error.message))
		workflow_isolate.manager_error_free(&manager_error)
		return 1
	}
	defer workflow_isolate.manager_close(&executor_manager)
	verify_error := workflow_isolate.manager_verify_host(&executor_manager)
	if verify_error.message != nil {
		fmt.eprintf("Workflow executor manifest verification failed: %s\n", string(verify_error.message))
		workflow_isolate.manager_error_free(&verify_error)
		return 1
	}

	start := Start_State{
		client = &client,
		greeting_id = greeting_id,
		farewell_id = farewell_id,
		task_queue = task_queue,
	}
	run_stats, run_error := worker.run(&sdk_worker, {
		max_workflow_executions = 2,
		workflow_activation_processor = workflow_isolate.manager_process_activation,
		workflow_activation_processor_data = &executor_manager,
		on_workflow_activation = observe_activation,
		on_ready = start_when_ready,
		on_ready_data = &start,
	})
	if start.greeting_error.message != nil || start.farewell_error.message != nil {
		if start.greeting_error.message != nil {
			fmt.eprintf("Greeting Workflow start failed: %s\n", string(start.greeting_error.message))
		}
		if start.farewell_error.message != nil {
			fmt.eprintf("Farewell Workflow start failed: %s\n", string(start.farewell_error.message))
		}
		temporal_client.error_free(&start.greeting_error)
		temporal_client.error_free(&start.farewell_error)
		worker.error_free(&run_error)
		return 1
	}
	defer temporal_client.workflow_handle_free(&start.greeting)
	defer temporal_client.workflow_handle_free(&start.farewell)
	if run_error.message != nil {
		fmt.eprintf("Worker run failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		return 1
	}

	greeting_result: string
	greeting_result_error := temporal_client.get_result_json(
		&start.greeting,
		&greeting_result,
	)
	defer delete(greeting_result)
	if greeting_result_error.message != nil {
		fmt.eprintf("Greeting Workflow result failed: %s\n", string(greeting_result_error.message))
		temporal_client.error_free(&greeting_result_error)
		return 1
	}
	farewell_result: workflow_app.Farewell_Result
	farewell_result_error := temporal_client.get_result_json(
		&start.farewell,
		&farewell_result,
	)
	defer delete(farewell_result.message)
	if farewell_result_error.message != nil {
		fmt.eprintf("Farewell Workflow result failed: %s\n", string(farewell_result_error.message))
		temporal_client.error_free(&farewell_result_error)
		return 1
	}
	manager_stats := workflow_isolate.manager_stats(&executor_manager)
	expected_generations := 5
	if cache_mode == "sticky" {
		expected_generations = 3
	}
	if first_mode == "normal" {
		expected_generations -= 1
	}
	expected_graceful_shutdowns := expected_generations - expected_activation_failures + 1
	expected_incremental_activations := 2
	expected_timer_sequences := cache_mode == "sticky" ? 2 : 4
	if greeting_result != "Hello from an isolated Workflow, Odin" ||
	   farewell_result.message != "Goodbye from the shared registry, Odin" ||
	   farewell_result.code != 42 || run_stats.workflow_executions_completed != 2 ||
	   run_stats.workflow_activation_failures != expected_activation_failures ||
	   manager_stats.child_exits != expected_child_exits ||
	   manager_stats.activation_timeouts != expected_timeouts ||
	   manager_stats.generations_started != expected_generations ||
	   manager_stats.evictions != expected_generations ||
	   manager_stats.graceful_shutdowns != expected_graceful_shutdowns ||
	   manager_stats.incremental_activations != expected_incremental_activations ||
	   manager_stats.manifest_handshakes != manager_stats.generations_started + 1 ||
	   manager_stats.manifest_mismatches != 0 ||
	   manager_stats.shutdown_timeouts != 0 ||
	   manager_stats.crash_loop_rejections != 0 ||
	   manager_stats.capacity_rejections != 0 ||
	   manager_stats.protocol_failures != expected_protocol_failures ||
	   observations.timer_commands_with_sequence != expected_timer_sequences ||
	   observations.timer_commands_without_sequence != 0 ||
	   observations.diagnostic_logs != 2 || observations.diagnostic_metrics != 2 ||
	   observations.malformed_diagnostics != 0 ||
	   manager_stats.diagnostics_received != 4 ||
	   workflow_isolate.manager_route_count(&executor_manager) != 0 {
		fmt.eprintf(
			"Unexpected registry isolation cache_mode=%s greeting=%q farewell=%q code=%d completed=%d failures=%d exits=%d timeouts=%d generations=%d expected_generations=%d evictions=%d graceful=%d expected_graceful=%d incremental=%d expected_incremental=%d handshakes=%d manifest_mismatches=%d shutdown_timeouts=%d crash_loop_rejections=%d capacity_rejections=%d protocol_failures=%d expected_protocol_failures=%d timer_sequences=%d expected_timer_sequences=%d missing_timer_sequences=%d diagnostic_logs=%d diagnostic_metrics=%d malformed_diagnostics=%d diagnostics_received=%d routes=%d\n",
			cache_mode,
			greeting_result,
			farewell_result.message,
			farewell_result.code,
			run_stats.workflow_executions_completed,
			run_stats.workflow_activation_failures,
			manager_stats.child_exits,
			manager_stats.activation_timeouts,
			manager_stats.generations_started,
			expected_generations,
			manager_stats.evictions,
			manager_stats.graceful_shutdowns,
			expected_graceful_shutdowns,
			manager_stats.incremental_activations,
			expected_incremental_activations,
			manager_stats.manifest_handshakes,
			manager_stats.manifest_mismatches,
			manager_stats.shutdown_timeouts,
			manager_stats.crash_loop_rejections,
			manager_stats.capacity_rejections,
			manager_stats.protocol_failures,
			expected_protocol_failures,
			observations.timer_commands_with_sequence,
			expected_timer_sequences,
			observations.timer_commands_without_sequence,
			observations.diagnostic_logs,
			observations.diagnostic_metrics,
			observations.malformed_diagnostics,
			manager_stats.diagnostics_received,
			workflow_isolate.manager_route_count(&executor_manager),
		)
		return 1
	}
	fmt.printf(
		"Core registry isolation completed greeting_id=%s farewell_id=%s greeting=%q farewell=%q code=%d registry_types=%d cache_mode=%s first_mode=%s generations=%d child_exits=%d activation_timeouts=%d evictions=%d replay_activations=%d incremental_activations=%d graceful_shutdowns=%d manifest_handshakes=%d manifest_mismatches=%d shutdown_timeouts=%d crash_loop_rejections=%d aborted_children=%d capacity_rejections=%d protocol_failures=%d timer_sequences=%d missing_timer_sequences=%d diagnostic_logs=%d diagnostic_metrics=%d diagnostics_received=%d\n",
		greeting_id,
		farewell_id,
		greeting_result,
		farewell_result.message,
		farewell_result.code,
		workflow.registry_count(&registry),
		cache_mode,
		first_mode,
		manager_stats.generations_started,
		manager_stats.child_exits,
		manager_stats.activation_timeouts,
		manager_stats.evictions,
		run_stats.workflow_replay_activations,
		manager_stats.incremental_activations,
		manager_stats.graceful_shutdowns,
		manager_stats.manifest_handshakes,
		manager_stats.manifest_mismatches,
		manager_stats.shutdown_timeouts,
		manager_stats.crash_loop_rejections,
		manager_stats.aborted_children,
		manager_stats.capacity_rejections,
		manager_stats.protocol_failures,
		observations.timer_commands_with_sequence,
		observations.timer_commands_without_sequence,
		observations.diagnostic_logs,
		observations.diagnostic_metrics,
		manager_stats.diagnostics_received,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

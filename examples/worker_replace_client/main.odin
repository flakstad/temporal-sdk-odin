package main

import "core:fmt"
import "core:os"
import "core:sync"
import "core:thread"
import "core:time"
import temporal "../../temporal"
import temporal_client "../../temporal/client"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-worker-replace-client"
WORKFLOW_TYPE :: "odin_worker_replace_client_workflow"

Run_State :: struct {
	sdk_worker: ^worker.Worker,
	ready:      sync.Sema,
	stats:      worker.Run_Stats,
	error:      worker.Error,
}

replacement_workflow :: proc(_: ^workflow.Fiber_Context, input: string) -> string {
	return fmt.tprintf("replacement:%s", input)
}

notify_ready :: proc(user_data: rawptr) -> bool {
	state := cast(^Run_State)user_data
	if state == nil || state.sdk_worker == nil {
		return false
	}
	sync.sema_post(&state.ready)
	return true
}

run_worker :: proc(state: ^Run_State) {
	state.stats, state.error = worker.run(
		state.sdk_worker,
		{
			max_workflow_executions = 1,
			on_ready = notify_ready,
			on_ready_data = state,
		},
	)
}

run :: proc() -> int {
	initial_target_buffer: [256]u8
	initial_target := os.get_env(
		initial_target_buffer[:],
		"TEMPORAL_INITIAL_TARGET",
	)
	if len(initial_target) == 0 {
		initial_target = "http://localhost:17233"
	}
	replacement_target_buffer: [256]u8
	replacement_target := os.get_env(
		replacement_target_buffer[:],
		"TEMPORAL_REPLACEMENT_TARGET",
	)
	if len(replacement_target) == 0 {
		replacement_target = "http://localhost:7233"
	}

	initial_client, initial_error := temporal_client.connect(initial_target)
	if initial_error.message != nil {
		fmt.eprintf("Initial Temporal connection failed: %s\n", string(initial_error.message))
		temporal_client.error_free(&initial_error)
		return 1
	}
	defer temporal_client.close(&initial_client)
	replacement_client, replacement_error := temporal_client.connect(replacement_target)
	if replacement_error.message != nil {
		fmt.eprintf("Replacement Temporal connection failed: %s\n", string(replacement_error.message))
		temporal_client.error_free(&replacement_error)
		return 1
	}
	defer temporal_client.close(&replacement_client)

	sdk_worker, create_error := worker.create(
		temporal_client.core_client(&initial_client),
		TASK_QUEUE,
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
	register_error := worker.register_workflow(
		&sdk_worker,
		WORKFLOW_TYPE,
		replacement_workflow,
	)
	if register_error.message != nil {
		fmt.eprintf("Workflow registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}

	workflow_id := fmt.aprintf("odin-worker-replace-%d", time.now()._nsec)
	defer delete(workflow_id)
	run_state := Run_State{
		sdk_worker = &sdk_worker,
	}
	run_thread := thread.create_and_start_with_poly_data(&run_state, run_worker)
	if run_thread == nil {
		fmt.eprintln("Could not start Worker run thread")
		return 1
	}
	sync.sema_wait(&run_state.ready)
	// Give the poll thread time to enter its long poll against the initial
	// Service. The integration gate then stops that Service and releases us
	// after the poll fails, matching Core's recovery use case. Without a gate,
	// replacement applies after the already-issued long poll returns.
	gate_buffer: [1024]u8
	gate_directory := os.get_env(
		gate_buffer[:],
		"TEMPORAL_REPLACEMENT_GATE_DIR",
	)
	if len(gate_directory) > 0 {
		ready_path := fmt.aprintf("%s/ready", gate_directory)
		defer delete(ready_path)
		continue_path := fmt.aprintf("%s/continue", gate_directory)
		defer delete(continue_path)
		make_error := os.make_directory(ready_path)
		if make_error != nil {
			fmt.eprintf("Could not publish replacement readiness: %v\n", make_error)
			worker.request_shutdown(&sdk_worker)
			thread.join(run_thread)
			thread.destroy(run_thread)
			worker.error_free(&run_state.error)
			return 1
		}
		wait_started := time.tick_now()
		for !os.exists(continue_path) {
			if time.tick_diff(wait_started, time.tick_now()) > 15 * time.Second {
				fmt.eprintln("Timed out waiting for replacement integration gate")
				worker.request_shutdown(&sdk_worker)
				thread.join(run_thread)
				thread.destroy(run_thread)
				worker.error_free(&run_state.error)
				return 1
			}
			time.sleep(10 * time.Millisecond)
		}
	} else {
		time.sleep(250 * time.Millisecond)
	}
	replace_error := temporal.worker_replace_client(
		&sdk_worker,
		&replacement_client,
	)
	if replace_error.message != nil {
		fmt.eprintf("Worker client replacement failed: %s\n", string(replace_error.message))
		worker.error_free(&replace_error)
		worker.request_shutdown(&sdk_worker)
		thread.join(run_thread)
		thread.destroy(run_thread)
		worker.error_free(&run_state.error)
		return 1
	}

	handle, start_error := temporal_client.start_workflow_json(
		&replacement_client,
		WORKFLOW_TYPE,
		"second-server",
		{
			id = workflow_id,
			task_queue = TASK_QUEUE,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	if start_error.message != nil {
		fmt.eprintf("Replacement Workflow start failed: %s\n", string(start_error.message))
		temporal_client.error_free(&start_error)
		worker.request_shutdown(&sdk_worker)
		thread.join(run_thread)
		thread.destroy(run_thread)
		worker.error_free(&run_state.error)
		return 1
	}
	defer temporal_client.workflow_handle_free(&handle)
	thread.join(run_thread)
	thread.destroy(run_thread)
	if run_state.error.message != nil {
		fmt.eprintf("Worker run failed: %s\n", string(run_state.error.message))
		worker.error_free(&run_state.error)
		return 1
	}
	if run_state.stats.workflow_executions_completed != 1 {
		fmt.eprintln("Replacement Worker did not complete exactly one Workflow")
		return 1
	}
	result: string
	result_error := temporal_client.get_result_json(&handle, &result)
	defer delete(result)
	if result_error.message != nil {
		fmt.eprintf("Replacement Workflow result failed: %s\n", string(result_error.message))
		temporal_client.error_free(&result_error)
		return 1
	}
	if result != "replacement:second-server" {
		fmt.eprintf("Unexpected replacement Workflow result: %q\n", result)
		return 1
	}

	fmt.printf(
		"Worker moved from %s to %s and completed workflow_id=%s.\n",
		initial_target,
		replacement_target,
		handle.id,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

package main

import "core:fmt"
import "core:os"
import "core:time"
import activity "../../temporal/activity"
import temporal_client "../../temporal/client"
import temporal_core "../../temporal/core"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-eager-activity"
WORKFLOW_TYPE :: "odin_eager_activity_workflow"

Start_State :: struct {
	client:      ^temporal_client.Client,
	workflow_id: string,
	handle:      temporal_client.Workflow_Handle,
	error:       temporal_client.Error,
}

hello :: proc(_: ^activity.Context, name: string) -> string {
	return fmt.aprintf("Hello %s", name)
}

eager_activity_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	return workflow.execute_activity(ctx, "hello", name)
}

start_when_worker_ready :: proc(user_data: rawptr) -> bool {
	state := cast(^Start_State)user_data
	if state == nil || state.client == nil {
		return false
	}
	state.handle, state.error = temporal_client.start_workflow_json(
		state.client,
		WORKFLOW_TYPE,
		"Temporal",
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
	// This target is intentionally narrow: the integration harness uses Core's
	// own debug event as proof that RespondWorkflowTaskCompleted returned an
	// eager Activity task. Merely observing Activity completion would also pass
	// when the task was delivered by the ordinary long poll.
	logging := temporal_core.Runtime_Logging_Options{
		filter = "warn,temporalio_sdk_core::worker::workflow=debug",
	}
	runtime_options := temporal_core.runtime_options_default()
	runtime_options.logging = &logging
	client, connect_error := temporal_client.connect_with_options({
		target_url = "http://localhost:7233",
		namespace = "default",
		runtime_options = &runtime_options,
	})
	if connect_error.message != nil {
		fmt.eprintf("Eager Activity Client connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	worker_options := worker.options_default(TASK_QUEUE)
	// A SimpleMaximum(1) poller consumes at most one long-poll permit. Keep a
	// deliberately roomy fixed pool so a permit is available when the Workflow
	// completion asks Core to reserve an eager Activity.
	worker_options.max_concurrent_activities = 10
	worker_options.activity_task_poller_behavior = worker.poller_simple_maximum(1)
	worker_options.max_eager_activity_reservations_per_workflow_task = 1
	sdk_worker, create_error := worker.create_with_options(
		temporal_client.core_client(&client),
		worker_options,
	)
	if create_error.message != nil {
		fmt.eprintf("Eager Activity Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		return 1
	}
	defer {
		close_error := worker.close(&sdk_worker)
		if close_error.message != nil {
			fmt.eprintf("Eager Activity Worker close failed: %s\n", string(close_error.message))
			worker.error_free(&close_error)
		}
	}

	register_activity_error := worker.register_activity(&sdk_worker, "hello", hello)
	if register_activity_error.message != nil {
		fmt.eprintf(
			"Eager Activity registration failed: %s\n",
			string(register_activity_error.message),
		)
		worker.error_free(&register_activity_error)
		return 1
	}
	register_workflow_error := worker.register_workflow(
		&sdk_worker,
		WORKFLOW_TYPE,
		eager_activity_workflow,
	)
	if register_workflow_error.message != nil {
		fmt.eprintf(
			"Eager Activity Workflow registration failed: %s\n",
			string(register_workflow_error.message),
		)
		worker.error_free(&register_workflow_error)
		return 1
	}

	id_buffer: [256]u8
	workflow_id := os.get_env(id_buffer[:], "TEMPORAL_EAGER_ACTIVITY_WORKFLOW_ID")
	owned_workflow_id := ""
	if len(workflow_id) == 0 {
		owned_workflow_id = fmt.aprintf("odin-eager-activity-%d", time.now()._nsec)
		defer delete(owned_workflow_id)
		workflow_id = owned_workflow_id
	}
	state := Start_State{client = &client, workflow_id = workflow_id}
	run_stats, run_error := worker.run(
		&sdk_worker,
		{
			max_activity_tasks = 1,
			max_workflow_executions = 1,
			on_ready = start_when_worker_ready,
			on_ready_data = &state,
		},
	)
	if run_error.message != nil {
		fmt.eprintf("Eager Activity Worker run failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		if state.error.message != nil {
			fmt.eprintf("Eager Activity Workflow start failed: %s\n", string(state.error.message))
			temporal_client.error_free(&state.error)
		}
		return 1
	}
	defer temporal_client.workflow_handle_free(&state.handle)
	if run_stats.activity_tasks_completed != 1 || run_stats.workflow_executions_completed != 1 {
		fmt.eprintf(
			"Unexpected eager Activity stats: activities=%d workflows=%d\n",
			run_stats.activity_tasks_completed,
			run_stats.workflow_executions_completed,
		)
		return 1
	}

	result: string
	result_error := temporal_client.get_result_json(&state.handle, &result)
	defer delete(result)
	if result_error.message != nil {
		fmt.eprintf("Eager Activity result failed: %s\n", string(result_error.message))
		temporal_client.error_free(&result_error)
		return 1
	}
	if result != "Hello Temporal" {
		fmt.eprintf("Unexpected eager Activity result: %q\n", result)
		return 1
	}
	fmt.printf(
		"Eager Activity completed workflow_id=%s run_id=%s\n",
		state.handle.id,
		state.handle.run_id,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

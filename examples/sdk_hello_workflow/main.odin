package main

import "core:fmt"
import "core:os"
import "core:time"
import activity "../../temporal/activity"
import temporal "../../temporal"
import temporal_client "../../temporal/client"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-sdk-hello-workflow"
WORKFLOW_TYPE :: "odin_sdk_hello_workflow"

Start_State :: struct {
	client:      ^temporal.Client,
	workflow_id: string,
	handle:      temporal_client.Workflow_Handle,
	error:       temporal.Client_Error,
}

hello :: proc(_: ^activity.Context, name: string) -> string {
	return fmt.aprintf("Hello %s", name)
}

hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	return workflow.execute_activity(ctx, "hello", name)
}

start_when_ready :: proc(user_data: rawptr) -> bool {
	state := cast(^Start_State)user_data
	if state == nil || state.client == nil do return false
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
	client, connect_error := temporal.connect()
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal.client_error_free(&connect_error)
		return 1
	}
	defer temporal.client_close(&client)

	sdk_worker, create_error := temporal.worker_create(&client, TASK_QUEUE)
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		temporal.worker_error_free(&create_error)
		return 1
	}
	defer {
		close_error := temporal.worker_close(&sdk_worker)
		if close_error.message != nil {
			fmt.eprintf("Worker close failed: %s\n", string(close_error.message))
			temporal.worker_error_free(&close_error)
		}
	}

	activity_error := temporal.worker_register_activity(&sdk_worker, "hello", hello)
	if activity_error.message != nil {
		fmt.eprintf("Activity registration failed: %s\n", string(activity_error.message))
		temporal.worker_error_free(&activity_error)
		return 1
	}
	workflow_error := temporal.worker_register_workflow(
		&sdk_worker,
		WORKFLOW_TYPE,
		hello_workflow,
	)
	if workflow_error.message != nil {
		fmt.eprintf("Workflow registration failed: %s\n", string(workflow_error.message))
		temporal.worker_error_free(&workflow_error)
		return 1
	}

	workflow_id := fmt.aprintf("odin-sdk-core-%d", time.now()._nsec)
	defer delete(workflow_id)
	state := Start_State{client = &client, workflow_id = workflow_id}
	stats, run_error := temporal.worker_run(
		&sdk_worker,
		{
			max_activity_tasks = 1,
			max_workflow_executions = 1,
			on_ready = start_when_ready,
			on_ready_data = &state,
		},
	)
	if run_error.message != nil {
		fmt.eprintf("Worker run failed: %s\n", string(run_error.message))
		temporal.worker_error_free(&run_error)
		if state.error.message != nil {
			fmt.eprintf("Workflow start failed: %s\n", string(state.error.message))
			temporal.client_error_free(&state.error)
		}
		return 1
	}
	defer temporal_client.workflow_handle_free(&state.handle)
	if stats.activity_tasks_completed != 1 || stats.workflow_executions_completed != 1 {
		fmt.eprintf(
			"Unexpected Worker stats: activities=%d workflows=%d\n",
			stats.activity_tasks_completed,
			stats.workflow_executions_completed,
		)
		return 1
	}

	result: string
	result_error := temporal_client.get_result_json(&state.handle, &result)
	defer delete(result)
	if result_error.message != nil {
		fmt.eprintf("Workflow result failed: %s\n", string(result_error.message))
		temporal.client_error_free(&result_error)
		return 1
	}
	if result != "Hello Temporal" {
		fmt.eprintf("Unexpected Workflow result: %q\n", result)
		return 1
	}

	fmt.printf(
		"Core SDK path completed workflow_id=%s run_id=%s result=%q\n",
		state.handle.id,
		state.handle.run_id,
		result,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

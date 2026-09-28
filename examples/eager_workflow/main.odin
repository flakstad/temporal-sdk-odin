package main

import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-eager-workflow"
WORKFLOW_TYPE :: "odin_eager_workflow"

Eager_Start_State :: struct {
	client:      ^temporal_client.Client,
	workflow_id: string,
	handle:      temporal_client.Workflow_Handle,
	error:       temporal_client.Error,
}

eager_workflow :: proc(_: ^workflow.Fiber_Context, _: string) -> string {
	return "Eager Workflow completed"
}

start_when_worker_ready :: proc(user_data: rawptr) -> bool {
	state := cast(^Eager_Start_State)user_data
	if state == nil || state.client == nil {
		return false
	}
	state.handle, state.error = temporal_client.start_workflow_json(
		state.client,
		WORKFLOW_TYPE,
		"eager-input",
		{
			id = state.workflow_id,
			task_queue = TASK_QUEUE,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
			enable_eager_workflow_start = true,
		},
	)
	return state.error.message == nil
}

run :: proc() -> int {
	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		fmt.eprintf("Eager Client connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	sdk_worker, create_error := worker.create(
		temporal_client.core_client(&client),
		TASK_QUEUE,
	)
	if create_error.message != nil {
		fmt.eprintf("Eager Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		return 1
	}
	defer {
		close_error := worker.close(&sdk_worker)
		if close_error.message != nil {
			fmt.eprintf("Eager Worker close failed: %s\n", string(close_error.message))
			worker.error_free(&close_error)
		}
	}

	register_error := worker.register_workflow(
		&sdk_worker,
		WORKFLOW_TYPE,
		eager_workflow,
	)
	if register_error.message != nil {
		fmt.eprintf("Eager Workflow registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}

	id_buffer: [256]u8
	workflow_id := os.get_env(id_buffer[:], "TEMPORAL_EAGER_WORKFLOW_ID")
	owned_workflow_id := ""
	if len(workflow_id) == 0 {
		owned_workflow_id = fmt.aprintf("odin-eager-%d", time.now()._nsec)
		defer delete(owned_workflow_id)
		workflow_id = owned_workflow_id
	}
	state := Eager_Start_State{client = &client, workflow_id = workflow_id}
	run_stats, run_error := worker.run(
		&sdk_worker,
		{
			max_workflow_executions = 1,
			on_ready = start_when_worker_ready,
			on_ready_data = &state,
		},
	)
	if run_error.message != nil {
		fmt.eprintf("Eager Worker run failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		if state.error.message != nil {
			fmt.eprintf("Eager Workflow start failed: %s\n", string(state.error.message))
			temporal_client.error_free(&state.error)
		}
		return 1
	}
	defer temporal_client.workflow_handle_free(&state.handle)
	if !state.handle.eager_execution_accepted {
		fmt.eprintln("Core did not accept the eager Workflow start")
		return 1
	}
	if run_stats.workflow_executions_completed != 1 {
		fmt.eprintln("Eager Worker did not complete exactly one Workflow")
		return 1
	}

	result: string
	result_error := temporal_client.get_result_json(&state.handle, &result)
	defer delete(result)
	if result_error.message != nil {
		fmt.eprintf("Eager Workflow result failed: %s\n", string(result_error.message))
		temporal_client.error_free(&result_error)
		return 1
	}
	if result != "Eager Workflow completed" {
		fmt.eprintf("Unexpected eager Workflow result: %q\n", result)
		return 1
	}
	fmt.printf(
		"Eager Workflow start accepted and completed workflow_id=%s run_id=%s\n",
		state.handle.id,
		state.handle.run_id,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

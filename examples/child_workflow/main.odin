package main

import "core:fmt"
import "core:os"
import "core:strings"
import "core:time"
import temporal_core "../../temporal/core"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-child-workflow"
PARENT_WORKFLOW_TYPE :: "odin_parent_workflow"
CHILD_WORKFLOW_TYPE :: "odin_child_workflow"

child_workflow :: proc(_: ^workflow.Fiber_Context, name: string) -> string {
	message, allocation_error := strings.concatenate({"Hello from child, ", name})
	if allocation_error != nil {
		return ""
	}
	return message
}

parent_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	child_id, allocation_error := strings.concatenate({"odin-child-", name})
	if allocation_error != nil {
		return ""
	}
	return workflow.execute_child_workflow(
		ctx,
		CHILD_WORKFLOW_TYPE,
		name,
		{
			workflow_id = child_id,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
}

main :: proc() {
	runtime, runtime_error := temporal_core.runtime_create()
	if runtime_error.message != nil {
		fmt.eprintf("Core runtime initialization failed: %s\n", string(runtime_error.message))
		temporal_core.error_free(&runtime_error)
		os.exit(1)
	}
	defer temporal_core.runtime_close(&runtime)

	client, connect_error := temporal_core.client_connect(&runtime)
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal_core.error_free(&connect_error)
		os.exit(1)
	}
	defer temporal_core.client_close(&client)

	sdk_worker, create_error := worker.create(&client, TASK_QUEUE)
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		os.exit(1)
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}

	parent_error := worker.register_workflow(
		&sdk_worker,
		PARENT_WORKFLOW_TYPE,
		parent_workflow,
	)
	if parent_error.message != nil {
		fmt.eprintf("Parent registration failed: %s\n", string(parent_error.message))
		worker.error_free(&parent_error)
		os.exit(1)
	}
	child_error := worker.register_workflow(
		&sdk_worker,
		CHILD_WORKFLOW_TYPE,
		child_workflow,
	)
	if child_error.message != nil {
		fmt.eprintf("Child registration failed: %s\n", string(child_error.message))
		worker.error_free(&child_error)
		os.exit(1)
	}

	fmt.printf("Odin Child Workflow Worker is polling task queue %s\n", TASK_QUEUE)
	stats, run_error := worker.run(
		&sdk_worker,
		{
			max_workflow_executions = 2,
			disable_activity_polling = true,
		},
	)
	if run_error.message != nil {
		fmt.eprintf("Worker run failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		os.exit(1)
	}
	if stats.workflow_executions_completed != 2 || stats.workflow_evictions < 2 {
		fmt.eprintf(
			"Unexpected Child Workflow stats: completed=%d evictions=%d\n",
			stats.workflow_executions_completed,
			stats.workflow_evictions,
		)
		os.exit(1)
	}
	fmt.println("Child Workflow completed end-to-end and worker shut down cleanly")
}

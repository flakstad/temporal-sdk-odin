package main

import "core:fmt"
import "core:os"
import "core:strings"
import "core:time"
import temporal_core "../../temporal/core"
import codec "../../temporal/codec"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-child-workflow"
PARENT_WORKFLOW_TYPE :: "odin_parent_workflow"
CHILD_WORKFLOW_TYPE :: "odin_child_workflow"
FAILURE_PARENT_WORKFLOW_TYPE :: "odin_child_failure_parent_workflow"
FAILURE_CHILD_WORKFLOW_TYPE :: "odin_failing_child_workflow"

child_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	if strings.has_prefix(name, "cancel-") {
		workflow.sleep(ctx, time.Hour)
		return "child cancellation was not delivered"
	}
	message, allocation_error := strings.concatenate({"Hello from child, ", name})
	if allocation_error != nil {
		return ""
	}
	return message
}

failing_child_workflow :: proc(
	_: ^workflow.Fiber_Context,
	_: codec.Payload_View,
) -> workflow.Payload_Result {
	return workflow.payload_fail(
		"child rejected input",
		"OdinChildRejected",
	)
}

failure_parent_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	result := workflow.execute_child_workflow_json_result(
		ctx,
		FAILURE_CHILD_WORKFLOW_TYPE,
		name,
		{
			workflow_id = "odin-failing-child",
			workflow_execution_timeout = time.Minute,
		},
		string,
	)
	if result.status != .Failed ||
	   result.failure.workflow_id != "odin-failing-child" ||
	   result.failure.workflow_type != FAILURE_CHILD_WORKFLOW_TYPE ||
	   !result.failure.application.present ||
	   result.failure.application.message != "child rejected input" ||
	   result.failure.application.type != "OdinChildRejected" ||
	   !result.failure.application.non_retryable {
		return "structured Child Workflow failure mismatch"
	}
	return "Observed structured Child Workflow failure"
}

observe_activation :: proc(processed: ^workflow.Process_Result) {
	fmt.printf(
		"child activation history=%d replay=%v started=%d start_resolved=%d resolved=%d cancel_requested=%v canceled_child=%d canceled_workflow=%v completed=%v evicted=%v\n",
		processed.history_length,
		processed.is_replaying,
		processed.started_child_workflow_count,
		processed.resolved_child_workflow_start_seq,
		processed.resolved_child_workflow_seq,
		processed.requested_child_workflow_cancellation,
		processed.canceled_child_workflow_seq,
		processed.canceled_workflow,
		processed.completed,
		processed.evicted,
	)
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
	failure_parent_error := worker.register_workflow(
		&sdk_worker,
		FAILURE_PARENT_WORKFLOW_TYPE,
		failure_parent_workflow,
	)
	if failure_parent_error.message != nil {
		fmt.eprintf("Failure parent registration failed: %s\n", string(failure_parent_error.message))
		worker.error_free(&failure_parent_error)
		os.exit(1)
	}
	failure_child_error := worker.register_payload_workflow(
		&sdk_worker,
		FAILURE_CHILD_WORKFLOW_TYPE,
		failing_child_workflow,
	)
	if failure_child_error.message != nil {
		fmt.eprintf("Failure child registration failed: %s\n", string(failure_child_error.message))
		worker.error_free(&failure_child_error)
		os.exit(1)
	}

	fmt.printf("Odin Child Workflow Worker is polling task queue %s\n", TASK_QUEUE)
	stats, run_error := worker.run(
		&sdk_worker,
		{
			max_workflow_executions = 2,
			disable_activity_polling = true,
			on_workflow_activation = observe_activation,
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

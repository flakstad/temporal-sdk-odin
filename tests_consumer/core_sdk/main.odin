package main

import "core:fmt"
import "core:os"
import "core:time"
import activity "sdk:temporal/activity"
import temporal "sdk:temporal"
import temporal_client "sdk:temporal/client"
import temporal_core "sdk:temporal/core"
import workflow "sdk:temporal/workflow"

hello :: proc(_: ^activity.Context, name: string) -> string {
	return name
}

hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	return workflow.execute_activity(ctx, "hello", name)
}

// This is compiled but deliberately not called: it proves that a consumer can
// instantiate the ordinary typed Client start/result API without a server.
compile_client_contract :: proc(client: ^temporal.Client) {
	handle, start_error := temporal_client.start_workflow_json(
		client,
		"consumer_workflow",
		"Temporal",
		{
			id = "consumer-workflow-id",
			task_queue = "consumer-queue",
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	temporal_client.workflow_handle_free(&handle)
	temporal.client_error_free(&start_error)
}

fail :: proc(message: string) {
	fmt.eprintln(message)
	os.exit(1)
}

main :: proc() {
	for iteration in 0..<16 {
		runtime, runtime_error := temporal_core.runtime_create()
		if runtime_error.message != nil {
			temporal_core.error_free(&runtime_error)
			fail(fmt.tprintf("external consumer could not initialize SDK Core at iteration %d", iteration))
		}
		temporal_core.runtime_close(&runtime)
		temporal_core.runtime_close(&runtime)
	}

	options := temporal.worker_options_default(nil, "consumer-queue")
	if options.namespace != "default" || options.task_queue != "consumer-queue" {
		fail("external consumer observed invalid facade defaults")
	}

	worker := temporal.Worker{}
	activity_error := temporal.worker_register_activity(&worker, "hello", hello)
	if activity_error.message == nil {
		fail("external consumer accepted registration on an empty Worker")
	}
	temporal.worker_error_free(&activity_error)
	workflow_error := temporal.worker_register_workflow(
		&worker,
		"consumer_workflow",
		hello_workflow,
	)
	if workflow_error.message == nil {
		fail("external consumer accepted Workflow registration on an empty Worker")
	}
	temporal.worker_error_free(&workflow_error)

	fmt.println("External Core SDK consumer completed 16 init/double-close cycles")
}

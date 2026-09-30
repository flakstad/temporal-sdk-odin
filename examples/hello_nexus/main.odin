package main

import "core:fmt"
import "core:os"
import nexus "../../temporal/nexus"
import temporal_core "../../temporal/core"
import worker "../../temporal/worker"

hello :: proc(ctx: ^nexus.Context, name: string) -> string {
	operation_info := nexus.info(ctx)
	if operation_info.service != "odin-greeting-service" ||
	   operation_info.operation != "hello" ||
	   len(operation_info.request_id) == 0 ||
	   len(operation_info.task_token) == 0 {
		return "invalid Nexus operation context"
	}
	return fmt.aprintf("Hello %s", name)
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

	options := worker.options_default("odin-hello-nexus")
	options.identity_override = "temporal-sdk-odin-hello-nexus"
	options.max_concurrent_nexus_tasks = 2
	options.nexus_task_poller_behavior = worker.poller_simple_maximum(1)
	nexus_worker, create_error := worker.create_with_options(&client, options)
	if create_error.message != nil {
		fmt.eprintf("Nexus Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		os.exit(1)
	}

	register_error := worker.register_nexus_operation(
		&nexus_worker,
		"odin-greeting-service",
		"hello",
		hello,
	)
	if register_error.message != nil {
		fmt.eprintf("Nexus operation registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		_ = worker.close(&nexus_worker)
		os.exit(1)
	}

	fmt.println("Odin Nexus Worker is polling task queue odin-hello-nexus")
	stats, run_error := worker.run(&nexus_worker, {max_nexus_tasks = 1})
	close_error := worker.close(&nexus_worker)
	if run_error.message != nil {
		fmt.eprintf("Nexus Worker failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		worker.error_free(&close_error)
		os.exit(1)
	}
	if close_error.message != nil {
		fmt.eprintf("Nexus Worker shutdown failed: %s\n", string(close_error.message))
		worker.error_free(&close_error)
		os.exit(1)
	}
	if stats.nexus_tasks_completed != 1 || stats.nexus_executions_peak_owned != 1 {
		fmt.eprintf(
			"Unexpected Nexus Worker stats: completed=%d peak=%d\n",
			stats.nexus_tasks_completed,
			stats.nexus_executions_peak_owned,
		)
		os.exit(1)
	}
	fmt.println("one synchronous Nexus operation completed and worker shut down cleanly")
}

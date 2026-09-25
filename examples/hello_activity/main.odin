package main

import "core:fmt"
import "core:os"
import "core:strings"
import activity "../../temporal/activity"
import temporal_core "../../temporal/core"
import worker "../../temporal/worker"

hello :: proc(ctx: ^activity.Context, name: string) -> string {
	_ = ctx
	greeting, allocation_error := strings.concatenate({"Hello ", name})
	if allocation_error != nil {
		return ""
	}
	return greeting
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

	activity_worker, create_error := worker.create(&client, "odin-hello-activity")
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		os.exit(1)
	}

	register_error := worker.register_activity(&activity_worker, "hello", hello)
	if register_error.message != nil {
		fmt.eprintf("Activity registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		_ = worker.close(&activity_worker)
		os.exit(1)
	}

	fmt.println("Odin Activity Worker is polling task queue odin-hello-activity")
	_, run_error := worker.run(
		&activity_worker,
		{max_activity_tasks = 1},
	)
	close_error := worker.close(&activity_worker)
	if run_error.message != nil {
		fmt.eprintf("Worker failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		worker.error_free(&close_error)
		os.exit(1)
	}
	if close_error.message != nil {
		fmt.eprintf("Worker shutdown failed: %s\n", string(close_error.message))
		worker.error_free(&close_error)
		os.exit(1)
	}
	fmt.println("one Activity Task completed and worker shut down cleanly")
}

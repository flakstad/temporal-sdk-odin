package main

import "core:fmt"
import "core:os"
import activity "../../temporal/activity"
import temporal "../../temporal"

TASK_QUEUE :: "odin-sdk-facade"

hello :: proc(_: ^activity.Context, name: string) -> string {
	return fmt.tprintf("Hello %s", name)
}

main :: proc() {
	client, connect_error := temporal.connect()
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal.client_error_free(&connect_error)
		os.exit(1)
	}
	defer temporal.client_close(&client)

	sdk_worker, create_error := temporal.worker_create(&client, TASK_QUEUE)
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		temporal.worker_error_free(&create_error)
		os.exit(1)
	}
	defer {
		close_error := temporal.worker_close(&sdk_worker)
		temporal.worker_error_free(&close_error)
	}

	registration_error := temporal.worker_register_activity(&sdk_worker, "hello", hello)
	if registration_error.message != nil {
		fmt.eprintf("Activity registration failed: %s\n", string(registration_error.message))
		temporal.worker_error_free(&registration_error)
		os.exit(1)
	}

	stats, run_error := temporal.worker_run(&sdk_worker, {max_activity_tasks = 1})
	if run_error.message != nil {
		fmt.eprintf("Worker failed: %s\n", string(run_error.message))
		temporal.worker_error_free(&run_error)
		os.exit(1)
	}
	fmt.printf("Top-level SDK Worker completed %d Activity Task.\n", stats.activity_tasks_completed)
}

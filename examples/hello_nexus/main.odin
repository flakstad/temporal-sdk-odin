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

wait_for_cancel :: proc(ctx: ^nexus.Context, _: string) -> nexus.Result(string) {
	operation_info := nexus.info(ctx)
	token := fmt.aprintf("standalone-%s", operation_info.request_id)
	defer delete(token)
	return nexus.result_async_copy(string, token)
}

cancel_wait :: proc(ctx: ^nexus.Context, operation_token: string) -> nexus.Cancel_Result {
	operation_info := nexus.info(ctx)
	if len(operation_token) == 0 || operation_info.operation_token != operation_token {
		return nexus.cancel_handler_error(
			"standalone cancellation token mismatch",
			"BAD_REQUEST",
			.Non_Retryable,
		)
	}
	return nexus.cancel_complete()
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
	cancel_mode_buffer: [64]u8
	cancel_mode := os.get_env(cancel_mode_buffer[:], "TEMPORAL_NEXUS_STANDALONE_CANCEL") == "1"
	lifecycle_mode_buffer: [64]u8
	lifecycle_mode := os.get_env(lifecycle_mode_buffer[:], "TEMPORAL_NEXUS_STANDALONE_LIFECYCLE") == "1"
	max_tasks := 1
	if cancel_mode {
		max_tasks = 2
		wait_error := worker.register_json_nexus_operation_result(
			&nexus_worker,
			"odin-greeting-service",
			"wait-for-cancel",
			wait_for_cancel,
		)
		if wait_error.message != nil {
			fmt.eprintf("Nexus async operation registration failed: %s\n", string(wait_error.message))
			worker.error_free(&wait_error)
			_ = worker.close(&nexus_worker)
			os.exit(1)
		}
		cancel_error := worker.register_nexus_cancel_handler(
			&nexus_worker,
			"odin-greeting-service",
			"wait-for-cancel",
			cancel_wait,
		)
		if cancel_error.message != nil {
			fmt.eprintf("Nexus cancel handler registration failed: %s\n", string(cancel_error.message))
			worker.error_free(&cancel_error)
			_ = worker.close(&nexus_worker)
			os.exit(1)
		}
	} else if lifecycle_mode {
		wait_error := worker.register_json_nexus_operation_result(
			&nexus_worker,
			"odin-greeting-service",
			"wait-for-terminate",
			wait_for_cancel,
		)
		if wait_error.message != nil {
			fmt.eprintf("Nexus async operation registration failed: %s\n", string(wait_error.message))
			worker.error_free(&wait_error)
			_ = worker.close(&nexus_worker)
			os.exit(1)
		}
	}

	fmt.println("Odin Nexus Worker is polling task queue odin-hello-nexus")
	stats, run_error := worker.run(&nexus_worker, {max_nexus_tasks = max_tasks})
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
	if stats.nexus_tasks_completed != max_tasks || stats.nexus_executions_peak_owned < 1 ||
	   stats.nexus_executions_peak_owned > max_tasks {
		fmt.eprintf(
			"Unexpected Nexus Worker stats: completed=%d peak=%d\n",
			stats.nexus_tasks_completed,
			stats.nexus_executions_peak_owned,
		)
		os.exit(1)
	}
	if cancel_mode {
		if stats.nexus_cancel_operations_received != 1 {
			fmt.eprintf(
				"Expected one standalone Nexus cancellation, observed %d\n",
				stats.nexus_cancel_operations_received,
			)
			os.exit(1)
		}
		fmt.println("one asynchronous Nexus operation was canceled and worker shut down cleanly")
	} else if lifecycle_mode {
		fmt.println("one asynchronous Nexus operation was started for lifecycle control and worker shut down cleanly")
	} else {
		fmt.println("one synchronous Nexus operation completed and worker shut down cleanly")
	}
}

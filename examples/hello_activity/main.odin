package main

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:fmt"
import "core:mem"
import "core:os"
import "core:strings"
import "core:sync"
import "core:time"
import activity "../../temporal/activity"
import temporal_client "../../temporal/client"
import codec "../../temporal/codec"
import temporal_core "../../temporal/core"
import worker "../../temporal/worker"

concurrent_activities: int
concurrent_overlap_observed: bool
async_task_token: []u8
async_workflow_id: string
async_workflow_run_id: string
async_activity_id: string

Greeting_Input :: struct {
	name:       string,
	salutation: string,
}

Greeting_Output :: struct {
	message:     string,
	name_length: int,
}

Retry_Greeting_Input :: struct {
	name: string,
}

Retry_Greeting_Output :: struct {
	message:             string,
	attempt:             u32,
	heartbeat_completed: int,
}

Retry_Failure_Details :: struct {
	dependency: string,
	attempt:    u32,
}

Retry_Heartbeat_Details :: struct {
	completed: int,
}

Async_Heartbeat_Details :: struct {
	mode:      string,
	completed: int,
}

Async_Failure_Details :: struct {
	dependency: string,
	mode:       string,
}

Async_Cancellation_Details :: struct {
	reason: string,
	mode:   string,
}

async_identity_free :: proc() {
	heap := allocator_runtime.heap_allocator()
	delete(async_task_token, heap)
	delete(async_workflow_id, heap)
	delete(async_workflow_run_id, heap)
	delete(async_activity_id, heap)
	async_task_token = nil
	async_workflow_id = ""
	async_workflow_run_id = ""
	async_activity_id = ""
}

hello :: proc(ctx: ^activity.Context, name: string) -> string {
	_ = ctx
	if strings.has_prefix(name, "concurrent-") {
		sync.atomic_add(&concurrent_activities, 1)
		defer sync.atomic_sub(&concurrent_activities, 1)
		if sync.atomic_load(&concurrent_activities) >= 2 {
			sync.atomic_store_explicit(&concurrent_overlap_observed, true, .Release)
		}
		fmt.printf(
			"Activity %s entered concurrency barrier (active=%d)\n",
			name,
			sync.atomic_load(&concurrent_activities),
		)
		for _ in 0 ..< 500 {
			if sync.atomic_load_explicit(&concurrent_overlap_observed, .Acquire) {
				break
			}
			time.sleep(10 * time.Millisecond)
		}
		if !sync.atomic_load_explicit(&concurrent_overlap_observed, .Acquire) {
			return "Activity executor did not overlap callbacks"
		}
	}
	greeting, allocation_error := strings.concatenate({"Hello ", name})
	if allocation_error != nil {
		return ""
	}
	return greeting
}

typed_greeting :: proc(_: ^activity.Context, input: Greeting_Input) -> Greeting_Output {
	message, allocation_error := strings.concatenate({input.salutation, ", ", input.name, "!"})
	if allocation_error != nil {
		return {}
	}
	return {message = message, name_length = len(input.name)}
}

retry_greeting :: proc(
	ctx: ^activity.Context,
	input: Retry_Greeting_Input,
) -> activity.Result(Retry_Greeting_Output) {
	current_attempt := activity.attempt(ctx)
	if current_attempt == 1 {
		heartbeat_status := activity.heartbeat_json(ctx, Retry_Heartbeat_Details{completed = 17})
		if heartbeat_status != .Recorded {
			return activity.result_failure(
				Retry_Greeting_Output,
				"could not record retry progress heartbeat",
				failure_type = "OdinHeartbeatError",
				non_retryable = true,
			)
		}
		return activity.result_failure_json(
			Retry_Greeting_Output,
			"greeting dependency is temporarily unavailable",
			Retry_Failure_Details{dependency = "greeting-service", attempt = current_attempt},
			failure_type = "TransientGreetingError",
			next_retry_delay = 10 * time.Millisecond,
		)
	}
	heartbeat_details: Retry_Heartbeat_Details
	detail_found, detail_error := activity.heartbeat_detail_json(ctx, 0, &heartbeat_details)
	if !detail_found || detail_error != .None {
		return activity.result_failure(
			Retry_Greeting_Output,
			"retry did not receive the previous heartbeat details",
			failure_type = "OdinHeartbeatDetailsError",
			non_retryable = true,
		)
	}
	message, allocation_error := strings.concatenate({"Hello after retry, ", input.name, "!"})
	if allocation_error != nil {
		return activity.result_failure(
			Retry_Greeting_Output,
			"could not allocate greeting result",
			failure_type = "OdinAllocationError",
			non_retryable = true,
		)
	}
	return activity.result_success(
		Retry_Greeting_Output{
			message = message,
			attempt = current_attempt,
			heartbeat_completed = heartbeat_details.completed,
		},
	)
}

async_hello :: proc(ctx: ^activity.Context, name: string) -> activity.Result(string) {
	if name != "Odin" && name != "__cancel__" {
		return activity.result_failure(
			string,
			"async hello expected Odin",
			failure_type = "OdinAsyncActivityInputError",
			non_retryable = true,
		)
	}
	info := activity.info(ctx)
	if len(info.task_token) == 0 {
		return activity.result_failure(
			string,
			"async Activity task token is missing",
			failure_type = "OdinAsyncActivityTokenError",
			non_retryable = true,
		)
	}
	if len(info.workflow_id) == 0 || len(info.workflow_run_id) == 0 || len(info.activity_id) == 0 {
		return activity.result_failure(
			string,
			"async Activity execution identity is incomplete",
			failure_type = "OdinAsyncActivityIdentityError",
			non_retryable = true,
		)
	}
	heap := allocator_runtime.heap_allocator()
	async_identity_free()
	async_task_token = bytes.clone(info.task_token, heap)
	workflow_id_error: mem.Allocator_Error
	async_workflow_id, workflow_id_error = strings.clone(info.workflow_id, heap)
	workflow_run_id_error: mem.Allocator_Error
	async_workflow_run_id, workflow_run_id_error = strings.clone(info.workflow_run_id, heap)
	activity_id_error: mem.Allocator_Error
	async_activity_id, activity_id_error = strings.clone(info.activity_id, heap)
	if len(async_task_token) == 0 || workflow_id_error != nil ||
	   workflow_run_id_error != nil || activity_id_error != nil {
		async_identity_free()
		return activity.result_failure(
			string,
			"could not preserve async Activity execution identity",
			failure_type = "OdinAllocationError",
			non_retryable = true,
		)
	}
	return activity.result_will_complete_async(string)
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
	register_typed_error := worker.register_json_activity(
		&activity_worker,
		"typed_greeting",
		typed_greeting,
	)
	if register_typed_error.message != nil {
		fmt.eprintf("Typed Activity registration failed: %s\n", string(register_typed_error.message))
		worker.error_free(&register_typed_error)
		_ = worker.close(&activity_worker)
		os.exit(1)
	}
	register_retry_error := worker.register_json_activity_result(
		&activity_worker,
		"retry_greeting",
		retry_greeting,
	)
	if register_retry_error.message != nil {
		fmt.eprintf("Retry Activity registration failed: %s\n", string(register_retry_error.message))
		worker.error_free(&register_retry_error)
		_ = worker.close(&activity_worker)
		os.exit(1)
	}
	register_async_error := worker.register_json_activity_result(
		&activity_worker,
		"async_hello",
		async_hello,
	)
	if register_async_error.message != nil {
		fmt.eprintf("Async Activity registration failed: %s\n", string(register_async_error.message))
		worker.error_free(&register_async_error)
		_ = worker.close(&activity_worker)
		os.exit(1)
	}

	fmt.println("Odin Activity Worker is polling task queue odin-hello-activity")
	env_buffer: [64]u8
	concurrency_value := os.get_env(env_buffer[:], "ACTIVITY_EXPECT_CONCURRENT")
	concurrency_case := concurrency_value != ""
	retry_value := os.get_env(env_buffer[:], "ACTIVITY_EXPECT_RETRY")
	retry_case := retry_value != ""
	async_value := os.get_env(env_buffer[:], "ACTIVITY_EXPECT_ASYNC")
	async_case := async_value != ""
	async_mode := os.get_env(env_buffer[:], "ACTIVITY_ASYNC_MODE")
	if async_case && len(async_mode) == 0 {
		async_mode = "token-complete"
	}
	max_tasks := 1
	if concurrency_case || retry_case {
		max_tasks = 2
	}
	_, run_error := worker.run(
		&activity_worker,
		{max_activity_tasks = max_tasks},
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
	if async_case {
		defer async_identity_free()
		if len(async_task_token) == 0 || len(async_workflow_id) == 0 ||
		   len(async_workflow_run_id) == 0 || len(async_activity_id) == 0 {
			fmt.eprintln("Async Activity did not preserve its execution identity")
			os.exit(1)
		}
		completion_client, completion_connect_error := temporal_client.connect()
		if completion_connect_error.message != nil {
			fmt.eprintf(
				"Async Activity Client connection failed: %s\n",
				string(completion_connect_error.message),
			)
			temporal_client.error_free(&completion_connect_error)
			os.exit(1)
		}
		defer temporal_client.close(&completion_client)
		identifier := temporal_client.async_activity_from_task_token(async_task_token)
		if async_mode == "id-complete" || async_mode == "id-cancel" {
			identifier = temporal_client.async_activity_from_workflow(
				async_workflow_id,
				async_activity_id,
				async_workflow_run_id,
			)
		}
		heartbeat: temporal_client.Async_Activity_Heartbeat
		heartbeat_error: temporal_client.Error
		for attempt in 0 ..< 100 {
			heartbeat, heartbeat_error = temporal_client.heartbeat_async_activity_json(
				&completion_client,
				identifier,
				Async_Heartbeat_Details{mode = async_mode, completed = 17},
			)
			if heartbeat_error.message != nil || async_mode != "id-cancel" || heartbeat.cancel_requested {
				break
			}
			time.sleep(20 * time.Millisecond)
		}
		if heartbeat_error.message != nil {
			fmt.eprintf(
				"Async Activity heartbeat failed: %s\n",
				string(heartbeat_error.message),
			)
			temporal_client.error_free(&heartbeat_error)
			os.exit(1)
		}
		if async_mode == "id-cancel" && !heartbeat.cancel_requested {
			fmt.eprintln("Async Activity cancellation was not requested before its deadline")
			os.exit(1)
		}
		if (async_mode != "id-cancel" && heartbeat.cancel_requested) ||
		   heartbeat.activity_paused || heartbeat.activity_reset {
			fmt.eprintln("Async Activity heartbeat unexpectedly requested a state transition")
			os.exit(1)
		}
		terminal_error: temporal_client.Error
		switch async_mode {
		case "token-complete":
			terminal_error = temporal_client.complete_async_activity_json(
				&completion_client,
				identifier,
				"Hello async Odin",
			)
		case "id-complete":
			terminal_error = temporal_client.complete_async_activity_json(
				&completion_client,
				identifier,
				"Hello async by ID Odin",
			)
		case "token-failure":
			failure_result := activity.result_failure_json(
				string,
				"async dependency failed",
				Async_Failure_Details{dependency = "async-greeting", mode = async_mode},
				failure_type = "AsyncGreetingError",
				non_retryable = true,
			)
			defer activity.application_failure_free(&failure_result.failure)
			heartbeat_payload, encode_error := codec.encode_json(
				Async_Heartbeat_Details{mode = async_mode, completed = 17},
			)
			if encode_error != .None {
				fmt.eprintln("Could not encode async Activity final heartbeat details")
				os.exit(1)
			}
			defer codec.payload_free(&heartbeat_payload)
			heartbeat_view := codec.payload_view(&heartbeat_payload)
			terminal_error = temporal_client.fail_async_activity(
				&completion_client,
				identifier,
				failure_result.failure,
				&heartbeat_view,
			)
		case "id-cancel":
			terminal_error = temporal_client.report_async_activity_cancellation_json(
				&completion_client,
				identifier,
				Async_Cancellation_Details{reason = "integration requested cancellation", mode = async_mode},
			)
		case:
			fmt.eprintf("Unsupported ACTIVITY_ASYNC_MODE: %s\n", async_mode)
			os.exit(1)
		}
		if terminal_error.message != nil {
			fmt.eprintf("Async Activity terminal operation failed: %s\n", string(terminal_error.message))
			temporal_client.error_free(&terminal_error)
			os.exit(1)
		}
	}
	if concurrency_case {
		fmt.println("two Activity callbacks overlapped and worker shut down cleanly")
	} else if retry_case {
		fmt.println("retryable Activity failed once, then completed and worker shut down cleanly")
	} else if async_case {
		fmt.printf("Activity deferred completion and Odin Client applied mode=%s\n", async_mode)
	} else {
		fmt.println("one Activity Task completed and worker shut down cleanly")
	}
}

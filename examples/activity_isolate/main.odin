package main

import "core:fmt"
import "core:os"
import "core:sync"
import "core:thread"
import "core:time"
import activity "../../temporal/activity"
import activity_isolate "../../temporal/activity/isolate"
import codec "../../temporal/codec"
import temporal_core "../../temporal/core"
import worker "../../temporal/worker"

parent_heartbeat_interceptor_calls: int

parent_heartbeat_interceptor :: proc(
	_: rawptr,
	input: ^worker.Activity_Heartbeat_Input,
	next: worker.Activity_Heartbeat_Next,
) -> activity.Heartbeat_Status {
	sync.atomic_add_explicit(&parent_heartbeat_interceptor_calls, 1, .Acq_Rel)
	return worker.activity_heartbeat_next(next, input)
}

Cancellation_Shutdown_State :: struct {
	manager:    ^activity_isolate.Manager,
	sdk_worker: ^worker.Worker,
}

shutdown_after_cancellation :: proc(state: ^Cancellation_Shutdown_State) {
	for _ in 0 ..< 6_000 {
		stats := activity_isolate.manager_stats(state.manager)
		if stats.children_completed >= 1 || stats.child_failures > 0 {
			worker.request_shutdown(state.sdk_worker)
			return
		}
		time.sleep(10 * time.Millisecond)
	}
	worker.request_shutdown(state.sdk_worker)
}

isolated_hello :: proc(ctx: ^activity.Context, inputs: []codec.Payload_View) -> activity.Payload_Result {
	if len(inputs) != 1 {
		return activity.payload_fail(
			"isolated hello expected one input",
			failure_type = "OdinActivityArgumentCountError",
		)
	}
	name: string
	if codec.decode_json(inputs[0], &name) != .None {
		return activity.payload_fail(
			"isolated hello input was not json/plain",
			failure_type = "OdinPayloadConversionError",
		)
	}
	defer delete(name)
	attempt := activity.attempt(ctx)
	if name == "cancel-uncooperative" {
		// Heartbeat until Core delivers cancellation, then deliberately ignore
		// the child Context so the manager's bounded fallback must terminate
		// this one invocation.
		for !activity.is_cancellation_requested(ctx) {
			_ = activity.heartbeat(ctx, "waiting before ignoring cancellation")
			time.sleep(10 * time.Millisecond)
		}
		time.sleep(2 * time.Second)
		payload, encode_error := codec.encode_json("unexpected uncooperative success")
		if encode_error != .None {
			return activity.payload_fail("could not encode uncooperative result")
		}
		return activity.payload_complete(payload)
	}
	if name == "cancel-isolated" {
		for !activity.is_cancellation_requested(ctx) {
			status := activity.heartbeat(ctx, "waiting for cooperative cancellation")
			if status == .Cancellation_Requested {
				break
			}
			if status != .Recorded {
				return activity.payload_fail(
					"isolated Activity cancellation heartbeat failed",
					failure_type = "OdinIsolatedActivityHeartbeatError",
				)
			}
			time.sleep(10 * time.Millisecond)
		}
		reason := activity.cancellation_reason(ctx)
		details, present := activity.cancellation_details(ctx)
		if reason != .Cancelled || !present || !details.is_cancelled {
			return activity.payload_fail(
				"isolated Activity received incomplete cancellation metadata",
				failure_type = "OdinIsolatedActivityCancellationError",
			)
		}
		return activity.payload_cancel_json(
			"child acknowledged cooperative cancellation",
			"isolated Activity canceled",
		)
	}
	if name == "panic-recovery" && attempt == 1 {
		fmt.eprintln("isolated Activity executor forcing panic on attempt 1")
		panic("intentional per-invocation Activity executor panic")
	}
	if name == "panic-recovery" {
		heartbeat_status := activity.heartbeat_json(ctx, attempt)
		if heartbeat_status != .Recorded {
			return activity.payload_fail(
				"isolated Activity heartbeat did not reach Core",
				failure_type = "OdinIsolatedActivityHeartbeatError",
			)
		}
	}
	message := fmt.aprintf("Recovered on Activity attempt %d", attempt)
	defer delete(message)
	payload, encode_error := codec.encode_json(message)
	if encode_error != .None {
		return activity.payload_fail(
			"could not encode isolated Activity result",
			failure_type = "OdinPayloadConversionError",
		)
	}
	return activity.payload_complete(payload)
}

isolated_hello_invoke :: proc(
	_: rawptr,
	ctx: ^activity.Context,
	inputs: []codec.Payload_View,
) -> activity.Payload_Result {
	return isolated_hello(ctx, inputs)
}

run_child :: proc() {
	marker := true
	options := activity_isolate.host_options(
		"hello",
		&marker,
		isolated_hello_invoke,
	)
	host_error := activity_isolate.host_run(options)
	if host_error.kind != .None {
		fmt.eprintf("Activity executor host failed: %v\n", host_error.kind)
		os.exit(1)
	}
}

run_parent :: proc() {
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

	queue_buffer: [192]u8
	task_queue := os.get_env(queue_buffer[:], "TEMPORAL_ODIN_TASK_QUEUE")
	if len(task_queue) == 0 {
		task_queue = "odin-activity-isolate"
	}
	command := [?]string{os.args[0], "--child"}
	manager := activity_isolate.Manager{}
	manager_options := activity_isolate.options_default(command[:])
	manager_options.execution_timeout = 10 * time.Second
	manager_options.cancellation_grace = 500 * time.Millisecond
	manager_error := activity_isolate.manager_init(&manager, manager_options)
	if manager_error.message != nil {
		fmt.eprintf("Activity executor manager failed: %s\n", string(manager_error.message))
		activity_isolate.manager_error_free(&manager_error)
		os.exit(1)
	}
	defer activity_isolate.manager_close(&manager)

	activity_worker, create_error := worker.create(&client, task_queue)
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		os.exit(1)
	}
	interceptor_error := worker.add_activity_interceptor(&activity_worker, {
		heartbeat = parent_heartbeat_interceptor,
	})
	if interceptor_error.message != nil {
		fmt.eprintf("Heartbeat interceptor registration failed: %s\n", string(interceptor_error.message))
		worker.error_free(&interceptor_error)
		_ = worker.close(&activity_worker)
		os.exit(1)
	}
	register_error := worker.register_external_activity_processor(
		&activity_worker,
		"hello",
		&manager,
		activity_isolate.process_task,
		activity_isolate.cancel_task,
	)
	if register_error.message != nil {
		fmt.eprintf("External Activity registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		_ = worker.close(&activity_worker)
		os.exit(1)
	}
	cancel_buffer: [32]u8
	cancel_mode := os.get_env(cancel_buffer[:], "ACTIVITY_ISOLATE_CANCEL")
	cancel_case := cancel_mode == "cooperative" || cancel_mode == "forced"
	forced_cancel_case := cancel_mode == "forced"
	fmt.printf("Odin external Activity parent is polling task queue %s\n", task_queue)
	max_tasks := cancel_case ? 0 : 2
	cancel_shutdown_state := Cancellation_Shutdown_State{
		manager = &manager,
		sdk_worker = &activity_worker,
	}
	cancel_shutdown_thread: ^thread.Thread
	if cancel_case {
		cancel_shutdown_thread = thread.create_and_start_with_poly_data(
			&cancel_shutdown_state,
			shutdown_after_cancellation,
		)
		if cancel_shutdown_thread == nil {
			fmt.eprintln("could not start isolated Activity cancellation shutdown coordinator")
			_ = worker.close(&activity_worker)
			os.exit(1)
		}
	}
	stats, run_error := worker.run(&activity_worker, {max_activity_tasks = max_tasks})
	if cancel_shutdown_thread != nil {
		thread.join(cancel_shutdown_thread)
		thread.destroy(cancel_shutdown_thread)
	}
	close_error := worker.close(&activity_worker)
	if run_error.message != nil || close_error.message != nil {
		if run_error.message != nil {
			fmt.eprintf("External Activity Worker failed: %s\n", string(run_error.message))
		}
		worker.error_free(&run_error)
		worker.error_free(&close_error)
		os.exit(1)
	}
	executor_stats := activity_isolate.manager_stats(&manager)
	if cancel_case {
		expected_completed := forced_cancel_case ? 0 : 1
		expected_failures := forced_cancel_case ? 1 : 0
		expected_kills := forced_cancel_case ? 1 : 0
		if stats.activity_tasks_completed != 1 || executor_stats.children_started != 1 ||
		   executor_stats.children_completed != expected_completed ||
		   executor_stats.child_failures != expected_failures ||
		   executor_stats.cancellations_forwarded != 1 ||
		   executor_stats.forced_cancellation_kills != expected_kills ||
		   activity_isolate.manager_in_flight(&manager) != 0 {
			fmt.eprintf(
				"unexpected cancellation stats: tasks=%d started=%d completed=%d failures=%d cancellations=%d kills=%d in_flight=%d\n",
				stats.activity_tasks_completed,
				executor_stats.children_started,
				executor_stats.children_completed,
				executor_stats.child_failures,
				executor_stats.cancellations_forwarded,
				executor_stats.forced_cancellation_kills,
				activity_isolate.manager_in_flight(&manager),
			)
			os.exit(1)
		}
		if forced_cancel_case {
			fmt.println("uncooperative isolated Activity cancellation reached the bounded forced-kill fallback")
		} else {
			fmt.println("isolated Activity cancellation was delivered cooperatively without a forced kill")
		}
		return
	}
	if stats.activity_tasks_completed != 2 || executor_stats.children_started != 2 ||
	   executor_stats.children_completed != 1 || executor_stats.child_failures != 1 ||
	   executor_stats.heartbeats_forwarded != 1 ||
	   sync.atomic_load_explicit(&parent_heartbeat_interceptor_calls, .Acquire) != 1 ||
	   activity_isolate.manager_in_flight(&manager) != 0 {
		fmt.eprintf(
			"unexpected isolation stats: tasks=%d started=%d completed=%d failures=%d heartbeats=%d intercepted=%d in_flight=%d\n",
			stats.activity_tasks_completed,
			executor_stats.children_started,
			executor_stats.children_completed,
			executor_stats.child_failures,
			executor_stats.heartbeats_forwarded,
			sync.atomic_load_explicit(&parent_heartbeat_interceptor_calls, .Acquire),
			activity_isolate.manager_in_flight(&manager),
		)
		os.exit(1)
	}
	fmt.println("isolated Activity panic was contained, heartbeat forwarded, and retry completed")
}

main :: proc() {
	if len(os.args) >= 2 && os.args[1] == "--child" {
		run_child()
		return
	}
	run_parent()
}

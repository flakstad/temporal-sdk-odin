package main

import "core:fmt"
import "core:os"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"
import temporal_client "../../temporal/client"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"
import workflow_isolate "../../temporal/workflow/isolate"

EXECUTOR_THREADS :: 4
WORKFLOW_TYPE :: "in_process_concurrency_probe"
UNKNOWN_WORKFLOW_TYPE :: "in_process_unknown_probe"
CANCELLATION_WORKFLOW_TYPE :: "in_process_cancellation_probe"
NONDETERMINISM_WORKFLOW_TYPE :: "in_process_nondeterminism_probe"
CANCELLATION_REASON :: "affine scheduler cancellation probe"

Probe_State :: struct {
	mutex:                 sync.Mutex,
	active:                int,
	peak:                  int,
	migrations:            int,
	valid_started:         int,
	timer_activations_completed: int,
	cancellation_started:  bool,
	cancellation_observed: bool,
	cancellation_reason_ok: bool,
	nondeterminism_replay: bool,
}

probe_state: Probe_State

// The wall-clock pause is deliberately test-only instrumentation. Production
// Workflow code must use workflow.sleep and must not perform blocking I/O or
// observe wall time. Here it makes simultaneous executor entry measurable.
concurrency_probe :: proc(ctx: ^workflow.Fiber_Context, value: string) -> string {
	owner_thread := sync.current_thread_id()
	sync.mutex_lock(&probe_state.mutex)
	probe_state.valid_started += 1
	sync.mutex_unlock(&probe_state.mutex)
	workflow.sleep(ctx, 2 * time.Second)
	if sync.current_thread_id() != owner_thread {
		sync.mutex_lock(&probe_state.mutex)
		probe_state.migrations += 1
		sync.mutex_unlock(&probe_state.mutex)
	}
	sync.mutex_lock(&probe_state.mutex)
	probe_state.active += 1
	probe_state.peak = max(probe_state.peak, probe_state.active)
	sync.mutex_unlock(&probe_state.mutex)
	time.sleep(400 * time.Millisecond)
	sync.mutex_lock(&probe_state.mutex)
	probe_state.active -= 1
	sync.mutex_unlock(&probe_state.mutex)
	return value
}

observe_workflow_activation :: proc(result: ^workflow.Process_Result) {
	if result == nil || result.activation_failed || result.evicted ||
	   result.is_replaying || !result.started_timer {
		return
	}
	sync.mutex_lock(&probe_state.mutex)
	probe_state.timer_activations_completed += 1
	sync.mutex_unlock(&probe_state.mutex)
}

cancellation_probe :: proc(ctx: ^workflow.Fiber_Context, value: string) -> string {
	owner_thread := sync.current_thread_id()
	sync.mutex_lock(&probe_state.mutex)
	probe_state.cancellation_started = true
	sync.mutex_unlock(&probe_state.mutex)
	workflow.sleep(ctx, time.Hour)
	sync.mutex_lock(&probe_state.mutex)
	if sync.current_thread_id() != owner_thread {
		probe_state.migrations += 1
	}
	probe_state.cancellation_observed = workflow.is_cancellation_requested(ctx)
	probe_state.cancellation_reason_ok =
		workflow.cancellation_reason(ctx) == CANCELLATION_REASON
	sync.mutex_unlock(&probe_state.mutex)
	return value
}

// This intentionally violates determinism for the live fault-containment
// proof. The initial execution records a Timer. After explicit eviction, the
// fresh replay fiber emits CompleteWorkflow instead, which Core must reject.
nondeterminism_probe :: proc(ctx: ^workflow.Fiber_Context, value: string) -> string {
	sync.mutex_lock(&probe_state.mutex)
	replay := probe_state.nondeterminism_replay
	sync.mutex_unlock(&probe_state.mutex)
	if replay {
		return value
	}
	workflow.sleep(ctx, 2 * time.Second)
	return value
}

Concurrency_State :: struct {
	client:                  ^temporal_client.Client,
	sdk_worker:              ^worker.Worker,
	task_queue:              string,
	id_prefix:               string,
	handles:                 [dynamic]temporal_client.Workflow_Handle,
	failure_handle:          temporal_client.Workflow_Handle,
	has_failure_handle:      bool,
	failure_terminated:      bool,
	cancellation_handle:     temporal_client.Workflow_Handle,
	has_cancellation_handle: bool,
	cancellation_closed:     bool,
	nondeterminism_handle:   temporal_client.Workflow_Handle,
	has_nondeterminism_handle: bool,
	nondeterminism_closed:   bool,
	watcher:                 ^thread.Thread,
	start_error:             temporal_client.Error,
	result_error:            temporal_client.Error,
	cancel_error:            temporal_client.Error,
	eviction_error:          worker.Error,
	results_ok:              bool,
	cancellation_ok:         bool,
	nondeterminism_ok:       bool,
	force_replay:            bool,
}

wait_for_results :: proc(state: ^Concurrency_State) {
	state.results_ok = true
	started := false
	valid_started := 0
	timer_activations_completed := 0
	expected_timer_activations := len(state.handles) + 1
	if state.force_replay {
		expected_timer_activations += 1
	}
	for _ in 0 ..< 500 {
		sync.mutex_lock(&probe_state.mutex)
		started = probe_state.cancellation_started
		valid_started = probe_state.valid_started
		timer_activations_completed = probe_state.timer_activations_completed
		sync.mutex_unlock(&probe_state.mutex)
		if started && valid_started >= len(state.handles) &&
		   timer_activations_completed >= expected_timer_activations {
			break
		}
		time.sleep(10 * time.Millisecond)
	}
	if !started || valid_started < len(state.handles) ||
	   timer_activations_completed < expected_timer_activations {
		state.results_ok = false
		worker.request_shutdown(state.sdk_worker)
		return
	}
	if state.force_replay {
		// The observer runs after Core has accepted each completion. Give Core's
		// internal Workflow stream a short window to install the sticky cache
		// entry before requesting eviction from this coordinating thread.
		time.sleep(200 * time.Millisecond)
		sync.mutex_lock(&probe_state.mutex)
		probe_state.nondeterminism_replay = true
		sync.mutex_unlock(&probe_state.mutex)
		for &handle in state.handles {
			state.eviction_error = worker.request_workflow_eviction(
				state.sdk_worker,
				handle.run_id,
			)
			if state.eviction_error.message != nil {
				state.results_ok = false
				worker.request_shutdown(state.sdk_worker)
				return
			}
		}
		state.eviction_error = worker.request_workflow_eviction(
			state.sdk_worker,
			state.nondeterminism_handle.run_id,
		)
		if state.eviction_error.message != nil {
			state.results_ok = false
			worker.request_shutdown(state.sdk_worker)
			return
		}
	}
	state.cancel_error = temporal_client.cancel_workflow(
		&state.cancellation_handle,
		CANCELLATION_REASON,
	)
	if state.cancel_error.message != nil {
		state.results_ok = false
		worker.request_shutdown(state.sdk_worker)
		return
	}
	canceled_value: string
	canceled_result_error := temporal_client.get_result_json(
		&state.cancellation_handle,
		&canceled_value,
	)
	delete(canceled_value)
	if canceled_result_error.kind == .Workflow_Canceled {
		state.cancellation_ok = true
		state.cancellation_closed = true
		temporal_client.error_free(&canceled_result_error)
	} else {
		state.result_error = canceled_result_error
		state.results_ok = false
		worker.request_shutdown(state.sdk_worker)
		return
	}
	if state.force_replay {
		nondeterminism_result: string
		nondeterminism_error := temporal_client.get_result_json(
			&state.nondeterminism_handle,
			&nondeterminism_result,
		)
		delete(nondeterminism_result)
		has_diagnostic :=
			strings.contains(string(nondeterminism_error.message), "[TMPRL1100]") ||
			strings.contains(string(nondeterminism_error.details), "[TMPRL1100]")
		if nondeterminism_error.kind == .Workflow_Failed && has_diagnostic {
			state.nondeterminism_ok = true
			state.nondeterminism_closed = true
			temporal_client.error_free(&nondeterminism_error)
		} else {
			state.result_error = nondeterminism_error
			state.results_ok = false
			worker.request_shutdown(state.sdk_worker)
			return
		}
	}
	for &handle in state.handles {
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		if result_error.message != nil {
			state.result_error = result_error
			state.results_ok = false
			break
		}
		if result != handle.id {
			state.results_ok = false
		}
		delete(result)
		if !state.results_ok {
			break
		}
	}
	worker.request_shutdown(state.sdk_worker)
}

start_concurrent_workflows :: proc(user_data: rawptr) -> bool {
	state := cast(^Concurrency_State)user_data
	if state == nil || state.client == nil || state.sdk_worker == nil {
		return false
	}
	failure_id := fmt.aprintf("%s-invalid", state.id_prefix)
	failure_handle, failure_start_error := temporal_client.start_workflow_json(
		state.client,
		UNKNOWN_WORKFLOW_TYPE,
		"unknown Workflow type failure probe",
		{
			id = failure_id,
			task_queue = state.task_queue,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	delete(failure_id)
	if failure_start_error.message != nil {
		state.start_error = failure_start_error
		return false
	}
	state.failure_handle = failure_handle
	state.has_failure_handle = true
	cancellation_id := fmt.aprintf("%s-cancel", state.id_prefix)
	cancellation_handle, cancellation_start_error := temporal_client.start_workflow_json(
		state.client,
		CANCELLATION_WORKFLOW_TYPE,
		"cancel me on my owning shard",
		{
			id = cancellation_id,
			task_queue = state.task_queue,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	delete(cancellation_id)
	if cancellation_start_error.message != nil {
		state.start_error = cancellation_start_error
		return false
	}
	state.cancellation_handle = cancellation_handle
	state.has_cancellation_handle = true
	first_shard := -1
	found_distinct_shard := false
	for candidate in 0 ..< 64 {
		workflow_id := fmt.aprintf("%s-%d", state.id_prefix, candidate)
		handle, start_error := temporal_client.start_workflow_json(
			state.client,
			WORKFLOW_TYPE,
			workflow_id,
			{
				id = workflow_id,
				task_queue = state.task_queue,
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
		)
		delete(workflow_id)
		if start_error.message != nil {
			state.start_error = start_error
			return false
		}
		append(&state.handles, handle)
		shard, shard_ok := workflow_isolate.workflow_shard_index(
			handle.run_id,
			EXECUTOR_THREADS,
		)
		if !shard_ok {
			return false
		}
		if first_shard < 0 {
			first_shard = shard
		} else if shard != first_shard {
			found_distinct_shard = true
			break
		}
	}
	if !found_distinct_shard {
		return false
	}
	if state.force_replay {
		nondeterminism_id := fmt.aprintf("%s-nondeterminism", state.id_prefix)
		nondeterminism_handle, nondeterminism_start_error := temporal_client.start_workflow_json(
			state.client,
			NONDETERMINISM_WORKFLOW_TYPE,
			"intentionally incompatible replay",
			{
				id = nondeterminism_id,
				task_queue = state.task_queue,
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
		)
		delete(nondeterminism_id)
		if nondeterminism_start_error.message != nil {
			state.start_error = nondeterminism_start_error
			return false
		}
		state.nondeterminism_handle = nondeterminism_handle
		state.has_nondeterminism_handle = true
	}
	state.watcher = thread.create_and_start_with_poly_data(state, wait_for_results)
	return state.watcher != nil
}

run :: proc() -> int {
	replay_buffer: [64]u8
	force_replay := os.get_env(
		replay_buffer[:],
		"TEMPORAL_ODIN_FORCE_REPLAY",
	) == "1"
	task_queue_buffer: [256]u8
	task_queue := os.get_env(task_queue_buffer[:], "TEMPORAL_ODIN_TASK_QUEUE")
	if len(task_queue) == 0 {
		task_queue = "odin-workflow-in-process-concurrency"
	}
	id_buffer: [256]u8
	id_prefix := os.get_env(id_buffer[:], "TEMPORAL_ODIN_WORKFLOW_ID")
	owned_id_prefix := ""
	if len(id_prefix) == 0 {
		owned_id_prefix = fmt.aprintf("odin-in-process-concurrent-%d", time.now()._nsec)
		defer delete(owned_id_prefix)
		id_prefix = owned_id_prefix
	}

	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		fmt.eprintf("In-process concurrency Client connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	options := worker.options_default(task_queue)
	options.max_concurrent_workflow_tasks = EXECUTOR_THREADS
	options.max_workflow_task_slots = EXECUTOR_THREADS
	options.max_cached_workflows = 16
	options.nondeterminism_policy = {
		fail_workflow_types = []string{NONDETERMINISM_WORKFLOW_TYPE},
	}
	options.workflow_task_poller_behavior = worker.poller_simple_maximum(EXECUTOR_THREADS)
	sdk_worker, create_error := worker.create_with_options(
		temporal_client.core_client(&client),
		options,
	)
	if create_error.message != nil {
		fmt.eprintf("In-process concurrency Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		return 1
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}
	register_error := worker.register_json_workflow(
		&sdk_worker,
		WORKFLOW_TYPE,
		concurrency_probe,
	)
	if register_error.message != nil {
		fmt.eprintf("In-process concurrency registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}
	register_error = worker.register_json_workflow(
		&sdk_worker,
		CANCELLATION_WORKFLOW_TYPE,
		cancellation_probe,
	)
	if register_error.message != nil {
		fmt.eprintf("Cancellation Workflow registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}
	register_error = worker.register_json_workflow(
		&sdk_worker,
		NONDETERMINISM_WORKFLOW_TYPE,
		nondeterminism_probe,
	)
	if register_error.message != nil {
		fmt.eprintf("Nondeterminism Workflow registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}

	state := Concurrency_State{
		client = &client,
		sdk_worker = &sdk_worker,
		task_queue = task_queue,
		id_prefix = id_prefix,
		force_replay = force_replay,
	}
	run_stats, run_error := worker.run(&sdk_worker, {
		on_ready = start_concurrent_workflows,
		on_ready_data = &state,
		on_workflow_activation = observe_workflow_activation,
	})
	if state.watcher != nil {
		thread.join(state.watcher)
		thread.destroy(state.watcher)
	}
	defer {
		for &handle in state.handles {
			temporal_client.workflow_handle_free(&handle)
		}
		delete(state.handles)
		if state.has_failure_handle {
			if !state.failure_terminated {
				cleanup_error := temporal_client.terminate_workflow(
					&state.failure_handle,
					"in-process concurrency cleanup",
				)
				temporal_client.error_free(&cleanup_error)
			}
			temporal_client.workflow_handle_free(&state.failure_handle)
		}
		if state.has_cancellation_handle {
			if !state.cancellation_closed {
				cleanup_error := temporal_client.terminate_workflow(
					&state.cancellation_handle,
					"in-process concurrency cleanup",
				)
				temporal_client.error_free(&cleanup_error)
			}
			temporal_client.workflow_handle_free(&state.cancellation_handle)
		}
		if state.has_nondeterminism_handle {
			if !state.nondeterminism_closed {
				cleanup_error := temporal_client.terminate_workflow(
					&state.nondeterminism_handle,
					"in-process nondeterminism cleanup",
				)
				temporal_client.error_free(&cleanup_error)
			}
			temporal_client.workflow_handle_free(&state.nondeterminism_handle)
		}
	}
	if state.start_error.message != nil {
		fmt.eprintf("In-process concurrency start failed: %s\n", string(state.start_error.message))
		temporal_client.error_free(&state.start_error)
		worker.error_free(&run_error)
		return 1
	}
	if state.result_error.message != nil {
		fmt.eprintf("In-process concurrency result failed: %s\n", string(state.result_error.message))
		temporal_client.error_free(&state.result_error)
		worker.error_free(&run_error)
		return 1
	}
	if state.cancel_error.message != nil {
		fmt.eprintf("In-process cancellation request failed: %s\n", string(state.cancel_error.message))
		temporal_client.error_free(&state.cancel_error)
		worker.error_free(&run_error)
		return 1
	}
	if state.eviction_error.message != nil {
		fmt.eprintf("In-process Workflow eviction request failed: %s\n", string(state.eviction_error.message))
		worker.error_free(&state.eviction_error)
		worker.error_free(&run_error)
		return 1
	}
	if run_error.message != nil {
		fmt.eprintf("In-process concurrency Worker failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		return 1
	}
	if !state.has_failure_handle {
		fmt.eprintln("In-process concurrency failure probe was not started")
		return 1
	}
	terminate_error := temporal_client.terminate_workflow(
		&state.failure_handle,
		"in-process concurrency failure probe completed",
	)
	if terminate_error.message != nil {
		fmt.eprintf("Unknown Workflow termination failed: %s\n", string(terminate_error.message))
		temporal_client.error_free(&terminate_error)
		return 1
	}
	state.failure_terminated = true
	sync.mutex_lock(&probe_state.mutex)
	peak := probe_state.peak
	migrations := probe_state.migrations
	cancellation_observed := probe_state.cancellation_observed
	cancellation_reason_ok := probe_state.cancellation_reason_ok
	sync.mutex_unlock(&probe_state.mutex)
	if !state.results_ok || len(state.handles) < 2 ||
	   !state.cancellation_ok || !cancellation_observed || !cancellation_reason_ok ||
	   (force_replay && !state.nondeterminism_ok) ||
	   run_stats.workflow_executions_completed != len(state.handles) + 1 + int(force_replay) ||
	   run_stats.workflow_activation_failures < 1 ||
	   run_stats.workflow_evictions < len(state.handles) + 2 ||
	   run_stats.workflow_cancellations_received != 1 || run_stats.timers_canceled != 1 ||
	   (force_replay && run_stats.workflow_replay_activations < 1) ||
	   run_stats.workflow_activations_peak_owned < 2 ||
	   run_stats.workflow_executor_queue_peak < 1 || peak < 2 || migrations != 0 {
		fmt.eprintf(
			"In-process concurrency proof failed handles=%d completed=%d failures=%d evictions=%d cancellations=%d canceled_timers=%d replay_activations=%d owned_peak=%d queue_peak=%d callback_peak=%d migrations=%d cancellation_ok=%v reason_ok=%v nondeterminism_ok=%v\n",
			len(state.handles),
			run_stats.workflow_executions_completed,
			run_stats.workflow_activation_failures,
			run_stats.workflow_evictions,
			run_stats.workflow_cancellations_received,
			run_stats.timers_canceled,
			run_stats.workflow_replay_activations,
			run_stats.workflow_activations_peak_owned,
			run_stats.workflow_executor_queue_peak,
			peak,
			migrations,
			state.cancellation_ok && cancellation_observed,
			cancellation_reason_ok,
			state.nondeterminism_ok,
		)
		return 1
	}
	fmt.printf(
		"In-process Workflow concurrency proved handles=%d executor_threads=%d activation_failures=%d evictions=%d cancellations=%d canceled_timers=%d replay_activations=%d owned_peak=%d queue_peak=%d callback_peak=%d migrations=%d nondeterminism=%v\n",
		len(state.handles),
		EXECUTOR_THREADS,
		run_stats.workflow_activation_failures,
		run_stats.workflow_evictions,
		run_stats.workflow_cancellations_received,
		run_stats.timers_canceled,
		run_stats.workflow_replay_activations,
		run_stats.workflow_activations_peak_owned,
		run_stats.workflow_executor_queue_peak,
		peak,
		migrations,
		state.nondeterminism_ok,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

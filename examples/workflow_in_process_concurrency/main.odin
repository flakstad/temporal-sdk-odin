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
SIGNAL_HANDLER_WORKFLOW_TYPE :: "in_process_signal_handler_probe"
ASYNC_HANDLER_WORKFLOW_TYPE :: "in_process_async_handler_probe"
HANDLER_LOAD_WORKFLOW_TYPE :: "in_process_handler_load_probe"
SHUTDOWN_WORKFLOW_TYPE :: "in_process_shutdown_probe"
CANCELLATION_REASON :: "affine scheduler cancellation probe"
HANDLER_LOAD_ROUTINES :: 4
HANDLER_LOAD_RUNS_PER_SHARD :: 2

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
	shutdown_shards_entered: [EXECUTOR_THREADS]bool,
	shutdown_shard_count:    int,
	handler_load_started:    int,
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

shutdown_probe :: proc(ctx: ^workflow.Fiber_Context, value: string) -> string {
	owner_thread := sync.current_thread_id()
	info, info_present := workflow.info(ctx)
	shard := -1
	if info_present {
		shard, _ = workflow_isolate.workflow_shard_index(info.run_id, EXECUTOR_THREADS)
	}
	sync.mutex_lock(&probe_state.mutex)
	probe_state.valid_started += 1
	if shard >= 0 && !probe_state.shutdown_shards_entered[shard] {
		probe_state.shutdown_shards_entered[shard] = true
		probe_state.shutdown_shard_count += 1
	}
	sync.mutex_unlock(&probe_state.mutex)
	// Deliberate test-only blocking keeps one activation executing on every
	// shard while the coordinator requests Worker shutdown.
	time.sleep(2 * time.Second)
	workflow.sleep(ctx, time.Hour)
	if sync.current_thread_id() != owner_thread {
		sync.mutex_lock(&probe_state.mutex)
		probe_state.migrations += 1
		sync.mutex_unlock(&probe_state.mutex)
	}
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

Signal_Handler_State :: struct {
	owner_thread: int,
	value:        int,
}

signal_handler_apply :: proc(state: ^Signal_Handler_State, value: int) {
	if sync.current_thread_id() != state.owner_thread {
		sync.mutex_lock(&probe_state.mutex)
		probe_state.migrations += 1
		sync.mutex_unlock(&probe_state.mutex)
	}
	state.value += value
}

signal_handler_ready :: proc(state: ^Signal_Handler_State) -> bool {
	return state.value == 3
}

signal_handler_probe :: proc(ctx: ^workflow.Fiber_Context, _: string) -> string {
	state := Signal_Handler_State{owner_thread = sync.current_thread_id()}
	if !workflow.set_signal_handler_json(
		ctx,
		"accumulate",
		&state,
		signal_handler_apply,
	) {
		return "signal handler registration failed"
	}
	if !workflow.await_condition(ctx, &state, signal_handler_ready) {
		return "signal handler wait canceled"
	}
	return fmt.aprintf("signals:%d", state.value)
}

Async_Handler_State :: struct {
	owner_thread:  int,
	active:        int,
	peak:          int,
	signal_value:  int,
	update_value:  int,
	signal_done:   bool,
	update_done:   bool,
}

async_handler_enter :: proc(state: ^Async_Handler_State) {
	if sync.current_thread_id() != state.owner_thread {
		sync.mutex_lock(&probe_state.mutex)
		probe_state.migrations += 1
		sync.mutex_unlock(&probe_state.mutex)
	}
	state.active += 1
	state.peak = max(state.peak, state.active)
}

async_handler_leave :: proc(state: ^Async_Handler_State) {
	if sync.current_thread_id() != state.owner_thread {
		sync.mutex_lock(&probe_state.mutex)
		probe_state.migrations += 1
		sync.mutex_unlock(&probe_state.mutex)
	}
	state.active -= 1
}

async_signal_handler :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Async_Handler_State,
	value: int,
) {
	async_handler_enter(state)
	workflow.sleep(ctx, 2 * time.Second)
	state.signal_value = value
	state.signal_done = true
	async_handler_leave(state)
}

async_update_handler :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Async_Handler_State,
	value: int,
) -> int {
	async_handler_enter(state)
	workflow.sleep(ctx, 2 * time.Second)
	state.update_value = value
	state.update_done = true
	async_handler_leave(state)
	return value * 2
}

async_handlers_ready :: proc(state: ^Async_Handler_State) -> bool {
	return state.signal_done && state.update_done
}

async_handler_probe :: proc(ctx: ^workflow.Fiber_Context, _: string) -> string {
	state := Async_Handler_State{owner_thread = sync.current_thread_id()}
	if !workflow.set_signal_routine_json(
		ctx,
		"async_signal",
		&state,
		async_signal_handler,
	) || !workflow.set_update_routine_json(
		ctx,
		"async_update",
		&state,
		async_update_handler,
	) {
		return "asynchronous handler registration failed"
	}
	if !workflow.await_condition(ctx, &state, async_handlers_ready) {
		return "asynchronous handler wait canceled"
	}
	workflow.await_all_handlers_finished(ctx)
	return fmt.aprintf(
		"async:%d:%d",
		state.signal_value + state.update_value,
		state.peak,
	)
}

Handler_Load_State :: struct {
	owner_thread: int,
	active:       int,
	peak:         int,
	completed:    int,
	sum:          int,
}

handler_load_routine :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Handler_Load_State,
	value: int,
) {
	if sync.current_thread_id() != state.owner_thread {
		sync.mutex_lock(&probe_state.mutex)
		probe_state.migrations += 1
		sync.mutex_unlock(&probe_state.mutex)
	}
	state.active += 1
	state.peak = max(state.peak, state.active)
	sync.mutex_lock(&probe_state.mutex)
	probe_state.handler_load_started += 1
	sync.mutex_unlock(&probe_state.mutex)
	workflow.sleep(ctx, 5 * time.Second)
	if sync.current_thread_id() != state.owner_thread {
		sync.mutex_lock(&probe_state.mutex)
		probe_state.migrations += 1
		sync.mutex_unlock(&probe_state.mutex)
	}
	state.sum += value
	state.completed += 1
	state.active -= 1
}

handler_load_ready :: proc(state: ^Handler_Load_State) -> bool {
	return state.completed == HANDLER_LOAD_ROUTINES
}

handler_load_probe :: proc(ctx: ^workflow.Fiber_Context, _: string) -> string {
	state := Handler_Load_State{owner_thread = sync.current_thread_id()}
	if !workflow.set_signal_routine_json(
		ctx,
		"load",
		&state,
		handler_load_routine,
	) {
		return "handler load registration failed"
	}
	if !workflow.await_condition(ctx, &state, handler_load_ready) {
		return "handler load wait canceled"
	}
	workflow.await_all_handlers_finished(ctx)
	return fmt.aprintf("load:%d:%d", state.sum, state.peak)
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
	signal_handler_handle:  temporal_client.Workflow_Handle,
	has_signal_handler_handle: bool,
	signal_handler_closed:  bool,
	async_handler_handle:   temporal_client.Workflow_Handle,
	has_async_handler_handle: bool,
	async_handler_closed:   bool,
	watcher:                 ^thread.Thread,
	start_error:             temporal_client.Error,
	result_error:            temporal_client.Error,
	cancel_error:            temporal_client.Error,
	signal_error:            temporal_client.Error,
	async_handler_error:     temporal_client.Error,
	eviction_error:          worker.Error,
	results_ok:              bool,
	cancellation_ok:         bool,
	nondeterminism_ok:       bool,
	signal_handler_ok:      bool,
	async_handler_ok:       bool,
	force_replay:            bool,
	force_shutdown:          bool,
	force_handler_load:      bool,
}

wait_for_handler_load_results :: proc(state: ^Concurrency_State) {
	state.results_ok = true
	expected_handlers := len(state.handles) * HANDLER_LOAD_ROUTINES
	for &handle in state.handles {
		for value in 1 ..= HANDLER_LOAD_ROUTINES {
			state.signal_error = temporal_client.signal_workflow_json(
				&handle,
				"load",
				value,
			)
			if state.signal_error.message != nil {
				state.results_ok = false
				worker.request_shutdown(state.sdk_worker)
				return
			}
		}
	}
	started := 0
	for _ in 0 ..< 1000 {
		sync.mutex_lock(&probe_state.mutex)
		started = probe_state.handler_load_started
		sync.mutex_unlock(&probe_state.mutex)
		if started >= expected_handlers {
			break
		}
		time.sleep(10 * time.Millisecond)
	}
	if started < expected_handlers {
		state.results_ok = false
		worker.request_shutdown(state.sdk_worker)
		return
	}
	// Every routine has emitted its durable timer. Let Core install the sticky
	// completions, then discard every cached stack while all handlers are live.
	time.sleep(200 * time.Millisecond)
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
	for &handle in state.handles {
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		if result_error.message != nil {
			state.result_error = result_error
			state.results_ok = false
			delete(result)
			break
		}
		if result != "load:10:4" {
			state.results_ok = false
		}
		delete(result)
		if !state.results_ok {
			break
		}
	}
	worker.request_shutdown(state.sdk_worker)
}

start_handler_load_workflows :: proc(state: ^Concurrency_State) -> bool {
	if state == nil || state.client == nil || state.sdk_worker == nil {
		return false
	}
	runs_per_shard: [EXECUTOR_THREADS]int
	covered_shards := 0
	for candidate in 0 ..< 128 {
		workflow_id := fmt.aprintf("%s-load-%d", state.id_prefix, candidate)
		handle, start_error := temporal_client.start_workflow_json(
			state.client,
			HANDLER_LOAD_WORKFLOW_TYPE,
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
		runs_per_shard[shard] += 1
		if runs_per_shard[shard] == HANDLER_LOAD_RUNS_PER_SHARD {
			covered_shards += 1
		}
		if covered_shards == EXECUTOR_THREADS {
			break
		}
	}
	if covered_shards != EXECUTOR_THREADS {
		return false
	}
	state.watcher = thread.create_and_start_with_poly_data(
		state,
		wait_for_handler_load_results,
	)
	return state.watcher != nil
}

wait_for_results :: proc(state: ^Concurrency_State) {
	state.results_ok = true
	started := false
	valid_started := 0
	timer_activations_completed := 0
	shutdown_shard_count := 0
	expected_timer_activations := len(state.handles)
	if !state.force_shutdown {
		expected_timer_activations += 1
	}
	if state.force_replay && !state.force_shutdown {
		expected_timer_activations += 1
	}
	wait_iterations := state.force_shutdown ? 3000 : 500
	for _ in 0 ..< wait_iterations {
		sync.mutex_lock(&probe_state.mutex)
		started = probe_state.cancellation_started
		valid_started = probe_state.valid_started
		timer_activations_completed = probe_state.timer_activations_completed
		shutdown_shard_count = probe_state.shutdown_shard_count
		sync.mutex_unlock(&probe_state.mutex)
		if state.force_shutdown && shutdown_shard_count == EXECUTOR_THREADS {
			break
		}
		if !state.force_shutdown && started && valid_started >= len(state.handles) &&
		   timer_activations_completed >= expected_timer_activations {
			break
		}
		time.sleep(10 * time.Millisecond)
	}
	if (state.force_shutdown && shutdown_shard_count != EXECUTOR_THREADS) ||
	   (!state.force_shutdown && (!started || valid_started < len(state.handles) ||
	   timer_activations_completed < expected_timer_activations)) {
		state.results_ok = false
		worker.request_shutdown(state.sdk_worker)
		return
	}
	if state.force_shutdown {
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
	async_update, async_update_start_error := temporal_client.start_update_json(
		&state.async_handler_handle,
		"async_update",
		7,
	)
	if async_update_start_error.message != nil {
		state.async_handler_error = async_update_start_error
		state.results_ok = false
		worker.request_shutdown(state.sdk_worker)
		return
	}
	async_signal_error := temporal_client.signal_workflow_json(
		&state.async_handler_handle,
		"async_signal",
		5,
	)
	if async_signal_error.message != nil {
		state.async_handler_error = async_signal_error
		state.results_ok = false
		temporal_client.update_handle_free(&async_update)
		worker.request_shutdown(state.sdk_worker)
		return
	}
	async_update_result: int
	async_update_error := temporal_client.wait_update_json(
		&async_update,
		&async_update_result,
	)
	temporal_client.update_handle_free(&async_update)
	if async_update_error.message != nil {
		state.async_handler_error = async_update_error
		state.results_ok = false
		worker.request_shutdown(state.sdk_worker)
		return
	}
	async_workflow_result: string
	async_workflow_error := temporal_client.get_result_json(
		&state.async_handler_handle,
		&async_workflow_result,
	)
	if async_workflow_error.message != nil {
		state.async_handler_error = async_workflow_error
		state.results_ok = false
		delete(async_workflow_result)
		worker.request_shutdown(state.sdk_worker)
		return
	}
	state.async_handler_ok =
		async_update_result == 14 && async_workflow_result == "async:12:2"
	state.async_handler_closed = true
	delete(async_workflow_result)
	if !state.async_handler_ok {
		state.results_ok = false
		worker.request_shutdown(state.sdk_worker)
		return
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
	signal_result: string
	signal_result_error := temporal_client.get_result_json(
		&state.signal_handler_handle,
		&signal_result,
	)
	if signal_result_error.message == nil && signal_result == "signals:3" {
		state.signal_handler_ok = true
		state.signal_handler_closed = true
	} else if signal_result_error.message != nil {
		state.result_error = signal_result_error
		state.results_ok = false
	}
	delete(signal_result)
	if !state.signal_handler_ok {
		worker.request_shutdown(state.sdk_worker)
		return
	}
	worker.request_shutdown(state.sdk_worker)
}

start_shutdown_workflows :: proc(state: ^Concurrency_State) -> bool {
	if state == nil || state.client == nil || state.sdk_worker == nil {
		return false
	}
	seen_shards: [EXECUTOR_THREADS]bool
	seen_count := 0
	for candidate in 0 ..< 128 {
		workflow_id := fmt.aprintf("%s-shutdown-%d", state.id_prefix, candidate)
		handle, start_error := temporal_client.start_workflow_json(
			state.client,
			SHUTDOWN_WORKFLOW_TYPE,
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
		if !seen_shards[shard] {
			seen_shards[shard] = true
			seen_count += 1
		}
		if seen_count == EXECUTOR_THREADS {
			break
		}
	}
	if seen_count != EXECUTOR_THREADS {
		return false
	}
	state.watcher = thread.create_and_start_with_poly_data(state, wait_for_results)
	return state.watcher != nil
}

start_concurrent_workflows :: proc(user_data: rawptr) -> bool {
	state := cast(^Concurrency_State)user_data
	if state == nil || state.client == nil || state.sdk_worker == nil {
		return false
	}
	if state.force_shutdown {
		return start_shutdown_workflows(state)
	}
	if state.force_handler_load {
		return start_handler_load_workflows(state)
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
	signal_handler_id := fmt.aprintf("%s-signal-handler", state.id_prefix)
	signal_handler_handle, signal_handler_start_error := temporal_client.start_workflow_json(
		state.client,
		SIGNAL_HANDLER_WORKFLOW_TYPE,
		"affine signal handler",
		{
			id = signal_handler_id,
			task_queue = state.task_queue,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	delete(signal_handler_id)
	if signal_handler_start_error.message != nil {
		state.start_error = signal_handler_start_error
		return false
	}
	state.signal_handler_handle = signal_handler_handle
	state.has_signal_handler_handle = true
	state.signal_error = temporal_client.signal_workflow_json(
		&state.signal_handler_handle,
		"accumulate",
		1,
	)
	if state.signal_error.message != nil {
		return false
	}
	state.signal_error = temporal_client.signal_workflow_json(
		&state.signal_handler_handle,
		"accumulate",
		2,
	)
	if state.signal_error.message != nil {
		return false
	}
	async_handler_id := fmt.aprintf("%s-async-handler", state.id_prefix)
	async_handler_handle, async_handler_start_error := temporal_client.start_workflow_json(
		state.client,
		ASYNC_HANDLER_WORKFLOW_TYPE,
		"affine asynchronous handlers",
		{
			id = async_handler_id,
			task_queue = state.task_queue,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	delete(async_handler_id)
	if async_handler_start_error.message != nil {
		state.start_error = async_handler_start_error
		return false
	}
	state.async_handler_handle = async_handler_handle
	state.has_async_handler_handle = true
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
	shutdown_buffer: [64]u8
	force_shutdown := os.get_env(
		shutdown_buffer[:],
		"TEMPORAL_ODIN_FORCE_SHUTDOWN",
	) == "1"
	handler_load_buffer: [64]u8
	force_handler_load := os.get_env(
		handler_load_buffer[:],
		"TEMPORAL_ODIN_HANDLER_LOAD",
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
	stress_mode := force_shutdown || force_handler_load
	options.max_workflow_task_slots = stress_mode ? 128 : EXECUTOR_THREADS
	options.max_cached_workflows = stress_mode ? 128 : 16
	options.nondeterminism_policy = {
		fail_workflow_types = []string{NONDETERMINISM_WORKFLOW_TYPE},
	}
	options.workflow_task_poller_behavior = worker.poller_simple_maximum(
		stress_mode ? 16 : EXECUTOR_THREADS,
	)
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
		SHUTDOWN_WORKFLOW_TYPE,
		shutdown_probe,
	)
	if register_error.message != nil {
		fmt.eprintf("Shutdown Workflow registration failed: %s\n", string(register_error.message))
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
		SIGNAL_HANDLER_WORKFLOW_TYPE,
		signal_handler_probe,
	)
	if register_error.message != nil {
		fmt.eprintf("Signal-handler Workflow registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}
	register_error = worker.register_json_workflow(
		&sdk_worker,
		ASYNC_HANDLER_WORKFLOW_TYPE,
		async_handler_probe,
	)
	if register_error.message != nil {
		fmt.eprintf("Async-handler Workflow registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}
	register_error = worker.register_json_workflow(
		&sdk_worker,
		HANDLER_LOAD_WORKFLOW_TYPE,
		handler_load_probe,
	)
	if register_error.message != nil {
		fmt.eprintf("Handler-load Workflow registration failed: %s\n", string(register_error.message))
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
		force_shutdown = force_shutdown,
		force_handler_load = force_handler_load,
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
			if state.force_shutdown || (state.force_handler_load && !state.results_ok) {
				cleanup_error := temporal_client.terminate_workflow(
					&handle,
					"in-process affine shutdown proof completed",
				)
				temporal_client.error_free(&cleanup_error)
			}
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
		if state.has_signal_handler_handle {
			if !state.signal_handler_closed {
				cleanup_error := temporal_client.terminate_workflow(
					&state.signal_handler_handle,
					"in-process signal-handler cleanup",
				)
				temporal_client.error_free(&cleanup_error)
			}
			temporal_client.workflow_handle_free(&state.signal_handler_handle)
		}
		if state.has_async_handler_handle {
			if !state.async_handler_closed {
				cleanup_error := temporal_client.terminate_workflow(
					&state.async_handler_handle,
					"in-process async-handler cleanup",
				)
				temporal_client.error_free(&cleanup_error)
			}
			temporal_client.workflow_handle_free(&state.async_handler_handle)
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
	if state.signal_error.message != nil {
		fmt.eprintf("In-process signal delivery failed: %s\n", string(state.signal_error.message))
		temporal_client.error_free(&state.signal_error)
		worker.error_free(&run_error)
		return 1
	}
	if state.async_handler_error.message != nil {
		fmt.eprintf("In-process asynchronous handler failed: %s\n", string(state.async_handler_error.message))
		temporal_client.error_free(&state.async_handler_error)
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
	if force_shutdown {
		sync.mutex_lock(&probe_state.mutex)
		shutdown_migrations := probe_state.migrations
		shutdown_shards_entered := probe_state.shutdown_shard_count
		sync.mutex_unlock(&probe_state.mutex)
		if !state.results_ok || len(state.handles) < EXECUTOR_THREADS ||
		   shutdown_shards_entered != EXECUTOR_THREADS ||
		   run_stats.workflow_activations_completed !=
		   len(state.handles) + run_stats.workflow_evictions ||
		   run_stats.workflow_executions_completed != 0 ||
		   run_stats.workflow_activation_failures != 0 ||
		   run_stats.workflow_cached_fibers_shutdown < 1 ||
		   run_stats.workflow_cached_fibers_shutdown + run_stats.workflow_evictions !=
		   len(state.handles) ||
		   run_stats.workflow_executor_shards_used != EXECUTOR_THREADS ||
		   run_stats.workflow_activations_peak_owned < 1 ||
		   run_stats.workflow_executor_queue_peak < 1 || shutdown_migrations != 0 {
			fmt.eprintf(
				"In-process Workflow shutdown proof failed handles=%d mid_activation_shards=%d stats_shards=%d activations=%d completed=%d failures=%d evictions=%d cached_shutdown=%d owned_peak=%d queue_peak=%d migrations=%d\n",
				len(state.handles),
				shutdown_shards_entered,
				run_stats.workflow_executor_shards_used,
				run_stats.workflow_activations_completed,
				run_stats.workflow_executions_completed,
				run_stats.workflow_activation_failures,
				run_stats.workflow_evictions,
				run_stats.workflow_cached_fibers_shutdown,
				run_stats.workflow_activations_peak_owned,
				run_stats.workflow_executor_queue_peak,
				shutdown_migrations,
			)
			return 1
		}
		fmt.printf(
			"In-process Workflow shutdown proved handles=%d shards=%d mid_activation_shards=%d stats_shards=%d activations=%d evictions=%d cached_shutdown=%d owned_peak=%d queue_peak=%d migrations=%d\n",
			len(state.handles),
			EXECUTOR_THREADS,
			shutdown_shards_entered,
			run_stats.workflow_executor_shards_used,
			run_stats.workflow_activations_completed,
			run_stats.workflow_evictions,
			run_stats.workflow_cached_fibers_shutdown,
			run_stats.workflow_activations_peak_owned,
			run_stats.workflow_executor_queue_peak,
			shutdown_migrations,
		)
		return 0
	}
	if force_handler_load {
		sync.mutex_lock(&probe_state.mutex)
		migrations := probe_state.migrations
		handler_load_started := probe_state.handler_load_started
		sync.mutex_unlock(&probe_state.mutex)
		expected_handlers := len(state.handles) * HANDLER_LOAD_ROUTINES
		if !state.results_ok || len(state.handles) < EXECUTOR_THREADS * HANDLER_LOAD_RUNS_PER_SHARD ||
		   handler_load_started < expected_handlers ||
		   run_stats.workflow_executions_completed != len(state.handles) ||
		   run_stats.workflow_activation_failures != 0 ||
		   run_stats.workflow_signals_received != expected_handlers ||
		   run_stats.workflow_signal_handlers_started != expected_handlers ||
		   run_stats.workflow_signal_handlers_completed != expected_handlers ||
		   run_stats.workflow_signal_handlers_abandoned != 0 ||
		   run_stats.workflow_signal_handler_warnings != 0 ||
		   run_stats.workflow_replay_activations < len(state.handles) ||
		   run_stats.workflow_evictions < len(state.handles) ||
		   run_stats.workflow_executor_shards_used != EXECUTOR_THREADS ||
		   run_stats.workflow_executor_shard_activations_min < 1 ||
		   run_stats.workflow_executor_shard_activations_max <
			   run_stats.workflow_executor_shard_activations_min ||
		   run_stats.workflow_activations_peak_owned < EXECUTOR_THREADS ||
		   run_stats.workflow_executor_queue_peak < 1 || migrations != 0 {
			fmt.eprintf(
				"In-process Workflow handler load failed handles=%d shards=%d signals=%d handlers_started=%d handlers_completed=%d replay_activations=%d evictions=%d shard_activations_min=%d shard_activations_max=%d owned_peak=%d queue_peak=%d migrations=%d results_ok=%v\n",
				len(state.handles),
				run_stats.workflow_executor_shards_used,
				run_stats.workflow_signals_received,
				run_stats.workflow_signal_handlers_started,
				run_stats.workflow_signal_handlers_completed,
				run_stats.workflow_replay_activations,
				run_stats.workflow_evictions,
				run_stats.workflow_executor_shard_activations_min,
				run_stats.workflow_executor_shard_activations_max,
				run_stats.workflow_activations_peak_owned,
				run_stats.workflow_executor_queue_peak,
				migrations,
				state.results_ok,
			)
			return 1
		}
		fmt.printf(
			"In-process Workflow handler load proved handles=%d shards=%d signals=%d handlers=%d replay_activations=%d evictions=%d shard_activations_min=%d shard_activations_max=%d owned_peak=%d queue_peak=%d migrations=%d\n",
			len(state.handles),
			EXECUTOR_THREADS,
			run_stats.workflow_signals_received,
			run_stats.workflow_signal_handlers_completed,
			run_stats.workflow_replay_activations,
			run_stats.workflow_evictions,
			run_stats.workflow_executor_shard_activations_min,
			run_stats.workflow_executor_shard_activations_max,
			run_stats.workflow_activations_peak_owned,
			run_stats.workflow_executor_queue_peak,
			migrations,
		)
		return 0
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
	   !state.signal_handler_ok || !state.async_handler_ok ||
	   (force_replay && !state.nondeterminism_ok) ||
	   run_stats.workflow_executions_completed != len(state.handles) + 3 + int(force_replay) ||
	   run_stats.workflow_activation_failures < 1 ||
	   run_stats.workflow_evictions < len(state.handles) + 2 ||
	   run_stats.workflow_cancellations_received != 1 || run_stats.timers_canceled != 1 ||
	   run_stats.workflow_signals_received != 3 ||
	   run_stats.workflow_signal_handlers_started != 3 ||
	   run_stats.workflow_signal_handlers_completed != 3 ||
	   run_stats.workflow_signal_handlers_abandoned != 0 ||
	   run_stats.workflow_signal_handler_warnings != 0 ||
	   run_stats.workflow_updates_accepted != 1 ||
	   run_stats.workflow_updates_rejected != 0 ||
	   run_stats.workflow_updates_completed != 1 ||
	   run_stats.workflow_update_handlers_abandoned != 0 ||
	   run_stats.workflow_update_handler_warnings != 0 ||
	   (force_replay && run_stats.workflow_replay_activations < 1) ||
	   run_stats.workflow_activations_peak_owned < 2 ||
	   run_stats.workflow_executor_queue_peak < 1 || peak < 2 || migrations != 0 {
		fmt.eprintf(
			"In-process concurrency proof failed handles=%d completed=%d failures=%d evictions=%d cancellations=%d canceled_timers=%d signals=%d signal_handlers=%d updates=%d replay_activations=%d owned_peak=%d queue_peak=%d callback_peak=%d migrations=%d cancellation_ok=%v reason_ok=%v signal_handler_ok=%v async_handler_ok=%v nondeterminism_ok=%v\n",
			len(state.handles),
			run_stats.workflow_executions_completed,
			run_stats.workflow_activation_failures,
			run_stats.workflow_evictions,
			run_stats.workflow_cancellations_received,
			run_stats.timers_canceled,
			run_stats.workflow_signals_received,
			run_stats.workflow_signal_handlers_completed,
			run_stats.workflow_updates_completed,
			run_stats.workflow_replay_activations,
			run_stats.workflow_activations_peak_owned,
			run_stats.workflow_executor_queue_peak,
			peak,
			migrations,
			state.cancellation_ok && cancellation_observed,
			cancellation_reason_ok,
			state.signal_handler_ok,
			state.async_handler_ok,
			state.nondeterminism_ok,
		)
		return 1
	}
	fmt.printf(
		"In-process Workflow concurrency proved handles=%d executor_threads=%d activation_failures=%d evictions=%d cancellations=%d canceled_timers=%d signals=%d signal_handlers=%d updates=%d replay_activations=%d owned_peak=%d queue_peak=%d callback_peak=%d migrations=%d nondeterminism=%v\n",
		len(state.handles),
		EXECUTOR_THREADS,
		run_stats.workflow_activation_failures,
		run_stats.workflow_evictions,
		run_stats.workflow_cancellations_received,
		run_stats.timers_canceled,
		run_stats.workflow_signals_received,
		run_stats.workflow_signal_handlers_completed,
		run_stats.workflow_updates_completed,
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

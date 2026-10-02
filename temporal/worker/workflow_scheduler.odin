package worker

import allocator_runtime "base:runtime"
import "core:fmt"
import "core:strings"
import "core:sync"
import "core:thread"
import temporal_core "../core"
import workflow "../workflow"

// In_Process_Workflow_Shard owns one fixed executor thread and a complete set
// of Workflow runtimes. Activations are assigned by run ID, so a live fiber is
// created, resumed, evicted, and destroyed on the same OS thread.
In_Process_Workflow_Scheduler :: struct {
	mutex:              sync.Mutex,
	activations_owned:  int,
	peak_owned:         int,
	queue_peak:         int,
	shards_used:        int,
	shards_observed:    int,
	shard_activations_min: int,
	shard_activations_max: int,
}

In_Process_Workflow_Shard :: struct {
	worker:      ^Worker,
	options:     Run_Options,
	scheduler:   ^In_Process_Workflow_Scheduler,
	runtime_set: Workflow_Runtime_Set,
	mutex:       sync.Mutex,
	available:   sync.Sema,
	ready:       sync.Sema,
	queue:       [dynamic][]u8,
	used:        bool,
	accepting:   bool,
	stop:        bool,
	thread:      ^thread.Thread,
	stats:       Run_Stats,
	activations_enqueued: int,
	error:       Error,
}

workflow_scheduler_retain_activation :: proc(
	scheduler: ^In_Process_Workflow_Scheduler,
	queue_depth: int,
	first_shard_use := false,
) {
	if scheduler == nil || queue_depth < 0 {
		return
	}
	sync.mutex_lock(&scheduler.mutex)
	scheduler.activations_owned += 1
	scheduler.peak_owned = max(scheduler.peak_owned, scheduler.activations_owned)
	scheduler.queue_peak = max(scheduler.queue_peak, queue_depth)
	if first_shard_use {
		scheduler.shards_used += 1
	}
	sync.mutex_unlock(&scheduler.mutex)
}

workflow_scheduler_release_activations :: proc(
	scheduler: ^In_Process_Workflow_Scheduler,
	count: int,
) -> bool {
	if scheduler == nil || count < 0 {
		return false
	}
	sync.mutex_lock(&scheduler.mutex)
	balanced := count <= scheduler.activations_owned
	if balanced {
		scheduler.activations_owned -= count
	}
	sync.mutex_unlock(&scheduler.mutex)
	return balanced
}

workflow_scheduler_observe_shard_activations :: proc(
	scheduler: ^In_Process_Workflow_Scheduler,
	count: int,
) {
	if scheduler == nil || count <= 0 {
		return
	}
	sync.mutex_lock(&scheduler.mutex)
	if scheduler.shards_observed == 0 || count < scheduler.shard_activations_min {
		scheduler.shard_activations_min = count
	}
	scheduler.shard_activations_max = max(scheduler.shard_activations_max, count)
	scheduler.shards_observed += 1
	sync.mutex_unlock(&scheduler.mutex)
}

workflow_runtime_set_close_on_executor :: proc(
	runtime_set: ^Workflow_Runtime_Set,
	destroyed: ^int = nil,
) -> (error: Error) {
	if runtime_set == nil {
		return
	}
	for &registration in runtime_set.registrations {
		if destroyed != nil {
			destroyed^ += len(registration.runtime.instances)
		}
		shutdown_error := workflow.fiber_runtime_executor_shutdown(&registration.runtime)
		if shutdown_error.message != nil {
			if error.message == nil {
				error = error_from_string(string(shutdown_error.message))
			}
			workflow.error_free(&shutdown_error)
		}
		workflow.fiber_runtime_close(&registration.runtime)
		delete(registration.name, allocator_runtime.heap_allocator())
	}
	for run in runtime_set.runs {
		delete(run.run_id, allocator_runtime.heap_allocator())
	}
	for rejected in runtime_set.rejected_runs {
		delete(rejected.run_id, allocator_runtime.heap_allocator())
	}
	delete(runtime_set.registrations)
	delete(runtime_set.runs)
	delete(runtime_set.rejected_runs)
	runtime_set^ = {}
	return
}

workflow_runtime_set_init :: proc(
	worker: ^Worker,
	runtime_set: ^Workflow_Runtime_Set,
) -> Error {
	if worker == nil || runtime_set == nil || runtime_set.registrations != nil {
		return error_from_string("Workflow runtime set initialization is invalid")
	}
	heap := allocator_runtime.heap_allocator()
	count := workflow.registry_count(&worker.workflow_registry)
	if count == 0 {
		return error_from_string("Workflow runtime factory registry is empty")
	}
	runtime_set.registrations = make([dynamic]Workflow_Registration, 0, count, heap)
	runtime_set.runs = make([dynamic]Workflow_Run, heap)
	runtime_set.rejected_runs = make([dynamic]Rejected_Workflow_Run, heap)
	for index in 0 ..< count {
		name, found := workflow.registry_name(&worker.workflow_registry, index)
		if !found || len(name) == 0 {
			cleanup_error := workflow_runtime_set_close_on_executor(runtime_set)
			error_free(&cleanup_error)
			return error_from_string("Workflow runtime factory has an invalid name")
		}
		owned_name, clone_error := strings.clone(name, heap)
		if clone_error != nil {
			cleanup_error := workflow_runtime_set_close_on_executor(runtime_set)
			error_free(&cleanup_error)
			return error_from_string("could not allocate executor Workflow name")
		}
		runtime := workflow.Fiber_Runtime{}
		instantiate_error := workflow.registry_instantiate(
			&worker.workflow_registry,
			index,
			&runtime,
			worker.task_queue,
			namespace = workflow_runtime_namespace(worker),
			stack_options = worker.workflow_stacks,
		)
		if instantiate_error.message != nil {
			delete(owned_name, heap)
			result := error_from_string(string(instantiate_error.message))
			workflow.error_free(&instantiate_error)
			cleanup_error := workflow_runtime_set_close_on_executor(runtime_set)
			error_free(&cleanup_error)
			return result
		}
		configuration_error := configure_workflow_runtime(worker, &runtime)
		if configuration_error.message != nil {
			delete(owned_name, heap)
			workflow.fiber_runtime_close(&runtime)
			cleanup_error := workflow_runtime_set_close_on_executor(runtime_set)
			error_free(&cleanup_error)
			return configuration_error
		}
		append(
			&runtime_set.registrations,
			Workflow_Registration{name = owned_name, runtime = runtime},
		)
	}
	return {}
}

workflow_stats_add :: proc(total: ^Run_Stats, value: Run_Stats) {
	total.workflow_activations_completed += value.workflow_activations_completed
	total.workflow_activation_failures += value.workflow_activation_failures
	total.workflow_executions_completed += value.workflow_executions_completed
	total.workflow_replay_activations += value.workflow_replay_activations
	total.workflow_evictions += value.workflow_evictions
	total.workflow_cached_fibers_shutdown += value.workflow_cached_fibers_shutdown
	total.workflow_signals_received += value.workflow_signals_received
	total.workflow_signal_handlers_started += value.workflow_signal_handlers_started
	total.workflow_signal_handlers_completed += value.workflow_signal_handlers_completed
	total.workflow_signal_handlers_abandoned += value.workflow_signal_handlers_abandoned
	total.workflow_signal_handler_warnings += value.workflow_signal_handler_warnings
	total.workflow_queries_answered += value.workflow_queries_answered
	total.workflow_queries_failed += value.workflow_queries_failed
	total.workflow_updates_accepted += value.workflow_updates_accepted
	total.workflow_updates_rejected += value.workflow_updates_rejected
	total.workflow_updates_completed += value.workflow_updates_completed
	total.workflow_update_handlers_abandoned += value.workflow_update_handlers_abandoned
	total.workflow_update_handler_warnings += value.workflow_update_handler_warnings
	total.workflow_cancellations_received += value.workflow_cancellations_received
	total.timers_canceled += value.timers_canceled
	total.activity_cancellations_requested += value.activity_cancellations_requested
}

workflow_shard_mark_failed :: proc(shard: ^In_Process_Workflow_Shard, failure: Error) {
	owned_error := failure
	if shard == nil {
		error_free(&owned_error)
		return
	}
	sync.mutex_lock(&shard.mutex)
	shard.accepting = false
	if shard.error.message == nil {
		shard.error = owned_error
		owned_error = {}
	}
	sync.mutex_unlock(&shard.mutex)
	error_free(&owned_error)
	worker_initiate_shutdown(shard.worker)
}

workflow_shard_process_activation :: proc(
	shard: ^In_Process_Workflow_Shard,
	activation: []u8,
) -> bool {
	processed, dispatch_error := process_workflow_activation_in_set(
		&shard.runtime_set,
		activation,
	)
	if dispatch_error.message != nil {
		workflow_shard_mark_failed(shard, dispatch_error)
		return false
	}
	defer {
		delete(processed.completion)
		workflow.error_free(&processed.error)
	}
	if processed.completion == nil {
		workflow_shard_mark_failed(
			shard,
			error_from_string("Workflow activation produced no completion"),
		)
		return false
	}
	complete_error := temporal_core.worker_complete_workflow(
		&shard.worker.core,
		processed.completion[:],
	)
	if shard.options.on_workflow_activation != nil {
		shard.options.on_workflow_activation(&processed)
	}
	if complete_error.message != nil {
		workflow_shard_mark_failed(shard, error_from_core(&complete_error))
		return false
	}
	record_workflow_result(&shard.stats, &processed)
	if !processed.is_replaying && processed.warned_unfinished_update_handlers > 0 {
		fmt.eprintf(
			"Temporal warning: Workflow %s closed with %d unfinished Update handler(s); handlers were abandoned\n",
			processed.run_id,
			processed.warned_unfinished_update_handlers,
		)
	}
	if !processed.is_replaying && processed.warned_unfinished_signal_handlers > 0 {
		fmt.eprintf(
			"Temporal warning: Workflow %s closed with %d unfinished Signal handler(s); handlers were abandoned\n",
			processed.run_id,
			processed.warned_unfinished_signal_handlers,
		)
	}
	if processed.error.message != nil && !processed.activation_failed {
		workflow_shard_mark_failed(
			shard,
			error_from_string(string(processed.error.message)),
		)
		return false
	}
	return true
}

workflow_shard_thread_main :: proc(shard: ^In_Process_Workflow_Shard) {
	init_error := workflow_runtime_set_init(shard.worker, &shard.runtime_set)
	sync.mutex_lock(&shard.mutex)
	if init_error.message == nil {
		shard.accepting = true
	} else {
		shard.error = init_error
		init_error = {}
	}
	sync.mutex_unlock(&shard.mutex)
	sync.sema_post(&shard.ready)
	if shard.error.message != nil {
		return
	}

	for {
		sync.sema_wait(&shard.available)
		activation: []u8
		should_stop := false
		sync.mutex_lock(&shard.mutex)
		if len(shard.queue) > 0 {
			activation = shard.queue[0]
			ordered_remove(&shard.queue, 0)
		} else if shard.stop {
			should_stop = true
		}
		sync.mutex_unlock(&shard.mutex)
		if should_stop {
			break
		}
		if activation == nil {
			continue
		}
		processed := workflow_shard_process_activation(shard, activation)
		delete(activation)
		if !workflow_scheduler_release_activations(shard.scheduler, 1) {
			workflow_shard_mark_failed(
				shard,
				error_from_string("Workflow scheduler activation ownership underflow"),
			)
			break
		}
		if !processed {
			break
		}
	}

	sync.mutex_lock(&shard.mutex)
	shard.accepting = false
	dropped := len(shard.queue)
	for activation in shard.queue {
		delete(activation)
	}
	clear(&shard.queue)
	sync.mutex_unlock(&shard.mutex)
	if !workflow_scheduler_release_activations(shard.scheduler, dropped) {
		workflow_shard_mark_failed(
			shard,
			error_from_string("Workflow scheduler queued ownership underflow"),
		)
	}
	shutdown_count := 0
	close_error := workflow_runtime_set_close_on_executor(
		&shard.runtime_set,
		&shutdown_count,
	)
	shard.stats.workflow_cached_fibers_shutdown += shutdown_count
	if close_error.message != nil {
		workflow_shard_mark_failed(shard, close_error)
	}
}

workflow_shard_enqueue :: proc(
	shard: ^In_Process_Workflow_Shard,
	activation: []u8,
) -> Error {
	if shard == nil || activation == nil {
		return error_from_string("Workflow shard enqueue is invalid")
	}
	sync.mutex_lock(&shard.mutex)
	if !shard.accepting || shard.stop {
		message := "Workflow executor shard is not accepting activations"
		if shard.error.message != nil {
			message = string(shard.error.message)
		}
		result := error_from_string(message)
		sync.mutex_unlock(&shard.mutex)
		return result
	}
	append(&shard.queue, activation)
	shard.activations_enqueued += 1
	queue_depth := len(shard.queue)
	first_shard_use := !shard.used
	shard.used = true
	workflow_scheduler_retain_activation(
		shard.scheduler,
		queue_depth,
		first_shard_use,
	)
	sync.mutex_unlock(&shard.mutex)
	sync.sema_post(&shard.available)
	return {}
}

workflow_shard_request_stop :: proc(shard: ^In_Process_Workflow_Shard) {
	if shard == nil {
		return
	}
	sync.mutex_lock(&shard.mutex)
	shard.accepting = false
	shard.stop = true
	sync.mutex_unlock(&shard.mutex)
	sync.sema_post(&shard.available)
}

workflow_shards_finish :: proc(
	shards: []^In_Process_Workflow_Shard,
	stats: ^Run_Stats,
) -> (error: Error) {
	for shard in shards {
		workflow_shard_request_stop(shard)
	}
	for shard in shards {
		if shard.thread != nil {
			thread.join(shard.thread)
		}
		workflow_scheduler_observe_shard_activations(
			shard.scheduler,
			shard.activations_enqueued,
		)
		workflow_stats_add(stats, shard.stats)
		if shard.error.message != nil {
			if error.message == nil {
				error = shard.error
				shard.error = {}
			} else {
				error_free(&shard.error)
			}
		}
		if shard.thread != nil {
			thread.destroy(shard.thread)
		}
		delete(shard.queue)
		free(shard, allocator_runtime.heap_allocator())
	}
	return
}

run_in_process_workflow_executor_loop :: proc(
	worker: ^Worker,
	options: Run_Options,
	executor_threads: int,
) -> (stats: Run_Stats, error: Error) {
	if worker == nil || executor_threads <= 1 {
		return {}, error_from_string("concurrent in-process Workflow executor configuration is invalid")
	}
	heap := allocator_runtime.heap_allocator()
	scheduler := In_Process_Workflow_Scheduler{}
	shards := make([]^In_Process_Workflow_Shard, executor_threads, heap)
	started := 0
	defer delete(shards, heap)
	for index in 0 ..< executor_threads {
		shard, allocation_error := new(In_Process_Workflow_Shard, heap)
		if allocation_error != nil {
			error = error_from_string("could not allocate Workflow executor shard")
			break
		}
		shard.worker = worker
		shard.options = options
		shard.scheduler = &scheduler
		shard.queue = make([dynamic][]u8, heap)
		shards[index] = shard
		shard.thread = thread.create_and_start_with_poly_data(
			shard,
			workflow_shard_thread_main,
		)
		if shard.thread == nil {
			error = error_from_string("could not start Workflow executor shard")
			break
		}
		started += 1
	}

	if error.message == nil {
		for index in 0 ..< started {
			shard := shards[index]
			sync.sema_wait(&shard.ready)
			if shard.error.message != nil {
				error = error_from_string(string(shard.error.message))
				break
			}
		}
	}

	if error.message == nil {
		for {
			poll := temporal_core.worker_poll_workflow(&worker.core)
			if poll.error.message != nil {
				error = error_from_core(&poll.error)
				break
			}
			if poll.shutdown {
				break
			}
			route, route_ok := decode_workflow_activation_route(poll.protobuf)
			if !route_ok {
				delete(poll.protobuf)
				error = error_from_string("Core returned a malformed WorkflowActivation protobuf")
				worker_initiate_shutdown(worker)
				break
			}
			shard_index, shard_ok := workflow.activation_shard_index(
				route.run_id,
				started,
			)
			if !shard_ok {
				delete(poll.protobuf)
				error = error_from_string("Workflow activation has no routable run ID")
				worker_initiate_shutdown(worker)
				break
			}
			enqueue_error := workflow_shard_enqueue(shards[shard_index], poll.protobuf)
			if enqueue_error.message != nil {
				delete(poll.protobuf)
				error = enqueue_error
				worker_initiate_shutdown(worker)
				break
			}
		}
	}

	finish_error := workflow_shards_finish(shards[:started], &stats)
	sync.mutex_lock(&scheduler.mutex)
	stats.workflow_activations_peak_owned = scheduler.peak_owned
	stats.workflow_executor_queue_peak = scheduler.queue_peak
	stats.workflow_executor_shards_used = scheduler.shards_used
	stats.workflow_executor_shard_activations_min = scheduler.shard_activations_min
	stats.workflow_executor_shard_activations_max = scheduler.shard_activations_max
	remaining_owned := scheduler.activations_owned
	sync.mutex_unlock(&scheduler.mutex)
	if remaining_owned != 0 {
		ownership_error := error_from_string("Workflow scheduler did not release every owned activation")
		if finish_error.message == nil {
			finish_error = ownership_error
		} else {
			error_free(&ownership_error)
		}
	}
	for index in started ..< executor_threads {
		shard := shards[index]
		if shard == nil {
			continue
		}
		delete(shard.queue)
		free(shard, heap)
	}
	if error.message == nil {
		error = finish_error
	} else {
		error_free(&finish_error)
	}
	return
}

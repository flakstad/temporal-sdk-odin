// Package worker owns registration, Core polling, dispatch, and lifecycle.
// The one-string codec and Workflow fiber runtime remain experimental.
package worker

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:fmt"
import "core:mem/virtual"
import "core:strings"
import "core:thread"
import activity "../activity"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"
import workflow "../workflow"

String_Activity_Proc :: #type proc(ctx: ^activity.Context, input: string) -> string

Activity_Registration :: struct {
	name:     string,
	callback: String_Activity_Proc,
}

Workflow_Registration :: struct {
	name:    string,
	runtime: workflow.Fiber_Runtime,
}

Workflow_Run :: struct {
	run_id:             string,
	registration_index: int,
}

Workflow_Activation_Observer :: #type proc(result: ^workflow.Process_Result)

Run_Options :: struct {
	// Zero means no bound. These limits exist for tests and examples; production
	// workers normally run until request_shutdown is called.
	max_activity_tasks:       int,
	max_workflow_executions: int,
	disable_activity_polling: bool,
	on_workflow_activation:  Workflow_Activation_Observer,
}

Run_Stats :: struct {
	activity_tasks_completed:             int,
	activity_cancellations_received:      int,
	workflow_activations_completed:       int,
	workflow_executions_completed:        int,
	workflow_replay_activations:          int,
	workflow_evictions:                   int,
	workflow_signals_received:            int,
	workflow_cancellations_received:      int,
	timers_canceled:                      int,
	activity_cancellations_requested:     int,
}

Lifecycle :: enum {
	Uninitialized,
	Configuring,
	Running,
	Stopped,
	Closed,
}

Worker :: struct {
	core:                   temporal_core.Worker,
	client:                 ^temporal_core.Client,
	namespace:              string,
	task_queue:             string,
	activity_registrations: [dynamic]Activity_Registration,
	workflow_registrations: [dynamic]Workflow_Registration,
	workflow_runs:          [dynamic]Workflow_Run,
	lifecycle:              Lifecycle,
	max_concurrent_activities: int,
}

Error :: struct {
	message: []u8,
}

error_free :: proc(error: ^Error) {
	if error == nil || error.message == nil {
		return
	}
	delete(error.message)
	error.message = nil
}

error_from_string :: proc(message: string) -> Error {
	return Error{message = bytes.clone(transmute([]u8)message)}
}

error_from_core :: proc(core_error: ^temporal_core.Error) -> Error {
	if core_error == nil || core_error.message == nil {
		return {}
	}
	result := Error{message = core_error.message}
	core_error.message = nil
	return result
}

create :: proc(
	client: ^temporal_core.Client,
	task_queue: string,
	namespace := "default",
	max_concurrent_activities := 10,
) -> (worker: Worker, error: Error) {
	if client == nil || client.handle == nil || client.runtime == nil {
		return {}, error_from_string("client is not initialized")
	}
	if len(task_queue) == 0 {
		return {}, error_from_string("task queue must not be empty")
	}
	if len(namespace) == 0 {
		return {}, error_from_string("namespace must not be empty")
	}
	if max_concurrent_activities <= 0 {
		return {}, error_from_string("max concurrent activities must be positive")
	}
	owned_task_queue, allocation_error := strings.clone(task_queue)
	if allocation_error != nil {
		return {}, error_from_string("could not allocate worker task queue")
	}
	owned_namespace, namespace_allocation_error := strings.clone(namespace)
	if namespace_allocation_error != nil {
		delete(owned_task_queue)
		return {}, error_from_string("could not allocate worker namespace")
	}
	worker.client = client
	worker.namespace = owned_namespace
	worker.task_queue = owned_task_queue
	worker.lifecycle = .Configuring
	worker.max_concurrent_activities = max_concurrent_activities
	worker.activity_registrations = make([dynamic]Activity_Registration)
	worker.workflow_registrations = make([dynamic]Workflow_Registration)
	worker.workflow_runs = make([dynamic]Workflow_Run)
	return
}

register_activity :: proc(worker: ^Worker, name: string, callback: String_Activity_Proc) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("activities cannot be registered after worker.run starts")
	}
	if len(name) == 0 || callback == nil {
		return error_from_string("activity name and callback are required")
	}
	for registration in worker.activity_registrations {
		if registration.name == name {
			return error_from_string("activity is already registered")
		}
	}
	owned_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		return error_from_string("could not allocate activity name")
	}
	append(&worker.activity_registrations, Activity_Registration{name = owned_name, callback = callback})
	return {}
}

register_workflow :: proc(worker: ^Worker, name: string, callback: workflow.Fiber_Proc) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("workflows cannot be registered after worker.run starts")
	}
	if len(name) == 0 || callback == nil {
		return error_from_string("workflow name and callback are required")
	}
	for registration in worker.workflow_registrations {
		if registration.name == name {
			return error_from_string("workflow is already registered")
		}
	}
	owned_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		return error_from_string("could not allocate workflow name")
	}
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(&runtime, name, worker.task_queue, callback)
	if init_error.message != nil {
		delete(owned_name)
		result := error_from_string(string(init_error.message))
		workflow.error_free(&init_error)
		return result
	}
	append(
		&worker.workflow_registrations,
		Workflow_Registration{name = owned_name, runtime = runtime},
	)
	return {}
}

find_activity :: proc(worker: ^Worker, name: string) -> (String_Activity_Proc, bool) {
	for registration in worker.activity_registrations {
		if registration.name == name {
			return registration.callback, true
		}
	}
	return nil, false
}

completion_failure :: proc(task_token: []u8, message: string) -> [dynamic]u8 {
	return proto.encode_activity_failure(task_token, message)
}

execute_start :: proc(worker: ^Worker, task: ^proto.Activity_Task) -> (completion: [dynamic]u8, error: Error) {
	callback, found := find_activity(worker, task.activity_type)
	if !found {
		message := fmt.aprintf("activity %q is not registered", task.activity_type)
		defer delete(message)
		return completion_failure(task.task_token, message), {}
	}

	input, decode_error := codec.decode_string(task.input.data, task.input.encoding)
	if decode_error != .None {
		return completion_failure(task.task_token, "activity input must be one json/plain string"), {}
	}
	defer delete(input)

	activity_context := activity.Context{
	}
	activity.context_init(&activity_context, task.activity_id, task.attempt, nil, nil)
	// Activity-local allocations, including the returned string, live only until
	// its payload has been copied into the completion.
	arena: virtual.Arena
	arena_error := virtual.arena_init_growing(&arena)
	if arena_error != nil {
		return nil, error_from_string("could not initialize activity allocation arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	output := callback(&activity_context, input)
	context.allocator = previous_allocator

	result_data, encode_error := codec.encode_string(output)
	if encode_error != .None {
		return nil, error_from_string("could not encode activity result")
	}
	defer delete(result_data)
	return proto.encode_activity_success(task.task_token, result_data), {}
}

Activity_Execution :: struct {
	worker:     ^Worker,
	callback:   String_Activity_Proc,
	task_token: []u8,
	activity_id: string,
	input:       string,
	ctx:         activity.Context,
	error:       Error,
}

activity_cancel_reason :: proc(reason: proto.Coresdk_Activity_Task_ActivityCancelReason) -> activity.Cancel_Reason {
	switch reason {
	case .NOT_FOUND:
		return .Not_Found
	case .CANCELLED:
		return .Cancelled
	case .TIMED_OUT:
		return .Timed_Out
	case .WORKER_SHUTDOWN:
		return .Worker_Shutdown
	case .PAUSED:
		return .Paused
	case .RESET:
		return .Reset
	}
	return .Cancelled
}

activity_heartbeat_recorder :: proc(user_data: rawptr, details: string) -> activity.Heartbeat_Status {
	execution := cast(^Activity_Execution)user_data
	if execution == nil || execution.worker == nil {
		return .Invalid_Context
	}
	details_data, encode_error := codec.encode_string(details)
	if encode_error != .None {
		return .Encoding_Failed
	}
	defer delete(details_data)
	heartbeat := proto.encode_activity_heartbeat(execution.task_token, details_data)
	defer delete(heartbeat)
	core_error := temporal_core.worker_record_activity_heartbeat(&execution.worker.core, heartbeat[:])
	if core_error.message != nil {
		temporal_core.error_free(&core_error)
		return .Core_Error
	}
	return .Recorded
}

activity_execution_main :: proc(execution: ^Activity_Execution) {
	// Activity-local allocations, including the returned string, live only until
	// its payload has been copied into the completion.
	arena: virtual.Arena
	arena_error := virtual.arena_init_growing(&arena)
	if arena_error != nil {
		execution.error = error_from_string("could not initialize activity allocation arena")
		temporal_core.worker_initiate_shutdown(&execution.worker.core)
		return
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	output := execution.callback(&execution.ctx, execution.input)
	context.allocator = previous_allocator

	completion: [dynamic]u8
	if activity.is_cancellation_requested(&execution.ctx) {
		completion = proto.encode_activity_cancellation(execution.task_token)
	} else {
		result_data, encode_error := codec.encode_string(output)
		if encode_error != .None {
			execution.error = error_from_string("could not encode activity result")
			temporal_core.worker_initiate_shutdown(&execution.worker.core)
			return
		}
		defer delete(result_data)
		completion = proto.encode_activity_success(execution.task_token, result_data)
	}
	defer delete(completion)
	core_error := temporal_core.worker_complete_activity(&execution.worker.core, completion[:])
	if core_error.message != nil {
		execution.error = error_from_core(&core_error)
		temporal_core.worker_initiate_shutdown(&execution.worker.core)
	}
}

activity_execution_task :: proc(task: thread.Task) {
	activity_execution_main(cast(^Activity_Execution)task.data)
}

activity_execution_destroy :: proc(execution: ^Activity_Execution) {
	if execution == nil {
		return
	}
	delete(execution.task_token)
	delete(execution.activity_id)
	delete(execution.input)
	error_free(&execution.error)
	free(execution)
}

activity_execution_start :: proc(
	worker: ^Worker,
	pool: ^thread.Pool,
	task: ^proto.Activity_Task,
) -> (execution: ^Activity_Execution, error: Error) {
	callback, found := find_activity(worker, task.activity_type)
	if !found {
		message := fmt.aprintf("activity %q is not registered", task.activity_type)
		defer delete(message)
		completion := completion_failure(task.task_token, message)
		defer delete(completion)
		core_error := temporal_core.worker_complete_activity(&worker.core, completion[:])
		return nil, error_from_core(&core_error)
	}

	input, decode_error := codec.decode_string(task.input.data, task.input.encoding)
	if decode_error != .None {
		completion := completion_failure(task.task_token, "activity input must be one json/plain string")
		defer delete(completion)
		core_error := temporal_core.worker_complete_activity(&worker.core, completion[:])
		return nil, error_from_core(&core_error)
	}
	execution = new(Activity_Execution)
	execution.worker = worker
	execution.callback = callback
	execution.task_token = bytes.clone(task.task_token)
	execution.input = input
	owned_activity_id, allocation_error := strings.clone(task.activity_id)
	if allocation_error != nil {
		activity_execution_destroy(execution)
		return nil, error_from_string("could not allocate Activity execution")
	}
	execution.activity_id = owned_activity_id
	activity.context_init(
		&execution.ctx,
		execution.activity_id,
		task.attempt,
		cast(rawptr)execution,
		activity_heartbeat_recorder,
	)
	thread.pool_add_task(pool, context.allocator, activity_execution_task, execution)
	return execution, {}
}

Workflow_Activation_Route :: struct {
	run_id:        string,
	workflow_type: string,
	initializes:   bool,
}

decode_workflow_activation_route :: proc(data: []u8) -> (route: Workflow_Activation_Route, ok: bool) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{}
	ok = proto.decode_Coresdk_Workflow_Activation_WorkflowActivation(data, &activation)
	context.allocator = previous_allocator
	if !ok || len(activation.run_id) == 0 {
		ok = false
		return
	}
	route.run_id = activation.run_id
	for job in activation.jobs {
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow:
			if variant.value == nil || route.initializes {
				ok = false
				return
			}
			route.initializes = true
			route.workflow_type = variant.value.workflow_type
		}
	}
	return
}

find_workflow_registration :: proc(worker: ^Worker, name: string) -> (int, bool) {
	for registration, index in worker.workflow_registrations {
		if registration.name == name {
			return index, true
		}
	}
	return 0, false
}

find_workflow_run :: proc(worker: ^Worker, run_id: string) -> (int, bool) {
	for workflow_run, index in worker.workflow_runs {
		if workflow_run.run_id == run_id {
			return index, true
		}
	}
	return 0, false
}

remove_workflow_run :: proc(worker: ^Worker, index: int) {
	delete(worker.workflow_runs[index].run_id)
	ordered_remove(&worker.workflow_runs, index)
}

process_workflow_activation :: proc(
	worker: ^Worker,
	data: []u8,
) -> (processed: workflow.Process_Result, error: Error) {
	route, route_ok := decode_workflow_activation_route(data)
	if !route_ok {
		return {}, error_from_string("Core returned a malformed WorkflowActivation protobuf")
	}

	registration_index := 0
	run_index, run_found := find_workflow_run(worker, route.run_id)
	owned_run_id := ""
	if route.initializes {
		if run_found {
			return {}, error_from_string("Workflow run was initialized more than once without eviction")
		}
		registration_found: bool
		registration_index, registration_found = find_workflow_registration(worker, route.workflow_type)
		if !registration_found {
			message := fmt.aprintf("workflow %q is not registered", route.workflow_type)
			defer delete(message)
			return {}, error_from_string(message)
		}
		cloned_run_id, allocation_error := strings.clone(route.run_id)
		if allocation_error != nil {
			return {}, error_from_string("could not allocate Workflow run route")
		}
		owned_run_id = cloned_run_id
	} else {
		if !run_found {
			return {}, error_from_string("Workflow activation does not match a registered run")
		}
		registration_index = worker.workflow_runs[run_index].registration_index
	}

	registration := &worker.workflow_registrations[registration_index]
	processed = workflow.process_fiber_activation(&registration.runtime, data)
	if route.initializes {
		if workflow.fiber_runtime_has_run(&registration.runtime, route.run_id) {
			append(
				&worker.workflow_runs,
				Workflow_Run{
					run_id = owned_run_id,
					registration_index = registration_index,
				},
			)
			owned_run_id = ""
		}
		delete(owned_run_id)
	}
	if processed.evicted {
		run_index, run_found = find_workflow_run(worker, route.run_id)
		if run_found {
			remove_workflow_run(worker, run_index)
		}
	}
	return
}

find_activity_execution :: proc(
	executions: []^Activity_Execution,
	task_token: []u8,
) -> (^Activity_Execution, bool) {
	for execution in executions {
		if bytes.equal(execution.task_token, task_token) {
			return execution, true
		}
	}
	return nil, false
}

reap_activity_executions :: proc(
	executions: ^[dynamic]^Activity_Execution,
	pool: ^thread.Pool,
) -> (completed: int, error: Error) {
	for {
		task, done := thread.pool_pop_done(pool)
		if !done {
			break
		}
		execution := cast(^Activity_Execution)task.data
		index := -1
		for candidate, candidate_index in executions^ {
			if candidate == execution {
				index = candidate_index
				break
			}
		}
		if index < 0 {
			activity_execution_destroy(execution)
			if error.message == nil {
				error = error_from_string("Activity executor returned an unknown task")
			}
			continue
		}
		if execution.error.message != nil {
			if error.message == nil {
				error = execution.error
				execution.error = {}
			}
		} else {
			completed += 1
		}
		activity_execution_destroy(execution)
		unordered_remove(executions, index)
	}
	return
}

run_activity_loop :: proc(
	worker: ^Worker,
	max_tasks: int,
) -> (completed: int, cancellations: int, error: Error) {
	executions := make([dynamic]^Activity_Execution)
	defer delete(executions)
	pool := thread.Pool{}
	thread.pool_init(
		&pool,
		allocator_runtime.heap_allocator(),
		worker.max_concurrent_activities,
	)
	thread.pool_start(&pool)
	pool_finished := false
	defer {
		if !pool_finished {
			thread.pool_finish(&pool)
			_, cleanup_error := reap_activity_executions(&executions, &pool)
			error_free(&cleanup_error)
		}
		thread.pool_destroy(&pool)
	}
	started := 0
	for max_tasks <= 0 || started < max_tasks {
		reaped, reap_error := reap_activity_executions(&executions, &pool)
		completed += reaped
		if reap_error.message != nil {
			return completed, cancellations, reap_error
		}
		poll := temporal_core.worker_poll_activity(&worker.core)
		if poll.error.message != nil {
			return completed, cancellations, error_from_core(&poll.error)
		}
		if poll.shutdown {
			break
		}

		decode_arena: virtual.Arena
		arena_error := virtual.arena_init_growing(&decode_arena)
		if arena_error != nil {
			delete(poll.protobuf)
			return completed, cancellations, error_from_string("could not initialize protobuf decode arena")
		}
		previous_allocator := context.allocator
		context.allocator = virtual.arena_allocator(&decode_arena)
		task, decode_error := proto.decode_activity_task(poll.protobuf)
		context.allocator = previous_allocator
		if decode_error != .None {
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			return completed, cancellations, error_from_string("Core returned a malformed ActivityTask protobuf")
		}
		if task.kind == .Cancel {
			execution, found := find_activity_execution(executions[:], task.task_token)
			if found {
				activity.context_request_cancellation(
					&execution.ctx,
					activity_cancel_reason(task.cancel_reason),
				)
				cancellations += 1
			}
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			continue
		}

		execution, execute_error := activity_execution_start(worker, &pool, &task)
		virtual.arena_destroy(&decode_arena)
		delete(poll.protobuf)
		if execute_error.message != nil {
			return completed, cancellations, execute_error
		}
		started += 1
		if execution == nil {
			completed += 1
		} else {
			append(&executions, execution)
		}
	}
	thread.pool_finish(&pool)
	pool_finished = true
	reaped, reap_error := reap_activity_executions(&executions, &pool)
	completed += reaped
	return completed, cancellations, reap_error
}

record_workflow_result :: proc(stats: ^Run_Stats, processed: ^workflow.Process_Result) {
	stats.workflow_activations_completed += 1
	if processed.completed {
		stats.workflow_executions_completed += 1
	}
	if processed.is_replaying {
		stats.workflow_replay_activations += 1
	}
	if processed.evicted {
		stats.workflow_evictions += 1
	}
	if !processed.is_replaying {
		stats.workflow_signals_received += processed.received_signal_count
		if processed.received_cancellation {
			stats.workflow_cancellations_received += 1
		}
		if processed.canceled_timer {
			stats.timers_canceled += 1
		}
		stats.activity_cancellations_requested += processed.requested_activity_cancellation_count
	}
}

run_workflow_loop :: proc(
	worker: ^Worker,
	options: Run_Options,
) -> (stats: Run_Stats, error: Error) {
	target_reached := false
	for {
		poll := temporal_core.worker_poll_workflow(&worker.core)
		if poll.error.message != nil {
			return stats, error_from_core(&poll.error)
		}
		if poll.shutdown {
			break
		}

		processed, dispatch_error := process_workflow_activation(worker, poll.protobuf)
		delete(poll.protobuf)
		if dispatch_error.message != nil {
			return stats, dispatch_error
		}
		if processed.completion == nil {
			workflow.error_free(&processed.error)
			return stats, error_from_string("Workflow activation produced no completion")
		}
		complete_error := temporal_core.worker_complete_workflow(
			&worker.core,
			processed.completion[:],
		)
		if options.on_workflow_activation != nil {
			options.on_workflow_activation(&processed)
		}
		delete(processed.completion)
		processed.completion = nil
		if complete_error.message != nil {
			workflow.error_free(&processed.error)
			return stats, error_from_core(&complete_error)
		}
		record_workflow_result(&stats, &processed)
		if processed.error.message != nil {
			message := string(processed.error.message)
			result := error_from_string(message)
			workflow.error_free(&processed.error)
			return stats, result
		}
		workflow.error_free(&processed.error)

		if options.max_workflow_executions > 0 &&
		   stats.workflow_executions_completed >= options.max_workflow_executions {
			target_reached = true
		}
		// Core owns cache eviction. A bounded run waits for the first eviction
		// after its terminal command so the language-owned fiber is destroyed.
		if target_reached && processed.evicted {
			break
		}
	}
	return
}

Activity_Thread_State :: struct {
	worker:    ^Worker,
	max_tasks: int,
	completed: int,
	cancellations: int,
	error:     Error,
}

Workflow_Thread_State :: struct {
	worker:  ^Worker,
	options: Run_Options,
	stats:   Run_Stats,
	error:   Error,
}

activity_thread_main :: proc(state: ^Activity_Thread_State) {
	state.completed, state.cancellations, state.error = run_activity_loop(state.worker, state.max_tasks)
	if state.error.message != nil {
		temporal_core.worker_initiate_shutdown(&state.worker.core)
	}
}

workflow_thread_main :: proc(state: ^Workflow_Thread_State) {
	state.stats, state.error = run_workflow_loop(state.worker, state.options)
	if state.error.message != nil {
		temporal_core.worker_initiate_shutdown(&state.worker.core)
	}
}

run :: proc(worker: ^Worker, options := Run_Options{}) -> (stats: Run_Stats, error: Error) {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return {}, error_from_string("worker is not initialized")
	}
	if worker.lifecycle == .Running {
		return {}, error_from_string("worker is already running")
	}
	if worker.lifecycle == .Stopped {
		return {}, error_from_string("worker cannot be run more than once")
	}
	if options.max_activity_tasks < 0 || options.max_workflow_executions < 0 {
		return {}, error_from_string("worker run limits must not be negative")
	}
	poll_activities := len(worker.activity_registrations) > 0 && !options.disable_activity_polling
	poll_workflows := len(worker.workflow_registrations) > 0
	if !poll_activities && !poll_workflows {
		return {}, error_from_string("worker has no enabled registered task types")
	}
	if worker.client == nil || worker.client.handle == nil || worker.client.runtime == nil {
		return {}, error_from_string("client must remain open until worker.close returns")
	}

	core_worker, create_error := temporal_core.worker_create(
		worker.client,
		worker.task_queue,
		worker.namespace,
		enable_workflows = poll_workflows,
		enable_remote_activities = poll_activities,
		max_activity_slots = worker.max_concurrent_activities,
	)
	if create_error.message != nil {
		return {}, error_from_core(&create_error)
	}
	worker.core = core_worker

	validate_error := temporal_core.worker_validate(&worker.core)
	if validate_error.message != nil {
		result := error_from_core(&validate_error)
		close_error := temporal_core.worker_close(&worker.core)
		temporal_core.error_free(&close_error)
		return {}, result
	}
	worker.lifecycle = .Running
	defer { worker.lifecycle = .Stopped }

	if poll_activities && !poll_workflows {
		stats.activity_tasks_completed,
		stats.activity_cancellations_received,
		error = run_activity_loop(worker, options.max_activity_tasks)
		return
	}
	if poll_workflows && !poll_activities {
		return run_workflow_loop(worker, options)
	}

	activity_state := Activity_Thread_State{
		worker = worker,
		max_tasks = options.max_activity_tasks,
	}
	workflow_state := Workflow_Thread_State{worker = worker, options = options}
	workflow_thread := thread.create_and_start_with_poly_data(&workflow_state, workflow_thread_main)
	if workflow_thread == nil {
		return {}, error_from_string("could not start Workflow poll thread")
	}
	activity_thread := thread.create_and_start_with_poly_data(&activity_state, activity_thread_main)
	if activity_thread == nil {
		temporal_core.worker_initiate_shutdown(&worker.core)
		thread.join(workflow_thread)
		thread.destroy(workflow_thread)
		error_free(&workflow_state.error)
		return {}, error_from_string("could not start Activity poll thread")
	}
	thread.join(workflow_thread)
	// Bounded Workflow runs are an example/test facility. Once that target has
	// evicted, stop the otherwise-unbounded Activity poll so both loops can join.
	if options.max_workflow_executions > 0 && workflow_state.error.message == nil {
		temporal_core.worker_initiate_shutdown(&worker.core)
	}
	thread.join(activity_thread)
	thread.destroy(workflow_thread)
	thread.destroy(activity_thread)

	stats = workflow_state.stats
	stats.activity_tasks_completed = activity_state.completed
	stats.activity_cancellations_received = activity_state.cancellations
	if workflow_state.error.message != nil {
		error = workflow_state.error
		error_free(&activity_state.error)
		return
	}
	if activity_state.error.message != nil {
		error = activity_state.error
		return
	}
	return
}

request_shutdown :: proc(worker: ^Worker) {
	if worker == nil || worker.lifecycle != .Running {
		return
	}
	temporal_core.worker_initiate_shutdown(&worker.core)
}

close :: proc(worker: ^Worker) -> Error {
	if worker == nil {
		return {}
	}
	if worker.lifecycle == .Closed {
		return {}
	}
	if worker.lifecycle == .Running {
		return error_from_string("worker cannot be closed while worker.run is active")
	}
	core_error := temporal_core.worker_close(&worker.core)
	for registration in worker.activity_registrations {
		delete(registration.name)
	}
	for &registration in worker.workflow_registrations {
		workflow.fiber_runtime_close(&registration.runtime)
		delete(registration.name)
	}
	for workflow_run in worker.workflow_runs {
		delete(workflow_run.run_id)
	}
	delete(worker.activity_registrations)
	delete(worker.workflow_registrations)
	delete(worker.workflow_runs)
	delete(worker.namespace)
	delete(worker.task_queue)
	worker.activity_registrations = nil
	worker.workflow_registrations = nil
	worker.workflow_runs = nil
	worker.client = nil
	worker.namespace = ""
	worker.task_queue = ""
	worker.lifecycle = .Closed
	worker.max_concurrent_activities = 0
	return error_from_core(&core_error)
}

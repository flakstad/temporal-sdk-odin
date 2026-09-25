// Package worker is the first public vertical slice. It supports synchronous
// one-string/one-string Activities while the API shape is still experimental.
package worker

import "core:bytes"
import "core:fmt"
import "core:mem/virtual"
import "core:strings"
import activity "../activity"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"

String_Activity_Proc :: #type proc(ctx: ^activity.Context, input: string) -> string

Registration :: struct {
	name:     string,
	callback: String_Activity_Proc,
}

Worker :: struct {
	core:          temporal_core.Worker,
	registrations: [dynamic]Registration,
	running:       bool,
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

create :: proc(client: ^temporal_core.Client, task_queue: string, namespace := "default") -> (worker: Worker, error: Error) {
	core_worker, core_error := temporal_core.worker_create(client, task_queue, namespace)
	if core_error.message != nil {
		return {}, error_from_core(&core_error)
	}
	worker.core = core_worker
	worker.registrations = make([dynamic]Registration)
	return
}

register_activity :: proc(worker: ^Worker, name: string, callback: String_Activity_Proc) -> Error {
	if worker == nil || worker.core.handle == nil {
		return error_from_string("worker is not initialized")
	}
	if worker.running {
		return error_from_string("activities cannot be registered after worker.run starts")
	}
	if len(name) == 0 || callback == nil {
		return error_from_string("activity name and callback are required")
	}
	for registration in worker.registrations {
		if registration.name == name {
			return error_from_string("activity is already registered")
		}
	}
	owned_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		return error_from_string("could not allocate activity name")
	}
	append(&worker.registrations, Registration{name = owned_name, callback = callback})
	return {}
}

find_activity :: proc(worker: ^Worker, name: string) -> (String_Activity_Proc, bool) {
	for registration in worker.registrations {
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
		activity_id = task.activity_id,
		attempt = task.attempt,
	}
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

run :: proc(worker: ^Worker, max_tasks := 0) -> Error {
	if worker == nil || worker.core.handle == nil {
		return error_from_string("worker is not initialized")
	}
	if worker.running {
		return error_from_string("worker is already running")
	}
	if len(worker.registrations) == 0 {
		return error_from_string("worker has no registered activities")
	}
	worker.running = true
	defer { worker.running = false }

	validate_error := temporal_core.worker_validate(&worker.core)
	if validate_error.message != nil {
		return error_from_core(&validate_error)
	}

	completed := 0
	for max_tasks <= 0 || completed < max_tasks {
		poll := temporal_core.worker_poll_activity(&worker.core)
		if poll.error.message != nil {
			return error_from_core(&poll.error)
		}
		if poll.shutdown {
			break
		}

		task, decode_error := proto.decode_activity_task(poll.protobuf)
		if decode_error != .None {
			delete(poll.protobuf)
			return error_from_string("Core returned a malformed ActivityTask protobuf")
		}
		if task.kind == .Cancel {
			// Concurrent cancellation is intentionally deferred. The current worker
			// executes one synchronous task at a time and cannot have an in-flight
			// task to cancel while it is polling.
			delete(poll.protobuf)
			continue
		}

		completion, execute_error := execute_start(worker, &task)
		if execute_error.message != nil {
			delete(poll.protobuf)
			return execute_error
		}
		complete_error := temporal_core.worker_complete_activity(&worker.core, completion[:])
		delete(completion)
		delete(poll.protobuf)
		if complete_error.message != nil {
			return error_from_core(&complete_error)
		}
		completed += 1
	}
	return {}
}

close :: proc(worker: ^Worker) -> Error {
	if worker == nil {
		return {}
	}
	core_error := temporal_core.worker_close(&worker.core)
	for registration in worker.registrations {
		delete(registration.name)
	}
	delete(worker.registrations)
	worker.registrations = nil
	worker.running = false
	return error_from_core(&core_error)
}

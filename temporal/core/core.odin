// Package core owns the blocking lifecycle wrapper around the raw C ABI.
package core

import "core:c"
import "core:bytes"
import "core:sync"
import raw "./raw"

Error :: struct {
	message:     []u8,
	status_code: u32,
	details:     []u8,
}

Runtime :: struct {
	handle: ^raw.Runtime,
}

Client :: struct {
	handle:  ^raw.Connection,
	runtime: ^Runtime,
}

Worker :: struct {
	handle:                    ^raw.Worker,
	runtime:                   ^Runtime,
	workflows_enabled:         bool,
	remote_activities_enabled: bool,
}

Get_System_Info_Result :: struct {
	protobuf: []u8,
	error:    Error,
}

byte_ref :: proc(value: string) -> raw.Byte_Array_Ref {
	return raw.Byte_Array_Ref{
		data = raw_data(value),
		size = c.size_t(len(value)),
	}
}

bytes_ref :: proc(value: []u8) -> raw.Byte_Array_Ref {
	if len(value) == 0 {
		return {}
	}
	return raw.Byte_Array_Ref{
		data = raw_data(value),
		size = c.size_t(len(value)),
	}
}

copy_core_bytes :: proc(value: ^raw.Byte_Array) -> []u8 {
	if value == nil || value.size == 0 {
		return nil
	}
	result := make([]u8, int(value.size))
	copy(result, ([^]u8)(value.data)[:int(value.size)])
	return result
}

error_free :: proc(error: ^Error) {
	if error == nil {
		return
	}
	delete(error.message)
	delete(error.details)
	error.message = nil
	error.details = nil
	error.status_code = 0
}

runtime_create :: proc() -> (runtime: Runtime, error: Error) {
	options := raw.Runtime_Options{}
	result := raw.runtime_new(&options)
	if result.fail != nil {
		error.message = copy_core_bytes(result.fail)
		raw.byte_array_free(result.runtime, result.fail)
		raw.runtime_free(result.runtime)
		return
	}
	runtime.handle = result.runtime
	return
}

runtime_close :: proc(runtime: ^Runtime) {
	if runtime == nil || runtime.handle == nil {
		return
	}
	raw.runtime_free(runtime.handle)
	runtime.handle = nil
}

Connect_State :: struct {
	done:    sync.One_Shot_Event,
	success: ^raw.Connection,
	fail:    ^raw.Byte_Array,
}

connect_callback :: proc "c" (user_data: rawptr, success: ^raw.Connection, fail: ^raw.Byte_Array) {
	state := cast(^Connect_State)user_data
	state.success = success
	state.fail = fail
	sync.one_shot_event_signal(&state.done)
}

wait_until :: proc(done: ^sync.One_Shot_Event) {
	sync.one_shot_event_wait(done)
}

client_connect :: proc(runtime: ^Runtime, target_url := "http://localhost:7233") -> (client: Client, error: Error) {
	if runtime == nil || runtime.handle == nil {
		error.message = bytes.clone(transmute([]u8)string("runtime is not initialized"))
		return
	}

	options := raw.Connection_Options{
		target_url     = byte_ref(target_url),
		client_name    = byte_ref("temporal-sdk-odin"),
		client_version = byte_ref("0.0.1-experimental"),
		identity       = byte_ref("temporal-sdk-odin@local"),
		grpc_compression = .Gzip,
	}
	state := Connect_State{}
	raw.client_connect(runtime.handle, &options, &state, connect_callback)
	wait_until(&state.done)

	if state.fail != nil {
		error.message = copy_core_bytes(state.fail)
		raw.byte_array_free(runtime.handle, state.fail)
		return
	}
	client.handle = state.success
	client.runtime = runtime
	return
}

client_close :: proc(client: ^Client) {
	if client == nil || client.handle == nil {
		return
	}
	raw.client_free(client.handle)
	client.handle = nil
	client.runtime = nil
}

fixed_slot_supplier :: proc(count: uintptr) -> raw.Slot_Supplier {
	return raw.Slot_Supplier{
		tag = .Fixed_Size,
		fixed_size = raw.Fixed_Size_Slot_Supplier{num_slots = count},
	}
}

worker_create :: proc(
	client: ^Client,
	task_queue: string,
	namespace := "default",
	enable_workflows := false,
	enable_remote_activities := true,
	max_activity_slots := 10,
) -> (worker: Worker, error: Error) {
	if client == nil || client.handle == nil || client.runtime == nil {
		error.message = bytes.clone(transmute([]u8)string("client is not initialized"))
		return
	}
	if len(task_queue) == 0 {
		error.message = bytes.clone(transmute([]u8)string("task queue must not be empty"))
		return
	}
	if max_activity_slots <= 0 {
		error.message = bytes.clone(transmute([]u8)string("max activity slots must be positive"))
		return
	}

	options := raw.Worker_Options{
		namespace = byte_ref(namespace),
		task_queue = byte_ref(task_queue),
		versioning_strategy = raw.Worker_Versioning_Strategy{
			tag = .None,
			none = raw.Worker_Versioning_None{build_id = byte_ref("temporal-sdk-odin-experimental")},
		},
		tuner = raw.Tuner_Holder{
			workflow_slot_supplier       = fixed_slot_supplier(1),
			activity_slot_supplier       = fixed_slot_supplier(uintptr(max_activity_slots)),
			local_activity_slot_supplier = fixed_slot_supplier(1),
			nexus_task_slot_supplier     = fixed_slot_supplier(1),
		},
		task_types = raw.Worker_Task_Types{
			enable_workflows = enable_workflows,
			enable_remote_activities = enable_remote_activities,
		},
		max_heartbeat_throttle_interval_ms     = 60_000,
		default_heartbeat_throttle_interval_ms = 1_000,
		graceful_shutdown_period_ms            = 5_000,
		nonsticky_to_sticky_poll_ratio         = 0.2,
	}
	result := raw.worker_new(client.handle, &options)
	if result.fail != nil {
		error.message = copy_core_bytes(result.fail)
		raw.byte_array_free(client.runtime.handle, result.fail)
		return
	}
	worker.handle = result.worker
	worker.runtime = client.runtime
	worker.workflows_enabled = enable_workflows
	worker.remote_activities_enabled = enable_remote_activities
	return
}

Worker_Callback_State :: struct {
	done: sync.One_Shot_Event,
	fail: ^raw.Byte_Array,
}

worker_callback :: proc "c" (user_data: rawptr, fail: ^raw.Byte_Array) {
	state := cast(^Worker_Callback_State)user_data
	state.fail = fail
	sync.one_shot_event_signal(&state.done)
}

worker_wait_call :: proc(worker: ^Worker, invoke: proc(^raw.Worker, rawptr, raw.Worker_Callback)) -> Error {
	state := Worker_Callback_State{}
	invoke(worker.handle, &state, worker_callback)
	wait_until(&state.done)
	if state.fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(state.fail)}
	raw.byte_array_free(worker.runtime.handle, state.fail)
	return error
}

worker_validate_invoke :: proc(handle: ^raw.Worker, user_data: rawptr, callback: raw.Worker_Callback) {
	raw.worker_validate(handle, user_data, callback)
}

worker_validate :: proc(worker: ^Worker) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil {
		return Error{message = bytes.clone(transmute([]u8)string("worker is not initialized"))}
	}
	return worker_wait_call(worker, worker_validate_invoke)
}

Poll_Result :: struct {
	protobuf: []u8,
	error:    Error,
	shutdown: bool,
}

Worker_Poll_State :: struct {
	done:    sync.One_Shot_Event,
	success: ^raw.Byte_Array,
	fail:    ^raw.Byte_Array,
}

worker_poll_callback :: proc "c" (user_data: rawptr, success, fail: ^raw.Byte_Array) {
	state := cast(^Worker_Poll_State)user_data
	state.success = success
	state.fail = fail
	sync.one_shot_event_signal(&state.done)
}

worker_poll_activity :: proc(worker: ^Worker) -> Poll_Result {
	result := Poll_Result{}
	if worker == nil || worker.handle == nil || worker.runtime == nil {
		result.error.message = bytes.clone(transmute([]u8)string("worker is not initialized"))
		return result
	}
	state := Worker_Poll_State{}
	raw.worker_poll_activity_task(worker.handle, &state, worker_poll_callback)
	wait_until(&state.done)
	if state.fail != nil {
		result.error.message = copy_core_bytes(state.fail)
		raw.byte_array_free(worker.runtime.handle, state.fail)
		return result
	}
	if state.success == nil {
		result.shutdown = true
		return result
	}
	result.protobuf = copy_core_bytes(state.success)
	raw.byte_array_free(worker.runtime.handle, state.success)
	return result
}

worker_poll_workflow :: proc(worker: ^Worker) -> Poll_Result {
	result := Poll_Result{}
	if worker == nil || worker.handle == nil || worker.runtime == nil || !worker.workflows_enabled {
		result.error.message = bytes.clone(transmute([]u8)string("workflow polling is not enabled"))
		return result
	}
	state := Worker_Poll_State{}
	raw.worker_poll_workflow_activation(worker.handle, &state, worker_poll_callback)
	wait_until(&state.done)
	if state.fail != nil {
		result.error.message = copy_core_bytes(state.fail)
		raw.byte_array_free(worker.runtime.handle, state.fail)
		return result
	}
	if state.success == nil {
		result.shutdown = true
		return result
	}
	result.protobuf = copy_core_bytes(state.success)
	raw.byte_array_free(worker.runtime.handle, state.success)
	return result
}

Complete_State :: struct {
	done: sync.One_Shot_Event,
	fail: ^raw.Byte_Array,
}

complete_callback :: proc "c" (user_data: rawptr, fail: ^raw.Byte_Array) {
	state := cast(^Complete_State)user_data
	state.fail = fail
	sync.one_shot_event_signal(&state.done)
}

worker_complete_activity :: proc(worker: ^Worker, completion: []u8) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil {
		return Error{message = bytes.clone(transmute([]u8)string("worker is not initialized"))}
	}
	state := Complete_State{}
	raw.worker_complete_activity_task(worker.handle, bytes_ref(completion), &state, complete_callback)
	wait_until(&state.done)
	if state.fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(state.fail)}
	raw.byte_array_free(worker.runtime.handle, state.fail)
	return error
}

worker_record_activity_heartbeat :: proc(worker: ^Worker, heartbeat: []u8) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil {
		return Error{message = bytes.clone(transmute([]u8)string("worker is not initialized"))}
	}
	fail := raw.worker_record_activity_heartbeat(worker.handle, bytes_ref(heartbeat))
	if fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(fail)}
	raw.byte_array_free(worker.runtime.handle, fail)
	return error
}

worker_complete_workflow :: proc(worker: ^Worker, completion: []u8) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil || !worker.workflows_enabled {
		return Error{message = bytes.clone(transmute([]u8)string("workflow completion is not enabled"))}
	}
	state := Complete_State{}
	raw.worker_complete_workflow_activation(worker.handle, bytes_ref(completion), &state, complete_callback)
	wait_until(&state.done)
	if state.fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(state.fail)}
	raw.byte_array_free(worker.runtime.handle, state.fail)
	return error
}

worker_request_workflow_eviction :: proc(worker: ^Worker, run_id: string) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil || !worker.workflows_enabled {
		return Error{message = bytes.clone(transmute([]u8)string("workflow eviction is not enabled"))}
	}
	if len(run_id) == 0 {
		return Error{message = bytes.clone(transmute([]u8)string("workflow run id must not be empty"))}
	}
	raw.worker_request_workflow_eviction(worker.handle, byte_ref(run_id))
	return {}
}

worker_initiate_shutdown :: proc(worker: ^Worker) {
	if worker == nil || worker.handle == nil {
		return
	}
	raw.worker_initiate_shutdown(worker.handle)
}

finalize_shutdown_invoke :: proc(handle: ^raw.Worker, user_data: rawptr, callback: raw.Worker_Callback) {
	raw.worker_finalize_shutdown(handle, user_data, callback)
}

worker_close :: proc(worker: ^Worker) -> Error {
	if worker == nil || worker.handle == nil {
		return {}
	}
	worker_initiate_shutdown(worker)
	// Core shutdown completes only after every enabled poll stream observes
	// PollError::ShutDown. The synchronous wrapper has no outstanding polls, so
	// drain each enabled stream here before finalize_shutdown.
	error := Error{}
	if worker.workflows_enabled {
		poll := worker_poll_workflow(worker)
		if poll.protobuf != nil {
			delete(poll.protobuf)
		}
		error = poll.error
		if error.message == nil && !poll.shutdown {
			error.message = bytes.clone(transmute([]u8)string("workflow shutdown poll returned an unexpected activation"))
		}
	}
	if worker.remote_activities_enabled {
		poll := worker_poll_activity(worker)
		if poll.protobuf != nil {
			delete(poll.protobuf)
		}
		if error.message == nil {
			error = poll.error
		} else {
			error_free(&poll.error)
		}
		if error.message == nil && !poll.shutdown {
			error.message = bytes.clone(transmute([]u8)string("activity shutdown poll returned an unexpected task"))
		}
	}
	finalize_error := worker_wait_call(worker, finalize_shutdown_invoke)
	if error.message == nil {
		error = finalize_error
	} else {
		error_free(&finalize_error)
	}
	raw.worker_free(worker.handle)
	worker.handle = nil
	worker.runtime = nil
	worker.workflows_enabled = false
	worker.remote_activities_enabled = false
	return error
}

Rpc_State :: struct {
	done:            sync.One_Shot_Event,
	success:         ^raw.Byte_Array,
	status_code:     u32,
	failure_message: ^raw.Byte_Array,
	failure_details: ^raw.Byte_Array,
}

Rpc_Result :: struct {
	protobuf: []u8,
	error:    Error,
}

rpc_callback :: proc "c" (
	user_data: rawptr,
	success: ^raw.Byte_Array,
	status_code: u32,
	failure_message: ^raw.Byte_Array,
	failure_details: ^raw.Byte_Array,
) {
	state := cast(^Rpc_State)user_data
	state.success = success
	state.status_code = status_code
	state.failure_message = failure_message
	state.failure_details = failure_details
	sync.one_shot_event_signal(&state.done)
}

client_get_system_info :: proc(client: ^Client) -> Get_System_Info_Result {
	rpc_result := client_rpc_call(
		client,
		.Workflow,
		"GetSystemInfo",
		nil,
		retry = true,
		timeout_millis = 10_000,
	)
	return {protobuf = rpc_result.protobuf, error = rpc_result.error}
}

client_rpc_call :: proc(
	client: ^Client,
	service: raw.Rpc_Service,
	rpc: string,
	request: []u8,
	retry := true,
	timeout_millis: u32 = 10_000,
) -> Rpc_Result {
	result := Rpc_Result{}
	if client == nil || client.handle == nil || client.runtime == nil {
		result.error.message = bytes.clone(transmute([]u8)string("client is not initialized"))
		return result
	}

	options := raw.Rpc_Call_Options{
		service = service,
		rpc = byte_ref(rpc),
		req = bytes_ref(request),
		retry = retry,
		timeout_millis = timeout_millis,
	}
	state := Rpc_State{}
	raw.client_rpc_call(client.handle, &options, &state, rpc_callback)
	wait_until(&state.done)

	runtime := client.runtime.handle
	if state.failure_message != nil {
		result.error.message = copy_core_bytes(state.failure_message)
		result.error.status_code = state.status_code
		raw.byte_array_free(runtime, state.failure_message)
		if state.failure_details != nil {
			result.error.details = copy_core_bytes(state.failure_details)
			raw.byte_array_free(runtime, state.failure_details)
		}
		return result
	}
	if state.success == nil {
		result.error.message = bytes.clone(transmute([]u8)string("Core RPC returned neither success nor failure"))
		return result
	}

	result.protobuf = copy_core_bytes(state.success)
	raw.byte_array_free(runtime, state.success)
	return result
}

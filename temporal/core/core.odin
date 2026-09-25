// Package core owns the minimal safe-ish lifecycle wrapper used by Milestone 1.
package core

import "core:c"
import "core:bytes"
import "core:sync"
import raw "./raw"

Error :: struct {
	message: []u8,
}

Runtime :: struct {
	handle: ^raw.Runtime,
}

Client :: struct {
	handle:  ^raw.Connection,
	runtime: ^Runtime,
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

copy_core_bytes :: proc(value: ^raw.Byte_Array) -> []u8 {
	if value == nil || value.size == 0 {
		return nil
	}
	result := make([]u8, int(value.size))
	copy(result, ([^]u8)(value.data)[:int(value.size)])
	return result
}

error_free :: proc(error: ^Error) {
	if error == nil || error.message == nil {
		return
	}
	delete(error.message)
	error.message = nil
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

Rpc_State :: struct {
	done:            sync.One_Shot_Event,
	success:         ^raw.Byte_Array,
	status_code:     u32,
	failure_message: ^raw.Byte_Array,
	failure_details: ^raw.Byte_Array,
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
	result := Get_System_Info_Result{}
	if client == nil || client.handle == nil || client.runtime == nil {
		result.error.message = bytes.clone(transmute([]u8)string("client is not initialized"))
		return result
	}

	options := raw.Rpc_Call_Options{
		service = .Workflow,
		rpc = byte_ref("GetSystemInfo"),
		// GetSystemInfoRequest is an empty protobuf message.
		req = raw.Byte_Array_Ref{},
		retry = true,
		timeout_millis = 10_000,
	}
	state := Rpc_State{}
	raw.client_rpc_call(client.handle, &options, &state, rpc_callback)
	wait_until(&state.done)

	runtime := client.runtime.handle
	if state.failure_message != nil {
		result.error.message = copy_core_bytes(state.failure_message)
		raw.byte_array_free(runtime, state.failure_message)
		if state.failure_details != nil {
			raw.byte_array_free(runtime, state.failure_details)
		}
		return result
	}

	result.protobuf = copy_core_bytes(state.success)
	raw.byte_array_free(runtime, state.success)
	return result
}

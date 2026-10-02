package activity_isolate

import "core:bytes"
import "core:os"
import "core:mem/virtual"
import "core:sync"
import "core:thread"
import activity ".."
import codec "../../codec"
import proto "../../core/proto"
import worker "../../worker"

Host_Options :: struct {
	activity_name: string,
	callback_data: rawptr,
	invoke:        worker.Activity_Invoke_Proc,
	namespace:     string,
	task_queue:    string,
	maximum_payload_bytes: u32,
}

host_options :: proc(
	activity_name: string,
	callback_data: rawptr,
	invoke: worker.Activity_Invoke_Proc,
) -> Host_Options {
	return {
		activity_name = activity_name,
		callback_data = callback_data,
		invoke = invoke,
		namespace = "default",
		task_queue = "external-activity",
		maximum_payload_bytes = DEFAULT_MAXIMUM_PAYLOAD_BYTES,
	}
}

Host_Control_State :: struct {
	input:      ^os.File,
	output:     ^os.File,
	request_id: u64,
	task_token: []u8,
	maximum_payload_bytes: u32,
	protocol_failed: bool,
	ctx:        ^activity.Context,
	reader:     ^thread.Thread,
	heartbeat_ack: sync.Sema,
	heartbeat_status: int,
}

host_control_fail :: proc(state: ^Host_Control_State) {
	if state == nil {
		return
	}
	sync.atomic_store_explicit(&state.protocol_failed, true, .Release)
	sync.sema_post(&state.heartbeat_ack)
}

host_apply_cancel :: proc(state: ^Host_Control_State, payload: []u8) -> bool {
	if state == nil || state.ctx == nil {
		return false
	}
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return false
	}
	defer virtual.arena_destroy(&arena)
	previous := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	task, decode_error := proto.decode_activity_task(payload)
	context.allocator = previous
	if decode_error != .None || task.kind != .Cancel ||
	   !bytes.equal(task.task_token, state.task_token) {
		return false
	}
	activity.context_request_cancellation(
		state.ctx,
		worker.activity_cancel_reason(task.cancel_reason),
		worker.activity_cancel_details(task.cancel_details),
	)
	return true
}

host_control_reader :: proc(state: ^Host_Control_State) {
	for {
		response, read_error := read(state.input, state.maximum_payload_bytes)
		if read_error.kind != .None {
			owned_frame_free(&response)
			host_control_fail(state)
			return
		}
		if response.frame.request_id != state.request_id {
			owned_frame_free(&response)
			host_control_fail(state)
			return
		}
		#partial switch response.frame.kind {
		case .Heartbeat_Ack:
			if len(response.frame.payload) != 1 ||
			   response.frame.payload[0] > u8(activity.Heartbeat_Status.Core_Error) {
				owned_frame_free(&response)
				host_control_fail(state)
				return
			}
			sync.atomic_store_explicit(
				&state.heartbeat_status,
				int(response.frame.payload[0]),
				.Release,
			)
			owned_frame_free(&response)
			sync.sema_post(&state.heartbeat_ack)
		case .Cancel:
			cancel_ok := host_apply_cancel(state, response.frame.payload)
			owned_frame_free(&response)
			if !cancel_ok {
				host_control_fail(state)
				return
			}
		case .Control_Stop:
			owned_frame_free(&response)
			return
		case:
			owned_frame_free(&response)
			host_control_fail(state)
			return
		}
	}
}

host_context_ready :: proc(user_data: rawptr, ctx: ^activity.Context) {
	state := cast(^Host_Control_State)user_data
	if state == nil || ctx == nil {
		return
	}
	state.ctx = ctx
	state.reader = thread.create_and_start_with_poly_data(state, host_control_reader)
	if state.reader == nil {
		host_control_fail(state)
	}
}

host_context_done :: proc(user_data: rawptr, _: ^activity.Context) {
	state := cast(^Host_Control_State)user_data
	if state == nil || state.reader == nil {
		return
	}
	done_payload := [1]u8{0}
	write_error := write(state.output, {
		kind = .Control_Done,
		request_id = state.request_id,
		payload = done_payload[:],
	}, state.maximum_payload_bytes)
	if write_error.kind != .None {
		os.exit(1)
	}
	thread.join(state.reader)
	thread.destroy(state.reader)
	state.reader = nil
}

host_heartbeat_recorder :: proc(
	user_data: rawptr,
	details: codec.Payload_View,
) -> activity.Heartbeat_Status {
	state := cast(^Host_Control_State)user_data
	if state == nil || sync.atomic_load_explicit(&state.protocol_failed, .Acquire) {
		return .Core_Error
	}
	heartbeat := proto.encode_activity_heartbeat_payload(state.task_token, details)
	defer delete(heartbeat)
	write_error := write(state.output, {
		kind = .Heartbeat,
		request_id = state.request_id,
		payload = heartbeat[:],
	}, state.maximum_payload_bytes)
	if write_error.kind != .None {
		host_control_fail(state)
		return .Core_Error
	}
	sync.sema_wait(&state.heartbeat_ack)
	if sync.atomic_load_explicit(&state.protocol_failed, .Acquire) {
		return .Core_Error
	}
	status_value := sync.atomic_load_explicit(&state.heartbeat_status, .Acquire)
	return activity.Heartbeat_Status(status_value)
}

// host_run executes exactly one Activity task and exits. A fresh process per
// invocation makes fatal Odin panics/native faults containable; Temporal's
// retry policy remains authoritative in the Core-owning parent.
host_run :: proc(options: Host_Options) -> Error {
	request, read_error := read(os.stdin, options.maximum_payload_bytes)
	defer owned_frame_free(&request)
	if read_error.kind != .None {
		return read_error
	}
	if request.frame.kind != .Execute {
		return {kind = .Invalid_Argument}
	}
	task_token, token_ok := decode_task_token(request.frame.payload)
	if !token_ok {
		return write(os.stdout, {
			kind = .Executor_Error,
			request_id = request.frame.request_id,
			payload = transmute([]u8)string("Activity executor received a malformed task"),
		}, options.maximum_payload_bytes)
	}
	defer delete(task_token)
	control_state := Host_Control_State{
		input = os.stdin,
		output = os.stdout,
		request_id = request.frame.request_id,
		task_token = task_token,
		maximum_payload_bytes = options.maximum_payload_bytes,
	}
	processed := worker.execute_activity_task_callback(
		options.activity_name,
		options.callback_data,
		options.invoke,
		request.frame.payload,
		options.namespace,
		options.task_queue,
		&control_state,
		host_heartbeat_recorder,
		&control_state,
		host_context_ready,
		host_context_done,
	)
	defer worker.activity_task_process_result_free(&processed)
	if sync.atomic_load_explicit(&control_state.protocol_failed, .Acquire) {
		return {kind = .IO}
	}
	if processed.error.message != nil {
		return write(os.stdout, {
			kind = .Executor_Error,
			request_id = request.frame.request_id,
			payload = processed.error.message,
		}, options.maximum_payload_bytes)
	}
	return write(os.stdout, {
		kind = .Completion,
		request_id = request.frame.request_id,
		payload = processed.completion[:],
	}, options.maximum_payload_bytes)
}

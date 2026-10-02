package activity_isolate

import "core:bytes"
import "core:os"
import "core:mem/virtual"
import "core:strings"
import "core:sync"
import "core:thread"
import activity ".."
import codec "../../codec"
import proto "../../core/proto"
import worker "../../worker"

Host_Activity :: struct {
	name:          string,
	callback_data: rawptr,
	invoke:        worker.Activity_Invoke_Proc,
}

Host_Options :: struct {
	// activities is the multi-Activity manifest used by persistent or shared
	// executor binaries. The singular fields remain as a source-compatible
	// convenience for one-Activity hosts.
	activities:    []Host_Activity,
	activity_name: string,
	callback_data: rawptr,
	invoke:        worker.Activity_Invoke_Proc,
	namespace:     string,
	task_queue:    string,
	maximum_payload_bytes: u32,
}

host_activity :: proc(
	name: string,
	callback_data: rawptr,
	invoke: worker.Activity_Invoke_Proc,
) -> Host_Activity {
	return {name = name, callback_data = callback_data, invoke = invoke}
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

host_options_many :: proc(activities: []Host_Activity) -> Host_Options {
	return {
		activities = activities,
		namespace = "default",
		task_queue = "external-activity",
		maximum_payload_bytes = DEFAULT_MAXIMUM_PAYLOAD_BYTES,
	}
}

host_options_valid :: proc(options: Host_Options) -> bool {
	if options.maximum_payload_bytes == 0 || len(options.namespace) == 0 ||
	   len(options.task_queue) == 0 {
		return false
	}
	if len(options.activities) == 0 {
		return len(options.activity_name) > 0 && options.callback_data != nil &&
		       options.invoke != nil
	}
	for activity_registration, index in options.activities {
		if len(activity_registration.name) == 0 || activity_registration.callback_data == nil ||
		   activity_registration.invoke == nil {
			return false
		}
		for previous in options.activities[:index] {
			if previous.name == activity_registration.name {
				return false
			}
		}
	}
	return true
}

host_activity_find :: proc(options: Host_Options, name: string) -> (Host_Activity, bool) {
	if len(options.activities) == 0 {
		if options.activity_name == name {
			return {
				name = options.activity_name,
				callback_data = options.callback_data,
				invoke = options.invoke,
			}, true
		}
		return {}, false
	}
	for activity_registration in options.activities {
		if activity_registration.name == name {
			return activity_registration, true
		}
	}
	return {}, false
}

decode_activity_type :: proc(task: []u8) -> (activity_type: string, ok: bool) {
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return
	}
	defer virtual.arena_destroy(&arena)
	previous := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	decoded, decode_error := proto.decode_activity_task(task)
	context.allocator = previous
	if decode_error != .None || decoded.kind != .Start || len(decoded.activity_type) == 0 {
		return
	}
	owned, allocation_error := strings.clone(decoded.activity_type)
	if allocation_error != nil {
		return
	}
	return owned, true
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

host_execute_request :: proc(options: Host_Options, request: ^Owned_Frame) -> Error {
	if request == nil {
		return {kind = .Invalid_Argument}
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
	activity_type, activity_type_ok := decode_activity_type(request.frame.payload)
	if !activity_type_ok {
		return write(os.stdout, {
			kind = .Executor_Error,
			request_id = request.frame.request_id,
			payload = transmute([]u8)string("Activity executor received a malformed start task"),
		}, options.maximum_payload_bytes)
	}
	defer delete(activity_type)
	activity_registration, activity_found := host_activity_find(options, activity_type)
	if !activity_found {
		return write(os.stdout, {
			kind = .Executor_Error,
			request_id = request.frame.request_id,
			payload = transmute([]u8)string("Activity executor has no callback for the requested type"),
		}, options.maximum_payload_bytes)
	}
	control_state := Host_Control_State{
		input = os.stdin,
		output = os.stdout,
		request_id = request.frame.request_id,
		task_token = task_token,
		maximum_payload_bytes = options.maximum_payload_bytes,
	}
	processed := worker.execute_activity_task_callback(
		activity_registration.name,
		activity_registration.callback_data,
		activity_registration.invoke,
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

// host_run executes exactly one Activity task and exits. It remains the
// lowest-cost crash-containment mode and preserves the original one-shot host
// API.
host_run :: proc(options: Host_Options) -> Error {
	if !host_options_valid(options) {
		return {kind = .Invalid_Argument}
	}
	request, read_error := read(os.stdin, options.maximum_payload_bytes)
	defer owned_frame_free(&request)
	if read_error.kind != .None {
		return read_error
	}
	return host_execute_request(options, &request)
}

// host_run_loop executes Activities serially until the parent sends Shutdown.
// A callback panic still terminates the process; the parent retires that child
// and Temporal remains authoritative for retry. Each Execute has a distinct
// request ID so stale control/completion frames cannot cross invocations.
host_run_loop :: proc(options: Host_Options) -> Error {
	if !host_options_valid(options) {
		return {kind = .Invalid_Argument}
	}
	manifest, manifest_ok := host_options_manifest_encode(options)
	if !manifest_ok {
		return {kind = .Invalid_Argument}
	}
	defer delete(manifest)
	manifest_sent := false
	for {
		request, read_error := read(os.stdin, options.maximum_payload_bytes)
		if read_error.kind == .End_Of_Stream {
			owned_frame_free(&request)
			return {}
		}
		if read_error.kind != .None {
			owned_frame_free(&request)
			return read_error
		}
		if request.frame.kind == .Shutdown {
			ack_error := write(os.stdout, {
				kind = .Shutdown_Ack,
				request_id = request.frame.request_id,
				payload = transmute([]u8)string("ok"),
			}, options.maximum_payload_bytes)
			owned_frame_free(&request)
			return ack_error
		}
		if request.frame.kind == .Manifest_Request && !manifest_sent {
			manifest_error := write(os.stdout, {
				kind = .Manifest,
				request_id = request.frame.request_id,
				payload = manifest,
			}, options.maximum_payload_bytes)
			manifest_sent = manifest_error.kind == .None
			owned_frame_free(&request)
			if manifest_error.kind != .None {
				return manifest_error
			}
			continue
		}
		if !manifest_sent {
			owned_frame_free(&request)
			return {kind = .Invalid_Argument}
		}
		execute_error := host_execute_request(options, &request)
		owned_frame_free(&request)
		if execute_error.kind != .None {
			return execute_error
		}
	}
}

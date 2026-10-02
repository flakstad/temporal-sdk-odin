package activity_isolate

import "core:os"
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

Host_Heartbeat_State :: struct {
	input:      ^os.File,
	output:     ^os.File,
	request_id: u64,
	task_token: []u8,
	maximum_payload_bytes: u32,
	protocol_failed: bool,
}

host_heartbeat_recorder :: proc(
	user_data: rawptr,
	details: codec.Payload_View,
) -> activity.Heartbeat_Status {
	state := cast(^Host_Heartbeat_State)user_data
	if state == nil || state.protocol_failed {
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
		state.protocol_failed = true
		return .Core_Error
	}
	response, read_error := read(state.input, state.maximum_payload_bytes)
	defer owned_frame_free(&response)
	if read_error.kind != .None || response.frame.kind != .Heartbeat_Ack ||
	   response.frame.request_id != state.request_id || len(response.frame.payload) != 1 {
		state.protocol_failed = true
		return .Core_Error
	}
	status_value := response.frame.payload[0]
	if status_value > u8(activity.Heartbeat_Status.Core_Error) {
		state.protocol_failed = true
		return .Core_Error
	}
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
	heartbeat_state := Host_Heartbeat_State{
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
		&heartbeat_state,
		host_heartbeat_recorder,
	)
	defer worker.activity_task_process_result_free(&processed)
	if heartbeat_state.protocol_failed {
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

package activity_isolate

import "core:os"
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
	processed := worker.execute_activity_task_callback(
		options.activity_name,
		options.callback_data,
		options.invoke,
		request.frame.payload,
		options.namespace,
		options.task_queue,
	)
	defer worker.activity_task_process_result_free(&processed)
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

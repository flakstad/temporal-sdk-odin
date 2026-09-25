// Package activity contains values available only while running an Activity.
package activity

import "core:sync"
import codec "../codec"

Cancel_Reason :: enum u32 {
	None,
	Not_Found,
	Cancelled,
	Timed_Out,
	Worker_Shutdown,
	Paused,
	Reset,
}

Heartbeat_Status :: enum {
	Recorded,
	Cancellation_Requested,
	Invalid_Context,
	Encoding_Failed,
	Core_Error,
}

Heartbeat_Recorder :: #type proc(user_data: rawptr, details: codec.Payload_View) -> Heartbeat_Status

Payload_Result :: struct {
	payload:       codec.Payload,
	failure_message: string,
	non_retryable: bool,
}

payload_complete :: proc(payload: codec.Payload) -> Payload_Result {
	return {payload = payload}
}

payload_fail :: proc(message: string, non_retryable := true) -> Payload_Result {
	return {failure_message = message, non_retryable = non_retryable}
}

Context :: struct {
	activity_id: string,
	attempt:     u32,
	cancel_requested: bool,
	cancel_reason:    Cancel_Reason,
	heartbeat_user_data: rawptr,
	heartbeat_recorder:  Heartbeat_Recorder,
}

context_init :: proc(
	ctx: ^Context,
	activity_id: string,
	attempt: u32,
	heartbeat_user_data: rawptr,
	heartbeat_recorder: Heartbeat_Recorder,
) {
	ctx^ = Context{
		activity_id = activity_id,
		attempt = attempt,
		heartbeat_user_data = heartbeat_user_data,
		heartbeat_recorder = heartbeat_recorder,
	}
}

context_request_cancellation :: proc(ctx: ^Context, reason: Cancel_Reason) {
	if ctx == nil {
		return
	}
	sync.atomic_store_explicit(&ctx.cancel_reason, reason, .Relaxed)
	sync.atomic_store_explicit(&ctx.cancel_requested, true, .Release)
}

is_cancellation_requested :: proc(ctx: ^Context) -> bool {
	return ctx != nil && sync.atomic_load_explicit(&ctx.cancel_requested, .Acquire)
}

cancellation_reason :: proc(ctx: ^Context) -> Cancel_Reason {
	if !is_cancellation_requested(ctx) {
		return .None
	}
	return sync.atomic_load_explicit(&ctx.cancel_reason, .Relaxed)
}

activity_id :: proc(ctx: ^Context) -> string {
	if ctx == nil {
		return ""
	}
	return ctx.activity_id
}

attempt :: proc(ctx: ^Context) -> u32 {
	if ctx == nil {
		return 0
	}
	return ctx.attempt
}

heartbeat_payload :: proc(ctx: ^Context, details: codec.Payload_View) -> Heartbeat_Status {
	if ctx == nil || ctx.heartbeat_recorder == nil {
		return .Invalid_Context
	}
	if is_cancellation_requested(ctx) {
		return .Cancellation_Requested
	}
	status := ctx.heartbeat_recorder(ctx.heartbeat_user_data, details)
	if status == .Recorded && is_cancellation_requested(ctx) {
		return .Cancellation_Requested
	}
	return status
}

heartbeat_json :: proc(ctx: ^Context, details: $T) -> Heartbeat_Status {
	payload, encode_error := codec.encode_json(details)
	if encode_error != .None {
		return .Encoding_Failed
	}
	defer codec.payload_free(&payload)
	return heartbeat_payload(ctx, codec.payload_view(&payload))
}

heartbeat :: proc(ctx: ^Context, details: string) -> Heartbeat_Status {
	return heartbeat_json(ctx, details)
}

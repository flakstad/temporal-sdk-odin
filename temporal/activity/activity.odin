// Package activity contains values available only while running an Activity.
package activity

import "core:sync"
import "core:time"
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

Result_Status :: enum {
	Completed,
	Failed,
}

Application_Failure :: struct {
	message:          string,
	type:             string,
	non_retryable:    bool,
	details:          codec.Payload,
	has_details:      bool,
	next_retry_delay: time.Duration,
}

Result :: struct(T: typeid) {
	status:  Result_Status,
	value:   T,
	failure: Application_Failure,
}

result_success :: proc(value: $T) -> Result(T) {
	return {status = .Completed, value = value}
}

result_failure :: proc(
	$T: typeid,
	message: string,
	failure_type := "OdinActivityError",
	non_retryable := false,
	next_retry_delay: time.Duration = 0,
) -> Result(T) {
	return {
		status = .Failed,
		failure = {
			message = message,
			type = failure_type,
			non_retryable = non_retryable,
			next_retry_delay = next_retry_delay,
		},
	}
}

result_failure_json :: proc(
	$T: typeid,
	message: string,
	details: $Details,
	failure_type := "OdinActivityError",
	non_retryable := false,
	next_retry_delay: time.Duration = 0,
) -> Result(T) {
	payload, encode_error := codec.encode_json(details)
	if encode_error != .None {
		return result_failure(
			T,
			"could not encode Activity failure details as json/plain",
			failure_type = "OdinPayloadConversionError",
			non_retryable = true,
		)
	}
	return {
		status = .Failed,
		failure = {
			message = message,
			type = failure_type,
			non_retryable = non_retryable,
			details = payload,
			has_details = true,
			next_retry_delay = next_retry_delay,
		},
	}
}

application_failure_free :: proc(failure: ^Application_Failure) {
	if failure == nil {
		return
	}
	codec.payload_free(&failure.details)
	failure^ = {}
}

Payload_Result :: struct {
	status:  Result_Status,
	payload: codec.Payload,
	failure: Application_Failure,
}

Retry_Policy :: struct {
	initial_interval:          time.Duration,
	has_initial_interval:      bool,
	backoff_coefficient:       f64,
	maximum_interval:          time.Duration,
	has_maximum_interval:      bool,
	maximum_attempts:          i32,
	non_retryable_error_types: []string,
}

Priority :: struct {
	priority_key:    i32,
	fairness_key:    string,
	fairness_weight: f32,
}

// Info and all nested slices borrow their backing storage from Context. They
// are valid only for the duration of the Activity callback.
Info :: struct {
	activity_id:     string,
	activity_type:   string,
	activity_run_id: string,
	attempt:         u32,
	namespace:       string,
	task_queue:      string,
	task_token:      []u8,
	workflow_id:        string,
	workflow_namespace: string,
	workflow_run_id:    string,
	workflow_type:      string,
	is_local: bool,
	scheduled_time:                 time.Time,
	has_scheduled_time:             bool,
	current_attempt_scheduled_time: time.Time,
	has_current_attempt_scheduled_time: bool,
	started_time:     time.Time,
	has_started_time: bool,
	schedule_to_close_timeout:     time.Duration,
	has_schedule_to_close_timeout: bool,
	start_to_close_timeout:        time.Duration,
	has_start_to_close_timeout:    bool,
	heartbeat_timeout:     time.Duration,
	has_heartbeat_timeout: bool,
	deadline:     time.Time,
	has_deadline: bool,
	retry_policy:     Retry_Policy,
	has_retry_policy: bool,
	priority:     Priority,
	has_priority: bool,
	heartbeat_details: []codec.Payload_View,
}

payload_complete :: proc(payload: codec.Payload) -> Payload_Result {
	return {status = .Completed, payload = payload}
}

payload_fail :: proc(
	message: string,
	failure_type := "OdinActivityError",
	non_retryable := true,
	next_retry_delay: time.Duration = 0,
) -> Payload_Result {
	return {
		status = .Failed,
		failure = {
			message = message,
			type = failure_type,
			non_retryable = non_retryable,
			next_retry_delay = next_retry_delay,
		},
	}
}

Context :: struct {
	info_value: Info,
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
		info_value = Info{activity_id = activity_id, attempt = attempt},
		heartbeat_user_data = heartbeat_user_data,
		heartbeat_recorder = heartbeat_recorder,
	}
}

context_init_info :: proc(
	ctx: ^Context,
	activity_info: Info,
	heartbeat_user_data: rawptr,
	heartbeat_recorder: Heartbeat_Recorder,
) {
	if ctx == nil {
		return
	}
	ctx^ = Context{
		info_value = activity_info,
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
	return ctx.info_value.activity_id
}

attempt :: proc(ctx: ^Context) -> u32 {
	if ctx == nil {
		return 0
	}
	return ctx.info_value.attempt
}

info :: proc(ctx: ^Context) -> Info {
	if ctx == nil {
		return {}
	}
	return ctx.info_value
}

is_workflow_activity :: proc(ctx: ^Context) -> bool {
	return ctx != nil && len(ctx.info_value.workflow_id) > 0
}

deadline :: proc(ctx: ^Context) -> (value: time.Time, present: bool) {
	if ctx == nil || !ctx.info_value.has_deadline {
		return
	}
	return ctx.info_value.deadline, true
}

heartbeat_detail_payload :: proc(
	ctx: ^Context,
	index: int,
) -> (payload: codec.Payload_View, found: bool) {
	if ctx == nil || index < 0 || index >= len(ctx.info_value.heartbeat_details) {
		return
	}
	return ctx.info_value.heartbeat_details[index], true
}

heartbeat_detail_json :: proc(
	ctx: ^Context,
	index: int,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := heartbeat_detail_payload(ctx, index)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
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

// Package activity contains values available only while running an Activity.
package activity

import "core:sync"
import "core:time"
import codec "../codec"
import temporal_core "../core"

Cancel_Reason :: enum u32 {
	None,
	Not_Found,
	Cancelled,
	Timed_Out,
	Worker_Shutdown,
	Paused,
	Reset,
}

// Cancellation_Details surfaces every reason Core observed. More than one
// flag may be true even though cancellation_reason returns one primary reason.
Cancellation_Details :: struct {
	is_not_found:       bool,
	is_cancelled:       bool,
	is_paused:          bool,
	is_timed_out:       bool,
	is_worker_shutdown: bool,
	is_reset:           bool,
}

CANCELLATION_NOT_FOUND       :: u32(1 << 0)
CANCELLATION_CANCELLED       :: u32(1 << 1)
CANCELLATION_PAUSED          :: u32(1 << 2)
CANCELLATION_TIMED_OUT       :: u32(1 << 3)
CANCELLATION_WORKER_SHUTDOWN :: u32(1 << 4)
CANCELLATION_RESET           :: u32(1 << 5)

cancellation_details_bits :: proc(details: Cancellation_Details) -> u32 {
	bits: u32
	if details.is_not_found       do bits |= CANCELLATION_NOT_FOUND
	if details.is_cancelled       do bits |= CANCELLATION_CANCELLED
	if details.is_paused          do bits |= CANCELLATION_PAUSED
	if details.is_timed_out       do bits |= CANCELLATION_TIMED_OUT
	if details.is_worker_shutdown do bits |= CANCELLATION_WORKER_SHUTDOWN
	if details.is_reset           do bits |= CANCELLATION_RESET
	return bits
}

cancellation_details_from_bits :: proc(bits: u32) -> Cancellation_Details {
	return {
		is_not_found = bits & CANCELLATION_NOT_FOUND != 0,
		is_cancelled = bits & CANCELLATION_CANCELLED != 0,
		is_paused = bits & CANCELLATION_PAUSED != 0,
		is_timed_out = bits & CANCELLATION_TIMED_OUT != 0,
		is_worker_shutdown = bits & CANCELLATION_WORKER_SHUTDOWN != 0,
		is_reset = bits & CANCELLATION_RESET != 0,
	}
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
	Canceled,
	Will_Complete_Async,
}

Application_Failure :: struct {
	message:          string,
	type:             string,
	non_retryable:    bool,
	details:          codec.Payload,
	has_details:      bool,
	next_retry_delay: time.Duration,
}

Cancellation :: struct {
	message:     string,
	details:     codec.Payload,
	has_details: bool,
}

Result :: struct(T: typeid) {
	status:       Result_Status,
	value:        T,
	failure:      Application_Failure,
	cancellation: Cancellation,
}

result_canceled :: proc($T: typeid, message := "Activity canceled") -> Result(T) {
	return {status = .Canceled, cancellation = {message = message}}
}

result_canceled_json :: proc(
	$T: typeid,
	details: $Details,
	message := "Activity canceled",
) -> Result(T) {
	payload, encode_error := codec.encode_json(details)
	if encode_error != .None {
		return result_failure(
			T,
			"could not encode Activity cancellation details as json/plain",
			failure_type = "OdinPayloadConversionError",
			non_retryable = true,
		)
	}
	return {
		status = .Canceled,
		cancellation = {
			message = message,
			details = payload,
			has_details = true,
		},
	}
}

result_success :: proc(value: $T) -> Result(T) {
	return {status = .Completed, value = value}
}

// result_will_complete_async tells Core that another client process will
// heartbeat and eventually complete this Activity using its task token or IDs.
// The callback must copy activity.info(ctx).task_token before returning when it
// intends to use token-based completion.
result_will_complete_async :: proc($T: typeid) -> Result(T) {
	return {status = .Will_Complete_Async}
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

cancellation_free :: proc(cancellation: ^Cancellation) {
	if cancellation == nil {
		return
	}
	codec.payload_free(&cancellation.details)
	cancellation^ = {}
}

Payload_Result :: struct {
	status:       Result_Status,
	payload:      codec.Payload,
	failure:      Application_Failure,
	cancellation: Cancellation,
}

payload_cancel :: proc(
	message := "Activity canceled",
	details := codec.Payload{},
	has_details := false,
) -> Payload_Result {
	return {
		status = .Canceled,
		cancellation = {
			message = message,
			details = details,
			has_details = has_details,
		},
	}
}

payload_cancel_json :: proc(details: $T, message := "Activity canceled") -> Payload_Result {
	payload, encode_error := codec.encode_json(details)
	if encode_error != .None {
		return payload_fail(
			"could not encode Activity cancellation details as json/plain",
			failure_type = "OdinPayloadConversionError",
		)
	}
	return payload_cancel(message, payload, true)
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

Header :: struct {
	key:     string,
	payload: codec.Payload_View,
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
	headers:           []Header,
}

payload_complete :: proc(payload: codec.Payload) -> Payload_Result {
	return {status = .Completed, payload = payload}
}

payload_will_complete_async :: proc() -> Payload_Result {
	return {status = .Will_Complete_Async}
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
	metric_meter_value: ^temporal_core.Metric_Meter,
	cancel_requested: bool,
	cancel_reason:    Cancel_Reason,
	cancel_details:   u32,
	heartbeat_user_data: rawptr,
	heartbeat_recorder:  Heartbeat_Recorder,
}

context_init :: proc(
	ctx: ^Context,
	activity_id: string,
	attempt: u32,
	heartbeat_user_data: rawptr,
	heartbeat_recorder: Heartbeat_Recorder,
	metric_meter: ^temporal_core.Metric_Meter = nil,
) {
	ctx^ = Context{
		info_value = Info{activity_id = activity_id, attempt = attempt},
		metric_meter_value = metric_meter,
		heartbeat_user_data = heartbeat_user_data,
		heartbeat_recorder = heartbeat_recorder,
	}
}

context_init_info :: proc(
	ctx: ^Context,
	activity_info: Info,
	heartbeat_user_data: rawptr,
	heartbeat_recorder: Heartbeat_Recorder,
	metric_meter: ^temporal_core.Metric_Meter = nil,
) {
	if ctx == nil {
		return
	}
	ctx^ = Context{
		info_value = activity_info,
		metric_meter_value = metric_meter,
		heartbeat_user_data = heartbeat_user_data,
		heartbeat_recorder = heartbeat_recorder,
	}
}

// metric_meter returns the callback-scoped application meter. Instruments
// created from it must be closed before the callback returns. A no-op meter is
// supplied when the Core Runtime has no exporter.
metric_meter :: proc(ctx: ^Context) -> ^temporal_core.Metric_Meter {
	if ctx == nil {
		return nil
	}
	return ctx.metric_meter_value
}

context_request_cancellation :: proc(
	ctx: ^Context,
	reason: Cancel_Reason,
	details: Cancellation_Details = {},
) {
	if ctx == nil {
		return
	}
	sync.atomic_store_explicit(&ctx.cancel_reason, reason, .Relaxed)
	sync.atomic_store_explicit(&ctx.cancel_details, cancellation_details_bits(details), .Relaxed)
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

cancellation_details :: proc(ctx: ^Context) -> (details: Cancellation_Details, present: bool) {
	if !is_cancellation_requested(ctx) {
		return {}, false
	}
	bits := sync.atomic_load_explicit(&ctx.cancel_details, .Relaxed)
	return cancellation_details_from_bits(bits), true
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

header_payload :: proc(ctx: ^Context, key: string) -> (payload: codec.Payload_View, found: bool) {
	if ctx == nil {
		return
	}
	for entry in ctx.info_value.headers {
		if entry.key == key {
			return entry.payload, true
		}
	}
	return
}

header_json :: proc(
	ctx: ^Context,
	key: string,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := header_payload(ctx, key)
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

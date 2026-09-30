// Package nexus contains values available while handling a Nexus operation.
// Start handlers may complete synchronously or return a durable async operation
// token; separately registered cancel handlers receive that token. Context
// values are borrowed and valid only for the duration of the handler call.
package nexus

import allocator_runtime "base:runtime"
import "core:strings"
import "core:sync"
import "core:time"
import codec "../codec"

Cancel_Reason :: enum u32 {
	None,
	Timed_Out,
	Worker_Shutdown,
}

Retry_Behavior :: enum {
	Unspecified,
	Retryable,
	Non_Retryable,
}

Header :: struct {
	key:   string,
	value: string,
}

Info :: struct {
	service:          string,
	operation:        string,
	request_id:       string,
	operation_token:  string,
	callback_url:     string,
	endpoint:         string,
	namespace:        string,
	task_queue:       string,
	task_token:       []u8,
	headers:          []Header,
	callback_headers: []Header,
	scheduled_time:     time.Time,
	has_scheduled_time: bool,
	deadline:            time.Time,
	has_deadline:        bool,
}

Context :: struct {
	info_value:       Info,
	cancel_requested: bool,
	cancel_reason:    Cancel_Reason,
}

context_init :: proc(ctx: ^Context, value: Info) {
	if ctx == nil {
		return
	}
	ctx^ = Context{info_value = value}
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

info :: proc(ctx: ^Context) -> Info {
	if ctx == nil {
		return {}
	}
	return ctx.info_value
}

header :: proc(ctx: ^Context, key: string) -> (value: string, found: bool) {
	if ctx == nil {
		return
	}
	for entry in ctx.info_value.headers {
		if entry.key == key {
			return entry.value, true
		}
	}
	return
}

callback_header :: proc(ctx: ^Context, key: string) -> (value: string, found: bool) {
	if ctx == nil {
		return
	}
	for entry in ctx.info_value.callback_headers {
		if entry.key == key {
			return entry.value, true
		}
	}
	return
}

Result_Status :: enum {
	Completed,
	Started_Async,
	Handler_Error,
}

Handler_Error :: struct {
	message:        string,
	type:           string,
	retry_behavior: Retry_Behavior,
}

Result :: struct(T: typeid) {
	status:          Result_Status,
	value:           T,
	operation_token: string,
	operation_token_owned: bool,
	handler_error:   Handler_Error,
}

result_success :: proc(value: $T) -> Result(T) {
	return {status = .Completed, value = value}
}

// result_async acknowledges a durable asynchronous operation. The handler must
// arrange eventual completion through the callback URL and headers in Info.
// operation_token is borrowed until the handler returns and must be nonempty.
result_async :: proc($T: typeid, operation_token: string) -> Result(T) {
	return {status = .Started_Async, operation_token = operation_token}
}

// result_async_copy is the safe form for a token backed by temporary storage.
// The Worker takes ownership of the copied token and releases it after the
// Nexus completion has been encoded. result_async remains allocation-free for
// literals and other storage that outlives the handler adapter call.
result_async_copy :: proc($T: typeid, operation_token: string) -> Result(T) {
	owned_token, allocation_error := strings.clone(
		operation_token,
		allocator_runtime.heap_allocator(),
	)
	if allocation_error != nil {
		return result_handler_error(
			T,
			"could not preserve asynchronous Nexus operation token",
			"INTERNAL",
			.Retryable,
		)
	}
	return {
		status = .Started_Async,
		operation_token = owned_token,
		operation_token_owned = true,
	}
}

result_free :: proc(result: ^Result($T)) {
	if result == nil {
		return
	}
	if result.operation_token_owned {
		delete(result.operation_token, allocator_runtime.heap_allocator())
	}
	result^ = {}
}

result_handler_error :: proc(
	$T: typeid,
	message: string,
	error_type := "INTERNAL",
	retry_behavior := Retry_Behavior.Unspecified,
) -> Result(T) {
	return {
		status = .Handler_Error,
		handler_error = {
			message = message,
			type = error_type,
			retry_behavior = retry_behavior,
		},
	}
}

Payload_Result :: struct {
	status:          Result_Status,
	payload:         codec.Payload,
	operation_token: string,
	operation_token_owned: bool,
	handler_error:   Handler_Error,
}

payload_complete :: proc(payload: codec.Payload) -> Payload_Result {
	return {status = .Completed, payload = payload}
}

payload_async :: proc(operation_token: string) -> Payload_Result {
	return {status = .Started_Async, operation_token = operation_token}
}

payload_handler_error :: proc(
	message: string,
	error_type := "INTERNAL",
	retry_behavior := Retry_Behavior.Unspecified,
) -> Payload_Result {
	return {
		status = .Handler_Error,
		handler_error = {
			message = message,
			type = error_type,
			retry_behavior = retry_behavior,
		},
	}
}

payload_result_free :: proc(result: ^Payload_Result) {
	if result == nil {
		return
	}
	codec.payload_free(&result.payload)
	if result.operation_token_owned {
		delete(result.operation_token, allocator_runtime.heap_allocator())
	}
	result^ = {}
}

Cancel_Result_Status :: enum {
	Completed,
	Handler_Error,
}

Cancel_Result :: struct {
	status:        Cancel_Result_Status,
	handler_error: Handler_Error,
}

cancel_complete :: proc() -> Cancel_Result {
	return {status = .Completed}
}

cancel_handler_error :: proc(
	message: string,
	error_type := "INTERNAL",
	retry_behavior := Retry_Behavior.Unspecified,
) -> Cancel_Result {
	return {
		status = .Handler_Error,
		handler_error = {
			message = message,
			type = error_type,
			retry_behavior = retry_behavior,
		},
	}
}

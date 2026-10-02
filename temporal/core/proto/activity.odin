// Activity-facing adapters over the schema-generated protobuf types.
package proto

import "core:time"
import "core:mem/virtual"
import codec "../../codec"

Task_Kind :: enum {
	Invalid,
	Start,
	Cancel,
}

Payload_View :: struct {
	encoding:         string,
	metadata:         []Temporal_Api_Common_V1_Payload_MetadataEntry,
	data:             []u8,
	external_payloads: []Temporal_Api_Common_V1_Payload_ExternalPayloadDetails,
}

Timestamp_Value :: struct {
	value:   time.Time,
	present: bool,
}

Duration_Value :: struct {
	value:   time.Duration,
	present: bool,
}

Retry_Policy_View :: struct {
	initial_interval:          Duration_Value,
	backoff_coefficient:       f64,
	maximum_interval:          Duration_Value,
	maximum_attempts:          i32,
	non_retryable_error_types: []string,
	present:                   bool,
}

Priority_View :: struct {
	priority_key:    i32,
	fairness_key:    string,
	fairness_weight: f32,
	present:         bool,
}

Named_Payload_View :: struct {
	key:     string,
	payload: Payload_View,
}

Activity_Task :: struct {
	kind:          Task_Kind,
	task_token:    []u8,
	activity_id:   string,
	activity_type: string,
	attempt:       u32,
	inputs:        []Payload_View,
	// input remains a borrowed alias of inputs[0] for source compatibility.
	input:         Payload_View,
	headers:       []Named_Payload_View,
	heartbeat_details:              []Payload_View,
	workflow_namespace:             string,
	workflow_type:                  string,
	workflow_id:                    string,
	workflow_run_id:                string,
	activity_run_id:                string,
	scheduled_time:                 Timestamp_Value,
	current_attempt_scheduled_time: Timestamp_Value,
	started_time:                   Timestamp_Value,
	schedule_to_close_timeout:      Duration_Value,
	start_to_close_timeout:         Duration_Value,
	heartbeat_timeout:              Duration_Value,
	retry_policy:                   Retry_Policy_View,
	priority:                       Priority_View,
	is_local:                       bool,
	cancel_reason: Coresdk_Activity_Task_ActivityCancelReason,
	cancel_details: Activity_Cancel_Details,
}

Activity_Cancel_Details :: struct {
	is_not_found:       bool,
	is_cancelled:       bool,
	is_paused:          bool,
	is_timed_out:       bool,
	is_worker_shutdown: bool,
	is_reset:           bool,
}

Decode_Error :: enum {
	None,
	Malformed,
	Missing_Task_Token,
	Missing_Variant,
}

payload_view :: proc(payload: ^Temporal_Api_Common_V1_Payload) -> Payload_View {
	result := Payload_View{}
	if payload == nil {
		return result
	}
	result.data = payload.data
	result.metadata = payload.metadata[:]
	result.external_payloads = payload.external_payloads[:]
	for entry in payload.metadata {
		if entry.key == "encoding" {
			result.encoding = string(entry.value)
		}
	}
	return result
}

timestamp_value :: proc(value: ^Google_Protobuf_Timestamp) -> Timestamp_Value {
	if value == nil {
		return {}
	}
	return {
		value = time.unix(value.seconds, i64(value.nanos)),
		present = true,
	}
}

duration_value :: proc(value: ^Google_Protobuf_Duration) -> Duration_Value {
	if value == nil {
		return {}
	}
	duration := time.Duration(value.seconds * i64(time.Second) + i64(value.nanos))
	return {value = duration, present = duration > 0}
}

retry_policy_view :: proc(value: ^Temporal_Api_Common_V1_RetryPolicy) -> Retry_Policy_View {
	if value == nil {
		return {}
	}
	return {
		initial_interval = duration_value(value.initial_interval),
		backoff_coefficient = value.backoff_coefficient,
		maximum_interval = duration_value(value.maximum_interval),
		maximum_attempts = value.maximum_attempts,
		non_retryable_error_types = value.non_retryable_error_types[:],
		present = true,
	}
}

priority_view :: proc(value: ^Temporal_Api_Common_V1_Priority) -> Priority_View {
	if value == nil {
		return {}
	}
	return {
		priority_key = value.priority_key,
		fairness_key = value.fairness_key,
		fairness_weight = value.fairness_weight,
		present = true,
	}
}

clone_payload :: proc(
	payload: ^Temporal_Api_Common_V1_Payload,
) -> (owned: codec.Payload, error: codec.Error) {
	if payload == nil {
		return {}, .Allocation_Failed
	}
	owned = codec.payload_create(payload.data)
	for entry in payload.metadata {
		if codec.payload_add_metadata(&owned, entry.key, entry.value) != .None {
			codec.payload_free(&owned)
			return {}, .Allocation_Failed
		}
	}
	for external in payload.external_payloads {
		if codec.payload_add_external(&owned, external.size_bytes) != .None {
			codec.payload_free(&owned)
			return {}, .Allocation_Failed
		}
	}
	return owned, .None
}

clone_payload_view :: proc(
	view: Payload_View,
	allocator := context.allocator,
) -> (owned: codec.Payload, error: codec.Error) {
	owned = codec.payload_create(view.data, allocator)
	for entry in view.metadata {
		if codec.payload_add_metadata(&owned, entry.key, entry.value) != .None {
			codec.payload_free(&owned)
			return {}, .Allocation_Failed
		}
	}
	for external in view.external_payloads {
		if codec.payload_add_external(&owned, external.size_bytes) != .None {
			codec.payload_free(&owned)
			return {}, .Allocation_Failed
		}
	}
	return owned, .None
}

// Generated decoding allocates nested messages and repeated fields through
// context.allocator. Callers should use an arena whose lifetime covers all
// returned views.
decode_activity_task :: proc(data: []u8) -> (task: Activity_Task, error: Decode_Error) {
	decoded := Coresdk_Activity_Task_ActivityTask{}
	if !decode_Coresdk_Activity_Task_ActivityTask(data, &decoded) {
		return {}, .Malformed
	}
	if len(decoded.task_token) == 0 {
		return {}, .Missing_Task_Token
	}

	task.task_token = decoded.task_token
	#partial switch variant in decoded.variant {
	case Coresdk_Activity_Task_ActivityTask_Variant_Start:
		if variant.value == nil || len(variant.value.activity_type) == 0 {
			return {}, .Malformed
		}
		task.kind = .Start
		task.activity_id = variant.value.activity_id
		task.activity_type = variant.value.activity_type
		task.attempt = variant.value.attempt
		task.workflow_namespace = variant.value.workflow_namespace
		task.workflow_type = variant.value.workflow_type
		if variant.value.workflow_execution != nil {
			task.workflow_id = variant.value.workflow_execution.workflow_id
			task.workflow_run_id = variant.value.workflow_execution.run_id
		}
		task.activity_run_id = variant.value.run_id
		task.scheduled_time = timestamp_value(variant.value.scheduled_time)
		task.current_attempt_scheduled_time = timestamp_value(
			variant.value.current_attempt_scheduled_time,
		)
		task.started_time = timestamp_value(variant.value.started_time)
		task.schedule_to_close_timeout = duration_value(variant.value.schedule_to_close_timeout)
		task.start_to_close_timeout = duration_value(variant.value.start_to_close_timeout)
		task.heartbeat_timeout = duration_value(variant.value.heartbeat_timeout)
		task.retry_policy = retry_policy_view(variant.value.retry_policy)
		task.priority = priority_view(variant.value.priority)
		task.is_local = variant.value.is_local
		if len(variant.value.header_fields) > 0 {
			headers := make([]Named_Payload_View, len(variant.value.header_fields))
			for &entry, index in variant.value.header_fields {
				if len(entry.key) == 0 || entry.value == nil {
					return {}, .Malformed
				}
				headers[index] = {key = entry.key, payload = payload_view(entry.value)}
			}
			task.headers = headers
		}
		if len(variant.value.input) > 0 {
			inputs := make([]Payload_View, len(variant.value.input))
			for &payload, index in variant.value.input {
				inputs[index] = payload_view(&payload)
			}
			task.inputs = inputs
			task.input = inputs[0]
		}
		if len(variant.value.heartbeat_details) > 0 {
			details := make([]Payload_View, len(variant.value.heartbeat_details))
			for &payload, index in variant.value.heartbeat_details {
				details[index] = payload_view(&payload)
			}
			task.heartbeat_details = details
		}
	case Coresdk_Activity_Task_ActivityTask_Variant_Cancel:
		task.kind = .Cancel
		if variant.value == nil {
			return {}, .Malformed
		}
		task.cancel_reason = variant.value.reason
		if variant.value.details != nil {
			task.cancel_details = Activity_Cancel_Details{
				is_not_found = variant.value.details.is_not_found,
				is_cancelled = variant.value.details.is_cancelled,
				is_paused = variant.value.details.is_paused,
				is_timed_out = variant.value.details.is_timed_out,
				is_worker_shutdown = variant.value.details.is_worker_shutdown,
				is_reset = variant.value.details.is_reset,
			}
		}
	}
	if task.kind == .Invalid {
		return {}, .Missing_Variant
	}
	return task, .None
}

// activity_completion_matches_task validates the minimum trust boundary for
// an externally executed Activity: a well-formed completion with the exact
// opaque task token and a result variant. Decoded views borrow data and the
// caller controls their allocation lifetime through context.allocator.
activity_completion_matches_task :: proc(data, task_token: []u8) -> bool {
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return false
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	completion := Coresdk_ActivityTaskCompletion{}
	decoded := decode_Coresdk_ActivityTaskCompletion(data, &completion)
	context.allocator = previous_allocator
	if !decoded {
		return false
	}
	if completion.result == nil || len(completion.task_token) != len(task_token) {
		return false
	}
	for value, index in task_token {
		if completion.task_token[index] != value {
			return false
		}
	}
	return true
}

activity_completion_is_cancellation_for_task :: proc(data, task_token: []u8) -> bool {
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return false
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	completion := Coresdk_ActivityTaskCompletion{}
	decoded := decode_Coresdk_ActivityTaskCompletion(data, &completion)
	context.allocator = previous_allocator
	if !decoded || completion.result == nil || len(completion.task_token) != len(task_token) {
		return false
	}
	for value, index in task_token {
		if completion.task_token[index] != value {
			return false
		}
	}
	#partial switch status in completion.result.status {
	case Coresdk_Activity_Result_ActivityExecutionResult_Status_Cancelled:
		return status.value != nil
	case:
	}
	return false
}

encode_payload :: proc(view: codec.Payload_View) -> Temporal_Api_Common_V1_Payload {
	payload := Temporal_Api_Common_V1_Payload{data = view.data}
	for entry in view.metadata {
		append(
			&payload.metadata,
			Temporal_Api_Common_V1_Payload_MetadataEntry{
				key = entry.key,
				value = entry.value,
			},
		)
	}
	for external in view.external_payloads {
		append(
			&payload.external_payloads,
			Temporal_Api_Common_V1_Payload_ExternalPayloadDetails{
				size_bytes = external.size_bytes,
			},
		)
	}
	return payload
}

encode_activity_heartbeat_payload :: proc(task_token: []u8, details: codec.Payload_View) -> [dynamic]u8 {
	payload := encode_payload(details)
	defer delete(payload.metadata)
	defer delete(payload.external_payloads)
	heartbeat := Coresdk_ActivityHeartbeat{task_token = task_token}
	defer delete(heartbeat.details)
	append(&heartbeat.details, payload)
	return encode_Coresdk_ActivityHeartbeat(&heartbeat)
}

encode_activity_heartbeat :: proc(task_token, details_data: []u8) -> [dynamic]u8 {
	metadata := [1]codec.Metadata_Entry{{
		key = "encoding",
		value = transmute([]u8)string("json/plain"),
	}}
	return encode_activity_heartbeat_payload(
		task_token,
		codec.Payload_View{metadata = metadata[:], data = details_data},
	)
}

encode_activity_success_payload :: proc(task_token: []u8, result: codec.Payload_View) -> [dynamic]u8 {
	payload := encode_payload(result)
	defer delete(payload.metadata)
	defer delete(payload.external_payloads)
	success := Coresdk_Activity_Result_Success{result = &payload}
	execution_result := Coresdk_Activity_Result_ActivityExecutionResult{
		status = Coresdk_Activity_Result_ActivityExecutionResult_Status_Completed{value = &success},
	}
	completion := Coresdk_ActivityTaskCompletion{
		task_token = task_token,
		result = &execution_result,
	}
	return encode_Coresdk_ActivityTaskCompletion(&completion)
}

encode_activity_success :: proc(task_token, result_data: []u8) -> [dynamic]u8 {
	metadata := [1]codec.Metadata_Entry{{
		key = "encoding",
		value = transmute([]u8)string("json/plain"),
	}}
	return encode_activity_success_payload(
		task_token,
		codec.Payload_View{metadata = metadata[:], data = result_data},
	)
}

encode_activity_will_complete_async :: proc(task_token: []u8) -> [dynamic]u8 {
	async := Coresdk_Activity_Result_WillCompleteAsync{}
	execution_result := Coresdk_Activity_Result_ActivityExecutionResult{
		status = Coresdk_Activity_Result_ActivityExecutionResult_Status_Will_Complete_Async{
			value = &async,
		},
	}
	completion := Coresdk_ActivityTaskCompletion{
		task_token = task_token,
		result = &execution_result,
	}
	return encode_Coresdk_ActivityTaskCompletion(&completion)
}

encode_activity_failure :: proc(
	task_token: []u8,
	message: string,
	failure_type := "OdinActivityError",
	non_retryable := true,
	details := codec.Payload_View{},
	has_details := false,
	next_retry_delay: time.Duration = 0,
) -> [dynamic]u8 {
	details_payload := encode_payload(details)
	defer delete(details_payload.metadata)
	defer delete(details_payload.external_payloads)
	detail_payloads := Temporal_Api_Common_V1_Payloads{}
	defer delete(detail_payloads.payloads)
	if has_details {
		append(&detail_payloads.payloads, details_payload)
	}

	retry_delay := Google_Protobuf_Duration{}
	if next_retry_delay > 0 {
		nanoseconds := i64(next_retry_delay)
		retry_delay.seconds = nanoseconds / i64(time.Second)
		retry_delay.nanos = i32(nanoseconds % i64(time.Second))
	}
	application_info := Temporal_Api_Failure_V1_ApplicationFailureInfo{
		type = failure_type,
		non_retryable = non_retryable,
	}
	if has_details {
		application_info.details = &detail_payloads
	}
	if next_retry_delay > 0 {
		application_info.next_retry_delay = &retry_delay
	}
	failure := Temporal_Api_Failure_V1_Failure{
		message = message,
		source = "OdinSDK",
		failure_info = Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = &application_info,
		},
	}
	activity_failure := Coresdk_Activity_Result_Failure{
		failure = &failure,
		cause = .ACTIVITY_TASK_FAILED_CAUSE_ACTIVITY_WORKER_UNHANDLED_FAILURE,
	}
	execution_result := Coresdk_Activity_Result_ActivityExecutionResult{
		status = Coresdk_Activity_Result_ActivityExecutionResult_Status_Failed{
			value = &activity_failure,
		},
	}
	completion := Coresdk_ActivityTaskCompletion{
		task_token = task_token,
		result = &execution_result,
	}
	return encode_Coresdk_ActivityTaskCompletion(&completion)
}

encode_activity_cancellation :: proc(
	task_token: []u8,
	message := "Activity canceled",
	details := codec.Payload_View{},
	has_details := false,
) -> [dynamic]u8 {
	details_payload := encode_payload(details)
	defer delete(details_payload.metadata)
	defer delete(details_payload.external_payloads)
	detail_payloads := Temporal_Api_Common_V1_Payloads{}
	defer delete(detail_payloads.payloads)
	if has_details {
		append(&detail_payloads.payloads, details_payload)
	}
	canceled_info := Temporal_Api_Failure_V1_CanceledFailureInfo{}
	if has_details {
		canceled_info.details = &detail_payloads
	}
	failure := Temporal_Api_Failure_V1_Failure{
		message = message,
		source = "OdinSDK",
		failure_info = Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info{
			value = &canceled_info,
		},
	}
	cancellation := Coresdk_Activity_Result_Cancellation{failure = &failure}
	execution_result := Coresdk_Activity_Result_ActivityExecutionResult{
		status = Coresdk_Activity_Result_ActivityExecutionResult_Status_Cancelled{
			value = &cancellation,
		},
	}
	completion := Coresdk_ActivityTaskCompletion{
		task_token = task_token,
		result = &execution_result,
	}
	return encode_Coresdk_ActivityTaskCompletion(&completion)
}

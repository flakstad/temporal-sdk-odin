// Activity-facing adapters over the schema-generated protobuf types.
package proto

import "core:time"
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

Activity_Task :: struct {
	kind:          Task_Kind,
	task_token:    []u8,
	activity_id:   string,
	activity_type: string,
	attempt:       u32,
	input:         Payload_View,
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
		if len(variant.value.input) > 0 {
			task.input = payload_view(&variant.value.input[0])
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

encode_activity_cancellation :: proc(task_token: []u8, message := "Activity canceled") -> [dynamic]u8 {
	canceled_info := Temporal_Api_Failure_V1_CanceledFailureInfo{}
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

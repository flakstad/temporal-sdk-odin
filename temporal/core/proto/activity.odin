// Activity-facing adapters over the schema-generated protobuf types.
package proto

Task_Kind :: enum {
	Invalid,
	Start,
	Cancel,
}

Payload_View :: struct {
	encoding: string,
	data:     []u8,
}

Activity_Task :: struct {
	kind:          Task_Kind,
	task_token:    []u8,
	activity_id:   string,
	activity_type: string,
	attempt:       u32,
	input:         Payload_View,
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
	}
	if task.kind == .Invalid {
		return {}, .Missing_Variant
	}
	return task, .None
}

encode_activity_success :: proc(task_token, result_data: []u8) -> [dynamic]u8 {
	payload := Temporal_Api_Common_V1_Payload{data = result_data}
	defer delete(payload.metadata)
	append(
		&payload.metadata,
		Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
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

encode_activity_failure :: proc(task_token: []u8, message: string, non_retryable := true) -> [dynamic]u8 {
	application_info := Temporal_Api_Failure_V1_ApplicationFailureInfo{
		type = "OdinActivityError",
		non_retryable = non_retryable,
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

// Nexus-facing adapters over the schema-generated protobuf types.
package proto

import codec "../../codec"

Nexus_Task_Kind :: enum {
	Invalid,
	Start_Operation,
	Cancel_Operation,
	Cancel_Task,
}

Nexus_Header_View :: struct {
	key:   string,
	value: string,
}

Nexus_Task :: struct {
	kind:             Nexus_Task_Kind,
	task_token:       []u8,
	service:          string,
	operation:        string,
	request_id:       string,
	callback_url:     string,
	operation_token:  string,
	endpoint:         string,
	input:            Payload_View,
	headers:          []Nexus_Header_View,
	callback_headers: []Nexus_Header_View,
	scheduled_time:   Timestamp_Value,
	request_deadline: Timestamp_Value,
	cancel_reason:    Coresdk_Nexus_NexusTaskCancelReason,
}

Nexus_Decode_Error :: enum {
	None,
	Malformed,
	Missing_Task_Token,
	Missing_Request,
	Missing_Variant,
}

decode_nexus_task :: proc(data: []u8) -> (task: Nexus_Task, error: Nexus_Decode_Error) {
	decoded := Coresdk_Nexus_NexusTask{}
	if !decode_Coresdk_Nexus_NexusTask(data, &decoded) {
		return {}, .Malformed
	}
	task.endpoint = decoded.endpoint
	task.request_deadline = timestamp_value(decoded.request_deadline)

	#partial switch variant in decoded.variant {
	case Coresdk_Nexus_NexusTask_Variant_Cancel_Task:
		if variant.value == nil || len(variant.value.task_token) == 0 {
			return {}, .Missing_Task_Token
		}
		task.kind = .Cancel_Task
		task.task_token = variant.value.task_token
		task.cancel_reason = variant.value.reason
		return task, .None
	case Coresdk_Nexus_NexusTask_Variant_Task:
		if variant.value == nil || len(variant.value.task_token) == 0 {
			return {}, .Missing_Task_Token
		}
		if variant.value.request == nil {
			return {}, .Missing_Request
		}
		task.task_token = variant.value.task_token
		request := variant.value.request
		task.scheduled_time = timestamp_value(request.scheduled_time)
		if len(task.endpoint) == 0 {
			task.endpoint = request.endpoint
		}
		if len(request.header) > 0 {
			headers := make([]Nexus_Header_View, len(request.header))
			for entry, index in request.header {
				headers[index] = {key = entry.key, value = entry.value}
			}
			task.headers = headers
		}
		#partial switch request_variant in request.variant {
		case Temporal_Api_Nexus_V1_Request_Variant_Start_Operation:
			if request_variant.value == nil {
				return {}, .Missing_Request
			}
			start := request_variant.value
			task.kind = .Start_Operation
			task.service = start.service
			task.operation = start.operation
			task.request_id = start.request_id
			task.callback_url = start.callback
			task.input = payload_view(start.payload)
			if len(start.callback_header) > 0 {
				callback_headers := make([]Nexus_Header_View, len(start.callback_header))
				for entry, index in start.callback_header {
					callback_headers[index] = {key = entry.key, value = entry.value}
				}
				task.callback_headers = callback_headers
			}
			return task, .None
		case Temporal_Api_Nexus_V1_Request_Variant_Cancel_Operation:
			if request_variant.value == nil {
				return {}, .Missing_Request
			}
			cancel := request_variant.value
			task.kind = .Cancel_Operation
			task.service = cancel.service
			task.operation = cancel.operation
			task.operation_token = cancel.operation_token
			if len(task.operation_token) == 0 {
				task.operation_token = cancel.operation_id
			}
			return task, .None
		}
		return {}, .Missing_Variant
	}
	return {}, .Missing_Variant
}

encode_nexus_sync_success :: proc(task_token: []u8, result: codec.Payload_View) -> [dynamic]u8 {
	payload := encode_payload(result)
	defer delete(payload.metadata)
	defer delete(payload.external_payloads)
	sync_result := Temporal_Api_Nexus_V1_StartOperationResponse_Sync{payload = &payload}
	start_response := Temporal_Api_Nexus_V1_StartOperationResponse{
		variant = Temporal_Api_Nexus_V1_StartOperationResponse_Variant_Sync_Success{
			value = &sync_result,
		},
	}
	response := Temporal_Api_Nexus_V1_Response{
		variant = Temporal_Api_Nexus_V1_Response_Variant_Start_Operation{
			value = &start_response,
		},
	}
	completion := Coresdk_Nexus_NexusTaskCompletion{
		task_token = task_token,
		status = Coresdk_Nexus_NexusTaskCompletion_Status_Completed{value = &response},
	}
	return encode_Coresdk_Nexus_NexusTaskCompletion(&completion)
}

encode_nexus_cancel_success :: proc(task_token: []u8) -> [dynamic]u8 {
	cancel_response := Temporal_Api_Nexus_V1_CancelOperationResponse{}
	response := Temporal_Api_Nexus_V1_Response{
		variant = Temporal_Api_Nexus_V1_Response_Variant_Cancel_Operation{
			value = &cancel_response,
		},
	}
	completion := Coresdk_Nexus_NexusTaskCompletion{
		task_token = task_token,
		status = Coresdk_Nexus_NexusTaskCompletion_Status_Completed{value = &response},
	}
	return encode_Coresdk_Nexus_NexusTaskCompletion(&completion)
}

encode_nexus_ack_cancel :: proc(task_token: []u8) -> [dynamic]u8 {
	completion := Coresdk_Nexus_NexusTaskCompletion{
		task_token = task_token,
		status = Coresdk_Nexus_NexusTaskCompletion_Status_Ack_Cancel{value = true},
	}
	return encode_Coresdk_Nexus_NexusTaskCompletion(&completion)
}

encode_nexus_handler_failure :: proc(
	task_token: []u8,
	message: string,
	error_type := "INTERNAL",
	retry_behavior := Temporal_Api_Enums_V1_NexusHandlerErrorRetryBehavior.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
) -> [dynamic]u8 {
	info := Temporal_Api_Failure_V1_NexusHandlerFailureInfo{
		type = error_type,
		retry_behavior = retry_behavior,
	}
	failure := Temporal_Api_Failure_V1_Failure{
		message = message,
		source = "OdinSDK",
		failure_info = Temporal_Api_Failure_V1_Failure_Failure_Info_Nexus_Handler_Failure_Info{
			value = &info,
		},
	}
	completion := Coresdk_Nexus_NexusTaskCompletion{
		task_token = task_token,
		status = Coresdk_Nexus_NexusTaskCompletion_Status_Failure{value = &failure},
	}
	return encode_Coresdk_Nexus_NexusTaskCompletion(&completion)
}

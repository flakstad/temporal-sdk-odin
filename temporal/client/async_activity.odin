package client

import "core:mem/virtual"
import "core:time"
import activity "../activity"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"

// Async_Activity_Identifier borrows its strings/token for the duration of one
// synchronous Client call. A task token takes precedence over ID fields.
Async_Activity_Identifier :: struct {
	task_token:  []u8,
	workflow_id: string,
	run_id:      string,
	activity_id: string,
}

Async_Activity_Heartbeat :: struct {
	cancel_requested: bool,
	activity_paused:   bool,
	activity_reset:    bool,
}

async_activity_from_task_token :: proc(task_token: []u8) -> Async_Activity_Identifier {
	return {task_token = task_token}
}

async_activity_from_workflow :: proc(
	workflow_id, activity_id: string,
	run_id := "",
) -> Async_Activity_Identifier {
	return {workflow_id = workflow_id, run_id = run_id, activity_id = activity_id}
}

async_activity_standalone :: proc(
	activity_id: string,
	run_id := "",
) -> Async_Activity_Identifier {
	return {run_id = run_id, activity_id = activity_id}
}

async_activity_validate :: proc(
	client: ^Client,
	identifier: Async_Activity_Identifier,
) -> Error {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if len(identifier.task_token) > 0 {
		return {}
	}
	if len(identifier.activity_id) == 0 {
		return error_from_string(.Invalid_Argument, "async Activity task token or Activity ID is required")
	}
	return {}
}

async_activity_identity :: proc(client: ^Client) -> string {
	if client != nil && len(client.identity) > 0 {
		return client.identity
	}
	return "temporal-sdk-odin@local"
}

async_activity_payloads :: proc(
	value: ^codec.Payload_View,
	payload: ^proto.Temporal_Api_Common_V1_Payload,
	payloads: ^proto.Temporal_Api_Common_V1_Payloads,
) {
	if value == nil {
		return
	}
	payload^ = proto.encode_payload(value^)
	append(&payloads.payloads, payload^)
}

async_activity_payloads_free :: proc(
	payload: ^proto.Temporal_Api_Common_V1_Payload,
	payloads: ^proto.Temporal_Api_Common_V1_Payloads,
) {
	delete(payload.metadata)
	delete(payload.external_payloads)
	delete(payloads.payloads)
	payload^ = {}
	payloads^ = {}
}

async_activity_rpc :: proc(
	client: ^Client,
	method: string,
	request: []u8,
) -> Error {
	result := temporal_core.client_rpc_call(client.connection, .Workflow, method, request)
	defer delete(result.protobuf)
	if result.error.message != nil {
		return error_from_core(&result.error)
	}
	return {}
}

complete_async_activity_payload :: proc(
	client: ^Client,
	identifier: Async_Activity_Identifier,
	result: ^codec.Payload_View = nil,
) -> Error {
	validation_error := async_activity_validate(client, identifier)
	if validation_error.message != nil {
		return validation_error
	}
	payload: proto.Temporal_Api_Common_V1_Payload
	payloads: proto.Temporal_Api_Common_V1_Payloads
	async_activity_payloads(result, &payload, &payloads)
	defer async_activity_payloads_free(&payload, &payloads)
	request: [dynamic]u8
	method: string
	if len(identifier.task_token) > 0 {
		message := proto.Temporal_Api_Workflowservice_V1_RespondActivityTaskCompletedRequest{
			task_token = identifier.task_token,
			identity = async_activity_identity(client),
			namespace = client.namespace,
		}
		if result != nil do message.result = &payloads
		request = proto.encode_Temporal_Api_Workflowservice_V1_RespondActivityTaskCompletedRequest(&message)
		method = "RespondActivityTaskCompleted"
	} else {
		message := proto.Temporal_Api_Workflowservice_V1_RespondActivityTaskCompletedByIdRequest{
			namespace = client.namespace,
			workflow_id = identifier.workflow_id,
			run_id = identifier.run_id,
			activity_id = identifier.activity_id,
			identity = async_activity_identity(client),
		}
		if result != nil do message.result = &payloads
		request = proto.encode_Temporal_Api_Workflowservice_V1_RespondActivityTaskCompletedByIdRequest(&message)
		method = "RespondActivityTaskCompletedById"
	}
	defer delete(request)
	return async_activity_rpc(client, method, request[:])
}

complete_async_activity_json :: proc(
	client: ^Client,
	identifier: Async_Activity_Identifier,
	result: $T,
) -> Error {
	payload, encode_error := codec.encode_json(result)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode async Activity result")
	}
	defer codec.payload_free(&payload)
	view := codec.payload_view(&payload)
	return complete_async_activity_payload(client, identifier, &view)
}

fail_async_activity :: proc(
	client: ^Client,
	identifier: Async_Activity_Identifier,
	failure: activity.Application_Failure,
	last_heartbeat: ^codec.Payload_View = nil,
) -> Error {
	validation_error := async_activity_validate(client, identifier)
	if validation_error.message != nil {
		return validation_error
	}
	if failure.next_retry_delay < 0 {
		return error_from_string(.Invalid_Argument, "async Activity failure retry delay must not be negative")
	}
	detail_payload: proto.Temporal_Api_Common_V1_Payload
	details: proto.Temporal_Api_Common_V1_Payloads
	if failure.has_details {
		failure_details := failure.details
		view := codec.payload_view(&failure_details)
		async_activity_payloads(&view, &detail_payload, &details)
	}
	defer async_activity_payloads_free(&detail_payload, &details)
	failure_type := failure.type
	if len(failure_type) == 0 {
		failure_type = "OdinActivityError"
	}
	application := proto.Temporal_Api_Failure_V1_ApplicationFailureInfo{
		type = failure_type,
		non_retryable = failure.non_retryable,
	}
	if failure.has_details do application.details = &details
	retry_delay := proto.Google_Protobuf_Duration{}
	if failure.next_retry_delay > 0 {
		nanoseconds := i64(failure.next_retry_delay)
		retry_delay.seconds = nanoseconds / i64(time.Second)
		retry_delay.nanos = i32(nanoseconds % i64(time.Second))
		application.next_retry_delay = &retry_delay
	}
	failure_proto := proto.Temporal_Api_Failure_V1_Failure{
		message = failure.message,
		source = "OdinSDK",
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = &application,
		},
	}
	heartbeat_payload: proto.Temporal_Api_Common_V1_Payload
	heartbeat_payloads: proto.Temporal_Api_Common_V1_Payloads
	async_activity_payloads(last_heartbeat, &heartbeat_payload, &heartbeat_payloads)
	defer async_activity_payloads_free(&heartbeat_payload, &heartbeat_payloads)
	request: [dynamic]u8
	method: string
	if len(identifier.task_token) > 0 {
		message := proto.Temporal_Api_Workflowservice_V1_RespondActivityTaskFailedRequest{
			task_token = identifier.task_token,
			failure = &failure_proto,
			identity = async_activity_identity(client),
			namespace = client.namespace,
		}
		if last_heartbeat != nil do message.last_heartbeat_details = &heartbeat_payloads
		request = proto.encode_Temporal_Api_Workflowservice_V1_RespondActivityTaskFailedRequest(&message)
		method = "RespondActivityTaskFailed"
	} else {
		message := proto.Temporal_Api_Workflowservice_V1_RespondActivityTaskFailedByIdRequest{
			namespace = client.namespace,
			workflow_id = identifier.workflow_id,
			run_id = identifier.run_id,
			activity_id = identifier.activity_id,
			failure = &failure_proto,
			identity = async_activity_identity(client),
		}
		if last_heartbeat != nil do message.last_heartbeat_details = &heartbeat_payloads
		request = proto.encode_Temporal_Api_Workflowservice_V1_RespondActivityTaskFailedByIdRequest(&message)
		method = "RespondActivityTaskFailedById"
	}
	defer delete(request)
	return async_activity_rpc(client, method, request[:])
}

report_async_activity_cancellation_payload :: proc(
	client: ^Client,
	identifier: Async_Activity_Identifier,
	details: ^codec.Payload_View = nil,
) -> Error {
	validation_error := async_activity_validate(client, identifier)
	if validation_error.message != nil {
		return validation_error
	}
	payload: proto.Temporal_Api_Common_V1_Payload
	payloads: proto.Temporal_Api_Common_V1_Payloads
	async_activity_payloads(details, &payload, &payloads)
	defer async_activity_payloads_free(&payload, &payloads)
	request: [dynamic]u8
	method: string
	if len(identifier.task_token) > 0 {
		message := proto.Temporal_Api_Workflowservice_V1_RespondActivityTaskCanceledRequest{
			task_token = identifier.task_token,
			identity = async_activity_identity(client),
			namespace = client.namespace,
		}
		if details != nil do message.details = &payloads
		request = proto.encode_Temporal_Api_Workflowservice_V1_RespondActivityTaskCanceledRequest(&message)
		method = "RespondActivityTaskCanceled"
	} else {
		message := proto.Temporal_Api_Workflowservice_V1_RespondActivityTaskCanceledByIdRequest{
			namespace = client.namespace,
			workflow_id = identifier.workflow_id,
			run_id = identifier.run_id,
			activity_id = identifier.activity_id,
			identity = async_activity_identity(client),
		}
		if details != nil do message.details = &payloads
		request = proto.encode_Temporal_Api_Workflowservice_V1_RespondActivityTaskCanceledByIdRequest(&message)
		method = "RespondActivityTaskCanceledById"
	}
	defer delete(request)
	return async_activity_rpc(client, method, request[:])
}

report_async_activity_cancellation_json :: proc(
	client: ^Client,
	identifier: Async_Activity_Identifier,
	details: $T,
) -> Error {
	payload, encode_error := codec.encode_json(details)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode async Activity cancellation details")
	}
	defer codec.payload_free(&payload)
	view := codec.payload_view(&payload)
	return report_async_activity_cancellation_payload(client, identifier, &view)
}

heartbeat_async_activity_payload :: proc(
	client: ^Client,
	identifier: Async_Activity_Identifier,
	details: ^codec.Payload_View = nil,
) -> (Async_Activity_Heartbeat, Error) {
	validation_error := async_activity_validate(client, identifier)
	if validation_error.message != nil {
		return {}, validation_error
	}
	payload: proto.Temporal_Api_Common_V1_Payload
	payloads: proto.Temporal_Api_Common_V1_Payloads
	async_activity_payloads(details, &payload, &payloads)
	defer async_activity_payloads_free(&payload, &payloads)
	request: [dynamic]u8
	method: string
	by_token := len(identifier.task_token) > 0
	if by_token {
		message := proto.Temporal_Api_Workflowservice_V1_RecordActivityTaskHeartbeatRequest{
			task_token = identifier.task_token,
			identity = async_activity_identity(client),
			namespace = client.namespace,
		}
		if details != nil do message.details = &payloads
		request = proto.encode_Temporal_Api_Workflowservice_V1_RecordActivityTaskHeartbeatRequest(&message)
		method = "RecordActivityTaskHeartbeat"
	} else {
		message := proto.Temporal_Api_Workflowservice_V1_RecordActivityTaskHeartbeatByIdRequest{
			namespace = client.namespace,
			workflow_id = identifier.workflow_id,
			run_id = identifier.run_id,
			activity_id = identifier.activity_id,
			identity = async_activity_identity(client),
		}
		if details != nil do message.details = &payloads
		request = proto.encode_Temporal_Api_Workflowservice_V1_RecordActivityTaskHeartbeatByIdRequest(&message)
		method = "RecordActivityTaskHeartbeatById"
	}
	defer delete(request)
	rpc_result := temporal_core.client_rpc_call(client.connection, .Workflow, method, request[:])
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize heartbeat response arena")
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	response := Async_Activity_Heartbeat{}
	decoded := false
	if by_token {
		value := proto.Temporal_Api_Workflowservice_V1_RecordActivityTaskHeartbeatResponse{}
		decoded = proto.decode_Temporal_Api_Workflowservice_V1_RecordActivityTaskHeartbeatResponse(
			rpc_result.protobuf,
			&value,
		)
		response = {value.cancel_requested, value.activity_paused, value.activity_reset}
	} else {
		value := proto.Temporal_Api_Workflowservice_V1_RecordActivityTaskHeartbeatByIdResponse{}
		decoded = proto.decode_Temporal_Api_Workflowservice_V1_RecordActivityTaskHeartbeatByIdResponse(
			rpc_result.protobuf,
			&value,
		)
		response = {value.cancel_requested, value.activity_paused, value.activity_reset}
	}
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed async Activity heartbeat response")
	}
	return response, {}
}

heartbeat_async_activity_json :: proc(
	client: ^Client,
	identifier: Async_Activity_Identifier,
	details: $T,
) -> (Async_Activity_Heartbeat, Error) {
	payload, encode_error := codec.encode_json(details)
	if encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode async Activity heartbeat details")
	}
	defer codec.payload_free(&payload)
	view := codec.payload_view(&payload)
	return heartbeat_async_activity_payload(client, identifier, &view)
}

package client

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:encoding/uuid"
import "core:math"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"

Standalone_Activity_Id_Reuse_Policy :: enum {
	Unspecified,
	Allow_Duplicate,
	Allow_Duplicate_Failed_Only,
	Reject_Duplicate,
}

Standalone_Activity_Id_Conflict_Policy :: enum {
	Unspecified,
	Fail,
	Use_Existing,
}

// Start_Standalone_Activity_Options borrows all values for the synchronous
// call. At least one close timeout must be positive.
Start_Standalone_Activity_Options :: struct {
	id:                        string,
	activity_type:             string,
	task_queue:                string,
	request_id:               string,
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	heartbeat_timeout:         time.Duration,
	start_delay:               time.Duration,
	retry_policy:              ^Retry_Policy,
	priority:                  ^Priority,
	id_reuse_policy:           Standalone_Activity_Id_Reuse_Policy,
	id_conflict_policy:        Standalone_Activity_Id_Conflict_Policy,
}

// Standalone_Activity_Handle owns its IDs and borrows the Client. Free the
// handle before closing the Client.
Standalone_Activity_Handle :: struct {
	client:             ^Client,
	id:                 string,
	run_id:             string,
	started_by_request: bool,
}

Standalone_Activity_Wait_Options :: struct {
	timeout:            time.Duration,
	cancellation_token: ^Client_Cancellation_Token,
}

Standalone_Activity_Poll_Result :: struct {
	run_id:    string,
	result:    codec.Payload,
	has_result: bool,
}

standalone_activity_handle_free :: proc(handle: ^Standalone_Activity_Handle) {
	if handle == nil do return
	heap := allocator_runtime.heap_allocator()
	delete(handle.id, heap)
	delete(handle.run_id, heap)
	handle^ = {}
}

standalone_activity_poll_result_free :: proc(result: ^Standalone_Activity_Poll_Result) {
	if result == nil do return
	delete(result.run_id)
	codec.payload_free(&result.result)
	result^ = {}
}

standalone_activity_handle_valid :: proc(handle: ^Standalone_Activity_Handle) -> bool {
	return handle != nil && handle.client != nil && handle.client.connection != nil &&
	       handle.client.connection.handle != nil && len(handle.id) > 0
}

get_standalone_activity_handle :: proc(
	client: ^Client,
	activity_id, run_id: string,
) -> (Standalone_Activity_Handle, Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if len(activity_id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity ID must not be empty")
	}
	heap := allocator_runtime.heap_allocator()
	owned_id, id_error := strings.clone(activity_id, heap)
	if id_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not allocate standalone Activity ID")
	}
	owned_run_id, run_id_error := strings.clone(run_id, heap)
	if run_id_error != nil {
		delete(owned_id, heap)
		return {}, error_from_string(.Malformed_Response, "could not allocate standalone Activity Run ID")
	}
	return {client = client, id = owned_id, run_id = owned_run_id}, {}
}

standalone_activity_id_reuse_policy_to_proto :: proc(
	policy: Standalone_Activity_Id_Reuse_Policy,
) -> proto.Temporal_Api_Enums_V1_ActivityIdReusePolicy {
	switch policy {
	case .Unspecified:                 return .ACTIVITY_ID_REUSE_POLICY_UNSPECIFIED
	case .Allow_Duplicate:             return .ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE
	case .Allow_Duplicate_Failed_Only: return .ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	case .Reject_Duplicate:            return .ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE
	}
	return .ACTIVITY_ID_REUSE_POLICY_UNSPECIFIED
}

standalone_activity_id_conflict_policy_to_proto :: proc(
	policy: Standalone_Activity_Id_Conflict_Policy,
) -> proto.Temporal_Api_Enums_V1_ActivityIdConflictPolicy {
	switch policy {
	case .Unspecified:  return .ACTIVITY_ID_CONFLICT_POLICY_UNSPECIFIED
	case .Fail:         return .ACTIVITY_ID_CONFLICT_POLICY_FAIL
	case .Use_Existing: return .ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING
	}
	return .ACTIVITY_ID_CONFLICT_POLICY_UNSPECIFIED
}

start_standalone_activity_options_error :: proc(options: Start_Standalone_Activity_Options) -> string {
	if len(options.id) == 0 do return "standalone Activity ID must not be empty"
	if len(options.activity_type) == 0 do return "standalone Activity type must not be empty"
	if len(options.task_queue) == 0 do return "standalone Activity task queue must not be empty"
	if options.schedule_to_close_timeout < 0 || options.schedule_to_start_timeout < 0 ||
	   options.start_to_close_timeout < 0 || options.heartbeat_timeout < 0 || options.start_delay < 0 {
		return "standalone Activity timeouts and start delay must not be negative"
	}
	if options.schedule_to_close_timeout == 0 && options.start_to_close_timeout == 0 {
		return "standalone Activity requires a schedule-to-close or start-to-close timeout"
	}
	if i32(options.id_reuse_policy) < i32(Standalone_Activity_Id_Reuse_Policy.Unspecified) ||
	   i32(options.id_reuse_policy) > i32(Standalone_Activity_Id_Reuse_Policy.Reject_Duplicate) {
		return "standalone Activity ID reuse policy is invalid"
	}
	if i32(options.id_conflict_policy) < i32(Standalone_Activity_Id_Conflict_Policy.Unspecified) ||
	   i32(options.id_conflict_policy) > i32(Standalone_Activity_Id_Conflict_Policy.Use_Existing) {
		return "standalone Activity ID conflict policy is invalid"
	}
	if options.retry_policy != nil {
		policy := options.retry_policy
		if policy.initial_interval < 0 || policy.maximum_interval < 0 || policy.maximum_attempts < 0 {
			return "standalone Activity Retry Policy values must not be negative"
		}
		if math.is_nan(policy.backoff_coefficient) || math.is_inf(policy.backoff_coefficient) ||
		   (policy.backoff_coefficient != 0 && policy.backoff_coefficient < 1) {
			return "standalone Activity Retry Policy backoff coefficient must be zero or at least one"
		}
		if policy.maximum_interval > 0 && policy.initial_interval > 0 &&
		   policy.maximum_interval < policy.initial_interval {
			return "standalone Activity Retry Policy maximum interval must not be shorter than its initial interval"
		}
	}
	if options.priority != nil {
		if options.priority.priority_key < 0 do return "standalone Activity priority key must not be negative"
		if len(options.priority.fairness_key) > 64 do return "standalone Activity priority fairness key must not exceed 64 bytes"
		if math.is_nan(options.priority.fairness_weight) || math.is_inf(options.priority.fairness_weight) ||
		   options.priority.fairness_weight < 0 {
			return "standalone Activity priority fairness weight must be finite and non-negative"
		}
	}
	return ""
}

// encode_start_standalone_activity_request is public for exact wire tests.
// The caller owns the returned bytes.
encode_start_standalone_activity_request :: proc(
	namespace, identity: string,
	input: codec.Payload_View,
	options: Start_Standalone_Activity_Options,
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Temporal namespace must not be empty")
	}
	if message := start_standalone_activity_options_error(options); len(message) > 0 {
		return nil, error_from_string(.Invalid_Argument, message)
	}
	request_id := options.request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(request_id) == 0 {
		request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	}
	activity_type := proto.Temporal_Api_Common_V1_ActivityType{name = options.activity_type}
	task_queue := proto.Temporal_Api_Taskqueue_V1_TaskQueue{name = options.task_queue}
	input_payload := proto.encode_payload(input)
	defer delete(input_payload.metadata)
	defer delete(input_payload.external_payloads)
	input_payloads := proto.Temporal_Api_Common_V1_Payloads{}
	append(&input_payloads.payloads, input_payload)
	defer delete(input_payloads.payloads)
	request := proto.Temporal_Api_Workflowservice_V1_StartActivityExecutionRequest{
		namespace = namespace,
		identity = identity,
		request_id = request_id,
		activity_id = options.id,
		activity_type = &activity_type,
		task_queue = &task_queue,
		input = &input_payloads,
		id_reuse_policy = standalone_activity_id_reuse_policy_to_proto(options.id_reuse_policy),
		id_conflict_policy = standalone_activity_id_conflict_policy_to_proto(options.id_conflict_policy),
	}
	schedule_to_close := duration_proto(options.schedule_to_close_timeout)
	schedule_to_start := duration_proto(options.schedule_to_start_timeout)
	start_to_close := duration_proto(options.start_to_close_timeout)
	heartbeat := duration_proto(options.heartbeat_timeout)
	start_delay := duration_proto(options.start_delay)
	if options.schedule_to_close_timeout > 0 do request.schedule_to_close_timeout = &schedule_to_close
	if options.schedule_to_start_timeout > 0 do request.schedule_to_start_timeout = &schedule_to_start
	if options.start_to_close_timeout > 0 do request.start_to_close_timeout = &start_to_close
	if options.heartbeat_timeout > 0 do request.heartbeat_timeout = &heartbeat
	if options.start_delay > 0 do request.start_delay = &start_delay
	retry_policy := proto.Temporal_Api_Common_V1_RetryPolicy{}
	retry_initial := proto.Google_Protobuf_Duration{}
	retry_maximum := proto.Google_Protobuf_Duration{}
	defer delete(retry_policy.non_retryable_error_types)
	if options.retry_policy != nil {
		policy := options.retry_policy
		retry_policy.backoff_coefficient = policy.backoff_coefficient
		retry_policy.maximum_attempts = policy.maximum_attempts
		if policy.initial_interval > 0 {
			retry_initial = duration_proto(policy.initial_interval)
			retry_policy.initial_interval = &retry_initial
		}
		if policy.maximum_interval > 0 {
			retry_maximum = duration_proto(policy.maximum_interval)
			retry_policy.maximum_interval = &retry_maximum
		}
		for error_type in policy.non_retryable_error_types {
			append(&retry_policy.non_retryable_error_types, error_type)
		}
		request.retry_policy = &retry_policy
	}
	priority := proto.Temporal_Api_Common_V1_Priority{}
	if options.priority != nil {
		priority = {
			priority_key = options.priority.priority_key,
			fairness_key = options.priority.fairness_key,
			fairness_weight = options.priority.fairness_weight,
		}
		request.priority = &priority
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_StartActivityExecutionRequest(&request), {}
}

start_standalone_activity_payload :: proc(
	client: ^Client,
	input: codec.Payload_View,
	options: Start_Standalone_Activity_Options,
) -> (Standalone_Activity_Handle, Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	request, encode_error := encode_start_standalone_activity_request(
		client.namespace,
		client_request_identity(client),
		input,
		options,
	)
	if encode_error.message != nil do return {}, encode_error
	defer delete(request)
	rpc_result := temporal_core.client_rpc_call(client.connection, .Workflow, "StartActivityExecution", request[:])
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return {}, error_from_core(&rpc_result.error)
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize standalone Activity start arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_StartActivityExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_StartActivityExecutionResponse(rpc_result.protobuf, &response)
	context.allocator = previous_allocator
	if !decoded || len(response.run_id) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed standalone Activity start response")
	}
	handle, handle_error := get_standalone_activity_handle(client, options.id, response.run_id)
	if handle_error.message != nil do return {}, handle_error
	handle.started_by_request = response.started
	return handle, {}
}

start_standalone_activity_json :: proc(
	client: ^Client,
	input: $T,
	options: Start_Standalone_Activity_Options,
) -> (Standalone_Activity_Handle, Error) {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode standalone Activity input")
	}
	defer codec.payload_free(&payload)
	return start_standalone_activity_payload(client, codec.payload_view(&payload), options)
}

standalone_activity_failure_kind :: proc(failure: ^proto.Temporal_Api_Failure_V1_Failure) -> Error_Kind {
	kind := Error_Kind.Standalone_Activity_Failed
	current := failure
	for current != nil {
		#partial switch info in current.failure_info {
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info:
			kind = .Standalone_Activity_Canceled
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Terminated_Failure_Info:
			kind = .Standalone_Activity_Terminated
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Timeout_Failure_Info:
			kind = .Standalone_Activity_Timed_Out
		}
		if kind != .Standalone_Activity_Failed do break
		current = current.cause
	}
	return kind
}

standalone_activity_failure_error :: proc(failure: ^proto.Temporal_Api_Failure_V1_Failure) -> Error {
	if failure == nil {
		return error_from_string(.Malformed_Response, "standalone Activity failure outcome is missing")
	}
	message := failure.message
	current := failure.cause
	for len(message) == 0 && current != nil {
		message = current.message
		current = current.cause
	}
	if len(message) == 0 do message = "standalone Activity failed"
	details := proto.encode_Temporal_Api_Failure_V1_Failure(failure)
	return {
		kind = standalone_activity_failure_kind(failure),
		message = bytes.clone(transmute([]u8)message),
		details = details[:],
	}
}

decode_standalone_activity_poll_response :: proc(
	data: []u8,
) -> (result: Standalone_Activity_Poll_Result, empty: bool, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, false, error_from_string(.Malformed_Response, "could not initialize standalone Activity poll arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_PollActivityExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_PollActivityExecutionResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, false, error_from_string(.Malformed_Response, "Temporal returned a malformed standalone Activity poll response")
	}
	if response.outcome == nil {
		return {}, true, {}
	}
	owned_run_id, run_id_error := strings.clone(response.run_id)
	if run_id_error != nil {
		return {}, false, error_from_string(.Malformed_Response, "could not allocate standalone Activity Run ID")
	}
	result.run_id = owned_run_id
	#partial switch outcome in response.outcome.value {
	case proto.Temporal_Api_Activity_V1_ActivityExecutionOutcome_Value_Result:
		if outcome.value == nil {
			standalone_activity_poll_result_free(&result)
			return {}, false, error_from_string(.Malformed_Response, "standalone Activity result outcome is missing")
		}
		if len(outcome.value.payloads) > 1 {
			standalone_activity_poll_result_free(&result)
			return {}, false, error_from_string(.Malformed_Response, "standalone Activity returned more than one result Payload")
		}
		if len(outcome.value.payloads) == 1 {
			clone_error: codec.Error
			result.result, clone_error = proto.clone_payload(&outcome.value.payloads[0])
			if clone_error != .None {
				standalone_activity_poll_result_free(&result)
				return {}, false, error_from_string(.Payload_Conversion, "could not preserve standalone Activity result")
			}
			result.has_result = true
		}
	case proto.Temporal_Api_Activity_V1_ActivityExecutionOutcome_Value_Failure:
		activity_error := standalone_activity_failure_error(outcome.value)
		return result, false, activity_error
	case:
		standalone_activity_poll_result_free(&result)
		return {}, false, error_from_string(.Malformed_Response, "standalone Activity outcome has no result or failure")
	}
	return result, false, {}
}

poll_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	options: Standalone_Activity_Wait_Options = {},
) -> (Standalone_Activity_Poll_Result, Error) {
	if !standalone_activity_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	if options.timeout < 0 {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity wait timeout must not be negative")
	}
	if options.cancellation_token != nil && options.cancellation_token.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity wait cancellation token is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_PollActivityExecutionRequest{
		namespace = handle.client.namespace,
		activity_id = handle.id,
		run_id = handle.run_id,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_PollActivityExecutionRequest(&request)
	defer delete(request_bytes)
	started := time.tick_now()
	for {
		timeout_millis: u32
		if options.timeout > 0 {
			remaining := options.timeout - time.tick_since(started)
			if remaining <= 0 {
				return {}, error_from_string(.Wait_Timed_Out, "standalone Activity wait timed out")
			}
			millis := i64(remaining / time.Millisecond)
			if remaining % time.Millisecond != 0 do millis += 1
			if millis > i64(4_294_967_295) do millis = i64(4_294_967_295)
			timeout_millis = u32(millis)
		}
		rpc_result := temporal_core.client_rpc_call(
			handle.client.connection,
			.Workflow,
			"PollActivityExecution",
			request_bytes[:],
			retry = options.timeout == 0,
			timeout_millis = timeout_millis,
			cancellation_token = options.cancellation_token,
		)
		if rpc_result.error.message != nil {
			delete(rpc_result.protobuf)
			deadline_reached := options.timeout > 0 && time.tick_since(started) >= options.timeout
			if options.timeout > 0 && update_poll_failure_is_timeout(
				rpc_result.error.status_code,
				string(rpc_result.error.message),
				deadline_reached,
			) {
				return {}, error_from_core(&rpc_result.error, .Wait_Timed_Out)
			}
			if options.cancellation_token != nil && rpc_result.error.status_code == 0 &&
			   string(rpc_result.error.message) == "Cancelled" {
				return {}, error_from_core(&rpc_result.error, .Wait_Canceled)
			}
			return {}, error_from_core(&rpc_result.error)
		}
		result, empty, decode_error := decode_standalone_activity_poll_response(rpc_result.protobuf)
		delete(rpc_result.protobuf)
		if decode_error.message != nil do return result, decode_error
		if empty do continue
		return result, {}
	}
}

get_standalone_activity_result_payload :: proc(
	handle: ^Standalone_Activity_Handle,
	options: Standalone_Activity_Wait_Options = {},
) -> (codec.Payload, Error) {
	result, poll_error := poll_standalone_activity(handle, options)
	if poll_error.message != nil {
		standalone_activity_poll_result_free(&result)
		return {}, poll_error
	}
	if !result.has_result {
		standalone_activity_poll_result_free(&result)
		return {}, error_from_string(.Malformed_Response, "standalone Activity completed without a result")
	}
	payload := result.result
	result.result = {}
	standalone_activity_poll_result_free(&result)
	return payload, {}
}

get_standalone_activity_result_json :: proc(
	handle: ^Standalone_Activity_Handle,
	value: ^$T,
	options: Standalone_Activity_Wait_Options = {},
) -> Error {
	if value == nil {
		return error_from_string(.Invalid_Argument, "standalone Activity result output pointer is required")
	}
	payload, result_error := get_standalone_activity_result_payload(handle, options)
	if result_error.message != nil do return result_error
	defer codec.payload_free(&payload)
	if codec.decode_json(codec.payload_view(&payload), value) != .None {
		return error_from_string(.Payload_Conversion, "could not decode standalone Activity result")
	}
	return {}
}

cancel_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	reason := "",
	request_id := "",
) -> Error {
	if !standalone_activity_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	actual_request_id := request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(actual_request_id) == 0 do actual_request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	request := proto.Temporal_Api_Workflowservice_V1_RequestCancelActivityExecutionRequest{
		namespace = handle.client.namespace,
		activity_id = handle.id,
		run_id = handle.run_id,
		identity = client_request_identity(handle.client),
		request_id = actual_request_id,
		reason = reason,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_RequestCancelActivityExecutionRequest(&request)
	defer delete(request_bytes)
	return standalone_activity_empty_rpc(handle.client, "RequestCancelActivityExecution", request_bytes[:])
}

terminate_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	reason := "",
	request_id := "",
) -> Error {
	if !standalone_activity_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	actual_request_id := request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(actual_request_id) == 0 do actual_request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	request := proto.Temporal_Api_Workflowservice_V1_TerminateActivityExecutionRequest{
		namespace = handle.client.namespace,
		activity_id = handle.id,
		run_id = handle.run_id,
		identity = client_request_identity(handle.client),
		request_id = actual_request_id,
		reason = reason,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_TerminateActivityExecutionRequest(&request)
	defer delete(request_bytes)
	return standalone_activity_empty_rpc(handle.client, "TerminateActivityExecution", request_bytes[:])
}

delete_standalone_activity :: proc(handle: ^Standalone_Activity_Handle) -> Error {
	if !standalone_activity_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_DeleteActivityExecutionRequest{
		namespace = handle.client.namespace,
		activity_id = handle.id,
		run_id = handle.run_id,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_DeleteActivityExecutionRequest(&request)
	defer delete(request_bytes)
	return standalone_activity_empty_rpc(handle.client, "DeleteActivityExecution", request_bytes[:])
}

standalone_activity_empty_rpc :: proc(client: ^Client, method: string, request: []u8) -> Error {
	rpc_result := temporal_core.client_rpc_call(client.connection, .Workflow, method, request)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return error_from_core(&rpc_result.error)
	return {}
}

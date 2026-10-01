package client

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:encoding/uuid"
import "core:mem"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"

Nexus_Operation_Id_Reuse_Policy :: enum {
	Unspecified,
	Allow_Duplicate,
	Allow_Duplicate_Failed_Only,
	Reject_Duplicate,
}

Nexus_Operation_Id_Conflict_Policy :: enum {
	Unspecified,
	Fail,
	Use_Existing,
}

Nexus_Operation_Wait_Stage :: enum {
	Unspecified,
	Started,
	Closed,
}

// Nexus_Operation_Header is a protocol string header delivered to the Nexus
// handler. It is not a Temporal Payload Header.
Nexus_Operation_Header :: struct {
	key:   string,
	value: string,
}

// Start_Nexus_Operation_Options borrows all strings and slices for the duration
// of the synchronous start call. The returned handle owns its IDs.
Start_Nexus_Operation_Options :: struct {
	id:                        string,
	request_id:                string,
	endpoint:                  string,
	service:                   string,
	operation:                 string,
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	id_reuse_policy:           Nexus_Operation_Id_Reuse_Policy,
	id_conflict_policy:        Nexus_Operation_Id_Conflict_Policy,
	headers:                   []Nexus_Operation_Header,
	links:                     []Link,
	completion_callbacks:      []Nexus_Completion_Callback,
}

// Nexus_Operation_Handle owns its operation/run IDs but borrows the Client.
// Keep the Client open until no operation using the handle remains.
Nexus_Operation_Handle :: struct {
	client:             ^Client,
	id:                 string,
	run_id:             string,
	started_by_request: bool,
}

Nexus_Operation_Wait_Options :: struct {
	timeout:            time.Duration,
	cancellation_token: ^Client_Cancellation_Token,
}

Nexus_Operation_Poll_Result :: struct {
	wait_stage:      Nexus_Operation_Wait_Stage,
	operation_token: string,
	result:          codec.Payload,
	has_result:      bool,
}

Nexus_Operation_Status :: enum i32 {
	Unspecified = 0,
	Running = 1,
	Completed = 2,
	Failed = 3,
	Canceled = 4,
	Terminated = 5,
	Timed_Out = 6,
}

Pending_Nexus_Operation_State :: enum i32 {
	Unspecified = 0,
	Scheduled = 1,
	Backing_Off = 2,
	Started = 3,
	Blocked = 4,
}

Nexus_Operation_Cancellation_State :: enum i32 {
	Unspecified = 0,
	Scheduled = 1,
	Backing_Off = 2,
	Succeeded = 3,
	Failed = 4,
	Timed_Out = 5,
	Blocked = 6,
}

Nexus_Operation_Cancellation_Description :: struct {
	present:                    bool,
	requested_time:             Optional_Time,
	state:                      Nexus_Operation_Cancellation_State,
	attempt:                    i32,
	last_attempt_complete_time: Optional_Time,
	next_attempt_schedule_time: Optional_Time,
	last_failure_message:       string,
	last_failure_proto:         []u8,
	blocked_reason:             string,
	reason:                     string,
}

Describe_Nexus_Operation_Options :: struct {
	include_input:      bool,
	include_outcome:    bool,
	long_poll_token:    []u8,
	timeout:            time.Duration,
	cancellation_token: ^Client_Cancellation_Token,
}

Nexus_Operation_Description :: struct {
	operation_id:               string,
	run_id:                     string,
	endpoint:                   string,
	service:                    string,
	operation:                  string,
	identity:                   string,
	request_id:                 string,
	operation_token:            string,
	status:                     Nexus_Operation_Status,
	state:                      Pending_Nexus_Operation_State,
	schedule_to_close_timeout:  Optional_Duration,
	schedule_to_start_timeout:  Optional_Duration,
	start_to_close_timeout:     Optional_Duration,
	attempt:                    i32,
	schedule_time:              Optional_Time,
	expiration_time:            Optional_Time,
	close_time:                 Optional_Time,
	last_attempt_complete_time: Optional_Time,
	next_attempt_schedule_time: Optional_Time,
	execution_duration:         Optional_Duration,
	last_failure_message:       string,
	last_failure_proto:         []u8,
	blocked_reason:             string,
	state_transition_count:     i64,
	state_size_bytes:           i64,
	cancellation:               Nexus_Operation_Cancellation_Description,
	headers:                    [dynamic]Nexus_Operation_Header,
	search_attributes:          [dynamic]Named_Payload,
	static_summary:             string,
	has_static_summary:         bool,
	static_details:             string,
	has_static_details:         bool,
	input:                      codec.Payload,
	has_input:                  bool,
	result:                     codec.Payload,
	has_result:                 bool,
	failure_kind:               Error_Kind,
	failure_message:            string,
	failure_proto:              []u8,
	has_failure:                bool,
	long_poll_token:            []u8,
	allocator:                  mem.Allocator,
}

nexus_operation_handle_free :: proc(handle: ^Nexus_Operation_Handle) {
	if handle == nil do return
	heap := allocator_runtime.heap_allocator()
	delete(handle.id, heap)
	delete(handle.run_id, heap)
	handle^ = {}
}

nexus_operation_poll_result_free :: proc(result: ^Nexus_Operation_Poll_Result) {
	if result == nil do return
	delete(result.operation_token)
	codec.payload_free(&result.result)
	result^ = {}
}

nexus_operation_description_free :: proc(description: ^Nexus_Operation_Description) {
	if description == nil do return
	allocator := description.allocator
	if allocator.procedure == nil {
		description^ = {}
		return
	}
	delete(description.operation_id, allocator)
	delete(description.run_id, allocator)
	delete(description.endpoint, allocator)
	delete(description.service, allocator)
	delete(description.operation, allocator)
	delete(description.identity, allocator)
	delete(description.request_id, allocator)
	delete(description.operation_token, allocator)
	delete(description.last_failure_message, allocator)
	delete(description.last_failure_proto, allocator)
	delete(description.blocked_reason, allocator)
	delete(description.cancellation.last_failure_message, allocator)
	delete(description.cancellation.last_failure_proto, allocator)
	delete(description.cancellation.blocked_reason, allocator)
	delete(description.cancellation.reason, allocator)
	for &header in description.headers {
		delete(header.key, allocator)
		delete(header.value, allocator)
	}
	for &attribute in description.search_attributes {
		delete(attribute.key, allocator)
		codec.payload_free(&attribute.payload)
	}
	delete(description.headers)
	delete(description.search_attributes)
	delete(description.static_summary, allocator)
	delete(description.static_details, allocator)
	codec.payload_free(&description.input)
	codec.payload_free(&description.result)
	delete(description.failure_message, allocator)
	delete(description.failure_proto, allocator)
	delete(description.long_poll_token, allocator)
	description^ = {}
}

nexus_operation_handle_valid :: proc(handle: ^Nexus_Operation_Handle) -> bool {
	return handle != nil && handle.client != nil && handle.client.connection != nil &&
	       handle.client.connection.handle != nil && len(handle.id) > 0
}

get_nexus_operation_handle :: proc(
	client: ^Client,
	operation_id, run_id: string,
) -> (Nexus_Operation_Handle, Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if len(operation_id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation ID must not be empty")
	}
	heap := allocator_runtime.heap_allocator()
	owned_id, id_error := strings.clone(operation_id, heap)
	if id_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not allocate Nexus operation ID")
	}
	owned_run_id, run_id_error := strings.clone(run_id, heap)
	if run_id_error != nil {
		delete(owned_id, heap)
		return {}, error_from_string(.Malformed_Response, "could not allocate Nexus operation Run ID")
	}
	return {client = client, id = owned_id, run_id = owned_run_id}, {}
}

nexus_operation_id_reuse_policy_to_proto :: proc(
	policy: Nexus_Operation_Id_Reuse_Policy,
) -> proto.Temporal_Api_Enums_V1_NexusOperationIdReusePolicy {
	switch policy {
	case .Unspecified:                  return .NEXUS_OPERATION_ID_REUSE_POLICY_UNSPECIFIED
	case .Allow_Duplicate:              return .NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE
	case .Allow_Duplicate_Failed_Only:  return .NEXUS_OPERATION_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	case .Reject_Duplicate:             return .NEXUS_OPERATION_ID_REUSE_POLICY_REJECT_DUPLICATE
	}
	return .NEXUS_OPERATION_ID_REUSE_POLICY_UNSPECIFIED
}

nexus_operation_id_conflict_policy_to_proto :: proc(
	policy: Nexus_Operation_Id_Conflict_Policy,
) -> proto.Temporal_Api_Enums_V1_NexusOperationIdConflictPolicy {
	switch policy {
	case .Unspecified:  return .NEXUS_OPERATION_ID_CONFLICT_POLICY_UNSPECIFIED
	case .Fail:         return .NEXUS_OPERATION_ID_CONFLICT_POLICY_FAIL
	case .Use_Existing: return .NEXUS_OPERATION_ID_CONFLICT_POLICY_USE_EXISTING
	}
	return .NEXUS_OPERATION_ID_CONFLICT_POLICY_UNSPECIFIED
}

nexus_operation_wait_stage_to_proto :: proc(
	stage: Nexus_Operation_Wait_Stage,
) -> proto.Temporal_Api_Enums_V1_NexusOperationWaitStage {
	switch stage {
	case .Unspecified: return .NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED
	case .Started:     return .NEXUS_OPERATION_WAIT_STAGE_STARTED
	case .Closed:      return .NEXUS_OPERATION_WAIT_STAGE_CLOSED
	}
	return .NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED
}

nexus_operation_wait_stage_from_proto :: proc(
	stage: proto.Temporal_Api_Enums_V1_NexusOperationWaitStage,
) -> (Nexus_Operation_Wait_Stage, bool) {
	switch stage {
	case .NEXUS_OPERATION_WAIT_STAGE_UNSPECIFIED: return .Unspecified, true
	case .NEXUS_OPERATION_WAIT_STAGE_STARTED:     return .Started, true
	case .NEXUS_OPERATION_WAIT_STAGE_CLOSED:      return .Closed, true
	}
	return .Unspecified, false
}

start_nexus_operation_options_error :: proc(options: Start_Nexus_Operation_Options) -> string {
	if len(options.id) == 0 do return "Nexus operation ID must not be empty"
	if len(options.endpoint) == 0 do return "Nexus endpoint must not be empty"
	if len(options.service) == 0 do return "Nexus service must not be empty"
	if len(options.operation) == 0 do return "Nexus operation name must not be empty"
	if options.schedule_to_close_timeout <= 0 {
		return "Nexus operation schedule-to-close timeout must be positive"
	}
	if options.schedule_to_start_timeout < 0 || options.start_to_close_timeout < 0 {
		return "Nexus operation timeouts must not be negative"
	}
	if i32(options.id_reuse_policy) < i32(Nexus_Operation_Id_Reuse_Policy.Unspecified) ||
	   i32(options.id_reuse_policy) > i32(Nexus_Operation_Id_Reuse_Policy.Reject_Duplicate) {
		return "Nexus operation ID reuse policy is invalid"
	}
	if i32(options.id_conflict_policy) < i32(Nexus_Operation_Id_Conflict_Policy.Unspecified) ||
	   i32(options.id_conflict_policy) > i32(Nexus_Operation_Id_Conflict_Policy.Use_Existing) {
		return "Nexus operation ID conflict policy is invalid"
	}
	for header in options.headers {
		if len(header.key) == 0 do return "Nexus operation header keys must not be empty"
	}
	for link in options.links {
		if message := link_error(link); len(message) > 0 do return message
	}
	for callback in options.completion_callbacks {
		if len(callback.url) == 0 do return "Nexus completion callback URL must not be empty"
		for header in callback.headers {
			if len(header.key) == 0 do return "Nexus completion callback header keys must not be empty"
		}
		for link in callback.links {
			if message := link_error(link); len(message) > 0 do return message
		}
		for link in callback.workflow_event_links {
			if message := workflow_event_link_error(link); len(message) > 0 do return message
		}
	}
	return ""
}

// encode_start_nexus_operation_request is public for exact wire-contract tests.
// Returned bytes are owned by the caller.
encode_start_nexus_operation_request :: proc(
	namespace, identity: string,
	input: codec.Payload_View,
	options: Start_Nexus_Operation_Options,
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 do return nil, error_from_string(.Invalid_Argument, "Temporal namespace must not be empty")
	if message := start_nexus_operation_options_error(options); len(message) > 0 {
		return nil, error_from_string(.Invalid_Argument, message)
	}
	request_id := options.request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(request_id) == 0 {
		request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	}
	input_payload := proto.encode_payload(input)
	defer delete(input_payload.metadata)
	defer delete(input_payload.external_payloads)
	schedule_to_close := duration_proto(options.schedule_to_close_timeout)
	schedule_to_start := duration_proto(options.schedule_to_start_timeout)
	start_to_close := duration_proto(options.start_to_close_timeout)
	request := proto.Temporal_Api_Workflowservice_V1_StartNexusOperationExecutionRequest{
		namespace = namespace,
		identity = identity,
		request_id = request_id,
		operation_id = options.id,
		endpoint = options.endpoint,
		service = options.service,
		operation = options.operation,
		schedule_to_close_timeout = &schedule_to_close,
		input = &input_payload,
		id_reuse_policy = nexus_operation_id_reuse_policy_to_proto(options.id_reuse_policy),
		id_conflict_policy = nexus_operation_id_conflict_policy_to_proto(options.id_conflict_policy),
	}
	if options.schedule_to_start_timeout > 0 do request.schedule_to_start_timeout = &schedule_to_start
	if options.start_to_close_timeout > 0 do request.start_to_close_timeout = &start_to_close
	defer delete(request.nexus_header)
	for header in options.headers {
		append(&request.nexus_header, proto.Temporal_Api_Workflowservice_V1_StartNexusOperationExecutionRequest_NexusHeaderEntry{
			key = header.key,
			value = header.value,
		})
	}

	total_link_count := len(options.links)
	for callback in options.completion_callbacks {
		total_link_count += len(callback.links) + len(callback.workflow_event_links)
	}
	link_storage := encoded_link_storage_make(total_link_count)
	defer encoded_link_storage_free(&link_storage)
	defer delete(request.links)
	for link in options.links do append(&request.links, encode_link(link, &link_storage))

	callback_values := make([]proto.Temporal_Api_Common_V1_Callback_Nexus, len(options.completion_callbacks))
	callback_headers := make([][dynamic]proto.Temporal_Api_Common_V1_Callback_Nexus_HeaderEntry, len(options.completion_callbacks))
	callback_links := make([][dynamic]proto.Temporal_Api_Common_V1_Link, len(options.completion_callbacks))
	defer {
		for headers in callback_headers do delete(headers)
		for links in callback_links do delete(links)
		delete(callback_links)
		delete(callback_headers)
		delete(callback_values)
		delete(request.completion_callbacks)
	}
	for callback, index in options.completion_callbacks {
		callback_values[index].url = callback.url
		for header in callback.headers {
			append(&callback_headers[index], proto.Temporal_Api_Common_V1_Callback_Nexus_HeaderEntry{
				key = header.key,
				value = header.value,
			})
		}
		callback_values[index].header = callback_headers[index]
		for link in callback.links do append(&callback_links[index], encode_link(link, &link_storage))
		for link in callback.workflow_event_links {
			append(&callback_links[index], encode_link({kind = .Workflow_Event, workflow_event = link}, &link_storage))
		}
		append(&request.completion_callbacks, proto.Temporal_Api_Common_V1_Callback{
			links = callback_links[index],
			variant = proto.Temporal_Api_Common_V1_Callback_Variant_Nexus{value = &callback_values[index]},
		})
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_StartNexusOperationExecutionRequest(&request), {}
}

start_nexus_operation_payload :: proc(
	client: ^Client,
	input: codec.Payload_View,
	options: Start_Nexus_Operation_Options,
) -> (Nexus_Operation_Handle, Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	request_bytes, encode_error := encode_start_nexus_operation_request(
		client.namespace,
		client.identity,
		input,
		options,
	)
	if encode_error.message != nil do return {}, encode_error
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"StartNexusOperationExecution",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return {}, error_from_core(&rpc_result.error)

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Nexus operation start arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_StartNexusOperationExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_StartNexusOperationExecutionResponse(
		rpc_result.protobuf,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded || len(response.run_id) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Nexus operation start response")
	}
	handle, handle_error := get_nexus_operation_handle(client, options.id, response.run_id)
	if handle_error.message != nil do return {}, handle_error
	handle.started_by_request = response.started
	return handle, {}
}

start_nexus_operation_json :: proc(
	client: ^Client,
	input: $T,
	options: Start_Nexus_Operation_Options,
) -> (Nexus_Operation_Handle, Error) {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode Nexus operation input")
	}
	defer codec.payload_free(&payload)
	return start_nexus_operation_payload(client, codec.payload_view(&payload), options)
}

nexus_operation_failure_kind :: proc(
	failure: ^proto.Temporal_Api_Failure_V1_Failure,
) -> Error_Kind {
	kind := Error_Kind.Nexus_Operation_Failed
	current := failure
	for current != nil {
		#partial switch info in current.failure_info {
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info:
			kind = .Nexus_Operation_Canceled
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Terminated_Failure_Info:
			kind = .Nexus_Operation_Terminated
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Timeout_Failure_Info:
			kind = .Nexus_Operation_Timed_Out
		}
		if kind != .Nexus_Operation_Failed do break
		current = current.cause
	}
	return kind
}

nexus_operation_failure_error :: proc(
	failure: ^proto.Temporal_Api_Failure_V1_Failure,
) -> Error {
	if failure == nil {
		return error_from_string(.Malformed_Response, "Nexus operation failure outcome is missing")
	}
	kind := nexus_operation_failure_kind(failure)
	message := failure.message
	if len(message) == 0 {
		cause := failure.cause
		for cause != nil && len(message) == 0 {
			message = cause.message
			cause = cause.cause
		}
	}
	if len(message) == 0 do message = "Nexus operation failed"
	details := proto.encode_Temporal_Api_Failure_V1_Failure(failure)
	return {kind = kind, message = bytes.clone(transmute([]u8)message), details = details[:]}
}

nexus_operation_status_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_NexusOperationExecutionStatus,
) -> (Nexus_Operation_Status, bool) {
	numeric := i32(value)
	if numeric < i32(Nexus_Operation_Status.Unspecified) || numeric > i32(Nexus_Operation_Status.Timed_Out) {
		return {}, false
	}
	return Nexus_Operation_Status(numeric), true
}

pending_nexus_operation_state_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_PendingNexusOperationState,
) -> (Pending_Nexus_Operation_State, bool) {
	numeric := i32(value)
	if numeric < i32(Pending_Nexus_Operation_State.Unspecified) || numeric > i32(Pending_Nexus_Operation_State.Blocked) {
		return {}, false
	}
	return Pending_Nexus_Operation_State(numeric), true
}

nexus_operation_cancellation_state_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_NexusOperationCancellationState,
) -> (Nexus_Operation_Cancellation_State, bool) {
	numeric := i32(value)
	if numeric < i32(Nexus_Operation_Cancellation_State.Unspecified) || numeric > i32(Nexus_Operation_Cancellation_State.Blocked) {
		return {}, false
	}
	return Nexus_Operation_Cancellation_State(numeric), true
}

nexus_operation_clone_failure_proto :: proc(
	failure: ^proto.Temporal_Api_Failure_V1_Failure,
	allocator: mem.Allocator,
) -> ([]u8, bool) {
	if failure == nil do return nil, true
	encoded := proto.encode_Temporal_Api_Failure_V1_Failure(failure)
	defer delete(encoded)
	return bytes.clone(encoded[:], allocator), true
}

decode_nexus_operation_description :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (description: Nexus_Operation_Description, error: Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation description allocator is not initialized")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Nexus operation Describe arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_DescribeNexusOperationExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_DescribeNexusOperationExecutionResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded || response.info == nil || len(response.info.operation_id) == 0 || len(response.run_id) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Nexus operation Describe response")
	}

	description.allocator = allocator
	description.headers = make([dynamic]Nexus_Operation_Header, allocator)
	description.search_attributes = make([dynamic]Named_Payload, allocator)
	failed := false
	defer if failed {
		nexus_operation_description_free(&description)
	}
	info := response.info
	status_ok, state_ok: bool
	description.status, status_ok = nexus_operation_status_from_proto(info.status)
	description.state, state_ok = pending_nexus_operation_state_from_proto(info.state)
	if !status_ok || !state_ok {
		failed = true
		return {}, error_from_string(.Malformed_Response, "Temporal returned an unknown Nexus operation state")
	}

	clone_targets := []struct {source: string, target: ^string}{
		{info.operation_id, &description.operation_id},
		{response.run_id, &description.run_id},
		{info.endpoint, &description.endpoint},
		{info.service, &description.service},
		{info.operation, &description.operation},
		{info.identity, &description.identity},
		{info.request_id, &description.request_id},
		{info.operation_token, &description.operation_token},
		{info.blocked_reason, &description.blocked_reason},
	}
	for target in clone_targets {
		ok: bool
		target.target^, ok = description_clone_string(target.source, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate Nexus operation description")
		}
	}
	description.schedule_to_close_timeout = description_duration(info.schedule_to_close_timeout)
	description.schedule_to_start_timeout = description_duration(info.schedule_to_start_timeout)
	description.start_to_close_timeout = description_duration(info.start_to_close_timeout)
	description.attempt = info.attempt
	description.schedule_time = description_time(info.schedule_time)
	description.expiration_time = description_time(info.expiration_time)
	description.close_time = description_time(info.close_time)
	description.last_attempt_complete_time = description_time(info.last_attempt_complete_time)
	description.next_attempt_schedule_time = description_time(info.next_attempt_schedule_time)
	description.execution_duration = description_duration(info.execution_duration)
	description.state_transition_count = info.state_transition_count
	description.state_size_bytes = info.state_size_bytes
	ok: bool
	description.last_failure_message, ok = description_failure_message(info.last_attempt_failure, allocator)
	if ok {
		description.last_failure_proto, ok = nexus_operation_clone_failure_proto(info.last_attempt_failure, allocator)
	}
	if !ok {
		failed = true
		return {}, error_from_string(.Malformed_Response, "could not preserve Nexus operation attempt failure")
	}

	if info.cancellation_info != nil {
		source := info.cancellation_info
		description.cancellation.present = true
		description.cancellation.requested_time = description_time(source.requested_time)
		description.cancellation.state, ok = nexus_operation_cancellation_state_from_proto(source.state)
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "Temporal returned an unknown Nexus cancellation state")
		}
		description.cancellation.attempt = source.attempt
		description.cancellation.last_attempt_complete_time = description_time(source.last_attempt_complete_time)
		description.cancellation.next_attempt_schedule_time = description_time(source.next_attempt_schedule_time)
		description.cancellation.last_failure_message, ok = description_failure_message(source.last_attempt_failure, allocator)
		if ok {
			description.cancellation.last_failure_proto, ok = nexus_operation_clone_failure_proto(source.last_attempt_failure, allocator)
		}
		if ok {
			description.cancellation.blocked_reason, ok = description_clone_string(source.blocked_reason, allocator)
		}
		if ok {
			description.cancellation.reason, ok = description_clone_string(source.reason, allocator)
		}
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate Nexus cancellation description")
		}
	}

	for source in info.nexus_header {
		header := Nexus_Operation_Header{}
		header.key, ok = description_clone_string(source.key, allocator)
		if ok do header.value, ok = description_clone_string(source.value, allocator)
		if !ok {
			delete(header.key, allocator)
			delete(header.value, allocator)
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate Nexus operation header")
		}
		append(&description.headers, header)
	}
	if info.search_attributes != nil {
		for source in info.search_attributes.indexed_fields {
			attribute: Named_Payload
			attribute, ok = description_clone_named_payload(source.key, source.value, allocator)
			if !ok {
				failed = true
				return {}, error_from_string(.Payload_Conversion, "could not copy Nexus operation Search Attribute")
			}
			append(&description.search_attributes, attribute)
		}
	}
	if info.user_metadata != nil {
		description.static_summary, description.has_static_summary, ok = description_decode_user_metadata_string(
			info.user_metadata.summary,
			allocator,
		)
		if ok {
			description.static_details, description.has_static_details, ok = description_decode_user_metadata_string(
				info.user_metadata.details,
				allocator,
			)
		}
		if !ok {
			failed = true
			return {}, error_from_string(.Payload_Conversion, "could not decode Nexus operation user metadata")
		}
	}
	if response.input != nil {
		description.input, ok = description_clone_payload(response.input, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Payload_Conversion, "could not copy Nexus operation input")
		}
		description.has_input = true
	}
	#partial switch outcome in response.outcome {
	case proto.Temporal_Api_Workflowservice_V1_DescribeNexusOperationExecutionResponse_Outcome_Result:
		if outcome.value == nil {
			failed = true
			return {}, error_from_string(.Malformed_Response, "Nexus operation result outcome is missing")
		}
		description.result, ok = description_clone_payload(outcome.value, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Payload_Conversion, "could not copy Nexus operation result")
		}
		description.has_result = true
	case proto.Temporal_Api_Workflowservice_V1_DescribeNexusOperationExecutionResponse_Outcome_Failure:
		if outcome.value == nil {
			failed = true
			return {}, error_from_string(.Malformed_Response, "Nexus operation failure outcome is missing")
		}
		description.failure_kind = nexus_operation_failure_kind(outcome.value)
		description.failure_message, ok = description_failure_message(outcome.value, allocator)
		if ok do description.failure_proto, ok = nexus_operation_clone_failure_proto(outcome.value, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not preserve Nexus operation outcome failure")
		}
		description.has_failure = true
	}
	if len(response.long_poll_token) > 0 {
		description.long_poll_token = bytes.clone(response.long_poll_token, allocator)
	}
	return description, {}
}

decode_nexus_operation_poll_response :: proc(
	data: []u8,
	requested_stage: Nexus_Operation_Wait_Stage,
) -> (result: Nexus_Operation_Poll_Result, empty: bool, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, false, error_from_string(.Malformed_Response, "could not initialize Nexus operation poll arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_PollNexusOperationExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_PollNexusOperationExecutionResponse(
		data,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded {
		return {}, false, error_from_string(.Malformed_Response, "Temporal returned a malformed Nexus operation poll response")
	}
	stage, stage_ok := nexus_operation_wait_stage_from_proto(response.wait_stage)
	if !stage_ok {
		return {}, false, error_from_string(.Malformed_Response, "Temporal returned an unknown Nexus operation wait stage")
	}
	if stage == .Unspecified {
		return {}, true, {}
	}
	result.wait_stage = stage
	owned_token, token_error := strings.clone(response.operation_token)
	if token_error != nil {
		return {}, false, error_from_string(.Malformed_Response, "could not allocate Nexus operation token")
	}
	result.operation_token = owned_token
	#partial switch outcome in response.outcome {
	case proto.Temporal_Api_Workflowservice_V1_PollNexusOperationExecutionResponse_Outcome_Result:
		if outcome.value == nil {
			nexus_operation_poll_result_free(&result)
			return {}, false, error_from_string(.Malformed_Response, "Nexus operation result outcome is missing")
		}
		clone_error: codec.Error
		result.result, clone_error = proto.clone_payload(outcome.value)
		if clone_error != .None {
			nexus_operation_poll_result_free(&result)
			return {}, false, error_from_string(.Payload_Conversion, "could not preserve Nexus operation result")
		}
		result.has_result = true
	case proto.Temporal_Api_Workflowservice_V1_PollNexusOperationExecutionResponse_Outcome_Failure:
		operation_error := nexus_operation_failure_error(outcome.value)
		return result, false, operation_error
	}
	if requested_stage == .Closed && !result.has_result {
		nexus_operation_poll_result_free(&result)
		return {}, false, error_from_string(.Malformed_Response, "closed Nexus operation poll response has no outcome")
	}
	return result, false, {}
}

poll_nexus_operation :: proc(
	handle: ^Nexus_Operation_Handle,
	wait_stage := Nexus_Operation_Wait_Stage.Closed,
	options: Nexus_Operation_Wait_Options = {},
) -> (Nexus_Operation_Poll_Result, Error) {
	if !nexus_operation_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation handle is not initialized")
	}
	if wait_stage == .Unspecified || i32(wait_stage) > i32(Nexus_Operation_Wait_Stage.Closed) {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation poll wait stage must be Started or Closed")
	}
	if options.timeout < 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation wait timeout must not be negative")
	}
	if options.cancellation_token != nil && options.cancellation_token.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation wait cancellation token is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_PollNexusOperationExecutionRequest{
		namespace = handle.client.namespace,
		operation_id = handle.id,
		run_id = handle.run_id,
		wait_stage = nexus_operation_wait_stage_to_proto(wait_stage),
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_PollNexusOperationExecutionRequest(&request)
	defer delete(request_bytes)
	started := time.tick_now()
	for {
		timeout_millis: u32
		if options.timeout > 0 {
			remaining := options.timeout - time.tick_since(started)
			if remaining <= 0 {
				return {}, error_from_string(.Wait_Timed_Out, "Nexus operation wait timed out")
			}
			millis := i64(remaining / time.Millisecond)
			if remaining % time.Millisecond != 0 do millis += 1
			if millis > i64(4_294_967_295) do millis = i64(4_294_967_295)
			timeout_millis = u32(millis)
		}
		rpc_result := temporal_core.client_rpc_call(
			handle.client.connection,
			.Workflow,
			"PollNexusOperationExecution",
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
		result, empty, decode_error := decode_nexus_operation_poll_response(
			rpc_result.protobuf,
			wait_stage,
		)
		delete(rpc_result.protobuf)
		if decode_error.message != nil do return result, decode_error
		if empty do continue
		return result, {}
	}
}

get_nexus_operation_result_payload :: proc(
	handle: ^Nexus_Operation_Handle,
	options: Nexus_Operation_Wait_Options = {},
) -> (codec.Payload, Error) {
	result, poll_error := poll_nexus_operation(handle, .Closed, options)
	if poll_error.message != nil {
		nexus_operation_poll_result_free(&result)
		return {}, poll_error
	}
	if !result.has_result {
		nexus_operation_poll_result_free(&result)
		return {}, error_from_string(.Malformed_Response, "Nexus operation completed without a result")
	}
	payload := result.result
	result.result = {}
	nexus_operation_poll_result_free(&result)
	return payload, {}
}

get_nexus_operation_result_json :: proc(
	handle: ^Nexus_Operation_Handle,
	value: ^$T,
	options: Nexus_Operation_Wait_Options = {},
) -> Error {
	if value == nil do return error_from_string(.Invalid_Argument, "Nexus operation result output pointer is required")
	payload, result_error := get_nexus_operation_result_payload(handle, options)
	if result_error.message != nil do return result_error
	defer codec.payload_free(&payload)
	if codec.decode_json(codec.payload_view(&payload), value) != .None {
		return error_from_string(.Payload_Conversion, "could not decode Nexus operation result")
	}
	return {}
}

cancel_nexus_operation :: proc(
	handle: ^Nexus_Operation_Handle,
	reason := "",
	request_id := "",
) -> Error {
	if !nexus_operation_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Nexus operation handle is not initialized")
	}
	actual_request_id := request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(actual_request_id) == 0 {
		actual_request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	}
	request := proto.Temporal_Api_Workflowservice_V1_RequestCancelNexusOperationExecutionRequest{
		namespace = handle.client.namespace,
		operation_id = handle.id,
		run_id = handle.run_id,
		identity = handle.client.identity,
		request_id = actual_request_id,
		reason = reason,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_RequestCancelNexusOperationExecutionRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"RequestCancelNexusOperationExecution",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return error_from_core(&rpc_result.error)
	return {}
}

describe_nexus_operation :: proc(
	handle: ^Nexus_Operation_Handle,
	options: Describe_Nexus_Operation_Options = {},
	allocator := context.allocator,
) -> (Nexus_Operation_Description, Error) {
	if !nexus_operation_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation handle is not initialized")
	}
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation description allocator is not initialized")
	}
	if options.timeout < 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation Describe timeout must not be negative")
	}
	if options.cancellation_token != nil && options.cancellation_token.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation Describe cancellation token is not initialized")
	}
	if len(options.long_poll_token) > 0 && len(handle.run_id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation Run ID is required for long-poll Describe")
	}
	request := proto.Temporal_Api_Workflowservice_V1_DescribeNexusOperationExecutionRequest{
		namespace = handle.client.namespace,
		operation_id = handle.id,
		run_id = handle.run_id,
		include_input = options.include_input,
		include_outcome = options.include_outcome,
		long_poll_token = options.long_poll_token,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_DescribeNexusOperationExecutionRequest(&request)
	defer delete(request_bytes)
	timeout_millis := u32(10_000)
	retry := true
	if len(options.long_poll_token) > 0 {
		timeout_millis = 0
	}
	if options.timeout > 0 {
		millis := i64(options.timeout / time.Millisecond)
		if options.timeout % time.Millisecond != 0 do millis += 1
		if millis > i64(4_294_967_295) do millis = i64(4_294_967_295)
		timeout_millis = u32(millis)
		retry = false
	}
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"DescribeNexusOperationExecution",
		request_bytes[:],
		retry = retry,
		timeout_millis = timeout_millis,
		cancellation_token = options.cancellation_token,
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		if options.timeout > 0 && update_poll_failure_is_timeout(
			rpc_result.error.status_code,
			string(rpc_result.error.message),
			true,
		) {
			return {}, error_from_core(&rpc_result.error, .Wait_Timed_Out)
		}
		if options.cancellation_token != nil && rpc_result.error.status_code == 0 &&
		   string(rpc_result.error.message) == "Cancelled" {
			return {}, error_from_core(&rpc_result.error, .Wait_Canceled)
		}
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_nexus_operation_description(rpc_result.protobuf, allocator)
}

terminate_nexus_operation :: proc(
	handle: ^Nexus_Operation_Handle,
	reason := "",
	request_id := "",
) -> Error {
	if !nexus_operation_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Nexus operation handle is not initialized")
	}
	actual_request_id := request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(actual_request_id) == 0 {
		actual_request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	}
	request := proto.Temporal_Api_Workflowservice_V1_TerminateNexusOperationExecutionRequest{
		namespace = handle.client.namespace,
		operation_id = handle.id,
		run_id = handle.run_id,
		identity = handle.client.identity,
		request_id = actual_request_id,
		reason = reason,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_TerminateNexusOperationExecutionRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"TerminateNexusOperationExecution",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return error_from_core(&rpc_result.error)
	return {}
}

delete_nexus_operation :: proc(handle: ^Nexus_Operation_Handle) -> Error {
	if !nexus_operation_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Nexus operation handle is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_DeleteNexusOperationExecutionRequest{
		namespace = handle.client.namespace,
		operation_id = handle.id,
		run_id = handle.run_id,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_DeleteNexusOperationExecutionRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"DeleteNexusOperationExecution",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return error_from_core(&rpc_result.error)
	return {}
}

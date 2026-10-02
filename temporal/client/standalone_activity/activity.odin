package standalone_activity

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:encoding/uuid"
import "core:math"
import "core:mem"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import client ".."
import codec "../../codec"
import temporal_core "../../core"
import proto "../../core/proto"

Client :: client.Client
Client_Cancellation_Token :: client.Client_Cancellation_Token
Error :: client.Error
Error_Kind :: client.Error_Kind
Link :: client.Link
Named_Payload :: client.Named_Payload
Nexus_Completion_Callback :: client.Nexus_Completion_Callback
Optional_Duration :: client.Optional_Duration
Optional_Time :: client.Optional_Time
Pending_Activity_State :: client.Pending_Activity_State
Priority :: client.Priority
Retry_Policy :: client.Retry_Policy
Standalone_Activity_Options :: client.Standalone_Activity_Options
Start_Metadata :: client.Start_Metadata
Workflow_Priority :: client.Workflow_Priority

client_request_identity :: client.client_request_identity
description_clone_named_payload :: client.description_clone_named_payload
description_clone_payload :: client.description_clone_payload
description_clone_string :: client.description_clone_string
description_decode_user_metadata_string :: client.description_decode_user_metadata_string
description_duration :: client.description_duration
description_failure_message :: client.description_failure_message
description_priority :: client.description_priority
description_time :: client.description_time
duration_proto :: client.duration_proto
encode_link :: client.encode_link
encoded_link_storage_free :: client.encoded_link_storage_free
encoded_link_storage_make :: client.encoded_link_storage_make
error_free :: client.error_free
error_from_core :: client.error_from_core
error_from_string :: client.error_from_string
free_start_proto_payloads :: client.free_start_proto_payloads
link_error :: client.link_error
link_free :: client.link_free
link_from_proto :: client.link_from_proto
request_identity :: client.request_identity
resolve_request_id :: client.resolve_request_id
standalone_activity_options_free :: client.standalone_activity_options_free
standalone_activity_clone_retry_policy :: client.standalone_activity_clone_retry_policy
clone_activity_options :: client.clone_activity_options
update_poll_failure_is_timeout :: client.update_poll_failure_is_timeout
workflow_event_link_error :: client.workflow_event_link_error

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

Standalone_Activity_On_Conflict_Options :: struct {
	attach_request_id:           bool,
	attach_completion_callbacks: bool,
	attach_links:                bool,
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
	on_conflict_options:       ^Standalone_Activity_On_Conflict_Options,
	// Header Payloads, Search Attributes, and static summary/details apply.
	// Start_Metadata.memo is ignored because standalone Activities have no Memo.
	metadata:                  ^Start_Metadata,
	links:                     []Link,
	completion_callbacks:      []Nexus_Completion_Callback,
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

Standalone_Activity_Status :: enum i32 {
	Unspecified = 0,
	Running = 1,
	Completed = 2,
	Failed = 3,
	Canceled = 4,
	Terminated = 5,
	Timed_Out = 6,
	Paused = 7,
}

Standalone_Activity_Retry_State :: enum i32 {
	Unspecified = 0,
	In_Progress = 1,
	Non_Retryable_Failure = 2,
	Timeout = 3,
	Maximum_Attempts_Reached = 4,
	Retry_Policy_Not_Set = 5,
	Internal_Server_Error = 6,
	Cancel_Requested = 7,
}

Describe_Standalone_Activity_Options :: struct {
	include_input:             bool,
	include_outcome:           bool,
	include_heartbeat_details: bool,
	include_last_failure:      bool,
	long_poll_token:           []u8,
	timeout:                   time.Duration,
	cancellation_token:        ^Client_Cancellation_Token,
}

Standalone_Activity_Description :: struct {
	activity_id:                 string,
	run_id:                      string,
	activity_type:               string,
	task_queue:                  string,
	status:                      Standalone_Activity_Status,
	run_state:                   Pending_Activity_State,
	schedule_to_close_timeout:   Optional_Duration,
	schedule_to_start_timeout:   Optional_Duration,
	start_to_close_timeout:      Optional_Duration,
	heartbeat_timeout:           Optional_Duration,
	start_delay:                 Optional_Duration,
	execution_duration:          Optional_Duration,
	current_retry_interval:      Optional_Duration,
	schedule_time:               Optional_Time,
	execution_time:              Optional_Time,
	expiration_time:             Optional_Time,
	close_time:                  Optional_Time,
	last_heartbeat_time:         Optional_Time,
	last_started_time:           Optional_Time,
	last_attempt_complete_time:  Optional_Time,
	next_attempt_schedule_time:  Optional_Time,
	attempt:                     i32,
	state_transition_count:      i64,
	state_size_bytes:            i64,
	total_heartbeat_count:       i64,
	last_worker_identity:        string,
	last_deployment_name:        string,
	last_deployment_build_id:    string,
	sdk_name:                    string,
	sdk_version:                 string,
	canceled_reason:             string,
	retry_policy:                Retry_Policy,
	has_retry_policy:            bool,
	priority:                    Workflow_Priority,
	retry_state:                 Standalone_Activity_Retry_State,
	heartbeat_details:           [dynamic]codec.Payload,
	headers:                     [dynamic]Named_Payload,
	search_attributes:           [dynamic]Named_Payload,
	links:                       [dynamic]Link,
	static_summary:              string,
	has_static_summary:          bool,
	static_details:              string,
	has_static_details:          bool,
	input:                       [dynamic]codec.Payload,
	result:                      [dynamic]codec.Payload,
	last_failure_message:        string,
	last_failure_proto:          []u8,
	failure_kind:                Error_Kind,
	failure_message:             string,
	failure_proto:               []u8,
	has_failure:                 bool,
	completion_callback_protos:  [dynamic][]u8,
	long_poll_token:             []u8,
	allocator:                   mem.Allocator,
}

standalone_activity_description_free :: proc(description: ^Standalone_Activity_Description) {
	if description == nil do return
	allocator := description.allocator
	if allocator.procedure == nil {
		description^ = {}
		return
	}
	strings_to_delete := []^string{
		&description.activity_id,
		&description.run_id,
		&description.activity_type,
		&description.task_queue,
		&description.last_worker_identity,
		&description.last_deployment_name,
		&description.last_deployment_build_id,
		&description.sdk_name,
		&description.sdk_version,
		&description.canceled_reason,
		&description.priority.fairness_key,
		&description.static_summary,
		&description.static_details,
		&description.last_failure_message,
		&description.failure_message,
	}
	for value in strings_to_delete do delete(value^, allocator)
	for error_type in description.retry_policy.non_retryable_error_types do delete(error_type, allocator)
	delete(description.retry_policy.non_retryable_error_types, allocator)
	for &payload in description.heartbeat_details do codec.payload_free(&payload)
	for &entry in description.headers {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	for &entry in description.search_attributes {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	for &link in description.links do link_free(&link)
	for &payload in description.input do codec.payload_free(&payload)
	for &payload in description.result do codec.payload_free(&payload)
	for callback in description.completion_callback_protos do delete(callback, allocator)
	delete(description.heartbeat_details)
	delete(description.headers)
	delete(description.search_attributes)
	delete(description.links)
	delete(description.input)
	delete(description.result)
	delete(description.completion_callback_protos)
	delete(description.last_failure_proto, allocator)
	delete(description.failure_proto, allocator)
	delete(description.long_poll_token, allocator)
	description^ = {}
}

standalone_activity_description_error :: proc(
	description: ^Standalone_Activity_Description,
	kind: Error_Kind,
	message: string,
) -> (Standalone_Activity_Description, Error) {
	standalone_activity_description_free(description)
	return {}, error_from_string(kind, message)
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
	if options.on_conflict_options != nil && options.id_conflict_policy != .Use_Existing {
		return "standalone Activity on-conflict options require Use Existing conflict policy"
	}
	if options.metadata != nil && options.metadata.allocator.procedure == nil {
		return "standalone Activity start metadata is not initialized"
	}
	for link in options.links {
		if message := link_error(link); len(message) > 0 do return message
	}
	for callback in options.completion_callbacks {
		if len(callback.url) == 0 do return "standalone Activity completion callback URL must not be empty"
		for header in callback.headers {
			if len(header.key) == 0 do return "standalone Activity completion callback header keys must not be empty"
		}
		for link in callback.links {
			if message := link_error(link); len(message) > 0 do return message
		}
		for link in callback.workflow_event_links {
			if message := workflow_event_link_error(link); len(message) > 0 do return message
		}
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
	on_conflict_options := proto.Temporal_Api_Common_V1_OnConflictOptions{}
	if options.on_conflict_options != nil {
		on_conflict_options = {
			attach_request_id = options.on_conflict_options.attach_request_id,
			attach_completion_callbacks = options.on_conflict_options.attach_completion_callbacks,
			attach_links = options.on_conflict_options.attach_links,
		}
		request.on_conflict_options = &on_conflict_options
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
	header := proto.Temporal_Api_Common_V1_Header{}
	search_attributes := proto.Temporal_Api_Common_V1_SearchAttributes{}
	user_metadata := proto.Temporal_Api_Sdk_V1_UserMetadata{}
	static_summary := proto.Temporal_Api_Common_V1_Payload{}
	static_details := proto.Temporal_Api_Common_V1_Payload{}
	header_payloads: []proto.Temporal_Api_Common_V1_Payload
	search_payloads: []proto.Temporal_Api_Common_V1_Payload
	defer delete(header.fields)
	defer delete(search_attributes.indexed_fields)
	defer free_start_proto_payloads(header_payloads)
	defer free_start_proto_payloads(search_payloads)
	defer delete(static_summary.metadata)
	defer delete(static_summary.external_payloads)
	defer delete(static_details.metadata)
	defer delete(static_details.external_payloads)
	if options.metadata != nil {
		metadata := options.metadata
		if metadata.has_static_summary {
			static_summary = proto.encode_payload(codec.payload_view(&metadata.static_summary))
			user_metadata.summary = &static_summary
		}
		if metadata.has_static_details {
			static_details = proto.encode_payload(codec.payload_view(&metadata.static_details))
			user_metadata.details = &static_details
		}
		if metadata.has_static_summary || metadata.has_static_details {
			request.user_metadata = &user_metadata
		}
		if len(metadata.headers) > 0 {
			header_payloads = make([]proto.Temporal_Api_Common_V1_Payload, len(metadata.headers))
			for &entry, index in metadata.headers {
				header_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
			}
			for entry, index in metadata.headers {
				append(&header.fields, proto.Temporal_Api_Common_V1_Header_FieldsEntry{
					key = entry.key,
					value = &header_payloads[index],
				})
			}
			request.header = &header
		}
		if len(metadata.search_attributes) > 0 {
			search_payloads = make([]proto.Temporal_Api_Common_V1_Payload, len(metadata.search_attributes))
			for &entry, index in metadata.search_attributes {
				search_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
			}
			for entry, index in metadata.search_attributes {
				append(
					&search_attributes.indexed_fields,
					proto.Temporal_Api_Common_V1_SearchAttributes_IndexedFieldsEntry{
						key = entry.key,
						value = &search_payloads[index],
					},
				)
			}
			request.search_attributes = &search_attributes
		}
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
		for callback_header in callback.headers {
			append(
				&callback_headers[index],
				proto.Temporal_Api_Common_V1_Callback_Nexus_HeaderEntry{
					key = callback_header.key,
					value = callback_header.value,
				},
			)
		}
		callback_values[index].header = callback_headers[index]
		for link in callback.links do append(&callback_links[index], encode_link(link, &link_storage))
		for link in callback.workflow_event_links {
			append(&callback_links[index], encode_link({kind = .Workflow_Event, workflow_event = link}, &link_storage))
		}
		append(&request.completion_callbacks, proto.Temporal_Api_Common_V1_Callback{
			links = callback_links[index],
			variant = proto.Temporal_Api_Common_V1_Callback_Variant_Nexus{
				value = &callback_values[index],
			},
		})
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_StartActivityExecutionRequest(&request), {}
}

start_standalone_activity_payload_terminal :: proc(
	interceptor_input: ^Client_Start_Standalone_Activity_Input,
) -> (Standalone_Activity_Handle, Error) {
	if interceptor_input == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity start interceptor input is missing")
	}
	client := interceptor_input.client
	input := interceptor_input.input
	options := interceptor_input.options
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

start_standalone_activity_payload :: proc(
	client: ^Client,
	input: codec.Payload_View,
	options: Start_Standalone_Activity_Options,
) -> (Standalone_Activity_Handle, Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	interceptor_input := Client_Start_Standalone_Activity_Input{
		client = client,
		input = input,
		options = options,
	}
	state := client_interceptor_state(client)
	return client_start_standalone_activity_chain(&state, &interceptor_input)
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

standalone_activity_status_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_ActivityExecutionStatus,
) -> (Standalone_Activity_Status, bool) {
	numeric := i32(value)
	if numeric < i32(Standalone_Activity_Status.Unspecified) ||
	   numeric > i32(Standalone_Activity_Status.Paused) {
		return {}, false
	}
	return Standalone_Activity_Status(numeric), true
}

standalone_activity_run_state_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_PendingActivityState,
) -> (Pending_Activity_State, bool) {
	numeric := i32(value)
	if numeric < i32(Pending_Activity_State.Unspecified) ||
	   numeric > i32(Pending_Activity_State.Pause_Requested) {
		return {}, false
	}
	return Pending_Activity_State(numeric), true
}

standalone_activity_retry_state_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_RetryState,
) -> (Standalone_Activity_Retry_State, bool) {
	numeric := i32(value)
	if numeric < i32(Standalone_Activity_Retry_State.Unspecified) ||
	   numeric > i32(Standalone_Activity_Retry_State.Cancel_Requested) {
		return {}, false
	}
	return Standalone_Activity_Retry_State(numeric), true
}

standalone_activity_clone_failure_proto :: proc(
	failure: ^proto.Temporal_Api_Failure_V1_Failure,
	allocator: mem.Allocator,
) -> ([]u8, bool) {
	if failure == nil do return nil, true
	encoded := proto.encode_Temporal_Api_Failure_V1_Failure(failure)
	defer delete(encoded)
	return bytes.clone(encoded[:], allocator), true
}

standalone_activity_clone_payloads :: proc(
	source: ^proto.Temporal_Api_Common_V1_Payloads,
	target: ^[dynamic]codec.Payload,
	allocator: mem.Allocator,
) -> bool {
	if source == nil do return true
	for &value in source.payloads {
		owned, ok := description_clone_payload(&value, allocator)
		if !ok do return false
		append(target, owned)
	}
	return true
}

decode_standalone_activity_description :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (description: Standalone_Activity_Description, error: Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity description allocator is not initialized")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize standalone Activity Describe arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_DescribeActivityExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_DescribeActivityExecutionResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded || response.info == nil || len(response.run_id) == 0 ||
	   len(response.info.activity_id) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed standalone Activity Describe response")
	}

	description.allocator = allocator
	description.heartbeat_details = make([dynamic]codec.Payload, allocator)
	description.headers = make([dynamic]Named_Payload, allocator)
	description.search_attributes = make([dynamic]Named_Payload, allocator)
	description.links = make([dynamic]Link, allocator)
	description.input = make([dynamic]codec.Payload, allocator)
	description.result = make([dynamic]codec.Payload, allocator)
	description.completion_callback_protos = make([dynamic][]u8, allocator)
	info := response.info
	status_ok, state_ok: bool
	description.status, status_ok = standalone_activity_status_from_proto(info.status)
	description.run_state, state_ok = standalone_activity_run_state_from_proto(info.run_state)
	if !status_ok || !state_ok {
		return standalone_activity_description_error(
			&description,
			.Malformed_Response,
			"Temporal returned an unknown standalone Activity state",
		)
	}
	clone_targets := []struct {source: string, target: ^string}{
		{info.activity_id, &description.activity_id},
		{response.run_id, &description.run_id},
		{info.task_queue, &description.task_queue},
		{info.last_worker_identity, &description.last_worker_identity},
		{info.sdk_name, &description.sdk_name},
		{info.sdk_version, &description.sdk_version},
		{info.canceled_reason, &description.canceled_reason},
	}
	ok := true
	for target in clone_targets {
		target.target^, ok = description_clone_string(target.source, allocator)
		if !ok {
			return standalone_activity_description_error(
				&description,
				.Malformed_Response,
				"could not allocate standalone Activity description",
			)
		}
	}
	if info.activity_type != nil {
		description.activity_type, ok = description_clone_string(info.activity_type.name, allocator)
	}
	if ok && info.last_deployment_version != nil {
		description.last_deployment_name, ok = description_clone_string(
			info.last_deployment_version.deployment_name,
			allocator,
		)
		if ok {
			description.last_deployment_build_id, ok = description_clone_string(
				info.last_deployment_version.build_id,
				allocator,
			)
		}
	}
	if !ok {
		return standalone_activity_description_error(
			&description,
			.Malformed_Response,
			"could not allocate standalone Activity description",
		)
	}
	description.schedule_to_close_timeout = description_duration(info.schedule_to_close_timeout)
	description.schedule_to_start_timeout = description_duration(info.schedule_to_start_timeout)
	description.start_to_close_timeout = description_duration(info.start_to_close_timeout)
	description.heartbeat_timeout = description_duration(info.heartbeat_timeout)
	description.start_delay = description_duration(info.start_delay)
	description.execution_duration = description_duration(info.execution_duration)
	description.current_retry_interval = description_duration(info.current_retry_interval)
	description.schedule_time = description_time(info.schedule_time)
	description.execution_time = description_time(info.execution_time)
	description.expiration_time = description_time(info.expiration_time)
	description.close_time = description_time(info.close_time)
	description.last_heartbeat_time = description_time(info.last_heartbeat_time)
	description.last_started_time = description_time(info.last_started_time)
	description.last_attempt_complete_time = description_time(info.last_attempt_complete_time)
	description.next_attempt_schedule_time = description_time(info.next_attempt_schedule_time)
	description.attempt = info.attempt
	description.state_transition_count = info.state_transition_count
	description.state_size_bytes = info.state_size_bytes
	description.total_heartbeat_count = info.total_heartbeat_count
	description.retry_policy,
	description.has_retry_policy,
	ok = standalone_activity_clone_retry_policy(info.retry_policy, allocator)
	if ok do description.priority, ok = description_priority(info.priority, allocator)
	if !ok {
		return standalone_activity_description_error(
			&description,
			.Malformed_Response,
			"could not copy standalone Activity policy",
		)
	}
	if !standalone_activity_clone_payloads(info.heartbeat_details, &description.heartbeat_details, allocator) {
		return standalone_activity_description_error(
			&description,
			.Payload_Conversion,
			"could not copy standalone Activity heartbeat details",
		)
	}
	if info.header != nil {
		for entry in info.header.fields {
			owned, cloned := description_clone_named_payload(entry.key, entry.value, allocator)
			if !cloned {
				return standalone_activity_description_error(
					&description,
					.Payload_Conversion,
					"could not copy standalone Activity Header",
				)
			}
			append(&description.headers, owned)
		}
	}
	if info.search_attributes != nil {
		for entry in info.search_attributes.indexed_fields {
			owned, cloned := description_clone_named_payload(entry.key, entry.value, allocator)
			if !cloned {
				return standalone_activity_description_error(
					&description,
					.Payload_Conversion,
					"could not copy standalone Activity Search Attribute",
				)
			}
			append(&description.search_attributes, owned)
		}
	}
	if info.user_metadata != nil {
		description.static_summary, description.has_static_summary, ok =
			description_decode_user_metadata_string(info.user_metadata.summary, allocator)
		if ok {
			description.static_details, description.has_static_details, ok =
				description_decode_user_metadata_string(info.user_metadata.details, allocator)
		}
		if !ok {
			return standalone_activity_description_error(
				&description,
				.Payload_Conversion,
				"could not decode standalone Activity user metadata",
			)
		}
	}
	for &source_link in info.links {
		owned_link, link_error := link_from_proto(&source_link, allocator)
		if link_error.message != nil {
			error_free(&link_error)
			return standalone_activity_description_error(
				&description,
				.Malformed_Response,
				"could not decode standalone Activity Link",
			)
		}
		append(&description.links, owned_link)
	}
	description.last_failure_message, ok = description_failure_message(info.last_failure, allocator)
	if ok {
		description.last_failure_proto, ok = standalone_activity_clone_failure_proto(info.last_failure, allocator)
	}
	if !ok {
		return standalone_activity_description_error(
			&description,
			.Malformed_Response,
			"could not preserve standalone Activity attempt failure",
		)
	}
	if !standalone_activity_clone_payloads(response.input, &description.input, allocator) {
		return standalone_activity_description_error(
			&description,
			.Payload_Conversion,
			"could not copy standalone Activity input",
		)
	}
	if response.outcome != nil {
		description.retry_state, ok = standalone_activity_retry_state_from_proto(response.outcome.retry_state)
		if !ok {
			return standalone_activity_description_error(
				&description,
				.Malformed_Response,
				"Temporal returned an unknown standalone Activity retry state",
			)
		}
		#partial switch outcome in response.outcome.value {
		case proto.Temporal_Api_Activity_V1_ActivityExecutionOutcome_Value_Result:
			if outcome.value == nil ||
			   !standalone_activity_clone_payloads(outcome.value, &description.result, allocator) {
				return standalone_activity_description_error(
					&description,
					.Payload_Conversion,
					"could not copy standalone Activity result",
				)
			}
		case proto.Temporal_Api_Activity_V1_ActivityExecutionOutcome_Value_Failure:
			if outcome.value == nil {
				return standalone_activity_description_error(
					&description,
					.Malformed_Response,
					"standalone Activity failure outcome is missing",
				)
			}
			description.failure_kind = standalone_activity_failure_kind(outcome.value)
			description.failure_message, ok = description_failure_message(outcome.value, allocator)
			if ok {
				description.failure_proto, ok = standalone_activity_clone_failure_proto(outcome.value, allocator)
			}
			if !ok {
				return standalone_activity_description_error(
					&description,
					.Malformed_Response,
					"could not preserve standalone Activity outcome failure",
				)
			}
			description.has_failure = true
		case:
			return standalone_activity_description_error(
				&description,
				.Malformed_Response,
				"standalone Activity outcome has no result or failure",
			)
		}
	}
	for &callback in response.callbacks {
		encoded := proto.encode_Temporal_Api_Activity_V1_CallbackInfo(&callback)
		owned := bytes.clone(encoded[:], allocator)
		delete(encoded)
		append(&description.completion_callback_protos, owned)
	}
	description.long_poll_token = bytes.clone(response.long_poll_token, allocator)
	return description, {}
}

describe_standalone_activity_terminal :: proc(
	interceptor_input: ^Client_Describe_Standalone_Activity_Input,
) -> (Standalone_Activity_Description, Error) {
	if interceptor_input == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity Describe interceptor input is missing")
	}
	handle := interceptor_input.handle
	options := interceptor_input.options
	allocator := interceptor_input.allocator
	if !standalone_activity_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity description allocator is not initialized")
	}
	if options.timeout < 0 {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity Describe timeout must not be negative")
	}
	if options.cancellation_token != nil && options.cancellation_token.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity Describe cancellation token is not initialized")
	}
	if len(options.long_poll_token) > 0 && len(handle.run_id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity Run ID is required for long-poll Describe")
	}
	request := proto.Temporal_Api_Workflowservice_V1_DescribeActivityExecutionRequest{
		namespace = handle.client.namespace,
		activity_id = handle.id,
		run_id = handle.run_id,
		include_input = options.include_input,
		include_outcome = options.include_outcome,
		long_poll_token = options.long_poll_token,
		include_heartbeat_details = options.include_heartbeat_details,
		include_last_failure = options.include_last_failure,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_DescribeActivityExecutionRequest(&request)
	defer delete(request_bytes)
	timeout_millis := u32(10_000)
	retry := true
	if len(options.long_poll_token) > 0 do timeout_millis = 0
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
		"DescribeActivityExecution",
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
	return decode_standalone_activity_description(rpc_result.protobuf, allocator)
}

describe_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	options: Describe_Standalone_Activity_Options = {},
	allocator := context.allocator,
) -> (Standalone_Activity_Description, Error) {
	if !standalone_activity_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	interceptor_input := Client_Describe_Standalone_Activity_Input{
		handle = handle,
		options = options,
		allocator = allocator,
	}
	state := client_interceptor_state(handle.client)
	return client_describe_standalone_activity_chain(&state, &interceptor_input)
}

poll_standalone_activity_terminal :: proc(
	interceptor_input: ^Client_Poll_Standalone_Activity_Input,
) -> (Standalone_Activity_Poll_Result, Error) {
	if interceptor_input == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity poll interceptor input is missing")
	}
	handle := interceptor_input.handle
	options := interceptor_input.options
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

poll_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	options: Standalone_Activity_Wait_Options = {},
) -> (Standalone_Activity_Poll_Result, Error) {
	if !standalone_activity_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	interceptor_input := Client_Poll_Standalone_Activity_Input{
		handle = handle,
		options = options,
	}
	state := client_interceptor_state(handle.client)
	return client_poll_standalone_activity_chain(&state, &interceptor_input)
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

cancel_standalone_activity_rpc :: proc(
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

terminate_standalone_activity_rpc :: proc(
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

delete_standalone_activity_rpc :: proc(handle: ^Standalone_Activity_Handle) -> Error {
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

standalone_activity_mutation_terminal :: proc(
	input: ^Client_Standalone_Activity_Mutation_Input,
) -> Error {
	if input == nil {
		return error_from_string(.Invalid_Argument, "standalone Activity mutation interceptor input is missing")
	}
	switch input.kind {
	case .Cancel:
		return cancel_standalone_activity_rpc(input.handle, input.reason, input.request_id)
	case .Terminate:
		return terminate_standalone_activity_rpc(input.handle, input.reason, input.request_id)
	case .Delete:
		return delete_standalone_activity_rpc(input.handle)
	}
	return error_from_string(.Invalid_Argument, "standalone Activity mutation kind is invalid")
}

standalone_activity_mutation :: proc(
	handle: ^Standalone_Activity_Handle,
	kind: Standalone_Activity_Mutation_Kind,
	reason := "",
	request_id := "",
) -> Error {
	if !standalone_activity_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	if int(kind) < int(Standalone_Activity_Mutation_Kind.Cancel) ||
	   int(kind) > int(Standalone_Activity_Mutation_Kind.Delete) {
		return error_from_string(.Invalid_Argument, "standalone Activity mutation kind is invalid")
	}
	input := Client_Standalone_Activity_Mutation_Input{
		handle = handle,
		kind = kind,
		reason = reason,
		request_id = request_id,
	}
	state := client_interceptor_state(handle.client)
	return client_standalone_activity_mutation_chain(&state, &input)
}

cancel_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	reason := "",
	request_id := "",
) -> Error {
	return standalone_activity_mutation(handle, .Cancel, reason, request_id)
}

terminate_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	reason := "",
	request_id := "",
) -> Error {
	return standalone_activity_mutation(handle, .Terminate, reason, request_id)
}

delete_standalone_activity :: proc(handle: ^Standalone_Activity_Handle) -> Error {
	return standalone_activity_mutation(handle, .Delete)
}

standalone_activity_empty_rpc :: proc(client: ^Client, method: string, request: []u8) -> Error {
	rpc_result := temporal_core.client_rpc_call(client.connection, .Workflow, method, request)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return error_from_core(&rpc_result.error)
	return {}
}

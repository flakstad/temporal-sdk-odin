// Package client provides the experimental public Temporal client boundary.
package client

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:encoding/uuid"
import "core:math"
import "core:mem"
import "core:mem/virtual"
import "core:strconv"
import "core:strings"
import "core:time"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"
import temporal_failure "../failure"
import temporal_headers "../headers"

Error_Kind :: enum {
	None,
	Invalid_Argument,
	Connection,
	Rpc,
	Malformed_Response,
	Payload_Conversion,
	Workflow_Failed,
	Workflow_Canceled,
	Workflow_Terminated,
	Workflow_Timed_Out,
	Workflow_Continued_As_New,
	Workflow_Query_Rejected,
	Workflow_Update_Failed,
	Nexus_Operation_Failed,
	Nexus_Operation_Canceled,
	Nexus_Operation_Terminated,
	Nexus_Operation_Timed_Out,
	Wait_Canceled,
	Wait_Timed_Out,
}

Error :: struct {
	kind:                Error_Kind,
	message:             []u8,
	status_code:         u32,
	details:             []u8,
	workflow_status:     Workflow_Status,
	has_workflow_status: bool,
}

error_free :: proc(error: ^Error) {
	if error == nil {
		return
	}
	delete(error.message)
	delete(error.details)
	error^ = {}
}

// decode_error_failure decodes the complete recursive Temporal Failure kept in
// Error.details. Workflow execution, Workflow Update, and standalone Nexus
// operation failures populate this field; other error kinds return No_Data.
decode_error_failure :: proc(
	error: ^Error,
	allocator := context.allocator,
) -> (temporal_failure.Failure, temporal_failure.Decode_Error) {
	if error == nil {
		return {}, .No_Data
	}
	return temporal_failure.decode(error.details, allocator)
}

error_from_string :: proc(kind: Error_Kind, message: string) -> Error {
	return {kind = kind, message = bytes.clone(transmute([]u8)message)}
}

error_clone :: proc(error: ^Error) -> Error {
	if error == nil {
		return {}
	}
	result := Error{
		kind = error.kind,
		message = bytes.clone(error.message),
		status_code = error.status_code,
		details = bytes.clone(error.details),
		workflow_status = error.workflow_status,
		has_workflow_status = error.has_workflow_status,
	}
	if result.kind == .Workflow_Update_Failed && len(error.details) > 0 {
		delete(result.message)
		result.message = nil
		decode_arena: virtual.Arena
		if virtual.arena_init_growing(&decode_arena) == nil {
			previous_allocator := context.allocator
			context.allocator = virtual.arena_allocator(&decode_arena)
			failure := proto.Temporal_Api_Failure_V1_Failure{}
			decoded := proto.decode_Temporal_Api_Failure_V1_Failure(error.details, &failure)
			context.allocator = previous_allocator
			if decoded {
				current := &failure
				for current != nil && len(current.message) == 0 {
					current = current.cause
				}
				if current != nil {
					result.message = bytes.clone(
						transmute([]u8)current.message,
						previous_allocator,
					)
				}
			}
			virtual.arena_destroy(&decode_arena)
		}
	}
	if result.kind == .Workflow_Update_Failed && len(result.message) == 0 {
		fallback := "Workflow Update failed"
		result.message = bytes.clone(transmute([]u8)fallback)
	}
	return result
}

error_from_core :: proc(core_error: ^temporal_core.Error, kind := Error_Kind.Rpc) -> Error {
	if core_error == nil || core_error.message == nil {
		return {}
	}
	result := Error{
		kind = kind,
		message = core_error.message,
		status_code = core_error.status_code,
		details = core_error.details,
	}
	core_error^ = {}
	return result
}

Client :: struct {
	runtime:      ^temporal_core.Runtime,
	connection:   ^temporal_core.Client,
	namespace:    string,
	identity:     string,
	interceptors: [dynamic]Client_Interceptor,
}

connect :: proc(
	target_url := "http://localhost:7233",
	namespace := "default",
) -> (client: Client, error: Error) {
	return connect_with_options({target_url = target_url, namespace = namespace})
}

connect_with_options :: proc(options: Connect_Options) -> (client: Client, error: Error) {
	if len(options.namespace) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Temporal namespace must not be empty")
	}
	identity := options.identity
	if len(identity) == 0 {
		identity = "temporal-sdk-odin@local"
	}
	core_options := temporal_core.Client_Connection_Options{
		target_url = options.target_url,
		client_name = "temporal-sdk-odin",
		client_version = "0.0.1-experimental",
		identity = identity,
		metadata = options.metadata,
		binary_metadata = options.binary_metadata,
		api_key = options.api_key,
		tls = options.tls,
		retry = options.retry,
		keep_alive = options.keep_alive,
		http_connect_proxy = options.http_connect_proxy,
		dns_load_balancing = options.dns_load_balancing,
		grpc_compression = options.grpc_compression,
		payloads_warn_size = options.payloads_warn_size,
		memo_warn_size = options.memo_warn_size,
	}
	validation_error := temporal_core.client_connection_options_validate(core_options)
	if validation_error.message != nil {
		return {}, error_from_core(&validation_error, .Invalid_Argument)
	}
	heap := allocator_runtime.heap_allocator()
	owned_namespace, allocation_error := strings.clone(options.namespace, heap)
	if allocation_error != nil {
		return {}, error_from_string(.Connection, "could not allocate Temporal namespace")
	}
	owned_identity, identity_allocation_error := strings.clone(identity, heap)
	if identity_allocation_error != nil {
		delete(owned_namespace, heap)
		return {}, error_from_string(.Connection, "could not allocate Temporal Client identity")
	}
	core_options.identity = owned_identity

	runtime := new(temporal_core.Runtime, heap)
	created_runtime: temporal_core.Runtime
	core_error: temporal_core.Error
	if options.runtime_options == nil {
		created_runtime, core_error = temporal_core.runtime_create()
	} else {
		created_runtime, core_error = temporal_core.runtime_create_with_options(options.runtime_options^)
	}
	if core_error.message != nil {
		delete(owned_identity, heap)
		delete(owned_namespace, heap)
		free(runtime, heap)
		return {}, error_from_core(&core_error, .Connection)
	}
	runtime^ = created_runtime
	connection := new(temporal_core.Client, heap)
	created_connection, connect_error := temporal_core.client_connect_with_options(runtime, core_options)
	if connect_error.message != nil {
		temporal_core.runtime_close(runtime)
		free(connection, heap)
		free(runtime, heap)
		delete(owned_namespace, heap)
		delete(owned_identity, heap)
		return {}, error_from_core(&connect_error, .Connection)
	}
	connection^ = created_connection
	interceptors := make([dynamic]Client_Interceptor, heap)
	append(&interceptors, ..options.interceptors)
	return {
		runtime = runtime,
		connection = connection,
		namespace = owned_namespace,
		identity = owned_identity,
		interceptors = interceptors,
	}, {}
}

close :: proc(client: ^Client) {
	if client == nil {
		return
	}
	heap := allocator_runtime.heap_allocator()
	if client.connection != nil {
		temporal_core.client_close(client.connection)
		free(client.connection, heap)
	}
	if client.runtime != nil {
		temporal_core.runtime_close(client.runtime)
		free(client.runtime, heap)
	}
	delete(client.namespace, heap)
	delete(client.identity, heap)
	delete(client.interceptors)
	client^ = {}
}

core_client :: proc(client: ^Client) -> ^temporal_core.Client {
	if client == nil {
		return nil
	}
	return client.connection
}

Start_Workflow_Options :: struct {
	id:                         string,
	task_queue:                 string,
	request_id:                string,
	workflow_execution_timeout: time.Duration,
	workflow_run_timeout:       time.Duration,
	workflow_task_timeout:      time.Duration,
	id_reuse_policy:            Workflow_Id_Reuse_Policy,
	id_conflict_policy:         Workflow_Id_Conflict_Policy,
	retry_policy:               ^Retry_Policy,
	cron_schedule:              string,
	start_delay:                time.Duration,
	priority:                   ^Priority,
	metadata:                   ^Start_Metadata,
	links:                      []Link,
	// Deprecated compatibility field. Prefer links with kind Workflow_Event.
	workflow_event_links:       []Workflow_Event_Link,
	nexus_completion_callbacks: []Nexus_Completion_Callback,
	enable_eager_workflow_start: bool,
}

// Nexus_Completion_Callback is borrowed during synchronous Workflow start
// encoding. It attaches Temporal's server-owned Nexus callback to the new
// Workflow so its terminal result completes an asynchronous Nexus operation.
Nexus_Completion_Callback :: struct {
	url:     string,
	headers: []Nexus_Callback_Header,
	links:   []Link,
	// Deprecated compatibility field. Prefer links with kind Workflow_Event.
	workflow_event_links: []Workflow_Event_Link,
}

Nexus_Callback_Header :: struct {
	key:   string,
	value: string,
}

Workflow_Event_Type :: enum i32 {
	Unspecified = 0,
	Workflow_Execution_Started = 1,
	Workflow_Execution_Completed = 2,
	Workflow_Execution_Failed = 3,
	Workflow_Execution_Timed_Out = 4,
	Workflow_Task_Scheduled = 5,
	Workflow_Task_Started = 6,
	Workflow_Task_Completed = 7,
	Workflow_Task_Timed_Out = 8,
	Workflow_Task_Failed = 9,
	Activity_Task_Scheduled = 10,
	Activity_Task_Started = 11,
	Activity_Task_Completed = 12,
	Activity_Task_Failed = 13,
	Activity_Task_Timed_Out = 14,
	Activity_Task_Cancel_Requested = 15,
	Activity_Task_Canceled = 16,
	Timer_Started = 17,
	Timer_Fired = 18,
	Timer_Canceled = 19,
	Workflow_Execution_Cancel_Requested = 20,
	Workflow_Execution_Canceled = 21,
	Request_Cancel_External_Workflow_Execution_Initiated = 22,
	Request_Cancel_External_Workflow_Execution_Failed = 23,
	External_Workflow_Execution_Cancel_Requested = 24,
	Marker_Recorded = 25,
	Workflow_Execution_Signaled = 26,
	Workflow_Execution_Terminated = 27,
	Workflow_Execution_Continued_As_New = 28,
	Start_Child_Workflow_Execution_Initiated = 29,
	Start_Child_Workflow_Execution_Failed = 30,
	Child_Workflow_Execution_Started = 31,
	Child_Workflow_Execution_Completed = 32,
	Child_Workflow_Execution_Failed = 33,
	Child_Workflow_Execution_Canceled = 34,
	Child_Workflow_Execution_Timed_Out = 35,
	Child_Workflow_Execution_Terminated = 36,
	Signal_External_Workflow_Execution_Initiated = 37,
	Signal_External_Workflow_Execution_Failed = 38,
	External_Workflow_Execution_Signaled = 39,
	Upsert_Workflow_Search_Attributes = 40,
	Workflow_Execution_Update_Accepted = 41,
	Workflow_Execution_Update_Rejected = 42,
	Workflow_Execution_Update_Completed = 43,
	Workflow_Properties_Modified_Externally = 44,
	Activity_Properties_Modified_Externally = 45,
	Workflow_Properties_Modified = 46,
	Workflow_Execution_Update_Admitted = 47,
	Nexus_Operation_Scheduled = 48,
	Nexus_Operation_Started = 49,
	Nexus_Operation_Completed = 50,
	Nexus_Operation_Failed = 51,
	Nexus_Operation_Canceled = 52,
	Nexus_Operation_Timed_Out = 53,
	Nexus_Operation_Cancel_Requested = 54,
	Workflow_Execution_Options_Updated = 55,
	Nexus_Operation_Cancel_Request_Completed = 56,
	Nexus_Operation_Cancel_Request_Failed = 57,
	Workflow_Execution_Paused = 58,
	Workflow_Execution_Unpaused = 59,
	Workflow_Execution_Time_Skipping_Transitioned = 60,
}

// Workflow_Event_Link values and their strings are borrowed during synchronous
// Workflow-start encoding.
Workflow_Event_Link :: struct {
	namespace:           string,
	workflow_id:         string,
	run_id:              string,
	reference_kind:      Workflow_Event_Reference_Kind,
	has_event_reference: bool,
	event_id:            i64,
	request_id:          string,
	event_type:          Workflow_Event_Type,
}

// workflow_event_link_from_nexus_url translates Temporal's Nexus link URL to
// the common WorkflowEvent link attached to a completer Workflow. Returned
// strings borrow url and must not outlive it.
workflow_event_link_from_nexus_url :: proc(url: string) -> (Workflow_Event_Link, Error) {
	SCHEME :: "temporal://"
	if !strings.has_prefix(url, SCHEME) {
		return {}, error_from_string(.Invalid_Argument, "Nexus link must use the temporal scheme")
	}
	without_scheme := url[len(SCHEME):]
	path_offset := strings.index(without_scheme, "/namespaces/")
	if path_offset < 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus link has no Temporal Workflow path")
	}
	path_and_query := without_scheme[path_offset:]
	path := path_and_query
	query := ""
	if query_offset := strings.index_byte(path_and_query, '?'); query_offset >= 0 {
		path = path_and_query[:query_offset]
		query = path_and_query[query_offset + 1:]
	}
	parts, split_error := strings.split(path, "/")
	if split_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not parse Nexus Workflow link")
	}
	defer delete(parts)
	if len(parts) != 7 || parts[1] != "namespaces" || parts[3] != "workflows" ||
	   parts[6] != "history" || len(parts[2]) == 0 || len(parts[4]) == 0 || len(parts[5]) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus link has an invalid Temporal Workflow path")
	}
	result := Workflow_Event_Link{
		namespace = parts[2],
		workflow_id = parts[4],
		run_id = parts[5],
	}
	if len(query) == 0 {
		return result, {}
	}
	event_id: i64
	event_type := Workflow_Event_Type.Unspecified
	query_remaining := query
	for {
		part := query_remaining
		if separator := strings.index_byte(query_remaining, '&'); separator >= 0 {
			part = query_remaining[:separator]
			query_remaining = query_remaining[separator + 1:]
		} else {
			query_remaining = ""
		}
		separator := strings.index_byte(part, '=')
		if separator > 0 {
			key := part[:separator]
			value := part[separator + 1:]
			if key == "eventID" {
				parsed, ok := strconv.parse_i64(value)
				if !ok || parsed <= 0 {
					return {}, error_from_string(.Invalid_Argument, "Nexus link has an invalid event ID")
				}
				event_id = parsed
			} else if key == "eventType" {
				if value != "NexusOperationScheduled" &&
				   value != "EVENT_TYPE_NEXUS_OPERATION_SCHEDULED" {
					return {}, error_from_string(.Invalid_Argument, "Nexus link has an unsupported event type")
				}
				event_type = .Nexus_Operation_Scheduled
			}
		}
		if len(query_remaining) == 0 do break
	}
	if event_id == 0 || event_type == .Unspecified {
		return {}, error_from_string(.Invalid_Argument, "Nexus link has an incomplete event reference")
	}
	result.has_event_reference = true
	result.event_id = event_id
	result.event_type = event_type
	return result, {}
}

// Signal_With_Start_Options borrows ordinary start options plus the one Header
// map exposed by Temporal's SignalWithStart RPC. That Header belongs to the
// Signal event. The wire contract cannot represent independent Workflow-start
// and Signal headers, so start_options.metadata.headers must be empty.
Signal_With_Start_Options :: struct {
	start_options:  Start_Workflow_Options,
	signal_headers: ^Headers,
}

// Workflow_Id_Reuse_Policy controls reuse after a previous execution with the
// same ID has closed. Unspecified uses the Service default (Allow_Duplicate).
Workflow_Id_Reuse_Policy :: enum {
	Unspecified,
	Allow_Duplicate,
	Allow_Duplicate_Failed_Only,
	Reject_Duplicate,
	// Deprecated by Temporal. Prefer Allow_Duplicate together with the
	// Terminate_Existing conflict policy.
	Terminate_If_Running,
}

// Workflow_Id_Conflict_Policy controls a start that conflicts with a currently
// running execution. Unspecified uses the Service default (Fail).
Workflow_Id_Conflict_Policy :: enum {
	Unspecified,
	Fail,
	Use_Existing,
	Terminate_Existing,
}

// Retry_Policy is borrowed by Start_Workflow_Options during synchronous request
// encoding. Zero intervals/coefficient use Temporal Service defaults; zero
// maximum_attempts means unlimited attempts up to Workflow timeouts.
Retry_Policy :: struct {
	initial_interval:          time.Duration,
	backoff_coefficient:       f64,
	maximum_interval:          time.Duration,
	maximum_attempts:          i32,
	non_retryable_error_types: []string,
}

// Priority is borrowed during synchronous request encoding. Zero values select
// Service defaults. Lower positive priority keys are dispatched first.
Priority :: struct {
	priority_key:    i32,
	fairness_key:    string,
	fairness_weight: f32,
}

Search_Attribute_Type :: codec.Search_Attribute_Type

Start_Metadata_Named_Payload :: struct {
	key:     string,
	payload: codec.Payload,
}

// Headers is shared with Workflow command options. It owns a deterministic
// insertion-ordered map of Temporal Payloads and must outlive each synchronous
// call whose options borrow it.
Headers :: temporal_headers.Headers

headers_init :: proc(headers: ^Headers, allocator := context.allocator) -> Error {
	result := temporal_headers.init(headers, allocator)
	if result == .None {
		return {}
	}
	if result == .Invalid_Argument && headers != nil && headers.allocator.procedure != nil {
		return error_from_string(.Invalid_Argument, "Temporal headers are already initialized")
	}
	if result == .Invalid_Argument {
		return error_from_string(.Invalid_Argument, "Temporal headers require an allocator")
	}
	return error_from_string(.Payload_Conversion, "could not initialize Temporal headers")
}

headers_free :: proc(headers: ^Headers) {
	temporal_headers.free(headers)
}

Start_Metadata_Search_Attribute :: struct {
	key:            string,
	attribute_type: Search_Attribute_Type,
	payload:        codec.Payload,
}

// Start_Metadata explicitly owns cloned header, memo, and Search Attribute
// Payloads. Initialize it before adding values and free it after the synchronous
// start call returns.
Start_Metadata :: struct {
	headers:           [dynamic]Start_Metadata_Named_Payload,
	memo:              [dynamic]Start_Metadata_Named_Payload,
	search_attributes: [dynamic]Start_Metadata_Search_Attribute,
	static_summary:    codec.Payload,
	has_static_summary: bool,
	static_details:    codec.Payload,
	has_static_details: bool,
	allocator:         mem.Allocator,
}

start_metadata_init :: proc(
	metadata: ^Start_Metadata,
	allocator := context.allocator,
) -> Error {
	if metadata == nil || allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata requires an allocator")
	}
	if metadata.allocator.procedure != nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is already initialized")
	}
	metadata.headers = make([dynamic]Start_Metadata_Named_Payload, allocator)
	metadata.memo = make([dynamic]Start_Metadata_Named_Payload, allocator)
	metadata.search_attributes = make([dynamic]Start_Metadata_Search_Attribute, allocator)
	metadata.allocator = allocator
	return {}
}

start_metadata_free :: proc(metadata: ^Start_Metadata) {
	if metadata == nil || metadata.allocator.procedure == nil {
		return
	}
	allocator := metadata.allocator
	for &entry in metadata.headers {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	for &entry in metadata.memo {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	for &entry in metadata.search_attributes {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	codec.payload_free(&metadata.static_summary)
	codec.payload_free(&metadata.static_details)
	delete(metadata.headers)
	delete(metadata.memo)
	delete(metadata.search_attributes)
	metadata^ = {}
}

// Static summary/details are UI-facing strings stored as Temporal UserMetadata.
// They are owned by Start_Metadata and encoded with the default JSON Payload
// convention required by Temporal.
start_metadata_set_static_summary :: proc(metadata: ^Start_Metadata, value: string) -> Error {
	if metadata == nil || metadata.allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	payload, encode_error := codec.encode_json(value, metadata.allocator)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow static summary")
	}
	if metadata.has_static_summary {
		codec.payload_free(&metadata.static_summary)
	}
	metadata.static_summary = payload
	metadata.has_static_summary = true
	return {}
}

start_metadata_set_static_details :: proc(metadata: ^Start_Metadata, value: string) -> Error {
	if metadata == nil || metadata.allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	payload, encode_error := codec.encode_json(value, metadata.allocator)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow static details")
	}
	if metadata.has_static_details {
		codec.payload_free(&metadata.static_details)
	}
	metadata.static_details = payload
	metadata.has_static_details = true
	return {}
}

start_metadata_put_named_owned :: proc(
	entries: ^[dynamic]Start_Metadata_Named_Payload,
	key: string,
	payload: codec.Payload,
	allocator: mem.Allocator,
) -> Error {
	owned_payload := payload
	if len(key) == 0 {
		codec.payload_free(&owned_payload)
		return error_from_string(.Invalid_Argument, "Workflow metadata key must not be empty")
	}
	for &entry in entries^ {
		if entry.key == key {
			codec.payload_free(&entry.payload)
			entry.payload = owned_payload
			return {}
		}
	}
	owned_key, key_error := strings.clone(key, allocator)
	if key_error != nil {
		codec.payload_free(&owned_payload)
		return error_from_string(.Payload_Conversion, "could not allocate Workflow metadata key")
	}
	append(entries, Start_Metadata_Named_Payload{key = owned_key, payload = owned_payload})
	return {}
}

headers_add_payload :: proc(headers: ^Headers, key: string, value: codec.Payload_View) -> Error {
	switch result := temporal_headers.add_payload(headers, key, value); result {
	case .None:
		return {}
	case .Invalid_Argument:
		return error_from_string(.Invalid_Argument, "Temporal headers are not initialized or the key is empty")
	case .Payload_Conversion, .Allocation_Failed:
		return error_from_string(.Payload_Conversion, "could not clone Temporal header Payload")
	}
	return error_from_string(.Payload_Conversion, "could not clone Temporal header Payload")
}

headers_add_json :: proc(headers: ^Headers, key: string, value: $T) -> Error {
	switch result := temporal_headers.add_json(headers, key, value); result {
	case .None:
		return {}
	case .Invalid_Argument:
		return error_from_string(.Invalid_Argument, "Temporal headers are not initialized or the key is empty")
	case .Payload_Conversion, .Allocation_Failed:
		return error_from_string(.Payload_Conversion, "could not encode Temporal header")
	}
	return error_from_string(.Payload_Conversion, "could not encode Temporal header")
}

start_metadata_add_named_payload :: proc(
	metadata: ^Start_Metadata,
	entries: ^[dynamic]Start_Metadata_Named_Payload,
	key: string,
	value: codec.Payload_View,
) -> Error {
	if metadata == nil || metadata.allocator.procedure == nil || entries == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	payload, clone_error := codec.payload_clone(value, metadata.allocator)
	if clone_error != .None {
		return error_from_string(.Payload_Conversion, "could not clone Workflow metadata Payload")
	}
	return start_metadata_put_named_owned(entries, key, payload, metadata.allocator)
}

start_metadata_add_header_payload :: proc(
	metadata: ^Start_Metadata,
	key: string,
	value: codec.Payload_View,
) -> Error {
	if metadata == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	return start_metadata_add_named_payload(metadata, &metadata.headers, key, value)
}

start_metadata_add_header_json :: proc(metadata: ^Start_Metadata, key: string, value: $T) -> Error {
	if metadata == nil || metadata.allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	payload, encode_error := codec.encode_json(value, metadata.allocator)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow header")
	}
	return start_metadata_put_named_owned(&metadata.headers, key, payload, metadata.allocator)
}

start_metadata_add_memo_payload :: proc(
	metadata: ^Start_Metadata,
	key: string,
	value: codec.Payload_View,
) -> Error {
	if metadata == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	return start_metadata_add_named_payload(metadata, &metadata.memo, key, value)
}

start_metadata_add_memo_json :: proc(metadata: ^Start_Metadata, key: string, value: $T) -> Error {
	if metadata == nil || metadata.allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	payload, encode_error := codec.encode_json(value, metadata.allocator)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow memo")
	}
	return start_metadata_put_named_owned(&metadata.memo, key, payload, metadata.allocator)
}

start_metadata_put_search_owned :: proc(
	metadata: ^Start_Metadata,
	key: string,
	attribute_type: Search_Attribute_Type,
	payload: codec.Payload,
) -> Error {
	owned_payload := payload
	if len(key) == 0 {
		codec.payload_free(&owned_payload)
		return error_from_string(.Invalid_Argument, "Search Attribute key must not be empty")
	}
	if codec.payload_set_search_attribute_type(&owned_payload, attribute_type) != .None {
		codec.payload_free(&owned_payload)
		return error_from_string(.Payload_Conversion, "could not encode Search Attribute type")
	}
	for &entry in metadata.search_attributes {
		if entry.key == key {
			codec.payload_free(&entry.payload)
			entry.attribute_type = attribute_type
			entry.payload = owned_payload
			return {}
		}
	}
	owned_key, key_error := strings.clone(key, metadata.allocator)
	if key_error != nil {
		codec.payload_free(&owned_payload)
		return error_from_string(.Payload_Conversion, "could not allocate Search Attribute key")
	}
	append(
		&metadata.search_attributes,
		Start_Metadata_Search_Attribute{
			key = owned_key,
			attribute_type = attribute_type,
			payload = owned_payload,
		},
	)
	return {}
}

start_metadata_add_search_attribute_payload :: proc(
	metadata: ^Start_Metadata,
	key: string,
	attribute_type: Search_Attribute_Type,
	value: codec.Payload_View,
) -> Error {
	if metadata == nil || metadata.allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	payload, clone_error := codec.payload_clone(value, metadata.allocator)
	if clone_error != .None {
		return error_from_string(.Payload_Conversion, "could not clone Search Attribute Payload")
	}
	return start_metadata_put_search_owned(metadata, key, attribute_type, payload)
}

start_metadata_add_search_attribute_json :: proc(
	metadata: ^Start_Metadata,
	key: string,
	attribute_type: Search_Attribute_Type,
	value: $T,
) -> Error {
	if metadata == nil || metadata.allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Workflow start metadata is not initialized")
	}
	payload, encode_error := codec.encode_json(value, metadata.allocator)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Search Attribute")
	}
	return start_metadata_put_search_owned(metadata, key, attribute_type, payload)
}

Workflow_Handle :: struct {
	client:                 ^Client,
	id:                     string,
	run_id:                 string,
	first_execution_run_id: string,
	started_by_request:      bool,
	eager_execution_accepted: bool,
}

Signal_Workflow_Options :: struct {
	request_id: string,
	headers:    ^Headers,
}

Update_Workflow_Options :: struct {
	update_id: string,
	headers:   ^Headers,
}

// Update_With_Start_Options describes the two operations Temporal starts
// atomically. Use_Existing is required so the Update can attach to an already
// running execution instead of failing the Start operation.
Update_With_Start_Options :: struct {
	start_options:  Start_Workflow_Options,
	update_options: Update_Workflow_Options,
}

// Update_With_Start_Result owns both handles returned by ExecuteMultiOperation.
// Neither handle owns the Client, which must remain open while either is used.
Update_With_Start_Result :: struct {
	workflow: Workflow_Handle,
	update:   Update_Handle,
}

Client_Cancellation_Token :: temporal_core.Rpc_Cancellation_Token

client_cancellation_token_create :: proc() -> Client_Cancellation_Token {
	return temporal_core.rpc_cancellation_token_create()
}

client_cancellation_token_cancel :: proc(token: ^Client_Cancellation_Token) {
	temporal_core.rpc_cancellation_token_cancel(token)
}

client_cancellation_token_close :: proc(token: ^Client_Cancellation_Token) {
	temporal_core.rpc_cancellation_token_close(token)
}

// Update_Wait_Options controls only the local wait for a durable Update. A
// timeout or cancellation never cancels an Update already accepted by Server.
Update_Wait_Options :: struct {
	timeout:            time.Duration,
	cancellation_token: ^Client_Cancellation_Token,
}

// Query_Reject_Condition controls whether Temporal may deliver a Query for a
// closed execution. Unspecified preserves the Service default.
Query_Reject_Condition :: enum {
	Unspecified,
	None,
	Not_Open,
	Not_Completed_Cleanly,
}

Query_Workflow_Options :: struct {
	reject_condition: Query_Reject_Condition,
	headers:          ^Headers,
}

// Reset_Reapply_Exclude_Type identifies event families that must not be copied
// from after the reset point into the new run. An empty slice preserves the
// Service behavior; the pinned API schema documents signal reapplication as
// the default of its deprecated compatibility field.
Reset_Reapply_Exclude_Type :: enum {
	Signals,
	Updates,
	Nexus,
}

Post_Reset_Operation_Kind :: enum {
	Invalid,
	Signal_Workflow,
	Update_Workflow_Options,
}

Post_Reset_Signal_Workflow :: struct {
	signal_name: string,
	inputs:      []codec.Payload_View,
	headers:     ^Headers,
	links:       []Link,
}

Workflow_Versioning_Override_Kind :: enum {
	Unspecified,
	Pinned,
	Auto_Upgrade,
	One_Time,
}

// Deployment identity is required for One_Time and optional for Pinned. An
// omitted Pinned version asks Server to retain the execution's current pin.
Workflow_Versioning_Override :: struct {
	kind:            Workflow_Versioning_Override_Kind,
	deployment_name: string,
	build_id:         string,
}

Workflow_Fast_Forward_Config :: struct {
	id:       string,
	duration: time.Duration,
}

Workflow_Time_Skipping_Config :: struct {
	enabled:                bool,
	fast_forward:           ^Workflow_Fast_Forward_Config,
	disable_propagation:    bool,
	max_session_skip_count: i32,
}

// update_mask uses the pinned proto field names: "versioning_override",
// "priority", and "time_skipping_config". A masked field whose pointer is nil
// is explicitly cleared by Server.
Workflow_Execution_Options_Update :: struct {
	update_mask:          []string,
	versioning_override:  ^Workflow_Versioning_Override,
	priority:             ^Priority,
	time_skipping_config: ^Workflow_Time_Skipping_Config,
}

// Compatibility name for the same update contract when embedded in Reset.
Post_Reset_Update_Workflow_Options :: Workflow_Execution_Options_Update

Post_Reset_Operation :: struct {
	kind:                    Post_Reset_Operation_Kind,
	signal_workflow:         Post_Reset_Signal_Workflow,
	update_workflow_options: Workflow_Execution_Options_Update,
}

Reset_Workflow_Options :: struct {
	// Must identify a WorkflowTaskCompleted, WorkflowTaskTimedOut,
	// WorkflowTaskFailed, or WorkflowTaskStarted event in the selected run.
	workflow_task_finish_event_id: i64,
	reason:                        string,
	request_id:                    string,
	reapply_exclude_types:          []Reset_Reapply_Exclude_Type,
	post_reset_operations:         []Post_Reset_Operation,
}

query_reject_condition_to_proto :: proc(
	condition: Query_Reject_Condition,
) -> proto.Temporal_Api_Enums_V1_QueryRejectCondition {
	switch condition {
	case .Unspecified:
		return .QUERY_REJECT_CONDITION_UNSPECIFIED
	case .None:
		return .QUERY_REJECT_CONDITION_NONE
	case .Not_Open:
		return .QUERY_REJECT_CONDITION_NOT_OPEN
	case .Not_Completed_Cleanly:
		return .QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY
	}
	return .QUERY_REJECT_CONDITION_UNSPECIFIED
}

// Update_Handle owns the stable server reference returned after an Update has
// reached Accepted (or Completed). It does not own the Client; the Client must
// remain open until the handle is no longer used.
Update_Handle :: struct {
	client:          ^Client,
	workflow_id:     string,
	run_id:          string,
	update_id:       string,
	outcome_present: bool,
	outcome:         codec.Payload,
	outcome_error:   Error,
}

workflow_handle_free :: proc(handle: ^Workflow_Handle) {
	if handle == nil {
		return
	}
	heap := allocator_runtime.heap_allocator()
	delete(handle.id, heap)
	delete(handle.run_id, heap)
	delete(handle.first_execution_run_id, heap)
	handle^ = {}
}

update_handle_free :: proc(handle: ^Update_Handle) {
	if handle == nil {
		return
	}
	heap := allocator_runtime.heap_allocator()
	delete(handle.workflow_id, heap)
	delete(handle.run_id, heap)
	delete(handle.update_id, heap)
	codec.payload_free(&handle.outcome)
	delete(handle.outcome_error.message, heap)
	delete(handle.outcome_error.details, heap)
	handle^ = {}
}

update_with_start_result_free :: proc(result: ^Update_With_Start_Result) {
	if result == nil {
		return
	}
	workflow_handle_free(&result.workflow)
	update_handle_free(&result.update)
	result^ = {}
}

get_update_handle :: proc(
	client: ^Client,
	workflow_id, run_id, update_id: string,
) -> (handle: Update_Handle, error: Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	if len(workflow_id) == 0 || len(update_id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow and Update IDs must not be empty")
	}
	heap := allocator_runtime.heap_allocator()
	owned_workflow_id, workflow_id_error := strings.clone(workflow_id, heap)
	if workflow_id_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not allocate Update handle")
	}
	owned_run_id, run_id_error := strings.clone(run_id, heap)
	if run_id_error != nil {
		delete(owned_workflow_id, heap)
		return {}, error_from_string(.Malformed_Response, "could not allocate Update handle")
	}
	owned_update_id, update_id_error := strings.clone(update_id, heap)
	if update_id_error != nil {
		delete(owned_run_id, heap)
		delete(owned_workflow_id, heap)
		return {}, error_from_string(.Malformed_Response, "could not allocate Update handle")
	}
	return {
		client = client,
		workflow_id = owned_workflow_id,
		run_id = owned_run_id,
		update_id = owned_update_id,
	}, {}
}

get_workflow_handle :: proc(
	client: ^Client,
	id: string,
	run_id := "",
	first_execution_run_id := "",
) -> (handle: Workflow_Handle, error: Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	if len(id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow ID must not be empty")
	}
	heap := allocator_runtime.heap_allocator()
	owned_id, id_error := strings.clone(id, heap)
	if id_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow handle")
	}
	owned_run_id, run_id_error := strings.clone(run_id, heap)
	if run_id_error != nil {
		delete(owned_id, heap)
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow handle")
	}
	owned_first_run_id, first_run_error := strings.clone(first_execution_run_id, heap)
	if first_run_error != nil {
		delete(owned_run_id, heap)
		delete(owned_id, heap)
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow handle")
	}
	return {
		client = client,
		id = owned_id,
		run_id = owned_run_id,
		first_execution_run_id = owned_first_run_id,
	}, {}
}

duration_proto :: proc(value: time.Duration) -> proto.Google_Protobuf_Duration {
	nanoseconds := i64(value)
	return {
		seconds = nanoseconds / i64(time.Second),
		nanos = i32(nanoseconds % i64(time.Second)),
	}
}

workflow_id_reuse_policy_to_proto :: proc(
	policy: Workflow_Id_Reuse_Policy,
) -> proto.Temporal_Api_Enums_V1_WorkflowIdReusePolicy {
	switch policy {
	case .Unspecified:
		return .WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED
	case .Allow_Duplicate:
		return .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	case .Allow_Duplicate_Failed_Only:
		return .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	case .Reject_Duplicate:
		return .WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	case .Terminate_If_Running:
		return .WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
	}
	return .WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED
}

workflow_id_conflict_policy_to_proto :: proc(
	policy: Workflow_Id_Conflict_Policy,
) -> proto.Temporal_Api_Enums_V1_WorkflowIdConflictPolicy {
	switch policy {
	case .Unspecified:
		return .WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED
	case .Fail:
		return .WORKFLOW_ID_CONFLICT_POLICY_FAIL
	case .Use_Existing:
		return .WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
	case .Terminate_Existing:
		return .WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
	}
	return .WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED
}

start_workflow_options_error :: proc(options: Start_Workflow_Options) -> string {
	if len(options.id) == 0 {
		return "Workflow ID must not be empty"
	}
	if len(options.task_queue) == 0 {
		return "Workflow task queue must not be empty"
	}
	if options.workflow_execution_timeout < 0 || options.workflow_run_timeout < 0 ||
	   options.workflow_task_timeout < 0 || options.start_delay < 0 {
		return "Workflow timeouts and start delay must not be negative"
	}
	if options.workflow_execution_timeout > 0 &&
	   options.workflow_run_timeout > options.workflow_execution_timeout {
		return "Workflow Run timeout must not exceed Execution timeout"
	}
	if options.start_delay > 0 && len(options.cron_schedule) > 0 {
		return "Workflow start delay cannot be combined with a cron schedule"
	}
	if options.id_reuse_policy == .Terminate_If_Running &&
	   options.id_conflict_policy != .Unspecified {
		return "Terminate_If_Running ID reuse cannot be combined with an ID conflict policy"
	}
	if options.retry_policy != nil {
		policy := options.retry_policy
		if policy.initial_interval < 0 || policy.maximum_interval < 0 ||
		   policy.maximum_attempts < 0 {
			return "Workflow Retry Policy values must not be negative"
		}
		if math.is_nan(policy.backoff_coefficient) || math.is_inf(policy.backoff_coefficient) ||
		   (policy.backoff_coefficient != 0 && policy.backoff_coefficient < 1) {
			return "Workflow Retry Policy backoff coefficient must be zero or at least one"
		}
		if policy.maximum_interval > 0 && policy.initial_interval > 0 &&
		   policy.maximum_interval < policy.initial_interval {
			return "Workflow Retry Policy maximum interval must not be shorter than its initial interval"
		}
	}
	if options.priority != nil {
		priority := options.priority
		if priority.priority_key < 0 {
			return "Workflow priority key must not be negative"
		}
		if len(priority.fairness_key) > 64 {
			return "Workflow priority fairness key must not exceed 64 bytes"
		}
		if math.is_nan(priority.fairness_weight) || math.is_inf(priority.fairness_weight) ||
		   priority.fairness_weight < 0 {
			return "Workflow priority fairness weight must be finite and non-negative"
		}
	}
	if options.metadata != nil && options.metadata.allocator.procedure == nil {
		return "Workflow start metadata is not initialized"
	}
	for link in options.links {
		if message := link_error(link); len(message) > 0 do return message
	}
	for link in options.workflow_event_links {
		if message := workflow_event_link_error(link); len(message) > 0 do return message
	}
	for callback in options.nexus_completion_callbacks {
		if len(callback.url) == 0 {
			return "Nexus completion callback URL must not be empty"
		}
		for header in callback.headers {
			if len(header.key) == 0 {
				return "Nexus completion callback header keys must not be empty"
			}
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

workflow_event_link_error :: proc(link: Workflow_Event_Link) -> string {
	if len(link.namespace) == 0 || len(link.workflow_id) == 0 || len(link.run_id) == 0 {
		return "Workflow event links require namespace, Workflow ID, and Run ID"
	}
	if i32(link.event_type) < i32(Workflow_Event_Type.Unspecified) ||
	   i32(link.event_type) > i32(Workflow_Event_Type.Workflow_Execution_Time_Skipping_Transitioned) {
		return "Workflow event links require a known event type"
	}
	reference_kind := link.reference_kind
	if reference_kind == .None && link.has_event_reference do reference_kind = .Event_Id
	switch reference_kind {
	case .Event_Id:
		if link.event_id <= 0 || link.event_type == .Unspecified {
			return "Workflow event links require a positive event ID and event type"
		}
	case .Request_Id:
		if len(link.request_id) == 0 || link.event_type == .Unspecified {
			return "Workflow event links require a request ID and event type"
		}
	case .None:
	case:
		return "Workflow event links require a known reference kind"
	}
	if link.has_event_reference && link.reference_kind == .Request_Id {
		return "Workflow event links cannot combine legacy event-reference state with a request reference"
	}
	if reference_kind != .Event_Id && link.event_id != 0 {
		return "Workflow event links cannot carry an event ID without an event reference"
	}
	if reference_kind != .Request_Id && len(link.request_id) > 0 {
		return "Workflow event links cannot carry a request ID without a request reference"
	}
	if reference_kind == .None && link.event_type != .Unspecified {
		return "Workflow event links cannot carry an event type without a reference"
	}
	if reference_kind == .Event_Id && len(link.request_id) > 0 {
		return "Workflow event links cannot combine event and request references"
	}
	if reference_kind == .Request_Id && link.event_id != 0 {
		return "Workflow event links cannot combine request and event references"
	}
	if link.has_event_reference &&
	   (link.event_id <= 0 || link.event_type == .Unspecified) {
		return "Workflow event links require a positive event ID and event type"
	}
	return ""
}

free_start_proto_payloads :: proc(payloads: []proto.Temporal_Api_Common_V1_Payload) {
	for &payload in payloads {
		delete(payload.metadata)
		delete(payload.external_payloads)
	}
	delete(payloads)
}

// encode_start_workflow_request_payloads is the deterministic repeated-input
// request boundary used by the Client and wire-contract tests. Returned bytes
// are owned by the caller.
encode_start_workflow_request_payloads :: proc(
	namespace: string,
	workflow_type: string,
	inputs: []codec.Payload_View,
	options: Start_Workflow_Options,
	header_override: ^Headers = nil,
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Temporal namespace must not be empty")
	}
	if len(workflow_type) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Workflow type must not be empty")
	}
	if options_message := start_workflow_options_error(options); len(options_message) > 0 {
		return nil, error_from_string(.Invalid_Argument, options_message)
	}
	if header_override != nil && header_override.allocator.procedure == nil {
		return nil, error_from_string(.Invalid_Argument, "Workflow start interceptor headers are not initialized")
	}

	request_id := options.request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(request_id) == 0 {
		request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	}
	workflow_type_value := proto.Temporal_Api_Common_V1_WorkflowType{name = workflow_type}
	task_queue := proto.Temporal_Api_Taskqueue_V1_TaskQueue{name = options.task_queue}
	encoded_inputs := make([]proto.Temporal_Api_Common_V1_Payload, len(inputs))
	defer free_start_proto_payloads(encoded_inputs)
	input_payloads := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(input_payloads.payloads)
	for input, index in inputs {
		encoded_inputs[index] = proto.encode_payload(input)
		append(&input_payloads.payloads, encoded_inputs[index])
	}

	request := proto.Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest{
		namespace = namespace,
		workflow_id = options.id,
		workflow_type = &workflow_type_value,
		task_queue = &task_queue,
		input = &input_payloads,
		identity = "temporal-sdk-odin@local",
		request_id = request_id,
		workflow_id_reuse_policy = workflow_id_reuse_policy_to_proto(options.id_reuse_policy),
		workflow_id_conflict_policy = workflow_id_conflict_policy_to_proto(options.id_conflict_policy),
		cron_schedule = options.cron_schedule,
		request_eager_execution = options.enable_eager_workflow_start,
	}
	total_link_count := len(options.links) + len(options.workflow_event_links)
	for callback in options.nexus_completion_callbacks {
		total_link_count += len(callback.links) + len(callback.workflow_event_links)
	}
	link_storage := encoded_link_storage_make(total_link_count)
	defer encoded_link_storage_free(&link_storage)
	for link in options.links {
		append(&request.links, encode_link(link, &link_storage))
	}
	for link in options.workflow_event_links {
		append(
			&request.links,
			encode_link({kind = .Workflow_Event, workflow_event = link}, &link_storage),
		)
	}
	callback_values := make(
		[]proto.Temporal_Api_Common_V1_Callback_Nexus,
		len(options.nexus_completion_callbacks),
	)
	callback_headers := make(
		[][dynamic]proto.Temporal_Api_Common_V1_Callback_Nexus_HeaderEntry,
		len(options.nexus_completion_callbacks),
	)
	callback_links := make(
		[][dynamic]proto.Temporal_Api_Common_V1_Link,
		len(options.nexus_completion_callbacks),
	)
	defer {
		for headers in callback_headers do delete(headers)
		for links in callback_links do delete(links)
		delete(callback_links)
		delete(callback_headers)
		delete(callback_values)
		delete(request.completion_callbacks)
		delete(request.links)
	}
	for callback, index in options.nexus_completion_callbacks {
		callback_values[index].url = callback.url
		for header in callback.headers {
			append(
				&callback_headers[index],
				proto.Temporal_Api_Common_V1_Callback_Nexus_HeaderEntry{
					key = header.key,
					value = header.value,
				},
			)
		}
		callback_values[index].header = callback_headers[index]
		for link in callback.links {
			append(&callback_links[index], encode_link(link, &link_storage))
		}
		for link in callback.workflow_event_links {
			append(
				&callback_links[index],
				encode_link({kind = .Workflow_Event, workflow_event = link}, &link_storage),
			)
		}
		append(
			&request.completion_callbacks,
			proto.Temporal_Api_Common_V1_Callback{
				links = callback_links[index],
				variant = proto.Temporal_Api_Common_V1_Callback_Variant_Nexus{
					value = &callback_values[index],
				},
			},
		)
	}
	execution_timeout := duration_proto(options.workflow_execution_timeout)
	run_timeout := duration_proto(options.workflow_run_timeout)
	task_timeout := duration_proto(options.workflow_task_timeout)
	if options.workflow_execution_timeout > 0 {
		request.workflow_execution_timeout = &execution_timeout
	}
	if options.workflow_run_timeout > 0 {
		request.workflow_run_timeout = &run_timeout
	}
	if options.workflow_task_timeout > 0 {
		request.workflow_task_timeout = &task_timeout
	}
	start_delay := duration_proto(options.start_delay)
	if options.start_delay > 0 {
		request.workflow_start_delay = &start_delay
	}

	retry_policy := proto.Temporal_Api_Common_V1_RetryPolicy{}
	retry_initial_interval := proto.Google_Protobuf_Duration{}
	retry_maximum_interval := proto.Google_Protobuf_Duration{}
	defer delete(retry_policy.non_retryable_error_types)
	if options.retry_policy != nil {
		policy := options.retry_policy
		retry_policy.backoff_coefficient = policy.backoff_coefficient
		retry_policy.maximum_attempts = policy.maximum_attempts
		if policy.initial_interval > 0 {
			retry_initial_interval = duration_proto(policy.initial_interval)
			retry_policy.initial_interval = &retry_initial_interval
		}
		if policy.maximum_interval > 0 {
			retry_maximum_interval = duration_proto(policy.maximum_interval)
			retry_policy.maximum_interval = &retry_maximum_interval
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
	memo := proto.Temporal_Api_Common_V1_Memo{}
	search_attributes := proto.Temporal_Api_Common_V1_SearchAttributes{}
	user_metadata := proto.Temporal_Api_Sdk_V1_UserMetadata{}
	static_summary := proto.Temporal_Api_Common_V1_Payload{}
	static_details := proto.Temporal_Api_Common_V1_Payload{}
	header_payloads: []proto.Temporal_Api_Common_V1_Payload
	memo_payloads: []proto.Temporal_Api_Common_V1_Payload
	search_payloads: []proto.Temporal_Api_Common_V1_Payload
	defer delete(header.fields)
	defer delete(memo.fields)
	defer delete(search_attributes.indexed_fields)
	defer free_start_proto_payloads(header_payloads)
	defer free_start_proto_payloads(memo_payloads)
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
		if header_override == nil && len(metadata.headers) > 0 {
			header_payloads = make([]proto.Temporal_Api_Common_V1_Payload, len(metadata.headers))
			for &entry, index in metadata.headers {
				header_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
			}
			for entry, index in metadata.headers {
				append(
					&header.fields,
					proto.Temporal_Api_Common_V1_Header_FieldsEntry{
						key = entry.key,
						value = &header_payloads[index],
					},
				)
			}
			request.header = &header
		}
		if len(metadata.memo) > 0 {
			memo_payloads = make([]proto.Temporal_Api_Common_V1_Payload, len(metadata.memo))
			for &entry, index in metadata.memo {
				memo_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
			}
			for entry, index in metadata.memo {
				append(
					&memo.fields,
					proto.Temporal_Api_Common_V1_Memo_FieldsEntry{
						key = entry.key,
						value = &memo_payloads[index],
					},
				)
			}
			request.memo = &memo
		}
		if len(metadata.search_attributes) > 0 {
			search_payloads = make(
				[]proto.Temporal_Api_Common_V1_Payload,
				len(metadata.search_attributes),
			)
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
	if header_override != nil && len(header_override.entries) > 0 {
		header_payloads = make([]proto.Temporal_Api_Common_V1_Payload, len(header_override.entries))
		for &entry, index in header_override.entries {
			header_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
		}
		for entry, index in header_override.entries {
			append(
				&header.fields,
				proto.Temporal_Api_Common_V1_Header_FieldsEntry{
					key = entry.key,
					value = &header_payloads[index],
				},
			)
		}
		request.header = &header
	}

	return proto.encode_Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest(&request), {}
}

// encode_start_workflow_request preserves the original single-argument API.
// Temporal's wire contract is repeated, so new codec-neutral callers should
// use encode_start_workflow_request_payloads.
encode_start_workflow_request :: proc(
	namespace: string,
	workflow_type: string,
	input: codec.Payload_View,
	options: Start_Workflow_Options,
	header_override: ^Headers = nil,
) -> (request_bytes: [dynamic]u8, error: Error) {
	inputs := [?]codec.Payload_View{input}
	return encode_start_workflow_request_payloads(
		namespace,
		workflow_type,
		inputs[:],
		options,
		header_override,
	)
}

start_workflow_payloads_unintercepted :: proc(
	client: ^Client,
	workflow_type: string,
	inputs: []codec.Payload_View,
	options: Start_Workflow_Options,
	headers: ^Headers,
) -> (handle: Workflow_Handle, error: Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	request_bytes, request_error := encode_start_workflow_request_payloads(
		client.namespace,
		workflow_type,
		inputs,
		options,
		headers,
	)
	if request_error.message != nil {
		return {}, request_error
	}
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"StartWorkflowExecution",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize client response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_StartWorkflowExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_StartWorkflowExecutionResponse(
		rpc_result.protobuf,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded || len(response.run_id) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed start response")
	}

	first_execution_run_id := response.first_execution_run_id
	if len(first_execution_run_id) == 0 {
		first_execution_run_id = response.run_id
	}
	handle_error: Error
	handle, handle_error = get_workflow_handle(
		client,
		options.id,
		response.run_id,
		first_execution_run_id,
	)
	if handle_error.message != nil {
		return {}, handle_error
	}
	handle.eager_execution_accepted = response.eager_workflow_task != nil
	handle.started_by_request = response.started
	return handle, {}
}

start_workflow_payload_terminal :: proc(
	input: ^Client_Start_Workflow_Input,
) -> (Workflow_Handle, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "start Workflow input is missing")
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "start Workflow interceptor headers are invalid")
	}
	return start_workflow_payloads_unintercepted(
		input.client,
		input.workflow_type,
		input.inputs,
		input.options,
		input.headers,
	)
}

start_workflow_payloads :: proc(
	client: ^Client,
	workflow_type: string,
	inputs: []codec.Payload_View,
	options: Start_Workflow_Options,
) -> (Workflow_Handle, Error) {
	headers := Headers{}
	headers_error := client_start_interceptor_headers_init(&headers, options.metadata)
	if headers_error.message != nil {
		return {}, headers_error
	}
	defer headers_free(&headers)
	interceptor_input := Client_Start_Workflow_Input{
		client = client,
		workflow_type = workflow_type,
		inputs = inputs,
		options = options,
		headers = &headers,
	}
	state := Client_Interceptor_State{client = client}
	return client_start_workflow_chain(&state, &interceptor_input)
}

start_workflow_payload :: proc(
	client: ^Client,
	workflow_type: string,
	input: codec.Payload_View,
	options: Start_Workflow_Options,
) -> (Workflow_Handle, Error) {
	inputs := [?]codec.Payload_View{input}
	return start_workflow_payloads(client, workflow_type, inputs[:], options)
}

start_workflow_json :: proc(
	client: ^Client,
	workflow_type: string,
	input: $T,
	options: Start_Workflow_Options,
) -> (handle: Workflow_Handle, error: Error) {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode Workflow input")
	}
	defer codec.payload_free(&payload)
	return start_workflow_payload(client, workflow_type, codec.payload_view(&payload), options)
}

update_with_start_options_error :: proc(options: Update_With_Start_Options) -> string {
	if message := start_workflow_options_error(options.start_options); len(message) > 0 {
		return message
	}
	if options.start_options.id_conflict_policy != .Use_Existing {
		return "Update-With-Start requires the Use_Existing Workflow ID conflict policy"
	}
	if len(options.start_options.cron_schedule) > 0 {
		return "Update-With-Start cannot use a cron schedule"
	}
	if options.start_options.enable_eager_workflow_start {
		return "Update-With-Start cannot request eager Workflow execution"
	}
	if options.start_options.start_delay != 0 {
		return "Update-With-Start cannot use a Workflow start delay"
	}
	return ""
}

// encode_update_with_start_request_payloads builds Temporal's only currently
// valid ExecuteMultiOperation shape: [StartWorkflow, UpdateWorkflow]. It
// decodes and nests the ordinary request encodings so those two established
// wire boundaries remain the single source of truth.
encode_update_with_start_request_payloads :: proc(
	namespace, workflow_type: string,
	workflow_inputs: []codec.Payload_View,
	update_name: string,
	update_inputs: []codec.Payload_View,
	options: Update_With_Start_Options,
	start_headers: ^Headers = nil,
	update_headers: ^Headers = nil,
	explicit_update_request_id := "",
) -> (request_bytes: [dynamic]u8, error: Error) {
	if message := update_with_start_options_error(options); len(message) > 0 {
		return nil, error_from_string(.Invalid_Argument, message)
	}
	if len(update_name) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Workflow Update name must not be empty")
	}
	update_options := options.update_options
	if update_headers != nil {
		update_options.headers = update_headers
	}
	start_bytes, start_error := encode_start_workflow_request_payloads(
		namespace,
		workflow_type,
		workflow_inputs,
		options.start_options,
		start_headers,
	)
	if start_error.message != nil {
		return nil, start_error
	}
	defer delete(start_bytes)
	update_bytes, update_error := encode_update_workflow_request_payloads(
		namespace,
		options.start_options.id,
		"",
		"",
		update_name,
		update_inputs,
		update_options,
		explicit_update_request_id,
	)
	if update_error.message != nil {
		return nil, update_error
	}
	defer delete(update_bytes)

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return nil, error_from_string(.Payload_Conversion, "could not initialize Update-With-Start request arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	start_request := proto.Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest{}
	update_request := proto.Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionRequest{}
	start_decoded := proto.decode_Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest(
		start_bytes[:],
		&start_request,
	)
	update_decoded := proto.decode_Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionRequest(
		update_bytes[:],
		&update_request,
	)
	context.allocator = previous_allocator
	if !start_decoded || !update_decoded {
		return nil, error_from_string(.Payload_Conversion, "could not compose Update-With-Start request")
	}

	operations := make(
		[dynamic]proto.Temporal_Api_Workflowservice_V1_ExecuteMultiOperationRequest_Operation,
		0,
		2,
	)
	defer delete(operations)
	append(
		&operations,
		proto.Temporal_Api_Workflowservice_V1_ExecuteMultiOperationRequest_Operation{
			operation = proto.Temporal_Api_Workflowservice_V1_ExecuteMultiOperationRequest_Operation_Operation_Start_Workflow{
				value = &start_request,
			},
		},
		proto.Temporal_Api_Workflowservice_V1_ExecuteMultiOperationRequest_Operation{
			operation = proto.Temporal_Api_Workflowservice_V1_ExecuteMultiOperationRequest_Operation_Operation_Update_Workflow{
				value = &update_request,
			},
		},
	)
	request := proto.Temporal_Api_Workflowservice_V1_ExecuteMultiOperationRequest{
		namespace = namespace,
		operations = operations,
		resource_id = options.start_options.id,
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_ExecuteMultiOperationRequest(&request), {}
}

preserve_update_handle :: proc(
	client: ^Client,
	response: ^proto.Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionResponse,
) -> (Update_Handle, Error) {
	if response == nil || response.update_ref == nil ||
	   response.update_ref.workflow_execution == nil ||
	   len(response.update_ref.workflow_execution.workflow_id) == 0 ||
	   len(response.update_ref.update_id) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Update response")
	}
	accepted := response.outcome != nil ||
		response.stage == .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED ||
		response.stage == .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
	if !accepted {
		return {}, error_from_string(.Malformed_Response, "Temporal returned Update-With-Start before Update acceptance")
	}
	handle, handle_error := get_update_handle(
		client,
		response.update_ref.workflow_execution.workflow_id,
		response.update_ref.workflow_execution.run_id,
		response.update_ref.update_id,
	)
	if handle_error.message != nil {
		return {}, handle_error
	}
	if response.outcome != nil {
		heap := allocator_runtime.heap_allocator()
		previous_allocator := context.allocator
		context.allocator = heap
		handle.outcome, handle.outcome_error = update_outcome_payload(response.outcome)
		context.allocator = previous_allocator
		if handle.outcome_error.message != nil &&
		   handle.outcome_error.kind != .Workflow_Update_Failed {
			error := handle.outcome_error
			handle.outcome_error = {}
			update_handle_free(&handle)
			return {}, error
		}
		handle.outcome_present = true
	}
	return handle, {}
}

decode_update_with_start_response :: proc(
	client: ^Client,
	workflow_id: string,
	data: []u8,
) -> (Update_With_Start_Result, Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Update-With-Start response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_ExecuteMultiOperationResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_ExecuteMultiOperationResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded || len(response.responses) != 2 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Update-With-Start response")
	}
	start_variant, start_ok := response.responses[0].response.(proto.Temporal_Api_Workflowservice_V1_ExecuteMultiOperationResponse_Response_Response_Start_Workflow)
	update_variant, update_ok := response.responses[1].response.(proto.Temporal_Api_Workflowservice_V1_ExecuteMultiOperationResponse_Response_Response_Update_Workflow)
	if !start_ok || start_variant.value == nil || len(start_variant.value.run_id) == 0 ||
	   !update_ok || update_variant.value == nil {
		return {}, error_from_string(.Malformed_Response, "Temporal returned Update-With-Start operations out of order")
	}
	first_execution_run_id := start_variant.value.first_execution_run_id
	if len(first_execution_run_id) == 0 {
		first_execution_run_id = start_variant.value.run_id
	}
	workflow_handle, workflow_error := get_workflow_handle(
		client,
		workflow_id,
		start_variant.value.run_id,
		first_execution_run_id,
	)
	if workflow_error.message != nil {
		return {}, workflow_error
	}
	workflow_handle.started_by_request = start_variant.value.started
	workflow_handle.eager_execution_accepted = false
	update_handle, update_error := preserve_update_handle(client, update_variant.value)
	if update_error.message != nil {
		workflow_handle_free(&workflow_handle)
		return {}, update_error
	}
	if update_handle.workflow_id != workflow_id ||
	   (len(update_handle.run_id) > 0 && update_handle.run_id != workflow_handle.run_id) {
		workflow_handle_free(&workflow_handle)
		update_handle_free(&update_handle)
		return {}, error_from_string(.Malformed_Response, "Temporal returned mismatched Update-With-Start handles")
	}
	return {workflow = workflow_handle, update = update_handle}, {}
}

update_with_start_payloads_unintercepted :: proc(
	client: ^Client,
	workflow_type: string,
	workflow_inputs: []codec.Payload_View,
	update_name: string,
	update_inputs: []codec.Payload_View,
	options: Update_With_Start_Options,
	start_headers, update_headers: ^Headers,
) -> (Update_With_Start_Result, Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	request_bytes, request_error := encode_update_with_start_request_payloads(
		client.namespace,
		workflow_type,
		workflow_inputs,
		update_name,
		update_inputs,
		options,
		start_headers,
		update_headers,
	)
	if request_error.message != nil {
		return {}, request_error
	}
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"ExecuteMultiOperation",
		request_bytes[:],
		timeout_millis = 0,
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_update_with_start_response(client, options.start_options.id, rpc_result.protobuf)
}

update_with_start_payloads_terminal :: proc(
	input: ^Client_Update_With_Start_Input,
) -> (Update_With_Start_Result, Error) {
	if input == nil || input.start_headers == nil || input.update_headers == nil ||
	   input.start_headers.allocator.procedure == nil ||
	   input.update_headers.allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Update-With-Start interceptor input is invalid")
	}
	return update_with_start_payloads_unintercepted(
		input.client,
		input.workflow_type,
		input.workflow_inputs,
		input.update_name,
		input.update_inputs,
		input.options,
		input.start_headers,
		input.update_headers,
	)
}

update_with_start_payloads :: proc(
	client: ^Client,
	workflow_type: string,
	workflow_inputs: []codec.Payload_View,
	update_name: string,
	update_inputs: []codec.Payload_View,
	options: Update_With_Start_Options,
) -> (Update_With_Start_Result, Error) {
	start_headers := Headers{}
	start_headers_error := client_start_interceptor_headers_init(
		&start_headers,
		options.start_options.metadata,
	)
	if start_headers_error.message != nil {
		return {}, start_headers_error
	}
	defer headers_free(&start_headers)
	update_headers := Headers{}
	update_headers_error := client_interceptor_headers_init(
		&update_headers,
		options.update_options.headers,
	)
	if update_headers_error.message != nil {
		return {}, update_headers_error
	}
	defer headers_free(&update_headers)
	input := Client_Update_With_Start_Input{
		client = client,
		workflow_type = workflow_type,
		workflow_inputs = workflow_inputs,
		update_name = update_name,
		update_inputs = update_inputs,
		options = options,
		start_headers = &start_headers,
		update_headers = &update_headers,
	}
	state := Client_Interceptor_State{client = client}
	return client_update_with_start_chain(&state, &input)
}

update_with_start_payload :: proc(
	client: ^Client,
	workflow_type: string,
	workflow_input: codec.Payload_View,
	update_name: string,
	update_input: codec.Payload_View,
	options: Update_With_Start_Options,
) -> (Update_With_Start_Result, Error) {
	workflow_inputs := [?]codec.Payload_View{workflow_input}
	update_inputs := [?]codec.Payload_View{update_input}
	return update_with_start_payloads(
		client,
		workflow_type,
		workflow_inputs[:],
		update_name,
		update_inputs[:],
		options,
	)
}

update_with_start_json :: proc(
	client: ^Client,
	workflow_type: string,
	workflow_input: $Workflow_Input,
	update_name: string,
	update_input: $Update_Input,
	options: Update_With_Start_Options,
) -> (Update_With_Start_Result, Error) {
	workflow_payload, workflow_encode_error := codec.encode_json(workflow_input)
	if workflow_encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode Workflow input")
	}
	defer codec.payload_free(&workflow_payload)
	update_payload, update_encode_error := codec.encode_json(update_input)
	if update_encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode Workflow Update input")
	}
	defer codec.payload_free(&update_payload)
	return update_with_start_payload(
		client,
		workflow_type,
		codec.payload_view(&workflow_payload),
		update_name,
		codec.payload_view(&update_payload),
		options,
	)
}

signal_with_start_options_error :: proc(options: Signal_With_Start_Options) -> string {
	start_options := options.start_options
	if message := start_workflow_options_error(start_options); len(message) > 0 {
		return message
	}
	if start_options.id_conflict_policy == .Fail {
		return "Signal-With-Start cannot use the Fail Workflow ID conflict policy"
	}
	if start_options.enable_eager_workflow_start {
		return "Signal-With-Start does not support eager Workflow start"
	}
	if len(start_options.nexus_completion_callbacks) > 0 {
		return "Signal-With-Start cannot attach Nexus completion callbacks"
	}
	if start_options.metadata != nil && len(start_options.metadata.headers) > 0 {
		return "Signal-With-Start cannot represent separate Workflow-start headers; use signal_headers"
	}
	if options.signal_headers != nil && options.signal_headers.allocator.procedure == nil {
		return "Signal-With-Start headers are not initialized"
	}
	return ""
}

// encode_signal_with_start_workflow_request is exposed for wire-contract
// testing. It deliberately projects the already-tested Start request into the
// combined RPC so new start fields cannot silently drift between the two paths.
// Returned bytes are owned by the caller.
encode_signal_with_start_workflow_request :: proc(
	namespace: string,
	workflow_type: string,
	workflow_input: codec.Payload_View,
	signal_name: string,
	signal_input: codec.Payload_View,
	options: Signal_With_Start_Options,
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(signal_name) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Workflow signal name must not be empty")
	}
	if message := signal_with_start_options_error(options); len(message) > 0 {
		return nil, error_from_string(.Invalid_Argument, message)
	}

	start_bytes, start_error := encode_start_workflow_request(
		namespace,
		workflow_type,
		workflow_input,
		options.start_options,
	)
	if start_error.message != nil {
		return nil, start_error
	}
	defer delete(start_bytes)

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return nil, error_from_string(.Payload_Conversion, "could not initialize Signal-With-Start request arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	start := proto.Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest(
		start_bytes[:],
		&start,
	)
	context.allocator = previous_allocator
	if !decoded {
		return nil, error_from_string(.Payload_Conversion, "could not project Workflow start request")
	}

	encoded_signal := proto.encode_payload(signal_input)
	defer delete(encoded_signal.metadata)
	defer delete(encoded_signal.external_payloads)
	signal_inputs := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(signal_inputs.payloads)
	append(&signal_inputs.payloads, encoded_signal)

	header := proto.Temporal_Api_Common_V1_Header{}
	header_payloads: []proto.Temporal_Api_Common_V1_Payload
	defer delete(header.fields)
	defer free_start_proto_payloads(header_payloads)
	if options.signal_headers != nil && len(options.signal_headers.entries) > 0 {
		header_payloads = make(
			[]proto.Temporal_Api_Common_V1_Payload,
			len(options.signal_headers.entries),
		)
		for &entry, index in options.signal_headers.entries {
			header_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
		}
		for entry, index in options.signal_headers.entries {
			append(
				&header.fields,
				proto.Temporal_Api_Common_V1_Header_FieldsEntry{
					key = entry.key,
					value = &header_payloads[index],
				},
			)
		}
	}

	request := proto.Temporal_Api_Workflowservice_V1_SignalWithStartWorkflowExecutionRequest{
		namespace = start.namespace,
		workflow_id = start.workflow_id,
		workflow_type = start.workflow_type,
		task_queue = start.task_queue,
		input = start.input,
		workflow_execution_timeout = start.workflow_execution_timeout,
		workflow_run_timeout = start.workflow_run_timeout,
		workflow_task_timeout = start.workflow_task_timeout,
		identity = start.identity,
		request_id = start.request_id,
		workflow_id_reuse_policy = start.workflow_id_reuse_policy,
		workflow_id_conflict_policy = start.workflow_id_conflict_policy,
		signal_name = signal_name,
		signal_input = &signal_inputs,
		retry_policy = start.retry_policy,
		cron_schedule = start.cron_schedule,
		memo = start.memo,
		search_attributes = start.search_attributes,
		workflow_start_delay = start.workflow_start_delay,
		user_metadata = start.user_metadata,
		links = start.links,
		versioning_override = start.versioning_override,
		priority = start.priority,
		time_skipping_config = start.time_skipping_config,
	}
	if len(header.fields) > 0 {
		request.header = &header
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_SignalWithStartWorkflowExecutionRequest(&request), {}
}

signal_with_start_workflow_payload_unintercepted :: proc(
	client: ^Client,
	workflow_type: string,
	workflow_input: codec.Payload_View,
	signal_name: string,
	signal_input: codec.Payload_View,
	options: Signal_With_Start_Options,
) -> (handle: Workflow_Handle, error: Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	request_bytes, request_error := encode_signal_with_start_workflow_request(
		client.namespace,
		workflow_type,
		workflow_input,
		signal_name,
		signal_input,
		options,
	)
	if request_error.message != nil {
		return {}, request_error
	}
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"SignalWithStartWorkflowExecution",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize client response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_SignalWithStartWorkflowExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_SignalWithStartWorkflowExecutionResponse(
		rpc_result.protobuf,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded || len(response.run_id) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Signal-With-Start response")
	}

	first_execution_run_id := response.first_execution_run_id
	if len(first_execution_run_id) == 0 {
		first_execution_run_id = response.run_id
	}
	handle_error: Error
	handle, handle_error = get_workflow_handle(
		client,
		options.start_options.id,
		response.run_id,
		first_execution_run_id,
	)
	if handle_error.message != nil {
		return {}, handle_error
	}
	handle.started_by_request = response.started
	return handle, {}
}

signal_with_start_workflow_payload_terminal :: proc(
	input: ^Client_Signal_With_Start_Input,
) -> (Workflow_Handle, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "Signal-With-Start input is missing")
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Signal-With-Start interceptor headers are invalid")
	}
	options := input.options
	options.signal_headers = input.headers
	return signal_with_start_workflow_payload_unintercepted(
		input.client,
		input.workflow_type,
		input.workflow_input,
		input.signal_name,
		input.signal_input,
		options,
	)
}

signal_with_start_workflow_payload :: proc(
	client: ^Client,
	workflow_type: string,
	workflow_input: codec.Payload_View,
	signal_name: string,
	signal_input: codec.Payload_View,
	options: Signal_With_Start_Options,
) -> (handle: Workflow_Handle, error: Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	headers := Headers{}
	headers_error := client_interceptor_headers_init(&headers, options.signal_headers)
	if headers_error.message != nil {
		return {}, headers_error
	}
	defer headers_free(&headers)
	interceptor_input := Client_Signal_With_Start_Input{
		client = client,
		workflow_type = workflow_type,
		workflow_input = workflow_input,
		signal_name = signal_name,
		signal_input = signal_input,
		options = options,
		headers = &headers,
	}
	state := Client_Interceptor_State{client = client}
	return client_signal_with_start_chain(&state, &interceptor_input)
}

signal_with_start_workflow_json :: proc(
	client: ^Client,
	workflow_type: string,
	workflow_input: $Workflow_Input,
	signal_name: string,
	signal_input: $Signal_Input,
	options: Signal_With_Start_Options,
) -> (handle: Workflow_Handle, error: Error) {
	encoded_workflow_input, workflow_encode_error := codec.encode_json(workflow_input)
	if workflow_encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode Workflow input")
	}
	defer codec.payload_free(&encoded_workflow_input)
	encoded_signal_input, signal_encode_error := codec.encode_json(signal_input)
	if signal_encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode Workflow signal input")
	}
	defer codec.payload_free(&encoded_signal_input)
	return signal_with_start_workflow_payload(
		client,
		workflow_type,
		codec.payload_view(&encoded_workflow_input),
		signal_name,
		codec.payload_view(&encoded_signal_input),
		options,
	)
}

workflow_handle_valid :: proc(handle: ^Workflow_Handle) -> bool {
	return handle != nil && handle.client != nil && handle.client.connection != nil &&
		handle.client.connection.handle != nil && len(handle.id) > 0
}

update_handle_valid :: proc(handle: ^Update_Handle) -> bool {
	return handle != nil && handle.client != nil && handle.client.connection != nil &&
		handle.client.connection.handle != nil && len(handle.workflow_id) > 0 &&
		len(handle.update_id) > 0
}

workflow_execution :: proc(handle: ^Workflow_Handle) -> proto.Temporal_Api_Common_V1_WorkflowExecution {
	return {
		workflow_id = handle.id,
		run_id = handle.run_id,
	}
}

resolve_request_id :: proc(explicit: string, buffer: []u8) -> string {
	if len(explicit) > 0 {
		return explicit
	}
	return uuid.to_string_buffer(uuid.generate_v4(), buffer)
}

rpc_empty_response :: proc(
	handle: ^Workflow_Handle,
	rpc: string,
	request: []u8,
) -> Error {
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		rpc,
		request,
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return error_from_core(&rpc_result.error)
	}
	return {}
}

signal_workflow_payload_terminal :: proc(input: ^Client_Signal_Workflow_Input) -> Error {
	if input == nil || !workflow_handle_valid(input.handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Signal interceptor headers are invalid")
	}
	if len(input.signal_name) == 0 {
		return error_from_string(.Invalid_Argument, "Workflow signal name must not be empty")
	}
	options := input.options
	options.headers = input.headers
	request_bytes, request_error := encode_signal_workflow_request_payloads(
		input.handle.client.namespace,
		input.handle.id,
		input.handle.run_id,
		input.signal_name,
		input.inputs,
		options,
	)
	if request_error.message != nil {
		return request_error
	}
	defer delete(request_bytes)
	return rpc_empty_response(input.handle, "SignalWorkflowExecution", request_bytes[:])
}

signal_workflow_intercept :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	inputs: []codec.Payload_View,
	options: Signal_Workflow_Options,
) -> Error {
	headers := Headers{}
	headers_error := client_interceptor_headers_init(&headers, options.headers)
	if headers_error.message != nil {
		return headers_error
	}
	defer headers_free(&headers)
	interceptor_input := Client_Signal_Workflow_Input{
		handle = handle,
		signal_name = signal_name,
		inputs = inputs,
		options = options,
		headers = &headers,
	}
	client: ^Client
	if handle != nil {
		client = handle.client
	}
	state := Client_Interceptor_State{client = client}
	return client_signal_workflow_chain(&state, &interceptor_input)
}

signal_workflow :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	explicit_request_id := "",
) -> Error {
	return signal_workflow_with_options(
		handle,
		signal_name,
		{request_id = explicit_request_id},
	)
}

signal_workflow_with_options :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	options: Signal_Workflow_Options,
) -> Error {
	return signal_workflow_intercept(handle, signal_name, nil, options)
}

// encode_signal_workflow_request_payloads is the codec-neutral repeated-input
// Signal boundary. Returned bytes are owned by the caller.
encode_signal_workflow_request_payloads :: proc(
	namespace, workflow_id, run_id, signal_name: string,
	inputs: []codec.Payload_View,
	options: Signal_Workflow_Options = {},
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 || len(workflow_id) == 0 || len(signal_name) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Signal namespace, Workflow ID, and name are required")
	}
	if options.headers != nil && options.headers.allocator.procedure == nil {
		return nil, error_from_string(.Invalid_Argument, "Workflow signal headers are not initialized")
	}
	execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
		workflow_id = workflow_id,
		run_id = run_id,
	}
	encoded_inputs := make([]proto.Temporal_Api_Common_V1_Payload, len(inputs))
	defer free_start_proto_payloads(encoded_inputs)
	input_payloads := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(input_payloads.payloads)
	for input, index in inputs {
		encoded_inputs[index] = proto.encode_payload(input)
		append(&input_payloads.payloads, encoded_inputs[index])
	}
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest{
		namespace = namespace,
		workflow_execution = &execution,
		signal_name = signal_name,
		identity = "temporal-sdk-odin@local",
		request_id = resolve_request_id(options.request_id, request_id_buffer[:]),
	}
	if len(inputs) > 0 {
		request.input = &input_payloads
	}
	header := proto.Temporal_Api_Common_V1_Header{}
	header_payloads: []proto.Temporal_Api_Common_V1_Payload
	defer delete(header.fields)
	defer free_start_proto_payloads(header_payloads)
	if options.headers != nil && len(options.headers.entries) > 0 {
		header_payloads = make([]proto.Temporal_Api_Common_V1_Payload, len(options.headers.entries))
		for &entry, index in options.headers.entries {
			header_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
		}
		for entry, index in options.headers.entries {
			append(
				&header.fields,
				proto.Temporal_Api_Common_V1_Header_FieldsEntry{
					key = entry.key,
					value = &header_payloads[index],
				},
			)
		}
		request.header = &header
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest(&request), {}
}

encode_signal_workflow_request :: proc(
	namespace, workflow_id, run_id, signal_name: string,
	input: codec.Payload_View,
	options: Signal_Workflow_Options = {},
) -> (request_bytes: [dynamic]u8, error: Error) {
	inputs := [?]codec.Payload_View{input}
	return encode_signal_workflow_request_payloads(
		namespace,
		workflow_id,
		run_id,
		signal_name,
		inputs[:],
		options,
	)
}

signal_workflow_payloads :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	inputs: []codec.Payload_View,
	explicit_request_id := "",
) -> Error {
	return signal_workflow_payloads_with_options(
		handle,
		signal_name,
		inputs,
		{request_id = explicit_request_id},
	)
}

signal_workflow_payloads_with_options :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	inputs: []codec.Payload_View,
	options: Signal_Workflow_Options,
) -> Error {
	return signal_workflow_intercept(handle, signal_name, inputs, options)
}

signal_workflow_payload :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	input: codec.Payload_View,
	explicit_request_id := "",
) -> Error {
	return signal_workflow_payload_with_options(
		handle,
		signal_name,
		input,
		{request_id = explicit_request_id},
	)
}

signal_workflow_payload_with_options :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	input: codec.Payload_View,
	options: Signal_Workflow_Options,
) -> Error {
	inputs := [?]codec.Payload_View{input}
	return signal_workflow_payloads_with_options(handle, signal_name, inputs[:], options)
}

signal_workflow_json :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	input: $T,
	explicit_request_id := "",
) -> Error {
	return signal_workflow_json_with_options(
		handle,
		signal_name,
		input,
		{request_id = explicit_request_id},
	)
}

signal_workflow_json_with_options :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	input: $T,
	options: Signal_Workflow_Options,
) -> Error {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow signal input")
	}
	defer codec.payload_free(&payload)
	return signal_workflow_payload_with_options(
		handle,
		signal_name,
		codec.payload_view(&payload),
		options,
	)
}

// encode_query_workflow_request_payloads is the codec-neutral repeated-input
// Query boundary. Returned bytes are owned by the caller.
encode_query_workflow_request_payloads :: proc(
	namespace, workflow_id, run_id, query_type: string,
	inputs: []codec.Payload_View,
	options: Query_Workflow_Options = {},
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Temporal namespace must not be empty")
	}
	if len(workflow_id) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Workflow ID must not be empty")
	}
	if len(query_type) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Workflow query type must not be empty")
	}
	if options.headers != nil && options.headers.allocator.procedure == nil {
		return nil, error_from_string(.Invalid_Argument, "Workflow query headers are not initialized")
	}
	execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
		workflow_id = workflow_id,
		run_id = run_id,
	}
	encoded_inputs := make([]proto.Temporal_Api_Common_V1_Payload, len(inputs))
	defer free_start_proto_payloads(encoded_inputs)
	arguments := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(arguments.payloads)
	for input, index in inputs {
		encoded_inputs[index] = proto.encode_payload(input)
		append(&arguments.payloads, encoded_inputs[index])
	}
	query := proto.Temporal_Api_Query_V1_WorkflowQuery{
		query_type = query_type,
	}
	if len(inputs) > 0 {
		query.query_args = &arguments
	}
	header := proto.Temporal_Api_Common_V1_Header{}
	header_payloads: []proto.Temporal_Api_Common_V1_Payload
	defer delete(header.fields)
	defer free_start_proto_payloads(header_payloads)
	if options.headers != nil && len(options.headers.entries) > 0 {
		header_payloads = make(
			[]proto.Temporal_Api_Common_V1_Payload,
			len(options.headers.entries),
		)
		for &entry, index in options.headers.entries {
			header_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
		}
		for entry, index in options.headers.entries {
			append(
				&header.fields,
				proto.Temporal_Api_Common_V1_Header_FieldsEntry{
					key = entry.key,
					value = &header_payloads[index],
				},
			)
		}
		query.header = &header
	}
	request := proto.Temporal_Api_Workflowservice_V1_QueryWorkflowRequest{
		namespace = namespace,
		execution = &execution,
		query = &query,
		query_reject_condition = query_reject_condition_to_proto(options.reject_condition),
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_QueryWorkflowRequest(&request), {}
}

encode_query_workflow_request :: proc(
	namespace, workflow_id, run_id, query_type: string,
	input: codec.Payload_View,
	options: Query_Workflow_Options = {},
) -> (request_bytes: [dynamic]u8, error: Error) {
	inputs := [?]codec.Payload_View{input}
	return encode_query_workflow_request_payloads(
		namespace,
		workflow_id,
		run_id,
		query_type,
		inputs[:],
		options,
	)
}

query_workflow_payloads_unintercepted :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	inputs: []codec.Payload_View,
	options: Query_Workflow_Options,
) -> (payload: codec.Payload, error: Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	request_bytes, request_error := encode_query_workflow_request_payloads(
		handle.client.namespace,
		handle.id,
		handle.run_id,
		query_type,
		inputs,
		options,
	)
	if request_error.message != nil {
		return {}, request_error
	}
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"QueryWorkflow",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_query_workflow_response(rpc_result.protobuf)
}

query_workflow_payload_terminal :: proc(
	input: ^Client_Query_Workflow_Input,
) -> (codec.Payload, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "Query input is missing")
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Query interceptor headers are invalid")
	}
	options := input.options
	options.headers = input.headers
	return query_workflow_payloads_unintercepted(
		input.handle,
		input.query_type,
		input.inputs,
		options,
	)
}

query_workflow_payloads_with_options :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	inputs: []codec.Payload_View,
	options: Query_Workflow_Options,
) -> (codec.Payload, Error) {
	headers := Headers{}
	headers_error := client_interceptor_headers_init(&headers, options.headers)
	if headers_error.message != nil {
		return {}, headers_error
	}
	defer headers_free(&headers)
	interceptor_input := Client_Query_Workflow_Input{
		handle = handle,
		query_type = query_type,
		inputs = inputs,
		options = options,
		headers = &headers,
	}
	client: ^Client
	if handle != nil {
		client = handle.client
	}
	state := Client_Interceptor_State{client = client}
	return client_query_workflow_chain(&state, &interceptor_input)
}

query_workflow_payload_with_options :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	input: codec.Payload_View,
	options: Query_Workflow_Options,
) -> (codec.Payload, Error) {
	inputs := [?]codec.Payload_View{input}
	return query_workflow_payloads_with_options(handle, query_type, inputs[:], options)
}

// decode_query_workflow_response preserves the result Payload or returns a
// typed rejection carrying the Workflow status that caused the rejection.
decode_query_workflow_response :: proc(data: []u8) -> (payload: codec.Payload, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize query response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_QueryWorkflowResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_QueryWorkflowResponse(
		data,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed query response")
	}
	if response.query_rejected != nil {
		rejection := error_from_string(
			.Workflow_Query_Rejected,
			"Temporal rejected the Workflow query for the current execution status",
		)
		rejection.workflow_status = Workflow_Status(response.query_rejected.status)
		rejection.has_workflow_status = true
		return {}, rejection
	}
	if response.query_result == nil || len(response.query_result.payloads) != 1 {
		return {}, error_from_string(.Malformed_Response, "Workflow query response must contain one Payload")
	}
	cloned_payload, clone_error := proto.clone_payload(&response.query_result.payloads[0])
	if clone_error != .None {
		return {}, error_from_string(.Malformed_Response, "could not preserve Workflow query result")
	}
	return cloned_payload, {}
}

query_workflow_payload :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	input: codec.Payload_View,
) -> (payload: codec.Payload, error: Error) {
	return query_workflow_payload_with_options(handle, query_type, input, {})
}

query_workflow_payloads :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	inputs: []codec.Payload_View,
) -> (payload: codec.Payload, error: Error) {
	return query_workflow_payloads_with_options(handle, query_type, inputs, {})
}

query_workflow_json :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	input: $Input,
	value: ^$Output,
) -> Error {
	return query_workflow_json_with_options(handle, query_type, input, value, {})
}

query_workflow_json_with_options :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	input: $Input,
	value: ^$Output,
	options: Query_Workflow_Options,
) -> Error {
	if value == nil {
		return error_from_string(.Invalid_Argument, "Workflow query output pointer is required")
	}
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow query input")
	}
	defer codec.payload_free(&payload)
	result, query_error := query_workflow_payload_with_options(
		handle,
		query_type,
		codec.payload_view(&payload),
		options,
	)
	if query_error.message != nil {
		return query_error
	}
	defer codec.payload_free(&result)
	if codec.decode_json(codec.payload_view(&result), value) != .None {
		return error_from_string(.Payload_Conversion, "could not decode Workflow query result")
	}
	return {}
}

update_outcome_payload :: proc(
	outcome: ^proto.Temporal_Api_Update_V1_Outcome,
) -> (codec.Payload, Error) {
	if outcome == nil {
		return {}, error_from_string(.Malformed_Response, "Temporal Update response has no outcome")
	}
	#partial switch value in outcome.value {
	case proto.Temporal_Api_Update_V1_Outcome_Value_Success:
		if value.value == nil || len(value.value.payloads) != 1 {
			return {}, error_from_string(.Malformed_Response, "Workflow Update result must contain one Payload")
		}
		payload, clone_error := proto.clone_payload(&value.value.payloads[0])
		if clone_error != .None {
			return {}, error_from_string(.Malformed_Response, "could not preserve Workflow Update result")
		}
		return payload, {}
	case proto.Temporal_Api_Update_V1_Outcome_Value_Failure:
		if value.value == nil {
			return {}, error_from_string(.Malformed_Response, "Workflow Update failure is missing")
		}
		failure_message := value.value.message
		cause := value.value.cause
		for len(failure_message) == 0 && cause != nil {
			failure_message = cause.message
			cause = cause.cause
		}
		if len(failure_message) == 0 {
			failure_message = "Workflow Update failed"
		}
		details := proto.encode_Temporal_Api_Failure_V1_Failure(value.value)
		return {}, Error{
			kind = .Workflow_Update_Failed,
			message = bytes.clone(transmute([]u8)failure_message),
			details = details[:],
		}
	}
	return {}, error_from_string(.Malformed_Response, "Workflow Update outcome has no value")
}

// A request deadline can close tonic's HTTP/2 stream before tonic preserves a
// DeadlineExceeded status. Treat that transport shape as the configured local
// timeout only after the monotonic deadline has actually elapsed.
update_poll_failure_is_timeout :: proc(
	status_code: u32,
	message: string,
	deadline_reached: bool,
) -> bool {
	if status_code == 4 {
		return true
	}
	if status_code != 1 {
		return false
	}
	return message == "Timeout expired" ||
	       (deadline_reached && strings.contains(message, "h2 protocol error"))
}

poll_update_result_payload :: proc(
	client: ^Client,
	workflow_id, run_id, update_id: string,
	options: Update_Wait_Options = {},
) -> (codec.Payload, Error) {
	if options.timeout < 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update wait timeout must not be negative")
	}
	if options.cancellation_token != nil && options.cancellation_token.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update wait cancellation token is not initialized")
	}
	execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
		workflow_id = workflow_id,
		run_id = run_id,
	}
	update_ref := proto.Temporal_Api_Update_V1_UpdateRef{
		workflow_execution = &execution,
		update_id = update_id,
	}
	wait_policy := proto.Temporal_Api_Update_V1_WaitPolicy{
		lifecycle_stage = .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
	}
	started := time.tick_now()
	for {
		timeout_millis: u32
		if options.timeout > 0 {
			remaining := options.timeout - time.tick_since(started)
			if remaining <= 0 {
				return {}, error_from_string(.Wait_Timed_Out, "Workflow Update result wait timed out")
			}
			millis := i64(remaining / time.Millisecond)
			if remaining % time.Millisecond != 0 do millis += 1
			if millis > i64(4_294_967_295) do millis = i64(4_294_967_295)
			timeout_millis = u32(millis)
		}
		request := proto.Temporal_Api_Workflowservice_V1_PollWorkflowExecutionUpdateRequest{
			namespace = client.namespace,
			update_ref = &update_ref,
			identity = "temporal-sdk-odin@local",
			wait_policy = &wait_policy,
		}
		request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_PollWorkflowExecutionUpdateRequest(&request)
		rpc_result := temporal_core.client_rpc_call(
			client.connection,
			.Workflow,
			"PollWorkflowExecutionUpdate",
			request_bytes[:],
			retry = options.timeout == 0,
			timeout_millis = timeout_millis,
			cancellation_token = options.cancellation_token,
		)
		delete(request_bytes)
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

		decode_arena: virtual.Arena
		if virtual.arena_init_growing(&decode_arena) != nil {
			delete(rpc_result.protobuf)
			return {}, error_from_string(.Malformed_Response, "could not initialize Update poll arena")
		}
		previous_allocator := context.allocator
		context.allocator = virtual.arena_allocator(&decode_arena)
		response := proto.Temporal_Api_Workflowservice_V1_PollWorkflowExecutionUpdateResponse{}
		decoded := proto.decode_Temporal_Api_Workflowservice_V1_PollWorkflowExecutionUpdateResponse(
			rpc_result.protobuf,
			&response,
		)
		context.allocator = previous_allocator
		if !decoded {
			delete(rpc_result.protobuf)
			virtual.arena_destroy(&decode_arena)
			return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Update poll response")
		}
		if response.outcome != nil {
			payload, outcome_error := update_outcome_payload(response.outcome)
			delete(rpc_result.protobuf)
			virtual.arena_destroy(&decode_arena)
			return payload, outcome_error
		}
		delete(rpc_result.protobuf)
		virtual.arena_destroy(&decode_arena)
	}
}

// encode_update_workflow_request_payloads is the codec-neutral repeated-input
// Update boundary. The returned request waits only for acceptance; result
// polling remains a separate Client operation.
encode_update_workflow_request_payloads :: proc(
	namespace, workflow_id, run_id, first_execution_run_id, update_name: string,
	inputs: []codec.Payload_View,
	options: Update_Workflow_Options = {},
	explicit_request_id := "",
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 || len(workflow_id) == 0 || len(update_name) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Update namespace, Workflow ID, and name are required")
	}
	if options.headers != nil && options.headers.allocator.procedure == nil {
		return nil, error_from_string(.Invalid_Argument, "Workflow Update headers are not initialized")
	}
	execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
		workflow_id = workflow_id,
		run_id = run_id,
	}
	encoded_inputs := make([]proto.Temporal_Api_Common_V1_Payload, len(inputs))
	defer free_start_proto_payloads(encoded_inputs)
	arguments := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(arguments.payloads)
	for input, index in inputs {
		encoded_inputs[index] = proto.encode_payload(input)
		append(&arguments.payloads, encoded_inputs[index])
	}
	update_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	meta := proto.Temporal_Api_Update_V1_Meta{
		update_id = resolve_request_id(options.update_id, update_id_buffer[:]),
		identity = "temporal-sdk-odin@local",
	}
	update_input := proto.Temporal_Api_Update_V1_Input{
		name = update_name,
		args = &arguments,
	}
	header := proto.Temporal_Api_Common_V1_Header{}
	header_payloads: []proto.Temporal_Api_Common_V1_Payload
	defer delete(header.fields)
	defer free_start_proto_payloads(header_payloads)
	if options.headers != nil && len(options.headers.entries) > 0 {
		header_payloads = make([]proto.Temporal_Api_Common_V1_Payload, len(options.headers.entries))
		for &entry, index in options.headers.entries {
			header_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
		}
		for entry, index in options.headers.entries {
			append(
				&header.fields,
				proto.Temporal_Api_Common_V1_Header_FieldsEntry{
					key = entry.key,
					value = &header_payloads[index],
				},
			)
		}
		update_input.header = &header
	}
	update_request := proto.Temporal_Api_Update_V1_Request{
		meta = &meta,
		input = &update_input,
		request_id = resolve_request_id(explicit_request_id, request_id_buffer[:]),
	}
	wait_policy := proto.Temporal_Api_Update_V1_WaitPolicy{
		lifecycle_stage = .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
	}
	request := proto.Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionRequest{
		namespace = namespace,
		workflow_execution = &execution,
		first_execution_run_id = first_execution_run_id,
		wait_policy = &wait_policy,
		request = &update_request,
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionRequest(&request), {}
}

encode_update_workflow_request :: proc(
	namespace, workflow_id, run_id, first_execution_run_id, update_name: string,
	input: codec.Payload_View,
	options: Update_Workflow_Options = {},
	explicit_request_id := "",
) -> (request_bytes: [dynamic]u8, error: Error) {
	inputs := [?]codec.Payload_View{input}
	return encode_update_workflow_request_payloads(
		namespace,
		workflow_id,
		run_id,
		first_execution_run_id,
		update_name,
		inputs[:],
		options,
		explicit_request_id,
	)
}

start_update_payload :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	input: codec.Payload_View,
	explicit_update_id := "",
) -> (Update_Handle, Error) {
	return start_update_payload_with_options(
		handle,
		update_name,
		input,
		{update_id = explicit_update_id},
	)
}

start_update_payloads_unintercepted :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	inputs: []codec.Payload_View,
	options: Update_Workflow_Options,
) -> (Update_Handle, Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if len(update_name) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update name must not be empty")
	}
	if options.headers != nil && options.headers.allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update headers are not initialized")
	}
	request_bytes, request_error := encode_update_workflow_request_payloads(
		handle.client.namespace,
		handle.id,
		handle.run_id,
		handle.first_execution_run_id,
		update_name,
		inputs,
		options,
	)
	if request_error.message != nil {
		return {}, request_error
	}
	defer delete(request_bytes)
	for {
		rpc_result := temporal_core.client_rpc_call(
			handle.client.connection,
			.Workflow,
			"UpdateWorkflowExecution",
			request_bytes[:],
			timeout_millis = 0,
		)
		if rpc_result.error.message != nil {
			delete(rpc_result.protobuf)
			return {}, error_from_core(&rpc_result.error)
		}

		decode_arena: virtual.Arena
		if virtual.arena_init_growing(&decode_arena) != nil {
			delete(rpc_result.protobuf)
			return {}, error_from_string(.Malformed_Response, "could not initialize Update response arena")
		}
		previous_allocator := context.allocator
		context.allocator = virtual.arena_allocator(&decode_arena)
		response := proto.Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionResponse{}
		decoded := proto.decode_Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionResponse(
			rpc_result.protobuf,
			&response,
		)
		context.allocator = previous_allocator
		if !decoded || response.update_ref == nil ||
		   response.update_ref.workflow_execution == nil ||
		   len(response.update_ref.workflow_execution.workflow_id) == 0 ||
		   len(response.update_ref.update_id) == 0 {
			delete(rpc_result.protobuf)
			virtual.arena_destroy(&decode_arena)
			return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Update response")
		}
		accepted := response.outcome != nil ||
			response.stage == .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED ||
			response.stage == .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
		if !accepted {
			delete(rpc_result.protobuf)
			virtual.arena_destroy(&decode_arena)
			continue
		}

		heap := allocator_runtime.heap_allocator()
		workflow_id, workflow_id_error := strings.clone(
			response.update_ref.workflow_execution.workflow_id,
			heap,
		)
		if workflow_id_error != nil {
			delete(rpc_result.protobuf)
			virtual.arena_destroy(&decode_arena)
			return {}, error_from_string(.Malformed_Response, "could not preserve Update Workflow ID")
		}
		run_id, run_id_error := strings.clone(
			response.update_ref.workflow_execution.run_id,
			heap,
		)
		if run_id_error != nil {
			delete(workflow_id, heap)
			delete(rpc_result.protobuf)
			virtual.arena_destroy(&decode_arena)
			return {}, error_from_string(.Malformed_Response, "could not preserve Update run ID")
		}
		owned_update_id, update_id_error := strings.clone(response.update_ref.update_id, heap)
		if update_id_error != nil {
			delete(run_id, heap)
			delete(workflow_id, heap)
			delete(rpc_result.protobuf)
			virtual.arena_destroy(&decode_arena)
			return {}, error_from_string(.Malformed_Response, "could not preserve Update ID")
		}
		outcome_present := response.outcome != nil
		outcome: codec.Payload
		outcome_error: Error
		if outcome_present {
			context.allocator = heap
			outcome, outcome_error = update_outcome_payload(response.outcome)
			context.allocator = previous_allocator
			if outcome_error.message != nil && outcome_error.kind != .Workflow_Update_Failed {
				codec.payload_free(&outcome)
				delete(owned_update_id, heap)
				delete(run_id, heap)
				delete(workflow_id, heap)
				delete(rpc_result.protobuf)
				virtual.arena_destroy(&decode_arena)
				return {}, outcome_error
			}
		}
		result := Update_Handle{
			client = handle.client,
			workflow_id = workflow_id,
			run_id = run_id,
			update_id = owned_update_id,
			outcome_present = outcome_present,
			outcome = outcome,
			outcome_error = outcome_error,
		}
		delete(rpc_result.protobuf)
		virtual.arena_destroy(&decode_arena)
		return result, {}
	}
}

start_update_payload_terminal :: proc(
	input: ^Client_Start_Update_Input,
) -> (Update_Handle, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "Update input is missing")
	}
	if input.headers == nil || input.headers.allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Update interceptor headers are invalid")
	}
	options := input.options
	options.headers = input.headers
	return start_update_payloads_unintercepted(
		input.handle,
		input.update_name,
		input.inputs,
		options,
	)
}

start_update_payloads_with_options :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	inputs: []codec.Payload_View,
	options: Update_Workflow_Options,
) -> (Update_Handle, Error) {
	headers := Headers{}
	headers_error := client_interceptor_headers_init(&headers, options.headers)
	if headers_error.message != nil {
		return {}, headers_error
	}
	defer headers_free(&headers)
	interceptor_input := Client_Start_Update_Input{
		handle = handle,
		update_name = update_name,
		inputs = inputs,
		options = options,
		headers = &headers,
	}
	client: ^Client
	if handle != nil {
		client = handle.client
	}
	state := Client_Interceptor_State{client = client}
	return client_start_update_chain(&state, &interceptor_input)
}

start_update_payload_with_options :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	input: codec.Payload_View,
	options: Update_Workflow_Options,
) -> (Update_Handle, Error) {
	inputs := [?]codec.Payload_View{input}
	return start_update_payloads_with_options(handle, update_name, inputs[:], options)
}

start_update_payloads :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	inputs: []codec.Payload_View,
	explicit_update_id := "",
) -> (Update_Handle, Error) {
	return start_update_payloads_with_options(
		handle,
		update_name,
		inputs,
		{update_id = explicit_update_id},
	)
}

start_update_json :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	input: $Input,
	explicit_update_id := "",
) -> (Update_Handle, Error) {
	return start_update_json_with_options(
		handle,
		update_name,
		input,
		{update_id = explicit_update_id},
	)
}

start_update_json_with_options :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	input: $Input,
	options: Update_Workflow_Options,
) -> (Update_Handle, Error) {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode Workflow Update input")
	}
	defer codec.payload_free(&payload)
	return start_update_payload_with_options(
		handle,
		update_name,
		codec.payload_view(&payload),
		options,
	)
}

wait_update_payload_terminal :: proc(
	input: ^Client_Wait_Update_Input,
) -> (codec.Payload, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update wait input is missing")
	}
	handle := input.handle
	options := input.options
	if !update_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update handle is not initialized")
	}
	if options.timeout < 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update wait timeout must not be negative")
	}
	if options.cancellation_token != nil && options.cancellation_token.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update wait cancellation token is not initialized")
	}
	if handle.outcome_present {
		if handle.outcome_error.message != nil {
			return {}, error_clone(&handle.outcome_error)
		}
		result, clone_error := codec.payload_clone(codec.payload_view(&handle.outcome))
		if clone_error != .None {
			return {}, error_from_string(.Payload_Conversion, "could not clone Workflow Update result")
		}
		return result, {}
	}
	return poll_update_result_payload(
		handle.client,
		handle.workflow_id,
		handle.run_id,
		handle.update_id,
		options,
	)
}

wait_update_payload_with_options :: proc(
	handle: ^Update_Handle,
	options: Update_Wait_Options,
) -> (codec.Payload, Error) {
	input := Client_Wait_Update_Input{handle = handle, options = options}
	client: ^Client
	if handle != nil do client = handle.client
	state := Client_Interceptor_State{client = client}
	return client_wait_update_chain(&state, &input)
}

wait_update_payload :: proc(handle: ^Update_Handle) -> (codec.Payload, Error) {
	return wait_update_payload_with_options(handle, {})
}

wait_update_json_with_options :: proc(
	handle: ^Update_Handle,
	value: ^$Output,
	options: Update_Wait_Options,
) -> Error {
	if value == nil {
		return error_from_string(.Invalid_Argument, "Workflow Update output pointer is required")
	}
	result, update_error := wait_update_payload_with_options(handle, options)
	if update_error.message != nil {
		return update_error
	}
	defer codec.payload_free(&result)
	if codec.decode_json(codec.payload_view(&result), value) != .None {
		return error_from_string(.Payload_Conversion, "could not decode Workflow Update result")
	}
	return {}
}

wait_update_json :: proc(handle: ^Update_Handle, value: ^$Output) -> Error {
	return wait_update_json_with_options(handle, value, {})
}

execute_update_payload :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	input: codec.Payload_View,
	explicit_update_id := "",
) -> (codec.Payload, Error) {
	update, start_error := start_update_payload(handle, update_name, input, explicit_update_id)
	if start_error.message != nil {
		return {}, start_error
	}
	defer update_handle_free(&update)
	return wait_update_payload(&update)
}

execute_update_payloads :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	inputs: []codec.Payload_View,
	explicit_update_id := "",
) -> (codec.Payload, Error) {
	update, start_error := start_update_payloads(
		handle,
		update_name,
		inputs,
		explicit_update_id,
	)
	if start_error.message != nil {
		return {}, start_error
	}
	defer update_handle_free(&update)
	return wait_update_payload(&update)
}

execute_update_json :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	input: $Input,
	value: ^$Output,
	explicit_update_id := "",
) -> Error {
	if value == nil {
		return error_from_string(.Invalid_Argument, "Workflow Update output pointer is required")
	}
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow Update input")
	}
	defer codec.payload_free(&payload)
	result, update_error := execute_update_payload(
		handle,
		update_name,
		codec.payload_view(&payload),
		explicit_update_id,
	)
	if update_error.message != nil {
		return update_error
	}
	defer codec.payload_free(&result)
	if codec.decode_json(codec.payload_view(&result), value) != .None {
		return error_from_string(.Payload_Conversion, "could not decode Workflow Update result")
	}
	return {}
}

cancel_workflow_terminal :: proc(input: ^Client_Cancel_Workflow_Input) -> Error {
	if input == nil || !workflow_handle_valid(input.handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	execution := workflow_execution(input.handle)
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_RequestCancelWorkflowExecutionRequest{
		namespace = input.handle.client.namespace,
		workflow_execution = &execution,
		identity = "temporal-sdk-odin@local",
		request_id = resolve_request_id(input.request_id, request_id_buffer[:]),
		first_execution_run_id = input.handle.first_execution_run_id,
		reason = input.reason,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_RequestCancelWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(input.handle, "RequestCancelWorkflowExecution", request_bytes[:])
}

cancel_workflow :: proc(
	handle: ^Workflow_Handle,
	reason := "",
	explicit_request_id := "",
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	input := Client_Cancel_Workflow_Input{
		handle = handle,
		reason = reason,
		request_id = explicit_request_id,
	}
	state := Client_Interceptor_State{client = handle.client}
	return client_cancel_workflow_chain(&state, &input)
}

reset_reapply_exclude_type_to_proto :: proc(
	value: Reset_Reapply_Exclude_Type,
) -> (proto.Temporal_Api_Enums_V1_ResetReapplyExcludeType, bool) {
	switch value {
	case .Signals:
		return .RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL, true
	case .Updates:
		return .RESET_REAPPLY_EXCLUDE_TYPE_UPDATE, true
	case .Nexus:
		return .RESET_REAPPLY_EXCLUDE_TYPE_NEXUS, true
	}
	return .RESET_REAPPLY_EXCLUDE_TYPE_UNSPECIFIED, false
}

post_reset_update_mask_kind :: proc(path: string) -> int {
	switch path {
	case "versioning_override":
		return 1
	case "priority":
		return 2
	case "time_skipping_config":
		return 3
	}
	return 0
}

post_reset_operation_error :: proc(operation: Post_Reset_Operation) -> string {
	switch operation.kind {
	case .Signal_Workflow:
		signal := operation.signal_workflow
		if len(signal.signal_name) == 0 {
			return "post-reset Workflow signal name must not be empty"
		}
		if signal.headers != nil && signal.headers.allocator.procedure == nil {
			return "post-reset Workflow signal headers are not initialized"
		}
		for link in signal.links {
			if message := link_error(link); len(message) > 0 do return message
		}
	case .Update_Workflow_Options:
		update := operation.update_workflow_options
		if len(update.update_mask) == 0 {
			return "post-reset Workflow options update mask must not be empty"
		}
		masked: [4]bool
		for path in update.update_mask {
			kind := post_reset_update_mask_kind(path)
			if kind == 0 {
				return "post-reset Workflow options update mask contains an unsupported path"
			}
			if masked[kind] {
				return "post-reset Workflow options update mask contains a duplicate path"
			}
			masked[kind] = true
		}
		if update.versioning_override != nil && !masked[1] {
			return "post-reset versioning override requires its update-mask path"
		}
		if update.priority != nil && !masked[2] {
			return "post-reset priority requires its update-mask path"
		}
		if update.time_skipping_config != nil && !masked[3] {
			return "post-reset time-skipping config requires its update-mask path"
		}
		if update.versioning_override != nil {
			override := update.versioning_override
			has_deployment := len(override.deployment_name) > 0
			has_build := len(override.build_id) > 0
			if has_deployment != has_build {
				return "Workflow versioning override requires both deployment name and build ID"
			}
			switch override.kind {
			case .Pinned:
			case .Auto_Upgrade:
				if has_deployment {
					return "Auto-Upgrade Workflow versioning override must not name a deployment version"
				}
			case .One_Time:
				if !has_deployment {
					return "one-time Workflow versioning override requires a deployment version"
				}
			case .Unspecified:
				return "Workflow versioning override kind is invalid"
			}
		}
		if update.priority != nil {
			priority := update.priority
			if priority.priority_key < 0 {
				return "post-reset Workflow priority key must not be negative"
			}
			if len(priority.fairness_key) > 64 {
				return "post-reset Workflow priority fairness key must not exceed 64 bytes"
			}
			if math.is_nan(priority.fairness_weight) || math.is_inf(priority.fairness_weight) ||
			   priority.fairness_weight < 0 {
				return "post-reset Workflow priority fairness weight must be finite and non-negative"
			}
		}
		if update.time_skipping_config != nil {
			config := update.time_skipping_config
			if config.max_session_skip_count < 0 {
				return "post-reset Workflow time-skipping count must not be negative"
			}
			if config.fast_forward != nil &&
			   (len(config.fast_forward.id) == 0 || config.fast_forward.duration <= 0) {
				return "post-reset Workflow fast-forward requires an ID and positive duration"
			}
		}
	case .Invalid:
		return "post-reset operation kind is invalid"
	}
	return ""
}

Post_Reset_Proto_Storage :: struct {
	operations:             [dynamic]proto.Temporal_Api_Workflow_V1_PostResetOperation,
	signals:                []proto.Temporal_Api_Workflow_V1_PostResetOperation_SignalWorkflow,
	signal_inputs:          []proto.Temporal_Api_Common_V1_Payloads,
	signal_payloads:        [][]proto.Temporal_Api_Common_V1_Payload,
	signal_headers:         []proto.Temporal_Api_Common_V1_Header,
	signal_header_payloads: [][]proto.Temporal_Api_Common_V1_Payload,
	updates:                []proto.Temporal_Api_Workflow_V1_PostResetOperation_UpdateWorkflowOptions,
	workflow_options:       []proto.Temporal_Api_Workflow_V1_WorkflowExecutionOptions,
	update_masks:           []proto.Google_Protobuf_FieldMask,
	versioning_overrides:   []proto.Temporal_Api_Workflow_V1_VersioningOverride,
	pinned_overrides:       []proto.Temporal_Api_Workflow_V1_VersioningOverride_PinnedOverride,
	one_time_overrides:     []proto.Temporal_Api_Workflow_V1_VersioningOverride_OneTimeOverride,
	deployment_versions:    []proto.Temporal_Api_Deployment_V1_WorkerDeploymentVersion,
	priorities:             []proto.Temporal_Api_Common_V1_Priority,
	time_skipping_configs:  []proto.Temporal_Api_Common_V1_TimeSkippingConfig,
	fast_forward_configs:   []proto.Temporal_Api_Common_V1_FastForwardConfig,
	fast_forward_durations: []proto.Google_Protobuf_Duration,
	links:                  Encoded_Link_Storage,
}

post_reset_proto_storage_make :: proc(operations: []Post_Reset_Operation) -> Post_Reset_Proto_Storage {
	count := len(operations)
	link_count := 0
	for operation in operations {
		if operation.kind == .Signal_Workflow do link_count += len(operation.signal_workflow.links)
	}
	return {
		operations = make([dynamic]proto.Temporal_Api_Workflow_V1_PostResetOperation, 0, count),
		signals = make([]proto.Temporal_Api_Workflow_V1_PostResetOperation_SignalWorkflow, count),
		signal_inputs = make([]proto.Temporal_Api_Common_V1_Payloads, count),
		signal_payloads = make([][]proto.Temporal_Api_Common_V1_Payload, count),
		signal_headers = make([]proto.Temporal_Api_Common_V1_Header, count),
		signal_header_payloads = make([][]proto.Temporal_Api_Common_V1_Payload, count),
		updates = make([]proto.Temporal_Api_Workflow_V1_PostResetOperation_UpdateWorkflowOptions, count),
		workflow_options = make([]proto.Temporal_Api_Workflow_V1_WorkflowExecutionOptions, count),
		update_masks = make([]proto.Google_Protobuf_FieldMask, count),
		versioning_overrides = make([]proto.Temporal_Api_Workflow_V1_VersioningOverride, count),
		pinned_overrides = make([]proto.Temporal_Api_Workflow_V1_VersioningOverride_PinnedOverride, count),
		one_time_overrides = make([]proto.Temporal_Api_Workflow_V1_VersioningOverride_OneTimeOverride, count),
		deployment_versions = make([]proto.Temporal_Api_Deployment_V1_WorkerDeploymentVersion, count),
		priorities = make([]proto.Temporal_Api_Common_V1_Priority, count),
		time_skipping_configs = make([]proto.Temporal_Api_Common_V1_TimeSkippingConfig, count),
		fast_forward_configs = make([]proto.Temporal_Api_Common_V1_FastForwardConfig, count),
		fast_forward_durations = make([]proto.Google_Protobuf_Duration, count),
		links = encoded_link_storage_make(link_count),
	}
}

post_reset_proto_storage_free :: proc(storage: ^Post_Reset_Proto_Storage) {
	if storage == nil do return
	for index in 0..<len(storage.signals) {
		free_start_proto_payloads(storage.signal_payloads[index])
		free_start_proto_payloads(storage.signal_header_payloads[index])
		delete(storage.signal_inputs[index].payloads)
		delete(storage.signal_headers[index].fields)
		delete(storage.signals[index].links)
		delete(storage.update_masks[index].paths)
	}
	delete(storage.operations)
	delete(storage.signals)
	delete(storage.signal_inputs)
	delete(storage.signal_payloads)
	delete(storage.signal_headers)
	delete(storage.signal_header_payloads)
	delete(storage.updates)
	delete(storage.workflow_options)
	delete(storage.update_masks)
	delete(storage.versioning_overrides)
	delete(storage.pinned_overrides)
	delete(storage.one_time_overrides)
	delete(storage.deployment_versions)
	delete(storage.priorities)
	delete(storage.time_skipping_configs)
	delete(storage.fast_forward_configs)
	delete(storage.fast_forward_durations)
	encoded_link_storage_free(&storage.links)
	storage^ = {}
}

post_reset_operations_encode :: proc(
	operations: []Post_Reset_Operation,
	storage: ^Post_Reset_Proto_Storage,
) -> Error {
	if storage == nil {
		return error_from_string(.Invalid_Argument, "post-reset protobuf storage is missing")
	}
	for operation, index in operations {
		if message := post_reset_operation_error(operation); len(message) > 0 {
			return error_from_string(.Invalid_Argument, message)
		}
		switch operation.kind {
		case .Signal_Workflow:
			source := operation.signal_workflow
			signal := &storage.signals[index]
			signal.signal_name = source.signal_name
			if len(source.inputs) > 0 {
				storage.signal_payloads[index] = make(
					[]proto.Temporal_Api_Common_V1_Payload,
					len(source.inputs),
				)
				for input, input_index in source.inputs {
					storage.signal_payloads[index][input_index] = proto.encode_payload(input)
					append(&storage.signal_inputs[index].payloads, storage.signal_payloads[index][input_index])
				}
				signal.input = &storage.signal_inputs[index]
			}
			if source.headers != nil && len(source.headers.entries) > 0 {
				storage.signal_header_payloads[index] = make(
					[]proto.Temporal_Api_Common_V1_Payload,
					len(source.headers.entries),
				)
				for &entry, header_index in source.headers.entries {
					storage.signal_header_payloads[index][header_index] = proto.encode_payload(
						codec.payload_view(&entry.payload),
					)
				}
				for entry, header_index in source.headers.entries {
					append(&storage.signal_headers[index].fields, proto.Temporal_Api_Common_V1_Header_FieldsEntry{
						key = entry.key,
						value = &storage.signal_header_payloads[index][header_index],
					})
				}
				signal.header = &storage.signal_headers[index]
			}
			for link in source.links do append(&signal.links, encode_link(link, &storage.links))
			append(&storage.operations, proto.Temporal_Api_Workflow_V1_PostResetOperation{
				variant = proto.Temporal_Api_Workflow_V1_PostResetOperation_Variant_Signal_Workflow{
					value = signal,
				},
			})
		case .Update_Workflow_Options:
			source := operation.update_workflow_options
			update := &storage.updates[index]
			options := &storage.workflow_options[index]
			for path in source.update_mask do append(&storage.update_masks[index].paths, path)
			update.workflow_execution_options = options
			update.update_mask = &storage.update_masks[index]
			if source.versioning_override != nil {
				override := source.versioning_override
				version := &storage.deployment_versions[index]
				version.deployment_name = override.deployment_name
				version.build_id = override.build_id
				switch override.kind {
				case .Pinned:
					pinned := &storage.pinned_overrides[index]
					pinned.behavior = .PINNED_OVERRIDE_BEHAVIOR_PINNED
					if len(override.deployment_name) > 0 do pinned.version = version
					storage.versioning_overrides[index].override = proto.Temporal_Api_Workflow_V1_VersioningOverride_Override_Pinned{
						value = pinned,
					}
				case .Auto_Upgrade:
					storage.versioning_overrides[index].override = proto.Temporal_Api_Workflow_V1_VersioningOverride_Override_Auto_Upgrade{
						value = true,
					}
				case .One_Time:
					one_time := &storage.one_time_overrides[index]
					one_time.target_deployment_version = version
					storage.versioning_overrides[index].override = proto.Temporal_Api_Workflow_V1_VersioningOverride_Override_One_Time{
						value = one_time,
					}
				case .Unspecified:
				}
				options.versioning_override = &storage.versioning_overrides[index]
			}
			if source.priority != nil {
				storage.priorities[index] = {
					priority_key = source.priority.priority_key,
					fairness_key = source.priority.fairness_key,
					fairness_weight = source.priority.fairness_weight,
				}
				options.priority = &storage.priorities[index]
			}
			if source.time_skipping_config != nil {
				config := source.time_skipping_config
				storage.time_skipping_configs[index] = {
					enabled = config.enabled,
					disable_propagation = config.disable_propagation,
					max_session_skip_count = config.max_session_skip_count,
				}
				if config.fast_forward != nil {
					storage.fast_forward_durations[index] = duration_proto(config.fast_forward.duration)
					storage.fast_forward_configs[index] = {
						id = config.fast_forward.id,
						duration = &storage.fast_forward_durations[index],
					}
					storage.time_skipping_configs[index].fast_forward_config = &storage.fast_forward_configs[index]
				}
				options.time_skipping_config = &storage.time_skipping_configs[index]
			}
			append(&storage.operations, proto.Temporal_Api_Workflow_V1_PostResetOperation{
				variant = proto.Temporal_Api_Workflow_V1_PostResetOperation_Variant_Update_Workflow_Options{
					value = update,
				},
			})
		case .Invalid:
		}
	}
	return {}
}

encode_reset_workflow_request :: proc(
	namespace, identity, workflow_id, run_id: string,
	options: Reset_Workflow_Options,
) -> ([dynamic]u8, Error) {
	if len(namespace) == 0 || len(workflow_id) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Workflow reset identity is incomplete")
	}
	if options.workflow_task_finish_event_id <= 0 {
		return nil, error_from_string(.Invalid_Argument, "Workflow reset event ID must be positive")
	}
	exclude_types := make(
		[dynamic]proto.Temporal_Api_Enums_V1_ResetReapplyExcludeType,
		0,
		len(options.reapply_exclude_types),
	)
	defer delete(exclude_types)
	for exclude_type in options.reapply_exclude_types {
		proto_type, valid := reset_reapply_exclude_type_to_proto(exclude_type)
		if !valid {
			return nil, error_from_string(.Invalid_Argument, "Workflow reset reapply exclusion is invalid")
		}
		append(&exclude_types, proto_type)
	}
	post_reset_storage := post_reset_proto_storage_make(options.post_reset_operations)
	defer post_reset_proto_storage_free(&post_reset_storage)
	post_reset_error := post_reset_operations_encode(
		options.post_reset_operations,
		&post_reset_storage,
	)
	if post_reset_error.message != nil {
		return nil, post_reset_error
	}
	execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
		workflow_id = workflow_id,
		run_id = run_id,
	}
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_ResetWorkflowExecutionRequest{
		namespace = namespace,
		workflow_execution = &execution,
		reason = options.reason,
		workflow_task_finish_event_id = options.workflow_task_finish_event_id,
		request_id = resolve_request_id(options.request_id, request_id_buffer[:]),
		reset_reapply_exclude_types = exclude_types,
		post_reset_operations = post_reset_storage.operations,
		identity = identity,
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_ResetWorkflowExecutionRequest(
		&request,
	), {}
}

reset_workflow_terminal :: proc(
	input: ^Client_Reset_Workflow_Input,
) -> (handle: Workflow_Handle, error: Error) {
	if input == nil || !workflow_handle_valid(input.handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	request_bytes, request_error := encode_reset_workflow_request(
		input.handle.client.namespace,
		input.handle.client.identity,
		input.handle.id,
		input.handle.run_id,
		input.options,
	)
	if request_error.message != nil {
		return {}, request_error
	}
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		input.handle.client.connection,
		.Workflow,
		"ResetWorkflowExecution",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize reset response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_ResetWorkflowExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_ResetWorkflowExecutionResponse(
		rpc_result.protobuf,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded || len(response.run_id) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed reset response")
	}
	return get_workflow_handle(
		input.handle.client,
		input.handle.id,
		response.run_id,
		input.handle.first_execution_run_id,
	)
}

reset_workflow :: proc(
	handle: ^Workflow_Handle,
	options: Reset_Workflow_Options,
) -> (Workflow_Handle, Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	input := Client_Reset_Workflow_Input{handle = handle, options = options}
	state := Client_Interceptor_State{client = handle.client}
	return client_reset_workflow_chain(&state, &input)
}

terminate_workflow_terminal :: proc(input: ^Client_Terminate_Workflow_Input) -> Error {
	if input == nil || !workflow_handle_valid(input.handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	execution := workflow_execution(input.handle)
	detail_payloads := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(detail_payloads.payloads)
	payload := proto.Temporal_Api_Common_V1_Payload{}
	if input.has_details {
		payload = proto.encode_payload(input.details)
		defer delete(payload.metadata)
		defer delete(payload.external_payloads)
		append(&detail_payloads.payloads, payload)
	}
	request := proto.Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest{
		namespace = input.handle.client.namespace,
		workflow_execution = &execution,
		reason = input.reason,
		identity = "temporal-sdk-odin@local",
		first_execution_run_id = input.handle.first_execution_run_id,
	}
	if input.has_details {
		request.details = &detail_payloads
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(input.handle, "TerminateWorkflowExecution", request_bytes[:])
}

terminate_workflow_intercept :: proc(
	handle: ^Workflow_Handle,
	reason: string,
	details: codec.Payload_View,
	has_details: bool,
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	input := Client_Terminate_Workflow_Input{
		handle = handle,
		reason = reason,
		details = details,
		has_details = has_details,
	}
	state := Client_Interceptor_State{client = handle.client}
	return client_terminate_workflow_chain(&state, &input)
}

terminate_workflow :: proc(
	handle: ^Workflow_Handle,
	reason := "",
) -> Error {
	return terminate_workflow_intercept(handle, reason, {}, false)
}

terminate_workflow_payload :: proc(
	handle: ^Workflow_Handle,
	reason: string,
	details: codec.Payload_View,
) -> Error {
	return terminate_workflow_intercept(handle, reason, details, true)
}

terminate_workflow_json :: proc(
	handle: ^Workflow_Handle,
	reason: string,
	details: $T,
) -> Error {
	payload, encode_error := codec.encode_json(details)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow termination details")
	}
	defer codec.payload_free(&payload)
	return terminate_workflow_payload(handle, reason, codec.payload_view(&payload))
}

Result_Page_Kind :: enum {
	Completed,
	Follow_Run,
	Failed,
	Canceled,
	Terminated,
	Timed_Out,
	Continued_As_New,
}

Result_Page :: struct {
	kind:          Result_Page_Kind,
	payload:       codec.Payload,
	has_payload:   bool,
	next_run_id:   string,
	message:       string,
	failure_proto: []u8,
}

result_page_free :: proc(page: ^Result_Page) {
	if page == nil {
		return
	}
	codec.payload_free(&page.payload)
	delete(page.next_run_id)
	delete(page.message)
	delete(page.failure_proto)
	page^ = {}
}

clone_result_string :: proc(value: string) -> (string, bool) {
	if len(value) == 0 {
		return "", true
	}
	owned, allocation_error := strings.clone(value)
	return owned, allocation_error == nil
}

decode_result_page :: proc(data: []u8) -> (page: Result_Page, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize history response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse(
		data,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded || response.history == nil || len(response.history.events) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned an empty close-event history")
	}

	for &event in response.history.events {
		#partial switch attributes in event.attributes {
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Completed_Event_Attributes:
			if attributes.value == nil {
				break
			}
			if len(attributes.value.new_execution_run_id) > 0 {
				page.kind = .Follow_Run
				clone_ok: bool
				page.next_run_id, clone_ok = clone_result_string(attributes.value.new_execution_run_id)
				if !clone_ok {
					return {}, error_from_string(.Malformed_Response, "could not allocate continued Workflow run ID")
				}
				return page, {}
			}
			page.kind = .Completed
			if attributes.value.result != nil && len(attributes.value.result.payloads) > 0 {
				clone_error: codec.Error
				page.payload, clone_error = proto.clone_payload(&attributes.value.result.payloads[0])
				if clone_error != .None {
					return {}, error_from_string(.Payload_Conversion, "could not copy Workflow result Payload")
				}
				page.has_payload = true
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Failed_Event_Attributes:
			if attributes.value == nil {
				break
			}
			if len(attributes.value.new_execution_run_id) > 0 {
				page.kind = .Follow_Run
				clone_ok: bool
				page.next_run_id, clone_ok = clone_result_string(attributes.value.new_execution_run_id)
				if !clone_ok {
					return {}, error_from_string(.Malformed_Response, "could not allocate retried Workflow run ID")
				}
				return page, {}
			}
			page.kind = .Failed
			message := "Workflow failed"
			if attributes.value.failure != nil && len(attributes.value.failure.message) > 0 {
				message = attributes.value.failure.message
			}
			if attributes.value.failure != nil {
				encoded_failure := proto.encode_Temporal_Api_Failure_V1_Failure(attributes.value.failure)
				page.failure_proto = encoded_failure[:]
			}
			message_ok: bool
			page.message, message_ok = clone_result_string(message)
			if !message_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow failure")
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Canceled_Event_Attributes:
			page.kind = .Canceled
			message_ok: bool
			page.message, message_ok = clone_result_string("Workflow canceled")
			if !message_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow cancellation")
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Terminated_Event_Attributes:
			page.kind = .Terminated
			message := "Workflow terminated"
			if attributes.value != nil && len(attributes.value.reason) > 0 {
				message = attributes.value.reason
			}
			message_ok: bool
			page.message, message_ok = clone_result_string(message)
			if !message_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow termination")
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Timed_Out_Event_Attributes:
			if attributes.value != nil && len(attributes.value.new_execution_run_id) > 0 {
				page.kind = .Follow_Run
				clone_ok: bool
				page.next_run_id, clone_ok = clone_result_string(attributes.value.new_execution_run_id)
				if !clone_ok {
					return {}, error_from_string(.Malformed_Response, "could not allocate timed-out Workflow run ID")
				}
				return page, {}
			}
			page.kind = .Timed_Out
			message_ok: bool
			page.message, message_ok = clone_result_string("Workflow timed out")
			if !message_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow timeout")
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Continued_As_New_Event_Attributes:
			if attributes.value == nil || len(attributes.value.new_execution_run_id) == 0 {
				return {}, error_from_string(.Malformed_Response, "Continue-As-New event omitted its new run ID")
			}
			page.kind = .Continued_As_New
			clone_ok: bool
			page.next_run_id, clone_ok = clone_result_string(attributes.value.new_execution_run_id)
			if !clone_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate continued Workflow run ID")
			}
			return page, {}
		}
	}
	return {}, error_from_string(.Malformed_Response, "Temporal close-event history had no terminal event")
}

Run_Id_Owner :: struct {
	value: string,
}

run_id_owner_free :: proc(owner: ^Run_Id_Owner) {
	if owner == nil {
		return
	}
	delete(owner.value)
	owner.value = ""
}

get_result_payload_unintercepted :: proc(
	handle: ^Workflow_Handle,
	follow_runs := true,
) -> (payload: codec.Payload, error: Error) {
	if handle == nil || handle.client == nil || handle.client.connection == nil || len(handle.id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	current_run_id, allocation_error := strings.clone(handle.run_id)
	if allocation_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow result run ID")
	}
	run_id := Run_Id_Owner{value = current_run_id}
	defer run_id_owner_free(&run_id)

	for {
		execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
			workflow_id = handle.id,
			run_id = run_id.value,
		}
		request := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryRequest{
			namespace = handle.client.namespace,
			execution = &execution,
			wait_new_event = true,
			history_event_filter_type = .HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
			skip_archival = true,
		}
		request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryRequest(&request)
		rpc_result := temporal_core.client_rpc_call(
			handle.client.connection,
			.Workflow,
			"GetWorkflowExecutionHistory",
			request_bytes[:],
			timeout_millis = 0,
		)
		delete(request_bytes)
		if rpc_result.error.message != nil {
			delete(rpc_result.protobuf)
			return {}, error_from_core(&rpc_result.error)
		}
		page, page_error := decode_result_page(rpc_result.protobuf)
		delete(rpc_result.protobuf)
		if page_error.message != nil {
			result_page_free(&page)
			return {}, page_error
		}

		switch page.kind {
		case .Completed:
			result := page.payload
			page.payload = {}
			result_page_free(&page)
			return result, {}
		case .Follow_Run:
			if !follow_runs {
				result_error := error_from_string(.Workflow_Continued_As_New, "Workflow completed in a new run")
				result_page_free(&page)
				return {}, result_error
			}
			delete(run_id.value)
			run_id.value = page.next_run_id
			page.next_run_id = ""
			result_page_free(&page)
			continue
		case .Continued_As_New:
			if !follow_runs {
				result_error := error_from_string(.Workflow_Continued_As_New, "Workflow continued as new")
				result_page_free(&page)
				return {}, result_error
			}
			delete(run_id.value)
			run_id.value = page.next_run_id
			page.next_run_id = ""
			result_page_free(&page)
			continue
		case .Failed:
			error = error_from_string(.Workflow_Failed, page.message)
			error.details = page.failure_proto
			page.failure_proto = nil
		case .Canceled:
			error = error_from_string(.Workflow_Canceled, page.message)
		case .Terminated:
			error = error_from_string(.Workflow_Terminated, page.message)
		case .Timed_Out:
			error = error_from_string(.Workflow_Timed_Out, page.message)
		}
		result_page_free(&page)
		return {}, error
	}
}

get_result_payload_terminal :: proc(
	input: ^Client_Get_Result_Input,
) -> (codec.Payload, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "Workflow result input is missing")
	}
	return get_result_payload_unintercepted(input.handle, input.follow_runs)
}

get_result_payload :: proc(
	handle: ^Workflow_Handle,
	follow_runs := true,
) -> (payload: codec.Payload, error: Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	input := Client_Get_Result_Input{handle = handle, follow_runs = follow_runs}
	state := Client_Interceptor_State{client = handle.client}
	return client_get_result_chain(&state, &input)
}

get_result_json :: proc(
	handle: ^Workflow_Handle,
	value: ^$T,
	follow_runs := true,
) -> Error {
	payload, result_error := get_result_payload(handle, follow_runs)
	if result_error.message != nil {
		return result_error
	}
	defer codec.payload_free(&payload)
	if payload.allocator.procedure == nil {
		return error_from_string(.Payload_Conversion, "Workflow completed without a result Payload")
	}
	decode_error := codec.decode_json(codec.payload_view(&payload), value)
	if decode_error != .None {
		return error_from_string(.Payload_Conversion, "could not decode Workflow result")
	}
	return {}
}

// Package client provides the experimental public Temporal client boundary.
package client

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:encoding/uuid"
import "core:math"
import "core:mem"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"
import temporal_failure "../failure"

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
// Error.details. Workflow execution and Workflow Update failures populate this
// field; other error kinds return No_Data.
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
	runtime:     ^temporal_core.Runtime,
	connection:  ^temporal_core.Client,
	namespace:   string,
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

	runtime := new(temporal_core.Runtime, heap)
	created_runtime, core_error := temporal_core.runtime_create()
	if core_error.message != nil {
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
		return {}, error_from_core(&connect_error, .Connection)
	}
	connection^ = created_connection
	return {
		runtime = runtime,
		connection = connection,
		namespace = owned_namespace,
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
	enable_eager_workflow_start: bool,
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

// Headers owns a deterministic insertion-ordered map of Temporal Payloads.
// It is reusable across synchronous Client calls, but must outlive each call
// whose options borrow it.
Headers :: struct {
	entries:   [dynamic]Start_Metadata_Named_Payload,
	allocator: mem.Allocator,
}

headers_init :: proc(headers: ^Headers, allocator := context.allocator) -> Error {
	if headers == nil || allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Temporal headers require an allocator")
	}
	if headers.allocator.procedure != nil {
		return error_from_string(.Invalid_Argument, "Temporal headers are already initialized")
	}
	headers.entries = make([dynamic]Start_Metadata_Named_Payload, allocator)
	headers.allocator = allocator
	return {}
}

headers_free :: proc(headers: ^Headers) {
	if headers == nil || headers.allocator.procedure == nil {
		return
	}
	allocator := headers.allocator
	for &entry in headers.entries {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	delete(headers.entries)
	headers^ = {}
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
	if headers == nil || headers.allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Temporal headers are not initialized")
	}
	payload, clone_error := codec.payload_clone(value, headers.allocator)
	if clone_error != .None {
		return error_from_string(.Payload_Conversion, "could not clone Temporal header Payload")
	}
	return start_metadata_put_named_owned(&headers.entries, key, payload, headers.allocator)
}

headers_add_json :: proc(headers: ^Headers, key: string, value: $T) -> Error {
	if headers == nil || headers.allocator.procedure == nil {
		return error_from_string(.Invalid_Argument, "Temporal headers are not initialized")
	}
	payload, encode_error := codec.encode_json(value, headers.allocator)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Temporal header")
	}
	return start_metadata_put_named_owned(&headers.entries, key, payload, headers.allocator)
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
	eager_execution_accepted: bool,
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
	return ""
}

free_start_proto_payloads :: proc(payloads: []proto.Temporal_Api_Common_V1_Payload) {
	for &payload in payloads {
		delete(payload.metadata)
		delete(payload.external_payloads)
	}
	delete(payloads)
}

// encode_start_workflow_request is the deterministic request-construction
// boundary used by the Client and wire-contract tests. Returned bytes are owned
// by the caller.
encode_start_workflow_request :: proc(
	namespace: string,
	workflow_type: string,
	input: codec.Payload_View,
	options: Start_Workflow_Options,
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

	request_id := options.request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(request_id) == 0 {
		request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	}
	workflow_type_value := proto.Temporal_Api_Common_V1_WorkflowType{name = workflow_type}
	task_queue := proto.Temporal_Api_Taskqueue_V1_TaskQueue{name = options.task_queue}
	payload := proto.encode_payload(input)
	defer delete(payload.metadata)
	defer delete(payload.external_payloads)
	inputs := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(inputs.payloads)
	append(&inputs.payloads, payload)

	request := proto.Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest{
		namespace = namespace,
		workflow_id = options.id,
		workflow_type = &workflow_type_value,
		task_queue = &task_queue,
		input = &inputs,
		identity = "temporal-sdk-odin@local",
		request_id = request_id,
		workflow_id_reuse_policy = workflow_id_reuse_policy_to_proto(options.id_reuse_policy),
		workflow_id_conflict_policy = workflow_id_conflict_policy_to_proto(options.id_conflict_policy),
		cron_schedule = options.cron_schedule,
		request_eager_execution = options.enable_eager_workflow_start,
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
		if len(metadata.headers) > 0 {
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

	return proto.encode_Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest(&request), {}
}

start_workflow_payload :: proc(
	client: ^Client,
	workflow_type: string,
	input: codec.Payload_View,
	options: Start_Workflow_Options,
) -> (handle: Workflow_Handle, error: Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	request_bytes, request_error := encode_start_workflow_request(
		client.namespace,
		workflow_type,
		input,
		options,
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
	return handle, {}
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

signal_workflow :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	explicit_request_id := "",
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if len(signal_name) == 0 {
		return error_from_string(.Invalid_Argument, "Workflow signal name must not be empty")
	}
	execution := workflow_execution(handle)
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		signal_name = signal_name,
		identity = "temporal-sdk-odin@local",
		request_id = resolve_request_id(explicit_request_id, request_id_buffer[:]),
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "SignalWorkflowExecution", request_bytes[:])
}

signal_workflow_payload :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	input: codec.Payload_View,
	explicit_request_id := "",
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if len(signal_name) == 0 {
		return error_from_string(.Invalid_Argument, "Workflow signal name must not be empty")
	}
	execution := workflow_execution(handle)
	payload := proto.encode_payload(input)
	defer delete(payload.metadata)
	defer delete(payload.external_payloads)
	inputs := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(inputs.payloads)
	append(&inputs.payloads, payload)
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		signal_name = signal_name,
		input = &inputs,
		identity = "temporal-sdk-odin@local",
		request_id = resolve_request_id(explicit_request_id, request_id_buffer[:]),
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "SignalWorkflowExecution", request_bytes[:])
}

signal_workflow_json :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	input: $T,
	explicit_request_id := "",
) -> Error {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow signal input")
	}
	defer codec.payload_free(&payload)
	return signal_workflow_payload(
		handle,
		signal_name,
		codec.payload_view(&payload),
		explicit_request_id,
	)
}

// encode_query_workflow_request is exposed for wire-contract testing. Returned
// bytes are owned by the caller.
encode_query_workflow_request :: proc(
	namespace, workflow_id, run_id, query_type: string,
	input: codec.Payload_View,
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
	encoded_input := proto.encode_payload(input)
	defer delete(encoded_input.metadata)
	defer delete(encoded_input.external_payloads)
	arguments := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(arguments.payloads)
	append(&arguments.payloads, encoded_input)
	query := proto.Temporal_Api_Query_V1_WorkflowQuery{
		query_type = query_type,
		query_args = &arguments,
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

query_workflow_payload_with_options :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	input: codec.Payload_View,
	options: Query_Workflow_Options,
) -> (payload: codec.Payload, error: Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	request_bytes, request_error := encode_query_workflow_request(
		handle.client.namespace,
		handle.id,
		handle.run_id,
		query_type,
		input,
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

poll_update_result_payload :: proc(
	client: ^Client,
	workflow_id, run_id, update_id: string,
) -> (codec.Payload, Error) {
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
	for {
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
			timeout_millis = 0,
		)
		delete(request_bytes)
		if rpc_result.error.message != nil {
			delete(rpc_result.protobuf)
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

start_update_payload :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	input: codec.Payload_View,
	explicit_update_id := "",
) -> (Update_Handle, Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if len(update_name) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update name must not be empty")
	}
	execution := workflow_execution(handle)
	encoded_input := proto.encode_payload(input)
	defer delete(encoded_input.metadata)
	defer delete(encoded_input.external_payloads)
	arguments := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(arguments.payloads)
	append(&arguments.payloads, encoded_input)
	update_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	update_id := resolve_request_id(explicit_update_id, update_id_buffer[:])
	meta := proto.Temporal_Api_Update_V1_Meta{
		update_id = update_id,
		identity = "temporal-sdk-odin@local",
	}
	update_input := proto.Temporal_Api_Update_V1_Input{
		name = update_name,
		args = &arguments,
	}
	update_request := proto.Temporal_Api_Update_V1_Request{
		meta = &meta,
		input = &update_input,
		request_id = resolve_request_id("", request_id_buffer[:]),
	}
	wait_policy := proto.Temporal_Api_Update_V1_WaitPolicy{
		lifecycle_stage = .UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED,
	}
	request := proto.Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		first_execution_run_id = handle.first_execution_run_id,
		wait_policy = &wait_policy,
		request = &update_request,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_UpdateWorkflowExecutionRequest(&request)
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

start_update_json :: proc(
	handle: ^Workflow_Handle,
	update_name: string,
	input: $Input,
	explicit_update_id := "",
) -> (Update_Handle, Error) {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode Workflow Update input")
	}
	defer codec.payload_free(&payload)
	return start_update_payload(
		handle,
		update_name,
		codec.payload_view(&payload),
		explicit_update_id,
	)
}

wait_update_payload :: proc(handle: ^Update_Handle) -> (codec.Payload, Error) {
	if !update_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow Update handle is not initialized")
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
	)
}

wait_update_json :: proc(handle: ^Update_Handle, value: ^$Output) -> Error {
	if value == nil {
		return error_from_string(.Invalid_Argument, "Workflow Update output pointer is required")
	}
	result, update_error := wait_update_payload(handle)
	if update_error.message != nil {
		return update_error
	}
	defer codec.payload_free(&result)
	if codec.decode_json(codec.payload_view(&result), value) != .None {
		return error_from_string(.Payload_Conversion, "could not decode Workflow Update result")
	}
	return {}
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

cancel_workflow :: proc(
	handle: ^Workflow_Handle,
	reason := "",
	explicit_request_id := "",
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	execution := workflow_execution(handle)
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_RequestCancelWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		identity = "temporal-sdk-odin@local",
		request_id = resolve_request_id(explicit_request_id, request_id_buffer[:]),
		first_execution_run_id = handle.first_execution_run_id,
		reason = reason,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_RequestCancelWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "RequestCancelWorkflowExecution", request_bytes[:])
}

terminate_workflow :: proc(
	handle: ^Workflow_Handle,
	reason := "",
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	execution := workflow_execution(handle)
	request := proto.Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		reason = reason,
		identity = "temporal-sdk-odin@local",
		first_execution_run_id = handle.first_execution_run_id,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "TerminateWorkflowExecution", request_bytes[:])
}

terminate_workflow_payload :: proc(
	handle: ^Workflow_Handle,
	reason: string,
	details: codec.Payload_View,
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	execution := workflow_execution(handle)
	payload := proto.encode_payload(details)
	defer delete(payload.metadata)
	defer delete(payload.external_payloads)
	detail_payloads := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(detail_payloads.payloads)
	append(&detail_payloads.payloads, payload)
	request := proto.Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		reason = reason,
		details = &detail_payloads,
		identity = "temporal-sdk-odin@local",
		first_execution_run_id = handle.first_execution_run_id,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "TerminateWorkflowExecution", request_bytes[:])
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

get_result_payload :: proc(
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

package client

import allocator_runtime "base:runtime"
import "core:net"
import "core:strconv"
import "core:strings"
import proto "../core/proto"

NEXUS_LINK_TYPE_WORKFLOW_EVENT :: "temporal.api.common.v1.Link.WorkflowEvent"
NEXUS_LINK_TYPE_BATCH_JOB :: "temporal.api.common.v1.Link.BatchJob"
NEXUS_LINK_TYPE_ACTIVITY :: "temporal.api.common.v1.Link.Activity"
NEXUS_LINK_TYPE_NEXUS_OPERATION :: "temporal.api.common.v1.Link.NexusOperation"
NEXUS_LINK_TYPE_WORKFLOW :: "temporal.api.common.v1.Link.Workflow"
NEXUS_LINK_TYPE_CALLBACK :: "temporal.api.common.v1.Link.Callback"

Workflow_Event_Reference_Kind :: enum {
	None,
	Event_Id,
	Request_Id,
}

Link_Kind :: enum {
	Workflow_Event,
	Batch_Job,
	Activity,
	Nexus_Operation,
	Workflow,
	Callback,
}

Execution_Kind :: enum i32 {
	Unspecified,
	Workflow,
	Activity,
	Nexus_Operation,
}

Batch_Job_Link :: struct {
	job_id: string,
}

Activity_Link :: struct {
	namespace:   string,
	activity_id: string,
	run_id:      string,
}

Nexus_Operation_Link :: struct {
	namespace:    string,
	operation_id: string,
	run_id:       string,
}

Workflow_Link :: struct {
	namespace:   string,
	workflow_id: string,
	run_id:      string,
	reason:      string,
}

Callback_Link :: struct {
	namespace:      string,
	execution_kind: Execution_Kind,
	business_id:    string,
	run_id:         string,
	component_path: []string,
	request_id:     string,
}

// Link mirrors the six variants in the pinned common.v1.Link protobuf. Values
// constructed by callers are borrowed during synchronous request encoding.
// Values returned by link_from_nexus own decoded strings and must be released
// with link_free.
Link :: struct {
	kind:            Link_Kind,
	workflow_event:  Workflow_Event_Link,
	batch_job:       Batch_Job_Link,
	activity:        Activity_Link,
	nexus_operation: Nexus_Operation_Link,
	workflow:        Workflow_Link,
	callback:        Callback_Link,
	owner_allocator: allocator_runtime.Allocator,
	owned_strings:   [dynamic]string,
}

link_free :: proc(link: ^Link) {
	if link == nil {
		return
	}
	if link.owner_allocator.procedure != nil {
		for value in link.owned_strings {
			delete(value, link.owner_allocator)
		}
		delete(link.owned_strings)
		delete(link.callback.component_path)
	}
	link^ = {}
}

link_clone_component :: proc(link: ^Link, value: string) -> (string, bool) {
	if link == nil || link.owner_allocator.procedure == nil {
		return "", false
	}
	if len(value) == 0 {
		return "", true
	}
	owned, allocation_error := strings.clone(value, link.owner_allocator)
	if allocation_error != nil {
		return "", false
	}
	append(&link.owned_strings, owned)
	return owned, true
}

link_decode_component :: proc(
	link: ^Link,
	value: string,
	query: bool,
) -> (decoded: string, ok: bool) {
	if link == nil || link.owner_allocator.procedure == nil {
		return "", false
	}
	builder := strings.builder_make(link.owner_allocator)
	strings.builder_grow(&builder, len(value))
	index := 0
	for index < len(value) {
		character := value[index]
		if query && character == '+' {
			strings.write_byte(&builder, ' ')
			index += 1
			continue
		}
		if character != '%' {
			strings.write_byte(&builder, character)
			index += 1
			continue
		}
		if index + 2 >= len(value) {
			strings.builder_destroy(&builder)
			return "", false
		}
		high, high_ok := strconv.parse_uint(value[index+1:index+2], 16)
		low, low_ok := strconv.parse_uint(value[index+2:index+3], 16)
		if !high_ok || !low_ok {
			strings.builder_destroy(&builder)
			return "", false
		}
		strings.write_byte(&builder, u8(high << 4 | low))
		index += 3
	}
	decoded = strings.to_string(builder)
	ok = true
	append(&link.owned_strings, decoded)
	return decoded, true
}

WORKFLOW_EVENT_TYPE_PROTO_NAMES := [61]string{
	"EVENT_TYPE_UNSPECIFIED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_STARTED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_FAILED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT",
	"EVENT_TYPE_WORKFLOW_TASK_SCHEDULED",
	"EVENT_TYPE_WORKFLOW_TASK_STARTED",
	"EVENT_TYPE_WORKFLOW_TASK_COMPLETED",
	"EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT",
	"EVENT_TYPE_WORKFLOW_TASK_FAILED",
	"EVENT_TYPE_ACTIVITY_TASK_SCHEDULED",
	"EVENT_TYPE_ACTIVITY_TASK_STARTED",
	"EVENT_TYPE_ACTIVITY_TASK_COMPLETED",
	"EVENT_TYPE_ACTIVITY_TASK_FAILED",
	"EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT",
	"EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED",
	"EVENT_TYPE_ACTIVITY_TASK_CANCELED",
	"EVENT_TYPE_TIMER_STARTED",
	"EVENT_TYPE_TIMER_FIRED",
	"EVENT_TYPE_TIMER_CANCELED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED",
	"EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED",
	"EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED",
	"EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED",
	"EVENT_TYPE_MARKER_RECORDED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW",
	"EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED",
	"EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED",
	"EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED",
	"EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED",
	"EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED",
	"EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED",
	"EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT",
	"EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED",
	"EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED",
	"EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED",
	"EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED",
	"EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES",
	"EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED",
	"EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED_EXTERNALLY",
	"EVENT_TYPE_ACTIVITY_PROPERTIES_MODIFIED_EXTERNALLY",
	"EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED",
	"EVENT_TYPE_NEXUS_OPERATION_SCHEDULED",
	"EVENT_TYPE_NEXUS_OPERATION_STARTED",
	"EVENT_TYPE_NEXUS_OPERATION_COMPLETED",
	"EVENT_TYPE_NEXUS_OPERATION_FAILED",
	"EVENT_TYPE_NEXUS_OPERATION_CANCELED",
	"EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT",
	"EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED",
	"EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED",
	"EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED",
	"EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED",
}

normalize_workflow_event_type_name :: proc(value: string) -> (normalized: string, allocated: bool) {
	if strings.has_prefix(value, "EVENT_TYPE_") {
		return value, false
	}
	builder := strings.builder_make()
	strings.builder_grow(&builder, len(value) + len("EVENT_TYPE_") + 8)
	strings.write_string(&builder, "EVENT_TYPE_")
	for character, index in value {
		upper := character
		if character >= 'A' && character <= 'Z' && index > 0 {
			strings.write_byte(&builder, '_')
		}
		if character >= 'a' && character <= 'z' {
			upper -= 'a' - 'A'
		}
		strings.write_rune(&builder, upper)
	}
	return strings.to_string(builder), true
}

workflow_event_type_from_nexus :: proc(value: string) -> (Workflow_Event_Type, bool) {
	normalized, allocated := normalize_workflow_event_type_name(value)
	if allocated do defer delete(normalized)
	for candidate, index in WORKFLOW_EVENT_TYPE_PROTO_NAMES {
		if normalized == candidate {
			return Workflow_Event_Type(index), true
		}
	}
	return .Unspecified, false
}

link_query_value :: proc(queries: map[string]string, key: string) -> (string, bool) {
	if queries == nil {
		return "", false
	}
	value, found := queries[key]
	return value, found
}

// link_from_nexus converts the upstream Nexus URL/type pair into the pinned
// Temporal common Link model. The four URL forms implemented here match the
// current TypeScript SDK converter. BatchJob is intentionally encoding-only:
// upstream defines no Nexus URL mapping for it.
link_from_nexus :: proc(
	url, link_type: string,
	allocator := context.allocator,
) -> (Link, Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Nexus Link allocator is invalid")
	}
	result := Link{
		owner_allocator = allocator,
		owned_strings = make([dynamic]string, allocator),
	}
	succeeded := false
	defer if !succeeded do link_free(&result)

	scheme, _, path, queries, fragment := net.split_url(url)
	defer delete(queries)
	if scheme != "temporal" {
		return {}, error_from_string(.Invalid_Argument, "Nexus link must use the temporal scheme")
	}
	if len(fragment) > 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus link must not contain a fragment")
	}
	parts, split_error := strings.split(path, "/")
	if split_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not parse Nexus link path")
	}
	defer delete(parts)

	collection := ""
	tail := ""
	expected_parts := 0
	switch link_type {
	case NEXUS_LINK_TYPE_WORKFLOW_EVENT:
		result.kind = .Workflow_Event
		collection = "workflows"
		tail = "history"
		expected_parts = 7
	case NEXUS_LINK_TYPE_NEXUS_OPERATION:
		result.kind = .Nexus_Operation
		collection = "nexus-operations"
		tail = "details"
		expected_parts = 7
	case NEXUS_LINK_TYPE_ACTIVITY:
		result.kind = .Activity
		collection = "activities"
		tail = "details"
		expected_parts = 7
	case NEXUS_LINK_TYPE_WORKFLOW:
		result.kind = .Workflow
		collection = "workflows"
		expected_parts = 6
	case NEXUS_LINK_TYPE_BATCH_JOB:
		return {}, error_from_string(.Invalid_Argument, "BatchJob links have no upstream Nexus URL mapping")
	case NEXUS_LINK_TYPE_CALLBACK:
		return {}, error_from_string(.Invalid_Argument, "Callback links have no upstream Nexus URL mapping")
	case:
		return {}, error_from_string(.Invalid_Argument, "Nexus link has an unsupported Temporal link type")
	}
	if len(parts) != expected_parts || parts[1] != "namespaces" ||
	   parts[3] != collection || (expected_parts == 7 && parts[6] != tail) ||
	   len(parts[2]) == 0 || len(parts[4]) == 0 || len(parts[5]) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus link path does not match its Temporal link type")
	}
	namespace, namespace_ok := link_decode_component(&result, parts[2], false)
	id, id_ok := link_decode_component(&result, parts[4], false)
	run_id, run_id_ok := link_decode_component(&result, parts[5], false)
	if !namespace_ok || !id_ok || !run_id_ok || len(namespace) == 0 || len(id) == 0 || len(run_id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus link has invalid percent-encoded identity")
	}

	switch result.kind {
	case .Workflow_Event:
		result.workflow_event.namespace = namespace
		result.workflow_event.workflow_id = id
		result.workflow_event.run_id = run_id
		reference_type, has_reference_type := link_query_value(queries, "referenceType")
		event_id_value, has_event_id := link_query_value(queries, "eventID")
		request_id_value, has_request_id := link_query_value(queries, "requestID")
		event_type_value, has_event_type := link_query_value(queries, "eventType")
		if !has_reference_type {
			if has_event_id {
				reference_type = "EventReference"
			} else if has_request_id {
				reference_type = "RequestIdReference"
			}
		}
		if len(reference_type) == 0 && !has_event_id && !has_request_id && !has_event_type {
			succeeded = true
			return result, {}
		}
		if !has_event_type {
			return {}, error_from_string(.Invalid_Argument, "Nexus WorkflowEvent link has no event type")
		}
		event_type, event_type_ok := workflow_event_type_from_nexus(event_type_value)
		if !event_type_ok || event_type == .Unspecified {
			return {}, error_from_string(.Invalid_Argument, "Nexus WorkflowEvent link has an unsupported event type")
		}
		result.workflow_event.event_type = event_type
		switch reference_type {
		case "EventReference":
			if !has_event_id || has_request_id {
				return {}, error_from_string(.Invalid_Argument, "Nexus WorkflowEvent event reference is incomplete")
			}
			event_id, event_id_ok := strconv.parse_i64(event_id_value)
			if !event_id_ok || event_id <= 0 {
				return {}, error_from_string(.Invalid_Argument, "Nexus WorkflowEvent link has an invalid event ID")
			}
			result.workflow_event.reference_kind = .Event_Id
			result.workflow_event.has_event_reference = true
			result.workflow_event.event_id = event_id
		case "RequestIdReference":
			if !has_request_id || has_event_id {
				return {}, error_from_string(.Invalid_Argument, "Nexus WorkflowEvent request reference is incomplete")
			}
			request_id, request_id_ok := link_decode_component(&result, request_id_value, true)
			if !request_id_ok || len(request_id) == 0 {
				return {}, error_from_string(.Invalid_Argument, "Nexus WorkflowEvent link has an invalid request ID")
			}
			result.workflow_event.reference_kind = .Request_Id
			result.workflow_event.request_id = request_id
		case:
			return {}, error_from_string(.Invalid_Argument, "Nexus WorkflowEvent link has an unknown reference type")
		}
	case .Nexus_Operation:
		result.nexus_operation = {namespace = namespace, operation_id = id, run_id = run_id}
	case .Activity:
		result.activity = {namespace = namespace, activity_id = id, run_id = run_id}
	case .Workflow:
		result.workflow = {namespace = namespace, workflow_id = id, run_id = run_id}
		if reason_value, has_reason := link_query_value(queries, "reason"); has_reason {
			reason, reason_ok := link_decode_component(&result, reason_value, true)
			if !reason_ok {
				return {}, error_from_string(.Invalid_Argument, "Nexus Workflow link has an invalid reason")
			}
			result.workflow.reason = reason
		}
	case .Batch_Job:
		unreachable()
	case .Callback:
		unreachable()
	}
	succeeded = true
	return result, {}
}

// link_from_proto clones every string and callback component path from a
// decoded common.v1.Link. The result outlives the protobuf decode arena and
// must be released with link_free.
link_from_proto :: proc(
	source: ^proto.Temporal_Api_Common_V1_Link,
	allocator := context.allocator,
) -> (result: Link, error: Error) {
	if source == nil || allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Link source and allocator are required")
	}
	result.owner_allocator = allocator
	result.owned_strings = make([dynamic]string, allocator)
	succeeded := false
	defer if !succeeded do link_free(&result)

	#partial switch variant in source.variant {
	case proto.Temporal_Api_Common_V1_Link_Variant_Workflow_Event:
		if variant.value == nil {
			return {}, error_from_string(.Malformed_Response, "Workflow Event Link has no value")
		}
		value := variant.value
		result.kind = .Workflow_Event
		ok: bool
		result.workflow_event.namespace, ok = link_clone_component(&result, value.namespace)
		if ok do result.workflow_event.workflow_id, ok = link_clone_component(&result, value.workflow_id)
		if ok do result.workflow_event.run_id, ok = link_clone_component(&result, value.run_id)
		if !ok {
			return {}, error_from_string(.Malformed_Response, "could not allocate Workflow Event Link")
		}
		#partial switch reference in value.reference {
		case proto.Temporal_Api_Common_V1_Link_WorkflowEvent_Reference_Event_Ref:
			if reference.value == nil || i32(reference.value.event_type) < 0 ||
			   i32(reference.value.event_type) >= len(WORKFLOW_EVENT_TYPE_PROTO_NAMES) {
				return {}, error_from_string(.Malformed_Response, "Workflow Event Link has an invalid event reference")
			}
			result.workflow_event.reference_kind = .Event_Id
			result.workflow_event.has_event_reference = true
			result.workflow_event.event_id = reference.value.event_id
			result.workflow_event.event_type = Workflow_Event_Type(reference.value.event_type)
		case proto.Temporal_Api_Common_V1_Link_WorkflowEvent_Reference_Request_Id_Ref:
			if reference.value == nil || i32(reference.value.event_type) < 0 ||
			   i32(reference.value.event_type) >= len(WORKFLOW_EVENT_TYPE_PROTO_NAMES) {
				return {}, error_from_string(.Malformed_Response, "Workflow Event Link has an invalid request reference")
			}
			result.workflow_event.reference_kind = .Request_Id
			result.workflow_event.event_type = Workflow_Event_Type(reference.value.event_type)
			result.workflow_event.request_id, ok = link_clone_component(&result, reference.value.request_id)
			if !ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow Event request ID")
			}
		}
	case proto.Temporal_Api_Common_V1_Link_Variant_Batch_Job:
		if variant.value == nil {
			return {}, error_from_string(.Malformed_Response, "Batch Job Link has no value")
		}
		result.kind = .Batch_Job
		ok: bool
		result.batch_job.job_id, ok = link_clone_component(&result, variant.value.job_id)
		if !ok do return {}, error_from_string(.Malformed_Response, "could not allocate Batch Job Link")
	case proto.Temporal_Api_Common_V1_Link_Variant_Activity:
		if variant.value == nil {
			return {}, error_from_string(.Malformed_Response, "Activity Link has no value")
		}
		result.kind = .Activity
		ok: bool
		result.activity.namespace, ok = link_clone_component(&result, variant.value.namespace)
		if ok do result.activity.activity_id, ok = link_clone_component(&result, variant.value.activity_id)
		if ok do result.activity.run_id, ok = link_clone_component(&result, variant.value.run_id)
		if !ok do return {}, error_from_string(.Malformed_Response, "could not allocate Activity Link")
	case proto.Temporal_Api_Common_V1_Link_Variant_Nexus_Operation:
		if variant.value == nil {
			return {}, error_from_string(.Malformed_Response, "Nexus Operation Link has no value")
		}
		result.kind = .Nexus_Operation
		ok: bool
		result.nexus_operation.namespace, ok = link_clone_component(&result, variant.value.namespace)
		if ok do result.nexus_operation.operation_id, ok = link_clone_component(&result, variant.value.operation_id)
		if ok do result.nexus_operation.run_id, ok = link_clone_component(&result, variant.value.run_id)
		if !ok do return {}, error_from_string(.Malformed_Response, "could not allocate Nexus Operation Link")
	case proto.Temporal_Api_Common_V1_Link_Variant_Workflow:
		if variant.value == nil {
			return {}, error_from_string(.Malformed_Response, "Workflow Link has no value")
		}
		result.kind = .Workflow
		ok: bool
		result.workflow.namespace, ok = link_clone_component(&result, variant.value.namespace)
		if ok do result.workflow.workflow_id, ok = link_clone_component(&result, variant.value.workflow_id)
		if ok do result.workflow.run_id, ok = link_clone_component(&result, variant.value.run_id)
		if ok do result.workflow.reason, ok = link_clone_component(&result, variant.value.reason)
		if !ok do return {}, error_from_string(.Malformed_Response, "could not allocate Workflow Link")
	case proto.Temporal_Api_Common_V1_Link_Variant_Callback:
		if variant.value == nil || variant.value.execution == nil {
			return {}, error_from_string(.Malformed_Response, "Callback Link has no execution")
		}
		value := variant.value
		execution_type := i32(value.execution.type)
		if execution_type <= i32(Execution_Kind.Unspecified) ||
		   execution_type > i32(Execution_Kind.Nexus_Operation) {
			return {}, error_from_string(.Malformed_Response, "Callback Link has an unknown execution type")
		}
		result.kind = .Callback
		result.callback.execution_kind = Execution_Kind(execution_type)
		result.callback.component_path = make([]string, len(value.component_path), allocator)
		ok: bool
		result.callback.namespace, ok = link_clone_component(&result, value.namespace)
		if ok do result.callback.business_id, ok = link_clone_component(&result, value.execution.business_id)
		if ok do result.callback.run_id, ok = link_clone_component(&result, value.execution.run_id)
		if ok do result.callback.request_id, ok = link_clone_component(&result, value.request_id)
		if !ok do return {}, error_from_string(.Malformed_Response, "could not allocate Callback Link")
		for component, index in value.component_path {
			owned: string
			owned, ok = link_clone_component(&result, component)
			if !ok do return {}, error_from_string(.Malformed_Response, "could not allocate Callback Link path")
			result.callback.component_path[index] = owned
		}
	case:
		return {}, error_from_string(.Malformed_Response, "Temporal Link has no variant")
	}
	if message := link_error(result); len(message) > 0 {
		return {}, error_from_string(.Malformed_Response, message)
	}
	succeeded = true
	return result, {}
}

link_error :: proc(link: Link) -> string {
	switch link.kind {
	case .Workflow_Event:
		return workflow_event_link_error(link.workflow_event)
	case .Batch_Job:
		if len(link.batch_job.job_id) == 0 do return "Batch job links require a job ID"
	case .Activity:
		if len(link.activity.namespace) == 0 || len(link.activity.activity_id) == 0 ||
		   len(link.activity.run_id) == 0 {
			return "Activity links require namespace, Activity ID, and Run ID"
		}
	case .Nexus_Operation:
		if len(link.nexus_operation.namespace) == 0 || len(link.nexus_operation.operation_id) == 0 ||
		   len(link.nexus_operation.run_id) == 0 {
			return "Nexus operation links require namespace, operation ID, and Run ID"
		}
	case .Workflow:
		if len(link.workflow.namespace) == 0 || len(link.workflow.workflow_id) == 0 ||
		   len(link.workflow.run_id) == 0 {
			return "Workflow links require namespace, Workflow ID, and Run ID"
		}
	case .Callback:
		if len(link.callback.namespace) == 0 || link.callback.execution_kind == .Unspecified ||
		   i32(link.callback.execution_kind) < i32(Execution_Kind.Unspecified) ||
		   i32(link.callback.execution_kind) > i32(Execution_Kind.Nexus_Operation) ||
		   len(link.callback.business_id) == 0 || len(link.callback.run_id) == 0 ||
		   len(link.callback.request_id) == 0 {
			return "Callback links require namespace, execution kind, business ID, Run ID, and request ID"
		}
		for component in link.callback.component_path {
			if len(component) == 0 do return "Callback link component paths must not contain empty values"
		}
	case:
		return "Temporal links require a known Link kind"
	}
	return ""
}

Encoded_Link_Storage :: struct {
	workflow_events:  []proto.Temporal_Api_Common_V1_Link_WorkflowEvent,
	event_references: []proto.Temporal_Api_Common_V1_Link_WorkflowEvent_EventReference,
	request_references: []proto.Temporal_Api_Common_V1_Link_WorkflowEvent_RequestIdReference,
	batch_jobs:       []proto.Temporal_Api_Common_V1_Link_BatchJob,
	activities:       []proto.Temporal_Api_Common_V1_Link_Activity,
	nexus_operations: []proto.Temporal_Api_Common_V1_Link_NexusOperation,
	workflows:        []proto.Temporal_Api_Common_V1_Link_Workflow,
	callbacks:        []proto.Temporal_Api_Common_V1_Link_Callback,
	executions:       []proto.Temporal_Api_Common_V1_Execution,
	callback_paths:   [][dynamic]string,
	cursor:           int,
}

encoded_link_storage_make :: proc(count: int) -> Encoded_Link_Storage {
	return {
		workflow_events = make([]proto.Temporal_Api_Common_V1_Link_WorkflowEvent, count),
		event_references = make([]proto.Temporal_Api_Common_V1_Link_WorkflowEvent_EventReference, count),
		request_references = make([]proto.Temporal_Api_Common_V1_Link_WorkflowEvent_RequestIdReference, count),
		batch_jobs = make([]proto.Temporal_Api_Common_V1_Link_BatchJob, count),
		activities = make([]proto.Temporal_Api_Common_V1_Link_Activity, count),
		nexus_operations = make([]proto.Temporal_Api_Common_V1_Link_NexusOperation, count),
		workflows = make([]proto.Temporal_Api_Common_V1_Link_Workflow, count),
		callbacks = make([]proto.Temporal_Api_Common_V1_Link_Callback, count),
		executions = make([]proto.Temporal_Api_Common_V1_Execution, count),
		callback_paths = make([][dynamic]string, count),
	}
}

encoded_link_storage_free :: proc(storage: ^Encoded_Link_Storage) {
	if storage == nil do return
	delete(storage.workflow_events)
	delete(storage.event_references)
	delete(storage.request_references)
	delete(storage.batch_jobs)
	delete(storage.activities)
	delete(storage.nexus_operations)
	delete(storage.workflows)
	delete(storage.callbacks)
	delete(storage.executions)
	for path in storage.callback_paths do delete(path)
	delete(storage.callback_paths)
	storage^ = {}
}

encode_link :: proc(link: Link, storage: ^Encoded_Link_Storage) -> proto.Temporal_Api_Common_V1_Link {
	index := storage.cursor
	storage.cursor += 1
	switch link.kind {
	case .Workflow_Event:
		value := link.workflow_event
		storage.workflow_events[index] = {
			namespace = value.namespace,
			workflow_id = value.workflow_id,
			run_id = value.run_id,
		}
		reference_kind := value.reference_kind
		if reference_kind == .None && value.has_event_reference do reference_kind = .Event_Id
		switch reference_kind {
		case .Event_Id:
			storage.event_references[index] = {
				event_id = value.event_id,
				event_type = transmute(proto.Temporal_Api_Enums_V1_EventType)value.event_type,
			}
			storage.workflow_events[index].reference = proto.Temporal_Api_Common_V1_Link_WorkflowEvent_Reference_Event_Ref{
				value = &storage.event_references[index],
			}
		case .Request_Id:
			storage.request_references[index] = {
				request_id = value.request_id,
				event_type = transmute(proto.Temporal_Api_Enums_V1_EventType)value.event_type,
			}
			storage.workflow_events[index].reference = proto.Temporal_Api_Common_V1_Link_WorkflowEvent_Reference_Request_Id_Ref{
				value = &storage.request_references[index],
			}
		case .None:
		}
		return {
			variant = proto.Temporal_Api_Common_V1_Link_Variant_Workflow_Event{
				value = &storage.workflow_events[index],
			},
		}
	case .Batch_Job:
		storage.batch_jobs[index] = {job_id = link.batch_job.job_id}
		return {variant = proto.Temporal_Api_Common_V1_Link_Variant_Batch_Job{value = &storage.batch_jobs[index]}}
	case .Activity:
		storage.activities[index] = {
			namespace = link.activity.namespace,
			activity_id = link.activity.activity_id,
			run_id = link.activity.run_id,
		}
		return {variant = proto.Temporal_Api_Common_V1_Link_Variant_Activity{value = &storage.activities[index]}}
	case .Nexus_Operation:
		storage.nexus_operations[index] = {
			namespace = link.nexus_operation.namespace,
			operation_id = link.nexus_operation.operation_id,
			run_id = link.nexus_operation.run_id,
		}
		return {variant = proto.Temporal_Api_Common_V1_Link_Variant_Nexus_Operation{value = &storage.nexus_operations[index]}}
	case .Workflow:
		storage.workflows[index] = {
			namespace = link.workflow.namespace,
			workflow_id = link.workflow.workflow_id,
			run_id = link.workflow.run_id,
			reason = link.workflow.reason,
		}
		return {variant = proto.Temporal_Api_Common_V1_Link_Variant_Workflow{value = &storage.workflows[index]}}
	case .Callback:
		storage.executions[index] = {
			type = transmute(proto.Temporal_Api_Enums_V1_ExecutionType)link.callback.execution_kind,
			business_id = link.callback.business_id,
			run_id = link.callback.run_id,
		}
		append(&storage.callback_paths[index], ..link.callback.component_path)
		storage.callbacks[index] = {
			namespace = link.callback.namespace,
			execution = &storage.executions[index],
			component_path = storage.callback_paths[index],
			request_id = link.callback.request_id,
		}
		return {variant = proto.Temporal_Api_Common_V1_Link_Variant_Callback{value = &storage.callbacks[index]}}
	}
	return {}
}

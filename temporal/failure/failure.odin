// Package failure provides an allocator-owned, recursive view of Temporal's
// Failure protobuf. It is shared by client and runtime APIs without exposing
// generated protobuf storage or its decoding lifetime.
package failure

import "core:mem"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import codec "../codec"
import proto "../core/proto"

MAX_CAUSE_DEPTH :: 128

Kind :: enum {
	Unknown,
	Application,
	Timeout,
	Canceled,
	Terminated,
	Server,
	Reset_Workflow,
	Activity,
	Child_Workflow,
	Nexus_Operation,
	Nexus_Handler,
}

Decode_Error :: enum {
	None,
	No_Data,
	Malformed,
	Too_Deep,
	Allocation_Failed,
}

Retry_State :: enum i32 {
	Unspecified             = 0,
	In_Progress             = 1,
	Non_Retryable_Failure   = 2,
	Timeout                 = 3,
	Maximum_Attempts_Reached = 4,
	Retry_Policy_Not_Set    = 5,
	Internal_Server_Error   = 6,
	Cancel_Requested        = 7,
}

Timeout_Type :: enum i32 {
	Unspecified       = 0,
	Start_To_Close    = 1,
	Schedule_To_Start = 2,
	Schedule_To_Close = 3,
	Heartbeat         = 4,
}

Application_Error_Category :: enum i32 {
	Unspecified = 0,
	Benign      = 1,
}

Nexus_Handler_Retry_Behavior :: enum i32 {
	Unspecified   = 0,
	Retryable     = 1,
	Non_Retryable = 2,
}

Application_Info :: struct {
	type:                 string,
	non_retryable:        bool,
	details:              [dynamic]codec.Payload,
	next_retry_delay:     time.Duration,
	has_next_retry_delay: bool,
	category:             Application_Error_Category,
}

Timeout_Info :: struct {
	timeout_type:           Timeout_Type,
	last_heartbeat_details: [dynamic]codec.Payload,
}

Canceled_Info :: struct {
	identity: string,
	details:  [dynamic]codec.Payload,
}

Terminated_Info :: struct {
	identity: string,
}

Server_Info :: struct {
	non_retryable: bool,
}

Reset_Workflow_Info :: struct {
	last_heartbeat_details: [dynamic]codec.Payload,
}

Activity_Info :: struct {
	scheduled_event_id: i64,
	started_event_id:   i64,
	identity:           string,
	activity_type:      string,
	activity_id:        string,
	retry_state:        Retry_State,
}

Child_Workflow_Info :: struct {
	namespace:          string,
	workflow_id:        string,
	run_id:             string,
	workflow_type:      string,
	initiated_event_id: i64,
	started_event_id:   i64,
	retry_state:        Retry_State,
}

Nexus_Operation_Info :: struct {
	scheduled_event_id: i64,
	endpoint:           string,
	service:            string,
	operation:          string,
	operation_id:       string,
	operation_token:    string,
}

Nexus_Handler_Info :: struct {
	type:           string,
	retry_behavior: Nexus_Handler_Retry_Behavior,
}

Failure :: struct {
	message:                string,
	source:                 string,
	stack_trace:            string,
	encoded_attributes:     codec.Payload,
	has_encoded_attributes: bool,
	kind:                   Kind,
	application:            Application_Info,
	timeout:                Timeout_Info,
	canceled:               Canceled_Info,
	terminated:             Terminated_Info,
	server:                 Server_Info,
	reset_workflow:         Reset_Workflow_Info,
	activity:               Activity_Info,
	child_workflow:         Child_Workflow_Info,
	nexus_operation:        Nexus_Operation_Info,
	nexus_handler:          Nexus_Handler_Info,
	cause:                  ^Failure,
	allocator:              mem.Allocator,
}

clone_string :: proc(value: string, allocator: mem.Allocator) -> (string, bool) {
	if len(value) == 0 {
		return "", true
	}
	result, allocation_error := strings.clone(value, allocator)
	return result, allocation_error == nil
}

clone_payload :: proc(
	value: ^proto.Temporal_Api_Common_V1_Payload,
	allocator: mem.Allocator,
) -> (codec.Payload, bool) {
	if value == nil {
		return {}, false
	}
	result := codec.payload_create(value.data, allocator)
	for entry in value.metadata {
		if codec.payload_add_metadata(&result, entry.key, entry.value) != .None {
			codec.payload_free(&result)
			return {}, false
		}
	}
	for external in value.external_payloads {
		if codec.payload_add_external(&result, external.size_bytes) != .None {
			codec.payload_free(&result)
			return {}, false
		}
	}
	return result, true
}

free_payloads :: proc(values: ^[dynamic]codec.Payload) {
	if values == nil {
		return
	}
	for &value in values {
		codec.payload_free(&value)
	}
	delete(values^)
	values^ = nil
}

clone_payloads :: proc(
	values: ^proto.Temporal_Api_Common_V1_Payloads,
	allocator: mem.Allocator,
) -> ([dynamic]codec.Payload, bool) {
	result := make([dynamic]codec.Payload, allocator)
	if values == nil {
		return result, true
	}
	for &value in values.payloads {
		owned, ok := clone_payload(&value, allocator)
		if !ok {
			free_payloads(&result)
			return nil, false
		}
		append(&result, owned)
	}
	return result, true
}

duration_from_proto :: proc(value: ^proto.Google_Protobuf_Duration) -> (time.Duration, bool) {
	if value == nil || value.seconds < 0 || value.nanos < 0 || value.nanos >= 1_000_000_000 {
		return 0, false
	}
	if value.seconds > 9_223_372_036 ||
	   (value.seconds == 9_223_372_036 && value.nanos > 854_775_807) {
		return 0, false
	}
	return time.Duration(value.seconds * i64(time.Second) + i64(value.nanos)), true
}

failure_free :: proc(value: ^Failure) {
	if value == nil {
		return
	}
	allocator := value.allocator
	if allocator.procedure == nil {
		value^ = {}
		return
	}
	delete(value.message, allocator)
	delete(value.source, allocator)
	delete(value.stack_trace, allocator)
	codec.payload_free(&value.encoded_attributes)
	delete(value.application.type, allocator)
	free_payloads(&value.application.details)
	free_payloads(&value.timeout.last_heartbeat_details)
	delete(value.canceled.identity, allocator)
	free_payloads(&value.canceled.details)
	delete(value.terminated.identity, allocator)
	free_payloads(&value.reset_workflow.last_heartbeat_details)
	delete(value.activity.identity, allocator)
	delete(value.activity.activity_type, allocator)
	delete(value.activity.activity_id, allocator)
	delete(value.child_workflow.namespace, allocator)
	delete(value.child_workflow.workflow_id, allocator)
	delete(value.child_workflow.run_id, allocator)
	delete(value.child_workflow.workflow_type, allocator)
	delete(value.nexus_operation.endpoint, allocator)
	delete(value.nexus_operation.service, allocator)
	delete(value.nexus_operation.operation, allocator)
	delete(value.nexus_operation.operation_id, allocator)
	delete(value.nexus_operation.operation_token, allocator)
	delete(value.nexus_handler.type, allocator)
	if value.cause != nil {
		failure_free(value.cause)
		free(value.cause, allocator)
	}
	value^ = {}
}

clone_failure :: proc(
	source: ^proto.Temporal_Api_Failure_V1_Failure,
	allocator: mem.Allocator,
	depth: int,
) -> (result: Failure, decode_error: Decode_Error) {
	if source == nil {
		return {}, .Malformed
	}
	if depth >= MAX_CAUSE_DEPTH {
		return {}, .Too_Deep
	}
	result.allocator = allocator
	defer if decode_error != .None {
		failure_free(&result)
	}
	string_ok: bool
	result.message, string_ok = clone_string(source.message, allocator)
	if !string_ok { return result, .Allocation_Failed }
	result.source, string_ok = clone_string(source.source, allocator)
	if !string_ok { return result, .Allocation_Failed }
	result.stack_trace, string_ok = clone_string(source.stack_trace, allocator)
	if !string_ok { return result, .Allocation_Failed }
	if source.encoded_attributes != nil {
		result.encoded_attributes, string_ok = clone_payload(source.encoded_attributes, allocator)
		if !string_ok { return result, .Allocation_Failed }
		result.has_encoded_attributes = true
	}

	#partial switch info in source.failure_info {
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
		result.kind = .Application
		if info.value == nil { return result, .Malformed }
		result.application.type, string_ok = clone_string(info.value.type, allocator)
		if !string_ok { return result, .Allocation_Failed }
		result.application.non_retryable = info.value.non_retryable
		result.application.category = cast(Application_Error_Category)info.value.category
		result.application.details, string_ok = clone_payloads(info.value.details, allocator)
		if !string_ok { return result, .Allocation_Failed }
		if info.value.next_retry_delay != nil {
			result.application.next_retry_delay, string_ok = duration_from_proto(info.value.next_retry_delay)
			if !string_ok { return result, .Malformed }
			result.application.has_next_retry_delay = true
		}
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Timeout_Failure_Info:
		result.kind = .Timeout
		if info.value == nil { return result, .Malformed }
		result.timeout.timeout_type = cast(Timeout_Type)info.value.timeout_type
		result.timeout.last_heartbeat_details, string_ok = clone_payloads(info.value.last_heartbeat_details, allocator)
		if !string_ok { return result, .Allocation_Failed }
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info:
		result.kind = .Canceled
		if info.value == nil { return result, .Malformed }
		result.canceled.identity, string_ok = clone_string(info.value.identity, allocator)
		if !string_ok { return result, .Allocation_Failed }
		result.canceled.details, string_ok = clone_payloads(info.value.details, allocator)
		if !string_ok { return result, .Allocation_Failed }
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Terminated_Failure_Info:
		result.kind = .Terminated
		if info.value == nil { return result, .Malformed }
		result.terminated.identity, string_ok = clone_string(info.value.identity, allocator)
		if !string_ok { return result, .Allocation_Failed }
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Server_Failure_Info:
		result.kind = .Server
		if info.value == nil { return result, .Malformed }
		result.server.non_retryable = info.value.non_retryable
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Reset_Workflow_Failure_Info:
		result.kind = .Reset_Workflow
		if info.value == nil { return result, .Malformed }
		result.reset_workflow.last_heartbeat_details, string_ok = clone_payloads(info.value.last_heartbeat_details, allocator)
		if !string_ok { return result, .Allocation_Failed }
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Activity_Failure_Info:
		result.kind = .Activity
		if info.value == nil { return result, .Malformed }
		result.activity.scheduled_event_id = info.value.scheduled_event_id
		result.activity.started_event_id = info.value.started_event_id
		result.activity.retry_state = cast(Retry_State)info.value.retry_state
		result.activity.identity, string_ok = clone_string(info.value.identity, allocator)
		if !string_ok { return result, .Allocation_Failed }
		result.activity.activity_id, string_ok = clone_string(info.value.activity_id, allocator)
		if !string_ok { return result, .Allocation_Failed }
		if info.value.activity_type != nil {
			result.activity.activity_type, string_ok = clone_string(info.value.activity_type.name, allocator)
			if !string_ok { return result, .Allocation_Failed }
		}
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Child_Workflow_Execution_Failure_Info:
		result.kind = .Child_Workflow
		if info.value == nil { return result, .Malformed }
		result.child_workflow.initiated_event_id = info.value.initiated_event_id
		result.child_workflow.started_event_id = info.value.started_event_id
		result.child_workflow.retry_state = cast(Retry_State)info.value.retry_state
		result.child_workflow.namespace, string_ok = clone_string(info.value.namespace, allocator)
		if !string_ok { return result, .Allocation_Failed }
		if info.value.workflow_execution != nil {
			result.child_workflow.workflow_id, string_ok = clone_string(info.value.workflow_execution.workflow_id, allocator)
			if !string_ok { return result, .Allocation_Failed }
			result.child_workflow.run_id, string_ok = clone_string(info.value.workflow_execution.run_id, allocator)
			if !string_ok { return result, .Allocation_Failed }
		}
		if info.value.workflow_type != nil {
			result.child_workflow.workflow_type, string_ok = clone_string(info.value.workflow_type.name, allocator)
			if !string_ok { return result, .Allocation_Failed }
		}
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Nexus_Operation_Execution_Failure_Info:
		result.kind = .Nexus_Operation
		if info.value == nil { return result, .Malformed }
		result.nexus_operation.scheduled_event_id = info.value.scheduled_event_id
		result.nexus_operation.endpoint, string_ok = clone_string(info.value.endpoint, allocator)
		if !string_ok { return result, .Allocation_Failed }
		result.nexus_operation.service, string_ok = clone_string(info.value.service, allocator)
		if !string_ok { return result, .Allocation_Failed }
		result.nexus_operation.operation, string_ok = clone_string(info.value.operation, allocator)
		if !string_ok { return result, .Allocation_Failed }
		result.nexus_operation.operation_id, string_ok = clone_string(info.value.operation_id, allocator)
		if !string_ok { return result, .Allocation_Failed }
		result.nexus_operation.operation_token, string_ok = clone_string(info.value.operation_token, allocator)
		if !string_ok { return result, .Allocation_Failed }
	case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Nexus_Handler_Failure_Info:
		result.kind = .Nexus_Handler
		if info.value == nil { return result, .Malformed }
		result.nexus_handler.type, string_ok = clone_string(info.value.type, allocator)
		if !string_ok { return result, .Allocation_Failed }
		result.nexus_handler.retry_behavior = cast(Nexus_Handler_Retry_Behavior)info.value.retry_behavior
	}

	if source.cause != nil {
		cause, cause_error := clone_failure(source.cause, allocator, depth + 1)
		if cause_error != .None {
			return result, cause_error
		}
		cause_ptr, allocation_error := new(Failure, allocator)
		if allocation_error != nil {
			failure_free(&cause)
			return result, .Allocation_Failed
		}
		cause_ptr^ = cause
		result.cause = cause_ptr
	}
	return result, .None
}

decode :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (result: Failure, decode_error: Decode_Error) {
	if len(data) == 0 {
		return {}, .No_Data
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, .Allocation_Failed
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	decoded_failure := proto.Temporal_Api_Failure_V1_Failure{}
	decoded := proto.decode_Temporal_Api_Failure_V1_Failure(data, &decoded_failure)
	context.allocator = previous_allocator
	if !decoded {
		return {}, .Malformed
	}
	return clone_failure(&decoded_failure, allocator, 0)
}

find_kind :: proc(value: ^Failure, kind: Kind) -> ^Failure {
	current := value
	for current != nil {
		if current.kind == kind {
			return current
		}
		current = current.cause
	}
	return nil
}

root_cause :: proc(value: ^Failure) -> ^Failure {
	current := value
	if current == nil {
		return nil
	}
	for current.cause != nil {
		current = current.cause
	}
	return current
}

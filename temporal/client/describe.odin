package client

import "core:mem"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"

Workflow_Status :: enum i32 {
	Unspecified = 0,
	Running = 1,
	Completed = 2,
	Failed = 3,
	Canceled = 4,
	Terminated = 5,
	Continued_As_New = 6,
	Timed_Out = 7,
	Paused = 8,
}

Pending_Activity_State :: enum i32 {
	Unspecified = 0,
	Scheduled = 1,
	Started = 2,
	Cancel_Requested = 3,
	Paused = 4,
	Pause_Requested = 5,
}

Pending_Workflow_Task_State :: enum i32 {
	Unspecified = 0,
	Scheduled = 1,
	Started = 2,
}

Parent_Close_Policy :: enum i32 {
	Unspecified = 0,
	Terminate = 1,
	Abandon = 2,
	Request_Cancel = 3,
}

Optional_Time :: struct {
	value:   time.Time,
	present: bool,
}

Optional_Duration :: struct {
	value:   time.Duration,
	present: bool,
}

Workflow_Priority :: struct {
	priority_key:    i32,
	fairness_key:    string,
	fairness_weight: f32,
	present:         bool,
}

Named_Payload :: struct {
	key:     string,
	payload: codec.Payload,
}

Pending_Activity :: struct {
	activity_id:                string,
	activity_type:              string,
	state:                      Pending_Activity_State,
	attempt:                    i32,
	maximum_attempts:            i32,
	last_worker_identity:        string,
	paused:                     bool,
	scheduled_time:              Optional_Time,
	last_started_time:           Optional_Time,
	last_heartbeat_time:         Optional_Time,
	expiration_time:             Optional_Time,
	last_attempt_complete_time:  Optional_Time,
	next_attempt_schedule_time:  Optional_Time,
	current_retry_interval:      Optional_Duration,
	priority:                   Workflow_Priority,
	heartbeat_details:           [dynamic]codec.Payload,
	last_failure_message:        string,
}

Pending_Child_Workflow :: struct {
	workflow_id:        string,
	run_id:             string,
	workflow_type:      string,
	initiated_event_id: i64,
	parent_close_policy: Parent_Close_Policy,
}

Pending_Workflow_Task :: struct {
	present:                 bool,
	state:                   Pending_Workflow_Task_State,
	scheduled_time:          Optional_Time,
	original_scheduled_time: Optional_Time,
	started_time:            Optional_Time,
	attempt:                 i32,
}

Workflow_Description :: struct {
	workflow_id:                 string,
	run_id:                      string,
	first_run_id:                string,
	workflow_type:               string,
	task_queue:                  string,
	status:                      Workflow_Status,
	start_time:                  Optional_Time,
	execution_time:              Optional_Time,
	close_time:                  Optional_Time,
	execution_duration:          Optional_Duration,
	workflow_execution_timeout:  Optional_Duration,
	workflow_run_timeout:        Optional_Duration,
	default_workflow_task_timeout: Optional_Duration,
	history_length:              i64,
	history_size_bytes:          i64,
	state_transition_count:      i64,
	external_payload_size_bytes: i64,
	external_payload_count:      i64,
	cancel_requested:            bool,
	priority:                    Workflow_Priority,
	static_summary:              string,
	has_static_summary:          bool,
	static_details:              string,
	has_static_details:          bool,
	memo:                        [dynamic]Named_Payload,
	search_attributes:           [dynamic]Named_Payload,
	pending_activities:          [dynamic]Pending_Activity,
	pending_children:            [dynamic]Pending_Child_Workflow,
	pending_workflow_task:       Pending_Workflow_Task,
	allocator:                   mem.Allocator,
}

description_clone_string :: proc(value: string, allocator: mem.Allocator) -> (string, bool) {
	if len(value) == 0 {
		return "", true
	}
	result, allocation_error := strings.clone(value, allocator)
	return result, allocation_error == nil
}

description_time :: proc(value: ^proto.Google_Protobuf_Timestamp) -> Optional_Time {
	if value == nil {
		return {}
	}
	return {value = time.unix(value.seconds, i64(value.nanos)), present = true}
}

description_duration :: proc(value: ^proto.Google_Protobuf_Duration) -> Optional_Duration {
	if value == nil {
		return {}
	}
	return {
		value = time.Duration(value.seconds * i64(time.Second) + i64(value.nanos)),
		present = true,
	}
}

description_priority :: proc(
	value: ^proto.Temporal_Api_Common_V1_Priority,
	allocator: mem.Allocator,
) -> (Workflow_Priority, bool) {
	if value == nil {
		return {}, true
	}
	fairness_key, ok := description_clone_string(value.fairness_key, allocator)
	if !ok {
		return {}, false
	}
	return {
		priority_key = value.priority_key,
		fairness_key = fairness_key,
		fairness_weight = value.fairness_weight,
		present = true,
	}, true
}

description_clone_payload :: proc(
	value: ^proto.Temporal_Api_Common_V1_Payload,
	allocator: mem.Allocator,
) -> (codec.Payload, bool) {
	if value == nil {
		return {}, false
	}
	payload, clone_error := proto.clone_payload_view(proto.payload_view(value), allocator)
	return payload, clone_error == .None
}

description_decode_user_metadata_string :: proc(
	value: ^proto.Temporal_Api_Common_V1_Payload,
	allocator: mem.Allocator,
) -> (result: string, present, ok: bool) {
	if value == nil {
		return "", false, true
	}
	payload, payload_ok := description_clone_payload(value, allocator)
	if !payload_ok {
		return "", true, false
	}
	defer codec.payload_free(&payload)
	previous_allocator := context.allocator
	context.allocator = allocator
	decode_error := codec.decode_json(codec.payload_view(&payload), &result)
	context.allocator = previous_allocator
	if decode_error != .None {
		delete(result, allocator)
		return "", true, false
	}
	return result, true, true
}

description_clone_named_payload :: proc(
	key: string,
	value: ^proto.Temporal_Api_Common_V1_Payload,
	allocator: mem.Allocator,
) -> (Named_Payload, bool) {
	owned_key, key_ok := description_clone_string(key, allocator)
	if !key_ok {
		return {}, false
	}
	payload, payload_ok := description_clone_payload(value, allocator)
	if !payload_ok {
		delete(owned_key, allocator)
		return {}, false
	}
	return {key = owned_key, payload = payload}, true
}

description_failure_message :: proc(
	failure: ^proto.Temporal_Api_Failure_V1_Failure,
	allocator: mem.Allocator,
) -> (string, bool) {
	current := failure
	for current != nil && len(current.message) == 0 {
		current = current.cause
	}
	if current == nil {
		return "", true
	}
	return description_clone_string(current.message, allocator)
}

workflow_description_free :: proc(description: ^Workflow_Description) {
	if description == nil {
		return
	}
	allocator := description.allocator
	if allocator.procedure == nil {
		description^ = {}
		return
	}
	delete(description.workflow_id, allocator)
	delete(description.run_id, allocator)
	delete(description.first_run_id, allocator)
	delete(description.workflow_type, allocator)
	delete(description.task_queue, allocator)
	delete(description.static_summary, allocator)
	delete(description.static_details, allocator)
	delete(description.priority.fairness_key, allocator)
	for &entry in description.memo {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	for &entry in description.search_attributes {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	for &activity in description.pending_activities {
		pending_activity_free(&activity, allocator)
	}
	for &child in description.pending_children {
		delete(child.workflow_id, allocator)
		delete(child.run_id, allocator)
		delete(child.workflow_type, allocator)
	}
	delete(description.memo)
	delete(description.search_attributes)
	delete(description.pending_activities)
	delete(description.pending_children)
	description^ = {}
}

pending_activity_free :: proc(activity: ^Pending_Activity, allocator: mem.Allocator) {
	if activity == nil {
		return
	}
	delete(activity.activity_id, allocator)
	delete(activity.activity_type, allocator)
	delete(activity.last_worker_identity, allocator)
	delete(activity.priority.fairness_key, allocator)
	delete(activity.last_failure_message, allocator)
	for &payload in activity.heartbeat_details {
		codec.payload_free(&payload)
	}
	delete(activity.heartbeat_details)
	activity^ = {}
}

decode_workflow_description :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (description: Workflow_Description, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Describe response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_DescribeWorkflowExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_DescribeWorkflowExecutionResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded || response.workflow_execution_info == nil || response.execution_config == nil {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Describe response")
	}

	description.allocator = allocator
	description.memo = make([dynamic]Named_Payload, allocator)
	description.search_attributes = make([dynamic]Named_Payload, allocator)
	description.pending_activities = make([dynamic]Pending_Activity, allocator)
	description.pending_children = make([dynamic]Pending_Child_Workflow, allocator)
	failed := false
	defer if failed {
		workflow_description_free(&description)
	}

	info := response.workflow_execution_info
	config := response.execution_config
	if info.execution == nil || info.type == nil || config.task_queue == nil {
		failed = true
		return {}, error_from_string(.Malformed_Response, "Describe response omitted Workflow identity or task queue")
	}

	clone_targets := []struct {
		source: string,
		target: ^string,
	}{
		{info.execution.workflow_id, &description.workflow_id},
		{info.execution.run_id, &description.run_id},
		{info.first_run_id, &description.first_run_id},
		{info.type.name, &description.workflow_type},
		{config.task_queue.name, &description.task_queue},
	}
	for target in clone_targets {
		ok: bool
		target.target^, ok = description_clone_string(target.source, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate Describe response")
		}
	}

	description.status = Workflow_Status(info.status)
	description.start_time = description_time(info.start_time)
	description.execution_time = description_time(info.execution_time)
	description.close_time = description_time(info.close_time)
	description.execution_duration = description_duration(info.execution_duration)
	description.workflow_execution_timeout = description_duration(config.workflow_execution_timeout)
	description.workflow_run_timeout = description_duration(config.workflow_run_timeout)
	description.default_workflow_task_timeout = description_duration(config.default_workflow_task_timeout)
	description.history_length = info.history_length
	description.history_size_bytes = info.history_size_bytes
	description.state_transition_count = info.state_transition_count
	description.external_payload_size_bytes = info.external_payload_size_bytes
	description.external_payload_count = info.external_payload_count
	if response.workflow_extended_info != nil {
		description.cancel_requested = response.workflow_extended_info.cancel_requested
	}
	priority_ok: bool
	description.priority, priority_ok = description_priority(info.priority, allocator)
	if !priority_ok {
		failed = true
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow priority")
	}
	if config.user_metadata != nil {
		metadata_ok: bool
		description.static_summary,
		description.has_static_summary,
		metadata_ok = description_decode_user_metadata_string(
			config.user_metadata.summary,
			allocator,
		)
		if !metadata_ok {
			failed = true
			return {}, error_from_string(.Payload_Conversion, "could not decode Workflow static summary")
		}
		description.static_details,
		description.has_static_details,
		metadata_ok = description_decode_user_metadata_string(
			config.user_metadata.details,
			allocator,
		)
		if !metadata_ok {
			failed = true
			return {}, error_from_string(.Payload_Conversion, "could not decode Workflow static details")
		}
	}

	if info.memo != nil {
		for entry in info.memo.fields {
			owned, ok := description_clone_named_payload(entry.key, entry.value, allocator)
			if !ok {
				failed = true
				return {}, error_from_string(.Payload_Conversion, "could not copy Workflow memo")
			}
			append(&description.memo, owned)
		}
	}
	if info.search_attributes != nil {
		for entry in info.search_attributes.indexed_fields {
			owned, ok := description_clone_named_payload(entry.key, entry.value, allocator)
			if !ok {
				failed = true
				return {}, error_from_string(.Payload_Conversion, "could not copy Workflow Search Attribute")
			}
			append(&description.search_attributes, owned)
		}
	}

	for source in response.pending_activities {
		activity := Pending_Activity{}
		activity.heartbeat_details = make([dynamic]codec.Payload, allocator)
		ok: bool
		activity.activity_id, ok = description_clone_string(source.activity_id, allocator)
		failed = !ok
		if !failed && source.activity_type != nil {
			activity.activity_type, ok = description_clone_string(source.activity_type.name, allocator)
			failed = !ok
		}
		if !failed {
			activity.last_worker_identity, ok = description_clone_string(source.last_worker_identity, allocator)
			failed = !ok
		}
		if !failed {
			activity.last_failure_message, ok = description_failure_message(source.last_failure, allocator)
			failed = !ok
		}
		if !failed {
			activity.priority, ok = description_priority(source.priority, allocator)
			failed = !ok
		}
		if failed {
			pending_activity_free(&activity, allocator)
			return {}, error_from_string(.Malformed_Response, "could not allocate pending Activity description")
		}
		activity.state = Pending_Activity_State(source.state)
		activity.attempt = source.attempt
		activity.maximum_attempts = source.maximum_attempts
		activity.paused = source.paused
		activity.scheduled_time = description_time(source.scheduled_time)
		activity.last_started_time = description_time(source.last_started_time)
		activity.last_heartbeat_time = description_time(source.last_heartbeat_time)
		activity.expiration_time = description_time(source.expiration_time)
		activity.last_attempt_complete_time = description_time(source.last_attempt_complete_time)
		activity.next_attempt_schedule_time = description_time(source.next_attempt_schedule_time)
		activity.current_retry_interval = description_duration(source.current_retry_interval)
		if source.heartbeat_details != nil {
			for &payload in source.heartbeat_details.payloads {
				owned, payload_ok := description_clone_payload(&payload, allocator)
				if !payload_ok {
					pending_activity_free(&activity, allocator)
					failed = true
					return {}, error_from_string(.Payload_Conversion, "could not copy Activity heartbeat details")
				}
				append(&activity.heartbeat_details, owned)
			}
		}
		append(&description.pending_activities, activity)
	}

	for source in response.pending_children {
		child := Pending_Child_Workflow{
			initiated_event_id = source.initiated_id,
			parent_close_policy = Parent_Close_Policy(source.parent_close_policy),
		}
		ok: bool
		child.workflow_id, ok = description_clone_string(source.workflow_id, allocator)
		failed = !ok
		if !failed {
			child.run_id, ok = description_clone_string(source.run_id, allocator)
			failed = !ok
		}
		if !failed {
			child.workflow_type, ok = description_clone_string(source.workflow_type_name, allocator)
			failed = !ok
		}
		if failed {
			delete(child.workflow_id, allocator)
			delete(child.run_id, allocator)
			delete(child.workflow_type, allocator)
			return {}, error_from_string(.Malformed_Response, "could not allocate pending child Workflow description")
		}
		append(&description.pending_children, child)
	}

	if response.pending_workflow_task != nil {
		source := response.pending_workflow_task
		description.pending_workflow_task = {
			present = true,
			state = Pending_Workflow_Task_State(source.state),
			scheduled_time = description_time(source.scheduled_time),
			original_scheduled_time = description_time(source.original_scheduled_time),
			started_time = description_time(source.started_time),
			attempt = source.attempt,
		}
	}
	return description, {}
}

describe_workflow :: proc(handle: ^Workflow_Handle) -> (Workflow_Description, Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	execution := workflow_execution(handle)
	request := proto.Temporal_Api_Workflowservice_V1_DescribeWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		execution = &execution,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_DescribeWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"DescribeWorkflowExecution",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_workflow_description(rpc_result.protobuf)
}

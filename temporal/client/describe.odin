package client

import "core:bytes"
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

Workflow_Versioning_Behavior :: temporal_core.Worker_Versioning_Behavior

Continue_As_New_Versioning_Behavior :: enum i32 {
	Unspecified = 0,
	Auto_Upgrade = 1,
	Use_Ramping_Version = 2,
}

Activity_Assigned_Build_Id_Kind :: enum {
	Unspecified,
	Use_Workflow_Build_Id,
	Independent_Build_Id,
}

Activity_Pause_Kind :: enum {
	Unspecified,
	Manual,
	Rule,
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

Deployment_Identity :: struct {
	name:     string,
	build_id: string,
	present:  bool,
}

Worker_Version_Stamp :: struct {
	build_id:       string,
	use_versioning: bool,
	present:        bool,
}

Workflow_Deployment_Transition :: struct {
	present:        bool,
	target_present: bool,
	target:         Deployment_Identity,
	legacy_version: string,
}

Workflow_Versioning_Info :: struct {
	present:                  bool,
	behavior:                 Workflow_Versioning_Behavior,
	legacy_deployment:        Deployment_Identity,
	legacy_version:           string,
	deployment_version:       Deployment_Identity,
	versioning_override:      Workflow_Versioning_Override,
	has_versioning_override:  bool,
	legacy_transition:        Workflow_Deployment_Transition,
	version_transition:       Workflow_Deployment_Transition,
	revision_number:          i64,
	continue_as_new_behavior: Continue_As_New_Versioning_Behavior,
}

Workflow_Execution_Reference :: struct {
	workflow_id: string,
	run_id:      string,
	present:     bool,
}

Workflow_Reset_Point :: struct {
	build_id:                         string,
	binary_checksum:                  string,
	run_id:                           string,
	first_workflow_task_completed_id: i64,
	create_time:                      Optional_Time,
	expire_time:                      Optional_Time,
	resettable:                       bool,
}

Workflow_Request_Id_Info :: struct {
	request_id: string,
	event_type: History_Event_Type,
	event_id:   i64,
	buffered:   bool,
}

Workflow_Time_Skipping_Fast_Forward_Info :: struct {
	present:       bool,
	duration:      Optional_Duration,
	id:            string,
	target_time:   Optional_Time,
	has_completed: bool,
}

Workflow_Time_Skipping_Info :: struct {
	present:                    bool,
	current_time:               Optional_Time,
	effective_config:           Workflow_Time_Skipping_State,
	fast_forward_info:          Workflow_Time_Skipping_Fast_Forward_Info,
	current_session_skip_count: i32,
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
	last_worker_version_stamp:   Worker_Version_Stamp,
	assigned_build_id_kind:      Activity_Assigned_Build_Id_Kind,
	assigned_build_id:           string,
	legacy_last_deployment:      Deployment_Identity,
	last_worker_deployment_version: string,
	last_deployment_version:     Deployment_Identity,
	pause_info:                  Pending_Activity_Pause_Info,
	activity_options:            Standalone_Activity_Options,
	has_activity_options:        bool,
}

Pending_Activity_Pause_Info :: struct {
	present:    bool,
	kind:       Activity_Pause_Kind,
	pause_time: Optional_Time,
	rule_id:    string,
	identity:   string,
	reason:     string,
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

Workflow_Pause_Info :: struct {
	present:     bool,
	identity:    string,
	reason:      string,
	paused_time: Optional_Time,
}

Pending_Nexus_Operation :: struct {
	endpoint:                   string,
	service:                    string,
	operation:                  string,
	operation_id:               string,
	operation_token:            string,
	state:                      Pending_Nexus_Operation_State,
	attempt:                    i32,
	scheduled_event_id:         i64,
	schedule_to_close_timeout:  Optional_Duration,
	schedule_to_start_timeout:  Optional_Duration,
	start_to_close_timeout:     Optional_Duration,
	scheduled_time:             Optional_Time,
	last_attempt_complete_time: Optional_Time,
	next_attempt_schedule_time: Optional_Time,
	last_failure_message:       string,
	last_failure_proto:         []u8,
	blocked_reason:             string,
	cancellation:               Nexus_Operation_Cancellation_Description,
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
	most_recent_worker_version_stamp: Worker_Version_Stamp,
	assigned_build_id:           string,
	inherited_build_id:          string,
	worker_deployment_name:      string,
	versioning_info:             Workflow_Versioning_Info,
	parent_namespace_id:         string,
	parent_execution:            Workflow_Execution_Reference,
	root_execution:              Workflow_Execution_Reference,
	auto_reset_points:           [dynamic]Workflow_Reset_Point,
	execution_expiration_time:   Optional_Time,
	run_expiration_time:         Optional_Time,
	last_reset_time:             Optional_Time,
	original_start_time:         Optional_Time,
	reset_run_id:                string,
	request_id_infos:            [dynamic]Workflow_Request_Id_Info,
	time_skipping_info:          Workflow_Time_Skipping_Info,
	static_summary:              string,
	has_static_summary:          bool,
	static_details:              string,
	has_static_details:          bool,
	memo:                        [dynamic]Named_Payload,
	search_attributes:           [dynamic]Named_Payload,
	pending_activities:          [dynamic]Pending_Activity,
	pending_children:            [dynamic]Pending_Child_Workflow,
	pending_nexus_operations:    [dynamic]Pending_Nexus_Operation,
	pending_workflow_task:       Pending_Workflow_Task,
	pause_info:                  Workflow_Pause_Info,
	// Callback schemas are still evolving upstream. Preserve each complete
	// CallbackInfo as owned canonical protobuf until the Odin model stabilizes.
	callback_protos:             [dynamic][]u8,
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

deployment_identity_free :: proc(value: ^Deployment_Identity, allocator: mem.Allocator) {
	if value == nil || allocator.procedure == nil do return
	delete(value.name, allocator)
	delete(value.build_id, allocator)
	value^ = {}
}

description_deployment_identity :: proc(
	name, build_id: string,
	present: bool,
	allocator: mem.Allocator,
) -> (result: Deployment_Identity, ok: bool) {
	if !present do return {}, true
	result.present = true
	result.name, ok = description_clone_string(name, allocator)
	if ok do result.build_id, ok = description_clone_string(build_id, allocator)
	if !ok do deployment_identity_free(&result, allocator)
	return result, ok
}

worker_version_stamp_free :: proc(value: ^Worker_Version_Stamp, allocator: mem.Allocator) {
	if value == nil || allocator.procedure == nil do return
	delete(value.build_id, allocator)
	value^ = {}
}

description_worker_version_stamp :: proc(
	source: ^proto.Temporal_Api_Common_V1_WorkerVersionStamp,
	allocator: mem.Allocator,
) -> (result: Worker_Version_Stamp, ok: bool) {
	if source == nil do return {}, true
	result.present = true
	result.use_versioning = source.use_versioning
	result.build_id, ok = description_clone_string(source.build_id, allocator)
	return result, ok
}

workflow_deployment_transition_free :: proc(
	transition: ^Workflow_Deployment_Transition,
	allocator: mem.Allocator,
) {
	if transition == nil do return
	deployment_identity_free(&transition.target, allocator)
	delete(transition.legacy_version, allocator)
	transition^ = {}
}

workflow_versioning_info_free :: proc(
	info: ^Workflow_Versioning_Info,
	allocator: mem.Allocator,
) {
	if info == nil do return
	deployment_identity_free(&info.legacy_deployment, allocator)
	delete(info.legacy_version, allocator)
	deployment_identity_free(&info.deployment_version, allocator)
	workflow_versioning_override_free(&info.versioning_override, allocator)
	workflow_deployment_transition_free(&info.legacy_transition, allocator)
	workflow_deployment_transition_free(&info.version_transition, allocator)
	info^ = {}
}

description_workflow_versioning_info :: proc(
	source: ^proto.Temporal_Api_Workflow_V1_WorkflowExecutionVersioningInfo,
	allocator: mem.Allocator,
) -> (result: Workflow_Versioning_Info, ok: bool) {
	if source == nil do return {}, true
	behavior := i32(source.behavior)
	continue_behavior := i32(source.continue_as_new_initial_versioning_behavior)
	if behavior < i32(Workflow_Versioning_Behavior.Unspecified) ||
	   behavior > i32(Workflow_Versioning_Behavior.Auto_Upgrade) ||
	   continue_behavior < i32(Continue_As_New_Versioning_Behavior.Unspecified) ||
	   continue_behavior > i32(Continue_As_New_Versioning_Behavior.Use_Ramping_Version) {
		return {}, false
	}
	result.present = true
	result.behavior = Workflow_Versioning_Behavior(behavior)
	result.revision_number = source.revision_number
	result.continue_as_new_behavior = Continue_As_New_Versioning_Behavior(continue_behavior)
	ok = true
	if source.deployment != nil {
		result.legacy_deployment, ok = description_deployment_identity(
			source.deployment.series_name,
			source.deployment.build_id,
			true,
			allocator,
		)
	}
	if ok do result.legacy_version, ok = description_clone_string(source.version, allocator)
	if ok && source.deployment_version != nil {
		result.deployment_version, ok = description_deployment_identity(
			source.deployment_version.deployment_name,
			source.deployment_version.build_id,
			true,
			allocator,
		)
	}
	if ok {
		result.versioning_override,
		result.has_versioning_override,
		ok = decode_workflow_versioning_override(source.versioning_override, allocator)
	}
	if ok && source.deployment_transition != nil {
		result.legacy_transition.present = true
		if source.deployment_transition.deployment != nil {
			result.legacy_transition.target_present = true
			result.legacy_transition.target, ok = description_deployment_identity(
				source.deployment_transition.deployment.series_name,
				source.deployment_transition.deployment.build_id,
				true,
				allocator,
			)
		}
	}
	if ok && source.version_transition != nil {
		result.version_transition.present = true
		result.version_transition.legacy_version,
		ok = description_clone_string(source.version_transition.version, allocator)
		if ok && source.version_transition.deployment_version != nil {
			result.version_transition.target_present = true
			result.version_transition.target, ok = description_deployment_identity(
				source.version_transition.deployment_version.deployment_name,
				source.version_transition.deployment_version.build_id,
				true,
				allocator,
			)
		}
	}
	if !ok do workflow_versioning_info_free(&result, allocator)
	return result, ok
}

workflow_execution_reference_free :: proc(
	reference: ^Workflow_Execution_Reference,
	allocator: mem.Allocator,
) {
	if reference == nil || allocator.procedure == nil do return
	delete(reference.workflow_id, allocator)
	delete(reference.run_id, allocator)
	reference^ = {}
}

description_workflow_execution_reference :: proc(
	source: ^proto.Temporal_Api_Common_V1_WorkflowExecution,
	allocator: mem.Allocator,
) -> (result: Workflow_Execution_Reference, ok: bool) {
	if source == nil do return {}, true
	result.present = true
	result.workflow_id, ok = description_clone_string(source.workflow_id, allocator)
	if ok do result.run_id, ok = description_clone_string(source.run_id, allocator)
	if !ok do workflow_execution_reference_free(&result, allocator)
	return result, ok
}

workflow_reset_point_free :: proc(point: ^Workflow_Reset_Point, allocator: mem.Allocator) {
	if point == nil || allocator.procedure == nil do return
	delete(point.build_id, allocator)
	delete(point.binary_checksum, allocator)
	delete(point.run_id, allocator)
	point^ = {}
}

workflow_time_skipping_info_free :: proc(
	info: ^Workflow_Time_Skipping_Info,
	allocator: mem.Allocator,
) {
	if info == nil || allocator.procedure == nil do return
	delete(info.effective_config.fast_forward.id, allocator)
	delete(info.fast_forward_info.id, allocator)
	info^ = {}
}

description_time_skipping_info :: proc(
	source: ^proto.Temporal_Api_Common_V1_TimeSkippingInfo,
	allocator: mem.Allocator,
) -> (result: Workflow_Time_Skipping_Info, ok: bool) {
	if source == nil do return {}, true
	result.present = true
	result.current_time = description_time(source.current_time)
	result.current_session_skip_count = source.current_session_skip_count
	ok = true
	if source.effective_config != nil {
		config := source.effective_config
		result.effective_config.present = true
		result.effective_config.enabled = config.enabled
		result.effective_config.disable_propagation = config.disable_propagation
		result.effective_config.max_session_skip_count = config.max_session_skip_count
		if config.fast_forward_config != nil {
			result.effective_config.fast_forward.present = true
			result.effective_config.fast_forward.duration = description_duration(
				config.fast_forward_config.duration,
			).value
			result.effective_config.fast_forward.id,
			ok = description_clone_string(config.fast_forward_config.id, allocator)
		}
	}
	if ok && source.fast_forward_info != nil {
		fast_forward := source.fast_forward_info
		result.fast_forward_info.present = true
		result.fast_forward_info.duration = description_duration(fast_forward.fast_forward_duration)
		result.fast_forward_info.target_time = description_time(fast_forward.target_time)
		result.fast_forward_info.has_completed = fast_forward.has_completed
		result.fast_forward_info.id,
		ok = description_clone_string(fast_forward.fast_forward_id, allocator)
	}
	if !ok do workflow_time_skipping_info_free(&result, allocator)
	return result, ok
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

description_failure_proto :: proc(
	failure: ^proto.Temporal_Api_Failure_V1_Failure,
	allocator: mem.Allocator,
) -> ([]u8, bool) {
	if failure == nil do return nil, true
	encoded := proto.encode_Temporal_Api_Failure_V1_Failure(failure)
	defer delete(encoded)
	return bytes.clone(encoded[:], allocator), true
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
	worker_version_stamp_free(&description.most_recent_worker_version_stamp, allocator)
	delete(description.assigned_build_id, allocator)
	delete(description.inherited_build_id, allocator)
	delete(description.worker_deployment_name, allocator)
	workflow_versioning_info_free(&description.versioning_info, allocator)
	delete(description.parent_namespace_id, allocator)
	workflow_execution_reference_free(&description.parent_execution, allocator)
	workflow_execution_reference_free(&description.root_execution, allocator)
	for &point in description.auto_reset_points {
		workflow_reset_point_free(&point, allocator)
	}
	delete(description.auto_reset_points)
	delete(description.reset_run_id, allocator)
	for &request_info in description.request_id_infos {
		delete(request_info.request_id, allocator)
	}
	delete(description.request_id_infos)
	workflow_time_skipping_info_free(&description.time_skipping_info, allocator)
	delete(description.pause_info.identity, allocator)
	delete(description.pause_info.reason, allocator)
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
	for &operation in description.pending_nexus_operations {
		pending_nexus_operation_free(&operation, allocator)
	}
	for callback in description.callback_protos do delete(callback, allocator)
	delete(description.memo)
	delete(description.search_attributes)
	delete(description.pending_activities)
	delete(description.pending_children)
	delete(description.pending_nexus_operations)
	delete(description.callback_protos)
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
	worker_version_stamp_free(&activity.last_worker_version_stamp, allocator)
	delete(activity.assigned_build_id, allocator)
	deployment_identity_free(&activity.legacy_last_deployment, allocator)
	delete(activity.last_worker_deployment_version, allocator)
	deployment_identity_free(&activity.last_deployment_version, allocator)
	delete(activity.pause_info.rule_id, allocator)
	delete(activity.pause_info.identity, allocator)
	delete(activity.pause_info.reason, allocator)
	if activity.has_activity_options {
		standalone_activity_options_free(&activity.activity_options)
	}
	for &payload in activity.heartbeat_details {
		codec.payload_free(&payload)
	}
	delete(activity.heartbeat_details)
	activity^ = {}
}

pending_nexus_operation_free :: proc(operation: ^Pending_Nexus_Operation, allocator: mem.Allocator) {
	if operation == nil do return
	delete(operation.endpoint, allocator)
	delete(operation.service, allocator)
	delete(operation.operation, allocator)
	delete(operation.operation_id, allocator)
	delete(operation.operation_token, allocator)
	delete(operation.last_failure_message, allocator)
	delete(operation.last_failure_proto, allocator)
	delete(operation.blocked_reason, allocator)
	delete(operation.cancellation.last_failure_message, allocator)
	delete(operation.cancellation.last_failure_proto, allocator)
	delete(operation.cancellation.blocked_reason, allocator)
	delete(operation.cancellation.reason, allocator)
	operation^ = {}
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
	description.pending_nexus_operations = make([dynamic]Pending_Nexus_Operation, allocator)
	description.callback_protos = make([dynamic][]u8, allocator)
	description.auto_reset_points = make([dynamic]Workflow_Reset_Point, allocator)
	description.request_id_infos = make([dynamic]Workflow_Request_Id_Info, allocator)
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
		{info.assigned_build_id, &description.assigned_build_id},
		{info.inherited_build_id, &description.inherited_build_id},
		{info.worker_deployment_name, &description.worker_deployment_name},
		{info.parent_namespace_id, &description.parent_namespace_id},
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
	reference_ok: bool
	description.parent_execution,
	reference_ok = description_workflow_execution_reference(info.parent_execution, allocator)
	if reference_ok {
		description.root_execution,
		reference_ok = description_workflow_execution_reference(info.root_execution, allocator)
	}
	if !reference_ok {
		failed = true
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow execution ancestry")
	}
	if info.auto_reset_points != nil {
		for source in info.auto_reset_points.points {
			point := Workflow_Reset_Point{
				first_workflow_task_completed_id = source.first_workflow_task_completed_id,
				create_time = description_time(source.create_time),
				expire_time = description_time(source.expire_time),
				resettable = source.resettable,
			}
			point_ok := true
			point.build_id, point_ok = description_clone_string(source.build_id, allocator)
			if point_ok {
				point.binary_checksum,
				point_ok = description_clone_string(source.binary_checksum, allocator)
			}
			if point_ok do point.run_id, point_ok = description_clone_string(source.run_id, allocator)
			if !point_ok {
				workflow_reset_point_free(&point, allocator)
				failed = true
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow reset point")
			}
			append(&description.auto_reset_points, point)
		}
	}
	if response.workflow_extended_info != nil {
		extended := response.workflow_extended_info
		description.cancel_requested = extended.cancel_requested
		description.execution_expiration_time = description_time(extended.execution_expiration_time)
		description.run_expiration_time = description_time(extended.run_expiration_time)
		description.last_reset_time = description_time(extended.last_reset_time)
		description.original_start_time = description_time(extended.original_start_time)
		extended_ok := true
		description.reset_run_id, extended_ok = description_clone_string(extended.reset_run_id, allocator)
		if extended_ok {
			description.time_skipping_info,
			extended_ok = description_time_skipping_info(extended.time_skipping_info, allocator)
		}
		if !extended_ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate extended Workflow info")
		}
		for source in extended.request_id_infos {
			if source.value == nil {
				failed = true
				return {}, error_from_string(.Malformed_Response, "Workflow request ID info omitted its value")
			}
			request_id, request_ok := description_clone_string(source.key, allocator)
			if !request_ok {
				failed = true
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow request ID info")
			}
			append(&description.request_id_infos, Workflow_Request_Id_Info{
				request_id = request_id,
				event_type = History_Event_Type(source.value.event_type),
				event_id = source.value.event_id,
				buffered = source.value.buffered,
			})
		}
		if extended.pause_info != nil {
			source := extended.pause_info
			description.pause_info.present = true
			description.pause_info.paused_time = description_time(source.paused_time)
			pause_ok: bool
			description.pause_info.identity, pause_ok = description_clone_string(source.identity, allocator)
			if pause_ok {
				description.pause_info.reason, pause_ok = description_clone_string(source.reason, allocator)
			}
			if !pause_ok {
				failed = true
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow pause info")
			}
		}
	}
	priority_ok: bool
	description.priority, priority_ok = description_priority(info.priority, allocator)
	if !priority_ok {
		failed = true
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow priority")
	}
	version_ok: bool
	description.most_recent_worker_version_stamp,
	version_ok = description_worker_version_stamp(info.most_recent_worker_version_stamp, allocator)
	if version_ok {
		description.versioning_info,
		version_ok = description_workflow_versioning_info(info.versioning_info, allocator)
	}
	if !version_ok {
		failed = true
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow versioning info")
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
		if !failed {
			activity.last_worker_version_stamp,
			ok = description_worker_version_stamp(source.last_worker_version_stamp, allocator)
			failed = !ok
		}
		if !failed {
			#partial switch assigned in source.assigned_build_id {
			case proto.Temporal_Api_Workflow_V1_PendingActivityInfo_Assigned_Build_Id_Use_Workflow_Build_Id:
				activity.assigned_build_id_kind = .Use_Workflow_Build_Id
			case proto.Temporal_Api_Workflow_V1_PendingActivityInfo_Assigned_Build_Id_Last_Independently_Assigned_Build_Id:
				activity.assigned_build_id_kind = .Independent_Build_Id
				activity.assigned_build_id, ok = description_clone_string(assigned.value, allocator)
			case:
			}
			failed = !ok
		}
		if !failed && source.last_deployment != nil {
			activity.legacy_last_deployment, ok = description_deployment_identity(
				source.last_deployment.series_name,
				source.last_deployment.build_id,
				true,
				allocator,
			)
			failed = !ok
		}
		if !failed {
			activity.last_worker_deployment_version,
			ok = description_clone_string(source.last_worker_deployment_version, allocator)
			failed = !ok
		}
		if !failed && source.last_deployment_version != nil {
			activity.last_deployment_version, ok = description_deployment_identity(
				source.last_deployment_version.deployment_name,
				source.last_deployment_version.build_id,
				true,
				allocator,
			)
			failed = !ok
		}
		if !failed && source.pause_info != nil {
			activity.pause_info.present = true
			activity.pause_info.pause_time = description_time(source.pause_info.pause_time)
			#partial switch paused_by in source.pause_info.paused_by {
			case proto.Temporal_Api_Workflow_V1_PendingActivityInfo_PauseInfo_Paused_By_Manual:
				activity.pause_info.kind = .Manual
				if paused_by.value == nil {
					ok = false
				} else {
					activity.pause_info.identity, ok = description_clone_string(paused_by.value.identity, allocator)
					if ok do activity.pause_info.reason, ok = description_clone_string(paused_by.value.reason, allocator)
				}
			case proto.Temporal_Api_Workflow_V1_PendingActivityInfo_PauseInfo_Paused_By_Rule:
				activity.pause_info.kind = .Rule
				if paused_by.value == nil {
					ok = false
				} else {
					activity.pause_info.rule_id, ok = description_clone_string(paused_by.value.rule_id, allocator)
					if ok do activity.pause_info.identity, ok = description_clone_string(paused_by.value.identity, allocator)
					if ok do activity.pause_info.reason, ok = description_clone_string(paused_by.value.reason, allocator)
				}
			case:
			}
			failed = !ok
		}
		if !failed && source.activity_options != nil {
			activity.activity_options, ok = clone_activity_options(source.activity_options, allocator)
			activity.has_activity_options = ok
			failed = !ok
		}
		if failed {
			pending_activity_free(&activity, allocator)
			return {}, error_from_string(.Malformed_Response, "could not allocate pending Activity description")
		}
		state := i32(source.state)
		if state < i32(Pending_Activity_State.Unspecified) ||
		   state > i32(Pending_Activity_State.Pause_Requested) {
			pending_activity_free(&activity, allocator)
			failed = true
			return {}, error_from_string(.Malformed_Response, "Temporal returned an unknown pending Activity state")
		}
		activity.state = Pending_Activity_State(state)
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

	for source in response.pending_nexus_operations {
		operation := Pending_Nexus_Operation{
			attempt = source.attempt,
			scheduled_event_id = source.scheduled_event_id,
			schedule_to_close_timeout = description_duration(source.schedule_to_close_timeout),
			schedule_to_start_timeout = description_duration(source.schedule_to_start_timeout),
			start_to_close_timeout = description_duration(source.start_to_close_timeout),
			scheduled_time = description_time(source.scheduled_time),
			last_attempt_complete_time = description_time(source.last_attempt_complete_time),
			next_attempt_schedule_time = description_time(source.next_attempt_schedule_time),
		}
		ok: bool
		operation.state, ok = pending_nexus_operation_state_from_proto(source.state)
		operation_clone_targets := []struct {
			source: string,
			target: ^string,
		}{
			{source.endpoint, &operation.endpoint},
			{source.service, &operation.service},
			{source.operation, &operation.operation},
			{source.operation_id, &operation.operation_id},
			{source.operation_token, &operation.operation_token},
			{source.blocked_reason, &operation.blocked_reason},
		}
		for target in operation_clone_targets {
			if !ok do break
			target.target^, ok = description_clone_string(target.source, allocator)
		}
		if ok do operation.last_failure_message, ok = description_failure_message(source.last_attempt_failure, allocator)
		if ok do operation.last_failure_proto, ok = description_failure_proto(source.last_attempt_failure, allocator)
		if ok && source.cancellation_info != nil {
			cancellation := source.cancellation_info
			operation.cancellation.present = true
			operation.cancellation.requested_time = description_time(cancellation.requested_time)
			operation.cancellation.state, ok = nexus_operation_cancellation_state_from_proto(cancellation.state)
			operation.cancellation.attempt = cancellation.attempt
			operation.cancellation.last_attempt_complete_time = description_time(cancellation.last_attempt_complete_time)
			operation.cancellation.next_attempt_schedule_time = description_time(cancellation.next_attempt_schedule_time)
			if ok do operation.cancellation.last_failure_message, ok = description_failure_message(cancellation.last_attempt_failure, allocator)
			if ok do operation.cancellation.last_failure_proto, ok = description_failure_proto(cancellation.last_attempt_failure, allocator)
			if ok do operation.cancellation.blocked_reason, ok = description_clone_string(cancellation.blocked_reason, allocator)
		}
		if !ok {
			pending_nexus_operation_free(&operation, allocator)
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not decode pending Nexus operation")
		}
		append(&description.pending_nexus_operations, operation)
	}

	for &source in response.callbacks {
		encoded_callback := proto.encode_Temporal_Api_Workflow_V1_CallbackInfo(&source)
		owned_callback := bytes.clone(encoded_callback[:], allocator)
		delete(encoded_callback)
		append(&description.callback_protos, owned_callback)
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

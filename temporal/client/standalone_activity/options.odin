package standalone_activity

import "core:encoding/uuid"
import "core:fmt"
import "core:math"
import "core:mem/virtual"
import "core:time"
import temporal_core "../../core"
import proto "../../core/proto"

Standalone_Activity_Options_Update :: struct {
	update_mask:               []string,
	task_queue:                string,
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	heartbeat_timeout:         time.Duration,
	retry_policy:              ^Retry_Policy,
	priority:                  ^Priority,
	start_delay:               time.Duration,
	restore_original:          bool,
	request_id:                string,
}

// Update masks use the canonical protobuf field paths accepted by Temporal
// Server. TaskQueue is intentionally writable only through its name; its kind
// and normalized name are server-owned routing fields.
Standalone_Activity_Option_Path_Task_Queue_Name              :: "task_queue.name"
Standalone_Activity_Option_Path_Schedule_To_Close_Timeout    :: "schedule_to_close_timeout"
Standalone_Activity_Option_Path_Schedule_To_Start_Timeout    :: "schedule_to_start_timeout"
Standalone_Activity_Option_Path_Start_To_Close_Timeout       :: "start_to_close_timeout"
Standalone_Activity_Option_Path_Heartbeat_Timeout            :: "heartbeat_timeout"
Standalone_Activity_Option_Path_Retry_Policy                 :: "retry_policy"
Standalone_Activity_Option_Path_Retry_Initial_Interval       :: "retry_policy.initial_interval"
Standalone_Activity_Option_Path_Retry_Backoff_Coefficient    :: "retry_policy.backoff_coefficient"
Standalone_Activity_Option_Path_Retry_Maximum_Interval       :: "retry_policy.maximum_interval"
Standalone_Activity_Option_Path_Retry_Maximum_Attempts       :: "retry_policy.maximum_attempts"
Standalone_Activity_Option_Path_Priority                     :: "priority"
Standalone_Activity_Option_Path_Priority_Key                 :: "priority.priority_key"
Standalone_Activity_Option_Path_Priority_Fairness_Key        :: "priority.fairness_key"
Standalone_Activity_Option_Path_Priority_Fairness_Weight     :: "priority.fairness_weight"
Standalone_Activity_Option_Path_Start_Delay                  :: "start_delay"

standalone_activity_option_path_valid :: proc(path: string) -> bool {
	switch path {
	case Standalone_Activity_Option_Path_Task_Queue_Name,
	     Standalone_Activity_Option_Path_Schedule_To_Close_Timeout,
	     Standalone_Activity_Option_Path_Schedule_To_Start_Timeout,
	     Standalone_Activity_Option_Path_Start_To_Close_Timeout,
	     Standalone_Activity_Option_Path_Heartbeat_Timeout,
	     Standalone_Activity_Option_Path_Retry_Policy,
	     Standalone_Activity_Option_Path_Retry_Initial_Interval,
	     Standalone_Activity_Option_Path_Retry_Backoff_Coefficient,
	     Standalone_Activity_Option_Path_Retry_Maximum_Interval,
	     Standalone_Activity_Option_Path_Retry_Maximum_Attempts,
	     Standalone_Activity_Option_Path_Priority,
	     Standalone_Activity_Option_Path_Priority_Key,
	     Standalone_Activity_Option_Path_Priority_Fairness_Key,
	     Standalone_Activity_Option_Path_Priority_Fairness_Weight,
	     Standalone_Activity_Option_Path_Start_Delay:
		return true
	}
	return false
}

standalone_activity_option_path_selected :: proc(paths: []string, target: string) -> bool {
	for path in paths do if path == target do return true
	return false
}

standalone_activity_retry_path_selected :: proc(paths: []string) -> bool {
	return standalone_activity_option_path_selected(paths, Standalone_Activity_Option_Path_Retry_Policy) ||
	       standalone_activity_option_path_selected(paths, Standalone_Activity_Option_Path_Retry_Initial_Interval) ||
	       standalone_activity_option_path_selected(paths, Standalone_Activity_Option_Path_Retry_Backoff_Coefficient) ||
	       standalone_activity_option_path_selected(paths, Standalone_Activity_Option_Path_Retry_Maximum_Interval) ||
	       standalone_activity_option_path_selected(paths, Standalone_Activity_Option_Path_Retry_Maximum_Attempts)
}

standalone_activity_priority_path_selected :: proc(paths: []string) -> bool {
	return standalone_activity_option_path_selected(paths, Standalone_Activity_Option_Path_Priority) ||
	       standalone_activity_option_path_selected(paths, Standalone_Activity_Option_Path_Priority_Key) ||
	       standalone_activity_option_path_selected(paths, Standalone_Activity_Option_Path_Priority_Fairness_Key) ||
	       standalone_activity_option_path_selected(paths, Standalone_Activity_Option_Path_Priority_Fairness_Weight)
}

standalone_activity_options_update_error :: proc(update: Standalone_Activity_Options_Update) -> string {
	if update.restore_original {
		if len(update.update_mask) > 0 do return "restore_original cannot be combined with an Activity options update mask"
		return ""
	}
	if len(update.update_mask) == 0 do return "standalone Activity options update mask must not be empty"
	for path, index in update.update_mask {
		if !standalone_activity_option_path_valid(path) do return "standalone Activity options update mask contains an unknown path"
		for previous in update.update_mask[:index] {
			if previous == path do return "standalone Activity options update mask contains a duplicate path"
		}
	}
	if standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Task_Queue_Name) && len(update.task_queue) == 0 {
		return "standalone Activity task queue must not be empty"
	}
	if (standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Schedule_To_Close_Timeout) && update.schedule_to_close_timeout < 0) ||
	   (standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Schedule_To_Start_Timeout) && update.schedule_to_start_timeout < 0) ||
	   (standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Start_To_Close_Timeout) && update.start_to_close_timeout < 0) ||
	   (standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Heartbeat_Timeout) && update.heartbeat_timeout < 0) ||
	   (standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Start_Delay) && update.start_delay < 0) {
		return "standalone Activity option durations must not be negative"
	}
	if standalone_activity_retry_path_selected(update.update_mask) {
		if update.retry_policy == nil do return "standalone Activity Retry Policy update requires a value"
		policy := update.retry_policy
		if policy.initial_interval < 0 || policy.maximum_interval < 0 || policy.maximum_attempts < 0 {
			return "standalone Activity Retry Policy values must not be negative"
		}
		if math.is_nan(policy.backoff_coefficient) || math.is_inf(policy.backoff_coefficient) ||
		   (policy.backoff_coefficient != 0 && policy.backoff_coefficient < 1) {
			return "standalone Activity Retry Policy backoff coefficient must be zero or at least one"
		}
		updates_complete_policy := standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Retry_Policy)
		updates_both_intervals := standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Retry_Initial_Interval) &&
		                          standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Retry_Maximum_Interval)
		if (updates_complete_policy || updates_both_intervals) && policy.maximum_interval > 0 &&
		   policy.initial_interval > 0 && policy.maximum_interval < policy.initial_interval {
			return "standalone Activity Retry Policy maximum interval must not be shorter than its initial interval"
		}
	}
	if standalone_activity_priority_path_selected(update.update_mask) {
		if update.priority == nil do return "standalone Activity priority update requires a value"
		if update.priority.priority_key < 0 do return "standalone Activity priority key must not be negative"
		if len(update.priority.fairness_key) > 64 do return "standalone Activity priority fairness key must not exceed 64 bytes"
		if math.is_nan(update.priority.fairness_weight) || math.is_inf(update.priority.fairness_weight) ||
		   update.priority.fairness_weight < 0 {
			return "standalone Activity priority fairness weight must be finite and non-negative"
		}
	}
	return ""
}

encode_update_standalone_activity_options_request :: proc(
	namespace, identity, activity_id, run_id: string,
	update: Standalone_Activity_Options_Update,
) -> ([dynamic]u8, Error) {
	if len(namespace) == 0 || len(activity_id) == 0 {
		return nil, error_from_string(.Invalid_Argument, "standalone Activity options identity is incomplete")
	}
	if message := standalone_activity_options_update_error(update); len(message) > 0 {
		return nil, error_from_string(.Invalid_Argument, message)
	}
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	resource_id := fmt.aprintf("activity:%s", activity_id)
	defer delete(resource_id)
	request := proto.Temporal_Api_Workflowservice_V1_UpdateActivityExecutionOptionsRequest{
		namespace = namespace,
		activity_id = activity_id,
		run_id = run_id,
		identity = identity,
		restore_original = update.restore_original,
		resource_id = resource_id,
		request_id = resolve_request_id(update.request_id, request_id_buffer[:]),
	}
	if update.restore_original {
		return proto.encode_Temporal_Api_Workflowservice_V1_UpdateActivityExecutionOptionsRequest(&request), {}
	}
	mask := proto.Google_Protobuf_FieldMask{}
	defer delete(mask.paths)
	for path in update.update_mask do append(&mask.paths, path)
	request.update_mask = &mask
	options := proto.Temporal_Api_Activity_V1_ActivityOptions{}
	request.activity_options = &options
	task_queue := proto.Temporal_Api_Taskqueue_V1_TaskQueue{name = update.task_queue}
	if standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Task_Queue_Name) do options.task_queue = &task_queue
	schedule_to_close := duration_proto(update.schedule_to_close_timeout)
	schedule_to_start := duration_proto(update.schedule_to_start_timeout)
	start_to_close := duration_proto(update.start_to_close_timeout)
	heartbeat := duration_proto(update.heartbeat_timeout)
	start_delay := duration_proto(update.start_delay)
	if standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Schedule_To_Close_Timeout) do options.schedule_to_close_timeout = &schedule_to_close
	if standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Schedule_To_Start_Timeout) do options.schedule_to_start_timeout = &schedule_to_start
	if standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Start_To_Close_Timeout) do options.start_to_close_timeout = &start_to_close
	if standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Heartbeat_Timeout) do options.heartbeat_timeout = &heartbeat
	if standalone_activity_option_path_selected(update.update_mask, Standalone_Activity_Option_Path_Start_Delay) do options.start_delay = &start_delay
	retry := proto.Temporal_Api_Common_V1_RetryPolicy{}
	retry_initial := proto.Google_Protobuf_Duration{}
	retry_maximum := proto.Google_Protobuf_Duration{}
	defer delete(retry.non_retryable_error_types)
	if standalone_activity_retry_path_selected(update.update_mask) {
		policy := update.retry_policy
		retry.backoff_coefficient = policy.backoff_coefficient
		retry.maximum_attempts = policy.maximum_attempts
		if policy.initial_interval > 0 {
			retry_initial = duration_proto(policy.initial_interval)
			retry.initial_interval = &retry_initial
		}
		if policy.maximum_interval > 0 {
			retry_maximum = duration_proto(policy.maximum_interval)
			retry.maximum_interval = &retry_maximum
		}
		for value in policy.non_retryable_error_types do append(&retry.non_retryable_error_types, value)
		options.retry_policy = &retry
	}
	priority := proto.Temporal_Api_Common_V1_Priority{}
	if standalone_activity_priority_path_selected(update.update_mask) {
		priority = {
			priority_key = update.priority.priority_key,
			fairness_key = update.priority.fairness_key,
			fairness_weight = update.priority.fairness_weight,
		}
		options.priority = &priority
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_UpdateActivityExecutionOptionsRequest(&request), {}
}

decode_update_standalone_activity_options_response :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (result: Standalone_Activity_Options, error: Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity options allocator is not initialized")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize standalone Activity options arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_UpdateActivityExecutionOptionsResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_UpdateActivityExecutionOptionsResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded || response.activity_options == nil {
		return {}, error_from_string(.Malformed_Response, "Temporal returned malformed standalone Activity options")
	}
	ok: bool
	result, ok = clone_activity_options(response.activity_options, allocator)
	if !ok {
		return {}, error_from_string(.Malformed_Response, "could not copy standalone Activity options")
	}
	return result, {}
}

update_standalone_activity_options_terminal :: proc(
	input: ^Client_Update_Standalone_Activity_Options_Input,
) -> (Standalone_Activity_Options, Error) {
	if input == nil || !standalone_activity_handle_valid(input.handle) {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	request_bytes, request_error := encode_update_standalone_activity_options_request(
		input.handle.client.namespace,
		client_request_identity(input.handle.client),
		input.handle.id,
		input.handle.run_id,
		input.update,
	)
	if request_error.message != nil do return {}, request_error
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		input.handle.client.connection,
		.Workflow,
		"UpdateActivityExecutionOptions",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return {}, error_from_core(&rpc_result.error)
	return decode_update_standalone_activity_options_response(rpc_result.protobuf, input.allocator)
}

update_standalone_activity_options :: proc(
	handle: ^Standalone_Activity_Handle,
	update: Standalone_Activity_Options_Update,
	allocator := context.allocator,
) -> (Standalone_Activity_Options, Error) {
	if !standalone_activity_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	input := Client_Update_Standalone_Activity_Options_Input{
		handle = handle,
		update = update,
		allocator = allocator,
	}
	state := client_interceptor_state(handle.client)
	return client_update_standalone_activity_options_chain(&state, &input)
}

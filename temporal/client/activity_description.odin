package client

import "core:mem"
import proto "../core/proto"

// Standalone_Activity_Options is also the normalized options snapshot embedded
// in an ordinary Workflow Describe response for pending Activities. It remains
// in the base Client package for that stable inspection path; the optional
// standalone Activity package aliases it for its options-update response.
Standalone_Activity_Options :: struct {
	task_queue:                string,
	schedule_to_close_timeout: Optional_Duration,
	schedule_to_start_timeout: Optional_Duration,
	start_to_close_timeout:    Optional_Duration,
	heartbeat_timeout:         Optional_Duration,
	retry_policy:              Retry_Policy,
	has_retry_policy:          bool,
	priority:                  Workflow_Priority,
	start_delay:               Optional_Duration,
	allocator:                 mem.Allocator,
}

standalone_activity_options_free :: proc(options: ^Standalone_Activity_Options) {
	if options == nil do return
	allocator := options.allocator
	if allocator.procedure != nil {
		delete(options.task_queue, allocator)
		for value in options.retry_policy.non_retryable_error_types do delete(value, allocator)
		delete(options.retry_policy.non_retryable_error_types, allocator)
		delete(options.priority.fairness_key, allocator)
	}
	options^ = {}
}

standalone_activity_clone_retry_policy :: proc(
	source: ^proto.Temporal_Api_Common_V1_RetryPolicy,
	allocator: mem.Allocator,
) -> (policy: Retry_Policy, present, ok: bool) {
	if source == nil do return {}, false, true
	policy.initial_interval = description_duration(source.initial_interval).value
	policy.backoff_coefficient = source.backoff_coefficient
	policy.maximum_interval = description_duration(source.maximum_interval).value
	policy.maximum_attempts = source.maximum_attempts
	policy.non_retryable_error_types = make([]string, len(source.non_retryable_error_types), allocator)
	for value, index in source.non_retryable_error_types {
		owned, cloned := description_clone_string(value, allocator)
		if !cloned {
			for existing in policy.non_retryable_error_types[:index] do delete(existing, allocator)
			delete(policy.non_retryable_error_types, allocator)
			return {}, true, false
		}
		policy.non_retryable_error_types[index] = owned
	}
	return policy, true, true
}

clone_activity_options :: proc(
	options: ^proto.Temporal_Api_Activity_V1_ActivityOptions,
	allocator: mem.Allocator,
) -> (result: Standalone_Activity_Options, ok: bool) {
	if options == nil || allocator.procedure == nil do return {}, false
	result.allocator = allocator
	ok = true
	if options.task_queue != nil {
		result.task_queue, ok = description_clone_string(options.task_queue.name, allocator)
	}
	if !ok {
		standalone_activity_options_free(&result)
		return {}, false
	}
	result.schedule_to_close_timeout = description_duration(options.schedule_to_close_timeout)
	result.schedule_to_start_timeout = description_duration(options.schedule_to_start_timeout)
	result.start_to_close_timeout = description_duration(options.start_to_close_timeout)
	result.heartbeat_timeout = description_duration(options.heartbeat_timeout)
	result.start_delay = description_duration(options.start_delay)
	result.retry_policy,
	result.has_retry_policy,
	ok = standalone_activity_clone_retry_policy(options.retry_policy, allocator)
	if ok do result.priority, ok = description_priority(options.priority, allocator)
	if !ok {
		standalone_activity_options_free(&result)
		return {}, false
	}
	return result, true
}

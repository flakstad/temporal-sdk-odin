package workflow

import "core:bytes"
import proto "../core/proto"

Error :: struct {
	message: []u8,
}

// Process_Result is the owned completion and diagnostic metadata returned by
// every Workflow activation processor, whether execution is in-process or in
// an isolated child. It is part of the active runtime contract, not a legacy
// state-machine implementation.
Process_Result :: struct {
	completion:         [dynamic]u8,
	run_id:             string,
	error:              Error,
	activation_failed:  bool,
	scheduled_activity: bool,
	scheduled_activity_count: int,
	scheduled_local_activity: bool,
	scheduled_local_activity_count: int,
	activity_seq:       u32,
	resolved_activity_seq: u32,
	resolved_activity_failed: bool,
	waiting_activity_seq: u32,
	scheduled_nexus_operation: bool,
	scheduled_nexus_operation_count: int,
	nexus_operation_seq: u32,
	resolved_nexus_operation_start_seq: u32,
	resolved_nexus_operation_seq: u32,
	resolved_nexus_operation_failed: bool,
	resolved_nexus_operation_canceled: bool,
	resolved_nexus_operation_timed_out: bool,
	waiting_nexus_operation_seq: u32,
	requested_nexus_operation_cancellation: bool,
	requested_nexus_operation_cancellation_count: int,
	canceled_nexus_operation_seq: u32,
	started_child_workflow: bool,
	started_child_workflow_count: int,
	child_workflow_seq: u32,
	resolved_child_workflow_start_seq: u32,
	resolved_child_workflow_seq: u32,
	waiting_child_workflow_start_seq: u32,
	waiting_child_workflow_seq: u32,
	signaled_child_workflow: bool,
	signaled_child_workflow_count: int,
	child_workflow_signal_seq: u32,
	resolved_child_workflow_signal_seq: u32,
	resolved_child_workflow_signal_failed: bool,
	canceled_child_workflow_signal: bool,
	signaled_external_workflow: bool,
	signaled_external_workflow_count: int,
	external_workflow_signal_seq: u32,
	resolved_external_workflow_signal_seq: u32,
	resolved_external_workflow_signal_failed: bool,
	canceled_external_workflow_signal: bool,
	requested_external_workflow_cancellation: bool,
	requested_external_workflow_cancellation_count: int,
	external_workflow_cancellation_seq: u32,
	resolved_external_workflow_cancellation_seq: u32,
	resolved_external_workflow_cancellation_failed: bool,
	received_signal:     bool,
	received_signal_count: int,
	signal_handlers_started:              int,
	signal_handlers_completed:            int,
	unfinished_signal_handlers:           int,
	warned_unfinished_signal_handlers:    int,
	queries_answered:    int,
	queries_failed:      int,
	updates_accepted:    int,
	updates_rejected:    int,
	updates_completed:   int,
	unfinished_update_handlers:        int,
	warned_unfinished_update_handlers: int,
	waiting_signal:      bool,
	waiting_condition:   bool,
	waiting_join:        bool,
	waiting_selector:    bool,
	waiting_handlers:    bool,
	started_timer:      bool,
	timer_seq:          u32,
	waiting_timer_seq:  u32,
	canceled_timer:     bool,
	canceled_timer_seq: u32,
	received_cancellation: bool,
	cancellation_reason:   string,
	requested_activity_cancellation: bool,
	requested_activity_cancellation_count: int,
	requested_local_activity_cancellation: bool,
	requested_local_activity_cancellation_count: int,
	canceled_activity_seq: u32,
	requested_child_workflow_cancellation: bool,
	requested_child_workflow_cancellation_count: int,
	canceled_child_workflow_seq: u32,
	canceled_workflow:     bool,
	continued_as_new:      bool,
	patch_markers_emitted: int,
	deprecated_patch_markers_emitted: int,
	search_attribute_upserts: int,
	memo_upserts:             int,
	completed:          bool,
	evicted:            bool,
	is_replaying:       bool,
	history_length:     u32,
}

error_free :: proc(error: ^Error) {
	if error == nil || error.message == nil {
		return
	}
	delete(error.message)
	error.message = nil
}

error_from_string :: proc(message: string) -> Error {
	return Error{message = bytes.clone(transmute([]u8)message)}
}

encode_activation_failure :: proc(run_id, message: string) -> [dynamic]u8 {
	application_info := proto.Temporal_Api_Failure_V1_ApplicationFailureInfo{
		type = "OdinWorkflowActivationError",
		non_retryable = true,
	}
	failure := proto.Temporal_Api_Failure_V1_Failure{
		message = message,
		source = "OdinSDK",
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = &application_info,
		},
	}
	failed := proto.Coresdk_Workflow_Completion_Failure{
		failure = &failure,
		force_cause = .WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
	}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{
		run_id = run_id,
		status = proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Failed{
			value = &failed,
		},
	}
	return proto.encode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(&completion)
}

encode_empty_activation_success :: proc(run_id: string) -> [dynamic]u8 {
	success := proto.Coresdk_Workflow_Completion_Success{}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{
		run_id = run_id,
		status = proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful{
			value = &success,
		},
	}
	return proto.encode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(&completion)
}

activation_failure_result :: proc(run_id, message: string) -> Process_Result {
	return Process_Result{
		completion = encode_activation_failure(run_id, message),
		error = error_from_string(message),
		activation_failed = true,
	}
}

process_error :: proc(result: ^Process_Result, run_id, message: string) {
	result.error = error_from_string(message)
	result.activation_failed = true
	if len(run_id) > 0 {
		result.completion = encode_activation_failure(run_id, message)
	}
}


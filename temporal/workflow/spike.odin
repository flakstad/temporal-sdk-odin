// Package workflow contains the bounded explicit-state-machine experiment.
// It is not the future user-facing sequential Workflow runtime.
package workflow

import "core:bytes"
import "core:mem/virtual"
import "core:strings"
import proto "../core/proto"

Phase :: enum {
	Waiting_Activity,
	Completed,
}

Instance :: struct {
	run_id: string,
	phase:  Phase,
}

Runtime :: struct {
	workflow_type:          string,
	task_queue:             string,
	instances:              [dynamic]Instance,
	activations:            int,
	replay_activations:     int,
	replay_initializations: int,
	evictions:              int,
}

Error :: struct {
	message: []u8,
}

Process_Result :: struct {
	completion:         [dynamic]u8,
	run_id:             string,
	error:              Error,
	activation_failed:  bool,
	scheduled_activity: bool,
	scheduled_activity_count: int,
	activity_seq:       u32,
	resolved_activity_seq: u32,
	resolved_activity_failed: bool,
	waiting_activity_seq: u32,
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
	canceled_activity_seq: u32,
	canceled_workflow:     bool,
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

runtime_init :: proc(runtime: ^Runtime, workflow_type, task_queue: string) -> Error {
	if runtime == nil || len(workflow_type) == 0 || len(task_queue) == 0 {
		return error_from_string("workflow type and task queue are required")
	}
	owned_type, type_error := strings.clone(workflow_type)
	if type_error != nil {
		return error_from_string("could not allocate workflow type")
	}
	owned_queue, queue_error := strings.clone(task_queue)
	if queue_error != nil {
		delete(owned_type)
		return error_from_string("could not allocate workflow task queue")
	}
	runtime.workflow_type = owned_type
	runtime.task_queue = owned_queue
	runtime.instances = make([dynamic]Instance)
	return {}
}

runtime_close :: proc(runtime: ^Runtime) {
	if runtime == nil {
		return
	}
	for instance in runtime.instances {
		delete(instance.run_id)
	}
	delete(runtime.instances)
	delete(runtime.workflow_type)
	delete(runtime.task_queue)
	runtime^ = {}
}

find_instance :: proc(runtime: ^Runtime, run_id: string) -> (^Instance, bool) {
	for index in 0..<len(runtime.instances) {
		if runtime.instances[index].run_id == run_id {
			return &runtime.instances[index], true
		}
	}
	return nil, false
}

add_instance :: proc(runtime: ^Runtime, run_id: string) -> (^Instance, Error) {
	owned_run_id, allocation_error := strings.clone(run_id)
	if allocation_error != nil {
		return nil, error_from_string("could not allocate workflow run id")
	}
	append(&runtime.instances, Instance{run_id = owned_run_id})
	return &runtime.instances[len(runtime.instances) - 1], {}
}

remove_instance :: proc(runtime: ^Runtime, run_id: string) -> bool {
	for index in 0..<len(runtime.instances) {
		if runtime.instances[index].run_id == run_id {
			delete(runtime.instances[index].run_id)
			ordered_remove(&runtime.instances, index)
			return true
		}
	}
	return false
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

process_activation :: proc(runtime: ^Runtime, data: []u8) -> (result: Process_Result) {
	if runtime == nil || len(runtime.workflow_type) == 0 {
		result.error = error_from_string("workflow runtime is not initialized")
		return
	}

	decode_arena: virtual.Arena
	arena_error := virtual.arena_init_growing(&decode_arena)
	if arena_error != nil {
		result.error = error_from_string("could not initialize workflow decode arena")
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{}
	decode_ok := proto.decode_Coresdk_Workflow_Activation_WorkflowActivation(data, &activation)
	context.allocator = previous_allocator
	if !decode_ok || len(activation.run_id) == 0 {
		result.error = error_from_string("Core returned a malformed WorkflowActivation protobuf")
		return
	}

	runtime.activations += 1
	result.is_replaying = activation.is_replaying
	result.history_length = activation.history_length
	if activation.is_replaying {
		runtime.replay_activations += 1
	}

	commands := make([dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand)
	defer delete(commands)
	schedule := proto.Coresdk_Workflow_Commands_ScheduleActivity{}
	defer delete(schedule.arguments)
	activity_timeout := proto.Google_Protobuf_Duration{seconds = 20}
	complete := proto.Coresdk_Workflow_Commands_CompleteWorkflowExecution{}
	fail := proto.Coresdk_Workflow_Commands_FailWorkflowExecution{}
	schedule_seen := false
	resolution_seen := false

	instance, instance_found := find_instance(runtime, activation.run_id)
	for job in activation.jobs {
		handled := false
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow:
			handled = true
			if variant.value == nil || variant.value.workflow_type != runtime.workflow_type {
				process_error(&result, activation.run_id, "unexpected workflow type in initialization")
				return
			}
			if instance_found || schedule_seen || len(variant.value.arguments) == 0 {
				process_error(&result, activation.run_id, "invalid duplicate or argument-less workflow initialization")
				return
			}
			add_error: Error
			instance, add_error = add_instance(runtime, activation.run_id)
			if add_error.message != nil {
				result.error = add_error
				result.completion = encode_activation_failure(activation.run_id, string(add_error.message))
				result.activation_failed = true
				return
			}
			instance_found = true
			if activation.is_replaying {
				runtime.replay_initializations += 1
			}
			schedule = proto.Coresdk_Workflow_Commands_ScheduleActivity{
				seq = 1,
				activity_id = "activity-1",
				activity_type = "hello",
				task_queue = runtime.task_queue,
				start_to_close_timeout = &activity_timeout,
				do_not_eagerly_execute = true,
			}
			append(&schedule.arguments, variant.value.arguments[0])
			append(
				&commands,
				proto.Coresdk_Workflow_Commands_WorkflowCommand{
					variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity{
						value = &schedule,
					},
				},
			)
			instance.phase = .Waiting_Activity
			schedule_seen = true
			result.scheduled_activity = true
			result.run_id = instance.run_id

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity:
			handled = true
			if variant.value == nil || variant.value.result == nil || !instance_found || instance.phase != .Waiting_Activity || variant.value.seq != 1 || resolution_seen {
				process_error(&result, activation.run_id, "activity resolution does not match workflow state")
				return
			}
			status_handled := false
			#partial switch status in variant.value.result.status {
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed:
				status_handled = true
				if status.value == nil || status.value.result == nil {
					process_error(&result, activation.run_id, "completed activity has no result")
					return
				}
				complete.result = status.value.result
				append(
					&commands,
					proto.Coresdk_Workflow_Commands_WorkflowCommand{
						variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution{
							value = &complete,
						},
					},
				)
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Failed:
				status_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "failed activity has no failure")
					return
				}
				fail.failure = status.value.failure
				append(
					&commands,
					proto.Coresdk_Workflow_Commands_WorkflowCommand{
						variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Fail_Workflow_Execution{
							value = &fail,
						},
					},
				)
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Cancelled:
				status_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "cancelled activity has no failure")
					return
				}
				fail.failure = status.value.failure
				append(
					&commands,
					proto.Coresdk_Workflow_Commands_WorkflowCommand{
						variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Fail_Workflow_Execution{
							value = &fail,
						},
					},
				)
			}
			if !status_handled {
				process_error(&result, activation.run_id, "activity backoff is outside the workflow spike")
				return
			}
			instance.phase = .Completed
			resolution_seen = true
			result.completed = true
			result.run_id = instance.run_id

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache:
			handled = true
			if len(activation.jobs) != 1 {
				process_error(&result, activation.run_id, "eviction activation contains additional jobs")
				return
			}
			_ = remove_instance(runtime, activation.run_id)
			instance = nil
			instance_found = false
			runtime.evictions += 1
			result.evicted = true
		}
		if !handled {
			process_error(&result, activation.run_id, "workflow spike received an unsupported activation job")
			return
		}
	}

	success := proto.Coresdk_Workflow_Completion_Success{commands = commands}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{
		run_id = activation.run_id,
		status = proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful{
			value = &success,
		},
	}
	result.completion = proto.encode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(&completion)
	return
}

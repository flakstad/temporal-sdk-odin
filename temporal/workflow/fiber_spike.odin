package workflow

import "base:runtime"
import "core:fmt"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import codec "../codec"
import proto "../core/proto"
import fiber_raw "fiber/raw"

// Fiber_Context is deliberately narrow: Workflow code receives Temporal-aware
// operations, not a general worker/client handle. This remains an experiment.
Fiber_Context :: struct {
	instance: ^Fiber_Instance,
}

Fiber_Proc :: proc(ctx: ^Fiber_Context, input: string) -> string

Fiber_Phase :: enum {
	Ready,
	Waiting_Activity,
	Waiting_Timer,
	Completed,
	Failed,
}

Fiber_Activity_Request :: struct {
	seq:           u32,
	activity_id:   string,
	activity_type: string,
	input:         []u8,
}

Fiber_Timer_Request :: struct {
	seq:      u32,
	duration: time.Duration,
}

Fiber_Instance :: struct {
	run_id:          string,
	input:           string,
	output:          string,
	activity_result: string,
	workflow:        Fiber_Proc,
	workflow_context: Fiber_Context,
	activity_request: Fiber_Activity_Request,
	timer_request:    Fiber_Timer_Request,
	next_activity_seq: u32,
	next_timer_seq:    u32,
	phase:           Fiber_Phase,
	fiber:           fiber_raw.Handle,
	arena:           virtual.Arena,
	odin_context:    runtime.Context,
	owner_allocator: runtime.Allocator,
}

Fiber_Runtime :: struct {
	workflow_type:          string,
	task_queue:             string,
	workflow:               Fiber_Proc,
	instances:              [dynamic]^Fiber_Instance,
	activations:            int,
	replay_activations:     int,
	replay_initializations: int,
	evictions:              int,
}

FIBER_STACK_SIZE :: 128 * 1024

fiber_runtime_init :: proc(
	runtime: ^Fiber_Runtime,
	workflow_type, task_queue: string,
	workflow: Fiber_Proc,
) -> Error {
	if runtime == nil || len(workflow_type) == 0 || len(task_queue) == 0 || workflow == nil {
		return error_from_string("workflow type, task queue, and workflow procedure are required")
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
	runtime.workflow = workflow
	runtime.instances = make([dynamic]^Fiber_Instance)
	return {}
}

fiber_result_error :: proc(operation: string, result: fiber_raw.Result) -> Error {
	description := fiber_raw.result_description(result)
	message, allocation_error := strings.concatenate({operation, ": ", string(description)})
	if allocation_error != nil {
		return error_from_string(operation)
	}
	defer delete(message)
	return error_from_string(message)
}

fiber_instance_destroy :: proc(instance: ^Fiber_Instance) -> Error {
	if instance == nil {
		return {}
	}
	fiber_result := fiber_raw.destroy(instance.fiber)
	if fiber_result != .Success {
		return fiber_result_error("could not destroy Workflow fiber", fiber_result)
	}
	instance.fiber = nil
	virtual.arena_destroy(&instance.arena)
	free(instance, instance.owner_allocator)
	return {}
}

fiber_runtime_close :: proc(runtime: ^Fiber_Runtime) {
	if runtime == nil {
		return
	}
	for instance in runtime.instances {
		_ = fiber_instance_destroy(instance)
	}
	delete(runtime.instances)
	delete(runtime.workflow_type)
	delete(runtime.task_queue)
	runtime^ = {}
}

find_fiber_instance :: proc(runtime: ^Fiber_Runtime, run_id: string) -> (^Fiber_Instance, bool) {
	for instance in runtime.instances {
		if instance.run_id == run_id {
			return instance, true
		}
	}
	return nil, false
}

remove_fiber_instance :: proc(runtime: ^Fiber_Runtime, run_id: string) -> Error {
	for index in 0..<len(runtime.instances) {
		if runtime.instances[index].run_id == run_id {
			instance := runtime.instances[index]
			destroy_error := fiber_instance_destroy(instance)
			if destroy_error.message != nil {
				return destroy_error
			}
			ordered_remove(&runtime.instances, index)
			return {}
		}
	}
	return {}
}

fiber_entry :: proc "c" (user_data: rawptr) {
	instance := (^Fiber_Instance)(user_data)
	context = instance.odin_context
	instance.output = instance.workflow(&instance.workflow_context, instance.input)
	if instance.phase != .Failed {
		instance.phase = .Completed
	}
}

add_fiber_instance :: proc(
	runtime: ^Fiber_Runtime,
	run_id: string,
	input_payload: ^proto.Temporal_Api_Common_V1_Payload,
) -> (^Fiber_Instance, Error) {
	if input_payload == nil {
		return nil, error_from_string("workflow initialization has no input")
	}

	instance := new(Fiber_Instance)
	instance.owner_allocator = context.allocator
	arena_error := virtual.arena_init_growing(&instance.arena)
	if arena_error != nil {
		free(instance, instance.owner_allocator)
		return nil, error_from_string("could not initialize Workflow instance arena")
	}

	instance.odin_context = context
	instance.odin_context.allocator = virtual.arena_allocator(&instance.arena)
	previous_allocator := context.allocator
	context.allocator = instance.odin_context.allocator
	owned_run_id, run_id_error := strings.clone(run_id)
	instance.run_id = owned_run_id
	payload := proto.payload_view(input_payload)
	decoded_input, decode_error := codec.decode_string(payload.data, payload.encoding)
	instance.input = decoded_input
	context.allocator = previous_allocator
	if run_id_error != nil || decode_error != .None {
		virtual.arena_destroy(&instance.arena)
		free(instance, instance.owner_allocator)
		return nil, error_from_string("could not decode Workflow input as json/plain string")
	}

	instance.workflow = runtime.workflow
	instance.workflow_context.instance = instance
	instance.phase = .Ready
	fiber_result := fiber_raw.create(
		&instance.fiber,
		fiber_entry,
		instance,
		FIBER_STACK_SIZE,
	)
	if fiber_result != .Success {
		virtual.arena_destroy(&instance.arena)
		free(instance, instance.owner_allocator)
		return nil, fiber_result_error("could not create Workflow fiber", fiber_result)
	}
	append(&runtime.instances, instance)
	return instance, {}
}

// execute_activity looks sequential to Workflow authors. Internally it records
// a deterministic request and suspends the entire Odin call stack.
execute_activity :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> string {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return ""
	}
	instance := ctx.instance
	encoded, encode_error := codec.encode_string(input)
	if encode_error != .None {
		instance.phase = .Failed
		return ""
	}
	instance.next_activity_seq += 1
	instance.activity_request = Fiber_Activity_Request{
		seq = instance.next_activity_seq,
		activity_id = fmt.aprintf("activity-%d", instance.next_activity_seq),
		activity_type = activity_type,
		input = encoded[:],
	}
	instance.phase = .Waiting_Activity
	yield_result := fiber_raw.yield(instance.fiber)
	if yield_result != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return ""
	}
	return instance.activity_result
}

// sleep schedules a durable Temporal timer and suspends the Workflow fiber.
// It never reads wall-clock time in the language runtime.
sleep :: proc(ctx: ^Fiber_Context, duration: time.Duration) {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || duration <= 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return
	}
	instance := ctx.instance
	instance.next_timer_seq += 1
	instance.timer_request = Fiber_Timer_Request{
		seq = instance.next_timer_seq,
		duration = duration,
	}
	instance.phase = .Waiting_Timer
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
	}
}

append_fiber_schedule_command :: proc(
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
	payload: ^proto.Temporal_Api_Common_V1_Payload,
	timeout: ^proto.Google_Protobuf_Duration,
	schedule: ^proto.Coresdk_Workflow_Commands_ScheduleActivity,
) {
	payload.data = instance.activity_request.input
	append(
		&payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	timeout.seconds = 20
	schedule^ = proto.Coresdk_Workflow_Commands_ScheduleActivity{
		seq = instance.activity_request.seq,
		activity_id = instance.activity_request.activity_id,
		activity_type = instance.activity_request.activity_type,
		task_queue = task_queue,
		start_to_close_timeout = timeout,
		do_not_eagerly_execute = true,
	}
	append(&schedule.arguments, payload^)
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity{
				value = schedule,
			},
		},
	)
}

append_fiber_timer_command :: proc(
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	timeout: ^proto.Google_Protobuf_Duration,
	start_timer: ^proto.Coresdk_Workflow_Commands_StartTimer,
) {
	nanoseconds := i64(instance.timer_request.duration)
	timeout.seconds = nanoseconds / i64(time.Second)
	timeout.nanos = i32(nanoseconds % i64(time.Second))
	start_timer^ = proto.Coresdk_Workflow_Commands_StartTimer{
		seq = instance.timer_request.seq,
		start_to_fire_timeout = timeout,
	}
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Timer{
				value = start_timer,
			},
		},
	)
}

append_fiber_suspension_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
	schedule_payload: ^proto.Temporal_Api_Common_V1_Payload,
	schedule_timeout: ^proto.Google_Protobuf_Duration,
	schedule: ^proto.Coresdk_Workflow_Commands_ScheduleActivity,
	timer_timeout: ^proto.Google_Protobuf_Duration,
	start_timer: ^proto.Coresdk_Workflow_Commands_StartTimer,
) -> Error {
	if len(commands^) != 0 {
		return error_from_string("fiber spike emitted more than one command in an activation")
	}
	switch instance.phase {
	case .Waiting_Activity:
		append_fiber_schedule_command(
			commands,
			instance,
			task_queue,
			schedule_payload,
			schedule_timeout,
			schedule,
		)
		result.scheduled_activity = true
		result.activity_seq = instance.activity_request.seq
	case .Waiting_Timer:
		append_fiber_timer_command(commands, instance, timer_timeout, start_timer)
		result.started_timer = true
		result.timer_seq = instance.timer_request.seq
	case .Ready, .Completed, .Failed:
		return error_from_string("Workflow fiber suspended without a supported pending operation")
	}
	result.run_id = instance.run_id
	return {}
}

append_fiber_completion_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	completion_output: ^[]u8,
	completion_payload: ^proto.Temporal_Api_Common_V1_Payload,
	complete: ^proto.Coresdk_Workflow_Commands_CompleteWorkflowExecution,
) -> Error {
	if len(commands^) != 0 {
		return error_from_string("fiber spike emitted completion after another command")
	}
	encode_error: codec.Error
	completion_output^, encode_error = codec.encode_string(instance.output)
	if encode_error != .None {
		return error_from_string("could not encode Workflow result")
	}
	completion_payload.data = completion_output^
	append(
		&completion_payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	complete.result = completion_payload
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution{
				value = complete,
			},
		},
	)
	result.completed = true
	result.run_id = instance.run_id
	return {}
}

append_fiber_progress_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
	schedule_payload: ^proto.Temporal_Api_Common_V1_Payload,
	schedule_timeout: ^proto.Google_Protobuf_Duration,
	schedule: ^proto.Coresdk_Workflow_Commands_ScheduleActivity,
	timer_timeout: ^proto.Google_Protobuf_Duration,
	start_timer: ^proto.Coresdk_Workflow_Commands_StartTimer,
	completion_output: ^[]u8,
	completion_payload: ^proto.Temporal_Api_Common_V1_Payload,
	complete: ^proto.Coresdk_Workflow_Commands_CompleteWorkflowExecution,
) -> Error {
	switch fiber_raw.status(instance.fiber) {
	case .Suspended:
		return append_fiber_suspension_command(
			result,
			commands,
			instance,
			task_queue,
			schedule_payload,
			schedule_timeout,
			schedule,
			timer_timeout,
			start_timer,
		)
	case .Dead:
		if instance.phase == .Completed {
			return append_fiber_completion_command(
				result,
				commands,
				instance,
				completion_output,
				completion_payload,
				complete,
			)
		}
	case .Normal, .Running:
	}
	return error_from_string("Workflow fiber reached an invalid state after resolution")
}

process_fiber_activation :: proc(runtime: ^Fiber_Runtime, data: []u8) -> (result: Process_Result) {
	if runtime == nil || runtime.workflow == nil {
		result.error = error_from_string("Workflow fiber runtime is not initialized")
		return
	}

	decode_arena: virtual.Arena
	arena_error := virtual.arena_init_growing(&decode_arena)
	if arena_error != nil {
		result.error = error_from_string("could not initialize Workflow decode arena")
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
	schedule_payload := proto.Temporal_Api_Common_V1_Payload{}
	defer delete(schedule_payload.metadata)
	schedule_timeout := proto.Google_Protobuf_Duration{}
	schedule := proto.Coresdk_Workflow_Commands_ScheduleActivity{}
	defer delete(schedule.arguments)
	timer_timeout := proto.Google_Protobuf_Duration{}
	start_timer := proto.Coresdk_Workflow_Commands_StartTimer{}
	completion_payload := proto.Temporal_Api_Common_V1_Payload{}
	defer delete(completion_payload.metadata)
	completion_output: []u8
	defer delete(completion_output)
	complete := proto.Coresdk_Workflow_Commands_CompleteWorkflowExecution{}
	fail := proto.Coresdk_Workflow_Commands_FailWorkflowExecution{}
	instance, instance_found := find_fiber_instance(runtime, activation.run_id)
	for job in activation.jobs {
		handled := false
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow:
			handled = true
			if variant.value == nil || variant.value.workflow_type != runtime.workflow_type || instance_found || len(variant.value.arguments) != 1 {
				process_error(&result, activation.run_id, "invalid Workflow fiber initialization")
				return
			}
			add_error: Error
			instance, add_error = add_fiber_instance(runtime, activation.run_id, &variant.value.arguments[0])
			if add_error.message != nil {
				result.error = add_error
				result.completion = encode_activation_failure(activation.run_id, string(add_error.message))
				return
			}
			instance_found = true
			if activation.is_replaying {
				runtime.replay_initializations += 1
			}
			resume_result := fiber_raw.resume(instance.fiber)
			if resume_result != .Success || fiber_raw.status(instance.fiber) != .Suspended {
				process_error(&result, activation.run_id, "Workflow did not suspend during initialization")
				return
			}
			suspension_error := append_fiber_suspension_command(
				&result,
				&commands,
				instance,
				runtime.task_queue,
				&schedule_payload,
				&schedule_timeout,
				&schedule,
				&timer_timeout,
				&start_timer,
			)
			if suspension_error.message != nil {
				result.error = suspension_error
				result.completion = encode_activation_failure(activation.run_id, string(suspension_error.message))
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity:
			handled = true
			if variant.value == nil || variant.value.result == nil || !instance_found || instance.phase != .Waiting_Activity || variant.value.seq != instance.activity_request.seq {
				process_error(&result, activation.run_id, "Activity resolution does not match suspended Workflow fiber")
				return
			}
			resolution_handled := false
			#partial switch status in variant.value.result.status {
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed:
				resolution_handled = true
				if status.value == nil || status.value.result == nil {
					process_error(&result, activation.run_id, "completed Activity has no result")
					return
				}
				payload := proto.payload_view(status.value.result)
				context.allocator = instance.odin_context.allocator
				decoded_result, decode_error := codec.decode_string(payload.data, payload.encoding)
				instance.activity_result = decoded_result
				context.allocator = previous_allocator
				if decode_error != .None {
					process_error(&result, activation.run_id, "could not decode Activity result as json/plain string")
					return
				}
				instance.phase = .Ready
				resume_result := fiber_raw.resume(instance.fiber)
				if resume_result != .Success {
					process_error(&result, activation.run_id, "could not resume Workflow fiber after Activity resolution")
					return
				}
				progress_error := append_fiber_progress_command(
					&result,
					&commands,
					instance,
					runtime.task_queue,
					&schedule_payload,
					&schedule_timeout,
					&schedule,
					&timer_timeout,
					&start_timer,
					&completion_output,
					&completion_payload,
					&complete,
				)
				if progress_error.message != nil {
					result.error = progress_error
					result.completion = encode_activation_failure(activation.run_id, string(progress_error.message))
					return
				}
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Failed:
				resolution_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "failed Activity has no failure")
					return
				}
				fail.failure = status.value.failure
				append(
					&commands,
					proto.Coresdk_Workflow_Commands_WorkflowCommand{
						variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Fail_Workflow_Execution{value = &fail},
					},
				)
				instance.phase = .Failed
				result.completed = true
				result.run_id = instance.run_id
			}
			if !resolution_handled {
				process_error(&result, activation.run_id, "Activity result is unsupported by the Workflow fiber spike")
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer:
			handled = true
			if variant.value == nil || !instance_found || instance.phase != .Waiting_Timer || variant.value.seq != instance.timer_request.seq {
				process_error(&result, activation.run_id, "timer firing does not match suspended Workflow fiber")
				return
			}
			instance.phase = .Ready
			resume_result := fiber_raw.resume(instance.fiber)
			if resume_result != .Success {
				process_error(&result, activation.run_id, "could not resume Workflow fiber after timer firing")
				return
			}
			progress_error := append_fiber_progress_command(
				&result,
				&commands,
				instance,
				runtime.task_queue,
				&schedule_payload,
				&schedule_timeout,
				&schedule,
				&timer_timeout,
				&start_timer,
				&completion_output,
				&completion_payload,
				&complete,
			)
			if progress_error.message != nil {
				result.error = progress_error
				result.completion = encode_activation_failure(activation.run_id, string(progress_error.message))
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache:
			handled = true
			if len(activation.jobs) != 1 {
				process_error(&result, activation.run_id, "eviction activation contains additional jobs")
				return
			}
			destroy_error := remove_fiber_instance(runtime, activation.run_id)
			if destroy_error.message != nil {
				result.error = destroy_error
				result.completion = encode_activation_failure(activation.run_id, string(destroy_error.message))
				return
			}
			instance = nil
			instance_found = false
			runtime.evictions += 1
			result.evicted = true
		}
		if !handled {
			process_error(&result, activation.run_id, "Workflow fiber spike received an unsupported activation job")
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

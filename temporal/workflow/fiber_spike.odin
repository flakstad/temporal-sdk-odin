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
	Waiting_Signal,
	Completed,
	Failed,
}

Fiber_Operation_State :: enum {
	Scheduled,
	Commanded,
	Resolved,
	Failed,
}

Activity_Future :: struct {
	seq: u32,
}

Activity_Result_Status :: enum {
	Succeeded,
	Failed,
}

Activity_Result :: struct {
	status:          Activity_Result_Status,
	value:           string,
	failure_message: string,
}

Fiber_Activity_Operation :: struct {
	seq:           u32,
	activity_id:   string,
	activity_type: string,
	input:         []u8,
	result:        string,
	failure_message: string,
	failure_proto:   []u8,
	state:         Fiber_Operation_State,
}

Fiber_Timer_Operation :: struct {
	seq:      u32,
	duration: time.Duration,
	state:    Fiber_Operation_State,
}

Fiber_Signal :: struct {
	name:     string,
	payload:  string,
	consumed: bool,
}

Fiber_Instance :: struct {
	run_id:          string,
	input:           string,
	output:          string,
	workflow:        Fiber_Proc,
	workflow_context: Fiber_Context,
	activities:       [dynamic]Fiber_Activity_Operation,
	timers:           [dynamic]Fiber_Timer_Operation,
	signals:          [dynamic]Fiber_Signal,
	waiting_activity_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	unhandled_activity_seq: u32,
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
	instance.activities = make([dynamic]Fiber_Activity_Operation)
	instance.timers = make([dynamic]Fiber_Timer_Operation)
	instance.signals = make([dynamic]Fiber_Signal)
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

find_activity_operation :: proc(instance: ^Fiber_Instance, seq: u32) -> (^Fiber_Activity_Operation, bool) {
	for index in 0..<len(instance.activities) {
		if instance.activities[index].seq == seq {
			return &instance.activities[index], true
		}
	}
	return nil, false
}

find_timer_operation :: proc(instance: ^Fiber_Instance, seq: u32) -> (^Fiber_Timer_Operation, bool) {
	for index in 0..<len(instance.timers) {
		if instance.timers[index].seq == seq {
			return &instance.timers[index], true
		}
	}
	return nil, false
}

find_buffered_signal :: proc(instance: ^Fiber_Instance, name: string) -> (^Fiber_Signal, bool) {
	for index in 0..<len(instance.signals) {
		signal := &instance.signals[index]
		if !signal.consumed && signal.name == name {
			return signal, true
		}
	}
	return nil, false
}

// start_activity records a deterministic Activity command without suspending.
// The handle contains only a sequence number because the operation table can move.
start_activity :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> Activity_Future {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {}
	}
	instance := ctx.instance
	encoded, encode_error := codec.encode_string(input)
	if encode_error != .None {
		instance.phase = .Failed
		return {}
	}
	instance.next_activity_seq += 1
	operation := Fiber_Activity_Operation{
		seq = instance.next_activity_seq,
		activity_id = fmt.aprintf("activity-%d", instance.next_activity_seq),
		activity_type = activity_type,
		input = encoded[:],
		state = .Scheduled,
	}
	append(&instance.activities, operation)
	return Activity_Future{seq = operation.seq}
}

activity_result_from_operation :: proc(operation: ^Fiber_Activity_Operation) -> Activity_Result {
	if operation.state == .Resolved {
		return Activity_Result{status = .Succeeded, value = operation.result}
	}
	if operation.state == .Failed {
		return Activity_Result{status = .Failed, failure_message = operation.failure_message}
	}
	return Activity_Result{status = .Failed, failure_message = "Activity has not resolved"}
}

// await_activity_result suspends the whole Odin call stack only while the
// selected operation is unresolved. Success and failure applied earlier in the
// same activation are consumed synchronously.
await_activity_result :: proc(ctx: ^Fiber_Context, future: Activity_Future) -> Activity_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return Activity_Result{status = .Failed, failure_message = "invalid Activity await"}
	}
	instance := ctx.instance
	operation, found := find_activity_operation(instance, future.seq)
	if !found {
		instance.phase = .Failed
		return Activity_Result{status = .Failed, failure_message = "Activity operation was not found"}
	}
	if operation.state == .Resolved || operation.state == .Failed {
		return activity_result_from_operation(operation)
	}
	instance.waiting_activity_seq = future.seq
	instance.phase = .Waiting_Activity
	yield_result := fiber_raw.yield(instance.fiber)
	if yield_result != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return Activity_Result{status = .Failed, failure_message = "could not resume Activity await"}
	}
	operation, found = find_activity_operation(instance, future.seq)
	if !found || (operation.state != .Resolved && operation.state != .Failed) {
		instance.phase = .Failed
		return Activity_Result{status = .Failed, failure_message = "Activity resumed without a resolution"}
	}
	return activity_result_from_operation(operation)
}

// await_activity is the convenience form for Workflows that want an unhandled
// Activity failure to fail the Workflow Execution.
await_activity :: proc(ctx: ^Fiber_Context, future: Activity_Future) -> string {
	result := await_activity_result(ctx, future)
	if result.status == .Failed {
		if ctx != nil && ctx.instance != nil {
			if future.seq != 0 {
				ctx.instance.unhandled_activity_seq = future.seq
			}
			ctx.instance.phase = .Failed
		}
		return ""
	}
	return result.value
}

// execute_activity is the sequential convenience form.
execute_activity :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> string {
	return await_activity(ctx, start_activity(ctx, activity_type, input))
}

execute_activity_result :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> Activity_Result {
	return await_activity_result(ctx, start_activity(ctx, activity_type, input))
}

// await_signal consumes the oldest buffered signal with the requested name, or
// suspends until Core delivers one. Signals with other names stay buffered.
await_signal :: proc(ctx: ^Fiber_Context, name: string) -> string {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || len(name) == 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return ""
	}
	instance := ctx.instance
	signal, found := find_buffered_signal(instance, name)
	if found {
		signal.consumed = true
		return signal.payload
	}
	waiting_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		instance.phase = .Failed
		return ""
	}
	instance.waiting_signal_name = waiting_name
	instance.phase = .Waiting_Signal
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return ""
	}
	signal, found = find_buffered_signal(instance, name)
	if !found {
		instance.phase = .Failed
		return ""
	}
	signal.consumed = true
	return signal.payload
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
	operation := Fiber_Timer_Operation{
		seq = instance.next_timer_seq,
		duration = duration,
		state = .Scheduled,
	}
	append(&instance.timers, operation)
	instance.waiting_timer_seq = operation.seq
	instance.phase = .Waiting_Timer
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
	}
}

append_fiber_schedule_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Activity_Operation,
	task_queue: string,
) {
	payload := new(proto.Temporal_Api_Common_V1_Payload)
	payload.data = operation.input
	append(
		&payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	timeout := new(proto.Google_Protobuf_Duration)
	timeout.seconds = 20
	// The bounded spike has no Activity options yet. One attempt makes failure
	// handling observable without waiting through Temporal's default retries.
	retry_policy := new(proto.Temporal_Api_Common_V1_RetryPolicy)
	retry_policy.maximum_attempts = 1
	schedule := new(proto.Coresdk_Workflow_Commands_ScheduleActivity)
	schedule^ = proto.Coresdk_Workflow_Commands_ScheduleActivity{
		seq = operation.seq,
		activity_id = operation.activity_id,
		activity_type = operation.activity_type,
		task_queue = task_queue,
		start_to_close_timeout = timeout,
		retry_policy = retry_policy,
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
	operation.state = .Commanded
	result.scheduled_activity = true
	result.scheduled_activity_count += 1
	result.activity_seq = operation.seq
}

append_fiber_timer_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Timer_Operation,
) {
	timeout := new(proto.Google_Protobuf_Duration)
	nanoseconds := i64(operation.duration)
	timeout.seconds = nanoseconds / i64(time.Second)
	timeout.nanos = i32(nanoseconds % i64(time.Second))
	start_timer := new(proto.Coresdk_Workflow_Commands_StartTimer)
	start_timer^ = proto.Coresdk_Workflow_Commands_StartTimer{
		seq = operation.seq,
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
	operation.state = .Commanded
	result.started_timer = true
	result.timer_seq = operation.seq
}

append_fiber_pending_commands :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
) -> Error {
	for index in 0..<len(instance.activities) {
		operation := &instance.activities[index]
		if operation.state == .Scheduled {
			append_fiber_schedule_command(result, commands, operation, task_queue)
		}
	}
	for index in 0..<len(instance.timers) {
		operation := &instance.timers[index]
		if operation.state == .Scheduled {
			append_fiber_timer_command(result, commands, operation)
		}
	}
	switch instance.phase {
	case .Waiting_Activity:
		result.waiting_activity_seq = instance.waiting_activity_seq
	case .Waiting_Timer:
	case .Waiting_Signal:
		result.waiting_signal = true
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
) -> Error {
	if len(commands^) != 0 {
		return error_from_string("fiber spike emitted completion after another command")
	}
	completion_output, encode_error := codec.encode_string(instance.output)
	if encode_error != .None {
		return error_from_string("could not encode Workflow result")
	}
	completion_payload := new(proto.Temporal_Api_Common_V1_Payload)
	completion_payload.data = completion_output[:]
	append(
		&completion_payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	complete := new(proto.Coresdk_Workflow_Commands_CompleteWorkflowExecution)
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

append_fiber_failure_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) -> Error {
	if len(commands^) != 0 {
		return error_from_string("fiber spike emitted failure after another command")
	}
	operation, found := find_activity_operation(instance, instance.unhandled_activity_seq)
	if !found || operation.state != .Failed || len(operation.failure_proto) == 0 {
		return error_from_string("Workflow fiber failed without a preserved Activity failure")
	}
	failure := new(proto.Temporal_Api_Failure_V1_Failure)
	if !proto.decode_Temporal_Api_Failure_V1_Failure(operation.failure_proto, failure) {
		return error_from_string("could not restore unhandled Activity failure")
	}
	fail := new(proto.Coresdk_Workflow_Commands_FailWorkflowExecution)
	fail.failure = failure
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Fail_Workflow_Execution{
				value = fail,
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
) -> Error {
	switch fiber_raw.status(instance.fiber) {
	case .Suspended:
		return append_fiber_pending_commands(result, commands, instance, task_queue)
	case .Dead:
		if instance.phase == .Completed {
			return append_fiber_completion_command(result, commands, instance)
		}
		if instance.phase == .Failed {
			return append_fiber_failure_command(result, commands, instance)
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
			if resume_result != .Success {
				process_error(&result, activation.run_id, "could not start Workflow fiber")
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity:
			handled = true
			if variant.value == nil || variant.value.result == nil || !instance_found {
				process_error(&result, activation.run_id, "Activity resolution has no Workflow fiber")
				return
			}
			operation, operation_found := find_activity_operation(instance, variant.value.seq)
			if !operation_found || operation.state != .Commanded {
				process_error(&result, activation.run_id, "Activity resolution does not match a pending Workflow operation")
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
				context.allocator = previous_allocator
				if decode_error != .None {
					process_error(&result, activation.run_id, "could not decode Activity result as json/plain string")
					return
				}
				operation.result = decoded_result
				operation.state = .Resolved
				result.resolved_activity_seq = operation.seq
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Failed:
				resolution_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "failed Activity has no failure")
					return
				}
				context.allocator = instance.odin_context.allocator
				failure_message, allocation_error := strings.clone(status.value.failure.message)
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
				context.allocator = previous_allocator
				if allocation_error != nil || len(failure_proto) == 0 {
					process_error(&result, activation.run_id, "could not preserve Activity failure")
					return
				}
				operation.failure_message = failure_message
				operation.failure_proto = failure_proto[:]
				operation.state = .Failed
				result.resolved_activity_seq = operation.seq
				result.resolved_activity_failed = true
			}
			if !resolution_handled {
				process_error(&result, activation.run_id, "Activity result is unsupported by the Workflow fiber spike")
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow:
			handled = true
			if variant.value == nil || !instance_found || len(variant.value.signal_name) == 0 || len(variant.value.input) != 1 {
				process_error(&result, activation.run_id, "Workflow signal must have a name and one payload")
				return
			}
			payload := proto.payload_view(&variant.value.input[0])
			context.allocator = instance.odin_context.allocator
			signal_name, name_error := strings.clone(variant.value.signal_name)
			signal_payload, decode_error := codec.decode_string(payload.data, payload.encoding)
			context.allocator = previous_allocator
			if name_error != nil || decode_error != .None {
				process_error(&result, activation.run_id, "could not preserve Workflow signal as json/plain string")
				return
			}
			append(
				&instance.signals,
				Fiber_Signal{name = signal_name, payload = signal_payload},
			)
			result.received_signal = true
			result.received_signal_count += 1

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "timer firing has no Workflow fiber")
				return
			}
			timer_operation, timer_found := find_timer_operation(instance, variant.value.seq)
			if !timer_found || timer_operation.state != .Commanded {
				process_error(&result, activation.run_id, "timer firing does not match a pending Workflow operation")
				return
			}
			timer_operation.state = .Resolved

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

	if instance_found {
		should_resume := false
		switch instance.phase {
		case .Waiting_Activity:
			operation, found := find_activity_operation(instance, instance.waiting_activity_seq)
			should_resume = found && (operation.state == .Resolved || operation.state == .Failed)
		case .Waiting_Timer:
			operation, found := find_timer_operation(instance, instance.waiting_timer_seq)
			should_resume = found && operation.state == .Resolved
		case .Waiting_Signal:
			_, found := find_buffered_signal(instance, instance.waiting_signal_name)
			should_resume = found
		case .Ready, .Completed, .Failed:
		}
		if should_resume {
			instance.phase = .Ready
			instance.waiting_activity_seq = 0
			instance.waiting_timer_seq = 0
			instance.waiting_signal_name = ""
			resume_result := fiber_raw.resume(instance.fiber)
			if resume_result != .Success {
				process_error(&result, activation.run_id, "could not resume Workflow fiber after applying activation jobs")
				return
			}
		}
	}

	context.allocator = virtual.arena_allocator(&decode_arena)
	commands := make([dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand)
	if instance_found {
		progress_error := append_fiber_progress_command(&result, &commands, instance, runtime.task_queue)
		if progress_error.message != nil {
			message := string(progress_error.message)
			context.allocator = previous_allocator
			result.error = error_from_string(message)
			result.completion = encode_activation_failure(activation.run_id, message)
			return
		}
	}
	context.allocator = previous_allocator

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

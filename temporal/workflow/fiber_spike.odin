package workflow

import allocator_runtime "base:runtime"
import "core:fmt"
import "core:math"
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
Payload_Proc :: #type proc(ctx: ^Fiber_Context, input: codec.Payload_View) -> Payload_Result
Workflow_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	input: codec.Payload_View,
) -> Payload_Result
Workflow_Callback_Destroy_Proc :: #type proc(callback_data: rawptr)

Payload_Result_Status :: enum {
	Completed,
	Failed,
}

Payload_Result :: struct {
	status:       Payload_Result_Status,
	payload:      codec.Payload,
	failure_message: string,
	failure_type:    string,
}

payload_complete :: proc(payload: codec.Payload) -> Payload_Result {
	return {status = .Completed, payload = payload}
}

payload_fail :: proc(
	message: string,
	failure_type := "OdinWorkflowError",
) -> Payload_Result {
	return {
		status = .Failed,
		failure_message = message,
		failure_type = failure_type,
	}
}

Activity_Cancellation_Type :: enum {
	Try_Cancel,
	Wait_Cancellation_Completed,
	Abandon,
}

Retry_Policy :: struct {
	initial_interval:          time.Duration,
	backoff_coefficient:       f64,
	maximum_interval:          time.Duration,
	maximum_attempts:          i32,
	non_retryable_error_types: []string,
}

// Activity_Options borrows its strings, slices, and retry_policy only for the
// start/execute call. The Workflow runtime copies them into the run arena before
// it can suspend. A zero duration means "not specified".
Activity_Options :: struct {
	activity_id:               string,
	task_queue:                string,
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	heartbeat_timeout:         time.Duration,
	retry_policy:              ^Retry_Policy,
	cancellation_type:         Activity_Cancellation_Type,
	disable_eager_execution:   bool,
}

Owned_Activity_Options :: struct {
	task_queue:                string,
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	heartbeat_timeout:         time.Duration,
	retry_policy:              Retry_Policy,
	has_retry_policy:          bool,
	cancellation_type:         Activity_Cancellation_Type,
	disable_eager_execution:   bool,
}

Fiber_Phase :: enum {
	Ready,
	Waiting_Activity,
	Waiting_Timer,
	Waiting_Signal,
	Waiting_Signal_Or_Timer,
	Canceled,
	Completed,
	Failed,
}

Fiber_Operation_State :: enum {
	Scheduled,
	Commanded,
	Resolved,
	Failed,
	Cancel_Requested,
	Canceled,
}

Activity_Future :: struct {
	seq: u32,
}

Activity_Result_Status :: enum {
	Succeeded,
	Failed,
}

Activity_Retry_State :: enum {
	Unspecified,
	In_Progress,
	Non_Retryable_Failure,
	Timed_Out,
	Maximum_Attempts_Reached,
	Retry_Policy_Not_Set,
	Internal_Server_Error,
	Cancel_Requested,
}

Application_Error_Category :: enum {
	Unspecified,
	Benign,
}

Application_Failure :: struct {
	present:              bool,
	message:              string,
	type:                 string,
	non_retryable:        bool,
	category:             Application_Error_Category,
	details:              []codec.Payload_View,
	next_retry_delay:     time.Duration,
	has_next_retry_delay: bool,
}

Activity_Failure :: struct {
	message:       string,
	source:        string,
	stack_trace:   string,
	activity_id:   string,
	activity_type: string,
	identity:      string,
	retry_state:   Activity_Retry_State,
	application:   Application_Failure,
}

Activity_Result :: struct {
	status:          Activity_Result_Status,
	value:           string,
	failure_message: string,
	failure:         Activity_Failure,
}

Payload_Activity_Result :: struct {
	status:          Activity_Result_Status,
	value:           codec.Payload_View,
	failure_message: string,
	failure:         Activity_Failure,
}

Json_Activity_Result :: struct(T: typeid) {
	status:          Activity_Result_Status,
	value:           T,
	failure_message: string,
	failure:         Activity_Failure,
}

Signal_Wait_Status :: enum {
	Received,
	Timed_Out,
}

Signal_Wait_Result :: struct {
	status: Signal_Wait_Status,
	value:  string,
}

Payload_Signal_Wait_Result :: struct {
	status: Signal_Wait_Status,
	value:  codec.Payload_View,
}

Json_Signal_Wait_Result :: struct(T: typeid) {
	status: Signal_Wait_Status,
	value:  T,
}

Fiber_Activity_Operation :: struct {
	seq:           u32,
	activity_id:   string,
	activity_type: string,
	options:       Owned_Activity_Options,
	input:         codec.Payload,
	result:        codec.Payload,
	failure_message: string,
	failure_proto:   []u8,
	failure:         Activity_Failure,
	failure_detail_payloads: [dynamic]codec.Payload,
	failure_detail_views:    []codec.Payload_View,
	state:         Fiber_Operation_State,
}

Fiber_Timer_Operation :: struct {
	seq:      u32,
	duration: time.Duration,
	state:    Fiber_Operation_State,
}

Fiber_Signal :: struct {
	name:     string,
	payload:  codec.Payload,
	consumed: bool,
}

Fiber_Instance :: struct {
	run_id:          string,
	input:           codec.Payload,
	output:          codec.Payload,
	callback_data:   rawptr,
	invoke:          Workflow_Invoke_Proc,
	workflow_context: Fiber_Context,
	activities:       [dynamic]Fiber_Activity_Operation,
	timers:           [dynamic]Fiber_Timer_Operation,
	signals:          [dynamic]Fiber_Signal,
	waiting_activity_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	cancellation_reason:  string,
	terminal_command_sent: bool,
	unhandled_activity_seq: u32,
	failure_message:       string,
	failure_type:          string,
	next_activity_seq: u32,
	next_timer_seq:    u32,
	phase:           Fiber_Phase,
	fiber:           fiber_raw.Handle,
	arena:           virtual.Arena,
	odin_context:    allocator_runtime.Context,
	owner_allocator: allocator_runtime.Allocator,
}

Fiber_Runtime :: struct {
	workflow_type:          string,
	task_queue:             string,
	callback_data:          rawptr,
	invoke:                 Workflow_Invoke_Proc,
	callback_destroy:       Workflow_Callback_Destroy_Proc,
	instances:              [dynamic]^Fiber_Instance,
	activations:            int,
	replay_activations:     int,
	replay_initializations: int,
	evictions:              int,
}

FIBER_STACK_SIZE :: 128 * 1024

fiber_runtime_init_invoker :: proc(
	runtime: ^Fiber_Runtime,
	workflow_type, task_queue: string,
	callback_data: rawptr,
	invoke: Workflow_Invoke_Proc,
	callback_destroy: Workflow_Callback_Destroy_Proc,
) -> Error {
	if runtime == nil || len(workflow_type) == 0 || len(task_queue) == 0 || callback_data == nil || invoke == nil || callback_destroy == nil {
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
	runtime.callback_data = callback_data
	runtime.invoke = invoke
	runtime.callback_destroy = callback_destroy
	runtime.instances = make([dynamic]^Fiber_Instance)
	return {}
}

fiber_runtime_init :: proc(
	runtime: ^Fiber_Runtime,
	workflow_type, task_queue: string,
	workflow: Fiber_Proc,
) -> Error {
	if workflow == nil {
		return error_from_string("workflow type, task queue, and workflow procedure are required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Fiber_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Workflow callback")
	}
	owned_callback^ = workflow
	init_error := fiber_runtime_init_invoker(
		runtime,
		workflow_type,
		task_queue,
		owned_callback,
		proc(data: rawptr, ctx: ^Fiber_Context, input: codec.Payload_View) -> Payload_Result {
			value: string
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow input as json/plain string",
					"OdinWorkflowPayloadConversionError",
				)
			}
			callback := cast(^Fiber_Proc)data
			output := callback^(ctx, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow result as json/plain string",
					"OdinWorkflowPayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
		proc(data: rawptr) {
			free(cast(^Fiber_Proc)data, allocator_runtime.heap_allocator())
		},
	)
	if init_error.message != nil {
		free(owned_callback, allocator)
	}
	return init_error
}

fiber_runtime_init_payload :: proc(
	runtime: ^Fiber_Runtime,
	workflow_type, task_queue: string,
	workflow: Payload_Proc,
) -> Error {
	if workflow == nil {
		return error_from_string("workflow type, task queue, and workflow procedure are required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Payload_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Workflow callback")
	}
	owned_callback^ = workflow
	init_error := fiber_runtime_init_invoker(
		runtime,
		workflow_type,
		task_queue,
		owned_callback,
		proc(data: rawptr, ctx: ^Fiber_Context, input: codec.Payload_View) -> Payload_Result {
			callback := cast(^Payload_Proc)data
			return callback^(ctx, input)
		},
		proc(data: rawptr) {
			free(cast(^Payload_Proc)data, allocator_runtime.heap_allocator())
		},
	)
	if init_error.message != nil {
		free(owned_callback, allocator)
	}
	return init_error
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
	codec.payload_free(&instance.input)
	codec.payload_free(&instance.output)
	for &operation in instance.activities {
		codec.payload_free(&operation.input)
		codec.payload_free(&operation.result)
		for &detail in operation.failure_detail_payloads {
			codec.payload_free(&detail)
		}
	}
	for &signal in instance.signals {
		codec.payload_free(&signal.payload)
	}
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
	if runtime.callback_data != nil && runtime.callback_destroy != nil {
		runtime.callback_destroy(runtime.callback_data)
	}
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

fiber_runtime_has_run :: proc(runtime: ^Fiber_Runtime, run_id: string) -> bool {
	if runtime == nil {
		return false
	}
	_, found := find_fiber_instance(runtime, run_id)
	return found
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
	workflow_result := instance.invoke(
		instance.callback_data,
		&instance.workflow_context,
		codec.payload_view(&instance.input),
	)
	if instance.phase == .Failed {
		codec.payload_free(&workflow_result.payload)
		return
	}
	if workflow_result.status == .Failed {
		instance.failure_message = workflow_result.failure_message
		instance.failure_type = workflow_result.failure_type
		instance.phase = .Failed
		return
	}
	instance.output = workflow_result.payload
	instance.phase = .Completed
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
	owned_input, clone_error := proto.clone_payload(input_payload)
	instance.input = owned_input
	instance.activities = make([dynamic]Fiber_Activity_Operation)
	instance.timers = make([dynamic]Fiber_Timer_Operation)
	instance.signals = make([dynamic]Fiber_Signal)
	context.allocator = previous_allocator
	if run_id_error != nil || clone_error != .None {
		virtual.arena_destroy(&instance.arena)
		free(instance, instance.owner_allocator)
		return nil, error_from_string("could not preserve Workflow input Payload")
	}

	instance.callback_data = runtime.callback_data
	instance.invoke = runtime.invoke
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

apply_fiber_cancellation :: proc(instance: ^Fiber_Instance, reason: string) {
	instance.cancellation_reason = reason
	for index in 0..<len(instance.activities) {
		operation := &instance.activities[index]
		switch operation.state {
		case .Scheduled:
			operation.state = .Canceled
		case .Commanded:
			operation.state = .Cancel_Requested
		case .Resolved, .Failed, .Cancel_Requested, .Canceled:
		}
	}
	for index in 0..<len(instance.timers) {
		operation := &instance.timers[index]
		switch operation.state {
		case .Scheduled:
			operation.state = .Canceled
		case .Commanded:
			operation.state = .Cancel_Requested
		case .Resolved, .Failed, .Cancel_Requested, .Canceled:
		}
	}
	instance.phase = .Canceled
}

activity_options_error :: proc(activity_type: string, options: Activity_Options) -> string {
	if len(activity_type) == 0 {
		return "Activity type must not be empty"
	}
	if options.schedule_to_close_timeout <= 0 && options.start_to_close_timeout <= 0 {
		return "Activity requires a positive Schedule-to-Close or Start-to-Close timeout"
	}
	if options.schedule_to_close_timeout < 0 ||
	   options.schedule_to_start_timeout < 0 ||
	   options.start_to_close_timeout < 0 ||
	   options.heartbeat_timeout < 0 {
		return "Activity timeouts must not be negative"
	}
	if options.schedule_to_close_timeout > 0 &&
	   options.start_to_close_timeout > options.schedule_to_close_timeout {
		return "Activity Start-to-Close timeout must not exceed Schedule-to-Close timeout"
	}
	if options.schedule_to_close_timeout > 0 &&
	   options.schedule_to_start_timeout > options.schedule_to_close_timeout {
		return "Activity Schedule-to-Start timeout must not exceed Schedule-to-Close timeout"
	}
	if options.retry_policy != nil {
		policy := options.retry_policy
		if policy.initial_interval < 0 || policy.maximum_interval < 0 {
			return "Activity Retry Policy intervals must not be negative"
		}
		if math.is_nan(policy.backoff_coefficient) || math.is_inf(policy.backoff_coefficient) {
			return "Activity Retry Policy backoff coefficient must be finite"
		}
		if policy.backoff_coefficient != 0 && policy.backoff_coefficient < 1 {
			return "Activity Retry Policy backoff coefficient must be zero or at least one"
		}
		if policy.maximum_interval > 0 &&
		   policy.initial_interval > 0 &&
		   policy.maximum_interval < policy.initial_interval {
			return "Activity Retry Policy maximum interval must not be shorter than its initial interval"
		}
		if policy.maximum_attempts < 0 {
			return "Activity Retry Policy maximum attempts must not be negative"
		}
	}
	return ""
}

mark_activity_options_failure :: proc(ctx: ^Fiber_Context, message: string) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	ctx.instance.failure_message = message
	ctx.instance.failure_type = "OdinActivityOptionsError"
	ctx.instance.phase = .Failed
}

clone_activity_options :: proc(
	instance: ^Fiber_Instance,
	options: Activity_Options,
) -> (owned: Owned_Activity_Options, ok: bool) {
	allocator := instance.odin_context.allocator
	if len(options.task_queue) > 0 {
		queue, queue_error := strings.clone(options.task_queue, allocator)
		if queue_error != nil {
			return {}, false
		}
		owned.task_queue = queue
	}
	owned.schedule_to_close_timeout = options.schedule_to_close_timeout
	owned.schedule_to_start_timeout = options.schedule_to_start_timeout
	owned.start_to_close_timeout = options.start_to_close_timeout
	owned.heartbeat_timeout = options.heartbeat_timeout
	owned.cancellation_type = options.cancellation_type
	owned.disable_eager_execution = options.disable_eager_execution
	if options.retry_policy != nil {
		owned.has_retry_policy = true
		owned.retry_policy = options.retry_policy^
		error_types := make(
			[]string,
			len(options.retry_policy.non_retryable_error_types),
			allocator,
		)
		owned.retry_policy.non_retryable_error_types = error_types
		for error_type, index in options.retry_policy.non_retryable_error_types {
			cloned, clone_error := strings.clone(error_type, allocator)
			if clone_error != nil {
				return {}, false
			}
			owned.retry_policy.non_retryable_error_types[index] = cloned
		}
	}
	return owned, true
}

start_activity_owned :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload,
	options: Activity_Options,
) -> Activity_Future {
	owned_input := input
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		codec.payload_free(&owned_input)
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {}
	}
	instance := ctx.instance
	validation_error := activity_options_error(activity_type, options)
	if len(validation_error) > 0 {
		codec.payload_free(&owned_input)
		mark_activity_options_failure(ctx, validation_error)
		return {}
	}
	owned_activity_type, type_error := strings.clone(
		activity_type,
		instance.odin_context.allocator,
	)
	if type_error != nil {
		codec.payload_free(&owned_input)
		mark_activity_options_failure(ctx, "could not preserve Activity type")
		return {}
	}
	owned_options, options_ok := clone_activity_options(instance, options)
	if !options_ok {
		codec.payload_free(&owned_input)
		mark_activity_options_failure(ctx, "could not preserve Activity options")
		return {}
	}
	instance.next_activity_seq += 1
	activity_id := options.activity_id
	if len(activity_id) == 0 {
		activity_id = fmt.aprintf("activity-%d", instance.next_activity_seq)
	} else {
		cloned_id, id_error := strings.clone(
			activity_id,
			instance.odin_context.allocator,
		)
		if id_error != nil {
			codec.payload_free(&owned_input)
			mark_activity_options_failure(ctx, "could not preserve Activity ID")
			return {}
		}
		activity_id = cloned_id
	}
	operation := Fiber_Activity_Operation{
		seq = instance.next_activity_seq,
		activity_id = activity_id,
		activity_type = owned_activity_type,
		options = owned_options,
		input = owned_input,
		state = .Scheduled,
	}
	append(&instance.activities, operation)
	return Activity_Future{seq = operation.seq}
}

// start_activity_payload records a deterministic Activity command without
// suspending. The handle contains only a sequence number because the operation
// table can move. The borrowed input is copied into the Workflow instance arena.
start_activity_payload :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
) -> Activity_Future {
	retry_policy := Retry_Policy{maximum_attempts = 1}
	return start_activity_payload_with_options(
		ctx,
		activity_type,
		input,
		{
			start_to_close_timeout = 20 * time.Second,
			retry_policy = &retry_policy,
			disable_eager_execution = true,
		},
	)
}

start_activity_payload_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
	options: Activity_Options,
) -> Activity_Future {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {}
	}
	owned_input, clone_error := codec.payload_clone(input, ctx.instance.odin_context.allocator)
	if clone_error != .None {
		ctx.instance.phase = .Failed
		ctx.instance.failure_message = "could not preserve Activity input Payload"
		ctx.instance.failure_type = "OdinWorkflowPayloadConversionError"
		return {}
	}
	return start_activity_owned(ctx, activity_type, owned_input, options)
}

start_activity_json :: proc(ctx: ^Fiber_Context, activity_type: string, input: $T) -> Activity_Future {
	retry_policy := Retry_Policy{maximum_attempts = 1}
	return start_activity_json_with_options(
		ctx,
		activity_type,
		input,
		{
			start_to_close_timeout = 20 * time.Second,
			retry_policy = &retry_policy,
			disable_eager_execution = true,
		},
	)
}

start_activity_json_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $T,
	options: Activity_Options,
) -> Activity_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		ctx.instance.phase = .Failed
		ctx.instance.failure_message = "could not encode Activity input as json/plain"
		ctx.instance.failure_type = "OdinWorkflowPayloadConversionError"
		return {}
	}
	return start_activity_owned(ctx, activity_type, payload, options)
}

start_activity :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> Activity_Future {
	return start_activity_json(ctx, activity_type, input)
}

start_activity_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
	options: Activity_Options,
) -> Activity_Future {
	return start_activity_json_with_options(ctx, activity_type, input, options)
}

activity_retry_state_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_RetryState,
) -> Activity_Retry_State {
	switch value {
	case .RETRY_STATE_IN_PROGRESS:
		return .In_Progress
	case .RETRY_STATE_NON_RETRYABLE_FAILURE:
		return .Non_Retryable_Failure
	case .RETRY_STATE_TIMEOUT:
		return .Timed_Out
	case .RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED:
		return .Maximum_Attempts_Reached
	case .RETRY_STATE_RETRY_POLICY_NOT_SET:
		return .Retry_Policy_Not_Set
	case .RETRY_STATE_INTERNAL_SERVER_ERROR:
		return .Internal_Server_Error
	case .RETRY_STATE_CANCEL_REQUESTED:
		return .Cancel_Requested
	case .RETRY_STATE_UNSPECIFIED:
		return .Unspecified
	}
	return .Unspecified
}

application_error_category_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_ApplicationErrorCategory,
) -> Application_Error_Category {
	if value == .APPLICATION_ERROR_CATEGORY_BENIGN {
		return .Benign
	}
	return .Unspecified
}

failure_duration_from_proto :: proc(value: ^proto.Google_Protobuf_Duration) -> time.Duration {
	if value == nil {
		return 0
	}
	return time.Duration(value.seconds * i64(time.Second) + i64(value.nanos))
}

clone_failure_string :: proc(value: string) -> (string, bool) {
	owned, allocation_error := strings.clone(value)
	return owned, allocation_error == nil
}

preserve_activity_failure :: proc(
	operation: ^Fiber_Activity_Operation,
	value: ^proto.Temporal_Api_Failure_V1_Failure,
) -> bool {
	if operation == nil || value == nil {
		return false
	}
	message_ok, source_ok, stack_ok: bool
	operation.failure.message, message_ok = clone_failure_string(value.message)
	operation.failure.source, source_ok = clone_failure_string(value.source)
	operation.failure.stack_trace, stack_ok = clone_failure_string(value.stack_trace)
	if !message_ok || !source_ok || !stack_ok {
		return false
	}

	current := value
	for current != nil {
		#partial switch info in current.failure_info {
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Activity_Failure_Info:
			if info.value != nil && len(operation.failure.activity_id) == 0 {
				id_ok, type_ok, identity_ok: bool
				operation.failure.activity_id, id_ok = clone_failure_string(info.value.activity_id)
				if info.value.activity_type != nil {
					operation.failure.activity_type, type_ok = clone_failure_string(info.value.activity_type.name)
				} else {
					type_ok = true
				}
				operation.failure.identity, identity_ok = clone_failure_string(info.value.identity)
				if !id_ok || !type_ok || !identity_ok {
					return false
				}
				operation.failure.retry_state = activity_retry_state_from_proto(info.value.retry_state)
			}
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
			if info.value != nil && !operation.failure.application.present {
				application := &operation.failure.application
				application.present = true
				app_message_ok, app_type_ok: bool
				application.message, app_message_ok = clone_failure_string(current.message)
				application.type, app_type_ok = clone_failure_string(info.value.type)
				if !app_message_ok || !app_type_ok {
					return false
				}
				application.non_retryable = info.value.non_retryable
				application.category = application_error_category_from_proto(info.value.category)
				if info.value.next_retry_delay != nil {
					application.has_next_retry_delay = true
					application.next_retry_delay = failure_duration_from_proto(info.value.next_retry_delay)
				}
				operation.failure_detail_payloads = make([dynamic]codec.Payload)
				if info.value.details != nil {
					for &detail in info.value.details.payloads {
						owned, clone_error := proto.clone_payload(&detail)
						if clone_error != .None {
							return false
						}
						append(&operation.failure_detail_payloads, owned)
					}
				}
				operation.failure_detail_views = make(
					[]codec.Payload_View,
					len(operation.failure_detail_payloads),
				)
				for &detail, index in operation.failure_detail_payloads {
					operation.failure_detail_views[index] = codec.payload_view(&detail)
				}
				application.details = operation.failure_detail_views
			}
		}
		current = current.cause
	}
	return true
}

activity_failure_detail_payload :: proc(
	failure: ^Activity_Failure,
	index: int,
) -> (payload: codec.Payload_View, found: bool) {
	if failure == nil || !failure.application.present || index < 0 ||
	   index >= len(failure.application.details) {
		return
	}
	return failure.application.details[index], true
}

activity_failure_detail_json :: proc(
	failure: ^Activity_Failure,
	index: int,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := activity_failure_detail_payload(failure, index)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

payload_activity_result_from_operation :: proc(operation: ^Fiber_Activity_Operation) -> Payload_Activity_Result {
	if operation.state == .Resolved {
		return {status = .Succeeded, value = codec.payload_view(&operation.result)}
	}
	if operation.state == .Failed {
		return {
			status = .Failed,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	}
	return {status = .Failed, failure_message = "Activity has not resolved"}
}

// await_activity_payload_result suspends the whole Odin call stack only while the
// selected operation is unresolved. Success and failure applied earlier in the
// same activation are consumed synchronously.
await_activity_payload_result :: proc(
	ctx: ^Fiber_Context,
	future: Activity_Future,
) -> Payload_Activity_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {status = .Failed, failure_message = "invalid Activity await"}
	}
	instance := ctx.instance
	operation, found := find_activity_operation(instance, future.seq)
	if !found {
		instance.phase = .Failed
		return {status = .Failed, failure_message = "Activity operation was not found"}
	}
	if operation.state == .Resolved || operation.state == .Failed {
		return payload_activity_result_from_operation(operation)
	}
	instance.waiting_activity_seq = future.seq
	instance.phase = .Waiting_Activity
	yield_result := fiber_raw.yield(instance.fiber)
	if yield_result != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return {status = .Failed, failure_message = "could not resume Activity await"}
	}
	operation, found = find_activity_operation(instance, future.seq)
	if !found || (operation.state != .Resolved && operation.state != .Failed) {
		instance.phase = .Failed
		return {status = .Failed, failure_message = "Activity resumed without a resolution"}
	}
	return payload_activity_result_from_operation(operation)
}

await_activity_json_result :: proc(
	ctx: ^Fiber_Context,
	future: Activity_Future,
	$T: typeid,
) -> Json_Activity_Result(T) {
	payload_result := await_activity_payload_result(ctx, future)
	if payload_result.status == .Failed {
		return {
			status = .Failed,
			failure_message = payload_result.failure_message,
			failure = payload_result.failure,
		}
	}
	value: T
	if codec.decode_json(payload_result.value, &value) != .None {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
			ctx.instance.failure_message = "could not decode Activity result as json/plain"
			ctx.instance.failure_type = "OdinWorkflowPayloadConversionError"
		}
		return {status = .Failed, failure_message = "could not decode Activity result as json/plain"}
	}
	return {status = .Succeeded, value = value}
}

await_activity_result :: proc(ctx: ^Fiber_Context, future: Activity_Future) -> Activity_Result {
	result := await_activity_json_result(ctx, future, string)
	return {
		status = result.status,
		value = result.value,
		failure_message = result.failure_message,
		failure = result.failure,
	}
}

await_activity_payload :: proc(
	ctx: ^Fiber_Context,
	future: Activity_Future,
) -> codec.Payload_View {
	result := await_activity_payload_result(ctx, future)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return {}
	}
	return result.value
}

mark_activity_failure_unhandled :: proc(ctx: ^Fiber_Context, future: Activity_Future) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	if future.seq != 0 {
		operation, found := find_activity_operation(ctx.instance, future.seq)
		if found && operation.state == .Failed {
			ctx.instance.unhandled_activity_seq = future.seq
		}
	}
	ctx.instance.phase = .Failed
}

// await_activity is the convenience form for Workflows that want an unhandled
// Activity failure to fail the Workflow Execution.
await_activity :: proc(ctx: ^Fiber_Context, future: Activity_Future) -> string {
	result := await_activity_result(ctx, future)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return ""
	}
	return result.value
}

// execute_activity is the sequential convenience form.
execute_activity :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> string {
	return await_activity(ctx, start_activity(ctx, activity_type, input))
}

execute_activity_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
	options: Activity_Options,
) -> string {
	return await_activity(
		ctx,
		start_activity_with_options(ctx, activity_type, input, options),
	)
}

execute_activity_result :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> Activity_Result {
	return await_activity_result(ctx, start_activity(ctx, activity_type, input))
}

execute_activity_result_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
	options: Activity_Options,
) -> Activity_Result {
	return await_activity_result(
		ctx,
		start_activity_with_options(ctx, activity_type, input, options),
	)
}

execute_activity_payload :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_activity_payload(ctx, activity_type, input),
	)
}

execute_activity_payload_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
	options: Activity_Options,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_activity_payload_with_options(ctx, activity_type, input, options),
	)
}

execute_activity_json_result :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
) -> Json_Activity_Result(Output) {
	return await_activity_json_result(
		ctx,
		start_activity_json(ctx, activity_type, input),
		Output,
	)
}

execute_activity_json_result_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
	options: Activity_Options,
) -> Json_Activity_Result(Output) {
	return await_activity_json_result(
		ctx,
		start_activity_json_with_options(ctx, activity_type, input, options),
		Output,
	)
}

execute_activity_json :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
) -> Output {
	future := start_activity_json(ctx, activity_type, input)
	result := await_activity_json_result(ctx, future, Output)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return {}
	}
	return result.value
}

execute_activity_json_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
	options: Activity_Options,
) -> Output {
	future := start_activity_json_with_options(ctx, activity_type, input, options)
	result := await_activity_json_result(ctx, future, Output)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return {}
	}
	return result.value
}

signal_payload_conversion_failed :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.phase = .Failed
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowPayloadConversionError"
}

// await_signal_payload consumes the oldest buffered signal with the requested
// name, or suspends until Core delivers one. Signals with other names stay
// buffered. The returned view is owned by the Workflow instance.
await_signal_payload :: proc(ctx: ^Fiber_Context, name: string) -> codec.Payload_View {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || len(name) == 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {}
	}
	instance := ctx.instance
	signal, found := find_buffered_signal(instance, name)
	if found {
		signal.consumed = true
		return codec.payload_view(&signal.payload)
	}
	waiting_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		instance.phase = .Failed
		return {}
	}
	instance.waiting_signal_name = waiting_name
	instance.phase = .Waiting_Signal
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return {}
	}
	signal, found = find_buffered_signal(instance, name)
	if !found {
		instance.phase = .Failed
		return {}
	}
	signal.consumed = true
	return codec.payload_view(&signal.payload)
}

// await_signal is the json/plain string compatibility adapter.
await_signal :: proc(ctx: ^Fiber_Context, name: string) -> string {
	payload := await_signal_payload(ctx, name)
	if ctx == nil || ctx.instance == nil || ctx.instance.phase == .Failed {
		return ""
	}
	value, decode_error := codec.decode_string(payload.data, codec.encoding(payload))
	if decode_error != .None {
		signal_payload_conversion_failed(ctx.instance, "could not decode Workflow signal as json/plain string")
		return ""
	}
	return value
}

await_signal_json :: proc(ctx: ^Fiber_Context, name: string, $T: typeid) -> T {
	payload := await_signal_payload(ctx, name)
	value: T
	if ctx == nil || ctx.instance == nil || ctx.instance.phase == .Failed {
		return value
	}
	if codec.decode_json(payload, &value) != .None {
		signal_payload_conversion_failed(ctx.instance, "could not decode Workflow signal as json/plain")
		return {}
	}
	return value
}

// await_signal_or_timeout races a named signal against a durable Temporal
// timer. Core orders signal jobs before timer jobs in the same activation, so a
// buffered matching signal wins that tie without cancelling an already-fired
// timer.
await_signal_payload_or_timeout :: proc(
	ctx: ^Fiber_Context,
	name: string,
	duration: time.Duration,
) -> Payload_Signal_Wait_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || len(name) == 0 || duration <= 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {status = .Timed_Out}
	}
	instance := ctx.instance
	signal, signal_found := find_buffered_signal(instance, name)
	if signal_found {
		signal.consumed = true
		return {status = .Received, value = codec.payload_view(&signal.payload)}
	}
	waiting_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		instance.phase = .Failed
		return {status = .Timed_Out}
	}
	instance.next_timer_seq += 1
	operation := Fiber_Timer_Operation{
		seq = instance.next_timer_seq,
		duration = duration,
		state = .Scheduled,
	}
	append(&instance.timers, operation)
	instance.waiting_signal_name = waiting_name
	instance.waiting_timer_seq = operation.seq
	instance.phase = .Waiting_Signal_Or_Timer
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return {status = .Timed_Out}
	}

	signal, signal_found = find_buffered_signal(instance, name)
	timer, timer_found := find_timer_operation(instance, operation.seq)
	if signal_found {
		signal.consumed = true
		if timer_found && timer.state == .Commanded {
			timer.state = .Cancel_Requested
		}
		return {status = .Received, value = codec.payload_view(&signal.payload)}
	}
	if timer_found && timer.state == .Resolved {
		return {status = .Timed_Out}
	}
	instance.phase = .Failed
	return {status = .Timed_Out}
}

await_signal_or_timeout :: proc(
	ctx: ^Fiber_Context,
	name: string,
	duration: time.Duration,
) -> Signal_Wait_Result {
	payload_result := await_signal_payload_or_timeout(ctx, name, duration)
	if payload_result.status == .Timed_Out {
		return {status = .Timed_Out}
	}
	value, decode_error := codec.decode_string(
		payload_result.value.data,
		codec.encoding(payload_result.value),
	)
	if decode_error != .None {
		if ctx != nil {
			signal_payload_conversion_failed(ctx.instance, "could not decode Workflow signal as json/plain string")
		}
		return {status = .Received}
	}
	return {status = .Received, value = value}
}

await_signal_json_or_timeout :: proc(
	ctx: ^Fiber_Context,
	name: string,
	duration: time.Duration,
	$T: typeid,
) -> Json_Signal_Wait_Result(T) {
	payload_result := await_signal_payload_or_timeout(ctx, name, duration)
	if payload_result.status == .Timed_Out {
		return {status = .Timed_Out}
	}
	value: T
	if codec.decode_json(payload_result.value, &value) != .None {
		if ctx != nil {
			signal_payload_conversion_failed(ctx.instance, "could not decode Workflow signal as json/plain")
		}
		return {status = .Received}
	}
	return {status = .Received, value = value}
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

duration_to_proto :: proc(duration: time.Duration) -> ^proto.Google_Protobuf_Duration {
	if duration <= 0 {
		return nil
	}
	nanoseconds := i64(duration)
	value := new(proto.Google_Protobuf_Duration)
	value.seconds = nanoseconds / i64(time.Second)
	value.nanos = i32(nanoseconds % i64(time.Second))
	return value
}

activity_cancellation_type_to_proto :: proc(
	cancellation_type: Activity_Cancellation_Type,
) -> proto.Coresdk_Workflow_Commands_ActivityCancellationType {
	switch cancellation_type {
	case .Try_Cancel:
		return .TRY_CANCEL
	case .Wait_Cancellation_Completed:
		return .WAIT_CANCELLATION_COMPLETED
	case .Abandon:
		return .ABANDON
	}
	return .TRY_CANCEL
}

retry_policy_to_proto :: proc(
	policy: ^Retry_Policy,
) -> ^proto.Temporal_Api_Common_V1_RetryPolicy {
	if policy == nil {
		return nil
	}
	result := new(proto.Temporal_Api_Common_V1_RetryPolicy)
	result.initial_interval = duration_to_proto(policy.initial_interval)
	result.backoff_coefficient = policy.backoff_coefficient
	result.maximum_interval = duration_to_proto(policy.maximum_interval)
	result.maximum_attempts = policy.maximum_attempts
	for error_type in policy.non_retryable_error_types {
		append(&result.non_retryable_error_types, error_type)
	}
	return result
}

append_fiber_schedule_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Activity_Operation,
	task_queue: string,
) {
	payload := new(proto.Temporal_Api_Common_V1_Payload)
	payload^ = proto.encode_payload(codec.payload_view(&operation.input))
	activity_task_queue := operation.options.task_queue
	if len(activity_task_queue) == 0 {
		activity_task_queue = task_queue
	}
	retry_policy: ^proto.Temporal_Api_Common_V1_RetryPolicy
	if operation.options.has_retry_policy {
		retry_policy = retry_policy_to_proto(&operation.options.retry_policy)
	}
	schedule := new(proto.Coresdk_Workflow_Commands_ScheduleActivity)
	schedule^ = proto.Coresdk_Workflow_Commands_ScheduleActivity{
		seq = operation.seq,
		activity_id = operation.activity_id,
		activity_type = operation.activity_type,
		task_queue = activity_task_queue,
		schedule_to_close_timeout = duration_to_proto(operation.options.schedule_to_close_timeout),
		schedule_to_start_timeout = duration_to_proto(operation.options.schedule_to_start_timeout),
		start_to_close_timeout = duration_to_proto(operation.options.start_to_close_timeout),
		heartbeat_timeout = duration_to_proto(operation.options.heartbeat_timeout),
		retry_policy = retry_policy,
		cancellation_type = activity_cancellation_type_to_proto(operation.options.cancellation_type),
		do_not_eagerly_execute = operation.options.disable_eager_execution,
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

append_fiber_cancel_timer_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Timer_Operation,
) {
	cancel_timer := new(proto.Coresdk_Workflow_Commands_CancelTimer)
	cancel_timer.seq = operation.seq
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Timer{
				value = cancel_timer,
			},
		},
	)
	operation.state = .Canceled
	result.canceled_timer = true
	result.canceled_timer_seq = operation.seq
}

append_fiber_timer_cancellations :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.timers) {
		operation := &instance.timers[index]
		if operation.state == .Cancel_Requested {
			append_fiber_cancel_timer_command(result, commands, operation)
		}
	}
}

append_fiber_cancel_activity_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Activity_Operation,
) {
	request := new(proto.Coresdk_Workflow_Commands_RequestCancelActivity)
	request.seq = operation.seq
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Request_Cancel_Activity{
				value = request,
			},
		},
	)
	operation.state = .Canceled
	result.requested_activity_cancellation = true
	result.requested_activity_cancellation_count += 1
	result.canceled_activity_seq = operation.seq
}

append_fiber_activity_cancellations :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.activities) {
		operation := &instance.activities[index]
		if operation.state == .Cancel_Requested {
			append_fiber_cancel_activity_command(result, commands, operation)
		}
	}
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
	append_fiber_timer_cancellations(result, commands, instance)
	switch instance.phase {
	case .Waiting_Activity:
		result.waiting_activity_seq = instance.waiting_activity_seq
	case .Waiting_Timer:
	case .Waiting_Signal:
		result.waiting_signal = true
	case .Waiting_Signal_Or_Timer:
		result.waiting_signal = true
		result.waiting_timer_seq = instance.waiting_timer_seq
	case .Ready, .Canceled, .Completed, .Failed:
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
	append_fiber_timer_cancellations(result, commands, instance)
	completion_payload := new(proto.Temporal_Api_Common_V1_Payload)
	completion_payload^ = proto.encode_payload(codec.payload_view(&instance.output))
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
	instance.terminal_command_sent = true
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
	failure := new(proto.Temporal_Api_Failure_V1_Failure)
	if instance.unhandled_activity_seq != 0 {
		operation, found := find_activity_operation(instance, instance.unhandled_activity_seq)
		if !found || operation.state != .Failed || len(operation.failure_proto) == 0 {
			return error_from_string("Workflow fiber failed without a preserved Activity failure")
		}
		if !proto.decode_Temporal_Api_Failure_V1_Failure(operation.failure_proto, failure) {
			return error_from_string("could not restore unhandled Activity failure")
		}
	} else {
		if len(instance.failure_message) == 0 {
			return error_from_string("Workflow fiber failed without failure details")
		}
		failure_type := instance.failure_type
		if len(failure_type) == 0 {
			failure_type = "OdinWorkflowError"
		}
		application_info := new(proto.Temporal_Api_Failure_V1_ApplicationFailureInfo)
		application_info.type = failure_type
		application_info.non_retryable = true
		failure^ = proto.Temporal_Api_Failure_V1_Failure{
			message = instance.failure_message,
			source = "OdinSDK",
			failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
				value = application_info,
			},
		}
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
	instance.terminal_command_sent = true
	return {}
}

append_fiber_cancellation_commands :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) -> Error {
	if len(commands^) != 0 {
		return error_from_string("fiber spike emitted cancellation after another command")
	}
	append_fiber_activity_cancellations(result, commands, instance)
	append_fiber_timer_cancellations(result, commands, instance)
	cancel := new(proto.Coresdk_Workflow_Commands_CancelWorkflowExecution)
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Workflow_Execution{
				value = cancel,
			},
		},
	)
	result.canceled_workflow = true
	result.completed = true
	result.cancellation_reason = instance.cancellation_reason
	result.run_id = instance.run_id
	instance.terminal_command_sent = true
	return {}
}

append_fiber_progress_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
) -> Error {
	if instance.terminal_command_sent {
		result.run_id = instance.run_id
		return {}
	}
	if instance.phase == .Canceled {
		return append_fiber_cancellation_commands(result, commands, instance)
	}
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
	if runtime == nil || runtime.invoke == nil || runtime.callback_data == nil {
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
				result.activation_failed = true
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
			if !operation_found {
				process_error(&result, activation.run_id, "Activity resolution does not match a pending Workflow operation")
				return
			}
			if operation.state == .Canceled && instance.terminal_command_sent {
				// Core may report the terminal cancellation of a requested Activity
				// after accepting CancelWorkflowExecution but before cache eviction.
				result.resolved_activity_seq = operation.seq
				continue
			}
			if operation.state != .Commanded && operation.state != .Cancel_Requested {
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
				context.allocator = instance.odin_context.allocator
				owned_result, clone_error := proto.clone_payload(status.value.result)
				context.allocator = previous_allocator
				if clone_error != .None {
					process_error(&result, activation.run_id, "could not preserve Activity result Payload")
					return
				}
				operation.result = owned_result
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
				failure_preserved := preserve_activity_failure(operation, status.value.failure)
				context.allocator = previous_allocator
				if allocation_error != nil || len(failure_proto) == 0 || !failure_preserved {
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
			context.allocator = instance.odin_context.allocator
			signal_name, name_error := strings.clone(variant.value.signal_name)
			signal_payload, clone_error := proto.clone_payload(&variant.value.input[0])
			context.allocator = previous_allocator
			if name_error != nil || clone_error != .None {
				process_error(&result, activation.run_id, "could not preserve Workflow signal Payload")
				return
			}
			append(
				&instance.signals,
				Fiber_Signal{name = signal_name, payload = signal_payload},
			)
			result.received_signal = true
			result.received_signal_count += 1

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow:
			handled = true
			if variant.value == nil || !instance_found || instance.phase == .Canceled || instance.phase == .Completed || instance.phase == .Failed {
				process_error(&result, activation.run_id, "Workflow cancellation has no active Workflow fiber")
				return
			}
			context.allocator = instance.odin_context.allocator
			cancellation_reason, allocation_error := strings.clone(variant.value.reason)
			context.allocator = previous_allocator
			if allocation_error != nil {
				process_error(&result, activation.run_id, "could not preserve Workflow cancellation reason")
				return
			}
			apply_fiber_cancellation(instance, cancellation_reason)
			result.received_cancellation = true
			result.cancellation_reason = cancellation_reason

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "timer firing has no Workflow fiber")
				return
			}
			timer_operation, timer_found := find_timer_operation(instance, variant.value.seq)
			if !timer_found {
				process_error(&result, activation.run_id, "timer firing does not match a pending Workflow operation")
				return
			}
			if timer_operation.state == .Canceled && instance.terminal_command_sent {
				continue
			}
			if timer_operation.state != .Commanded && timer_operation.state != .Cancel_Requested {
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
				result.activation_failed = true
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
		case .Waiting_Signal_Or_Timer:
			_, signal_found := find_buffered_signal(instance, instance.waiting_signal_name)
			timer, timer_found := find_timer_operation(instance, instance.waiting_timer_seq)
			should_resume = signal_found || (timer_found && timer.state == .Resolved)
		case .Ready, .Canceled, .Completed, .Failed:
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
			result.activation_failed = true
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

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
Query_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	input: codec.Payload_View,
) -> Payload_Result
Update_Validate_Proc :: #type proc(
	callback_data: rawptr,
	input: codec.Payload_View,
) -> Update_Validation
Update_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	input: codec.Payload_View,
) -> Payload_Result
Update_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	input: codec.Payload_View,
) -> Payload_Result
Signal_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	input: codec.Payload_View,
) -> Payload_Result
Signal_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	input: codec.Payload_View,
) -> Payload_Result

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

Update_Validation :: struct {
	accepted:     bool,
	message:      string,
	failure_type: string,
}

// Handler_Unfinished_Policy controls observability when the main Workflow
// closes while a concurrent Signal or Update routine is still running.
// Temporal cannot preserve that routine after Workflow close. The default
// mirrors the other Core-based SDKs: abandon it and report a warning through
// Process_Result.
Handler_Unfinished_Policy :: enum {
	Warn_And_Abandon,
	Abandon,
}

Update_Handler_Options :: struct {
	unfinished_policy: Handler_Unfinished_Policy,
}

Signal_Handler_Options :: struct {
	unfinished_policy: Handler_Unfinished_Policy,
}

update_accept :: proc() -> Update_Validation {
	return {accepted = true}
}

update_reject :: proc(
	message: string,
	failure_type := "OdinWorkflowUpdateRejected",
) -> Update_Validation {
	return {
		accepted = false,
		message = message,
		failure_type = failure_type,
	}
}

Activity_Cancellation_Type :: enum {
	Try_Cancel,
	Wait_Cancellation_Completed,
	Abandon,
}

// The zero value deliberately matches the safest Core SDK default: a canceled
// parent waits until the child reports that cancellation completed.
Child_Workflow_Cancellation_Type :: enum {
	Wait_Cancellation_Completed,
	Abandon,
	Try_Cancel,
	Wait_Cancellation_Requested,
}

Parent_Close_Policy :: enum {
	Unspecified,
	Terminate,
	Abandon,
	Request_Cancel,
}

Child_Workflow_Options :: struct {
	workflow_id:               string,
	task_queue:                string,
	workflow_execution_timeout: time.Duration,
	workflow_run_timeout:       time.Duration,
	workflow_task_timeout:      time.Duration,
	retry_policy:              ^Retry_Policy,
	cancellation_type:         Child_Workflow_Cancellation_Type,
	parent_close_policy:       Parent_Close_Policy,
}

Owned_Child_Workflow_Options :: struct {
	task_queue:                string,
	workflow_execution_timeout: time.Duration,
	workflow_run_timeout:       time.Duration,
	workflow_task_timeout:      time.Duration,
	retry_policy:              Retry_Policy,
	has_retry_policy:          bool,
	cancellation_type:         Child_Workflow_Cancellation_Type,
	parent_close_policy:       Parent_Close_Policy,
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
	Waiting_Child_Workflow_Start,
	Waiting_Child_Workflow,
	Waiting_External_Signal,
	Waiting_Timer,
	Waiting_Signal,
	Waiting_Signal_Or_Timer,
	Waiting_Handlers,
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

Child_Workflow_Future :: struct {
	seq: u32,
}

Child_Workflow_Signal_Status :: enum {
	Succeeded,
	Failed,
	Canceled,
}

Child_Workflow_Signal_Failure_Cause :: enum {
	Unspecified,
	Workflow_Not_Found,
	Namespace_Not_Found,
	Signal_Count_Limit_Exceeded,
}

Child_Workflow_Signal_Result :: struct {
	status:          Child_Workflow_Signal_Status,
	failure_message: string,
	cause:           Child_Workflow_Signal_Failure_Cause,
}

Child_Workflow_Start_Status :: enum {
	Succeeded,
	Failed,
	Canceled,
}

Child_Workflow_Start_Failure_Cause :: enum {
	Unspecified,
	Workflow_Already_Exists,
}

Child_Workflow_Start_Result :: struct {
	status:        Child_Workflow_Start_Status,
	run_id:        string,
	workflow_id:   string,
	workflow_type: string,
	cause:         Child_Workflow_Start_Failure_Cause,
	failure_message: string,
}

Child_Workflow_Result_Status :: enum {
	Succeeded,
	Failed,
	Canceled,
}

Failure_Timeout_Type :: enum {
	Unspecified,
	Start_To_Close,
	Schedule_To_Start,
	Schedule_To_Close,
	Heartbeat,
}

Child_Workflow_Failure :: struct {
	message:            string,
	source:             string,
	stack_trace:        string,
	namespace:          string,
	workflow_id:        string,
	run_id:             string,
	workflow_type:      string,
	initiated_event_id: i64,
	started_event_id:   i64,
	retry_state:        Activity_Retry_State,
	application:        Application_Failure,
	canceled:           bool,
	cancellation_details: []codec.Payload_View,
	timed_out:          bool,
	timeout_type:       Failure_Timeout_Type,
	terminated:         bool,
}

Payload_Child_Workflow_Result :: struct {
	status:          Child_Workflow_Result_Status,
	value:           codec.Payload_View,
	failure_message: string,
	failure:         Child_Workflow_Failure,
}

Json_Child_Workflow_Result :: struct($T: typeid) {
	status:          Child_Workflow_Result_Status,
	value:           T,
	failure_message: string,
	failure:         Child_Workflow_Failure,
}

Child_Workflow_Result :: struct {
	status:          Child_Workflow_Result_Status,
	value:           string,
	failure_message: string,
	failure:         Child_Workflow_Failure,
}

Child_Workflow_Start_State :: enum {
	Pending,
	Succeeded,
	Failed,
	Canceled,
}

Child_Workflow_Completion_State :: enum {
	Pending,
	Succeeded,
	Failed,
	Canceled,
}

Activity_Result_Status :: enum {
	Succeeded,
	Failed,
	Canceled,
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
	Canceled,
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

Cancellation_Scope :: struct {
	instance: ^Fiber_Instance,
	active:   bool,
}

Fiber_Query_Handler :: struct {
	name:          string,
	callback_data: rawptr,
	invoke:        Query_Invoke_Proc,
}

Fiber_Signal_Handler :: struct {
	name:              string,
	callback_data:     rawptr,
	invoke:            Signal_Invoke_Proc,
	invoke_routine:    Signal_Routine_Invoke_Proc,
	unfinished_policy: Handler_Unfinished_Policy,
}

Fiber_Update_Handler :: struct {
	name:          string,
	callback_data: rawptr,
	validate:      Update_Validate_Proc,
	invoke:        Update_Invoke_Proc,
	invoke_routine: Update_Routine_Invoke_Proc,
	unfinished_policy: Handler_Unfinished_Policy,
}

Pending_Query_Response :: struct {
	query_id: string,
	result:   Payload_Result,
}

Update_Response_Kind :: enum {
	Accepted,
	Rejected,
	Completed,
}

Pending_Update_Response :: struct {
	protocol_instance_id: string,
	kind:                 Update_Response_Kind,
	result:               Payload_Result,
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
	detached_from_root_cancellation: bool,
}

Fiber_Timer_Operation :: struct {
	seq:      u32,
	duration: time.Duration,
	state:    Fiber_Operation_State,
	detached_from_root_cancellation: bool,
}

Fiber_Child_Workflow_Operation :: struct {
	seq:           u32,
	workflow_id:   string,
	workflow_type: string,
	options:       Owned_Child_Workflow_Options,
	input:         codec.Payload,
	run_id:        string,
	result:        codec.Payload,
	failure_message: string,
	cancellation_reason: string,
	failure_proto:   []u8,
	failure:         Child_Workflow_Failure,
	failure_detail_payloads: [dynamic]codec.Payload,
	failure_detail_views:    []codec.Payload_View,
	cancellation_detail_payloads: [dynamic]codec.Payload,
	cancellation_detail_views:    []codec.Payload_View,
	command_state:   Fiber_Operation_State,
	start_state:     Child_Workflow_Start_State,
	completion_state: Child_Workflow_Completion_State,
	start_failure_cause: Child_Workflow_Start_Failure_Cause,
	detached_from_root_cancellation: bool,
}

Fiber_External_Signal_Operation :: struct {
	seq:                u32,
	child_workflow_seq: u32,
	child_workflow_id:  string,
	signal_name:        string,
	input:              codec.Payload,
	failure_message:    string,
	failure_proto:      []u8,
	cause:              Child_Workflow_Signal_Failure_Cause,
	state:              Fiber_Operation_State,
	detached_from_root_cancellation: bool,
}

Fiber_Signal :: struct {
	name:     string,
	payload:  codec.Payload,
	consumed: bool,
}

Fiber_Signal_Routine :: struct {
	instance:             ^Fiber_Instance,
	name:                 string,
	input:                codec.Payload,
	callback_data:        rawptr,
	invoke:               Signal_Routine_Invoke_Proc,
	ctx:                  Fiber_Context,
	result:               Payload_Result,
	waiting_activity_seq: u32,
	waiting_child_workflow_seq: u32,
	waiting_external_signal_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	detached_scope_depth: int,
	unhandled_activity_seq: u32,
	unhandled_child_workflow_seq: u32,
	failure_message:        string,
	failure_type:           string,
	phase:                  Fiber_Phase,
	fiber:                  fiber_raw.Handle,
	completion_observed:    bool,
	unfinished_policy:      Handler_Unfinished_Policy,
}

Fiber_Update_Routine :: struct {
	instance:             ^Fiber_Instance,
	protocol_instance_id: string,
	input:                codec.Payload,
	callback_data:        rawptr,
	invoke:               Update_Routine_Invoke_Proc,
	ctx:                  Fiber_Context,
	result:               Payload_Result,
	waiting_activity_seq: u32,
	waiting_child_workflow_seq: u32,
	waiting_external_signal_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	detached_scope_depth: int,
	unhandled_activity_seq: u32,
	unhandled_child_workflow_seq: u32,
	failure_message:        string,
	failure_type:           string,
	phase:                  Fiber_Phase,
	fiber:                  fiber_raw.Handle,
	response_emitted:       bool,
	unfinished_policy:      Handler_Unfinished_Policy,
}

Fiber_Instance :: struct {
	run_id:          string,
	input:           codec.Payload,
	output:          codec.Payload,
	callback_data:   rawptr,
	invoke:          Workflow_Invoke_Proc,
	workflow_context: Fiber_Context,
	activities:       [dynamic]Fiber_Activity_Operation,
	child_workflows:  [dynamic]Fiber_Child_Workflow_Operation,
	external_signals: [dynamic]Fiber_External_Signal_Operation,
	timers:           [dynamic]Fiber_Timer_Operation,
	signals:          [dynamic]Fiber_Signal,
	signal_handlers:  [dynamic]Fiber_Signal_Handler,
	signal_routines:  [dynamic]^Fiber_Signal_Routine,
	query_handlers:   [dynamic]Fiber_Query_Handler,
	update_handlers:  [dynamic]Fiber_Update_Handler,
	update_routines:  [dynamic]^Fiber_Update_Routine,
	waiting_activity_seq: u32,
	waiting_child_workflow_seq: u32,
	waiting_external_signal_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	cancellation_reason:  string,
	cancellation_requested: bool,
	detached_scope_depth:   int,
	terminal_command_sent: bool,
	unhandled_activity_seq: u32,
	unhandled_child_workflow_seq: u32,
	failure_message:       string,
	failure_type:          string,
	next_activity_seq: u32,
	next_child_workflow_seq: u32,
	next_external_signal_seq: u32,
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

Fiber_Execution_State :: struct {
	waiting_activity_seq: u32,
	waiting_child_workflow_seq: u32,
	waiting_external_signal_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	detached_scope_depth: int,
	unhandled_activity_seq: u32,
	unhandled_child_workflow_seq: u32,
	failure_message:        string,
	failure_type:           string,
	phase:                  Fiber_Phase,
	fiber:                  fiber_raw.Handle,
}

capture_instance_execution :: proc(instance: ^Fiber_Instance) -> Fiber_Execution_State {
	return {
		waiting_activity_seq = instance.waiting_activity_seq,
		waiting_child_workflow_seq = instance.waiting_child_workflow_seq,
		waiting_external_signal_seq = instance.waiting_external_signal_seq,
		waiting_timer_seq = instance.waiting_timer_seq,
		waiting_signal_name = instance.waiting_signal_name,
		detached_scope_depth = instance.detached_scope_depth,
		unhandled_activity_seq = instance.unhandled_activity_seq,
		unhandled_child_workflow_seq = instance.unhandled_child_workflow_seq,
		failure_message = instance.failure_message,
		failure_type = instance.failure_type,
		phase = instance.phase,
		fiber = instance.fiber,
	}
}

apply_instance_execution :: proc(instance: ^Fiber_Instance, state: Fiber_Execution_State) {
	instance.waiting_activity_seq = state.waiting_activity_seq
	instance.waiting_child_workflow_seq = state.waiting_child_workflow_seq
	instance.waiting_external_signal_seq = state.waiting_external_signal_seq
	instance.waiting_timer_seq = state.waiting_timer_seq
	instance.waiting_signal_name = state.waiting_signal_name
	instance.detached_scope_depth = state.detached_scope_depth
	instance.unhandled_activity_seq = state.unhandled_activity_seq
	instance.unhandled_child_workflow_seq = state.unhandled_child_workflow_seq
	instance.failure_message = state.failure_message
	instance.failure_type = state.failure_type
	instance.phase = state.phase
	instance.fiber = state.fiber
}

signal_routine_execution :: proc(routine: ^Fiber_Signal_Routine) -> Fiber_Execution_State {
	return {
		waiting_activity_seq = routine.waiting_activity_seq,
		waiting_child_workflow_seq = routine.waiting_child_workflow_seq,
		waiting_external_signal_seq = routine.waiting_external_signal_seq,
		waiting_timer_seq = routine.waiting_timer_seq,
		waiting_signal_name = routine.waiting_signal_name,
		detached_scope_depth = routine.detached_scope_depth,
		unhandled_activity_seq = routine.unhandled_activity_seq,
		unhandled_child_workflow_seq = routine.unhandled_child_workflow_seq,
		failure_message = routine.failure_message,
		failure_type = routine.failure_type,
		phase = routine.phase,
		fiber = routine.fiber,
	}
}

capture_signal_routine_execution :: proc(
	routine: ^Fiber_Signal_Routine,
	instance: ^Fiber_Instance,
) {
	routine.waiting_activity_seq = instance.waiting_activity_seq
	routine.waiting_child_workflow_seq = instance.waiting_child_workflow_seq
	routine.waiting_external_signal_seq = instance.waiting_external_signal_seq
	routine.waiting_timer_seq = instance.waiting_timer_seq
	routine.waiting_signal_name = instance.waiting_signal_name
	routine.detached_scope_depth = instance.detached_scope_depth
	routine.unhandled_activity_seq = instance.unhandled_activity_seq
	routine.unhandled_child_workflow_seq = instance.unhandled_child_workflow_seq
	routine.failure_message = instance.failure_message
	routine.failure_type = instance.failure_type
	routine.phase = instance.phase
	routine.fiber = instance.fiber
}

resume_signal_routine :: proc(routine: ^Fiber_Signal_Routine) -> Error {
	if routine == nil || routine.instance == nil || routine.fiber == nil {
		return error_from_string("Workflow Signal routine is not initialized")
	}
	instance := routine.instance
	main_execution := capture_instance_execution(instance)
	apply_instance_execution(instance, signal_routine_execution(routine))
	resume_result := fiber_raw.resume(routine.fiber)
	capture_signal_routine_execution(routine, instance)
	apply_instance_execution(instance, main_execution)
	if resume_result != .Success {
		return fiber_result_error("could not resume Workflow Signal routine", resume_result)
	}
	return {}
}

signal_routine_entry :: proc "c" (user_data: rawptr) {
	routine := cast(^Fiber_Signal_Routine)user_data
	instance := routine.instance
	context = instance.odin_context
	result := routine.invoke(
		routine.callback_data,
		&routine.ctx,
		codec.payload_view(&routine.input),
	)
	if instance.phase == .Failed {
		codec.payload_free(&result.payload)
		message := instance.failure_message
		failure_type := instance.failure_type
		if instance.unhandled_activity_seq != 0 {
			operation, found := find_activity_operation(instance, instance.unhandled_activity_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowSignalActivityFailure"
			}
		}
		if instance.unhandled_child_workflow_seq != 0 {
			operation, found := find_child_workflow_operation(instance, instance.unhandled_child_workflow_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowSignalChildWorkflowFailure"
			}
		}
		if len(message) == 0 {
			message = "Workflow Signal routine failed"
		}
		routine.result = payload_fail(message, failure_type)
		return
	}
	routine.result = result
	if result.status == .Failed {
		instance.phase = .Failed
		return
	}
	instance.phase = .Completed
}

update_routine_execution :: proc(routine: ^Fiber_Update_Routine) -> Fiber_Execution_State {
	return {
		waiting_activity_seq = routine.waiting_activity_seq,
		waiting_child_workflow_seq = routine.waiting_child_workflow_seq,
		waiting_external_signal_seq = routine.waiting_external_signal_seq,
		waiting_timer_seq = routine.waiting_timer_seq,
		waiting_signal_name = routine.waiting_signal_name,
		detached_scope_depth = routine.detached_scope_depth,
		unhandled_activity_seq = routine.unhandled_activity_seq,
		unhandled_child_workflow_seq = routine.unhandled_child_workflow_seq,
		failure_message = routine.failure_message,
		failure_type = routine.failure_type,
		phase = routine.phase,
		fiber = routine.fiber,
	}
}

capture_update_routine_execution :: proc(
	routine: ^Fiber_Update_Routine,
	instance: ^Fiber_Instance,
) {
	routine.waiting_activity_seq = instance.waiting_activity_seq
	routine.waiting_child_workflow_seq = instance.waiting_child_workflow_seq
	routine.waiting_external_signal_seq = instance.waiting_external_signal_seq
	routine.waiting_timer_seq = instance.waiting_timer_seq
	routine.waiting_signal_name = instance.waiting_signal_name
	routine.detached_scope_depth = instance.detached_scope_depth
	routine.unhandled_activity_seq = instance.unhandled_activity_seq
	routine.unhandled_child_workflow_seq = instance.unhandled_child_workflow_seq
	routine.failure_message = instance.failure_message
	routine.failure_type = instance.failure_type
	routine.phase = instance.phase
	routine.fiber = instance.fiber
}

resume_update_routine :: proc(routine: ^Fiber_Update_Routine) -> Error {
	if routine == nil || routine.instance == nil || routine.fiber == nil {
		return error_from_string("Workflow Update routine is not initialized")
	}
	instance := routine.instance
	main_execution := capture_instance_execution(instance)
	apply_instance_execution(instance, update_routine_execution(routine))
	resume_result := fiber_raw.resume(routine.fiber)
	capture_update_routine_execution(routine, instance)
	apply_instance_execution(instance, main_execution)
	if resume_result != .Success {
		return fiber_result_error("could not resume Workflow Update routine", resume_result)
	}
	return {}
}

update_routine_entry :: proc "c" (user_data: rawptr) {
	routine := cast(^Fiber_Update_Routine)user_data
	instance := routine.instance
	context = instance.odin_context
	result := routine.invoke(
		routine.callback_data,
		&routine.ctx,
		codec.payload_view(&routine.input),
	)
	if instance.phase == .Failed {
		codec.payload_free(&result.payload)
		message := instance.failure_message
		failure_type := instance.failure_type
		if instance.unhandled_activity_seq != 0 {
			operation, found := find_activity_operation(instance, instance.unhandled_activity_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowUpdateActivityFailure"
			}
		}
		if instance.unhandled_child_workflow_seq != 0 {
			operation, found := find_child_workflow_operation(instance, instance.unhandled_child_workflow_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowUpdateChildWorkflowFailure"
			}
		}
		if len(message) == 0 {
			message = "Workflow Update routine failed"
		}
		routine.result = payload_fail(message, failure_type)
		return
	}
	routine.result = result
	if result.status == .Failed {
		instance.phase = .Failed
		return
	}
	instance.phase = .Completed
}

fiber_instance_destroy :: proc(instance: ^Fiber_Instance) -> Error {
	if instance == nil {
		return {}
	}
	for routine in instance.signal_routines {
		if routine.fiber == nil {
			continue
		}
		routine_result := fiber_raw.destroy(routine.fiber)
		if routine_result != .Success {
			return fiber_result_error("could not destroy Workflow Signal routine", routine_result)
		}
		routine.fiber = nil
	}
	for routine in instance.update_routines {
		if routine.fiber == nil {
			continue
		}
		routine_result := fiber_raw.destroy(routine.fiber)
		if routine_result != .Success {
			return fiber_result_error("could not destroy Workflow Update routine", routine_result)
		}
		routine.fiber = nil
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
	for &operation in instance.child_workflows {
		codec.payload_free(&operation.input)
		codec.payload_free(&operation.result)
		for &detail in operation.failure_detail_payloads {
			codec.payload_free(&detail)
		}
		for &detail in operation.cancellation_detail_payloads {
			codec.payload_free(&detail)
		}
	}
	for &operation in instance.external_signals {
		codec.payload_free(&operation.input)
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
	if instance.cancellation_requested {
		codec.payload_free(&workflow_result.payload)
		instance.phase = .Canceled
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
	instance.child_workflows = make([dynamic]Fiber_Child_Workflow_Operation)
	instance.external_signals = make([dynamic]Fiber_External_Signal_Operation)
	instance.timers = make([dynamic]Fiber_Timer_Operation)
	instance.signals = make([dynamic]Fiber_Signal)
	instance.signal_handlers = make([dynamic]Fiber_Signal_Handler)
	instance.signal_routines = make([dynamic]^Fiber_Signal_Routine)
	instance.query_handlers = make([dynamic]Fiber_Query_Handler)
	instance.update_handlers = make([dynamic]Fiber_Update_Handler)
	instance.update_routines = make([dynamic]^Fiber_Update_Routine)
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

find_child_workflow_operation :: proc(
	instance: ^Fiber_Instance,
	seq: u32,
) -> (^Fiber_Child_Workflow_Operation, bool) {
	for index in 0..<len(instance.child_workflows) {
		if instance.child_workflows[index].seq == seq {
			return &instance.child_workflows[index], true
		}
	}
	return nil, false
}

find_external_signal_operation :: proc(
	instance: ^Fiber_Instance,
	seq: u32,
) -> (^Fiber_External_Signal_Operation, bool) {
	for index in 0..<len(instance.external_signals) {
		if instance.external_signals[index].seq == seq {
			return &instance.external_signals[index], true
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

find_query_handler :: proc(
	instance: ^Fiber_Instance,
	name: string,
) -> (^Fiber_Query_Handler, bool) {
	if instance == nil {
		return nil, false
	}
	for index in 0..<len(instance.query_handlers) {
		handler := &instance.query_handlers[index]
		if handler.name == name {
			return handler, true
		}
	}
	return nil, false
}

query_registration_failed :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowQueryRegistrationError"
	instance.phase = .Failed
}

set_query_handler_payload :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Query_Invoke_Proc,
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || callback == nil {
		if ctx != nil {
			query_registration_failed(ctx.instance, "invalid Workflow query handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_query_handler(instance, name)
	if duplicate {
		query_registration_failed(instance, "Workflow query handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		query_registration_failed(instance, "could not preserve Workflow query handler name")
		return false
	}
	append(
		&instance.query_handlers,
		Fiber_Query_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke = callback,
		},
	)
	return true
}

// set_query_handler_json registers a synchronous, read-only query view over
// Workflow-owned state. The callback receives a top-level copy of State and no
// Fiber_Context, so it cannot schedule Temporal commands through the normal API.
set_query_handler_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(state: State, input: $Input) -> $Output,
) -> bool {
	Callback :: #type proc(state: State, input: Input) -> Output
	Data :: struct {
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			query_registration_failed(ctx.instance, "invalid typed Workflow query handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		query_registration_failed(ctx.instance, "could not allocate typed Workflow query handler")
		return false
	}
	data.state = state
	data.callback = callback
	return set_query_handler_payload(
		ctx,
		name,
		data,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow query input as json/plain",
					"OdinWorkflowQueryPayloadConversionError",
				)
			}
			output := owned.callback(owned.state^, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow query result as json/plain",
					"OdinWorkflowQueryPayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
	)
}

find_signal_handler :: proc(
	instance: ^Fiber_Instance,
	name: string,
) -> (^Fiber_Signal_Handler, bool) {
	if instance == nil {
		return nil, false
	}
	for index in 0..<len(instance.signal_handlers) {
		handler := &instance.signal_handlers[index]
		if handler.name == name {
			return handler, true
		}
	}
	return nil, false
}

signal_registration_failed :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowSignalRegistrationError"
	instance.phase = .Failed
}

set_signal_handler_payload :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Signal_Invoke_Proc,
	routine_callback: Signal_Routine_Invoke_Proc = nil,
	options := Signal_Handler_Options{},
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || (callback == nil) == (routine_callback == nil) {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid Workflow Signal handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_signal_handler(instance, name)
	if duplicate {
		signal_registration_failed(instance, "Workflow Signal handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(instance, "could not preserve Workflow Signal handler name")
		return false
	}
	append(
		&instance.signal_handlers,
		Fiber_Signal_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke = callback,
			invoke_routine = routine_callback,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return true
}

// set_signal_handler_json registers an immediate deterministic Signal handler.
// It may mutate Workflow-owned state but cannot suspend or schedule operations.
set_signal_handler_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(state: ^State, input: $Input),
	options := Signal_Handler_Options{},
) -> bool {
	Callback :: #type proc(state: ^State, input: Input)
	Data :: struct {
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid typed Workflow Signal handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(ctx.instance, "could not allocate typed Workflow Signal handler")
		return false
	}
	data.state = state
	data.callback = callback
	return set_signal_handler_payload(
		ctx,
		name,
		data,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Signal input as json/plain",
					"OdinWorkflowSignalPayloadConversionError",
				)
			}
			owned.callback(owned.state, value)
			return payload_complete({})
		},
		nil,
		options,
	)
}

// set_signal_routine_json gives each Signal invocation an independent stackful
// routine that may await Activities, timers, or unhandled buffered signals.
set_signal_routine_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(ctx: ^Fiber_Context, state: ^State, input: $Input),
	options := Signal_Handler_Options{},
) -> bool {
	Callback :: #type proc(ctx: ^Fiber_Context, state: ^State, input: Input)
	Data :: struct {
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid typed Workflow Signal routine")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(ctx.instance, "could not allocate typed Workflow Signal routine")
		return false
	}
	data.state = state
	data.callback = callback
	return set_signal_handler_payload(
		ctx,
		name,
		data,
		nil,
		proc(
			callback_data: rawptr,
			routine_ctx: ^Fiber_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Signal input as json/plain",
					"OdinWorkflowSignalPayloadConversionError",
				)
			}
			owned.callback(routine_ctx, owned.state, value)
			return payload_complete({})
		},
		options,
	)
}

find_update_handler :: proc(
	instance: ^Fiber_Instance,
	name: string,
) -> (^Fiber_Update_Handler, bool) {
	if instance == nil {
		return nil, false
	}
	for index in 0..<len(instance.update_handlers) {
		handler := &instance.update_handlers[index]
		if handler.name == name {
			return handler, true
		}
	}
	return nil, false
}

update_registration_failed :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowUpdateRegistrationError"
	instance.phase = .Failed
}

set_update_handler_payload :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	validator: Update_Validate_Proc,
	callback: Update_Invoke_Proc,
	routine_callback: Update_Routine_Invoke_Proc = nil,
	options := Update_Handler_Options{},
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || (callback == nil) == (routine_callback == nil) {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid Workflow Update handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_update_handler(instance, name)
	if duplicate {
		update_registration_failed(instance, "Workflow Update handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(instance, "could not preserve Workflow Update handler name")
		return false
	}
	append(
		&instance.update_handlers,
		Fiber_Update_Handler{
			name = owned_name,
			callback_data = callback_data,
			validate = validator,
			invoke = callback,
			invoke_routine = routine_callback,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return true
}

// set_validated_update_routine_json registers an Update handler on its own
// stackful routine. Unlike the synchronous form, the handler receives a
// Fiber_Context and may await Activities, timers, or signals.
set_validated_update_routine_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	validator: proc(state: State, input: $Input) -> Update_Validation,
	callback: proc(ctx: ^Fiber_Context, state: ^State, input: Input) -> $Output,
	options := Update_Handler_Options{},
) -> bool {
	Validator :: #type proc(state: State, input: Input) -> Update_Validation
	Callback :: #type proc(ctx: ^Fiber_Context, state: ^State, input: Input) -> Output
	Data :: struct {
		instance:  ^Fiber_Instance,
		state:     ^State,
		validator: Validator,
		callback:  Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || validator == nil || callback == nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid validated Workflow Update routine")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(ctx.instance, "could not allocate typed Workflow Update routine")
		return false
	}
	data.instance = ctx.instance
	data.state = state
	data.validator = validator
	data.callback = callback
	return set_update_handler_payload(
		ctx,
		name,
		data,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Update_Validation {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return update_reject(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return owned.validator(owned.state^, value)
		},
		nil,
		proc(
			callback_data: rawptr,
			routine_ctx: ^Fiber_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			output := owned.callback(routine_ctx, owned.state, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow Update result as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
		options,
	)
}

set_update_routine_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(ctx: ^Fiber_Context, state: ^State, input: $Input) -> $Output,
	options := Update_Handler_Options{},
) -> bool {
	Callback :: #type proc(ctx: ^Fiber_Context, state: ^State, input: Input) -> Output
	Data :: struct {
		instance: ^Fiber_Instance,
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid Workflow Update routine")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(ctx.instance, "could not allocate typed Workflow Update routine")
		return false
	}
	data.instance = ctx.instance
	data.state = state
	data.callback = callback
	return set_update_handler_payload(
		ctx,
		name,
		data,
		nil,
		nil,
		proc(
			callback_data: rawptr,
			routine_ctx: ^Fiber_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			output := owned.callback(routine_ctx, owned.state, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow Update result as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
		options,
	)
}

// set_update_handler_json registers a synchronous state-mutating Update. The
// handler cannot suspend and receives no Fiber_Context in this first slice.
set_update_handler_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(state: ^State, input: $Input) -> $Output,
) -> bool {
	Callback :: #type proc(state: ^State, input: Input) -> Output
	Data :: struct {
		instance: ^Fiber_Instance,
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid typed Workflow Update handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(ctx.instance, "could not allocate typed Workflow Update handler")
		return false
	}
	data.instance = ctx.instance
	data.state = state
	data.callback = callback
	return set_update_handler_payload(
		ctx,
		name,
		data,
		nil,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Payload_Result {
			owned := cast(^Data)callback_data
			previous_allocator := context.allocator
			context.allocator = owned.instance.odin_context.allocator
			defer { context.allocator = previous_allocator }
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			output := owned.callback(owned.state, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow Update result as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
	)
}

// set_validated_update_handler_json runs a read-only validator on non-replay
// delivery, then invokes the synchronous state-mutating handler after acceptance.
set_validated_update_handler_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	validator: proc(state: State, input: $Input) -> Update_Validation,
	callback: proc(state: ^State, input: Input) -> $Output,
) -> bool {
	Validator :: #type proc(state: State, input: Input) -> Update_Validation
	Callback :: #type proc(state: ^State, input: Input) -> Output
	Data :: struct {
		instance:  ^Fiber_Instance,
		state:     ^State,
		validator: Validator,
		callback:  Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || validator == nil || callback == nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid validated Workflow Update handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(ctx.instance, "could not allocate validated Workflow Update handler")
		return false
	}
	data.instance = ctx.instance
	data.state = state
	data.validator = validator
	data.callback = callback
	return set_update_handler_payload(
		ctx,
		name,
		data,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Update_Validation {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return update_reject(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return owned.validator(owned.state^, value)
		},
		proc(callback_data: rawptr, input: codec.Payload_View) -> Payload_Result {
			owned := cast(^Data)callback_data
			previous_allocator := context.allocator
			context.allocator = owned.instance.odin_context.allocator
			defer { context.allocator = previous_allocator }
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			output := owned.callback(owned.state, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow Update result as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
	)
}

apply_fiber_cancellation :: proc(instance: ^Fiber_Instance, reason: string) {
	instance.cancellation_reason = reason
	instance.cancellation_requested = true
	for index in 0..<len(instance.activities) {
		operation := &instance.activities[index]
		if operation.detached_from_root_cancellation {
			continue
		}
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
		if operation.detached_from_root_cancellation {
			continue
		}
		switch operation.state {
		case .Scheduled:
			operation.state = .Canceled
		case .Commanded:
			operation.state = .Cancel_Requested
		case .Resolved, .Failed, .Cancel_Requested, .Canceled:
		}
	}
	for index in 0..<len(instance.child_workflows) {
		operation := &instance.child_workflows[index]
		if operation.detached_from_root_cancellation {
			continue
		}
		switch operation.command_state {
		case .Scheduled:
			operation.command_state = .Canceled
			operation.start_state = .Canceled
			operation.completion_state = .Canceled
			operation.cancellation_reason = reason
			operation.failure_message = reason
		case .Commanded:
			operation.cancellation_reason = reason
			switch operation.options.cancellation_type {
			case .Abandon:
				operation.command_state = .Resolved
				operation.start_state = .Canceled
				operation.completion_state = .Canceled
				operation.failure_message = reason
			case .Try_Cancel:
				operation.command_state = .Cancel_Requested
				operation.start_state = .Canceled
				operation.completion_state = .Canceled
				operation.failure_message = reason
			case .Wait_Cancellation_Completed, .Wait_Cancellation_Requested:
				operation.command_state = .Cancel_Requested
			}
		case .Resolved, .Failed, .Cancel_Requested, .Canceled:
		}
	}
	for index in 0..<len(instance.external_signals) {
		operation := &instance.external_signals[index]
		if operation.detached_from_root_cancellation {
			continue
		}
		switch operation.state {
		case .Scheduled:
			operation.state = .Canceled
			operation.failure_message = reason
		case .Commanded:
			operation.state = .Cancel_Requested
			operation.failure_message = reason
		case .Resolved, .Failed, .Cancel_Requested, .Canceled:
		}
	}
}

is_cancellation_requested :: proc(ctx: ^Fiber_Context) -> bool {
	return ctx != nil && ctx.instance != nil && ctx.instance.cancellation_requested
}

cancellation_reason :: proc(ctx: ^Fiber_Context) -> string {
	if ctx == nil || ctx.instance == nil || !ctx.instance.cancellation_requested {
		return ""
	}
	return ctx.instance.cancellation_reason
}

// detached_cancellation_scope prevents operations created inside the scope from
// inheriting root Workflow cancellation. It may be opened before cancellation
// for work that must finish independently, or afterward for deterministic
// cleanup. The scope is local to the currently executing main/handler fiber.
detached_cancellation_scope :: proc(ctx: ^Fiber_Context) -> Cancellation_Scope {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		return {}
	}
	ctx.instance.detached_scope_depth += 1
	return {instance = ctx.instance, active = true}
}

cancellation_scope_end :: proc(scope: ^Cancellation_Scope) {
	if scope == nil || !scope.active || scope.instance == nil {
		return
	}
	if scope.instance.detached_scope_depth > 0 {
		scope.instance.detached_scope_depth -= 1
	}
	scope.active = false
	scope.instance = nil
}

root_cancellation_applies :: proc(instance: ^Fiber_Instance) -> bool {
	return instance != nil && instance.cancellation_requested && instance.detached_scope_depth == 0
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
	if root_cancellation_applies(instance) {
		codec.payload_free(&owned_input)
		instance.next_activity_seq += 1
		append(
			&instance.activities,
			Fiber_Activity_Operation{
				seq = instance.next_activity_seq,
				state = .Canceled,
			},
		)
		return {seq = instance.next_activity_seq}
	}
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
		detached_from_root_cancellation = instance.detached_scope_depth > 0,
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

child_workflow_options_error :: proc(
	workflow_type: string,
	options: Child_Workflow_Options,
) -> string {
	if len(workflow_type) == 0 {
		return "Child Workflow type must not be empty"
	}
	if len(options.workflow_id) == 0 {
		return "Child Workflow ID must not be empty"
	}
	if options.workflow_execution_timeout < 0 || options.workflow_run_timeout < 0 ||
	   options.workflow_task_timeout < 0 {
		return "Child Workflow timeouts must not be negative"
	}
	if options.workflow_execution_timeout > 0 && options.workflow_run_timeout > options.workflow_execution_timeout {
		return "Child Workflow Run timeout must not exceed Execution timeout"
	}
	if options.retry_policy != nil {
		policy := options.retry_policy
		if policy.initial_interval < 0 || policy.maximum_interval < 0 || policy.maximum_attempts < 0 {
			return "Child Workflow Retry Policy values must not be negative"
		}
		if math.is_nan(policy.backoff_coefficient) || math.is_inf(policy.backoff_coefficient) ||
		   (policy.backoff_coefficient != 0 && policy.backoff_coefficient < 1) {
			return "Child Workflow Retry Policy backoff coefficient must be zero or at least one"
		}
	}
	return ""
}

clone_child_workflow_options :: proc(
	instance: ^Fiber_Instance,
	options: Child_Workflow_Options,
) -> (owned: Owned_Child_Workflow_Options, ok: bool) {
	allocator := instance.odin_context.allocator
	if len(options.task_queue) > 0 {
		owned.task_queue, _ = strings.clone(options.task_queue, allocator)
		if owned.task_queue == "" {
			return {}, false
		}
	}
	owned.workflow_execution_timeout = options.workflow_execution_timeout
	owned.workflow_run_timeout = options.workflow_run_timeout
	owned.workflow_task_timeout = options.workflow_task_timeout
	owned.cancellation_type = options.cancellation_type
	owned.parent_close_policy = options.parent_close_policy
	if options.retry_policy != nil {
		owned.has_retry_policy = true
		owned.retry_policy = options.retry_policy^
		owned.retry_policy.non_retryable_error_types = make(
			[]string,
			len(options.retry_policy.non_retryable_error_types),
			allocator,
		)
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

mark_child_workflow_failure :: proc(ctx: ^Fiber_Context, message, failure_type: string) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	ctx.instance.failure_message = message
	ctx.instance.failure_type = failure_type
	ctx.instance.phase = .Failed
}

start_child_workflow_owned :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	input: codec.Payload,
	options: Child_Workflow_Options,
) -> Child_Workflow_Future {
	owned_input := input
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		codec.payload_free(&owned_input)
		return {}
	}
	instance := ctx.instance
	validation_error := child_workflow_options_error(workflow_type, options)
	if len(validation_error) > 0 {
		codec.payload_free(&owned_input)
		mark_child_workflow_failure(ctx, validation_error, "OdinChildWorkflowOptionsError")
		return {}
	}
	owned_type, type_error := strings.clone(workflow_type, instance.odin_context.allocator)
	owned_id, id_error := strings.clone(options.workflow_id, instance.odin_context.allocator)
	owned_options, options_ok := clone_child_workflow_options(instance, options)
	if type_error != nil || id_error != nil || !options_ok {
		codec.payload_free(&owned_input)
		mark_child_workflow_failure(ctx, "could not preserve Child Workflow options", "OdinChildWorkflowOptionsError")
		return {}
	}
	instance.next_child_workflow_seq += 1
	operation := Fiber_Child_Workflow_Operation{
		seq = instance.next_child_workflow_seq,
		workflow_id = owned_id,
		workflow_type = owned_type,
		options = owned_options,
		input = owned_input,
		command_state = .Scheduled,
		start_state = .Pending,
		completion_state = .Pending,
		detached_from_root_cancellation = instance.detached_scope_depth > 0,
	}
	append(&instance.child_workflows, operation)
	return {seq = operation.seq}
}

start_child_workflow_payload :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	input: codec.Payload_View,
	options: Child_Workflow_Options,
) -> Child_Workflow_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	owned_input, clone_error := codec.payload_clone(input, ctx.instance.odin_context.allocator)
	if clone_error != .None {
		mark_child_workflow_failure(ctx, "could not preserve Child Workflow input Payload", "OdinWorkflowPayloadConversionError")
		return {}
	}
	return start_child_workflow_owned(ctx, workflow_type, owned_input, options)
}

start_child_workflow_json :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	input: $T,
	options: Child_Workflow_Options,
) -> Child_Workflow_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		mark_child_workflow_failure(ctx, "could not encode Child Workflow input as json/plain", "OdinWorkflowPayloadConversionError")
		return {}
	}
	return start_child_workflow_owned(ctx, workflow_type, payload, options)
}

child_workflow_start_result_from_operation :: proc(
	operation: ^Fiber_Child_Workflow_Operation,
) -> Child_Workflow_Start_Result {
	switch operation.start_state {
	case .Succeeded:
		return {status = .Succeeded, run_id = operation.run_id}
	case .Failed:
		return {
			status = .Failed,
			workflow_id = operation.workflow_id,
			workflow_type = operation.workflow_type,
			cause = operation.start_failure_cause,
			failure_message = operation.failure_message,
		}
	case .Canceled:
		return {status = .Canceled, failure_message = operation.failure_message}
	case .Pending:
	}
	return {status = .Failed, failure_message = "Child Workflow start has not resolved"}
}

await_child_workflow_start :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
) -> Child_Workflow_Start_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		return {status = .Failed, failure_message = "invalid Child Workflow start await"}
	}
	instance := ctx.instance
	operation, found := find_child_workflow_operation(instance, future.seq)
	if !found {
		mark_child_workflow_failure(ctx, "Child Workflow operation was not found", "OdinChildWorkflowRuntimeError")
		return {status = .Failed, failure_message = "Child Workflow operation was not found"}
	}
	if operation.start_state == .Pending {
		instance.waiting_child_workflow_seq = future.seq
		instance.phase = .Waiting_Child_Workflow_Start
		if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
			mark_child_workflow_failure(ctx, "could not resume Child Workflow start await", "OdinChildWorkflowRuntimeError")
			return {status = .Failed, failure_message = "could not resume Child Workflow start await"}
		}
		operation, found = find_child_workflow_operation(instance, future.seq)
	}
	if !found || operation.start_state == .Pending {
		mark_child_workflow_failure(ctx, "Child Workflow start resumed without a resolution", "OdinChildWorkflowRuntimeError")
		return {status = .Failed, failure_message = "Child Workflow start resumed without a resolution"}
	}
	return child_workflow_start_result_from_operation(operation)
}

payload_child_workflow_result_from_operation :: proc(
	operation: ^Fiber_Child_Workflow_Operation,
) -> Payload_Child_Workflow_Result {
	switch operation.completion_state {
	case .Succeeded:
		return {status = .Succeeded, value = codec.payload_view(&operation.result)}
	case .Failed:
		return {
			status = .Failed,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	case .Canceled:
		return {
			status = .Canceled,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	case .Pending:
	}
	return {status = .Failed, failure_message = "Child Workflow has not completed"}
}

await_child_workflow_payload_result :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
) -> Payload_Child_Workflow_Result {
	start_result := await_child_workflow_start(ctx, future)
	if start_result.status == .Failed {
		return {status = .Failed, failure_message = start_result.failure_message}
	}
	if start_result.status == .Canceled {
		return {status = .Canceled, failure_message = start_result.failure_message}
	}
	instance := ctx.instance
	operation, found := find_child_workflow_operation(instance, future.seq)
	if !found {
		return {status = .Failed, failure_message = "Child Workflow operation was not found"}
	}
	if operation.completion_state == .Pending {
		instance.waiting_child_workflow_seq = future.seq
		instance.phase = .Waiting_Child_Workflow
		if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
			mark_child_workflow_failure(ctx, "could not resume Child Workflow completion await", "OdinChildWorkflowRuntimeError")
			return {status = .Failed, failure_message = "could not resume Child Workflow completion await"}
		}
		operation, found = find_child_workflow_operation(instance, future.seq)
	}
	if !found || operation.completion_state == .Pending {
		mark_child_workflow_failure(ctx, "Child Workflow resumed without a completion", "OdinChildWorkflowRuntimeError")
		return {status = .Failed, failure_message = "Child Workflow resumed without a completion"}
	}
	return payload_child_workflow_result_from_operation(operation)
}

await_child_workflow_json_result :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	$T: typeid,
) -> Json_Child_Workflow_Result(T) {
	result := await_child_workflow_payload_result(ctx, future)
	if result.status != .Succeeded {
		return {
			status = result.status,
			failure_message = result.failure_message,
			failure = result.failure,
		}
	}
	value: T
	if codec.decode_json(result.value, &value) != .None {
		mark_child_workflow_failure(ctx, "could not decode Child Workflow result as json/plain", "OdinWorkflowPayloadConversionError")
		return {status = .Failed, failure_message = "could not decode Child Workflow result as json/plain"}
	}
	return {status = .Succeeded, value = value}
}

await_child_workflow_result :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
) -> Child_Workflow_Result {
	result := await_child_workflow_json_result(ctx, future, string)
	return {
		status = result.status,
		value = result.value,
		failure_message = result.failure_message,
		failure = result.failure,
	}
}

execute_child_workflow_json_result :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	input: $Input,
	options: Child_Workflow_Options,
	$Output: typeid,
) -> Json_Child_Workflow_Result(Output) {
	return await_child_workflow_json_result(
		ctx,
		start_child_workflow_json(ctx, workflow_type, input, options),
		Output,
	)
}

execute_child_workflow :: proc(
	ctx: ^Fiber_Context,
	workflow_type, input: string,
	options: Child_Workflow_Options,
) -> string {
	future := start_child_workflow_json(ctx, workflow_type, input, options)
	result := await_child_workflow_json_result(ctx, future, string)
	if result.status == .Failed ||
	   (result.status == .Canceled && ctx != nil && !root_cancellation_applies(ctx.instance)) {
		operation, found := find_child_workflow_operation(ctx.instance, future.seq)
		if found && len(operation.failure_proto) > 0 {
			ctx.instance.unhandled_child_workflow_seq = future.seq
			ctx.instance.phase = .Failed
		} else {
			mark_child_workflow_failure(ctx, result.failure_message, "OdinChildWorkflowExecutionError")
		}
		return ""
	}
	return result.value
}

// cancel_child_workflow suppresses a start command that has not been emitted,
// or requests cancellation through Core using the cancellation type selected
// when the child was started. The Future remains valid and can be awaited for
// the resulting terminal state.
cancel_child_workflow :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	reason := "Child Workflow cancellation requested",
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		return false
	}
	instance := ctx.instance
	operation, found := find_child_workflow_operation(instance, future.seq)
	if !found || operation.completion_state != .Pending {
		return false
	}
	if operation.command_state == .Cancel_Requested {
		return true
	}
	if operation.command_state != .Scheduled && operation.command_state != .Commanded {
		return false
	}
	owned_reason, allocation_error := strings.clone(reason, instance.odin_context.allocator)
	if allocation_error != nil {
		mark_child_workflow_failure(
			ctx,
			"could not preserve Child Workflow cancellation reason",
			"OdinChildWorkflowCancellationError",
		)
		return false
	}
	operation.cancellation_reason = owned_reason
	operation.failure_message = owned_reason

	if operation.command_state == .Scheduled {
		operation.command_state = .Canceled
		operation.start_state = .Canceled
		operation.completion_state = .Canceled
		return true
	}

	switch operation.options.cancellation_type {
	case .Abandon:
		operation.command_state = .Resolved
		if operation.start_state == .Pending {
			operation.start_state = .Canceled
		}
		operation.completion_state = .Canceled
	case .Try_Cancel:
		operation.command_state = .Cancel_Requested
	case .Wait_Cancellation_Completed, .Wait_Cancellation_Requested:
		operation.command_state = .Cancel_Requested
	}
	return true
}

child_workflow_signal_failure_cause_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_SignalExternalWorkflowExecutionFailedCause,
) -> Child_Workflow_Signal_Failure_Cause {
	switch value {
	case .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND:
		return .Workflow_Not_Found
	case .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND:
		return .Namespace_Not_Found
	case .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_SIGNAL_COUNT_LIMIT_EXCEEDED:
		return .Signal_Count_Limit_Exceeded
	case .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED:
		return .Unspecified
	}
	return .Unspecified
}

child_workflow_signal_result_from_operation :: proc(
	operation: ^Fiber_External_Signal_Operation,
) -> Child_Workflow_Signal_Result {
	switch operation.state {
	case .Resolved:
		return {status = .Succeeded}
	case .Failed:
		return {
			status = .Failed,
			failure_message = operation.failure_message,
			cause = operation.cause,
		}
	case .Canceled:
		return {status = .Canceled, failure_message = operation.failure_message}
	case .Cancel_Requested:
		return {status = .Canceled, failure_message = operation.failure_message}
	case .Scheduled, .Commanded:
	}
	return {status = .Failed, failure_message = "Child Workflow signal has not resolved"}
}

signal_child_workflow_owned :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	input: codec.Payload,
) -> Child_Workflow_Signal_Result {
	owned_input := input
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   future.seq == 0 || len(signal_name) == 0 {
		codec.payload_free(&owned_input)
		return {status = .Failed, failure_message = "invalid Child Workflow signal"}
	}
	start_result := await_child_workflow_start(ctx, future)
	if start_result.status != .Succeeded {
		codec.payload_free(&owned_input)
		status := Child_Workflow_Signal_Status.Failed
		if start_result.status == .Canceled {
			status = .Canceled
		}
		return {status = status, failure_message = start_result.failure_message}
	}
	instance := ctx.instance
	if root_cancellation_applies(instance) {
		codec.payload_free(&owned_input)
		return {status = .Canceled, failure_message = instance.cancellation_reason}
	}
	child, found := find_child_workflow_operation(instance, future.seq)
	if !found || child.completion_state != .Pending {
		codec.payload_free(&owned_input)
		return {status = .Failed, failure_message = "Child Workflow is not running"}
	}
	owned_name, allocation_error := strings.clone(signal_name, instance.odin_context.allocator)
	if allocation_error != nil {
		codec.payload_free(&owned_input)
		return {status = .Failed, failure_message = "could not preserve Child Workflow signal name"}
	}
	instance.next_external_signal_seq += 1
	operation := Fiber_External_Signal_Operation{
		seq = instance.next_external_signal_seq,
		child_workflow_seq = future.seq,
		child_workflow_id = child.workflow_id,
		signal_name = owned_name,
		input = owned_input,
		state = .Scheduled,
		detached_from_root_cancellation = instance.detached_scope_depth > 0,
	}
	append(&instance.external_signals, operation)
	instance.waiting_external_signal_seq = operation.seq
	instance.phase = .Waiting_External_Signal
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		mark_child_workflow_failure(
			ctx,
			"could not resume Child Workflow signal await",
			"OdinChildWorkflowSignalError",
		)
		return {status = .Failed, failure_message = "could not resume Child Workflow signal await"}
	}
	operation_ptr, operation_found := find_external_signal_operation(instance, operation.seq)
	if !operation_found {
		mark_child_workflow_failure(
			ctx,
			"Child Workflow signal operation was not found",
			"OdinChildWorkflowSignalError",
		)
		return {status = .Failed, failure_message = "Child Workflow signal operation was not found"}
	}
	return child_workflow_signal_result_from_operation(operation_ptr)
}

signal_child_workflow_payload :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	input: codec.Payload_View,
) -> Child_Workflow_Signal_Result {
	if ctx == nil || ctx.instance == nil {
		return {status = .Failed, failure_message = "invalid Child Workflow signal context"}
	}
	owned_input, clone_error := codec.payload_clone(input, ctx.instance.odin_context.allocator)
	if clone_error != .None {
		return {status = .Failed, failure_message = "could not preserve Child Workflow signal Payload"}
	}
	return signal_child_workflow_owned(ctx, future, signal_name, owned_input)
}

signal_child_workflow_json :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	input: $T,
) -> Child_Workflow_Signal_Result {
	if ctx == nil || ctx.instance == nil {
		return {status = .Failed, failure_message = "invalid Child Workflow signal context"}
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		return {status = .Failed, failure_message = "could not encode Child Workflow signal as json/plain"}
	}
	return signal_child_workflow_owned(ctx, future, signal_name, payload)
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

failure_timeout_type_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_TimeoutType,
) -> Failure_Timeout_Type {
	switch value {
	case .TIMEOUT_TYPE_START_TO_CLOSE:
		return .Start_To_Close
	case .TIMEOUT_TYPE_SCHEDULE_TO_START:
		return .Schedule_To_Start
	case .TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
		return .Schedule_To_Close
	case .TIMEOUT_TYPE_HEARTBEAT:
		return .Heartbeat
	case .TIMEOUT_TYPE_UNSPECIFIED:
		return .Unspecified
	}
	return .Unspecified
}

preserve_child_workflow_failure :: proc(
	operation: ^Fiber_Child_Workflow_Operation,
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
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Child_Workflow_Execution_Failure_Info:
			if info.value != nil && len(operation.failure.workflow_id) == 0 {
				child := info.value
				namespace_ok, id_ok, run_ok, type_ok: bool
				operation.failure.namespace, namespace_ok = clone_failure_string(child.namespace)
				if child.workflow_execution != nil {
					operation.failure.workflow_id, id_ok = clone_failure_string(child.workflow_execution.workflow_id)
					operation.failure.run_id, run_ok = clone_failure_string(child.workflow_execution.run_id)
				} else {
					id_ok = true
					run_ok = true
				}
				if child.workflow_type != nil {
					operation.failure.workflow_type, type_ok = clone_failure_string(child.workflow_type.name)
				} else {
					type_ok = true
				}
				if !namespace_ok || !id_ok || !run_ok || !type_ok {
					return false
				}
				operation.failure.initiated_event_id = child.initiated_event_id
				operation.failure.started_event_id = child.started_event_id
				operation.failure.retry_state = activity_retry_state_from_proto(child.retry_state)
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
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info:
			if info.value != nil && !operation.failure.canceled {
				operation.failure.canceled = true
				operation.cancellation_detail_payloads = make([dynamic]codec.Payload)
				if info.value.details != nil {
					for &detail in info.value.details.payloads {
						owned, clone_error := proto.clone_payload(&detail)
						if clone_error != .None {
							return false
						}
						append(&operation.cancellation_detail_payloads, owned)
					}
				}
				operation.cancellation_detail_views = make(
					[]codec.Payload_View,
					len(operation.cancellation_detail_payloads),
				)
				for &detail, index in operation.cancellation_detail_payloads {
					operation.cancellation_detail_views[index] = codec.payload_view(&detail)
				}
				operation.failure.cancellation_details = operation.cancellation_detail_views
			}
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Timeout_Failure_Info:
			if info.value != nil && !operation.failure.timed_out {
				operation.failure.timed_out = true
				operation.failure.timeout_type = failure_timeout_type_from_proto(info.value.timeout_type)
			}
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Terminated_Failure_Info:
			operation.failure.terminated = info.value != nil
		}
		current = current.cause
	}
	return true
}

child_workflow_failure_detail_payload :: proc(
	failure: ^Child_Workflow_Failure,
	index: int,
) -> (payload: codec.Payload_View, found: bool) {
	if failure == nil || !failure.application.present || index < 0 ||
	   index >= len(failure.application.details) {
		return
	}
	return failure.application.details[index], true
}

child_workflow_failure_detail_json :: proc(
	failure: ^Child_Workflow_Failure,
	index: int,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := child_workflow_failure_detail_payload(failure, index)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

child_workflow_cancellation_detail_json :: proc(
	failure: ^Child_Workflow_Failure,
	index: int,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	if failure == nil || !failure.canceled || index < 0 ||
	   index >= len(failure.cancellation_details) {
		return false, .None
	}
	return true, codec.decode_json(failure.cancellation_details[index], value)
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
	if operation.state == .Cancel_Requested || operation.state == .Canceled {
		return {status = .Canceled}
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
	if operation.state == .Resolved || operation.state == .Failed ||
	   operation.state == .Cancel_Requested || operation.state == .Canceled {
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
	if !found || (operation.state != .Resolved && operation.state != .Failed &&
	   operation.state != .Cancel_Requested && operation.state != .Canceled) {
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
	if payload_result.status != .Succeeded {
		return {
			status = payload_result.status,
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
	if result.status == .Canceled {
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
	if result.status == .Canceled {
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
	if result.status == .Canceled {
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
	if result.status == .Canceled {
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
	if root_cancellation_applies(instance) {
		return {}
	}
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
	if root_cancellation_applies(instance) {
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
	if root_cancellation_applies(ctx.instance) {
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
	if root_cancellation_applies(ctx.instance) {
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
	if root_cancellation_applies(instance) {
		return {status = .Canceled}
	}
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
		detached_from_root_cancellation = instance.detached_scope_depth > 0,
	}
	append(&instance.timers, operation)
	instance.waiting_signal_name = waiting_name
	instance.waiting_timer_seq = operation.seq
	instance.phase = .Waiting_Signal_Or_Timer
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return {status = .Timed_Out}
	}
	if root_cancellation_applies(instance) {
		return {status = .Canceled}
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
	if payload_result.status != .Received {
		return {status = payload_result.status}
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
	if payload_result.status != .Received {
		return {status = payload_result.status}
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
	if root_cancellation_applies(instance) {
		return
	}
	instance.next_timer_seq += 1
	operation := Fiber_Timer_Operation{
		seq = instance.next_timer_seq,
		duration = duration,
		state = .Scheduled,
		detached_from_root_cancellation = instance.detached_scope_depth > 0,
	}
	append(&instance.timers, operation)
	instance.waiting_timer_seq = operation.seq
	instance.phase = .Waiting_Timer
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
	}
}

// all_handlers_finished reports whether every concurrent Signal and accepted
// Update routine has finished.
all_handlers_finished :: proc(ctx: ^Fiber_Context) -> bool {
	if ctx == nil || ctx.instance == nil {
		return true
	}
	return !has_active_handlers(ctx.instance)
}

// await_all_handlers_finished suspends the main Workflow routine until every
// Signal and accepted Update routine has finished. Calling it from a handler
// routine would wait on itself, so that is rejected as a deterministic failure.
await_all_handlers_finished :: proc(ctx: ^Fiber_Context) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	instance := ctx.instance
	if ctx != &instance.workflow_context || instance.phase != .Ready {
		instance.failure_message = "await_all_handlers_finished must be called by the main Workflow routine"
		instance.failure_type = "OdinWorkflowHandlerAwaitError"
		instance.phase = .Failed
		return
	}
	if !has_active_handlers(instance) {
		return
	}
	instance.phase = .Waiting_Handlers
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.failure_message = "could not resume Workflow handler await"
		instance.failure_type = "OdinWorkflowHandlerAwaitError"
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

child_workflow_cancellation_type_to_proto :: proc(
	cancellation_type: Child_Workflow_Cancellation_Type,
) -> proto.Coresdk_Child_Workflow_ChildWorkflowCancellationType {
	switch cancellation_type {
	case .Wait_Cancellation_Completed:
		return .WAIT_CANCELLATION_COMPLETED
	case .Abandon:
		return .ABANDON
	case .Try_Cancel:
		return .TRY_CANCEL
	case .Wait_Cancellation_Requested:
		return .WAIT_CANCELLATION_REQUESTED
	}
	return .WAIT_CANCELLATION_COMPLETED
}

parent_close_policy_to_proto :: proc(
	policy: Parent_Close_Policy,
) -> proto.Coresdk_Child_Workflow_ParentClosePolicy {
	switch policy {
	case .Unspecified:
		return .PARENT_CLOSE_POLICY_UNSPECIFIED
	case .Terminate:
		return .PARENT_CLOSE_POLICY_TERMINATE
	case .Abandon:
		return .PARENT_CLOSE_POLICY_ABANDON
	case .Request_Cancel:
		return .PARENT_CLOSE_POLICY_REQUEST_CANCEL
	}
	return .PARENT_CLOSE_POLICY_UNSPECIFIED
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

append_fiber_start_child_workflow_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Child_Workflow_Operation,
	default_task_queue: string,
) {
	payload := new(proto.Temporal_Api_Common_V1_Payload)
	payload^ = proto.encode_payload(codec.payload_view(&operation.input))
	task_queue := operation.options.task_queue
	if len(task_queue) == 0 {
		task_queue = default_task_queue
	}
	retry_policy: ^proto.Temporal_Api_Common_V1_RetryPolicy
	if operation.options.has_retry_policy {
		retry_policy = retry_policy_to_proto(&operation.options.retry_policy)
	}
	start := new(proto.Coresdk_Workflow_Commands_StartChildWorkflowExecution)
	start^ = {
		seq = operation.seq,
		workflow_id = operation.workflow_id,
		workflow_type = operation.workflow_type,
		task_queue = task_queue,
		workflow_execution_timeout = duration_to_proto(operation.options.workflow_execution_timeout),
		workflow_run_timeout = duration_to_proto(operation.options.workflow_run_timeout),
		workflow_task_timeout = duration_to_proto(operation.options.workflow_task_timeout),
		parent_close_policy = parent_close_policy_to_proto(operation.options.parent_close_policy),
		retry_policy = retry_policy,
		cancellation_type = child_workflow_cancellation_type_to_proto(operation.options.cancellation_type),
	}
	append(&start.input, payload^)
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Child_Workflow_Execution{
				value = start,
			},
		},
	)
	operation.command_state = .Commanded
	result.started_child_workflow = true
	result.started_child_workflow_count += 1
	result.child_workflow_seq = operation.seq
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

append_fiber_cancel_child_workflow_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Child_Workflow_Operation,
) {
	request := new(proto.Coresdk_Workflow_Commands_CancelChildWorkflowExecution)
	request.child_workflow_seq = operation.seq
	request.reason = operation.cancellation_reason
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Child_Workflow_Execution{
				value = request,
			},
		},
	)
	operation.command_state = .Canceled
	result.requested_child_workflow_cancellation = true
	result.requested_child_workflow_cancellation_count += 1
	result.canceled_child_workflow_seq = operation.seq
}

append_fiber_signal_child_workflow_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_External_Signal_Operation,
) {
	payload := new(proto.Temporal_Api_Common_V1_Payload)
	payload^ = proto.encode_payload(codec.payload_view(&operation.input))
	request := new(proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution)
	request.seq = operation.seq
	request.signal_name = operation.signal_name
	request.target = proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution_Target_Child_Workflow_Id{
		value = operation.child_workflow_id,
	}
	append(&request.args, payload^)
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Signal_External_Workflow_Execution{
				value = request,
			},
		},
	)
	operation.state = .Commanded
	result.signaled_child_workflow = true
	result.signaled_child_workflow_count += 1
	result.child_workflow_signal_seq = operation.seq
}

append_fiber_cancel_external_signal_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_External_Signal_Operation,
) {
	request := new(proto.Coresdk_Workflow_Commands_CancelSignalWorkflow)
	request.seq = operation.seq
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Signal_Workflow{
				value = request,
			},
		},
	)
	operation.state = .Canceled
	result.canceled_child_workflow_signal = true
}

append_fiber_external_signal_cancellations :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.external_signals) {
		operation := &instance.external_signals[index]
		if operation.state == .Cancel_Requested {
			append_fiber_cancel_external_signal_command(result, commands, operation)
		}
	}
}

append_fiber_child_workflow_cancellations :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.child_workflows) {
		operation := &instance.child_workflows[index]
		if operation.command_state == .Cancel_Requested {
			append_fiber_cancel_child_workflow_command(result, commands, operation)
		}
	}
}

Workflow_Command_State :: struct {
	phase:             Fiber_Phase,
	next_activity_seq: u32,
	next_timer_seq:    u32,
	next_child_workflow_seq: u32,
	next_external_signal_seq: u32,
	activity_count:    int,
	timer_count:       int,
	child_workflow_count: int,
	external_signal_count: int,
}

workflow_command_state :: proc(instance: ^Fiber_Instance) -> Workflow_Command_State {
	return {
		phase = instance.phase,
		next_activity_seq = instance.next_activity_seq,
		next_timer_seq = instance.next_timer_seq,
		next_child_workflow_seq = instance.next_child_workflow_seq,
		next_external_signal_seq = instance.next_external_signal_seq,
		activity_count = len(instance.activities),
		timer_count = len(instance.timers),
		child_workflow_count = len(instance.child_workflows),
		external_signal_count = len(instance.external_signals),
	}
}

workflow_command_state_changed :: proc(
	instance: ^Fiber_Instance,
	before: Workflow_Command_State,
) -> bool {
	return instance.phase != before.phase ||
		instance.next_activity_seq != before.next_activity_seq ||
		instance.next_timer_seq != before.next_timer_seq ||
		instance.next_child_workflow_seq != before.next_child_workflow_seq ||
		instance.next_external_signal_seq != before.next_external_signal_seq ||
		len(instance.activities) != before.activity_count ||
		len(instance.timers) != before.timer_count ||
		len(instance.child_workflows) != before.child_workflow_count ||
		len(instance.external_signals) != before.external_signal_count
}

restore_workflow_command_state :: proc(
	instance: ^Fiber_Instance,
	before: Workflow_Command_State,
) {
	instance.phase = before.phase
	instance.next_activity_seq = before.next_activity_seq
	instance.next_timer_seq = before.next_timer_seq
	instance.next_child_workflow_seq = before.next_child_workflow_seq
	instance.next_external_signal_seq = before.next_external_signal_seq
	for len(instance.activities) > before.activity_count {
		ordered_remove(&instance.activities, len(instance.activities) - 1)
	}
	for len(instance.timers) > before.timer_count {
		ordered_remove(&instance.timers, len(instance.timers) - 1)
	}
	for len(instance.child_workflows) > before.child_workflow_count {
		ordered_remove(&instance.child_workflows, len(instance.child_workflows) - 1)
	}
	for len(instance.external_signals) > before.external_signal_count {
		ordered_remove(&instance.external_signals, len(instance.external_signals) - 1)
	}
}

invoke_workflow_query :: proc(
	instance: ^Fiber_Instance,
	query: ^proto.Coresdk_Workflow_Activation_QueryWorkflow,
) -> Payload_Result {
	if instance == nil || query == nil || len(query.query_id) == 0 || len(query.query_type) == 0 {
		return payload_fail("Workflow query is malformed", "OdinWorkflowQueryError")
	}
	if len(query.arguments) != 1 {
		return payload_fail(
			"Workflow query requires exactly one argument",
			"OdinWorkflowQueryArgumentError",
		)
	}
	handler, found := find_query_handler(instance, query.query_type)
	if !found {
		message := fmt.aprintf("Workflow query handler %s is not registered", query.query_type)
		return payload_fail(message, "OdinWorkflowQueryNotFound")
	}
	command_state := workflow_command_state(instance)
	input, clone_error := proto.clone_payload(&query.arguments[0])
	if clone_error != .None {
		return payload_fail(
			"could not preserve Workflow query input",
			"OdinWorkflowQueryPayloadConversionError",
		)
	}
	defer codec.payload_free(&input)
	result := handler.invoke(handler.callback_data, codec.payload_view(&input))
	if workflow_command_state_changed(instance, command_state) {
		codec.payload_free(&result.payload)
		restore_workflow_command_state(instance, command_state)
		return payload_fail(
			"Workflow query handler attempted to mutate runtime command state",
			"OdinWorkflowQueryMutationError",
		)
	}
	return result
}

append_query_response_command :: proc(
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	response: ^Pending_Query_Response,
) {
	query_result := new(proto.Coresdk_Workflow_Commands_QueryResult)
	query_result.query_id = response.query_id
	if response.result.status == .Completed {
		payload := new(proto.Temporal_Api_Common_V1_Payload)
		payload^ = proto.encode_payload(codec.payload_view(&response.result.payload))
		success := new(proto.Coresdk_Workflow_Commands_QuerySuccess)
		success.response = payload
		query_result.variant = proto.Coresdk_Workflow_Commands_QueryResult_Variant_Succeeded{
			value = success,
		}
	} else {
		failure_type := response.result.failure_type
		if len(failure_type) == 0 {
			failure_type = "OdinWorkflowQueryError"
		}
		application_info := new(proto.Temporal_Api_Failure_V1_ApplicationFailureInfo)
		application_info.type = failure_type
		application_info.non_retryable = true
		failure := new(proto.Temporal_Api_Failure_V1_Failure)
		failure.message = response.result.failure_message
		failure.source = "OdinSDK"
		failure.failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = application_info,
		}
		query_result.variant = proto.Coresdk_Workflow_Commands_QueryResult_Variant_Failed{
			value = failure,
		}
	}
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Respond_To_Query{
				value = query_result,
			},
		},
	)
}

append_pending_update_response :: proc(
	responses: ^[dynamic]Pending_Update_Response,
	protocol_instance_id: string,
	kind: Update_Response_Kind,
	result: Payload_Result = {},
) {
	append(
		responses,
		Pending_Update_Response{
			protocol_instance_id = protocol_instance_id,
			kind = kind,
			result = result,
		},
	)
}

start_signal_routine :: proc(
	instance: ^Fiber_Instance,
	handler: ^Fiber_Signal_Handler,
	name: string,
	input: codec.Payload_View,
) -> (^Fiber_Signal_Routine, Error) {
	if instance == nil || handler == nil || handler.invoke_routine == nil || len(name) == 0 {
		return nil, error_from_string("invalid Workflow Signal routine")
	}
	allocator := instance.odin_context.allocator
	routine, allocation_error := new(Fiber_Signal_Routine, allocator)
	if allocation_error != nil {
		return nil, error_from_string("could not allocate Workflow Signal routine")
	}
	owned_name, name_error := strings.clone(name, allocator)
	owned_input, input_error := codec.payload_clone(input, allocator)
	if name_error != nil || input_error != .None {
		return nil, error_from_string("could not preserve Workflow Signal routine input")
	}
	routine.instance = instance
	routine.name = owned_name
	routine.input = owned_input
	routine.callback_data = handler.callback_data
	routine.invoke = handler.invoke_routine
	routine.unfinished_policy = handler.unfinished_policy
	routine.ctx.instance = instance
	routine.phase = .Ready
	create_result := fiber_raw.create(
		&routine.fiber,
		signal_routine_entry,
		routine,
		FIBER_STACK_SIZE,
	)
	if create_result != .Success {
		return nil, fiber_result_error("could not create Workflow Signal routine", create_result)
	}
	resume_error := resume_signal_routine(routine)
	if resume_error.message != nil {
		if routine.fiber != nil {
			_ = fiber_raw.destroy(routine.fiber)
			routine.fiber = nil
		}
		return nil, resume_error
	}
	append(&instance.signal_routines, routine)
	return routine, {}
}

finish_signal_routine :: proc(
	routine: ^Fiber_Signal_Routine,
	result: ^Process_Result,
) -> Error {
	if routine == nil || routine.completion_observed || routine.fiber == nil ||
	   fiber_raw.status(routine.fiber) != .Dead {
		return {}
	}
	routine.completion_observed = true
	destroy_result := fiber_raw.destroy(routine.fiber)
	routine.fiber = nil
	if destroy_result != .Success {
		return fiber_result_error("could not destroy completed Workflow Signal routine", destroy_result)
	}
	if routine.result.status == .Failed || routine.phase == .Failed {
		message := routine.result.failure_message
		if len(message) == 0 {
			message = "Workflow Signal routine failed"
		}
		codec.payload_free(&routine.result.payload)
		return error_from_string(message)
	}
	codec.payload_free(&routine.result.payload)
	result.signal_handlers_completed += 1
	return {}
}

finish_signal_routines :: proc(
	instance: ^Fiber_Instance,
	result: ^Process_Result,
) -> Error {
	for routine in instance.signal_routines {
		finish_error := finish_signal_routine(routine, result)
		if finish_error.message != nil {
			return finish_error
		}
	}
	return {}
}

signal_routine_should_resume :: proc(routine: ^Fiber_Signal_Routine) -> bool {
	if routine == nil || routine.instance == nil || routine.fiber == nil ||
	   fiber_raw.status(routine.fiber) != .Suspended {
		return false
	}
	instance := routine.instance
	cancellation_applies := instance.cancellation_requested && routine.detached_scope_depth == 0
	switch routine.phase {
	case .Waiting_Activity:
		operation, found := find_activity_operation(instance, routine.waiting_activity_seq)
		return found && (operation.state == .Resolved || operation.state == .Failed ||
			operation.state == .Cancel_Requested || operation.state == .Canceled)
	case .Waiting_Child_Workflow_Start:
		operation, found := find_child_workflow_operation(instance, routine.waiting_child_workflow_seq)
		return found && operation.start_state != .Pending
	case .Waiting_Child_Workflow:
		operation, found := find_child_workflow_operation(instance, routine.waiting_child_workflow_seq)
		return found && operation.completion_state != .Pending
	case .Waiting_External_Signal:
		operation, found := find_external_signal_operation(instance, routine.waiting_external_signal_seq)
		return found && (operation.state == .Resolved || operation.state == .Failed ||
			operation.state == .Cancel_Requested || operation.state == .Canceled)
	case .Waiting_Timer:
		operation, found := find_timer_operation(instance, routine.waiting_timer_seq)
		return found && (operation.state == .Resolved || operation.state == .Cancel_Requested ||
			operation.state == .Canceled)
	case .Waiting_Signal:
		_, found := find_buffered_signal(instance, routine.waiting_signal_name)
		return found || cancellation_applies
	case .Waiting_Signal_Or_Timer:
		_, signal_found := find_buffered_signal(instance, routine.waiting_signal_name)
		timer, timer_found := find_timer_operation(instance, routine.waiting_timer_seq)
		return signal_found || cancellation_applies ||
			(timer_found && (timer.state == .Resolved || timer.state == .Cancel_Requested ||
			timer.state == .Canceled))
	case .Waiting_Handlers, .Ready, .Canceled, .Completed, .Failed:
	}
	return false
}

resume_ready_signal_routines :: proc(instance: ^Fiber_Instance) -> Error {
	for routine in instance.signal_routines {
		if !signal_routine_should_resume(routine) {
			continue
		}
		routine.phase = .Ready
		routine.waiting_activity_seq = 0
		routine.waiting_child_workflow_seq = 0
		routine.waiting_external_signal_seq = 0
		routine.waiting_timer_seq = 0
		routine.waiting_signal_name = ""
		resume_error := resume_signal_routine(routine)
		if resume_error.message != nil {
			return resume_error
		}
	}
	return {}
}

invoke_workflow_signal :: proc(
	instance: ^Fiber_Instance,
	signal: ^proto.Coresdk_Workflow_Activation_SignalWorkflow,
	result: ^Process_Result,
) -> Error {
	if instance == nil || signal == nil || len(signal.signal_name) == 0 || len(signal.input) != 1 {
		return error_from_string("Workflow signal must have a name and one payload")
	}
	input, clone_error := proto.clone_payload(&signal.input[0])
	if clone_error != .None {
		return error_from_string("could not preserve Workflow signal Payload")
	}
	handler, found := find_signal_handler(instance, signal.signal_name)
	if !found {
		signal_name, name_error := strings.clone(signal.signal_name, instance.odin_context.allocator)
		if name_error != nil {
			codec.payload_free(&input)
			return error_from_string("could not preserve Workflow signal Payload")
		}
		append(&instance.signals, Fiber_Signal{name = signal_name, payload = input})
		return {}
	}
	defer codec.payload_free(&input)
	input_view := codec.payload_view(&input)
	result.signal_handlers_started += 1
	if handler.invoke_routine != nil {
		routine, routine_error := start_signal_routine(
			instance,
			handler,
			signal.signal_name,
			input_view,
		)
		if routine_error.message != nil {
			return routine_error
		}
		return finish_signal_routine(routine, result)
	}
	command_state := workflow_command_state(instance)
	handler_result := handler.invoke(handler.callback_data, input_view)
	if workflow_command_state_changed(instance, command_state) {
		codec.payload_free(&handler_result.payload)
		restore_workflow_command_state(instance, command_state)
		return error_from_string("synchronous Workflow Signal handler attempted to mutate runtime command state")
	}
	if handler_result.status == .Failed {
		message := handler_result.failure_message
		if len(message) == 0 {
			message = "Workflow Signal handler failed"
		}
		codec.payload_free(&handler_result.payload)
		return error_from_string(message)
	}
	codec.payload_free(&handler_result.payload)
	result.signal_handlers_completed += 1
	return {}
}

find_update_routine :: proc(
	instance: ^Fiber_Instance,
	protocol_instance_id: string,
) -> (^Fiber_Update_Routine, bool) {
	for routine in instance.update_routines {
		if routine.protocol_instance_id == protocol_instance_id {
			return routine, true
		}
	}
	return nil, false
}

start_update_routine :: proc(
	instance: ^Fiber_Instance,
	handler: ^Fiber_Update_Handler,
	protocol_instance_id: string,
	input: codec.Payload_View,
) -> (^Fiber_Update_Routine, Error) {
	if instance == nil || handler == nil || handler.invoke_routine == nil ||
	   len(protocol_instance_id) == 0 {
		return nil, error_from_string("invalid Workflow Update routine")
	}
	_, duplicate := find_update_routine(instance, protocol_instance_id)
	if duplicate {
		return nil, error_from_string("Workflow Update routine already exists")
	}
	allocator := instance.odin_context.allocator
	routine, allocation_error := new(Fiber_Update_Routine, allocator)
	if allocation_error != nil {
		return nil, error_from_string("could not allocate Workflow Update routine")
	}
	owned_protocol_id, id_error := strings.clone(protocol_instance_id, allocator)
	owned_input, input_error := codec.payload_clone(input, allocator)
	if id_error != nil || input_error != .None {
		return nil, error_from_string("could not preserve Workflow Update routine input")
	}
	routine.instance = instance
	routine.protocol_instance_id = owned_protocol_id
	routine.input = owned_input
	routine.callback_data = handler.callback_data
	routine.invoke = handler.invoke_routine
	routine.unfinished_policy = handler.unfinished_policy
	routine.ctx.instance = instance
	routine.phase = .Ready
	create_result := fiber_raw.create(
		&routine.fiber,
		update_routine_entry,
		routine,
		FIBER_STACK_SIZE,
	)
	if create_result != .Success {
		return nil, fiber_result_error("could not create Workflow Update routine", create_result)
	}
	resume_error := resume_update_routine(routine)
	if resume_error.message != nil {
		if routine.fiber != nil {
			_ = fiber_raw.destroy(routine.fiber)
			routine.fiber = nil
		}
		return nil, resume_error
	}
	append(&instance.update_routines, routine)
	return routine, {}
}

append_finished_update_routine_response :: proc(
	routine: ^Fiber_Update_Routine,
	responses: ^[dynamic]Pending_Update_Response,
) -> Error {
	if routine == nil || routine.response_emitted || routine.fiber == nil ||
	   fiber_raw.status(routine.fiber) != .Dead {
		return {}
	}
	kind := Update_Response_Kind.Completed
	if routine.result.status == .Failed || routine.phase == .Failed {
		kind = .Rejected
	}
	append_pending_update_response(
		responses,
		routine.protocol_instance_id,
		kind,
		routine.result,
	)
	routine.result = {}
	routine.response_emitted = true
	destroy_result := fiber_raw.destroy(routine.fiber)
	if destroy_result != .Success {
		return fiber_result_error("could not destroy completed Workflow Update routine", destroy_result)
	}
	routine.fiber = nil
	return {}
}

append_finished_update_routine_responses :: proc(
	instance: ^Fiber_Instance,
	responses: ^[dynamic]Pending_Update_Response,
) -> Error {
	for routine in instance.update_routines {
		append_error := append_finished_update_routine_response(routine, responses)
		if append_error.message != nil {
			return append_error
		}
	}
	return {}
}

update_routine_should_resume :: proc(routine: ^Fiber_Update_Routine) -> bool {
	if routine == nil || routine.instance == nil || routine.fiber == nil ||
	   fiber_raw.status(routine.fiber) != .Suspended {
		return false
	}
	instance := routine.instance
	cancellation_applies := instance.cancellation_requested && routine.detached_scope_depth == 0
	switch routine.phase {
	case .Waiting_Activity:
		operation, found := find_activity_operation(instance, routine.waiting_activity_seq)
		return found && (operation.state == .Resolved || operation.state == .Failed ||
			operation.state == .Cancel_Requested || operation.state == .Canceled)
	case .Waiting_Child_Workflow_Start:
		operation, found := find_child_workflow_operation(instance, routine.waiting_child_workflow_seq)
		return found && operation.start_state != .Pending
	case .Waiting_Child_Workflow:
		operation, found := find_child_workflow_operation(instance, routine.waiting_child_workflow_seq)
		return found && operation.completion_state != .Pending
	case .Waiting_External_Signal:
		operation, found := find_external_signal_operation(instance, routine.waiting_external_signal_seq)
		return found && (operation.state == .Resolved || operation.state == .Failed ||
			operation.state == .Cancel_Requested || operation.state == .Canceled)
	case .Waiting_Timer:
		operation, found := find_timer_operation(instance, routine.waiting_timer_seq)
		return found && (operation.state == .Resolved || operation.state == .Cancel_Requested ||
			operation.state == .Canceled)
	case .Waiting_Signal:
		_, found := find_buffered_signal(instance, routine.waiting_signal_name)
		return found || cancellation_applies
	case .Waiting_Signal_Or_Timer:
		_, signal_found := find_buffered_signal(instance, routine.waiting_signal_name)
		timer, timer_found := find_timer_operation(instance, routine.waiting_timer_seq)
		return signal_found || cancellation_applies ||
			(timer_found && (timer.state == .Resolved || timer.state == .Cancel_Requested ||
			timer.state == .Canceled))
	case .Waiting_Handlers, .Ready, .Canceled, .Completed, .Failed:
	}
	return false
}

resume_ready_update_routines :: proc(instance: ^Fiber_Instance) -> Error {
	for routine in instance.update_routines {
		if !update_routine_should_resume(routine) {
			continue
		}
		routine.phase = .Ready
		routine.waiting_activity_seq = 0
		routine.waiting_child_workflow_seq = 0
		routine.waiting_external_signal_seq = 0
		routine.waiting_timer_seq = 0
		routine.waiting_signal_name = ""
		resume_error := resume_update_routine(routine)
		if resume_error.message != nil {
			return resume_error
		}
	}
	return {}
}

invoke_workflow_update :: proc(
	instance: ^Fiber_Instance,
	update: ^proto.Coresdk_Workflow_Activation_DoUpdate,
	responses: ^[dynamic]Pending_Update_Response,
) {
	protocol_instance_id := update.protocol_instance_id
	if len(update.name) == 0 || len(update.input) != 1 {
		append_pending_update_response(
			responses,
			protocol_instance_id,
			.Rejected,
			payload_fail(
				"Workflow Update requires a name and exactly one argument",
				"OdinWorkflowUpdateArgumentError",
			),
		)
		return
	}
	handler, found := find_update_handler(instance, update.name)
	if !found {
		message := fmt.aprintf("Workflow Update handler %s is not registered", update.name)
		append_pending_update_response(
			responses,
			protocol_instance_id,
			.Rejected,
			payload_fail(message, "OdinWorkflowUpdateNotFound"),
		)
		return
	}
	input, clone_error := proto.clone_payload(&update.input[0])
	if clone_error != .None {
		append_pending_update_response(
			responses,
			protocol_instance_id,
			.Rejected,
			payload_fail(
				"could not preserve Workflow Update input",
				"OdinWorkflowUpdatePayloadConversionError",
			),
		)
		return
	}
	defer codec.payload_free(&input)

	if update.run_validator && handler.validate != nil {
		command_state := workflow_command_state(instance)
		validation := handler.validate(handler.callback_data, codec.payload_view(&input))
		if workflow_command_state_changed(instance, command_state) {
			restore_workflow_command_state(instance, command_state)
			append_pending_update_response(
				responses,
				protocol_instance_id,
				.Rejected,
				payload_fail(
					"Workflow Update validator attempted to mutate runtime command state",
					"OdinWorkflowUpdateValidatorMutationError",
				),
			)
			return
		}
		if !validation.accepted {
			failure_type := validation.failure_type
			if len(failure_type) == 0 {
				failure_type = "OdinWorkflowUpdateRejected"
			}
			append_pending_update_response(
				responses,
				protocol_instance_id,
				.Rejected,
				payload_fail(validation.message, failure_type),
			)
			return
		}
	}

	append_pending_update_response(responses, protocol_instance_id, .Accepted)
	if handler.invoke_routine != nil {
		routine, routine_error := start_update_routine(
			instance,
			handler,
			protocol_instance_id,
			codec.payload_view(&input),
		)
		if routine_error.message != nil {
			failure_message := fmt.aprintf("%s", string(routine_error.message))
			append_pending_update_response(
				responses,
				protocol_instance_id,
				.Rejected,
				payload_fail(
					failure_message,
					"OdinWorkflowUpdateRoutineError",
				),
			)
			error_free(&routine_error)
			return
		}
		append_error := append_finished_update_routine_response(routine, responses)
		if append_error.message != nil {
			failure_message := fmt.aprintf("%s", string(append_error.message))
			append_pending_update_response(
				responses,
				protocol_instance_id,
				.Rejected,
				payload_fail(
					failure_message,
					"OdinWorkflowUpdateRoutineError",
				),
			)
			error_free(&append_error)
		}
		return
	}
	command_state := workflow_command_state(instance)
	result := handler.invoke(handler.callback_data, codec.payload_view(&input))
	if workflow_command_state_changed(instance, command_state) {
		codec.payload_free(&result.payload)
		restore_workflow_command_state(instance, command_state)
		result = payload_fail(
			"synchronous Workflow Update handler attempted to mutate runtime command state",
			"OdinWorkflowUpdateMutationError",
		)
	}
	if result.status == .Completed {
		append_pending_update_response(responses, protocol_instance_id, .Completed, result)
	} else {
		append_pending_update_response(responses, protocol_instance_id, .Rejected, result)
	}
}

append_update_response_command :: proc(
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	response: ^Pending_Update_Response,
) {
	update_response := new(proto.Coresdk_Workflow_Commands_UpdateResponse)
	update_response.protocol_instance_id = response.protocol_instance_id
	switch response.kind {
	case .Accepted:
		accepted := new(proto.Google_Protobuf_Empty)
		update_response.response = proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Accepted{
			value = accepted,
		}
	case .Completed:
		payload := new(proto.Temporal_Api_Common_V1_Payload)
		payload^ = proto.encode_payload(codec.payload_view(&response.result.payload))
		update_response.response = proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Completed{
			value = payload,
		}
	case .Rejected:
		failure_type := response.result.failure_type
		if len(failure_type) == 0 {
			failure_type = "OdinWorkflowUpdateRejected"
		}
		application_info := new(proto.Temporal_Api_Failure_V1_ApplicationFailureInfo)
		application_info.type = failure_type
		application_info.non_retryable = true
		failure := new(proto.Temporal_Api_Failure_V1_Failure)
		failure.message = response.result.failure_message
		failure.source = "OdinSDK"
		failure.failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = application_info,
		}
		update_response.response = proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Rejected{
			value = failure,
		}
	}
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Update_Response{
				value = update_response,
			},
		},
	)
}

append_shared_operation_commands :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
) {
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
	for index in 0..<len(instance.child_workflows) {
		operation := &instance.child_workflows[index]
		if operation.command_state == .Scheduled {
			append_fiber_start_child_workflow_command(result, commands, operation, task_queue)
		}
	}
	for index in 0..<len(instance.external_signals) {
		operation := &instance.external_signals[index]
		if operation.state == .Scheduled {
			append_fiber_signal_child_workflow_command(result, commands, operation)
		}
	}
	append_fiber_activity_cancellations(result, commands, instance)
	append_fiber_timer_cancellations(result, commands, instance)
	append_fiber_child_workflow_cancellations(result, commands, instance)
	append_fiber_external_signal_cancellations(result, commands, instance)
}

append_fiber_pending_commands :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
) -> Error {
	append_shared_operation_commands(result, commands, instance, task_queue)
	switch instance.phase {
	case .Waiting_Activity:
		result.waiting_activity_seq = instance.waiting_activity_seq
	case .Waiting_Child_Workflow_Start:
		result.waiting_child_workflow_start_seq = instance.waiting_child_workflow_seq
	case .Waiting_Child_Workflow:
		result.waiting_child_workflow_seq = instance.waiting_child_workflow_seq
	case .Waiting_External_Signal:
	case .Waiting_Timer:
	case .Waiting_Signal:
		result.waiting_signal = true
	case .Waiting_Signal_Or_Timer:
		result.waiting_signal = true
		result.waiting_timer_seq = instance.waiting_timer_seq
	case .Waiting_Handlers:
		result.waiting_handlers = true
	case .Ready, .Canceled, .Completed, .Failed:
		return error_from_string("Workflow fiber suspended without a supported pending operation")
	}
	result.run_id = instance.run_id
	return {}
}

has_active_signal_routines :: proc(instance: ^Fiber_Instance) -> bool {
	for routine in instance.signal_routines {
		if !routine.completion_observed {
			return true
		}
	}
	return false
}

has_active_update_routines :: proc(instance: ^Fiber_Instance) -> bool {
	for routine in instance.update_routines {
		if !routine.response_emitted {
			return true
		}
	}
	return false
}

has_active_handlers :: proc(instance: ^Fiber_Instance) -> bool {
	return has_active_signal_routines(instance) || has_active_update_routines(instance)
}

observe_unfinished_signal_routines :: proc(
	result: ^Process_Result,
	instance: ^Fiber_Instance,
) {
	for routine in instance.signal_routines {
		if routine.completion_observed {
			continue
		}
		result.unfinished_signal_handlers += 1
		if routine.unfinished_policy == .Warn_And_Abandon {
			result.warned_unfinished_signal_handlers += 1
		}
	}
}

observe_unfinished_update_routines :: proc(
	result: ^Process_Result,
	instance: ^Fiber_Instance,
) {
	for routine in instance.update_routines {
		if routine.response_emitted {
			continue
		}
		result.unfinished_update_handlers += 1
		if routine.unfinished_policy == .Warn_And_Abandon {
			result.warned_unfinished_update_handlers += 1
		}
	}
}

commands_are_update_responses :: proc(
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
) -> bool {
	for command in commands^ {
		is_update_response := false
		#partial switch variant in command.variant {
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Update_Response:
			is_update_response = variant.value != nil
		}
		if !is_update_response {
			return false
		}
	}
	return true
}

append_fiber_completion_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) -> Error {
	if !commands_are_update_responses(commands) {
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
	if !commands_are_update_responses(commands) {
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
	} else if instance.unhandled_child_workflow_seq != 0 {
		operation, found := find_child_workflow_operation(
			instance,
			instance.unhandled_child_workflow_seq,
		)
		if !found ||
		   (operation.completion_state != .Failed && operation.completion_state != .Canceled) ||
		   len(operation.failure_proto) == 0 {
			return error_from_string("Workflow fiber failed without a preserved Child Workflow failure")
		}
		if !proto.decode_Temporal_Api_Failure_V1_Failure(operation.failure_proto, failure) {
			return error_from_string("could not restore unhandled Child Workflow failure")
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
	if !commands_are_update_responses(commands) {
		return error_from_string("fiber spike emitted cancellation after another command")
	}
	append_fiber_activity_cancellations(result, commands, instance)
	append_fiber_timer_cancellations(result, commands, instance)
	append_fiber_child_workflow_cancellations(result, commands, instance)
	append_fiber_external_signal_cancellations(result, commands, instance)
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
		observe_unfinished_signal_routines(result, instance)
		observe_unfinished_update_routines(result, instance)
		return append_fiber_cancellation_commands(result, commands, instance)
	}
	main_status := fiber_raw.status(instance.fiber)
	switch main_status {
	case .Suspended:
		return append_fiber_pending_commands(result, commands, instance, task_queue)
	case .Dead:
		observe_unfinished_signal_routines(result, instance)
		observe_unfinished_update_routines(result, instance)
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

	context.allocator = virtual.arena_allocator(&decode_arena)
	pending_query_responses := make([dynamic]Pending_Query_Response)
	pending_update_responses := make([dynamic]Pending_Update_Response)
	context.allocator = previous_allocator
	defer {
		for &query_response in pending_query_responses {
			codec.payload_free(&query_response.result.payload)
		}
		for &update_response in pending_update_responses {
			codec.payload_free(&update_response.result.payload)
		}
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
			if operation.state == .Canceled {
				// Core may report the terminal resolution of a requested Activity
				// while detached cleanup is running or before cache eviction.
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
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Cancelled:
				resolution_handled = true
				if status.value == nil {
					process_error(&result, activation.run_id, "canceled Activity has no result")
					return
				}
				operation.state = .Canceled
				result.resolved_activity_seq = operation.seq
			}
			if !resolution_handled {
				process_error(&result, activation.run_id, "Activity result is unsupported by the Workflow fiber spike")
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Child Workflow start resolution has no Workflow fiber")
				return
			}
			operation, operation_found := find_child_workflow_operation(instance, variant.value.seq)
			if !operation_found ||
			   (operation.command_state != .Commanded && operation.command_state != .Cancel_Requested && operation.command_state != .Canceled) {
				process_error(&result, activation.run_id, "Child Workflow start resolution does not match a pending operation")
				return
			}
			if operation.start_state != .Pending {
				result.resolved_child_workflow_start_seq = operation.seq
				continue
			}
			resolution_handled := false
			#partial switch status in variant.value.status {
			case proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded:
				resolution_handled = true
				if status.value == nil || len(status.value.run_id) == 0 {
					process_error(&result, activation.run_id, "started Child Workflow has no Run ID")
					return
				}
				context.allocator = instance.odin_context.allocator
				operation.run_id, _ = strings.clone(status.value.run_id)
				context.allocator = previous_allocator
				if len(operation.run_id) == 0 {
					process_error(&result, activation.run_id, "could not preserve Child Workflow Run ID")
					return
				}
				operation.start_state = .Succeeded
			case proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Failed:
				resolution_handled = true
				if status.value == nil {
					process_error(&result, activation.run_id, "failed Child Workflow start has no result")
					return
				}
				operation.start_state = .Failed
				operation.failure_message = "Child Workflow failed to start"
				if status.value.cause == .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS {
					operation.start_failure_cause = .Workflow_Already_Exists
				}
			case proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Cancelled:
				resolution_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "canceled Child Workflow start has no failure")
					return
				}
				context.allocator = instance.odin_context.allocator
				operation.failure_message, _ = strings.clone(status.value.failure.message)
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
				operation.failure_proto = failure_proto[:]
				context.allocator = previous_allocator
				operation.start_state = .Canceled
			}
			if !resolution_handled {
				process_error(&result, activation.run_id, "Child Workflow start result is unsupported")
				return
			}
			result.resolved_child_workflow_start_seq = operation.seq

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution:
			handled = true
			if variant.value == nil || variant.value.result == nil || !instance_found {
				process_error(&result, activation.run_id, "Child Workflow completion has no Workflow fiber")
				return
			}
			operation, operation_found := find_child_workflow_operation(instance, variant.value.seq)
			if !operation_found ||
			   (operation.command_state != .Commanded && operation.command_state != .Cancel_Requested && operation.command_state != .Canceled) {
				process_error(&result, activation.run_id, "Child Workflow completion does not match a pending operation")
				return
			}
			if operation.completion_state != .Pending {
				result.resolved_child_workflow_seq = operation.seq
				continue
			}
			resolution_handled := false
			#partial switch status in variant.value.result.status {
			case proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Completed:
				resolution_handled = true
				if status.value == nil || status.value.result == nil {
					process_error(&result, activation.run_id, "completed Child Workflow has no result")
					return
				}
				context.allocator = instance.odin_context.allocator
				cloned_result, clone_error := proto.clone_payload(status.value.result)
				context.allocator = previous_allocator
				if clone_error != .None {
					process_error(&result, activation.run_id, "could not preserve Child Workflow result Payload")
					return
				}
				operation.result = cloned_result
				operation.completion_state = .Succeeded
			case proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Failed:
				resolution_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "failed Child Workflow has no failure")
					return
				}
				context.allocator = instance.odin_context.allocator
				failure_message, allocation_error := strings.clone(status.value.failure.message)
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
				failure_preserved := preserve_child_workflow_failure(operation, status.value.failure)
				context.allocator = previous_allocator
				if allocation_error != nil || len(failure_proto) == 0 || !failure_preserved {
					process_error(&result, activation.run_id, "could not preserve Child Workflow failure")
					return
				}
				operation.failure_message = failure_message
				operation.failure_proto = failure_proto[:]
				operation.completion_state = .Failed
			case proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Cancelled:
				resolution_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "canceled Child Workflow has no failure")
					return
				}
				context.allocator = instance.odin_context.allocator
				failure_message, allocation_error := strings.clone(status.value.failure.message)
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
				failure_preserved := preserve_child_workflow_failure(operation, status.value.failure)
				context.allocator = previous_allocator
				if allocation_error != nil || len(failure_proto) == 0 || !failure_preserved {
					process_error(&result, activation.run_id, "could not preserve canceled Child Workflow failure")
					return
				}
				operation.failure_message = failure_message
				operation.failure_proto = failure_proto[:]
				operation.completion_state = .Canceled
			}
			if !resolution_handled {
				process_error(&result, activation.run_id, "Child Workflow result is unsupported")
				return
			}
			result.resolved_child_workflow_seq = operation.seq

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Signal_External_Workflow:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Child Workflow signal resolution has no Workflow fiber")
				return
			}
			operation, operation_found := find_external_signal_operation(instance, variant.value.seq)
			if !operation_found {
				process_error(&result, activation.run_id, "Child Workflow signal resolution does not match a pending operation")
				return
			}
			if operation.state == .Resolved || operation.state == .Failed {
				result.resolved_child_workflow_signal_seq = operation.seq
				continue
			}
			if operation.state != .Commanded && operation.state != .Cancel_Requested && operation.state != .Canceled {
				process_error(&result, activation.run_id, "Child Workflow signal resolution does not match a commanded operation")
				return
			}
			was_canceled := operation.state == .Cancel_Requested || operation.state == .Canceled
			if variant.value.failure == nil {
				if was_canceled {
					operation.state = .Canceled
					result.canceled_child_workflow_signal = true
				} else {
					operation.state = .Resolved
				}
			} else {
				context.allocator = instance.odin_context.allocator
				failure_message, allocation_error := strings.clone(variant.value.failure.message)
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(variant.value.failure)
				context.allocator = previous_allocator
				if allocation_error != nil || len(failure_proto) == 0 {
					process_error(&result, activation.run_id, "could not preserve Child Workflow signal failure")
					return
				}
				operation.failure_message = failure_message
				operation.failure_proto = failure_proto[:]
				operation.cause = child_workflow_signal_failure_cause_from_proto(variant.value.cause)
				canceled_failure := false
				#partial switch failure_info in variant.value.failure.failure_info {
				case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info:
					canceled_failure = failure_info.value != nil
				}
				if was_canceled || canceled_failure {
					operation.state = .Canceled
					result.canceled_child_workflow_signal = true
				} else {
					operation.state = .Failed
					result.resolved_child_workflow_signal_failed = true
				}
			}
			result.resolved_child_workflow_signal_seq = operation.seq

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Workflow query has no active Workflow fiber")
				return
			}
			context.allocator = virtual.arena_allocator(&decode_arena)
			query_result := invoke_workflow_query(instance, variant.value)
			append(
				&pending_query_responses,
				Pending_Query_Response{
					query_id = variant.value.query_id,
					result = query_result,
				},
			)
			context.allocator = previous_allocator

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update:
			handled = true
			if variant.value == nil || !instance_found || len(variant.value.protocol_instance_id) == 0 {
				process_error(&result, activation.run_id, "Workflow Update has no active fiber or protocol instance ID")
				return
			}
			context.allocator = virtual.arena_allocator(&decode_arena)
			invoke_workflow_update(instance, variant.value, &pending_update_responses)
			context.allocator = previous_allocator

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Workflow signal must have a name and one payload")
				return
			}
			context.allocator = instance.odin_context.allocator
			signal_error := invoke_workflow_signal(instance, variant.value, &result)
			context.allocator = previous_allocator
			if signal_error.message != nil {
				message := string(signal_error.message)
				process_error(&result, activation.run_id, message)
				error_free(&signal_error)
				return
			}
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
		resume_signals_error := resume_ready_signal_routines(instance)
		if resume_signals_error.message != nil {
			message := string(resume_signals_error.message)
			process_error(&result, activation.run_id, message)
			error_free(&resume_signals_error)
			return
		}
		finished_signals_error := finish_signal_routines(instance, &result)
		if finished_signals_error.message != nil {
			message := string(finished_signals_error.message)
			process_error(&result, activation.run_id, message)
			error_free(&finished_signals_error)
			return
		}
		resume_updates_error := resume_ready_update_routines(instance)
		if resume_updates_error.message != nil {
			message := string(resume_updates_error.message)
			process_error(&result, activation.run_id, message)
			error_free(&resume_updates_error)
			return
		}
		finished_updates_error := append_finished_update_routine_responses(
			instance,
			&pending_update_responses,
		)
		if finished_updates_error.message != nil {
			message := string(finished_updates_error.message)
			process_error(&result, activation.run_id, message)
			error_free(&finished_updates_error)
			return
		}

		should_resume := false
		switch instance.phase {
		case .Waiting_Activity:
			operation, found := find_activity_operation(instance, instance.waiting_activity_seq)
			should_resume = found && (operation.state == .Resolved || operation.state == .Failed ||
				operation.state == .Cancel_Requested || operation.state == .Canceled)
		case .Waiting_Child_Workflow_Start:
			operation, found := find_child_workflow_operation(instance, instance.waiting_child_workflow_seq)
			should_resume = found && operation.start_state != .Pending
		case .Waiting_Child_Workflow:
			operation, found := find_child_workflow_operation(instance, instance.waiting_child_workflow_seq)
			should_resume = found && operation.completion_state != .Pending
		case .Waiting_External_Signal:
			operation, found := find_external_signal_operation(instance, instance.waiting_external_signal_seq)
			should_resume = found && (operation.state == .Resolved || operation.state == .Failed ||
				operation.state == .Cancel_Requested || operation.state == .Canceled)
		case .Waiting_Timer:
			operation, found := find_timer_operation(instance, instance.waiting_timer_seq)
			should_resume = found && (operation.state == .Resolved ||
				operation.state == .Cancel_Requested || operation.state == .Canceled)
		case .Waiting_Signal:
			_, found := find_buffered_signal(instance, instance.waiting_signal_name)
			should_resume = found || root_cancellation_applies(instance)
		case .Waiting_Signal_Or_Timer:
			_, signal_found := find_buffered_signal(instance, instance.waiting_signal_name)
			timer, timer_found := find_timer_operation(instance, instance.waiting_timer_seq)
			should_resume = signal_found || root_cancellation_applies(instance) ||
				(timer_found && (timer.state == .Resolved || timer.state == .Cancel_Requested ||
				timer.state == .Canceled))
		case .Waiting_Handlers:
			should_resume = !has_active_handlers(instance) || root_cancellation_applies(instance)
		case .Ready, .Canceled, .Completed, .Failed:
		}
		if should_resume {
			instance.phase = .Ready
			instance.waiting_activity_seq = 0
			instance.waiting_child_workflow_seq = 0
			instance.waiting_external_signal_seq = 0
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
	for &update_response in pending_update_responses {
		append_update_response_command(&commands, &update_response)
		switch update_response.kind {
		case .Accepted:
			result.updates_accepted += 1
		case .Rejected:
			result.updates_rejected += 1
		case .Completed:
			result.updates_completed += 1
		}
	}
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
	for &query_response in pending_query_responses {
		append_query_response_command(&commands, &query_response)
		result.queries_answered += 1
		if query_response.result.status == .Failed {
			result.queries_failed += 1
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

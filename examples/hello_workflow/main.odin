package main

import "core:fmt"
import "core:os"
import "core:strings"
import "core:time"
import activity "../../temporal/activity"
import codec "../../temporal/codec"
import temporal_core "../../temporal/core"
import temporal_failure "../../temporal/failure"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-hello-workflow"
WORKFLOW_TYPE :: "odin_hello_workflow"
TYPED_WORKFLOW_TYPE :: "odin_typed_workflow"
LOCAL_ACTIVITY_WORKFLOW_TYPE :: "odin_local_activity_workflow"
LOCAL_ACTIVITY_RETRY_WORKFLOW_TYPE :: "odin_local_activity_retry_workflow"
FAILURE_DETAILS_WORKFLOW_TYPE :: "odin_failure_details_workflow"
TERMINAL_FAILURE_WORKFLOW_TYPE :: "odin_terminal_failure_workflow"
PATCH_WORKFLOW_TYPE :: "odin_patch_workflow"
WORKFLOW_PROPERTIES_TYPE :: "odin_workflow_properties"
SIGNAL_WITH_START_WORKFLOW_TYPE :: "odin_signal_with_start_workflow"
PATCH_ID :: "odin-sdk-live-patch-v1"

Typed_Greeting_Input :: struct {
	name:       string,
	salutation: string,
}

Typed_Greeting_Output :: struct {
	message:                        string,
	name_length:                    int,
	activity_id:                    string,
	activity_type:                  string,
	task_queue:                     string,
	namespace:                      string,
	workflow_id:                    string,
	workflow_type:                  string,
	attempt:                        u32,
	is_workflow_activity:           bool,
	has_deadline:                   bool,
	retry_maximum_attempts:         i32,
	heartbeat_timeout_milliseconds: i64,
}

Failure_Details :: struct {
	dependency: string,
	code:       int,
}

Activity_Cancellation_Details :: struct {
	name:               string,
	is_not_found:       bool,
	is_cancelled:       bool,
	is_paused:          bool,
	is_timed_out:       bool,
	is_worker_shutdown: bool,
	is_reset:           bool,
}

Greeting_Query_State :: struct {
	phase: string,
	name:  string,
}

Greeting_Query_Input :: struct {
	include_name: bool,
}

Greeting_Query_Output :: struct {
	phase: string,
	name:  string,
	caller: string,
}

Greeting_Update_Input :: struct {
	name: string,
}

Greeting_Update_Output :: struct {
	previous_name: string,
	current_name:  string,
	caller:        string,
}

greeting_status_query :: proc(
	query: ^workflow.Query_Context,
	state: Greeting_Query_State,
	input: Greeting_Query_Input,
) -> Greeting_Query_Output {
	result := Greeting_Query_Output{phase = state.phase}
	_, _ = workflow.query_header_json(query, "caller", &result.caller)
	if input.include_name {
		result.name = state.name
	}
	return result
}

validate_greeting_name :: proc(
	update: ^workflow.Update_Context,
	_: Greeting_Query_State,
	input: Greeting_Update_Input,
) -> workflow.Update_Validation {
	if len(input.name) == 0 || input.name == "reject" {
		return workflow.update_reject("greeting name is invalid", "InvalidGreetingName")
	}
	caller: string
	found, decode_error := workflow.update_header_json(update, "caller", &caller)
	defer delete(caller)
	if !found || decode_error != .None || caller != "odin-client-control" ||
	   len(update.id) == 0 || update.name != "set_greeting_name" || len(update.identity) == 0 {
		return workflow.update_reject("greeting Update context is invalid", "InvalidUpdateContext")
	}
	return workflow.update_accept()
}

update_greeting_name :: proc(
	ctx: ^workflow.Fiber_Context,
	update: ^workflow.Update_Context,
	state: ^Greeting_Query_State,
	input: Greeting_Update_Input,
) -> Greeting_Update_Output {
	previous := state.name
	caller: string
	_, _ = workflow.update_header_json(update, "caller", &caller)
	_ = workflow.execute_activity(ctx, "hello", input.name)
	current_update, update_present := workflow.current_update(ctx)
	if !update_present || current_update != update {
		caller = "Update context lost across suspension"
	}
	state.name = input.name
	return {previous_name = previous, current_name = state.name, caller = caller}
}

hold_greeting_name :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Greeting_Query_State,
	input: Greeting_Update_Input,
) -> Greeting_Update_Output {
	previous := state.name
	_ = workflow.await_signal(ctx, "release_held_update")
	state.name = input.name
	return {previous_name = previous, current_name = state.name}
}

update_greeting_name_detached :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Greeting_Query_State,
	input: Greeting_Update_Input,
) -> Greeting_Update_Output {
	scope := workflow.detached_cancellation_scope(ctx)
	if !scope.active {
		return {}
	}
	defer workflow.cancellation_scope_end(&scope)
	previous := state.name
	activity_result := workflow.execute_activity_result(ctx, "hello", input.name)
	if activity_result.status != .Succeeded {
		return {previous_name = previous, current_name = "detached Activity failed"}
	}
	state.name = input.name
	return {previous_name = previous, current_name = state.name}
}

set_greeting_name_from_signal :: proc(
	ctx: ^workflow.Fiber_Context,
	signal: ^workflow.Signal_Context,
	state: ^Greeting_Query_State,
	name: string,
) {
	activity_result := workflow.execute_activity_result(ctx, "hello", name)
	if activity_result.status == .Succeeded {
		caller: string
		found, decode_error := workflow.signal_header_json(signal, "caller", &caller)
		defer delete(caller)
		if found && decode_error == .None && caller == "odin-client-control" {
			state.name = name
		} else {
			state.name = "Signal context lost across suspension"
		}
	}
}

hello :: proc(ctx: ^activity.Context, name: string) -> string {
	if name == "activity-interceptor" {
		workflow_header: string
		found, decode_error := activity.header_json(
			ctx,
			"workflow-interceptor",
			&workflow_header,
		)
		defer delete(workflow_header)
		if !found || decode_error != .None || workflow_header != "odin-workflow-outbound" {
			return "Workflow interceptor header mismatch"
		}
		status := activity.heartbeat(ctx, "activity-interceptor-heartbeat")
		if status != .Recorded {
			return "Activity interceptor heartbeat failed"
		}
		return "Activity interceptor executed"
	}
	if name == "activity-header" {
		trace_chain: string
		found, decode_error := activity.header_json(ctx, "trace-chain", &trace_chain)
		defer delete(trace_chain)
		if !found || decode_error != .None || trace_chain != "odin-workflow" {
			return "Activity header mismatch"
		}
		return "Activity header propagated"
	}
	// The integration scenario deliberately resolves the second concurrent
	// Workflow Update first. Sleeping in an Activity is safe: the ordering is
	// recorded in history and Workflow replay only observes the resolutions.
	if name == "Temporal-slow" {
		time.sleep(2 * time.Second)
	}
	if name == "cancel-activity" || name == "cancel-cleanup" {
		first_heartbeat := true
		for !activity.is_cancellation_requested(ctx) {
			status := activity.heartbeat(ctx, "waiting for cancellation")
			if first_heartbeat && status == .Recorded {
				fmt.printf("%s heartbeat recorded\n", name)
				first_heartbeat = false
			}
			if status == .Cancellation_Requested {
				break
			}
			time.sleep(100 * time.Millisecond)
		}
		fmt.printf(
			"%s observed cancellation reason=%v\n",
			name,
			activity.cancellation_reason(ctx),
		)
		return "cancellation acknowledged"
	}
	greeting, allocation_error := strings.concatenate({"Hello ", name})
	if allocation_error != nil {
		return ""
	}
	return greeting
}

multiple_arguments_activity :: proc(
	_: ^activity.Context,
	inputs: []codec.Payload_View,
) -> activity.Payload_Result {
	if len(inputs) != 3 {
		return activity.payload_fail(
			"multiple_arguments expected exactly three Payloads",
			failure_type = "OdinActivityArgumentCountError",
		)
	}
	name: string
	count: int
	enabled: bool
	if codec.decode_json(inputs[0], &name) != .None ||
	   codec.decode_json(inputs[1], &count) != .None ||
	   codec.decode_json(inputs[2], &enabled) != .None {
		delete(name)
		return activity.payload_fail(
			"multiple_arguments could not decode its Payloads",
			failure_type = "OdinPayloadConversionError",
		)
	}
	defer delete(name)
	message := fmt.aprintf("%s:%d:%v", name, count, enabled)
	defer delete(message)
	payload, encode_error := codec.encode_json(message)
	if encode_error != .None {
		return activity.payload_fail(
			"multiple_arguments could not encode its result",
			failure_type = "OdinPayloadConversionError",
		)
	}
	return activity.payload_complete(payload)
}

activity_execute_interceptor :: proc(
	_: rawptr,
	input: ^worker.Activity_Execute_Input,
	next: worker.Activity_Execute_Next,
) -> activity.Payload_Result {
	name: string
	decode_error := codec.Error.Unsupported_Encoding
	if len(input.inputs) > 0 {
		decode_error = codec.decode_json(input.inputs[0], &name)
	}
	defer delete(name)
	is_probe := decode_error == .None && name == "activity-interceptor"
	if is_probe {
		fmt.println("Activity execute interceptor entered before callback")
	}
	result := worker.activity_execute_next(next, input)
	if is_probe {
		fmt.printf("Activity execute interceptor exited after callback status=%v\n", result.status)
	}
	return result
}

activity_heartbeat_interceptor :: proc(
	_: rawptr,
	input: ^worker.Activity_Heartbeat_Input,
	next: worker.Activity_Heartbeat_Next,
) -> activity.Heartbeat_Status {
	details: string
	decode_error := codec.decode_json(input.details, &details)
	defer delete(details)
	is_probe := decode_error == .None && details == "activity-interceptor-heartbeat"
	if is_probe {
		fmt.println("Activity heartbeat interceptor entered before Core")
	}
	status := worker.activity_heartbeat_next(next, input)
	if is_probe {
		fmt.printf("Activity heartbeat interceptor exited after Core status=%v\n", status)
	}
	return status
}

workflow_activity_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Outbound_Start_Activity_Input,
	next: workflow.Outbound_Start_Activity_Next,
) -> workflow.Activity_Future {
	name: string
	decode_error := codec.Error.Unsupported_Encoding
	if len(input.inputs) > 0 {
		decode_error = codec.decode_json(input.inputs[0], &name)
	}
	defer delete(name)
	is_probe := decode_error == .None && name == "activity-interceptor"
	if is_probe {
		fmt.println("Workflow Activity interceptor entered before command")
		if !workflow.headers_add_json(
			input.headers,
			"workflow-interceptor",
			"odin-workflow-outbound",
		) {
			return {}
		}
	}
	result := workflow.outbound_start_activity_next(next, input)
	if is_probe {
		fmt.println("Workflow Activity interceptor exited after command")
	}
	return result
}

workflow_execute_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Inbound_Execute_Workflow_Input,
	next: workflow.Inbound_Execute_Workflow_Next,
) -> workflow.Payload_Result {
	name: string
	decode_error := codec.decode_json(input.input, &name)
	defer delete(name)
	is_probe := decode_error == .None && name == "activity-interceptor"
	if is_probe {
		fmt.println("Workflow execute interceptor entered before Workflow")
	}
	result := workflow.inbound_execute_workflow_next(next, input)
	if is_probe {
		fmt.println("Workflow execute interceptor exited after Workflow")
	}
	return result
}

workflow_query_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Inbound_Handle_Query_Input,
	next: workflow.Inbound_Handle_Query_Next,
) -> workflow.Payload_Result {
	is_probe := input.query != nil && input.query.type == "greeting_status"
	if is_probe {
		fmt.println("Workflow Query interceptor entered before handler")
	}
	result := workflow.inbound_handle_query_next(next, input)
	if is_probe {
		fmt.println("Workflow Query interceptor exited after handler")
	}
	return result
}

workflow_signal_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Inbound_Handle_Signal_Input,
	next: workflow.Inbound_Handle_Signal_Next,
) -> workflow.Payload_Result {
	is_probe := input.signal != nil && input.signal.name == "set_greeting_name_async"
	if is_probe {
		fmt.println("Workflow Signal interceptor entered before handler")
	}
	result := workflow.inbound_handle_signal_next(next, input)
	if is_probe {
		caller: string
		found, decode_error := workflow.signal_header_json(input.signal, "caller", &caller)
		defer delete(caller)
		if !found || decode_error != .None || caller != "odin-client-control" {
			codec.payload_free(&result.payload)
			return workflow.payload_fail(
				"Workflow Signal interceptor context did not survive suspension",
			)
		}
		fmt.println("Workflow Signal interceptor exited after handler")
	}
	return result
}

workflow_update_validator_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Inbound_Handle_Update_Input,
	next: workflow.Inbound_Handle_Update_Validator_Next,
) -> workflow.Update_Validation {
	is_probe := input.update != nil && input.update.name == "set_greeting_name"
	if is_probe {
		fmt.println("Workflow Update validator interceptor entered")
	}
	result := workflow.inbound_handle_update_validator_next(next, input)
	if is_probe {
		fmt.println("Workflow Update validator interceptor exited")
	}
	return result
}

workflow_update_handler_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Inbound_Handle_Update_Input,
	next: workflow.Inbound_Handle_Update_Handler_Next,
) -> workflow.Payload_Result {
	is_probe := input.update != nil && input.update.name == "set_greeting_name"
	if is_probe {
		fmt.println("Workflow Update handler interceptor entered before handler")
	}
	result := workflow.inbound_handle_update_handler_next(next, input)
	if is_probe {
		fmt.println("Workflow Update handler interceptor exited after handler")
	}
	return result
}

cancel_with_details :: proc(
	ctx: ^activity.Context,
	name: string,
) -> activity.Result(string) {
	for !activity.is_cancellation_requested(ctx) {
		if activity.heartbeat(ctx, "waiting for detailed cancellation") == .Cancellation_Requested {
			break
		}
		time.sleep(50 * time.Millisecond)
	}
	details, present := activity.cancellation_details(ctx)
	if !present {
		return activity.result_failure(
			string,
			"Activity cancellation details were absent",
			failure_type = "OdinActivityCancellationContractError",
			non_retryable = true,
		)
	}
	return activity.result_canceled_json(
		string,
		Activity_Cancellation_Details{
			name = name,
			is_not_found = details.is_not_found,
			is_cancelled = details.is_cancelled,
			is_paused = details.is_paused,
			is_timed_out = details.is_timed_out,
			is_worker_shutdown = details.is_worker_shutdown,
			is_reset = details.is_reset,
		},
		message = "Odin Activity acknowledged cancellation",
	)
}

typed_greeting :: proc(ctx: ^activity.Context, input: Typed_Greeting_Input) -> Typed_Greeting_Output {
	message, allocation_error := strings.concatenate({input.salutation, ", ", input.name, "!"})
	if allocation_error != nil {
		return {}
	}
	info := activity.info(ctx)
	heartbeat_timeout_milliseconds := i64(0)
	if info.has_heartbeat_timeout {
		heartbeat_timeout_milliseconds = i64(info.heartbeat_timeout / time.Millisecond)
	}
	return {
		message = message,
		name_length = len(input.name),
		activity_id = info.activity_id,
		activity_type = info.activity_type,
		task_queue = info.task_queue,
		namespace = info.namespace,
		workflow_id = info.workflow_id,
		workflow_type = info.workflow_type,
		attempt = info.attempt,
		is_workflow_activity = activity.is_workflow_activity(ctx),
		has_deadline = info.has_deadline,
		retry_maximum_attempts = info.retry_policy.maximum_attempts,
		heartbeat_timeout_milliseconds = heartbeat_timeout_milliseconds,
	}
}

failure_details_activity :: proc(
	_: ^activity.Context,
	name: string,
) -> activity.Result(string) {
	_ = name
	return activity.result_failure_json(
		string,
		"billing dependency rejected request",
		Failure_Details{dependency = "billing", code = 409},
		failure_type = "DependencyRejected",
		next_retry_delay = 250 * time.Millisecond,
	)
}

local_retry_activity :: proc(ctx: ^activity.Context, name: string) -> activity.Result(string) {
	if activity.info(ctx).attempt == 1 {
		return activity.result_failure(
			string,
			"retry the Local Activity through a Workflow timer",
			failure_type = "TransientLocalFailure",
			next_retry_delay = 250 * time.Millisecond,
		)
	}
	greeting, allocation_error := strings.concatenate({"Hello after Local Activity retry, ", name})
	if allocation_error != nil {
		return activity.result_failure(string, "could not allocate retry greeting")
	}
	return activity.result_success(greeting)
}

hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	if name == "multiple-activity-arguments" {
		name_payload, name_error := codec.encode_json("Odin")
		if name_error != .None do return "could not encode first Activity argument"
		defer codec.payload_free(&name_payload)
		count_payload, count_error := codec.encode_json(42)
		if count_error != .None do return "could not encode second Activity argument"
		defer codec.payload_free(&count_payload)
		enabled_payload, enabled_error := codec.encode_json(true)
		if enabled_error != .None do return "could not encode third Activity argument"
		defer codec.payload_free(&enabled_payload)
		inputs := [3]codec.Payload_View{
			codec.payload_view(&name_payload),
			codec.payload_view(&count_payload),
			codec.payload_view(&enabled_payload),
		}
		result := workflow.execute_activity_payloads(
			ctx,
			"multiple_arguments",
			inputs[:],
		)
		message: string
		if codec.decode_json(result, &message) != .None {
			return "could not decode multi-argument Activity result"
		}
		return message
	}
	if name == "activity-interceptor" {
		return workflow.execute_activity(ctx, "hello", name)
	}
	if name == "activity-header" {
		headers := workflow.Headers{}
		if !workflow.headers_init(&headers) {
			return "could not initialize Activity headers"
		}
		defer workflow.headers_free(&headers)
		if !workflow.headers_add_json(&headers, "trace-chain", "odin-workflow") {
			return "could not encode Activity header"
		}
		return workflow.execute_activity_with_options(
			ctx,
			"hello",
			name,
			{
				activity_id = "activity-header",
				start_to_close_timeout = 10 * time.Second,
				headers = &headers,
			},
		)
	}
	if name == "activity-cancel-details" {
		retry_policy := workflow.Retry_Policy{maximum_attempts = 1}
		pending := workflow.start_activity_json_with_options(
			ctx,
			"cancel_with_details",
			name,
			{
				activity_id = "activity-cancel-details",
				start_to_close_timeout = 10 * time.Second,
				heartbeat_timeout = time.Second,
				retry_policy = &retry_policy,
				cancellation_type = .Wait_Cancellation_Completed,
			},
		)
		workflow.sleep(ctx, 250 * time.Millisecond)
		if !workflow.cancel_activity(ctx, pending) {
			return "could not request Activity cancellation"
		}
		result := workflow.await_activity_json_result(ctx, pending, string)
		if result.status != .Canceled || !result.failure.canceled {
			return "Activity did not return a cancellation failure"
		}
		details: Activity_Cancellation_Details
		found, decode_error := workflow.activity_cancellation_detail_json(
			&result.failure,
			0,
			&details,
		)
		if !found || decode_error != .None || details.name != name || !details.is_cancelled ||
		   details.is_not_found || details.is_paused || details.is_timed_out ||
		   details.is_worker_shutdown || details.is_reset {
			return "Activity cancellation details mismatch"
		}
		return "Observed Activity cancellation details"
	}
	if name == "cancel-activity" {
		return workflow.execute_activity(ctx, "hello", name)
	}
	if name == "cancel-cleanup" {
		pending := workflow.execute_activity_result(ctx, "hello", name)
		if pending.status != .Canceled || !workflow.is_cancellation_requested(ctx) ||
		   workflow.cancellation_reason(ctx) != "Odin Client integration cancellation" {
			return "cancellation was not delivered to Workflow code"
		}
		scope := workflow.detached_cancellation_scope(ctx)
		if !scope.active {
			return "could not open detached cancellation scope"
		}
		defer workflow.cancellation_scope_end(&scope)
		cleanup := workflow.execute_activity_result(ctx, "hello", "cleanup")
		if cleanup.status != .Succeeded || cleanup.value != "Hello cleanup" ||
		   workflow.cancellation_reason(ctx) != "Odin Client integration cancellation" {
			return "cleanup Activity did not complete"
		}
		return "cleanup completed before cancellation"
	}

	query_state := Greeting_Query_State{phase = "waiting-for-approval", name = name}
	if !workflow.set_query_handler_json_with_context(
		ctx,
		"greeting_status",
		&query_state,
		greeting_status_query,
	) {
		return "could not register greeting status query"
	}
	if !workflow.set_validated_update_routine_json_with_context(
		ctx,
		"set_greeting_name",
		&query_state,
		validate_greeting_name,
		update_greeting_name,
	) {
		return "could not register greeting name Update"
	}
	if !workflow.set_update_routine_json(
		ctx,
		"hold_greeting_name",
		&query_state,
		hold_greeting_name,
	) {
		return "could not register held greeting name Update"
	}
	if !workflow.set_update_routine_json(
		ctx,
		"set_greeting_name_detached",
		&query_state,
		update_greeting_name_detached,
	) {
		return "could not register detached greeting name Update"
	}
	if !workflow.set_signal_routine_json_with_context(
		ctx,
		"set_greeting_name_async",
		&query_state,
		set_greeting_name_from_signal,
	) {
		return "could not register asynchronous greeting Signal"
	}
	approval := workflow.await_signal_or_timeout(ctx, "approval", 30 * time.Second)
	if approval.status == .Timed_Out {
		return "Approval timed out"
	}
	if name == "signal-header" {
		caller: string
		found, decode_error := workflow.signal_header_json(
			&approval.metadata,
			"caller",
			&caller,
		)
		defer delete(caller)
		if !found || decode_error != .None || caller != "odin-client-control" {
			return "Signal header mismatch"
		}
	}
	if approval.status == .Canceled && name == "cancel-update" {
		scope := workflow.detached_cancellation_scope(ctx)
		if !scope.active {
			return "could not detach Update drain"
		}
		defer workflow.cancellation_scope_end(&scope)
		workflow.await_all_handlers_finished(ctx)
		return "Update handlers drained"
	}
	query_state.phase = "running-activity"
	return workflow.execute_activity(ctx, "hello", approval.value)
}

typed_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Typed_Greeting_Input,
) -> Typed_Greeting_Output {
	non_retryable_types := [?]string{"InvalidGreeting"}
	retry_policy := workflow.Retry_Policy{
		initial_interval = 100 * time.Millisecond,
		backoff_coefficient = 2,
		maximum_interval = time.Second,
		maximum_attempts = 3,
		non_retryable_error_types = non_retryable_types[:],
	}
	return workflow.execute_activity_json_with_options(
		ctx,
		"typed_greeting",
		input,
		Typed_Greeting_Output,
		{
			activity_id = "typed-greeting-options",
			task_queue = TASK_QUEUE,
			schedule_to_close_timeout = 30 * time.Second,
			schedule_to_start_timeout = 5 * time.Second,
			start_to_close_timeout = 10 * time.Second,
			heartbeat_timeout = 2 * time.Second,
			retry_policy = &retry_policy,
			cancellation_type = .Wait_Cancellation_Completed,
			disable_eager_execution = true,
		},
	)
}

local_activity_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	name: string,
) -> string {
	retry_policy := workflow.Retry_Policy{maximum_attempts = 1}
	return workflow.execute_local_activity_with_options(
		ctx,
		"hello",
		name,
		{
			activity_id = "local-hello",
			schedule_to_close_timeout = 30 * time.Second,
			start_to_close_timeout = 10 * time.Second,
			retry_policy = &retry_policy,
			local_retry_threshold = 2 * time.Second,
			cancellation_type = .Wait_Cancellation_Completed,
			include_arguments_in_marker = true,
		},
	)
}

local_activity_retry_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	name: string,
) -> string {
	retry_policy := workflow.Retry_Policy{
		initial_interval = 250 * time.Millisecond,
		backoff_coefficient = 2,
		maximum_interval = time.Second,
		maximum_attempts = 2,
	}
	return workflow.execute_local_activity_with_options(
		ctx,
		"local_retry",
		name,
		{
			activity_id = "local-retry",
			schedule_to_close_timeout = 30 * time.Second,
			start_to_close_timeout = 10 * time.Second,
			retry_policy = &retry_policy,
			local_retry_threshold = 10 * time.Millisecond,
			include_arguments_in_marker = true,
		},
	)
}

failure_details_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	retry_policy := workflow.Retry_Policy{maximum_attempts = 1}
	result := workflow.execute_activity_result_with_options(
		ctx,
		"failure_details",
		name,
		{
			activity_id = "failure-details-activity",
			start_to_close_timeout = 10 * time.Second,
			retry_policy = &retry_policy,
		},
	)
	if result.status != .Failed {
		return "unexpected Activity success"
	}
	details: Failure_Details
	detail_found, detail_error := workflow.activity_failure_detail_json(
		&result.failure,
		0,
		&details,
	)
	application := result.failure.application
	failure_tree := workflow.activity_failure_tree(&result.failure)
	activity_node := temporal_failure.find_kind(failure_tree, .Activity)
	application_node := temporal_failure.find_kind(failure_tree, .Application)
	if result.failure.activity_id != "failure-details-activity" ||
	   result.failure.activity_type != "failure_details" ||
	   result.failure.retry_state != .Maximum_Attempts_Reached ||
	   !application.present || application.message != "billing dependency rejected request" ||
	   application.type != "DependencyRejected" || application.non_retryable ||
	   !application.has_next_retry_delay || application.next_retry_delay != 250 * time.Millisecond ||
	   !detail_found || detail_error != .None || details.dependency != "billing" || details.code != 409 ||
	   failure_tree == nil || activity_node == nil || application_node == nil ||
	   activity_node.activity.activity_id != "failure-details-activity" ||
	   application_node.application.type != "DependencyRejected" {
		return "structured Activity failure mismatch"
	}
	return "Observed structured Activity failure"
}

terminal_failure_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	retry_policy := workflow.Retry_Policy{maximum_attempts = 1}
	return workflow.execute_activity_with_options(
		ctx,
		"failure_details",
		name,
		{
			activity_id = "terminal-failure-activity",
			start_to_close_timeout = 10 * time.Second,
			retry_policy = &retry_policy,
		},
	)
}

patch_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	if workflow.patched(ctx, PATCH_ID) {
		activation_time, has_time := workflow.now(ctx)
		_ = activation_time
		identifier := workflow.uuid_v4(ctx)
		random_fraction := workflow.random_f64(ctx)
		if !has_time || workflow.history_length(ctx) == 0 || len(identifier) != 36 ||
		   random_fraction < 0 || random_fraction >= 1 {
			return "invalid deterministic Workflow context"
		}
		result, allocation_error := strings.concatenate({"Patched ", name, " ", identifier})
		if allocation_error != nil {
			return ""
		}
		return result
	}
	result, allocation_error := strings.concatenate({"Legacy ", name})
	if allocation_error != nil {
		return ""
	}
	return result
}

workflow_properties :: proc(ctx: ^workflow.Fiber_Context, expected_workflow_id: string) -> string {
	metadata, metadata_found := workflow.info(ctx)
	if !metadata_found || metadata.namespace != "default" ||
	   metadata.task_queue != TASK_QUEUE || metadata.workflow_id != expected_workflow_id ||
	   metadata.workflow_type != WORKFLOW_PROPERTIES_TYPE || metadata.attempt != 1 ||
	   len(metadata.run_id) == 0 || metadata.cron_schedule != "" ||
	   metadata.retry_policy.initial_interval != 2 * time.Second ||
	   metadata.retry_policy.backoff_coefficient != 2 ||
	   metadata.retry_policy.maximum_interval != 10 * time.Second ||
	   metadata.retry_policy.maximum_attempts != 4 ||
	   len(metadata.retry_policy.non_retryable_error_types) != 1 ||
	   metadata.retry_policy.non_retryable_error_types[0] != "DoNotRetry" ||
	   metadata.priority.priority_key != 2 || metadata.priority.fairness_key != "tenant-odin" ||
	   metadata.priority.fairness_weight != 3 {
		return "Workflow info mismatch"
	}
	initial_header: string
	header_found, header_error := workflow.header_json(ctx, "odin-header", &initial_header)
	interceptor_header: string
	interceptor_found, interceptor_error := workflow.header_json(
		ctx,
		"odin-interceptor",
		&interceptor_header,
	)
	initial_memo: string
	memo_found, memo_error := workflow.memo_json(ctx, "odin-start", &initial_memo)
	initial_search: string
	search_found, search_error := workflow.search_attribute_json(
		ctx,
		"OdinSdkKeyword",
		&initial_search,
	)
	if !header_found || header_error != .None || initial_header != "client-header" ||
	   !interceptor_found || interceptor_error != .None ||
	   interceptor_header != "odin-client-start" ||
	   !memo_found || memo_error != .None || initial_memo != "before" ||
	   !search_found || search_error != .None || initial_search != "before" {
		return "Initial Workflow properties mismatch"
	}
	if !workflow.upsert_memo_json(ctx, "odin-status", "after") ||
	   !workflow.remove_memo(ctx, "odin-start") ||
	   !workflow.upsert_search_attribute_json(
		ctx,
		"OdinSdkKeyword",
		.Keyword,
		"after",
	) {
		return "Workflow property mutation failed"
	}
	updated_memo: string
	updated_memo_found, updated_memo_error := workflow.memo_json(
		ctx,
		"odin-status",
		&updated_memo,
	)
	_, removed_memo_found := workflow.memo_payload(ctx, "odin-start")
	updated_search: string
	updated_search_found, updated_search_error := workflow.search_attribute_json(
		ctx,
		"OdinSdkKeyword",
		&updated_search,
	)
	if !updated_memo_found || updated_memo_error != .None || updated_memo != "after" ||
	   removed_memo_found || !updated_search_found || updated_search_error != .None ||
	   updated_search != "after" {
		return "Local Workflow property view mismatch"
	}
	return "Workflow properties updated"
}

signal_with_start_workflow :: proc(ctx: ^workflow.Fiber_Context, seed: string) -> string {
	first := workflow.await_signal_json_with_context(ctx, "append", string)
	first_caller: string
	first_found, first_error := workflow.signal_header_json(
		&first.metadata,
		"caller",
		&first_caller,
	)
	first_interceptor: string
	first_interceptor_found, first_interceptor_error := workflow.signal_header_json(
		&first.metadata,
		"odin-signal-with-start-interceptor",
		&first_interceptor,
	)
	second := workflow.await_signal_json_with_context(ctx, "append", string)
	second_caller: string
	second_found, second_error := workflow.signal_header_json(
		&second.metadata,
		"caller",
		&second_caller,
	)
	second_interceptor: string
	second_interceptor_found, second_interceptor_error := workflow.signal_header_json(
		&second.metadata,
		"odin-signal-with-start-interceptor",
		&second_interceptor,
	)
	if !first_found || first_error != .None || !second_found || second_error != .None {
		return "Signal-With-Start metadata missing"
	}
	if !first_interceptor_found || first_interceptor_error != .None ||
	   first_interceptor != "odin-client-signal-with-start" ||
	   !second_interceptor_found || second_interceptor_error != .None ||
	   second_interceptor != "odin-client-signal-with-start" {
		return "Signal-With-Start interceptor metadata missing"
	}
	result, allocation_error := strings.concatenate(
		{
			seed,
			":",
			first.value,
			"/",
			first_caller,
			":",
			second.value,
			"/",
			second_caller,
		},
	)
	if allocation_error != nil {
		return ""
	}
	return result
}

observe_activation :: proc(processed: ^workflow.Process_Result) {
	error_message := ""
	if processed.error.message != nil {
		error_message = string(processed.error.message)
	}
	fmt.printf(
		"activation history=%d replay=%v activation_failed=%v error=%q scheduled=%d local_scheduled=%d last_activity=%d resolved=%d failed=%v signal=%v signal_handlers_started=%d signal_handlers_completed=%d unfinished_signals=%d signal_warnings=%d queries=%d query_failures=%d updates_accepted=%d updates_rejected=%d updates_completed=%d unfinished_updates=%d update_warnings=%d patches=%d deprecated_patches=%d memo_upserts=%d search_upserts=%d cancel=%v canceled_workflow=%v started_timer=%v canceled_timer=%v canceled_activity=%v waiting_activity=%d waiting_timer=%d waiting_signal=%v waiting_handlers=%v completed=%v evicted=%v\n",
		processed.history_length,
		processed.is_replaying,
		processed.activation_failed,
		error_message,
		processed.scheduled_activity_count,
		processed.scheduled_local_activity_count,
		processed.activity_seq,
		processed.resolved_activity_seq,
		processed.resolved_activity_failed,
		processed.received_signal,
		processed.signal_handlers_started,
		processed.signal_handlers_completed,
		processed.unfinished_signal_handlers,
		processed.warned_unfinished_signal_handlers,
		processed.queries_answered,
		processed.queries_failed,
		processed.updates_accepted,
		processed.updates_rejected,
		processed.updates_completed,
		processed.unfinished_update_handlers,
		processed.warned_unfinished_update_handlers,
		processed.patch_markers_emitted,
		processed.deprecated_patch_markers_emitted,
		processed.memo_upserts,
		processed.search_attribute_upserts,
		processed.received_cancellation,
		processed.canceled_workflow,
		processed.started_timer,
		processed.canceled_timer,
		processed.requested_activity_cancellation,
		processed.waiting_activity_seq,
		processed.waiting_timer_seq,
		processed.waiting_signal,
		processed.waiting_handlers,
		processed.completed,
		processed.evicted,
	)
}

main :: proc() {
	runtime, runtime_error := temporal_core.runtime_create()
	if runtime_error.message != nil {
		fmt.eprintf("Core runtime initialization failed: %s\n", string(runtime_error.message))
		temporal_core.error_free(&runtime_error)
		os.exit(1)
	}

	client, connect_error := temporal_core.client_connect(&runtime)
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal_core.error_free(&connect_error)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}

	activity_worker, create_error := worker.create(
		&client,
		TASK_QUEUE,
	)
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	interceptor_error := worker.add_activity_interceptor(
		&activity_worker,
		{
			execute = activity_execute_interceptor,
			heartbeat = activity_heartbeat_interceptor,
		},
	)
	if interceptor_error.message != nil {
		fmt.eprintf(
			"Activity interceptor registration failed: %s\n",
			string(interceptor_error.message),
		)
		worker.error_free(&interceptor_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	workflow_interceptor_error := worker.add_workflow_interceptor(
		&activity_worker,
		{start_activity = workflow_activity_interceptor},
	)
	if workflow_interceptor_error.message != nil {
		fmt.eprintf(
			"Workflow interceptor registration failed: %s\n",
			string(workflow_interceptor_error.message),
		)
		worker.error_free(&workflow_interceptor_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	workflow_inbound_interceptor_error := worker.add_workflow_inbound_interceptor(
		&activity_worker,
		{
			execute_workflow = workflow_execute_interceptor,
			handle_query = workflow_query_interceptor,
			handle_signal = workflow_signal_interceptor,
			handle_update_validator = workflow_update_validator_interceptor,
			handle_update_handler = workflow_update_handler_interceptor,
		},
	)
	if workflow_inbound_interceptor_error.message != nil {
		fmt.eprintf(
			"Workflow inbound interceptor registration failed: %s\n",
			string(workflow_inbound_interceptor_error.message),
		)
		worker.error_free(&workflow_inbound_interceptor_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}

	register_multiple_arguments_error := worker.register_payloads_activity(
		&activity_worker,
		"multiple_arguments",
		multiple_arguments_activity,
	)
	if register_multiple_arguments_error.message != nil {
		fmt.eprintf(
			"Multi-argument Activity registration failed: %s\n",
			string(register_multiple_arguments_error.message),
		)
		worker.error_free(&register_multiple_arguments_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	register_error := worker.register_activity(&activity_worker, "hello", hello)
	if register_error.message != nil {
		fmt.eprintf("Activity registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	register_typed_activity_error := worker.register_json_activity(
		&activity_worker,
		"typed_greeting",
		typed_greeting,
	)
	if register_typed_activity_error.message != nil {
		fmt.eprintf("Typed Activity registration failed: %s\n", string(register_typed_activity_error.message))
		worker.error_free(&register_typed_activity_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	register_failure_activity_error := worker.register_json_activity_result(
		&activity_worker,
		"failure_details",
		failure_details_activity,
	)
	if register_failure_activity_error.message != nil {
		fmt.eprintf("Failure Activity registration failed: %s\n", string(register_failure_activity_error.message))
		worker.error_free(&register_failure_activity_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	register_cancel_details_error := worker.register_json_activity_result(
		&activity_worker,
		"cancel_with_details",
		cancel_with_details,
	)
	if register_cancel_details_error.message != nil {
		fmt.eprintf(
			"Cancellation-details Activity registration failed: %s\n",
			string(register_cancel_details_error.message),
		)
		worker.error_free(&register_cancel_details_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	register_local_retry_error := worker.register_json_activity_result(
		&activity_worker,
		"local_retry",
		local_retry_activity,
	)
	if register_local_retry_error.message != nil {
		fmt.eprintf("Retrying Local Activity registration failed: %s\n", string(register_local_retry_error.message))
		worker.error_free(&register_local_retry_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}

	workflow_register_error := worker.register_workflow(
		&activity_worker,
		WORKFLOW_TYPE,
		hello_workflow,
	)
	if workflow_register_error.message != nil {
		fmt.eprintf("Workflow registration failed: %s\n", string(workflow_register_error.message))
		worker.error_free(&workflow_register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	typed_workflow_register_error := worker.register_json_workflow(
		&activity_worker,
		TYPED_WORKFLOW_TYPE,
		typed_workflow,
	)
	if typed_workflow_register_error.message != nil {
		fmt.eprintf("Typed Workflow registration failed: %s\n", string(typed_workflow_register_error.message))
		worker.error_free(&typed_workflow_register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	local_activity_workflow_register_error := worker.register_workflow(
		&activity_worker,
		LOCAL_ACTIVITY_WORKFLOW_TYPE,
		local_activity_workflow,
	)
	if local_activity_workflow_register_error.message != nil {
		fmt.eprintf("Local Activity Workflow registration failed: %s\n", string(local_activity_workflow_register_error.message))
		worker.error_free(&local_activity_workflow_register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	local_activity_retry_workflow_register_error := worker.register_workflow(
		&activity_worker,
		LOCAL_ACTIVITY_RETRY_WORKFLOW_TYPE,
		local_activity_retry_workflow,
	)
	if local_activity_retry_workflow_register_error.message != nil {
		fmt.eprintf("Retrying Local Activity Workflow registration failed: %s\n", string(local_activity_retry_workflow_register_error.message))
		worker.error_free(&local_activity_retry_workflow_register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	failure_workflow_register_error := worker.register_workflow(
		&activity_worker,
		FAILURE_DETAILS_WORKFLOW_TYPE,
		failure_details_workflow,
	)
	if failure_workflow_register_error.message != nil {
		fmt.eprintf("Failure Workflow registration failed: %s\n", string(failure_workflow_register_error.message))
		worker.error_free(&failure_workflow_register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	terminal_failure_workflow_register_error := worker.register_workflow(
		&activity_worker,
		TERMINAL_FAILURE_WORKFLOW_TYPE,
		terminal_failure_workflow,
	)
	if terminal_failure_workflow_register_error.message != nil {
		fmt.eprintf("Terminal Failure Workflow registration failed: %s\n", string(terminal_failure_workflow_register_error.message))
		worker.error_free(&terminal_failure_workflow_register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	patch_workflow_register_error := worker.register_workflow(
		&activity_worker,
		PATCH_WORKFLOW_TYPE,
		patch_workflow,
	)
	if patch_workflow_register_error.message != nil {
		fmt.eprintf("Patch Workflow registration failed: %s\n", string(patch_workflow_register_error.message))
		worker.error_free(&patch_workflow_register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	properties_workflow_register_error := worker.register_workflow(
		&activity_worker,
		WORKFLOW_PROPERTIES_TYPE,
		workflow_properties,
	)
	if properties_workflow_register_error.message != nil {
		fmt.eprintf(
			"Workflow properties registration failed: %s\n",
			string(properties_workflow_register_error.message),
		)
		worker.error_free(&properties_workflow_register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}
	signal_with_start_register_error := worker.register_workflow(
		&activity_worker,
		SIGNAL_WITH_START_WORKFLOW_TYPE,
		signal_with_start_workflow,
	)
	if signal_with_start_register_error.message != nil {
		fmt.eprintf(
			"Signal-With-Start Workflow registration failed: %s\n",
			string(signal_with_start_register_error.message),
		)
		worker.error_free(&signal_with_start_register_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}

	fmt.printf("Odin Workflow Worker is polling task queue %s\n", TASK_QUEUE)
	env_buffer: [64]u8
	scenario := os.get_env(env_buffer[:], "TEMPORAL_ODIN_SCENARIO")
	expect_cancel_timer := scenario == "cancel-timer"
	expect_cancel_activity := scenario == "cancel-activity"
	expect_cancel_cleanup := scenario == "cancel-cleanup"
	expect_update := scenario == "update"
	expect_abandon_update := scenario == "abandon-update"
	expect_cancel_update := scenario == "cancel-update"
	expect_signal_handler := scenario == "signal-handler"
	expect_query := scenario == "query" || expect_update || expect_signal_handler
	expect_typed_payload := scenario == "typed-payload"
	expect_local_activity := scenario == "local-activity"
	expect_local_activity_retry := scenario == "local-activity-retry"
	expect_failure_details := scenario == "failure-details"
	expect_terminal_failure := scenario == "terminal-failure"
	expect_patch := scenario == "patch"
	expect_properties := scenario == "workflow-properties"
	expect_signal_with_start := scenario == "signal-with-start"
	expect_activity_cancel_details := scenario == "activity-cancel-details"
	expect_activity_headers := scenario == "activity-headers"
	expect_activity_interceptor := scenario == "activity-interceptor"
	expect_multiple_activity_arguments := scenario == "multiple-activity-arguments"
	expect_cancel := expect_cancel_timer || expect_cancel_activity || expect_cancel_cleanup ||
		expect_activity_cancel_details
	run_stats, run_error := worker.run(
		&activity_worker,
		{
			max_activity_tasks = expect_cancel ? 0 : (expect_update ? 3 : ((expect_signal_handler || expect_local_activity_retry) ? 2 : 1)),
			max_workflow_executions = 1,
			disable_activity_polling = expect_cancel_timer || expect_patch || expect_properties || expect_signal_with_start,
			enable_local_activities = expect_local_activity || expect_local_activity_retry,
			on_workflow_activation = observe_activation,
		},
	)
	failed := false
	if run_error.message != nil {
		fmt.eprintf("Worker run failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		failed = true
	}
	if !failed && run_stats.workflow_executions_completed != 1 {
		fmt.eprintln("Workflow worker did not complete exactly one Workflow Execution")
		failed = true
	}
	if !failed && run_stats.workflow_evictions < 1 {
		fmt.eprintln("Workflow worker did not observe final cache eviction")
		failed = true
	}
	if !failed {
		if expect_cancel_timer {
			if run_stats.activity_tasks_completed != 0 || run_stats.workflow_signals_received != 0 || run_stats.workflow_cancellations_received != 1 || run_stats.timers_canceled != 1 || run_stats.activity_cancellations_requested != 0 {
				fmt.eprintln("Workflow spike did not cancel its pending timer and Workflow Execution")
				failed = true
			}
		} else if expect_cancel_activity {
			if run_stats.activity_tasks_completed != 1 || run_stats.activity_cancellations_received != 1 || run_stats.workflow_signals_received != 0 || run_stats.workflow_cancellations_received != 1 || run_stats.timers_canceled != 0 || run_stats.activity_cancellations_requested != 1 {
				fmt.eprintln("Workflow spike did not deliver and acknowledge Activity cancellation")
				failed = true
			}
		} else if expect_cancel_cleanup {
			if run_stats.activity_tasks_completed != 2 || run_stats.activity_cancellations_received != 1 || run_stats.workflow_signals_received != 0 || run_stats.workflow_cancellations_received != 1 || run_stats.timers_canceled != 0 || run_stats.activity_cancellations_requested != 1 {
				fmt.eprintln("Workflow spike did not complete detached cancellation cleanup")
				failed = true
			}
		} else if expect_patch || expect_properties || expect_signal_with_start {
			expected_signals := 0
			if expect_signal_with_start {
				expected_signals = 2
			}
			if run_stats.activity_tasks_completed != 0 || run_stats.workflow_signals_received != expected_signals ||
			   run_stats.timers_canceled != 0 {
				fmt.eprintln("Command-only Workflow path produced unexpected worker stats")
				failed = true
			}
		} else if expect_local_activity_retry {
			if run_stats.activity_tasks_completed != 2 || run_stats.workflow_signals_received != 0 ||
			   run_stats.timers_canceled != 0 {
				fmt.eprintln("Retrying Local Activity Workflow path produced unexpected worker stats")
				failed = true
			}
		} else if expect_typed_payload || expect_failure_details || expect_terminal_failure ||
		           expect_local_activity || expect_activity_headers || expect_activity_interceptor ||
		           expect_multiple_activity_arguments {
			if run_stats.activity_tasks_completed != 1 || run_stats.workflow_signals_received != 0 || run_stats.timers_canceled != 0 {
				fmt.eprintln("Single-Activity Workflow path produced unexpected worker stats")
				failed = true
			}
		} else if expect_abandon_update {
			if run_stats.activity_tasks_completed != 1 || run_stats.workflow_signals_received != 1 ||
			   run_stats.timers_canceled != 1 || run_stats.workflow_updates_accepted != 1 ||
			   run_stats.workflow_updates_completed != 0 ||
			   run_stats.workflow_update_handlers_abandoned != 1 ||
			   run_stats.workflow_update_handler_warnings != 1 {
				fmt.eprintln("Workflow Update abandonment stats did not match")
				failed = true
			}
		} else if expect_cancel_update {
			if run_stats.activity_tasks_completed != 1 || run_stats.workflow_signals_received != 0 ||
			   run_stats.workflow_cancellations_received != 1 || run_stats.timers_canceled != 1 ||
			   run_stats.activity_cancellations_requested != 0 ||
			   run_stats.workflow_updates_accepted != 1 ||
			   run_stats.workflow_updates_completed != 1 ||
			   run_stats.workflow_update_handlers_abandoned != 0 ||
			   run_stats.workflow_update_handler_warnings != 0 {
				fmt.eprintln("Detached Workflow Update cancellation stats did not match")
				failed = true
			}
		} else if expect_signal_handler {
			if run_stats.activity_tasks_completed != 2 || run_stats.workflow_signals_received != 2 ||
			   run_stats.workflow_signal_handlers_started != 1 ||
			   run_stats.workflow_signal_handlers_completed != 1 ||
			   run_stats.workflow_signal_handlers_abandoned != 0 ||
			   run_stats.workflow_signal_handler_warnings != 0 ||
			   run_stats.workflow_queries_answered != 1 || run_stats.timers_canceled != 1 {
				fmt.eprintln("Asynchronous Workflow Signal handler stats did not match")
				failed = true
			}
		} else if expect_activity_cancel_details {
			if run_stats.activity_tasks_completed != 1 ||
			   run_stats.activity_cancellations_received != 1 ||
			   run_stats.activity_cancellations_requested != 1 ||
			   run_stats.workflow_executions_completed != 1 {
				fmt.eprintln("Activity cancellation-details stats did not match")
				failed = true
			}
		} else if run_stats.activity_tasks_completed != (expect_update ? 3 : 1) || run_stats.workflow_signals_received != 1 || run_stats.timers_canceled != 1 || run_stats.workflow_replay_activations < 1 ||
		   (expect_query && (run_stats.workflow_queries_answered != 1 || run_stats.workflow_queries_failed != 0)) ||
		   (expect_update && (run_stats.workflow_updates_accepted != 2 || run_stats.workflow_updates_rejected != 1 || run_stats.workflow_updates_completed != 2)) {
			fmt.eprintf(
				"Workflow spike stats mismatch: activities=%d signals=%d queries=%d query_failures=%d updates_accepted=%d updates_rejected=%d updates_completed=%d timers_canceled=%d replay_activations=%d\n",
				run_stats.activity_tasks_completed,
				run_stats.workflow_signals_received,
				run_stats.workflow_queries_answered,
				run_stats.workflow_queries_failed,
				run_stats.workflow_updates_accepted,
				run_stats.workflow_updates_rejected,
				run_stats.workflow_updates_completed,
				run_stats.timers_canceled,
				run_stats.workflow_replay_activations,
			)
			failed = true
		}
	}

	close_error := worker.close(&activity_worker)
	if close_error.message != nil {
		fmt.eprintf("Worker shutdown failed: %s\n", string(close_error.message))
		worker.error_free(&close_error)
		failed = true
	}
	temporal_core.client_close(&client)
	temporal_core.runtime_close(&runtime)

	if failed {
		os.exit(1)
	}
	if expect_cancel_timer {
		fmt.println("Workflow cancellation canceled the pending timer and closed the execution")
	} else if expect_cancel_activity {
		fmt.println("Activity heartbeat delivered cancellation and Odin acknowledged it")
	} else if expect_cancel_cleanup {
		fmt.println("Workflow ran detached cleanup before acknowledging cancellation")
	} else if expect_typed_payload {
		fmt.println("typed Workflow and Activity Payloads completed end-to-end")
	} else if expect_multiple_activity_arguments {
		fmt.println("multiple Activity arguments completed end-to-end through Core")
	} else if expect_local_activity {
		fmt.println("Local Activity completed through Core in the Workflow Worker")
	} else if expect_local_activity_retry {
		fmt.println("Local Activity timer-backed retry completed through Core and Temporal Server")
	} else if expect_failure_details {
		fmt.println("structured Activity failure details reached Workflow code")
	} else if expect_terminal_failure {
		fmt.println("unhandled Activity failure reached the Client as a recursive Temporal Failure")
	} else if expect_activity_cancel_details {
		fmt.println("Activity cancellation flags and user details completed end-to-end")
	} else if expect_activity_headers {
		fmt.println("Activity header crossed Workflow command, Core, and Activity Context")
	} else if expect_patch {
		fmt.println("Workflow patch marker completed through Core and Temporal Server")
	} else if expect_properties {
		fmt.println("Workflow info, memo, and Search Attributes completed through Core and Temporal Server")
	} else if expect_signal_with_start {
		fmt.println("Signal-With-Start created once, reused the execution, and delivered both Signal headers")
	} else if expect_abandon_update {
		fmt.println("Workflow close abandoned the unfinished Update handler with a warning")
	} else if expect_cancel_update {
		fmt.println("detached Workflow Update completed before cancellation acknowledgement")
	} else if expect_signal_handler {
		fmt.println("asynchronous Workflow Signal handler awaited an Activity and replayed")
	} else {
		fmt.println("signal won the durable timer race and replay reproduced timer cancellation")
	}
}

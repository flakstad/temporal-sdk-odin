package main

import "core:fmt"
import "core:os"
import "core:strings"
import "core:time"
import temporal_core "../../temporal/core"
import codec "../../temporal/codec"
import temporal_failure "../../temporal/failure"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-child-workflow"
PARENT_WORKFLOW_TYPE :: "odin_parent_workflow"
CHILD_WORKFLOW_TYPE :: "odin_child_workflow"
REPEATED_PARENT_WORKFLOW_TYPE :: "odin_repeated_child_parent_workflow"
REPEATED_CHILD_WORKFLOW_TYPE :: "odin_repeated_child_workflow"
FAILURE_PARENT_WORKFLOW_TYPE :: "odin_child_failure_parent_workflow"
FAILURE_CHILD_WORKFLOW_TYPE :: "odin_failing_child_workflow"
SIGNAL_PARENT_WORKFLOW_TYPE :: "odin_child_signal_parent_workflow"
SIGNAL_CHILD_WORKFLOW_TYPE :: "odin_signaled_child_workflow"
SIGNAL_TOKEN_CANCEL_PARENT_WORKFLOW_TYPE :: "odin_child_signal_token_cancel_parent_workflow"
SIGNAL_TOKEN_CANCEL_CHILD_WORKFLOW_TYPE :: "odin_child_signal_token_cancel_child_workflow"
EXPLICIT_CANCEL_PARENT_WORKFLOW_TYPE :: "odin_child_explicit_cancel_parent_workflow"
DUPLICATE_PARENT_WORKFLOW_TYPE :: "odin_child_duplicate_parent_workflow"
EXTERNAL_TARGET_WORKFLOW_TYPE :: "odin_external_target_workflow"
EXTERNAL_CALLER_WORKFLOW_TYPE :: "odin_external_caller_workflow"
CONTINUE_AS_NEW_WORKFLOW_TYPE :: "odin_continue_as_new_workflow"
REPEATED_CONTINUE_AS_NEW_WORKFLOW_TYPE :: "odin_repeated_continue_as_new_workflow"

workflow_start_child_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Outbound_Start_Child_Workflow_Input,
	next: workflow.Outbound_Start_Child_Workflow_Next,
) -> workflow.Child_Workflow_Future {
	if !workflow.headers_add_json(input.headers, "odin-workflow-start-child", "intercepted") {
		return {}
	}
	fmt.println("Workflow start-Child interceptor entered")
	return workflow.outbound_start_child_workflow_next(next, input)
}

workflow_signal_child_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Outbound_Signal_Child_Workflow_Input,
	next: workflow.Outbound_Signal_Child_Workflow_Next,
) -> workflow.Child_Workflow_Signal_Result {
	if !workflow.headers_add_json(input.headers, "odin-workflow-signal-child", "intercepted") {
		return {status = .Failed, failure_message = "could not add Child Workflow Signal interceptor header"}
	}
	fmt.println("Workflow signal-Child interceptor entered")
	return workflow.outbound_signal_child_workflow_next(next, input)
}

workflow_signal_external_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Outbound_Signal_External_Workflow_Input,
	next: workflow.Outbound_Signal_External_Workflow_Next,
) -> workflow.External_Workflow_Signal_Result {
	if !workflow.headers_add_json(input.headers, "odin-workflow-signal-external", "intercepted") {
		return {status = .Failed, failure_message = "could not add external Workflow Signal interceptor header"}
	}
	fmt.println("Workflow signal-external interceptor entered")
	return workflow.outbound_signal_external_workflow_next(next, input)
}

workflow_cancel_external_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Outbound_Cancel_External_Workflow_Input,
	next: workflow.Outbound_Cancel_External_Workflow_Next,
) -> workflow.External_Workflow_Cancel_Result {
	fmt.println("Workflow cancel-external interceptor entered")
	input.reason = "cancellation passed through Odin Workflow interceptor"
	return workflow.outbound_cancel_external_workflow_next(next, input)
}

workflow_continue_as_new_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Outbound_Continue_As_New_Input,
	next: workflow.Outbound_Continue_As_New_Next,
) -> ! {
	if !workflow.headers_add_json(input.headers, "odin-workflow-continue-as-new", "intercepted") {
		panic("could not add Continue-As-New interceptor header")
	}
	fmt.println("Workflow Continue-As-New interceptor entered")
	workflow.outbound_continue_as_new_next(next, input)
}

workflow_sleep_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Outbound_Sleep_Input,
	next: workflow.Outbound_Sleep_Next,
) {
	fmt.printf("Workflow sleep interceptor entered duration=%v\n", input.options.duration)
	workflow.outbound_sleep_next(next, input)
	fmt.println("Workflow sleep interceptor exited")
}

workflow_cancel_child_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Outbound_Cancel_Child_Workflow_Input,
	next: workflow.Outbound_Cancel_Child_Workflow_Next,
) -> bool {
	fmt.println("Workflow cancel-Child interceptor entered")
	input.reason = "Child Workflow cancellation passed through Odin interceptor"
	return workflow.outbound_cancel_child_workflow_next(next, input)
}

child_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	interceptor_header: string
	header_found, header_error := workflow.header_json(
		ctx,
		"odin-workflow-start-child",
		&interceptor_header,
	)
	if !header_found || header_error != .None || interceptor_header != "intercepted" {
		return "Child Workflow interceptor header mismatch"
	}
	memo_value: string
	memo_found, memo_error := workflow.memo_json(ctx, "odin-child-memo", &memo_value)
	defer delete(memo_value)
	search_value: string
	search_found, search_error := workflow.search_attribute_json(
		ctx,
		"OdinSdkKeyword",
		&search_value,
	)
	defer delete(search_value)
	info, info_found := workflow.info(ctx)
	if !memo_found || memo_error != .None || memo_value != "child-options" ||
	   !search_found || search_error != .None || search_value != "child-options" ||
	   !info_found || info.priority.priority_key != 4 ||
	   info.priority.fairness_key != "odin-child-fairness" ||
	   info.priority.fairness_weight != 1.5 {
		return "Child Workflow metadata options mismatch"
	}
	if strings.has_prefix(name, "cancel-") {
		workflow.sleep(ctx, time.Hour)
		return "child cancellation was not delivered"
	}
	message, allocation_error := strings.concatenate({"Hello from child, ", name})
	if allocation_error != nil {
		return ""
	}
	return message
}

failing_child_workflow :: proc(
	_: ^workflow.Fiber_Context,
	_: codec.Payload_View,
) -> workflow.Payload_Result {
	return workflow.payload_fail(
		"child rejected input",
		"OdinChildRejected",
	)
}

failure_parent_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	result := workflow.execute_child_workflow_json_result(
		ctx,
		FAILURE_CHILD_WORKFLOW_TYPE,
		name,
		{
			workflow_id = "odin-failing-child",
			workflow_execution_timeout = time.Minute,
		},
		string,
	)
	failure_tree := workflow.child_workflow_failure_tree(&result.failure)
	child_node := temporal_failure.find_kind(failure_tree, .Child_Workflow)
	application_node := temporal_failure.find_kind(failure_tree, .Application)
	if result.status != .Failed ||
	   result.failure.workflow_id != "odin-failing-child" ||
	   result.failure.workflow_type != FAILURE_CHILD_WORKFLOW_TYPE ||
	   !result.failure.application.present ||
	   result.failure.application.message != "child rejected input" ||
	   result.failure.application.type != "OdinChildRejected" ||
	   !result.failure.application.non_retryable || failure_tree == nil ||
	   child_node == nil || application_node == nil ||
	   child_node.child_workflow.workflow_id != "odin-failing-child" ||
	   application_node.application.type != "OdinChildRejected" {
		return "structured Child Workflow failure mismatch"
	}
	return "Observed structured Child Workflow failure"
}

signaled_child_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	start_header: string
	start_found, start_error := workflow.header_json(
		ctx,
		"odin-workflow-start-child",
		&start_header,
	)
	approval := workflow.await_signal_payloads_with_context(ctx, "approve")
	signal_header: string
	signal_found, signal_error := workflow.signal_header_json(
		&approval.metadata,
		"odin-workflow-signal-child",
		&signal_header,
	)
	if !start_found || start_error != .None || start_header != "intercepted" ||
	   !signal_found || signal_error != .None || signal_header != "intercepted" {
		return "Child Workflow interceptor header mismatch"
	}
	if len(approval.values) != 3 || len(approval.values[0].external_payloads) != 1 ||
	   approval.values[0].external_payloads[0].size_bytes != 7168 {
		return "Child Workflow repeated signal shape mismatch"
	}
	message_type, metadata_found := codec.metadata_get(approval.values[0], "messageType")
	if !metadata_found || string(message_type) != "odin.RepeatedChildSignal" {
		return "Child Workflow repeated signal metadata mismatch"
	}
	approval_value: string
	count: int
	enabled: bool
	if codec.decode_json(approval.values[0], &approval_value) != .None ||
	   codec.decode_json(approval.values[1], &count) != .None ||
	   codec.decode_json(approval.values[2], &enabled) != .None {
		delete(approval_value)
		return "Child Workflow repeated signal decoding failed"
	}
	defer delete(approval_value)
	message := fmt.aprintf("%s received %s:%d:%v", name, approval_value, count, enabled)
	return message
}

signal_parent_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	future := workflow.start_child_workflow_json(
		ctx,
		SIGNAL_CHILD_WORKFLOW_TYPE,
		name,
		{
			workflow_id = "odin-signaled-child",
			workflow_execution_timeout = time.Minute,
		},
	)
	approval, approval_error := codec.encode_json("approval-from-parent")
	if approval_error != .None {
		return "Child Workflow signal encoding failed"
	}
	defer codec.payload_free(&approval)
	count, count_error := codec.encode_json(42)
	if count_error != .None {
		return "Child Workflow signal encoding failed"
	}
	defer codec.payload_free(&count)
	enabled, enabled_error := codec.encode_json(true)
	if enabled_error != .None {
		return "Child Workflow signal encoding failed"
	}
	defer codec.payload_free(&enabled)
	if codec.payload_add_metadata(
		&approval,
		"messageType",
		transmute([]u8)string("odin.RepeatedChildSignal"),
	) != .None || codec.payload_add_external(&approval, 7168) != .None {
		return "Child Workflow signal decoration failed"
	}
	inputs := [?]codec.Payload_View{
		codec.payload_view(&approval),
		codec.payload_view(&count),
		codec.payload_view(&enabled),
	}
	signal := workflow.signal_child_workflow_payloads_with_options(
		ctx,
		future,
		"approve",
		inputs[:],
		{summary = "Approve Odin Child Workflow"},
	)
	if signal.status != .Succeeded {
		return "Child Workflow signal failed"
	}
	result := workflow.await_child_workflow_result(ctx, future)
	if result.status != .Succeeded {
		return "Signaled Child Workflow did not complete"
	}
	return result.value
}

Signal_Token_Cancel_State :: struct {
	token:     workflow.Cancellation_Token,
	requested: bool,
}

cancel_child_signal_token :: proc(
	_: ^workflow.Fiber_Context,
	state: ^Signal_Token_Cancel_State,
	_: string,
) {
	state.requested = workflow.cancellation_request(
		state.token,
		"live Child Signal token canceled before Child start",
	)
}

signal_token_cancel_child_workflow :: proc(
	_: ^workflow.Fiber_Context,
	_: string,
) -> string {
	return "Child Signal token cancellation target completed"
}

signal_token_cancel_parent_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	workflow_id: string,
) -> string {
	state := Signal_Token_Cancel_State{
		token = workflow.cancellation_detached(ctx),
	}
	if state.token.id == 0 ||
	   !workflow.set_signal_routine_json(
		   ctx,
		   "cancel-child-signal-token",
		   &state,
		   cancel_child_signal_token,
	   ) {
		return "could not configure live Child Signal token cancellation"
	}
	child_id, allocation_error := strings.concatenate({workflow_id, "-child"})
	if allocation_error != nil {
		return "could not allocate live Child Workflow ID"
	}
	defer delete(child_id)
	future := workflow.start_child_workflow_json(
		ctx,
		SIGNAL_TOKEN_CANCEL_CHILD_WORKFLOW_TYPE,
		workflow_id,
		{
			workflow_id = child_id,
			workflow_execution_timeout = time.Minute,
		},
	)
	signal := workflow.signal_child_workflow_json_with_options(
		ctx,
		future,
		"must-not-reach-child",
		"not-delivered",
		{cancellation_token = state.token},
	)
	if !state.requested || signal.status != .Canceled ||
	   signal.failure_message != "live Child Signal token canceled before Child start" {
		return "live Child Signal start wait ignored cancellation"
	}
	result := workflow.await_child_workflow_result(ctx, future)
	if result.status != .Succeeded ||
	   result.value != "Child Signal token cancellation target completed" {
		return "Child Signal token cancellation target did not complete"
	}
	return "Child Signal token canceled before Child start"
}

explicit_cancel_parent_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	future := workflow.start_child_workflow_json(
		ctx,
		CHILD_WORKFLOW_TYPE,
		name,
		{
			workflow_id = "odin-explicitly-canceled-child",
			workflow_execution_timeout = time.Minute,
		},
	)
	start := workflow.await_child_workflow_start(ctx, future)
	if start.status != .Succeeded {
		return "Explicitly canceled Child Workflow did not start"
	}
	if !workflow.cancel_child_workflow(ctx, future, "parent explicitly canceled child") {
		return "Explicit Child Workflow cancellation was rejected"
	}
	result := workflow.await_child_workflow_result(ctx, future)
	if result.status != .Canceled {
		return "Explicit Child Workflow cancellation did not resolve"
	}
	return "Observed explicit Child Workflow cancellation"
}

duplicate_parent_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	options := workflow.Child_Workflow_Options{
		workflow_id = "odin-live-duplicate-child",
		workflow_execution_timeout = time.Minute,
	}
	first := workflow.start_child_workflow_json(ctx, CHILD_WORKFLOW_TYPE, name, options)
	first_start := workflow.await_child_workflow_start(ctx, first)
	if first_start.status != .Succeeded {
		return "First Child Workflow did not start"
	}
	duplicate := workflow.start_child_workflow_json(ctx, CHILD_WORKFLOW_TYPE, name, options)
	duplicate_start := workflow.await_child_workflow_start(ctx, duplicate)
	if duplicate_start.status != .Failed ||
	   duplicate_start.cause != .Workflow_Already_Exists {
		return "Duplicate Child Workflow did not produce an Already Exists start failure"
	}
	if !workflow.cancel_child_workflow(ctx, first, "duplicate start test cleanup") {
		return "Duplicate Child Workflow cleanup cancellation failed"
	}
	first_result := workflow.await_child_workflow_result(ctx, first)
	if first_result.status != .Canceled {
		return "Duplicate Child Workflow cleanup did not resolve"
	}
	return "Observed Child Workflow Already Exists start failure"
}

external_target_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	_ = name
	approval := workflow.await_signal_payloads_with_context(ctx, "external-approve")
	signal_header: string
	header_found, header_error := workflow.signal_header_json(
		&approval.metadata,
		"odin-workflow-signal-external",
		&signal_header,
	)
	if len(approval.values) != 3 || !header_found ||
	   header_error != .None || signal_header != "intercepted" {
		return "External Workflow signal payload mismatch"
	}
	approval_value: string
	count: int
	enabled: bool
	message_type, metadata_found := codec.metadata_get(approval.values[0], "messageType")
	if !metadata_found || string(message_type) != "odin.RepeatedExternalSignal" ||
	   len(approval.values[0].external_payloads) != 1 ||
	   approval.values[0].external_payloads[0].size_bytes != 8192 ||
	   codec.decode_json(approval.values[0], &approval_value) != .None ||
	   codec.decode_json(approval.values[1], &count) != .None ||
	   codec.decode_json(approval.values[2], &enabled) != .None ||
	   approval_value != "approval-from-external-caller" || count != 42 || !enabled {
		delete(approval_value)
		return "External Workflow repeated signal payload mismatch"
	}
	delete(approval_value)
	workflow.sleep(ctx, time.Hour)
	return "External target was not canceled"
}

external_caller_workflow :: proc(ctx: ^workflow.Fiber_Context, target_workflow_id: string) -> string {
	execution := workflow.External_Workflow_Execution{
		namespace = "default",
		workflow_id = target_workflow_id,
	}
	approval, approval_error := codec.encode_json("approval-from-external-caller")
	if approval_error != .None {
		return "External Workflow signal encoding failed"
	}
	defer codec.payload_free(&approval)
	count, count_error := codec.encode_json(42)
	if count_error != .None {
		return "External Workflow signal encoding failed"
	}
	defer codec.payload_free(&count)
	enabled, enabled_error := codec.encode_json(true)
	if enabled_error != .None {
		return "External Workflow signal encoding failed"
	}
	defer codec.payload_free(&enabled)
	if codec.payload_add_metadata(
		&approval,
		"messageType",
		transmute([]u8)string("odin.RepeatedExternalSignal"),
	) != .None || codec.payload_add_external(&approval, 8192) != .None {
		return "External Workflow signal decoration failed"
	}
	inputs := [?]codec.Payload_View{
		codec.payload_view(&approval),
		codec.payload_view(&count),
		codec.payload_view(&enabled),
	}
	signal := workflow.signal_external_workflow_payloads_with_options(
		ctx,
		execution,
		"external-approve",
		inputs[:],
		{summary = "Approve external Odin Workflow"},
	)
	if signal.status != .Succeeded {
		return "External Workflow signal failed"
	}
	cancel := workflow.request_cancel_external_workflow(
		ctx,
		execution,
		"external caller completed its signal test",
	)
	if cancel.status != .Succeeded {
		return "External Workflow cancellation failed"
	}
	return "External Workflow signaled and cancellation requested"
}

continue_as_new_workflow :: proc(ctx: ^workflow.Fiber_Context, value: string) -> string {
	if value == "first-run" {
		memo := workflow.Payload_Map{}
		if !workflow.payload_map_init(&memo) ||
		   !workflow.payload_map_add_json(&memo, "odin-continue-memo", "carried") {
			return "Continue-As-New memo setup failed"
		}
		search_attributes := workflow.Payload_Map{}
		if !workflow.payload_map_init(&search_attributes) ||
		   !workflow.payload_map_add_search_attribute_json(
			&search_attributes,
			"OdinSdkKeyword",
			workflow.Search_Attribute_Type.Keyword,
			"continue-as-new",
		) {
			return "Continue-As-New Search Attribute setup failed"
		}
		workflow.continue_as_new_json(
			ctx,
			"continued-run",
			{
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
				memo = &memo,
				search_attributes = &search_attributes,
			},
		)
	}
	memo_value: string
	memo_found, memo_error := workflow.memo_json(
		ctx,
		"odin-continue-memo",
		&memo_value,
	)
	defer delete(memo_value)
	search_value: string
	search_found, search_error := workflow.search_attribute_json(
		ctx,
		"OdinSdkKeyword",
		&search_value,
	)
	defer delete(search_value)
	interceptor_header: string
	header_found, header_error := workflow.header_json(
		ctx,
		"odin-workflow-continue-as-new",
		&interceptor_header,
	)
	if !header_found || header_error != .None || interceptor_header != "intercepted" ||
	   !memo_found || memo_error != .None || memo_value != "carried" ||
	   !search_found || search_error != .None || search_value != "continue-as-new" {
		return "Continue-As-New interceptor header mismatch"
	}
	result, allocation_error := strings.concatenate({"Continue-As-New completed on ", value})
	if allocation_error != nil {
		return ""
	}
	return result
}

repeated_continue_as_new_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	inputs: []codec.Payload_View,
) -> workflow.Payload_Result {
	if len(inputs) == 1 {
		value: string
		if codec.decode_json(inputs[0], &value) != .None || value != "first-run" {
			delete(value)
			return workflow.payload_fail(
				"repeated Continue-As-New initial input mismatch",
				"OdinContinueAsNewPayloadConversionError",
			)
		}
		delete(value)
		name, name_error := codec.encode_json("continued-run")
		if name_error != .None {
			return workflow.payload_fail("could not encode repeated Continue-As-New name")
		}
		count, count_error := codec.encode_json(42)
		if count_error != .None {
			return workflow.payload_fail("could not encode repeated Continue-As-New count")
		}
		enabled, enabled_error := codec.encode_json(true)
		if enabled_error != .None {
			return workflow.payload_fail("could not encode repeated Continue-As-New flag")
		}
		if codec.payload_add_metadata(
			&name,
			"messageType",
			transmute([]u8)string("odin.RepeatedContinueAsNew"),
		) != .None || codec.payload_add_external(&name, 9216) != .None {
			return workflow.payload_fail("could not decorate repeated Continue-As-New input")
		}
		next_inputs := [?]codec.Payload_View{
			codec.payload_view(&name),
			codec.payload_view(&count),
			codec.payload_view(&enabled),
		}
		workflow.continue_as_new_payloads(
			ctx,
			next_inputs[:],
			{
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
		)
	}
	if len(inputs) != 3 || len(inputs[0].external_payloads) != 1 ||
	   inputs[0].external_payloads[0].size_bytes != 9216 {
		return workflow.payload_fail(
			"repeated Continue-As-New arguments mismatch",
			"OdinContinueAsNewArgumentCountError",
		)
	}
	message_type, metadata_found := codec.metadata_get(inputs[0], "messageType")
	interceptor_header: string
	header_found, header_error := workflow.header_json(
		ctx,
		"odin-workflow-continue-as-new",
		&interceptor_header,
	)
	defer delete(interceptor_header)
	if !metadata_found || string(message_type) != "odin.RepeatedContinueAsNew" ||
	   !header_found || header_error != .None || interceptor_header != "intercepted" {
		return workflow.payload_fail(
			"repeated Continue-As-New metadata mismatch",
			"OdinContinueAsNewMetadataError",
		)
	}
	name: string
	count: int
	enabled: bool
	if codec.decode_json(inputs[0], &name) != .None ||
	   codec.decode_json(inputs[1], &count) != .None ||
	   codec.decode_json(inputs[2], &enabled) != .None {
		delete(name)
		return workflow.payload_fail("could not decode repeated Continue-As-New arguments")
	}
	defer delete(name)
	message := fmt.aprintf("Continue-As-New received %s:%d:%v", name, count, enabled)
	defer delete(message)
	payload, encode_error := codec.encode_json(message)
	if encode_error != .None {
		return workflow.payload_fail("could not encode repeated Continue-As-New result")
	}
	return workflow.payload_complete(payload)
}

observe_activation :: proc(processed: ^workflow.Process_Result) {
	fmt.printf(
		"child activation history=%d replay=%v started=%d start_resolved=%d resolved=%d child_signaled=%d child_signal_resolved=%d external_signaled=%d external_signal_resolved=%d external_cancel_requested=%v external_cancel_resolved=%d cancel_requested=%v canceled_child=%d canceled_workflow=%v continued_as_new=%v completed=%v evicted=%v\n",
		processed.history_length,
		processed.is_replaying,
		processed.started_child_workflow_count,
		processed.resolved_child_workflow_start_seq,
		processed.resolved_child_workflow_seq,
		processed.signaled_child_workflow_count,
		processed.resolved_child_workflow_signal_seq,
		processed.signaled_external_workflow_count,
		processed.resolved_external_workflow_signal_seq,
		processed.requested_external_workflow_cancellation,
		processed.resolved_external_workflow_cancellation_seq,
		processed.requested_child_workflow_cancellation,
		processed.canceled_child_workflow_seq,
		processed.canceled_workflow,
		processed.continued_as_new,
		processed.completed,
		processed.evicted,
	)
}

parent_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	child_id, allocation_error := strings.concatenate({"odin-child-", name})
	if allocation_error != nil {
		return ""
	}
	memo := workflow.Payload_Map{}
	if !workflow.payload_map_init(&memo) ||
	   !workflow.payload_map_add_json(&memo, "odin-child-memo", "child-options") {
		return "could not build Child Workflow memo"
	}
	search_attributes := workflow.Payload_Map{}
	if !workflow.payload_map_init(&search_attributes) ||
	   !workflow.payload_map_add_search_attribute_json(
		   &search_attributes,
		   "OdinSdkKeyword",
		   workflow.Search_Attribute_Type.Keyword,
		   "child-options",
	   ) {
		return "could not build Child Workflow Search Attributes"
	}
	priority := workflow.Workflow_Priority{
		priority_key = 4,
		fairness_key = "odin-child-fairness",
		fairness_weight = 1.5,
	}
	return workflow.execute_child_workflow(
		ctx,
		CHILD_WORKFLOW_TYPE,
		name,
		{
			workflow_id = child_id,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
			id_reuse_policy = .Reject_Duplicate,
			memo = &memo,
			search_attributes = &search_attributes,
			versioning_intent = .Default,
			priority = &priority,
			static_summary = "Odin Child Workflow",
			static_details = "Started by the Odin SDK Child Workflow integration example",
		},
	)
}

repeated_child_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	inputs: []codec.Payload_View,
) -> workflow.Payload_Result {
	if len(inputs) != 3 || len(inputs[0].external_payloads) != 1 ||
	   inputs[0].external_payloads[0].size_bytes != 6144 {
		return workflow.payload_fail(
			"repeated Child Workflow input shape mismatch",
			"OdinChildWorkflowArgumentCountError",
		)
	}
	message_type, found := codec.metadata_get(inputs[0], "messageType")
	if !found || string(message_type) != "odin.RepeatedChild" {
		return workflow.payload_fail(
			"repeated Child Workflow metadata mismatch",
			"OdinChildWorkflowMetadataError",
		)
	}
	interceptor_header: string
	header_found, header_error := workflow.header_json(
		ctx,
		"odin-workflow-start-child",
		&interceptor_header,
	)
	defer delete(interceptor_header)
	if !header_found || header_error != .None || interceptor_header != "intercepted" {
		return workflow.payload_fail(
			"repeated Child Workflow interceptor header mismatch",
			"OdinChildWorkflowHeaderError",
		)
	}
	name: string
	count: int
	enabled: bool
	if codec.decode_json(inputs[0], &name) != .None ||
	   codec.decode_json(inputs[1], &count) != .None ||
	   codec.decode_json(inputs[2], &enabled) != .None {
		delete(name)
		return workflow.payload_fail(
			"repeated Child Workflow could not decode its inputs",
			"OdinChildWorkflowPayloadConversionError",
		)
	}
	defer delete(name)
	message := fmt.aprintf("Repeated child received %s:%d:%v", name, count, enabled)
	defer delete(message)
	payload, encode_error := codec.encode_json(message)
	if encode_error != .None {
		return workflow.payload_fail(
			"repeated Child Workflow could not encode its result",
			"OdinChildWorkflowPayloadConversionError",
		)
	}
	return workflow.payload_complete(payload)
}

repeated_parent_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	name_payload, name_error := codec.encode_json(name)
	if name_error != .None {
		return "could not encode repeated Child Workflow name"
	}
	defer codec.payload_free(&name_payload)
	count_payload, count_error := codec.encode_json(42)
	if count_error != .None {
		return "could not encode repeated Child Workflow count"
	}
	defer codec.payload_free(&count_payload)
	enabled_payload, enabled_error := codec.encode_json(true)
	if enabled_error != .None {
		return "could not encode repeated Child Workflow flag"
	}
	defer codec.payload_free(&enabled_payload)
	if codec.payload_add_metadata(
		&name_payload,
		"messageType",
		transmute([]u8)string("odin.RepeatedChild"),
	) != .None || codec.payload_add_external(&name_payload, 6144) != .None {
		return "could not decorate repeated Child Workflow input"
	}
	inputs := [?]codec.Payload_View{
		codec.payload_view(&name_payload),
		codec.payload_view(&count_payload),
		codec.payload_view(&enabled_payload),
	}
	child_id, allocation_error := strings.concatenate({"odin-repeated-child-", name})
	if allocation_error != nil {
		return "could not allocate repeated Child Workflow ID"
	}
	defer delete(child_id)
	future := workflow.start_child_workflow_payloads(
		ctx,
		REPEATED_CHILD_WORKFLOW_TYPE,
		inputs[:],
		{
			workflow_id = child_id,
			workflow_execution_timeout = time.Minute,
		},
	)
	result := workflow.await_child_workflow_json_result(ctx, future, string)
	if result.status != .Succeeded {
		return "Repeated Child Workflow did not complete"
	}
	return result.value
}

main :: proc() {
	runtime, runtime_error := temporal_core.runtime_create()
	if runtime_error.message != nil {
		fmt.eprintf("Core runtime initialization failed: %s\n", string(runtime_error.message))
		temporal_core.error_free(&runtime_error)
		os.exit(1)
	}
	defer temporal_core.runtime_close(&runtime)

	client, connect_error := temporal_core.client_connect(&runtime)
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal_core.error_free(&connect_error)
		os.exit(1)
	}
	defer temporal_core.client_close(&client)

	sdk_worker, create_error := worker.create(&client, TASK_QUEUE)
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		os.exit(1)
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}

	workflow_interceptor_error := worker.add_workflow_interceptor(
		&sdk_worker,
		{
			start_child_workflow = workflow_start_child_interceptor,
			signal_child_workflow = workflow_signal_child_interceptor,
			signal_external_workflow = workflow_signal_external_interceptor,
			cancel_external_workflow = workflow_cancel_external_interceptor,
			continue_as_new = workflow_continue_as_new_interceptor,
			sleep = workflow_sleep_interceptor,
			cancel_child_workflow = workflow_cancel_child_interceptor,
		},
	)
	if workflow_interceptor_error.message != nil {
		fmt.eprintf(
			"Workflow interceptor registration failed: %s\n",
			string(workflow_interceptor_error.message),
		)
		worker.error_free(&workflow_interceptor_error)
		os.exit(1)
	}

	parent_error := worker.register_workflow(
		&sdk_worker,
		PARENT_WORKFLOW_TYPE,
		parent_workflow,
	)
	if parent_error.message != nil {
		fmt.eprintf("Parent registration failed: %s\n", string(parent_error.message))
		worker.error_free(&parent_error)
		os.exit(1)
	}
	child_error := worker.register_workflow(
		&sdk_worker,
		CHILD_WORKFLOW_TYPE,
		child_workflow,
	)
	if child_error.message != nil {
		fmt.eprintf("Child registration failed: %s\n", string(child_error.message))
		worker.error_free(&child_error)
		os.exit(1)
	}
	repeated_parent_error := worker.register_workflow(
		&sdk_worker,
		REPEATED_PARENT_WORKFLOW_TYPE,
		repeated_parent_workflow,
	)
	if repeated_parent_error.message != nil {
		fmt.eprintf("Repeated parent registration failed: %s\n", string(repeated_parent_error.message))
		worker.error_free(&repeated_parent_error)
		os.exit(1)
	}
	repeated_child_error := worker.register_payloads_workflow(
		&sdk_worker,
		REPEATED_CHILD_WORKFLOW_TYPE,
		repeated_child_workflow,
	)
	if repeated_child_error.message != nil {
		fmt.eprintf("Repeated child registration failed: %s\n", string(repeated_child_error.message))
		worker.error_free(&repeated_child_error)
		os.exit(1)
	}
	failure_parent_error := worker.register_workflow(
		&sdk_worker,
		FAILURE_PARENT_WORKFLOW_TYPE,
		failure_parent_workflow,
	)
	if failure_parent_error.message != nil {
		fmt.eprintf("Failure parent registration failed: %s\n", string(failure_parent_error.message))
		worker.error_free(&failure_parent_error)
		os.exit(1)
	}
	failure_child_error := worker.register_payload_workflow(
		&sdk_worker,
		FAILURE_CHILD_WORKFLOW_TYPE,
		failing_child_workflow,
	)
	if failure_child_error.message != nil {
		fmt.eprintf("Failure child registration failed: %s\n", string(failure_child_error.message))
		worker.error_free(&failure_child_error)
		os.exit(1)
	}
	signal_parent_error := worker.register_workflow(
		&sdk_worker,
		SIGNAL_PARENT_WORKFLOW_TYPE,
		signal_parent_workflow,
	)
	if signal_parent_error.message != nil {
		fmt.eprintf("Signal parent registration failed: %s\n", string(signal_parent_error.message))
		worker.error_free(&signal_parent_error)
		os.exit(1)
	}
	signal_child_error := worker.register_workflow(
		&sdk_worker,
		SIGNAL_CHILD_WORKFLOW_TYPE,
		signaled_child_workflow,
	)
	if signal_child_error.message != nil {
		fmt.eprintf("Signal child registration failed: %s\n", string(signal_child_error.message))
		worker.error_free(&signal_child_error)
		os.exit(1)
	}
	signal_token_cancel_parent_error := worker.register_workflow(
		&sdk_worker,
		SIGNAL_TOKEN_CANCEL_PARENT_WORKFLOW_TYPE,
		signal_token_cancel_parent_workflow,
	)
	if signal_token_cancel_parent_error.message != nil {
		fmt.eprintf(
			"Signal-token-cancel parent registration failed: %s\n",
			string(signal_token_cancel_parent_error.message),
		)
		worker.error_free(&signal_token_cancel_parent_error)
		os.exit(1)
	}
	signal_token_cancel_child_error := worker.register_workflow(
		&sdk_worker,
		SIGNAL_TOKEN_CANCEL_CHILD_WORKFLOW_TYPE,
		signal_token_cancel_child_workflow,
	)
	if signal_token_cancel_child_error.message != nil {
		fmt.eprintf(
			"Signal-token-cancel child registration failed: %s\n",
			string(signal_token_cancel_child_error.message),
		)
		worker.error_free(&signal_token_cancel_child_error)
		os.exit(1)
	}
	explicit_cancel_parent_error := worker.register_workflow(
		&sdk_worker,
		EXPLICIT_CANCEL_PARENT_WORKFLOW_TYPE,
		explicit_cancel_parent_workflow,
	)
	if explicit_cancel_parent_error.message != nil {
		fmt.eprintf("Explicit-cancel parent registration failed: %s\n", string(explicit_cancel_parent_error.message))
		worker.error_free(&explicit_cancel_parent_error)
		os.exit(1)
	}
	duplicate_parent_error := worker.register_workflow(
		&sdk_worker,
		DUPLICATE_PARENT_WORKFLOW_TYPE,
		duplicate_parent_workflow,
	)
	if duplicate_parent_error.message != nil {
		fmt.eprintf("Duplicate parent registration failed: %s\n", string(duplicate_parent_error.message))
		worker.error_free(&duplicate_parent_error)
		os.exit(1)
	}
	external_target_error := worker.register_workflow(
		&sdk_worker,
		EXTERNAL_TARGET_WORKFLOW_TYPE,
		external_target_workflow,
	)
	if external_target_error.message != nil {
		fmt.eprintf("External target registration failed: %s\n", string(external_target_error.message))
		worker.error_free(&external_target_error)
		os.exit(1)
	}
	external_caller_error := worker.register_workflow(
		&sdk_worker,
		EXTERNAL_CALLER_WORKFLOW_TYPE,
		external_caller_workflow,
	)
	if external_caller_error.message != nil {
		fmt.eprintf("External caller registration failed: %s\n", string(external_caller_error.message))
		worker.error_free(&external_caller_error)
		os.exit(1)
	}
	continue_as_new_error := worker.register_workflow(
		&sdk_worker,
		CONTINUE_AS_NEW_WORKFLOW_TYPE,
		continue_as_new_workflow,
	)
	if continue_as_new_error.message != nil {
		fmt.eprintf("Continue-As-New registration failed: %s\n", string(continue_as_new_error.message))
		worker.error_free(&continue_as_new_error)
		os.exit(1)
	}
	repeated_continue_as_new_error := worker.register_payloads_workflow(
		&sdk_worker,
		REPEATED_CONTINUE_AS_NEW_WORKFLOW_TYPE,
		repeated_continue_as_new_workflow,
	)
	if repeated_continue_as_new_error.message != nil {
		fmt.eprintf(
			"Repeated Continue-As-New registration failed: %s\n",
			string(repeated_continue_as_new_error.message),
		)
		worker.error_free(&repeated_continue_as_new_error)
		os.exit(1)
	}

	fmt.printf("Odin Child Workflow Worker is polling task queue %s\n", TASK_QUEUE)
	stats, run_error := worker.run(
		&sdk_worker,
		{
			max_workflow_executions = 2,
			disable_activity_polling = true,
			on_workflow_activation = observe_activation,
		},
	)
	if run_error.message != nil {
		fmt.eprintf("Worker run failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		os.exit(1)
	}
	if stats.workflow_executions_completed != 2 || stats.workflow_evictions < 2 {
		fmt.eprintf(
			"Unexpected Child Workflow stats: completed=%d evictions=%d\n",
			stats.workflow_executions_completed,
			stats.workflow_evictions,
		)
		os.exit(1)
	}
	fmt.println("Child Workflow completed end-to-end and worker shut down cleanly")
}

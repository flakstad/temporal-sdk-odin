package main

import "core:fmt"
import "core:os"
import "core:strings"
import "core:time"
import activity "../../temporal/activity"
import temporal_core "../../temporal/core"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

TASK_QUEUE :: "odin-hello-workflow"
WORKFLOW_TYPE :: "odin_hello_workflow"
TYPED_WORKFLOW_TYPE :: "odin_typed_workflow"
LOCAL_ACTIVITY_WORKFLOW_TYPE :: "odin_local_activity_workflow"
LOCAL_ACTIVITY_RETRY_WORKFLOW_TYPE :: "odin_local_activity_retry_workflow"
FAILURE_DETAILS_WORKFLOW_TYPE :: "odin_failure_details_workflow"
PATCH_WORKFLOW_TYPE :: "odin_patch_workflow"
WORKFLOW_PROPERTIES_TYPE :: "odin_workflow_properties"
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
}

Greeting_Update_Input :: struct {
	name: string,
}

Greeting_Update_Output :: struct {
	previous_name: string,
	current_name:  string,
}

greeting_status_query :: proc(
	state: Greeting_Query_State,
	input: Greeting_Query_Input,
) -> Greeting_Query_Output {
	result := Greeting_Query_Output{phase = state.phase}
	if input.include_name {
		result.name = state.name
	}
	return result
}

validate_greeting_name :: proc(
	_: Greeting_Query_State,
	input: Greeting_Update_Input,
) -> workflow.Update_Validation {
	if len(input.name) == 0 || input.name == "reject" {
		return workflow.update_reject("greeting name is invalid", "InvalidGreetingName")
	}
	return workflow.update_accept()
}

update_greeting_name :: proc(
	ctx: ^workflow.Fiber_Context,
	state: ^Greeting_Query_State,
	input: Greeting_Update_Input,
) -> Greeting_Update_Output {
	previous := state.name
	_ = workflow.execute_activity(ctx, "hello", input.name)
	state.name = input.name
	return {previous_name = previous, current_name = state.name}
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
	state: ^Greeting_Query_State,
	name: string,
) {
	activity_result := workflow.execute_activity_result(ctx, "hello", name)
	if activity_result.status == .Succeeded {
		state.name = name
	}
}

hello :: proc(ctx: ^activity.Context, name: string) -> string {
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
	if !workflow.set_query_handler_json(
		ctx,
		"greeting_status",
		&query_state,
		greeting_status_query,
	) {
		return "could not register greeting status query"
	}
	if !workflow.set_validated_update_routine_json(
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
	if !workflow.set_signal_routine_json(
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
	if result.failure.activity_id != "failure-details-activity" ||
	   result.failure.activity_type != "failure_details" ||
	   result.failure.retry_state != .Maximum_Attempts_Reached ||
	   !application.present || application.message != "billing dependency rejected request" ||
	   application.type != "DependencyRejected" || application.non_retryable ||
	   !application.has_next_retry_delay || application.next_retry_delay != 250 * time.Millisecond ||
	   !detail_found || detail_error != .None || details.dependency != "billing" || details.code != 409 {
		return "structured Activity failure mismatch"
	}
	return "Observed structured Activity failure"
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
	   len(metadata.run_id) == 0 {
		return "Workflow info mismatch"
	}
	initial_header: string
	header_found, header_error := workflow.header_json(ctx, "odin-header", &initial_header)
	initial_memo: string
	memo_found, memo_error := workflow.memo_json(ctx, "odin-start", &initial_memo)
	initial_search: string
	search_found, search_error := workflow.search_attribute_json(
		ctx,
		"OdinSdkKeyword",
		&initial_search,
	)
	if !header_found || header_error != .None || initial_header != "client-header" ||
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
	expect_patch := scenario == "patch"
	expect_properties := scenario == "workflow-properties"
	expect_cancel := expect_cancel_timer || expect_cancel_activity || expect_cancel_cleanup
	run_stats, run_error := worker.run(
		&activity_worker,
		{
			max_activity_tasks = expect_cancel ? 0 : (expect_update ? 3 : ((expect_signal_handler || expect_local_activity_retry) ? 2 : 1)),
			max_workflow_executions = 1,
			disable_activity_polling = expect_cancel_timer || expect_patch || expect_properties,
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
		} else if expect_patch || expect_properties {
			if run_stats.activity_tasks_completed != 0 || run_stats.workflow_signals_received != 0 ||
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
		} else if expect_typed_payload || expect_failure_details || expect_local_activity {
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
	} else if expect_local_activity {
		fmt.println("Local Activity completed through Core in the Workflow Worker")
	} else if expect_local_activity_retry {
		fmt.println("Local Activity timer-backed retry completed through Core and Temporal Server")
	} else if expect_failure_details {
		fmt.println("structured Activity failure details reached Workflow code")
	} else if expect_patch {
		fmt.println("Workflow patch marker completed through Core and Temporal Server")
	} else if expect_properties {
		fmt.println("Workflow info, memo, and Search Attributes completed through Core and Temporal Server")
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

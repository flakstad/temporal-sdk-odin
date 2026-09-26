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

hello :: proc(ctx: ^activity.Context, name: string) -> string {
	if name == "cancel-activity" {
		first_heartbeat := true
		for !activity.is_cancellation_requested(ctx) {
			status := activity.heartbeat(ctx, "waiting for cancellation")
			if first_heartbeat && status == .Recorded {
				fmt.println("cancel-activity heartbeat recorded")
				first_heartbeat = false
			}
			if status == .Cancellation_Requested {
				break
			}
			time.sleep(100 * time.Millisecond)
		}
		fmt.printf(
			"cancel-activity observed cancellation reason=%v\n",
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

hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	if name == "cancel-activity" {
		return workflow.execute_activity(ctx, "hello", name)
	}

	approval := workflow.await_signal_or_timeout(ctx, "approval", 30 * time.Second)
	if approval.status == .Timed_Out {
		return "Approval timed out"
	}
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

observe_activation :: proc(processed: ^workflow.Process_Result) {
	error_message := ""
	if processed.error.message != nil {
		error_message = string(processed.error.message)
	}
	fmt.printf(
		"activation history=%d replay=%v activation_failed=%v error=%q scheduled=%d last_activity=%d resolved=%d failed=%v signal=%v cancel=%v canceled_workflow=%v started_timer=%v canceled_timer=%v canceled_activity=%v waiting_activity=%d waiting_timer=%d waiting_signal=%v completed=%v evicted=%v\n",
		processed.history_length,
		processed.is_replaying,
		processed.activation_failed,
		error_message,
		processed.scheduled_activity_count,
		processed.activity_seq,
		processed.resolved_activity_seq,
		processed.resolved_activity_failed,
		processed.received_signal,
		processed.received_cancellation,
		processed.canceled_workflow,
		processed.started_timer,
		processed.canceled_timer,
		processed.requested_activity_cancellation,
		processed.waiting_activity_seq,
		processed.waiting_timer_seq,
		processed.waiting_signal,
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

	fmt.printf("Odin Workflow Worker is polling task queue %s\n", TASK_QUEUE)
	env_buffer: [64]u8
	scenario := os.get_env(env_buffer[:], "TEMPORAL_ODIN_SCENARIO")
	expect_cancel_timer := scenario == "cancel-timer"
	expect_cancel_activity := scenario == "cancel-activity"
	expect_typed_payload := scenario == "typed-payload"
	expect_cancel := expect_cancel_timer || expect_cancel_activity
	run_stats, run_error := worker.run(
		&activity_worker,
		{
			max_activity_tasks = expect_cancel ? 0 : 1,
			max_workflow_executions = 1,
			disable_activity_polling = expect_cancel_timer,
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
		} else if expect_typed_payload {
			if run_stats.activity_tasks_completed != 1 || run_stats.workflow_signals_received != 0 || run_stats.timers_canceled != 0 {
				fmt.eprintln("Typed Workflow payload path produced unexpected worker stats")
				failed = true
			}
		} else if run_stats.activity_tasks_completed != 1 || run_stats.workflow_signals_received != 1 || run_stats.timers_canceled != 1 || run_stats.workflow_replay_activations < 1 {
			fmt.eprintf(
				"Workflow spike stats mismatch: activities=%d signals=%d timers_canceled=%d replay_activations=%d\n",
				run_stats.activity_tasks_completed,
				run_stats.workflow_signals_received,
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
	} else if expect_typed_payload {
		fmt.println("typed Workflow and Activity Payloads completed end-to-end")
	} else {
		fmt.println("signal won the durable timer race and replay reproduced timer cancellation")
	}
}

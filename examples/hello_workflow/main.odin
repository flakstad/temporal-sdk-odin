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

hello :: proc(ctx: ^activity.Context, name: string) -> string {
	_ = ctx
	greeting, allocation_error := strings.concatenate({"Hello ", name})
	if allocation_error != nil {
		return ""
	}
	return greeting
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

observe_activation :: proc(processed: ^workflow.Process_Result) {
	fmt.printf(
		"activation history=%d replay=%v scheduled=%d last_activity=%d resolved=%d failed=%v signal=%v cancel=%v canceled_workflow=%v started_timer=%v canceled_timer=%v canceled_activity=%v waiting_activity=%d waiting_timer=%d waiting_signal=%v completed=%v evicted=%v\n",
		processed.history_length,
		processed.is_replaying,
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
		enable_workflows = true,
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

	fmt.printf("Odin Workflow Worker is polling task queue %s\n", TASK_QUEUE)
	env_buffer: [64]u8
	scenario := os.get_env(env_buffer[:], "TEMPORAL_ODIN_SCENARIO")
	expect_cancel_timer := scenario == "cancel-timer"
	expect_cancel_activity := scenario == "cancel-activity"
	expect_cancel := expect_cancel_timer || expect_cancel_activity
	run_stats, run_error := worker.run(
		&activity_worker,
		{
			max_activity_tasks = expect_cancel ? 0 : 1,
			max_workflow_executions = 1,
			disable_activity_polling = expect_cancel,
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
			if run_stats.activity_tasks_completed != 0 || run_stats.workflow_signals_received != 0 || run_stats.workflow_cancellations_received != 1 || run_stats.timers_canceled != 0 || run_stats.activity_cancellations_requested != 1 {
				fmt.eprintln("Workflow spike did not cancel its pending Activity and Workflow Execution")
				failed = true
			}
		} else if run_stats.activity_tasks_completed != 1 || run_stats.workflow_signals_received != 1 || run_stats.timers_canceled != 1 || run_stats.workflow_replay_activations < 1 {
			fmt.eprintln("Workflow spike did not deterministically replay the signal-wins timer race")
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
		fmt.println("Workflow cancellation requested pending Activity cancellation and closed the execution")
	} else {
		fmt.println("signal won the durable timer race and replay reproduced timer cancellation")
	}
}

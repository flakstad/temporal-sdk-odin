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
	first_greeting := workflow.execute_activity(ctx, "hello", name)
	workflow.sleep(ctx, 250 * time.Millisecond)
	second_greeting := workflow.execute_activity(ctx, "hello", first_greeting)
	return second_greeting
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

	workflow_runtime := workflow.Fiber_Runtime{}
	workflow_init_error := workflow.fiber_runtime_init(
		&workflow_runtime,
		WORKFLOW_TYPE,
		TASK_QUEUE,
		hello_workflow,
	)
	if workflow_init_error.message != nil {
		fmt.eprintf("Workflow runtime initialization failed: %s\n", string(workflow_init_error.message))
		workflow.error_free(&workflow_init_error)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}

	validate_error := temporal_core.worker_validate(&activity_worker.core)
	if validate_error.message != nil {
		fmt.eprintf("Worker validation failed: %s\n", string(validate_error.message))
		temporal_core.error_free(&validate_error)
		workflow.fiber_runtime_close(&workflow_runtime)
		_ = worker.close(&activity_worker)
		temporal_core.client_close(&client)
		temporal_core.runtime_close(&runtime)
		os.exit(1)
	}

	fmt.printf("Odin Workflow Worker is polling task queue %s\n", TASK_QUEUE)
	evictions_requested := 0
	evictions_observed := 0
	activities_completed := 0
	timer_started := false
	workflow_completed := false
	final_eviction := false
	failed := false

	for poll_count := 0; poll_count < 32 && !(workflow_completed && final_eviction); poll_count += 1 {
		poll := temporal_core.worker_poll_workflow(&activity_worker.core)
		if poll.error.message != nil {
			fmt.eprintf("Workflow poll failed: %s\n", string(poll.error.message))
			temporal_core.error_free(&poll.error)
			failed = true
			break
		}
		if poll.shutdown {
			fmt.eprintln("Workflow poll shut down before the Workflow completed")
			failed = true
			break
		}

		processed := workflow.process_fiber_activation(&workflow_runtime, poll.protobuf)
		delete(poll.protobuf)
		if processed.completion != nil {
			completion_error := temporal_core.worker_complete_workflow(
				&activity_worker.core,
				processed.completion[:],
			)
			delete(processed.completion)
			if completion_error.message != nil {
				fmt.eprintf("Workflow completion failed: %s\n", string(completion_error.message))
				temporal_core.error_free(&completion_error)
				workflow.error_free(&processed.error)
				failed = true
				break
			}
		}

		fmt.printf(
			"activation history=%d replay=%v activity=%d timer=%d completed=%v evicted=%v\n",
			processed.history_length,
			processed.is_replaying,
			processed.activity_seq,
			processed.timer_seq,
			processed.completed,
			processed.evicted,
		)
		if processed.error.message != nil {
			fmt.eprintf("Workflow state machine failed: %s\n", string(processed.error.message))
			workflow.error_free(&processed.error)
			failed = true
			break
		}

		request_eviction :=
			(evictions_requested == 0 && processed.scheduled_activity && processed.activity_seq == 1) ||
			(evictions_requested == 1 && processed.started_timer && processed.timer_seq == 1) ||
			(evictions_requested == 2 && processed.scheduled_activity && processed.activity_seq == 2)
		if request_eviction {
			eviction_error := temporal_core.worker_request_workflow_eviction(
				&activity_worker.core,
				processed.run_id,
			)
			if eviction_error.message != nil {
				fmt.eprintf("Workflow eviction request failed: %s\n", string(eviction_error.message))
				temporal_core.error_free(&eviction_error)
				failed = true
				break
			}
			evictions_requested += 1
			if processed.started_timer {
				timer_started = true
			}
		}

		if processed.evicted {
			evictions_observed += 1
			if evictions_observed == 1 || evictions_observed == 3 {
				activity_error := worker.run(&activity_worker, max_tasks = 1)
				if activity_error.message != nil {
					fmt.eprintf("Activity execution failed: %s\n", string(activity_error.message))
					worker.error_free(&activity_error)
					failed = true
					break
				}
				activities_completed += 1
			}
		}

		if processed.completed {
			workflow_completed = true
		}
		if processed.evicted && workflow_completed {
			final_eviction = true
		}
	}

	if !failed && (evictions_requested != 3 || activities_completed != 2 || !timer_started || !workflow_completed || !final_eviction) {
		fmt.eprintln("Workflow spike did not observe Activity -> timer -> Activity with eviction at every suspension")
		failed = true
	}
	if !failed && (workflow_runtime.replay_initializations < 3 || workflow_runtime.evictions < 4) {
		fmt.eprintf(
			"Workflow spike expected three replays and four evictions; replay_initializations=%d evictions=%d\n",
			workflow_runtime.replay_initializations,
			workflow_runtime.evictions,
		)
		failed = true
	}

	workflow.fiber_runtime_close(&workflow_runtime)
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
	fmt.println("sequential Activity -> timer -> Activity Workflow completed across three forced evictions")
}

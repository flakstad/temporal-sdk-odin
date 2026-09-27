package tests

import allocator_runtime "base:runtime"
import "core:testing"
import "core:bytes"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import activity "../temporal/activity"
import temporal_client "../temporal/client"
import temporal_core "../temporal/core"
import raw "../temporal/core/raw"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import protobuf "../temporal/protobuf"
import worker "../temporal/worker"
import workflow "../temporal/workflow"

Heartbeat_Capture :: struct {
	calls:   int,
	details: string,
}

@(test)
child_workflow_start_completion_and_fresh_stack_replay :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"child_parent_test_workflow",
		"parent-task-queue",
		child_parent_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "child_parent_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "child-parent-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)

	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, initial.error.message == nil, "Child Workflow start failed: %s", string(initial.error.message))
	testing.expect(t, initial.started_child_workflow)
	testing.expect_value(t, initial.child_workflow_seq, u32(1))
	testing.expect_value(t, initial.waiting_child_workflow_start_seq, u32(1))
	initial_bytes := bytes.clone(initial.completion[:])
	inspection := inspect_workflow_completion(initial.completion[:])
	testing.expect(t, inspection.decoded)
	testing.expect_value(t, inspection.command_count, 1)
	testing.expect_value(t, inspection.started_child_workflow_seq, u32(1))
	testing.expect_value(t, string(inspection.child_workflow_id), "child-test-id")
	testing.expect_value(t, string(inspection.child_workflow_type), "child_test_workflow")
	testing.expect_value(t, string(inspection.child_workflow_task_queue), "parent-task-queue")
	testing.expect_value(
		t,
		inspection.child_workflow_cancellation_type,
		proto.Coresdk_Child_Workflow_ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
	)
	delete(inspection.child_workflow_id)
	delete(inspection.child_workflow_type)
	delete(inspection.child_workflow_task_queue)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, evicted.error.message == nil)
	testing.expect(t, evicted.evicted)
	delete(evicted.completion)
	workflow.error_free(&evicted.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, replayed.error.message == nil)
	testing.expect(t, replayed.started_child_workflow)
	testing.expect(t, bytes.equal(initial_bytes, replayed.completion[:]))
	delete(initial_bytes)
	delete(replayed.completion)
	workflow.error_free(&replayed.error)

	start_success := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartSuccess{
		run_id = "child-run-id",
	}
	resolve_start := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded{
			value = &start_success,
		},
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	started := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, started.error.message == nil, "Child Workflow start resolution failed: %s", string(started.error.message))
	testing.expect_value(t, started.resolved_child_workflow_start_seq, u32(1))
	testing.expect_value(t, started.waiting_child_workflow_seq, u32(1))
	testing.expect(t, !started.completed)
	delete(started.completion)
	workflow.error_free(&started.error)

	child_output, output_error := codec.encode_json("Hello from child")
	testing.expect(t, output_error == .None)
	defer codec.payload_free(&child_output)
	proto_output := proto.encode_payload(codec.payload_view(&child_output))
	defer delete(proto_output.metadata)
	defer delete(proto_output.external_payloads)
	child_success := proto.Coresdk_Child_Workflow_Success{result = &proto_output}
	child_result := proto.Coresdk_Child_Workflow_ChildWorkflowResult{
		status = proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Completed{
			value = &child_success,
		},
	}
	resolve_child := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecution{
		seq = 1,
		result = &child_result,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 11
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution{
				value = &resolve_child,
			},
		},
	)
	completed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, completed.error.message == nil, "Child Workflow completion failed: %s", string(completed.error.message))
	testing.expect_value(t, completed.resolved_child_workflow_seq, u32(1))
	testing.expect(t, completed.completed)
	completed_inspection := inspect_workflow_completion(completed.completion[:])
	testing.expect(t, completed_inspection.decoded)
	testing.expect(t, completed_inspection.completed)
	testing.expect_value(t, string(completed_inspection.completion_result), `"Hello from child"`)
	delete(completed_inspection.completion_result)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
child_workflow_cancellation_requests_core_and_waits_for_start_resolution :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"child_parent_test_workflow",
		"parent-task-queue",
		child_parent_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("cancel-me")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "child_parent_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "child-parent-cancel-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, initial.error.message == nil)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	cancel := proto.Coresdk_Workflow_Activation_CancelWorkflow{reason = "cancel parent and child"}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 6
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	cancel_requested := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, cancel_requested.error.message == nil)
	testing.expect(t, cancel_requested.requested_child_workflow_cancellation)
	testing.expect_value(t, cancel_requested.canceled_child_workflow_seq, u32(1))
	testing.expect_value(t, cancel_requested.waiting_child_workflow_start_seq, u32(1))
	cancel_inspection := inspect_workflow_completion(cancel_requested.completion[:])
	testing.expect_value(t, cancel_inspection.command_count, 1)
	testing.expect_value(t, cancel_inspection.canceled_child_workflow_seq, u32(1))
	testing.expect_value(
		t,
		string(cancel_inspection.canceled_child_workflow_reason),
		"cancel parent and child",
	)
	delete(cancel_inspection.canceled_child_workflow_reason)
	delete(cancel_requested.completion)
	workflow.error_free(&cancel_requested.error)

	cancel_failure := proto.Temporal_Api_Failure_V1_Failure{message = "child start canceled"}
	start_canceled := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartCancelled{
		failure = &cancel_failure,
	}
	resolve_start := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Cancelled{
			value = &start_canceled,
		},
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	completed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, completed.error.message == nil, "Child cancellation resolution failed: %s", string(completed.error.message))
	testing.expect(t, completed.canceled_workflow)
	testing.expect(t, completed.completed)
	terminal := inspect_workflow_completion(completed.completion[:])
	testing.expect_value(t, terminal.command_count, 1)
	testing.expect(t, terminal.canceled_workflow)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
child_workflow_handle_signals_and_explicitly_cancels :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"child_handle_signal_cancel_test_workflow",
		"parent-task-queue",
		child_handle_signal_cancel_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "child_handle_signal_cancel_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "child-handle-parent-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, initial.error.message == nil, "Child handle initialization failed: %s", string(initial.error.message))
	testing.expect(t, initial.started_child_workflow)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	start_success := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartSuccess{
		run_id = "child-handle-run",
	}
	resolve_start := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded{
			value = &start_success,
		},
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	signaled := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, signaled.error.message == nil, "Child signal command failed: %s", string(signaled.error.message))
	testing.expect(t, signaled.signaled_child_workflow)
	testing.expect_value(t, signaled.child_workflow_signal_seq, u32(1))
	signal_inspection := inspect_workflow_completion(signaled.completion[:])
	testing.expect_value(t, signal_inspection.command_count, 1)
	testing.expect_value(t, signal_inspection.child_workflow_signal_seq, u32(1))
	testing.expect_value(t, string(signal_inspection.child_workflow_signal_id), "child-handle-test-id")
	testing.expect_value(t, string(signal_inspection.child_workflow_signal_name), "approve")
	testing.expect_value(t, string(signal_inspection.child_workflow_signal_input), `"approved-by-parent"`)
	signal_command_bytes := bytes.clone(signaled.completion[:])
	delete(signal_inspection.child_workflow_signal_id)
	delete(signal_inspection.child_workflow_signal_name)
	delete(signal_inspection.child_workflow_signal_input)
	delete(signaled.completion)
	workflow.error_free(&signaled.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, evicted.error.message == nil)
	testing.expect(t, evicted.evicted)
	delete(evicted.completion)
	workflow.error_free(&evicted.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed_start := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, replayed_start.error.message == nil)
	testing.expect(t, replayed_start.started_child_workflow)
	delete(replayed_start.completion)
	workflow.error_free(&replayed_start.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	replayed_signal := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, replayed_signal.error.message == nil)
	testing.expect(t, replayed_signal.signaled_child_workflow)
	testing.expect(t, bytes.equal(signal_command_bytes, replayed_signal.completion[:]))
	delete(signal_command_bytes)
	delete(replayed_signal.completion)
	workflow.error_free(&replayed_signal.error)

	resolve_signal := proto.Coresdk_Workflow_Activation_ResolveSignalExternalWorkflow{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 11
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Signal_External_Workflow{
				value = &resolve_signal,
			},
		},
	)
	cancel_requested := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, cancel_requested.error.message == nil, "explicit Child cancellation failed: %s", string(cancel_requested.error.message))
	testing.expect_value(t, cancel_requested.resolved_child_workflow_signal_seq, u32(1))
	testing.expect(t, cancel_requested.requested_child_workflow_cancellation)
	cancel_inspection := inspect_workflow_completion(cancel_requested.completion[:])
	testing.expect_value(t, cancel_inspection.command_count, 1)
	testing.expect_value(t, cancel_inspection.canceled_child_workflow_seq, u32(1))
	testing.expect_value(
		t,
		string(cancel_inspection.canceled_child_workflow_reason),
		"parent requested explicit child cancellation",
	)
	delete(cancel_inspection.canceled_child_workflow_reason)
	delete(cancel_requested.completion)
	workflow.error_free(&cancel_requested.error)

	canceled_info := proto.Temporal_Api_Failure_V1_CanceledFailureInfo{}
	canceled_failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "Child Workflow canceled",
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info{
			value = &canceled_info,
		},
	}
	child_cancellation := proto.Coresdk_Child_Workflow_Cancellation{failure = &canceled_failure}
	child_result := proto.Coresdk_Child_Workflow_ChildWorkflowResult{
		status = proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Cancelled{
			value = &child_cancellation,
		},
	}
	resolve_child := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecution{
		seq = 1,
		result = &child_result,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 15
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution{
				value = &resolve_child,
			},
		},
	)
	completed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, completed.error.message == nil, "explicit Child cancellation resolution failed: %s", string(completed.error.message))
	testing.expect(t, completed.completed)
	completion := inspect_workflow_completion(completed.completion[:])
	testing.expect(t, completion.completed)
	testing.expect_value(
		t,
		string(completion.completion_result),
		`"Child Workflow signaled and explicitly canceled"`,
	)
	delete(completion.completion_result)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
child_workflow_signal_failure_cause_is_catchable :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"child_signal_failure_test_workflow",
		"parent-task-queue",
		child_signal_failure_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "child_signal_failure_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "child-signal-failure-parent-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, initial.error.message == nil)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	start_success := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartSuccess{
		run_id = "missing-child-handle-run",
	}
	resolve_start := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded{
			value = &start_success,
		},
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	signaled := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, signaled.error.message == nil)
	testing.expect(t, signaled.signaled_child_workflow)
	delete(signaled.completion)
	workflow.error_free(&signaled.error)

	signal_failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "Child Workflow signal failed",
	}
	resolve_signal := proto.Coresdk_Workflow_Activation_ResolveSignalExternalWorkflow{
		seq = 1,
		failure = &signal_failure,
		cause = .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 11
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Signal_External_Workflow{
				value = &resolve_signal,
			},
		},
	)
	completed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, completed.error.message == nil, "Child signal failure resolution failed: %s", string(completed.error.message))
	testing.expect(t, completed.resolved_child_workflow_signal_failed)
	testing.expect(t, completed.completed)
	completion := inspect_workflow_completion(completed.completion[:])
	testing.expect(t, completion.completed)
	testing.expect_value(
		t,
		string(completion.completion_result),
		`"Observed Child Workflow signal failure"`,
	)
	delete(completion.completion_result)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
workflow_cancellation_cancels_in_flight_child_signal :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"child_handle_signal_cancel_test_workflow",
		"parent-task-queue",
		child_handle_signal_cancel_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "child_handle_signal_cancel_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "child-signal-root-cancel-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, initial.error.message == nil)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	start_success := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartSuccess{
		run_id = "child-handle-run",
	}
	resolve_start := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded{
			value = &start_success,
		},
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	signaled := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, signaled.error.message == nil)
	testing.expect(t, signaled.signaled_child_workflow)
	delete(signaled.completion)
	workflow.error_free(&signaled.error)

	cancel := proto.Coresdk_Workflow_Activation_CancelWorkflow{
		reason = "cancel parent while signaling child",
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 10
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	canceled := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, canceled.error.message == nil, "in-flight Child signal cancellation failed: %s", string(canceled.error.message))
	testing.expect(t, canceled.canceled_workflow)
	testing.expect(t, canceled.requested_child_workflow_cancellation)
	testing.expect(t, canceled.canceled_child_workflow_signal)
	inspection := inspect_workflow_completion(canceled.completion[:])
	testing.expect_value(t, inspection.command_count, 3)
	testing.expect_value(t, inspection.canceled_child_workflow_seq, u32(1))
	testing.expect_value(t, inspection.canceled_external_signal_seq, u32(1))
	testing.expect(t, inspection.canceled_workflow)
	delete(inspection.canceled_child_workflow_reason)
	delete(canceled.completion)
	workflow.error_free(&canceled.error)
}

@(test)
child_workflow_start_failure_is_catchable :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"child_start_failure_test_workflow",
		"parent-task-queue",
		child_start_failure_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "child_start_failure_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "child-start-failure-parent-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, initial.error.message == nil)
	testing.expect(t, initial.started_child_workflow)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	start_failed := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartFailure{
		workflow_id = "duplicate-child-id",
		workflow_type = "child_start_failure_workflow",
		cause = .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS,
	}
	resolve_start := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Failed{
			value = &start_failed,
		},
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	completed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, completed.error.message == nil, "Child start failure resolution failed: %s", string(completed.error.message))
	testing.expect_value(t, completed.resolved_child_workflow_start_seq, u32(1))
	testing.expect(t, completed.completed)
	completion := inspect_workflow_completion(completed.completion[:])
	testing.expect(t, completion.completed)
	testing.expect_value(
		t,
		string(completion.completion_result),
		`"Observed Child Workflow start failure"`,
	)
	delete(completion.completion_result)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
external_workflow_signal_and_cancel_are_replay_safe :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"external_signal_cancel_test_workflow",
		"caller-task-queue",
		external_signal_cancel_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("target-workflow")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "external_signal_cancel_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "external-signal-cancel-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)

	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, initial.error.message == nil, "external signal scheduling failed: %s", string(initial.error.message))
	testing.expect(t, initial.signaled_external_workflow)
	testing.expect(t, !initial.signaled_child_workflow)
	initial_bytes := bytes.clone(initial.completion[:])
	signal_command := inspect_workflow_completion(initial.completion[:])
	testing.expect_value(t, signal_command.command_count, 1)
	testing.expect_value(t, signal_command.child_workflow_signal_seq, u32(1))
	testing.expect_value(t, string(signal_command.external_workflow_signal_namespace), "billing")
	testing.expect_value(t, string(signal_command.external_workflow_signal_id), "target-workflow")
	testing.expect_value(t, string(signal_command.external_workflow_signal_run_id), "target-run")
	testing.expect_value(t, string(signal_command.child_workflow_signal_name), "approve")
	testing.expect_value(t, string(signal_command.child_workflow_signal_input), `"approved-by-caller"`)
	delete(signal_command.child_workflow_signal_name)
	delete(signal_command.child_workflow_signal_input)
	delete(signal_command.external_workflow_signal_namespace)
	delete(signal_command.external_workflow_signal_id)
	delete(signal_command.external_workflow_signal_run_id)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, evicted.error.message == nil)
	delete(evicted.completion)
	workflow.error_free(&evicted.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, replayed.error.message == nil)
	testing.expect(t, bytes.equal(initial_bytes, replayed.completion[:]))
	delete(initial_bytes)
	delete(replayed.completion)
	workflow.error_free(&replayed.error)

	resolve_signal := proto.Coresdk_Workflow_Activation_ResolveSignalExternalWorkflow{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Signal_External_Workflow{
				value = &resolve_signal,
			},
		},
	)
	cancel_requested := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, cancel_requested.error.message == nil, "external cancellation scheduling failed: %s", string(cancel_requested.error.message))
	testing.expect_value(t, cancel_requested.resolved_external_workflow_signal_seq, u32(1))
	testing.expect_value(t, cancel_requested.resolved_child_workflow_signal_seq, u32(0))
	testing.expect(t, cancel_requested.requested_external_workflow_cancellation)
	cancel_command := inspect_workflow_completion(cancel_requested.completion[:])
	testing.expect_value(t, cancel_command.command_count, 1)
	testing.expect_value(t, cancel_command.external_workflow_cancel_seq, u32(1))
	testing.expect_value(t, string(cancel_command.external_workflow_cancel_namespace), "billing")
	testing.expect_value(t, string(cancel_command.external_workflow_cancel_id), "target-workflow")
	testing.expect_value(t, string(cancel_command.external_workflow_cancel_run_id), "target-run")
	testing.expect_value(t, string(cancel_command.external_workflow_cancel_reason), "caller requested cancellation")
	delete(cancel_command.external_workflow_cancel_namespace)
	delete(cancel_command.external_workflow_cancel_id)
	delete(cancel_command.external_workflow_cancel_run_id)
	delete(cancel_command.external_workflow_cancel_reason)
	delete(cancel_requested.completion)
	workflow.error_free(&cancel_requested.error)

	resolve_cancel := proto.Coresdk_Workflow_Activation_ResolveRequestCancelExternalWorkflow{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 11
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Request_Cancel_External_Workflow{
				value = &resolve_cancel,
			},
		},
	)
	completed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, completed.error.message == nil, "external cancellation resolution failed: %s", string(completed.error.message))
	testing.expect_value(t, completed.resolved_external_workflow_cancellation_seq, u32(1))
	testing.expect(t, completed.completed)
	completion := inspect_workflow_completion(completed.completion[:])
	testing.expect(t, completion.completed)
	testing.expect_value(t, string(completion.completion_result), `"External Workflow signaled and canceled"`)
	delete(completion.completion_result)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
external_workflow_cancel_failure_cause_is_catchable :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"external_cancel_failure_test_workflow",
		"caller-task-queue",
		external_cancel_failure_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("missing-workflow")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "external_cancel_failure_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "external-cancel-failure-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, initial.error.message == nil)
	testing.expect(t, initial.requested_external_workflow_cancellation)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "External Workflow cancellation failed",
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveRequestCancelExternalWorkflow{
		seq = 1,
		failure = &failure,
		cause = .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Request_Cancel_External_Workflow{
				value = &resolve,
			},
		},
	)
	completed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, completed.error.message == nil, "external cancellation failure resolution failed: %s", string(completed.error.message))
	testing.expect(t, completed.resolved_external_workflow_cancellation_failed)
	testing.expect(t, completed.completed)
	completion := inspect_workflow_completion(completed.completion[:])
	testing.expect_value(t, string(completion.completion_result), `"Observed external Workflow cancellation failure"`)
	delete(completion.completion_result)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
continue_as_new_is_terminal_and_replay_safe :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"continue_as_new_test_workflow",
		"continue-task-queue",
		continue_as_new_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("first")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "continue_as_new_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "continue-first-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, initial.error.message == nil, "Continue-As-New command failed: %s", string(initial.error.message))
	testing.expect(t, initial.continued_as_new)
	testing.expect(t, initial.completed)
	initial_bytes := bytes.clone(initial.completion[:])
	inspection := inspect_workflow_completion(initial.completion[:])
	testing.expect_value(t, inspection.command_count, 1)
	testing.expect(t, inspection.continued_as_new)
	testing.expect_value(t, string(inspection.continue_workflow_type), "continue_as_new_test_workflow")
	testing.expect_value(t, string(inspection.continue_task_queue), "continue-task-queue")
	testing.expect_value(t, string(inspection.continue_input), `"second"`)
	testing.expect_value(t, inspection.continue_run_timeout, 5 * time.Minute)
	testing.expect_value(t, inspection.continue_task_timeout, 12 * time.Second)
	testing.expect_value(t, inspection.continue_backoff, 3 * time.Second)
	testing.expect_value(t, inspection.continue_retry_attempts, i32(7))
	testing.expect_value(t, inspection.continue_retry_initial, 2 * time.Second)
	delete(inspection.continue_workflow_type)
	delete(inspection.continue_task_queue)
	delete(inspection.continue_input)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, evicted.error.message == nil)
	delete(evicted.completion)
	workflow.error_free(&evicted.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, replayed.error.message == nil)
	testing.expect(t, replayed.continued_as_new)
	testing.expect(t, bytes.equal(initial_bytes, replayed.completion[:]))
	delete(initial_bytes)
	delete(replayed.completion)
	workflow.error_free(&replayed.error)

	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	replay_evicted := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, replay_evicted.error.message == nil)
	delete(replay_evicted.completion)
	workflow.error_free(&replay_evicted.error)

	second_input, second_input_error := codec.encode_json("second")
	testing.expect(t, second_input_error == .None)
	defer codec.payload_free(&second_input)
	proto_second_input := proto.encode_payload(codec.payload_view(&second_input))
	defer delete(proto_second_input.metadata)
	defer delete(proto_second_input.external_payloads)
	delete(initialize.arguments)
	initialize.arguments = nil
	append(&initialize.arguments, proto_second_input)
	delete(activation.jobs)
	activation.jobs = nil
	activation.run_id = "continue-second-run"
	activation.is_replaying = false
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	second := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, second.error.message == nil)
	testing.expect(t, second.completed)
	testing.expect(t, !second.continued_as_new)
	second_inspection := inspect_workflow_completion(second.completion[:])
	testing.expect(t, second_inspection.completed)
	testing.expect_value(t, string(second_inspection.completion_result), `"second"`)
	delete(second_inspection.completion_result)
	delete(second.completion)
	workflow.error_free(&second.error)
}

@(test)
continue_as_new_invalid_options_fail_the_workflow :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"continue_as_new_invalid_options_test_workflow",
		"continue-task-queue",
		continue_as_new_invalid_options_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("invalid")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "continue_as_new_invalid_options_test_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "continue-invalid-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "invalid Continue-As-New handling failed: %s", string(processed.error.message))
	testing.expect(t, processed.completed)
	inspection := inspect_workflow_failure_command(processed.completion[:])
	testing.expect(t, inspection.found)
	testing.expect_value(t, string(inspection.failure_type), "OdinContinueAsNewOptionsError")
	testing.expect_value(
		t,
		string(inspection.message),
		"Continue-As-New timeouts and backoff must not be negative",
	)
	delete(inspection.message)
	delete(inspection.failure_type)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
child_workflow_failure_is_structured_and_catchable :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"child_failure_observer_test_workflow",
		"parent-task-queue",
		child_failure_observer_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	completed := drive_child_failure_test(
		t,
		&runtime,
		"child_failure_observer_test_workflow",
		"child-failure-observer-run",
		"child-failure-id",
	)
	testing.expectf(t, completed.error.message == nil, "structured Child failure failed: %s", string(completed.error.message))
	testing.expect(t, completed.completed)
	payload, found := clone_workflow_completion_payload(completed.completion[:])
	testing.expect(t, found)
	if found {
		value: string
		decode_error := codec.decode_json(codec.payload_view(&payload), &value)
		testing.expect(t, decode_error == .None)
		testing.expect_value(t, value, "Observed structured Child Workflow failure")
		delete(value)
	}
	codec.payload_free(&payload)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
unhandled_child_workflow_failure_preserves_original_temporal_wrapper :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"child_failure_unhandled_test_workflow",
		"parent-task-queue",
		child_failure_unhandled_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	completed := drive_child_failure_test(
		t,
		&runtime,
		"child_failure_unhandled_test_workflow",
		"child-failure-unhandled-run",
		"child-failure-unhandled-id",
	)
	testing.expectf(t, completed.error.message == nil, "unhandled Child failure failed: %s", string(completed.error.message))
	testing.expect(t, completed.completed)
	inspection := inspect_workflow_failure_command(completed.completion[:])
	testing.expect(t, inspection.found)
	testing.expect(t, inspection.child_workflow)
	testing.expect_value(t, string(inspection.message), "Child Workflow failed")
	testing.expect_value(t, string(inspection.child_workflow_id), "child-failure-unhandled-id")
	testing.expect_value(t, string(inspection.child_workflow_type), "child_failure_test_workflow")
	testing.expect_value(t, string(inspection.cause_message), "billing rejected child")
	testing.expect_value(t, string(inspection.cause_type), "ChildBillingRejected")
	delete(inspection.message)
	delete(inspection.failure_type)
	delete(inspection.child_workflow_id)
	delete(inspection.child_workflow_type)
	delete(inspection.cause_message)
	delete(inspection.cause_type)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

Heartbeat_Progress :: struct {
	completed: int,
}

Heartbeat_Progress_Capture :: struct {
	calls:     int,
	completed: int,
}

capture_heartbeat :: proc(
	user_data: rawptr,
	details: codec.Payload_View,
) -> activity.Heartbeat_Status {
	capture := cast(^Heartbeat_Capture)user_data
	capture.calls += 1
	decoded, decode_error := codec.decode_string(details.data, codec.encoding(details))
	if decode_error != .None {
		return .Encoding_Failed
	}
	capture.details = decoded
	return .Recorded
}

capture_progress_heartbeat :: proc(
	user_data: rawptr,
	details: codec.Payload_View,
) -> activity.Heartbeat_Status {
	capture := cast(^Heartbeat_Progress_Capture)user_data
	progress: Heartbeat_Progress
	if codec.decode_json(details, &progress) != .None {
		return .Encoding_Failed
	}
	capture.calls += 1
	capture.completed = progress.completed
	return .Recorded
}

Codec_Test_Value :: struct {
	name:    string,
	count:   int,
	enabled: bool,
}

typed_activity_test :: proc(
	_: ^activity.Context,
	input: Codec_Test_Value,
) -> Codec_Test_Value {
	return {
		name = input.name,
		count = input.count + 1,
		enabled = !input.enabled,
	}
}

Activity_Failure_Details :: struct {
	dependency: string,
	attempt:    u32,
}

typed_activity_result_test :: proc(
	_: ^activity.Context,
	input: Codec_Test_Value,
) -> activity.Result(Codec_Test_Value) {
	if input.count == 0 {
		return activity.result_failure(
			Codec_Test_Value,
			"input count must not be zero",
			failure_type = "InvalidCount",
			non_retryable = true,
		)
	}
	if input.count < 0 {
		return activity.result_failure_json(
			Codec_Test_Value,
			"dependency is temporarily unavailable",
			Activity_Failure_Details{dependency = "test", attempt = 1},
			failure_type = "TransientTestFailure",
			next_retry_delay = 250 * time.Millisecond,
		)
	}
	return activity.result_success(typed_activity_test(nil, input))
}

Typed_Workflow_Input :: struct {
	name:       string,
	salutation: string,
}

Typed_Workflow_Output :: struct {
	message:     string,
	name_length: int,
}

typed_payload_workflow_test :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Typed_Workflow_Input,
) -> Typed_Workflow_Output {
	return workflow.execute_activity_json(
		ctx,
		"typed_greeting",
		input,
		Typed_Workflow_Output,
	)
}

activity_options_workflow_test :: proc(ctx: ^workflow.Fiber_Context, input: string) -> string {
	non_retryable_types := [?]string{"PermanentFailure", "InvalidInput"}
	retry_policy := workflow.Retry_Policy{
		initial_interval = 250 * time.Millisecond,
		backoff_coefficient = 1.5,
		maximum_interval = 3 * time.Second,
		maximum_attempts = 7,
		non_retryable_error_types = non_retryable_types[:],
	}
	return workflow.execute_activity_json_with_options(
		ctx,
		"configured_activity",
		input,
		string,
		{
			activity_id = "business-activity-id",
			task_queue = "configured-activity-queue",
			schedule_to_close_timeout = 45 * time.Second,
			schedule_to_start_timeout = 4 * time.Second,
			start_to_close_timeout = 12 * time.Second,
			heartbeat_timeout = 2 * time.Second,
			retry_policy = &retry_policy,
			cancellation_type = .Wait_Cancellation_Completed,
			disable_eager_execution = true,
		},
	)
}

invalid_activity_options_workflow_test :: proc(ctx: ^workflow.Fiber_Context, input: string) -> string {
	return workflow.execute_activity_json_with_options(
		ctx,
		"invalid_activity",
		input,
		string,
		{},
	)
}

payload_passthrough_workflow_test :: proc(
	ctx: ^workflow.Fiber_Context,
	input: codec.Payload_View,
) -> workflow.Payload_Result {
	result := workflow.execute_activity_payload(ctx, "payload_echo", input)
	owned, clone_error := codec.payload_clone(result)
	if clone_error != .None {
		return workflow.payload_fail(
			"could not preserve payload echo result",
			"OdinWorkflowPayloadConversionError",
		)
	}
	return workflow.payload_complete(owned)
}

fiber_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	greeting := workflow.execute_activity(ctx, "hello", name)
	return greeting
}

fiber_chain_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	first := workflow.execute_activity(ctx, "hello", name)
	workflow.sleep(ctx, time.Millisecond)
	return workflow.execute_activity(ctx, "hello", first)
}

fiber_parallel_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	first := workflow.start_activity(ctx, "hello", name)
	second := workflow.start_activity(ctx, "hello", "Temporal")
	first_result := workflow.await_activity(ctx, first)
	second_result := workflow.await_activity(ctx, second)
	result, allocation_error := strings.concatenate({first_result, " & ", second_result})
	if allocation_error != nil {
		return ""
	}
	return result
}

Failure_Test_Details :: struct {
	dependency: string,
	code:       int,
}

failure_test_matches :: proc(failure: ^workflow.Activity_Failure) -> bool {
	application := failure.application
	details: Failure_Test_Details
	detail_found, detail_error := workflow.activity_failure_detail_json(
		failure,
		0,
		&details,
	)
	return failure.message == "Activity task failed" &&
	       failure.activity_id == "failure-activity" &&
	       failure.activity_type == "missing" &&
	       failure.identity == "test-worker" &&
	       failure.retry_state == .Maximum_Attempts_Reached &&
	       application.present && application.message == "dependency rejected request" &&
	       application.type == "DependencyRejected" && !application.non_retryable &&
	       application.category == .Benign && application.has_next_retry_delay &&
	       application.next_retry_delay == 250 * time.Millisecond &&
	       detail_found && detail_error == .None &&
	       details.dependency == "billing" && details.code == 409
}

fiber_failure_recovery_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	failed := workflow.execute_activity_result(ctx, "missing", name)
	if failed.status != .Failed {
		return "unexpected Activity success"
	}
	if !failure_test_matches(&failed.failure) {
		return "structured Activity failure did not survive"
	}
	recovery_input, allocation_error := strings.concatenate({"Recovered ", name})
	if allocation_error != nil {
		return ""
	}
	return workflow.execute_activity(ctx, "hello", recovery_input)
}

fiber_delayed_failure_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	first := workflow.start_activity(ctx, "hello", name)
	second := workflow.start_activity(ctx, "missing", name)
	_ = workflow.await_activity(ctx, first)
	failed := workflow.await_activity_result(ctx, second)
	if failed.status == .Failed && failure_test_matches(&failed.failure) {
		return "preserved"
	}
	return "lost"
}

fiber_cancellation_cleanup_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	name: string,
) -> string {
	pending := workflow.execute_activity_result(ctx, "hello", name)
	if pending.status != .Canceled || !workflow.is_cancellation_requested(ctx) ||
	   workflow.cancellation_reason(ctx) != "operator cleanup request" {
		return ""
	}
	scope := workflow.detached_cancellation_scope(ctx)
	if !scope.active {
		return ""
	}
	defer workflow.cancellation_scope_end(&scope)
	cleanup := workflow.execute_activity_result(ctx, "cleanup", name)
	if cleanup.status != .Succeeded || cleanup.value != "Cleaned Odin" ||
	   workflow.cancellation_reason(ctx) != "operator cleanup request" {
		return ""
	}
	return ""
}

fiber_preemptive_detached_activity_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	name: string,
) -> string {
	scope := workflow.detached_cancellation_scope(ctx)
	if !scope.active {
		return "could not open preemptive detached scope"
	}
	defer workflow.cancellation_scope_end(&scope)
	result := workflow.execute_activity_result(ctx, "hello", name)
	if result.status != .Succeeded {
		return "detached Activity did not finish"
	}
	return result.value
}

Query_Test_State :: struct {
	phase: string,
	count: int,
}

Query_Test_Input :: struct {
	prefix: string,
}

Query_Test_Output :: struct {
	message: string,
	count:   int,
}

Query_Mutation_State :: struct {
	ctx: ^workflow.Fiber_Context,
}

Update_Test_Input :: struct {
	amount: int,
}

Update_Test_Output :: struct {
	previous: int,
	current:  int,
}

Update_Routine_Input :: struct {
	name: string,
}

Update_Routine_Output :: struct {
	previous:        string,
	current:         string,
	activity_result: string,
}

Update_Routine_State :: struct {
	name: string,
}

Signal_Routine_Input :: struct {
	name: string,
}

Signal_Routine_State :: struct {
	name: string,
}

fiber_signal_handler_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	state := Signal_Routine_State{name = name}
	registered := workflow.set_signal_handler_json(
		ctx,
		"rename_immediately",
		&state,
		proc(state: ^Signal_Routine_State, input: string) {
			state.name = input
		},
	)
	if !registered {
		return "Workflow Signal handler registration failed"
	}
	_ = workflow.await_signal(ctx, "finish")
	return state.name
}

fiber_signal_routine_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	state := Signal_Routine_State{name = name}
	registered := workflow.set_signal_routine_json(
		ctx,
		"rename_with_activity",
		&state,
		proc(
			routine_ctx: ^workflow.Fiber_Context,
			state: ^Signal_Routine_State,
			input: Signal_Routine_Input,
		) {
			state.name = workflow.execute_activity(routine_ctx, "decorate_name", input.name)
		},
	)
	if !registered {
		return "Workflow Signal routine registration failed"
	}
	finish := workflow.await_signal(ctx, "finish")
	workflow.await_all_handlers_finished(ctx)
	result, allocation_error := strings.concatenate({state.name, " / ", finish})
	if allocation_error != nil {
		return ""
	}
	return result
}

fiber_update_routine_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	state := Update_Routine_State{name = name}
	registered := workflow.set_validated_update_routine_json(
		ctx,
		"rename_with_activity",
		&state,
		proc(_: Update_Routine_State, input: Update_Routine_Input) -> workflow.Update_Validation {
			if len(input.name) == 0 {
				return workflow.update_reject("name is required", "InvalidName")
			}
			return workflow.update_accept()
		},
		proc(
			routine_ctx: ^workflow.Fiber_Context,
			state: ^Update_Routine_State,
			input: Update_Routine_Input,
		) -> Update_Routine_Output {
			previous := state.name
			activity_result := workflow.execute_activity(routine_ctx, "decorate_name", input.name)
			state.name = activity_result
			return {
				previous = previous,
				current = state.name,
				activity_result = activity_result,
			}
		},
	)
	if !registered {
		return "Workflow Update routine registration failed"
	}
	result := workflow.await_signal(ctx, "finish")
	workflow.await_all_handlers_finished(ctx)
	return result
}

fiber_abandon_update_routine_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	name: string,
) -> string {
	state := Update_Routine_State{name = name}
	registered := workflow.set_update_routine_json(
		ctx,
		"rename_with_activity",
		&state,
		proc(
			routine_ctx: ^workflow.Fiber_Context,
			state: ^Update_Routine_State,
			input: Update_Routine_Input,
		) -> Update_Routine_Output {
			previous := state.name
			activity_result := workflow.execute_activity(routine_ctx, "decorate_name", input.name)
			state.name = activity_result
			return {
				previous = previous,
				current = state.name,
				activity_result = activity_result,
			}
		},
	)
	if !registered {
		return "Workflow Update routine registration failed"
	}
	return workflow.await_signal(ctx, "finish")
}

fiber_silent_abandon_registration_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	name: string,
) -> string {
	state := Update_Routine_State{name = name}
	registered := workflow.set_update_routine_json(
		ctx,
		"silent_update",
		&state,
		proc(
			routine_ctx: ^workflow.Fiber_Context,
			_: ^Update_Routine_State,
			input: Update_Routine_Input,
		) -> Update_Routine_Output {
			_ = workflow.await_signal(routine_ctx, "release_silent_update")
			return {current = input.name}
		},
		{unfinished_policy = .Abandon},
	)
	if !registered {
		return "silent Workflow Update routine registration failed"
	}
	return workflow.await_signal(ctx, "finish")
}

fiber_detached_update_cancellation_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	name: string,
) -> string {
	state := Update_Routine_State{name = name}
	registered := workflow.set_update_routine_json(
		ctx,
		"detached_rename",
		&state,
		proc(
			routine_ctx: ^workflow.Fiber_Context,
			state: ^Update_Routine_State,
			input: Update_Routine_Input,
		) -> Update_Routine_Output {
			scope := workflow.detached_cancellation_scope(routine_ctx)
			if !scope.active {
				return {}
			}
			defer workflow.cancellation_scope_end(&scope)
			previous := state.name
			activity_result := workflow.execute_activity_result(
				routine_ctx,
				"decorate_name",
				input.name,
			)
			if activity_result.status != .Succeeded {
				return {previous = previous, current = "detached Activity failed"}
			}
			state.name = activity_result.value
			return {
				previous = previous,
				current = state.name,
				activity_result = activity_result.value,
			}
		},
	)
	if !registered {
		return "detached Workflow Update routine registration failed"
	}
	result := workflow.await_signal(ctx, "finish")
	if workflow.is_cancellation_requested(ctx) {
		scope := workflow.detached_cancellation_scope(ctx)
		if !scope.active {
			return "could not detach handler drain"
		}
		defer workflow.cancellation_scope_end(&scope)
		workflow.await_all_handlers_finished(ctx)
	}
	return result
}

fiber_query_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	state := Query_Test_State{phase = "waiting", count = len(name)}
	registered := workflow.set_query_handler_json(
		ctx,
		"status",
		&state,
		proc(state: Query_Test_State, input: Query_Test_Input) -> Query_Test_Output {
			message, allocation_error := strings.concatenate({input.prefix, state.phase})
			if allocation_error != nil {
				return {}
			}
			return {message = message, count = state.count}
		},
	)
	if !registered {
		return "query registration failed"
	}
	mutation_state := Query_Mutation_State{ctx = ctx}
	mutation_registered := workflow.set_query_handler_json(
		ctx,
		"mutate",
		&mutation_state,
		proc(state: Query_Mutation_State, _: Query_Test_Input) -> Query_Test_Output {
			_ = workflow.start_activity(state.ctx, "illegal-query-activity", "input")
			return {message = "mutation was not rejected"}
		},
	)
	if !mutation_registered {
		return "query mutation handler registration failed"
	}
	update_registered := workflow.set_validated_update_handler_json(
		ctx,
		"increment",
		&state,
		proc(state: Query_Test_State, input: Update_Test_Input) -> workflow.Update_Validation {
			if state.phase != "waiting" || input.amount <= 0 {
				return workflow.update_reject(
					"increment must be positive while waiting",
					"InvalidIncrement",
				)
			}
			return workflow.update_accept()
		},
		proc(state: ^Query_Test_State, input: Update_Test_Input) -> Update_Test_Output {
			previous := state.count
			state.count += input.amount
			return {previous = previous, current = state.count}
		},
	)
	if !update_registered {
		return "Workflow Update handler registration failed"
	}
	approval := workflow.await_signal(ctx, "finish")
	state.phase = "finished"
	return approval
}

fiber_buffered_signal_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	greeting := workflow.execute_activity(ctx, "hello", name)
	approval := workflow.await_signal(ctx, "approval")
	result, allocation_error := strings.concatenate({greeting, " / ", approval})
	if allocation_error != nil {
		return ""
	}
	return result
}

Signal_Test_Value :: struct {
	message:  string,
	approved: bool,
}

fiber_wait_signal_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	_ = name
	approval := workflow.await_signal_json(ctx, "approval", Signal_Test_Value)
	if !approval.approved {
		return "Denied"
	}
	return approval.message
}

fiber_signal_timeout_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	_ = name
	result := workflow.await_signal_or_timeout(ctx, "approval", 30 * time.Second)
	if result.status == .Timed_Out {
		return "Timed out"
	}
	return result.value
}

fiber_signal_timeout_activity_test_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	_ = name
	result := workflow.await_signal_or_timeout(ctx, "approval", 30 * time.Second)
	if result.status == .Timed_Out {
		return "Timed out"
	}
	return workflow.execute_activity(ctx, "hello", result.value)
}

registry_first_test_workflow :: proc(ctx: ^workflow.Fiber_Context, input: string) -> string {
	_ = ctx
	_ = input
	return "first"
}

registry_second_test_workflow :: proc(ctx: ^workflow.Fiber_Context, input: string) -> string {
	_ = ctx
	_ = input
	return "second"
}

child_parent_test_workflow :: proc(ctx: ^workflow.Fiber_Context, input: string) -> string {
	return workflow.execute_child_workflow(
		ctx,
		"child_test_workflow",
		input,
		{
			workflow_id = "child-test-id",
			workflow_execution_timeout = 10 * time.Minute,
			workflow_run_timeout = 5 * time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
}

child_handle_signal_cancel_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> string {
	future := workflow.start_child_workflow_json(
		ctx,
		"child_handle_test_workflow",
		input,
		{
			workflow_id = "child-handle-test-id",
			workflow_execution_timeout = 10 * time.Minute,
		},
	)
	start := workflow.await_child_workflow_start(ctx, future)
	if start.status != .Succeeded || start.run_id != "child-handle-run" {
		return "Child Workflow did not start"
	}
	signal := workflow.signal_child_workflow_json(ctx, future, "approve", "approved-by-parent")
	if signal.status != .Succeeded {
		return "Child Workflow signal failed"
	}
	if !workflow.cancel_child_workflow(ctx, future, "parent requested explicit child cancellation") {
		return "Child Workflow cancellation request failed"
	}
	result := workflow.await_child_workflow_result(ctx, future)
	if result.status != .Canceled || !result.failure.canceled {
		return "Child Workflow was not canceled"
	}
	return "Child Workflow signaled and explicitly canceled"
}

child_signal_failure_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> string {
	future := workflow.start_child_workflow_json(
		ctx,
		"child_handle_test_workflow",
		input,
		{
			workflow_id = "missing-child-handle-id",
			workflow_execution_timeout = 10 * time.Minute,
		},
	)
	signal := workflow.signal_child_workflow_json(ctx, future, "approve", "never-delivered")
	if signal.status == .Failed && signal.cause == .Workflow_Not_Found &&
	   signal.failure_message == "Child Workflow signal failed" {
		return "Observed Child Workflow signal failure"
	}
	return "Child Workflow signal failure mismatch"
}

child_start_failure_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> string {
	future := workflow.start_child_workflow_json(
		ctx,
		"child_start_failure_workflow",
		input,
		{
			workflow_id = "duplicate-child-id",
			workflow_execution_timeout = 10 * time.Minute,
		},
	)
	start := workflow.await_child_workflow_start(ctx, future)
	if start.status == .Failed && start.cause == .Workflow_Already_Exists &&
	   start.workflow_id == "duplicate-child-id" &&
	   start.workflow_type == "child_start_failure_workflow" {
		return "Observed Child Workflow start failure"
	}
	return "Child Workflow start failure mismatch"
}

external_signal_cancel_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	workflow_id: string,
) -> string {
	execution := workflow.External_Workflow_Execution{
		namespace = "billing",
		workflow_id = workflow_id,
		run_id = "target-run",
	}
	signal := workflow.signal_external_workflow_json(
		ctx,
		execution,
		"approve",
		"approved-by-caller",
	)
	if signal.status != .Succeeded {
		return "External Workflow signal failed"
	}
	cancel := workflow.request_cancel_external_workflow(
		ctx,
		execution,
		"caller requested cancellation",
	)
	if cancel.status != .Succeeded {
		return "External Workflow cancellation failed"
	}
	return "External Workflow signaled and canceled"
}

external_cancel_failure_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	workflow_id: string,
) -> string {
	result := workflow.request_cancel_external_workflow(
		ctx,
		{
			namespace = "default",
			workflow_id = workflow_id,
		},
		"cancel missing Workflow",
	)
	if result.status == .Failed && result.cause == .Workflow_Not_Found &&
	   result.failure_message == "External Workflow cancellation failed" {
		return "Observed external Workflow cancellation failure"
	}
	return "External Workflow cancellation failure mismatch"
}

continue_as_new_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> string {
	if input == "first" {
		retry_policy := workflow.Retry_Policy{
			initial_interval = 2 * time.Second,
			backoff_coefficient = 2,
			maximum_interval = 30 * time.Second,
			maximum_attempts = 7,
		}
		workflow.continue_as_new_json(
			ctx,
			"second",
			{
				workflow_run_timeout = 5 * time.Minute,
				workflow_task_timeout = 12 * time.Second,
				retry_policy = &retry_policy,
				backoff_start_interval = 3 * time.Second,
			},
		)
	}
	return input
}

continue_as_new_invalid_options_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> string {
	workflow.continue_as_new_json(
		ctx,
		input,
		{workflow_run_timeout = -time.Second},
	)
}

Child_Failure_Details :: struct {
	dependency: string,
	code:       int,
}

child_failure_observer_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> string {
	result := workflow.execute_child_workflow_json_result(
		ctx,
		"child_failure_test_workflow",
		input,
		{
			workflow_id = "child-failure-id",
			workflow_execution_timeout = time.Minute,
		},
		string,
	)
	if result.status != .Failed {
		return "expected Child Workflow failure"
	}
	failure := &result.failure
	details: Child_Failure_Details
	detail_found, detail_error := workflow.child_workflow_failure_detail_json(
		failure,
		0,
		&details,
	)
	if failure.message != "Child Workflow failed" || failure.source != "TemporalSDK" ||
	   failure.namespace != "default" || failure.workflow_id != "child-failure-id" ||
	   failure.run_id != "child-failure-run" || failure.workflow_type != "child_failure_test_workflow" ||
	   failure.initiated_event_id != 5 || failure.started_event_id != 6 ||
	   failure.retry_state != .Maximum_Attempts_Reached || !failure.application.present ||
	   failure.application.message != "billing rejected child" ||
	   failure.application.type != "ChildBillingRejected" ||
	   !failure.application.non_retryable || !detail_found || detail_error != .None ||
	   details.dependency != "billing" || details.code != 409 {
		return "structured Child Workflow failure mismatch"
	}
	return "Observed structured Child Workflow failure"
}

child_failure_unhandled_test_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: string,
) -> string {
	return workflow.execute_child_workflow(
		ctx,
		"child_failure_test_workflow",
		input,
		{
			workflow_id = "child-failure-unhandled-id",
			workflow_execution_timeout = time.Minute,
		},
	)
}

process_fiber_test_activation :: proc(
	runtime: ^workflow.Fiber_Runtime,
	activation: ^proto.Coresdk_Workflow_Activation_WorkflowActivation,
) -> workflow.Process_Result {
	encoded := proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(activation)
	defer delete(encoded)
	return workflow.process_fiber_activation(runtime, encoded[:])
}

drive_child_failure_test :: proc(
	t: ^testing.T,
	runtime: ^workflow.Fiber_Runtime,
	parent_workflow_type, run_id, child_workflow_id: string,
) -> workflow.Process_Result {
	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = parent_workflow_type,
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = run_id,
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_fiber_test_activation(runtime, &activation)
	testing.expectf(t, initial.error.message == nil, "Child failure test initialization failed: %s", string(initial.error.message))
	testing.expect(t, initial.started_child_workflow)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	start_success := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStartSuccess{
		run_id = "child-failure-run",
	}
	resolve_start := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart{
		seq = 1,
		status = proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded{
			value = &start_success,
		},
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start{
				value = &resolve_start,
			},
		},
	)
	started := process_fiber_test_activation(runtime, &activation)
	testing.expectf(t, started.error.message == nil, "Child failure start resolution failed: %s", string(started.error.message))
	delete(started.completion)
	workflow.error_free(&started.error)

	detail, detail_error := codec.encode_json(Child_Failure_Details{dependency = "billing", code = 409})
	testing.expect(t, detail_error == .None)
	defer codec.payload_free(&detail)
	proto_detail := proto.encode_payload(codec.payload_view(&detail))
	defer delete(proto_detail.metadata)
	defer delete(proto_detail.external_payloads)
	details := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(details.payloads)
	append(&details.payloads, proto_detail)
	application_info := proto.Temporal_Api_Failure_V1_ApplicationFailureInfo{
		type = "ChildBillingRejected",
		non_retryable = true,
		details = &details,
	}
	cause := proto.Temporal_Api_Failure_V1_Failure{
		message = "billing rejected child",
		source = "OdinSDK",
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = &application_info,
		},
	}
	workflow_execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
		workflow_id = child_workflow_id,
		run_id = "child-failure-run",
	}
	workflow_type := proto.Temporal_Api_Common_V1_WorkflowType{
		name = "child_failure_test_workflow",
	}
	child_info := proto.Temporal_Api_Failure_V1_ChildWorkflowExecutionFailureInfo{
		namespace = "default",
		workflow_execution = &workflow_execution,
		workflow_type = &workflow_type,
		initiated_event_id = 5,
		started_event_id = 6,
		retry_state = .RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
	}
	failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "Child Workflow failed",
		source = "TemporalSDK",
		stack_trace = "child stack",
		cause = &cause,
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Child_Workflow_Execution_Failure_Info{
			value = &child_info,
		},
	}
	child_failed := proto.Coresdk_Child_Workflow_Failure{failure = &failure}
	child_result := proto.Coresdk_Child_Workflow_ChildWorkflowResult{
		status = proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Failed{
			value = &child_failed,
		},
	}
	resolve_child := proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecution{
		seq = 1,
		result = &child_result,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 12
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution{
				value = &resolve_child,
			},
		},
	)
	return process_fiber_test_activation(runtime, &activation)
}

Workflow_Completion_Inspection :: struct {
	decoded:          bool,
	failed:           bool,
	failure_message:  []u8,
	failure_cause:    proto.Temporal_Api_Enums_V1_WorkflowTaskFailedCause,
	command_count:    int,
	scheduled_activity_seq: u32,
	started_child_workflow_seq: u32,
	child_workflow_id: []u8,
	child_workflow_type: []u8,
	child_workflow_task_queue: []u8,
	child_workflow_cancellation_type: proto.Coresdk_Child_Workflow_ChildWorkflowCancellationType,
	canceled_child_workflow_seq: u32,
	canceled_child_workflow_reason: []u8,
	child_workflow_signal_seq: u32,
	child_workflow_signal_id: []u8,
	child_workflow_signal_name: []u8,
	child_workflow_signal_input: []u8,
	external_workflow_signal_namespace: []u8,
	external_workflow_signal_id: []u8,
	external_workflow_signal_run_id: []u8,
	external_workflow_cancel_seq: u32,
	external_workflow_cancel_namespace: []u8,
	external_workflow_cancel_id: []u8,
	external_workflow_cancel_run_id: []u8,
	external_workflow_cancel_reason: []u8,
	continued_as_new: bool,
	continue_workflow_type: []u8,
	continue_task_queue: []u8,
	continue_input: []u8,
	continue_run_timeout: time.Duration,
	continue_task_timeout: time.Duration,
	continue_backoff: time.Duration,
	continue_retry_attempts: i32,
	continue_retry_initial: time.Duration,
	canceled_external_signal_seq: u32,
	canceled_timer_seq:     u32,
	canceled_activity_seq:  u32,
	canceled_workflow:      bool,
	completed:        bool,
	completion_result: []u8,
}

inspect_workflow_completion :: proc(data: []u8) -> (inspection: Workflow_Completion_Inspection) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	inspection.decoded = proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
		data,
		&completion,
	)
	context.allocator = previous_allocator
	if !inspection.decoded {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return
		}
		inspection.command_count = len(status.value.commands)
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity:
				if variant.value != nil {
					inspection.scheduled_activity_seq = variant.value.seq
				}
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Child_Workflow_Execution:
				if variant.value != nil {
					inspection.started_child_workflow_seq = variant.value.seq
					inspection.child_workflow_id = bytes.clone(transmute([]u8)variant.value.workflow_id)
					inspection.child_workflow_type = bytes.clone(transmute([]u8)variant.value.workflow_type)
					inspection.child_workflow_task_queue = bytes.clone(transmute([]u8)variant.value.task_queue)
					inspection.child_workflow_cancellation_type = variant.value.cancellation_type
				}
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Child_Workflow_Execution:
				if variant.value != nil {
					inspection.canceled_child_workflow_seq = variant.value.child_workflow_seq
					inspection.canceled_child_workflow_reason = bytes.clone(
						transmute([]u8)variant.value.reason,
					)
				}
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Signal_External_Workflow_Execution:
				if variant.value != nil {
					inspection.child_workflow_signal_seq = variant.value.seq
					inspection.child_workflow_signal_name = bytes.clone(
						transmute([]u8)variant.value.signal_name,
					)
					#partial switch target in variant.value.target {
					case proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution_Target_Child_Workflow_Id:
						inspection.child_workflow_signal_id = bytes.clone(
							transmute([]u8)target.value,
						)
					case proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution_Target_Workflow_Execution:
						if target.value != nil {
							inspection.external_workflow_signal_namespace = bytes.clone(
								transmute([]u8)target.value.namespace,
							)
							inspection.external_workflow_signal_id = bytes.clone(
								transmute([]u8)target.value.workflow_id,
							)
							inspection.external_workflow_signal_run_id = bytes.clone(
								transmute([]u8)target.value.run_id,
							)
						}
					}
					if len(variant.value.args) == 1 {
						inspection.child_workflow_signal_input = bytes.clone(variant.value.args[0].data)
					}
				}
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Signal_Workflow:
				if variant.value != nil {
					inspection.canceled_external_signal_seq = variant.value.seq
				}
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Request_Cancel_External_Workflow_Execution:
				if variant.value != nil {
					inspection.external_workflow_cancel_seq = variant.value.seq
					inspection.external_workflow_cancel_reason = bytes.clone(
						transmute([]u8)variant.value.reason,
					)
					if variant.value.workflow_execution != nil {
						inspection.external_workflow_cancel_namespace = bytes.clone(
							transmute([]u8)variant.value.workflow_execution.namespace,
						)
						inspection.external_workflow_cancel_id = bytes.clone(
							transmute([]u8)variant.value.workflow_execution.workflow_id,
						)
						inspection.external_workflow_cancel_run_id = bytes.clone(
							transmute([]u8)variant.value.workflow_execution.run_id,
						)
					}
				}
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Continue_As_New_Workflow_Execution:
				if variant.value != nil {
					inspection.continued_as_new = true
					inspection.continue_workflow_type = bytes.clone(
						transmute([]u8)variant.value.workflow_type,
					)
					inspection.continue_task_queue = bytes.clone(
						transmute([]u8)variant.value.task_queue,
					)
					if len(variant.value.arguments) == 1 {
						inspection.continue_input = bytes.clone(variant.value.arguments[0].data)
					}
					inspection.continue_run_timeout = proto_duration_value(
						variant.value.workflow_run_timeout,
					)
					inspection.continue_task_timeout = proto_duration_value(
						variant.value.workflow_task_timeout,
					)
					inspection.continue_backoff = proto_duration_value(
						variant.value.backoff_start_interval,
					)
					if variant.value.retry_policy != nil {
						inspection.continue_retry_attempts = variant.value.retry_policy.maximum_attempts
						inspection.continue_retry_initial = proto_duration_value(
							variant.value.retry_policy.initial_interval,
						)
					}
				}
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Timer:
				if variant.value != nil {
					inspection.canceled_timer_seq = variant.value.seq
				}
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Request_Cancel_Activity:
				if variant.value != nil {
					inspection.canceled_activity_seq = variant.value.seq
				}
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Workflow_Execution:
				inspection.canceled_workflow = variant.value != nil
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution:
				inspection.completed = variant.value != nil
				if variant.value != nil && variant.value.result != nil {
					inspection.completion_result = bytes.clone(variant.value.result.data)
				}
			}
		}
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Failed:
		inspection.failed = status.value != nil
		if status.value != nil {
			inspection.failure_cause = status.value.force_cause
			if status.value.failure != nil {
				inspection.failure_message = bytes.clone(
					transmute([]u8)status.value.failure.message,
				)
			}
		}
	}
	return
}

Query_Response_Inspection :: struct {
	found:           bool,
	failed:          bool,
	command_count:   int,
	payload:         codec.Payload,
	failure_message: []u8,
	failure_type:    []u8,
}

inspect_query_response :: proc(
	data: []u8,
	query_id: string,
) -> (inspection: Query_Response_Inspection) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
		data,
		&completion,
	)
	context.allocator = previous_allocator
	if !decoded {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return
		}
		inspection.command_count = len(status.value.commands)
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Respond_To_Query:
				if variant.value == nil || variant.value.query_id != query_id {
					continue
				}
				inspection.found = true
				#partial switch query_variant in variant.value.variant {
				case proto.Coresdk_Workflow_Commands_QueryResult_Variant_Succeeded:
					if query_variant.value != nil && query_variant.value.response != nil {
						cloned, clone_error := proto.clone_payload(query_variant.value.response)
						if clone_error == .None {
							inspection.payload = cloned
						}
					}
				case proto.Coresdk_Workflow_Commands_QueryResult_Variant_Failed:
					inspection.failed = query_variant.value != nil
					if query_variant.value != nil {
						inspection.failure_message = bytes.clone(
							transmute([]u8)query_variant.value.message,
						)
						#partial switch failure_info in query_variant.value.failure_info {
						case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
							if failure_info.value != nil {
								inspection.failure_type = bytes.clone(
									transmute([]u8)failure_info.value.type,
								)
							}
						}
					}
				}
				return
			}
		}
	}
	return
}

Update_Response_Inspection :: struct {
	accepted:        int,
	rejected:        int,
	completed:       int,
	command_count:   int,
	accepted_index:  int,
	completed_index: int,
	payload:          codec.Payload,
	failure_message:  []u8,
	failure_type:     []u8,
}

inspect_update_responses :: proc(
	data: []u8,
	protocol_instance_id: string,
) -> (inspection: Update_Response_Inspection) {
	inspection.accepted_index = -1
	inspection.completed_index = -1
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
		data,
		&completion,
	)
	context.allocator = previous_allocator
	if !decoded {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return
		}
		inspection.command_count = len(status.value.commands)
		for command, index in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Update_Response:
				if variant.value == nil || variant.value.protocol_instance_id != protocol_instance_id {
					continue
				}
				#partial switch response in variant.value.response {
				case proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Accepted:
					inspection.accepted += 1
					inspection.accepted_index = index
				case proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Completed:
					if response.value != nil {
						inspection.completed += 1
						inspection.completed_index = index
						cloned, clone_error := proto.clone_payload(response.value)
						if clone_error == .None {
							inspection.payload = cloned
						}
					}
				case proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Rejected:
					if response.value != nil {
						inspection.rejected += 1
						inspection.failure_message = bytes.clone(
							transmute([]u8)response.value.message,
						)
						#partial switch failure_info in response.value.failure_info {
						case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
							if failure_info.value != nil {
								inspection.failure_type = bytes.clone(
									transmute([]u8)failure_info.value.type,
								)
							}
						}
					}
				}
			}
		}
	}
	return
}

update_response_inspection_free :: proc(inspection: ^Update_Response_Inspection) {
	if inspection == nil {
		return
	}
	codec.payload_free(&inspection.payload)
	delete(inspection.failure_message)
	delete(inspection.failure_type)
	inspection^ = {}
}

clone_scheduled_activity_payload :: proc(
	data: []u8,
) -> (payload: codec.Payload, found: bool) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion)
	context.allocator = previous_allocator
	if !decoded {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return
		}
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity:
				if variant.value != nil && len(variant.value.arguments) == 1 {
					clone_error: codec.Error
					payload, clone_error = proto.clone_payload(&variant.value.arguments[0])
					found = clone_error == .None
					return
				}
			}
		}
	}
	return
}

Scheduled_Activity_Inspection :: struct {
	found:                     bool,
	activity_id:               string,
	activity_type:             string,
	task_queue:                string,
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	heartbeat_timeout:         time.Duration,
	retry_policy_present:      bool,
	retry_initial_interval:    time.Duration,
	retry_backoff_coefficient: f64,
	retry_maximum_interval:    time.Duration,
	retry_maximum_attempts:    i32,
	non_retryable_error_types: [dynamic]string,
	cancellation_type: proto.Coresdk_Workflow_Commands_ActivityCancellationType,
	disable_eager_execution: bool,
}

proto_duration_value :: proc(value: ^proto.Google_Protobuf_Duration) -> time.Duration {
	if value == nil {
		return 0
	}
	return time.Duration(value.seconds * i64(time.Second) + i64(value.nanos))
}

scheduled_activity_inspection_free :: proc(inspection: ^Scheduled_Activity_Inspection) {
	if inspection == nil {
		return
	}
	delete(inspection.activity_id)
	delete(inspection.activity_type)
	delete(inspection.task_queue)
	for error_type in inspection.non_retryable_error_types {
		delete(error_type)
	}
	delete(inspection.non_retryable_error_types)
	inspection^ = {}
}

inspect_scheduled_activity :: proc(
	data: []u8,
) -> (inspection: Scheduled_Activity_Inspection) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion)
	context.allocator = previous_allocator
	if !decoded {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return
		}
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity:
				if variant.value == nil {
					return
				}
				schedule := variant.value
				inspection.found = true
				inspection.activity_id, _ = strings.clone(schedule.activity_id)
				inspection.activity_type, _ = strings.clone(schedule.activity_type)
				inspection.task_queue, _ = strings.clone(schedule.task_queue)
				inspection.schedule_to_close_timeout = proto_duration_value(schedule.schedule_to_close_timeout)
				inspection.schedule_to_start_timeout = proto_duration_value(schedule.schedule_to_start_timeout)
				inspection.start_to_close_timeout = proto_duration_value(schedule.start_to_close_timeout)
				inspection.heartbeat_timeout = proto_duration_value(schedule.heartbeat_timeout)
				inspection.cancellation_type = schedule.cancellation_type
				inspection.disable_eager_execution = schedule.do_not_eagerly_execute
				if schedule.retry_policy != nil {
					policy := schedule.retry_policy
					inspection.retry_policy_present = true
					inspection.retry_initial_interval = proto_duration_value(policy.initial_interval)
					inspection.retry_backoff_coefficient = policy.backoff_coefficient
					inspection.retry_maximum_interval = proto_duration_value(policy.maximum_interval)
					inspection.retry_maximum_attempts = policy.maximum_attempts
					inspection.non_retryable_error_types = make([dynamic]string)
					for error_type in policy.non_retryable_error_types {
						cloned, _ := strings.clone(error_type)
						append(&inspection.non_retryable_error_types, cloned)
					}
				}
				return
			}
		}
	}
	return
}

clone_workflow_completion_payload :: proc(
	data: []u8,
) -> (payload: codec.Payload, found: bool) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion)
	context.allocator = previous_allocator
	if !decoded {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return
		}
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution:
				if variant.value != nil && variant.value.result != nil {
					clone_error: codec.Error
					payload, clone_error = proto.clone_payload(variant.value.result)
					found = clone_error == .None
					return
				}
			}
		}
	}
	return
}

Workflow_Failure_Inspection :: struct {
	found:         bool,
	message:       []u8,
	failure_type:  []u8,
	non_retryable: bool,
	child_workflow: bool,
	child_workflow_id: []u8,
	child_workflow_type: []u8,
	cause_message: []u8,
	cause_type: []u8,
}

inspect_workflow_failure_command :: proc(
	data: []u8,
) -> (inspection: Workflow_Failure_Inspection) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion)
	context.allocator = previous_allocator
	if !decoded {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return
		}
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Fail_Workflow_Execution:
				if variant.value == nil || variant.value.failure == nil {
					return
				}
				inspection.message = bytes.clone(transmute([]u8)variant.value.failure.message)
				inspection.found = true
				#partial switch failure_info in variant.value.failure.failure_info {
				case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
					if failure_info.value != nil {
						inspection.failure_type = bytes.clone(
							transmute([]u8)failure_info.value.type,
						)
						inspection.non_retryable = failure_info.value.non_retryable
					}
				case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Child_Workflow_Execution_Failure_Info:
					if failure_info.value != nil {
						inspection.child_workflow = true
						if failure_info.value.workflow_execution != nil {
							inspection.child_workflow_id = bytes.clone(
								transmute([]u8)failure_info.value.workflow_execution.workflow_id,
							)
						}
						if failure_info.value.workflow_type != nil {
							inspection.child_workflow_type = bytes.clone(
								transmute([]u8)failure_info.value.workflow_type.name,
							)
						}
					}
				}
				if variant.value.failure.cause != nil {
					cause := variant.value.failure.cause
					inspection.cause_message = bytes.clone(transmute([]u8)cause.message)
					#partial switch cause_info in cause.failure_info {
					case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
						if cause_info.value != nil {
							inspection.cause_type = bytes.clone(transmute([]u8)cause_info.value.type)
						}
					}
				}
				return
			}
		}
	}
	return
}

@(test)
runtime_init_and_shutdown :: proc(t: ^testing.T) {
	runtime, error := temporal_core.runtime_create()
	if error.message != nil {
		defer temporal_core.error_free(&error)
		testing.expectf(t, false, "runtime_create failed: %s", string(error.message))
		return
	}
	testing.expect(t, runtime.handle != nil)
	temporal_core.runtime_close(&runtime)
	testing.expect(t, runtime.handle == nil)
	// Closing twice is intentionally safe in the Odin lifecycle wrapper.
	temporal_core.runtime_close(&runtime)
}

@(test)
language_worker_delays_core_construction_until_run :: proc(t: ^testing.T) {
	raw_runtime := raw.Runtime{}
	runtime := temporal_core.Runtime{handle = &raw_runtime}
	raw_connection := raw.Connection{}
	client := temporal_core.Client{handle = &raw_connection, runtime = &runtime}

	sdk_worker, create_error := worker.create(&client, "delayed-core", "test-namespace")
	testing.expectf(t, create_error.message == nil, "worker.create failed: %s", string(create_error.message))
	worker.error_free(&create_error)
	testing.expect(t, sdk_worker.core.handle == nil)
	testing.expect(t, sdk_worker.client == &client)
	testing.expect_value(t, sdk_worker.namespace, "test-namespace")
	testing.expect_value(t, sdk_worker.task_queue, "delayed-core")
	testing.expect(t, sdk_worker.lifecycle == .Configuring)

	register_error := worker.register_workflow(&sdk_worker, "first_workflow", registry_first_test_workflow)
	testing.expectf(t, register_error.message == nil, "workflow registration failed: %s", string(register_error.message))
	worker.error_free(&register_error)
	testing.expect(t, sdk_worker.core.handle == nil)

	client.handle = nil
	_, run_error := worker.run(&sdk_worker)
	testing.expect(t, run_error.message != nil)
	worker.error_free(&run_error)
	testing.expect(t, sdk_worker.core.handle == nil)
	testing.expect(t, sdk_worker.lifecycle == .Configuring)
	client.handle = &raw_connection

	close_error := worker.close(&sdk_worker)
	testing.expectf(t, close_error.message == nil, "worker.close before run failed: %s", string(close_error.message))
	worker.error_free(&close_error)
	testing.expect(t, sdk_worker.lifecycle == .Closed)
	testing.expect(t, sdk_worker.client == nil)
	testing.expect(t, sdk_worker.namespace == "")
	testing.expect(t, sdk_worker.task_queue == "")

	// The language lifecycle wrapper intentionally makes close idempotent.
	close_error = worker.close(&sdk_worker)
	testing.expect(t, close_error.message == nil)
	worker.error_free(&close_error)
}

@(test)
language_worker_rejects_run_without_materializing_empty_core_worker :: proc(t: ^testing.T) {
	raw_runtime := raw.Runtime{}
	runtime := temporal_core.Runtime{handle = &raw_runtime}
	raw_connection := raw.Connection{}
	client := temporal_core.Client{handle = &raw_connection, runtime = &runtime}

	sdk_worker, create_error := worker.create(&client, "empty-worker")
	testing.expect(t, create_error.message == nil)
	worker.error_free(&create_error)
	_, run_error := worker.run(&sdk_worker)
	testing.expect(t, run_error.message != nil)
	worker.error_free(&run_error)
	testing.expect(t, sdk_worker.core.handle == nil)
	testing.expect(t, sdk_worker.lifecycle == .Configuring)

	close_error := worker.close(&sdk_worker)
	testing.expect(t, close_error.message == nil)
	worker.error_free(&close_error)
}

@(test)
error_free_is_idempotent :: proc(t: ^testing.T) {
	error := temporal_core.Error{
		message = bytes.clone(transmute([]u8)string("owned")),
		status_code = 13,
		details = bytes.clone(transmute([]u8)string("grpc-status-details")),
	}
	temporal_core.error_free(&error)
	testing.expect(t, error.message == nil)
	testing.expect(t, error.details == nil)
	testing.expect_value(t, error.status_code, u32(0))
	temporal_core.error_free(&error)
}

@(test)
client_close_event_result_owns_payload_after_decode :: proc(t: ^testing.T) {
	expected := Codec_Test_Value{name = "client-result", count = 41, enabled = true}
	owned_payload, encode_error := codec.encode_json(expected)
	testing.expect(t, encode_error == .None)
	defer codec.payload_free(&owned_payload)
	proto_payload := proto.encode_payload(codec.payload_view(&owned_payload))
	defer delete(proto_payload.metadata)
	defer delete(proto_payload.external_payloads)
	payloads := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(payloads.payloads)
	append(&payloads.payloads, proto_payload)
	completed := proto.Temporal_Api_History_V1_WorkflowExecutionCompletedEventAttributes{
		result = &payloads,
	}
	event := proto.Temporal_Api_History_V1_HistoryEvent{
		event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		attributes = proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Completed_Event_Attributes{
			value = &completed,
		},
	}
	history := proto.Temporal_Api_History_V1_History{}
	defer delete(history.events)
	append(&history.events, event)
	response := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse{
		history = &history,
	}
	encoded := proto.encode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse(&response)

	page, page_error := temporal_client.decode_result_page(encoded[:])
	delete(encoded)
	defer temporal_client.result_page_free(&page)
	defer temporal_client.error_free(&page_error)
	testing.expect(t, page_error.message == nil)
	testing.expect(t, page.kind == .Completed)
	testing.expect(t, page.has_payload)
	actual: Codec_Test_Value
	decode_error := codec.decode_json(codec.payload_view(&page.payload), &actual)
	defer delete(actual.name)
	testing.expect(t, decode_error == .None)
	testing.expect_value(t, actual.name, expected.name)
	testing.expect_value(t, actual.count, expected.count)
	testing.expect_value(t, actual.enabled, expected.enabled)
}

@(test)
client_close_event_maps_failure_and_follow_run :: proc(t: ^testing.T) {
	failure := proto.Temporal_Api_Failure_V1_Failure{message = "order workflow failed"}
	failed := proto.Temporal_Api_History_V1_WorkflowExecutionFailedEventAttributes{
		failure = &failure,
	}
	event := proto.Temporal_Api_History_V1_HistoryEvent{
		event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		attributes = proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Failed_Event_Attributes{
			value = &failed,
		},
	}
	history := proto.Temporal_Api_History_V1_History{}
	defer delete(history.events)
	append(&history.events, event)
	response := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse{
		history = &history,
	}
	encoded := proto.encode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse(&response)
	page, page_error := temporal_client.decode_result_page(encoded[:])
	delete(encoded)
	testing.expect(t, page_error.message == nil)
	testing.expect(t, page.kind == .Failed)
	testing.expect_value(t, page.message, "order workflow failed")
	temporal_client.result_page_free(&page)
	temporal_client.error_free(&page_error)

	continued := proto.Temporal_Api_History_V1_WorkflowExecutionContinuedAsNewEventAttributes{
		new_execution_run_id = "continued-run-id",
	}
	history.events[0] = proto.Temporal_Api_History_V1_HistoryEvent{
		event_type = .EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		attributes = proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Continued_As_New_Event_Attributes{
			value = &continued,
		},
	}
	encoded = proto.encode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse(&response)
	page, page_error = temporal_client.decode_result_page(encoded[:])
	delete(encoded)
	defer temporal_client.result_page_free(&page)
	defer temporal_client.error_free(&page_error)
	testing.expect(t, page_error.message == nil)
	testing.expect(t, page.kind == .Continued_As_New)
	testing.expect_value(t, page.next_run_id, "continued-run-id")
}

@(test)
client_workflow_handle_owns_identifiers :: proc(t: ^testing.T) {
	connection := temporal_core.Client{handle = cast(^raw.Connection)(uintptr(1))}
	client := temporal_client.Client{connection = &connection}
	id, _ := strings.clone("owned-workflow-id")
	run_id, _ := strings.clone("owned-run-id")
	first_run_id, _ := strings.clone("owned-first-run-id")
	handle, handle_error := temporal_client.get_workflow_handle(
		&client,
		id,
		run_id,
		first_run_id,
	)
	delete(id)
	delete(run_id)
	delete(first_run_id)
	defer temporal_client.workflow_handle_free(&handle)
	defer temporal_client.error_free(&handle_error)
	testing.expect(t, handle_error.message == nil)
	testing.expect_value(t, handle.id, "owned-workflow-id")
	testing.expect_value(t, handle.run_id, "owned-run-id")
	testing.expect_value(t, handle.first_execution_run_id, "owned-first-run-id")
}

@(test)
client_control_operations_reject_invalid_handles :: proc(t: ^testing.T) {
	handle := temporal_client.Workflow_Handle{}
	signal_error := temporal_client.signal_workflow(&handle, "approval")
	defer temporal_client.error_free(&signal_error)
	testing.expect(t, signal_error.kind == .Invalid_Argument)
	cancel_error := temporal_client.cancel_workflow(&handle)
	defer temporal_client.error_free(&cancel_error)
	testing.expect(t, cancel_error.kind == .Invalid_Argument)
	terminate_error := temporal_client.terminate_workflow(&handle)
	defer temporal_client.error_free(&terminate_error)
	testing.expect(t, terminate_error.kind == .Invalid_Argument)
	update_handle := temporal_client.Update_Handle{}
	_, update_error := temporal_client.wait_update_payload(&update_handle)
	defer temporal_client.error_free(&update_error)
	testing.expect(t, update_error.kind == .Invalid_Argument)
}

@(test)
client_update_handle_owns_immediate_nested_failure :: proc(t: ^testing.T) {
	application := proto.Temporal_Api_Failure_V1_ApplicationFailureInfo{
		type = "InvalidGreetingName",
		non_retryable = true,
	}
	cause := proto.Temporal_Api_Failure_V1_Failure{
		message = "greeting name is invalid",
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = &application,
		},
	}
	failure := proto.Temporal_Api_Failure_V1_Failure{cause = &cause}
	outcome := proto.Temporal_Api_Update_V1_Outcome{
		value = proto.Temporal_Api_Update_V1_Outcome_Value_Failure{value = &failure},
	}
	heap := allocator_runtime.heap_allocator()
	previous_allocator := context.allocator
	context.allocator = heap
	_, owned_failure := temporal_client.update_outcome_payload(&outcome)
	workflow_id, _ := strings.clone("owned-workflow", heap)
	run_id, _ := strings.clone("owned-run", heap)
	update_id, _ := strings.clone("owned-update", heap)
	context.allocator = previous_allocator
	connection := temporal_core.Client{handle = cast(^raw.Connection)(uintptr(1))}
	client := temporal_client.Client{connection = &connection}
	handle := temporal_client.Update_Handle{
		client = &client,
		workflow_id = workflow_id,
		run_id = run_id,
		update_id = update_id,
		outcome_present = true,
		outcome_error = owned_failure,
	}
	defer temporal_client.update_handle_free(&handle)
	_, update_error := temporal_client.wait_update_payload(&handle)
	defer temporal_client.error_free(&update_error)
	testing.expect(t, update_error.kind == .Workflow_Update_Failed)
	testing.expect_value(t, string(update_error.message), "greeting name is invalid")
}

@(test)
json_string_codec_round_trip :: proc(t: ^testing.T) {
	encoded, encode_error := codec.encode_string("Odin")
	testing.expect(t, encode_error == .None)
	defer delete(encoded)
	testing.expect_value(t, string(encoded), `"Odin"`)

	decoded, decode_error := codec.decode_string(encoded, "json/plain")
	testing.expect(t, decode_error == .None)
	defer delete(decoded)
	testing.expect_value(t, decoded, "Odin")

	_, unsupported := codec.decode_string(encoded, "binary/plain")
	testing.expect(t, unsupported == .Unsupported_Encoding)
}

@(test)
json_payload_codec_round_trips_structs_and_owns_metadata :: proc(t: ^testing.T) {
	expected := Codec_Test_Value{name = "Odin", count = 7, enabled = true}
	payload, encode_error := codec.encode_json(expected)
	testing.expect(t, encode_error == .None)
	defer codec.payload_free(&payload)
	testing.expect_value(t, codec.encoding(codec.payload_view(&payload)), "json/plain")
	testing.expect(
		t,
		codec.payload_add_metadata(&payload, "test-key", transmute([]u8)string("first")) == .None,
	)
	testing.expect(
		t,
		codec.payload_add_metadata(&payload, "test-key", transmute([]u8)string("second")) == .None,
	)
	metadata_value, metadata_found := codec.metadata_get(codec.payload_view(&payload), "test-key")
	testing.expect(t, metadata_found)
	testing.expect_value(t, string(metadata_value), "second")
	testing.expect_value(t, len(payload.metadata), 2)

	decoded: Codec_Test_Value
	decode_error := codec.decode_json(codec.payload_view(&payload), &decoded)
	testing.expect(t, decode_error == .None)
	testing.expect_value(t, decoded.name, expected.name)
	testing.expect_value(t, decoded.count, expected.count)
	testing.expect_value(t, decoded.enabled, expected.enabled)
	delete(decoded.name)

	cloned, clone_error := codec.payload_clone(codec.payload_view(&payload))
	testing.expect(t, clone_error == .None)
	codec.payload_free(&payload)
	testing.expect_value(t, codec.encoding(codec.payload_view(&cloned)), "json/plain")
	testing.expect(t, len(cloned.data) > 0)
	codec.payload_free(&cloned)
}

@(test)
worker_json_activity_registration_adapts_typed_odin_values :: proc(t: ^testing.T) {
	sdk_worker := worker.Worker{
		activity_registrations = make([dynamic]worker.Activity_Registration),
		lifecycle = .Configuring,
	}
	defer {
		close_error := worker.close(&sdk_worker)
		testing.expect(t, close_error.message == nil)
		worker.error_free(&close_error)
	}

	register_error := worker.register_json_activity(
		&sdk_worker,
		"typed_activity",
		typed_activity_test,
	)
	testing.expect(t, register_error.message == nil)
	worker.error_free(&register_error)
	testing.expect_value(t, len(sdk_worker.activity_registrations), 1)
	if len(sdk_worker.activity_registrations) != 1 {
		return
	}

	input := Codec_Test_Value{name = "Temporal", count = 2, enabled = false}
	input_payload, encode_error := codec.encode_json(input)
	testing.expect(t, encode_error == .None)
	defer codec.payload_free(&input_payload)
	ctx := activity.Context{}
	registration := &sdk_worker.activity_registrations[0]
	callback_arena: virtual.Arena
	callback_arena_error := virtual.arena_init_growing(&callback_arena)
	testing.expect(t, callback_arena_error == nil)
	if callback_arena_error != nil {
		return
	}
	defer virtual.arena_destroy(&callback_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&callback_arena)
	result := registration.invoke(
		registration.callback_data,
		&ctx,
		codec.payload_view(&input_payload),
	)
	context.allocator = previous_allocator
	defer codec.payload_free(&result.payload)
	defer activity.application_failure_free(&result.failure)
	testing.expect(t, result.status == .Completed)

	output: Codec_Test_Value
	decode_error := codec.decode_json(codec.payload_view(&result.payload), &output)
	testing.expect(t, decode_error == .None)
	testing.expect_value(t, output.name, "Temporal")
	testing.expect_value(t, output.count, 3)
	testing.expect(t, output.enabled)
	delete(output.name)

	unsupported_metadata := [1]codec.Metadata_Entry{{
		key = "encoding",
		value = transmute([]u8)string("binary/plain"),
	}}
	failed := registration.invoke(
		registration.callback_data,
		&ctx,
		codec.Payload_View{metadata = unsupported_metadata[:], data = transmute([]u8)string("raw")},
	)
	defer codec.payload_free(&failed.payload)
	defer activity.application_failure_free(&failed.failure)
	testing.expect(t, failed.status == .Failed)
	testing.expect(t, len(failed.failure.message) > 0)
	testing.expect_value(t, failed.failure.type, "OdinPayloadConversionError")
	testing.expect(t, failed.failure.non_retryable)
}

@(test)
worker_json_activity_result_preserves_application_failure_fields :: proc(t: ^testing.T) {
	sdk_worker := worker.Worker{
		activity_registrations = make([dynamic]worker.Activity_Registration),
		lifecycle = .Configuring,
	}
	defer {
		close_error := worker.close(&sdk_worker)
		testing.expect(t, close_error.message == nil)
		worker.error_free(&close_error)
	}

	register_error := worker.register_json_activity_result(
		&sdk_worker,
		"typed_result",
		typed_activity_result_test,
	)
	testing.expect(t, register_error.message == nil)
	worker.error_free(&register_error)
	if len(sdk_worker.activity_registrations) != 1 {
		return
	}

	input_payload, encode_error := codec.encode_json(
		Codec_Test_Value{name = "Temporal", count = -1},
	)
	testing.expect(t, encode_error == .None)
	defer codec.payload_free(&input_payload)
	registration := &sdk_worker.activity_registrations[0]
	callback_arena: virtual.Arena
	testing.expect(t, virtual.arena_init_growing(&callback_arena) == nil)
	defer virtual.arena_destroy(&callback_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&callback_arena)
	result := registration.invoke(
		registration.callback_data,
		&activity.Context{},
		codec.payload_view(&input_payload),
	)
	context.allocator = previous_allocator
	defer codec.payload_free(&result.payload)
	defer activity.application_failure_free(&result.failure)

	testing.expect(t, result.status == .Failed)
	testing.expect_value(t, result.failure.message, "dependency is temporarily unavailable")
	testing.expect_value(t, result.failure.type, "TransientTestFailure")
	testing.expect(t, !result.failure.non_retryable)
	testing.expect(t, result.failure.has_details)
	testing.expect_value(t, result.failure.next_retry_delay, 250 * time.Millisecond)
	details: Activity_Failure_Details
	details_error := codec.decode_json(codec.payload_view(&result.failure.details), &details)
	testing.expect(t, details_error == .None)
	testing.expect_value(t, details.dependency, "test")
	testing.expect_value(t, details.attempt, u32(1))
	delete(details.dependency)
}

@(test)
typed_workflow_payloads_survive_activity_and_fresh_stack_replay :: proc(t: ^testing.T) {
	owned_task_queue, task_queue_error := strings.clone("test-queue")
	testing.expect(t, task_queue_error == nil)
	if task_queue_error != nil {
		return
	}
	sdk_worker := worker.Worker{
		task_queue = owned_task_queue,
		workflow_registrations = make([dynamic]worker.Workflow_Registration),
		lifecycle = .Configuring,
	}
	defer {
		close_error := worker.close(&sdk_worker)
		testing.expect(t, close_error.message == nil)
		worker.error_free(&close_error)
	}
	register_error := worker.register_json_workflow(
		&sdk_worker,
		"typed_payload_workflow",
		typed_payload_workflow_test,
	)
	testing.expect(t, register_error.message == nil)
	worker.error_free(&register_error)
	if len(sdk_worker.workflow_registrations) != 1 {
		return
	}
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input_value := Typed_Workflow_Input{name = "Odin", salutation = "Hello"}
	input, input_error := codec.encode_json(input_value)
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "typed_payload_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "typed-payload-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)

	initial := process_fiber_test_activation(runtime, &activation)
	testing.expect(t, initial.error.message == nil)
	testing.expect(t, initial.scheduled_activity)
	initial_completion := bytes.clone(initial.completion[:])
	scheduled_input, scheduled_found := clone_scheduled_activity_payload(initial.completion[:])
	testing.expect(t, scheduled_found)
	if scheduled_found {
		decoded_input: Typed_Workflow_Input
		decode_error := codec.decode_json(codec.payload_view(&scheduled_input), &decoded_input)
		testing.expect(t, decode_error == .None)
		testing.expect_value(t, decoded_input.name, "Odin")
		testing.expect_value(t, decoded_input.salutation, "Hello")
		delete(decoded_input.name)
		delete(decoded_input.salutation)
	}
	codec.payload_free(&scheduled_input)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_fiber_test_activation(runtime, &activation)
	testing.expect(t, evicted.error.message == nil)
	testing.expect(t, evicted.evicted)
	delete(evicted.completion)
	workflow.error_free(&evicted.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed := process_fiber_test_activation(runtime, &activation)
	testing.expect(t, replayed.error.message == nil)
	testing.expect(t, replayed.scheduled_activity)
	testing.expect(t, bytes.equal(initial_completion, replayed.completion[:]))
	delete(initial_completion)
	delete(replayed.completion)
	workflow.error_free(&replayed.error)

	activity_output, output_error := codec.encode_json(
		Typed_Workflow_Output{message = "Hello, Odin!", name_length = 4},
	)
	testing.expect(t, output_error == .None)
	defer codec.payload_free(&activity_output)
	proto_output := proto.encode_payload(codec.payload_view(&activity_output))
	defer delete(proto_output.metadata)
	defer delete(proto_output.external_payloads)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &proto_output}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 9
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	completed := process_fiber_test_activation(runtime, &activation)
	testing.expect(t, completed.error.message == nil)
	testing.expect(t, completed.completed)
	completion_payload, completion_found := clone_workflow_completion_payload(completed.completion[:])
	testing.expect(t, completion_found)
	if completion_found {
		decoded_output: Typed_Workflow_Output
		decode_error := codec.decode_json(codec.payload_view(&completion_payload), &decoded_output)
		testing.expect(t, decode_error == .None)
		testing.expect_value(t, decoded_output.message, "Hello, Odin!")
		testing.expect_value(t, decoded_output.name_length, 4)
		delete(decoded_output.message)
	}
	codec.payload_free(&completion_payload)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
typed_workflow_rejects_incompatible_input_as_application_failure :: proc(t: ^testing.T) {
	owned_task_queue, task_queue_error := strings.clone("test-queue")
	testing.expect(t, task_queue_error == nil)
	if task_queue_error != nil {
		return
	}
	sdk_worker := worker.Worker{
		task_queue = owned_task_queue,
		workflow_registrations = make([dynamic]worker.Workflow_Registration),
		lifecycle = .Configuring,
	}
	defer {
		close_error := worker.close(&sdk_worker)
		testing.expect(t, close_error.message == nil)
		worker.error_free(&close_error)
	}
	register_error := worker.register_json_workflow(
		&sdk_worker,
		"typed_payload_workflow",
		typed_payload_workflow_test,
	)
	testing.expect(t, register_error.message == nil)
	worker.error_free(&register_error)
	if len(sdk_worker.workflow_registrations) != 1 {
		return
	}

	input, input_error := codec.encode_json("not a structured input")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "typed_payload_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "typed-invalid-input-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(
		&sdk_worker.workflow_registrations[0].runtime,
		&activation,
	)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.completed)
	inspection := inspect_workflow_failure_command(processed.completion[:])
	testing.expect(t, inspection.found)
	testing.expect_value(t, string(inspection.message), "could not decode Workflow input as json/plain")
	testing.expect_value(t, string(inspection.failure_type), "OdinWorkflowPayloadConversionError")
	testing.expect(t, inspection.non_retryable)
	delete(inspection.message)
	delete(inspection.failure_type)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
payload_workflow_preserves_metadata_and_external_details :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init_payload(
		&runtime,
		"payload_passthrough",
		"test-queue",
		payload_passthrough_workflow_test,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := codec.payload_create(transmute([]u8)string("opaque-input"))
	defer codec.payload_free(&input)
	testing.expect(
		t,
		codec.payload_add_metadata(
			&input,
			"encoding",
			transmute([]u8)string("binary/custom"),
		) == .None,
	)
	testing.expect(
		t,
		codec.payload_add_metadata(
			&input,
			"messageType",
			transmute([]u8)string("tests.CustomInput"),
		) == .None,
	)
	testing.expect(t, codec.payload_add_external(&input, 2048) == .None)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "payload_passthrough",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "payload-passthrough-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, initial.error.message == nil)
	scheduled, scheduled_found := clone_scheduled_activity_payload(initial.completion[:])
	testing.expect(t, scheduled_found)
	if scheduled_found {
		message_type, found := codec.metadata_get(codec.payload_view(&scheduled), "messageType")
		testing.expect(t, found)
		testing.expect_value(t, string(message_type), "tests.CustomInput")
		testing.expect_value(t, codec.encoding(codec.payload_view(&scheduled)), "binary/custom")
		testing.expect_value(t, string(scheduled.data), "opaque-input")
		testing.expect_value(t, len(scheduled.external_payloads), 1)
		if len(scheduled.external_payloads) == 1 {
			testing.expect_value(t, scheduled.external_payloads[0].size_bytes, i64(2048))
		}
	}
	codec.payload_free(&scheduled)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	activity_output := codec.payload_create(transmute([]u8)string("opaque-output"))
	defer codec.payload_free(&activity_output)
	testing.expect(
		t,
		codec.payload_add_metadata(
			&activity_output,
			"encoding",
			transmute([]u8)string("binary/custom"),
		) == .None,
	)
	testing.expect(
		t,
		codec.payload_add_metadata(
			&activity_output,
			"messageType",
			transmute([]u8)string("tests.CustomOutput"),
		) == .None,
	)
	testing.expect(t, codec.payload_add_external(&activity_output, 4096) == .None)
	proto_output := proto.encode_payload(codec.payload_view(&activity_output))
	defer delete(proto_output.metadata)
	defer delete(proto_output.external_payloads)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &proto_output}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	completed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, completed.error.message == nil)
	completion, completion_found := clone_workflow_completion_payload(completed.completion[:])
	testing.expect(t, completion_found)
	if completion_found {
		message_type, found := codec.metadata_get(codec.payload_view(&completion), "messageType")
		testing.expect(t, found)
		testing.expect_value(t, string(message_type), "tests.CustomOutput")
		testing.expect_value(t, codec.encoding(codec.payload_view(&completion)), "binary/custom")
		testing.expect_value(t, string(completion.data), "opaque-output")
		testing.expect_value(t, len(completion.external_payloads), 1)
		if len(completion.external_payloads) == 1 {
			testing.expect_value(t, completion.external_payloads[0].size_bytes, i64(4096))
		}
	}
	codec.payload_free(&completion)
	delete(completed.completion)
	workflow.error_free(&completed.error)
}

@(test)
activity_options_survive_command_encoding_and_fresh_stack_replay :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"activity_options_workflow",
		"workflow-default-queue",
		activity_options_workflow_test,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "activity_options_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "activity-options-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)

	initial := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, initial.error.message == nil)
	testing.expect(t, initial.scheduled_activity)
	initial_bytes := bytes.clone(initial.completion[:])
	inspection := inspect_scheduled_activity(initial.completion[:])
	defer scheduled_activity_inspection_free(&inspection)
	testing.expect(t, inspection.found)
	testing.expect_value(t, inspection.activity_id, "business-activity-id")
	testing.expect_value(t, inspection.activity_type, "configured_activity")
	testing.expect_value(t, inspection.task_queue, "configured-activity-queue")
	testing.expect_value(t, inspection.schedule_to_close_timeout, 45 * time.Second)
	testing.expect_value(t, inspection.schedule_to_start_timeout, 4 * time.Second)
	testing.expect_value(t, inspection.start_to_close_timeout, 12 * time.Second)
	testing.expect_value(t, inspection.heartbeat_timeout, 2 * time.Second)
	testing.expect(t, inspection.retry_policy_present)
	testing.expect_value(t, inspection.retry_initial_interval, 250 * time.Millisecond)
	testing.expect_value(t, inspection.retry_backoff_coefficient, 1.5)
	testing.expect_value(t, inspection.retry_maximum_interval, 3 * time.Second)
	testing.expect_value(t, inspection.retry_maximum_attempts, i32(7))
	testing.expect_value(t, len(inspection.non_retryable_error_types), 2)
	if len(inspection.non_retryable_error_types) == 2 {
		testing.expect_value(t, inspection.non_retryable_error_types[0], "PermanentFailure")
		testing.expect_value(t, inspection.non_retryable_error_types[1], "InvalidInput")
	}
	testing.expect(
		t,
		inspection.cancellation_type == .WAIT_CANCELLATION_COMPLETED,
	)
	testing.expect(t, inspection.disable_eager_execution)
	delete(initial.completion)
	workflow.error_free(&initial.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	evicted := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, evicted.error.message == nil)
	testing.expect(t, evicted.evicted)
	delete(evicted.completion)
	workflow.error_free(&evicted.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, replayed.error.message == nil)
	testing.expect(t, replayed.scheduled_activity)
	testing.expect(t, bytes.equal(initial_bytes, replayed.completion[:]))
	delete(initial_bytes)
	delete(replayed.completion)
	workflow.error_free(&replayed.error)
}

@(test)
invalid_activity_options_fail_the_workflow_deterministically :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"invalid_activity_options_workflow",
		"test-queue",
		invalid_activity_options_workflow_test,
	)
	testing.expect(t, init_error.message == nil)
	workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "invalid_activity_options_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "invalid-activity-options-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)

	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.completed)
	testing.expect(t, !processed.scheduled_activity)
	inspection := inspect_workflow_failure_command(processed.completion[:])
	testing.expect(t, inspection.found)
	testing.expect_value(t, string(inspection.failure_type), "OdinActivityOptionsError")
	testing.expect_value(
		t,
		string(inspection.message),
		"Activity requires a positive Schedule-to-Close or Start-to-Close timeout",
	)
	testing.expect(t, inspection.non_retryable)
	delete(inspection.message)
	delete(inspection.failure_type)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
worker_routes_registered_workflow_types_and_evictions :: proc(t: ^testing.T) {
	sdk_worker := worker.Worker{
		task_queue = "test-queue",
		workflow_registrations = make([dynamic]worker.Workflow_Registration),
		workflow_runs = make([dynamic]worker.Workflow_Run),
	}
	defer {
		for &registration in sdk_worker.workflow_registrations {
			workflow.fiber_runtime_close(&registration.runtime)
		}
		for workflow_run in sdk_worker.workflow_runs {
			delete(workflow_run.run_id)
		}
		delete(sdk_worker.workflow_registrations)
		delete(sdk_worker.workflow_runs)
	}

	first_runtime := workflow.Fiber_Runtime{}
	first_init_error := workflow.fiber_runtime_init(
		&first_runtime,
		"first_workflow",
		"test-queue",
		registry_first_test_workflow,
	)
	testing.expect(t, first_init_error.message == nil)
	workflow.error_free(&first_init_error)
	second_runtime := workflow.Fiber_Runtime{}
	second_init_error := workflow.fiber_runtime_init(
		&second_runtime,
		"second_workflow",
		"test-queue",
		registry_second_test_workflow,
	)
	testing.expect(t, second_init_error.message == nil)
	workflow.error_free(&second_init_error)
	append(
		&sdk_worker.workflow_registrations,
		worker.Workflow_Registration{name = "first_workflow", runtime = first_runtime},
		worker.Workflow_Registration{name = "second_workflow", runtime = second_runtime},
	)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"input"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	first_initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "first_workflow",
	}
	defer delete(first_initialize.arguments)
	append(&first_initialize.arguments, input)
	second_initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "second_workflow",
	}
	defer delete(second_initialize.arguments)
	append(&second_initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "registry-run-first",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &first_initialize,
			},
		},
	)
	encoded := proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed, dispatch_error := worker.process_workflow_activation(&sdk_worker, encoded[:])
	delete(encoded)
	testing.expectf(t, dispatch_error.message == nil, "first Workflow dispatch failed: %s", string(dispatch_error.message))
	worker.error_free(&dispatch_error)
	testing.expect(t, processed.error.message == nil)
	first_inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, first_inspection.decoded)
	testing.expect_value(t, string(first_inspection.completion_result), `"first"`)
	delete(first_inspection.completion_result)
	delete(processed.completion)
	workflow.error_free(&processed.error)
	testing.expect_value(t, len(sdk_worker.workflow_runs), 1)

	delete(activation.jobs)
	activation.jobs = nil
	activation.run_id = "registry-run-second"
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &second_initialize,
			},
		},
	)
	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed, dispatch_error = worker.process_workflow_activation(&sdk_worker, encoded[:])
	delete(encoded)
	testing.expectf(t, dispatch_error.message == nil, "second Workflow dispatch failed: %s", string(dispatch_error.message))
	worker.error_free(&dispatch_error)
	testing.expect(t, processed.error.message == nil)
	second_inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, second_inspection.decoded)
	testing.expect_value(t, string(second_inspection.completion_result), `"second"`)
	delete(second_inspection.completion_result)
	delete(processed.completion)
	workflow.error_free(&processed.error)
	testing.expect_value(t, len(sdk_worker.workflow_runs), 2)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	activation.run_id = "registry-run-first"
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed, dispatch_error = worker.process_workflow_activation(&sdk_worker, encoded[:])
	delete(encoded)
	testing.expect(t, dispatch_error.message == nil)
	worker.error_free(&dispatch_error)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)
	testing.expect_value(t, len(sdk_worker.workflow_runs), 1)
	testing.expect_value(t, sdk_worker.workflow_runs[0].run_id, "registry-run-second")

	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed, dispatch_error = worker.process_workflow_activation(&sdk_worker, encoded[:])
	delete(encoded)
	testing.expect(t, dispatch_error.message != nil)
	worker.error_free(&dispatch_error)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
worker_fails_unknown_workflow_activation_without_losing_poll_loop_state :: proc(t: ^testing.T) {
	sdk_worker := worker.Worker{
		task_queue = "test-queue",
		workflow_registrations = make([dynamic]worker.Workflow_Registration),
		workflow_runs = make([dynamic]worker.Workflow_Run),
		rejected_workflow_runs = make([dynamic]worker.Rejected_Workflow_Run),
	}
	defer {
		for rejected_run in sdk_worker.rejected_workflow_runs {
			delete(rejected_run.run_id)
		}
		delete(sdk_worker.workflow_registrations)
		delete(sdk_worker.workflow_runs)
		delete(sdk_worker.rejected_workflow_runs)
	}

	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "missing_workflow",
	}
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "unknown-workflow-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	encoded := proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed, dispatch_error := worker.process_workflow_activation(&sdk_worker, encoded[:])
	delete(encoded)
	testing.expectf(t, dispatch_error.message == nil, "unknown Workflow dispatch failed worker: %s", string(dispatch_error.message))
	worker.error_free(&dispatch_error)
	testing.expect(t, processed.activation_failed)
	testing.expect(t, processed.error.message != nil)
	testing.expect_value(t, len(sdk_worker.rejected_workflow_runs), 1)
	inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, inspection.decoded)
	testing.expect(t, inspection.failed)
	testing.expect_value(t, string(inspection.failure_message), "Workflow type missing_workflow not found")
	testing.expect(
		t,
		inspection.failure_cause == .WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
	)
	delete(inspection.failure_message)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{message = "activation failed"}
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed, dispatch_error = worker.process_workflow_activation(&sdk_worker, encoded[:])
	delete(encoded)
	testing.expectf(t, dispatch_error.message == nil, "unknown Workflow eviction failed: %s", string(dispatch_error.message))
	worker.error_free(&dispatch_error)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	testing.expect_value(t, len(sdk_worker.rejected_workflow_runs), 0)
	inspection = inspect_workflow_completion(processed.completion[:])
	testing.expect(t, inspection.decoded)
	testing.expect(t, !inspection.failed)
	testing.expect_value(t, inspection.command_count, 0)
	delete(inspection.failure_message)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed, dispatch_error = worker.process_workflow_activation(&sdk_worker, encoded[:])
	delete(encoded)
	testing.expect(t, dispatch_error.message != nil)
	worker.error_free(&dispatch_error)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
activity_task_protobuf_round_trip :: proc(t: ^testing.T) {
	payload := proto.Temporal_Api_Common_V1_Payload{
		data = transmute([]u8)string(`"Odin"`),
	}
	defer delete(payload.metadata)
	append(
		&payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	append(
		&payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "messageType",
			value = transmute([]u8)string("tests.Greeting"),
		},
	)
	defer delete(payload.external_payloads)
	append(
		&payload.external_payloads,
		proto.Temporal_Api_Common_V1_Payload_ExternalPayloadDetails{size_bytes = 2048},
	)
	start := proto.Coresdk_Activity_Task_Start{
		activity_id = "activity-id",
		activity_type = "hello",
		attempt = 2,
	}
	defer delete(start.input)
	append(&start.input, payload)
	raw_task := proto.Coresdk_Activity_Task_ActivityTask{
		task_token = transmute([]u8)string("token"),
		variant = proto.Coresdk_Activity_Task_ActivityTask_Variant_Start{value = &start},
	}
	task_bytes := proto.encode_Coresdk_Activity_Task_ActivityTask(&raw_task)
	defer delete(task_bytes)
	// Schema evolution must not make older bindings reject valid unknown fields.
	protobuf.append_varint_field(&task_bytes, 100, 7)

	decode_arena: virtual.Arena
	arena_error := virtual.arena_init_growing(&decode_arena)
	testing.expect(t, arena_error == nil)
	if arena_error != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)

	task, decode_error := proto.decode_activity_task(task_bytes[:])
	context.allocator = previous_allocator
	testing.expect(t, decode_error == .None)
	testing.expect(t, task.kind == .Start)
	testing.expect_value(t, string(task.task_token), "token")
	testing.expect_value(t, task.activity_id, "activity-id")
	testing.expect_value(t, task.activity_type, "hello")
	testing.expect_value(t, task.attempt, u32(2))
	testing.expect_value(t, task.input.encoding, "json/plain")
	testing.expect_value(t, string(task.input.data), `"Odin"`)
	testing.expect_value(t, len(task.input.metadata), 2)
	testing.expect_value(t, len(task.input.external_payloads), 1)
	if len(task.input.external_payloads) == 1 {
		testing.expect_value(t, task.input.external_payloads[0].size_bytes, i64(2048))
	}

	result_payload := codec.payload_create(transmute([]u8)string(`{"message":"Hello Odin"}`))
	defer codec.payload_free(&result_payload)
	testing.expect(
		t,
		codec.payload_add_metadata(
			&result_payload,
			"encoding",
			transmute([]u8)string("json/plain"),
		) == .None,
	)
	testing.expect(
		t,
		codec.payload_add_metadata(
			&result_payload,
			"messageType",
			transmute([]u8)string("tests.GreetingResult"),
		) == .None,
	)
	testing.expect(t, codec.payload_add_external(&result_payload, 4096) == .None)
	completion := proto.encode_activity_success_payload(
		task.task_token,
		codec.payload_view(&result_payload),
	)
	defer delete(completion)
	decoded_completion := proto.Coresdk_ActivityTaskCompletion{}
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion_ok := proto.decode_Coresdk_ActivityTaskCompletion(completion[:], &decoded_completion)
	context.allocator = previous_allocator
	testing.expect(t, completion_ok)
	testing.expect_value(t, string(decoded_completion.task_token), "token")
	testing.expect(t, decoded_completion.result != nil)
	if decoded_completion.result == nil {
		return
	}
	found_success := false
	#partial switch status in decoded_completion.result.status {
	case proto.Coresdk_Activity_Result_ActivityExecutionResult_Status_Completed:
		found_success = true
		testing.expect(t, status.value != nil)
		if status.value != nil {
			testing.expect(t, status.value.result != nil)
			if status.value.result != nil {
				testing.expect_value(t, string(status.value.result.data), `{"message":"Hello Odin"}`)
				testing.expect_value(t, len(status.value.result.metadata), 2)
				testing.expect_value(t, len(status.value.result.external_payloads), 1)
				if len(status.value.result.external_payloads) == 1 {
					testing.expect_value(
						t,
						status.value.result.external_payloads[0].size_bytes,
						i64(4096),
					)
				}
			}
		}
	}
	testing.expect(t, found_success)
}

@(test)
activity_context_info_survives_task_decode_arena :: proc(t: ^testing.T) {
	heartbeat_payload := proto.Temporal_Api_Common_V1_Payload{
		data = transmute([]u8)string(`{"completed":17}`),
	}
	defer delete(heartbeat_payload.metadata)
	append(
		&heartbeat_payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	scheduled_time := proto.Google_Protobuf_Timestamp{seconds = 1_000, nanos = 123}
	attempt_scheduled_time := proto.Google_Protobuf_Timestamp{seconds = 1_019, nanos = 456}
	started_time := proto.Google_Protobuf_Timestamp{seconds = 1_020, nanos = 789}
	schedule_to_close := proto.Google_Protobuf_Duration{seconds = 60}
	start_to_close := proto.Google_Protobuf_Duration{seconds = 20}
	heartbeat_timeout := proto.Google_Protobuf_Duration{seconds = 5}
	retry_initial := proto.Google_Protobuf_Duration{seconds = 1}
	retry_maximum := proto.Google_Protobuf_Duration{seconds = 30}
	retry_policy := proto.Temporal_Api_Common_V1_RetryPolicy{
		initial_interval = &retry_initial,
		backoff_coefficient = 2.5,
		maximum_interval = &retry_maximum,
		maximum_attempts = 7,
	}
	defer delete(retry_policy.non_retryable_error_types)
	append(&retry_policy.non_retryable_error_types, "PermanentFailure")
	priority := proto.Temporal_Api_Common_V1_Priority{
		priority_key = 42,
		fairness_key = "account-7",
		fairness_weight = 1.5,
	}
	workflow_execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
		workflow_id = "workflow-id",
		run_id = "workflow-run-id",
	}
	start := proto.Coresdk_Activity_Task_Start{
		workflow_namespace = "business-namespace",
		workflow_type = "OrderWorkflow",
		workflow_execution = &workflow_execution,
		activity_id = "charge-card-17",
		activity_type = "charge_card",
		scheduled_time = &scheduled_time,
		current_attempt_scheduled_time = &attempt_scheduled_time,
		started_time = &started_time,
		attempt = 3,
		schedule_to_close_timeout = &schedule_to_close,
		start_to_close_timeout = &start_to_close,
		heartbeat_timeout = &heartbeat_timeout,
		retry_policy = &retry_policy,
		priority = &priority,
	}
	defer delete(start.heartbeat_details)
	append(&start.heartbeat_details, heartbeat_payload)
	raw_task := proto.Coresdk_Activity_Task_ActivityTask{
		task_token = transmute([]u8)string("context-task-token"),
		variant = proto.Coresdk_Activity_Task_ActivityTask_Variant_Start{value = &start},
	}
	encoded := proto.encode_Coresdk_Activity_Task_ActivityTask(&raw_task)
	defer delete(encoded)

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		testing.expect(t, false)
		return
	}
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	decoded_task, decode_error := proto.decode_activity_task(encoded[:])
	context.allocator = previous_allocator
	testing.expect(t, decode_error == .None)
	task_token := bytes.clone(decoded_task.task_token)
	defer delete(task_token)
	sdk_worker := worker.Worker{
		namespace = "fallback-namespace",
		task_queue = "activity-task-queue",
	}
	owner := worker.Activity_Info_Owner{}
	owner_error := worker.activity_info_owner_init(
		&owner,
		&sdk_worker,
		&decoded_task,
		task_token,
	)
	virtual.arena_destroy(&decode_arena)
	defer worker.activity_info_owner_destroy(&owner)
	testing.expect(t, owner_error.message == nil)
	defer worker.error_free(&owner_error)
	if owner_error.message != nil {
		return
	}

	ctx := activity.Context{}
	activity.context_init_info(&ctx, owner.info, nil, nil)
	info := activity.info(&ctx)
	testing.expect_value(t, info.activity_id, "charge-card-17")
	testing.expect_value(t, info.activity_type, "charge_card")
	testing.expect_value(t, info.attempt, u32(3))
	testing.expect_value(t, info.namespace, "business-namespace")
	testing.expect_value(t, info.task_queue, "activity-task-queue")
	testing.expect_value(t, string(info.task_token), "context-task-token")
	testing.expect_value(t, info.workflow_id, "workflow-id")
	testing.expect_value(t, info.workflow_run_id, "workflow-run-id")
	testing.expect_value(t, info.workflow_type, "OrderWorkflow")
	testing.expect(t, activity.is_workflow_activity(&ctx))
	testing.expect(t, info.has_scheduled_time)
	testing.expect(t, info.has_current_attempt_scheduled_time)
	testing.expect(t, info.has_started_time)
	testing.expect_value(t, time.to_unix_nanoseconds(info.scheduled_time), i64(1_000_000_000_123))
	testing.expect_value(t, info.schedule_to_close_timeout, 60 * time.Second)
	testing.expect_value(t, info.start_to_close_timeout, 20 * time.Second)
	testing.expect_value(t, info.heartbeat_timeout, 5 * time.Second)
	deadline, deadline_present := activity.deadline(&ctx)
	testing.expect(t, deadline_present)
	testing.expect_value(t, time.to_unix_nanoseconds(deadline), i64(1_040_000_000_789))
	testing.expect(t, info.has_retry_policy)
	testing.expect_value(t, info.retry_policy.initial_interval, time.Second)
	testing.expect_value(t, info.retry_policy.backoff_coefficient, 2.5)
	testing.expect_value(t, info.retry_policy.maximum_interval, 30 * time.Second)
	testing.expect_value(t, info.retry_policy.maximum_attempts, i32(7))
	testing.expect_value(t, len(info.retry_policy.non_retryable_error_types), 1)
	if len(info.retry_policy.non_retryable_error_types) == 1 {
		testing.expect_value(t, info.retry_policy.non_retryable_error_types[0], "PermanentFailure")
	}
	testing.expect(t, info.has_priority)
	testing.expect_value(t, info.priority.priority_key, i32(42))
	testing.expect_value(t, info.priority.fairness_key, "account-7")
	testing.expect_value(t, info.priority.fairness_weight, f32(1.5))
	progress: Heartbeat_Progress
	found, detail_error := activity.heartbeat_detail_json(&ctx, 0, &progress)
	testing.expect(t, found)
	testing.expect(t, detail_error == .None)
	testing.expect_value(t, progress.completed, 17)
	_, missing := activity.heartbeat_detail_payload(&ctx, 1)
	testing.expect(t, !missing)
}

@(test)
activity_cancel_and_heartbeat_protobuf_round_trip :: proc(t: ^testing.T) {
	cancel_details := proto.Coresdk_Activity_Task_ActivityCancellationDetails{
		is_cancelled = true,
	}
	cancel := proto.Coresdk_Activity_Task_Cancel{
		reason = .CANCELLED,
		details = &cancel_details,
	}
	raw_task := proto.Coresdk_Activity_Task_ActivityTask{
		task_token = transmute([]u8)string("cancel-token"),
		variant = proto.Coresdk_Activity_Task_ActivityTask_Variant_Cancel{value = &cancel},
	}
	encoded_task := proto.encode_Coresdk_Activity_Task_ActivityTask(&raw_task)
	defer delete(encoded_task)

	decode_arena: virtual.Arena
	testing.expect(t, virtual.arena_init_growing(&decode_arena) == nil)
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	task, decode_error := proto.decode_activity_task(encoded_task[:])
	context.allocator = previous_allocator
	testing.expect(t, decode_error == .None)
	testing.expect(t, task.kind == .Cancel)
	testing.expect(t, task.cancel_reason == .CANCELLED)
	testing.expect(t, task.cancel_details.is_cancelled)

	heartbeat := proto.encode_activity_heartbeat(
		transmute([]u8)string("heartbeat-token"),
		transmute([]u8)string(`"halfway"`),
	)
	defer delete(heartbeat)
	decoded_heartbeat := proto.Coresdk_ActivityHeartbeat{}
	context.allocator = virtual.arena_allocator(&decode_arena)
	heartbeat_ok := proto.decode_Coresdk_ActivityHeartbeat(heartbeat[:], &decoded_heartbeat)
	context.allocator = previous_allocator
	testing.expect(t, heartbeat_ok)
	testing.expect_value(t, string(decoded_heartbeat.task_token), "heartbeat-token")
	testing.expect_value(t, len(decoded_heartbeat.details), 1)
	if len(decoded_heartbeat.details) == 1 {
		testing.expect_value(t, string(decoded_heartbeat.details[0].data), `"halfway"`)
	}

	completion := proto.encode_activity_cancellation(transmute([]u8)string("cancel-token"))
	defer delete(completion)
	decoded_completion := proto.Coresdk_ActivityTaskCompletion{}
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion_ok := proto.decode_Coresdk_ActivityTaskCompletion(completion[:], &decoded_completion)
	context.allocator = previous_allocator
	testing.expect(t, completion_ok)
	cancelled := false
	if decoded_completion.result != nil {
		#partial switch status in decoded_completion.result.status {
		case proto.Coresdk_Activity_Result_ActivityExecutionResult_Status_Cancelled:
			cancelled = status.value != nil && status.value.failure != nil
			if cancelled {
				#partial switch failure_info in status.value.failure.failure_info {
				case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Canceled_Failure_Info:
					cancelled = failure_info.value != nil
				case:
					cancelled = false
				}
			}
		}
	}
	testing.expect(t, cancelled)
}

@(test)
activity_application_failure_protobuf_preserves_retry_contract :: proc(t: ^testing.T) {
	details, details_error := codec.encode_json(
		Activity_Failure_Details{dependency = "payments", attempt = 3},
	)
	testing.expect(t, details_error == .None)
	defer codec.payload_free(&details)
	completion := proto.encode_activity_failure(
		transmute([]u8)string("failure-token"),
		"payments are temporarily unavailable",
		"PaymentsUnavailable",
		false,
		codec.payload_view(&details),
		true,
		250 * time.Millisecond,
	)
	defer delete(completion)

	decode_arena: virtual.Arena
	testing.expect(t, virtual.arena_init_growing(&decode_arena) == nil)
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	decoded := proto.Coresdk_ActivityTaskCompletion{}
	ok := proto.decode_Coresdk_ActivityTaskCompletion(completion[:], &decoded)
	context.allocator = previous_allocator
	testing.expect(t, ok)
	testing.expect_value(t, string(decoded.task_token), "failure-token")
	if !ok || decoded.result == nil {
		return
	}

	found_failure := false
	#partial switch status in decoded.result.status {
	case proto.Coresdk_Activity_Result_ActivityExecutionResult_Status_Failed:
		if status.value == nil || status.value.failure == nil {
			break
		}
		failure := status.value.failure
		testing.expect_value(t, failure.message, "payments are temporarily unavailable")
		testing.expect_value(t, failure.source, "OdinSDK")
		#partial switch failure_info in failure.failure_info {
		case proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info:
			if failure_info.value == nil {
				break
			}
			info := failure_info.value
			found_failure = true
			testing.expect_value(t, info.type, "PaymentsUnavailable")
			testing.expect(t, !info.non_retryable)
			testing.expect(t, info.details != nil)
			if info.details != nil {
				testing.expect_value(t, len(info.details.payloads), 1)
				if len(info.details.payloads) == 1 {
					testing.expect_value(
						t,
						string(info.details.payloads[0].data),
						`{"dependency":"payments","attempt":3}`,
					)
				}
			}
			testing.expect(t, info.next_retry_delay != nil)
			if info.next_retry_delay != nil {
				testing.expect_value(t, info.next_retry_delay.seconds, i64(0))
				testing.expect_value(t, info.next_retry_delay.nanos, i32(250_000_000))
			}
		}
	}
	testing.expect(t, found_failure)
}

@(test)
activity_context_reports_heartbeat_and_cooperative_cancellation :: proc(t: ^testing.T) {
	capture := Heartbeat_Capture{}
	defer delete(capture.details)
	ctx := activity.Context{}
	activity.context_init(&ctx, "activity-id", 3, &capture, capture_heartbeat)

	testing.expect_value(t, activity.activity_id(&ctx), "activity-id")
	testing.expect_value(t, activity.attempt(&ctx), u32(3))
	testing.expect(t, !activity.is_cancellation_requested(&ctx))
	testing.expect(t, activity.heartbeat(&ctx, "halfway") == .Recorded)
	testing.expect_value(t, capture.calls, 1)
	testing.expect_value(t, capture.details, "halfway")

	activity.context_request_cancellation(&ctx, .Worker_Shutdown)
	testing.expect(t, activity.is_cancellation_requested(&ctx))
	testing.expect(t, activity.cancellation_reason(&ctx) == .Worker_Shutdown)
	testing.expect(t, activity.heartbeat(&ctx, "too late") == .Cancellation_Requested)
	testing.expect_value(t, capture.calls, 1)

	progress_capture := Heartbeat_Progress_Capture{}
	progress_ctx := activity.Context{}
	activity.context_init(
		&progress_ctx,
		"typed-heartbeat-activity",
		1,
		&progress_capture,
		capture_progress_heartbeat,
	)
	testing.expect(
		t,
		activity.heartbeat_json(&progress_ctx, Heartbeat_Progress{completed = 42}) == .Recorded,
	)
	testing.expect_value(t, progress_capture.calls, 1)
	testing.expect_value(t, progress_capture.completed, 42)
}

@(test)
generated_protobuf_rejects_truncated_bytes :: proc(t: ^testing.T) {
	malformed := [3]u8{0x0a, 0x05, 0x01}
	decoded := proto.Coresdk_ActivityTaskCompletion{}
	testing.expect(t, !proto.decode_Coresdk_ActivityTaskCompletion(malformed[:], &decoded))
}

@(test)
workflow_state_machine_recreates_after_eviction :: proc(t: ^testing.T) {
	runtime := workflow.Runtime{}
	init_error := workflow.runtime_init(&runtime, "odin_hello_workflow", "test-queue")
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_hello_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "run-1",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)

	encoded := proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed := workflow.process_activation(&runtime, encoded[:])
	delete(encoded)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect(t, !processed.is_replaying)
	testing.expect_value(t, len(runtime.instances), 1)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed = workflow.process_activation(&runtime, encoded[:])
	delete(encoded)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	testing.expect_value(t, len(runtime.instances), 0)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed = workflow.process_activation(&runtime, encoded[:])
	delete(encoded)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect(t, processed.is_replaying)
	testing.expect_value(t, runtime.replay_initializations, 1)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 9
	output := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Hello Odin"`)}
	activity_success := proto.Coresdk_Activity_Result_Success{result = &output}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed = workflow.process_activation(&runtime, encoded[:])
	delete(encoded)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.completed)
	testing.expect_value(t, processed.history_length, u32(9))
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_recreates_stack_after_eviction :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_hello_workflow",
		"test-queue",
		fiber_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_hello_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-run-1",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)

	encoded := proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed := workflow.process_fiber_activation(&runtime, encoded[:])
	delete(encoded)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, len(runtime.instances), 1)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed = workflow.process_fiber_activation(&runtime, encoded[:])
	delete(encoded)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	testing.expect_value(t, len(runtime.instances), 0)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed = workflow.process_fiber_activation(&runtime, encoded[:])
	delete(encoded)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect(t, processed.is_replaying)
	testing.expect_value(t, runtime.replay_initializations, 1)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 9
	output := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Hello Odin"`)}
	defer delete(output.metadata)
	append(
		&output.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &output}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	encoded = proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&activation)
	processed = workflow.process_fiber_activation(&runtime, encoded[:])
	delete(encoded)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.completed)
	testing.expect_value(t, processed.history_length, u32(9))
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_sequences_activity_timer_activity :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_chain_workflow",
		"test-queue",
		fiber_chain_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_chain_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-chain-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.activity_seq, u32(1))
	delete(processed.completion)
	workflow.error_free(&processed.error)

	first_output := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Hello Odin"`)}
	defer delete(first_output.metadata)
	append(
		&first_output.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	first_success := proto.Coresdk_Activity_Result_Success{result = &first_output}
	first_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &first_success,
		},
	}
	resolve_first := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &first_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_first,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.started_timer)
	testing.expect_value(t, processed.timer_seq, u32(1))
	delete(processed.completion)
	workflow.error_free(&processed.error)

	fire_timer := proto.Coresdk_Workflow_Activation_FireTimer{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{
				value = &fire_timer,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.activity_seq, u32(2))
	delete(processed.completion)
	workflow.error_free(&processed.error)

	second_output := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Hello Hello Odin"`)}
	defer delete(second_output.metadata)
	append(
		&second_output.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	second_success := proto.Coresdk_Activity_Result_Success{result = &second_output}
	second_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &second_success,
		},
	}
	resolve_second := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 2,
		result = &second_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 20
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_second,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.completed)
	testing.expect(t, !processed.scheduled_activity)
	testing.expect(t, !processed.started_timer)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_applies_all_activity_resolutions_before_resume :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_parallel_workflow",
		"test-queue",
		fiber_parallel_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_parallel_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-parallel-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)

	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.scheduled_activity_count, 2)
	testing.expect_value(t, processed.activity_seq, u32(2))
	testing.expect_value(t, processed.waiting_activity_seq, u32(1))

	decode_arena: virtual.Arena
	arena_error := virtual.arena_init_growing(&decode_arena)
	testing.expect(t, arena_error == nil)
	if arena_error != nil {
		delete(processed.completion)
		workflow.error_free(&processed.error)
		return
	}
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	initial_completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	completion_ok := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
		processed.completion[:],
		&initial_completion,
	)
	context.allocator = previous_allocator
	testing.expect(t, completion_ok)
	command_count := 0
	#partial switch status in initial_completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value != nil {
			command_count = len(status.value.commands)
			if command_count == 2 {
				first_seq: u32
				second_seq: u32
				#partial switch command in status.value.commands[0].variant {
				case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity:
					if command.value != nil {
						first_seq = command.value.seq
					}
				}
				#partial switch command in status.value.commands[1].variant {
				case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity:
					if command.value != nil {
						second_seq = command.value.seq
					}
				}
				testing.expect_value(t, first_seq, u32(1))
				testing.expect_value(t, second_seq, u32(2))
			}
		}
	}
	testing.expect_value(t, command_count, 2)
	virtual.arena_destroy(&decode_arena)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	first_output := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Hello Odin"`)}
	second_output := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Hello Temporal"`)}
	defer delete(first_output.metadata)
	defer delete(second_output.metadata)
	encoding := proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
		key = "encoding",
		value = transmute([]u8)string("json/plain"),
	}
	append(&first_output.metadata, encoding)
	append(&second_output.metadata, encoding)
	first_success := proto.Coresdk_Activity_Result_Success{result = &first_output}
	second_success := proto.Coresdk_Activity_Result_Success{result = &second_output}
	first_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{value = &first_success},
	}
	second_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{value = &second_success},
	}
	resolve_first := proto.Coresdk_Workflow_Activation_ResolveActivity{seq = 1, result = &first_resolution}
	resolve_second := proto.Coresdk_Workflow_Activation_ResolveActivity{seq = 2, result = &second_resolution}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	// Reverse order is deliberate: both results must be stored before the fiber
	// resumes and awaits them in source order.
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_second,
			},
		},
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_first,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "parallel resolution failed: %s", string(processed.error.message))
	testing.expect(t, processed.completed)
	testing.expect(t, !processed.scheduled_activity)
	testing.expect_value(t, processed.resolved_activity_seq, u32(1))
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_preserves_structured_activity_failure_across_activations :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_delayed_failure_workflow",
		"test-queue",
		fiber_delayed_failure_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_delayed_failure_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-delayed-failure-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)

	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.scheduled_activity_count, 2)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	failure_details, failure_details_error := codec.encode_json(
		Failure_Test_Details{dependency = "billing", code = 409},
	)
	testing.expect(t, failure_details_error == .None)
	defer codec.payload_free(&failure_details)
	failure_details_proto := proto.encode_payload(codec.payload_view(&failure_details))
	defer delete(failure_details_proto.metadata)
	defer delete(failure_details_proto.external_payloads)
	failure_detail_payloads := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(failure_detail_payloads.payloads)
	append(&failure_detail_payloads.payloads, failure_details_proto)
	next_retry_delay := proto.Google_Protobuf_Duration{nanos = 250_000_000}
	application_info := proto.Temporal_Api_Failure_V1_ApplicationFailureInfo{
		type = "DependencyRejected",
		details = &failure_detail_payloads,
		next_retry_delay = &next_retry_delay,
		category = .APPLICATION_ERROR_CATEGORY_BENIGN,
	}
	application_failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "dependency rejected request",
		source = "OdinSDK",
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = &application_info,
		},
	}
	activity_type := proto.Temporal_Api_Common_V1_ActivityType{name = "missing"}
	activity_info := proto.Temporal_Api_Failure_V1_ActivityFailureInfo{
		identity = "test-worker",
		activity_type = &activity_type,
		activity_id = "failure-activity",
		retry_state = .RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
	}
	activity_failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "Activity task failed",
		source = "TemporalServer",
		cause = &application_failure,
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Activity_Failure_Info{
			value = &activity_info,
		},
	}
	failed_result := proto.Coresdk_Activity_Result_Failure{failure = &activity_failure}
	failure_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Failed{
			value = &failed_result,
		},
	}
	resolve_failure := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 2,
		result = &failure_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_failure,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "delayed failure failed: %s", string(processed.error.message))
	testing.expect(t, processed.resolved_activity_failed)
	testing.expect_value(t, processed.resolved_activity_seq, u32(2))
	testing.expect(t, !processed.completed)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	// process_fiber_test_activation has now deleted its encoded activation and
	// process_fiber_activation has destroyed its protobuf decode arena. The
	// Workflow still cannot inspect seq 2 because it remains suspended on seq 1.
	first_output := proto.Temporal_Api_Common_V1_Payload{
		data = transmute([]u8)string(`"Hello Odin"`),
	}
	defer delete(first_output.metadata)
	append(
		&first_output.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	first_success := proto.Coresdk_Activity_Result_Success{result = &first_output}
	first_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &first_success,
		},
	}
	resolve_first := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &first_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_first,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "delayed failure resume failed: %s", string(processed.error.message))
	testing.expect(t, processed.completed)
	completion, completion_found := clone_workflow_completion_payload(processed.completion[:])
	testing.expect(t, completion_found)
	if completion_found {
		message, decode_error := codec.decode_string(
			completion.data,
			codec.encoding(codec.payload_view(&completion)),
		)
		testing.expect(t, decode_error == .None)
		testing.expect_value(t, message, "preserved")
		delete(message)
	}
	codec.payload_free(&completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_update_routine_awaits_activity_and_replays_across_activations :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_update_routine_workflow",
		"test-queue",
		fiber_update_routine_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	workflow_input, workflow_input_error := codec.encode_json("Odin")
	testing.expect(t, workflow_input_error == .None)
	defer codec.payload_free(&workflow_input)
	workflow_input_proto := proto.encode_payload(codec.payload_view(&workflow_input))
	defer delete(workflow_input_proto.metadata)
	defer delete(workflow_input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_update_routine_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, workflow_input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-update-routine-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	update_input, update_input_error := codec.encode_json(Update_Routine_Input{name = "Temporal"})
	testing.expect(t, update_input_error == .None)
	defer codec.payload_free(&update_input)
	update_input_proto := proto.encode_payload(codec.payload_view(&update_input))
	defer delete(update_input_proto.metadata)
	defer delete(update_input_proto.external_payloads)
	update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "rename-with-activity-1",
		protocol_instance_id = "update-routine-protocol-1",
		name = "rename_with_activity",
		run_validator = true,
	}
	defer delete(update.input)
	append(&update.input, update_input_proto)
	finish_input, finish_input_error := codec.encode_json("done")
	testing.expect(t, finish_input_error == .None)
	defer codec.payload_free(&finish_input)
	finish_input_proto := proto.encode_payload(codec.payload_view(&finish_input))
	defer delete(finish_input_proto.metadata)
	defer delete(finish_input_proto.external_payloads)
	finish_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "finish",
	}
	defer delete(finish_signal.input)
	append(&finish_signal.input, finish_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &update,
			},
		},
	)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Update routine start failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.updates_accepted, 1)
	testing.expect_value(t, processed.updates_completed, 0)
	testing.expect(t, processed.scheduled_activity)
	testing.expect(t, !processed.completed)
	testing.expect(t, processed.waiting_handlers)
	testing.expect_value(t, processed.activity_seq, u32(1))
	started_update := inspect_update_responses(
		processed.completion[:],
		"update-routine-protocol-1",
	)
	testing.expect_value(t, started_update.command_count, 2)
	testing.expect_value(t, started_update.accepted, 1)
	testing.expect_value(t, started_update.completed, 0)
	update_response_inspection_free(&started_update)
	update_start_completion := processed.completion
	workflow.error_free(&processed.error)

	activity_output, activity_output_error := codec.encode_json("Hello Temporal")
	testing.expect(t, activity_output_error == .None)
	defer codec.payload_free(&activity_output)
	activity_output_proto := proto.encode_payload(codec.payload_view(&activity_output))
	defer delete(activity_output_proto.metadata)
	defer delete(activity_output_proto.external_payloads)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &activity_output_proto}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 10
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Update routine completion failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.updates_accepted, 0)
	testing.expect_value(t, processed.updates_completed, 1)
	testing.expect(t, processed.completed)
	testing.expect(t, !processed.waiting_handlers)
	completed_update := inspect_update_responses(
		processed.completion[:],
		"update-routine-protocol-1",
	)
	testing.expect_value(t, completed_update.command_count, 2)
	testing.expect_value(t, completed_update.accepted, 0)
	testing.expect_value(t, completed_update.completed, 1)
	testing.expect_value(t, completed_update.completed_index, 0)
	output: Update_Routine_Output
	decode_error := codec.decode_json(codec.payload_view(&completed_update.payload), &output)
	testing.expect(t, decode_error == .None)
	testing.expect_value(t, output.previous, "Odin")
	testing.expect_value(t, output.current, "Hello Temporal")
	testing.expect_value(t, output.activity_result, "Hello Temporal")
	delete(output.previous)
	delete(output.current)
	delete(output.activity_result)
	update_response_inspection_free(&completed_update)
	update_finish_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	update.run_validator = false
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &update,
			},
		},
	)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Update routine replay start failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(update_start_completion[:], processed.completion[:]))
	delete(update_start_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 10
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Update routine replay completion failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(update_finish_completion[:], processed.completion[:]))
	delete(update_finish_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_close_abandons_unfinished_update_routine_and_replays :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_abandon_update_routine_workflow",
		"test-queue",
		fiber_abandon_update_routine_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	workflow_input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&workflow_input)
	workflow_input_proto := proto.encode_payload(codec.payload_view(&workflow_input))
	defer delete(workflow_input_proto.metadata)
	defer delete(workflow_input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_abandon_update_routine_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, workflow_input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-abandon-update-routine-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	update_input, update_error := codec.encode_json(Update_Routine_Input{name = "Temporal"})
	testing.expect(t, update_error == .None)
	defer codec.payload_free(&update_input)
	update_input_proto := proto.encode_payload(codec.payload_view(&update_input))
	defer delete(update_input_proto.metadata)
	defer delete(update_input_proto.external_payloads)
	update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "rename-abandoned",
		protocol_instance_id = "update-routine-abandoned",
		name = "rename_with_activity",
	}
	defer delete(update.input)
	append(&update.input, update_input_proto)
	finish_input, finish_error := codec.encode_json("done")
	testing.expect(t, finish_error == .None)
	defer codec.payload_free(&finish_input)
	finish_input_proto := proto.encode_payload(codec.payload_view(&finish_input))
	defer delete(finish_input_proto.metadata)
	defer delete(finish_input_proto.external_payloads)
	finish_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "finish"}
	defer delete(finish_signal.input)
	append(&finish_signal.input, finish_input_proto)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &update,
			},
		},
	)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Workflow close failed: %s", string(processed.error.message))
	testing.expect(t, processed.completed)
	testing.expect(t, !processed.scheduled_activity)
	testing.expect_value(t, processed.updates_accepted, 1)
	testing.expect_value(t, processed.updates_completed, 0)
	testing.expect_value(t, processed.unfinished_update_handlers, 1)
	testing.expect_value(t, processed.warned_unfinished_update_handlers, 1)
	closed := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, closed.decoded)
	testing.expect(t, closed.completed)
	testing.expect_value(t, closed.command_count, 2)
	delete(closed.failure_message)
	delete(closed.completion_result)
	update_closed := inspect_update_responses(processed.completion[:], "update-routine-abandoned")
	testing.expect_value(t, update_closed.accepted, 1)
	testing.expect_value(t, update_closed.completed, 0)
	update_response_inspection_free(&update_closed)
	close_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &update,
			},
		},
	)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Workflow close replay failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(close_completion[:], processed.completion[:]))
	testing.expect_value(t, processed.unfinished_update_handlers, 1)
	testing.expect_value(t, processed.warned_unfinished_update_handlers, 1)
	delete(close_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_unfinished_update_policy_can_suppress_warning :: proc(t: ^testing.T) {
	warn_routine := workflow.Fiber_Update_Routine{}
	abandon_routine := workflow.Fiber_Update_Routine{
		unfinished_policy = .Abandon,
	}
	finished_routine := workflow.Fiber_Update_Routine{response_emitted = true}
	instance := workflow.Fiber_Instance{}
	defer delete(instance.update_routines)
	append(&instance.update_routines, &warn_routine, &abandon_routine, &finished_routine)
	result := workflow.Process_Result{}
	workflow.observe_unfinished_update_routines(&result, &instance)
	testing.expect_value(t, result.unfinished_update_handlers, 2)
	testing.expect_value(t, result.warned_unfinished_update_handlers, 1)
}

@(test)
workflow_unfinished_signal_policy_can_suppress_warning :: proc(t: ^testing.T) {
	warn_routine := workflow.Fiber_Signal_Routine{}
	abandon_routine := workflow.Fiber_Signal_Routine{
		unfinished_policy = .Abandon,
	}
	finished_routine := workflow.Fiber_Signal_Routine{completion_observed = true}
	instance := workflow.Fiber_Instance{}
	defer delete(instance.signal_routines)
	append(&instance.signal_routines, &warn_routine, &abandon_routine, &finished_routine)
	result := workflow.Process_Result{}
	workflow.observe_unfinished_signal_routines(&result, &instance)
	testing.expect_value(t, result.unfinished_signal_handlers, 2)
	testing.expect_value(t, result.warned_unfinished_signal_handlers, 1)
}

@(test)
workflow_update_routine_registration_preserves_unfinished_policy :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_silent_update_registration_workflow",
		"test-queue",
		fiber_silent_abandon_registration_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)
	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_silent_update_registration_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "silent-update-registration-run",
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	defer delete(processed.completion)
	defer workflow.error_free(&processed.error)
	testing.expect(t, processed.error.message == nil)
	testing.expect_value(t, len(runtime.instances), 1)
	if len(runtime.instances) == 1 {
		instance := runtime.instances[0]
		testing.expect_value(t, len(instance.update_handlers), 1)
		if len(instance.update_handlers) == 1 {
			testing.expect(
				t,
				instance.update_handlers[0].unfinished_policy == .Abandon,
			)
		}
	}
}

@(test)
workflow_synchronous_signal_handler_mutates_state_before_main_resumes :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_signal_handler_workflow",
		"test-queue",
		fiber_signal_handler_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	workflow_input, workflow_input_error := codec.encode_json("Odin")
	testing.expect(t, workflow_input_error == .None)
	defer codec.payload_free(&workflow_input)
	workflow_input_proto := proto.encode_payload(codec.payload_view(&workflow_input))
	defer delete(workflow_input_proto.metadata)
	defer delete(workflow_input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_signal_handler_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, workflow_input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-signal-handler-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	rename_input, rename_input_error := codec.encode_json("Temporal")
	testing.expect(t, rename_input_error == .None)
	defer codec.payload_free(&rename_input)
	rename_input_proto := proto.encode_payload(codec.payload_view(&rename_input))
	defer delete(rename_input_proto.metadata)
	defer delete(rename_input_proto.external_payloads)
	rename_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "rename_immediately",
	}
	defer delete(rename_signal.input)
	append(&rename_signal.input, rename_input_proto)

	finish_input, finish_input_error := codec.encode_json("done")
	testing.expect(t, finish_input_error == .None)
	defer codec.payload_free(&finish_input)
	finish_input_proto := proto.encode_payload(codec.payload_view(&finish_input))
	defer delete(finish_input_proto.metadata)
	defer delete(finish_input_proto.external_payloads)
	finish_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "finish"}
	defer delete(finish_signal.input)
	append(&finish_signal.input, finish_input_proto)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &rename_signal,
			},
		},
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "synchronous Signal handler failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.received_signal_count, 2)
	testing.expect_value(t, processed.signal_handlers_started, 1)
	testing.expect_value(t, processed.signal_handlers_completed, 1)
	testing.expect(t, processed.completed)
	inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, inspection.decoded)
	testing.expect_value(t, inspection.command_count, 1)
	testing.expect(t, inspection.completed)
	testing.expect_value(t, string(inspection.completion_result), `"Temporal"`)
	delete(inspection.failure_message)
	delete(inspection.completion_result)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_signal_routine_awaits_activity_drains_and_replays :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_signal_routine_workflow",
		"test-queue",
		fiber_signal_routine_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	workflow_input, workflow_input_error := codec.encode_json("Odin")
	testing.expect(t, workflow_input_error == .None)
	defer codec.payload_free(&workflow_input)
	workflow_input_proto := proto.encode_payload(codec.payload_view(&workflow_input))
	defer delete(workflow_input_proto.metadata)
	defer delete(workflow_input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_signal_routine_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, workflow_input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-signal-routine-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	handler_input, handler_input_error := codec.encode_json(Signal_Routine_Input{name = "Temporal"})
	testing.expect(t, handler_input_error == .None)
	defer codec.payload_free(&handler_input)
	handler_input_proto := proto.encode_payload(codec.payload_view(&handler_input))
	defer delete(handler_input_proto.metadata)
	defer delete(handler_input_proto.external_payloads)
	handler_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "rename_with_activity",
	}
	defer delete(handler_signal.input)
	append(&handler_signal.input, handler_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &handler_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Signal routine start failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.received_signal_count, 1)
	testing.expect_value(t, processed.signal_handlers_started, 1)
	testing.expect_value(t, processed.signal_handlers_completed, 0)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.activity_seq, u32(1))
	handler_start_completion := processed.completion
	workflow.error_free(&processed.error)

	finish_input, finish_input_error := codec.encode_json("done")
	testing.expect(t, finish_input_error == .None)
	defer codec.payload_free(&finish_input)
	finish_input_proto := proto.encode_payload(codec.payload_view(&finish_input))
	defer delete(finish_input_proto.metadata)
	defer delete(finish_input_proto.external_payloads)
	finish_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "finish"}
	defer delete(finish_signal.input)
	append(&finish_signal.input, finish_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Signal handler drain start failed: %s", string(processed.error.message))
	testing.expect(t, processed.waiting_handlers)
	testing.expect(t, !processed.completed)
	finish_completion := processed.completion
	workflow.error_free(&processed.error)

	activity_output, activity_output_error := codec.encode_json("Hello Temporal")
	testing.expect(t, activity_output_error == .None)
	defer codec.payload_free(&activity_output)
	activity_output_proto := proto.encode_payload(codec.payload_view(&activity_output))
	defer delete(activity_output_proto.metadata)
	defer delete(activity_output_proto.external_payloads)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &activity_output_proto}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Signal routine resolution failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.signal_handlers_completed, 1)
	testing.expect(t, processed.completed)
	testing.expect_value(t, processed.unfinished_signal_handlers, 0)
	terminal_inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, terminal_inspection.decoded)
	testing.expect_value(t, terminal_inspection.command_count, 1)
	testing.expect(t, terminal_inspection.completed)
	testing.expect_value(t, string(terminal_inspection.completion_result), `"Hello Temporal / done"`)
	delete(terminal_inspection.failure_message)
	delete(terminal_inspection.completion_result)
	terminal_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &handler_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(handler_start_completion[:], processed.completion[:]))
	delete(handler_start_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(finish_completion[:], processed.completion[:]))
	delete(finish_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Signal routine replay failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(terminal_completion[:], processed.completion[:]))
	delete(terminal_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_detached_update_finishes_before_cancellation_and_replays :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_detached_update_cancellation_workflow",
		"test-queue",
		fiber_detached_update_cancellation_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	workflow_input, workflow_input_error := codec.encode_json("Odin")
	testing.expect(t, workflow_input_error == .None)
	defer codec.payload_free(&workflow_input)
	workflow_input_proto := proto.encode_payload(codec.payload_view(&workflow_input))
	defer delete(workflow_input_proto.metadata)
	defer delete(workflow_input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_detached_update_cancellation_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, workflow_input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-detached-update-cancellation-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	update_input, update_input_error := codec.encode_json(Update_Routine_Input{name = "Temporal"})
	testing.expect(t, update_input_error == .None)
	defer codec.payload_free(&update_input)
	update_input_proto := proto.encode_payload(codec.payload_view(&update_input))
	defer delete(update_input_proto.metadata)
	defer delete(update_input_proto.external_payloads)
	update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "detached-update",
		protocol_instance_id = "detached-update-protocol",
		name = "detached_rename",
	}
	defer delete(update.input)
	append(&update.input, update_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &update,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "detached Update start failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.updates_accepted, 1)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.activity_seq, u32(1))
	update_start_completion := processed.completion
	workflow.error_free(&processed.error)

	cancel := proto.Coresdk_Workflow_Activation_CancelWorkflow{
		reason = "finish detached Update before cancellation",
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "detached Update cancellation failed: %s", string(processed.error.message))
	testing.expect(t, processed.received_cancellation)
	testing.expect(t, processed.waiting_handlers)
	testing.expect(t, !processed.requested_activity_cancellation)
	testing.expect(t, !processed.canceled_workflow)
	testing.expect(t, !processed.completed)
	cancel_inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, cancel_inspection.decoded)
	testing.expect_value(t, cancel_inspection.command_count, 0)
	delete(cancel_inspection.failure_message)
	delete(cancel_inspection.completion_result)
	cancel_completion := processed.completion
	workflow.error_free(&processed.error)

	activity_output, activity_output_error := codec.encode_json("Hello Temporal")
	testing.expect(t, activity_output_error == .None)
	defer codec.payload_free(&activity_output)
	activity_output_proto := proto.encode_payload(codec.payload_view(&activity_output))
	defer delete(activity_output_proto.metadata)
	defer delete(activity_output_proto.external_payloads)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &activity_output_proto}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "detached Update resolution failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.updates_completed, 1)
	testing.expect(t, processed.canceled_workflow)
	testing.expect(t, processed.completed)
	testing.expect_value(t, processed.unfinished_update_handlers, 0)
	terminal_inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, terminal_inspection.decoded)
	testing.expect_value(t, terminal_inspection.command_count, 2)
	testing.expect(t, terminal_inspection.canceled_workflow)
	delete(terminal_inspection.failure_message)
	delete(terminal_inspection.completion_result)
	update_finished := inspect_update_responses(processed.completion[:], "detached-update-protocol")
	testing.expect_value(t, update_finished.completed, 1)
	testing.expect_value(t, update_finished.completed_index, 0)
	update_output: Update_Routine_Output
	decode_error := codec.decode_json(codec.payload_view(&update_finished.payload), &update_output)
	testing.expect(t, decode_error == .None)
	testing.expect_value(t, update_output.previous, "Odin")
	testing.expect_value(t, update_output.current, "Hello Temporal")
	testing.expect_value(t, update_output.activity_result, "Hello Temporal")
	delete(update_output.previous)
	delete(update_output.current)
	delete(update_output.activity_result)
	update_response_inspection_free(&update_finished)
	terminal_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &update,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(update_start_completion[:], processed.completion[:]))
	delete(update_start_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(cancel_completion[:], processed.completion[:]))
	delete(cancel_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "detached Update replay failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(terminal_completion[:], processed.completion[:]))
	delete(terminal_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_update_routines_complete_out_of_order_and_replay :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_concurrent_update_routine_workflow",
		"test-queue",
		fiber_update_routine_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	workflow_input, workflow_input_error := codec.encode_json("Odin")
	testing.expect(t, workflow_input_error == .None)
	defer codec.payload_free(&workflow_input)
	workflow_input_proto := proto.encode_payload(codec.payload_view(&workflow_input))
	defer delete(workflow_input_proto.metadata)
	defer delete(workflow_input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_concurrent_update_routine_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, workflow_input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-concurrent-update-routine-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	first_input, first_input_error := codec.encode_json(Update_Routine_Input{name = "First"})
	second_input, second_input_error := codec.encode_json(Update_Routine_Input{name = "Second"})
	testing.expect(t, first_input_error == .None)
	testing.expect(t, second_input_error == .None)
	defer codec.payload_free(&first_input)
	defer codec.payload_free(&second_input)
	first_input_proto := proto.encode_payload(codec.payload_view(&first_input))
	second_input_proto := proto.encode_payload(codec.payload_view(&second_input))
	defer delete(first_input_proto.metadata)
	defer delete(first_input_proto.external_payloads)
	defer delete(second_input_proto.metadata)
	defer delete(second_input_proto.external_payloads)
	first_update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "rename-first",
		protocol_instance_id = "update-routine-protocol-first",
		name = "rename_with_activity",
		run_validator = true,
	}
	second_update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "rename-second",
		protocol_instance_id = "update-routine-protocol-second",
		name = "rename_with_activity",
		run_validator = true,
	}
	defer delete(first_update.input)
	defer delete(second_update.input)
	append(&first_update.input, first_input_proto)
	append(&second_update.input, second_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &first_update,
			},
		},
	)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &second_update,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "concurrent Updates failed to start: %s", string(processed.error.message))
	testing.expect_value(t, processed.updates_accepted, 2)
	testing.expect_value(t, processed.updates_completed, 0)
	testing.expect_value(t, processed.scheduled_activity_count, 2)
	testing.expect_value(t, processed.activity_seq, u32(2))
	testing.expect(t, !processed.completed)
	first_started := inspect_update_responses(
		processed.completion[:],
		"update-routine-protocol-first",
	)
	second_started := inspect_update_responses(
		processed.completion[:],
		"update-routine-protocol-second",
	)
	testing.expect_value(t, first_started.command_count, 4)
	testing.expect_value(t, first_started.accepted, 1)
	testing.expect_value(t, first_started.accepted_index, 0)
	testing.expect_value(t, second_started.accepted, 1)
	testing.expect_value(t, second_started.accepted_index, 1)
	update_response_inspection_free(&first_started)
	update_response_inspection_free(&second_started)
	updates_started_completion := processed.completion
	workflow.error_free(&processed.error)

	second_output, second_output_error := codec.encode_json("Hello Second")
	testing.expect(t, second_output_error == .None)
	defer codec.payload_free(&second_output)
	second_output_proto := proto.encode_payload(codec.payload_view(&second_output))
	defer delete(second_output_proto.metadata)
	defer delete(second_output_proto.external_payloads)
	second_success := proto.Coresdk_Activity_Result_Success{result = &second_output_proto}
	second_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &second_success,
		},
	}
	resolve_second := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 2,
		result = &second_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 10
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_second,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "second Update resolution failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.updates_completed, 1)
	testing.expect_value(t, processed.resolved_activity_seq, u32(2))
	testing.expect(t, !processed.completed)
	second_completed := inspect_update_responses(
		processed.completion[:],
		"update-routine-protocol-second",
	)
	testing.expect_value(t, second_completed.command_count, 1)
	testing.expect_value(t, second_completed.completed, 1)
	second_result: Update_Routine_Output
	second_decode_error := codec.decode_json(
		codec.payload_view(&second_completed.payload),
		&second_result,
	)
	testing.expect(t, second_decode_error == .None)
	testing.expect_value(t, second_result.previous, "Odin")
	testing.expect_value(t, second_result.current, "Hello Second")
	testing.expect_value(t, second_result.activity_result, "Hello Second")
	delete(second_result.previous)
	delete(second_result.current)
	delete(second_result.activity_result)
	update_response_inspection_free(&second_completed)
	second_completed_completion := processed.completion
	workflow.error_free(&processed.error)

	first_output, first_output_error := codec.encode_json("Hello First")
	testing.expect(t, first_output_error == .None)
	defer codec.payload_free(&first_output)
	first_output_proto := proto.encode_payload(codec.payload_view(&first_output))
	defer delete(first_output_proto.metadata)
	defer delete(first_output_proto.external_payloads)
	first_success := proto.Coresdk_Activity_Result_Success{result = &first_output_proto}
	first_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &first_success,
		},
	}
	resolve_first := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &first_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 15
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_first,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "first Update resolution failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.updates_completed, 1)
	testing.expect_value(t, processed.resolved_activity_seq, u32(1))
	testing.expect(t, !processed.completed)
	first_completed := inspect_update_responses(
		processed.completion[:],
		"update-routine-protocol-first",
	)
	testing.expect_value(t, first_completed.command_count, 1)
	testing.expect_value(t, first_completed.completed, 1)
	first_result: Update_Routine_Output
	first_decode_error := codec.decode_json(
		codec.payload_view(&first_completed.payload),
		&first_result,
	)
	testing.expect(t, first_decode_error == .None)
	testing.expect_value(t, first_result.previous, "Odin")
	testing.expect_value(t, first_result.current, "Hello First")
	testing.expect_value(t, first_result.activity_result, "Hello First")
	delete(first_result.previous)
	delete(first_result.current)
	delete(first_result.activity_result)
	update_response_inspection_free(&first_completed)
	first_completed_completion := processed.completion
	workflow.error_free(&processed.error)

	finish_input, finish_input_error := codec.encode_json("done")
	testing.expect(t, finish_input_error == .None)
	defer codec.payload_free(&finish_input)
	finish_input_proto := proto.encode_payload(codec.payload_view(&finish_input))
	defer delete(finish_input_proto.metadata)
	defer delete(finish_input_proto.external_payloads)
	finish_signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{
		signal_name = "finish",
	}
	defer delete(finish_signal.input)
	append(&finish_signal.input, finish_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 20
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.completed)
	workflow_completed_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	activation.is_replaying = true
	activation.history_length = 3
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	first_update.run_validator = false
	second_update.run_validator = false
	activation.history_length = 5
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &first_update,
			},
		},
	)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &second_update,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(updates_started_completion[:], processed.completion[:]))
	delete(updates_started_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	activation.history_length = 10
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_second,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(second_completed_completion[:], processed.completion[:]))
	delete(second_completed_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	activation.history_length = 15
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_first,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(first_completed_completion[:], processed.completion[:]))
	delete(first_completed_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	activation.history_length = 20
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &finish_signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(workflow_completed_completion[:], processed.completion[:]))
	delete(workflow_completed_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_applies_validated_updates_and_replays_state :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_update_workflow",
		"test-queue",
		fiber_query_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	workflow_input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(workflow_input.metadata)
	append(
		&workflow_input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_update_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, workflow_input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-update-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	update_input, update_input_error := codec.encode_json(Update_Test_Input{amount = 3})
	testing.expect(t, update_input_error == .None)
	defer codec.payload_free(&update_input)
	update_input_proto := proto.encode_payload(codec.payload_view(&update_input))
	defer delete(update_input_proto.metadata)
	defer delete(update_input_proto.external_payloads)
	update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "increment-1",
		protocol_instance_id = "update-protocol-1",
		name = "increment",
		run_validator = true,
	}
	defer delete(update.input)
	append(&update.input, update_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &update,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	testing.expect_value(t, processed.updates_accepted, 1)
	testing.expect_value(t, processed.updates_rejected, 0)
	testing.expect_value(t, processed.updates_completed, 1)
	update_inspection := inspect_update_responses(processed.completion[:], "update-protocol-1")
	testing.expect_value(t, update_inspection.command_count, 2)
	testing.expect_value(t, update_inspection.accepted, 1)
	testing.expect_value(t, update_inspection.completed, 1)
	testing.expect_value(t, update_inspection.rejected, 0)
	testing.expect_value(t, update_inspection.accepted_index, 0)
	testing.expect_value(t, update_inspection.completed_index, 1)
	update_output: Update_Test_Output
	update_decode_error := codec.decode_json(
		codec.payload_view(&update_inspection.payload),
		&update_output,
	)
	testing.expect(t, update_decode_error == .None)
	testing.expect_value(t, update_output.previous, 4)
	testing.expect_value(t, update_output.current, 7)
	update_response_inspection_free(&update_inspection)
	update_completion := processed.completion
	workflow.error_free(&processed.error)

	query_input, query_input_error := codec.encode_json(Query_Test_Input{prefix = "After "})
	testing.expect(t, query_input_error == .None)
	defer codec.payload_free(&query_input)
	query_input_proto := proto.encode_payload(codec.payload_view(&query_input))
	defer delete(query_input_proto.metadata)
	defer delete(query_input_proto.external_payloads)
	query := proto.Coresdk_Workflow_Activation_QueryWorkflow{
		query_id = "query-after-update",
		query_type = "status",
	}
	defer delete(query.arguments)
	append(&query.arguments, query_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow{
				value = &query,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	query_inspection := inspect_query_response(processed.completion[:], "query-after-update")
	query_output: Query_Test_Output
	query_decode_error := codec.decode_json(
		codec.payload_view(&query_inspection.payload),
		&query_output,
	)
	testing.expect(t, query_decode_error == .None)
	testing.expect_value(t, query_output.message, "After waiting")
	testing.expect_value(t, query_output.count, 7)
	delete(query_output.message)
	codec.payload_free(&query_inspection.payload)
	delete(query_inspection.failure_message)
	delete(query_inspection.failure_type)
	query_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 3
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 5
	update.run_validator = false
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &update,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(update_completion[:], processed.completion[:]))
	delete(update_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow{
				value = &query,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, bytes.equal(query_completion[:], processed.completion[:]))
	delete(query_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	rejected_input, rejected_input_error := codec.encode_json(Update_Test_Input{amount = -1})
	testing.expect(t, rejected_input_error == .None)
	defer codec.payload_free(&rejected_input)
	rejected_input_proto := proto.encode_payload(codec.payload_view(&rejected_input))
	defer delete(rejected_input_proto.metadata)
	defer delete(rejected_input_proto.external_payloads)
	rejected_update := proto.Coresdk_Workflow_Activation_DoUpdate{
		id = "increment-rejected",
		protocol_instance_id = "update-protocol-rejected",
		name = "increment",
		run_validator = true,
	}
	defer delete(rejected_update.input)
	append(&rejected_update.input, rejected_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update{
				value = &rejected_update,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect_value(t, processed.updates_accepted, 0)
	testing.expect_value(t, processed.updates_rejected, 1)
	testing.expect_value(t, processed.updates_completed, 0)
	rejected_inspection := inspect_update_responses(
		processed.completion[:],
		"update-protocol-rejected",
	)
	testing.expect_value(t, rejected_inspection.command_count, 1)
	testing.expect_value(t, rejected_inspection.accepted, 0)
	testing.expect_value(t, rejected_inspection.rejected, 1)
	testing.expect_value(t, string(rejected_inspection.failure_type), "InvalidIncrement")
	update_response_inspection_free(&rejected_inspection)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_answers_typed_queries_without_resuming_execution :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_query_workflow",
		"test-queue",
		fiber_query_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_query_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-query-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	query_input, query_input_error := codec.encode_json(Query_Test_Input{prefix = "Before "})
	testing.expect(t, query_input_error == .None)
	defer codec.payload_free(&query_input)
	query_input_proto := proto.encode_payload(codec.payload_view(&query_input))
	defer delete(query_input_proto.metadata)
	defer delete(query_input_proto.external_payloads)
	query := proto.Coresdk_Workflow_Activation_QueryWorkflow{
		query_id = "query-1",
		query_type = "status",
	}
	defer delete(query.arguments)
	append(&query.arguments, query_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow{
				value = &query,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "typed query failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.queries_answered, 1)
	testing.expect_value(t, processed.queries_failed, 0)
	testing.expect(t, processed.waiting_signal)
	query_inspection := inspect_query_response(processed.completion[:], "query-1")
	testing.expect(t, query_inspection.found)
	testing.expect(t, !query_inspection.failed)
	testing.expect_value(t, query_inspection.command_count, 1)
	query_output: Query_Test_Output
	query_decode_error := codec.decode_json(codec.payload_view(&query_inspection.payload), &query_output)
	testing.expect(t, query_decode_error == .None)
	testing.expect_value(t, query_output.message, "Before waiting")
	testing.expect_value(t, query_output.count, 4)
	delete(query_output.message)
	codec.payload_free(&query_inspection.payload)
	delete(query_inspection.failure_message)
	delete(query_inspection.failure_type)
	query_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow{
				value = &query,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(query_completion[:], processed.completion[:]))
	delete(query_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	unknown_query := proto.Coresdk_Workflow_Activation_QueryWorkflow{
		query_id = "query-missing",
		query_type = "missing",
	}
	defer delete(unknown_query.arguments)
	append(&unknown_query.arguments, query_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow{
				value = &unknown_query,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect_value(t, processed.queries_answered, 1)
	testing.expect_value(t, processed.queries_failed, 1)
	missing_inspection := inspect_query_response(processed.completion[:], "query-missing")
	testing.expect(t, missing_inspection.found)
	testing.expect(t, missing_inspection.failed)
	testing.expect_value(t, string(missing_inspection.failure_type), "OdinWorkflowQueryNotFound")
	codec.payload_free(&missing_inspection.payload)
	delete(missing_inspection.failure_message)
	delete(missing_inspection.failure_type)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	mutation_query := proto.Coresdk_Workflow_Activation_QueryWorkflow{
		query_id = "query-mutation",
		query_type = "mutate",
	}
	defer delete(mutation_query.arguments)
	append(&mutation_query.arguments, query_input_proto)
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow{
				value = &mutation_query,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	testing.expect(t, !processed.scheduled_activity)
	testing.expect_value(t, processed.queries_failed, 1)
	mutation_inspection := inspect_query_response(processed.completion[:], "query-mutation")
	testing.expect(t, mutation_inspection.found)
	testing.expect(t, mutation_inspection.failed)
	testing.expect_value(
		t,
		string(mutation_inspection.failure_type),
		"OdinWorkflowQueryMutationError",
	)
	codec.payload_free(&mutation_inspection.payload)
	delete(mutation_inspection.failure_message)
	delete(mutation_inspection.failure_type)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	query.query_id = "query-with-signal"
	signal_payload := proto.Temporal_Api_Common_V1_Payload{
		data = transmute([]u8)string(`"Approved"`),
	}
	defer delete(signal_payload.metadata)
	append(
		&signal_payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "finish"}
	defer delete(signal.input)
	append(&signal.input, signal_payload)
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &signal,
			},
		},
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow{
				value = &query,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "query plus signal failed: %s", string(processed.error.message))
	testing.expect(t, processed.completed)
	testing.expect(t, processed.received_signal)
	testing.expect_value(t, processed.queries_answered, 1)
	combined_inspection := inspect_query_response(processed.completion[:], "query-with-signal")
	testing.expect(t, combined_inspection.found)
	testing.expect(t, !combined_inspection.failed)
	testing.expect_value(t, combined_inspection.command_count, 2)
	combined_output: Query_Test_Output
	combined_decode_error := codec.decode_json(
		codec.payload_view(&combined_inspection.payload),
		&combined_output,
	)
	testing.expect(t, combined_decode_error == .None)
	testing.expect_value(t, combined_output.message, "Before waiting")
	delete(combined_output.message)
	codec.payload_free(&combined_inspection.payload)
	delete(combined_inspection.failure_message)
	delete(combined_inspection.failure_type)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_replays_handled_activity_failure :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_failure_recovery_workflow",
		"test-queue",
		fiber_failure_recovery_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_failure_recovery_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-failure-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.activity_seq, u32(1))
	delete(processed.completion)
	workflow.error_free(&processed.error)

	failure_details, failure_details_error := codec.encode_json(
		Failure_Test_Details{dependency = "billing", code = 409},
	)
	testing.expect(t, failure_details_error == .None)
	defer codec.payload_free(&failure_details)
	failure_details_proto := proto.encode_payload(codec.payload_view(&failure_details))
	defer delete(failure_details_proto.metadata)
	defer delete(failure_details_proto.external_payloads)
	failure_detail_payloads := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(failure_detail_payloads.payloads)
	append(&failure_detail_payloads.payloads, failure_details_proto)
	next_retry_delay := proto.Google_Protobuf_Duration{nanos = 250_000_000}
	application_info := proto.Temporal_Api_Failure_V1_ApplicationFailureInfo{
		type = "DependencyRejected",
		details = &failure_detail_payloads,
		next_retry_delay = &next_retry_delay,
		category = .APPLICATION_ERROR_CATEGORY_BENIGN,
	}
	application_failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "dependency rejected request",
		source = "OdinSDK",
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = &application_info,
		},
	}
	activity_type := proto.Temporal_Api_Common_V1_ActivityType{name = "missing"}
	activity_info := proto.Temporal_Api_Failure_V1_ActivityFailureInfo{
		identity = "test-worker",
		activity_type = &activity_type,
		activity_id = "failure-activity",
		retry_state = .RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED,
	}
	activity_failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "Activity task failed",
		source = "TemporalServer",
		cause = &application_failure,
		failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Activity_Failure_Info{
			value = &activity_info,
		},
	}
	failed_result := proto.Coresdk_Activity_Result_Failure{failure = &activity_failure}
	failure_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Failed{value = &failed_result},
	}
	resolve_failure := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &failure_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_failure,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "handled failure failed: %s", string(processed.error.message))
	testing.expect(t, processed.resolved_activity_failed)
	testing.expect_value(t, processed.resolved_activity_seq, u32(1))
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.activity_seq, u32(2))
	testing.expect(t, !processed.completed)
	failure_branch_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_failure,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.resolved_activity_failed)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.activity_seq, u32(2))
	testing.expect(t, bytes.equal(failure_branch_completion[:], processed.completion[:]))
	delete(failure_branch_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	recovery_output := proto.Temporal_Api_Common_V1_Payload{
		data = transmute([]u8)string(`"Hello Recovered Odin"`),
	}
	defer delete(recovery_output.metadata)
	append(
		&recovery_output.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	recovery_success := proto.Coresdk_Activity_Result_Success{result = &recovery_output}
	recovery_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &recovery_success,
		},
	}
	resolve_recovery := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 2,
		result = &recovery_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 15
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_recovery,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.completed)
	testing.expect_value(t, processed.resolved_activity_seq, u32(2))
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_propagates_unhandled_activity_failure :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_unhandled_failure_workflow",
		"test-queue",
		fiber_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_unhandled_failure_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{run_id = "fiber-unhandled-run"}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	failure := proto.Temporal_Api_Failure_V1_Failure{
		message = "preserved Activity failure",
		source = "OdinSDK",
	}
	failed_result := proto.Coresdk_Activity_Result_Failure{failure = &failure}
	resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Failed{value = &failed_result},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{seq = 1, result = &resolution}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "unhandled failure failed: %s", string(processed.error.message))
	testing.expect(t, processed.completed)
	testing.expect(t, processed.resolved_activity_failed)

	decode_arena: virtual.Arena
	arena_error := virtual.arena_init_growing(&decode_arena)
	testing.expect(t, arena_error == nil)
	if arena_error == nil {
		previous_allocator := context.allocator
		context.allocator = virtual.arena_allocator(&decode_arena)
		completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
		completion_ok := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
			processed.completion[:],
			&completion,
		)
		context.allocator = previous_allocator
		testing.expect(t, completion_ok)
		preserved_message := ""
		#partial switch status in completion.status {
		case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
			if status.value != nil && len(status.value.commands) == 1 {
				#partial switch command in status.value.commands[0].variant {
				case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Fail_Workflow_Execution:
					if command.value != nil && command.value.failure != nil {
						preserved_message = command.value.failure.message
					}
				}
			}
		}
		testing.expect_value(t, preserved_message, "preserved Activity failure")
		virtual.arena_destroy(&decode_arena)
	}
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_buffers_signal_while_waiting_for_activity :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_buffered_signal_workflow",
		"test-queue",
		fiber_buffered_signal_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_buffered_signal_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-signal-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.waiting_activity_seq, u32(1))
	delete(processed.completion)
	workflow.error_free(&processed.error)

	signal_payload := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Approved"`)}
	defer delete(signal_payload.metadata)
	append(
		&signal_payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "approval"}
	defer delete(signal.input)
	append(&signal.input, signal_payload)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "buffering signal failed: %s", string(processed.error.message))
	testing.expect(t, processed.received_signal)
	testing.expect_value(t, processed.received_signal_count, 1)
	testing.expect_value(t, processed.waiting_activity_seq, u32(1))
	testing.expect(t, !processed.waiting_signal)
	testing.expect(t, !processed.completed)
	testing.expect(t, !processed.scheduled_activity)
	buffered_signal_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.received_signal)
	testing.expect(t, processed.is_replaying)
	testing.expect(t, bytes.equal(buffered_signal_completion[:], processed.completion[:]))
	delete(buffered_signal_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	activity_output := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Hello Odin"`)}
	defer delete(activity_output.metadata)
	append(
		&activity_output.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &activity_output}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve_activity := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_activity,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "consuming buffered signal failed: %s", string(processed.error.message))
	testing.expect(t, processed.completed)
	testing.expect_value(t, processed.resolved_activity_seq, u32(1))
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_resumes_direct_signal_await :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_wait_signal_workflow",
		"test-queue",
		fiber_wait_signal_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_wait_signal_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{run_id = "fiber-wait-signal-run"}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.waiting_signal)
	testing.expect(t, !processed.completed)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	owned_signal_payload, signal_encode_error := codec.encode_json(
		Signal_Test_Value{message = "Approved", approved = true},
	)
	testing.expect(t, signal_encode_error == .None)
	defer codec.payload_free(&owned_signal_payload)
	signal_payload := proto.encode_payload(codec.payload_view(&owned_signal_payload))
	defer delete(signal_payload.metadata)
	defer delete(signal_payload.external_payloads)
	signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "approval"}
	defer delete(signal.input)
	append(&signal.input, signal_payload)
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "direct signal await failed: %s", string(processed.error.message))
	testing.expect(t, processed.received_signal)
	testing.expect(t, processed.completed)
	inspection := inspect_workflow_completion(processed.completion[:])
	defer delete(inspection.completion_result)
	testing.expect(t, inspection.decoded)
	testing.expect(t, inspection.completed)
	testing.expect_value(t, string(inspection.completion_result), `"Approved"`)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_cancels_signal_timeout_timer_across_replay :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_signal_timeout_activity_workflow",
		"test-queue",
		fiber_signal_timeout_activity_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_signal_timeout_activity_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-signal-timeout-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "starting signal timeout failed: %s", string(processed.error.message))
	testing.expect(t, processed.started_timer)
	testing.expect(t, processed.waiting_signal)
	testing.expect_value(t, processed.timer_seq, u32(1))
	testing.expect_value(t, processed.waiting_timer_seq, u32(1))
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	signal_payload := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Approved"`)}
	defer delete(signal_payload.metadata)
	append(
		&signal_payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "approval"}
	defer delete(signal.input)
	append(&signal.input, signal_payload)
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "signal did not win timer race: %s", string(processed.error.message))
	testing.expect(t, processed.received_signal)
	testing.expect(t, processed.canceled_timer)
	testing.expect_value(t, processed.canceled_timer_seq, u32(1))
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.waiting_activity_seq, u32(1))
	testing.expect(t, !processed.completed)
	inspection := inspect_workflow_completion(processed.completion[:])
	defer delete(inspection.completion_result)
	testing.expect(t, inspection.decoded)
	testing.expect_value(t, inspection.command_count, 2)
	testing.expect_value(t, inspection.scheduled_activity_seq, u32(1))
	testing.expect_value(t, inspection.canceled_timer_seq, u32(1))
	signal_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &signal,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "replaying signal timeout race failed: %s", string(processed.error.message))
	testing.expect(t, processed.is_replaying)
	testing.expect(t, processed.canceled_timer)
	testing.expect(t, processed.scheduled_activity)
	testing.expect(t, bytes.equal(signal_completion[:], processed.completion[:]))
	delete(signal_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	activity_output := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Hello Approved"`)}
	defer delete(activity_output.metadata)
	append(
		&activity_output.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &activity_output}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve_activity := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 15
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_activity,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "Activity after signal timeout race failed: %s", string(processed.error.message))
	testing.expect(t, processed.completed)
	testing.expect_value(t, processed.resolved_activity_seq, u32(1))
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_signal_timeout_timer_wins :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_signal_timeout_workflow",
		"test-queue",
		fiber_signal_timeout_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_signal_timeout_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-timeout-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.started_timer)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	fire_timer := proto.Coresdk_Workflow_Activation_FireTimer{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{
				value = &fire_timer,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "timer did not win signal race: %s", string(processed.error.message))
	testing.expect(t, processed.completed)
	testing.expect(t, !processed.canceled_timer)
	inspection := inspect_workflow_completion(processed.completion[:])
	defer delete(inspection.completion_result)
	testing.expect(t, inspection.decoded)
	testing.expect_value(t, inspection.command_count, 1)
	testing.expect(t, inspection.completed)
	testing.expect_value(t, string(inspection.completion_result), `"Timed out"`)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_signal_wins_same_activation_without_canceling_fired_timer :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_signal_timeout_workflow",
		"test-queue",
		fiber_signal_timeout_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_signal_timeout_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-signal-tie-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	signal_payload := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Approved"`)}
	defer delete(signal_payload.metadata)
	append(
		&signal_payload.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	signal := proto.Coresdk_Workflow_Activation_SignalWorkflow{signal_name = "approval"}
	defer delete(signal.input)
	append(&signal.input, signal_payload)
	fire_timer := proto.Coresdk_Workflow_Activation_FireTimer{seq = 1}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow{
				value = &signal,
			},
		},
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{
				value = &fire_timer,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "same-activation race failed: %s", string(processed.error.message))
	testing.expect(t, processed.received_signal)
	testing.expect(t, processed.completed)
	testing.expect(t, !processed.canceled_timer)
	inspection := inspect_workflow_completion(processed.completion[:])
	defer delete(inspection.completion_result)
	testing.expect(t, inspection.decoded)
	testing.expect_value(t, inspection.command_count, 1)
	testing.expect(t, inspection.completed)
	testing.expect_value(t, string(inspection.completion_result), `"Approved"`)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_cancels_pending_timer_across_replay :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_cancel_timer_workflow",
		"test-queue",
		fiber_signal_timeout_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_cancel_timer_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-cancel-timer-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.started_timer)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	cancel := proto.Coresdk_Workflow_Activation_CancelWorkflow{reason = "operator request"}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "canceling pending timer failed: %s", string(processed.error.message))
	testing.expect(t, processed.received_cancellation)
	testing.expect(t, processed.canceled_timer)
	testing.expect(t, processed.canceled_workflow)
	testing.expect(t, processed.completed)
	testing.expect_value(t, processed.cancellation_reason, "operator request")
	inspection := inspect_workflow_completion(processed.completion[:])
	defer delete(inspection.completion_result)
	testing.expect(t, inspection.decoded)
	testing.expect_value(t, inspection.command_count, 2)
	testing.expect_value(t, inspection.canceled_timer_seq, u32(1))
	testing.expect(t, inspection.canceled_workflow)
	cancel_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "replaying timer cancellation failed: %s", string(processed.error.message))
	testing.expect(t, processed.is_replaying)
	testing.expect(t, bytes.equal(cancel_completion[:], processed.completion[:]))
	delete(cancel_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_cancels_pending_activity :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_cancel_activity_workflow",
		"test-queue",
		fiber_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_cancel_activity_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-cancel-activity-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	cancel := proto.Coresdk_Workflow_Activation_CancelWorkflow{reason = "stop Activity"}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "canceling pending Activity failed: %s", string(processed.error.message))
	testing.expect(t, processed.received_cancellation)
	testing.expect(t, processed.requested_activity_cancellation)
	testing.expect_value(t, processed.requested_activity_cancellation_count, 1)
	testing.expect_value(t, processed.canceled_activity_seq, u32(1))
	testing.expect(t, processed.canceled_workflow)
	inspection := inspect_workflow_completion(processed.completion[:])
	defer delete(inspection.completion_result)
	testing.expect(t, inspection.decoded)
	testing.expect_value(t, inspection.command_count, 2)
	testing.expect_value(t, inspection.canceled_activity_seq, u32(1))
	testing.expect(t, inspection.canceled_workflow)
	cancel_completion := processed.completion
	workflow.error_free(&processed.error)

	activity_cancellation := proto.Coresdk_Activity_Result_Cancellation{}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Cancelled{
			value = &activity_cancellation,
		},
	}
	resolve_activity := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_activity,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "late canceled Activity resolution failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.resolved_activity_seq, u32(1))
	late_inspection := inspect_workflow_completion(processed.completion[:])
	defer delete(late_inspection.completion_result)
	testing.expect(t, late_inspection.decoded)
	testing.expect_value(t, late_inspection.command_count, 0)
	late_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "replaying canceled Activity initialization failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "replaying pending Activity cancellation failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(cancel_completion[:], processed.completion[:]))
	delete(cancel_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 9
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_activity,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "replaying late Activity cancellation resolution failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(late_completion[:], processed.completion[:]))
	delete(late_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_runs_detached_cleanup_before_acknowledging_cancellation :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_cancellation_cleanup_workflow",
		"test-queue",
		fiber_cancellation_cleanup_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_cancellation_cleanup_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-cancellation-cleanup-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.activity_seq, u32(1))
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	cancel := proto.Coresdk_Workflow_Activation_CancelWorkflow{
		reason = "operator cleanup request",
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "starting cancellation cleanup failed: %s", string(processed.error.message))
	testing.expect(t, processed.received_cancellation)
	testing.expect(t, processed.requested_activity_cancellation)
	testing.expect_value(t, processed.canceled_activity_seq, u32(1))
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.activity_seq, u32(2))
	testing.expect(t, !processed.canceled_workflow)
	testing.expect(t, !processed.completed)
	cleanup_inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, cleanup_inspection.decoded)
	testing.expect_value(t, cleanup_inspection.command_count, 2)
	testing.expect_value(t, cleanup_inspection.canceled_activity_seq, u32(1))
	testing.expect_value(t, cleanup_inspection.scheduled_activity_seq, u32(2))
	testing.expect(t, !cleanup_inspection.canceled_workflow)
	delete(cleanup_inspection.failure_message)
	delete(cleanup_inspection.completion_result)
	cleanup_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "replaying cancellation cleanup failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(cleanup_completion[:], processed.completion[:]))
	delete(cleanup_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	cleanup_output := proto.Temporal_Api_Common_V1_Payload{
		data = transmute([]u8)string(`"Cleaned Odin"`),
	}
	defer delete(cleanup_output.metadata)
	append(
		&cleanup_output.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	cleanup_success := proto.Coresdk_Activity_Result_Success{result = &cleanup_output}
	cleanup_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &cleanup_success,
		},
	}
	resolve_cleanup := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 2,
		result = &cleanup_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = false
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve_cleanup,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "finishing cancellation cleanup failed: %s", string(processed.error.message))
	testing.expect(t, processed.canceled_workflow)
	testing.expect(t, processed.completed)
	terminal_inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, terminal_inspection.decoded)
	testing.expect_value(t, terminal_inspection.command_count, 1)
	testing.expect(t, terminal_inspection.canceled_workflow)
	delete(terminal_inspection.failure_message)
	delete(terminal_inspection.completion_result)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_preemptive_detached_activity_survives_cancellation_and_replays :: proc(
	t: ^testing.T,
) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_preemptive_detached_activity_workflow",
		"test-queue",
		fiber_preemptive_detached_activity_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input, input_error := codec.encode_json("Odin")
	testing.expect(t, input_error == .None)
	defer codec.payload_free(&input)
	input_proto := proto.encode_payload(codec.payload_view(&input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_preemptive_detached_activity_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-preemptive-detached-activity-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.scheduled_activity)
	testing.expect_value(t, processed.waiting_activity_seq, u32(1))
	initial_completion := processed.completion
	workflow.error_free(&processed.error)

	cancel := proto.Coresdk_Workflow_Activation_CancelWorkflow{
		reason = "detached Activity must finish",
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "detached cancellation failed: %s", string(processed.error.message))
	testing.expect(t, processed.received_cancellation)
	testing.expect(t, !processed.requested_activity_cancellation)
	testing.expect(t, !processed.canceled_workflow)
	testing.expect(t, !processed.completed)
	testing.expect_value(t, processed.waiting_activity_seq, u32(1))
	cancel_inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, cancel_inspection.decoded)
	testing.expect_value(t, cancel_inspection.command_count, 0)
	delete(cancel_inspection.failure_message)
	delete(cancel_inspection.completion_result)
	cancel_completion := processed.completion
	workflow.error_free(&processed.error)

	activity_output, output_error := codec.encode_json("Hello Odin")
	testing.expect(t, output_error == .None)
	defer codec.payload_free(&activity_output)
	activity_output_proto := proto.encode_payload(codec.payload_view(&activity_output))
	defer delete(activity_output_proto.metadata)
	defer delete(activity_output_proto.external_payloads)
	activity_success := proto.Coresdk_Activity_Result_Success{result = &activity_output_proto}
	activity_resolution := proto.Coresdk_Activity_Result_ActivityResolution{
		status = proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed{
			value = &activity_success,
		},
	}
	resolve := proto.Coresdk_Workflow_Activation_ResolveActivity{
		seq = 1,
		result = &activity_resolution,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "detached Activity resolution failed: %s", string(processed.error.message))
	testing.expect_value(t, processed.resolved_activity_seq, u32(1))
	testing.expect(t, processed.canceled_workflow)
	testing.expect(t, processed.completed)
	terminal_inspection := inspect_workflow_completion(processed.completion[:])
	testing.expect(t, terminal_inspection.decoded)
	testing.expect_value(t, terminal_inspection.command_count, 1)
	testing.expect(t, terminal_inspection.canceled_workflow)
	delete(terminal_inspection.failure_message)
	delete(terminal_inspection.completion_result)
	terminal_completion := processed.completion
	workflow.error_free(&processed.error)

	eviction := proto.Coresdk_Workflow_Activation_RemoveFromCache{}
	delete(activation.jobs)
	activation.jobs = nil
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache{
				value = &eviction,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, processed.evicted)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.is_replaying = true
	activation.history_length = 3
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expect(t, processed.error.message == nil)
	testing.expect(t, bytes.equal(initial_completion[:], processed.completion[:]))
	delete(initial_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 8
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "detached cancellation replay failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(cancel_completion[:], processed.completion[:]))
	delete(cancel_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 14
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity{
				value = &resolve,
			},
		},
	)
	processed = process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "detached Activity replay failed: %s", string(processed.error.message))
	testing.expect(t, bytes.equal(terminal_completion[:], processed.completion[:]))
	delete(terminal_completion)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

@(test)
workflow_fiber_cancellation_before_initial_commands_are_sent :: proc(t: ^testing.T) {
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		"odin_immediate_cancel_workflow",
		"test-queue",
		fiber_signal_timeout_test_workflow,
	)
	testing.expect(t, init_error.message == nil)
	defer workflow.error_free(&init_error)
	defer workflow.fiber_runtime_close(&runtime)

	input := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)string(`"Odin"`)}
	defer delete(input.metadata)
	append(
		&input.metadata,
		proto.Temporal_Api_Common_V1_Payload_MetadataEntry{
			key = "encoding",
			value = transmute([]u8)string("json/plain"),
		},
	)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "odin_immediate_cancel_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input)
	cancel := proto.Coresdk_Workflow_Activation_CancelWorkflow{reason = "already canceled"}
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "fiber-immediate-cancel-run",
		history_length = 3,
	}
	defer delete(activation.jobs)
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow{
				value = &cancel,
			},
		},
	)
	processed := process_fiber_test_activation(&runtime, &activation)
	testing.expectf(t, processed.error.message == nil, "immediate cancellation failed: %s", string(processed.error.message))
	testing.expect(t, processed.received_cancellation)
	testing.expect(t, processed.canceled_workflow)
	testing.expect(t, !processed.started_timer)
	testing.expect(t, !processed.canceled_timer)
	inspection := inspect_workflow_completion(processed.completion[:])
	defer delete(inspection.completion_result)
	testing.expect(t, inspection.decoded)
	testing.expect_value(t, inspection.command_count, 1)
	testing.expect(t, inspection.canceled_workflow)
	delete(processed.completion)
	workflow.error_free(&processed.error)
}

package workflow_pbt_test

import "core:bytes"
import "core:mem/virtual"
import "core:strings"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import codec "../temporal/codec"
import proto "../temporal/core/proto"
import worker "../temporal/worker"
import workflow "../temporal/workflow"

Generated_Deadline_Input :: struct {
	parent_minutes: i64,
	child_minutes:  i64,
}

Generated_Deadline_Output :: struct {
	effective_minutes: i64,
	parent_canceled:   bool,
	child_canceled:    bool,
	reason_matches:    bool,
}

generated_cancellation_deadline_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	input: Generated_Deadline_Input,
) -> Generated_Deadline_Output {
	activation_time, has_time := workflow.now(ctx)
	if !has_time || input.parent_minutes <= 0 || input.child_minutes <= 0 {
		return {}
	}
	parent := workflow.cancellation_child_with_timeout(
		workflow.cancellation_root(ctx),
		time.Duration(input.parent_minutes) * time.Minute,
	)
	child := workflow.cancellation_child_with_timeout(
		parent,
		time.Duration(input.child_minutes) * time.Minute,
	)
	parent_deadline, parent_has_deadline := workflow.cancellation_deadline(parent)
	child_deadline, child_has_deadline := workflow.cancellation_deadline(child)
	if parent.id == 0 || child.id == 0 || !parent_has_deadline || !child_has_deadline {
		return {}
	}
	expected_minutes := min(input.parent_minutes, input.child_minutes)
	if time.time_to_unix_nano(parent_deadline) !=
	   time.time_to_unix_nano(activation_time) + input.parent_minutes * i64(time.Minute) ||
	   time.time_to_unix_nano(child_deadline) !=
	   time.time_to_unix_nano(activation_time) + expected_minutes * i64(time.Minute) {
		return {}
	}
	scope := workflow.cancellation_scope(ctx, child)
	defer workflow.cancellation_scope_end(&scope)
	_ = workflow.await_signal(ctx, "never-generated")
	return {
		effective_minutes = expected_minutes,
		parent_canceled = workflow.cancellation_requested(parent),
		child_canceled = workflow.cancellation_requested(child),
		reason_matches = workflow.cancellation_token_reason(child) ==
			"Workflow cancellation deadline exceeded",
	}
}

decode_deadline_completion :: proc(
	data: []u8,
) -> (command_count: int, completed: bool, output: Generated_Deadline_Output, ok: bool) {
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	defer {context.allocator = previous_allocator}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	if !proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(data, &completion) {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return
		}
		command_count = len(status.value.commands)
		for &command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution:
				if variant.value == nil || variant.value.result == nil {
					return
				}
				owned, clone_error := proto.clone_payload(variant.value.result)
				if clone_error != .None {
					return
				}
				defer codec.payload_free(&owned)
				if codec.decode_json(codec.payload_view(&owned), &output) != .None {
					return
				}
				completed = true
			}
		}
		ok = true
	}
	return
}

cancellation_deadline_property :: proc(t: ^pbt.T) -> pbt.Result {
	parent_minutes := i64(pbt.draw(t, pbt.int_range(1, 120)))
	child_minutes := i64(pbt.draw(t, pbt.int_range(1, 120)))
	pbt.cover(t, child_minutes < parent_minutes, 25.0, "child deadline wins")
	pbt.cover(t, child_minutes == parent_minutes, 0.5, "equal deadlines")
	pbt.cover(t, child_minutes > parent_minutes, 25.0, "parent deadline wins")

	owned_queue, queue_error := strings.clone("pbt-cancellation-deadline-queue")
	if queue_error != nil {
		return pbt.error("could not allocate deadline queue")
	}
	sdk_worker := worker.Worker{
		task_queue = owned_queue,
		workflow_registrations = make([dynamic]worker.Workflow_Registration),
		lifecycle = .Configuring,
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}
	register_error := worker.register_json_workflow(
		&sdk_worker,
		"generated_cancellation_deadline_workflow",
		generated_cancellation_deadline_workflow,
	)
	if register_error.message != nil {
		worker.error_free(&register_error)
		return pbt.error("could not register generated deadline Workflow")
	}
	worker.error_free(&register_error)
	runtime := &sdk_worker.workflow_registrations[0].runtime

	input := Generated_Deadline_Input{
		parent_minutes = parent_minutes,
		child_minutes = child_minutes,
	}
	encoded_input, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return pbt.error("could not encode generated deadline input")
	}
	defer codec.payload_free(&encoded_input)
	input_proto := proto.encode_payload(codec.payload_view(&encoded_input))
	defer delete(input_proto.metadata)
	defer delete(input_proto.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = "generated_cancellation_deadline_workflow",
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, input_proto)
	timestamp := proto.Google_Protobuf_Timestamp{
		seconds = 1_700_000_000,
		nanos = 123_456_789,
	}
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = "pbt-cancellation-deadline-run",
		history_length = 3,
		timestamp = &timestamp,
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
	initial := process_activation(runtime, &activation)
	defer destroy_process_result(&initial)
	initial_commands, initial_completed, _, initial_decoded := decode_deadline_completion(
		initial.completion[:],
	)
	expected_initial_commands := child_minutes < parent_minutes ? 2 : 1
	if initial.error.message != nil || initial_completed || !initial_decoded ||
	   initial_commands != expected_initial_commands || !initial.waiting_signal {
		return pbt.fail("generated cancellation deadlines emitted unexpected timers")
	}
	initial_completion := bytes.clone(initial.completion[:])
	defer delete(initial_completion)

	effective_minutes := min(parent_minutes, child_minutes)
	fire := proto.Coresdk_Workflow_Activation_FireTimer{
		seq = child_minutes < parent_minutes ? 2 : 1,
	}
	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	activation.timestamp = &proto.Google_Protobuf_Timestamp{
		seconds = timestamp.seconds + effective_minutes * 60,
		nanos = timestamp.nanos,
	}
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{
				value = &fire,
			},
		},
	)
	terminal := process_activation(runtime, &activation)
	defer destroy_process_result(&terminal)
	terminal_commands, completed, output, terminal_decoded := decode_deadline_completion(
		terminal.completion[:],
	)
	if terminal.error.message != nil || !terminal_decoded || !completed ||
	   terminal_commands != 1 || output.effective_minutes != effective_minutes ||
	   output.parent_canceled != (parent_minutes <= child_minutes) ||
	   !output.child_canceled || !output.reason_matches {
		return pbt.fail("generated cancellation deadline produced the wrong branch result")
	}
	terminal_completion := bytes.clone(terminal.completion[:])
	defer delete(terminal_completion)

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
	evicted := process_activation(runtime, &activation)
	defer destroy_process_result(&evicted)
	if evicted.error.message != nil || !evicted.evicted {
		return pbt.fail("generated cancellation deadline did not evict")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 3
	activation.timestamp = &timestamp
	activation.is_replaying = true
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
				value = &initialize,
			},
		},
	)
	replayed_initial := process_activation(runtime, &activation)
	defer destroy_process_result(&replayed_initial)
	if replayed_initial.error.message != nil ||
	   !bytes.equal(initial_completion, replayed_initial.completion[:]) {
		return pbt.fail("generated cancellation deadline initialization changed on replay")
	}

	delete(activation.jobs)
	activation.jobs = nil
	activation.history_length = 7
	activation.timestamp = &proto.Google_Protobuf_Timestamp{
		seconds = timestamp.seconds + effective_minutes * 60,
		nanos = timestamp.nanos,
	}
	append(
		&activation.jobs,
		proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
			variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{
				value = &fire,
			},
		},
	)
	replayed_terminal := process_activation(runtime, &activation)
	defer destroy_process_result(&replayed_terminal)
	if replayed_terminal.error.message != nil ||
	   !bytes.equal(terminal_completion, replayed_terminal.completion[:]) {
		return pbt.fail("generated cancellation deadline completion changed on replay")
	}
	return pbt.pass()
}

@(test)
workflow_cancellation_deadlines_propagate_and_replay :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow cancellation deadlines propagate and replay",
		cancellation_deadline_property,
		{num_tests = 300, seed = 0x4445_4144_4c49_4e45},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

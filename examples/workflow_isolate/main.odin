package main

import "core:bytes"
import "core:fmt"
import "core:mem/virtual"
import "core:os"
import "core:time"
import codec "../../temporal/codec"
import proto "../../temporal/core/proto"
import workflow "../../temporal/workflow"
import workflow_isolate "../../temporal/workflow/isolate"
import workflow_app "../workflow_isolate_app"

WORKFLOW_TYPE :: workflow_app.GREETING_WORKFLOW_TYPE
TASK_QUEUE :: workflow_app.DEFAULT_TASK_QUEUE
RUN_ID :: "isolated-workflow-run"

Child_Fault_Mode :: enum {
	None,
	Crash,
	Hang,
	Late,
	Stale_Request,
	Malformed_Metadata,
	Malformed_Completion,
	Metadata_Mismatch,
	Nondeterministic,
	Stack_Overflow,
	Panic,
}

executor_log :: proc(user_data: rawptr, record: ^workflow.Log_Record) {
	if record == nil {
		return
	}
	host := cast(^workflow_isolate.Executor_Host)user_data
	_ = workflow_isolate.executor_host_emit_diagnostic(
		host,
		.Log,
		transmute([]u8)record.message,
	)
	fmt.eprintf(
		"isolated-executor workflow_log message=%q type=%s replay=%v replaying_history=%v history=%d\n",
		record.message,
		record.workflow_type,
		record.replaying,
		record.replaying_history_events,
		record.history_length,
	)
}

executor_metric_emit :: proc(user_data: rawptr, record: ^workflow.Metric_Record) {
	if record == nil {
		return
	}
	host := cast(^workflow_isolate.Executor_Host)user_data
	_ = workflow_isolate.executor_host_emit_diagnostic(
		host,
		.Metric,
		transmute([]u8)record.name,
	)
	fmt.eprintf(
		"isolated-executor workflow_metric name=%s value=%d type=%s replay=%v replaying_history=%v\n",
		record.name,
		record.integer_value,
		record.workflow_type,
		record.replaying,
		record.replaying_history_events,
	)
}

executor_metric_emitter_factory :: proc(
	user_data: rawptr,
	_, _, _: string,
) -> (workflow.Metric_Emitter, bool) {
	return workflow.Metric_Emitter{
		user_data = user_data,
		emit = executor_metric_emit,
	}, true
}

executor_execute_workflow_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Inbound_Execute_Workflow_Input,
	next: workflow.Inbound_Execute_Workflow_Next,
) -> workflow.Payload_Result {
	fields := [1]workflow.Log_Field{
		workflow.log_field_string("hook", "executor-host-inbound"),
	}
	workflow.log_info(input.ctx, "workflow_started", fields[:])
	workflow.metric_counter_add(input.ctx, "workflow_started", 1)
	return next.invoke(next.data, input)
}

run_child :: proc(
	fault_mode := Child_Fault_Mode.None,
	requested_task_queue := "",
) -> int {
	task_queue_buffer: [256]u8
	task_queue := requested_task_queue
	if len(task_queue) == 0 {
		task_queue = os.get_env(task_queue_buffer[:], "TEMPORAL_ODIN_TASK_QUEUE")
	}
	if len(task_queue) == 0 {
		task_queue = TASK_QUEUE
	}
	registry := workflow.Registry{}
	registry_init_error := workflow.registry_init(&registry)
	if registry_init_error.message != nil {
		workflow.error_free(&registry_init_error)
		return 1
	}
	defer workflow.registry_close(&registry)
	registry_revision := workflow_app.Registry_Revision.Ordinary
	if fault_mode == .Nondeterministic {
		registry_revision = .Incompatible_Nondeterminism
	} else if fault_mode == .Stack_Overflow {
		registry_revision = .Stack_Overflow
	} else if fault_mode == .Panic {
		registry_revision = .Panic
	}
	registration_error := workflow_app.register_revision(&registry, registry_revision)
	if registration_error.message != nil {
		fmt.eprintf("Could not register executor Workflows: %s\n", string(registration_error.message))
		workflow.error_free(&registration_error)
		return 1
	}
	host := workflow_isolate.Executor_Host{}
	inbound_interceptors := [1]workflow.Inbound_Interceptor{{
		execute_workflow = executor_execute_workflow_interceptor,
	}}
	host_options := workflow_isolate.executor_host_options_default()
	host_options.logger = {user_data = &host, emit = executor_log}
	host_options.inbound_interceptors = inbound_interceptors[:]
	host_options.metric_emitter_factory_data = &host
	host_options.metric_emitter_factory = executor_metric_emitter_factory
	host_init_error := workflow_isolate.executor_host_init_with_options(
		&host,
		&registry,
		task_queue,
		host_options,
	)
	if host_init_error.message != nil {
		fmt.eprintf("Could not initialize Workflow executor host: %s\n", string(host_init_error.message))
		workflow_isolate.host_error_free(&host_init_error)
		return 1
	}
	defer workflow_isolate.executor_host_close(&host)
	if fault_mode != .None && fault_mode != .Nondeterministic &&
	   fault_mode != .Stack_Overflow && fault_mode != .Panic {
		manifest_request, read_error := workflow_isolate.read(os.stdin)
		if read_error.kind != .None ||
		   manifest_request.frame.kind != .Manifest_Request {
			fmt.eprintln("Could not read fault-injection manifest request")
			workflow_isolate.owned_frame_free(&manifest_request)
			return 1
		}
		write_error := workflow_isolate.write(os.stdout, {
			kind = .Manifest,
			request_id = manifest_request.frame.request_id,
			payload = workflow_isolate.executor_host_manifest(&host),
		})
		workflow_isolate.owned_frame_free(&manifest_request)
		if write_error.kind != .None {
			return 1
		}
		request, activation_read_error := workflow_isolate.read(os.stdin)
		if activation_read_error.kind != .None || request.frame.kind != .Activation {
			fmt.eprintln("Could not read fault-injection activation")
			workflow_isolate.owned_frame_free(&request)
			return 1
		}
		if fault_mode == .Crash {
			workflow_isolate.owned_frame_free(&request)
			os.exit(86)
		}
		if fault_mode == .Hang {
			workflow_isolate.owned_frame_free(&request)
			for {
				time.sleep(time.Hour)
			}
		}
		if fault_mode == .Late {
			time.sleep(time.Second)
		}
		response_request_id := request.frame.request_id
		metadata_payload := []u8{'{', '}'}
		if fault_mode == .Stale_Request {
			response_request_id -= 1
		} else if fault_mode == .Malformed_Metadata {
			metadata_payload = []u8{'{'}
		} else if fault_mode == .Metadata_Mismatch {
			metadata, metadata_ok := workflow_isolate.process_result_metadata_encode(
				&workflow.Process_Result{},
			)
			if !metadata_ok {
				workflow_isolate.owned_frame_free(&request)
				return 1
			}
			metadata_payload = metadata
			defer delete(metadata)
		}
		metadata_error := workflow_isolate.write(os.stdout, {
			kind = .Completion_Metadata,
			request_id = response_request_id,
			run_id = request.frame.run_id,
			payload = metadata_payload,
		})
		if metadata_error.kind != .None {
			workflow_isolate.owned_frame_free(&request)
			return 1
		}
		if fault_mode == .Malformed_Completion || fault_mode == .Metadata_Mismatch ||
		   fault_mode == .Late {
			completion_payload := []u8{0xff}
			failure := workflow.Process_Result{}
			if fault_mode == .Metadata_Mismatch || fault_mode == .Late {
				failure = workflow.activation_failure_result(
					string(request.frame.run_id),
					"fault-injected executor completion",
				)
				completion_payload = failure.completion[:]
			}
			completion_error := workflow_isolate.write(os.stdout, {
				kind = .Completion,
				request_id = response_request_id,
				run_id = request.frame.run_id,
				payload = completion_payload,
			})
			delete(failure.completion)
			workflow.error_free(&failure.error)
			if completion_error.kind != .None {
				workflow_isolate.owned_frame_free(&request)
				return 1
			}
		}
		workflow_isolate.owned_frame_free(&request)
		for {
			time.sleep(time.Hour)
		}
	}
	run_error := workflow_isolate.executor_host_run(&host)
	if run_error.message != nil {
		fmt.eprintf("Workflow executor host failed: %s\n", string(run_error.message))
		workflow_isolate.host_error_free(&run_error)
		return 1
	}
	return 0
}

Completion_Kind :: enum {
	Invalid,
	Timer_Started,
	Workflow_Completed,
}

inspect_completion :: proc(data: []u8) -> (kind: Completion_Kind, result: string) {
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
	if !decoded || completion.run_id != RUN_ID {
		return
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil || len(status.value.commands) != 1 {
			return
		}
		#partial switch variant in status.value.commands[0].variant {
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Timer:
			if variant.value != nil && variant.value.seq == 1 {
				kind = .Timer_Started
			}
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution:
			if variant.value == nil || variant.value.result == nil {
				return
			}
			metadata := make(
				[]codec.Metadata_Entry,
				len(variant.value.result.metadata),
			)
			for entry, index in variant.value.result.metadata {
				metadata[index] = {key = entry.key, value = entry.value}
			}
			payload_view := codec.Payload_View{
				metadata = metadata,
				data = variant.value.result.data,
			}
			decode_error := codec.decode_json(payload_view, &result)
			delete(metadata)
			if decode_error == .None {
				kind = .Workflow_Completed
			}
		}
	}
	return
}

exchange :: proc(
	input: ^os.File,
	output: ^os.File,
	request_id: u64,
	payload: []u8,
) -> (response: workflow_isolate.Owned_Frame, ok: bool) {
	write_error := workflow_isolate.write(input, {
		kind = .Activation,
		request_id = request_id,
		run_id = transmute([]u8)string(RUN_ID),
		payload = payload,
	})
	if write_error.kind != .None {
		fmt.eprintf("Could not send activation %d: %v\n", request_id, write_error.kind)
		return
	}
	metadata_frame: workflow_isolate.Owned_Frame
	metadata_read_error: workflow_isolate.Error
	for {
		metadata_frame, metadata_read_error = workflow_isolate.read(output)
		if metadata_read_error.kind != .None || metadata_frame.frame.kind != .Diagnostic {
			break
		}
		_, _, diagnostic_ok := workflow_isolate.diagnostic_decode(
			metadata_frame.frame.payload,
		)
		if !diagnostic_ok || metadata_frame.frame.request_id != request_id ||
		   !bytes.equal(metadata_frame.frame.run_id, transmute([]u8)string(RUN_ID)) {
			workflow_isolate.owned_frame_free(&metadata_frame)
			fmt.eprintf("Executor returned a malformed diagnostic for activation %d\n", request_id)
			return {}, false
		}
		workflow_isolate.owned_frame_free(&metadata_frame)
	}
	defer workflow_isolate.owned_frame_free(&metadata_frame)
	if metadata_read_error.kind != .None {
		fmt.eprintf("Could not read completion metadata %d: %v\n", request_id, metadata_read_error.kind)
		return
	}
	if metadata_frame.frame.kind == .Executor_Error {
		fmt.eprintf("Executor rejected activation %d: %s\n", request_id, string(metadata_frame.frame.payload))
		return {}, false
	}
	if metadata_frame.frame.kind != .Completion_Metadata ||
	   metadata_frame.frame.request_id != request_id ||
	   !bytes.equal(metadata_frame.frame.run_id, transmute([]u8)string(RUN_ID)) {
		fmt.eprintf("Executor returned mismatched completion metadata for activation %d\n", request_id)
		return {}, false
	}
	metadata, metadata_ok := workflow_isolate.process_result_metadata_decode(
		metadata_frame.frame.payload,
	)
	if !metadata_ok {
		fmt.eprintf("Executor returned malformed completion metadata for activation %d\n", request_id)
		return {}, false
	}
	workflow_isolate.process_result_metadata_free(&metadata)
	read_error: workflow_isolate.Error
	response, read_error = workflow_isolate.read(output)
	if read_error.kind != .None {
		fmt.eprintf("Could not read completion %d: %v\n", request_id, read_error.kind)
		return
	}
	if response.frame.kind == .Executor_Error {
		fmt.eprintf("Executor rejected activation %d: %s\n", request_id, string(response.frame.payload))
		return response, false
	}
	if response.frame.kind != .Completion || response.frame.request_id != request_id ||
	   !bytes.equal(response.frame.run_id, transmute([]u8)string(RUN_ID)) {
		fmt.eprintf("Executor returned a mismatched completion for activation %d\n", request_id)
		return response, false
	}
	return response, true
}

run_parent :: proc() -> int {
	child_input, parent_input, input_pipe_error := os.pipe()
	if input_pipe_error != nil {
		fmt.eprintf("Could not create executor input pipe: %s\n", os.error_string(input_pipe_error))
		return 1
	}
	parent_output, child_output, output_pipe_error := os.pipe()
	if output_pipe_error != nil {
		os.close(child_input)
		os.close(parent_input)
		fmt.eprintf("Could not create executor output pipe: %s\n", os.error_string(output_pipe_error))
		return 1
	}

	command := [?]string{os.args[0], "--child"}
	process, start_error := os.process_start({
		command = command[:],
		stdin = child_input,
		stdout = child_output,
		stderr = os.stderr,
	})
	os.close(child_input)
	os.close(child_output)
	if start_error != nil {
		os.close(parent_input)
		os.close(parent_output)
		fmt.eprintf("Could not start Workflow executor child: %s\n", os.error_string(start_error))
		return 1
	}
	defer os.close(parent_input)
	defer os.close(parent_output)
	child_reaped := false
	defer if !child_reaped {
		_ = os.process_kill(process)
		_, _ = os.process_wait(process)
	}

	input, input_error := codec.encode_json("Odin")
	if input_error != .None {
		return 1
	}
	defer codec.payload_free(&input)
	proto_input := proto.encode_payload(codec.payload_view(&input))
	defer delete(proto_input.metadata)
	defer delete(proto_input.external_payloads)
	initialize := proto.Coresdk_Workflow_Activation_InitializeWorkflow{
		workflow_type = WORKFLOW_TYPE,
	}
	defer delete(initialize.arguments)
	append(&initialize.arguments, proto_input)
	initial_activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = RUN_ID,
		history_length = 3,
	}
	defer delete(initial_activation.jobs)
	append(&initial_activation.jobs, proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow{
			value = &initialize,
		},
	})
	initial_bytes := proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&initial_activation)
	defer delete(initial_bytes)
	initial_response, initial_ok := exchange(parent_input, parent_output, 1, initial_bytes[:])
	defer workflow_isolate.owned_frame_free(&initial_response)
	if !initial_ok {
		return 1
	}
	initial_kind, _ := inspect_completion(initial_response.frame.payload)
	if initial_kind != .Timer_Started {
		fmt.eprintln("Isolated Workflow did not suspend on its timer")
		return 1
	}

	fire_timer := proto.Coresdk_Workflow_Activation_FireTimer{seq = 1}
	resume_activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{
		run_id = RUN_ID,
		history_length = 7,
	}
	defer delete(resume_activation.jobs)
	append(&resume_activation.jobs, proto.Coresdk_Workflow_Activation_WorkflowActivationJob{
		variant = proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer{
			value = &fire_timer,
		},
	})
	resume_bytes := proto.encode_Coresdk_Workflow_Activation_WorkflowActivation(&resume_activation)
	defer delete(resume_bytes)
	resume_response, resume_ok := exchange(parent_input, parent_output, 2, resume_bytes[:])
	defer workflow_isolate.owned_frame_free(&resume_response)
	if !resume_ok {
		return 1
	}
	resume_kind, result := inspect_completion(resume_response.frame.payload)
	defer delete(result)
	if resume_kind != .Workflow_Completed ||
	   result != "Hello from an isolated Workflow, Odin" {
		fmt.eprintf("Isolated Workflow returned an unexpected result: %s\n", result)
		return 1
	}

	shutdown_error := workflow_isolate.write(parent_input, {
		kind = .Shutdown,
		request_id = 3,
	})
	if shutdown_error.kind != .None {
		return 1
	}
	shutdown_response, shutdown_read_error := workflow_isolate.read(parent_output)
	defer workflow_isolate.owned_frame_free(&shutdown_response)
	if shutdown_read_error.kind != .None ||
	   shutdown_response.frame.kind != .Shutdown_Acknowledged ||
	   shutdown_response.frame.request_id != 3 {
		return 1
	}
	state, wait_error := os.process_wait(process)
	child_reaped = true
	if wait_error != nil || !state.success {
		fmt.eprintln("Workflow executor child did not exit cleanly")
		return 1
	}

	fmt.printf(
		"Workflow process isolation proved two correlated activations, suspension, resumption, and completion for run %s.\n",
		RUN_ID,
	)
	return 0
}

main :: proc() {
	if len(os.args) >= 2 && os.args[1] == "--child" {
		task_queue := ""
		if len(os.args) >= 3 {
			task_queue = os.args[2]
		}
		os.exit(run_child(.None, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-crash" {
		task_queue := ""
		if len(os.args) >= 3 {
			task_queue = os.args[2]
		}
		os.exit(run_child(.Crash, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-hang" {
		task_queue := ""
		if len(os.args) >= 3 {
			task_queue = os.args[2]
		}
		os.exit(run_child(.Hang, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-late" {
		task_queue := len(os.args) >= 3 ? os.args[2] : ""
		os.exit(run_child(.Late, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-stale" {
		task_queue := len(os.args) >= 3 ? os.args[2] : ""
		os.exit(run_child(.Stale_Request, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-malformed-metadata" {
		task_queue := len(os.args) >= 3 ? os.args[2] : ""
		os.exit(run_child(.Malformed_Metadata, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-malformed-completion" {
		task_queue := len(os.args) >= 3 ? os.args[2] : ""
		os.exit(run_child(.Malformed_Completion, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-metadata-mismatch" {
		task_queue := len(os.args) >= 3 ? os.args[2] : ""
		os.exit(run_child(.Metadata_Mismatch, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-nondeterministic" {
		task_queue := len(os.args) >= 3 ? os.args[2] : ""
		os.exit(run_child(.Nondeterministic, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-stack-overflow" {
		task_queue := len(os.args) >= 3 ? os.args[2] : ""
		os.exit(run_child(.Stack_Overflow, task_queue))
	}
	if len(os.args) >= 2 && os.args[1] == "--child-panic" {
		task_queue := len(os.args) >= 3 ? os.args[2] : ""
		os.exit(run_child(.Panic, task_queue))
	}
	os.exit(run_parent())
}

package main

import "core:bytes"
import "core:fmt"
import "core:mem/virtual"
import "core:os"
import "core:strings"
import "core:time"
import temporal_client "../../temporal/client"
import proto "../../temporal/core/proto"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"
import workflow_isolate "../../temporal/workflow/isolate"

WORKFLOW_TYPE :: "odin_isolated_workflow_spike"
DEFAULT_TASK_QUEUE :: "odin-isolated-workflow-core"

// Core requires a registered Workflow type before polling. Online activations
// are deliberately routed to the external processor below, so this procedure
// must never execute in the parent process.
parent_registration_only :: proc(_: ^workflow.Fiber_Context, _: string) -> string {
	return "parent Workflow dispatcher was unexpectedly invoked"
}

Child_Session :: struct {
	process:    os.Process,
	input:      ^os.File,
	output:     ^os.File,
	run_id:     string,
	request_id: u64,
	active:     bool,
}

Processor_State :: struct {
	child_command:    string,
	task_queue:       string,
	session:          Child_Session,
	report_run_id:    string,
	generations:      int,
	crashes:          int,
	evictions:        int,
	crash_first_child: bool,
}

processor_error :: proc(run_id, message: string) -> workflow.Process_Result {
	return workflow.activation_failure_result(run_id, message)
}

session_abort :: proc(session: ^Child_Session) {
	if session == nil || !session.active {
		return
	}
	os.close(session.input)
	os.close(session.output)
	state, _ := os.process_wait(session.process, 0)
	if !state.exited {
		_ = os.process_kill(session.process)
		_, _ = os.process_wait(session.process)
	}
	delete(session.run_id)
	session^ = {}
}

session_shutdown :: proc(session: ^Child_Session) -> bool {
	if session == nil || !session.active {
		return true
	}
	session.request_id += 1
	write_error := workflow_isolate.write(session.input, {
		kind = .Shutdown,
		request_id = session.request_id,
	})
	if write_error.kind != .None {
		session_abort(session)
		return false
	}
	response, read_error := workflow_isolate.read(session.output)
	defer workflow_isolate.owned_frame_free(&response)
	if read_error.kind != .None ||
	   response.frame.kind != .Shutdown_Acknowledged ||
	   response.frame.request_id != session.request_id {
		session_abort(session)
		return false
	}
	os.close(session.input)
	os.close(session.output)
	process_state, wait_error := os.process_wait(session.process)
	ok := wait_error == nil && process_state.success
	delete(session.run_id)
	session^ = {}
	return ok
}

session_start :: proc(state: ^Processor_State, run_id: string) -> bool {
	child_input, parent_input, input_error := os.pipe()
	if input_error != nil {
		return false
	}
	parent_output, child_output, output_error := os.pipe()
	if output_error != nil {
		os.close(child_input)
		os.close(parent_input)
		return false
	}
	state.generations += 1
	child_mode := "--child"
	if state.crash_first_child && state.generations == 1 {
		child_mode = "--child-crash"
	}
	command := [?]string{state.child_command, child_mode, state.task_queue}
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
		return false
	}
	owned_run_id, clone_error := strings.clone(run_id)
	if clone_error != nil {
		os.close(parent_input)
		os.close(parent_output)
		_ = os.process_kill(process)
		_, _ = os.process_wait(process)
		return false
	}
	state.session = {
		process = process,
		input = parent_input,
		output = parent_output,
		run_id = owned_run_id,
		active = true,
	}
	fmt.eprintf("isolate: started generation=%d run_id=%s mode=%s\n", state.generations, run_id, child_mode)
	return true
}

session_exchange :: proc(
	session: ^Child_Session,
	activation: []u8,
) -> (completion: [dynamic]u8, ok: bool) {
	session.request_id += 1
	request_id := session.request_id
	write_error := workflow_isolate.write(session.input, {
		kind = .Activation,
		request_id = request_id,
		run_id = transmute([]u8)session.run_id,
		payload = activation,
	})
	if write_error.kind != .None {
		return
	}
	response, read_error := workflow_isolate.read(session.output)
	defer workflow_isolate.owned_frame_free(&response)
	if read_error.kind != .None ||
	   response.frame.kind != .Completion ||
	   response.frame.request_id != request_id ||
	   !bytes.equal(response.frame.run_id, transmute([]u8)session.run_id) {
		return
	}
	completion = make([dynamic]u8, 0, len(response.frame.payload))
	append(&completion, ..response.frame.payload)
	return completion, true
}

inspect_completion :: proc(data: []u8, result: ^workflow.Process_Result) -> bool {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return false
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
		return false
	}
	#partial switch status in completion.status {
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Failed:
		result.activation_failed = true
	case proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful:
		if status.value == nil {
			return false
		}
		for command in status.value.commands {
			#partial switch variant in command.variant {
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Timer:
				result.started_timer = true
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution:
				result.completed = true
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Fail_Workflow_Execution:
				result.completed = true
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Workflow_Execution:
				result.completed = true
				result.canceled_workflow = true
			case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Continue_As_New_Workflow_Execution:
				result.completed = true
				result.continued_as_new = true
			}
		}
	}
	return true
}

process_activation :: proc(user_data: rawptr, activation: []u8) -> workflow.Process_Result {
	state := cast(^Processor_State)user_data
	route, route_ok := worker.decode_workflow_activation_route(activation)
	if state == nil || !route_ok {
		return processor_error("unknown", "isolated processor received a malformed activation")
	}
	delete(state.report_run_id)
	state.report_run_id, _ = strings.clone(route.run_id)

	if route.evicts {
		state.evictions += 1
		result := workflow.Process_Result{
			completion = workflow.encode_empty_activation_success(route.run_id),
			run_id = state.report_run_id,
			evicted = true,
		}
		if state.session.active {
			completion, ok := session_exchange(&state.session, activation)
			if ok {
				delete(result.completion)
				result.completion = completion
			} else {
				delete(completion)
			}
			_ = session_shutdown(&state.session)
		}
		fmt.eprintf("isolate: eviction run_id=%s generation=%d\n", route.run_id, state.generations)
		return result
	}

	if !state.session.active {
		if !route.initializes || !session_start(state, route.run_id) {
			return processor_error(route.run_id, "could not start isolated Workflow executor")
		}
	} else if state.session.run_id != route.run_id {
		return processor_error(route.run_id, "isolated Workflow executor route collision")
	}

	completion, ok := session_exchange(&state.session, activation)
	if !ok {
		state.crashes += 1
		generation := state.generations
		session_abort(&state.session)
		fmt.eprintf("isolate: generation=%d exited before completion; reporting activation failure\n", generation)
		result := processor_error(route.run_id, "isolated Workflow executor exited before completing activation")
		result.run_id = state.report_run_id
		return result
	}
	result := workflow.Process_Result{
		completion = completion,
		run_id = state.report_run_id,
	}
	if !inspect_completion(completion[:], &result) {
		delete(result.completion)
		result = processor_error(route.run_id, "isolated Workflow executor returned a malformed completion")
		result.run_id = state.report_run_id
		return result
	}
	result.is_replaying = route.is_replaying
	result.history_length = route.history_length
	return result
}

Start_State :: struct {
	client:      ^temporal_client.Client,
	workflow_id: string,
	task_queue:  string,
	handle:      temporal_client.Workflow_Handle,
	error:       temporal_client.Error,
}

start_when_ready :: proc(user_data: rawptr) -> bool {
	state := cast(^Start_State)user_data
	state.handle, state.error = temporal_client.start_workflow_json(
		state.client,
		WORKFLOW_TYPE,
		"Odin",
		{
			id = state.workflow_id,
			task_queue = state.task_queue,
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	return state.error.message == nil
}

run :: proc() -> int {
	child_buffer: [1024]u8
	child_command := os.get_env(child_buffer[:], "TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND")
	if len(child_command) == 0 {
		fmt.eprintln("TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND must name the workflow_isolate executable")
		return 1
	}
	task_queue_buffer: [256]u8
	task_queue := os.get_env(task_queue_buffer[:], "TEMPORAL_ODIN_TASK_QUEUE")
	if len(task_queue) == 0 {
		task_queue = DEFAULT_TASK_QUEUE
	}
	id_buffer: [256]u8
	workflow_id := os.get_env(id_buffer[:], "TEMPORAL_ODIN_WORKFLOW_ID")
	owned_workflow_id := ""
	if len(workflow_id) == 0 {
		owned_workflow_id = fmt.aprintf("odin-isolate-%d", time.now()._nsec)
		defer delete(owned_workflow_id)
		workflow_id = owned_workflow_id
	}

	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	worker_options := worker.options_default(task_queue)
	worker_options.max_cached_workflows = 0
	worker_options.workflow_task_poller_behavior = worker.poller_simple_maximum(1)
	sdk_worker, create_error := worker.create_with_options(
		temporal_client.core_client(&client),
		worker_options,
	)
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		return 1
	}
	defer {
		close_error := worker.close(&sdk_worker)
		worker.error_free(&close_error)
	}
	register_error := worker.register_workflow(&sdk_worker, WORKFLOW_TYPE, parent_registration_only)
	if register_error.message != nil {
		fmt.eprintf("Workflow registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}

	processor := Processor_State{
		child_command = child_command,
		task_queue = task_queue,
		crash_first_child = true,
	}
	defer {
		session_abort(&processor.session)
		delete(processor.report_run_id)
	}
	start := Start_State{
		client = &client,
		workflow_id = workflow_id,
		task_queue = task_queue,
	}
	run_stats, run_error := worker.run(&sdk_worker, {
		max_workflow_executions = 1,
		workflow_activation_processor = process_activation,
		workflow_activation_processor_data = &processor,
		on_ready = start_when_ready,
		on_ready_data = &start,
	})
	if start.error.message != nil {
		fmt.eprintf("Workflow start failed: %s\n", string(start.error.message))
		temporal_client.error_free(&start.error)
		worker.error_free(&run_error)
		return 1
	}
	defer temporal_client.workflow_handle_free(&start.handle)
	if run_error.message != nil {
		fmt.eprintf("Worker run failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		return 1
	}

	result: string
	result_error := temporal_client.get_result_json(&start.handle, &result)
	defer delete(result)
	if result_error.message != nil {
		fmt.eprintf("Workflow result failed: %s\n", string(result_error.message))
		temporal_client.error_free(&result_error)
		return 1
	}
	if result != "Hello from an isolated Workflow, Odin" ||
	   run_stats.workflow_executions_completed != 1 ||
	   run_stats.workflow_activation_failures != 1 ||
	   processor.crashes != 1 ||
	   processor.generations < 2 ||
	   processor.session.active {
		fmt.eprintf(
			"Unexpected isolation result=%q completed=%d failures=%d crashes=%d generations=%d evictions=%d active=%v\n",
			result,
			run_stats.workflow_executions_completed,
			run_stats.workflow_activation_failures,
			processor.crashes,
			processor.generations,
			processor.evictions,
			processor.session.active,
		)
		return 1
	}
	fmt.printf(
		"Core process isolation completed workflow_id=%s result=%q generations=%d crashes=%d evictions=%d replay_activations=%d\n",
		workflow_id,
		result,
		processor.generations,
		processor.crashes,
		processor.evictions,
		run_stats.workflow_replay_activations,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

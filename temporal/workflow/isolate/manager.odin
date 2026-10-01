package workflow_isolate

import "core:bytes"
import "core:mem/virtual"
import "core:os"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"
import proto "../../core/proto"
import workflow ".."

Manager_Error :: struct {
	message: []u8,
}

manager_error_free :: proc(error: ^Manager_Error) {
	if error == nil {
		return
	}
	delete(error.message)
	error^ = {}
}

manager_error :: proc(message: string) -> Manager_Error {
	return {message = bytes.clone(transmute([]u8)message)}
}

Lifecycle_State :: enum {
	Absent,
	Live,
	Failed_Awaiting_Eviction,
}

Lifecycle_Event :: enum {
	Initialize,
	Activation_Succeeded,
	Activation_Failed,
	Remove_From_Cache,
}

Lifecycle_Effect :: enum {
	Reject,
	Start_Executor,
	Complete_Activation,
	Fail_And_Abort,
	Fail_Awaiting_Eviction,
	Complete_Eviction_And_Shutdown,
	Acknowledge_Eviction,
}

lifecycle_step :: proc(
	state: Lifecycle_State,
	event: Lifecycle_Event,
) -> (next: Lifecycle_State, effect: Lifecycle_Effect) {
	next = state
	switch state {
	case .Absent:
		switch event {
		case .Initialize:
			return .Live, .Start_Executor
		case .Remove_From_Cache:
			return .Absent, .Acknowledge_Eviction
		case .Activation_Succeeded, .Activation_Failed:
			return .Absent, .Reject
		}
	case .Live:
		switch event {
		case .Initialize, .Activation_Failed:
			return .Failed_Awaiting_Eviction, .Fail_And_Abort
		case .Activation_Succeeded:
			return .Live, .Complete_Activation
		case .Remove_From_Cache:
			return .Absent, .Complete_Eviction_And_Shutdown
		}
	case .Failed_Awaiting_Eviction:
		if event == .Remove_From_Cache {
			return .Absent, .Acknowledge_Eviction
		}
		return .Failed_Awaiting_Eviction, .Fail_Awaiting_Eviction
	}
	return .Absent, .Reject
}

Activation_Route :: struct {
	run_id:         string,
	workflow_type:  string,
	initializes:    bool,
	evicts:         bool,
	is_replaying:   bool,
	history_length: u32,
	job_count:      int,
}

decode_activation_route :: proc(data: []u8) -> (route: Activation_Route, ok: bool) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{}
	ok = proto.decode_Coresdk_Workflow_Activation_WorkflowActivation(data, &activation)
	context.allocator = previous_allocator
	if !ok || len(activation.run_id) == 0 {
		ok = false
		return
	}
	route.run_id = activation.run_id
	route.is_replaying = activation.is_replaying
	route.history_length = activation.history_length
	route.job_count = len(activation.jobs)
	for job in activation.jobs {
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow:
			if variant.value == nil || route.initializes {
				ok = false
				return
			}
			route.initializes = true
			route.workflow_type = variant.value.workflow_type
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache:
			route.evicts = variant.value != nil
		}
	}
	return
}

Manager_Options :: struct {
	// command is copied by manager_init and executed for ordinary generations.
	command: []string,
	// first_command is an optional copied fault-injection/bootstrapping command
	// used for the first generation only. Empty means command.
	first_command: []string,
	// expected_registry is borrowed only by manager_init. When present, every
	// child must return the same canonical type manifest before activation.
	expected_registry: ^workflow.Registry,
	maximum_executors: int,
	// Zero waits indefinitely. A positive deadline kills the child, waits for
	// the pipe reader to observe EOF, and reports one activation failure.
	activation_timeout: time.Duration,
	// Zero waits indefinitely. A positive deadline bounds both the shutdown
	// acknowledgement and process exit; expiry kills and reaps the child.
	shutdown_timeout: time.Duration,
	crash_loop: Crash_Loop_Policy,
}

Crash_Loop_Policy :: struct {
	// Zero disables the breaker. After this many consecutive executor/session
	// failures, new children are rejected until cooldown has elapsed.
	maximum_consecutive_failures: int,
	cooldown:                     time.Duration,
}

manager_options_default :: proc(command: []string) -> Manager_Options {
	return {
		command = command,
		maximum_executors = 1,
	}
}

Manager_Stats :: struct {
	generations_started: int,
	child_exits:         int,
	protocol_failures:   int,
	activation_timeouts: int,
	capacity_rejections: int,
	evictions:           int,
	graceful_shutdowns:  int,
	aborted_children:    int,
	peak_routes:         int,
	manifest_handshakes: int,
	manifest_mismatches: int,
	shutdown_timeouts:   int,
	start_failures:      int,
	crash_loop_rejections: int,
	crash_loop_recoveries: int,
	incremental_activations: int,
}

Child_Session :: struct {
	process:    os.Process,
	input:      ^os.File,
	output:     ^os.File,
	request_id: u64,
	active:     bool,
}

Executor_Route :: struct {
	run_id:     string,
	generation: u64,
	state:      Lifecycle_State,
	session:    Child_Session,
}

Manager :: struct {
	command:            [dynamic]string,
	first_command:      [dynamic]string,
	maximum_executors:  int,
	activation_timeout: time.Duration,
	shutdown_timeout:   time.Duration,
	crash_loop:         Crash_Loop_Policy,
	expected_manifest:  []u8,
	routes:             [dynamic]Executor_Route,
	next_generation:    u64,
	first_command_used: bool,
	manifest_verified:  bool,
	consecutive_failures: int,
	last_failure:         time.Tick,
	report_run_id:      string,
	report_cancellation_reason: string,
	stats:              Manager_Stats,
	initialized:        bool,
}

process_start_mutex: sync.Mutex

clone_command :: proc(input: []string) -> (output: [dynamic]string, ok: bool) {
	if len(input) == 0 || len(input[0]) == 0 {
		return nil, false
	}
	output = make([dynamic]string, 0, len(input))
	for argument in input {
		owned, clone_error := strings.clone(argument)
		if clone_error != nil {
			for item in output {
				delete(item)
			}
			delete(output)
			return nil, false
		}
		append(&output, owned)
	}
	return output, true
}

free_command :: proc(command: ^[dynamic]string) {
	if command == nil {
		return
	}
	for argument in command^ {
		delete(argument)
	}
	delete(command^)
	command^ = nil
}

manager_init :: proc(manager: ^Manager, options: Manager_Options) -> Manager_Error {
	if manager == nil || manager.initialized {
		return manager_error("Workflow executor manager is already initialized or missing")
	}
	if options.maximum_executors <= 0 {
		return manager_error("Workflow executor manager capacity must be positive")
	}
	if options.activation_timeout < 0 || options.shutdown_timeout < 0 {
		return manager_error("Workflow executor timeouts must not be negative")
	}
	if options.crash_loop.maximum_consecutive_failures < 0 ||
	   options.crash_loop.cooldown < 0 ||
	   (options.crash_loop.maximum_consecutive_failures > 0 &&
	    options.crash_loop.cooldown <= 0) {
		return manager_error("Workflow executor crash-loop policy is invalid")
	}
	command, command_ok := clone_command(options.command)
	if !command_ok {
		return manager_error("Workflow executor command is missing or could not be copied")
	}
	first_command: [dynamic]string
	if len(options.first_command) > 0 {
		first_command, command_ok = clone_command(options.first_command)
		if !command_ok {
			free_command(&command)
			return manager_error("first Workflow executor command could not be copied")
		}
	}
	manager.command = command
	manager.first_command = first_command
	if options.expected_registry != nil {
		manager.expected_manifest, command_ok = registry_manifest_encode(
			options.expected_registry,
		)
		if !command_ok {
			free_command(&manager.command)
			free_command(&manager.first_command)
			return manager_error("Workflow executor registry manifest could not be encoded")
		}
	}
	manager.maximum_executors = options.maximum_executors
	manager.activation_timeout = options.activation_timeout
	manager.shutdown_timeout = options.shutdown_timeout
	manager.crash_loop = options.crash_loop
	manager.routes = make([dynamic]Executor_Route, 0, options.maximum_executors)
	manager.initialized = true
	return {}
}

session_abort :: proc(manager: ^Manager, session: ^Child_Session) {
	if session == nil || !session.active {
		return
	}
	if session.input != nil {
		os.close(session.input)
		session.input = nil
	}
	if session.output != nil {
		os.close(session.output)
		session.output = nil
	}
	process_state, _ := os.process_wait(session.process, 0)
	if !process_state.exited {
		_ = os.process_kill(session.process)
		_, _ = os.process_wait(session.process)
	}
	manager.stats.aborted_children += 1
	session^ = {}
}

session_shutdown :: proc(manager: ^Manager, session: ^Child_Session) -> bool {
	if session == nil || !session.active {
		return true
	}
	session.request_id += 1
	write_error := write(session.input, {
		kind = .Shutdown,
		request_id = session.request_id,
	})
	if write_error.kind != .None {
		failure := Exchange_Failure.Protocol
		if write_error.kind == .IO || write_error.kind == .Truncated {
			failure = .Child_Exit
		}
		record_session_failure(manager, session, failure)
		return false
	}
	response, read_error, read_failure := read_with_deadline(
		session,
		manager.shutdown_timeout,
	)
	defer owned_frame_free(&response)
	if read_failure == .Timed_Out {
		manager.stats.shutdown_timeouts += 1
		manager_record_failure(manager)
		session_abort(manager, session)
		return false
	}
	if read_failure != .None || read_error.kind != .None ||
	   response.frame.kind != .Shutdown_Acknowledged ||
	   response.frame.request_id != session.request_id {
		failure := Exchange_Failure.Protocol
		if read_failure == .Child_Exit || read_error.kind == .End_Of_Stream ||
		   read_error.kind == .IO {
			failure = .Child_Exit
		}
		record_session_failure(manager, session, failure)
		return false
	}
	os.close(session.input)
	session.input = nil
	os.close(session.output)
	session.output = nil
	process_state: os.Process_State
	wait_error: os.Error
	if manager.shutdown_timeout > 0 {
		process_state, wait_error = os.process_wait(
			session.process,
			manager.shutdown_timeout,
		)
	} else {
		process_state, wait_error = os.process_wait(session.process)
	}
	if manager.shutdown_timeout > 0 && !process_state.exited {
		manager.stats.shutdown_timeouts += 1
		manager_record_failure(manager)
		_ = os.process_kill(session.process)
		_, _ = os.process_wait(session.process)
		manager.stats.aborted_children += 1
		session^ = {}
		return false
	}
	if wait_error != nil || !process_state.success {
		if wait_error != nil {
			manager.stats.protocol_failures += 1
		} else {
			manager.stats.child_exits += 1
		}
		manager_record_failure(manager)
		session^ = {}
		return false
	}
	manager.stats.graceful_shutdowns += 1
	session^ = {}
	return true
}

manager_close :: proc(manager: ^Manager) {
	if manager == nil {
		return
	}
	for &route in manager.routes {
		session_abort(manager, &route.session)
		delete(route.run_id)
	}
	delete(manager.routes)
	free_command(&manager.command)
	free_command(&manager.first_command)
	delete(manager.expected_manifest)
	delete(manager.report_run_id)
	delete(manager.report_cancellation_reason)
	manager^ = {}
}

find_route :: proc(manager: ^Manager, run_id: string) -> (index: int, found: bool) {
	for route, route_index in manager.routes {
		if route.run_id == run_id {
			return route_index, true
		}
	}
	return
}

capacity_allows_start :: proc(route_count, maximum_executors: int) -> bool {
	return route_count >= 0 && maximum_executors > 0 && route_count < maximum_executors
}

// crash_loop_allows_start is pure so generated tests can check the breaker
// independently of the process clock. reset is true for the first trial after
// cooldown.
crash_loop_allows_start :: proc(
	consecutive_failures: int,
	elapsed_since_failure: time.Duration,
	policy: Crash_Loop_Policy,
) -> (allowed, reset: bool) {
	if policy.maximum_consecutive_failures == 0 ||
	   consecutive_failures < policy.maximum_consecutive_failures {
		return true, false
	}
	if elapsed_since_failure >= policy.cooldown {
		return true, true
	}
	return false, false
}

manager_allows_start :: proc(manager: ^Manager) -> bool {
	if manager.crash_loop.maximum_consecutive_failures == 0 {
		return true
	}
	elapsed := time.Duration(0)
	if manager.last_failure._nsec != 0 {
		elapsed = time.tick_diff(manager.last_failure, time.tick_now())
	}
	allowed, reset := crash_loop_allows_start(
		manager.consecutive_failures,
		elapsed,
		manager.crash_loop,
	)
	if !allowed {
		manager.stats.crash_loop_rejections += 1
		return false
	}
	if reset {
		manager.consecutive_failures = 0
		manager.last_failure = {}
		manager.stats.crash_loop_recoveries += 1
	}
	return true
}

manager_record_success :: proc(manager: ^Manager) {
	manager.consecutive_failures = 0
	manager.last_failure = {}
}

manager_record_failure :: proc(manager: ^Manager) {
	manager.consecutive_failures += 1
	manager.last_failure = time.tick_now()
}

remove_route :: proc(manager: ^Manager, index: int) {
	delete(manager.routes[index].run_id)
	ordered_remove(&manager.routes, index)
}

Route_Start_Failure :: enum {
	None,
	Capacity,
	Crash_Loop,
	Process,
}

start_session :: proc(command: []string) -> (session: Child_Session, ok: bool) {
	child_input, parent_input, input_error := os.pipe()
	if input_error != nil {
		return
	}
	parent_output, child_output, output_error := os.pipe()
	if output_error != nil {
		os.close(child_input)
		os.close(parent_input)
		return
	}
	sync.mutex_lock(&process_start_mutex)
	process, start_error := os.process_start({
		command = command,
		stdin = child_input,
		stdout = child_output,
		stderr = os.stderr,
	})
	sync.mutex_unlock(&process_start_mutex)
	os.close(child_input)
	os.close(child_output)
	if start_error != nil {
		os.close(parent_input)
		os.close(parent_output)
		return
	}
	return Child_Session{
		process = process,
		input = parent_input,
		output = parent_output,
		active = true,
	}, true
}

start_route :: proc(
	manager: ^Manager,
	run_id: string,
) -> (index: int, failure: Route_Start_Failure) {
	if !capacity_allows_start(len(manager.routes), manager.maximum_executors) {
		manager.stats.capacity_rejections += 1
		return 0, .Capacity
	}
	if !manager_allows_start(manager) {
		return 0, .Crash_Loop
	}
	command := manager.command[:]
	if !manager.first_command_used && len(manager.first_command) > 0 {
		command = manager.first_command[:]
	}
	manager.first_command_used = true
	session, session_ok := start_session(command)
	if !session_ok {
		manager.stats.start_failures += 1
		manager_record_failure(manager)
		return 0, .Process
	}
	if len(manager.expected_manifest) > 0 {
		handshake_failure := session_manifest_handshake(manager, &session)
		if handshake_failure != .None {
			record_session_failure(manager, &session, handshake_failure)
			return 0, .Process
		}
	}
	owned_run_id, clone_error := strings.clone(run_id)
	if clone_error != nil {
		session_abort(manager, &session)
		return 0, .Process
	}
	manager.next_generation += 1
	next_state, effect := lifecycle_step(.Absent, .Initialize)
	assert(effect == .Start_Executor)
	append(&manager.routes, Executor_Route{
		run_id = owned_run_id,
		generation = manager.next_generation,
		state = next_state,
		session = session,
	})
	manager.stats.generations_started += 1
	manager.stats.peak_routes = max(manager.stats.peak_routes, len(manager.routes))
	return len(manager.routes) - 1, .None
}

route_start_failure_message :: proc(failure: Route_Start_Failure) -> string {
	switch failure {
	case .Capacity:
		return "Workflow executor capacity is exhausted; activation was rejected without waiting"
	case .Crash_Loop:
		return "Workflow executor crash-loop breaker is open"
	case .Process:
		return "Workflow executor child could not start or complete its handshake"
	case .None:
		return ""
	}
	return "Workflow executor could not start"
}

Exchange_Failure :: enum {
	None,
	Child_Exit,
	Protocol,
	Timed_Out,
}

Exchange_Read :: struct {
	file:     ^os.File,
	response: Owned_Frame,
	error:    Error,
	done:     sync.Sema,
}

exchange_read_main :: proc(state: ^Exchange_Read) {
	if state == nil {
		return
	}
	state.response, state.error = read(state.file)
	sync.sema_post(&state.done)
}

read_with_deadline :: proc(
	session: ^Child_Session,
	deadline: time.Duration,
) -> (response: Owned_Frame, error: Error, failure: Exchange_Failure) {
	if deadline <= 0 {
		response, error = read(session.output)
		return
	}
	read_state := Exchange_Read{file = session.output}
	read_thread := thread.create_and_start_with_poly_data(&read_state, exchange_read_main)
	if read_thread == nil {
		return {}, {}, .Protocol
	}
	finished := sync.sema_wait_with_timeout(&read_state.done, deadline)
	if !finished {
		_ = os.process_kill(session.process)
	}
	thread.join(read_thread)
	thread.destroy(read_thread)
	if !finished {
		owned_frame_free(&read_state.response)
		return {}, {}, .Timed_Out
	}
	return read_state.response, read_state.error, .None
}

read_with_timeout :: proc(
	manager: ^Manager,
	session: ^Child_Session,
) -> (response: Owned_Frame, error: Error, failure: Exchange_Failure) {
	return read_with_deadline(session, manager.activation_timeout)
}

session_manifest_handshake :: proc(
	manager: ^Manager,
	session: ^Child_Session,
) -> Exchange_Failure {
	if len(manager.expected_manifest) == 0 {
		return .None
	}
	session.request_id += 1
	request_id := session.request_id
	write_error := write(session.input, {
		kind = .Manifest_Request,
		request_id = request_id,
	})
	if write_error.kind != .None {
		if write_error.kind == .IO || write_error.kind == .Truncated {
			return .Child_Exit
		}
		return .Protocol
	}
	response, read_error, read_failure := read_with_timeout(manager, session)
	defer owned_frame_free(&response)
	if read_failure != .None {
		return read_failure
	}
	if read_error.kind != .None {
		if read_error.kind == .End_Of_Stream || read_error.kind == .IO {
			return .Child_Exit
		}
		return .Protocol
	}
	if response.frame.kind != .Manifest ||
	   response.frame.request_id != request_id ||
	   !bytes.equal(response.frame.payload, manager.expected_manifest) {
		manager.stats.manifest_mismatches += 1
		return .Protocol
	}
	manager.stats.manifest_handshakes += 1
	return .None
}

// manager_verify_host starts the ordinary executor command, verifies its
// registry manifest, and shuts it down before the Worker begins polling.
manager_verify_host :: proc(manager: ^Manager) -> Manager_Error {
	if manager == nil || !manager.initialized || len(manager.expected_manifest) == 0 {
		return manager_error("Workflow executor manager has no expected registry manifest")
	}
	if manager.manifest_verified {
		return {}
	}
	session, start_ok := start_session(manager.command[:])
	if !start_ok {
		return manager_error("Workflow executor verification child could not start")
	}
	failure := session_manifest_handshake(manager, &session)
	if failure != .None {
		record_session_failure(manager, &session, failure)
		return manager_error(exchange_failure_message(failure))
	}
	if !session_shutdown(manager, &session) {
		return manager_error("Workflow executor verification child did not shut down cleanly")
	}
	manager.manifest_verified = true
	return {}
}

session_exchange :: proc(
	manager: ^Manager,
	route: ^Executor_Route,
	activation: []u8,
) -> (
	completion: [dynamic]u8,
	metadata: workflow.Process_Result,
	failure: Exchange_Failure,
) {
	route.session.request_id += 1
	request_id := route.session.request_id
	write_error := write(route.session.input, {
		kind = .Activation,
		request_id = request_id,
		run_id = transmute([]u8)route.run_id,
		payload = activation,
	})
	if write_error.kind != .None {
		if write_error.kind == .IO || write_error.kind == .Truncated {
			return nil, {}, .Child_Exit
		}
		return nil, {}, .Protocol
	}
	metadata_response, read_error, read_failure := read_with_timeout(manager, &route.session)
	if read_failure != .None {
		owned_frame_free(&metadata_response)
		return nil, {}, read_failure
	}
	if read_error.kind != .None {
		owned_frame_free(&metadata_response)
		if read_error.kind == .End_Of_Stream || read_error.kind == .IO {
			return nil, {}, .Child_Exit
		}
		return nil, {}, .Protocol
	}
	if metadata_response.frame.kind != .Completion_Metadata ||
	   metadata_response.frame.request_id != request_id ||
	   !bytes.equal(metadata_response.frame.run_id, transmute([]u8)route.run_id) {
		owned_frame_free(&metadata_response)
		return nil, {}, .Protocol
	}
	decoded_metadata, metadata_ok := process_result_metadata_decode(
		metadata_response.frame.payload,
	)
	owned_frame_free(&metadata_response)
	if !metadata_ok {
		return nil, {}, .Protocol
	}
	metadata = decoded_metadata
	response, completion_read_error, completion_read_failure := read_with_timeout(
		manager,
		&route.session,
	)
	defer owned_frame_free(&response)
	if completion_read_failure != .None {
		process_result_metadata_free(&metadata)
		return nil, {}, completion_read_failure
	}
	if completion_read_error.kind != .None {
		process_result_metadata_free(&metadata)
		if completion_read_error.kind == .End_Of_Stream ||
		   completion_read_error.kind == .IO {
			return nil, {}, .Child_Exit
		}
		return nil, {}, .Protocol
	}
	if response.frame.kind != .Completion ||
	   response.frame.request_id != request_id ||
	   !bytes.equal(response.frame.run_id, transmute([]u8)route.run_id) {
		process_result_metadata_free(&metadata)
		return nil, {}, .Protocol
	}
	completion = make([dynamic]u8, 0, len(response.frame.payload))
	append(&completion, ..response.frame.payload)
	return completion, metadata, .None
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

set_report_run_id :: proc(manager: ^Manager, run_id: string) -> bool {
	delete(manager.report_run_id)
	manager.report_run_id = ""
	delete(manager.report_cancellation_reason)
	manager.report_cancellation_reason = ""
	owned, clone_error := strings.clone(run_id)
	if clone_error != nil {
		return false
	}
	manager.report_run_id = owned
	return true
}

metadata_matches_completion :: proc(
	metadata: ^workflow.Process_Result,
	completion: []u8,
) -> bool {
	authoritative := workflow.Process_Result{}
	if !inspect_completion(completion, &authoritative) {
		return false
	}
	return metadata.activation_failed == authoritative.activation_failed &&
	       metadata.completed == authoritative.completed &&
	       metadata.canceled_workflow == authoritative.canceled_workflow &&
	       metadata.continued_as_new == authoritative.continued_as_new &&
	       metadata.started_timer == authoritative.started_timer
}

adopt_exchange_result :: proc(
	manager: ^Manager,
	metadata: workflow.Process_Result,
	completion: [dynamic]u8,
	route: Activation_Route,
) -> workflow.Process_Result {
	result := metadata
	result.completion = completion
	result.run_id = manager.report_run_id
	result.is_replaying = route.is_replaying
	result.history_length = route.history_length
	result.evicted = route.evicts
	manager.report_cancellation_reason = result.cancellation_reason
	return result
}

failure_result :: proc(manager: ^Manager, run_id, message: string) -> workflow.Process_Result {
	result := workflow.activation_failure_result(run_id, message)
	result.run_id = manager.report_run_id
	return result
}

record_exchange_failure :: proc(
	manager: ^Manager,
	route: ^Executor_Route,
	failure: Exchange_Failure,
) {
	record_session_failure(manager, &route.session, failure)
	route.state, _ = lifecycle_step(route.state, .Activation_Failed)
}

record_session_failure :: proc(
	manager: ^Manager,
	session: ^Child_Session,
	failure: Exchange_Failure,
) {
	if failure == .Child_Exit {
		manager.stats.child_exits += 1
	} else if failure == .Timed_Out {
		manager.stats.activation_timeouts += 1
	} else {
		manager.stats.protocol_failures += 1
	}
	manager_record_failure(manager)
	session_abort(manager, session)
}

exchange_failure_message :: proc(failure: Exchange_Failure) -> string {
	switch failure {
	case .Child_Exit:
		return "isolated Workflow executor exited before completing activation"
	case .Timed_Out:
		return "isolated Workflow executor exceeded its activation deadline"
	case .Protocol:
		return "isolated Workflow executor violated the process protocol"
	case .None:
		return ""
	}
	return "isolated Workflow executor failed"
}

manager_process_activation :: proc(
	user_data: rawptr,
	activation: []u8,
) -> workflow.Process_Result {
	manager := cast(^Manager)user_data
	route_info, route_ok := decode_activation_route(activation)
	if manager == nil || !manager.initialized || !route_ok {
		return workflow.activation_failure_result(
			"unknown",
			"Workflow executor manager received a malformed activation",
		)
	}
	_ = set_report_run_id(manager, route_info.run_id)
	route_index, route_found := find_route(manager, route_info.run_id)

	if route_info.evicts {
		manager.stats.evictions += 1
		result := workflow.Process_Result{
			completion = workflow.encode_empty_activation_success(route_info.run_id),
			run_id = manager.report_run_id,
			evicted = true,
			is_replaying = route_info.is_replaying,
			history_length = route_info.history_length,
		}
		if !route_found {
			return result
		}
		route := &manager.routes[route_index]
		if route.state == .Live {
			completion, metadata, exchange_failure := session_exchange(manager, route, activation)
			if exchange_failure == .None &&
			   metadata_matches_completion(&metadata, completion[:]) {
				manager_record_success(manager)
				delete(result.completion)
				result = adopt_exchange_result(manager, metadata, completion, route_info)
			} else {
				delete(completion)
				process_result_metadata_free(&metadata)
				if exchange_failure != .None {
					record_exchange_failure(manager, route, exchange_failure)
				} else {
					manager.stats.protocol_failures += 1
					manager_record_failure(manager)
					session_abort(manager, &route.session)
					route.state, _ = lifecycle_step(route.state, .Activation_Failed)
				}
			}
			if route.session.active {
				_ = session_shutdown(manager, &route.session)
			}
		}
		route.state, _ = lifecycle_step(route.state, .Remove_From_Cache)
		assert(route.state == .Absent)
		remove_route(manager, route_index)
		return result
	}

	if route_found {
		route := &manager.routes[route_index]
		if route.state == .Failed_Awaiting_Eviction {
			route.state, _ = lifecycle_step(route.state, .Activation_Failed)
			return failure_result(
				manager,
				route_info.run_id,
				"Workflow executor failed and is awaiting Core eviction",
			)
		}
		if route_info.initializes {
			route.state, _ = lifecycle_step(route.state, .Initialize)
			manager.stats.protocol_failures += 1
			session_abort(manager, &route.session)
			return failure_result(
				manager,
				route_info.run_id,
				"Workflow executor route was initialized more than once",
			)
		}
		manager.stats.incremental_activations += 1
	} else {
		if !route_info.initializes {
			return failure_result(
				manager,
				route_info.run_id,
				"Workflow activation has no live executor route",
			)
		}
		start_failure: Route_Start_Failure
		route_index, start_failure = start_route(manager, route_info.run_id)
		if start_failure != .None {
			return failure_result(
				manager,
				route_info.run_id,
				route_start_failure_message(start_failure),
			)
		}
	}

	route := &manager.routes[route_index]
	completion, metadata, exchange_failure := session_exchange(manager, route, activation)
	if exchange_failure != .None {
		process_result_metadata_free(&metadata)
		record_exchange_failure(manager, route, exchange_failure)
		return failure_result(
			manager,
			route_info.run_id,
			exchange_failure_message(exchange_failure),
		)
	}
	if !metadata_matches_completion(&metadata, completion[:]) {
		delete(completion)
		process_result_metadata_free(&metadata)
		manager.stats.protocol_failures += 1
		manager_record_failure(manager)
		session_abort(manager, &route.session)
		route.state, _ = lifecycle_step(route.state, .Activation_Failed)
		return failure_result(
			manager,
			route_info.run_id,
			"isolated Workflow executor returned a malformed completion",
		)
	}
	result := adopt_exchange_result(manager, metadata, completion, route_info)
	manager_record_success(manager)
	route.state, _ = lifecycle_step(route.state, .Activation_Succeeded)
	return result
}

manager_stats :: proc(manager: ^Manager) -> Manager_Stats {
	if manager == nil {
		return {}
	}
	return manager.stats
}

manager_route_count :: proc(manager: ^Manager) -> int {
	if manager == nil {
		return 0
	}
	return len(manager.routes)
}

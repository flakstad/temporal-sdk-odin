package activity_isolate

import "core:bytes"
import "core:mem/virtual"
import "core:os"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"
import activity ".."
import codec "../../codec"
import process_start "../../internal/process_start"
import proto "../../core/proto"
import worker "../../worker"

Manager_Error :: struct { message: []u8 }

manager_error :: proc(message: string) -> Manager_Error {
	return {message = bytes.clone(transmute([]u8)message)}
}

manager_error_free :: proc(error: ^Manager_Error) {
	if error == nil {
		return
	}
	delete(error.message)
	error^ = {}
}

Options :: struct {
	command:               []string,
	execution_timeout:     time.Duration,
	cancellation_grace:    time.Duration,
	maximum_payload_bytes: u32,
}

options_default :: proc(command: []string) -> Options {
	return {
		command = command,
		cancellation_grace = 5 * time.Second,
		maximum_payload_bytes = DEFAULT_MAXIMUM_PAYLOAD_BYTES,
	}
}

Stats :: struct {
	children_started: int,
	children_completed: int,
	child_failures: int,
	protocol_failures: int,
	timeouts: int,
	cancellations_forwarded: int,
	forced_cancellation_kills: int,
	heartbeats_forwarded: int,
	peak_in_flight: int,
}

Session :: struct {
	manager:         ^Manager,
	process:         os.Process,
	input:           ^os.File,
	write_mutex:     sync.Mutex,
	cancel_sent:     bool,
	finished:        bool,
	done:            sync.Sema,
	fallback_thread: ^thread.Thread,
}

Route :: struct {
	task_token: []u8,
	session:    ^Session,
}

Manager :: struct {
	command:               [dynamic]string,
	execution_timeout:     time.Duration,
	cancellation_grace:    time.Duration,
	maximum_payload_bytes: u32,
	routes:                [dynamic]Route,
	mutex:                 sync.Mutex,
	stats:                 Stats,
	initialized:           bool,
}

manager_init :: proc(manager: ^Manager, options: Options) -> Manager_Error {
	if manager == nil || len(options.command) == 0 || len(options.command[0]) == 0 {
		return manager_error("Activity executor command must not be empty")
	}
	if options.execution_timeout < 0 || options.cancellation_grace < 0 {
		return manager_error("Activity executor timeouts must not be negative")
	}
	if options.maximum_payload_bytes == 0 {
		return manager_error("Activity executor payload limit must be positive")
	}
	manager^ = {}
	manager.command = make([dynamic]string, 0, len(options.command))
	for argument in options.command {
		owned, allocation_error := strings.clone(argument)
		if allocation_error != nil {
			manager_close(manager)
			return manager_error("could not allocate Activity executor command")
		}
		append(&manager.command, owned)
	}
	manager.routes = make([dynamic]Route)
	manager.execution_timeout = options.execution_timeout
	manager.cancellation_grace = options.cancellation_grace
	manager.maximum_payload_bytes = options.maximum_payload_bytes
	manager.initialized = true
	return {}
}

manager_close :: proc(manager: ^Manager) {
	if manager == nil {
		return
	}
	sync.mutex_lock(&manager.mutex)
	for route in manager.routes {
		_ = os.process_kill(route.session.process)
		delete(route.task_token)
	}
	delete(manager.routes)
	manager.routes = nil
	sync.mutex_unlock(&manager.mutex)
	for argument in manager.command {
		delete(argument)
	}
	delete(manager.command)
	manager.command = nil
	manager.initialized = false
}

manager_stats :: proc(manager: ^Manager) -> Stats {
	if manager == nil {
		return {}
	}
	sync.mutex_lock(&manager.mutex)
	defer sync.mutex_unlock(&manager.mutex)
	return manager.stats
}

manager_in_flight :: proc(manager: ^Manager) -> int {
	if manager == nil {
		return 0
	}
	sync.mutex_lock(&manager.mutex)
	defer sync.mutex_unlock(&manager.mutex)
	return len(manager.routes)
}

decode_task_token :: proc(task: []u8) -> (token: []u8, ok: bool) {
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return
	}
	defer virtual.arena_destroy(&arena)
	previous := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	decoded, decode_error := proto.decode_activity_task(task)
	context.allocator = previous
	if decode_error != .None {
		return
	}
	token = bytes.clone(decoded.task_token)
	ok = len(token) > 0
	return
}

route_add :: proc(manager: ^Manager, token: []u8, session: ^Session) -> bool {
	sync.mutex_lock(&manager.mutex)
	defer sync.mutex_unlock(&manager.mutex)
	for route in manager.routes {
		if bytes.equal(route.task_token, token) {
			return false
		}
	}
	append(&manager.routes, Route{task_token = token, session = session})
	manager.stats.children_started += 1
	manager.stats.peak_in_flight = max(manager.stats.peak_in_flight, len(manager.routes))
	return true
}

session_write :: proc(session: ^Session, frame: Frame, maximum_payload_bytes: u32) -> Error {
	if session == nil || session.input == nil {
		return {kind = .Invalid_Argument}
	}
	sync.mutex_lock(&session.write_mutex)
	defer sync.mutex_unlock(&session.write_mutex)
	return write(session.input, frame, maximum_payload_bytes)
}

cancellation_fallback_main :: proc(session: ^Session) {
	if session == nil || session.manager == nil {
		return
	}
	finished := sync.sema_wait_with_timeout(
		&session.done,
		session.manager.cancellation_grace,
	)
	if finished || sync.atomic_load_explicit(&session.finished, .Acquire) {
		return
	}
	_ = os.process_kill(session.process)
	sync.mutex_lock(&session.manager.mutex)
	session.manager.stats.forced_cancellation_kills += 1
	sync.mutex_unlock(&session.manager.mutex)
}

session_finish :: proc(manager: ^Manager, session: ^Session) {
	if manager == nil || session == nil {
		return
	}
	sync.mutex_lock(&manager.mutex)
	sync.atomic_store_explicit(&session.finished, true, .Release)
	fallback_thread := session.fallback_thread
	sync.mutex_unlock(&manager.mutex)
	if fallback_thread != nil {
		sync.sema_post(&session.done)
		thread.join(fallback_thread)
		thread.destroy(fallback_thread)
	}
}

route_remove :: proc(manager: ^Manager, token: []u8) {
	sync.mutex_lock(&manager.mutex)
	defer sync.mutex_unlock(&manager.mutex)
	for &route, index in manager.routes {
		if bytes.equal(route.task_token, token) {
			delete(route.task_token)
			ordered_remove(&manager.routes, index)
			return
		}
	}
}

Read_State :: struct {
	file: ^os.File,
	maximum_payload_bytes: u32,
	frame: Owned_Frame,
	error: Error,
	done: sync.Sema,
}

read_main :: proc(state: ^Read_State) {
	state.frame, state.error = read(state.file, state.maximum_payload_bytes)
	sync.sema_post(&state.done)
}

read_with_timeout :: proc(
	file: ^os.File,
	maximum_payload_bytes: u32,
	timeout: time.Duration,
	process: os.Process,
) -> (frame: Owned_Frame, error: Error, timed_out: bool) {
	if timeout <= 0 {
		frame, error = read(file, maximum_payload_bytes)
		return
	}
	state := Read_State{file = file, maximum_payload_bytes = maximum_payload_bytes}
	reader := thread.create_and_start_with_poly_data(&state, read_main)
	if reader == nil {
		return {}, {kind = .IO}, false
	}
	finished := sync.sema_wait_with_timeout(&state.done, timeout)
	if !finished {
		_ = os.process_kill(process)
	}
	thread.join(reader)
	thread.destroy(reader)
	return state.frame, state.error, !finished
}

forward_heartbeat :: proc(
	ctx: ^activity.Context,
	payload: []u8,
	task_token: []u8,
) -> (status: activity.Heartbeat_Status, ok: bool) {
	if ctx == nil {
		return .Invalid_Context, false
	}
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		return .Core_Error, false
	}
	defer virtual.arena_destroy(&arena)
	previous := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	heartbeat := proto.Coresdk_ActivityHeartbeat{}
	decoded := proto.decode_Coresdk_ActivityHeartbeat(payload, &heartbeat)
	context.allocator = previous
	if !decoded || !bytes.equal(heartbeat.task_token, task_token) || len(heartbeat.details) != 1 {
		return .Core_Error, false
	}
	details, clone_error := proto.clone_payload(&heartbeat.details[0])
	if clone_error != .None {
		return .Core_Error, false
	}
	defer codec.payload_free(&details)
	return activity.heartbeat_payload(ctx, codec.payload_view(&details)), true
}

process_task :: proc(
	processor_data: rawptr,
	ctx: ^activity.Context,
	task: []u8,
) -> worker.Activity_Task_Process_Result {
	manager := cast(^Manager)processor_data
	if manager == nil || !manager.initialized {
		return {error = worker.error_from_string("Activity executor manager is not initialized")}
	}
	token, token_ok := decode_task_token(task)
	if !token_ok {
		return {error = worker.error_from_string("external Activity task is malformed")}
	}

	child_input, parent_input, input_error := os.pipe()
	if input_error != nil {
		delete(token)
		return {error = worker.error_from_string("could not create Activity executor input pipe")}
	}
	parent_output, child_output, output_error := os.pipe()
	if output_error != nil {
		os.close(child_input)
		os.close(parent_input)
		delete(token)
		return {error = worker.error_from_string("could not create Activity executor output pipe")}
	}
	process, start_error := process_start.start({
		command = manager.command[:],
		stdin = child_input,
		stdout = child_output,
		stderr = os.stderr,
	})
	os.close(child_input)
	os.close(child_output)
	if start_error != nil {
		os.close(parent_input)
		os.close(parent_output)
		delete(token)
		sync.mutex_lock(&manager.mutex)
		manager.stats.child_failures += 1
		sync.mutex_unlock(&manager.mutex)
		return {error = worker.error_from_string("could not start Activity executor child")}
	}
	session := Session{
		manager = manager,
		process = process,
		input = parent_input,
	}
	if !route_add(manager, token, &session) {
		_ = os.process_kill(process)
		_, _ = os.process_wait(process)
		os.close(parent_input)
		os.close(parent_output)
		delete(token)
		return {error = worker.error_from_string("duplicate Activity executor task token")}
	}
	defer os.close(parent_input)
	defer os.close(parent_output)
	defer session_finish(manager, &session)
	defer route_remove(manager, token)

	write_error := session_write(&session, {
		kind = .Execute,
		request_id = 1,
		payload = task,
	}, manager.maximum_payload_bytes)
	if write_error.kind != .None {
		_ = os.process_kill(process)
		_, _ = os.process_wait(process)
		sync.mutex_lock(&manager.mutex)
		manager.stats.protocol_failures += 1
		sync.mutex_unlock(&manager.mutex)
		return {error = worker.error_from_string("could not send Activity task to executor child")}
	}

	completion_allocator := context.allocator
	completion: []u8
	execution_started := time.tick_now()
	for {
		read_timeout := manager.execution_timeout
		if read_timeout > 0 {
			read_timeout -= time.tick_diff(execution_started, time.tick_now())
			if read_timeout <= 0 {
				_ = os.process_kill(process)
				_, _ = os.process_wait(process)
				sync.mutex_lock(&manager.mutex)
				manager.stats.timeouts += 1
				manager.stats.child_failures += 1
				sync.mutex_unlock(&manager.mutex)
				return {error = worker.error_from_string("Activity executor timed out")}
			}
		}
		response, read_error, timed_out := read_with_timeout(
			parent_output,
			manager.maximum_payload_bytes,
			read_timeout,
			process,
		)
		if timed_out {
			owned_frame_free(&response)
			sync.mutex_lock(&manager.mutex)
			manager.stats.timeouts += 1
			manager.stats.child_failures += 1
			sync.mutex_unlock(&manager.mutex)
			_, _ = os.process_wait(process)
			return {error = worker.error_from_string("Activity executor timed out")}
		}
		if read_error.kind != .None {
			owned_frame_free(&response)
			_, _ = os.process_wait(process)
			sync.mutex_lock(&manager.mutex)
			manager.stats.child_failures += 1
			sync.mutex_unlock(&manager.mutex)
			return {error = worker.error_from_string("Activity executor exited without a completion")}
		}
		if response.frame.request_id != 1 {
			owned_frame_free(&response)
			_ = os.process_kill(process)
			_, _ = os.process_wait(process)
			sync.mutex_lock(&manager.mutex)
			manager.stats.protocol_failures += 1
			sync.mutex_unlock(&manager.mutex)
			return {error = worker.error_from_string("Activity executor returned a mismatched request id")}
		}
		if response.frame.kind == .Heartbeat {
			heartbeat_status, heartbeat_ok := forward_heartbeat(ctx, response.frame.payload, token)
			owned_frame_free(&response)
			if !heartbeat_ok {
				_ = os.process_kill(process)
				_, _ = os.process_wait(process)
				sync.mutex_lock(&manager.mutex)
				manager.stats.protocol_failures += 1
				sync.mutex_unlock(&manager.mutex)
				return {error = worker.error_from_string("Activity executor returned an invalid heartbeat")}
			}
			ack := [1]u8{u8(heartbeat_status)}
			ack_error := session_write(&session, {
				kind = .Heartbeat_Ack,
				request_id = 1,
				payload = ack[:],
			}, manager.maximum_payload_bytes)
			if ack_error.kind != .None {
				_ = os.process_kill(process)
				_, _ = os.process_wait(process)
				return {error = worker.error_from_string("could not acknowledge Activity heartbeat")}
			}
			sync.mutex_lock(&manager.mutex)
			manager.stats.heartbeats_forwarded += 1
			sync.mutex_unlock(&manager.mutex)
			continue
		}
		if response.frame.kind == .Control_Done {
			owned_frame_free(&response)
			stop_payload := [1]u8{0}
			stop_error := session_write(&session, {
				kind = .Control_Stop,
				request_id = 1,
				payload = stop_payload[:],
			}, manager.maximum_payload_bytes)
			if stop_error.kind != .None {
				_ = os.process_kill(process)
				_, _ = os.process_wait(process)
				return {error = worker.error_from_string("could not stop Activity child control loop")}
			}
			continue
		}
		if response.frame.kind == .Executor_Error {
			message := string(bytes.clone(response.frame.payload))
			owned_frame_free(&response)
			_, _ = os.process_wait(process)
			sync.mutex_lock(&manager.mutex)
			manager.stats.child_failures += 1
			sync.mutex_unlock(&manager.mutex)
			error := worker.error_from_string(message)
			delete(message)
			return {error = error}
		}
		if response.frame.kind != .Completion {
			owned_frame_free(&response)
			_ = os.process_kill(process)
			_, _ = os.process_wait(process)
			sync.mutex_lock(&manager.mutex)
			manager.stats.protocol_failures += 1
			sync.mutex_unlock(&manager.mutex)
			return {error = worker.error_from_string("Activity executor returned an invalid response")}
		}
		completion = bytes.clone(response.frame.payload, completion_allocator)
		owned_frame_free(&response)
		break
	}
	state, _ := os.process_wait(process)
	if !state.exited || !state.success {
		delete(completion, completion_allocator)
		sync.mutex_lock(&manager.mutex)
		manager.stats.child_failures += 1
		sync.mutex_unlock(&manager.mutex)
		return {error = worker.error_from_string("Activity executor exited unsuccessfully")}
	}
	sync.mutex_lock(&manager.mutex)
	manager.stats.children_completed += 1
	sync.mutex_unlock(&manager.mutex)
	return {completion = completion, allocator = completion_allocator}
}

cancel_task :: proc(processor_data: rawptr, task: []u8) {
	manager := cast(^Manager)processor_data
	if manager == nil || !manager.initialized {
		return
	}
	token, token_ok := decode_task_token(task)
	if !token_ok {
		return
	}
	defer delete(token)
	sync.mutex_lock(&manager.mutex)
	defer sync.mutex_unlock(&manager.mutex)
	for route in manager.routes {
		if bytes.equal(route.task_token, token) {
			session := route.session
			if session == nil || session.cancel_sent {
				return
			}
			session.cancel_sent = true
			cancel_error := session_write(session, {
				kind = .Cancel,
				request_id = 1,
				payload = task,
			}, manager.maximum_payload_bytes)
			if cancel_error.kind != .None {
				_ = os.process_kill(session.process)
				manager.stats.forced_cancellation_kills += 1
				return
			}
			manager.stats.cancellations_forwarded += 1
			session.fallback_thread = thread.create_and_start_with_poly_data(
				session,
				cancellation_fallback_main,
			)
			if session.fallback_thread == nil {
				_ = os.process_kill(session.process)
				manager.stats.forced_cancellation_kills += 1
			}
			return
		}
	}
}

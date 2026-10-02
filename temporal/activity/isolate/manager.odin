package activity_isolate

import allocator_runtime "base:runtime"
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

Crash_Loop_Policy :: struct {
	// Zero disables the breaker. Once the threshold is reached, new child
	// starts are rejected until cooldown admits one trial generation.
	maximum_consecutive_failures: int,
	cooldown:                     time.Duration,
}

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
	startup_timeout:       time.Duration,
	shutdown_timeout:      time.Duration,
	warm_pool_size:        int,
	crash_loop:            Crash_Loop_Policy,
	maximum_payload_bytes: u32,
}

options_default :: proc(command: []string) -> Options {
	return {
		command = command,
		cancellation_grace = 5 * time.Second,
		startup_timeout = 5 * time.Second,
		shutdown_timeout = 5 * time.Second,
		maximum_payload_bytes = DEFAULT_MAXIMUM_PAYLOAD_BYTES,
	}
}

Stats :: struct {
	children_started: int,
	children_completed: int,
	children_reused: int,
	children_retired: int,
	children_shutdown: int,
	manifest_handshakes: int,
	manifest_mismatches: int,
	startup_failures: int,
	crash_loop_rejections: int,
	crash_loop_recoveries: int,
	child_failures: int,
	protocol_failures: int,
	timeouts: int,
	cancellations_forwarded: int,
	forced_cancellation_kills: int,
	heartbeats_forwarded: int,
	peak_in_flight: int,
	peak_pool_children: int,
}

Session :: struct {
	manager:         ^Manager,
	process:         os.Process,
	input:           ^os.File,
	output:          ^os.File,
	write_mutex:     sync.Mutex,
	cancel_sent:     bool,
	finished:        bool,
	done:            sync.Sema,
	fallback_thread: ^thread.Thread,
	request_id:      u64,
	uses:            int,
	busy:            bool,
	persistent:      bool,
	reaped:          bool,
	exit_state:      os.Process_State,
}

Route :: struct {
	task_token: []u8,
	session:    ^Session,
}

Manager :: struct {
	command:               [dynamic]string,
	execution_timeout:     time.Duration,
	cancellation_grace:    time.Duration,
	startup_timeout:       time.Duration,
	shutdown_timeout:      time.Duration,
	warm_pool_size:        int,
	crash_loop:            Crash_Loop_Policy,
	maximum_payload_bytes: u32,
	expected_manifest:     []u8,
	routes:                [dynamic]Route,
	pool:                  [dynamic]^Session,
	pool_available:        sync.Sema,
	pool_starting:         int,
	pool_waiters:          int,
	pool_wakeups:          int,
	closing:               bool,
	consecutive_failures:  int,
	last_failure:          time.Tick,
	crash_loop_trial:      bool,
	mutex:                 sync.Mutex,
	stats:                 Stats,
	initialized:           bool,
}

manager_init :: proc(manager: ^Manager, options: Options) -> Manager_Error {
	if manager == nil || len(options.command) == 0 || len(options.command[0]) == 0 {
		return manager_error("Activity executor command must not be empty")
	}
	if options.execution_timeout < 0 || options.cancellation_grace < 0 ||
	   options.startup_timeout < 0 || options.shutdown_timeout < 0 ||
	   options.warm_pool_size < 0 {
		return manager_error("Activity executor durations and pool size must not be negative")
	}
	if options.crash_loop.maximum_consecutive_failures < 0 ||
	   options.crash_loop.cooldown < 0 ||
	   (options.crash_loop.maximum_consecutive_failures > 0 &&
	    options.crash_loop.cooldown <= 0) {
		return manager_error("Activity executor crash-loop policy is invalid")
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
	manager.pool = make([dynamic]^Session)
	manager.execution_timeout = options.execution_timeout
	manager.cancellation_grace = options.cancellation_grace
	manager.startup_timeout = options.startup_timeout
	manager.shutdown_timeout = options.shutdown_timeout
	manager.warm_pool_size = options.warm_pool_size
	manager.crash_loop = options.crash_loop
	manager.maximum_payload_bytes = options.maximum_payload_bytes
	manager.initialized = true
	return {}
}

// register_external_activity_manifest copies only the manifest's Activity type
// names into the Core-owning Worker. Callback data and procedure pointers stay
// process-local and are used only when the same application manifest is built
// inside the executor child.
register_external_activity_manifest :: proc(
	sdk_worker: ^worker.Worker,
	manager: ^Manager,
	activities: []Host_Activity,
) -> worker.Error {
	if manager == nil || !manager.initialized ||
	   !host_options_valid(host_options_many(activities)) {
		return worker.error_from_string("Activity executor manifest is invalid")
	}
	manifest, manifest_ok := host_manifest_encode(activities)
	if !manifest_ok {
		return worker.error_from_string("Activity executor manifest could not be encoded")
	}
	sync.mutex_lock(&manager.mutex)
	manifest_already_set := len(manager.expected_manifest) != 0 ||
	                        len(manager.pool) != 0 || len(manager.routes) != 0
	sync.mutex_unlock(&manager.mutex)
	if manifest_already_set {
		delete(manifest)
		return worker.error_from_string("Activity executor manifest is already registered")
	}
	for activity_registration in activities {
		registration_error := worker.register_external_activity_processor(
			sdk_worker,
			activity_registration.name,
			manager,
			process_task,
			cancel_task,
		)
		if registration_error.message != nil {
			delete(manifest)
			return registration_error
		}
	}
	sync.mutex_lock(&manager.mutex)
	manager.expected_manifest = manifest
	sync.mutex_unlock(&manager.mutex)
	return {}
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

session_start :: proc(manager: ^Manager, persistent: bool) -> ^Session {
	child_input, parent_input, input_error := os.pipe()
	if input_error != nil {
		return nil
	}
	parent_output, child_output, output_error := os.pipe()
	if output_error != nil {
		os.close(child_input)
		os.close(parent_input)
		return nil
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
		return nil
	}
	heap := allocator_runtime.heap_allocator()
	session, allocation_error := new(Session, heap)
	if allocation_error != nil {
		_ = os.process_kill(process)
		_, _ = os.process_wait(process)
		os.close(parent_input)
		os.close(parent_output)
		return nil
	}
	session^ = {
		manager = manager,
		process = process,
		input = parent_input,
		output = parent_output,
		persistent = persistent,
	}
	sync.mutex_lock(&manager.mutex)
	manager.stats.children_started += 1
	sync.mutex_unlock(&manager.mutex)
	return session
}

session_manifest_handshake :: proc(manager: ^Manager, session: ^Session) -> bool {
	if manager == nil || session == nil || len(manager.expected_manifest) == 0 {
		if manager != nil {
			sync.mutex_lock(&manager.mutex)
			manager.stats.startup_failures += 1
			manager.stats.manifest_mismatches += 1
			sync.mutex_unlock(&manager.mutex)
		}
		return false
	}
	session.request_id += 1
	request_id := session.request_id
	request_payload := transmute([]u8)string("manifest")
	write_error := session_write(session, {
		kind = .Manifest_Request,
		request_id = request_id,
		payload = request_payload,
	}, manager.maximum_payload_bytes)
	if write_error.kind != .None {
		sync.mutex_lock(&manager.mutex)
		manager.stats.startup_failures += 1
		manager.stats.protocol_failures += 1
		sync.mutex_unlock(&manager.mutex)
		return false
	}
	response, read_error, timed_out := read_with_timeout(
		session.output,
		manager.maximum_payload_bytes,
		manager.startup_timeout,
		session.process,
	)
	defer owned_frame_free(&response)
	valid := !timed_out && read_error.kind == .None &&
	         response.frame.kind == .Manifest &&
	         response.frame.request_id == request_id &&
	         bytes.equal(response.frame.payload, manager.expected_manifest)
	sync.mutex_lock(&manager.mutex)
	if valid {
		manager.stats.manifest_handshakes += 1
	} else {
		manager.stats.startup_failures += 1
		manager.stats.manifest_mismatches += 1
		manager.stats.protocol_failures += 1
	}
	sync.mutex_unlock(&manager.mutex)
	return valid
}

session_prepare :: proc(manager: ^Manager, session: ^Session) {
	if manager == nil || session == nil {
		return
	}
	session.request_id += 1
	session.cancel_sent = false
	session.fallback_thread = nil
	sync.atomic_store_explicit(&session.finished, false, .Release)
	session.busy = true
}

session_destroy :: proc(session: ^Session, kill: bool) -> os.Process_State {
	if session == nil {
		return {}
	}
	if kill && !session.reaped {
		_ = os.process_kill(session.process)
	}
	state := session.exit_state
	if !session.reaped {
		state, _ = os.process_wait(session.process)
		session.reaped = true
		session.exit_state = state
	}
	if session.input != nil {
		os.close(session.input)
		session.input = nil
	}
	if session.output != nil {
		os.close(session.output)
		session.output = nil
	}
	free(session, allocator_runtime.heap_allocator())
	return state
}

// crash_loop_allows_start is pure so generated tests can cover threshold and
// cooldown boundaries independently of the process clock.
crash_loop_allows_start :: proc(
	consecutive_failures: int,
	elapsed_since_failure: time.Duration,
	policy: Crash_Loop_Policy,
) -> (allowed, trial: bool) {
	if policy.maximum_consecutive_failures == 0 ||
	   consecutive_failures < policy.maximum_consecutive_failures {
		return true, false
	}
	if elapsed_since_failure >= policy.cooldown {
		return true, true
	}
	return false, false
}

manager_allows_start_locked :: proc(manager: ^Manager) -> bool {
	if manager.crash_loop.maximum_consecutive_failures == 0 {
		return true
	}
	if manager.crash_loop_trial {
		manager.stats.crash_loop_rejections += 1
		return false
	}
	elapsed := time.Duration(0)
	if manager.last_failure._nsec != 0 {
		elapsed = time.tick_diff(manager.last_failure, time.tick_now())
	}
	allowed, trial := crash_loop_allows_start(
		manager.consecutive_failures,
		elapsed,
		manager.crash_loop,
	)
	if !allowed {
		manager.stats.crash_loop_rejections += 1
		return false
	}
	manager.crash_loop_trial = trial
	return true
}

manager_record_success_locked :: proc(manager: ^Manager) {
	if manager.consecutive_failures > 0 || manager.crash_loop_trial {
		manager.stats.crash_loop_recoveries += 1
	}
	manager.consecutive_failures = 0
	manager.last_failure = {}
	manager.crash_loop_trial = false
}

manager_record_failure_locked :: proc(manager: ^Manager) {
	manager.consecutive_failures += 1
	manager.last_failure = time.tick_now()
	manager.crash_loop_trial = false
}

pool_acquire :: proc(manager: ^Manager) -> ^Session {
	if manager.warm_pool_size == 0 {
		sync.mutex_lock(&manager.mutex)
		allowed := manager_allows_start_locked(manager)
		sync.mutex_unlock(&manager.mutex)
		if !allowed {
			return nil
		}
		session := session_start(manager, false)
		if session != nil {
			session_prepare(manager, session)
		} else {
			sync.mutex_lock(&manager.mutex)
			manager_record_failure_locked(manager)
			sync.mutex_unlock(&manager.mutex)
		}
		return session
	}
	for {
		sync.mutex_lock(&manager.mutex)
		if manager.closing || !manager.initialized {
			sync.mutex_unlock(&manager.mutex)
			return nil
		}
		for session in manager.pool {
			if !session.busy {
				session_prepare(manager, session)
				if session.uses > 0 {
					manager.stats.children_reused += 1
				}
				sync.mutex_unlock(&manager.mutex)
				return session
			}
		}
		if len(manager.pool) + manager.pool_starting < manager.warm_pool_size {
			if !manager_allows_start_locked(manager) {
				sync.mutex_unlock(&manager.mutex)
				return nil
			}
			manager.pool_starting += 1
			sync.mutex_unlock(&manager.mutex)
			session := session_start(manager, true)
			if session != nil && !session_manifest_handshake(manager, session) {
				_ = session_destroy(session, true)
				session = nil
			}
			sync.mutex_lock(&manager.mutex)
			manager.pool_starting -= 1
			if session == nil {
				manager_record_failure_locked(manager)
			}
			if session != nil && !manager.closing {
				session_prepare(manager, session)
				append(&manager.pool, session)
				manager.stats.peak_pool_children = max(
					manager.stats.peak_pool_children,
					len(manager.pool),
				)
				sync.mutex_unlock(&manager.mutex)
				return session
			}
			sync.mutex_unlock(&manager.mutex)
			if session != nil {
				_ = session_destroy(session, true)
			}
			sync.mutex_lock(&manager.mutex)
			notify := manager.pool_waiters > manager.pool_wakeups
			if notify {
				manager.pool_wakeups += 1
			}
			sync.mutex_unlock(&manager.mutex)
			if notify {
				sync.sema_post(&manager.pool_available)
			}
			return nil
		}
		manager.pool_waiters += 1
		sync.mutex_unlock(&manager.mutex)
		sync.sema_wait(&manager.pool_available)
		sync.mutex_lock(&manager.mutex)
		manager.pool_waiters -= 1
		manager.pool_wakeups -= 1
		sync.mutex_unlock(&manager.mutex)
	}
}

pool_release :: proc(manager: ^Manager, session: ^Session) {
	sync.mutex_lock(&manager.mutex)
	session.busy = false
	session.uses += 1
	manager.stats.children_completed += 1
	manager_record_success_locked(manager)
	notify := manager.pool_waiters > manager.pool_wakeups
	if notify {
		manager.pool_wakeups += 1
	}
	sync.mutex_unlock(&manager.mutex)
	if notify {
		sync.sema_post(&manager.pool_available)
	}
}

pool_retire :: proc(manager: ^Manager, session: ^Session) {
	if manager == nil || session == nil {
		return
	}
	sync.mutex_lock(&manager.mutex)
	for pooled, index in manager.pool {
		if pooled == session {
			ordered_remove(&manager.pool, index)
			break
		}
	}
	manager.stats.children_retired += 1
	manager_record_failure_locked(manager)
	notify := manager.pool_waiters > manager.pool_wakeups
	if notify {
		manager.pool_wakeups += 1
	}
	sync.mutex_unlock(&manager.mutex)
	_ = session_destroy(session, true)
	if notify {
		sync.sema_post(&manager.pool_available)
	}
}

execution_finish :: proc(
	manager: ^Manager,
	session: ^Session,
	token: []u8,
	healthy: ^bool,
) {
	route_remove(manager, token)
	session_finish(manager, session)
	if session.persistent {
		if healthy != nil && healthy^ {
			pool_release(manager, session)
		} else {
			pool_retire(manager, session)
		}
		return
	}
	state := session_destroy(session, healthy == nil || !healthy^)
	sync.mutex_lock(&manager.mutex)
	if healthy != nil && healthy^ {
		if state.exited && state.success {
			manager.stats.children_completed += 1
			manager_record_success_locked(manager)
		} else {
			healthy^ = false
			manager.stats.child_failures += 1
			manager_record_failure_locked(manager)
		}
	} else {
		manager_record_failure_locked(manager)
	}
	sync.mutex_unlock(&manager.mutex)
}

Process_Wait_State :: struct {
	process: os.Process,
	state:   os.Process_State,
	error:   os.Error,
	done:    sync.Sema,
}

process_wait_main :: proc(wait: ^Process_Wait_State) {
	wait.state, wait.error = os.process_wait(wait.process)
	sync.sema_post(&wait.done)
}

process_wait_with_timeout :: proc(
	process: os.Process,
	timeout: time.Duration,
) -> (state: os.Process_State, timed_out: bool) {
	if timeout <= 0 {
		state, _ = os.process_wait(process)
		return
	}
	wait := Process_Wait_State{process = process}
	wait_thread := thread.create_and_start_with_poly_data(&wait, process_wait_main)
	if wait_thread == nil {
		_ = os.process_kill(process)
		state, _ = os.process_wait(process)
		return state, true
	}
	finished := sync.sema_wait_with_timeout(&wait.done, timeout)
	if !finished {
		_ = os.process_kill(process)
	}
	thread.join(wait_thread)
	thread.destroy(wait_thread)
	return wait.state, !finished
}

session_shutdown :: proc(manager: ^Manager, session: ^Session) -> bool {
	if manager == nil || session == nil {
		return false
	}
	session.request_id += 1
	request_id := session.request_id
	payload := [1]u8{0}
	write_error := session_write(session, {
		kind = .Shutdown,
		request_id = request_id,
		payload = payload[:],
	}, manager.maximum_payload_bytes)
	ack_ok := false
	if write_error.kind == .None {
		response, read_error, timed_out := read_with_timeout(
			session.output,
			manager.maximum_payload_bytes,
			manager.shutdown_timeout,
			session.process,
		)
		ack_ok = !timed_out && read_error.kind == .None &&
		         response.frame.kind == .Shutdown_Ack &&
		         response.frame.request_id == request_id
		owned_frame_free(&response)
	}
	if !ack_ok {
		_ = os.process_kill(session.process)
	}
	state, timed_out := process_wait_with_timeout(
		session.process,
		manager.shutdown_timeout,
	)
	session.exit_state = state
	session.reaped = true
	graceful := ack_ok && !timed_out && state.exited && state.success
	sync.mutex_lock(&manager.mutex)
	if graceful {
		manager.stats.children_shutdown += 1
	} else {
		manager.stats.child_failures += 1
	}
	sync.mutex_unlock(&manager.mutex)
	_ = session_destroy(session, false)
	return graceful
}

// manager_shutdown is valid after Worker polling and Activity execution have
// drained. It closes every idle warm child through protocol v5 and bounds both
// acknowledgement and process-exit waits.
manager_shutdown :: proc(manager: ^Manager) -> Manager_Error {
	if manager == nil || !manager.initialized {
		return manager_error("Activity executor manager is not initialized")
	}
	sync.mutex_lock(&manager.mutex)
	if manager.closing {
		sync.mutex_unlock(&manager.mutex)
		return {}
	}
	if len(manager.routes) != 0 || manager.pool_starting != 0 {
		sync.mutex_unlock(&manager.mutex)
		return manager_error("Activity executor manager cannot shut down with work in flight")
	}
	manager.closing = true
	sessions := make([]^Session, len(manager.pool))
	copy(sessions, manager.pool[:])
	delete(manager.pool)
	manager.pool = nil
	sync.mutex_unlock(&manager.mutex)
	all_graceful := true
	for session in sessions {
		was_busy := session.busy
		shutdown_ok := session_shutdown(manager, session)
		if was_busy || !shutdown_ok {
			all_graceful = false
		}
	}
	delete(sessions)
	if !all_graceful {
		return manager_error("one or more Activity executor children did not shut down cleanly")
	}
	return {}
}

manager_close :: proc(manager: ^Manager) {
	if manager == nil {
		return
	}
	if manager.initialized && !manager.closing {
		shutdown_error := manager_shutdown(manager)
		manager_error_free(&shutdown_error)
	}
	delete(manager.routes)
	manager.routes = nil
	delete(manager.pool)
	manager.pool = nil
	delete(manager.expected_manifest)
	manager.expected_manifest = nil
	for argument in manager.command {
		delete(argument)
	}
	delete(manager.command)
	manager.command = nil
	manager.initialized = false
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
	session := pool_acquire(manager)
	if session == nil {
		delete(token)
		sync.mutex_lock(&manager.mutex)
		manager.stats.child_failures += 1
		sync.mutex_unlock(&manager.mutex)
		return {error = worker.error_from_string("could not start Activity executor child")}
	}
	if !route_add(manager, token, session) {
		if session.persistent {
			pool_retire(manager, session)
		} else {
			_ = session_destroy(session, true)
		}
		delete(token)
		return {error = worker.error_from_string("duplicate Activity executor task token")}
	}
	healthy := false
	defer execution_finish(manager, session, token, &healthy)

	write_error := session_write(session, {
		kind = .Execute,
		request_id = session.request_id,
		payload = task,
	}, manager.maximum_payload_bytes)
	if write_error.kind != .None {
		_ = os.process_kill(session.process)
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
				_ = os.process_kill(session.process)
				sync.mutex_lock(&manager.mutex)
				manager.stats.timeouts += 1
				manager.stats.child_failures += 1
				sync.mutex_unlock(&manager.mutex)
				return {error = worker.error_from_string("Activity executor timed out")}
			}
		}
		response, read_error, timed_out := read_with_timeout(
			session.output,
			manager.maximum_payload_bytes,
			read_timeout,
			session.process,
		)
		if timed_out {
			owned_frame_free(&response)
			sync.mutex_lock(&manager.mutex)
			manager.stats.timeouts += 1
			manager.stats.child_failures += 1
			sync.mutex_unlock(&manager.mutex)
			return {error = worker.error_from_string("Activity executor timed out")}
		}
		if read_error.kind != .None {
			owned_frame_free(&response)
			sync.mutex_lock(&manager.mutex)
			manager.stats.child_failures += 1
			sync.mutex_unlock(&manager.mutex)
			return {error = worker.error_from_string("Activity executor exited without a completion")}
		}
		if response.frame.request_id != session.request_id {
			owned_frame_free(&response)
			_ = os.process_kill(session.process)
			sync.mutex_lock(&manager.mutex)
			manager.stats.protocol_failures += 1
			sync.mutex_unlock(&manager.mutex)
			return {error = worker.error_from_string("Activity executor returned a mismatched request id")}
		}
		if response.frame.kind == .Heartbeat {
			heartbeat_status, heartbeat_ok := forward_heartbeat(ctx, response.frame.payload, token)
			owned_frame_free(&response)
			if !heartbeat_ok {
				_ = os.process_kill(session.process)
				sync.mutex_lock(&manager.mutex)
				manager.stats.protocol_failures += 1
				sync.mutex_unlock(&manager.mutex)
				return {error = worker.error_from_string("Activity executor returned an invalid heartbeat")}
			}
			ack := [1]u8{u8(heartbeat_status)}
			ack_error := session_write(session, {
				kind = .Heartbeat_Ack,
				request_id = session.request_id,
				payload = ack[:],
			}, manager.maximum_payload_bytes)
			if ack_error.kind != .None {
				_ = os.process_kill(session.process)
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
			stop_error := session_write(session, {
				kind = .Control_Stop,
				request_id = session.request_id,
				payload = stop_payload[:],
			}, manager.maximum_payload_bytes)
			if stop_error.kind != .None {
				_ = os.process_kill(session.process)
				return {error = worker.error_from_string("could not stop Activity child control loop")}
			}
			continue
		}
		if response.frame.kind == .Executor_Error {
			message := string(bytes.clone(response.frame.payload))
			owned_frame_free(&response)
			sync.mutex_lock(&manager.mutex)
			manager.stats.child_failures += 1
			sync.mutex_unlock(&manager.mutex)
			error := worker.error_from_string(message)
			delete(message)
			return {error = error}
		}
		if response.frame.kind != .Completion {
			owned_frame_free(&response)
			_ = os.process_kill(session.process)
			sync.mutex_lock(&manager.mutex)
			manager.stats.protocol_failures += 1
			sync.mutex_unlock(&manager.mutex)
			return {error = worker.error_from_string("Activity executor returned an invalid response")}
		}
		completion = bytes.clone(response.frame.payload, completion_allocator)
		owned_frame_free(&response)
		break
	}
	if !session.persistent {
		session.exit_state, _ = os.process_wait(session.process)
		session.reaped = true
		if !session.exit_state.exited || !session.exit_state.success {
			delete(completion, completion_allocator)
			sync.mutex_lock(&manager.mutex)
			manager.stats.child_failures += 1
			sync.mutex_unlock(&manager.mutex)
			return {error = worker.error_from_string("Activity executor exited unsuccessfully")}
		}
	}
	healthy = true
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
				request_id = session.request_id,
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

package main

import allocator_runtime "base:runtime"
import "core:fmt"
import "core:os"
import "core:strings"
import "core:time"
import supervisor "../../temporal/supervisor"

Probe_State :: struct {
	log_path: string,
	marker:   string,
}

readiness_probe :: proc(user_data: rawptr, _: int, generation: int) -> bool {
	state := cast(^Probe_State)user_data
	contents, read_error := os.read_entire_file(
		state.log_path,
		allocator_runtime.heap_allocator(),
	)
	if read_error != nil {
		return false
	}
	defer delete(contents, allocator_runtime.heap_allocator())
	return strings.count(string(contents), state.marker) >= generation
}

event_name :: proc(kind: supervisor.Event_Kind) -> string {
	switch kind {
	case .Child_Started:
		return "child-started"
	case .Child_Ready:
		return "child-ready"
	case .Child_Exited:
		return "child-exited"
	case .Restart_Scheduled:
		return "restart-scheduled"
	case .Stop_Requested:
		return "stop-requested"
	case .Child_Terminating:
		return "child-terminating"
	case .Child_Killed:
		return "child-killed"
	case .Stopped:
		return "stopped"
	}
	return "unknown"
}

observe :: proc(_: rawptr, event: supervisor.Event) {
	fmt.printf(
		"Supervisor event=%s pid=%d generation=%d exit=%d success=%v ready=%v restart_delay=%v\n",
		event_name(event.kind),
		event.pid,
		event.generation,
		event.exit_code,
		event.success,
		event.ready,
		event.restart_delay,
	)
	_ = os.flush(os.stdout)
}

main :: proc() {
	command_path_buffer: [1024]u8
	command_path := os.get_env(
		command_path_buffer[:],
		"TEMPORAL_SUPERVISOR_CHILD_COMMAND",
	)
	working_dir_buffer: [1024]u8
	working_dir := os.get_env(
		working_dir_buffer[:],
		"TEMPORAL_SUPERVISOR_WORKING_DIR",
	)
	log_path_buffer: [1024]u8
	log_path := os.get_env(
		log_path_buffer[:],
		"TEMPORAL_SUPERVISOR_CHILD_LOG",
	)
	ready_marker_buffer: [1024]u8
	ready_marker := os.get_env(
		ready_marker_buffer[:],
		"TEMPORAL_SUPERVISOR_READY_MARKER",
	)
	if len(command_path) == 0 || len(log_path) == 0 || len(ready_marker) == 0 {
		fmt.eprintln(
			"TEMPORAL_SUPERVISOR_CHILD_COMMAND, TEMPORAL_SUPERVISOR_CHILD_LOG, and TEMPORAL_SUPERVISOR_READY_MARKER are required",
		)
		os.exit(1)
	}

	child_log, create_error := os.create(log_path)
	if create_error != nil {
		fmt.eprintf("Could not create supervised Worker log: %s\n", os.error_string(create_error))
		os.exit(1)
	}
	defer os.close(child_log)

	command := [?]string{command_path}
	probe := Probe_State{log_path = log_path, marker = ready_marker}
	options := supervisor.options_default(command[:])
	options.working_dir = working_dir
	options.stdout = child_log
	options.stderr = child_log
	options.policy.maximum_consecutive_restarts = 3
	options.policy.initial_backoff = 100 * time.Millisecond
	options.policy.maximum_backoff = time.Second
	options.policy.stable_after = 30 * time.Second
	options.policy.poll_interval = 20 * time.Millisecond
	options.policy.readiness_timeout = 20 * time.Second
	options.policy.termination_grace = 2 * time.Second
	options.readiness_probe = readiness_probe
	options.observer = observe
	options.user_data = &probe
	control := supervisor.Control{}
	stats, run_error := supervisor.run(&control, options)
	defer supervisor.error_free(&run_error)
	if run_error.message != nil {
		fmt.eprintf("Worker supervision failed: %s\n", string(run_error.message))
		os.exit(1)
	}
	fmt.printf(
		"Worker supervision completed starts=%d ready=%d restarts=%d clean=%d failed=%d forced_kills=%d\n",
		stats.children_started,
		stats.children_ready,
		stats.restarts,
		stats.clean_exits,
		stats.failed_exits,
		stats.forced_kills,
	)
}

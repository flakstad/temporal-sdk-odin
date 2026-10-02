// Package supervisor manages a whole Temporal Worker child process. It is a
// process boundary for fatal Odin/runtime failures, not an in-process Workflow
// executor and not a replacement for Temporal's durable task retry.
package supervisor

import "core:bytes"
import "core:fmt"
import "core:os"
import "core:sync"
import "core:time"
import process_start "../internal/process_start"

Restart_Policy :: enum {
	On_Failure,
	Always,
	Never,
}

Event_Kind :: enum {
	Child_Started,
	Child_Ready,
	Child_Exited,
	Restart_Scheduled,
	Stop_Requested,
	Child_Terminating,
	Child_Killed,
	Stopped,
}

Event :: struct {
	kind:          Event_Kind,
	pid:           int,
	generation:    int,
	exit_code:     int,
	success:       bool,
	ready:         bool,
	restart_delay: time.Duration,
}

Observer :: #type proc(user_data: rawptr, event: Event)
Readiness_Probe :: #type proc(user_data: rawptr, pid: int, generation: int) -> bool

Policy :: struct {
	restart:                     Restart_Policy,
	// Zero allows unlimited consecutive restarts. A child that remains alive
	// for stable_after resets this counter and the current backoff.
	maximum_consecutive_restarts: int,
	initial_backoff:              time.Duration,
	maximum_backoff:              time.Duration,
	backoff_multiplier:           u32,
	stable_after:                 time.Duration,
	poll_interval:                time.Duration,
	// Zero disables the deadline. A nonzero value requires readiness_probe.
	readiness_timeout:            time.Duration,
	termination_grace:            time.Duration,
}

policy_default :: proc() -> Policy {
	return {
		restart = .On_Failure,
		maximum_consecutive_restarts = 10,
		initial_backoff = 100 * time.Millisecond,
		maximum_backoff = 30 * time.Second,
		backoff_multiplier = 2,
		stable_after = time.Minute,
		poll_interval = 50 * time.Millisecond,
		readiness_timeout = 0,
		termination_grace = 5 * time.Second,
	}
}

Options :: struct {
	command:        []string,
	working_dir:    string,
	env:            []string,
	stdin:          ^os.File,
	stdout:         ^os.File,
	stderr:         ^os.File,
	policy:         Policy,
	readiness_probe: Readiness_Probe,
	observer:       Observer,
	user_data:      rawptr,
}

options_default :: proc(command: []string) -> Options {
	return {
		command = command,
		stdout = os.stdout,
		stderr = os.stderr,
		policy = policy_default(),
	}
}

Control :: struct {
	stop_requested: bool,
}

request_stop :: proc(control: ^Control) {
	if control == nil {
		return
	}
	sync.atomic_store_explicit(&control.stop_requested, true, .Release)
}

stop_requested :: proc(control: ^Control) -> bool {
	return control != nil &&
	       sync.atomic_load_explicit(&control.stop_requested, .Acquire)
}

Stats :: struct {
	children_started:       int,
	children_ready:         int,
	restarts:              int,
	start_failures:         int,
	clean_exits:            int,
	failed_exits:           int,
	readiness_timeouts:     int,
	termination_requests:   int,
	forced_kills:           int,
	last_pid:               int,
	last_exit_code:         int,
	last_child_was_ready:   bool,
	stop_was_requested:     bool,
}

Error :: struct {
	message: []u8,
}

error_free :: proc(error: ^Error) {
	if error == nil || error.message == nil {
		return
	}
	delete(error.message)
	error.message = nil
}

error_from_string :: proc(message: string) -> Error {
	return {message = bytes.clone(transmute([]u8)message)}
}

error_from_os :: proc(prefix: string, os_error: os.Error) -> Error {
	message := fmt.aprintf("%s: %s", prefix, os.error_string(os_error))
	defer delete(message)
	return error_from_string(message)
}

emit :: proc(options: ^Options, event: Event) {
	if options != nil && options.observer != nil {
		options.observer(options.user_data, event)
	}
}

os_error_is_timeout :: proc(value: os.Error) -> bool {
	if error_value, ok := value.(os.General_Error); ok {
		return error_value == .Timeout
	}
	return false
}

options_validate :: proc(options: Options) -> Error {
	if len(options.command) == 0 || len(options.command[0]) == 0 {
		return error_from_string("supervisor child command must not be empty")
	}
	policy := options.policy
	if policy.maximum_consecutive_restarts < 0 {
		return error_from_string("maximum consecutive restarts must not be negative")
	}
	if policy.initial_backoff < 0 || policy.maximum_backoff < 0 ||
	   policy.stable_after < 0 || policy.poll_interval <= 0 ||
	   policy.readiness_timeout < 0 || policy.termination_grace < 0 {
		return error_from_string("supervisor durations are invalid")
	}
	if policy.maximum_backoff < policy.initial_backoff {
		return error_from_string("maximum backoff must not be shorter than initial backoff")
	}
	if policy.backoff_multiplier == 0 {
		return error_from_string("backoff multiplier must be positive")
	}
	if options.readiness_probe == nil && policy.readiness_timeout > 0 {
		return error_from_string("readiness timeout requires a readiness probe")
	}
	return {}
}

next_backoff :: proc(current: time.Duration, policy: Policy) -> time.Duration {
	if current <= 0 || policy.backoff_multiplier <= 1 {
		return current
	}
	maximum := policy.maximum_backoff
	if maximum <= 0 {
		return 0
	}
	multiplier := time.Duration(policy.backoff_multiplier)
	if current > maximum / multiplier {
		return maximum
	}
	result := current * multiplier
	if result > maximum {
		return maximum
	}
	return result
}

wait_backoff :: proc(control: ^Control, duration, poll_interval: time.Duration) -> bool {
	if duration <= 0 {
		return !stop_requested(control)
	}
	remaining := duration
	for remaining > 0 {
		if stop_requested(control) {
			return false
		}
		slice := poll_interval
		if slice > remaining {
			slice = remaining
		}
		time.sleep(slice)
		remaining -= slice
	}
	return !stop_requested(control)
}

terminate_child :: proc(
	process: os.Process,
	options: ^Options,
	stats: ^Stats,
	generation: int,
) -> (state: os.Process_State, error: Error) {
	stats.termination_requests += 1
	emit(options, {kind = .Child_Terminating, pid = process.pid, generation = generation})
	terminate_error := os.process_terminate(process)
	if terminate_error == nil {
		state, _ = os.process_wait(process, options.policy.termination_grace)
	} else {
		state, _ = os.process_wait(process, 0)
	}
	if state.exited {
		return
	}
	kill_error := os.process_kill(process)
	if kill_error != nil {
		state, _ = os.process_wait(process, 0)
		if state.exited {
			return
		}
		return state, error_from_os("could not kill supervised child", kill_error)
	}
	stats.forced_kills += 1
	emit(options, {kind = .Child_Killed, pid = process.pid, generation = generation})
	wait_error: os.Error
	state, wait_error = os.process_wait(process)
	if wait_error != nil {
		return state, error_from_os("could not reap supervised child", wait_error)
	}
	return
}

run :: proc(control: ^Control, requested_options: Options) -> (stats: Stats, error: Error) {
	options := requested_options
	validation_error := options_validate(options)
	if validation_error.message != nil {
		return stats, validation_error
	}

	generation := 0
	consecutive_restarts := 0
	backoff := options.policy.initial_backoff
	for {
		if stop_requested(control) {
			stats.stop_was_requested = true
			emit(&options, {kind = .Stopped})
			return
		}

		process: os.Process
		start_error: os.Error
		process, start_error = process_start.start({
			working_dir = options.working_dir,
			command = options.command,
			env = options.env,
			stdin = options.stdin,
			stdout = options.stdout,
			stderr = options.stderr,
		})
		if start_error != nil {
			stats.start_failures += 1
			if options.policy.restart == .Never ||
			   (options.policy.maximum_consecutive_restarts > 0 &&
			    consecutive_restarts >= options.policy.maximum_consecutive_restarts) {
				return stats, error_from_os("could not start supervised child", start_error)
			}
			emit(&options, {kind = .Restart_Scheduled, restart_delay = backoff})
			if !wait_backoff(control, backoff, options.policy.poll_interval) {
				stats.stop_was_requested = true
				emit(&options, {kind = .Stopped})
				return
			}
			consecutive_restarts += 1
			stats.restarts += 1
			backoff = next_backoff(backoff, options.policy)
			continue
		}

		generation += 1
		stats.children_started += 1
		stats.last_pid = process.pid
		started_at := time.tick_now()
		ready := options.readiness_probe == nil
		if ready {
			stats.children_ready += 1
		}
		emit(&options, {
			kind = .Child_Started,
			pid = process.pid,
			generation = generation,
			ready = ready,
		})
		if ready {
			emit(&options, {
				kind = .Child_Ready,
				pid = process.pid,
				generation = generation,
				ready = true,
			})
		}

		state: os.Process_State
		readiness_timed_out := false
		for {
			wait_error: os.Error
			state, wait_error = os.process_wait(process, 0)
			if wait_error != nil && !os_error_is_timeout(wait_error) {
				_, cleanup_error := terminate_child(process, &options, &stats, generation)
				error_free(&cleanup_error)
				return stats, error_from_os("could not inspect supervised child", wait_error)
			}
			if state.exited {
				break
			}
			if !ready && options.readiness_probe(
				options.user_data,
				process.pid,
				generation,
			) {
				ready = true
				stats.children_ready += 1
				emit(&options, {
					kind = .Child_Ready,
					pid = process.pid,
					generation = generation,
					ready = true,
				})
			}
			elapsed := time.tick_diff(started_at, time.tick_now())
			if !ready && options.policy.readiness_timeout > 0 &&
			   elapsed >= options.policy.readiness_timeout {
				readiness_timed_out = true
				stats.readiness_timeouts += 1
				state, error = terminate_child(process, &options, &stats, generation)
				if error.message != nil {
					return
				}
				break
			}
			if stop_requested(control) {
				stats.stop_was_requested = true
				emit(&options, {
					kind = .Stop_Requested,
					pid = process.pid,
					generation = generation,
					ready = ready,
				})
				state, error = terminate_child(process, &options, &stats, generation)
				if error.message != nil {
					return
				}
				stats.last_exit_code = state.exit_code
				stats.last_child_was_ready = ready
				emit(&options, {
					kind = .Stopped,
					pid = process.pid,
					generation = generation,
					exit_code = state.exit_code,
					ready = ready,
				})
				return
			}
			time.sleep(options.policy.poll_interval)
		}

		uptime := time.tick_diff(started_at, time.tick_now())
		stats.last_exit_code = state.exit_code
		stats.last_child_was_ready = ready
		child_success := state.success && !readiness_timed_out && ready
		if child_success {
			stats.clean_exits += 1
		} else {
			stats.failed_exits += 1
		}
		emit(&options, {
			kind = .Child_Exited,
			pid = process.pid,
			generation = generation,
			exit_code = state.exit_code,
			success = child_success,
			ready = ready,
		})

		should_restart := options.policy.restart == .Always ||
		                  (options.policy.restart == .On_Failure && !child_success)
		if !should_restart {
			if child_success {
				emit(&options, {
					kind = .Stopped,
					pid = process.pid,
					generation = generation,
					exit_code = state.exit_code,
					success = true,
					ready = ready,
				})
				return
			}
			return stats, error_from_string("supervised child exited unsuccessfully")
		}

		if options.policy.stable_after > 0 && uptime >= options.policy.stable_after {
			consecutive_restarts = 0
			backoff = options.policy.initial_backoff
		}
		if options.policy.maximum_consecutive_restarts > 0 &&
		   consecutive_restarts >= options.policy.maximum_consecutive_restarts {
			return stats, error_from_string("supervised child exceeded the consecutive restart limit")
		}
		emit(&options, {
			kind = .Restart_Scheduled,
			pid = process.pid,
			generation = generation,
			exit_code = state.exit_code,
			ready = ready,
			restart_delay = backoff,
		})
		if !wait_backoff(control, backoff, options.policy.poll_interval) {
			stats.stop_was_requested = true
			emit(&options, {kind = .Stopped})
			return
		}
		consecutive_restarts += 1
		stats.restarts += 1
		backoff = next_backoff(backoff, options.policy)
	}
}

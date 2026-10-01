package workflow_pbt_test

import "core:bytes"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import sc "statecharts:statecharts"
import workflow_isolate "../temporal/workflow/isolate"

workflow_isolation_frame_property :: proc(t: ^pbt.T) -> pbt.Result {
	run_id := pbt.draw(t, pbt.non_empty_byte_array(96))
	payload := pbt.draw(t, pbt.non_empty_byte_array(8_192))
	request_high := u64(pbt.draw(t, pbt.int_range(0, 0x7fff_ffff)))
	request_low := u64(pbt.draw(t, pbt.int_range(0, 0x7fff_ffff)))
	request_id := request_high << 32 | request_low
	if pbt.draw(t, pbt.boolean()) {
		request_id |= u64(1) << 63
	}
	kinds := [?]workflow_isolate.Kind{
		.Activation,
		.Completion,
		.Completion_Metadata,
		.Diagnostic,
	}
	kind := kinds[pbt.draw(t, pbt.int_range(0, len(kinds) - 1))]
	has_zero := false
	for value in payload {
		if value == 0 {
			has_zero = true
			break
		}
	}
	pbt.cover(t, kind == .Completion, 15, "completion frame")
	pbt.cover(t, kind == .Diagnostic, 15, "diagnostic frame")
	pbt.cover(t, request_id & (u64(1) << 63) != 0, 35, "high-bit correlation ID")
	pbt.cover(t, len(payload) >= 4_096, 25, "large frame payload")
	pbt.cover(t, has_zero, 35, "embedded zero payload byte")

	frame := workflow_isolate.Frame{
		kind = kind,
		request_id = request_id,
		run_id = run_id,
		payload = payload,
	}
	encoded, encode_error := workflow_isolate.encode(frame)
	defer delete(encoded)
	if encode_error.kind != .None {
		return pbt.fail("valid generated isolation frame did not encode")
	}
	decoded, decode_error := workflow_isolate.decode(encoded)
	defer workflow_isolate.owned_frame_free(&decoded)
	if decode_error.kind != .None || decoded.frame.kind != kind ||
	   decoded.frame.request_id != request_id ||
	   !bytes.equal(decoded.frame.run_id, run_id) ||
	   !bytes.equal(decoded.frame.payload, payload) {
		return pbt.fail("generated isolation frame did not roundtrip exactly")
	}
	return pbt.pass()
}

@(test)
workflow_isolation_frames_roundtrip_generated_binary_payloads :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow isolation frames roundtrip generated binary payloads",
		workflow_isolation_frame_property,
		{num_tests = 500, seed = 0x4953_4f4c_4154_4544},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

workflow_isolation_diagnostic_budget_property :: proc(t: ^pbt.T) -> pbt.Result {
	maximum := pbt.draw(t, pbt.int_range(1, 1_048_576))
	payloads := pbt.draw(t, pbt.array(pbt.int_range(0, 65_536), 0, 128))
	consumed := 0
	reference: u64
	rejected := false
	for payload_bytes in payloads {
		updated, allowed := workflow_isolate.diagnostic_payload_budget_consume(
			consumed,
			payload_bytes,
			maximum,
		)
		reference_allowed := reference + u64(payload_bytes) <= u64(maximum)
		if allowed != reference_allowed {
			return pbt.fail("diagnostic byte budget diverged from wide reference arithmetic")
		}
		if allowed {
			reference += u64(payload_bytes)
			if updated != int(reference) {
				return pbt.fail("diagnostic byte budget returned the wrong cumulative total")
			}
			consumed = updated
		} else {
			rejected = true
			if updated != consumed {
				return pbt.fail("rejected diagnostic changed the authoritative byte total")
			}
		}
	}
	force_boundary := pbt.draw(t, pbt.boolean())
	if force_boundary {
		updated, allowed := workflow_isolate.diagnostic_payload_budget_consume(
			consumed,
			maximum - consumed,
			maximum,
		)
		if !allowed || updated != maximum {
			return pbt.fail("diagnostic byte budget rejected its exact remaining capacity")
		}
		consumed = updated
	}
	pbt.cover(t, consumed == maximum, 35, "exact byte-budget boundary")
	pbt.cover(t, rejected, 70, "diagnostic rejected by byte budget")
	return pbt.pass()
}

@(test)
workflow_executor_diagnostic_byte_budget_matches_wide_model :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow executor diagnostic byte budget matches wide model",
		workflow_isolation_diagnostic_budget_property,
		{num_tests = 1000, seed = 0x4449_4147_4255_4447},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

Lifecycle_Observation :: struct {
	effect: workflow_isolate.Lifecycle_Effect,
	seen:   bool,
}

lifecycle_observation :: proc(ctx: rawptr) -> ^Lifecycle_Observation {
	return cast(^Lifecycle_Observation)sc.user_context(ctx)
}

record_start :: proc(ctx, _: rawptr) {
	observation := lifecycle_observation(ctx)
	observation.effect = .Start_Executor
	observation.seen = true
}

record_complete :: proc(ctx, _: rawptr) {
	observation := lifecycle_observation(ctx)
	observation.effect = .Complete_Activation
	observation.seen = true
}

record_fail_abort :: proc(ctx, _: rawptr) {
	observation := lifecycle_observation(ctx)
	observation.effect = .Fail_And_Abort
	observation.seen = true
}

record_fail_awaiting :: proc(ctx, _: rawptr) {
	observation := lifecycle_observation(ctx)
	observation.effect = .Fail_Awaiting_Eviction
	observation.seen = true
}

record_evict_shutdown :: proc(ctx, _: rawptr) {
	observation := lifecycle_observation(ctx)
	observation.effect = .Complete_Eviction_And_Shutdown
	observation.seen = true
}

record_evict_ack :: proc(ctx, _: rawptr) {
	observation := lifecycle_observation(ctx)
	observation.effect = .Acknowledge_Eviction
	observation.seen = true
}

record_reject :: proc(ctx, _: rawptr) {
	observation := lifecycle_observation(ctx)
	observation.effect = .Reject
	observation.seen = true
}

lifecycle_states := [?]sc.State_Def(workflow_isolate.Lifecycle_State){
	{id = .Absent},
	{id = .Live},
	{id = .Failed_Awaiting_Eviction},
}

lifecycle_transitions := [?]sc.Transition_Def(
	workflow_isolate.Lifecycle_State,
	workflow_isolate.Lifecycle_Event,
){
	sc.on(workflow_isolate.Lifecycle_State.Absent, workflow_isolate.Lifecycle_Event.Initialize, workflow_isolate.Lifecycle_State.Live, action = record_start),
	sc.on(workflow_isolate.Lifecycle_State.Absent, workflow_isolate.Lifecycle_Event.Activation_Succeeded, workflow_isolate.Lifecycle_State.Absent, action = record_reject),
	sc.on(workflow_isolate.Lifecycle_State.Absent, workflow_isolate.Lifecycle_Event.Activation_Failed, workflow_isolate.Lifecycle_State.Absent, action = record_reject),
	sc.on(workflow_isolate.Lifecycle_State.Absent, workflow_isolate.Lifecycle_Event.Remove_From_Cache, workflow_isolate.Lifecycle_State.Absent, action = record_evict_ack),
	sc.on(workflow_isolate.Lifecycle_State.Live, workflow_isolate.Lifecycle_Event.Initialize, workflow_isolate.Lifecycle_State.Failed_Awaiting_Eviction, action = record_fail_abort),
	sc.on(workflow_isolate.Lifecycle_State.Live, workflow_isolate.Lifecycle_Event.Activation_Succeeded, workflow_isolate.Lifecycle_State.Live, action = record_complete),
	sc.on(workflow_isolate.Lifecycle_State.Live, workflow_isolate.Lifecycle_Event.Activation_Failed, workflow_isolate.Lifecycle_State.Failed_Awaiting_Eviction, action = record_fail_abort),
	sc.on(workflow_isolate.Lifecycle_State.Live, workflow_isolate.Lifecycle_Event.Remove_From_Cache, workflow_isolate.Lifecycle_State.Absent, action = record_evict_shutdown),
	sc.on(workflow_isolate.Lifecycle_State.Failed_Awaiting_Eviction, workflow_isolate.Lifecycle_Event.Initialize, workflow_isolate.Lifecycle_State.Failed_Awaiting_Eviction, action = record_fail_awaiting),
	sc.on(workflow_isolate.Lifecycle_State.Failed_Awaiting_Eviction, workflow_isolate.Lifecycle_Event.Activation_Succeeded, workflow_isolate.Lifecycle_State.Failed_Awaiting_Eviction, action = record_fail_awaiting),
	sc.on(workflow_isolate.Lifecycle_State.Failed_Awaiting_Eviction, workflow_isolate.Lifecycle_Event.Activation_Failed, workflow_isolate.Lifecycle_State.Failed_Awaiting_Eviction, action = record_fail_awaiting),
	sc.on(workflow_isolate.Lifecycle_State.Failed_Awaiting_Eviction, workflow_isolate.Lifecycle_Event.Remove_From_Cache, workflow_isolate.Lifecycle_State.Absent, action = record_evict_ack),
}

active_lifecycle_state :: proc(
	machine: ^sc.Instance(workflow_isolate.Lifecycle_State, workflow_isolate.Lifecycle_Event),
) -> (workflow_isolate.Lifecycle_State, bool) {
	for state in workflow_isolate.Lifecycle_State {
		if sc.is_active(machine, state) {
			return state, true
		}
	}
	return {}, false
}

workflow_isolation_lifecycle_property :: proc(t: ^pbt.T) -> pbt.Result {
	events := pbt.draw(t, pbt.array(pbt.int_range(0, 3), 0, 64))
	for event_index in 0 ..< 4 {
		covered := false
		for encoded_event in events {
			if encoded_event == event_index {
				covered = true
				break
			}
		}
		pbt.cover(t, covered, 35, "each lifecycle event")
	}

	chart: sc.Chart(workflow_isolate.Lifecycle_State, workflow_isolate.Lifecycle_Event)
	compiled := sc.compile(
		&chart,
		sc.define(
			workflow_isolate.Lifecycle_State.Absent,
			lifecycle_states[:],
			lifecycle_transitions[:],
		),
	)
	defer sc.destroy_compile_result(&compiled)
	defer sc.destroy_chart(&chart)
	if !compiled.ok {
		return pbt.fail("Workflow executor lifecycle statechart did not compile")
	}
	machine: sc.Instance(workflow_isolate.Lifecycle_State, workflow_isolate.Lifecycle_Event)
	if !sc.init(&machine, &chart) {
		return pbt.fail("Workflow executor lifecycle statechart did not initialize")
	}
	defer sc.destroy_instance(&machine)
	entered := sc.enter_initial(&machine)
	sc.destroy_dispatch_result(&entered)

	implementation_state := workflow_isolate.Lifecycle_State.Absent
	saw_failed_await := false
	for encoded_event in events {
		event := workflow_isolate.Lifecycle_Event(encoded_event)
		expected_state, expected_effect := workflow_isolate.lifecycle_step(
			implementation_state,
			event,
		)
		observation := Lifecycle_Observation{}
		dispatched := sc.dispatch_run_to_completion(
			&machine,
			sc.event(event),
			&observation,
		)
		if dispatched.status == .Error || dispatched.status == .Conflict {
			sc.destroy_dispatch_result(&dispatched)
			return pbt.fail("Workflow executor lifecycle statechart rejected a trace")
		}
		sc.destroy_dispatch_result(&dispatched)
		chart_state, state_found := active_lifecycle_state(&machine)
		if !state_found || !observation.seen ||
		   chart_state != expected_state || observation.effect != expected_effect {
			return pbt.fail("Workflow executor implementation diverged from its lifecycle statechart")
		}
		if implementation_state == .Failed_Awaiting_Eviction &&
		   event != .Remove_From_Cache && expected_state != .Failed_Awaiting_Eviction {
			return pbt.fail("failed Workflow executor was replaced before Core eviction")
		}
		if expected_effect == .Fail_Awaiting_Eviction {
			saw_failed_await = true
		}
		implementation_state = expected_state
	}
	pbt.cover(t, saw_failed_await, 20, "activation while awaiting eviction")
	return pbt.pass()
}

@(test)
workflow_executor_lifecycle_matches_statechart_model :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow executor lifecycle matches statechart model",
		workflow_isolation_lifecycle_property,
		{num_tests = 1000, seed = 0x4953_4f4c_4d47_5254},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

workflow_isolation_capacity_property :: proc(t: ^pbt.T) -> pbt.Result {
	capacity := pbt.draw(t, pbt.int_range(1, 8))
	encoded_events := pbt.draw(t, pbt.array(pbt.int_range(0, 31), 0, 128))
	states: [8]workflow_isolate.Lifecycle_State
	generations: [8]u64
	next_generation: u64
	route_count := 0
	capacity_rejections := 0
	saw_failed_route := false

	for encoded in encoded_events {
		route_index := encoded / 4
		event := workflow_isolate.Lifecycle_Event(encoded % 4)
		before_state := states[route_index]
		before_generation := generations[route_index]
		if before_state == .Absent && event == .Initialize &&
		   !workflow_isolate.capacity_allows_start(route_count, capacity) {
			capacity_rejections += 1
		} else {
			next_state, _ := workflow_isolate.lifecycle_step(before_state, event)
			states[route_index] = next_state
			if before_state == .Absent && next_state == .Live {
				next_generation += 1
				generations[route_index] = next_generation
				route_count += 1
			} else if before_state != .Absent && next_state == .Absent {
				generations[route_index] = 0
				route_count -= 1
			}
		}
		if before_state == .Failed_Awaiting_Eviction &&
		   event != .Remove_From_Cache &&
		   (states[route_index] != .Failed_Awaiting_Eviction ||
		    generations[route_index] != before_generation) {
			return pbt.fail("failed route changed generation before eviction")
		}
		if states[route_index] == .Failed_Awaiting_Eviction {
			saw_failed_route = true
		}

		observed_routes := 0
		for index in 0 ..< len(states) {
			if states[index] != .Absent {
				observed_routes += 1
				if generations[index] == 0 {
					return pbt.fail("owned route has no executor generation")
				}
			}
		}
		if route_count != observed_routes || route_count < 0 || route_count > capacity {
			return pbt.fail("Workflow executor route capacity invariant was violated")
		}
	}
	pbt.cover(t, capacity == 1, 10, "single executor capacity")
	pbt.cover(t, capacity_rejections > 0, 25, "capacity rejection")
	pbt.cover(t, saw_failed_route, 25, "failed route retained until eviction")
	return pbt.pass()
}

@(test)
workflow_executor_routes_remain_bounded_and_generation_fenced :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow executor routes remain bounded and generation fenced",
		workflow_isolation_capacity_property,
		{num_tests = 1000, seed = 0x4953_4f4c_4341_5041},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

workflow_executor_crash_loop_breaker_property :: proc(t: ^pbt.T) -> pbt.Result {
	maximum := pbt.draw(t, pbt.int_range(0, 16))
	failures := pbt.draw(t, pbt.int_range(0, 24))
	cooldown_millis := pbt.draw(t, pbt.int_range(1, 10_000))
	elapsed_millis := pbt.draw(t, pbt.int_range(0, 20_000))
	policy := workflow_isolate.Crash_Loop_Policy{
		maximum_consecutive_failures = maximum,
		cooldown = time.Duration(cooldown_millis) * time.Millisecond,
	}
	elapsed := time.Duration(elapsed_millis) * time.Millisecond
	allowed, reset := workflow_isolate.crash_loop_allows_start(
		failures,
		elapsed,
		policy,
	)
	pbt.cover(t, maximum == 0, 5, "breaker disabled")
	pbt.cover(t, maximum > 0 && failures >= maximum && elapsed < policy.cooldown, 15, "breaker open")
	pbt.cover(t, maximum > 0 && failures >= maximum && elapsed >= policy.cooldown, 15, "cooldown trial")
	if maximum == 0 || failures < maximum {
		if !allowed || reset {
			return pbt.fail("crash-loop breaker rejected below its threshold")
		}
	} else if elapsed < policy.cooldown {
		if allowed || reset {
			return pbt.fail("crash-loop breaker admitted a child before cooldown")
		}
	} else if !allowed || !reset {
		return pbt.fail("crash-loop breaker did not admit one post-cooldown trial")
	}
	return pbt.pass()
}

@(test)
workflow_executor_crash_loop_breaker_obeys_threshold_and_cooldown :: proc(
	t: ^testing.T,
) {
	result := pbt.check(
		"Workflow executor crash-loop breaker obeys threshold and cooldown",
		workflow_executor_crash_loop_breaker_property,
		{num_tests = 1000, seed = 0x4953_4f4c_4352_4153},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

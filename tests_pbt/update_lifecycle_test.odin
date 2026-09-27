package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import sc "statecharts:statecharts"

Update_Lifecycle_State :: enum {
	Running,
	Handling,
	Waiting_Handlers,
	Completed,
	Canceled,
}

Update_Lifecycle_Event :: enum {
	Start_Update,
	Handler_Finished,
	Main_Returned,
	Await_Handlers,
	Cancel_Workflow,
	Late_Resolution,
}

Update_Lifecycle_Observation :: struct {
	terminal_commands: int,
	update_responses:  int,
	abandon_warnings:  int,
}

update_lifecycle_observation :: proc(ctx: rawptr) -> ^Update_Lifecycle_Observation {
	return cast(^Update_Lifecycle_Observation)sc.user_context(ctx)
}

record_update_response :: proc(ctx, event: rawptr) {
	_ = event
	update_lifecycle_observation(ctx).update_responses += 1
}

record_clean_close :: proc(ctx, event: rawptr) {
	_ = event
	update_lifecycle_observation(ctx).terminal_commands += 1
}

record_abandoned_close :: proc(ctx, event: rawptr) {
	_ = event
	observation := update_lifecycle_observation(ctx)
	observation.terminal_commands += 1
	observation.abandon_warnings += 1
}

record_update_response_and_close :: proc(ctx, event: rawptr) {
	_ = event
	observation := update_lifecycle_observation(ctx)
	observation.update_responses += 1
	observation.terminal_commands += 1
}

update_lifecycle_states := [?]sc.State_Def(Update_Lifecycle_State){
	{id = .Running},
	{id = .Handling},
	{id = .Waiting_Handlers},
	{id = .Completed},
	{id = .Canceled},
}

update_lifecycle_transitions := [?]sc.Transition_Def(
	Update_Lifecycle_State,
	Update_Lifecycle_Event,
){
	sc.on(Update_Lifecycle_State.Running, Update_Lifecycle_Event.Start_Update, Update_Lifecycle_State.Handling),
	sc.on(Update_Lifecycle_State.Running, Update_Lifecycle_Event.Main_Returned, Update_Lifecycle_State.Completed, action = record_clean_close),
	sc.on(Update_Lifecycle_State.Running, Update_Lifecycle_Event.Cancel_Workflow, Update_Lifecycle_State.Canceled, action = record_clean_close),
	sc.on(Update_Lifecycle_State.Handling, Update_Lifecycle_Event.Handler_Finished, Update_Lifecycle_State.Running, action = record_update_response),
	sc.on(Update_Lifecycle_State.Handling, Update_Lifecycle_Event.Main_Returned, Update_Lifecycle_State.Completed, action = record_abandoned_close),
	sc.on(Update_Lifecycle_State.Handling, Update_Lifecycle_Event.Await_Handlers, Update_Lifecycle_State.Waiting_Handlers),
	sc.on(Update_Lifecycle_State.Handling, Update_Lifecycle_Event.Cancel_Workflow, Update_Lifecycle_State.Canceled, action = record_abandoned_close),
	sc.on(
		Update_Lifecycle_State.Waiting_Handlers,
		Update_Lifecycle_Event.Handler_Finished,
		Update_Lifecycle_State.Completed,
		action = record_update_response_and_close,
	),
	sc.on(Update_Lifecycle_State.Waiting_Handlers, Update_Lifecycle_Event.Cancel_Workflow, Update_Lifecycle_State.Canceled, action = record_abandoned_close),
}

Update_Lifecycle_Run :: struct {
	state:       Update_Lifecycle_State,
	observation: Update_Lifecycle_Observation,
}

run_update_lifecycle :: proc(events: []int) -> (run: Update_Lifecycle_Run, ok: bool) {
	chart: sc.Chart(Update_Lifecycle_State, Update_Lifecycle_Event)
	compiled := sc.compile(
		&chart,
		sc.define(
			Update_Lifecycle_State.Running,
			update_lifecycle_states[:],
			update_lifecycle_transitions[:],
		),
	)
	defer sc.destroy_compile_result(&compiled)
	defer sc.destroy_chart(&chart)
	if !compiled.ok {
		return {}, false
	}
	machine: sc.Instance(Update_Lifecycle_State, Update_Lifecycle_Event)
	if !sc.init(&machine, &chart) {
		return {}, false
	}
	defer sc.destroy_instance(&machine)
	entered := sc.enter_initial(&machine)
	sc.destroy_dispatch_result(&entered)
	for encoded_event in events {
		was_closed := sc.is_active(&machine, Update_Lifecycle_State.Completed) ||
			sc.is_active(&machine, Update_Lifecycle_State.Canceled)
		before := run.observation
		event := Update_Lifecycle_Event(encoded_event)
		dispatched := sc.dispatch_run_to_completion(
			&machine,
			sc.event(event),
			&run.observation,
		)
		if dispatched.status == .Error || dispatched.status == .Conflict {
			sc.destroy_dispatch_result(&dispatched)
			return {}, false
		}
		sc.destroy_dispatch_result(&dispatched)
		if run.observation.terminal_commands > 1 ||
		   run.observation.abandon_warnings > run.observation.terminal_commands {
			return {}, false
		}
		if was_closed && run.observation != before {
			return {}, false
		}
	}
	for state in Update_Lifecycle_State {
		if sc.is_active(&machine, state) {
			run.state = state
			return run, true
		}
	}
	return {}, false
}

update_lifecycle_property :: proc(t: ^pbt.T) -> pbt.Result {
	events := pbt.draw(t, pbt.array(pbt.int_range(0, 5), 0, 32))
	event_labels := [?]string{
		"start update",
		"handler finished",
		"main returned",
		"await handlers",
		"cancel Workflow",
		"late resolution",
	}
	for event_index in 0 ..< 6 {
		covered := false
		for event in events {
			if event == event_index {
				covered = true
				break
			}
		}
		pbt.cover(t, covered, 20.0, event_labels[event_index])
	}
	first, first_ok := run_update_lifecycle(events)
	second, second_ok := run_update_lifecycle(events)
	if !first_ok || !second_ok {
		return pbt.fail("Update lifecycle statechart rejected a generated trace")
	}
	if first != second {
		return pbt.fail("Update lifecycle statechart was not deterministic")
	}
	if first.state == .Waiting_Handlers && first.observation.terminal_commands != 0 {
		return pbt.fail("explicit handler await emitted a terminal command too early")
	}
	if (first.state == .Completed || first.state == .Canceled) &&
	   first.observation.terminal_commands != 1 {
		return pbt.fail("closed lifecycle did not emit exactly one terminal command")
	}
	return pbt.pass()
}

@(test)
workflow_update_close_lifecycle_matches_statechart_model :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow Update close lifecycle remains deterministic",
		update_lifecycle_property,
		{num_tests = 1000, seed = 0x5550_4441_5445_5254},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

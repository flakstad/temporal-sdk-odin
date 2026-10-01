package workflow_pbt_test

import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import workflow "../temporal/workflow"

Workflow_Log_Property_Capture :: struct {
	calls:                     int,
	valid:                     bool,
	expected_level:            workflow.Log_Level,
	expected_integer:          i64,
	expected_float:            f64,
	expected_boolean:          bool,
	expected_replaying:        bool,
	expected_replaying_history: bool,
}

workflow_log_property_capture :: proc(user_data: rawptr, record: ^workflow.Log_Record) {
	state := cast(^Workflow_Log_Property_Capture)user_data
	if state == nil || record == nil {
		return
	}
	state.calls += 1
	state.valid = len(record.fields) == 4 &&
	              record.level == state.expected_level &&
	              record.message == "generated workflow log" &&
	              record.fields[0].key == "text" &&
	              record.fields[0].kind == .String &&
	              record.fields[0].string_value == "value" &&
	              record.fields[1].key == "integer" &&
	              record.fields[1].kind == .Integer &&
	              record.fields[1].integer_value == state.expected_integer &&
	              record.fields[2].key == "float" &&
	              record.fields[2].kind == .Float &&
	              record.fields[2].float_value == state.expected_float &&
	              record.fields[3].key == "boolean" &&
	              record.fields[3].kind == .Boolean &&
	              record.fields[3].boolean_value == state.expected_boolean &&
	              record.namespace == "property-namespace" &&
	              record.workflow_id == "property-workflow" &&
	              record.run_id == "property-run" &&
	              record.workflow_type == "property_workflow" &&
	              record.task_queue == "property-queue" &&
	              record.history_length == 37 &&
	              record.replaying == state.expected_replaying &&
	              record.replaying_history_events == state.expected_replaying_history &&
	              record.has_timestamp &&
	              time.to_unix_nanoseconds(record.timestamp) == 1_800_000_000_123_000_000
}

workflow_logging_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	replaying := pbt.draw(t, pbt.boolean())
	replaying_history := replaying && pbt.draw(t, pbt.boolean())
	log_replay := pbt.draw(t, pbt.boolean())
	level := workflow.Log_Level(pbt.draw(t, pbt.int_range(0, 4)))
	integer := i64(pbt.draw(t, pbt.int_range(-1_000_000, 1_000_000)))
	float_numerator := pbt.draw(t, pbt.int_range(-100_000, 100_000))
	float_value := f64(float_numerator) / 1000.0
	boolean := pbt.draw(t, pbt.boolean())
	pbt.cover(t, replaying_history && !log_replay, 8, "history replay suppressed")
	pbt.cover(t, replaying_history && log_replay, 8, "history replay explicitly logged")
	pbt.cover(t, replaying && !replaying_history, 15, "query-only replay logged")

	capture := Workflow_Log_Property_Capture{
		expected_level = level,
		expected_integer = integer,
		expected_float = float_value,
		expected_boolean = boolean,
		expected_replaying = replaying,
		expected_replaying_history = replaying_history,
	}
	runtime := workflow.Fiber_Runtime{logger = {
		user_data = &capture,
		emit = workflow_log_property_capture,
		log_replay_history_events = log_replay,
	}}
	instance := workflow.Fiber_Instance{
		runtime = &runtime,
		namespace = "property-namespace",
		workflow_id = "property-workflow",
		run_id = "property-run",
		workflow_type = "property_workflow",
		task_queue = "property-queue",
		activation_is_replaying = replaying,
		activation_is_replaying_history_events = replaying_history,
		activation_history_length = 37,
		activation_time = time.unix(1_800_000_000, 123_000_000),
		has_activation_time = true,
	}
	ctx := workflow.Fiber_Context{instance = &instance}
	fields := [?]workflow.Log_Field{
		workflow.log_field_string("text", "value"),
		workflow.log_field_i64("integer", integer),
		workflow.log_field_f64("float", float_value),
		workflow.log_field_bool("boolean", boolean),
	}
	workflow.log(&ctx, level, "generated workflow log", fields[:])
	expected_calls := replaying_history && !log_replay ? 0 : 1
	if capture.calls != expected_calls {
		return pbt.fail("Workflow log replay suppression changed")
	}
	if expected_calls == 1 && !capture.valid {
		return pbt.fail("Workflow log metadata or typed fields changed")
	}
	return pbt.pass()
}

@(test)
workflow_logging_preserves_fields_and_replay_policy :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow logging preserves typed fields and replay policy",
		workflow_logging_replay_property,
		{num_tests = 500, seed = 0x5746_4c4f_4747_494e},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

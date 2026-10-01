package workflow_pbt_test

import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import workflow "../temporal/workflow"

Workflow_Metric_Property_Capture :: struct {
	calls: int,
	metadata_valid: bool,
}

workflow_metric_property_capture :: proc(
	user_data: rawptr,
	record: ^workflow.Metric_Record,
) {
	capture := cast(^Workflow_Metric_Property_Capture)user_data
	if capture == nil || record == nil do return
	capture.calls += 1
	capture.metadata_valid = record.namespace == "generated-namespace" &&
	                         record.workflow_id == "generated-workflow" &&
	                         record.run_id == "generated-run" &&
	                         record.workflow_type == "generated-type" &&
	                         record.task_queue == "generated-queue"
}

workflow_metric_replay_property :: proc(t: ^pbt.T) -> pbt.Result {
	kind := pbt.draw(t, pbt.int_range(0, 6))
	is_replaying := pbt.draw(t, pbt.boolean())
	replays_history := is_replaying && pbt.draw(t, pbt.boolean())
	empty_name := pbt.draw(t, pbt.boolean())
	empty_attribute_key := pbt.draw(t, pbt.boolean())
	signed_value := pbt.draw(t, pbt.int_range(-1000, 1000))

	pbt.cover(t, replays_history, 20.0, "history replay")
	pbt.cover(t, is_replaying && !replays_history, 20.0, "query-only replay")
	pbt.cover(t, empty_name, 30.0, "invalid empty name")
	pbt.cover(t, signed_value < 0, 30.0, "negative generated value")

	capture := Workflow_Metric_Property_Capture{}
	runtime := workflow.Fiber_Runtime{
		metric_emitter = {
			user_data = &capture,
			emit = workflow_metric_property_capture,
		},
	}
	instance := workflow.Fiber_Instance{
		runtime = &runtime,
		namespace = "generated-namespace",
		workflow_id = "generated-workflow",
		run_id = "generated-run",
		workflow_type = "generated-type",
		task_queue = "generated-queue",
		activation_is_replaying = is_replaying,
		activation_is_replaying_history_events = replays_history,
	}
	ctx := workflow.Fiber_Context{instance = &instance}
	name := "generated_metric"
	if empty_name do name = ""
	attribute_key := "generated_key"
	if empty_attribute_key do attribute_key = ""
	attributes := []workflow.Metric_Attribute{
		workflow.metric_attribute_i64(attribute_key, i64(signed_value)),
	}

	value_valid := true
	switch kind {
	case 0:
		workflow.metric_counter_add(&ctx, name, u64(abs(signed_value)), attributes)
	case 1:
		workflow.metric_histogram_record_integer(&ctx, name, u64(abs(signed_value)), attributes)
	case 2:
		value_valid = signed_value >= 0
		workflow.metric_histogram_record_float(&ctx, name, f64(signed_value) / 10, attributes)
	case 3:
		value_valid = signed_value >= 0
		workflow.metric_histogram_record_duration(
			&ctx,
			name,
			time.Duration(signed_value) * time.Millisecond,
			attributes,
		)
	case 4:
		workflow.metric_gauge_record_integer(&ctx, name, u64(abs(signed_value)), attributes)
	case 5:
		workflow.metric_gauge_record_float(&ctx, name, f64(signed_value) / 10, attributes)
	case 6:
		workflow.metric_up_down_counter_add(&ctx, name, i64(signed_value), attributes)
	}
	expected := !replays_history && !empty_name && !empty_attribute_key && value_valid
	if (capture.calls == 1) != expected {
		return pbt.fail("Workflow metric replay/validation policy changed")
	}
	if capture.calls == 1 && !capture.metadata_valid {
		return pbt.fail("Workflow metric activation metadata changed")
	}
	return pbt.pass()
}

@(test)
workflow_metrics_obey_replay_and_validation_policy :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow metrics obey replay and validation policy",
		workflow_metric_replay_property,
		{num_tests = 500, seed = 0x4d45_5452_4943_5350},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

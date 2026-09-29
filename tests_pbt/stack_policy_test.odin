package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import workflow "../temporal/workflow"

generated_stack_size :: proc(t: ^pbt.T, mode: int) -> int {
	switch mode {
	case 0:
		return 0
	case 1:
		return workflow.MIN_FIBER_STACK_SIZE
	case 2:
		return pbt.draw(
			t,
			pbt.int_range(workflow.MIN_FIBER_STACK_SIZE, workflow.MAX_FIBER_STACK_SIZE),
		)
	case 3:
		return pbt.draw(t, pbt.int_range(-1024, workflow.MIN_FIBER_STACK_SIZE - 1))
	case 4:
		return pbt.draw(
			t,
			pbt.int_range(workflow.MAX_FIBER_STACK_SIZE + 1, workflow.MAX_FIBER_STACK_SIZE + 65536),
		)
	}
	return 0
}

stack_size_is_valid :: proc(value: int) -> bool {
	return value == 0 ||
	       (value >= workflow.MIN_FIBER_STACK_SIZE && value <= workflow.MAX_FIBER_STACK_SIZE)
}

expected_stack_size :: proc(value: int) -> int {
	if value == 0 {
		return workflow.DEFAULT_FIBER_STACK_SIZE
	}
	return ((value + workflow.FIBER_STACK_ALIGNMENT - 1) / workflow.FIBER_STACK_ALIGNMENT) *
	       workflow.FIBER_STACK_ALIGNMENT
}

workflow_stack_policy_property :: proc(t: ^pbt.T) -> pbt.Result {
	field := pbt.draw(t, pbt.int_range(0, 2))
	mode := pbt.draw(t, pbt.int_range(0, 4))
	value := generated_stack_size(t, mode)
	pbt.cover(t, mode == 0, 10.0, "default size")
	pbt.cover(t, mode == 1, 10.0, "minimum size")
	pbt.cover(t, mode == 2, 10.0, "custom valid size")
	pbt.cover(t, mode == 3 && value != 0, 10.0, "below minimum")
	pbt.cover(t, mode == 4, 10.0, "above maximum")
	pbt.cover(t, field == 0, 20.0, "Workflow stack")
	pbt.cover(t, field == 1, 20.0, "Signal handler stack")
	pbt.cover(t, field == 2, 20.0, "Update handler stack")

	options := workflow.Stack_Options{}
	switch field {
	case 0: options.workflow = value
	case 1: options.signal_handler = value
	case 2: options.update_handler = value
	}
	resolved, resolve_error := workflow.stack_options_resolve(options)
	defer workflow.error_free(&resolve_error)
	if !stack_size_is_valid(value) {
		if resolve_error.message == nil {
			return pbt.fail("out-of-bounds Workflow stack size was accepted")
		}
		return pbt.pass()
	}
	if resolve_error.message != nil {
		return pbt.fail("valid Workflow stack size was rejected")
	}
	expected := expected_stack_size(value)
	actual := resolved.workflow
	if field == 1 {
		actual = resolved.signal_handler
	} else if field == 2 {
		actual = resolved.update_handler
	}
	if actual != expected {
		return pbt.fail("Workflow stack size was not normalized to the native alignment")
	}
	if field != 0 && resolved.workflow != workflow.DEFAULT_FIBER_STACK_SIZE {
		return pbt.fail("Workflow stack default changed while resolving another field")
	}
	if field != 1 && resolved.signal_handler != workflow.DEFAULT_FIBER_STACK_SIZE {
		return pbt.fail("Signal stack default changed while resolving another field")
	}
	if field != 2 && resolved.update_handler != workflow.DEFAULT_FIBER_STACK_SIZE {
		return pbt.fail("Update stack default changed while resolving another field")
	}
	return pbt.pass()
}

@(test)
workflow_stack_policy_validates_generated_resource_bounds :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow stack policy validates and aligns generated sizes",
		workflow_stack_policy_property,
		{num_tests = 500, seed = 0x5354_4143_4b50_4f4c},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

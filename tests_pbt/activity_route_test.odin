package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import worker "../temporal/worker"

activity_route_property :: proc(t: ^pbt.T) -> pbt.Result {
	count := pbt.draw(t, pbt.int_range(1, 64))
	prefix := pbt.draw(t, pbt.byte_array(0, 32))
	offset := pbt.draw(t, pbt.int_range(0, count - 1))
	reverse := pbt.draw(t, pbt.boolean())
	pbt.cover(t, count == 1, 1.0, "single Activity route")
	pbt.cover(t, count >= 48, 15.0, "large Activity route table")
	pbt.cover(t, len(prefix) == 0, 1.0, "empty generated token prefix")
	pbt.cover(t, reverse, 35.0, "reverse route removal")
	has_zero := false
	for value in prefix {
		if value == 0 {
			has_zero = true
			break
		}
	}
	pbt.cover(t, has_zero, 1.0, "embedded zero task token byte")

	routes := make(worker.Activity_Execution_Routes, count)
	defer delete(routes)
	executions := make([]worker.Activity_Execution, count)
	defer delete(executions)
	tokens := make([][]u8, count)
	defer {
		for token in tokens {
			delete(token)
		}
		delete(tokens)
	}
	for index in 0..<count {
		token := make([]u8, len(prefix) + 4)
		copy(token, prefix)
		token[len(prefix) + 0] = u8(index >> 24)
		token[len(prefix) + 1] = u8(index >> 16)
		token[len(prefix) + 2] = u8(index >> 8)
		token[len(prefix) + 3] = u8(index)
		tokens[index] = token
		executions[index].task_token = token
		if !worker.activity_execution_route_add(&routes, &executions[index]) {
			return pbt.fail("unique generated Activity task token was rejected")
		}
	}
	if len(routes) != count {
		return pbt.fail("generated Activity route table lost an execution")
	}

	duplicate_token := make([]u8, len(tokens[offset]))
	defer delete(duplicate_token)
	copy(duplicate_token, tokens[offset])
	duplicate := worker.Activity_Execution{task_token = duplicate_token}
	if worker.activity_execution_route_add(&routes, &duplicate) {
		return pbt.fail("duplicate generated Activity task token was accepted")
	}
	routed, found := worker.activity_execution_route_find(routes, duplicate_token)
	if !found || routed != &executions[offset] {
		return pbt.fail("duplicate lookup did not preserve the original Activity route")
	}

	for position in 0..<count {
		index := (offset + position) % count
		if reverse {
			index = (offset - position + count) % count
		}
		routed, found = worker.activity_execution_route_find(routes, tokens[index])
		if !found || routed != &executions[index] {
			return pbt.fail("generated Activity cancellation routed to the wrong execution")
		}
		if !worker.activity_execution_route_remove(&routes, &executions[index]) {
			return pbt.fail("generated Activity route could not be removed")
		}
		if _, still_found := worker.activity_execution_route_find(routes, tokens[index]); still_found {
			return pbt.fail("removed generated Activity route remained visible")
		}
	}
	if len(routes) != 0 {
		return pbt.fail("generated Activity route table did not drain")
	}
	return pbt.pass()
}

@(test)
activity_task_tokens_route_under_generated_load :: proc(t: ^testing.T) {
	result := pbt.check(
		"Activity task tokens route under generated load",
		activity_route_property,
		{num_tests = 500, seed = 0x524f_5554_455f_544b},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

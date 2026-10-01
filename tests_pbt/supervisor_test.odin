package workflow_pbt_test

import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import supervisor "../temporal/supervisor"

supervisor_backoff_property :: proc(t: ^pbt.T) -> pbt.Result {
	initial_milliseconds := pbt.draw(t, pbt.int_range(0, 1000))
	maximum_milliseconds := initial_milliseconds + pbt.draw(t, pbt.int_range(0, 10_000))
	multiplier := u32(pbt.draw(t, pbt.int_range(1, 8)))
	steps := pbt.draw(t, pbt.int_range(0, 32))
	pbt.cover(t, initial_milliseconds <= 100, 8, "small initial backoff")
	pbt.cover(t, multiplier == 1, 10, "constant multiplier")

	policy := supervisor.policy_default()
	policy.initial_backoff = time.Duration(initial_milliseconds) * time.Millisecond
	policy.maximum_backoff = time.Duration(maximum_milliseconds) * time.Millisecond
	policy.backoff_multiplier = multiplier
	current := policy.initial_backoff
	for _ in 0 ..< steps {
		expected := current
		if current > 0 && multiplier > 1 {
			expected = min(
				current * time.Duration(multiplier),
				policy.maximum_backoff,
			)
		}
		next := supervisor.next_backoff(current, policy)
		if next != expected || next < current || next > policy.maximum_backoff {
			return pbt.fail("Supervisor backoff violated monotonic saturation")
		}
		current = next
	}
	pbt.cover(t, current == policy.maximum_backoff, 30, "saturated at maximum")
	return pbt.pass()
}

@(test)
supervisor_backoff_is_monotonic_and_saturating :: proc(t: ^testing.T) {
	result := pbt.check(
		"Supervisor backoff is monotonic and saturating",
		supervisor_backoff_property,
		{num_tests = 500, seed = 0x5355_5045_5256_4953},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

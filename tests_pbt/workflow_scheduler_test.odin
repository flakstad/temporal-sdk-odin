package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import worker "../temporal/worker"

workflow_scheduler_ownership_property :: proc(t: ^pbt.T) -> pbt.Result {
	steps := pbt.draw(t, pbt.int_range(1, 128))
	scheduler := worker.In_Process_Workflow_Scheduler{}
	model_owned := 0
	model_peak := 0
	model_queue_peak := 0
	for _ in 0 ..< steps {
		retain := model_owned == 0 || pbt.draw(t, pbt.boolean())
		if retain {
			model_owned += 1
			queue_depth := pbt.draw(t, pbt.int_range(1, model_owned))
			worker.workflow_scheduler_retain_activation(&scheduler, queue_depth)
			model_peak = max(model_peak, model_owned)
			model_queue_peak = max(model_queue_peak, queue_depth)
		} else {
			release_count := pbt.draw(t, pbt.int_range(1, model_owned))
			if !worker.workflow_scheduler_release_activations(&scheduler, release_count) {
				return pbt.fail("valid generated Workflow ownership release underflowed")
			}
			model_owned -= release_count
		}
		if scheduler.activations_owned != model_owned ||
		   scheduler.peak_owned != model_peak ||
		   scheduler.queue_peak != model_queue_peak {
			return pbt.fail("Workflow scheduler ownership diverged from reference model")
		}
	}
	pbt.cover(t, steps >= 96, 20, "long ownership trace")
	pbt.cover(t, model_peak >= 8, 5, "many simultaneously owned activations")
	pbt.cover(t, model_queue_peak >= 4, 10, "deep shard queue")
	if !worker.workflow_scheduler_release_activations(&scheduler, model_owned) ||
	   scheduler.activations_owned != 0 {
		return pbt.fail("Workflow scheduler did not drain generated ownership")
	}
	if worker.workflow_scheduler_release_activations(&scheduler, 1) ||
	   scheduler.activations_owned != 0 {
		return pbt.fail("Workflow scheduler accepted generated ownership underflow")
	}
	return pbt.pass()
}

@(test)
workflow_scheduler_ownership_balances_generated_interleavings :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow scheduler ownership balances generated interleavings",
		workflow_scheduler_ownership_property,
		{num_tests = 1000, seed = 0x5746_5343_4845_4455},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

package workflow_pbt_test

import "core:fmt"
import "core:testing"
import pbt "pbt:pbt"
import worker "../temporal/worker"
import workflow_isolate "../temporal/workflow/isolate"

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

workflow_scheduler_shard_load_property :: proc(t: ^pbt.T) -> pbt.Result {
	shard_count := pbt.draw(t, pbt.int_range(1, 16))
	events := pbt.draw(t, pbt.array(pbt.int_range(0, 4095), 1, 256))
	scheduler := worker.In_Process_Workflow_Scheduler{}
	seen: [16]bool
	queue_depths: [16]int
	model_owned := 0
	model_owned_peak := 0
	model_queue_peak := 0
	model_shards_used := 0
	model_shard_arrivals: [16]int

	for encoded in events {
		run_id := fmt.aprintf("generated-run-%d", encoded % 64)
		shard, shard_ok := workflow_isolate.workflow_shard_index(run_id, shard_count)
		delete(run_id)
		if !shard_ok || shard < 0 || shard >= shard_count {
			return pbt.fail("generated Workflow run did not map to a valid shard")
		}
		first_use := !seen[shard]
		if first_use {
			seen[shard] = true
			model_shards_used += 1
		}
		queue_depths[shard] += 1
		model_shard_arrivals[shard] += 1
		model_owned += 1
		model_owned_peak = max(model_owned_peak, model_owned)
		model_queue_peak = max(model_queue_peak, queue_depths[shard])
		worker.workflow_scheduler_retain_activation(
			&scheduler,
			queue_depths[shard],
			first_use,
		)

		// Generated completion interleavings drain a possibly different shard.
		drain_shard := (encoded / 64) % shard_count
		if encoded % 3 == 0 && queue_depths[drain_shard] > 0 {
			queue_depths[drain_shard] -= 1
			model_owned -= 1
			if !worker.workflow_scheduler_release_activations(&scheduler, 1) {
				return pbt.fail("generated shard completion underflowed ownership")
			}
		}
		if scheduler.activations_owned != model_owned ||
		   scheduler.peak_owned != model_owned_peak ||
		   scheduler.queue_peak != model_queue_peak ||
		   scheduler.shards_used != model_shards_used {
			return pbt.fail("generated shard load diverged from scheduler observations")
		}
	}

	pbt.cover(t, model_shards_used == shard_count, 30, "all configured shards used")
	pbt.cover(t, model_queue_peak >= 8, 20, "skewed shard queue")
	pbt.cover(t, model_owned_peak >= 32, 30, "many owned activations")
	model_shard_min := 0
	model_shard_max := 0
	model_shards_observed := 0
	for shard in 0 ..< shard_count {
		arrivals := model_shard_arrivals[shard]
		worker.workflow_scheduler_observe_shard_activations(&scheduler, arrivals)
		if arrivals > 0 {
			if model_shards_observed == 0 || arrivals < model_shard_min {
				model_shard_min = arrivals
			}
			model_shard_max = max(model_shard_max, arrivals)
			model_shards_observed += 1
		}
	}
	if scheduler.shards_observed != model_shards_observed ||
	   scheduler.shard_activations_min != model_shard_min ||
	   scheduler.shard_activations_max != model_shard_max {
		return pbt.fail("Workflow scheduler shard distribution diverged from reference model")
	}
	if !worker.workflow_scheduler_release_activations(&scheduler, model_owned) ||
	   scheduler.activations_owned != 0 {
		return pbt.fail("generated shard load did not drain ownership")
	}
	return pbt.pass()
}

@(test)
workflow_scheduler_observes_generated_shard_skew_and_drain :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow scheduler observes generated shard skew and drain",
		workflow_scheduler_shard_load_property,
		{num_tests = 1000, seed = 0x5746_5348_4152_444c},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

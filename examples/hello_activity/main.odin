package main

import "core:fmt"
import "core:os"
import "core:strings"
import "core:sync"
import "core:time"
import activity "../../temporal/activity"
import temporal_core "../../temporal/core"
import worker "../../temporal/worker"

concurrent_activities: int
concurrent_overlap_observed: bool

hello :: proc(ctx: ^activity.Context, name: string) -> string {
	_ = ctx
	if strings.has_prefix(name, "concurrent-") {
		sync.atomic_add(&concurrent_activities, 1)
		defer sync.atomic_sub(&concurrent_activities, 1)
		if sync.atomic_load(&concurrent_activities) >= 2 {
			sync.atomic_store_explicit(&concurrent_overlap_observed, true, .Release)
		}
		fmt.printf(
			"Activity %s entered concurrency barrier (active=%d)\n",
			name,
			sync.atomic_load(&concurrent_activities),
		)
		for _ in 0 ..< 500 {
			if sync.atomic_load_explicit(&concurrent_overlap_observed, .Acquire) {
				break
			}
			time.sleep(10 * time.Millisecond)
		}
		if !sync.atomic_load_explicit(&concurrent_overlap_observed, .Acquire) {
			return "Activity executor did not overlap callbacks"
		}
	}
	greeting, allocation_error := strings.concatenate({"Hello ", name})
	if allocation_error != nil {
		return ""
	}
	return greeting
}

main :: proc() {
	runtime, runtime_error := temporal_core.runtime_create()
	if runtime_error.message != nil {
		fmt.eprintf("Core runtime initialization failed: %s\n", string(runtime_error.message))
		temporal_core.error_free(&runtime_error)
		os.exit(1)
	}
	defer temporal_core.runtime_close(&runtime)

	client, connect_error := temporal_core.client_connect(&runtime)
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal_core.error_free(&connect_error)
		os.exit(1)
	}
	defer temporal_core.client_close(&client)

	activity_worker, create_error := worker.create(&client, "odin-hello-activity")
	if create_error.message != nil {
		fmt.eprintf("Worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		os.exit(1)
	}

	register_error := worker.register_activity(&activity_worker, "hello", hello)
	if register_error.message != nil {
		fmt.eprintf("Activity registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		_ = worker.close(&activity_worker)
		os.exit(1)
	}

	fmt.println("Odin Activity Worker is polling task queue odin-hello-activity")
	env_buffer: [64]u8
	concurrency_value := os.get_env(env_buffer[:], "ACTIVITY_EXPECT_CONCURRENT")
	concurrency_case := concurrency_value != ""
	max_tasks := 1
	if concurrency_case {
		max_tasks = 2
	}
	_, run_error := worker.run(
		&activity_worker,
		{max_activity_tasks = max_tasks},
	)
	close_error := worker.close(&activity_worker)
	if run_error.message != nil {
		fmt.eprintf("Worker failed: %s\n", string(run_error.message))
		worker.error_free(&run_error)
		worker.error_free(&close_error)
		os.exit(1)
	}
	if close_error.message != nil {
		fmt.eprintf("Worker shutdown failed: %s\n", string(close_error.message))
		worker.error_free(&close_error)
		os.exit(1)
	}
	if concurrency_case {
		fmt.println("two Activity callbacks overlapped and worker shut down cleanly")
	} else {
		fmt.println("one Activity Task completed and worker shut down cleanly")
	}
}

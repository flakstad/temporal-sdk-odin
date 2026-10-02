package main

import "core:fmt"
import "core:os"
import temporal_core "../../temporal/core"
import history_json "../../temporal/history_json"
import worker "../../temporal/worker"
import workflow "../../temporal/workflow"

// This matches examples/eager_workflow. Replace the registration and callback
// when using the tool as the starting point for an application's replay corpus.
WORKFLOW_TYPE :: "odin_eager_workflow"

replayed_workflow :: proc(_: ^workflow.Fiber_Context, _: string) -> string {
	return "Eager Workflow completed"
}

Owned_History :: struct {
	bytes: []u8,
}

run :: proc() -> int {
	if len(os.args) < 2 {
		fmt.eprintln("usage: replay_history <history.pb|history.json> [...]")
		return 2
	}
	runtime_handle, runtime_error := temporal_core.runtime_create()
	if runtime_error.message != nil {
		fmt.eprintf("Core runtime creation failed: %s\n", string(runtime_error.message))
		temporal_core.error_free(&runtime_error)
		return 1
	}
	defer temporal_core.runtime_close(&runtime_handle)
	replayer, create_error := worker.create_replayer(&runtime_handle)
	if create_error.message != nil {
		fmt.eprintf("Replay worker creation failed: %s\n", string(create_error.message))
		worker.error_free(&create_error)
		return 1
	}
	defer {
		close_error := worker.close(&replayer)
		if close_error.message != nil {
			fmt.eprintf("Replay worker close failed: %s\n", string(close_error.message))
			worker.error_free(&close_error)
		}
	}
	register_error := worker.register_workflow(
		&replayer,
		WORKFLOW_TYPE,
		replayed_workflow,
	)
	if register_error.message != nil {
		fmt.eprintf("Replay Workflow registration failed: %s\n", string(register_error.message))
		worker.error_free(&register_error)
		return 1
	}

	owned := make([dynamic]Owned_History, 0, len(os.args) - 1)
	defer {
		for history in owned {
			delete(history.bytes)
		}
		delete(owned)
	}
	histories := make([dynamic]worker.Replay_History, 0, len(os.args) - 1)
	defer delete(histories)
	for path in os.args[1:] {
		contents, read_error := os.read_entire_file(path, context.allocator)
		if read_error != nil {
			fmt.eprintf("Could not read %s: %v\n", path, read_error)
			return 1
		}
		history_bytes := contents
		for byte in contents {
			if byte == ' ' || byte == '\t' || byte == '\r' || byte == '\n' {
				continue
			}
			if byte == '{' {
				converted, conversion_error := history_json.to_protobuf(contents)
				if conversion_error.message != nil {
					fmt.eprintf(
						"Could not convert Temporal History JSON %s: %s\n",
						path,
						string(conversion_error.message),
					)
					history_json.error_free(&conversion_error)
					delete(contents)
					return 1
				}
				delete(contents)
				history_bytes = converted
			}
			break
		}
		append(&owned, Owned_History{bytes = history_bytes})
		append(&histories, worker.Replay_History{
			workflow_id = path,
			protobuf = history_bytes,
		})
	}

	results, replay_error := worker.replay_histories(&replayer, histories[:])
	defer worker.replay_results_free(&results)
	if replay_error.message != nil {
		fmt.eprintf("Replay worker failed: %s\n", string(replay_error.message))
		worker.error_free(&replay_error)
		return 1
	}
	failed := false
	for result, index in results {
		path := os.args[index + 1]
		if result.failure == .None {
			fmt.printf(
				"PASS %s activations=%d eviction=%v\n",
				path,
				result.activations_processed,
				result.eviction_reason,
			)
			continue
		}
		failed = true
		fmt.eprintf(
			"FAIL %s failure=%v eviction=%v message=%s\n",
			path,
			result.failure,
			result.eviction_reason,
			string(result.message),
		)
	}
	if failed {
		return 1
	}
	return 0
}

main :: proc() {
	os.exit(run())
}

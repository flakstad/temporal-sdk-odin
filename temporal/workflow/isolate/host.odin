package workflow_isolate

import "core:bytes"
import "core:mem/virtual"
import "core:os"
import "core:strings"
import proto "../../core/proto"
import workflow ".."

Host_Error :: struct {
	message: []u8,
}

host_error_free :: proc(error: ^Host_Error) {
	if error == nil {
		return
	}
	delete(error.message)
	error^ = {}
}

host_error :: proc(message: string) -> Host_Error {
	return {message = bytes.clone(transmute([]u8)message)}
}

Host_Registration :: struct {
	name:    string,
	runtime: workflow.Fiber_Runtime,
}

Host_Run :: struct {
	run_id:             string,
	registration_index: int,
}

Host_Rejected_Run :: struct {
	run_id: string,
}

Executor_Host :: struct {
	registrations: [dynamic]Host_Registration,
	runs:          [dynamic]Host_Run,
	rejected_runs: [dynamic]Host_Rejected_Run,
	manifest:      []u8,
	initialized:   bool,
	activations:   int,
	evictions:     int,
}

executor_host_init :: proc(
	host: ^Executor_Host,
	registry: ^workflow.Registry,
	task_queue: string,
	namespace := "default",
	stack_options := workflow.Stack_Options{},
) -> Host_Error {
	if host == nil || host.initialized {
		return host_error("Workflow executor host is already initialized or missing")
	}
	if registry == nil || !registry.initialized ||
	   workflow.registry_count(registry) == 0 || len(task_queue) == 0 ||
	   len(namespace) == 0 {
		return host_error("Workflow executor host requires a registry, task queue, and namespace")
	}
	host.registrations = make(
		[dynamic]Host_Registration,
		0,
		workflow.registry_count(registry),
	)
	host.runs = make([dynamic]Host_Run)
	host.rejected_runs = make([dynamic]Host_Rejected_Run)
	host.manifest, _ = registry_manifest_encode(registry)
	if host.manifest == nil {
		executor_host_close(host)
		return host_error("could not encode Workflow executor registry manifest")
	}
	for index in 0 ..< workflow.registry_count(registry) {
		name, name_ok := workflow.registry_name(registry, index)
		if !name_ok {
			executor_host_close(host)
			return host_error("Workflow executor registry name is invalid")
		}
		owned_name, clone_error := strings.clone(name)
		if clone_error != nil {
			executor_host_close(host)
			return host_error("could not allocate Workflow executor registration name")
		}
		runtime := workflow.Fiber_Runtime{}
		instantiate_error := workflow.registry_instantiate(
			registry,
			index,
			&runtime,
			task_queue,
			namespace,
			stack_options,
		)
		if instantiate_error.message != nil {
			delete(owned_name)
			message := string(instantiate_error.message)
			result := host_error(message)
			workflow.error_free(&instantiate_error)
			executor_host_close(host)
			return result
		}
		append(&host.registrations, Host_Registration{
			name = owned_name,
			runtime = runtime,
		})
	}
	host.initialized = true
	return {}
}

executor_host_close :: proc(host: ^Executor_Host) {
	if host == nil {
		return
	}
	for &registration in host.registrations {
		workflow.fiber_runtime_close(&registration.runtime)
		delete(registration.name)
	}
	for run in host.runs {
		delete(run.run_id)
	}
	for rejected in host.rejected_runs {
		delete(rejected.run_id)
	}
	delete(host.registrations)
	delete(host.runs)
	delete(host.rejected_runs)
	delete(host.manifest)
	host^ = {}
}

// executor_host_manifest borrows the canonical manifest until host close.
executor_host_manifest :: proc(host: ^Executor_Host) -> []u8 {
	if host == nil || !host.initialized {
		return nil
	}
	return host.manifest
}

host_find_registration :: proc(host: ^Executor_Host, name: string) -> (int, bool) {
	for registration, index in host.registrations {
		if registration.name == name {
			return index, true
		}
	}
	return 0, false
}

host_find_run :: proc(host: ^Executor_Host, run_id: string) -> (int, bool) {
	for run, index in host.runs {
		if run.run_id == run_id {
			return index, true
		}
	}
	return 0, false
}

host_find_rejected_run :: proc(host: ^Executor_Host, run_id: string) -> (int, bool) {
	for run, index in host.rejected_runs {
		if run.run_id == run_id {
			return index, true
		}
	}
	return 0, false
}

host_remove_run :: proc(host: ^Executor_Host, index: int) {
	delete(host.runs[index].run_id)
	ordered_remove(&host.runs, index)
}

host_remove_rejected_run :: proc(host: ^Executor_Host, index: int) {
	delete(host.rejected_runs[index].run_id)
	ordered_remove(&host.rejected_runs, index)
}

host_add_rejected_run :: proc(host: ^Executor_Host, run_id: string) -> bool {
	owned_run_id, clone_error := strings.clone(run_id)
	if clone_error != nil {
		return false
	}
	append(&host.rejected_runs, Host_Rejected_Run{run_id = owned_run_id})
	return true
}

executor_host_process_activation :: proc(
	host: ^Executor_Host,
	data: []u8,
) -> (processed: workflow.Process_Result, error: Host_Error) {
	if host == nil || !host.initialized {
		return {}, host_error("Workflow executor host is not initialized")
	}
	route, route_ok := decode_activation_route(data)
	if !route_ok {
		return {}, host_error("Workflow executor host received a malformed activation")
	}
	host.activations += 1
	registration_index := 0
	run_index, run_found := host_find_run(host, route.run_id)
	rejected_index, rejected_found := host_find_rejected_run(host, route.run_id)
	owned_run_id := ""
	if route.initializes {
		if run_found || rejected_found {
			return workflow.activation_failure_result(
				route.run_id,
				"Workflow executor run was initialized more than once without eviction",
			), {}
		}
		registration_found: bool
		registration_index, registration_found = host_find_registration(
			host,
			route.workflow_type,
		)
		if !registration_found {
			if !host_add_rejected_run(host, route.run_id) {
				return {}, host_error("could not allocate rejected Workflow executor route")
			}
			return workflow.activation_failure_result(
				route.run_id,
				"Workflow type is not registered in the executor host",
			), {}
		}
		cloned_run_id, clone_error := strings.clone(route.run_id)
		if clone_error != nil {
			return {}, host_error("could not allocate Workflow executor run route")
		}
		owned_run_id = cloned_run_id
	} else if rejected_found {
		if !route.evicts || route.job_count != 1 {
			return workflow.activation_failure_result(
				route.run_id,
				"rejected Workflow executor run received a non-eviction activation",
			), {}
		}
		host_remove_rejected_run(host, rejected_index)
		host.evictions += 1
		return workflow.Process_Result{
			completion = workflow.encode_empty_activation_success(route.run_id),
			run_id = route.run_id,
			evicted = true,
			is_replaying = route.is_replaying,
			history_length = route.history_length,
		}, {}
	} else {
		if !run_found {
			return {}, host_error("Workflow activation does not match an executor run")
		}
		registration_index = host.runs[run_index].registration_index
	}

	registration := &host.registrations[registration_index]
	processed = workflow.process_fiber_activation(&registration.runtime, data)
	if route.initializes {
		if workflow.fiber_runtime_has_run(&registration.runtime, route.run_id) {
			append(&host.runs, Host_Run{
				run_id = owned_run_id,
				registration_index = registration_index,
			})
			owned_run_id = ""
		} else if processed.activation_failed {
			append(&host.rejected_runs, Host_Rejected_Run{run_id = owned_run_id})
			owned_run_id = ""
		}
		delete(owned_run_id)
	}
	if processed.evicted {
		host.evictions += 1
		run_index, run_found = host_find_run(host, route.run_id)
		if run_found {
			host_remove_run(host, run_index)
		}
	}
	return processed, {}
}

completion_matches_run_id :: proc(data, expected_run_id: []u8) -> bool {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return false
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{}
	decoded := proto.decode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(
		data,
		&completion,
	)
	context.allocator = previous_allocator
	return decoded && bytes.equal(transmute([]u8)completion.run_id, expected_run_id)
}

host_write_executor_error :: proc(
	output: ^os.File,
	request_id: u64,
	run_id: []u8,
	message: string,
) {
	_ = write(output, {
		kind = .Executor_Error,
		request_id = request_id,
		run_id = run_id,
		payload = transmute([]u8)message,
	})
}

executor_host_run :: proc(
	host: ^Executor_Host,
	input: ^os.File = os.stdin,
	output: ^os.File = os.stdout,
) -> Host_Error {
	if host == nil || !host.initialized || input == nil || output == nil {
		return host_error("Workflow executor host run arguments are invalid")
	}
	for {
		request, read_error := read(input)
		if read_error.kind == .End_Of_Stream {
			return {}
		}
		if read_error.kind != .None {
			return host_error("could not read Workflow executor frame")
		}
		#partial switch request.frame.kind {
		case .Manifest_Request:
			write_error := write(output, {
				kind = .Manifest,
				request_id = request.frame.request_id,
				payload = host.manifest,
			})
			owned_frame_free(&request)
			if write_error.kind != .None {
				return host_error("could not write Workflow executor manifest")
			}
		case .Activation:
			processed, process_error := executor_host_process_activation(
				host,
				request.frame.payload,
			)
			if process_error.message != nil || processed.completion == nil {
				message := "Workflow executor produced no completion"
				if process_error.message != nil {
					message = string(process_error.message)
				} else if processed.error.message != nil {
					message = string(processed.error.message)
				}
				host_write_executor_error(
					output,
					request.frame.request_id,
					request.frame.run_id,
					message,
				)
				result_error := host_error(message)
				host_error_free(&process_error)
				workflow.error_free(&processed.error)
				delete(processed.completion)
				owned_frame_free(&request)
				return result_error
			}
			if !completion_matches_run_id(
				processed.completion[:],
				request.frame.run_id,
			) {
				host_write_executor_error(
					output,
					request.frame.request_id,
					request.frame.run_id,
					"Workflow completion run ID does not match its executor route",
				)
				host_error_free(&process_error)
				workflow.error_free(&processed.error)
				delete(processed.completion)
				owned_frame_free(&request)
				return host_error("Workflow completion run ID does not match its executor route")
			}
			metadata, metadata_ok := process_result_metadata_encode(&processed)
			if !metadata_ok {
				host_write_executor_error(
					output,
					request.frame.request_id,
					request.frame.run_id,
					"Workflow executor could not encode completion metadata",
				)
				host_error_free(&process_error)
				workflow.error_free(&processed.error)
				delete(processed.completion)
				owned_frame_free(&request)
				return host_error("Workflow executor could not encode completion metadata")
			}
			metadata_write_error := write(output, {
				kind = .Completion_Metadata,
				request_id = request.frame.request_id,
				run_id = request.frame.run_id,
				payload = metadata,
			})
			delete(metadata)
			if metadata_write_error.kind != .None {
				host_error_free(&process_error)
				workflow.error_free(&processed.error)
				delete(processed.completion)
				owned_frame_free(&request)
				return host_error("could not write Workflow executor completion metadata")
			}
			write_error := write(output, {
				kind = .Completion,
				request_id = request.frame.request_id,
				run_id = request.frame.run_id,
				payload = processed.completion[:],
			})
			host_error_free(&process_error)
			workflow.error_free(&processed.error)
			delete(processed.completion)
			owned_frame_free(&request)
			if write_error.kind != .None {
				return host_error("could not write Workflow executor completion")
			}
		case .Shutdown:
			write_error := write(output, {
				kind = .Shutdown_Acknowledged,
				request_id = request.frame.request_id,
			})
			if write_error.kind != .None {
				owned_frame_free(&request)
				return host_error("could not acknowledge Workflow executor shutdown")
			}
			owned_frame_free(&request)
			return {}
		case:
				host_write_executor_error(
				output,
				request.frame.request_id,
				request.frame.run_id,
				"Workflow executor received an invalid request kind",
			)
			owned_frame_free(&request)
			return host_error("Workflow executor received an invalid request kind")
		}
	}
}

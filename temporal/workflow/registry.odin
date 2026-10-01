package workflow

import allocator_runtime "base:runtime"
import "core:strings"
import codec "../codec"

Registry_Payload_View :: codec.Payload_View

Registry_Instantiate_Proc :: #type proc(
	user_data: rawptr,
	runtime: ^Fiber_Runtime,
	workflow_type, task_queue, namespace: string,
	stack_options: Stack_Options,
) -> Error

Registry_Destroy_Proc :: #type proc(user_data: rawptr)

Registry_Registration :: struct {
	name:        string,
	user_data:   rawptr,
	instantiate: Registry_Instantiate_Proc,
	destroy:     Registry_Destroy_Proc,
}

Registry :: struct {
	registrations: [dynamic]Registry_Registration,
	initialized:   bool,
}

registry_init :: proc(registry: ^Registry) -> Error {
	if registry == nil || registry.initialized {
		return error_from_string("Workflow registry is already initialized or missing")
	}
	registry.registrations = make([dynamic]Registry_Registration)
	registry.initialized = true
	return {}
}

registry_close :: proc(registry: ^Registry) {
	if registry == nil {
		return
	}
	for &registration in registry.registrations {
		if registration.user_data != nil && registration.destroy != nil {
			registration.destroy(registration.user_data)
		}
		delete(registration.name)
	}
	delete(registry.registrations)
	registry^ = {}
}

registry_count :: proc(registry: ^Registry) -> int {
	if registry == nil || !registry.initialized {
		return 0
	}
	return len(registry.registrations)
}

registry_name :: proc(registry: ^Registry, index: int) -> (name: string, ok: bool) {
	if registry == nil || !registry.initialized || index < 0 ||
	   index >= len(registry.registrations) {
		return
	}
	return registry.registrations[index].name, true
}

registry_find :: proc(registry: ^Registry, name: string) -> (index: int, found: bool) {
	if registry == nil || !registry.initialized {
		return
	}
	for registration, registration_index in registry.registrations {
		if registration.name == name {
			return registration_index, true
		}
	}
	return
}

registry_add :: proc(
	registry: ^Registry,
	name: string,
	user_data: rawptr,
	instantiate: Registry_Instantiate_Proc,
	destroy: Registry_Destroy_Proc,
) -> Error {
	if registry == nil || !registry.initialized {
		return error_from_string("Workflow registry is not initialized")
	}
	if len(name) == 0 || user_data == nil || instantiate == nil || destroy == nil {
		return error_from_string("Workflow registry name and callback are required")
	}
	if _, found := registry_find(registry, name); found {
		return error_from_string("Workflow type is already registered")
	}
	owned_name, clone_error := strings.clone(name)
	if clone_error != nil {
		return error_from_string("could not allocate Workflow registry name")
	}
	append(&registry.registrations, Registry_Registration{
		name = owned_name,
		user_data = user_data,
		instantiate = instantiate,
		destroy = destroy,
	})
	return {}
}

registry_register_workflow :: proc(
	registry: ^Registry,
	name: string,
	callback: Fiber_Proc,
) -> Error {
	if callback == nil {
		return error_from_string("Workflow procedure is required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned, allocation_error := new(Fiber_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Workflow registry callback")
	}
	owned^ = callback
	add_error := registry_add(
		registry,
		name,
		owned,
		proc(
			data: rawptr,
			runtime: ^Fiber_Runtime,
			workflow_type, task_queue, namespace: string,
			stack_options: Stack_Options,
		) -> Error {
			registered := cast(^Fiber_Proc)data
			return fiber_runtime_init(
				runtime,
				workflow_type,
				task_queue,
				registered^,
				namespace,
				stack_options,
			)
		},
		proc(data: rawptr) {
			free(cast(^Fiber_Proc)data, allocator_runtime.heap_allocator())
		},
	)
	if add_error.message != nil {
		free(owned, allocator)
	}
	return add_error
}

registry_register_payload_workflow :: proc(
	registry: ^Registry,
	name: string,
	callback: Payload_Proc,
) -> Error {
	if callback == nil {
		return error_from_string("Payload Workflow procedure is required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned, allocation_error := new(Payload_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Payload Workflow registry callback")
	}
	owned^ = callback
	add_error := registry_add(
		registry,
		name,
		owned,
		proc(
			data: rawptr,
			runtime: ^Fiber_Runtime,
			workflow_type, task_queue, namespace: string,
			stack_options: Stack_Options,
		) -> Error {
			registered := cast(^Payload_Proc)data
			return fiber_runtime_init_payload(
				runtime,
				workflow_type,
				task_queue,
				registered^,
				namespace,
				stack_options,
			)
		},
		proc(data: rawptr) {
			free(cast(^Payload_Proc)data, allocator_runtime.heap_allocator())
		},
	)
	if add_error.message != nil {
		free(owned, allocator)
	}
	return add_error
}

registry_register_payloads_workflow :: proc(
	registry: ^Registry,
	name: string,
	callback: Payloads_Proc,
) -> Error {
	if callback == nil {
		return error_from_string("Payloads Workflow procedure is required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned, allocation_error := new(Payloads_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Payloads Workflow registry callback")
	}
	owned^ = callback
	add_error := registry_add(
		registry,
		name,
		owned,
		proc(
			data: rawptr,
			runtime: ^Fiber_Runtime,
			workflow_type, task_queue, namespace: string,
			stack_options: Stack_Options,
		) -> Error {
			registered := cast(^Payloads_Proc)data
			return fiber_runtime_init_payloads(
				runtime,
				workflow_type,
				task_queue,
				registered^,
				namespace,
				stack_options,
			)
		},
		proc(data: rawptr) {
			free(cast(^Payloads_Proc)data, allocator_runtime.heap_allocator())
		},
	)
	if add_error.message != nil {
		free(owned, allocator)
	}
	return add_error
}

registry_register_json_workflow :: proc(
	registry: ^Registry,
	name: string,
	callback: proc(ctx: ^Fiber_Context, input: $Input) -> $Output,
) -> Error {
	if callback == nil {
		return error_from_string("typed Workflow procedure is required")
	}
	Callback :: #type proc(ctx: ^Fiber_Context, input: Input) -> Output
	allocator := allocator_runtime.heap_allocator()
	owned, allocation_error := new(Callback, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate typed Workflow registry callback")
	}
	owned^ = callback
	add_error := registry_add(
		registry,
		name,
		owned,
		proc(
			data: rawptr,
			runtime: ^Fiber_Runtime,
			workflow_type, task_queue, namespace: string,
			stack_options: Stack_Options,
		) -> Error {
			registered := cast(^Callback)data
			runtime_allocator := allocator_runtime.heap_allocator()
			runtime_callback, callback_error := new(Callback, runtime_allocator)
			if callback_error != nil {
				return error_from_string("could not allocate typed Workflow runtime callback")
			}
			runtime_callback^ = registered^
			init_error := fiber_runtime_init_invoker(
				runtime,
				workflow_type,
				task_queue,
				runtime_callback,
				proc(
					callback_data: rawptr,
					ctx: ^Fiber_Context,
					inputs: []Registry_Payload_View,
				) -> Payload_Result {
					if len(inputs) != 1 {
						return payload_fail(
							"typed Workflow requires exactly one argument",
							"OdinWorkflowArgumentCountError",
						)
					}
					value: Input
					if codec.decode_json(inputs[0], &value) != .None {
						return payload_fail(
							"could not decode Workflow input as json/plain",
							"OdinWorkflowPayloadConversionError",
						)
					}
					callback := cast(^Callback)callback_data
					output := callback^(ctx, value)
					payload, encode_error := codec.encode_json(output)
					if encode_error != .None {
						return payload_fail(
							"could not encode Workflow result as json/plain",
							"OdinWorkflowPayloadConversionError",
						)
					}
					return payload_complete(payload)
				},
				proc(callback_data: rawptr) {
					free(
						cast(^Callback)callback_data,
						allocator_runtime.heap_allocator(),
					)
				},
				namespace,
				stack_options,
			)
			if init_error.message != nil {
				free(runtime_callback, runtime_allocator)
			}
			return init_error
		},
		proc(data: rawptr) {
			free(cast(^Callback)data, allocator_runtime.heap_allocator())
		},
	)
	if add_error.message != nil {
		free(owned, allocator)
	}
	return add_error
}

registry_instantiate :: proc(
	registry: ^Registry,
	index: int,
	runtime: ^Fiber_Runtime,
	task_queue: string,
	namespace := "default",
	stack_options := Stack_Options{},
) -> Error {
	if registry == nil || !registry.initialized || runtime == nil ||
	   index < 0 || index >= len(registry.registrations) {
		return error_from_string("Workflow registry instantiation is invalid")
	}
	registration := &registry.registrations[index]
	return registration.instantiate(
		registration.user_data,
		runtime,
		registration.name,
		task_queue,
		namespace,
		stack_options,
	)
}

package workflow_isolate_app

import "core:fmt"
import "core:time"
import workflow "../../temporal/workflow"

GREETING_WORKFLOW_TYPE :: "odin_isolated_greeting"
FAREWELL_WORKFLOW_TYPE :: "odin_isolated_farewell"
NONDETERMINISM_WORKFLOW_TYPE :: "odin_isolated_nondeterminism"
DEFAULT_TASK_QUEUE :: "odin-isolated-workflow-core"

Farewell_Result :: struct {
	message: string,
	code:    int,
}

Registry_Revision :: enum {
	Ordinary,
	Incompatible_Nondeterminism,
	Stack_Overflow,
	Panic,
	Infinite_Loop,
}

stack_overflow_enabled := true

consume_workflow_stack :: #force_no_inline proc(depth: u64) -> u64 {
	frame: [1024]u8
	for &item, index in frame {
		item = u8((depth + u64(index)) & 0xff)
	}
	if stack_overflow_enabled {
		nested := consume_workflow_stack(depth + u64(frame[depth % len(frame)]) + 1)
		return nested + u64(frame[(depth + nested) % len(frame)])
	}
	return u64(frame[depth % len(frame)])
}

force_workflow_stack_overflow :: proc() {
	fmt.eprintln("isolated-executor forcing guarded Workflow stack overflow")
	_ = consume_workflow_stack(1)
}

force_workflow_panic :: proc() -> ! {
	fmt.eprintln("isolated-executor forcing Odin Workflow panic")
	panic("intentional isolated Workflow panic containment probe")
}

infinite_loop_enabled := true

force_workflow_infinite_loop :: #force_no_inline proc() -> ! {
	fmt.eprintln("isolated-executor entering infinite Odin Workflow loop")
	for infinite_loop_enabled {
	}
	panic("unreachable infinite Workflow loop exit")
}

greeting_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	workflow.sleep(ctx, time.Second)
	return fmt.aprintf("Hello from an isolated Workflow, %s", name)
}

farewell_workflow :: proc(
	ctx: ^workflow.Fiber_Context,
	name: string,
) -> Farewell_Result {
	workflow.sleep(ctx, 500 * time.Millisecond)
	return {
		message = fmt.aprintf("Goodbye from the shared registry, %s", name),
		code = 42,
	}
}

nondeterminism_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	workflow.sleep(ctx, time.Hour)
	return fmt.aprintf("Timer completed for %s", name)
}

incompatible_nondeterminism_workflow :: proc(
	_: ^workflow.Fiber_Context,
	name: string,
) -> string {
	return fmt.aprintf("Incompatible result for %s", name)
}

stack_overflow_string_workflow :: proc(
	_: ^workflow.Fiber_Context,
	_: string,
) -> string {
	force_workflow_stack_overflow()
	return "unreachable"
}

stack_overflow_json_workflow :: proc(
	_: ^workflow.Fiber_Context,
	_: string,
) -> Farewell_Result {
	force_workflow_stack_overflow()
	return {}
}

panic_string_workflow :: proc(
	_: ^workflow.Fiber_Context,
	_: string,
) -> string {
	force_workflow_panic()
}

panic_json_workflow :: proc(
	_: ^workflow.Fiber_Context,
	_: string,
) -> Farewell_Result {
	force_workflow_panic()
}

infinite_loop_string_workflow :: proc(
	_: ^workflow.Fiber_Context,
	_: string,
) -> string {
	force_workflow_infinite_loop()
}

infinite_loop_json_workflow :: proc(
	_: ^workflow.Fiber_Context,
	_: string,
) -> Farewell_Result {
	force_workflow_infinite_loop()
}

register :: proc(registry: ^workflow.Registry) -> workflow.Error {
	return register_revision(registry, .Ordinary)
}

// register_revision preserves the canonical type manifest while integration
// proofs deliberately replace callback implementations.
register_revision :: proc(
	registry: ^workflow.Registry,
	revision: Registry_Revision,
) -> workflow.Error {
	greeting_callback := greeting_workflow
	farewell_callback := farewell_workflow
	if revision == .Stack_Overflow {
		greeting_callback = stack_overflow_string_workflow
		farewell_callback = stack_overflow_json_workflow
	} else if revision == .Panic {
		greeting_callback = panic_string_workflow
		farewell_callback = panic_json_workflow
	} else if revision == .Infinite_Loop {
		greeting_callback = infinite_loop_string_workflow
		farewell_callback = infinite_loop_json_workflow
	}
	greeting_error := workflow.registry_register_workflow(
		registry,
		GREETING_WORKFLOW_TYPE,
		greeting_callback,
	)
	if greeting_error.message != nil {
		return greeting_error
	}
	farewell_error := workflow.registry_register_json_workflow(
		registry,
		FAREWELL_WORKFLOW_TYPE,
		farewell_callback,
	)
	if farewell_error.message != nil {
		return farewell_error
	}
	if revision == .Incompatible_Nondeterminism {
		return workflow.registry_register_workflow(
			registry,
			NONDETERMINISM_WORKFLOW_TYPE,
			incompatible_nondeterminism_workflow,
		)
	}
	if revision == .Stack_Overflow {
		return workflow.registry_register_workflow(
			registry,
			NONDETERMINISM_WORKFLOW_TYPE,
			stack_overflow_string_workflow,
		)
	}
	if revision == .Panic {
		return workflow.registry_register_workflow(
			registry,
			NONDETERMINISM_WORKFLOW_TYPE,
			panic_string_workflow,
		)
	}
	if revision == .Infinite_Loop {
		return workflow.registry_register_workflow(
			registry,
			NONDETERMINISM_WORKFLOW_TYPE,
			infinite_loop_string_workflow,
		)
	}
	return workflow.registry_register_workflow(
		registry,
		NONDETERMINISM_WORKFLOW_TYPE,
		nondeterminism_workflow,
	)
}

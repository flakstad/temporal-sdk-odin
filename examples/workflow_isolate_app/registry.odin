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

register :: proc(registry: ^workflow.Registry) -> workflow.Error {
	return register_revision(registry, false)
}

// register_revision preserves the canonical type manifest while permitting the
// integration proof to replay one type with deliberately incompatible code.
register_revision :: proc(
	registry: ^workflow.Registry,
	incompatible_nondeterminism: bool,
) -> workflow.Error {
	greeting_error := workflow.registry_register_workflow(
		registry,
		GREETING_WORKFLOW_TYPE,
		greeting_workflow,
	)
	if greeting_error.message != nil {
		return greeting_error
	}
	farewell_error := workflow.registry_register_json_workflow(
		registry,
		FAREWELL_WORKFLOW_TYPE,
		farewell_workflow,
	)
	if farewell_error.message != nil {
		return farewell_error
	}
	if incompatible_nondeterminism {
		return workflow.registry_register_workflow(
			registry,
			NONDETERMINISM_WORKFLOW_TYPE,
			incompatible_nondeterminism_workflow,
		)
	}
	return workflow.registry_register_workflow(
		registry,
		NONDETERMINISM_WORKFLOW_TYPE,
		nondeterminism_workflow,
	)
}

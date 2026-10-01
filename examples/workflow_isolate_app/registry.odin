package workflow_isolate_app

import "core:fmt"
import "core:time"
import workflow "../../temporal/workflow"

GREETING_WORKFLOW_TYPE :: "odin_isolated_greeting"
FAREWELL_WORKFLOW_TYPE :: "odin_isolated_farewell"
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

register :: proc(registry: ^workflow.Registry) -> workflow.Error {
	greeting_error := workflow.registry_register_workflow(
		registry,
		GREETING_WORKFLOW_TYPE,
		greeting_workflow,
	)
	if greeting_error.message != nil {
		return greeting_error
	}
	return workflow.registry_register_json_workflow(
		registry,
		FAREWELL_WORKFLOW_TYPE,
		farewell_workflow,
	)
}

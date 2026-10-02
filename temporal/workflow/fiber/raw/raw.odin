// Package fiber_raw is the private C ABI used by the Workflow fiber runtime.
package fiber_raw

import "core:c"

Handle :: distinct rawptr

Entry :: proc "c" (user_data: rawptr)

State :: enum c.int {
	Dead      = 0,
	Normal    = 1,
	Running   = 2,
	Suspended = 3,
}

Result :: enum c.int {
	Success             = 0,
	Generic_Error       = 1,
	Invalid_Pointer     = 2,
	Invalid_Coroutine   = 3,
	Not_Suspended       = 4,
	Not_Running         = 5,
	Make_Context_Error  = 6,
	Switch_Context_Error = 7,
	Not_Enough_Space    = 8,
	Out_Of_Memory       = 9,
	Invalid_Arguments   = 10,
	Invalid_Operation   = 11,
	Stack_Overflow      = 12,
}

foreign import fiber_lib "system:temporal_odin_fiber"

foreign fiber_lib {
	@(link_name = "temporal_odin_fiber_create")
	create :: proc(out_fiber: ^Handle, entry: Entry, user_data: rawptr, stack_size: c.size_t) -> Result ---

	@(link_name = "temporal_odin_fiber_resume")
	resume :: proc(fiber: Handle) -> Result ---

	@(link_name = "temporal_odin_fiber_yield")
	yield :: proc(fiber: Handle) -> Result ---

	@(link_name = "temporal_odin_fiber_status")
	status :: proc(fiber: Handle) -> State ---

	@(link_name = "temporal_odin_fiber_stack_size")
	stack_size :: proc(fiber: Handle) -> c.size_t ---

	@(link_name = "temporal_odin_fiber_has_guard_page")
	has_guard_page :: proc(fiber: Handle) -> c.int ---

	@(link_name = "temporal_odin_fiber_guard_page_size")
	guard_page_size :: proc(fiber: Handle) -> c.size_t ---

	@(link_name = "temporal_odin_fiber_reserved_size")
	reserved_size :: proc(fiber: Handle) -> c.size_t ---

	@(link_name = "temporal_odin_fiber_destroy")
	destroy :: proc(fiber: Handle) -> Result ---

	@(link_name = "temporal_odin_fiber_result_description")
	result_description :: proc(result: Result) -> cstring ---
}

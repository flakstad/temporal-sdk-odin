package tests

import "core:testing"
import "core:bytes"
import temporal_core "../temporal/core"

@(test)
runtime_init_and_shutdown :: proc(t: ^testing.T) {
	runtime, error := temporal_core.runtime_create()
	if error.message != nil {
		defer temporal_core.error_free(&error)
		testing.expectf(t, false, "runtime_create failed: %s", string(error.message))
		return
	}
	testing.expect(t, runtime.handle != nil)
	temporal_core.runtime_close(&runtime)
	testing.expect(t, runtime.handle == nil)
	// Closing twice is intentionally safe in the Odin lifecycle wrapper.
	temporal_core.runtime_close(&runtime)
}

@(test)
error_free_is_idempotent :: proc(t: ^testing.T) {
	error := temporal_core.Error{message = bytes.clone(transmute([]u8)string("owned"))}
	temporal_core.error_free(&error)
	testing.expect(t, error.message == nil)
	temporal_core.error_free(&error)
}

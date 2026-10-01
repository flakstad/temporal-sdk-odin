package workflow_isolate

import "core:mem"

Diagnostic_Kind :: enum u8 {
	Application = 1,
	Log         = 2,
	Metric      = 3,
}

Diagnostic_Event_View :: struct {
	kind:       Diagnostic_Kind,
	payload:    []u8,
	run_id:     string,
	generation: u64,
}

diagnostic_kind_valid :: proc(kind: Diagnostic_Kind) -> bool {
	return kind == .Application || kind == .Log || kind == .Metric
}

diagnostic_encode :: proc(
	kind: Diagnostic_Kind,
	payload: []u8,
	allocator: mem.Allocator = context.allocator,
) -> (encoded: []u8, ok: bool) {
	if !diagnostic_kind_valid(kind) || len(payload) == 0 {
		return
	}
	encoded = make([]u8, len(payload) + 1, allocator)
	encoded[0] = u8(kind)
	copy(encoded[1:], payload)
	return encoded, true
}

diagnostic_decode :: proc(data: []u8) -> (kind: Diagnostic_Kind, payload: []u8, ok: bool) {
	if len(data) < 2 {
		return
	}
	kind = Diagnostic_Kind(data[0])
	if !diagnostic_kind_valid(kind) {
		return {}, nil, false
	}
	return kind, data[1:], true
}

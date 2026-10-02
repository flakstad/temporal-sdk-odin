// Package history_json_raw binds the replay-only Temporal History protobuf-JSON converter.
package history_json_raw

import "core:c"

when ODIN_OS == .Darwin || ODIN_OS == .Linux {
	foreign import history_json_lib "system:temporal_odin_history_json"
} else {
	#panic("Temporal History JSON conversion currently supports macOS and Linux")
}

Owned_Bytes :: struct {
	data:     ^u8,
	size:     c.size_t,
	capacity: c.size_t,
}

Conversion_Result :: struct {
	protobuf: ^Owned_Bytes,
	fail:     ^Owned_Bytes,
}

#assert(offset_of(Owned_Bytes, data) == 0)
#assert(offset_of(Owned_Bytes, size) == size_of(^u8))
#assert(offset_of(Owned_Bytes, capacity) == size_of(^u8) + size_of(c.size_t))
#assert(size_of(Owned_Bytes) == size_of(^u8) + 2 * size_of(c.size_t))
#assert(offset_of(Conversion_Result, protobuf) == 0)
#assert(offset_of(Conversion_Result, fail) == size_of(^Owned_Bytes))
#assert(size_of(Conversion_Result) == 2 * size_of(^Owned_Bytes))

foreign history_json_lib {
	@(link_name = "temporal_odin_history_json_to_protobuf")
	to_protobuf :: proc(data: ^u8, size: c.size_t) -> Conversion_Result ---

	@(link_name = "temporal_odin_history_json_bytes_free")
	bytes_free :: proc(bytes: ^Owned_Bytes) ---
}

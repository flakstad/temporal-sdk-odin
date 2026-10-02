// Package history_json converts canonical protobuf JSON exported by Temporal
// CLI into the binary History protobuf consumed by SDK Core's replayer.
package history_json

import "core:bytes"
import "core:c"
import raw "./raw"

Error :: struct {
	message: []u8,
}

error_free :: proc(error: ^Error) {
	if error == nil {
		return
	}
	delete(error.message)
	error^ = {}
}

copy_owned_bytes :: proc(value: ^raw.Owned_Bytes) -> []u8 {
	if value == nil || value.size == 0 {
		return nil
	}
	return bytes.clone(([^]u8)(value.data)[:int(value.size)])
}

// to_protobuf borrows json for this synchronous call and returns owned binary
// temporal.api.history.v1.History bytes. Unknown fields are rejected so a
// history from a newer schema cannot be silently weakened before replay.
to_protobuf :: proc(json: []u8) -> (protobuf: []u8, error: Error) {
	data: ^u8
	if len(json) > 0 {
		data = raw_data(json)
	}
	result := raw.to_protobuf(data, c.size_t(len(json)))
	defer raw.bytes_free(result.protobuf)
	defer raw.bytes_free(result.fail)
	if result.protobuf != nil && result.fail == nil {
		protobuf = copy_owned_bytes(result.protobuf)
		return
	}
	if result.fail != nil && result.protobuf == nil {
		error.message = copy_owned_bytes(result.fail)
		return
	}
	error.message = bytes.clone(
		transmute([]u8)string("History JSON bridge returned an invalid ownership result"),
	)
	return
}

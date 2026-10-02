// Package activity_isolate defines the versioned process boundary for one
// externally executed Activity invocation. Payloads are pinned SDK Core
// ActivityTask/ActivityTaskCompletion protobuf bytes and remain opaque here.
package activity_isolate

import "core:encoding/endian"
import "core:mem"
import "core:os"

MAGIC :: u32(0x54414953) // "TAIS": Temporal Activity isolation stream.
VERSION :: u16(5)
HEADER_SIZE :: 16

Kind :: enum u16 {
	Execute        = 1,
	Completion     = 2,
	Executor_Error = 3,
	Heartbeat      = 4,
	Heartbeat_Ack  = 5,
	Cancel         = 6,
	Control_Done   = 7,
	Control_Stop   = 8,
	Shutdown       = 9,
	Shutdown_Ack   = 10,
	Manifest_Request = 11,
	Manifest         = 12,
}

Error_Kind :: enum {
	None,
	End_Of_Stream,
	Invalid_Argument,
	IO,
	Truncated,
	Invalid_Magic,
	Unsupported_Version,
	Invalid_Kind,
	Too_Large,
	Trailing_Data,
}

Error :: struct {
	kind:     Error_Kind,
	os_error: os.Error,
}

DEFAULT_MAXIMUM_PAYLOAD_BYTES :: u32(32 * 1024 * 1024)

Frame :: struct {
	kind:       Kind,
	request_id: u64,
	payload:    []u8,
}

Owned_Frame :: struct {
	frame:     Frame,
	allocator: mem.Allocator,
}

owned_frame_free :: proc(frame: ^Owned_Frame) {
	if frame == nil {
		return
	}
	if frame.allocator.procedure != nil {
		delete(frame.frame.payload, frame.allocator)
	}
	frame^ = {}
}

kind_valid :: proc(kind: Kind) -> bool {
	return kind == .Execute || kind == .Completion || kind == .Executor_Error ||
	       kind == .Heartbeat || kind == .Heartbeat_Ack || kind == .Cancel ||
	       kind == .Control_Done || kind == .Control_Stop || kind == .Shutdown ||
	       kind == .Shutdown_Ack || kind == .Manifest_Request || kind == .Manifest
}

frame_validate :: proc(frame: Frame, maximum_payload_bytes := DEFAULT_MAXIMUM_PAYLOAD_BYTES) -> Error {
	if !kind_valid(frame.kind) || frame.request_id == 0 || len(frame.payload) == 0 {
		return {kind = .Invalid_Argument}
	}
	if u64(len(frame.payload)) > u64(maximum_payload_bytes) {
		return {kind = .Too_Large}
	}
	if frame.kind == .Manifest && !host_manifest_valid(frame.payload) {
		return {kind = .Invalid_Argument}
	}
	return {}
}

encode :: proc(
	frame: Frame,
	maximum_payload_bytes := DEFAULT_MAXIMUM_PAYLOAD_BYTES,
	allocator := context.allocator,
) -> (encoded: []u8, error: Error) {
	error = frame_validate(frame, maximum_payload_bytes)
	if error.kind != .None {
		return
	}
	encoded = make([]u8, HEADER_SIZE + len(frame.payload), allocator)
	endian.put_u32(encoded[0:4], .Big, MAGIC)
	endian.put_u16(encoded[4:6], .Big, VERSION)
	endian.put_u16(encoded[6:8], .Big, u16(frame.kind))
	endian.put_u64(encoded[8:16], .Big, frame.request_id)
	copy(encoded[HEADER_SIZE:], frame.payload)
	return
}

decode_header :: proc(
	data: []u8,
	maximum_payload_bytes: u32,
) -> (kind: Kind, request_id: u64, payload_length: u32, error: Error) {
	if len(data) < HEADER_SIZE {
		return {}, 0, 0, {kind = .Truncated}
	}
	magic, _ := endian.get_u32(data[0:4], .Big)
	if magic != MAGIC {
		return {}, 0, 0, {kind = .Invalid_Magic}
	}
	version, _ := endian.get_u16(data[4:6], .Big)
	if version != VERSION {
		return {}, 0, 0, {kind = .Unsupported_Version}
	}
	kind_value, _ := endian.get_u16(data[6:8], .Big)
	kind = Kind(kind_value)
	if !kind_valid(kind) {
		return {}, 0, 0, {kind = .Invalid_Kind}
	}
	request_id, _ = endian.get_u64(data[8:16], .Big)
	// The payload length is derived by stream framing in read and by the total
	// byte slice in decode. It is not duplicated in this fixed header.
	_ = maximum_payload_bytes
	return
}

decode :: proc(
	data: []u8,
	maximum_payload_bytes := DEFAULT_MAXIMUM_PAYLOAD_BYTES,
	allocator := context.allocator,
) -> (owned: Owned_Frame, error: Error) {
	kind, request_id, _, header_error := decode_header(data, maximum_payload_bytes)
	if header_error.kind != .None {
		return {}, header_error
	}
	if len(data) <= HEADER_SIZE {
		return {}, {kind = .Truncated}
	}
	payload_length := len(data) - HEADER_SIZE
	if u64(payload_length) > u64(maximum_payload_bytes) {
		return {}, {kind = .Too_Large}
	}
	owned.allocator = allocator
	owned.frame = {kind = kind, request_id = request_id}
	owned.frame.payload = make([]u8, payload_length, allocator)
	copy(owned.frame.payload, data[HEADER_SIZE:])
	validation_error := frame_validate(owned.frame, maximum_payload_bytes)
	if validation_error.kind != .None {
		owned_frame_free(&owned)
		return {}, validation_error
	}
	return
}

// Stream records prefix the encoded frame with a u32 length so multiple
// messages and truncated children are unambiguous.
write :: proc(
	file: ^os.File,
	frame: Frame,
	maximum_payload_bytes := DEFAULT_MAXIMUM_PAYLOAD_BYTES,
) -> Error {
	if file == nil {
		return {kind = .Invalid_Argument}
	}
	encoded, encode_error := encode(frame, maximum_payload_bytes)
	if encode_error.kind != .None {
		return encode_error
	}
	defer delete(encoded)
	if u64(len(encoded)) > u64(0xffff_ffff) {
		return {kind = .Too_Large}
	}
	prefix: [4]u8
	endian.put_u32(prefix[:], .Big, u32(len(encoded)))
	written, os_error := os.write(file, prefix[:])
	if os_error != nil {
		return {kind = .IO, os_error = os_error}
	}
	if written != len(prefix) {
		return {kind = .Truncated}
	}
	written, os_error = os.write(file, encoded)
	if os_error != nil {
		return {kind = .IO, os_error = os_error}
	}
	if written != len(encoded) {
		return {kind = .Truncated}
	}
	return {}
}

read :: proc(
	file: ^os.File,
	maximum_payload_bytes := DEFAULT_MAXIMUM_PAYLOAD_BYTES,
	allocator := context.allocator,
) -> (owned: Owned_Frame, error: Error) {
	if file == nil {
		return {}, {kind = .Invalid_Argument}
	}
	prefix: [4]u8
	count, os_error := os.read_full(file, prefix[:])
	if os_error != nil {
		if count == 0 {
			return {}, {kind = .End_Of_Stream, os_error = os_error}
		}
		return {}, {kind = .Truncated, os_error = os_error}
	}
	record_length, _ := endian.get_u32(prefix[:], .Big)
	if record_length <= HEADER_SIZE {
		return {}, {kind = .Truncated}
	}
	if u64(record_length) > u64(HEADER_SIZE) + u64(maximum_payload_bytes) {
		return {}, {kind = .Too_Large}
	}
	record := make([]u8, int(record_length), allocator)
	count, os_error = os.read_full(file, record)
	if os_error != nil || count != len(record) {
		delete(record, allocator)
		return {}, {kind = .Truncated, os_error = os_error}
	}
	defer delete(record, allocator)
	return decode(record, maximum_payload_bytes, allocator)
}

// Package workflow_isolate defines the experimental process protocol between
// a Core-owning Worker parent and a language Workflow executor child. Frames
// carry the pinned opaque WorkflowActivation/WorkflowActivationCompletion
// protobuf bytes; this package does not reinterpret Temporal's wire protocol.
package workflow_isolate

import "core:encoding/endian"
import "core:mem"
import "core:os"

MAGIC :: u32(0x544f4953) // "TOIS": Temporal Odin isolation stream.
VERSION :: u16(3)
HEADER_SIZE :: 24

Kind :: enum u16 {
	Activation             = 1,
	Completion             = 2,
	Shutdown               = 3,
	Shutdown_Acknowledged  = 4,
	Executor_Error         = 5,
	Manifest_Request       = 6,
	Manifest               = 7,
	Completion_Metadata    = 8,
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

Limits :: struct {
	maximum_run_id_bytes: u32,
	maximum_payload_bytes: u32,
}

DEFAULT_LIMITS :: Limits{
	maximum_run_id_bytes = 1_024,
	maximum_payload_bytes = 32 * 1024 * 1024,
}

limits_default :: proc() -> Limits {
	return DEFAULT_LIMITS
}

Frame :: struct {
	kind:       Kind,
	request_id: u64,
	run_id:     []u8,
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
		delete(frame.frame.run_id, frame.allocator)
		delete(frame.frame.payload, frame.allocator)
	}
	frame^ = {}
}

kind_valid :: proc(kind: Kind) -> bool {
	switch kind {
	case .Activation, .Completion, .Shutdown, .Shutdown_Acknowledged, .Executor_Error,
	     .Manifest_Request, .Manifest, .Completion_Metadata:
		return true
	}
	return false
}

validate_lengths :: proc(run_id_length, payload_length: u64, limits: Limits) -> Error {
	if run_id_length > u64(limits.maximum_run_id_bytes) ||
	   payload_length > u64(limits.maximum_payload_bytes) {
		return {kind = .Too_Large}
	}
	return {}
}

frame_validate :: proc(frame: Frame, limits := DEFAULT_LIMITS) -> Error {
	if !kind_valid(frame.kind) {
		return {kind = .Invalid_Kind}
	}
	if frame.kind == .Activation || frame.kind == .Completion ||
	   frame.kind == .Completion_Metadata {
		if len(frame.run_id) == 0 || len(frame.payload) == 0 {
			return {kind = .Invalid_Argument}
		}
	}
	if (frame.kind == .Shutdown || frame.kind == .Shutdown_Acknowledged) &&
	   (len(frame.run_id) != 0 || len(frame.payload) != 0) {
		return {kind = .Invalid_Argument}
	}
	if frame.kind == .Manifest_Request &&
	   (len(frame.run_id) != 0 || len(frame.payload) != 0) {
		return {kind = .Invalid_Argument}
	}
	if frame.kind == .Manifest &&
	   (len(frame.run_id) != 0 || !registry_manifest_valid(frame.payload)) {
		return {kind = .Invalid_Argument}
	}
	return validate_lengths(u64(len(frame.run_id)), u64(len(frame.payload)), limits)
}

encode :: proc(
	frame: Frame,
	limits := DEFAULT_LIMITS,
	allocator := context.allocator,
) -> (encoded: []u8, error: Error) {
	error = frame_validate(frame, limits)
	if error.kind != .None {
		return
	}
	total := HEADER_SIZE + len(frame.run_id) + len(frame.payload)
	encoded = make([]u8, total, allocator)
	endian.put_u32(encoded[0:4], .Big, MAGIC)
	endian.put_u16(encoded[4:6], .Big, VERSION)
	endian.put_u16(encoded[6:8], .Big, u16(frame.kind))
	endian.put_u64(encoded[8:16], .Big, frame.request_id)
	endian.put_u32(encoded[16:20], .Big, u32(len(frame.run_id)))
	endian.put_u32(encoded[20:24], .Big, u32(len(frame.payload)))
	copy(encoded[HEADER_SIZE:], frame.run_id)
	copy(encoded[HEADER_SIZE + len(frame.run_id):], frame.payload)
	return
}

Decoded_Header :: struct {
	kind:           Kind,
	request_id:     u64,
	run_id_length:  u32,
	payload_length: u32,
}

decode_header :: proc(data: []u8, limits: Limits) -> (header: Decoded_Header, error: Error) {
	if len(data) < HEADER_SIZE {
		return {}, {kind = .Truncated}
	}
	magic, _ := endian.get_u32(data[0:4], .Big)
	if magic != MAGIC {
		return {}, {kind = .Invalid_Magic}
	}
	version, _ := endian.get_u16(data[4:6], .Big)
	if version != VERSION {
		return {}, {kind = .Unsupported_Version}
	}
	kind_value, _ := endian.get_u16(data[6:8], .Big)
	header.kind = Kind(kind_value)
	if !kind_valid(header.kind) {
		return {}, {kind = .Invalid_Kind}
	}
	header.request_id, _ = endian.get_u64(data[8:16], .Big)
	header.run_id_length, _ = endian.get_u32(data[16:20], .Big)
	header.payload_length, _ = endian.get_u32(data[20:24], .Big)
	error = validate_lengths(
		u64(header.run_id_length),
		u64(header.payload_length),
		limits,
	)
	return
}

decode :: proc(
	data: []u8,
	limits := DEFAULT_LIMITS,
	allocator := context.allocator,
) -> (owned: Owned_Frame, error: Error) {
	header, header_error := decode_header(data, limits)
	if header_error.kind != .None {
		return {}, header_error
	}
	total := u64(HEADER_SIZE) + u64(header.run_id_length) + u64(header.payload_length)
	if u64(len(data)) < total {
		return {}, {kind = .Truncated}
	}
	if u64(len(data)) > total {
		return {}, {kind = .Trailing_Data}
	}
	owned.allocator = allocator
	owned.frame.kind = header.kind
	owned.frame.request_id = header.request_id
	run_id_end := HEADER_SIZE + int(header.run_id_length)
	owned.frame.run_id = make([]u8, int(header.run_id_length), allocator)
	owned.frame.payload = make([]u8, int(header.payload_length), allocator)
	copy(owned.frame.run_id, data[HEADER_SIZE:run_id_end])
	copy(owned.frame.payload, data[run_id_end:])
	validation_error := frame_validate(owned.frame, limits)
	if validation_error.kind != .None {
		owned_frame_free(&owned)
		return {}, validation_error
	}
	return
}

write :: proc(file: ^os.File, frame: Frame, limits := DEFAULT_LIMITS) -> Error {
	if file == nil {
		return {kind = .Invalid_Argument}
	}
	encoded, encode_error := encode(frame, limits)
	if encode_error.kind != .None {
		return encode_error
	}
	defer delete(encoded)
	written, write_error := os.write(file, encoded)
	if write_error != nil {
		return {kind = .IO, os_error = write_error}
	}
	if written != len(encoded) {
		return {kind = .Truncated}
	}
	return {}
}

read :: proc(
	file: ^os.File,
	limits := DEFAULT_LIMITS,
	allocator := context.allocator,
) -> (owned: Owned_Frame, error: Error) {
	if file == nil {
		return {}, {kind = .Invalid_Argument}
	}
	header_bytes: [HEADER_SIZE]u8
	read_count, read_error := os.read_full(file, header_bytes[:])
	if read_error != nil {
		if read_count == 0 {
			return {}, {kind = .End_Of_Stream, os_error = read_error}
		}
		return {}, {kind = .Truncated, os_error = read_error}
	}
	header, header_error := decode_header(header_bytes[:], limits)
	if header_error.kind != .None {
		return {}, header_error
	}
	owned.allocator = allocator
	owned.frame.kind = header.kind
	owned.frame.request_id = header.request_id
	owned.frame.run_id = make([]u8, int(header.run_id_length), allocator)
	owned.frame.payload = make([]u8, int(header.payload_length), allocator)
	if len(owned.frame.run_id) > 0 {
		_, read_error = os.read_full(file, owned.frame.run_id)
		if read_error != nil {
			owned_frame_free(&owned)
			return {}, {kind = .Truncated, os_error = read_error}
		}
	}
	if len(owned.frame.payload) > 0 {
		_, read_error = os.read_full(file, owned.frame.payload)
		if read_error != nil {
			owned_frame_free(&owned)
			return {}, {kind = .Truncated, os_error = read_error}
		}
	}
	validation_error := frame_validate(owned.frame, limits)
	if validation_error.kind != .None {
		owned_frame_free(&owned)
		return {}, validation_error
	}
	return
}

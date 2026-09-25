package tests

import "core:testing"
import "core:bytes"
import temporal_core "../temporal/core"
import codec "../temporal/codec"
import proto "../temporal/core/proto"

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

@(test)
json_string_codec_round_trip :: proc(t: ^testing.T) {
	encoded, encode_error := codec.encode_string("Odin")
	testing.expect(t, encode_error == .None)
	defer delete(encoded)
	testing.expect_value(t, string(encoded), `"Odin"`)

	decoded, decode_error := codec.decode_string(encoded, "json/plain")
	testing.expect(t, decode_error == .None)
	defer delete(decoded)
	testing.expect_value(t, decoded, "Odin")

	_, unsupported := codec.decode_string(encoded, "binary/plain")
	testing.expect(t, unsupported == .Unsupported_Encoding)
}

@(test)
activity_task_protobuf_round_trip :: proc(t: ^testing.T) {
	payload := proto.encode_payload(transmute([]u8)string(`"Odin"`))
	defer delete(payload)
	start := make([dynamic]u8)
	defer delete(start)
	proto.append_string_field(&start, 4, "activity-id")
	proto.append_string_field(&start, 5, "hello")
	proto.append_bytes_field(&start, 7, payload[:])
	proto.append_key(&start, 12, 0)
	proto.append_varint(&start, 2)
	task_bytes := make([dynamic]u8)
	defer delete(task_bytes)
	proto.append_string_field(&task_bytes, 1, "token")
	proto.append_bytes_field(&task_bytes, 3, start[:])

	task, decode_error := proto.decode_activity_task(task_bytes[:])
	testing.expect(t, decode_error == .None)
	testing.expect(t, task.kind == .Start)
	testing.expect_value(t, string(task.task_token), "token")
	testing.expect_value(t, task.activity_id, "activity-id")
	testing.expect_value(t, task.activity_type, "hello")
	testing.expect_value(t, task.attempt, u32(2))
	testing.expect_value(t, task.input.encoding, "json/plain")
	testing.expect_value(t, string(task.input.data), `"Odin"`)

	completion := proto.encode_activity_success(task.task_token, transmute([]u8)string(`"Hello Odin"`))
	defer delete(completion)
	testing.expect(t, len(completion) > len(task.task_token))
}

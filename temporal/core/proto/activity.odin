// Package proto implements only the protobuf messages required by the
// Activity Worker spike. It is intentionally not a general protobuf runtime.
package proto

Task_Kind :: enum {
	Invalid,
	Start,
	Cancel,
}

Payload_View :: struct {
	encoding: string,
	data:     []u8,
}

Activity_Task :: struct {
	kind:          Task_Kind,
	task_token:    []u8,
	activity_id:   string,
	activity_type: string,
	attempt:       u32,
	input:         Payload_View,
}

Decode_Error :: enum {
	None,
	Malformed,
	Missing_Task_Token,
	Missing_Variant,
}

Cursor :: struct {
	data: []u8,
	pos:  int,
	ok:   bool,
}

cursor_create :: proc(data: []u8) -> Cursor {
	return Cursor{data = data, ok = true}
}

read_varint :: proc(cursor: ^Cursor) -> u64 {
	value: u64
	shift: u64
	for shift < 64 {
		if cursor.pos >= len(cursor.data) {
			cursor.ok = false
			return 0
		}
		byte := cursor.data[cursor.pos]
		cursor.pos += 1
		value |= u64(byte & 0x7f) << shift
		if byte & 0x80 == 0 {
			return value
		}
		shift += 7
	}
	cursor.ok = false
	return 0
}

read_bytes :: proc(cursor: ^Cursor) -> []u8 {
	size_u64 := read_varint(cursor)
	if !cursor.ok || size_u64 > u64(len(cursor.data) - cursor.pos) {
		cursor.ok = false
		return nil
	}
	size := int(size_u64)
	result := cursor.data[cursor.pos:cursor.pos + size]
	cursor.pos += size
	return result
}

skip_field :: proc(cursor: ^Cursor, wire_type: u64) {
	switch wire_type {
	case 0:
		_ = read_varint(cursor)
	case 1:
		if cursor.pos + 8 > len(cursor.data) {
			cursor.ok = false
			return
		}
		cursor.pos += 8
	case 2:
		_ = read_bytes(cursor)
	case 5:
		if cursor.pos + 4 > len(cursor.data) {
			cursor.ok = false
			return
		}
		cursor.pos += 4
	case:
		cursor.ok = false
	}
}

decode_metadata_entry :: proc(data: []u8) -> (key: string, value: []u8, ok: bool) {
	cursor := cursor_create(data)
	for cursor.ok && cursor.pos < len(cursor.data) {
		tag := read_varint(&cursor)
		field := tag >> 3
		wire := tag & 7
		if wire != 2 {
			skip_field(&cursor, wire)
			continue
		}
		switch field {
		case 1:
			key = string(read_bytes(&cursor))
		case 2:
			value = read_bytes(&cursor)
		case:
			skip_field(&cursor, wire)
		}
	}
	return key, value, cursor.ok
}

decode_payload :: proc(data: []u8) -> (payload: Payload_View, ok: bool) {
	cursor := cursor_create(data)
	for cursor.ok && cursor.pos < len(cursor.data) {
		tag := read_varint(&cursor)
		field := tag >> 3
		wire := tag & 7
		if wire != 2 {
			skip_field(&cursor, wire)
			continue
		}
		switch field {
		case 1:
			entry := read_bytes(&cursor)
			key, value, entry_ok := decode_metadata_entry(entry)
			if !entry_ok {
				cursor.ok = false
				continue
			}
			if key == "encoding" {
				payload.encoding = string(value)
			}
		case 2:
			payload.data = read_bytes(&cursor)
		case:
			skip_field(&cursor, wire)
		}
	}
	return payload, cursor.ok
}

decode_start :: proc(data: []u8, task: ^Activity_Task) -> bool {
	cursor := cursor_create(data)
	input_seen := false
	for cursor.ok && cursor.pos < len(cursor.data) {
		tag := read_varint(&cursor)
		field := tag >> 3
		wire := tag & 7
		switch field {
		case 4:
			if wire != 2 {
				cursor.ok = false
				continue
			}
			task.activity_id = string(read_bytes(&cursor))
		case 5:
			if wire != 2 {
				cursor.ok = false
				continue
			}
			task.activity_type = string(read_bytes(&cursor))
		case 7:
			if wire != 2 {
				cursor.ok = false
				continue
			}
			payload_bytes := read_bytes(&cursor)
			if !input_seen {
				payload, payload_ok := decode_payload(payload_bytes)
				if !payload_ok {
					cursor.ok = false
					continue
				}
				task.input = payload
				input_seen = true
			}
		case 12:
			if wire != 0 {
				cursor.ok = false
				continue
			}
			task.attempt = u32(read_varint(&cursor))
		case:
			skip_field(&cursor, wire)
		}
	}
	return cursor.ok && len(task.activity_type) > 0
}

decode_activity_task :: proc(data: []u8) -> (task: Activity_Task, error: Decode_Error) {
	cursor := cursor_create(data)
	for cursor.ok && cursor.pos < len(cursor.data) {
		tag := read_varint(&cursor)
		field := tag >> 3
		wire := tag & 7
		if wire != 2 {
			skip_field(&cursor, wire)
			continue
		}
		switch field {
		case 1:
			task.task_token = read_bytes(&cursor)
		case 3:
			start := read_bytes(&cursor)
			task.kind = .Start
			if !decode_start(start, &task) {
				return {}, .Malformed
			}
		case 4:
			_ = read_bytes(&cursor)
			task.kind = .Cancel
		case:
			skip_field(&cursor, wire)
		}
	}
	if !cursor.ok {
		return {}, .Malformed
	}
	if len(task.task_token) == 0 {
		return {}, .Missing_Task_Token
	}
	if task.kind == .Invalid {
		return {}, .Missing_Variant
	}
	return task, .None
}

append_varint :: proc(buffer: ^[dynamic]u8, value: u64) {
	remaining := value
	for remaining >= 0x80 {
		append(buffer, u8(remaining) | 0x80)
		remaining >>= 7
	}
	append(buffer, u8(remaining))
}

append_key :: proc(buffer: ^[dynamic]u8, field: u64, wire_type: u64) {
	append_varint(buffer, field << 3 | wire_type)
}

append_bytes_field :: proc(buffer: ^[dynamic]u8, field: u64, value: []u8) {
	append_key(buffer, field, 2)
	append_varint(buffer, u64(len(value)))
	append(buffer, ..value)
}

append_string_field :: proc(buffer: ^[dynamic]u8, field: u64, value: string) {
	append_bytes_field(buffer, field, transmute([]u8)value)
}

encode_payload :: proc(data: []u8, encoding := "json/plain") -> [dynamic]u8 {
	entry := make([dynamic]u8, 0, 32)
	append_string_field(&entry, 1, "encoding")
	append_string_field(&entry, 2, encoding)

	payload := make([dynamic]u8, 0, len(data) + len(entry) + 16)
	append_bytes_field(&payload, 1, entry[:])
	append_bytes_field(&payload, 2, data)
	delete(entry)
	return payload
}

encode_activity_success :: proc(task_token, result_data: []u8) -> [dynamic]u8 {
	payload := encode_payload(result_data)
	success := make([dynamic]u8, 0, len(payload) + 8)
	append_bytes_field(&success, 1, payload[:])
	execution_result := make([dynamic]u8, 0, len(success) + 8)
	append_bytes_field(&execution_result, 1, success[:])
	completion := make([dynamic]u8, 0, len(task_token) + len(execution_result) + 16)
	append_bytes_field(&completion, 1, task_token)
	append_bytes_field(&completion, 2, execution_result[:])
	delete(payload)
	delete(success)
	delete(execution_result)
	return completion
}

encode_activity_failure :: proc(task_token: []u8, message: string, non_retryable := true) -> [dynamic]u8 {
	application_info := make([dynamic]u8, 0, 48)
	append_string_field(&application_info, 1, "OdinActivityError")
	if non_retryable {
		append_key(&application_info, 2, 0)
		append_varint(&application_info, 1)
	}

	failure := make([dynamic]u8, 0, len(message) + len(application_info) + 32)
	append_string_field(&failure, 1, message)
	append_string_field(&failure, 2, "OdinSDK")
	append_bytes_field(&failure, 5, application_info[:])

	activity_failure := make([dynamic]u8, 0, len(failure) + 8)
	append_bytes_field(&activity_failure, 1, failure[:])
	append_key(&activity_failure, 2, 0)
	append_varint(&activity_failure, 3)

	execution_result := make([dynamic]u8, 0, len(activity_failure) + 8)
	append_bytes_field(&execution_result, 2, activity_failure[:])
	completion := make([dynamic]u8, 0, len(task_token) + len(execution_result) + 16)
	append_bytes_field(&completion, 1, task_token)
	append_bytes_field(&completion, 2, execution_result[:])

	delete(application_info)
	delete(failure)
	delete(activity_failure)
	delete(execution_result)
	return completion
}

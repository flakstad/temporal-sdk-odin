// Package protobuf is the small wire-format runtime used by generated Core
// message code. Schema knowledge belongs in generated files, not here.
package protobuf

Decoder :: struct {
	data: []u8,
	pos:  int,
	ok:   bool,
}

decoder :: proc(data: []u8) -> Decoder {
	return Decoder{data = data, ok = true}
}

has_more :: proc(value: ^Decoder) -> bool {
	return value != nil && value.ok && value.pos < len(value.data)
}

read_varint :: proc(value: ^Decoder) -> (result: u64, ok: bool) {
	if value == nil || !value.ok {
		return 0, false
	}
	for shift: u64 = 0; shift < 64; shift += 7 {
		if value.pos >= len(value.data) {
			value.ok = false
			return 0, false
		}
		byte := value.data[value.pos]
		value.pos += 1
		if shift == 63 && byte > 1 {
			value.ok = false
			return 0, false
		}
		result |= u64(byte & 0x7f) << shift
		if byte & 0x80 == 0 {
			return result, true
		}
	}
	value.ok = false
	return 0, false
}

read_key :: proc(value: ^Decoder) -> (field_number: u32, wire_type: u8, ok: bool) {
	key, key_ok := read_varint(value)
	if !key_ok {
		return 0, 0, false
	}
	field_number_raw := key >> 3
	wire_type = u8(key & 7)
	if field_number_raw == 0 || field_number_raw > 0x1fff_ffff || wire_type > 5 || wire_type == 3 || wire_type == 4 {
		value.ok = false
		return 0, 0, false
	}
	field_number = u32(field_number_raw)
	return field_number, wire_type, true
}

read_bytes :: proc(value: ^Decoder) -> ([]u8, bool) {
	size, size_ok := read_varint(value)
	if !size_ok || size > u64(len(value.data) - value.pos) {
		value.ok = false
		return nil, false
	}
	end := value.pos + int(size)
	result := value.data[value.pos:end]
	value.pos = end
	return result, true
}

read_fixed32 :: proc(value: ^Decoder) -> (u32, bool) {
	if value == nil || !value.ok || value.pos + 4 > len(value.data) {
		if value != nil { value.ok = false }
		return 0, false
	}
	data := value.data[value.pos:]
	value.pos += 4
	return u32(data[0]) | u32(data[1]) << 8 | u32(data[2]) << 16 | u32(data[3]) << 24, true
}

read_fixed64 :: proc(value: ^Decoder) -> (u64, bool) {
	if value == nil || !value.ok || value.pos + 8 > len(value.data) {
		if value != nil { value.ok = false }
		return 0, false
	}
	data := value.data[value.pos:]
	value.pos += 8
	result: u64
	for index in 0..<8 {
		result |= u64(data[index]) << u64(index * 8)
	}
	return result, true
}

read_float32 :: proc(value: ^Decoder) -> (f32, bool) {
	bits, ok := read_fixed32(value)
	return transmute(f32)bits, ok
}

read_float64 :: proc(value: ^Decoder) -> (f64, bool) {
	bits, ok := read_fixed64(value)
	return transmute(f64)bits, ok
}

skip_value :: proc(value: ^Decoder, wire_type: u8) -> bool {
	switch wire_type {
	case 0:
		_, ok := read_varint(value)
		return ok
	case 1:
		if value.pos + 8 > len(value.data) {
			value.ok = false
			return false
		}
		value.pos += 8
		return true
	case 2:
		_, ok := read_bytes(value)
		return ok
	case 5:
		if value.pos + 4 > len(value.data) {
			value.ok = false
			return false
		}
		value.pos += 4
		return true
	case:
		value.ok = false
		return false
	}
}

decode_zigzag32 :: proc(value: u64) -> i32 {
	return i32(value >> 1) ~ -i32(value & 1)
}

decode_zigzag64 :: proc(value: u64) -> i64 {
	return i64(value >> 1) ~ -i64(value & 1)
}

encode_zigzag32 :: proc(value: i32) -> u64 {
	return u64(u32(value) << 1 ~ u32(value >> 31))
}

encode_zigzag64 :: proc(value: i64) -> u64 {
	return u64(value << 1 ~ value >> 63)
}

append_varint :: proc(buffer: ^[dynamic]u8, value: u64) {
	remaining := value
	for remaining >= 0x80 {
		append(buffer, u8(remaining) | 0x80)
		remaining >>= 7
	}
	append(buffer, u8(remaining))
}

append_key :: proc(buffer: ^[dynamic]u8, field_number: int, wire_type: u8) {
	append_varint(buffer, u64(field_number) << 3 | u64(wire_type))
}

append_varint_field :: proc(buffer: ^[dynamic]u8, field_number: int, value: u64) {
	append_key(buffer, field_number, 0)
	append_varint(buffer, value)
}

append_bytes_field :: proc(buffer: ^[dynamic]u8, field_number: int, value: []u8) {
	append_key(buffer, field_number, 2)
	append_varint(buffer, u64(len(value)))
	append(buffer, ..value)
}

append_string_field :: proc(buffer: ^[dynamic]u8, field_number: int, value: string) {
	append_bytes_field(buffer, field_number, transmute([]u8)value)
}

append_fixed32_field :: proc(buffer: ^[dynamic]u8, field_number: int, value: u32) {
	append_key(buffer, field_number, 5)
	for index in 0..<4 {
		append(buffer, u8(value >> u32(index * 8)))
	}
}

append_fixed64_field :: proc(buffer: ^[dynamic]u8, field_number: int, value: u64) {
	append_key(buffer, field_number, 1)
	for index in 0..<8 {
		append(buffer, u8(value >> u64(index * 8)))
	}
}

append_float32_field :: proc(buffer: ^[dynamic]u8, field_number: int, value: f32) {
	append_fixed32_field(buffer, field_number, transmute(u32)value)
}

append_float64_field :: proc(buffer: ^[dynamic]u8, field_number: int, value: f64) {
	append_fixed64_field(buffer, field_number, transmute(u64)value)
}

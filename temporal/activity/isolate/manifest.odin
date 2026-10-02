package activity_isolate

import "core:encoding/endian"

MANIFEST_HEADER_SIZE :: 4

// host_manifest_encode returns the canonical registration-order manifest used
// to fence parent/child binary skew. The caller owns the returned bytes.
host_manifest_encode :: proc(
	activities: []Host_Activity,
	allocator := context.allocator,
) -> (encoded: []u8, ok: bool) {
	if len(activities) == 0 || u64(len(activities)) > u64(max(u32)) {
		return
	}
	total := u64(MANIFEST_HEADER_SIZE)
	for activity_registration in activities {
		if len(activity_registration.name) == 0 ||
		   u64(len(activity_registration.name)) > u64(max(u32)) {
			return
		}
		total += 4 + u64(len(activity_registration.name))
		if total > u64(max(int)) {
			return
		}
	}
	encoded = make([]u8, int(total), allocator)
	endian.put_u32(encoded[0:4], .Big, u32(len(activities)))
	offset := 4
	for activity_registration in activities {
		name_bytes := transmute([]u8)activity_registration.name
		endian.put_u32(encoded[offset:offset + 4], .Big, u32(len(name_bytes)))
		offset += 4
		copy(encoded[offset:offset + len(name_bytes)], name_bytes)
		offset += len(name_bytes)
	}
	return encoded, true
}

host_options_manifest_encode :: proc(
	options: Host_Options,
	allocator := context.allocator,
) -> (encoded: []u8, ok: bool) {
	if !host_options_valid(options) {
		return
	}
	if len(options.activities) > 0 {
		return host_manifest_encode(options.activities, allocator)
	}
	activity_registration := Host_Activity{
		name = options.activity_name,
		callback_data = options.callback_data,
		invoke = options.invoke,
	}
	return host_manifest_encode([]Host_Activity{activity_registration}, allocator)
}

host_manifest_valid :: proc(data: []u8) -> bool {
	if len(data) < MANIFEST_HEADER_SIZE {
		return false
	}
	count, _ := endian.get_u32(data[0:4], .Big)
	if count == 0 {
		return false
	}
	offset := u64(MANIFEST_HEADER_SIZE)
	for _ in 0 ..< int(count) {
		if offset + 4 > u64(len(data)) {
			return false
		}
		name_length, _ := endian.get_u32(data[int(offset):int(offset + 4)], .Big)
		offset += 4
		if name_length == 0 || offset + u64(name_length) > u64(len(data)) {
			return false
		}
		offset += u64(name_length)
	}
	return offset == u64(len(data))
}

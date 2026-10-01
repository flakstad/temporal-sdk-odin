package workflow_isolate

import "core:encoding/endian"
import workflow ".."

MANIFEST_HEADER_SIZE :: 4

// registry_manifest_encode returns a canonical, length-prefixed manifest in
// registration order. The caller owns the returned bytes.
registry_manifest_encode :: proc(
	registry: ^workflow.Registry,
) -> (encoded: []u8, ok: bool) {
	count := workflow.registry_count(registry)
	if count <= 0 || u64(count) > u64(max(u32)) {
		return
	}
	total := u64(MANIFEST_HEADER_SIZE)
	for index in 0 ..< count {
		name, name_ok := workflow.registry_name(registry, index)
		if !name_ok || len(name) == 0 || u64(len(name)) > u64(max(u32)) {
			return
		}
		total += 4 + u64(len(name))
		if total > u64(max(int)) {
			return
		}
	}
	encoded = make([]u8, int(total))
	endian.put_u32(encoded[0:4], .Big, u32(count))
	offset := 4
	for index in 0 ..< count {
		name, _ := workflow.registry_name(registry, index)
		endian.put_u32(encoded[offset:offset + 4], .Big, u32(len(name)))
		offset += 4
		copy(encoded[offset:offset + len(name)], transmute([]u8)name)
		offset += len(name)
	}
	return encoded, true
}

registry_manifest_valid :: proc(data: []u8) -> bool {
	if len(data) < MANIFEST_HEADER_SIZE {
		return false
	}
	count, _ := endian.get_u32(data[0:4], .Big)
	if count == 0 {
		return false
	}
	offset := u64(4)
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

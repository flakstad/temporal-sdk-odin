package workflow_pbt_test

import "core:c"
import "core:testing"
import pbt "pbt:pbt"
import history_json_raw "../temporal/history_json/raw"

history_json_ffi_ownership_property :: proc(t: ^pbt.T) -> pbt.Result {
	input := pbt.draw(t, pbt.byte_array(0, 256))
	pbt.cover(t, len(input) == 0, 0.2, "empty input")
	has_zero := false
	for value in input {
		if value == 0 {
			has_zero = true
			break
		}
	}
	pbt.cover(t, has_zero, 5, "embedded zero byte")

	data: ^u8
	if len(input) > 0 {
		data = raw_data(input)
	}
	result := history_json_raw.to_protobuf(data, c.size_t(len(input)))
	defer history_json_raw.bytes_free(result.protobuf)
	defer history_json_raw.bytes_free(result.fail)
	if (result.protobuf == nil) == (result.fail == nil) {
		return pbt.fail("History JSON FFI did not return exactly one owned result")
	}
	owned := result.protobuf if result.protobuf != nil else result.fail
	if owned.size > owned.capacity {
		return pbt.fail("History JSON FFI returned an invalid owned-buffer layout")
	}
	if owned.capacity > 0 && owned.data == nil {
		return pbt.fail("History JSON FFI returned a null allocation with positive capacity")
	}
	return pbt.pass()
}

@(test)
history_json_ffi_owns_one_result_for_arbitrary_bytes :: proc(t: ^testing.T) {
	result := pbt.check(
		"History JSON FFI owns one result for arbitrary bytes",
		history_json_ffi_ownership_property,
		{num_tests = 1000, seed = 0x4849_5354_4a53_4f4e},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

package workflow_pbt_test

import "core:bytes"
import "core:testing"
import pbt "pbt:pbt"
import workflow_isolate "../temporal/workflow/isolate"

workflow_isolation_frame_property :: proc(t: ^pbt.T) -> pbt.Result {
	run_id := pbt.draw(t, pbt.non_empty_byte_array(96))
	payload := pbt.draw(t, pbt.non_empty_byte_array(8_192))
	request_high := u64(pbt.draw(t, pbt.int_range(0, 0x7fff_ffff)))
	request_low := u64(pbt.draw(t, pbt.int_range(0, 0x7fff_ffff)))
	request_id := request_high << 32 | request_low
	if pbt.draw(t, pbt.boolean()) {
		request_id |= u64(1) << 63
	}
	kind := workflow_isolate.Kind.Activation
	if pbt.draw(t, pbt.boolean()) {
		kind = .Completion
	}
	has_zero := false
	for value in payload {
		if value == 0 {
			has_zero = true
			break
		}
	}
	pbt.cover(t, kind == .Completion, 35, "completion frame")
	pbt.cover(t, request_id & (u64(1) << 63) != 0, 35, "high-bit correlation ID")
	pbt.cover(t, len(payload) >= 4_096, 25, "large frame payload")
	pbt.cover(t, has_zero, 35, "embedded zero payload byte")

	frame := workflow_isolate.Frame{
		kind = kind,
		request_id = request_id,
		run_id = run_id,
		payload = payload,
	}
	encoded, encode_error := workflow_isolate.encode(frame)
	defer delete(encoded)
	if encode_error.kind != .None {
		return pbt.fail("valid generated isolation frame did not encode")
	}
	decoded, decode_error := workflow_isolate.decode(encoded)
	defer workflow_isolate.owned_frame_free(&decoded)
	if decode_error.kind != .None || decoded.frame.kind != kind ||
	   decoded.frame.request_id != request_id ||
	   !bytes.equal(decoded.frame.run_id, run_id) ||
	   !bytes.equal(decoded.frame.payload, payload) {
		return pbt.fail("generated isolation frame did not roundtrip exactly")
	}
	return pbt.pass()
}

@(test)
workflow_isolation_frames_roundtrip_generated_binary_payloads :: proc(t: ^testing.T) {
	result := pbt.check(
		"Workflow isolation frames roundtrip generated binary payloads",
		workflow_isolation_frame_property,
		{num_tests = 500, seed = 0x4953_4f4c_4154_4544},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

package workflow_isolate

import "core:encoding/json"
import workflow ".."

// Process metadata is diagnostic language-runtime state. The Temporal command
// completion remains a separate opaque Core protobuf frame and is authoritative
// for command/terminal status.
process_result_metadata_encode :: proc(
	result: ^workflow.Process_Result,
) -> (encoded: []u8, ok: bool) {
	if result == nil {
		return
	}
	metadata := result^
	metadata.completion = nil
	metadata.run_id = ""
	marshaled, encode_error := json.marshal(metadata)
	if encode_error != nil {
		delete(marshaled)
		return nil, false
	}
	return marshaled, true
}

process_result_metadata_free :: proc(result: ^workflow.Process_Result) {
	if result == nil {
		return
	}
	delete(result.completion)
	delete(result.run_id)
	delete(result.cancellation_reason)
	workflow.error_free(&result.error)
	result^ = {}
}

process_result_metadata_decode :: proc(
	data: []u8,
) -> (result: workflow.Process_Result, ok: bool) {
	decode_error := json.unmarshal(data, &result)
	if decode_error != nil || result.completion != nil || len(result.run_id) != 0 {
		process_result_metadata_free(&result)
		return {}, false
	}
	return result, true
}

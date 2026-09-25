// Package codec isolates user payload serialization from Core's protobuf
// envelopes. JSON helpers accept ordinary Odin values and structs.
package codec

import "core:encoding/json"

Error :: enum {
	None,
	Unsupported_Encoding,
	Invalid_JSON,
	Encode_Failed,
	Allocation_Failed,
}

decode_json :: proc(payload: Payload_View, value: ^$T) -> Error {
	if encoding(payload) != "json/plain" {
		return .Unsupported_Encoding
	}
	json_error := json.unmarshal(payload.data, value)
	if json_error != nil {
		return .Invalid_JSON
	}
	return .None
}

encode_json :: proc(value: $T, allocator := context.allocator) -> (payload: Payload, error: Error) {
	data, json_error := json.marshal(value, allocator = allocator)
	if json_error != nil {
		return {}, .Encode_Failed
	}
	payload = {
		metadata = make([dynamic]Metadata_Entry, allocator),
		data = data,
		external_payloads = make([dynamic]External_Payload_Details, allocator),
		allocator = allocator,
	}
	if payload_add_metadata(&payload, "encoding", transmute([]u8)string("json/plain")) != .None {
		payload_free(&payload)
		return {}, .Allocation_Failed
	}
	return payload, .None
}

decode_string :: proc(data: []u8, encoding: string) -> (value: string, error: Error) {
	metadata := [1]Metadata_Entry{{key = "encoding", value = transmute([]u8)encoding}}
	error = decode_json(Payload_View{metadata = metadata[:], data = data}, &value)
	return
}

encode_string :: proc(value: string) -> (data: []u8, error: Error) {
	json_data, json_error := json.marshal(value)
	if json_error != nil {
		return nil, .Encode_Failed
	}
	return json_data, .None
}

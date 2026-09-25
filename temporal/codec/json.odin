// Package codec isolates user payload serialization from Core's protobuf
// envelopes. Milestone 2 intentionally supports one JSON string argument.
package codec

import "core:encoding/json"

Error :: enum {
	None,
	Unsupported_Encoding,
	Invalid_JSON,
	Encode_Failed,
}

decode_string :: proc(data: []u8, encoding: string) -> (value: string, error: Error) {
	if encoding != "json/plain" {
		return "", .Unsupported_Encoding
	}
	json_error := json.unmarshal(data, &value)
	if json_error != nil {
		return "", .Invalid_JSON
	}
	return value, .None
}

encode_string :: proc(value: string) -> (data: []u8, error: Error) {
	json_data, json_error := json.marshal(value)
	if json_error != nil {
		return nil, .Encode_Failed
	}
	return json_data, .None
}

// Package headers owns Temporal Header keys and codec-neutral Payload values.
// Headers are transport metadata used by interceptors and context propagation;
// they are separate from Workflow/Activity user arguments.
package headers

import "core:mem"
import "core:strings"
import codec "../codec"

Error :: enum {
	None,
	Invalid_Argument,
	Payload_Conversion,
	Allocation_Failed,
}

Entry :: struct {
	key:     string,
	payload: codec.Payload,
}

Headers :: struct {
	entries:   [dynamic]Entry,
	allocator: mem.Allocator,
}

init :: proc(headers: ^Headers, allocator := context.allocator) -> Error {
	if headers == nil || allocator.procedure == nil || headers.allocator.procedure != nil {
		return .Invalid_Argument
	}
	headers.entries = make([dynamic]Entry, allocator)
	headers.allocator = allocator
	return .None
}

free :: proc(headers: ^Headers) {
	if headers == nil || headers.allocator.procedure == nil {
		return
	}
	allocator := headers.allocator
	for &entry in headers.entries {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	delete(headers.entries)
	headers^ = {}
}

put_owned :: proc(headers: ^Headers, key: string, payload: codec.Payload) -> Error {
	owned_payload := payload
	if headers == nil || headers.allocator.procedure == nil || len(key) == 0 {
		codec.payload_free(&owned_payload)
		return .Invalid_Argument
	}
	for &entry in headers.entries {
		if entry.key == key {
			codec.payload_free(&entry.payload)
			entry.payload = owned_payload
			return .None
		}
	}
	owned_key, allocation_error := strings.clone(key, headers.allocator)
	if allocation_error != nil {
		codec.payload_free(&owned_payload)
		return .Allocation_Failed
	}
	append(&headers.entries, Entry{key = owned_key, payload = owned_payload})
	return .None
}

add_payload :: proc(headers: ^Headers, key: string, value: codec.Payload_View) -> Error {
	if headers == nil || headers.allocator.procedure == nil {
		return .Invalid_Argument
	}
	payload, clone_error := codec.payload_clone(value, headers.allocator)
	if clone_error != .None {
		return .Payload_Conversion
	}
	return put_owned(headers, key, payload)
}

add_json :: proc(headers: ^Headers, key: string, value: $T) -> Error {
	if headers == nil || headers.allocator.procedure == nil {
		return .Invalid_Argument
	}
	payload, encode_error := codec.encode_json(value, headers.allocator)
	if encode_error != .None {
		return .Payload_Conversion
	}
	return put_owned(headers, key, payload)
}

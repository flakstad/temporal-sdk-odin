// Package codec owns language-facing payload values. Payload_View borrows all
// bytes; Payload owns its data and metadata through its recorded allocator.
package codec

import "core:bytes"
import "core:mem"
import "core:strings"

Metadata_Entry :: struct {
	key:   string,
	value: []u8,
}

External_Payload_Details :: struct {
	size_bytes: i64,
}

Payload_View :: struct {
	metadata:          []Metadata_Entry,
	data:              []u8,
	external_payloads: []External_Payload_Details,
}

Payload :: struct {
	metadata:          [dynamic]Metadata_Entry,
	data:              []u8,
	external_payloads: [dynamic]External_Payload_Details,
	allocator:         mem.Allocator,
}

payload_view :: proc(payload: ^Payload) -> Payload_View {
	if payload == nil {
		return {}
	}
	return {
		metadata = payload.metadata[:],
		data = payload.data,
		external_payloads = payload.external_payloads[:],
	}
}

metadata_get :: proc(payload: Payload_View, key: string) -> ([]u8, bool) {
	value: []u8
	found := false
	for entry in payload.metadata {
		if entry.key == key {
			value = entry.value
			found = true
		}
	}
	return value, found
}

encoding :: proc(payload: Payload_View) -> string {
	value, found := metadata_get(payload, "encoding")
	if !found {
		return ""
	}
	return string(value)
}

payload_create :: proc(data: []u8, allocator := context.allocator) -> Payload {
	return {
		metadata = make([dynamic]Metadata_Entry, allocator),
		data = bytes.clone(data, allocator),
		external_payloads = make([dynamic]External_Payload_Details, allocator),
		allocator = allocator,
	}
}

payload_add_metadata :: proc(payload: ^Payload, key: string, value: []u8) -> Error {
	if payload == nil || payload.allocator.procedure == nil {
		return .Allocation_Failed
	}
	for &entry in payload.metadata {
		if entry.key == key {
			owned_value := bytes.clone(value, payload.allocator)
			delete(entry.value, payload.allocator)
			entry.value = owned_value
			return .None
		}
	}
	owned_key, allocation_error := strings.clone(key, payload.allocator)
	if allocation_error != nil {
		return .Allocation_Failed
	}
	owned_value := bytes.clone(value, payload.allocator)
	append(&payload.metadata, Metadata_Entry{key = owned_key, value = owned_value})
	return .None
}

payload_add_external :: proc(payload: ^Payload, size_bytes: i64) -> Error {
	if payload == nil || payload.allocator.procedure == nil {
		return .Allocation_Failed
	}
	append(&payload.external_payloads, External_Payload_Details{size_bytes = size_bytes})
	return .None
}

payload_clone :: proc(view: Payload_View, allocator := context.allocator) -> (payload: Payload, error: Error) {
	payload = payload_create(view.data, allocator)
	for entry in view.metadata {
		if payload_add_metadata(&payload, entry.key, entry.value) != .None {
			payload_free(&payload)
			return {}, .Allocation_Failed
		}
	}
	for external in view.external_payloads {
		if payload_add_external(&payload, external.size_bytes) != .None {
			payload_free(&payload)
			return {}, .Allocation_Failed
		}
	}
	return payload, .None
}

payload_free :: proc(payload: ^Payload) {
	if payload == nil {
		return
	}
	if payload.allocator.procedure != nil {
		for entry in payload.metadata {
			delete(entry.key, payload.allocator)
			delete(entry.value, payload.allocator)
		}
		delete(payload.metadata)
		delete(payload.data, payload.allocator)
		delete(payload.external_payloads)
	}
	payload^ = {}
}

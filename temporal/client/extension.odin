package client

import allocator_runtime "base:runtime"
import "core:strings"

// Client_Extension_State lets an optional client package attach owned state
// without making the base Client depend on that package's concrete types.
// Registration is intended during Client setup and is not concurrent with use.
Client_Extension_Destroy_Proc :: #type proc(state: rawptr)

Client_Extension_State :: struct {
	key:     string,
	state:   rawptr,
	destroy: Client_Extension_Destroy_Proc,
}

client_extension_state_find :: proc(client: ^Client, key: string) -> rawptr {
	if client == nil || len(key) == 0 {
		return nil
	}
	for &extension in client.extensions {
		if extension.key == key {
			return extension.state
		}
	}
	return nil
}

client_extension_state_register :: proc(
	client: ^Client,
	key: string,
	state: rawptr,
	destroy: Client_Extension_Destroy_Proc,
) -> Error {
	if client == nil || client.connection == nil {
		return error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if len(key) == 0 || state == nil || destroy == nil {
		return error_from_string(.Invalid_Argument, "Client extension registration is incomplete")
	}
	if client_extension_state_find(client, key) != nil {
		return error_from_string(.Invalid_Argument, "Client extension is already registered")
	}
	heap := allocator_runtime.heap_allocator()
	owned_key, clone_error := strings.clone(key, heap)
	if clone_error != nil {
		return error_from_string(.Connection, "could not allocate Client extension key")
	}
	if client.extensions == nil {
		client.extensions = make([dynamic]Client_Extension_State, heap)
	}
	append(&client.extensions, Client_Extension_State{
		key = owned_key,
		state = state,
		destroy = destroy,
	})
	return {}
}

client_extensions_close :: proc(client: ^Client) {
	if client == nil {
		return
	}
	heap := allocator_runtime.heap_allocator()
	for &extension in client.extensions {
		if extension.destroy != nil && extension.state != nil {
			extension.destroy(extension.state)
		}
		delete(extension.key, heap)
	}
	delete(client.extensions)
	client.extensions = nil
}

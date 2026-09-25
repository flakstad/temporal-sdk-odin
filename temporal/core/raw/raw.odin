// Package raw is a deliberately thin subset of Temporal SDK Core's C ABI.
// Keep field order and widths synchronized with the pinned C header.
package raw

import "core:c"

when ODIN_OS == .Darwin || ODIN_OS == .Linux {
	foreign import temporal_core "system:temporalio_sdk_core_c_bridge"
} else {
	#panic("Temporal SDK Core spike currently supports macOS and Linux")
}

Runtime    :: struct { _opaque: u8 }
Connection :: struct { _opaque: u8 }

Byte_Array_Ref :: struct {
	data: ^u8,
	size: c.size_t,
}

Byte_Array_Ref_Array :: struct {
	data: ^Byte_Array_Ref,
	size: c.size_t,
}

Metadata_Ref :: Byte_Array_Ref_Array

Byte_Array :: struct {
	data:         ^u8,
	size:         c.size_t,
	cap:          c.size_t,
	disable_free: bool,
}

Runtime_Type :: enum c.int {
	Unspecified     = 0,
	Jvm             = 1,
	Cpython         = 2,
	Node            = 3,
	Bun             = 4,
	Cruby           = 5,
	Go              = 6,
	Dotnet_Framework = 7,
	Dotnet_Core      = 8,
	Native           = 9,
	Roadrunner       = 10,
}

Runtime_Info :: struct {
	runtime_type: Runtime_Type,
	version:      Byte_Array_Ref,
}

Runtime_Info_Array :: struct {
	data: ^Runtime_Info,
	size: c.size_t,
}

Telemetry_Options :: struct {
	logging: rawptr,
	metrics: rawptr,
}

Runtime_Options :: struct {
	telemetry:                           ^Telemetry_Options,
	worker_heartbeat_interval_millis:    u64,
	runtime_info:                        Runtime_Info_Array,
	disable_environment_info:            bool,
}

Runtime_Or_Fail :: struct {
	runtime: ^Runtime,
	fail:    ^Byte_Array,
}

Client_Grpc_Compression :: enum c.int {
	Gzip = 0,
	None = 1,
}

Client_Grpc_Override_Callback :: #type proc "c" (request, user_data: rawptr)

Connection_Options :: struct {
	target_url:                       Byte_Array_Ref,
	client_name:                      Byte_Array_Ref,
	client_version:                   Byte_Array_Ref,
	metadata:                         Metadata_Ref,
	binary_metadata:                  Metadata_Ref,
	api_key:                          Byte_Array_Ref,
	identity:                         Byte_Array_Ref,
	tls_options:                      rawptr,
	retry_options:                    rawptr,
	keep_alive_options:               rawptr,
	http_connect_proxy_options:       rawptr,
	grpc_override_callback:           Client_Grpc_Override_Callback,
	grpc_override_callback_user_data: rawptr,
	dns_load_balancing_options:       rawptr,
	grpc_compression:                 Client_Grpc_Compression,
	payloads_warn_size:               u64,
	memo_warn_size:                   u64,
}

Client_Connect_Callback :: #type proc "c" (
	user_data: rawptr,
	success:   ^Connection,
	fail:      ^Byte_Array,
)

Rpc_Service :: enum c.int {
	Workflow = 1,
	Operator,
	Cloud,
	Test,
	Health,
}

Rpc_Call_Options :: struct {
	service:            Rpc_Service,
	rpc:                Byte_Array_Ref,
	req:                Byte_Array_Ref,
	retry:              bool,
	metadata:           Metadata_Ref,
	binary_metadata:    Metadata_Ref,
	timeout_millis:      u32,
	cancellation_token: rawptr,
}

Client_Rpc_Call_Callback :: #type proc "c" (
	user_data:       rawptr,
	success:         ^Byte_Array,
	status_code:     u32,
	failure_message: ^Byte_Array,
	failure_details: ^Byte_Array,
)

#assert(size_of(bool) == 1)
#assert(size_of(Runtime_Type) == size_of(c.int))
#assert(size_of(Rpc_Service) == size_of(c.int))
when size_of(uintptr) == 8 {
	// Values verified against Clang's layout of the pinned generated C header.
	#assert(size_of(Byte_Array_Ref) == 16)
	#assert(size_of(Byte_Array) == 32)
	#assert(offset_of(Byte_Array, disable_free) == 24)
	#assert(size_of(Runtime_Options) == 40)
	#assert(offset_of(Runtime_Options, disable_environment_info) == 32)
	#assert(size_of(Connection_Options) == 192)
	#assert(offset_of(Connection_Options, grpc_compression) == 168)
	#assert(offset_of(Connection_Options, memo_warn_size) == 184)
	#assert(size_of(Rpc_Call_Options) == 96)
	#assert(offset_of(Rpc_Call_Options, retry) == 40)
	#assert(offset_of(Rpc_Call_Options, cancellation_token) == 88)
}

@(default_calling_convention = "c", link_prefix = "temporal_core_")
foreign temporal_core {
	runtime_new       :: proc(options: ^Runtime_Options) -> Runtime_Or_Fail ---
	runtime_free      :: proc(runtime: ^Runtime) ---
	byte_array_free   :: proc(runtime: ^Runtime, bytes: ^Byte_Array) ---
	client_connect    :: proc(
		runtime: ^Runtime,
		options: ^Connection_Options,
		user_data: rawptr,
		callback: Client_Connect_Callback,
	) ---
	client_free       :: proc(client: ^Connection) ---
	client_rpc_call   :: proc(
		client: ^Connection,
		options: ^Rpc_Call_Options,
		user_data: rawptr,
		callback: Client_Rpc_Call_Callback,
	) ---
}

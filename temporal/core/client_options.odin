package core

import "core:bytes"
import "core:mem"
import "core:strings"
import "core:time"
import raw "./raw"

Client_Metadata_Entry :: struct {
	key:   string,
	value: []u8,
}

Client_TLS_Options :: struct {
	server_root_ca_cert: []u8,
	domain:              string,
	client_cert:         []u8,
	client_private_key:  []u8,
}

Client_Retry_Options :: struct {
	initial_interval:    time.Duration,
	randomization_factor: f64,
	multiplier:          f64,
	maximum_interval:    time.Duration,
	maximum_elapsed_time: time.Duration,
	maximum_retries:     uintptr,
}

Client_Keep_Alive_Options :: struct {
	interval: time.Duration,
	timeout:  time.Duration,
}

Client_HTTP_Connect_Proxy_Options :: struct {
	target_host: string,
	username:    string,
	password:    string,
}

Client_DNS_Load_Balancing_Options :: struct {
	resolution_interval: time.Duration,
}

Client_Grpc_Compression :: enum {
	Gzip,
	None,
}

Client_Connection_Options :: struct {
	target_url:                 string,
	client_name:                string,
	client_version:             string,
	identity:                   string,
	metadata:                   []Client_Metadata_Entry,
	binary_metadata:            []Client_Metadata_Entry,
	api_key:                    string,
	tls:                        ^Client_TLS_Options,
	tls_disabled:               bool,
	retry:                      ^Client_Retry_Options,
	keep_alive:                 ^Client_Keep_Alive_Options,
	http_connect_proxy:         ^Client_HTTP_Connect_Proxy_Options,
	dns_load_balancing:         ^Client_DNS_Load_Balancing_Options,
	grpc_compression:           Client_Grpc_Compression,
	payloads_warn_size:         u64,
	memo_warn_size:             u64,
}

Client_Connection_Bindings :: struct {
	raw_options:                raw.Connection_Options,
	tls:                        raw.Client_TLS_Options,
	retry:                      raw.Client_Retry_Options,
	keep_alive:                 raw.Client_Keep_Alive_Options,
	http_connect_proxy:         raw.Client_HTTP_Connect_Proxy_Options,
	dns_load_balancing:         raw.Client_DNS_Load_Balancing_Options,
	metadata_buffers:           [dynamic][]u8,
	metadata_refs:              [dynamic]raw.Byte_Array_Ref,
	binary_metadata_buffers:    [dynamic][]u8,
	binary_metadata_refs:       [dynamic]raw.Byte_Array_Ref,
	allocator:                  mem.Allocator,
}

client_option_error :: proc(message: string) -> Error {
	return {message = bytes.clone(transmute([]u8)message)}
}

client_metadata_key_valid :: proc(key: string, binary: bool) -> bool {
	if len(key) == 0 {
		return false
	}
	for character in transmute([]u8)key {
		if !((character >= 'a' && character <= 'z') ||
		     (character >= '0' && character <= '9') ||
		     character == '-' || character == '_' || character == '.') {
			return false
		}
	}
	has_binary_suffix := strings.has_suffix(key, "-bin")
	return has_binary_suffix == binary
}

client_text_metadata_value_valid :: proc(value: []u8) -> bool {
	for character in value {
		if character < 0x20 || character > 0x7e {
			return false
		}
	}
	return true
}

client_metadata_valid :: proc(entries: []Client_Metadata_Entry, binary: bool) -> bool {
	for entry, index in entries {
		if !client_metadata_key_valid(entry.key, binary) ||
		   (!binary && !client_text_metadata_value_valid(entry.value)) {
			return false
		}
		for prior_index in 0 ..< index {
			if entries[prior_index].key == entry.key {
				return false
			}
		}
	}
	return true
}

client_duration_millis_valid :: proc(value: time.Duration, allow_zero: bool) -> bool {
	if value < 0 {
		return false
	}
	if value == 0 {
		return allow_zero
	}
	return value >= time.Millisecond
}

client_connection_options_validate :: proc(options: Client_Connection_Options) -> Error {
	if len(options.target_url) == 0 {
		return client_option_error("Temporal target URL must not be empty")
	}
	if len(options.client_name) == 0 || len(options.client_version) == 0 {
		return client_option_error("Temporal client name and version must not be empty")
	}
	if len(options.identity) == 0 {
		return client_option_error("Temporal client identity must not be empty")
	}
	if !client_metadata_valid(options.metadata, false) {
		return client_option_error("Temporal RPC metadata contains an invalid or duplicate ASCII entry")
	}
	if !client_metadata_valid(options.binary_metadata, true) {
		return client_option_error("Temporal binary RPC metadata contains an invalid or duplicate entry")
	}
	if len(options.api_key) > 0 &&
	   !client_text_metadata_value_valid(transmute([]u8)options.api_key) {
		return client_option_error("Temporal API key is not valid ASCII metadata")
	}
	if options.tls != nil {
		if options.tls_disabled {
			return client_option_error("Temporal TLS options cannot be combined with explicit TLS disablement")
		}
		has_cert := len(options.tls.client_cert) > 0
		has_key := len(options.tls.client_private_key) > 0
		if has_cert != has_key {
			return client_option_error("Temporal mTLS requires both a client certificate and private key")
		}
	}
	if options.retry != nil {
		if !client_duration_millis_valid(options.retry.initial_interval, false) ||
		   !client_duration_millis_valid(options.retry.maximum_interval, false) ||
		   !client_duration_millis_valid(options.retry.maximum_elapsed_time, true) ||
		   options.retry.randomization_factor < 0 || options.retry.randomization_factor > 1 ||
		   options.retry.multiplier < 1 {
			return client_option_error("Temporal Client Retry Policy is invalid")
		}
	}
	if options.keep_alive != nil &&
	   (!client_duration_millis_valid(options.keep_alive.interval, false) ||
	    !client_duration_millis_valid(options.keep_alive.timeout, false)) {
		return client_option_error("Temporal Client keepalive durations must be at least one millisecond")
	}
	if options.http_connect_proxy != nil {
		if len(options.http_connect_proxy.target_host) == 0 {
			return client_option_error("Temporal HTTP CONNECT proxy target must not be empty")
		}
		has_username := len(options.http_connect_proxy.username) > 0
		has_password := len(options.http_connect_proxy.password) > 0
		if has_username != has_password {
			return client_option_error("Temporal HTTP CONNECT proxy requires both username and password")
		}
	}
	if options.dns_load_balancing != nil &&
	   !client_duration_millis_valid(options.dns_load_balancing.resolution_interval, false) {
		return client_option_error("Temporal DNS resolution interval must be at least one millisecond")
	}
	return {}
}

client_metadata_bind :: proc(
	entries: []Client_Metadata_Entry,
	buffers: ^[dynamic][]u8,
	refs: ^[dynamic]raw.Byte_Array_Ref,
	allocator: mem.Allocator,
) {
	for entry in entries {
		buffer := make([]u8, len(entry.key) + 1 + len(entry.value), allocator)
		copy(buffer, transmute([]u8)entry.key)
		buffer[len(entry.key)] = '\n'
		copy(buffer[len(entry.key) + 1:], entry.value)
		append(buffers, buffer)
		append(refs, bytes_ref(buffer))
	}
}

client_metadata_ref :: proc(refs: []raw.Byte_Array_Ref) -> raw.Metadata_Ref {
	if len(refs) == 0 {
		return {}
	}
	return {data = raw_data(refs), size = len(refs)}
}

client_connection_bindings_free :: proc(bindings: ^Client_Connection_Bindings) {
	if bindings == nil {
		return
	}
	allocator := bindings.allocator
	if allocator.procedure != nil {
		for buffer in bindings.metadata_buffers {
			delete(buffer, allocator)
		}
		for buffer in bindings.binary_metadata_buffers {
			delete(buffer, allocator)
		}
		delete(bindings.metadata_buffers)
		delete(bindings.metadata_refs)
		delete(bindings.binary_metadata_buffers)
		delete(bindings.binary_metadata_refs)
	}
	bindings^ = {}
}

client_connection_bindings_init :: proc(
	bindings: ^Client_Connection_Bindings,
	options: Client_Connection_Options,
	allocator := context.allocator,
) -> Error {
	if bindings == nil || allocator.procedure == nil {
		return client_option_error("Temporal Client connection bindings require an allocator")
	}
	validation_error := client_connection_options_validate(options)
	if validation_error.message != nil {
		return validation_error
	}
	if bindings.allocator.procedure != nil {
		return client_option_error("Temporal Client connection bindings are already initialized")
	}
	bindings.allocator = allocator
	bindings.metadata_buffers = make([dynamic][]u8, allocator)
	bindings.metadata_refs = make([dynamic]raw.Byte_Array_Ref, allocator)
	bindings.binary_metadata_buffers = make([dynamic][]u8, allocator)
	bindings.binary_metadata_refs = make([dynamic]raw.Byte_Array_Ref, allocator)
	client_metadata_bind(options.metadata, &bindings.metadata_buffers, &bindings.metadata_refs, allocator)
	client_metadata_bind(
		options.binary_metadata,
		&bindings.binary_metadata_buffers,
		&bindings.binary_metadata_refs,
		allocator,
	)

	bindings.raw_options = {
		target_url = byte_ref(options.target_url),
		client_name = byte_ref(options.client_name),
		client_version = byte_ref(options.client_version),
		metadata = client_metadata_ref(bindings.metadata_refs[:]),
		binary_metadata = client_metadata_ref(bindings.binary_metadata_refs[:]),
		api_key = byte_ref(options.api_key),
		identity = byte_ref(options.identity),
		grpc_compression = raw.Client_Grpc_Compression(options.grpc_compression),
		payloads_warn_size = options.payloads_warn_size,
		memo_warn_size = options.memo_warn_size,
	}
	if options.tls != nil || (len(options.api_key) > 0 && !options.tls_disabled) {
		if options.tls != nil {
			bindings.tls = {
				server_root_ca_cert = bytes_ref(options.tls.server_root_ca_cert),
				domain = byte_ref(options.tls.domain),
				client_cert = bytes_ref(options.tls.client_cert),
				client_private_key = bytes_ref(options.tls.client_private_key),
			}
		}
		bindings.raw_options.tls_options = &bindings.tls
	}
	if options.retry != nil {
		bindings.retry = {
			initial_interval_millis = u64(options.retry.initial_interval / time.Millisecond),
			randomization_factor = options.retry.randomization_factor,
			multiplier = options.retry.multiplier,
			max_interval_millis = u64(options.retry.maximum_interval / time.Millisecond),
			max_elapsed_time_millis = u64(options.retry.maximum_elapsed_time / time.Millisecond),
			max_retries = options.retry.maximum_retries,
		}
		bindings.raw_options.retry_options = &bindings.retry
	}
	if options.keep_alive != nil {
		bindings.keep_alive = {
			interval_millis = u64(options.keep_alive.interval / time.Millisecond),
			timeout_millis = u64(options.keep_alive.timeout / time.Millisecond),
		}
		bindings.raw_options.keep_alive_options = &bindings.keep_alive
	}
	if options.http_connect_proxy != nil {
		bindings.http_connect_proxy = {
			target_host = byte_ref(options.http_connect_proxy.target_host),
			username = byte_ref(options.http_connect_proxy.username),
			password = byte_ref(options.http_connect_proxy.password),
		}
		bindings.raw_options.http_connect_proxy_options = &bindings.http_connect_proxy
	}
	if options.dns_load_balancing != nil {
		bindings.dns_load_balancing = {
			resolution_interval_millis = u64(options.dns_load_balancing.resolution_interval / time.Millisecond),
		}
		bindings.raw_options.dns_load_balancing_options = &bindings.dns_load_balancing
	}
	return {}
}

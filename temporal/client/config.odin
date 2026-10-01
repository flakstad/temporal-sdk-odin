package client

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:encoding/json"
import "core:fmt"
import "core:mem"
import "core:mem/virtual"
import "core:os"
import "core:strings"
import temporal_core "../core"

Config_Source :: temporal_core.Client_Env_Config_Source

Config_Environment_Entry :: struct {
	key:   string,
	value: string,
}

Config_Load_Options :: struct {
	source:                   Config_Source,
	config_file_strict:       bool,
	use_environment_override: bool,
	environment:              []Config_Environment_Entry,
}

Config_Profile_Load_Options :: struct {
	profile:                  string,
	source:                   Config_Source,
	disable_file:             bool,
	disable_environment:      bool,
	config_file_strict:       bool,
	use_environment_override: bool,
	environment:              []Config_Environment_Entry,
}

Config_Data_Source_Kind :: enum {
	None,
	Path,
	Data,
}

Config_Data_Source :: struct {
	kind: Config_Data_Source_Kind,
	path: string,
	data: []u8,
}

Config_TLS :: struct {
	present:             bool,
	disabled:            bool,
	server_name:         string,
	server_ca_cert:      Config_Data_Source,
	client_cert:         Config_Data_Source,
	client_key:          Config_Data_Source,
}

Config_Codec :: struct {
	present:  bool,
	endpoint: string,
	auth:     string,
}

// Config_Profile owns every string, byte slice, and metadata entry. Do not
// value-copy it; free it once with config_profile_free. Codec settings are
// exposed as configuration data; remote codec transport is not part of the SDK
// yet.
Config_Profile :: struct {
	address:       string,
	namespace:     string,
	api_key:       string,
	tls:           Config_TLS,
	codec:         Config_Codec,
	grpc_metadata: [dynamic]RPC_Metadata_Entry,
	allocator:     mem.Allocator,
}

Named_Config_Profile :: struct {
	name:    string,
	profile: Config_Profile,
}

// Config owns its named profiles as one graph. Do not value-copy it; pointers
// returned by config_find_profile remain borrowed until config_free.
Config :: struct {
	profiles:  [dynamic]Named_Config_Profile,
	allocator: mem.Allocator,
}

Config_Wire_Data_Source :: struct {
	path: string,
	data: []u8,
}

Config_Wire_Optional_Data_Source :: union {
	Config_Wire_Data_Source,
}

Config_Wire_TLS :: struct {
	disabled:       bool,
	server_name:    string,
	server_ca_cert: Config_Wire_Optional_Data_Source,
	client_cert:    Config_Wire_Optional_Data_Source,
	client_key:     Config_Wire_Optional_Data_Source,
}

Config_Wire_Optional_TLS :: union {
	Config_Wire_TLS,
}

Config_Wire_Codec :: struct {
	endpoint: string,
	auth:     string,
}

Config_Wire_Optional_Codec :: union {
	Config_Wire_Codec,
}

Config_Wire_Profile :: struct {
	address:   string,
	namespace: string,
	api_key:   string,
	tls:       Config_Wire_Optional_TLS,
	codec:     Config_Wire_Optional_Codec,
	grpc_meta: map[string]string,
}

Config_Wire :: struct {
	profiles: map[string]Config_Wire_Profile,
}

config_environment_json :: proc(
	use_override: bool,
	entries: []Config_Environment_Entry,
	allocator: mem.Allocator,
) -> ([]u8, Error) {
	if !use_override {
		if len(entries) > 0 {
			return nil, error_from_string(
				.Configuration,
				"Temporal configuration environment entries require use_environment_override",
			)
		}
		return nil, {}
	}
	values := make(map[string]string, len(entries), allocator)
	defer delete(values)
	for entry in entries {
		if len(entry.key) == 0 {
			return nil, error_from_string(
				.Configuration,
				"Temporal configuration environment keys must not be empty",
			)
		}
		if _, found := values[entry.key]; found {
			return nil, error_from_string(
				.Configuration,
				"Temporal configuration environment keys must be unique",
			)
		}
		values[entry.key] = entry.value
	}
	encoded, encode_error := json.marshal(values, allocator = allocator)
	if encode_error != nil {
		return nil, error_from_string(
			.Configuration,
			"could not encode Temporal configuration environment override",
		)
	}
	return encoded, {}
}

config_string_clone :: proc(value: string, allocator: mem.Allocator) -> (string, bool) {
	if len(value) == 0 {
		return "", true
	}
	owned, allocation_error := strings.clone(value, allocator)
	return owned, allocation_error == nil
}

config_data_source_free :: proc(source: ^Config_Data_Source, allocator: mem.Allocator) {
	if source == nil {
		return
	}
	delete(source.path, allocator)
	delete(source.data, allocator)
	source^ = {}
}

config_data_source_clone :: proc(
	wire: Config_Wire_Optional_Data_Source,
	allocator: mem.Allocator,
) -> (source: Config_Data_Source, ok: bool) {
	ok = true
	#partial switch value in wire {
	case Config_Wire_Data_Source:
		if len(value.path) > 0 {
			source.kind = .Path
			source.path, ok = config_string_clone(value.path, allocator)
			return
		}
		if len(value.data) > 0 {
			source.kind = .Data
			allocation_error: allocator_runtime.Allocator_Error
			source.data, allocation_error = bytes.clone_safe(value.data, allocator)
			ok = allocation_error == nil
		}
	}
	return
}

config_profile_free :: proc(profile: ^Config_Profile) {
	if profile == nil {
		return
	}
	allocator := profile.allocator
	if allocator.procedure != nil {
		delete(profile.address, allocator)
		delete(profile.namespace, allocator)
		delete(profile.api_key, allocator)
		delete(profile.tls.server_name, allocator)
		config_data_source_free(&profile.tls.server_ca_cert, allocator)
		config_data_source_free(&profile.tls.client_cert, allocator)
		config_data_source_free(&profile.tls.client_key, allocator)
		delete(profile.codec.endpoint, allocator)
		delete(profile.codec.auth, allocator)
		for &entry in profile.grpc_metadata {
			delete(entry.key, allocator)
			delete(entry.value, allocator)
		}
		delete(profile.grpc_metadata)
	}
	profile^ = {}
}

config_profile_clone_wire :: proc(
	wire: Config_Wire_Profile,
	allocator: mem.Allocator,
) -> (profile: Config_Profile, ok: bool) {
	profile.allocator = allocator
	profile.grpc_metadata = make([dynamic]RPC_Metadata_Entry, 0, len(wire.grpc_meta), allocator)
	profile.address, ok = config_string_clone(wire.address, allocator)
	if !ok {
		config_profile_free(&profile)
		return
	}
	profile.namespace, ok = config_string_clone(wire.namespace, allocator)
	if !ok {
		config_profile_free(&profile)
		return
	}
	profile.api_key, ok = config_string_clone(wire.api_key, allocator)
	if !ok {
		config_profile_free(&profile)
		return
	}
	#partial switch tls in wire.tls {
	case Config_Wire_TLS:
		profile.tls.present = true
		profile.tls.disabled = tls.disabled
		profile.tls.server_name, ok = config_string_clone(tls.server_name, allocator)
		if !ok {
			config_profile_free(&profile)
			return
		}
		profile.tls.server_ca_cert, ok =
			config_data_source_clone(tls.server_ca_cert, allocator)
		if !ok {
			config_profile_free(&profile)
			return
		}
		profile.tls.client_cert, ok = config_data_source_clone(tls.client_cert, allocator)
		if !ok {
			config_profile_free(&profile)
			return
		}
		profile.tls.client_key, ok = config_data_source_clone(tls.client_key, allocator)
		if !ok {
			config_profile_free(&profile)
			return
		}
	}
	#partial switch codec in wire.codec {
	case Config_Wire_Codec:
		profile.codec.present = true
		profile.codec.endpoint, ok = config_string_clone(codec.endpoint, allocator)
		if !ok {
			config_profile_free(&profile)
			return
		}
		profile.codec.auth, ok = config_string_clone(codec.auth, allocator)
		if !ok {
			config_profile_free(&profile)
			return
		}
	}
	for key, value in wire.grpc_meta {
		owned_key, key_ok := config_string_clone(key, allocator)
		if !key_ok {
			config_profile_free(&profile)
			return {}, false
		}
		owned_value, value_ok := bytes.clone_safe(transmute([]u8)value, allocator)
		if value_ok != nil {
			delete(owned_key, allocator)
			config_profile_free(&profile)
			return {}, false
		}
		append(
			&profile.grpc_metadata,
			RPC_Metadata_Entry{key = owned_key, value = owned_value},
		)
	}
	return profile, true
}

config_profile_decode :: proc(
	data: []u8,
	allocator: mem.Allocator,
) -> (Config_Profile, Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Configuration, "could not initialize configuration decode arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	wire := Config_Wire_Profile{}
	decode_error := json.unmarshal(
		data,
		&wire,
		allocator = virtual.arena_allocator(&decode_arena),
	)
	if decode_error != nil {
		return {}, error_from_string(.Configuration, "Core returned malformed client profile JSON")
	}
	profile, cloned := config_profile_clone_wire(wire, allocator)
	if !cloned {
		return {}, error_from_string(.Configuration, "could not allocate Temporal client profile")
	}
	return profile, {}
}

load_config_profile :: proc(
	options: Config_Profile_Load_Options = {},
	allocator := context.allocator,
) -> (Config_Profile, Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Configuration, "Temporal client profile requires an allocator")
	}
	if options.disable_environment && options.use_environment_override {
		return {}, error_from_string(
			.Configuration,
			"Temporal configuration cannot disable and override the environment",
		)
	}
	env_json, env_error := config_environment_json(
		options.use_environment_override,
		options.environment,
		allocator,
	)
	defer delete(env_json, allocator)
	if env_error.message != nil {
		return {}, env_error
	}
	data, core_error := temporal_core.client_env_config_profile_load_json({
		profile = options.profile,
		source = options.source,
		disable_file = options.disable_file,
		disable_env = options.disable_environment,
		config_file_strict = options.config_file_strict,
		env_vars_json = string(env_json),
	})
	defer delete(data)
	if core_error.message != nil {
		return {}, error_from_core(&core_error, .Configuration)
	}
	return config_profile_decode(data, allocator)
}

config_free :: proc(config: ^Config) {
	if config == nil {
		return
	}
	allocator := config.allocator
	if allocator.procedure != nil {
		for &named in config.profiles {
			delete(named.name, allocator)
			config_profile_free(&named.profile)
		}
		delete(config.profiles)
	}
	config^ = {}
}

load_config :: proc(
	options: Config_Load_Options = {},
	allocator := context.allocator,
) -> (config: Config, error: Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Configuration, "Temporal client configuration requires an allocator")
	}
	env_json, env_error := config_environment_json(
		options.use_environment_override,
		options.environment,
		allocator,
	)
	defer delete(env_json, allocator)
	if env_error.message != nil {
		return {}, env_error
	}
	data, core_error := temporal_core.client_env_config_load_json({
		source = options.source,
		config_file_strict = options.config_file_strict,
		env_vars_json = string(env_json),
	})
	defer delete(data)
	if core_error.message != nil {
		return {}, error_from_core(&core_error, .Configuration)
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Configuration, "could not initialize configuration decode arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	wire := Config_Wire{}
	decode_error := json.unmarshal(
		data,
		&wire,
		allocator = virtual.arena_allocator(&decode_arena),
	)
	if decode_error != nil {
		return {}, error_from_string(.Configuration, "Core returned malformed client configuration JSON")
	}
	config.allocator = allocator
	config.profiles = make([dynamic]Named_Config_Profile, 0, len(wire.profiles), allocator)
	for name, wire_profile in wire.profiles {
		owned_name, name_ok := config_string_clone(name, allocator)
		if !name_ok {
			config_free(&config)
			return {}, error_from_string(.Configuration, "could not allocate Temporal profile name")
		}
		profile, profile_ok := config_profile_clone_wire(wire_profile, allocator)
		if !profile_ok {
			delete(owned_name, allocator)
			config_free(&config)
			return {}, error_from_string(.Configuration, "could not allocate Temporal client profile")
		}
		append(&config.profiles, Named_Config_Profile{name = owned_name, profile = profile})
	}
	return config, {}
}

config_find_profile :: proc(config: ^Config, name: string) -> (^Config_Profile, bool) {
	if config == nil {
		return nil, false
	}
	for &named in config.profiles {
		if named.name == name {
			return &named.profile, true
		}
	}
	return nil, false
}

config_data_source_resolve :: proc(
	source: Config_Data_Source,
	name: string,
	allocator: mem.Allocator,
) -> ([]u8, Error) {
	switch source.kind {
	case .None:
		return nil, {}
	case .Data:
		owned, allocation_error := bytes.clone_safe(source.data, allocator)
		if allocation_error != nil {
			return nil, error_from_string(.Configuration, fmt.tprintf("could not allocate %s", name))
		}
		return owned, {}
	case .Path:
		data, read_error := os.read_entire_file(source.path, allocator)
		if read_error != nil {
			return nil, error_from_string(.Configuration, fmt.tprintf("could not read %s", name))
		}
		return data, {}
	}
	return nil, error_from_string(.Configuration, fmt.tprintf("%s source kind is invalid", name))
}

// connect_with_config_profile applies address, namespace, API key, TLS, and
// ASCII gRPC metadata from an owned profile. The remaining Connect_Options
// fields (identity, runtime, retry, keepalive, proxy, warning limits, and
// interceptors) remain caller-controlled.
connect_with_config_profile :: proc(
	profile: ^Config_Profile,
	options: Connect_Options = {},
) -> (Client, Error) {
	if profile == nil {
		return {}, error_from_string(.Configuration, "Temporal client profile must not be nil")
	}
	heap := allocator_runtime.heap_allocator()
	tls_explicitly_disabled := profile.tls.present && profile.tls.disabled
	tls_enabled := !tls_explicitly_disabled && (profile.tls.present || len(profile.api_key) > 0)
	if tls_enabled {
		has_client_cert := profile.tls.client_cert.kind != .None
		has_client_key := profile.tls.client_key.kind != .None
		if has_client_cert != has_client_key {
			return {}, error_from_string(
				.Configuration,
				"Temporal mTLS configuration requires both a client certificate and private key",
			)
		}
	}
	ca_cert: []u8
	client_cert: []u8
	client_key: []u8
	defer delete(ca_cert, heap)
	defer delete(client_cert, heap)
	defer delete(client_key, heap)
	if tls_enabled {
		ca_error: Error
		ca_cert, ca_error = config_data_source_resolve(
			profile.tls.server_ca_cert,
			"Temporal server CA certificate",
			heap,
		)
		if ca_error.message != nil do return {}, ca_error
		cert_error: Error
		client_cert, cert_error = config_data_source_resolve(
			profile.tls.client_cert,
			"Temporal client certificate",
			heap,
		)
		if cert_error.message != nil do return {}, cert_error
		key_error: Error
		client_key, key_error = config_data_source_resolve(
			profile.tls.client_key,
			"Temporal client private key",
			heap,
		)
		if key_error.message != nil do return {}, key_error
	}

	tls_options := TLS_Options{
		server_root_ca_cert = ca_cert,
		domain = profile.tls.server_name,
		client_cert = client_cert,
		client_private_key = client_key,
	}
	target_url := profile.address
	if len(target_url) == 0 {
		target_url = "http://localhost:7233"
	}
	owned_target_url: string
	defer delete(owned_target_url, heap)
	if !strings.contains(target_url, "://") {
		scheme := "http://"
		if tls_enabled {
			scheme = "https://"
		}
		allocation_error: allocator_runtime.Allocator_Error
		owned_target_url, allocation_error = strings.concatenate({scheme, target_url}, heap)
		if allocation_error != nil {
			return {}, error_from_string(.Configuration, "could not allocate Temporal profile address")
		}
		target_url = owned_target_url
	}
	resolved := options
	resolved.target_url = target_url
	resolved.namespace = profile.namespace
	if len(resolved.namespace) == 0 {
		resolved.namespace = "default"
	}
	resolved.api_key = profile.api_key
	resolved.metadata = profile.grpc_metadata[:]
	resolved.tls_disabled = tls_explicitly_disabled
	if tls_enabled {
		resolved.tls = &tls_options
	} else {
		resolved.tls = nil
	}
	return connect_with_options(resolved)
}

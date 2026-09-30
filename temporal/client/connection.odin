package client

import temporal_core "../core"

RPC_Metadata_Entry :: temporal_core.Client_Metadata_Entry
TLS_Options :: temporal_core.Client_TLS_Options
Client_Retry_Options :: temporal_core.Client_Retry_Options
Keep_Alive_Options :: temporal_core.Client_Keep_Alive_Options
HTTP_Connect_Proxy_Options :: temporal_core.Client_HTTP_Connect_Proxy_Options
DNS_Load_Balancing_Options :: temporal_core.Client_DNS_Load_Balancing_Options
Grpc_Compression :: temporal_core.Client_Grpc_Compression

Connect_Options :: struct {
	target_url:             string,
	namespace:              string,
	identity:               string,
	// Nil uses core.runtime_options_default(). The value is borrowed only while
	// connect constructs the Client-owned Core runtime.
	runtime_options:         ^temporal_core.Runtime_Options,
	api_key:                string,
	metadata:               []RPC_Metadata_Entry,
	binary_metadata:        []RPC_Metadata_Entry,
	tls:                    ^TLS_Options,
	retry:                  ^Client_Retry_Options,
	keep_alive:             ^Keep_Alive_Options,
	http_connect_proxy:     ^HTTP_Connect_Proxy_Options,
	dns_load_balancing:     ^DNS_Load_Balancing_Options,
	grpc_compression:       Grpc_Compression,
	payloads_warn_size:     u64,
	memo_warn_size:         u64,
	interceptors:           []Client_Interceptor,
}

update_rpc_metadata :: proc(
	client: ^Client,
	metadata: []RPC_Metadata_Entry,
	binary_metadata: []RPC_Metadata_Entry = nil,
) -> Error {
	input := Client_Connection_Mutation_Input{
		client = client,
		kind = .RPC_Metadata,
		metadata = metadata,
		binary_metadata = binary_metadata,
	}
	return client_connection_mutation_chain(&Client_Interceptor_State{client = client}, &input)
}

update_api_key :: proc(client: ^Client, api_key: string) -> Error {
	input := Client_Connection_Mutation_Input{client = client, kind = .API_Key, api_key = api_key}
	return client_connection_mutation_chain(&Client_Interceptor_State{client = client}, &input)
}

connection_mutation_terminal :: proc(input: ^Client_Connection_Mutation_Input) -> Error {
	if input == nil || input.client == nil || input.client.connection == nil {
		return error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	core_error: temporal_core.Error
	switch input.kind {
	case .RPC_Metadata:
		core_error = temporal_core.client_update_metadata(
			input.client.connection,
			input.metadata,
			input.binary_metadata,
		)
	case .API_Key:
		core_error = temporal_core.client_update_api_key(input.client.connection, input.api_key)
	}
	if core_error.message != nil {
		return error_from_core(&core_error, .Invalid_Argument)
	}
	return {}
}

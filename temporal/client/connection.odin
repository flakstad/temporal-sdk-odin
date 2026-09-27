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
}

update_rpc_metadata :: proc(
	client: ^Client,
	metadata: []RPC_Metadata_Entry,
	binary_metadata: []RPC_Metadata_Entry = nil,
) -> Error {
	if client == nil || client.connection == nil {
		return error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	core_error := temporal_core.client_update_metadata(client.connection, metadata, binary_metadata)
	if core_error.message != nil {
		return error_from_core(&core_error, .Invalid_Argument)
	}
	return {}
}

update_api_key :: proc(client: ^Client, api_key: string) -> Error {
	if client == nil || client.connection == nil {
		return error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	core_error := temporal_core.client_update_api_key(client.connection, api_key)
	if core_error.message != nil {
		return error_from_core(&core_error, .Invalid_Argument)
	}
	return {}
}

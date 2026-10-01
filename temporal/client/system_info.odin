package client

import "core:mem"
import "core:mem/virtual"
import temporal_core "../core"
import proto "../core/proto"

// System_Capabilities is the capability set advertised by GetSystemInfo in the
// pinned Temporal API. A false value can mean either unsupported or disabled
// by server configuration. Newer experimental RPCs do not necessarily have a
// corresponding bit and still require an operation-specific capability probe.
System_Capabilities :: struct {
	signal_and_query_header:              bool,
	internal_error_differentiation:       bool,
	activity_failure_include_heartbeat:   bool,
	supports_schedules:                   bool,
	encoded_failure_attributes:           bool,
	build_id_based_versioning:            bool,
	upsert_memo:                          bool,
	eager_workflow_start:                 bool,
	sdk_metadata:                         bool,
	count_group_by_execution_status:      bool,
	nexus:                                bool,
	server_scaled_deployments:            bool,
	server_scaled_provider_cloud_run:     bool,
}

System_Info :: struct {
	server_version: string,
	capabilities:   System_Capabilities,
	allocator:      mem.Allocator,
}

system_info_free :: proc(info: ^System_Info) {
	if info == nil {
		return
	}
	if info.allocator.procedure != nil {
		delete(info.server_version, info.allocator)
	}
	info^ = {}
}

decode_system_info :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (info: System_Info, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize System Info response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_GetSystemInfoResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_GetSystemInfoResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned malformed System Info")
	}

	server_version, cloned := description_clone_string(response.server_version, allocator)
	if !cloned {
		return {}, error_from_string(.Payload_Conversion, "could not copy Temporal Server version")
	}
	info = {
		server_version = server_version,
		allocator = allocator,
	}
	if response.capabilities != nil {
		capabilities := response.capabilities
		info.capabilities = {
			signal_and_query_header = capabilities.signal_and_query_header,
			internal_error_differentiation = capabilities.internal_error_differentiation,
			activity_failure_include_heartbeat = capabilities.activity_failure_include_heartbeat,
			supports_schedules = capabilities.supports_schedules,
			encoded_failure_attributes = capabilities.encoded_failure_attributes,
			build_id_based_versioning = capabilities.build_id_based_versioning,
			upsert_memo = capabilities.upsert_memo,
			eager_workflow_start = capabilities.eager_workflow_start,
			sdk_metadata = capabilities.sdk_metadata,
			count_group_by_execution_status = capabilities.count_group_by_execution_status,
			nexus = capabilities.nexus,
			server_scaled_deployments = capabilities.server_scaled_deployments,
			server_scaled_provider_cloud_run = capabilities.server_scaled_provider_cloud_run,
		}
	}
	return info, {}
}

get_system_info_terminal :: proc(
	input: ^Client_Get_System_Info_Input,
) -> (System_Info, Error) {
	if input == nil || input.client == nil || input.client.connection == nil || input.client.runtime == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_GetSystemInfoRequest{}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_GetSystemInfoRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		input.client.connection,
		.Workflow,
		"GetSystemInfo",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_system_info(rpc_result.protobuf, input.allocator)
}

get_system_info :: proc(
	client: ^Client,
	allocator := context.allocator,
) -> (System_Info, Error) {
	input := Client_Get_System_Info_Input{client = client, allocator = allocator}
	state := Client_Interceptor_State{client = client}
	return client_get_system_info_chain(&state, &input)
}

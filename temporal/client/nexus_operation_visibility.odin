package client

import "core:bytes"
import "core:mem"
import "core:mem/virtual"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"

List_Nexus_Operations_Options :: struct {
	page_size:       i32,
	next_page_token: []u8,
	query:           string,
}

Nexus_Operation_Execution_Summary :: struct {
	operation_id:           string,
	run_id:                 string,
	endpoint:               string,
	service:                string,
	operation:              string,
	status:                 Nexus_Operation_Status,
	schedule_time:          Optional_Time,
	close_time:             Optional_Time,
	execution_duration:     Optional_Duration,
	state_transition_count: i64,
	state_size_bytes:       i64,
	search_attributes:      [dynamic]Named_Payload,
}

Nexus_Operation_List_Page :: struct {
	operations:      [dynamic]Nexus_Operation_Execution_Summary,
	next_page_token: []u8,
	allocator:       mem.Allocator,
}

Nexus_Operation_Count_Group :: struct {
	values: [dynamic]codec.Payload,
	count:  i64,
}

Nexus_Operation_Count :: struct {
	count:     i64,
	groups:    [dynamic]Nexus_Operation_Count_Group,
	allocator: mem.Allocator,
}

nexus_operation_execution_summary_free :: proc(
	summary: ^Nexus_Operation_Execution_Summary,
	allocator: mem.Allocator,
) {
	if summary == nil do return
	delete(summary.operation_id, allocator)
	delete(summary.run_id, allocator)
	delete(summary.endpoint, allocator)
	delete(summary.service, allocator)
	delete(summary.operation, allocator)
	for &attribute in summary.search_attributes {
		delete(attribute.key, allocator)
		codec.payload_free(&attribute.payload)
	}
	delete(summary.search_attributes)
	summary^ = {}
}

nexus_operation_list_page_free :: proc(page: ^Nexus_Operation_List_Page) {
	if page == nil do return
	if page.allocator.procedure != nil {
		for &summary in page.operations {
			nexus_operation_execution_summary_free(&summary, page.allocator)
		}
		delete(page.next_page_token, page.allocator)
	}
	delete(page.operations)
	page^ = {}
}

nexus_operation_count_free :: proc(result: ^Nexus_Operation_Count) {
	if result == nil do return
	for &group in result.groups {
		for &value in group.values {
			codec.payload_free(&value)
		}
		delete(group.values)
	}
	delete(result.groups)
	result^ = {}
}

nexus_operation_visibility_clone_summary :: proc(
	source: ^proto.Temporal_Api_Nexus_V1_NexusOperationExecutionListInfo,
	allocator: mem.Allocator,
) -> (summary: Nexus_Operation_Execution_Summary, ok: bool) {
	if source == nil || len(source.operation_id) == 0 || len(source.run_id) == 0 {
		return {}, false
	}
	summary.search_attributes = make([dynamic]Named_Payload, allocator)
	failed := false
	defer if failed { nexus_operation_execution_summary_free(&summary, allocator) }
	clone_targets := []struct {source: string, target: ^string}{
		{source.operation_id, &summary.operation_id},
		{source.run_id, &summary.run_id},
		{source.endpoint, &summary.endpoint},
		{source.service, &summary.service},
		{source.operation, &summary.operation},
	}
	for target in clone_targets {
		target.target^, ok = description_clone_string(target.source, allocator)
		if !ok {
			failed = true
			return {}, false
		}
	}
	summary.status, ok = nexus_operation_status_from_proto(source.status)
	if !ok {
		failed = true
		return {}, false
	}
	summary.schedule_time = description_time(source.schedule_time)
	summary.close_time = description_time(source.close_time)
	summary.execution_duration = description_duration(source.execution_duration)
	summary.state_transition_count = source.state_transition_count
	summary.state_size_bytes = source.state_size_bytes
	if source.search_attributes != nil {
		for entry in source.search_attributes.indexed_fields {
			owned, payload_ok := description_clone_named_payload(entry.key, entry.value, allocator)
			if !payload_ok {
				failed = true
				return {}, false
			}
			append(&summary.search_attributes, owned)
		}
	}
	return summary, true
}

decode_nexus_operation_list_page :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (page: Nexus_Operation_List_Page, error: Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation list allocator is not initialized")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Nexus operation list arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_ListNexusOperationExecutionsResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_ListNexusOperationExecutionsResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Nexus operation list")
	}

	page.allocator = allocator
	page.operations = make([dynamic]Nexus_Operation_Execution_Summary, allocator)
	page.next_page_token = bytes.clone(response.next_page_token, allocator)
	failed := false
	defer if failed { nexus_operation_list_page_free(&page) }
	for &source in response.operations {
		summary, cloned := nexus_operation_visibility_clone_summary(&source, allocator)
		if !cloned {
			failed = true
			return {}, error_from_string(.Payload_Conversion, "could not copy Nexus operation list entry")
		}
		append(&page.operations, summary)
	}
	return page, {}
}

decode_nexus_operation_count :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (result: Nexus_Operation_Count, error: Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation count allocator is not initialized")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Nexus operation count arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_CountNexusOperationExecutionsResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_CountNexusOperationExecutionsResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Nexus operation count")
	}

	result.count = response.count
	result.groups = make([dynamic]Nexus_Operation_Count_Group, allocator)
	result.allocator = allocator
	failed := false
	defer if failed { nexus_operation_count_free(&result) }
	for &source in response.groups {
		group := Nexus_Operation_Count_Group{count = source.count}
		group.values = make([dynamic]codec.Payload, allocator)
		for &value in source.group_values {
			owned, cloned := description_clone_payload(&value, allocator)
			if !cloned {
				for &payload in group.values {
					codec.payload_free(&payload)
				}
				delete(group.values)
				failed = true
				return {}, error_from_string(.Payload_Conversion, "could not copy Nexus operation count group")
			}
			append(&group.values, owned)
		}
		append(&result.groups, group)
	}
	return result, {}
}

list_nexus_operations_terminal :: proc(
	input: ^Client_List_Nexus_Operations_Input,
) -> (Nexus_Operation_List_Page, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation list interceptor input is missing")
	}
	client := input.client
	options := input.options
	if client == nil || client.connection == nil || client.runtime == nil || len(client.namespace) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if options.page_size < 0 {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation list page size must not be negative")
	}
	request := proto.Temporal_Api_Workflowservice_V1_ListNexusOperationExecutionsRequest{
		namespace = client.namespace,
		page_size = options.page_size,
		next_page_token = options.next_page_token,
		query = options.query,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_ListNexusOperationExecutionsRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"ListNexusOperationExecutions",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return {}, error_from_core(&rpc_result.error)
	return decode_nexus_operation_list_page(rpc_result.protobuf, input.allocator)
}

list_nexus_operations :: proc(
	client: ^Client,
	options: List_Nexus_Operations_Options = {},
	allocator := context.allocator,
) -> (Nexus_Operation_List_Page, Error) {
	input := Client_List_Nexus_Operations_Input{
		client = client,
		options = options,
		allocator = allocator,
	}
	state := Client_Interceptor_State{client = client}
	return client_list_nexus_operations_chain(&state, &input)
}

count_nexus_operations_terminal :: proc(
	input: ^Client_Count_Nexus_Operations_Input,
) -> (Nexus_Operation_Count, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "Nexus operation count interceptor input is missing")
	}
	client := input.client
	if client == nil || client.connection == nil || client.runtime == nil || len(client.namespace) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_CountNexusOperationExecutionsRequest{
		namespace = client.namespace,
		query = input.query,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_CountNexusOperationExecutionsRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"CountNexusOperationExecutions",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return {}, error_from_core(&rpc_result.error)
	return decode_nexus_operation_count(rpc_result.protobuf, input.allocator)
}

count_nexus_operations :: proc(
	client: ^Client,
	query := "",
	allocator := context.allocator,
) -> (Nexus_Operation_Count, Error) {
	input := Client_Count_Nexus_Operations_Input{
		client = client,
		query = query,
		allocator = allocator,
	}
	state := Client_Interceptor_State{client = client}
	return client_count_nexus_operations_chain(&state, &input)
}

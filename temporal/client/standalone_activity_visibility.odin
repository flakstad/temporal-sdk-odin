package client

import "core:bytes"
import "core:mem"
import "core:mem/virtual"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"

List_Standalone_Activities_Options :: struct {
	page_size:       i32,
	next_page_token: []u8,
	query:           string,
}

Standalone_Activity_Execution_Summary :: struct {
	activity_id:           string,
	run_id:                string,
	activity_type:         string,
	task_queue:            string,
	status:                Standalone_Activity_Status,
	schedule_time:         Optional_Time,
	execution_time:        Optional_Time,
	close_time:            Optional_Time,
	execution_duration:    Optional_Duration,
	state_transition_count: i64,
	state_size_bytes:      i64,
	search_attributes:     [dynamic]Named_Payload,
}

Standalone_Activity_List_Page :: struct {
	activities:      [dynamic]Standalone_Activity_Execution_Summary,
	next_page_token: []u8,
	allocator:       mem.Allocator,
}

Standalone_Activity_Count_Group :: struct {
	values: [dynamic]codec.Payload,
	count:  i64,
}

Standalone_Activity_Count :: struct {
	count:     i64,
	groups:    [dynamic]Standalone_Activity_Count_Group,
	allocator: mem.Allocator,
}

standalone_activity_execution_summary_free :: proc(
	summary: ^Standalone_Activity_Execution_Summary,
	allocator: mem.Allocator,
) {
	if summary == nil do return
	delete(summary.activity_id, allocator)
	delete(summary.run_id, allocator)
	delete(summary.activity_type, allocator)
	delete(summary.task_queue, allocator)
	for &attribute in summary.search_attributes {
		delete(attribute.key, allocator)
		codec.payload_free(&attribute.payload)
	}
	delete(summary.search_attributes)
	summary^ = {}
}

standalone_activity_list_page_free :: proc(page: ^Standalone_Activity_List_Page) {
	if page == nil do return
	if page.allocator.procedure != nil {
		for &summary in page.activities {
			standalone_activity_execution_summary_free(&summary, page.allocator)
		}
		delete(page.next_page_token, page.allocator)
	}
	delete(page.activities)
	page^ = {}
}

standalone_activity_count_free :: proc(result: ^Standalone_Activity_Count) {
	if result == nil do return
	for &group in result.groups {
		for &value in group.values do codec.payload_free(&value)
		delete(group.values)
	}
	delete(result.groups)
	result^ = {}
}

standalone_activity_visibility_clone_summary :: proc(
	source: ^proto.Temporal_Api_Activity_V1_ActivityExecutionListInfo,
	allocator: mem.Allocator,
) -> (summary: Standalone_Activity_Execution_Summary, ok: bool) {
	if source == nil || len(source.activity_id) == 0 || len(source.run_id) == 0 {
		return {}, false
	}
	summary.search_attributes = make([dynamic]Named_Payload, allocator)
	clone_targets := []struct {source: string, target: ^string}{
		{source.activity_id, &summary.activity_id},
		{source.run_id, &summary.run_id},
		{source.task_queue, &summary.task_queue},
	}
	for target in clone_targets {
		target.target^, ok = description_clone_string(target.source, allocator)
		if !ok {
			standalone_activity_execution_summary_free(&summary, allocator)
			return {}, false
		}
	}
	if source.activity_type != nil {
		summary.activity_type, ok = description_clone_string(source.activity_type.name, allocator)
		if !ok {
			standalone_activity_execution_summary_free(&summary, allocator)
			return {}, false
		}
	}
	summary.status, ok = standalone_activity_status_from_proto(source.status)
	if !ok {
		standalone_activity_execution_summary_free(&summary, allocator)
		return {}, false
	}
	summary.schedule_time = description_time(source.schedule_time)
	summary.execution_time = description_time(source.execution_time)
	summary.close_time = description_time(source.close_time)
	summary.execution_duration = description_duration(source.execution_duration)
	summary.state_transition_count = source.state_transition_count
	summary.state_size_bytes = source.state_size_bytes
	if source.search_attributes != nil {
		for entry in source.search_attributes.indexed_fields {
			owned, cloned := description_clone_named_payload(entry.key, entry.value, allocator)
			if !cloned {
				standalone_activity_execution_summary_free(&summary, allocator)
				return {}, false
			}
			append(&summary.search_attributes, owned)
		}
	}
	return summary, true
}

decode_standalone_activity_list_page :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (page: Standalone_Activity_List_Page, error: Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity list allocator is not initialized")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize standalone Activity list arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_ListActivityExecutionsResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_ListActivityExecutionsResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed standalone Activity list")
	}
	page.allocator = allocator
	page.activities = make([dynamic]Standalone_Activity_Execution_Summary, allocator)
	page.next_page_token = bytes.clone(response.next_page_token, allocator)
	for &source in response.executions {
		summary, cloned := standalone_activity_visibility_clone_summary(&source, allocator)
		if !cloned {
			standalone_activity_list_page_free(&page)
			return {}, error_from_string(
				.Payload_Conversion,
				"could not copy standalone Activity list entry",
			)
		}
		append(&page.activities, summary)
	}
	return page, {}
}

decode_standalone_activity_count :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (result: Standalone_Activity_Count, error: Error) {
	if allocator.procedure == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity count allocator is not initialized")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize standalone Activity count arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_CountActivityExecutionsResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_CountActivityExecutionsResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed standalone Activity count")
	}
	result.count = response.count
	result.groups = make([dynamic]Standalone_Activity_Count_Group, allocator)
	result.allocator = allocator
	for &source in response.groups {
		group := Standalone_Activity_Count_Group{count = source.count}
		group.values = make([dynamic]codec.Payload, allocator)
		for &value in source.group_values {
			owned, cloned := description_clone_payload(&value, allocator)
			if !cloned {
				for &payload in group.values do codec.payload_free(&payload)
				delete(group.values)
				standalone_activity_count_free(&result)
				return {}, error_from_string(
					.Payload_Conversion,
					"could not copy standalone Activity count group",
				)
			}
			append(&group.values, owned)
		}
		append(&result.groups, group)
	}
	return result, {}
}

list_standalone_activities_terminal :: proc(
	input: ^Client_List_Standalone_Activities_Input,
) -> (Standalone_Activity_List_Page, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity list interceptor input is missing")
	}
	client := input.client
	options := input.options
	if client == nil || client.connection == nil || client.connection.handle == nil ||
	   len(client.namespace) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if options.page_size < 0 {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity list page size must not be negative")
	}
	request := proto.Temporal_Api_Workflowservice_V1_ListActivityExecutionsRequest{
		namespace = client.namespace,
		page_size = options.page_size,
		next_page_token = options.next_page_token,
		query = options.query,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_ListActivityExecutionsRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"ListActivityExecutions",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return {}, error_from_core(&rpc_result.error)
	return decode_standalone_activity_list_page(rpc_result.protobuf, input.allocator)
}

list_standalone_activities :: proc(
	client: ^Client,
	options: List_Standalone_Activities_Options = {},
	allocator := context.allocator,
) -> (Standalone_Activity_List_Page, Error) {
	input := Client_List_Standalone_Activities_Input{
		client = client,
		options = options,
		allocator = allocator,
	}
	state := Client_Interceptor_State{client = client}
	return client_list_standalone_activities_chain(&state, &input)
}

count_standalone_activities_terminal :: proc(
	input: ^Client_Count_Standalone_Activities_Input,
) -> (Standalone_Activity_Count, Error) {
	if input == nil {
		return {}, error_from_string(.Invalid_Argument, "standalone Activity count interceptor input is missing")
	}
	client := input.client
	if client == nil || client.connection == nil || client.connection.handle == nil ||
	   len(client.namespace) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_CountActivityExecutionsRequest{
		namespace = client.namespace,
		query = input.query,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_CountActivityExecutionsRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"CountActivityExecutions",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return {}, error_from_core(&rpc_result.error)
	return decode_standalone_activity_count(rpc_result.protobuf, input.allocator)
}

count_standalone_activities :: proc(
	client: ^Client,
	query := "",
	allocator := context.allocator,
) -> (Standalone_Activity_Count, Error) {
	input := Client_Count_Standalone_Activities_Input{
		client = client,
		query = query,
		allocator = allocator,
	}
	state := Client_Interceptor_State{client = client}
	return client_count_standalone_activities_chain(&state, &input)
}

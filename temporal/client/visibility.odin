package client

import "core:bytes"
import "core:mem"
import "core:mem/virtual"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"

List_Workflows_Options :: struct {
	page_size:       i32,
	next_page_token: []u8,
	query:           string,
}

Workflow_Execution_Summary :: struct {
	workflow_id:                 string,
	run_id:                      string,
	first_run_id:                string,
	workflow_type:               string,
	task_queue:                  string,
	parent_namespace_id:         string,
	parent_workflow_id:          string,
	parent_run_id:               string,
	root_workflow_id:            string,
	root_run_id:                 string,
	worker_deployment_name:      string,
	status:                      Workflow_Status,
	start_time:                  Optional_Time,
	execution_time:              Optional_Time,
	close_time:                  Optional_Time,
	execution_duration:          Optional_Duration,
	history_length:              i64,
	history_size_bytes:          i64,
	state_transition_count:      i64,
	external_payload_size_bytes: i64,
	external_payload_count:      i64,
	priority:                    Workflow_Priority,
	memo:                        [dynamic]Named_Payload,
	search_attributes:           [dynamic]Named_Payload,
}

Workflow_List_Page :: struct {
	executions:      [dynamic]Workflow_Execution_Summary,
	next_page_token: []u8,
	allocator:       mem.Allocator,
}

Workflow_Count_Group :: struct {
	values: [dynamic]codec.Payload,
	count:  i64,
}

Workflow_Count :: struct {
	count:     i64,
	groups:    [dynamic]Workflow_Count_Group,
	allocator: mem.Allocator,
}

workflow_execution_summary_free :: proc(
	summary: ^Workflow_Execution_Summary,
	allocator: mem.Allocator,
) {
	if summary == nil {
		return
	}
	delete(summary.workflow_id, allocator)
	delete(summary.run_id, allocator)
	delete(summary.first_run_id, allocator)
	delete(summary.workflow_type, allocator)
	delete(summary.task_queue, allocator)
	delete(summary.parent_namespace_id, allocator)
	delete(summary.parent_workflow_id, allocator)
	delete(summary.parent_run_id, allocator)
	delete(summary.root_workflow_id, allocator)
	delete(summary.root_run_id, allocator)
	delete(summary.worker_deployment_name, allocator)
	delete(summary.priority.fairness_key, allocator)
	for &entry in summary.memo {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	for &entry in summary.search_attributes {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	delete(summary.memo)
	delete(summary.search_attributes)
	summary^ = {}
}

workflow_list_page_free :: proc(page: ^Workflow_List_Page) {
	if page == nil {
		return
	}
	if page.allocator.procedure != nil {
		for &summary in page.executions {
			workflow_execution_summary_free(&summary, page.allocator)
		}
		delete(page.next_page_token, page.allocator)
	}
	delete(page.executions)
	page^ = {}
}

workflow_count_free :: proc(result: ^Workflow_Count) {
	if result == nil {
		return
	}
	for &group in result.groups {
		for &value in group.values {
			codec.payload_free(&value)
		}
		delete(group.values)
	}
	delete(result.groups)
	result^ = {}
}

visibility_clone_summary :: proc(
	source: ^proto.Temporal_Api_Workflow_V1_WorkflowExecutionInfo,
	allocator: mem.Allocator,
) -> (summary: Workflow_Execution_Summary, ok: bool) {
	if source == nil || source.execution == nil || source.type == nil {
		return {}, false
	}
	summary.memo = make([dynamic]Named_Payload, allocator)
	summary.search_attributes = make([dynamic]Named_Payload, allocator)
	failed := false
	defer if failed { workflow_execution_summary_free(&summary, allocator) }

	clone_targets := []struct {
		source: string,
		target: ^string,
	}{
		{source.execution.workflow_id, &summary.workflow_id},
		{source.execution.run_id, &summary.run_id},
		{source.first_run_id, &summary.first_run_id},
		{source.type.name, &summary.workflow_type},
		{source.task_queue, &summary.task_queue},
		{source.parent_namespace_id, &summary.parent_namespace_id},
		{source.worker_deployment_name, &summary.worker_deployment_name},
	}
	for target in clone_targets {
		target.target^, ok = description_clone_string(target.source, allocator)
		if !ok {
			failed = true
			return {}, false
		}
	}
	if source.parent_execution != nil {
		summary.parent_workflow_id, ok = description_clone_string(source.parent_execution.workflow_id, allocator)
		if ok {
			summary.parent_run_id, ok = description_clone_string(source.parent_execution.run_id, allocator)
		}
		if !ok {
			failed = true
			return {}, false
		}
	}
	if source.root_execution != nil {
		summary.root_workflow_id, ok = description_clone_string(source.root_execution.workflow_id, allocator)
		if ok {
			summary.root_run_id, ok = description_clone_string(source.root_execution.run_id, allocator)
		}
		if !ok {
			failed = true
			return {}, false
		}
	}

	summary.status = Workflow_Status(source.status)
	summary.start_time = description_time(source.start_time)
	summary.execution_time = description_time(source.execution_time)
	summary.close_time = description_time(source.close_time)
	summary.execution_duration = description_duration(source.execution_duration)
	summary.history_length = source.history_length
	summary.history_size_bytes = source.history_size_bytes
	summary.state_transition_count = source.state_transition_count
	summary.external_payload_size_bytes = source.external_payload_size_bytes
	summary.external_payload_count = source.external_payload_count
	summary.priority, ok = description_priority(source.priority, allocator)
	if !ok {
		failed = true
		return {}, false
	}
	if source.memo != nil {
		for entry in source.memo.fields {
			owned, payload_ok := description_clone_named_payload(entry.key, entry.value, allocator)
			if !payload_ok {
				failed = true
				return {}, false
			}
			append(&summary.memo, owned)
		}
	}
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

decode_workflow_list_page :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (page: Workflow_List_Page, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Visibility response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_ListWorkflowExecutionsResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_ListWorkflowExecutionsResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Workflow list")
	}

	page.allocator = allocator
	page.executions = make([dynamic]Workflow_Execution_Summary, allocator)
	page.next_page_token = bytes.clone(response.next_page_token, allocator)
	failed := false
	defer if failed { workflow_list_page_free(&page) }
	for &source in response.executions {
		summary, cloned := visibility_clone_summary(&source, allocator)
		if !cloned {
			failed = true
			return {}, error_from_string(.Payload_Conversion, "could not copy Workflow list entry")
		}
		append(&page.executions, summary)
	}
	return page, {}
}

decode_workflow_count :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (result: Workflow_Count, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Count response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_CountWorkflowExecutionsResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_CountWorkflowExecutionsResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Workflow count")
	}

	result.count = response.count
	result.groups = make([dynamic]Workflow_Count_Group, allocator)
	result.allocator = allocator
	failed := false
	defer if failed { workflow_count_free(&result) }
	for &source in response.groups {
		group := Workflow_Count_Group{count = source.count}
		group.values = make([dynamic]codec.Payload, allocator)
		for &value in source.group_values {
			owned, cloned := description_clone_payload(&value, allocator)
			if !cloned {
				for &payload in group.values {
					codec.payload_free(&payload)
				}
				delete(group.values)
				failed = true
				return {}, error_from_string(.Payload_Conversion, "could not copy Workflow count group")
			}
			append(&group.values, owned)
		}
		append(&result.groups, group)
	}
	return result, {}
}

list_workflows :: proc(
	client: ^Client,
	options := List_Workflows_Options{},
) -> (Workflow_List_Page, Error) {
	if client == nil || client.connection == nil || client.runtime == nil || len(client.namespace) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if options.page_size < 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow list page size must not be negative")
	}
	request := proto.Temporal_Api_Workflowservice_V1_ListWorkflowExecutionsRequest{
		namespace = client.namespace,
		page_size = options.page_size,
		next_page_token = options.next_page_token,
		query = options.query,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_ListWorkflowExecutionsRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"ListWorkflowExecutions",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_workflow_list_page(rpc_result.protobuf)
}

count_workflows :: proc(client: ^Client, query := "") -> (Workflow_Count, Error) {
	if client == nil || client.connection == nil || client.runtime == nil || len(client.namespace) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_CountWorkflowExecutionsRequest{
		namespace = client.namespace,
		query = query,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_CountWorkflowExecutionsRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"CountWorkflowExecutions",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_workflow_count(rpc_result.protobuf)
}

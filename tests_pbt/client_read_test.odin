package workflow_pbt_test

import "core:bytes"
import "core:testing"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"
import proto "../temporal/core/proto"

client_history_page_property :: proc(t: ^pbt.T) -> pbt.Result {
	event_count := pbt.draw(t, pbt.int_range(1, 24))
	first_event_id := i64(pbt.draw(t, pbt.int_range(1, 100000)))
	version := i64(pbt.draw(t, pbt.int_range(0, 100)))
	next_token_string := pbt.draw(t, pbt.string_ascii(0, 48))
	archived := pbt.draw(t, pbt.boolean())
	pbt.cover(t, event_count == 1, 2.0, "single event page")
	pbt.cover(t, event_count >= 20, 10.0, "large event page")
	pbt.cover(t, len(next_token_string) == 0, 1.0, "terminal page")
	pbt.cover(t, archived, 30.0, "archived history")

	history := proto.Temporal_Api_History_V1_History{}
	defer delete(history.events)
	for index in 0 ..< event_count {
		event_type_number := i32((index % 60) + 1)
		append(&history.events, proto.Temporal_Api_History_V1_HistoryEvent{
			event_id = first_event_id + i64(index),
			event_type = proto.Temporal_Api_Enums_V1_EventType(event_type_number),
			version = version,
			task_id = i64(index * 10),
			worker_may_ignore = index % 2 == 0,
		})
	}
	response := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse{
		history = &history,
		next_page_token = transmute([]u8)next_token_string,
		archived = archived,
	}
	encoded := proto.encode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse(&response)
	page, page_error := temporal_client.decode_history_page(encoded[:])
	delete(encoded)
	defer temporal_client.history_page_free(&page)
	defer temporal_client.error_free(&page_error)
	if page_error.message != nil {
		return pbt.fail("generated history page did not decode")
	}
	if len(page.events) != event_count || page.archived != archived ||
	   !bytes.equal(page.next_page_token, transmute([]u8)next_token_string) {
		return pbt.fail("history page metadata changed during round trip")
	}
	for event, index in page.events {
		if event.event_id != first_event_id + i64(index) || event.version != version ||
		   len(event.protobuf) == 0 {
			return pbt.fail("history event changed during round trip")
		}
	}
	return pbt.pass()
}

@(test)
client_history_pages_preserve_generated_events_and_tokens :: proc(t: ^testing.T) {
	result := pbt.check(
		"Client history pages preserve events and pagination tokens",
		client_history_page_property,
		{num_tests = 500, seed = 0x4849_5354_4f52_5950},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

client_visibility_property :: proc(t: ^pbt.T) -> pbt.Result {
	execution_count := pbt.draw(t, pbt.int_range(0, 16))
	status_number := i32(pbt.draw(t, pbt.int_range(1, 8)))
	next_token_string := pbt.draw(t, pbt.string_ascii(0, 48))
	group_count := pbt.draw(t, pbt.int_range(0, 8))
	group_value := pbt.draw(t, pbt.string_ascii(0, 40))
	pbt.cover(t, execution_count == 0, 2.0, "empty list page")
	pbt.cover(t, execution_count >= 12, 15.0, "large list page")
	pbt.cover(t, len(next_token_string) == 0, 1.0, "terminal list page")
	pbt.cover(t, group_count > 0, 50.0, "grouped count")

	executions := make([]proto.Temporal_Api_Common_V1_WorkflowExecution, execution_count)
	types := make([]proto.Temporal_Api_Common_V1_WorkflowType, execution_count)
	defer delete(executions)
	defer delete(types)
	response := proto.Temporal_Api_Workflowservice_V1_ListWorkflowExecutionsResponse{
		next_page_token = transmute([]u8)next_token_string,
	}
	defer delete(response.executions)
	for index in 0 ..< execution_count {
		executions[index] = {workflow_id = "property-workflow", run_id = "property-run"}
		types[index] = {name = "property_type"}
		append(&response.executions, proto.Temporal_Api_Workflow_V1_WorkflowExecutionInfo{
			execution = &executions[index],
			type = &types[index],
			status = proto.Temporal_Api_Enums_V1_WorkflowExecutionStatus(status_number),
			history_length = i64(index + 1),
			task_queue = "property-queue",
		})
	}
	encoded := proto.encode_Temporal_Api_Workflowservice_V1_ListWorkflowExecutionsResponse(&response)
	page, page_error := temporal_client.decode_workflow_list_page(encoded[:])
	delete(encoded)
	defer temporal_client.workflow_list_page_free(&page)
	defer temporal_client.error_free(&page_error)
	if page_error.message != nil {
		return pbt.fail("generated Workflow list did not decode")
	}
	if len(page.executions) != execution_count ||
	   !bytes.equal(page.next_page_token, transmute([]u8)next_token_string) {
		return pbt.fail("Workflow list metadata changed during round trip")
	}
	for summary, index in page.executions {
		if summary.workflow_id != "property-workflow" ||
		   summary.status != temporal_client.Workflow_Status(status_number) ||
		   summary.history_length != i64(index + 1) {
			return pbt.fail("Workflow summary changed during round trip")
		}
	}

	count_response := proto.Temporal_Api_Workflowservice_V1_CountWorkflowExecutionsResponse{
		count = i64(group_count * 3),
	}
	defer delete(count_response.groups)
	for index in 0 ..< group_count {
		group := proto.Temporal_Api_Workflowservice_V1_CountWorkflowExecutionsResponse_AggregationGroup{
			count = i64(index + 1),
		}
		payload := proto.Temporal_Api_Common_V1_Payload{data = transmute([]u8)group_value}
		append(&group.group_values, payload)
		append(&count_response.groups, group)
	}
	defer for &group in count_response.groups { delete(group.group_values) }
	count_encoded := proto.encode_Temporal_Api_Workflowservice_V1_CountWorkflowExecutionsResponse(&count_response)
	count, count_error := temporal_client.decode_workflow_count(count_encoded[:])
	delete(count_encoded)
	defer temporal_client.workflow_count_free(&count)
	defer temporal_client.error_free(&count_error)
	if count_error.message != nil || len(count.groups) != group_count || count.count != i64(group_count * 3) {
		return pbt.fail("generated Workflow count did not round trip")
	}
	for group, index in count.groups {
		if group.count != i64(index + 1) || len(group.values) != 1 ||
		   !bytes.equal(group.values[0].data, transmute([]u8)group_value) {
			return pbt.fail("Workflow count group changed during round trip")
		}
	}
	return pbt.pass()
}

@(test)
client_visibility_preserves_generated_pages_and_groups :: proc(t: ^testing.T) {
	result := pbt.check(
		"Client Visibility preserves generated pages and grouped counts",
		client_visibility_property,
		{num_tests = 500, seed = 0x5649_5349_4249_4c49},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

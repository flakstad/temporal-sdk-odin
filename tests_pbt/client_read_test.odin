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

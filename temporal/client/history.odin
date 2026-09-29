package client

import "core:bytes"
import "core:mem"
import "core:mem/virtual"
import temporal_core "../core"
import proto "../core/proto"

History_Event_Filter :: enum i32 {
	All = 1,
	Close = 2,
}

History_Event_Type :: enum i32 {
	Unspecified = 0,
	Workflow_Execution_Started = 1,
	Workflow_Execution_Completed = 2,
	Workflow_Execution_Failed = 3,
	Workflow_Execution_Timed_Out = 4,
	Workflow_Task_Scheduled = 5,
	Workflow_Task_Started = 6,
	Workflow_Task_Completed = 7,
	Workflow_Task_Timed_Out = 8,
	Workflow_Task_Failed = 9,
	Activity_Task_Scheduled = 10,
	Activity_Task_Started = 11,
	Activity_Task_Completed = 12,
	Activity_Task_Failed = 13,
	Activity_Task_Timed_Out = 14,
	Activity_Task_Cancel_Requested = 15,
	Activity_Task_Canceled = 16,
	Timer_Started = 17,
	Timer_Fired = 18,
	Timer_Canceled = 19,
	Workflow_Execution_Cancel_Requested = 20,
	Workflow_Execution_Canceled = 21,
	Request_Cancel_External_Workflow_Execution_Initiated = 22,
	Request_Cancel_External_Workflow_Execution_Failed = 23,
	External_Workflow_Execution_Cancel_Requested = 24,
	Marker_Recorded = 25,
	Workflow_Execution_Signaled = 26,
	Workflow_Execution_Terminated = 27,
	Workflow_Execution_Continued_As_New = 28,
	Start_Child_Workflow_Execution_Initiated = 29,
	Start_Child_Workflow_Execution_Failed = 30,
	Child_Workflow_Execution_Started = 31,
	Child_Workflow_Execution_Completed = 32,
	Child_Workflow_Execution_Failed = 33,
	Child_Workflow_Execution_Canceled = 34,
	Child_Workflow_Execution_Timed_Out = 35,
	Child_Workflow_Execution_Terminated = 36,
	Signal_External_Workflow_Execution_Initiated = 37,
	Signal_External_Workflow_Execution_Failed = 38,
	External_Workflow_Execution_Signaled = 39,
	Upsert_Workflow_Search_Attributes = 40,
	Workflow_Execution_Update_Accepted = 41,
	Workflow_Execution_Update_Rejected = 42,
	Workflow_Execution_Update_Completed = 43,
	Workflow_Properties_Modified_Externally = 44,
	Activity_Properties_Modified_Externally = 45,
	Workflow_Properties_Modified = 46,
	Workflow_Execution_Update_Admitted = 47,
	Nexus_Operation_Scheduled = 48,
	Nexus_Operation_Started = 49,
	Nexus_Operation_Completed = 50,
	Nexus_Operation_Failed = 51,
	Nexus_Operation_Canceled = 52,
	Nexus_Operation_Timed_Out = 53,
	Nexus_Operation_Cancel_Requested = 54,
	Workflow_Execution_Options_Updated = 55,
	Nexus_Operation_Cancel_Request_Completed = 56,
	Nexus_Operation_Cancel_Request_Failed = 57,
	Workflow_Execution_Paused = 58,
	Workflow_Execution_Unpaused = 59,
	Workflow_Execution_Time_Skipping_Transitioned = 60,
}

History_Page_Options :: struct {
	maximum_page_size: i32,
	next_page_token:   []u8,
	wait_new_event:    bool,
	filter:            History_Event_Filter,
	skip_archival:     bool,
}

History_Event :: struct {
	event_id:          i64,
	event_time:        Optional_Time,
	event_type:        History_Event_Type,
	version:           i64,
	task_id:           i64,
	worker_may_ignore: bool,
	protobuf:          [dynamic]u8,
}

History_Page :: struct {
	events:          [dynamic]History_Event,
	next_page_token: []u8,
	archived:        bool,
	allocator:       mem.Allocator,
}

history_page_free :: proc(page: ^History_Page) {
	if page == nil {
		return
	}
	for &event in page.events {
		delete(event.protobuf)
	}
	delete(page.events)
	if page.allocator.procedure != nil {
		delete(page.next_page_token, page.allocator)
	}
	page^ = {}
}

decode_history_page :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (page: History_Page, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize history response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned malformed Workflow history")
	}
	if len(response.raw_history) > 0 && response.history == nil {
		return {}, error_from_string(.Malformed_Response, "raw history blobs are not supported by the typed history API")
	}

	page.allocator = allocator
	page.events = make([dynamic]History_Event, allocator)
	page.archived = response.archived
	page.next_page_token = bytes.clone(response.next_page_token, allocator)
	failed := false
	defer if failed { history_page_free(&page) }
	if response.history == nil {
		return page, {}
	}
	for &source in response.history.events {
		previous := context.allocator
		context.allocator = allocator
		protobuf := proto.encode_Temporal_Api_History_V1_HistoryEvent(&source)
		context.allocator = previous
		if protobuf == nil {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not encode owned history event")
		}
		append(&page.events, History_Event{
			event_id = source.event_id,
			event_time = description_time(source.event_time),
			event_type = History_Event_Type(source.event_type),
			version = source.version,
			task_id = source.task_id,
			worker_may_ignore = source.worker_may_ignore,
			protobuf = protobuf,
		})
	}
	return page, {}
}

fetch_history_page :: proc(
	handle: ^Workflow_Handle,
	options := History_Page_Options{filter = .All, skip_archival = true},
) -> (History_Page, Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if options.maximum_page_size < 0 {
		return {}, error_from_string(.Invalid_Argument, "history page size must not be negative")
	}
	execution := workflow_execution(handle)
	filter := proto.Temporal_Api_Enums_V1_HistoryEventFilterType(options.filter)
	if filter == .HISTORY_EVENT_FILTER_TYPE_UNSPECIFIED {
		filter = .HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
	}
	request := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryRequest{
		namespace = handle.client.namespace,
		execution = &execution,
		maximum_page_size = options.maximum_page_size,
		next_page_token = options.next_page_token,
		wait_new_event = options.wait_new_event,
		history_event_filter_type = filter,
		skip_archival = options.skip_archival,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryRequest(&request)
	defer delete(request_bytes)
	timeout_millis: u32 = 30_000
	if options.wait_new_event {
		timeout_millis = 0
	}
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"GetWorkflowExecutionHistory",
		request_bytes[:],
		timeout_millis = timeout_millis,
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_history_page(rpc_result.protobuf)
}

// fetch_history_protobuf eagerly follows every history page and returns the
// binary temporal.api.history.v1.History message expected by SDK Core's replay
// worker. The caller owns the returned bytes. This intentionally does not use
// Temporal CLI JSON as an intermediate representation.
fetch_history_protobuf :: proc(
	handle: ^Workflow_Handle,
	maximum_page_size: i32 = 1_000,
) -> (encoded: []u8, error: Error) {
	if !workflow_handle_valid(handle) {
		return nil, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if maximum_page_size < 0 {
		return nil, error_from_string(.Invalid_Argument, "history page size must not be negative")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return nil, error_from_string(.Malformed_Response, "could not initialize history replay arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	arena_allocator := virtual.arena_allocator(&decode_arena)
	history := proto.Temporal_Api_History_V1_History{}
	previous_allocator := context.allocator
	context.allocator = arena_allocator
	history.events = make([dynamic]proto.Temporal_Api_History_V1_HistoryEvent)
	context.allocator = previous_allocator
	event_protobufs := make([dynamic][]u8)
	defer {
		for event_protobuf in event_protobufs {
			delete(event_protobuf)
		}
		delete(event_protobufs)
	}

	next_page_token: []u8
	defer delete(next_page_token)
	page_count := 0
	for {
		page, page_error := fetch_history_page(handle, {
			maximum_page_size = maximum_page_size,
			next_page_token = next_page_token,
			filter = .All,
			skip_archival = true,
		})
		if page_error.message != nil {
			return nil, page_error
		}
		page_count += 1
		for event in page.events {
			// Generated protobuf decoders borrow byte/string fields from their
			// input. Keep every encoded event alive until the aggregate History
			// has been encoded below.
			append(&event_protobufs, bytes.clone(event.protobuf[:]))
		}
		delete(next_page_token)
		next_page_token = bytes.clone(page.next_page_token)
		history_page_free(&page)
		if len(next_page_token) == 0 {
			break
		}
		if page_count >= 10_000 {
			return nil, error_from_string(
				.Malformed_Response,
				"Workflow history pagination did not terminate",
			)
		}
	}
	if len(event_protobufs) == 0 {
		return nil, error_from_string(.Malformed_Response, "Workflow history is empty")
	}
	for event_protobuf in event_protobufs {
		decoded_event := proto.Temporal_Api_History_V1_HistoryEvent{}
		context.allocator = arena_allocator
		decoded := proto.decode_Temporal_Api_History_V1_HistoryEvent(
			event_protobuf,
			&decoded_event,
		)
		if decoded {
			append(&history.events, decoded_event)
		}
		context.allocator = previous_allocator
		if !decoded {
			return nil, error_from_string(
				.Malformed_Response,
				"could not decode an owned Workflow history event",
			)
		}
	}
	encoded_dynamic := proto.encode_Temporal_Api_History_V1_History(&history)
	return encoded_dynamic[:], {}
}

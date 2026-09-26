// Package client provides the experimental public Temporal client boundary.
package client

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:encoding/uuid"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"

Error_Kind :: enum {
	None,
	Invalid_Argument,
	Connection,
	Rpc,
	Malformed_Response,
	Payload_Conversion,
	Workflow_Failed,
	Workflow_Canceled,
	Workflow_Terminated,
	Workflow_Timed_Out,
	Workflow_Continued_As_New,
	Workflow_Query_Rejected,
}

Error :: struct {
	kind:        Error_Kind,
	message:     []u8,
	status_code: u32,
	details:     []u8,
}

error_free :: proc(error: ^Error) {
	if error == nil {
		return
	}
	delete(error.message)
	delete(error.details)
	error^ = {}
}

error_from_string :: proc(kind: Error_Kind, message: string) -> Error {
	return {kind = kind, message = bytes.clone(transmute([]u8)message)}
}

error_from_core :: proc(core_error: ^temporal_core.Error, kind := Error_Kind.Rpc) -> Error {
	if core_error == nil || core_error.message == nil {
		return {}
	}
	result := Error{
		kind = kind,
		message = core_error.message,
		status_code = core_error.status_code,
		details = core_error.details,
	}
	core_error^ = {}
	return result
}

Client :: struct {
	runtime:     ^temporal_core.Runtime,
	connection:  ^temporal_core.Client,
	namespace:   string,
}

connect :: proc(
	target_url := "http://localhost:7233",
	namespace := "default",
) -> (client: Client, error: Error) {
	if len(target_url) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Temporal target URL must not be empty")
	}
	if len(namespace) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Temporal namespace must not be empty")
	}
	heap := allocator_runtime.heap_allocator()
	owned_namespace, allocation_error := strings.clone(namespace, heap)
	if allocation_error != nil {
		return {}, error_from_string(.Connection, "could not allocate Temporal namespace")
	}

	runtime := new(temporal_core.Runtime, heap)
	created_runtime, core_error := temporal_core.runtime_create()
	if core_error.message != nil {
		delete(owned_namespace, heap)
		free(runtime, heap)
		return {}, error_from_core(&core_error, .Connection)
	}
	runtime^ = created_runtime
	connection := new(temporal_core.Client, heap)
	created_connection, connect_error := temporal_core.client_connect(runtime, target_url)
	if connect_error.message != nil {
		temporal_core.runtime_close(runtime)
		free(connection, heap)
		free(runtime, heap)
		delete(owned_namespace, heap)
		return {}, error_from_core(&connect_error, .Connection)
	}
	connection^ = created_connection
	return {
		runtime = runtime,
		connection = connection,
		namespace = owned_namespace,
	}, {}
}

close :: proc(client: ^Client) {
	if client == nil {
		return
	}
	heap := allocator_runtime.heap_allocator()
	if client.connection != nil {
		temporal_core.client_close(client.connection)
		free(client.connection, heap)
	}
	if client.runtime != nil {
		temporal_core.runtime_close(client.runtime)
		free(client.runtime, heap)
	}
	delete(client.namespace, heap)
	client^ = {}
}

core_client :: proc(client: ^Client) -> ^temporal_core.Client {
	if client == nil {
		return nil
	}
	return client.connection
}

Start_Workflow_Options :: struct {
	id:                         string,
	task_queue:                 string,
	request_id:                string,
	workflow_execution_timeout: time.Duration,
	workflow_run_timeout:       time.Duration,
	workflow_task_timeout:      time.Duration,
}

Workflow_Handle :: struct {
	client:                 ^Client,
	id:                     string,
	run_id:                 string,
	first_execution_run_id: string,
}

workflow_handle_free :: proc(handle: ^Workflow_Handle) {
	if handle == nil {
		return
	}
	heap := allocator_runtime.heap_allocator()
	delete(handle.id, heap)
	delete(handle.run_id, heap)
	delete(handle.first_execution_run_id, heap)
	handle^ = {}
}

get_workflow_handle :: proc(
	client: ^Client,
	id: string,
	run_id := "",
	first_execution_run_id := "",
) -> (handle: Workflow_Handle, error: Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	if len(id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow ID must not be empty")
	}
	heap := allocator_runtime.heap_allocator()
	owned_id, id_error := strings.clone(id, heap)
	if id_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow handle")
	}
	owned_run_id, run_id_error := strings.clone(run_id, heap)
	if run_id_error != nil {
		delete(owned_id, heap)
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow handle")
	}
	owned_first_run_id, first_run_error := strings.clone(first_execution_run_id, heap)
	if first_run_error != nil {
		delete(owned_run_id, heap)
		delete(owned_id, heap)
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow handle")
	}
	return {
		client = client,
		id = owned_id,
		run_id = owned_run_id,
		first_execution_run_id = owned_first_run_id,
	}, {}
}

duration_proto :: proc(value: time.Duration) -> proto.Google_Protobuf_Duration {
	nanoseconds := i64(value)
	return {
		seconds = nanoseconds / i64(time.Second),
		nanos = i32(nanoseconds % i64(time.Second)),
	}
}

start_workflow_payload :: proc(
	client: ^Client,
	workflow_type: string,
	input: codec.Payload_View,
	options: Start_Workflow_Options,
) -> (handle: Workflow_Handle, error: Error) {
	if client == nil || client.connection == nil || client.connection.handle == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal client is not connected")
	}
	if len(workflow_type) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow type must not be empty")
	}
	if len(options.id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow ID must not be empty")
	}
	if len(options.task_queue) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow task queue must not be empty")
	}
	if options.workflow_execution_timeout < 0 || options.workflow_run_timeout < 0 ||
	   options.workflow_task_timeout < 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow timeouts must not be negative")
	}

	request_id := options.request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(request_id) == 0 {
		request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	}
	workflow_type_value := proto.Temporal_Api_Common_V1_WorkflowType{name = workflow_type}
	task_queue := proto.Temporal_Api_Taskqueue_V1_TaskQueue{name = options.task_queue}
	payload := proto.encode_payload(input)
	defer delete(payload.metadata)
	defer delete(payload.external_payloads)
	inputs := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(inputs.payloads)
	append(&inputs.payloads, payload)

	request := proto.Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest{
		namespace = client.namespace,
		workflow_id = options.id,
		workflow_type = &workflow_type_value,
		task_queue = &task_queue,
		input = &inputs,
		identity = "temporal-sdk-odin@local",
		request_id = request_id,
	}
	execution_timeout := duration_proto(options.workflow_execution_timeout)
	run_timeout := duration_proto(options.workflow_run_timeout)
	task_timeout := duration_proto(options.workflow_task_timeout)
	if options.workflow_execution_timeout > 0 {
		request.workflow_execution_timeout = &execution_timeout
	}
	if options.workflow_run_timeout > 0 {
		request.workflow_run_timeout = &run_timeout
	}
	if options.workflow_task_timeout > 0 {
		request.workflow_task_timeout = &task_timeout
	}

	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_StartWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"StartWorkflowExecution",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize client response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_StartWorkflowExecutionResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_StartWorkflowExecutionResponse(
		rpc_result.protobuf,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded || len(response.run_id) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed start response")
	}

	first_execution_run_id := response.first_execution_run_id
	if len(first_execution_run_id) == 0 {
		first_execution_run_id = response.run_id
	}
	return get_workflow_handle(client, options.id, response.run_id, first_execution_run_id)
}

start_workflow_json :: proc(
	client: ^Client,
	workflow_type: string,
	input: $T,
	options: Start_Workflow_Options,
) -> (handle: Workflow_Handle, error: Error) {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode Workflow input")
	}
	defer codec.payload_free(&payload)
	return start_workflow_payload(client, workflow_type, codec.payload_view(&payload), options)
}

workflow_handle_valid :: proc(handle: ^Workflow_Handle) -> bool {
	return handle != nil && handle.client != nil && handle.client.connection != nil &&
		handle.client.connection.handle != nil && len(handle.id) > 0
}

workflow_execution :: proc(handle: ^Workflow_Handle) -> proto.Temporal_Api_Common_V1_WorkflowExecution {
	return {
		workflow_id = handle.id,
		run_id = handle.run_id,
	}
}

resolve_request_id :: proc(explicit: string, buffer: []u8) -> string {
	if len(explicit) > 0 {
		return explicit
	}
	return uuid.to_string_buffer(uuid.generate_v4(), buffer)
}

rpc_empty_response :: proc(
	handle: ^Workflow_Handle,
	rpc: string,
	request: []u8,
) -> Error {
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		rpc,
		request,
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return error_from_core(&rpc_result.error)
	}
	return {}
}

signal_workflow :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	explicit_request_id := "",
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if len(signal_name) == 0 {
		return error_from_string(.Invalid_Argument, "Workflow signal name must not be empty")
	}
	execution := workflow_execution(handle)
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		signal_name = signal_name,
		identity = "temporal-sdk-odin@local",
		request_id = resolve_request_id(explicit_request_id, request_id_buffer[:]),
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "SignalWorkflowExecution", request_bytes[:])
}

signal_workflow_payload :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	input: codec.Payload_View,
	explicit_request_id := "",
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if len(signal_name) == 0 {
		return error_from_string(.Invalid_Argument, "Workflow signal name must not be empty")
	}
	execution := workflow_execution(handle)
	payload := proto.encode_payload(input)
	defer delete(payload.metadata)
	defer delete(payload.external_payloads)
	inputs := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(inputs.payloads)
	append(&inputs.payloads, payload)
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		signal_name = signal_name,
		input = &inputs,
		identity = "temporal-sdk-odin@local",
		request_id = resolve_request_id(explicit_request_id, request_id_buffer[:]),
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_SignalWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "SignalWorkflowExecution", request_bytes[:])
}

signal_workflow_json :: proc(
	handle: ^Workflow_Handle,
	signal_name: string,
	input: $T,
	explicit_request_id := "",
) -> Error {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow signal input")
	}
	defer codec.payload_free(&payload)
	return signal_workflow_payload(
		handle,
		signal_name,
		codec.payload_view(&payload),
		explicit_request_id,
	)
}

query_workflow_payload :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	input: codec.Payload_View,
) -> (payload: codec.Payload, error: Error) {
	if !workflow_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	if len(query_type) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow query type must not be empty")
	}
	execution := workflow_execution(handle)
	encoded_input := proto.encode_payload(input)
	defer delete(encoded_input.metadata)
	defer delete(encoded_input.external_payloads)
	arguments := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(arguments.payloads)
	append(&arguments.payloads, encoded_input)
	query := proto.Temporal_Api_Query_V1_WorkflowQuery{
		query_type = query_type,
		query_args = &arguments,
	}
	request := proto.Temporal_Api_Workflowservice_V1_QueryWorkflowRequest{
		namespace = handle.client.namespace,
		execution = &execution,
		query = &query,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_QueryWorkflowRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"QueryWorkflow",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize query response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_QueryWorkflowResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_QueryWorkflowResponse(
		rpc_result.protobuf,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed query response")
	}
	if response.query_rejected != nil {
		return {}, error_from_string(
			.Workflow_Query_Rejected,
			"Temporal rejected the Workflow query for the current execution status",
		)
	}
	if response.query_result == nil || len(response.query_result.payloads) != 1 {
		return {}, error_from_string(.Malformed_Response, "Workflow query response must contain one Payload")
	}
	cloned_payload, clone_error := proto.clone_payload(&response.query_result.payloads[0])
	if clone_error != .None {
		return {}, error_from_string(.Malformed_Response, "could not preserve Workflow query result")
	}
	return cloned_payload, {}
}

query_workflow_json :: proc(
	handle: ^Workflow_Handle,
	query_type: string,
	input: $Input,
	value: ^$Output,
) -> Error {
	if value == nil {
		return error_from_string(.Invalid_Argument, "Workflow query output pointer is required")
	}
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow query input")
	}
	defer codec.payload_free(&payload)
	result, query_error := query_workflow_payload(
		handle,
		query_type,
		codec.payload_view(&payload),
	)
	if query_error.message != nil {
		return query_error
	}
	defer codec.payload_free(&result)
	if codec.decode_json(codec.payload_view(&result), value) != .None {
		return error_from_string(.Payload_Conversion, "could not decode Workflow query result")
	}
	return {}
}

cancel_workflow :: proc(
	handle: ^Workflow_Handle,
	reason := "",
	explicit_request_id := "",
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	execution := workflow_execution(handle)
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_RequestCancelWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		identity = "temporal-sdk-odin@local",
		request_id = resolve_request_id(explicit_request_id, request_id_buffer[:]),
		first_execution_run_id = handle.first_execution_run_id,
		reason = reason,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_RequestCancelWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "RequestCancelWorkflowExecution", request_bytes[:])
}

terminate_workflow :: proc(
	handle: ^Workflow_Handle,
	reason := "",
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	execution := workflow_execution(handle)
	request := proto.Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		reason = reason,
		identity = "temporal-sdk-odin@local",
		first_execution_run_id = handle.first_execution_run_id,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "TerminateWorkflowExecution", request_bytes[:])
}

terminate_workflow_payload :: proc(
	handle: ^Workflow_Handle,
	reason: string,
	details: codec.Payload_View,
) -> Error {
	if !workflow_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	execution := workflow_execution(handle)
	payload := proto.encode_payload(details)
	defer delete(payload.metadata)
	defer delete(payload.external_payloads)
	detail_payloads := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(detail_payloads.payloads)
	append(&detail_payloads.payloads, payload)
	request := proto.Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest{
		namespace = handle.client.namespace,
		workflow_execution = &execution,
		reason = reason,
		details = &detail_payloads,
		identity = "temporal-sdk-odin@local",
		first_execution_run_id = handle.first_execution_run_id,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_TerminateWorkflowExecutionRequest(&request)
	defer delete(request_bytes)
	return rpc_empty_response(handle, "TerminateWorkflowExecution", request_bytes[:])
}

terminate_workflow_json :: proc(
	handle: ^Workflow_Handle,
	reason: string,
	details: $T,
) -> Error {
	payload, encode_error := codec.encode_json(details)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode Workflow termination details")
	}
	defer codec.payload_free(&payload)
	return terminate_workflow_payload(handle, reason, codec.payload_view(&payload))
}

Result_Page_Kind :: enum {
	Completed,
	Follow_Run,
	Failed,
	Canceled,
	Terminated,
	Timed_Out,
	Continued_As_New,
}

Result_Page :: struct {
	kind:         Result_Page_Kind,
	payload:      codec.Payload,
	has_payload:  bool,
	next_run_id:  string,
	message:      string,
}

result_page_free :: proc(page: ^Result_Page) {
	if page == nil {
		return
	}
	codec.payload_free(&page.payload)
	delete(page.next_run_id)
	delete(page.message)
	page^ = {}
}

clone_result_string :: proc(value: string) -> (string, bool) {
	if len(value) == 0 {
		return "", true
	}
	owned, allocation_error := strings.clone(value)
	return owned, allocation_error == nil
}

decode_result_page :: proc(data: []u8) -> (page: Result_Page, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize history response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryResponse(
		data,
		&response,
	)
	context.allocator = previous_allocator
	if !decoded || response.history == nil || len(response.history.events) == 0 {
		return {}, error_from_string(.Malformed_Response, "Temporal returned an empty close-event history")
	}

	for &event in response.history.events {
		#partial switch attributes in event.attributes {
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Completed_Event_Attributes:
			if attributes.value == nil {
				break
			}
			if len(attributes.value.new_execution_run_id) > 0 {
				page.kind = .Follow_Run
				clone_ok: bool
				page.next_run_id, clone_ok = clone_result_string(attributes.value.new_execution_run_id)
				if !clone_ok {
					return {}, error_from_string(.Malformed_Response, "could not allocate continued Workflow run ID")
				}
				return page, {}
			}
			page.kind = .Completed
			if attributes.value.result != nil && len(attributes.value.result.payloads) > 0 {
				clone_error: codec.Error
				page.payload, clone_error = proto.clone_payload(&attributes.value.result.payloads[0])
				if clone_error != .None {
					return {}, error_from_string(.Payload_Conversion, "could not copy Workflow result Payload")
				}
				page.has_payload = true
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Failed_Event_Attributes:
			if attributes.value == nil {
				break
			}
			if len(attributes.value.new_execution_run_id) > 0 {
				page.kind = .Follow_Run
				clone_ok: bool
				page.next_run_id, clone_ok = clone_result_string(attributes.value.new_execution_run_id)
				if !clone_ok {
					return {}, error_from_string(.Malformed_Response, "could not allocate retried Workflow run ID")
				}
				return page, {}
			}
			page.kind = .Failed
			message := "Workflow failed"
			if attributes.value.failure != nil && len(attributes.value.failure.message) > 0 {
				message = attributes.value.failure.message
			}
			message_ok: bool
			page.message, message_ok = clone_result_string(message)
			if !message_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow failure")
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Canceled_Event_Attributes:
			page.kind = .Canceled
			message_ok: bool
			page.message, message_ok = clone_result_string("Workflow canceled")
			if !message_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow cancellation")
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Terminated_Event_Attributes:
			page.kind = .Terminated
			message := "Workflow terminated"
			if attributes.value != nil && len(attributes.value.reason) > 0 {
				message = attributes.value.reason
			}
			message_ok: bool
			page.message, message_ok = clone_result_string(message)
			if !message_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow termination")
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Timed_Out_Event_Attributes:
			if attributes.value != nil && len(attributes.value.new_execution_run_id) > 0 {
				page.kind = .Follow_Run
				clone_ok: bool
				page.next_run_id, clone_ok = clone_result_string(attributes.value.new_execution_run_id)
				if !clone_ok {
					return {}, error_from_string(.Malformed_Response, "could not allocate timed-out Workflow run ID")
				}
				return page, {}
			}
			page.kind = .Timed_Out
			message_ok: bool
			page.message, message_ok = clone_result_string("Workflow timed out")
			if !message_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate Workflow timeout")
			}
			return page, {}
		case proto.Temporal_Api_History_V1_HistoryEvent_Attributes_Workflow_Execution_Continued_As_New_Event_Attributes:
			if attributes.value == nil || len(attributes.value.new_execution_run_id) == 0 {
				return {}, error_from_string(.Malformed_Response, "Continue-As-New event omitted its new run ID")
			}
			page.kind = .Continued_As_New
			clone_ok: bool
			page.next_run_id, clone_ok = clone_result_string(attributes.value.new_execution_run_id)
			if !clone_ok {
				return {}, error_from_string(.Malformed_Response, "could not allocate continued Workflow run ID")
			}
			return page, {}
		}
	}
	return {}, error_from_string(.Malformed_Response, "Temporal close-event history had no terminal event")
}

Run_Id_Owner :: struct {
	value: string,
}

run_id_owner_free :: proc(owner: ^Run_Id_Owner) {
	if owner == nil {
		return
	}
	delete(owner.value)
	owner.value = ""
}

get_result_payload :: proc(
	handle: ^Workflow_Handle,
	follow_runs := true,
) -> (payload: codec.Payload, error: Error) {
	if handle == nil || handle.client == nil || handle.client.connection == nil || len(handle.id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Workflow handle is not initialized")
	}
	current_run_id, allocation_error := strings.clone(handle.run_id)
	if allocation_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not allocate Workflow result run ID")
	}
	run_id := Run_Id_Owner{value = current_run_id}
	defer run_id_owner_free(&run_id)

	for {
		execution := proto.Temporal_Api_Common_V1_WorkflowExecution{
			workflow_id = handle.id,
			run_id = run_id.value,
		}
		request := proto.Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryRequest{
			namespace = handle.client.namespace,
			execution = &execution,
			wait_new_event = true,
			history_event_filter_type = .HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
			skip_archival = true,
		}
		request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_GetWorkflowExecutionHistoryRequest(&request)
		rpc_result := temporal_core.client_rpc_call(
			handle.client.connection,
			.Workflow,
			"GetWorkflowExecutionHistory",
			request_bytes[:],
			timeout_millis = 0,
		)
		delete(request_bytes)
		if rpc_result.error.message != nil {
			delete(rpc_result.protobuf)
			return {}, error_from_core(&rpc_result.error)
		}
		page, page_error := decode_result_page(rpc_result.protobuf)
		delete(rpc_result.protobuf)
		if page_error.message != nil {
			result_page_free(&page)
			return {}, page_error
		}

		switch page.kind {
		case .Completed:
			result := page.payload
			page.payload = {}
			result_page_free(&page)
			return result, {}
		case .Follow_Run:
			if !follow_runs {
				result_error := error_from_string(.Workflow_Continued_As_New, "Workflow completed in a new run")
				result_page_free(&page)
				return {}, result_error
			}
			delete(run_id.value)
			run_id.value = page.next_run_id
			page.next_run_id = ""
			result_page_free(&page)
			continue
		case .Continued_As_New:
			if !follow_runs {
				result_error := error_from_string(.Workflow_Continued_As_New, "Workflow continued as new")
				result_page_free(&page)
				return {}, result_error
			}
			delete(run_id.value)
			run_id.value = page.next_run_id
			page.next_run_id = ""
			result_page_free(&page)
			continue
		case .Failed:
			error = error_from_string(.Workflow_Failed, page.message)
		case .Canceled:
			error = error_from_string(.Workflow_Canceled, page.message)
		case .Terminated:
			error = error_from_string(.Workflow_Terminated, page.message)
		case .Timed_Out:
			error = error_from_string(.Workflow_Timed_Out, page.message)
		}
		result_page_free(&page)
		return {}, error
	}
}

get_result_json :: proc(
	handle: ^Workflow_Handle,
	value: ^$T,
	follow_runs := true,
) -> Error {
	payload, result_error := get_result_payload(handle, follow_runs)
	if result_error.message != nil {
		return result_error
	}
	defer codec.payload_free(&payload)
	if payload.allocator.procedure == nil {
		return error_from_string(.Payload_Conversion, "Workflow completed without a result Payload")
	}
	decode_error := codec.decode_json(codec.payload_view(&payload), value)
	if decode_error != .None {
		return error_from_string(.Payload_Conversion, "could not decode Workflow result")
	}
	return {}
}

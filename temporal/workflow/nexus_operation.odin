package workflow

import "core:strings"
import "core:time"
import codec "../codec"
import proto "../core/proto"
import temporal_failure "../failure"
import fiber_raw "fiber/raw"

// Nexus_Operation_Cancellation_Type controls when a canceled Workflow caller
// may continue. The zero value matches Temporal's safest behavior.
Nexus_Operation_Cancellation_Type :: enum {
	Wait_Cancellation_Completed,
	Abandon,
	Try_Cancel,
	Wait_Cancellation_Requested,
}

// Nexus_Header is a Nexus protocol header, not a Temporal Payload header.
// Values are UTF-8 strings on the Core boundary. Options borrow the supplied
// slice only for the duration of start_nexus_operation_*.
Nexus_Header :: struct {
	key:   string,
	value: string,
}

Nexus_Operation_Options :: struct {
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	cancellation_type:         Nexus_Operation_Cancellation_Type,
	headers:                   []Nexus_Header,
}

Owned_Nexus_Operation_Options :: struct {
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	cancellation_type:         Nexus_Operation_Cancellation_Type,
	headers:                   [dynamic]Nexus_Header,
}

Nexus_Operation_Future :: struct {
	seq: u32,
}

Nexus_Operation_Result_Status :: enum {
	Succeeded,
	Failed,
	Canceled,
	Timed_Out,
}

Payload_Nexus_Operation_Result :: struct {
	status:          Nexus_Operation_Result_Status,
	value:           codec.Payload_View,
	failure_message: string,
	// failure borrows the Workflow instance arena.
	failure:         ^temporal_failure.Failure,
}

Json_Nexus_Operation_Result :: struct($T: typeid) {
	status:          Nexus_Operation_Result_Status,
	value:           T,
	failure_message: string,
	// failure borrows the Workflow instance arena.
	failure:         ^temporal_failure.Failure,
}

Nexus_Operation_Result :: struct {
	status:          Nexus_Operation_Result_Status,
	value:           string,
	failure_message: string,
	failure:         ^temporal_failure.Failure,
}

Nexus_Operation_Start_State :: enum {
	Pending,
	Succeeded,
	Failed,
}

Nexus_Operation_State :: enum {
	Scheduled,
	Commanded,
	Resolved,
	Failed,
	Canceled,
	Timed_Out,
	Cancel_Requested,
}

Fiber_Nexus_Operation :: struct {
	seq:                            u32,
	endpoint:                       string,
	service:                        string,
	operation:                      string,
	input:                          codec.Payload,
	options:                        Owned_Nexus_Operation_Options,
	operation_token:                string,
	result:                         codec.Payload,
	failure_message:                string,
	failure_proto:                  []u8,
	failure:                        ^temporal_failure.Failure,
	start_state:                    Nexus_Operation_Start_State,
	state:                          Nexus_Operation_State,
	cancellation_command_emitted:   bool,
	detached_from_root_cancellation: bool,
	cancellation_token_id:          u32,
}

find_nexus_operation :: proc(
	instance: ^Fiber_Instance,
	seq: u32,
) -> (^Fiber_Nexus_Operation, bool) {
	if instance == nil || seq == 0 {
		return nil, false
	}
	for index in 0..<len(instance.nexus_operations) {
		if instance.nexus_operations[index].seq == seq {
			return &instance.nexus_operations[index], true
		}
	}
	return nil, false
}

nexus_operation_options_error :: proc(
	endpoint, service, operation: string,
	options: Nexus_Operation_Options,
) -> string {
	if len(endpoint) == 0 || len(service) == 0 || len(operation) == 0 {
		return "Nexus endpoint, service, and operation are required"
	}
	if options.schedule_to_close_timeout <= 0 {
		return "Nexus schedule-to-close timeout must be positive"
	}
	if options.schedule_to_start_timeout < 0 || options.start_to_close_timeout < 0 {
		return "Nexus timeouts must not be negative"
	}
	for header in options.headers {
		if len(header.key) == 0 {
			return "Nexus header keys must not be empty"
		}
	}
	return ""
}

clone_nexus_operation_options :: proc(
	instance: ^Fiber_Instance,
	options: Nexus_Operation_Options,
) -> (Owned_Nexus_Operation_Options, bool) {
	owned := Owned_Nexus_Operation_Options{
		schedule_to_close_timeout = options.schedule_to_close_timeout,
		schedule_to_start_timeout = options.schedule_to_start_timeout,
		start_to_close_timeout = options.start_to_close_timeout,
		cancellation_type = options.cancellation_type,
		headers = make([dynamic]Nexus_Header, instance.odin_context.allocator),
	}
	for header in options.headers {
		key, key_error := strings.clone(header.key, instance.odin_context.allocator)
		value, value_error := strings.clone(header.value, instance.odin_context.allocator)
		if key_error != nil || value_error != nil {
			return {}, false
		}
		append(&owned.headers, Nexus_Header{key = key, value = value})
	}
	return owned, true
}

mark_nexus_operation_failure :: proc(ctx: ^Fiber_Context, message, failure_type: string) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	ctx.instance.phase = .Failed
	ctx.instance.failure_message = message
	ctx.instance.failure_type = failure_type
}

start_nexus_operation_owned :: proc(
	ctx: ^Fiber_Context,
	endpoint, service, operation: string,
	input: codec.Payload,
	options: Nexus_Operation_Options,
) -> Nexus_Operation_Future {
	owned_input := input
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		codec.payload_free(&owned_input)
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {}
	}
	instance := ctx.instance
	instance.next_nexus_operation_seq += 1
	seq := instance.next_nexus_operation_seq
	token_id := current_cancellation_token_id(instance)
	if current_cancellation_applies(instance) {
		codec.payload_free(&owned_input)
		append(
			&instance.nexus_operations,
			Fiber_Nexus_Operation{
				seq = seq,
				start_state = .Failed,
				state = .Canceled,
				failure_message = cancellation_token_reason_id(instance, token_id),
				cancellation_token_id = token_id,
			},
		)
		return {seq = seq}
	}
	validation_error := nexus_operation_options_error(endpoint, service, operation, options)
	if len(validation_error) > 0 {
		codec.payload_free(&owned_input)
		mark_nexus_operation_failure(ctx, validation_error, "OdinNexusOperationOptionsError")
		return {}
	}
	owned_endpoint, endpoint_error := strings.clone(endpoint, instance.odin_context.allocator)
	owned_service, service_error := strings.clone(service, instance.odin_context.allocator)
	owned_operation, operation_error := strings.clone(operation, instance.odin_context.allocator)
	owned_options, options_ok := clone_nexus_operation_options(instance, options)
	if endpoint_error != nil || service_error != nil || operation_error != nil || !options_ok {
		codec.payload_free(&owned_input)
		mark_nexus_operation_failure(
			ctx,
			"could not preserve Nexus operation",
			"OdinNexusOperationOptionsError",
		)
		return {}
	}
	append(
		&instance.nexus_operations,
		Fiber_Nexus_Operation{
			seq = seq,
			endpoint = owned_endpoint,
			service = owned_service,
			operation = owned_operation,
			input = owned_input,
			options = owned_options,
			start_state = .Pending,
			state = .Scheduled,
			detached_from_root_cancellation = !cancellation_token_linked_to_root(instance, token_id),
			cancellation_token_id = token_id,
		},
	)
	return {seq = seq}
}

start_nexus_operation_payload :: proc(
	ctx: ^Fiber_Context,
	endpoint, service, operation: string,
	input: codec.Payload_View,
	options := Nexus_Operation_Options{schedule_to_close_timeout = 20 * time.Second},
) -> Nexus_Operation_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	owned_input, clone_error := codec.payload_clone(input, ctx.instance.odin_context.allocator)
	if clone_error != .None {
		mark_nexus_operation_failure(
			ctx,
			"could not preserve Nexus operation input",
			"OdinWorkflowPayloadConversionError",
		)
		return {}
	}
	return start_nexus_operation_owned(ctx, endpoint, service, operation, owned_input, options)
}

start_nexus_operation_json :: proc(
	ctx: ^Fiber_Context,
	endpoint, service, operation: string,
	input: $T,
	options := Nexus_Operation_Options{schedule_to_close_timeout = 20 * time.Second},
) -> Nexus_Operation_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		mark_nexus_operation_failure(
			ctx,
			"could not encode Nexus operation input as json/plain",
			"OdinWorkflowPayloadConversionError",
		)
		return {}
	}
	return start_nexus_operation_owned(ctx, endpoint, service, operation, payload, options)
}

start_nexus_operation :: proc(
	ctx: ^Fiber_Context,
	endpoint, service, operation, input: string,
	options := Nexus_Operation_Options{schedule_to_close_timeout = 20 * time.Second},
) -> Nexus_Operation_Future {
	return start_nexus_operation_json(ctx, endpoint, service, operation, input, options)
}

nexus_operation_waits_for_cancellation :: proc(operation: ^Fiber_Nexus_Operation) -> bool {
	return operation != nil &&
		(operation.options.cancellation_type == .Wait_Cancellation_Completed ||
		 operation.options.cancellation_type == .Wait_Cancellation_Requested)
}

nexus_operation_ready_for_waiter :: proc(
	instance: ^Fiber_Instance,
	operation: ^Fiber_Nexus_Operation,
	waiter_token_id: u32,
) -> bool {
	if instance == nil || operation == nil {
		return false
	}
	switch operation.state {
	case .Resolved, .Failed, .Canceled, .Timed_Out:
		return true
	case .Cancel_Requested:
		return !nexus_operation_waits_for_cancellation(operation) ||
			(instance.cancellation_requested &&
			 cancellation_token_linked_to_root(instance, waiter_token_id) &&
			 !operation.detached_from_root_cancellation)
	case .Scheduled, .Commanded:
	}
	return false
}

payload_nexus_operation_result_from_operation :: proc(
	operation: ^Fiber_Nexus_Operation,
) -> Payload_Nexus_Operation_Result {
	switch operation.state {
	case .Resolved:
		return {status = .Succeeded, value = codec.payload_view(&operation.result)}
	case .Failed:
		return {
			status = .Failed,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	case .Timed_Out:
		return {
			status = .Timed_Out,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	case .Canceled, .Cancel_Requested:
		return {
			status = .Canceled,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	case .Scheduled, .Commanded:
	}
	return {status = .Failed, failure_message = "Nexus operation has not resolved"}
}

await_nexus_operation_payload_result :: proc(
	ctx: ^Fiber_Context,
	future: Nexus_Operation_Future,
) -> Payload_Nexus_Operation_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {status = .Failed, failure_message = "invalid Nexus operation await"}
	}
	instance := ctx.instance
	operation, found := find_nexus_operation(instance, future.seq)
	if !found {
		instance.phase = .Failed
		return {status = .Failed, failure_message = "Nexus operation was not found"}
	}
	if nexus_operation_ready_for_waiter(instance, operation, current_cancellation_token_id(instance)) {
		return payload_nexus_operation_result_from_operation(operation)
	}
	instance.waiting_nexus_operation_seq = future.seq
	instance.phase = .Waiting_Nexus_Operation
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return {status = .Failed, failure_message = "could not resume Nexus operation await"}
	}
	operation, found = find_nexus_operation(instance, future.seq)
	if !found || !nexus_operation_ready_for_waiter(
		instance,
		operation,
		current_cancellation_token_id(instance),
	) {
		instance.phase = .Failed
		return {status = .Failed, failure_message = "Nexus operation resumed without a resolution"}
	}
	return payload_nexus_operation_result_from_operation(operation)
}

await_nexus_operation_json_result :: proc(
	ctx: ^Fiber_Context,
	future: Nexus_Operation_Future,
	$T: typeid,
) -> Json_Nexus_Operation_Result(T) {
	payload_result := await_nexus_operation_payload_result(ctx, future)
	if payload_result.status != .Succeeded {
		return {
			status = payload_result.status,
			failure_message = payload_result.failure_message,
			failure = payload_result.failure,
		}
	}
	value: T
	if codec.decode_json(payload_result.value, &value) != .None {
		mark_nexus_operation_failure(
			ctx,
			"could not decode Nexus operation result as json/plain",
			"OdinWorkflowPayloadConversionError",
		)
		return {status = .Failed, failure_message = "could not decode Nexus operation result as json/plain"}
	}
	return {status = .Succeeded, value = value}
}

await_nexus_operation_result :: proc(
	ctx: ^Fiber_Context,
	future: Nexus_Operation_Future,
) -> Nexus_Operation_Result {
	result := await_nexus_operation_json_result(ctx, future, string)
	return {
		status = result.status,
		value = result.value,
		failure_message = result.failure_message,
		failure = result.failure,
	}
}

mark_nexus_operation_failure_unhandled :: proc(
	ctx: ^Fiber_Context,
	future: Nexus_Operation_Future,
) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	operation, found := find_nexus_operation(ctx.instance, future.seq)
	if found && (operation.state == .Failed || operation.state == .Timed_Out) {
		ctx.instance.unhandled_nexus_operation_seq = future.seq
	}
	ctx.instance.phase = .Failed
}

await_nexus_operation_payload :: proc(
	ctx: ^Fiber_Context,
	future: Nexus_Operation_Future,
) -> codec.Payload_View {
	result := await_nexus_operation_payload_result(ctx, future)
	if result.status == .Failed || result.status == .Timed_Out {
		mark_nexus_operation_failure_unhandled(ctx, future)
		return {}
	}
	if result.status == .Canceled {
		return {}
	}
	return result.value
}

await_nexus_operation :: proc(
	ctx: ^Fiber_Context,
	future: Nexus_Operation_Future,
) -> string {
	result := await_nexus_operation_result(ctx, future)
	if result.status == .Failed || result.status == .Timed_Out {
		mark_nexus_operation_failure_unhandled(ctx, future)
		return ""
	}
	if result.status == .Canceled {
		return ""
	}
	return result.value
}

execute_nexus_operation_json_result :: proc(
	ctx: ^Fiber_Context,
	endpoint, service, operation: string,
	input: $Input,
	$Output: typeid,
	options := Nexus_Operation_Options{schedule_to_close_timeout = 20 * time.Second},
) -> Json_Nexus_Operation_Result(Output) {
	return await_nexus_operation_json_result(
		ctx,
		start_nexus_operation_json(ctx, endpoint, service, operation, input, options),
		Output,
	)
}

execute_nexus_operation_json :: proc(
	ctx: ^Fiber_Context,
	endpoint, service, operation: string,
	input: $Input,
	$Output: typeid,
	options := Nexus_Operation_Options{schedule_to_close_timeout = 20 * time.Second},
) -> Output {
	future := start_nexus_operation_json(ctx, endpoint, service, operation, input, options)
	result := await_nexus_operation_json_result(ctx, future, Output)
	if result.status == .Failed || result.status == .Timed_Out {
		mark_nexus_operation_failure_unhandled(ctx, future)
		return {}
	}
	if result.status == .Canceled {
		return {}
	}
	return result.value
}

execute_nexus_operation :: proc(
	ctx: ^Fiber_Context,
	endpoint, service, operation, input: string,
	options := Nexus_Operation_Options{schedule_to_close_timeout = 20 * time.Second},
) -> string {
	return await_nexus_operation(
		ctx,
		start_nexus_operation(ctx, endpoint, service, operation, input, options),
	)
}

cancel_nexus_operation :: proc(
	ctx: ^Fiber_Context,
	future: Nexus_Operation_Future,
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		return false
	}
	operation, found := find_nexus_operation(ctx.instance, future.seq)
	if !found {
		return false
	}
	switch operation.state {
	case .Scheduled:
		operation.state = .Canceled
		operation.start_state = .Failed
		return true
	case .Commanded:
		if operation.options.cancellation_type == .Abandon {
			operation.state = .Canceled
		} else {
			operation.state = .Cancel_Requested
		}
		return true
	case .Cancel_Requested:
		return true
	case .Resolved, .Failed, .Canceled, .Timed_Out:
		return false
	}
	return false
}

nexus_operation_cancellation_type_to_proto :: proc(
	value: Nexus_Operation_Cancellation_Type,
) -> proto.Coresdk_Nexus_NexusOperationCancellationType {
	switch value {
	case .Wait_Cancellation_Completed: return .WAIT_CANCELLATION_COMPLETED
	case .Abandon:                    return .ABANDON
	case .Try_Cancel:                 return .TRY_CANCEL
	case .Wait_Cancellation_Requested: return .WAIT_CANCELLATION_REQUESTED
	}
	return .WAIT_CANCELLATION_COMPLETED
}

append_fiber_schedule_nexus_operation_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Nexus_Operation,
) {
	schedule := new(proto.Coresdk_Workflow_Commands_ScheduleNexusOperation)
	schedule^ = {
		seq = operation.seq,
		endpoint = operation.endpoint,
		service = operation.service,
		operation = operation.operation,
		schedule_to_close_timeout = duration_to_proto(operation.options.schedule_to_close_timeout),
		schedule_to_start_timeout = duration_to_proto(operation.options.schedule_to_start_timeout),
		start_to_close_timeout = duration_to_proto(operation.options.start_to_close_timeout),
		cancellation_type = nexus_operation_cancellation_type_to_proto(operation.options.cancellation_type),
	}
	input := new(proto.Temporal_Api_Common_V1_Payload)
	input^ = proto.encode_payload(codec.payload_view(&operation.input))
	schedule.input = input
	for header in operation.options.headers {
		append(
			&schedule.nexus_header,
			proto.Coresdk_Workflow_Commands_ScheduleNexusOperation_NexusHeaderEntry{
				key = header.key,
				value = header.value,
			},
		)
	}
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Nexus_Operation{
				value = schedule,
			},
		},
	)
	operation.state = .Commanded
	result.scheduled_nexus_operation = true
	result.scheduled_nexus_operation_count += 1
	result.nexus_operation_seq = operation.seq
}

append_fiber_cancel_nexus_operation_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Nexus_Operation,
) {
	if operation.cancellation_command_emitted {
		return
	}
	request := new(proto.Coresdk_Workflow_Commands_RequestCancelNexusOperation)
	request.seq = operation.seq
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Request_Cancel_Nexus_Operation{
				value = request,
			},
		},
	)
	operation.cancellation_command_emitted = true
	if operation.options.cancellation_type == .Try_Cancel {
		operation.state = .Canceled
	}
	result.requested_nexus_operation_cancellation = true
	result.requested_nexus_operation_cancellation_count += 1
	result.canceled_nexus_operation_seq = operation.seq
}

append_fiber_nexus_operation_cancellations :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.nexus_operations) {
		operation := &instance.nexus_operations[index]
		if operation.state == .Cancel_Requested && !operation.cancellation_command_emitted {
			append_fiber_cancel_nexus_operation_command(result, commands, operation)
		}
	}
}

preserve_nexus_operation_failure :: proc(
	operation: ^Fiber_Nexus_Operation,
	failure: ^proto.Temporal_Api_Failure_V1_Failure,
) -> bool {
	if operation == nil || failure == nil {
		return false
	}
	encoded := proto.encode_Temporal_Api_Failure_V1_Failure(failure)
	if len(encoded) == 0 {
		return false
	}
	tree, preserved := preserve_temporal_failure(encoded[:])
	if !preserved {
		return false
	}
	operation.failure_proto = encoded[:]
	operation.failure = tree
	operation.failure_message = tree.message
	return true
}

apply_nexus_operation_start_resolution :: proc(
	instance: ^Fiber_Instance,
	resolution: ^proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart,
	result: ^Process_Result,
) -> string {
	if instance == nil || resolution == nil {
		return "Nexus operation start resolution has no Workflow fiber"
	}
	operation, found := find_nexus_operation(instance, resolution.seq)
	if !found || operation.state == .Scheduled {
		return "Nexus operation start resolution does not match a pending operation"
	}
	if operation.start_state != .Pending {
		result.resolved_nexus_operation_start_seq = operation.seq
		return ""
	}
	handled := false
	#partial switch status in resolution.status {
	case proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart_Status_Operation_Token:
		handled = true
		if len(status.value) == 0 {
			return "started Nexus operation has no operation token"
		}
		operation.operation_token, _ = strings.clone(status.value, instance.odin_context.allocator)
		if len(operation.operation_token) == 0 {
			return "could not preserve Nexus operation token"
		}
		operation.start_state = .Succeeded
	case proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart_Status_Started_Sync:
		handled = true
		if !status.value {
			return "synchronous Nexus operation start was false"
		}
		operation.start_state = .Succeeded
	case proto.Coresdk_Workflow_Activation_ResolveNexusOperationStart_Status_Failed:
		handled = true
		if status.value == nil || !preserve_nexus_operation_failure(operation, status.value) {
			return "could not preserve Nexus operation start failure"
		}
		operation.start_state = .Failed
		operation.state = .Failed
		result.resolved_nexus_operation_failed = true
	}
	if !handled {
		return "Nexus operation start result is unsupported"
	}
	result.resolved_nexus_operation_start_seq = operation.seq
	return ""
}

apply_nexus_operation_resolution :: proc(
	instance: ^Fiber_Instance,
	resolution: ^proto.Coresdk_Workflow_Activation_ResolveNexusOperation,
	result: ^Process_Result,
) -> string {
	if instance == nil || resolution == nil || resolution.result == nil {
		return "Nexus operation resolution has no Workflow fiber or result"
	}
	operation, found := find_nexus_operation(instance, resolution.seq)
	if !found || operation.state == .Scheduled {
		return "Nexus operation resolution does not match a pending operation"
	}
	if operation.state == .Resolved || operation.state == .Failed || operation.state == .Timed_Out {
		result.resolved_nexus_operation_seq = operation.seq
		return ""
	}
	handled := false
	#partial switch status in resolution.result.status {
	case proto.Coresdk_Nexus_NexusOperationResult_Status_Completed:
		handled = true
		if status.value == nil {
			return "completed Nexus operation has no result"
		}
		owned, clone_error := proto.clone_payload(status.value)
		if clone_error != .None {
			return "could not preserve Nexus operation result"
		}
		operation.result = owned
		operation.state = .Resolved
	case proto.Coresdk_Nexus_NexusOperationResult_Status_Failed:
		handled = true
		if status.value == nil || !preserve_nexus_operation_failure(operation, status.value) {
			return "could not preserve Nexus operation failure"
		}
		operation.state = .Failed
		result.resolved_nexus_operation_failed = true
	case proto.Coresdk_Nexus_NexusOperationResult_Status_Cancelled:
		handled = true
		if status.value != nil && !preserve_nexus_operation_failure(operation, status.value) {
			return "could not preserve Nexus operation cancellation"
		}
		operation.state = .Canceled
		result.resolved_nexus_operation_canceled = true
	case proto.Coresdk_Nexus_NexusOperationResult_Status_Timed_Out:
		handled = true
		if status.value == nil || !preserve_nexus_operation_failure(operation, status.value) {
			return "could not preserve Nexus operation timeout"
		}
		operation.state = .Timed_Out
		result.resolved_nexus_operation_timed_out = true
	}
	if !handled {
		return "Nexus operation result is unsupported"
	}
	result.resolved_nexus_operation_seq = operation.seq
	return ""
}

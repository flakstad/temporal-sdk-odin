package standalone_activity

import "core:encoding/uuid"
import "core:fmt"
import temporal_core "../../core"
import proto "../../core/proto"

// encode_standalone_activity_control_request exposes the exact wire boundary
// for Pause/Unpause/Reset tests. The caller owns the returned bytes.
encode_standalone_activity_control_request :: proc(
	namespace, identity, activity_id, run_id: string,
	kind: Standalone_Activity_Control_Kind,
	options: Standalone_Activity_Control_Options = {},
) -> ([dynamic]u8, Error) {
	if len(namespace) == 0 || len(activity_id) == 0 {
		return nil, error_from_string(.Invalid_Argument, "standalone Activity control identity is incomplete")
	}
	if kind == .Invalid {
		return nil, error_from_string(.Invalid_Argument, "standalone Activity control kind is invalid")
	}
	if options.jitter < 0 {
		return nil, error_from_string(.Invalid_Argument, "standalone Activity control jitter must not be negative")
	}
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request_id := resolve_request_id(options.request_id, request_id_buffer[:])
	resource_id := fmt.aprintf("activity:%s", activity_id)
	defer delete(resource_id)
	jitter := duration_proto(options.jitter)
	switch kind {
	case .Pause:
		request := proto.Temporal_Api_Workflowservice_V1_PauseActivityExecutionRequest{
			namespace = namespace,
			activity_id = activity_id,
			run_id = run_id,
			identity = identity,
			reason = options.reason,
			resource_id = resource_id,
			request_id = request_id,
		}
		return proto.encode_Temporal_Api_Workflowservice_V1_PauseActivityExecutionRequest(&request), {}
	case .Unpause:
		request := proto.Temporal_Api_Workflowservice_V1_UnpauseActivityExecutionRequest{
			namespace = namespace,
			activity_id = activity_id,
			run_id = run_id,
			identity = identity,
			reason = options.reason,
			resource_id = resource_id,
			request_id = request_id,
		}
		if options.jitter > 0 do request.jitter = &jitter
		return proto.encode_Temporal_Api_Workflowservice_V1_UnpauseActivityExecutionRequest(&request), {}
	case .Reset:
		request := proto.Temporal_Api_Workflowservice_V1_ResetActivityExecutionRequest{
			namespace = namespace,
			activity_id = activity_id,
			run_id = run_id,
			identity = identity,
			keep_paused = options.keep_paused,
			restore_original_options = options.restore_original_options,
			resource_id = resource_id,
			request_id = request_id,
			reset_heartbeat = options.reset_heartbeat,
		}
		if options.jitter > 0 do request.jitter = &jitter
		return proto.encode_Temporal_Api_Workflowservice_V1_ResetActivityExecutionRequest(&request), {}
	case .Invalid:
		return nil, error_from_string(.Invalid_Argument, "standalone Activity control kind is invalid")
	}
	return nil, error_from_string(.Invalid_Argument, "standalone Activity control kind is invalid")
}

standalone_activity_control_terminal :: proc(
	input: ^Client_Standalone_Activity_Control_Input,
) -> Error {
	if input == nil || !standalone_activity_handle_valid(input.handle) {
		return error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	request_bytes, request_error := encode_standalone_activity_control_request(
		input.handle.client.namespace,
		client_request_identity(input.handle.client),
		input.handle.id,
		input.handle.run_id,
		input.kind,
		input.options,
	)
	if request_error.message != nil do return request_error
	defer delete(request_bytes)
	rpc_name := ""
	switch input.kind {
	case .Pause:   rpc_name = "PauseActivityExecution"
	case .Unpause: rpc_name = "UnpauseActivityExecution"
	case .Reset:   rpc_name = "ResetActivityExecution"
	case .Invalid:
		return error_from_string(.Invalid_Argument, "standalone Activity control kind is invalid")
	}
	rpc_result := temporal_core.client_rpc_call(
		input.handle.client.connection,
		.Workflow,
		rpc_name,
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil do return error_from_core(&rpc_result.error)
	return {}
}

standalone_activity_control :: proc(
	handle: ^Standalone_Activity_Handle,
	kind: Standalone_Activity_Control_Kind,
	options: Standalone_Activity_Control_Options = {},
) -> Error {
	if !standalone_activity_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "standalone Activity handle is not initialized")
	}
	if kind == .Invalid {
		return error_from_string(.Invalid_Argument, "standalone Activity control kind is invalid")
	}
	input := Client_Standalone_Activity_Control_Input{
		handle = handle,
		kind = kind,
		options = options,
	}
	state := client_interceptor_state(handle.client)
	return client_standalone_activity_control_chain(&state, &input)
}

pause_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	options: Standalone_Activity_Control_Options = {},
) -> Error {
	return standalone_activity_control(handle, .Pause, options)
}

unpause_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	options: Standalone_Activity_Control_Options = {},
) -> Error {
	return standalone_activity_control(handle, .Unpause, options)
}

reset_standalone_activity :: proc(
	handle: ^Standalone_Activity_Handle,
	options: Standalone_Activity_Control_Options = {},
) -> Error {
	return standalone_activity_control(handle, .Reset, options)
}

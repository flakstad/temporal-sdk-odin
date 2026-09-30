package main

import "core:fmt"
import "core:os"
import "core:strconv"
import "core:time"
import temporal_client "../../temporal/client"
import codec "../../temporal/codec"
import temporal_failure "../../temporal/failure"

env :: proc(name: string, buffer: []u8) -> string {
	return os.get_env(buffer, name)
}

print_error :: proc(operation: string, error: ^temporal_client.Error) {
	fmt.eprintf(
		"Workflow %s failed (kind=%v status=%d): %s\n",
		operation,
		error.kind,
		error.status_code,
		string(error.message),
	)
	temporal_client.error_free(error)
}

Greeting_Query_Input :: struct {
	include_name: bool,
}

Greeting_Query_Output :: struct {
	phase: string,
	name:  string,
	caller: string,
}

Greeting_Update_Input :: struct {
	name: string,
}

Greeting_Update_Output :: struct {
	previous_name: string,
	current_name:  string,
	caller:        string,
}

Failure_Details :: struct {
	dependency: string,
	code:       int,
}

add_client_context :: proc(headers: ^temporal_client.Headers) -> temporal_client.Error {
	return temporal_client.headers_add_json(headers, "caller", "odin-client-control")
}

signal_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Signal_Workflow_Input,
	next: temporal_client.Client_Signal_Workflow_Next,
) -> temporal_client.Error {
	header_error := add_client_context(input.headers)
	if header_error.message != nil {
		return header_error
	}
	return temporal_client.client_signal_workflow_next(next, input)
}

query_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Query_Workflow_Input,
	next: temporal_client.Client_Query_Workflow_Next,
) -> (codec.Payload, temporal_client.Error) {
	header_error := add_client_context(input.headers)
	if header_error.message != nil {
		return {}, header_error
	}
	return temporal_client.client_query_workflow_next(next, input)
}

update_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Start_Update_Input,
	next: temporal_client.Client_Start_Update_Next,
) -> (temporal_client.Update_Handle, temporal_client.Error) {
	header_error := add_client_context(input.headers)
	if header_error.message != nil {
		return {}, header_error
	}
	return temporal_client.client_start_update_next(next, input)
}

cancel_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Cancel_Workflow_Input,
	next: temporal_client.Client_Cancel_Workflow_Next,
) -> temporal_client.Error {
	fmt.printf("Client cancel interceptor entered reason=%q\n", input.reason)
	result := temporal_client.client_cancel_workflow_next(next, input)
	if result.message == nil {
		fmt.println("Client cancel interceptor exited after Core")
	}
	return result
}

reset_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Reset_Workflow_Input,
	next: temporal_client.Client_Reset_Workflow_Next,
) -> (temporal_client.Workflow_Handle, temporal_client.Error) {
	fmt.printf(
		"Client reset interceptor entered event_id=%d reason=%q\n",
		input.options.workflow_task_finish_event_id,
		input.options.reason,
	)
	result, result_error := temporal_client.client_reset_workflow_next(next, input)
	if result_error.message == nil {
		fmt.println("Client reset interceptor exited after Core")
	}
	return result, result_error
}

terminate_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Terminate_Workflow_Input,
	next: temporal_client.Client_Terminate_Workflow_Next,
) -> temporal_client.Error {
	fmt.printf(
		"Client terminate interceptor entered reason=%q has_details=%v\n",
		input.reason,
		input.has_details,
	)
	result := temporal_client.client_terminate_workflow_next(next, input)
	if result.message == nil {
		fmt.println("Client terminate interceptor exited after Core")
	}
	return result
}

run :: proc() -> int {
	action_buffer: [32]u8
	id_buffer: [256]u8
	name_buffer: [128]u8
	input_buffer: [1024]u8
	reason_buffer: [512]u8
	event_id_buffer: [32]u8
	action := env("TEMPORAL_CLIENT_ACTION", action_buffer[:])
	workflow_id := env("TEMPORAL_CLIENT_WORKFLOW_ID", id_buffer[:])
	if len(action) == 0 || len(workflow_id) == 0 {
		fmt.eprintln("TEMPORAL_CLIENT_ACTION and TEMPORAL_CLIENT_WORKFLOW_ID are required")
		return 2
	}

	interceptors := [?]temporal_client.Client_Interceptor{{
		signal_workflow = signal_interceptor,
		query_workflow = query_interceptor,
		start_update = update_interceptor,
		cancel_workflow = cancel_interceptor,
		reset_workflow = reset_interceptor,
		terminate_workflow = terminate_interceptor,
	}}
	client, connect_error := temporal_client.connect_with_options({
		target_url = "http://localhost:7233",
		namespace = "default",
		interceptors = interceptors[:],
	})
	if connect_error.message != nil {
		print_error("connection", &connect_error)
		return 1
	}
	defer temporal_client.close(&client)
	handle, handle_error := temporal_client.get_workflow_handle(&client, workflow_id)
	if handle_error.message != nil {
		print_error("handle creation", &handle_error)
		return 1
	}
	defer temporal_client.workflow_handle_free(&handle)

	operation_error: temporal_client.Error
	switch action {
	case "signal":
		signal_name := env("TEMPORAL_CLIENT_SIGNAL_NAME", name_buffer[:])
		if len(signal_name) == 0 {
			signal_name = "approval"
		}
		input := env("TEMPORAL_CLIENT_SIGNAL_INPUT", input_buffer[:])
		operation_error = temporal_client.signal_workflow_json_with_options(
			&handle,
			signal_name,
			input,
			{},
		)
	case "cancel":
		reason := env("TEMPORAL_CLIENT_REASON", reason_buffer[:])
		operation_error = temporal_client.cancel_workflow(&handle, reason)
	case "reset":
		reason := env("TEMPORAL_CLIENT_REASON", reason_buffer[:])
		event_id_value := env("TEMPORAL_CLIENT_RESET_EVENT_ID", event_id_buffer[:])
		event_id, event_id_ok := strconv.parse_i64(event_id_value)
		if !event_id_ok || event_id <= 0 {
			fmt.eprintln("TEMPORAL_CLIENT_RESET_EVENT_ID must be a positive integer")
			return 2
		}
		reset_handle, reset_error := temporal_client.reset_workflow(
			&handle,
			{
				workflow_task_finish_event_id = event_id,
				reason = reason,
			},
		)
		if reset_error.message != nil {
			print_error(action, &reset_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&reset_handle)
		fmt.printf(
			"Odin client reset workflow_id=%s new_run_id=%s event_id=%d\n",
			workflow_id,
			reset_handle.run_id,
			event_id,
		)
		return 0
	case "query":
		query_name := env("TEMPORAL_CLIENT_QUERY_NAME", name_buffer[:])
		if len(query_name) == 0 {
			query_name = "greeting_status"
		}
		output: Greeting_Query_Output
		operation_error = temporal_client.query_workflow_json_with_options(
			&handle,
			query_name,
			Greeting_Query_Input{include_name = true},
			&output,
			{},
		)
		if operation_error.message != nil {
			print_error(action, &operation_error)
			return 1
		}
		defer delete(output.phase)
		defer delete(output.name)
		defer delete(output.caller)
		fmt.printf(
			"Odin client query=%s workflow_id=%s phase=%q name=%q caller=%q\n",
			query_name,
			workflow_id,
			output.phase,
			output.name,
			output.caller,
		)
		return 0
	case "update", "update-timeout", "update-failure":
		update_name := env("TEMPORAL_CLIENT_UPDATE_NAME", name_buffer[:])
		if len(update_name) == 0 {
			update_name = action == "update-failure" ? "fail_greeting_name" : "set_greeting_name"
		}
		new_name := env("TEMPORAL_CLIENT_UPDATE_INPUT", input_buffer[:])
		if len(new_name) == 0 {
			new_name = "Temporal"
		}
		update_handle, start_update_error := temporal_client.start_update_json_with_options(
			&handle,
			update_name,
			Greeting_Update_Input{name = new_name},
			{},
		)
		if start_update_error.message != nil {
			operation_error = start_update_error
			print_error(action, &operation_error)
			return 1
		}
		defer temporal_client.update_handle_free(&update_handle)
		output: Greeting_Update_Output
		timeout_probe := action == "update-timeout"
		if timeout_probe {
			// Leave enough time for the long poll to be established before its
			// deadline. Very small deadlines can exercise a tonic/h2 teardown race
			// instead of the Update wait semantics this example is meant to prove.
			operation_error = temporal_client.wait_update_json_with_options(
				&update_handle,
				&output,
				{timeout = 250 * time.Millisecond},
			)
			if operation_error.kind != .Wait_Timed_Out {
				print_error("bounded Update wait", &operation_error)
				return 1
			}
			temporal_client.error_free(&operation_error)
			attached, attach_error := temporal_client.get_update_handle(
				&client,
				update_handle.workflow_id,
				update_handle.run_id,
				update_handle.update_id,
			)
			if attach_error.message != nil {
				print_error("Update handle attachment", &attach_error)
				return 1
			}
			defer temporal_client.update_handle_free(&attached)
			operation_error = temporal_client.wait_update_json(&attached, &output)
			if operation_error.message == nil {
				fmt.println("Update result wait timed out locally and reattached without canceling the durable Update")
			}
		} else {
			operation_error = temporal_client.wait_update_json(&update_handle, &output)
		}
		if action == "update-failure" {
			if operation_error.kind != .Workflow_Update_Failed {
				print_error("structured Update failure", &operation_error)
				return 1
			}
			decoded_failure, decode_error := temporal_client.decode_error_failure(&operation_error)
			defer temporal_failure.failure_free(&decoded_failure)
			activity_node := temporal_failure.find_kind(&decoded_failure, .Activity)
			application_node := temporal_failure.find_kind(&decoded_failure, .Application)
			valid := decode_error == .None && activity_node != nil && application_node != nil
			if valid {
				valid = activity_node.activity.activity_id == "update-failure-activity" &&
				        activity_node.activity.activity_type == "failure_details" &&
				        activity_node.activity.retry_state == .Maximum_Attempts_Reached &&
				        application_node.application.type == "DependencyRejected" &&
				        application_node.application.has_next_retry_delay &&
				        application_node.application.next_retry_delay == 250 * time.Millisecond &&
				        len(application_node.application.details) == 1
			}
			if valid {
				details: Failure_Details
				detail_error := codec.decode_json(
					codec.payload_view(&application_node.application.details[0]),
					&details,
				)
				valid = detail_error == .None && details.dependency == "billing" && details.code == 409
				delete(details.dependency)
			}
			if !valid {
				print_error("structured Update failure validation", &operation_error)
				return 1
			}
			temporal_client.error_free(&operation_error)
			fmt.println("Workflow Update preserved the structured Activity failure tree")
			return 0
		}
		if operation_error.message != nil {
			print_error(action, &operation_error)
			return 1
		}
		defer delete(output.previous_name)
		defer delete(output.current_name)
		defer delete(output.caller)
		fmt.printf(
			"Odin client update=%s workflow_id=%s previous=%q current=%q caller=%q\n",
			update_name,
			workflow_id,
			output.previous_name,
			output.current_name,
			output.caller,
		)
		return 0
	case "terminate":
		reason := env("TEMPORAL_CLIENT_REASON", reason_buffer[:])
		operation_error = temporal_client.terminate_workflow(&handle, reason)
	case:
		fmt.eprintf("unsupported TEMPORAL_CLIENT_ACTION: %s\n", action)
		return 2
	}
	if operation_error.message != nil {
		print_error(action, &operation_error)
		return 1
	}
	fmt.printf("Odin client operation=%s accepted workflow_id=%s\n", action, workflow_id)
	return 0
}

main :: proc() {
	exit_code := run()
	if exit_code != 0 {
		os.exit(exit_code)
	}
}

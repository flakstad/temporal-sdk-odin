package main

import "core:fmt"
import "core:os"
import temporal_client "../../temporal/client"

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

run :: proc() -> int {
	action_buffer: [32]u8
	id_buffer: [256]u8
	name_buffer: [128]u8
	input_buffer: [1024]u8
	reason_buffer: [512]u8
	action := env("TEMPORAL_CLIENT_ACTION", action_buffer[:])
	workflow_id := env("TEMPORAL_CLIENT_WORKFLOW_ID", id_buffer[:])
	if len(action) == 0 || len(workflow_id) == 0 {
		fmt.eprintln("TEMPORAL_CLIENT_ACTION and TEMPORAL_CLIENT_WORKFLOW_ID are required")
		return 2
	}

	client, connect_error := temporal_client.connect()
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
		headers := temporal_client.Headers{}
		headers_error := temporal_client.headers_init(&headers)
		if headers_error.message != nil {
			print_error("signal header initialization", &headers_error)
			return 1
		}
		temporal_client.error_free(&headers_error)
		defer temporal_client.headers_free(&headers)
		header_error := temporal_client.headers_add_json(
			&headers,
			"caller",
			"odin-client-control",
		)
		if header_error.message != nil {
			print_error("signal header encoding", &header_error)
			return 1
		}
		temporal_client.error_free(&header_error)
		operation_error = temporal_client.signal_workflow_json_with_options(
			&handle,
			signal_name,
			input,
			{headers = &headers},
		)
	case "cancel":
		reason := env("TEMPORAL_CLIENT_REASON", reason_buffer[:])
		operation_error = temporal_client.cancel_workflow(&handle, reason)
	case "query":
		query_name := env("TEMPORAL_CLIENT_QUERY_NAME", name_buffer[:])
		if len(query_name) == 0 {
			query_name = "greeting_status"
		}
		output: Greeting_Query_Output
		headers := temporal_client.Headers{}
		headers_error := temporal_client.headers_init(&headers)
		if headers_error.message != nil {
			print_error("query header initialization", &headers_error)
			return 1
		}
		temporal_client.error_free(&headers_error)
		defer temporal_client.headers_free(&headers)
		header_error := temporal_client.headers_add_json(
			&headers,
			"caller",
			"odin-client-control",
		)
		if header_error.message != nil {
			print_error("query header encoding", &header_error)
			return 1
		}
		temporal_client.error_free(&header_error)
		operation_error = temporal_client.query_workflow_json_with_options(
			&handle,
			query_name,
			Greeting_Query_Input{include_name = true},
			&output,
			{headers = &headers},
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
	case "update":
		update_name := env("TEMPORAL_CLIENT_UPDATE_NAME", name_buffer[:])
		if len(update_name) == 0 {
			update_name = "set_greeting_name"
		}
		new_name := env("TEMPORAL_CLIENT_UPDATE_INPUT", input_buffer[:])
		if len(new_name) == 0 {
			new_name = "Temporal"
		}
		headers := temporal_client.Headers{}
		headers_error := temporal_client.headers_init(&headers)
		if headers_error.message != nil {
			print_error("Update header initialization", &headers_error)
			return 1
		}
		temporal_client.error_free(&headers_error)
		defer temporal_client.headers_free(&headers)
		header_error := temporal_client.headers_add_json(
			&headers,
			"caller",
			"odin-client-control",
		)
		if header_error.message != nil {
			print_error("Update header encoding", &header_error)
			return 1
		}
		temporal_client.error_free(&header_error)
		update_handle, start_update_error := temporal_client.start_update_json_with_options(
			&handle,
			update_name,
			Greeting_Update_Input{name = new_name},
			{headers = &headers},
		)
		if start_update_error.message != nil {
			operation_error = start_update_error
			print_error(action, &operation_error)
			return 1
		}
		defer temporal_client.update_handle_free(&update_handle)
		output: Greeting_Update_Output
		operation_error = temporal_client.wait_update_json(&update_handle, &output)
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

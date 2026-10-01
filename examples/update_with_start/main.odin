package main

import "core:encoding/uuid"
import "core:fmt"
import "core:os"
import temporal_client "../../temporal/client"

Greeting_Update_Input :: struct {
	name: string,
}

Greeting_Update_Output :: struct {
	previous_name: string,
	current_name:  string,
	caller:        string,
}

print_error :: proc(operation: string, error: ^temporal_client.Error) {
	fmt.eprintf("%s failed kind=%v status=%d message=%s\n", operation, error.kind, error.status_code, string(error.message))
}

run :: proc() -> int {
	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		print_error("connect", &connect_error)
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	update_headers := temporal_client.Headers{}
	headers_error := temporal_client.headers_init(&update_headers)
	if headers_error.message != nil {
		print_error("initialize Update headers", &headers_error)
		temporal_client.error_free(&headers_error)
		return 1
	}
	defer temporal_client.headers_free(&update_headers)
	header_error := temporal_client.headers_add_json(
		&update_headers,
		"caller",
		"odin-client-control",
	)
	if header_error.message != nil {
		print_error("encode Update header", &header_error)
		temporal_client.error_free(&header_error)
		return 1
	}

	env_buffer: [128]u8
	workflow_id := os.get_env(env_buffer[:], "TEMPORAL_UPDATE_WITH_START_WORKFLOW_ID")
	generated_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(workflow_id) == 0 {
		workflow_id = uuid.to_string_buffer(uuid.generate_v4(), generated_id_buffer[:])
	}
	options := temporal_client.Update_With_Start_Options{
		start_options = {
			id = workflow_id,
			task_queue = "odin-hello-workflow",
			id_conflict_policy = .Use_Existing,
		},
		update_options = {
			update_id = "odin-update-with-start-create",
			headers = &update_headers,
		},
	}
	created, create_error := temporal_client.update_with_start_json(
		&client,
		"odin_hello_workflow",
		"Initial",
		"set_greeting_name",
		Greeting_Update_Input{name = "Created"},
		options,
	)
	if create_error.message != nil {
		print_error("Update-With-Start create", &create_error)
		temporal_client.error_free(&create_error)
		return 1
	}
	defer temporal_client.update_with_start_result_free(&created)
	if !created.workflow.started_by_request {
		fmt.eprintln("Update-With-Start did not create the missing Workflow")
		return 1
	}
	created_output: Greeting_Update_Output
	created_wait_error := temporal_client.wait_update_json(&created.update, &created_output)
	if created_wait_error.message != nil {
		print_error("wait for create-path Update", &created_wait_error)
		temporal_client.error_free(&created_wait_error)
		return 1
	}
	defer delete(created_output.previous_name)
	defer delete(created_output.current_name)
	defer delete(created_output.caller)
	if created_output.previous_name != "Initial" ||
	   created_output.current_name != "Created" ||
	   created_output.caller != "odin-client-control" {
		fmt.eprintf("Unexpected create-path Update result: %+v\n", created_output)
		return 1
	}

	options.update_options.update_id = "odin-update-with-start-attach"
	attached, attach_error := temporal_client.update_with_start_json(
		&client,
		"odin_hello_workflow",
		"Ignored",
		"set_greeting_name",
		Greeting_Update_Input{name = "Attached"},
		options,
	)
	if attach_error.message != nil {
		print_error("Update-With-Start attach", &attach_error)
		temporal_client.error_free(&attach_error)
		return 1
	}
	defer temporal_client.update_with_start_result_free(&attached)
	if attached.workflow.started_by_request ||
	   attached.workflow.run_id != created.workflow.run_id {
		fmt.eprintln("Update-With-Start did not attach to the running Workflow")
		return 1
	}
	attached_output: Greeting_Update_Output
	attached_wait_error := temporal_client.wait_update_json(&attached.update, &attached_output)
	if attached_wait_error.message != nil {
		print_error("wait for attach-path Update", &attached_wait_error)
		temporal_client.error_free(&attached_wait_error)
		return 1
	}
	defer delete(attached_output.previous_name)
	defer delete(attached_output.current_name)
	defer delete(attached_output.caller)
	if attached_output.previous_name != "Created" ||
	   attached_output.current_name != "Attached" ||
	   attached_output.caller != "odin-client-control" {
		fmt.eprintf("Unexpected attach-path Update result: %+v\n", attached_output)
		return 1
	}

	signal_error := temporal_client.signal_workflow_json(
		&created.workflow,
		"approval",
		"Odin",
	)
	if signal_error.message != nil {
		print_error("approve Workflow", &signal_error)
		temporal_client.error_free(&signal_error)
		return 1
	}
	workflow_result: string
	result_error := temporal_client.get_result_json(&created.workflow, &workflow_result)
	if result_error.message != nil {
		print_error("wait for Workflow result", &result_error)
		temporal_client.error_free(&result_error)
		return 1
	}
	defer delete(workflow_result)
	if workflow_result != "Hello Odin" {
		fmt.eprintf("Unexpected Workflow result: %s\n", workflow_result)
		return 1
	}
	fmt.printf(
		"Update-With-Start created then attached workflow_id=%s run_id=%s result=%q\n",
		created.workflow.id,
		created.workflow.run_id,
		workflow_result,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

package main

import "core:encoding/uuid"
import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"

Typed_Greeting_Input :: struct {
	name:       string,
	salutation: string,
}

Typed_Greeting_Output :: struct {
	message:                        string,
	name_length:                    int,
	activity_id:                    string,
	activity_type:                  string,
	task_queue:                     string,
	namespace:                      string,
	workflow_id:                    string,
	workflow_type:                  string,
	attempt:                        u32,
	is_workflow_activity:           bool,
	has_deadline:                   bool,
	retry_maximum_attempts:         i32,
	heartbeat_timeout_milliseconds: i64,
}

run :: proc() -> int {
	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		fmt.eprintf("Temporal client connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)

	env_buffer: [128]u8
	workflow_id := os.get_env(env_buffer[:], "TEMPORAL_CLIENT_WORKFLOW_ID")
	generated_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(workflow_id) == 0 {
		workflow_id = uuid.to_string_buffer(uuid.generate_v4(), generated_id_buffer[:])
	}
	scenario_buffer: [64]u8
	scenario := os.get_env(scenario_buffer[:], "TEMPORAL_CLIENT_SCENARIO")
	if scenario == "failure-details" {
		handle, start_error := temporal_client.start_workflow_json(
			&client,
			"odin_failure_details_workflow",
			"Odin",
			{
				id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
		)
		if start_error.message != nil {
			fmt.eprintf(
				"Failure-details Workflow start failed (status=%d): %s\n",
				start_error.status_code,
				string(start_error.message),
			)
			temporal_client.error_free(&start_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&handle)
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		defer delete(result)
		if result_error.message != nil {
			fmt.eprintf(
				"Failure-details Workflow result failed (kind=%v status=%d): %s\n",
				result_error.kind,
				result_error.status_code,
				string(result_error.message),
			)
			temporal_client.error_free(&result_error)
			return 1
		}
		fmt.printf(
			"Odin client completed failure-details workflow_id=%s result=%q\n",
			workflow_id,
			result,
		)
		return 0
	}
	if scenario == "workflow-properties" {
		metadata := temporal_client.Start_Metadata{}
		metadata_init_error := temporal_client.start_metadata_init(&metadata)
		if metadata_init_error.message != nil {
			fmt.eprintf("Workflow metadata initialization failed: %s\n", string(metadata_init_error.message))
			temporal_client.error_free(&metadata_init_error)
			return 1
		}
		temporal_client.error_free(&metadata_init_error)
		defer temporal_client.start_metadata_free(&metadata)
		header_error := temporal_client.start_metadata_add_header_json(
			&metadata,
			"odin-header",
			"client-header",
		)
		if header_error.message != nil {
			fmt.eprintf("Workflow header encoding failed: %s\n", string(header_error.message))
			temporal_client.error_free(&header_error)
			return 1
		}
		temporal_client.error_free(&header_error)
		memo_error := temporal_client.start_metadata_add_memo_json(
			&metadata,
			"odin-start",
			"before",
		)
		if memo_error.message != nil {
			fmt.eprintf("Workflow memo encoding failed: %s\n", string(memo_error.message))
			temporal_client.error_free(&memo_error)
			return 1
		}
		temporal_client.error_free(&memo_error)
		search_error := temporal_client.start_metadata_add_search_attribute_json(
			&metadata,
			"OdinSdkKeyword",
			.Keyword,
			"before",
		)
		if search_error.message != nil {
			fmt.eprintf("Search Attribute encoding failed: %s\n", string(search_error.message))
			temporal_client.error_free(&search_error)
			return 1
		}
		temporal_client.error_free(&search_error)
		handle, start_error := temporal_client.start_workflow_json(
			&client,
			"odin_workflow_properties",
			workflow_id,
			{
				id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
				metadata = &metadata,
			},
		)
		if start_error.message != nil {
			fmt.eprintf(
				"Workflow-properties start failed (status=%d): %s\n",
				start_error.status_code,
				string(start_error.message),
			)
			temporal_client.error_free(&start_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&handle)
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		defer delete(result)
		if result_error.message != nil {
			fmt.eprintf(
				"Workflow-properties result failed (kind=%v status=%d): %s\n",
				result_error.kind,
				result_error.status_code,
				string(result_error.message),
			)
			temporal_client.error_free(&result_error)
			return 1
		}
		fmt.printf(
			"Odin client completed workflow-properties workflow_id=%s result=%q\n",
			workflow_id,
			result,
		)
		return 0
	}
	handle, start_error := temporal_client.start_workflow_json(
		&client,
		"odin_typed_workflow",
		Typed_Greeting_Input{name = "Odin", salutation = "Hello"},
		{
			id = workflow_id,
			task_queue = "odin-hello-workflow",
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	if start_error.message != nil {
		fmt.eprintf(
			"Workflow start failed (status=%d): %s\n",
			start_error.status_code,
			string(start_error.message),
		)
		temporal_client.error_free(&start_error)
		return 1
	}
	defer temporal_client.workflow_handle_free(&handle)

	result: Typed_Greeting_Output
	defer delete(result.message)
	defer delete(result.activity_id)
	defer delete(result.activity_type)
	defer delete(result.task_queue)
	defer delete(result.namespace)
	defer delete(result.workflow_id)
	defer delete(result.workflow_type)
	result_error := temporal_client.get_result_json(&handle, &result)
	if result_error.message != nil {
		fmt.eprintf(
			"Workflow result failed (kind=%v status=%d): %s\n",
			result_error.kind,
			result_error.status_code,
			string(result_error.message),
		)
		temporal_client.error_free(&result_error)
		return 1
	}

	fmt.printf(
		"Odin client completed workflow_id=%s run_id=%s message=%q activity_id=%s task_queue=%s retry_attempts=%d\n",
		result.workflow_id,
		handle.run_id,
		result.message,
		result.activity_id,
		result.task_queue,
		result.retry_maximum_attempts,
	)
	return 0
}

main :: proc() {
	exit_code := run()
	if exit_code != 0 {
		os.exit(exit_code)
	}
}

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

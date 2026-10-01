package main

import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"

fail :: proc(message: string, error: ^temporal_client.Error = nil) {
	if error != nil && error.message != nil {
		fmt.eprintf("%s: %s (status=%d)\n", message, string(error.message), error.status_code)
		temporal_client.error_free(error)
	} else {
		fmt.eprintln(message)
	}
	os.exit(1)
}

main :: proc() {
	client, connect_error := temporal_client.connect()
	if connect_error.message != nil do fail("Temporal connection failed", &connect_error)
	defer temporal_client.close(&client)

	task_queue_buffer: [128]u8
	task_queue := os.get_env(task_queue_buffer[:], "TEMPORAL_ODIN_TASK_QUEUE")
	if len(task_queue) == 0 do task_queue = "odin-standalone-activity"
	mode_buffer: [32]u8
	mode := os.get_env(mode_buffer[:], "STANDALONE_ACTIVITY_MODE")
	activity_id := fmt.aprintf("odin-standalone-activity-%d", time.now()._nsec)
	defer delete(activity_id)

	handle, start_error := temporal_client.start_standalone_activity_json(
		&client,
		"Standalone Odin",
		{
			id = activity_id,
			activity_type = "hello",
			task_queue = task_queue,
			schedule_to_close_timeout = time.Minute,
			start_to_close_timeout = 10 * time.Second,
			id_reuse_policy = .Reject_Duplicate,
			id_conflict_policy = .Fail,
		},
	)
	if start_error.message != nil do fail("Standalone Activity start failed", &start_error)
	defer temporal_client.standalone_activity_handle_free(&handle)
	if !handle.started_by_request || len(handle.run_id) == 0 {
		fail("Temporal returned an invalid standalone Activity handle")
	}
	if mode == "cancel" || mode == "terminate" {
		mutation_error: temporal_client.Error
		if mode == "cancel" {
			mutation_error = temporal_client.cancel_standalone_activity(&handle, "Odin lifecycle cancellation")
		} else {
			mutation_error = temporal_client.terminate_standalone_activity(&handle, "Odin lifecycle termination")
		}
		if mutation_error.message != nil do fail("Standalone Activity mutation failed", &mutation_error)
		terminal, terminal_error := temporal_client.poll_standalone_activity(
			&handle,
			{timeout = 10 * time.Second},
		)
		temporal_client.standalone_activity_poll_result_free(&terminal)
		expected_kind := temporal_client.Error_Kind.Standalone_Activity_Terminated
		if mode == "cancel" do expected_kind = .Standalone_Activity_Canceled
		if terminal_error.kind != expected_kind {
			fail("Standalone Activity reached an unexpected terminal state", &terminal_error)
		}
		temporal_client.error_free(&terminal_error)
		delete_error := temporal_client.delete_standalone_activity(&handle)
		if delete_error.message != nil do fail("Standalone Activity deletion failed", &delete_error)
		terminal_name := "terminated"
		if mode == "cancel" do terminal_name = "canceled"
		fmt.printf(
			"Standalone Activity %s/%s was %s and deleted through the Odin Client.\n",
			handle.id,
			handle.run_id,
			terminal_name,
		)
		return
	}

	result: string
	result_error := temporal_client.get_standalone_activity_result_json(
		&handle,
		&result,
		{timeout = 10 * time.Second},
	)
	if result_error.message != nil do fail("Standalone Activity result failed", &result_error)
	if result != "Hello Standalone Odin" {
		fail("Standalone Activity returned an unexpected result")
	}
	fmt.printf(
		"Standalone Activity %s/%s completed through the Odin Client: %s\n",
		handle.id,
		handle.run_id,
		result,
	)
}

package main

import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"
import standalone_activity "../../temporal/client/standalone_activity"

fail :: proc(message: string, error: ^temporal_client.Error = nil) {
	if error != nil && error.message != nil {
		fmt.eprintf("%s: %s (status=%d)\n", message, string(error.message), error.status_code)
		temporal_client.error_free(error)
	} else {
		fmt.eprintln(message)
	}
	os.exit(1)
}

verify_visibility :: proc(client: ^temporal_client.Client, activity_id: string) {
	query := fmt.aprintf("ActivityId = '%s'", activity_id)
	defer delete(query)
	for _ in 0 ..< 50 {
		page, list_error := standalone_activity.list_standalone_activities(
			client,
			{page_size = 100, query = query},
		)
		if list_error.message != nil do fail("Standalone Activity list failed", &list_error)
		found := false
		for execution in page.activities {
			if execution.activity_id == activity_id {
				found = true
				break
			}
		}
		standalone_activity.standalone_activity_list_page_free(&page)

		count, count_error := standalone_activity.count_standalone_activities(client, query)
		if count_error.message != nil do fail("Standalone Activity count failed", &count_error)
		visible := found && count.count >= 1
		standalone_activity.standalone_activity_count_free(&count)
		if visible do return
		time.sleep(100 * time.Millisecond)
	}
	fail("Standalone Activity did not become visible through List/Count")
}

wait_for_status :: proc(
	handle: ^standalone_activity.Standalone_Activity_Handle,
	status: standalone_activity.Standalone_Activity_Status,
) {
	for _ in 0 ..< 50 {
		description, describe_error := standalone_activity.describe_standalone_activity(handle)
		if describe_error.message != nil do fail("Standalone Activity status Describe failed", &describe_error)
		matches := description.status == status
		standalone_activity.standalone_activity_description_free(&description)
		if matches do return
		time.sleep(100 * time.Millisecond)
	}
	fail("Standalone Activity did not reach the expected status")
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

	handle, start_error := standalone_activity.start_standalone_activity_json(
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
	defer standalone_activity.standalone_activity_handle_free(&handle)
	if !handle.started_by_request || len(handle.run_id) == 0 {
		fail("Temporal returned an invalid standalone Activity handle")
	}
	description, describe_error := standalone_activity.describe_standalone_activity(
		&handle,
		{include_input = true, include_outcome = true},
	)
	if describe_error.message != nil do fail("Standalone Activity Describe failed", &describe_error)
	if description.activity_id != handle.id || description.run_id != handle.run_id ||
	   description.activity_type != "hello" || description.task_queue != task_queue ||
	   len(description.input) != 1 {
		standalone_activity.standalone_activity_description_free(&description)
		fail("Temporal returned an invalid standalone Activity description")
	}
	standalone_activity.standalone_activity_description_free(&description)
	if mode == "control" {
		updated_task_queue := fmt.aprintf("%s-updated", task_queue)
		defer delete(updated_task_queue)
		update_paths := [?]string{
			standalone_activity.Standalone_Activity_Option_Path_Task_Queue_Name,
			standalone_activity.Standalone_Activity_Option_Path_Start_To_Close_Timeout,
			standalone_activity.Standalone_Activity_Option_Path_Priority,
		}
		updated_priority := temporal_client.Priority{priority_key = 7, fairness_key = "odin-control"}
		updated_options, update_error := standalone_activity.update_standalone_activity_options(
			&handle,
			{
				update_mask = update_paths[:],
				task_queue = updated_task_queue,
				start_to_close_timeout = 20 * time.Second,
				priority = &updated_priority,
			},
		)
		if update_error.message != nil do fail("Standalone Activity options update failed", &update_error)
		if updated_options.task_queue != updated_task_queue ||
		   !updated_options.start_to_close_timeout.present ||
		   updated_options.start_to_close_timeout.value != 20 * time.Second ||
		   !updated_options.priority.present || updated_options.priority.priority_key != 7 {
			standalone_activity.standalone_activity_options_free(&updated_options)
			fail("Standalone Activity options update returned unexpected values")
		}
		standalone_activity.standalone_activity_options_free(&updated_options)
		updated_description, updated_describe_error := standalone_activity.describe_standalone_activity(&handle)
		if updated_describe_error.message != nil {
			fail("Updated standalone Activity Describe failed", &updated_describe_error)
		}
		if updated_description.task_queue != updated_task_queue ||
		   !updated_description.start_to_close_timeout.present ||
		   updated_description.start_to_close_timeout.value != 20 * time.Second {
			standalone_activity.standalone_activity_description_free(&updated_description)
			fail("Describe did not observe updated standalone Activity options")
		}
		standalone_activity.standalone_activity_description_free(&updated_description)
		pause_error := standalone_activity.pause_standalone_activity(
			&handle,
			{reason = "Odin control integration pause"},
		)
		if pause_error.message != nil do fail("Standalone Activity pause failed", &pause_error)
		wait_for_status(&handle, .Paused)
		reset_error := standalone_activity.reset_standalone_activity(
			&handle,
			{keep_paused = true, reset_heartbeat = true},
		)
		if reset_error.message != nil do fail("Standalone Activity reset failed", &reset_error)
		wait_for_status(&handle, .Paused)
		unpause_error := standalone_activity.unpause_standalone_activity(
			&handle,
			{reason = "Odin control integration resume"},
		)
		if unpause_error.message != nil do fail("Standalone Activity unpause failed", &unpause_error)
		wait_for_status(&handle, .Running)
		terminate_error := standalone_activity.terminate_standalone_activity(
			&handle,
			"Odin control integration cleanup",
		)
		if terminate_error.message != nil do fail("Standalone Activity control cleanup failed", &terminate_error)
		terminal, terminal_error := standalone_activity.poll_standalone_activity(
			&handle,
			{timeout = 10 * time.Second},
		)
		standalone_activity.standalone_activity_poll_result_free(&terminal)
		if terminal_error.kind != .Standalone_Activity_Terminated {
			fail("Standalone Activity control cleanup reached an unexpected state", &terminal_error)
		}
		temporal_client.error_free(&terminal_error)
		delete_error := standalone_activity.delete_standalone_activity(&handle)
		if delete_error.message != nil do fail("Standalone Activity control deletion failed", &delete_error)
		fmt.printf(
			"Standalone Activity %s/%s was updated, paused, reset, unpaused, terminated, and deleted through the Odin Client.\n",
			handle.id,
			handle.run_id,
		)
		return
	}
	if mode == "cancel" || mode == "terminate" {
		mutation_error: temporal_client.Error
		if mode == "cancel" {
			mutation_error = standalone_activity.cancel_standalone_activity(&handle, "Odin lifecycle cancellation")
		} else {
			mutation_error = standalone_activity.terminate_standalone_activity(&handle, "Odin lifecycle termination")
		}
		if mutation_error.message != nil do fail("Standalone Activity mutation failed", &mutation_error)
		terminal, terminal_error := standalone_activity.poll_standalone_activity(
			&handle,
			{timeout = 10 * time.Second},
		)
		standalone_activity.standalone_activity_poll_result_free(&terminal)
		expected_kind := temporal_client.Error_Kind.Standalone_Activity_Terminated
		if mode == "cancel" do expected_kind = .Standalone_Activity_Canceled
		if terminal_error.kind != expected_kind {
			fail("Standalone Activity reached an unexpected terminal state", &terminal_error)
		}
		temporal_client.error_free(&terminal_error)
		delete_error := standalone_activity.delete_standalone_activity(&handle)
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
	result_error := standalone_activity.get_standalone_activity_result_json(
		&handle,
		&result,
		{timeout = 10 * time.Second},
	)
	if result_error.message != nil do fail("Standalone Activity result failed", &result_error)
	if result != "Hello Standalone Odin" {
		fail("Standalone Activity returned an unexpected result")
	}
	completed, completed_error := standalone_activity.describe_standalone_activity(
		&handle,
		{include_input = true, include_outcome = true},
	)
	if completed_error.message != nil {
		fail("Completed standalone Activity Describe failed", &completed_error)
	}
	if completed.status != .Completed || len(completed.input) != 1 || len(completed.result) != 1 {
		standalone_activity.standalone_activity_description_free(&completed)
		fail("Completed standalone Activity description did not contain its outcome")
	}
	standalone_activity.standalone_activity_description_free(&completed)
	verify_visibility(&client, handle.id)
	delete_error := standalone_activity.delete_standalone_activity(&handle)
	if delete_error.message != nil do fail("Standalone Activity deletion failed", &delete_error)
	fmt.printf(
		"Standalone Activity %s/%s completed, described, listed, counted, and deleted through the Odin Client: %s\n",
		handle.id,
		handle.run_id,
		result,
	)
}

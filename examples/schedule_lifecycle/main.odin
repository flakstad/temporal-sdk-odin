package main

import "core:fmt"
import "core:os"
import "core:strings"
import "core:time"
import temporal_client "../../temporal/client"

fail_error :: proc(label: string, error: ^temporal_client.Error) -> int {
	fmt.eprintf("%s (status=%d): %s\n", label, error.status_code, string(error.message))
	temporal_client.error_free(error)
	return 1
}

run :: proc() -> int {
	client, connect_error := temporal_client.connect()
	if connect_error.message != nil {
		return fail_error("Schedule example connection failed", &connect_error)
	}
	defer temporal_client.close(&client)

	env_buffer: [128]u8
	schedule_id := os.get_env(env_buffer[:], "TEMPORAL_SCHEDULE_ID")
	if len(schedule_id) == 0 {
		schedule_id = "odin-sdk-schedule-example"
	}
	workflow_id_buffer: [160]u8
	workflow_id := fmt.bprintf(workflow_id_buffer[:], "%s-workflow", schedule_id)
	intervals := [?]temporal_client.Schedule_Interval{{every = 24 * time.Hour}}
	handle, create_error := temporal_client.create_schedule_json(
		&client,
		schedule_id,
		"odin_patch_workflow",
		"Scheduled",
		{
			spec = {intervals = intervals[:]},
			action = {
				workflow_id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
			overlap_policy = .Skip,
			catchup_window = time.Minute,
			paused = true,
			notes = "created paused by Odin",
		},
	)
	if create_error.message != nil {
		return fail_error("Create Schedule failed", &create_error)
	}
	defer temporal_client.schedule_handle_free(&handle)
	deleted := false
	defer if !deleted {
		delete_error := temporal_client.delete_schedule(&handle)
		temporal_client.error_free(&delete_error)
	}

	description, describe_error := temporal_client.describe_schedule(&handle)
	if describe_error.message != nil {
		return fail_error("Describe Schedule failed", &describe_error)
	}
	if description.schedule_id != schedule_id || !description.paused ||
	   description.action.workflow_type != "odin_patch_workflow" ||
	   len(description.intervals) != 1 || description.intervals[0].every != 24 * time.Hour {
		fmt.eprintln("Describe Schedule returned unexpected state")
		temporal_client.schedule_description_free(&description)
		return 1
	}
	temporal_client.schedule_description_free(&description)

	list_page, list_error := temporal_client.list_schedules(&client, {maximum_page_size = 100})
	if list_error.message != nil {
		return fail_error("List Schedules failed", &list_error)
	}
	listed := false
	for entry in list_page.schedules {
		if entry.schedule_id == schedule_id && entry.workflow_type == "odin_patch_workflow" {
			listed = true
			break
		}
	}
	temporal_client.schedule_list_page_free(&list_page)
	if !listed {
		fmt.eprintln("created Schedule was absent from List Schedules")
		return 1
	}
	count, count_error := temporal_client.count_schedules(&client)
	count_supported := count_error.message == nil
	if count_error.message != nil && count_error.status_code != 12 {
		return fail_error("Count Schedules failed", &count_error)
	}
	if count_error.message != nil {
		temporal_client.error_free(&count_error)
	}
	if count_supported && count.count < 1 {
		fmt.eprintln("Count Schedules did not include the created Schedule")
		temporal_client.schedule_count_free(&count)
		return 1
	}
	schedule_count := count.count
	if !count_supported {
		schedule_count = -1
	}
	temporal_client.schedule_count_free(&count)

	unpause_error := temporal_client.unpause_schedule(&handle, "unpaused by Odin lifecycle test")
	if unpause_error.message != nil {
		return fail_error("Unpause Schedule failed", &unpause_error)
	}
	description, describe_error = temporal_client.describe_schedule(&handle)
	if describe_error.message != nil {
		return fail_error("Describe unpaused Schedule failed", &describe_error)
	}
	if description.paused {
		fmt.eprintln("Schedule remained paused after unpause")
		temporal_client.schedule_description_free(&description)
		return 1
	}
	temporal_client.schedule_description_free(&description)
	pause_error := temporal_client.pause_schedule(&handle, "paused before manual trigger")
	if pause_error.message != nil {
		return fail_error("Pause Schedule failed", &pause_error)
	}
	trigger_error := temporal_client.trigger_schedule(&handle)
	if trigger_error.message != nil {
		return fail_error("Trigger Schedule failed", &trigger_error)
	}

	started_workflow_id: string
	started_run_id: string
	defer delete(started_workflow_id)
	defer delete(started_run_id)
	for _ in 0 ..< 100 {
		description, describe_error = temporal_client.describe_schedule(&handle)
		if describe_error.message != nil {
			return fail_error("Describe triggered Schedule failed", &describe_error)
		}
		if len(description.recent_actions) > 0 {
			recent := description.recent_actions[len(description.recent_actions) - 1]
			if len(recent.workflow_id) > 0 && len(recent.run_id) > 0 {
				workflow_id_copy, workflow_id_error := strings.clone(recent.workflow_id)
				run_id_copy, run_id_error := strings.clone(recent.run_id)
				if workflow_id_error == nil && run_id_error == nil {
					started_workflow_id = workflow_id_copy
					started_run_id = run_id_copy
				} else {
					delete(workflow_id_copy)
					delete(run_id_copy)
				}
			}
		}
		temporal_client.schedule_description_free(&description)
		if len(started_run_id) > 0 {
			break
		}
		time.sleep(100 * time.Millisecond)
	}
	if len(started_run_id) == 0 {
		fmt.eprintln("triggered Schedule did not report a Workflow execution")
		return 1
	}
	workflow_handle, workflow_handle_error := temporal_client.get_workflow_handle(
		&client,
		started_workflow_id,
		started_run_id,
	)
	if workflow_handle_error.message != nil {
		return fail_error("Scheduled Workflow handle failed", &workflow_handle_error)
	}
	defer temporal_client.workflow_handle_free(&workflow_handle)
	result: string
	defer delete(result)
	result_error := temporal_client.get_result_json(&workflow_handle, &result)
	if result_error.message != nil {
		return fail_error("Scheduled Workflow result failed", &result_error)
	}
	if !strings.has_prefix(result, "Patched Scheduled ") {
		fmt.eprintf("unexpected scheduled Workflow result: %q\n", result)
		return 1
	}

	delete_error := temporal_client.delete_schedule(&handle)
	if delete_error.message != nil {
		return fail_error("Delete Schedule failed", &delete_error)
	}
	deleted = true
	fmt.printf(
		"Odin Schedule lifecycle completed schedule_id=%s listed=%v count_supported=%v count=%d paused_unpaused=true workflow_id=%s result=%q deleted=true\n",
		schedule_id,
		listed,
		count_supported,
		schedule_count,
		started_workflow_id,
		result,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

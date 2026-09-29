package main

import "core:bytes"
import "core:fmt"
import "core:os"
import "core:strings"
import "core:time"
import temporal_client "../../temporal/client"

Schedule_Read_Interceptor_State :: struct {
	described: bool,
	listed:    bool,
	counted:   bool,
	matched:   bool,
}

describe_schedule_interceptor :: proc(user_data: rawptr, input: ^temporal_client.Client_Describe_Schedule_Input, next: temporal_client.Client_Describe_Schedule_Next) -> (temporal_client.Schedule_Description, temporal_client.Error) {
	state := cast(^Schedule_Read_Interceptor_State)user_data
	state.described = true
	return temporal_client.client_describe_schedule_next(next, input)
}

list_schedules_interceptor :: proc(user_data: rawptr, input: ^temporal_client.Client_List_Schedules_Input, next: temporal_client.Client_List_Schedules_Next) -> (temporal_client.Schedule_List_Page, temporal_client.Error) {
	state := cast(^Schedule_Read_Interceptor_State)user_data
	state.listed = true
	input.options.maximum_page_size = 50
	return temporal_client.client_list_schedules_next(next, input)
}

count_schedules_interceptor :: proc(user_data: rawptr, input: ^temporal_client.Client_Count_Schedules_Input, next: temporal_client.Client_Count_Schedules_Next) -> (temporal_client.Schedule_Count, temporal_client.Error) {
	state := cast(^Schedule_Read_Interceptor_State)user_data
	state.counted = true
	return temporal_client.client_count_schedules_next(next, input)
}

matching_times_interceptor :: proc(user_data: rawptr, input: ^temporal_client.Client_List_Schedule_Matching_Times_Input, next: temporal_client.Client_List_Schedule_Matching_Times_Next) -> (temporal_client.Schedule_Matching_Times, temporal_client.Error) {
	state := cast(^Schedule_Read_Interceptor_State)user_data
	state.matched = true
	return temporal_client.client_list_schedule_matching_times_next(next, input)
}

fail_error :: proc(label: string, error: ^temporal_client.Error) -> int {
	fmt.eprintf("%s (status=%d): %s\n", label, error.status_code, string(error.message))
	temporal_client.error_free(error)
	return 1
}

run :: proc() -> int {
	interceptor_state := Schedule_Read_Interceptor_State{}
	interceptors := [?]temporal_client.Client_Interceptor{{
		user_data = &interceptor_state,
		describe_schedule = describe_schedule_interceptor,
		list_schedules = list_schedules_interceptor,
		count_schedules = count_schedules_interceptor,
		list_schedule_matching_times = matching_times_interceptor,
	}}
	client, connect_error := temporal_client.connect_with_options({
		target_url = "http://localhost:7233",
		namespace = "default",
		interceptors = interceptors[:],
	})
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
	conflict_token := bytes.clone(description.conflict_token)
	defer delete(conflict_token)
	if len(conflict_token) == 0 {
		fmt.eprintln("Describe Schedule returned an empty conflict token")
		temporal_client.schedule_description_free(&description)
		return 1
	}
	temporal_client.schedule_description_free(&description)

	updated_intervals := [?]temporal_client.Schedule_Interval{{every = 12 * time.Hour}}
	update_options := temporal_client.Schedule_Create_Options{
		spec = {intervals = updated_intervals[:]},
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
		notes = "replaced by Odin with optimistic concurrency",
	}
	update_error := temporal_client.update_schedule_json(
		&handle,
		"odin_patch_workflow",
		"Scheduled",
		update_options,
		conflict_token,
	)
	if update_error.message != nil {
		return fail_error("Update Schedule failed", &update_error)
	}
	description, describe_error = temporal_client.describe_schedule(&handle)
	if describe_error.message != nil {
		return fail_error("Describe updated Schedule failed", &describe_error)
	}
	if len(description.intervals) != 1 || description.intervals[0].every != 12 * time.Hour ||
	   description.notes != "replaced by Odin with optimistic concurrency" {
		fmt.eprintln("Update Schedule did not replace the Schedule definition")
		temporal_client.schedule_description_free(&description)
		return 1
	}
	updated_conflict_token := bytes.clone(description.conflict_token)
	defer delete(updated_conflict_token)
	conflict_token_rotated := !bytes.equal(conflict_token, updated_conflict_token)
	temporal_client.schedule_description_free(&description)

	intervening_patch_error := temporal_client.unpause_schedule(
		&handle,
		"intervening mutation for optimistic concurrency test",
	)
	if intervening_patch_error.message != nil {
		return fail_error("Intervening Schedule patch failed", &intervening_patch_error)
	}
	stale_intervals := [?]temporal_client.Schedule_Interval{{every = 6 * time.Hour}}
	stale_options := update_options
	stale_options.spec = {intervals = stale_intervals[:]}
	stale_options.notes = "stale update must not apply"
	stale_error := temporal_client.update_schedule_json(
		&handle,
		"odin_patch_workflow",
		"Scheduled",
		stale_options,
		conflict_token,
	)
	stale_rpc_rejected := stale_error.message != nil
	temporal_client.error_free(&stale_error)
	description, describe_error = temporal_client.describe_schedule(&handle)
	if describe_error.message != nil {
		return fail_error("Describe Schedule after stale update failed", &describe_error)
	}
	stale_update_ignored := description.notes != "stale update must not apply" &&
	                       (len(description.intervals) != 1 ||
	                        description.intervals[0].every != 6 * time.Hour)
	temporal_client.schedule_description_free(&description)
	stale_conflict_guarded := stale_rpc_rejected || stale_update_ignored
	if !stale_conflict_guarded {
		fmt.eprintln("Temporal applied an update with a stale Schedule conflict token")
		return 1
	}
	matching_start := time.now()
	matching, matching_error := temporal_client.list_schedule_matching_times(
		&handle,
		matching_start,
		time.time_add(matching_start, 2 * 24 * time.Hour),
	)
	if matching_error.message != nil {
		return fail_error("List Schedule Matching Times failed", &matching_error)
	}
	matching_count := len(matching.times)
	temporal_client.schedule_matching_times_free(&matching)
	if matching_count < 3 {
		fmt.eprintf("expected at least three matching Schedule times, got %d\n", matching_count)
		return 1
	}

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
	backfill_end := time.time_add(time.now(), -time.Minute)
	backfill_start := time.time_add(backfill_end, -24 * time.Hour)
	backfills := [?]temporal_client.Schedule_Backfill{{
		start_time = backfill_start,
		end_time = backfill_end,
		overlap_policy = .Buffer_All,
	}}
	backfill_error := temporal_client.backfill_schedule(&handle, backfills[:])
	if backfill_error.message != nil {
		return fail_error("Backfill Schedule failed", &backfill_error)
	}
	backfill_action_count: i64
	for _ in 0 ..< 200 {
		description, describe_error = temporal_client.describe_schedule(&handle)
		if describe_error.message != nil {
			return fail_error("Describe backfilled Schedule failed", &describe_error)
		}
		backfill_action_count = description.action_count
		backfill_finished := backfill_action_count >= 2 && len(description.running_workflows) == 0
		temporal_client.schedule_description_free(&description)
		if backfill_finished {
			break
		}
		time.sleep(100 * time.Millisecond)
	}
	if backfill_action_count != 2 {
		fmt.eprintf("expected exactly two backfilled Schedule actions, got %d\n", backfill_action_count)
		return 1
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
		if description.action_count >= backfill_action_count + 1 && len(description.recent_actions) > 0 {
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
	if !interceptor_state.described || !interceptor_state.listed ||
	   !interceptor_state.counted || !interceptor_state.matched {
		fmt.eprintln("Schedule read interceptors did not observe every lifecycle operation")
		return 1
	}

	delete_error := temporal_client.delete_schedule(&handle)
	if delete_error.message != nil {
		return fail_error("Delete Schedule failed", &delete_error)
	}
	deleted = true
	fmt.printf(
		"Odin Schedule lifecycle completed schedule_id=%s updated=true conflict_token_rotated=%v stale_conflict_guarded=%v stale_rpc_rejected=%v matching_times=%d listed=%v count_supported=%v count=%d read_interceptors=true paused_unpaused=true backfill_actions=%d workflow_id=%s result=%q deleted=true\n",
		schedule_id,
		conflict_token_rotated,
		stale_conflict_guarded,
		stale_rpc_rejected,
		matching_count,
		listed,
		count_supported,
		schedule_count,
		backfill_action_count,
		started_workflow_id,
		result,
	)
	return 0
}

main :: proc() {
	os.exit(run())
}

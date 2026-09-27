package workflow_pbt_test

import "core:bytes"
import "core:mem/virtual"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"
import codec "../temporal/codec"
import proto "../temporal/core/proto"

schedule_create_request_property :: proc(t: ^pbt.T) -> pbt.Result {
	interval_seconds := pbt.draw(t, pbt.int_range(2, 86_400))
	zero_phase := pbt.draw(t, pbt.boolean())
	phase_seconds := 0
	if !zero_phase {
		phase_seconds = pbt.draw(t, pbt.int_range(1, interval_seconds - 1))
	}
	overlap_number := i32(pbt.draw(t, pbt.int_range(0, 6)))
	paused := pbt.draw(t, pbt.boolean())
	limited := pbt.draw(t, pbt.boolean())
	remaining := i64(0)
	if limited {
		exhausted := pbt.draw(t, pbt.boolean())
		if !exhausted {
			remaining = i64(pbt.draw(t, pbt.int_range(1, 1000)))
		}
	}
	trigger := pbt.draw(t, pbt.boolean())
	pbt.cover(t, phase_seconds == 0, 30, "zero phase")
	pbt.cover(t, overlap_number == i32(temporal_client.Schedule_Overlap_Policy.Allow_All), 10, "allow all")
	pbt.cover(t, paused, 30, "initially paused")
	pbt.cover(t, limited && remaining == 0, 15, "exhausted limited schedule")
	pbt.cover(t, trigger, 30, "initial trigger")

	input := codec.payload_create(transmute([]u8)string(`"property-input"`))
	defer codec.payload_free(&input)
	intervals := [?]temporal_client.Schedule_Interval{{
		every = time.Duration(interval_seconds) * time.Second,
		phase = time.Duration(phase_seconds) * time.Second,
	}}
	encoded, encode_error := temporal_client.encode_create_schedule_request(
		"property-namespace",
		"property-schedule",
		"property_workflow",
		codec.payload_view(&input),
		{
			spec = {intervals = intervals[:]},
			action = {workflow_id = "property-workflow", task_queue = "property-queue"},
			overlap_policy = temporal_client.Schedule_Overlap_Policy(overlap_number),
			paused = paused,
			limited_actions = limited,
			remaining_actions = remaining,
			trigger_immediately = trigger,
			request_id = "property-request",
		},
	)
	defer delete(encoded)
	defer temporal_client.error_free(&encode_error)
	if encode_error.message != nil {
		return pbt.fail("valid generated Schedule request was rejected")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize Schedule property arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	request := proto.Temporal_Api_Workflowservice_V1_CreateScheduleRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_CreateScheduleRequest(encoded[:], &request)
	context.allocator = previous_allocator
	if !decoded || request.schedule == nil || request.schedule.spec == nil ||
	   request.schedule.action == nil || request.schedule.policies == nil ||
	   request.schedule.state == nil {
		return pbt.fail("generated Schedule request did not decode")
	}
	if len(request.schedule.spec.interval) != 1 ||
	   request.schedule.spec.interval[0].interval.seconds != i64(interval_seconds) ||
	   request.schedule.spec.interval[0].phase != nil &&
	   request.schedule.spec.interval[0].phase.seconds != i64(phase_seconds) ||
	   request.schedule.state.paused != paused ||
	   request.schedule.state.limited_actions != limited ||
	   request.schedule.state.remaining_actions != remaining ||
	   i32(request.schedule.policies.overlap_policy) != overlap_number ||
	   (request.initial_patch != nil) != trigger {
		return pbt.fail("generated Schedule fields changed during round trip")
	}
	return pbt.pass()
}

@(test)
schedule_create_requests_preserve_generated_contract :: proc(t: ^testing.T) {
	result := pbt.check(
		"Schedule create requests preserve generated interval and policy contract",
		schedule_create_request_property,
		{num_tests = 500, seed = 0x5343_4845_4455_4c45},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

schedule_update_calendar_property :: proc(t: ^pbt.T) -> pbt.Result {
	hour_start := i32(pbt.draw(t, pbt.int_range(0, 23)))
	hour_end := i32(pbt.draw(t, pbt.int_range(int(hour_start), 23)))
	minute_start := i32(pbt.draw(t, pbt.int_range(0, 59)))
	minute_end := i32(pbt.draw(t, pbt.int_range(int(minute_start), 59)))
	step := i32(pbt.draw(t, pbt.int_range(0, 12)))
	pbt.cover(t, step == 0, 5, "default calendar step")
	pbt.cover(t, hour_start == hour_end, 5, "single-hour range")
	hours := [?]temporal_client.Schedule_Range{{start = hour_start, end = hour_end, step = step}}
	minutes := [?]temporal_client.Schedule_Range{{start = minute_start, end = minute_end}}
	calendars := [?]temporal_client.Schedule_Calendar{{hour = hours[:], minute = minutes[:]}}
	input := codec.payload_create(transmute([]u8)string(`"property-calendar"`))
	defer codec.payload_free(&input)
	conflict_token := transmute([]u8)string("property-conflict")
	encoded, encode_error := temporal_client.encode_update_schedule_request(
		"property-namespace",
		"property-calendar-schedule",
		"property_workflow",
		codec.payload_view(&input),
		{
			spec = {structured_calendars = calendars[:]},
			action = {workflow_id = "property-workflow", task_queue = "property-queue"},
			request_id = "property-update-request",
		},
		conflict_token,
	)
	defer delete(encoded)
	defer temporal_client.error_free(&encode_error)
	if encode_error.message != nil {
		return pbt.fail("valid generated Schedule calendar update was rejected")
	}
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return pbt.error("could not initialize Schedule update property arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	request := proto.Temporal_Api_Workflowservice_V1_UpdateScheduleRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_UpdateScheduleRequest(encoded[:], &request)
	context.allocator = previous_allocator
	if !decoded || request.schedule == nil || request.schedule.spec == nil ||
	   len(request.schedule.spec.structured_calendar) != 1 || request.memo != nil ||
	   request.search_attributes != nil {
		return pbt.fail("generated Schedule calendar update did not decode")
	}
	calendar := request.schedule.spec.structured_calendar[0]
	if len(calendar.hour) != 1 || len(calendar.minute) != 1 ||
	   calendar.hour[0].start != hour_start || calendar.hour[0].end != hour_end ||
	   calendar.hour[0].step != step || calendar.minute[0].start != minute_start ||
	   calendar.minute[0].end != minute_end || !bytes.equal(request.conflict_token, conflict_token) {
		return pbt.fail("generated Schedule calendar fields changed during update round trip")
	}
	return pbt.pass()
}

@(test)
schedule_calendar_updates_preserve_generated_contract :: proc(t: ^testing.T) {
	result := pbt.check(
		"Schedule updates preserve generated structured calendars and conflict tokens",
		schedule_update_calendar_property,
		{num_tests = 500, seed = 0x5343_4845_4455_5042},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

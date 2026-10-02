package schedule

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:encoding/uuid"
import "core:mem"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import client ".."
import codec "../../codec"
import temporal_core "../../core"
import proto "../../core/proto"

Client :: client.Client
Error :: client.Error
Named_Payload :: client.Named_Payload
Optional_Duration :: client.Optional_Duration
Optional_Time :: client.Optional_Time
Priority :: client.Priority
Retry_Policy :: client.Retry_Policy
Start_Metadata :: client.Start_Metadata
Start_Workflow_Options :: client.Start_Workflow_Options
Workflow_Priority :: client.Workflow_Priority
Workflow_Status :: client.Workflow_Status

DEFAULT_CLIENT_IDENTITY :: client.DEFAULT_CLIENT_IDENTITY

client_request_identity :: client.client_request_identity
description_clone_payload :: client.description_clone_payload
description_clone_named_payload :: client.description_clone_named_payload
description_clone_string :: client.description_clone_string
description_duration :: client.description_duration
description_priority :: client.description_priority
description_time :: client.description_time
duration_proto :: client.duration_proto
error_from_core :: client.error_from_core
error_from_string :: client.error_from_string
free_start_proto_payloads :: client.free_start_proto_payloads
request_identity :: client.request_identity
resolve_request_id :: client.resolve_request_id
start_workflow_options_error :: client.start_workflow_options_error

Schedule_Overlap_Policy :: enum i32 {
	Unspecified = 0,
	Skip = 1,
	Buffer_One = 2,
	Buffer_All = 3,
	Cancel_Other = 4,
	Terminate_Other = 5,
	Allow_All = 6,
}

Schedule_Interval :: struct {
	every: time.Duration,
	phase: time.Duration,
}

Schedule_Range :: struct {
	start: i32,
	end:   i32,
	step:  i32,
}

// Empty calendar fields use Temporal's high-level defaults: second/minute/hour
// are zero, day_of_month/month/day_of_week match their full domains, and an
// empty year matches every year.
Schedule_Calendar :: struct {
	second:       []Schedule_Range,
	minute:       []Schedule_Range,
	hour:         []Schedule_Range,
	day_of_month: []Schedule_Range,
	month:        []Schedule_Range,
	year:         []Schedule_Range,
	day_of_week:  []Schedule_Range,
	comment:      string,
}

Schedule_Calendar_Snapshot :: struct {
	second:       [dynamic]Schedule_Range,
	minute:       [dynamic]Schedule_Range,
	hour:         [dynamic]Schedule_Range,
	day_of_month: [dynamic]Schedule_Range,
	month:        [dynamic]Schedule_Range,
	year:         [dynamic]Schedule_Range,
	day_of_week:  [dynamic]Schedule_Range,
	comment:      string,
}

Schedule_Spec :: struct {
	structured_calendars: []Schedule_Calendar,
	cron_expressions:     []string,
	intervals:            []Schedule_Interval,
	excluded_calendars:   []Schedule_Calendar,
	start_time:           Optional_Time,
	end_time:             Optional_Time,
	jitter:               time.Duration,
	timezone_name:        string,
	timezone_data:        []u8,
}

Schedule_Action_Options :: struct {
	workflow_id:               string,
	task_queue:                string,
	workflow_execution_timeout: time.Duration,
	workflow_run_timeout:       time.Duration,
	workflow_task_timeout:      time.Duration,
	retry_policy:               ^Retry_Policy,
	priority:                   ^Priority,
	metadata:                   ^Start_Metadata,
}

Schedule_Create_Options :: struct {
	spec:                      Schedule_Spec,
	action:                    Schedule_Action_Options,
	overlap_policy:            Schedule_Overlap_Policy,
	catchup_window:            time.Duration,
	pause_on_failure:          bool,
	keep_original_workflow_id: bool,
	paused:                    bool,
	notes:                     string,
	limited_actions:           bool,
	remaining_actions:         i64,
	trigger_immediately:       bool,
	trigger_overlap_policy:    Schedule_Overlap_Policy,
	metadata:                  ^Start_Metadata,
	request_id:                string,
}

Schedule_Handle :: struct {
	client:         ^Client,
	id:             string,
	conflict_token: []u8,
}

Schedule_Action_Snapshot :: struct {
	workflow_id:               string,
	workflow_type:             string,
	task_queue:                string,
	inputs:                    [dynamic]codec.Payload,
	workflow_execution_timeout: Optional_Duration,
	workflow_run_timeout:       Optional_Duration,
	workflow_task_timeout:      Optional_Duration,
	priority:                   Workflow_Priority,
}

Schedule_Action_Result :: struct {
	schedule_time: Optional_Time,
	actual_time:   Optional_Time,
	workflow_id:   string,
	run_id:        string,
	status:        Workflow_Status,
}

Schedule_Description :: struct {
	schedule_id:                string,
	structured_calendars:       [dynamic]Schedule_Calendar_Snapshot,
	cron_expressions:           [dynamic]string,
	intervals:                  [dynamic]Schedule_Interval,
	excluded_calendars:         [dynamic]Schedule_Calendar_Snapshot,
	start_time:                 Optional_Time,
	end_time:                   Optional_Time,
	jitter:                     Optional_Duration,
	timezone_name:              string,
	timezone_data:              []u8,
	action:                     Schedule_Action_Snapshot,
	overlap_policy:             Schedule_Overlap_Policy,
	catchup_window:             Optional_Duration,
	pause_on_failure:           bool,
	keep_original_workflow_id:  bool,
	notes:                      string,
	paused:                     bool,
	limited_actions:            bool,
	remaining_actions:          i64,
	action_count:               i64,
	missed_catchup_window:      i64,
	overlap_skipped:            i64,
	buffer_dropped:             i64,
	buffer_size:                i64,
	state_size_bytes:           i64,
	create_time:                Optional_Time,
	update_time:                Optional_Time,
	running_workflows:          [dynamic]Workflow_Execution_Identity,
	recent_actions:             [dynamic]Schedule_Action_Result,
	future_action_times:        [dynamic]time.Time,
	memo:                       [dynamic]Named_Payload,
	search_attributes:          [dynamic]Named_Payload,
	conflict_token:             []u8,
	allocator:                  mem.Allocator,
}

Workflow_Execution_Identity :: struct {
	workflow_id: string,
	run_id:      string,
}

List_Schedules_Options :: struct {
	maximum_page_size: i32,
	next_page_token:   []u8,
	query:             string,
}

Schedule_List_Entry :: struct {
	schedule_id:         string,
	workflow_type:       string,
	notes:               string,
	paused:              bool,
	state_size_bytes:    i64,
	recent_actions:      [dynamic]Schedule_Action_Result,
	future_action_times: [dynamic]time.Time,
	memo:                [dynamic]Named_Payload,
	search_attributes:   [dynamic]Named_Payload,
}

Schedule_List_Page :: struct {
	schedules:       [dynamic]Schedule_List_Entry,
	next_page_token: []u8,
	allocator:       mem.Allocator,
}

Schedule_Count_Group :: struct {
	values: [dynamic]codec.Payload,
	count:  i64,
}

Schedule_Count :: struct {
	count:     i64,
	groups:    [dynamic]Schedule_Count_Group,
	allocator: mem.Allocator,
}

Schedule_Backfill :: struct {
	// The pinned API documents (start, end], while newer scheduler workflow
	// versions make the start inclusive. Use a start strictly before the first
	// desired matching time when behavior must be portable across servers.
	start_time:     time.Time,
	end_time:       time.Time,
	overlap_policy: Schedule_Overlap_Policy,
}

Schedule_Matching_Times :: struct {
	times:     [dynamic]time.Time,
	allocator: mem.Allocator,
}

schedule_handle_free :: proc(handle: ^Schedule_Handle) {
	if handle == nil {
		return
	}
	heap := allocator_runtime.heap_allocator()
	delete(handle.id, heap)
	delete(handle.conflict_token, heap)
	handle^ = {}
}

get_schedule_handle :: proc(client: ^Client, schedule_id: string) -> (Schedule_Handle, Error) {
	if client == nil || client.connection == nil || client.runtime == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if len(schedule_id) == 0 {
		return {}, error_from_string(.Invalid_Argument, "Schedule ID must not be empty")
	}
	heap := allocator_runtime.heap_allocator()
	owned_id, allocation_error := strings.clone(schedule_id, heap)
	if allocation_error != nil {
		return {}, error_from_string(.Malformed_Response, "could not allocate Schedule handle")
	}
	return {client = client, id = owned_id}, {}
}

schedule_handle_valid :: proc(handle: ^Schedule_Handle) -> bool {
	return handle != nil && handle.client != nil && handle.client.connection != nil &&
	       handle.client.runtime != nil && len(handle.id) > 0
}

schedule_timestamp_proto :: proc(value: time.Time) -> proto.Google_Protobuf_Timestamp {
	nanoseconds := time.to_unix_nanoseconds(value)
	seconds := nanoseconds / i64(time.Second)
	nanos := i32(nanoseconds % i64(time.Second))
	if nanos < 0 {
		seconds -= 1
		nanos += i32(time.Second)
	}
	return {seconds = seconds, nanos = nanos}
}

schedule_overlap_to_proto :: proc(
	value: Schedule_Overlap_Policy,
) -> proto.Temporal_Api_Enums_V1_ScheduleOverlapPolicy {
	return proto.Temporal_Api_Enums_V1_ScheduleOverlapPolicy(value)
}

schedule_range_error :: proc(ranges: []Schedule_Range, minimum, maximum: i32) -> string {
	for value in ranges {
		if value.start < minimum || value.start > maximum {
			return "Schedule calendar range start is outside the field domain"
		}
		if value.end != 0 && (value.end < minimum || value.end > maximum) {
			return "Schedule calendar range end is outside the field domain"
		}
		if value.step < 0 {
			return "Schedule calendar range step must not be negative"
		}
	}
	return ""
}

schedule_calendar_error :: proc(calendar: Schedule_Calendar) -> string {
	checks := []struct { ranges: []Schedule_Range, minimum, maximum: i32 }{
		{calendar.second, 0, 59},
		{calendar.minute, 0, 59},
		{calendar.hour, 0, 23},
		{calendar.day_of_month, 1, 31},
		{calendar.month, 1, 12},
		{calendar.year, 0, 9999},
		{calendar.day_of_week, 0, 6},
	}
	for check in checks {
		if message := schedule_range_error(check.ranges, check.minimum, check.maximum); len(message) > 0 {
			return message
		}
	}
	return ""
}

schedule_append_ranges :: proc(
	destination: ^[dynamic]proto.Temporal_Api_Schedule_V1_Range,
	source: []Schedule_Range,
	default_start, default_end: i32,
) {
	if len(source) == 0 {
		append(destination, proto.Temporal_Api_Schedule_V1_Range{
			start = default_start,
			end = default_end,
		})
		return
	}
	for value in source {
		append(destination, proto.Temporal_Api_Schedule_V1_Range{
			start = value.start,
			end = value.end,
			step = value.step,
		})
	}
}

schedule_calendar_proto :: proc(value: Schedule_Calendar) -> proto.Temporal_Api_Schedule_V1_StructuredCalendarSpec {
	result := proto.Temporal_Api_Schedule_V1_StructuredCalendarSpec{comment = value.comment}
	schedule_append_ranges(&result.second, value.second, 0, 0)
	schedule_append_ranges(&result.minute, value.minute, 0, 0)
	schedule_append_ranges(&result.hour, value.hour, 0, 0)
	schedule_append_ranges(&result.day_of_month, value.day_of_month, 1, 31)
	schedule_append_ranges(&result.month, value.month, 1, 12)
	for year in value.year {
		append(&result.year, proto.Temporal_Api_Schedule_V1_Range{
			start = year.start,
			end = year.end,
			step = year.step,
		})
	}
	schedule_append_ranges(&result.day_of_week, value.day_of_week, 0, 6)
	return result
}

schedule_calendar_proto_free :: proc(value: ^proto.Temporal_Api_Schedule_V1_StructuredCalendarSpec) {
	delete(value.second)
	delete(value.minute)
	delete(value.hour)
	delete(value.day_of_month)
	delete(value.month)
	delete(value.year)
	delete(value.day_of_week)
	value^ = {}
}

schedule_create_options_error :: proc(
	schedule_id: string,
	workflow_type: string,
	options: Schedule_Create_Options,
) -> string {
	if len(schedule_id) == 0 {
		return "Schedule ID must not be empty"
	}
	if len(workflow_type) == 0 {
		return "scheduled Workflow type must not be empty"
	}
	if len(options.action.workflow_id) == 0 {
		return "scheduled Workflow ID must not be empty"
	}
	if len(options.action.task_queue) == 0 {
		return "scheduled Workflow task queue must not be empty"
	}
	if len(options.spec.structured_calendars) == 0 && len(options.spec.cron_expressions) == 0 &&
	   len(options.spec.intervals) == 0 {
		return "Schedule must contain at least one calendar, cron expression, or interval"
	}
	for calendar in options.spec.structured_calendars {
		if message := schedule_calendar_error(calendar); len(message) > 0 { return message }
	}
	for calendar in options.spec.excluded_calendars {
		if message := schedule_calendar_error(calendar); len(message) > 0 { return message }
	}
	for interval in options.spec.intervals {
		if interval.every <= 0 {
			return "Schedule interval must be positive"
		}
		if interval.phase < 0 || interval.phase >= interval.every {
			return "Schedule interval phase must be non-negative and shorter than the interval"
		}
	}
	if options.spec.jitter < 0 {
		return "Schedule jitter must not be negative"
	}
	if options.spec.start_time.present && options.spec.end_time.present &&
	   time.to_unix_nanoseconds(options.spec.start_time.value) >
	   time.to_unix_nanoseconds(options.spec.end_time.value) {
		return "Schedule start time must not be after end time"
	}
	if options.catchup_window < 0 ||
	   (options.catchup_window > 0 && options.catchup_window < 10 * time.Second) {
		return "Schedule catchup window must be zero or at least ten seconds"
	}
	if options.limited_actions && options.remaining_actions < 0 {
		return "Schedule remaining actions must not be negative"
	}
	if !options.limited_actions && options.remaining_actions != 0 {
		return "Schedule remaining actions require limited_actions"
	}
	start_options := Start_Workflow_Options{
		id = options.action.workflow_id,
		task_queue = options.action.task_queue,
		workflow_execution_timeout = options.action.workflow_execution_timeout,
		workflow_run_timeout = options.action.workflow_run_timeout,
		workflow_task_timeout = options.action.workflow_task_timeout,
		retry_policy = options.action.retry_policy,
		priority = options.action.priority,
		metadata = options.action.metadata,
	}
	if message := start_workflow_options_error(start_options); len(message) > 0 {
		return message
	}
	if options.metadata != nil && options.metadata.allocator.procedure == nil {
		return "Schedule metadata is not initialized"
	}
	return ""
}

Schedule_Metadata_Proto :: struct {
	header:          proto.Temporal_Api_Common_V1_Header,
	memo:            proto.Temporal_Api_Common_V1_Memo,
	search:          proto.Temporal_Api_Common_V1_SearchAttributes,
	header_payloads: []proto.Temporal_Api_Common_V1_Payload,
	memo_payloads:   []proto.Temporal_Api_Common_V1_Payload,
	search_payloads: []proto.Temporal_Api_Common_V1_Payload,
}

schedule_metadata_proto :: proc(metadata: ^Start_Metadata) -> Schedule_Metadata_Proto {
	result := Schedule_Metadata_Proto{}
	if metadata == nil {
		return result
	}
	if len(metadata.headers) > 0 {
		result.header_payloads = make([]proto.Temporal_Api_Common_V1_Payload, len(metadata.headers))
		for &entry, index in metadata.headers {
			result.header_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
		}
		for entry, index in metadata.headers {
			append(&result.header.fields, proto.Temporal_Api_Common_V1_Header_FieldsEntry{
				key = entry.key,
				value = &result.header_payloads[index],
			})
		}
	}
	if len(metadata.memo) > 0 {
		result.memo_payloads = make([]proto.Temporal_Api_Common_V1_Payload, len(metadata.memo))
		for &entry, index in metadata.memo {
			result.memo_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
		}
		for entry, index in metadata.memo {
			append(&result.memo.fields, proto.Temporal_Api_Common_V1_Memo_FieldsEntry{
				key = entry.key,
				value = &result.memo_payloads[index],
			})
		}
	}
	if len(metadata.search_attributes) > 0 {
		result.search_payloads = make(
			[]proto.Temporal_Api_Common_V1_Payload,
			len(metadata.search_attributes),
		)
		for &entry, index in metadata.search_attributes {
			result.search_payloads[index] = proto.encode_payload(codec.payload_view(&entry.payload))
		}
		for entry, index in metadata.search_attributes {
			append(
				&result.search.indexed_fields,
				proto.Temporal_Api_Common_V1_SearchAttributes_IndexedFieldsEntry{
					key = entry.key,
					value = &result.search_payloads[index],
				},
			)
		}
	}
	return result
}

schedule_metadata_proto_free :: proc(value: ^Schedule_Metadata_Proto) {
	if value == nil {
		return
	}
	delete(value.header.fields)
	delete(value.memo.fields)
	delete(value.search.indexed_fields)
	free_start_proto_payloads(value.header_payloads)
	free_start_proto_payloads(value.memo_payloads)
	free_start_proto_payloads(value.search_payloads)
	value^ = {}
}

encode_create_schedule_request :: proc(
	namespace: string,
	schedule_id: string,
	workflow_type: string,
	input: codec.Payload_View,
	options: Schedule_Create_Options,
	identity := DEFAULT_CLIENT_IDENTITY,
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Temporal namespace must not be empty")
	}
	if message := schedule_create_options_error(schedule_id, workflow_type, options); len(message) > 0 {
		return nil, error_from_string(.Invalid_Argument, message)
	}

	request_id := options.request_id
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(request_id) == 0 {
		request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	}

	spec := proto.Temporal_Api_Schedule_V1_ScheduleSpec{
		timezone_name = options.spec.timezone_name,
		timezone_data = options.spec.timezone_data,
	}
	defer delete(spec.structured_calendar)
	defer delete(spec.cron_string)
	defer delete(spec.interval)
	defer delete(spec.exclude_structured_calendar)
	for calendar in options.spec.structured_calendars {
		append(&spec.structured_calendar, schedule_calendar_proto(calendar))
	}
	defer for &calendar in spec.structured_calendar {
		schedule_calendar_proto_free(&calendar)
	}
	for expression in options.spec.cron_expressions {
		append(&spec.cron_string, expression)
	}
	interval_durations := make([]proto.Google_Protobuf_Duration, len(options.spec.intervals) * 2)
	defer delete(interval_durations)
	for interval, index in options.spec.intervals {
		interval_durations[index * 2] = duration_proto(interval.every)
		interval_durations[index * 2 + 1] = duration_proto(interval.phase)
		value := proto.Temporal_Api_Schedule_V1_IntervalSpec{
			interval = &interval_durations[index * 2],
		}
		if interval.phase > 0 {
			value.phase = &interval_durations[index * 2 + 1]
		}
		append(&spec.interval, value)
	}
	for calendar in options.spec.excluded_calendars {
		append(&spec.exclude_structured_calendar, schedule_calendar_proto(calendar))
	}
	defer for &calendar in spec.exclude_structured_calendar {
		schedule_calendar_proto_free(&calendar)
	}
	start_time := proto.Google_Protobuf_Timestamp{}
	end_time := proto.Google_Protobuf_Timestamp{}
	if options.spec.start_time.present {
		start_time = schedule_timestamp_proto(options.spec.start_time.value)
		spec.start_time = &start_time
	}
	if options.spec.end_time.present {
		end_time = schedule_timestamp_proto(options.spec.end_time.value)
		spec.end_time = &end_time
	}
	jitter := duration_proto(options.spec.jitter)
	if options.spec.jitter > 0 {
		spec.jitter = &jitter
	}

	workflow_type_value := proto.Temporal_Api_Common_V1_WorkflowType{name = workflow_type}
	task_queue := proto.Temporal_Api_Taskqueue_V1_TaskQueue{name = options.action.task_queue}
	input_payload := proto.encode_payload(input)
	defer delete(input_payload.metadata)
	defer delete(input_payload.external_payloads)
	inputs := proto.Temporal_Api_Common_V1_Payloads{}
	defer delete(inputs.payloads)
	append(&inputs.payloads, input_payload)
	action_metadata := schedule_metadata_proto(options.action.metadata)
	defer schedule_metadata_proto_free(&action_metadata)
	new_workflow := proto.Temporal_Api_Workflow_V1_NewWorkflowExecutionInfo{
		workflow_id = options.action.workflow_id,
		workflow_type = &workflow_type_value,
		task_queue = &task_queue,
		input = &inputs,
	}
	execution_timeout := duration_proto(options.action.workflow_execution_timeout)
	run_timeout := duration_proto(options.action.workflow_run_timeout)
	task_timeout := duration_proto(options.action.workflow_task_timeout)
	if options.action.workflow_execution_timeout > 0 {
		new_workflow.workflow_execution_timeout = &execution_timeout
	}
	if options.action.workflow_run_timeout > 0 {
		new_workflow.workflow_run_timeout = &run_timeout
	}
	if options.action.workflow_task_timeout > 0 {
		new_workflow.workflow_task_timeout = &task_timeout
	}

	retry_policy := proto.Temporal_Api_Common_V1_RetryPolicy{}
	retry_initial := proto.Google_Protobuf_Duration{}
	retry_maximum := proto.Google_Protobuf_Duration{}
	defer delete(retry_policy.non_retryable_error_types)
	if options.action.retry_policy != nil {
		policy := options.action.retry_policy
		retry_policy.backoff_coefficient = policy.backoff_coefficient
		retry_policy.maximum_attempts = policy.maximum_attempts
		if policy.initial_interval > 0 {
			retry_initial = duration_proto(policy.initial_interval)
			retry_policy.initial_interval = &retry_initial
		}
		if policy.maximum_interval > 0 {
			retry_maximum = duration_proto(policy.maximum_interval)
			retry_policy.maximum_interval = &retry_maximum
		}
		for error_type in policy.non_retryable_error_types {
			append(&retry_policy.non_retryable_error_types, error_type)
		}
		new_workflow.retry_policy = &retry_policy
	}
	priority := proto.Temporal_Api_Common_V1_Priority{}
	if options.action.priority != nil {
		priority = {
			priority_key = options.action.priority.priority_key,
			fairness_key = options.action.priority.fairness_key,
			fairness_weight = options.action.priority.fairness_weight,
		}
		new_workflow.priority = &priority
	}
	if len(action_metadata.header.fields) > 0 {
		new_workflow.header = &action_metadata.header
	}
	if len(action_metadata.memo.fields) > 0 {
		new_workflow.memo = &action_metadata.memo
	}
	if len(action_metadata.search.indexed_fields) > 0 {
		new_workflow.search_attributes = &action_metadata.search
	}
	action := proto.Temporal_Api_Schedule_V1_ScheduleAction{
		action = proto.Temporal_Api_Schedule_V1_ScheduleAction_Action_Start_Workflow{
			value = &new_workflow,
		},
	}

	catchup := duration_proto(options.catchup_window)
	policies := proto.Temporal_Api_Schedule_V1_SchedulePolicies{
		overlap_policy = schedule_overlap_to_proto(options.overlap_policy),
		pause_on_failure = options.pause_on_failure,
		keep_original_workflow_id = options.keep_original_workflow_id,
	}
	if options.catchup_window > 0 {
		policies.catchup_window = &catchup
	}
	state := proto.Temporal_Api_Schedule_V1_ScheduleState{
		notes = options.notes,
		paused = options.paused,
		limited_actions = options.limited_actions,
		remaining_actions = options.remaining_actions,
	}
	schedule := proto.Temporal_Api_Schedule_V1_Schedule{
		spec = &spec,
		action = &action,
		policies = &policies,
		state = &state,
	}

	initial_patch := proto.Temporal_Api_Schedule_V1_SchedulePatch{}
	trigger := proto.Temporal_Api_Schedule_V1_TriggerImmediatelyRequest{
		overlap_policy = schedule_overlap_to_proto(options.trigger_overlap_policy),
	}
	if options.trigger_immediately {
		initial_patch.trigger_immediately = &trigger
	}
	schedule_metadata := schedule_metadata_proto(options.metadata)
	defer schedule_metadata_proto_free(&schedule_metadata)
	request := proto.Temporal_Api_Workflowservice_V1_CreateScheduleRequest{
		namespace = namespace,
		schedule_id = schedule_id,
		schedule = &schedule,
		identity = request_identity(identity),
		request_id = request_id,
	}
	if options.trigger_immediately {
		request.initial_patch = &initial_patch
	}
	if len(schedule_metadata.memo.fields) > 0 {
		request.memo = &schedule_metadata.memo
	}
	if len(schedule_metadata.search.indexed_fields) > 0 {
		request.search_attributes = &schedule_metadata.search
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_CreateScheduleRequest(&request), {}
}

create_schedule_payload_terminal :: proc(interceptor_input: ^Client_Create_Schedule_Input) -> (handle: Schedule_Handle, error: Error) {
	if interceptor_input == nil do return {}, error_from_string(.Invalid_Argument, "Schedule Create interceptor input is missing")
	client := interceptor_input.client
	schedule_id := interceptor_input.schedule_id
	workflow_type := interceptor_input.workflow_type
	input := interceptor_input.input
	options := interceptor_input.options
	if client == nil || client.connection == nil || client.runtime == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	request_bytes, request_error := encode_create_schedule_request(
		client.namespace,
		schedule_id,
		workflow_type,
		input,
		options,
		client_request_identity(client),
	)
	if request_error.message != nil {
		return {}, request_error
	}
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"CreateSchedule",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	response := proto.Temporal_Api_Workflowservice_V1_CreateScheduleResponse{}
	if !proto.decode_Temporal_Api_Workflowservice_V1_CreateScheduleResponse(rpc_result.protobuf, &response) {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Create Schedule response")
	}
	created_handle, handle_error := get_schedule_handle(client, schedule_id)
	if handle_error.message != nil {
		return {}, handle_error
	}
	heap := allocator_runtime.heap_allocator()
	created_handle.conflict_token = bytes.clone(response.conflict_token, heap)
	return created_handle, {}
}

create_schedule_payload :: proc(client: ^Client, schedule_id, workflow_type: string, input: codec.Payload_View, options: Schedule_Create_Options) -> (Schedule_Handle, Error) {
	interceptor_input := Client_Create_Schedule_Input{client = client, schedule_id = schedule_id, workflow_type = workflow_type, input = input, options = options}
	state := client_interceptor_state(client)
	return client_create_schedule_chain(&state, &interceptor_input)
}

create_schedule_json :: proc(
	client: ^Client,
	schedule_id: string,
	workflow_type: string,
	input: $T,
	options: Schedule_Create_Options,
) -> (Schedule_Handle, Error) {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return {}, error_from_string(.Payload_Conversion, "could not encode scheduled Workflow input")
	}
	defer codec.payload_free(&payload)
	return create_schedule_payload(
		client,
		schedule_id,
		workflow_type,
		codec.payload_view(&payload),
		options,
	)
}

// UpdateSchedule is a full replacement of spec, action, policies, and state.
// Pass the conflict token from describe_schedule to guard against lost updates,
// or an empty token for an unconditional replacement. The legacy Temporal
// scheduler can acknowledge the RPC and asynchronously ignore a conflicting
// update, so RPC success alone is not proof that a guarded update was applied.
// Top-level memo/search
// attributes are preserved when options.metadata is nil and replaced (or
// cleared by initialized empty metadata) when it is non-nil.
encode_update_schedule_request :: proc(
	namespace: string,
	schedule_id: string,
	workflow_type: string,
	input: codec.Payload_View,
	options: Schedule_Create_Options,
	conflict_token: []u8,
	identity := DEFAULT_CLIENT_IDENTITY,
) -> (request_bytes: [dynamic]u8, error: Error) {
	if options.trigger_immediately {
		return nil, error_from_string(.Invalid_Argument, "Schedule update cannot trigger an initial action")
	}
	create_bytes, create_error := encode_create_schedule_request(
		namespace,
		schedule_id,
		workflow_type,
		input,
		options,
		identity,
	)
	if create_error.message != nil { return nil, create_error }
	defer delete(create_bytes)

	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return nil, error_from_string(.Malformed_Response, "could not initialize Schedule update arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	create_request := proto.Temporal_Api_Workflowservice_V1_CreateScheduleRequest{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_CreateScheduleRequest(
		create_bytes[:],
		&create_request,
	)
	context.allocator = previous_allocator
	if !decoded || create_request.schedule == nil {
		return nil, error_from_string(.Malformed_Response, "could not construct Schedule update")
	}

	empty_memo := proto.Temporal_Api_Common_V1_Memo{}
	empty_search := proto.Temporal_Api_Common_V1_SearchAttributes{}
	request := proto.Temporal_Api_Workflowservice_V1_UpdateScheduleRequest{
		namespace = namespace,
		schedule_id = schedule_id,
		schedule = create_request.schedule,
		conflict_token = conflict_token,
		identity = create_request.identity,
		request_id = create_request.request_id,
	}
	if options.metadata != nil {
		request.memo = create_request.memo
		if request.memo == nil { request.memo = &empty_memo }
		request.search_attributes = create_request.search_attributes
		if request.search_attributes == nil { request.search_attributes = &empty_search }
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_UpdateScheduleRequest(&request), {}
}

update_schedule_payload_unintercepted :: proc(
	handle: ^Schedule_Handle,
	workflow_type: string,
	input: codec.Payload_View,
	options: Schedule_Create_Options,
	conflict_token: []u8 = nil,
) -> Error {
	if !schedule_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Schedule handle is not initialized")
	}
	request_bytes, request_error := encode_update_schedule_request(
		handle.client.namespace,
		handle.id,
		workflow_type,
		input,
		options,
		conflict_token,
		client_request_identity(handle.client),
	)
	if request_error.message != nil { return request_error }
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"UpdateSchedule",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil { return error_from_core(&rpc_result.error) }
	response := proto.Temporal_Api_Workflowservice_V1_UpdateScheduleResponse{}
	if !proto.decode_Temporal_Api_Workflowservice_V1_UpdateScheduleResponse(rpc_result.protobuf, &response) {
		return error_from_string(.Malformed_Response, "Temporal returned a malformed Update Schedule response")
	}
	return {}
}

update_schedule_payload :: proc(handle: ^Schedule_Handle, workflow_type: string, input: codec.Payload_View, options: Schedule_Create_Options, conflict_token: []u8 = nil) -> Error {
	interceptor_input := Client_Schedule_Mutation_Input{handle = handle, kind = .Update, workflow_type = workflow_type, input = input, options = options, conflict_token = conflict_token}
	client: ^Client; if handle != nil do client = handle.client
	state := client_interceptor_state(client)
	return client_schedule_mutation_chain(&state, &interceptor_input)
}

update_schedule_json :: proc(
	handle: ^Schedule_Handle,
	workflow_type: string,
	input: $T,
	options: Schedule_Create_Options,
	conflict_token: []u8 = nil,
) -> Error {
	payload, encode_error := codec.encode_json(input)
	if encode_error != .None {
		return error_from_string(.Payload_Conversion, "could not encode scheduled Workflow input")
	}
	defer codec.payload_free(&payload)
	return update_schedule_payload(
		handle,
		workflow_type,
		codec.payload_view(&payload),
		options,
		conflict_token,
	)
}

schedule_named_payloads_free :: proc(values: ^[dynamic]Named_Payload, allocator: mem.Allocator) {
	if values == nil {
		return
	}
	for &entry in values^ {
		delete(entry.key, allocator)
		codec.payload_free(&entry.payload)
	}
	delete(values^)
	values^ = nil
}

schedule_action_result_free :: proc(value: ^Schedule_Action_Result, allocator: mem.Allocator) {
	if value == nil {
		return
	}
	delete(value.workflow_id, allocator)
	delete(value.run_id, allocator)
	value^ = {}
}

schedule_action_result_clone :: proc(
	source: ^proto.Temporal_Api_Schedule_V1_ScheduleActionResult,
	allocator: mem.Allocator,
) -> (result: Schedule_Action_Result, ok: bool) {
	if source == nil {
		return {}, false
	}
	result.schedule_time = description_time(source.schedule_time)
	result.actual_time = description_time(source.actual_time)
	result.status = Workflow_Status(source.start_workflow_status)
	if source.start_workflow_result != nil {
		result.workflow_id, ok = description_clone_string(
			source.start_workflow_result.workflow_id,
			allocator,
		)
		if !ok {
			return {}, false
		}
		result.run_id, ok = description_clone_string(source.start_workflow_result.run_id, allocator)
		if !ok {
			delete(result.workflow_id, allocator)
			return {}, false
		}
	}
	return result, true
}

schedule_calendar_snapshot_free :: proc(
	value: ^Schedule_Calendar_Snapshot,
	allocator: mem.Allocator,
) {
	if value == nil { return }
	delete(value.second)
	delete(value.minute)
	delete(value.hour)
	delete(value.day_of_month)
	delete(value.month)
	delete(value.year)
	delete(value.day_of_week)
	delete(value.comment, allocator)
	value^ = {}
}

schedule_clone_ranges :: proc(
	destination: ^[dynamic]Schedule_Range,
	source: []proto.Temporal_Api_Schedule_V1_Range,
) {
	for value in source {
		append(destination, Schedule_Range{start = value.start, end = value.end, step = value.step})
	}
}

schedule_calendar_snapshot :: proc(
	source: ^proto.Temporal_Api_Schedule_V1_StructuredCalendarSpec,
	allocator: mem.Allocator,
) -> (result: Schedule_Calendar_Snapshot, ok: bool) {
	if source == nil { return {}, false }
	result.second = make([dynamic]Schedule_Range, allocator)
	result.minute = make([dynamic]Schedule_Range, allocator)
	result.hour = make([dynamic]Schedule_Range, allocator)
	result.day_of_month = make([dynamic]Schedule_Range, allocator)
	result.month = make([dynamic]Schedule_Range, allocator)
	result.year = make([dynamic]Schedule_Range, allocator)
	result.day_of_week = make([dynamic]Schedule_Range, allocator)
	schedule_clone_ranges(&result.second, source.second[:])
	schedule_clone_ranges(&result.minute, source.minute[:])
	schedule_clone_ranges(&result.hour, source.hour[:])
	schedule_clone_ranges(&result.day_of_month, source.day_of_month[:])
	schedule_clone_ranges(&result.month, source.month[:])
	schedule_clone_ranges(&result.year, source.year[:])
	schedule_clone_ranges(&result.day_of_week, source.day_of_week[:])
	result.comment, ok = description_clone_string(source.comment, allocator)
	if !ok { schedule_calendar_snapshot_free(&result, allocator) }
	return result, ok
}

schedule_description_free :: proc(description: ^Schedule_Description) {
	if description == nil {
		return
	}
	allocator := description.allocator
	if allocator.procedure == nil {
		description^ = {}
		return
	}
	delete(description.schedule_id, allocator)
	for &calendar in description.structured_calendars {
		schedule_calendar_snapshot_free(&calendar, allocator)
	}
	delete(description.structured_calendars)
	for expression in description.cron_expressions {
		delete(expression, allocator)
	}
	delete(description.cron_expressions)
	delete(description.intervals)
	for &calendar in description.excluded_calendars {
		schedule_calendar_snapshot_free(&calendar, allocator)
	}
	delete(description.excluded_calendars)
	delete(description.timezone_name, allocator)
	delete(description.timezone_data, allocator)
	delete(description.action.workflow_id, allocator)
	delete(description.action.workflow_type, allocator)
	delete(description.action.task_queue, allocator)
	delete(description.action.priority.fairness_key, allocator)
	for &payload in description.action.inputs {
		codec.payload_free(&payload)
	}
	delete(description.action.inputs)
	delete(description.notes, allocator)
	for &identity in description.running_workflows {
		delete(identity.workflow_id, allocator)
		delete(identity.run_id, allocator)
	}
	delete(description.running_workflows)
	for &action in description.recent_actions {
		schedule_action_result_free(&action, allocator)
	}
	delete(description.recent_actions)
	delete(description.future_action_times)
	schedule_named_payloads_free(&description.memo, allocator)
	schedule_named_payloads_free(&description.search_attributes, allocator)
	delete(description.conflict_token, allocator)
	description^ = {}
}

schedule_clone_named_map :: proc(
	destination: ^[dynamic]Named_Payload,
	entries: []proto.Temporal_Api_Common_V1_Memo_FieldsEntry,
	allocator: mem.Allocator,
) -> bool {
	for entry in entries {
		owned, ok := description_clone_named_payload(entry.key, entry.value, allocator)
		if !ok {
			return false
		}
		append(destination, owned)
	}
	return true
}

schedule_clone_search_map :: proc(
	destination: ^[dynamic]Named_Payload,
	entries: []proto.Temporal_Api_Common_V1_SearchAttributes_IndexedFieldsEntry,
	allocator: mem.Allocator,
) -> bool {
	for entry in entries {
		owned, ok := description_clone_named_payload(entry.key, entry.value, allocator)
		if !ok {
			return false
		}
		append(destination, owned)
	}
	return true
}

decode_schedule_description :: proc(
	schedule_id: string,
	data: []u8,
	allocator := context.allocator,
) -> (description: Schedule_Description, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Schedule response arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_DescribeScheduleResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_DescribeScheduleResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded || response.schedule == nil || response.schedule.spec == nil ||
	   response.schedule.action == nil || response.schedule.policies == nil ||
	   response.schedule.state == nil || response.info == nil {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Schedule description")
	}

	description.allocator = allocator
	description.structured_calendars = make([dynamic]Schedule_Calendar_Snapshot, allocator)
	description.cron_expressions = make([dynamic]string, allocator)
	description.intervals = make([dynamic]Schedule_Interval, allocator)
	description.excluded_calendars = make([dynamic]Schedule_Calendar_Snapshot, allocator)
	description.action.inputs = make([dynamic]codec.Payload, allocator)
	description.running_workflows = make([dynamic]Workflow_Execution_Identity, allocator)
	description.recent_actions = make([dynamic]Schedule_Action_Result, allocator)
	description.future_action_times = make([dynamic]time.Time, allocator)
	description.memo = make([dynamic]Named_Payload, allocator)
	description.search_attributes = make([dynamic]Named_Payload, allocator)
	description.conflict_token = bytes.clone(response.conflict_token, allocator)
	description.timezone_data = bytes.clone(response.schedule.spec.timezone_data, allocator)
	failed := false
	defer if failed { schedule_description_free(&description) }
	clone_targets := []struct {
		source: string,
		target: ^string,
	}{
		{schedule_id, &description.schedule_id},
		{response.schedule.spec.timezone_name, &description.timezone_name},
		{response.schedule.state.notes, &description.notes},
	}
	for target in clone_targets {
		ok: bool
		target.target^, ok = description_clone_string(target.source, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate Schedule description")
		}
	}

	spec := response.schedule.spec
	for &calendar in spec.structured_calendar {
		owned, ok := schedule_calendar_snapshot(&calendar, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate Schedule calendar")
		}
		append(&description.structured_calendars, owned)
	}
	for expression in spec.cron_string {
		owned, ok := description_clone_string(expression, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate Schedule cron expression")
		}
		append(&description.cron_expressions, owned)
	}
	for interval in spec.interval {
		if interval.interval == nil {
			failed = true
			return {}, error_from_string(.Malformed_Response, "Schedule interval omitted its duration")
		}
		append(&description.intervals, Schedule_Interval{
			every = description_duration(interval.interval).value,
			phase = description_duration(interval.phase).value,
		})
	}
	for &calendar in spec.exclude_structured_calendar {
		owned, ok := schedule_calendar_snapshot(&calendar, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate excluded Schedule calendar")
		}
		append(&description.excluded_calendars, owned)
	}
	description.start_time = description_time(spec.start_time)
	description.end_time = description_time(spec.end_time)
	description.jitter = description_duration(spec.jitter)

	action_found := false
	#partial switch variant in response.schedule.action.action {
	case proto.Temporal_Api_Schedule_V1_ScheduleAction_Action_Start_Workflow:
		action_found = true
		workflow := variant.value
		if workflow == nil || workflow.workflow_type == nil || workflow.task_queue == nil {
			failed = true
			return {}, error_from_string(.Malformed_Response, "Schedule Workflow action omitted identity")
		}
		action_targets := []struct {
			source: string,
			target: ^string,
		}{
			{workflow.workflow_id, &description.action.workflow_id},
			{workflow.workflow_type.name, &description.action.workflow_type},
			{workflow.task_queue.name, &description.action.task_queue},
		}
		for target in action_targets {
			ok: bool
			target.target^, ok = description_clone_string(target.source, allocator)
			if !ok {
				failed = true
				return {}, error_from_string(.Malformed_Response, "could not allocate Schedule Workflow action")
			}
		}
		description.action.workflow_execution_timeout = description_duration(workflow.workflow_execution_timeout)
		description.action.workflow_run_timeout = description_duration(workflow.workflow_run_timeout)
		description.action.workflow_task_timeout = description_duration(workflow.workflow_task_timeout)
		priority_ok: bool
		description.action.priority, priority_ok = description_priority(workflow.priority, allocator)
		if !priority_ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate Schedule Workflow priority")
		}
		if workflow.input != nil {
			for &payload in workflow.input.payloads {
				owned, payload_ok := description_clone_payload(&payload, allocator)
				if !payload_ok {
					failed = true
					return {}, error_from_string(.Payload_Conversion, "could not copy Schedule Workflow input")
				}
				append(&description.action.inputs, owned)
			}
		}
	}
	if !action_found {
		failed = true
		return {}, error_from_string(.Malformed_Response, "Schedule omitted its Workflow action")
	}

	policies := response.schedule.policies
	state := response.schedule.state
	description.overlap_policy = Schedule_Overlap_Policy(policies.overlap_policy)
	description.catchup_window = description_duration(policies.catchup_window)
	description.pause_on_failure = policies.pause_on_failure
	description.keep_original_workflow_id = policies.keep_original_workflow_id
	description.paused = state.paused
	description.limited_actions = state.limited_actions
	description.remaining_actions = state.remaining_actions
	info := response.info
	description.action_count = info.action_count
	description.missed_catchup_window = info.missed_catchup_window
	description.overlap_skipped = info.overlap_skipped
	description.buffer_dropped = info.buffer_dropped
	description.buffer_size = info.buffer_size
	description.state_size_bytes = info.state_size_bytes
	description.create_time = description_time(info.create_time)
	description.update_time = description_time(info.update_time)
	for execution in info.running_workflows {
		identity := Workflow_Execution_Identity{}
		ok: bool
		identity.workflow_id, ok = description_clone_string(execution.workflow_id, allocator)
		if ok {
			identity.run_id, ok = description_clone_string(execution.run_id, allocator)
		}
		if !ok {
			delete(identity.workflow_id, allocator)
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate running Schedule execution")
		}
		append(&description.running_workflows, identity)
	}
	for &action_result in info.recent_actions {
		owned, ok := schedule_action_result_clone(&action_result, allocator)
		if !ok {
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate recent Schedule action")
		}
		append(&description.recent_actions, owned)
	}
	for &future_time in info.future_action_times {
		append(&description.future_action_times, description_time(&future_time).value)
	}
	if response.memo != nil && !schedule_clone_named_map(&description.memo, response.memo.fields[:], allocator) {
		failed = true
		return {}, error_from_string(.Payload_Conversion, "could not copy Schedule memo")
	}
	if response.search_attributes != nil &&
	   !schedule_clone_search_map(
		&description.search_attributes,
		response.search_attributes.indexed_fields[:],
		allocator,
	) {
		failed = true
		return {}, error_from_string(.Payload_Conversion, "could not copy Schedule Search Attributes")
	}
	return description, {}
}

describe_schedule_terminal :: proc(input: ^Client_Describe_Schedule_Input) -> (Schedule_Description, Error) {
	if input == nil do return {}, error_from_string(.Invalid_Argument, "Schedule Describe interceptor input is missing")
	handle := input.handle
	allocator := input.allocator
	if !schedule_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Schedule handle is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_DescribeScheduleRequest{
		namespace = handle.client.namespace,
		schedule_id = handle.id,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_DescribeScheduleRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"DescribeSchedule",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_schedule_description(handle.id, rpc_result.protobuf, allocator)
}

describe_schedule :: proc(handle: ^Schedule_Handle, allocator := context.allocator) -> (Schedule_Description, Error) {
	input := Client_Describe_Schedule_Input{handle = handle, allocator = allocator}
	client: ^Client
	if handle != nil do client = handle.client
	state := client_interceptor_state(client)
	return client_describe_schedule_chain(&state, &input)
}

schedule_list_entry_free :: proc(entry: ^Schedule_List_Entry, allocator: mem.Allocator) {
	if entry == nil {
		return
	}
	delete(entry.schedule_id, allocator)
	delete(entry.workflow_type, allocator)
	delete(entry.notes, allocator)
	for &action in entry.recent_actions {
		schedule_action_result_free(&action, allocator)
	}
	delete(entry.recent_actions)
	delete(entry.future_action_times)
	schedule_named_payloads_free(&entry.memo, allocator)
	schedule_named_payloads_free(&entry.search_attributes, allocator)
	entry^ = {}
}

schedule_list_page_free :: proc(page: ^Schedule_List_Page) {
	if page == nil {
		return
	}
	if page.allocator.procedure != nil {
		for &entry in page.schedules {
			schedule_list_entry_free(&entry, page.allocator)
		}
		delete(page.next_page_token, page.allocator)
	}
	delete(page.schedules)
	page^ = {}
}

decode_schedule_list_page :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (page: Schedule_List_Page, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Schedule list arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_ListSchedulesResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_ListSchedulesResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Schedule list")
	}

	page.allocator = allocator
	page.schedules = make([dynamic]Schedule_List_Entry, allocator)
	page.next_page_token = bytes.clone(response.next_page_token, allocator)
	failed := false
	defer if failed { schedule_list_page_free(&page) }
	for &source in response.schedules {
		if source.info == nil {
			failed = true
			return {}, error_from_string(.Malformed_Response, "Schedule list entry omitted its info")
		}
		entry := Schedule_List_Entry{
			paused = source.info.paused,
			state_size_bytes = source.info.state_size_bytes,
		}
		entry.recent_actions = make([dynamic]Schedule_Action_Result, allocator)
		entry.future_action_times = make([dynamic]time.Time, allocator)
		entry.memo = make([dynamic]Named_Payload, allocator)
		entry.search_attributes = make([dynamic]Named_Payload, allocator)
		ok: bool
		entry.schedule_id, ok = description_clone_string(source.schedule_id, allocator)
		if ok && source.info.workflow_type != nil {
			entry.workflow_type, ok = description_clone_string(source.info.workflow_type.name, allocator)
		}
		if ok {
			entry.notes, ok = description_clone_string(source.info.notes, allocator)
		}
		if !ok {
			schedule_list_entry_free(&entry, allocator)
			failed = true
			return {}, error_from_string(.Malformed_Response, "could not allocate Schedule list entry")
		}
		for &action_result in source.info.recent_actions {
			owned, cloned := schedule_action_result_clone(&action_result, allocator)
			if !cloned {
				schedule_list_entry_free(&entry, allocator)
				failed = true
				return {}, error_from_string(.Malformed_Response, "could not allocate Schedule list action")
			}
			append(&entry.recent_actions, owned)
		}
		for &future_time in source.info.future_action_times {
			append(&entry.future_action_times, description_time(&future_time).value)
		}
		if source.memo != nil && !schedule_clone_named_map(&entry.memo, source.memo.fields[:], allocator) {
			schedule_list_entry_free(&entry, allocator)
			failed = true
			return {}, error_from_string(.Payload_Conversion, "could not copy Schedule list memo")
		}
		if source.search_attributes != nil &&
		   !schedule_clone_search_map(
			&entry.search_attributes,
			source.search_attributes.indexed_fields[:],
			allocator,
		) {
			schedule_list_entry_free(&entry, allocator)
			failed = true
			return {}, error_from_string(.Payload_Conversion, "could not copy Schedule list Search Attributes")
		}
		append(&page.schedules, entry)
	}
	return page, {}
}

list_schedules_terminal :: proc(input: ^Client_List_Schedules_Input) -> (Schedule_List_Page, Error) {
	if input == nil do return {}, error_from_string(.Invalid_Argument, "Schedule list interceptor input is missing")
	client := input.client
	options := input.options
	if client == nil || client.connection == nil || client.runtime == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	if options.maximum_page_size < 0 {
		return {}, error_from_string(.Invalid_Argument, "Schedule list page size must not be negative")
	}
	request := proto.Temporal_Api_Workflowservice_V1_ListSchedulesRequest{
		namespace = client.namespace,
		maximum_page_size = options.maximum_page_size,
		next_page_token = options.next_page_token,
		query = options.query,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_ListSchedulesRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"ListSchedules",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_schedule_list_page(rpc_result.protobuf)
}

list_schedules :: proc(client: ^Client, options := List_Schedules_Options{}) -> (Schedule_List_Page, Error) {
	input := Client_List_Schedules_Input{client = client, options = options}
	state := client_interceptor_state(client)
	return client_list_schedules_chain(&state, &input)
}

schedule_count_free :: proc(result: ^Schedule_Count) {
	if result == nil {
		return
	}
	for &group in result.groups {
		for &payload in group.values {
			codec.payload_free(&payload)
		}
		delete(group.values)
	}
	delete(result.groups)
	result^ = {}
}

decode_schedule_count :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (result: Schedule_Count, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Schedule count arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_CountSchedulesResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_CountSchedulesResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned a malformed Schedule count")
	}
	result.count = response.count
	result.groups = make([dynamic]Schedule_Count_Group, allocator)
	result.allocator = allocator
	failed := false
	defer if failed { schedule_count_free(&result) }
	for &source in response.groups {
		group := Schedule_Count_Group{count = source.count}
		group.values = make([dynamic]codec.Payload, allocator)
		for &payload in source.group_values {
			owned, ok := description_clone_payload(&payload, allocator)
			if !ok {
				for &value in group.values {
					codec.payload_free(&value)
				}
				delete(group.values)
				failed = true
				return {}, error_from_string(.Payload_Conversion, "could not copy Schedule count group")
			}
			append(&group.values, owned)
		}
		append(&result.groups, group)
	}
	return result, {}
}

count_schedules_terminal :: proc(input: ^Client_Count_Schedules_Input) -> (Schedule_Count, Error) {
	if input == nil do return {}, error_from_string(.Invalid_Argument, "Schedule count interceptor input is missing")
	client := input.client
	query := input.query
	if client == nil || client.connection == nil || client.runtime == nil {
		return {}, error_from_string(.Invalid_Argument, "Temporal Client is not initialized")
	}
	request := proto.Temporal_Api_Workflowservice_V1_CountSchedulesRequest{
		namespace = client.namespace,
		query = query,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_CountSchedulesRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		client.connection,
		.Workflow,
		"CountSchedules",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return {}, error_from_core(&rpc_result.error)
	}
	return decode_schedule_count(rpc_result.protobuf)
}

count_schedules :: proc(client: ^Client, query := "") -> (Schedule_Count, Error) {
	input := Client_Count_Schedules_Input{client = client, query = query}
	state := client_interceptor_state(client)
	return client_count_schedules_chain(&state, &input)
}

encode_backfill_schedule_request :: proc(
	namespace, schedule_id: string,
	backfills: []Schedule_Backfill,
	request_id := "",
	identity := DEFAULT_CLIENT_IDENTITY,
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 || len(schedule_id) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Schedule backfill requires namespace and Schedule ID")
	}
	if len(backfills) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Schedule backfill must contain at least one range")
	}
	for backfill in backfills {
		if time.to_unix_nanoseconds(backfill.start_time) >= time.to_unix_nanoseconds(backfill.end_time) {
			return nil, error_from_string(.Invalid_Argument, "Schedule backfill start must be before end")
		}
		policy := i32(backfill.overlap_policy)
		if policy < i32(Schedule_Overlap_Policy.Unspecified) || policy > i32(Schedule_Overlap_Policy.Allow_All) {
			return nil, error_from_string(.Invalid_Argument, "Schedule backfill overlap policy is invalid")
		}
	}
	timestamps := make([]proto.Google_Protobuf_Timestamp, len(backfills) * 2)
	defer delete(timestamps)
	patch := proto.Temporal_Api_Schedule_V1_SchedulePatch{}
	defer delete(patch.backfill_request)
	for backfill, index in backfills {
		timestamps[index * 2] = schedule_timestamp_proto(backfill.start_time)
		timestamps[index * 2 + 1] = schedule_timestamp_proto(backfill.end_time)
		append(&patch.backfill_request, proto.Temporal_Api_Schedule_V1_BackfillRequest{
			start_time = &timestamps[index * 2],
			end_time = &timestamps[index * 2 + 1],
			overlap_policy = schedule_overlap_to_proto(backfill.overlap_policy),
		})
	}
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	resolved_request_id := request_id
	if len(resolved_request_id) == 0 {
		resolved_request_id = uuid.to_string_buffer(uuid.generate_v4(), request_id_buffer[:])
	}
	request := proto.Temporal_Api_Workflowservice_V1_PatchScheduleRequest{
		namespace = namespace,
		schedule_id = schedule_id,
		patch = &patch,
		identity = request_identity(identity),
		request_id = resolved_request_id,
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_PatchScheduleRequest(&request), {}
}

backfill_schedule_unintercepted :: proc(handle: ^Schedule_Handle, backfills: []Schedule_Backfill) -> Error {
	if !schedule_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Schedule handle is not initialized")
	}
	request_bytes, request_error := encode_backfill_schedule_request(
		handle.client.namespace,
		handle.id,
		backfills,
		"",
		client_request_identity(handle.client),
	)
	if request_error.message != nil { return request_error }
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"PatchSchedule",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil { return error_from_core(&rpc_result.error) }
	return {}
}

backfill_schedule :: proc(handle: ^Schedule_Handle, backfills: []Schedule_Backfill) -> Error {
	input := Client_Schedule_Mutation_Input{handle = handle, kind = .Backfill, backfills = backfills}
	client: ^Client; if handle != nil do client = handle.client
	state := client_interceptor_state(client)
	return client_schedule_mutation_chain(&state, &input)
}

schedule_matching_times_free :: proc(result: ^Schedule_Matching_Times) {
	if result == nil { return }
	delete(result.times)
	result^ = {}
}

decode_schedule_matching_times :: proc(
	data: []u8,
	allocator := context.allocator,
) -> (result: Schedule_Matching_Times, error: Error) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return {}, error_from_string(.Malformed_Response, "could not initialize Schedule matching-times arena")
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	response := proto.Temporal_Api_Workflowservice_V1_ListScheduleMatchingTimesResponse{}
	decoded := proto.decode_Temporal_Api_Workflowservice_V1_ListScheduleMatchingTimesResponse(data, &response)
	context.allocator = previous_allocator
	if !decoded {
		return {}, error_from_string(.Malformed_Response, "Temporal returned malformed Schedule matching times")
	}
	result.allocator = allocator
	result.times = make([dynamic]time.Time, allocator)
	for &value in response.start_time {
		converted := description_time(&value)
		if !converted.present {
			schedule_matching_times_free(&result)
			return {}, error_from_string(.Malformed_Response, "Temporal returned an invalid Schedule matching time")
		}
		append(&result.times, converted.value)
	}
	return result, {}
}

list_schedule_matching_times_terminal :: proc(input: ^Client_List_Schedule_Matching_Times_Input) -> (Schedule_Matching_Times, Error) {
	if input == nil do return {}, error_from_string(.Invalid_Argument, "Schedule matching-times interceptor input is missing")
	handle := input.handle
	start_time := input.start_time
	end_time := input.end_time
	allocator := input.allocator
	if !schedule_handle_valid(handle) {
		return {}, error_from_string(.Invalid_Argument, "Schedule handle is not initialized")
	}
	if time.to_unix_nanoseconds(start_time) >= time.to_unix_nanoseconds(end_time) {
		return {}, error_from_string(.Invalid_Argument, "Schedule matching-time start must be before end")
	}
	start_proto := schedule_timestamp_proto(start_time)
	end_proto := schedule_timestamp_proto(end_time)
	request := proto.Temporal_Api_Workflowservice_V1_ListScheduleMatchingTimesRequest{
		namespace = handle.client.namespace,
		schedule_id = handle.id,
		start_time = &start_proto,
		end_time = &end_proto,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_ListScheduleMatchingTimesRequest(&request)
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"ListScheduleMatchingTimes",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil { return {}, error_from_core(&rpc_result.error) }
	return decode_schedule_matching_times(rpc_result.protobuf, allocator)
}

list_schedule_matching_times :: proc(handle: ^Schedule_Handle, start_time, end_time: time.Time, allocator := context.allocator) -> (Schedule_Matching_Times, Error) {
	input := Client_List_Schedule_Matching_Times_Input{handle = handle, start_time = start_time, end_time = end_time, allocator = allocator}
	client: ^Client
	if handle != nil do client = handle.client
	state := client_interceptor_state(client)
	return client_list_schedule_matching_times_chain(&state, &input)
}

encode_schedule_patch_request :: proc(
	namespace, schedule_id: string,
	patch: ^proto.Temporal_Api_Schedule_V1_SchedulePatch,
	request_id := "",
	identity := DEFAULT_CLIENT_IDENTITY,
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 || len(schedule_id) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Schedule patch requires namespace and Schedule ID")
	}
	if patch == nil {
		return nil, error_from_string(.Invalid_Argument, "Schedule patch must not be nil")
	}
	request_id_buffer: [uuid.EXPECTED_LENGTH]u8
	request := proto.Temporal_Api_Workflowservice_V1_PatchScheduleRequest{
		namespace = namespace,
		schedule_id = schedule_id,
		patch = patch,
		identity = request_identity(identity),
		request_id = resolve_request_id(request_id, request_id_buffer[:]),
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_PatchScheduleRequest(&request), {}
}

schedule_patch_unintercepted :: proc(
	handle: ^Schedule_Handle,
	patch: ^proto.Temporal_Api_Schedule_V1_SchedulePatch,
) -> Error {
	if !schedule_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Schedule handle is not initialized")
	}
	request_bytes, request_error := encode_schedule_patch_request(
		handle.client.namespace,
		handle.id,
		patch,
		identity = client_request_identity(handle.client),
	)
	if request_error.message != nil {
		return request_error
	}
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"PatchSchedule",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return error_from_core(&rpc_result.error)
	}
	return {}
}

pause_schedule :: proc(handle: ^Schedule_Handle, note := "Paused by temporal-sdk-odin") -> Error {
	input := Client_Schedule_Mutation_Input{handle = handle, kind = .Pause, note = note}
	client: ^Client; if handle != nil do client = handle.client
	state := client_interceptor_state(client)
	return client_schedule_mutation_chain(&state, &input)
}

unpause_schedule :: proc(handle: ^Schedule_Handle, note := "Unpaused by temporal-sdk-odin") -> Error {
	input := Client_Schedule_Mutation_Input{handle = handle, kind = .Unpause, note = note}
	client: ^Client; if handle != nil do client = handle.client
	state := client_interceptor_state(client)
	return client_schedule_mutation_chain(&state, &input)
}

trigger_schedule :: proc(
	handle: ^Schedule_Handle,
	overlap_policy := Schedule_Overlap_Policy.Unspecified,
) -> Error {
	input := Client_Schedule_Mutation_Input{handle = handle, kind = .Trigger, overlap_policy = overlap_policy}
	client: ^Client; if handle != nil do client = handle.client
	state := client_interceptor_state(client)
	return client_schedule_mutation_chain(&state, &input)
}

encode_delete_schedule_request :: proc(
	namespace, schedule_id: string,
	identity := DEFAULT_CLIENT_IDENTITY,
) -> (request_bytes: [dynamic]u8, error: Error) {
	if len(namespace) == 0 || len(schedule_id) == 0 {
		return nil, error_from_string(.Invalid_Argument, "Schedule deletion requires namespace and Schedule ID")
	}
	request := proto.Temporal_Api_Workflowservice_V1_DeleteScheduleRequest{
		namespace = namespace,
		schedule_id = schedule_id,
		identity = request_identity(identity),
	}
	return proto.encode_Temporal_Api_Workflowservice_V1_DeleteScheduleRequest(&request), {}
}

delete_schedule_unintercepted :: proc(handle: ^Schedule_Handle) -> Error {
	if !schedule_handle_valid(handle) {
		return error_from_string(.Invalid_Argument, "Schedule handle is not initialized")
	}
	request_bytes, request_error := encode_delete_schedule_request(
		handle.client.namespace,
		handle.id,
		client_request_identity(handle.client),
	)
	if request_error.message != nil {
		return request_error
	}
	defer delete(request_bytes)
	rpc_result := temporal_core.client_rpc_call(
		handle.client.connection,
		.Workflow,
		"DeleteSchedule",
		request_bytes[:],
	)
	defer delete(rpc_result.protobuf)
	if rpc_result.error.message != nil {
		return error_from_core(&rpc_result.error)
	}
	return {}
}

delete_schedule :: proc(handle: ^Schedule_Handle) -> Error {
	input := Client_Schedule_Mutation_Input{handle = handle, kind = .Delete}
	client: ^Client; if handle != nil do client = handle.client
	state := client_interceptor_state(client)
	return client_schedule_mutation_chain(&state, &input)
}

probe_count_support :: proc(
	temporal_client: ^Client,
) -> (client.Server_Feature_Observation, Error) {
	count, probe_error := count_schedules(temporal_client)
	schedule_count_free(&count)
	observation := client.server_feature_from_rpc_error(.Schedule_Count, &probe_error)
	if observation.support == .Unsupported {
		client.error_free(&probe_error)
		return observation, {}
	}
	return observation, probe_error
}

schedule_mutation_terminal :: proc(input: ^Client_Schedule_Mutation_Input) -> Error {
	if input == nil do return error_from_string(.Invalid_Argument, "Schedule mutation interceptor input is missing")
	switch input.kind {
	case .Update:
		return update_schedule_payload_unintercepted(input.handle, input.workflow_type, input.input, input.options, input.conflict_token)
	case .Backfill:
		return backfill_schedule_unintercepted(input.handle, input.backfills)
	case .Pause:
		if len(input.note) == 0 do return error_from_string(.Invalid_Argument, "Schedule pause note must not be empty")
		patch := proto.Temporal_Api_Schedule_V1_SchedulePatch{pause = input.note}
		return schedule_patch_unintercepted(input.handle, &patch)
	case .Unpause:
		if len(input.note) == 0 do return error_from_string(.Invalid_Argument, "Schedule unpause note must not be empty")
		patch := proto.Temporal_Api_Schedule_V1_SchedulePatch{unpause = input.note}
		return schedule_patch_unintercepted(input.handle, &patch)
	case .Trigger:
		trigger := proto.Temporal_Api_Schedule_V1_TriggerImmediatelyRequest{overlap_policy = schedule_overlap_to_proto(input.overlap_policy)}
		patch := proto.Temporal_Api_Schedule_V1_SchedulePatch{trigger_immediately = &trigger}
		return schedule_patch_unintercepted(input.handle, &patch)
	case .Delete:
		return delete_schedule_unintercepted(input.handle)
	}
	return error_from_string(.Invalid_Argument, "unknown Schedule mutation kind")
}

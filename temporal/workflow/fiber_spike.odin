package workflow

import allocator_runtime "base:runtime"
import "core:encoding/uuid"
import "core:fmt"
import "core:math"
import "core:mem/virtual"
import "core:strings"
import "core:time"
import codec "../codec"
import proto "../core/proto"
import temporal_failure "../failure"
import temporal_headers "../headers"
import fiber_raw "fiber/raw"

Condition_Predicate :: proc(data: rawptr) -> bool

Log_Level :: enum {
	Trace,
	Debug,
	Info,
	Warn,
	Error,
}

Log_Field_Kind :: enum {
	String,
	Integer,
	Float,
	Boolean,
}

// Log_Field values and strings are borrowed for the synchronous logger call.
Log_Field :: struct {
	key:           string,
	kind:          Log_Field_Kind,
	string_value:  string,
	integer_value: i64,
	float_value:   f64,
	boolean_value: bool,
}

log_field_string :: proc(key, value: string) -> Log_Field {
	return {key = key, kind = .String, string_value = value}
}

log_field_i64 :: proc(key: string, value: i64) -> Log_Field {
	return {key = key, kind = .Integer, integer_value = value}
}

log_field_f64 :: proc(key: string, value: f64) -> Log_Field {
	return {key = key, kind = .Float, float_value = value}
}

log_field_bool :: proc(key: string, value: bool) -> Log_Field {
	return {key = key, kind = .Boolean, boolean_value = value}
}

// Log_Record is borrowed only for the synchronous Logger callback. Timestamp
// and execution metadata come from Core's current activation, never host state.
Log_Record :: struct {
	level:                    Log_Level,
	message:                  string,
	fields:                   []Log_Field,
	namespace:                string,
	workflow_id:              string,
	run_id:                   string,
	workflow_type:            string,
	task_queue:               string,
	history_length:           u32,
	timestamp:                time.Time,
	has_timestamp:            bool,
	replaying:                bool,
	replaying_history_events: bool,
}

Log_Emit_Proc :: #type proc(user_data: rawptr, record: ^Log_Record)

// Logger is copied into the Worker and each registered Workflow runtime.
// user_data remains application-owned until worker.close. Replay of history
// events is suppressed by default; query-only replay is not history replay.
Logger :: struct {
	user_data:                 rawptr,
	emit:                      Log_Emit_Proc,
	log_replay_history_events: bool,
}

Metric_Kind :: enum {
	Counter_Integer,
	Histogram_Integer,
	Histogram_Float,
	Histogram_Duration,
	Gauge_Integer,
	Gauge_Float,
	Up_Down_Counter_Integer,
}

Metric_Attribute_Kind :: enum {
	String,
	Integer,
	Float,
	Boolean,
}

// Metric attributes and all strings are borrowed for the synchronous emitter
// call. Workflow code cannot observe whether the host exporter accepted them.
Metric_Attribute :: struct {
	key:           string,
	kind:          Metric_Attribute_Kind,
	string_value:  string,
	integer_value: i64,
	float_value:   f64,
	boolean_value: bool,
}

metric_attribute_string :: proc(key, value: string) -> Metric_Attribute {
	return {key = key, kind = .String, string_value = value}
}

metric_attribute_i64 :: proc(key: string, value: i64) -> Metric_Attribute {
	return {key = key, kind = .Integer, integer_value = value}
}

metric_attribute_f64 :: proc(key: string, value: f64) -> Metric_Attribute {
	return {key = key, kind = .Float, float_value = value}
}

metric_attribute_bool :: proc(key: string, value: bool) -> Metric_Attribute {
	return {key = key, kind = .Boolean, boolean_value = value}
}

Metric_Record :: struct {
	name:                     string,
	kind:                     Metric_Kind,
	description:              string,
	unit:                     string,
	integer_value:            u64,
	signed_integer_value:     i64,
	float_value:              f64,
	duration_value:           time.Duration,
	attributes:               []Metric_Attribute,
	namespace:                string,
	workflow_id:              string,
	run_id:                   string,
	workflow_type:            string,
	task_queue:               string,
	replaying:                bool,
	replaying_history_events: bool,
}

Metric_Emit_Proc :: #type proc(user_data: rawptr, record: ^Metric_Record)
Metric_Destroy_Proc :: #type proc(user_data: rawptr)

// Metric_Emitter is owned by a Fiber_Runtime. emit is synchronous. destroy is
// called exactly once by fiber_runtime_close when user_data is non-nil.
Metric_Emitter :: struct {
	user_data: rawptr,
	emit:      Metric_Emit_Proc,
	destroy:   Metric_Destroy_Proc,
}

// Fiber_Context is deliberately narrow: Workflow code receives Temporal-aware
// operations, not a general worker/client handle. This remains an experiment.
Fiber_Context :: struct {
	instance:       ^Fiber_Instance,
	update_context: ^Update_Context,
	waiting_join_cases: []Selector_Case,
	waiting_join_cancellation_token_id: u32,
	waiting_selector_cases: []Selector_Case,
	waiting_selector_cancellation_token_id: u32,
	waiting_condition_data: rawptr,
	waiting_condition_predicate: Condition_Predicate,
	waiting_condition_cancellation_token_id: u32,
	waiting_mutex:  ^Mutex,
	waiting_mutex_cancellation_token_id: u32,
	mutex_granted:  bool,
	owned_mutexes:  [dynamic]^Mutex,
}

Fiber_Proc :: proc(ctx: ^Fiber_Context, input: string) -> string
Payload_Proc :: #type proc(ctx: ^Fiber_Context, input: codec.Payload_View) -> Payload_Result
Payloads_Proc :: #type proc(ctx: ^Fiber_Context, inputs: []codec.Payload_View) -> Payload_Result
Workflow_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	inputs: []codec.Payload_View,
) -> Payload_Result
Workflow_Callback_Destroy_Proc :: #type proc(callback_data: rawptr)
Query_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	input: codec.Payload_View,
) -> Payload_Result
Query_Context_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	query: ^Query_Context,
	input: codec.Payload_View,
) -> Payload_Result
Query_Payloads_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	inputs: []codec.Payload_View,
) -> Payload_Result
Query_Context_Payloads_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	query: ^Query_Context,
	inputs: []codec.Payload_View,
) -> Payload_Result
Update_Validate_Proc :: #type proc(
	callback_data: rawptr,
	input: codec.Payload_View,
) -> Update_Validation
Update_Context_Validate_Proc :: #type proc(
	callback_data: rawptr,
	update: ^Update_Context,
	input: codec.Payload_View,
) -> Update_Validation
Update_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	input: codec.Payload_View,
) -> Payload_Result
Update_Context_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	update: ^Update_Context,
	input: codec.Payload_View,
) -> Payload_Result
Update_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	input: codec.Payload_View,
) -> Payload_Result
Update_Context_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	update: ^Update_Context,
	input: codec.Payload_View,
) -> Payload_Result
Update_Payloads_Validate_Proc :: #type proc(
	callback_data: rawptr,
	inputs: []codec.Payload_View,
) -> Update_Validation
Update_Context_Payloads_Validate_Proc :: #type proc(
	callback_data: rawptr,
	update: ^Update_Context,
	inputs: []codec.Payload_View,
) -> Update_Validation
Update_Payloads_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	inputs: []codec.Payload_View,
) -> Payload_Result
Update_Context_Payloads_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	update: ^Update_Context,
	inputs: []codec.Payload_View,
) -> Payload_Result
Update_Payloads_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	inputs: []codec.Payload_View,
) -> Payload_Result
Update_Context_Payloads_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	update: ^Update_Context,
	inputs: []codec.Payload_View,
) -> Payload_Result
Signal_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	input: codec.Payload_View,
) -> Payload_Result
Signal_Context_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	signal: ^Signal_Context,
	input: codec.Payload_View,
) -> Payload_Result
Signal_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	input: codec.Payload_View,
) -> Payload_Result
Signal_Context_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	signal: ^Signal_Context,
	input: codec.Payload_View,
) -> Payload_Result
Signal_Payloads_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	inputs: []codec.Payload_View,
) -> Payload_Result
Signal_Context_Payloads_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	signal: ^Signal_Context,
	inputs: []codec.Payload_View,
) -> Payload_Result
Signal_Payloads_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	inputs: []codec.Payload_View,
) -> Payload_Result
Signal_Context_Payloads_Routine_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^Fiber_Context,
	signal: ^Signal_Context,
	inputs: []codec.Payload_View,
) -> Payload_Result

Payload_Result_Status :: enum {
	Completed,
	Failed,
}

Payload_Result :: struct {
	status:       Payload_Result_Status,
	payload:      codec.Payload,
	failure_message: string,
	failure_type:    string,
}

payload_complete :: proc(payload: codec.Payload) -> Payload_Result {
	return {status = .Completed, payload = payload}
}

payload_fail :: proc(
	message: string,
	failure_type := "OdinWorkflowError",
) -> Payload_Result {
	return {
		status = .Failed,
		failure_message = message,
		failure_type = failure_type,
	}
}

Update_Validation :: struct {
	accepted:     bool,
	message:      string,
	failure_type: string,
}

// Handler_Unfinished_Policy controls observability when the main Workflow
// closes while a concurrent Signal or Update routine is still running.
// Temporal cannot preserve that routine after Workflow close. The default
// mirrors the other Core-based SDKs: abandon it and report a warning through
// Process_Result.
Handler_Unfinished_Policy :: enum {
	Warn_And_Abandon,
	Abandon,
}

Update_Handler_Options :: struct {
	unfinished_policy: Handler_Unfinished_Policy,
}

Signal_Handler_Options :: struct {
	unfinished_policy: Handler_Unfinished_Policy,
}

update_accept :: proc() -> Update_Validation {
	return {accepted = true}
}

update_reject :: proc(
	message: string,
	failure_type := "OdinWorkflowUpdateRejected",
) -> Update_Validation {
	return {
		accepted = false,
		message = message,
		failure_type = failure_type,
	}
}

Activity_Cancellation_Type :: enum {
	Try_Cancel,
	Wait_Cancellation_Completed,
	Abandon,
}

// The zero value follows Core's required language-SDK default for Local
// Activities: wait until cancellation has been acknowledged by the Activity.
Local_Activity_Cancellation_Type :: enum {
	Wait_Cancellation_Completed,
	Try_Cancel,
	Abandon,
}

// The zero value deliberately matches the safest Core SDK default: a canceled
// parent waits until the child reports that cancellation completed.
Child_Workflow_Cancellation_Type :: enum {
	Wait_Cancellation_Completed,
	Abandon,
	Try_Cancel,
	Wait_Cancellation_Requested,
}

Parent_Close_Policy :: enum {
	Unspecified,
	Terminate,
	Abandon,
	Request_Cancel,
}

Workflow_Id_Reuse_Policy :: enum {
	Unspecified,
	Allow_Duplicate,
	Allow_Duplicate_Failed_Only,
	Reject_Duplicate,
	Terminate_If_Running,
}

Versioning_Intent :: enum {
	Unspecified,
	Compatible,
	Default,
}

// Child_Workflow_Options borrows all strings, maps, retry policy, and priority
// only for the start call. Empty task_queue uses the parent's task queue. Nil
// memo/search_attributes create no values; an initialized empty map is
// equivalent because a new Child Workflow has no inherited values. Empty
// static summary/details omit Temporal UserMetadata. A zero cancellation token
// inherits the token selected by the current Workflow fiber.
Child_Workflow_Options :: struct {
	workflow_id:               string,
	task_queue:                string,
	workflow_execution_timeout: time.Duration,
	workflow_run_timeout:       time.Duration,
	workflow_task_timeout:      time.Duration,
	retry_policy:              ^Retry_Policy,
	cancellation_type:         Child_Workflow_Cancellation_Type,
	parent_close_policy:       Parent_Close_Policy,
	id_reuse_policy:           Workflow_Id_Reuse_Policy,
	cron_schedule:             string,
	headers:                   ^Headers,
	memo:                      ^Payload_Map,
	search_attributes:         ^Payload_Map,
	versioning_intent:         Versioning_Intent,
	priority:                  ^Workflow_Priority,
	static_summary:            string,
	static_details:            string,
	cancellation_token:        Cancellation_Token,
}

Owned_Child_Workflow_Options :: struct {
	task_queue:                string,
	workflow_execution_timeout: time.Duration,
	workflow_run_timeout:       time.Duration,
	workflow_task_timeout:      time.Duration,
	retry_policy:              Retry_Policy,
	has_retry_policy:          bool,
	cancellation_type:         Child_Workflow_Cancellation_Type,
	parent_close_policy:       Parent_Close_Policy,
	id_reuse_policy:           Workflow_Id_Reuse_Policy,
	cron_schedule:             string,
	headers:                   [dynamic]Fiber_Named_Payload,
	memo:                      [dynamic]Fiber_Named_Payload,
	search_attributes:         [dynamic]Fiber_Named_Payload,
	has_search_attributes:     bool,
	versioning_intent:         Versioning_Intent,
	priority:                  Workflow_Priority,
	has_priority:              bool,
	static_summary:            codec.Payload,
	has_static_summary:        bool,
	static_details:            codec.Payload,
	has_static_details:        bool,
}

Retry_Policy :: struct {
	initial_interval:          time.Duration,
	backoff_coefficient:       f64,
	maximum_interval:          time.Duration,
	maximum_attempts:          i32,
	non_retryable_error_types: []string,
}

Workflow_Execution_Info :: struct {
	namespace:   string,
	workflow_id: string,
	run_id:      string,
}

Workflow_Priority :: struct {
	priority_key:    i32,
	fairness_key:    string,
	fairness_weight: f32,
}

Continue_Initiator :: enum {
	Unspecified,
	Workflow,
	Retry,
	Cron_Schedule,
}

// Worker_Deployment_Version borrows its strings from the Workflow run. The
// value remains valid until the run is evicted from the Worker cache.
Worker_Deployment_Version :: struct {
	deployment_name: string,
	build_id:        string,
}

// Info borrows all strings and slices from the current Workflow run. It is
// valid until the run is evicted from the Worker cache.
Info :: struct {
	namespace:   string,
	task_queue:  string,
	workflow_id: string,
	run_id:      string,
	workflow_type: string,
	identity:      string,
	attempt:       i32,
	first_execution_run_id:    string,
	original_execution_run_id: string,
	continued_from_run_id:     string,
	continued_initiator:       Continue_Initiator,
	cron_schedule:             string,
	cron_schedule_to_schedule_interval:     time.Duration,
	has_cron_schedule_to_schedule_interval: bool,
	continued_failure:     ^temporal_failure.Failure,
	has_continued_failure: bool,
	last_completion_results: []codec.Payload_View,
	execution_timeout:     time.Duration,
	has_execution_timeout: bool,
	run_timeout:           time.Duration,
	has_run_timeout:       bool,
	task_timeout:          time.Duration,
	has_task_timeout:      bool,
	start_time:            time.Time,
	has_start_time:        bool,
	execution_expiration_time:     time.Time,
	has_execution_expiration_time: bool,
	parent:     Workflow_Execution_Info,
	has_parent: bool,
	root:       Workflow_Execution_Info,
	has_root:   bool,
	retry_policy:     Retry_Policy,
	has_retry_policy: bool,
	priority: Workflow_Priority,
}

Search_Attribute_Type :: codec.Search_Attribute_Type

Headers :: temporal_headers.Headers
Payload_Map :: temporal_headers.Headers

headers_init :: proc(headers: ^Headers, allocator := context.allocator) -> bool {
	return temporal_headers.init(headers, allocator) == .None
}

headers_free :: proc(headers: ^Headers) {
	temporal_headers.free(headers)
}

headers_add_payload :: proc(headers: ^Headers, key: string, value: codec.Payload_View) -> bool {
	return temporal_headers.add_payload(headers, key, value) == .None
}

headers_add_json :: proc(headers: ^Headers, key: string, value: $T) -> bool {
	return temporal_headers.add_json(headers, key, value) == .None
}

payload_map_init :: proc(payloads: ^Payload_Map, allocator := context.allocator) -> bool {
	return temporal_headers.init(payloads, allocator) == .None
}

payload_map_free :: proc(payloads: ^Payload_Map) {
	temporal_headers.free(payloads)
}

payload_map_add_payload :: proc(
	payloads: ^Payload_Map,
	key: string,
	value: codec.Payload_View,
) -> bool {
	return temporal_headers.add_payload(payloads, key, value) == .None
}

payload_map_add_json :: proc(payloads: ^Payload_Map, key: string, value: $T) -> bool {
	return temporal_headers.add_json(payloads, key, value) == .None
}

payload_map_add_search_attribute_payload :: proc(
	payloads: ^Payload_Map,
	key: string,
	attribute_type: Search_Attribute_Type,
	value: codec.Payload_View,
) -> bool {
	if payloads == nil || payloads.allocator.procedure == nil {
		return false
	}
	payload, clone_error := codec.payload_clone(value, payloads.allocator)
	if clone_error != .None ||
	   codec.payload_set_search_attribute_type(&payload, attribute_type) != .None {
		codec.payload_free(&payload)
		return false
	}
	return temporal_headers.put_owned(payloads, key, payload) == .None
}

payload_map_add_search_attribute_json :: proc(
	payloads: ^Payload_Map,
	key: string,
	attribute_type: Search_Attribute_Type,
	value: $T,
) -> bool {
	if payloads == nil || payloads.allocator.procedure == nil {
		return false
	}
	payload, encode_error := codec.encode_json(value, payloads.allocator)
	if encode_error != .None ||
	   codec.payload_set_search_attribute_type(&payload, attribute_type) != .None {
		codec.payload_free(&payload)
		return false
	}
	return temporal_headers.put_owned(payloads, key, payload) == .None
}

// Signal_Options borrows headers, summary, and cancellation token only for the
// signal call. The runtime captures the token ID and copies retained values
// into the Workflow run arena before the signal suspends. Empty summary omits
// the UI-facing command metadata. A zero token inherits the token selected by
// the current Workflow fiber.
Signal_Options :: struct {
	headers: ^Headers,
	summary: string,
	cancellation_token: Cancellation_Token,
}

// Timer_Options borrows summary only for the start call. A zero-value
// cancellation token inherits the token selected by the current Workflow
// fiber. Empty summary means no UI-facing Timer summary.
Timer_Options :: struct {
	duration:           time.Duration,
	cancellation_token: Cancellation_Token,
	summary:            string,
}

// Activity_Options borrows its strings, slices, retry_policy, and priority only for the
// start/execute call. The Workflow runtime copies them into the run arena before
// it can suspend. A zero duration means "not specified". versioning_intent is
// the legacy Build-ID routing control retained by Core; new deployments should
// prefer Worker Deployment Versioning. A zero cancellation token inherits the
// token selected by the current Workflow fiber.
Activity_Options :: struct {
	activity_id:               string,
	task_queue:                string,
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	heartbeat_timeout:         time.Duration,
	retry_policy:              ^Retry_Policy,
	cancellation_type:         Activity_Cancellation_Type,
	disable_eager_execution:   bool,
	headers:                   ^Headers,
	versioning_intent:         Versioning_Intent,
	priority:                  ^Workflow_Priority,
	summary:                   string,
	cancellation_token:        Cancellation_Token,
}

Owned_Activity_Options :: struct {
	task_queue:                string,
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	heartbeat_timeout:         time.Duration,
	retry_policy:              Retry_Policy,
	has_retry_policy:          bool,
	cancellation_type:         Activity_Cancellation_Type,
	disable_eager_execution:   bool,
	headers:                   [dynamic]Fiber_Named_Payload,
	versioning_intent:         Versioning_Intent,
	priority:                  Workflow_Priority,
	has_priority:              bool,
	summary:                   codec.Payload,
	has_summary:               bool,
}

// Local Activities execute in the same Worker process as the Workflow but are
// still dispatched by Core as Activity tasks. They do not have a task queue or
// heartbeat timeout. Retries longer than Core's local threshold are bridged
// through durable Workflow timers without changing the public Activity_Future.
Local_Activity_Options :: struct {
	activity_id:               string,
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	retry_policy:              ^Retry_Policy,
	local_retry_threshold:     time.Duration,
	cancellation_type:         Local_Activity_Cancellation_Type,
	include_arguments_in_marker: bool,
	headers:                   ^Headers,
	summary:                   string,
	cancellation_token:        Cancellation_Token,
}

Owned_Local_Activity_Options :: struct {
	schedule_to_close_timeout: time.Duration,
	schedule_to_start_timeout: time.Duration,
	start_to_close_timeout:    time.Duration,
	retry_policy:              Retry_Policy,
	has_retry_policy:          bool,
	local_retry_threshold:     time.Duration,
	cancellation_type:         Local_Activity_Cancellation_Type,
	include_arguments_in_marker: bool,
	headers:                   [dynamic]Fiber_Named_Payload,
	summary:                   codec.Payload,
	has_summary:               bool,
}

Continue_As_New_Versioning_Intent :: Versioning_Intent

Continue_As_New_Versioning_Behavior :: enum {
	Unspecified,
	Auto_Upgrade,
	Use_Ramping_Version,
}

// Continue_As_New_Options borrows its strings, maps, and retry_policy only for
// the call. Empty workflow_type/task_queue values reuse the current run's
// registered values. Nil memo/search_attributes reuse the current values. An
// initialized empty Search Attribute map explicitly clears them; Core's proto3
// map cannot distinguish an empty memo override from an unset memo. A zero
// duration means "not specified".
Continue_As_New_Options :: struct {
	workflow_type:        string,
	task_queue:           string,
	workflow_run_timeout: time.Duration,
	workflow_task_timeout: time.Duration,
	retry_policy:         ^Retry_Policy,
	backoff_start_interval: time.Duration,
	memo:                 ^Payload_Map,
	headers:              ^Headers,
	search_attributes:    ^Payload_Map,
	versioning_intent:    Continue_As_New_Versioning_Intent,
	initial_versioning_behavior: Continue_As_New_Versioning_Behavior,
}

Owned_Continue_As_New_Options :: struct {
	workflow_type:        string,
	task_queue:           string,
	workflow_run_timeout: time.Duration,
	workflow_task_timeout: time.Duration,
	retry_policy:         Retry_Policy,
	has_retry_policy:     bool,
	backoff_start_interval: time.Duration,
	memo:                 [dynamic]Fiber_Named_Payload,
	headers:              [dynamic]Fiber_Named_Payload,
	search_attributes:    [dynamic]Fiber_Named_Payload,
	has_search_attributes: bool,
	versioning_intent:    Continue_As_New_Versioning_Intent,
	initial_versioning_behavior: Continue_As_New_Versioning_Behavior,
}

Fiber_Phase :: enum {
	Ready,
	Waiting_Condition,
	Waiting_Mutex,
	Waiting_Join,
	Waiting_Selector,
	Waiting_Activity,
	Waiting_Nexus_Operation,
	Waiting_Child_Workflow_Start,
	Waiting_Child_Workflow,
	Waiting_External_Signal,
	Waiting_External_Cancel,
	Waiting_Timer,
	Waiting_Signal,
	Waiting_Signal_Or_Timer,
	Waiting_Handlers,
	Continued_As_New,
	Canceled,
	Completed,
	Failed,
}

// Mutex is a zero-value, deterministic, Workflow-local FIFO mutex. It
// coordinates Odin fibers only; it never blocks an OS thread and has no server
// representation. Keep it in Workflow-owned state, do not copy it after first
// use, and do not share it across Workflow runs.
Mutex :: struct {
	instance:    ^Fiber_Instance,
	instance_id: u64,
	owner:       ^Fiber_Context,
	waiters:     [dynamic]^Fiber_Context,
}

Fiber_Operation_State :: enum {
	Scheduled,
	Commanded,
	Resolved,
	Failed,
	Cancel_Requested,
	Canceled,
}

Activity_Future :: struct {
	seq: u32,
}

Timer_Future :: struct {
	seq: u32,
}

Child_Workflow_Future :: struct {
	seq: u32,
}

Timer_Result_Status :: enum {
	Fired,
	Canceled,
}

Selector_Case_Kind :: enum {
	Activity,
	Nexus_Operation,
	Timer,
	Signal,
	Child_Workflow_Start,
	Child_Workflow,
}

// Selector_Case is a borrowed declaration of one deterministic select branch.
// Signal names and the cases slice must remain valid until selector_select
// returns; a suspended fiber naturally keeps stack-owned values alive.
Selector_Case :: struct {
	kind:           Selector_Case_Kind,
	activity:       Activity_Future,
	nexus_operation: Nexus_Operation_Future,
	timer:          Timer_Future,
	child_workflow: Child_Workflow_Future,
	signal_name:    string,
}

Selector_Status :: enum {
	Selected,
	Canceled,
}

Join_Status :: enum {
	Ready,
	Canceled,
}

Selector_Result :: struct {
	status: Selector_Status,
	index:  int,
	kind:   Selector_Case_Kind,
	// Populated only for a selected Signal case. Payload views and metadata
	// borrow the Workflow run.
	signal: Payloads_Signal,
}

Workflow_Signal_Status :: enum {
	Succeeded,
	Failed,
	Canceled,
}

Workflow_Signal_Failure_Cause :: enum {
	Unspecified,
	Workflow_Not_Found,
	Namespace_Not_Found,
	Signal_Count_Limit_Exceeded,
}

Workflow_Signal_Result :: struct {
	status:          Workflow_Signal_Status,
	failure_message: string,
	cause:           Workflow_Signal_Failure_Cause,
	// failure borrows the Workflow instance arena when Core supplied one.
	failure:         ^temporal_failure.Failure,
}

Child_Workflow_Signal_Status :: Workflow_Signal_Status
Child_Workflow_Signal_Failure_Cause :: Workflow_Signal_Failure_Cause
Child_Workflow_Signal_Result :: Workflow_Signal_Result
External_Workflow_Signal_Result :: Workflow_Signal_Result

External_Workflow_Execution :: struct {
	namespace:   string,
	workflow_id: string,
	run_id:      string,
}

External_Workflow_Cancel_Status :: enum {
	Succeeded,
	Failed,
}

External_Workflow_Cancel_Failure_Cause :: enum {
	Unspecified,
	Workflow_Not_Found,
	Namespace_Not_Found,
}

External_Workflow_Cancel_Result :: struct {
	status:          External_Workflow_Cancel_Status,
	failure_message: string,
	cause:           External_Workflow_Cancel_Failure_Cause,
	// failure borrows the Workflow instance arena when Core supplied one.
	failure:         ^temporal_failure.Failure,
}

Child_Workflow_Start_Status :: enum {
	Succeeded,
	Failed,
	Canceled,
}

Child_Workflow_Start_Failure_Cause :: enum {
	Unspecified,
	Workflow_Already_Exists,
}

Child_Workflow_Start_Result :: struct {
	status:        Child_Workflow_Start_Status,
	run_id:        string,
	workflow_id:   string,
	workflow_type: string,
	cause:         Child_Workflow_Start_Failure_Cause,
	failure_message: string,
}

Child_Workflow_Result_Status :: enum {
	Succeeded,
	Failed,
	Canceled,
}

Failure_Timeout_Type :: enum {
	Unspecified,
	Start_To_Close,
	Schedule_To_Start,
	Schedule_To_Close,
	Heartbeat,
}

Child_Workflow_Failure :: struct {
	// tree borrows the Workflow instance arena and must not be freed by Workflow code.
	tree:               ^temporal_failure.Failure,
	message:            string,
	source:             string,
	stack_trace:        string,
	namespace:          string,
	workflow_id:        string,
	run_id:             string,
	workflow_type:      string,
	initiated_event_id: i64,
	started_event_id:   i64,
	retry_state:        Activity_Retry_State,
	application:        Application_Failure,
	canceled:           bool,
	cancellation_details: []codec.Payload_View,
	timed_out:          bool,
	timeout_type:       Failure_Timeout_Type,
	terminated:         bool,
}

Payload_Child_Workflow_Result :: struct {
	status:          Child_Workflow_Result_Status,
	value:           codec.Payload_View,
	failure_message: string,
	failure:         Child_Workflow_Failure,
}

Json_Child_Workflow_Result :: struct($T: typeid) {
	status:          Child_Workflow_Result_Status,
	value:           T,
	failure_message: string,
	failure:         Child_Workflow_Failure,
}

Child_Workflow_Result :: struct {
	status:          Child_Workflow_Result_Status,
	value:           string,
	failure_message: string,
	failure:         Child_Workflow_Failure,
}

Child_Workflow_Start_State :: enum {
	Pending,
	Succeeded,
	Failed,
	Canceled,
}

Child_Workflow_Completion_State :: enum {
	Pending,
	Succeeded,
	Failed,
	Canceled,
}

Activity_Result_Status :: enum {
	Succeeded,
	Failed,
	Canceled,
}

Activity_Retry_State :: enum {
	Unspecified,
	In_Progress,
	Non_Retryable_Failure,
	Timed_Out,
	Maximum_Attempts_Reached,
	Retry_Policy_Not_Set,
	Internal_Server_Error,
	Cancel_Requested,
}

Application_Error_Category :: enum {
	Unspecified,
	Benign,
}

Application_Failure :: struct {
	present:              bool,
	message:              string,
	type:                 string,
	non_retryable:        bool,
	category:             Application_Error_Category,
	details:              []codec.Payload_View,
	next_retry_delay:     time.Duration,
	has_next_retry_delay: bool,
}

Activity_Failure :: struct {
	// tree borrows the Workflow instance arena and must not be freed by Workflow code.
	tree:          ^temporal_failure.Failure,
	message:       string,
	source:        string,
	stack_trace:   string,
	activity_id:   string,
	activity_type: string,
	identity:      string,
	retry_state:   Activity_Retry_State,
	application:   Application_Failure,
	canceled:      bool,
	cancellation_details: []codec.Payload_View,
}

Activity_Result :: struct {
	status:          Activity_Result_Status,
	value:           string,
	failure_message: string,
	failure:         Activity_Failure,
}

Payload_Activity_Result :: struct {
	status:          Activity_Result_Status,
	value:           codec.Payload_View,
	failure_message: string,
	failure:         Activity_Failure,
}

Json_Activity_Result :: struct(T: typeid) {
	status:          Activity_Result_Status,
	value:           T,
	failure_message: string,
	failure:         Activity_Failure,
}

Signal_Wait_Status :: enum {
	Received,
	Timed_Out,
	Canceled,
}

Signal_Wait_Result :: struct {
	status:   Signal_Wait_Status,
	value:    string,
	metadata: Signal_Context,
}

Payload_Signal_Wait_Result :: struct {
	status:   Signal_Wait_Status,
	value:    codec.Payload_View,
	metadata: Signal_Context,
}

Payloads_Signal_Wait_Result :: struct {
	status:   Signal_Wait_Status,
	values:   []codec.Payload_View,
	metadata: Signal_Context,
}

// Signal_Context is borrowed from the Workflow instance. Its strings, header
// Payload views, and the context itself must not escape Workflow execution.
Signal_Context :: struct {
	name:                 string,
	identity:             string,
	headers:              [dynamic]Fiber_Named_Payload,
	originating_event_id: i64,
}

Payload_Signal :: struct {
	value:    codec.Payload_View,
	metadata: Signal_Context,
}

Payloads_Signal :: struct {
	values:   []codec.Payload_View,
	metadata: Signal_Context,
}

Json_Signal :: struct(T: typeid) {
	value:    T,
	metadata: Signal_Context,
}

Json_Signal_Wait_Result :: struct(T: typeid) {
	status:   Signal_Wait_Status,
	value:    T,
	metadata: Signal_Context,
}

Cancellation_Scope :: struct {
	instance:                      ^Fiber_Instance,
	previous_token_id:             u32,
	selected_token_id:             u32,
	previous_detached_scope_depth: int,
	active:                        bool,
}

// Cancellation_Token is a borrowed handle into one Workflow run. It must not
// escape Workflow execution or be retained after cache eviction.
Cancellation_Token :: struct {
	instance: ^Fiber_Instance,
	id:       u32,
}

Fiber_Cancellation_Node :: struct {
	id:                     u32,
	parent_id:              u32,
	linked_to_root:         bool,
	cancellation_requested: bool,
	reason:                 string,
	has_deadline:           bool,
	deadline:               time.Time,
	deadline_timer_seq:     u32,
}

// Query_Context exists only for the synchronous query callback. Its identity,
// strings, and header Payload views are borrowed and must not escape it.
Query_Context :: struct {
	id:      string,
	type:    string,
	headers: [dynamic]Fiber_Named_Payload,
	instance: ^Fiber_Instance,
}

// Update_Context is owned by the Workflow run arena. Header Payload views and
// strings are valid until run eviction, but must not escape Workflow code.
Update_Context :: struct {
	id:       string,
	name:     string,
	identity: string,
	headers:  [dynamic]Fiber_Named_Payload,
}

Fiber_Query_Handler :: struct {
	name:                         string,
	callback_data:                rawptr,
	invoke:                       Query_Invoke_Proc,
	invoke_with_context:          Query_Context_Invoke_Proc,
	invoke_payloads:              Query_Payloads_Invoke_Proc,
	invoke_payloads_with_context: Query_Context_Payloads_Invoke_Proc,
}

Fiber_Signal_Handler :: struct {
	name:                        string,
	callback_data:               rawptr,
	invoke:                      Signal_Invoke_Proc,
	invoke_with_context:         Signal_Context_Invoke_Proc,
	invoke_routine:              Signal_Routine_Invoke_Proc,
	invoke_routine_with_context: Signal_Context_Routine_Invoke_Proc,
	invoke_payloads:              Signal_Payloads_Invoke_Proc,
	invoke_payloads_with_context: Signal_Context_Payloads_Invoke_Proc,
	invoke_payloads_routine:      Signal_Payloads_Routine_Invoke_Proc,
	invoke_payloads_routine_with_context: Signal_Context_Payloads_Routine_Invoke_Proc,
	unfinished_policy:           Handler_Unfinished_Policy,
}

Fiber_Update_Handler :: struct {
	name:                       string,
	callback_data:              rawptr,
	validate:                   Update_Validate_Proc,
	validate_with_context:      Update_Context_Validate_Proc,
	validate_payloads:          Update_Payloads_Validate_Proc,
	validate_payloads_with_context: Update_Context_Payloads_Validate_Proc,
	invoke:                     Update_Invoke_Proc,
	invoke_with_context:        Update_Context_Invoke_Proc,
	invoke_payloads:            Update_Payloads_Invoke_Proc,
	invoke_payloads_with_context: Update_Context_Payloads_Invoke_Proc,
	invoke_routine:             Update_Routine_Invoke_Proc,
	invoke_routine_with_context: Update_Context_Routine_Invoke_Proc,
	invoke_payloads_routine:    Update_Payloads_Routine_Invoke_Proc,
	invoke_payloads_routine_with_context: Update_Context_Payloads_Routine_Invoke_Proc,
	unfinished_policy: Handler_Unfinished_Policy,
}

Pending_Query_Response :: struct {
	query_id: string,
	result:   Payload_Result,
}

Update_Response_Kind :: enum {
	Accepted,
	Rejected,
	Completed,
}

Pending_Update_Response :: struct {
	protocol_instance_id: string,
	kind:                 Update_Response_Kind,
	result:               Payload_Result,
	// Borrowed from the Workflow instance until this activation completion has
	// been encoded. This preserves Core's recursive Activity/Child failure.
	failure_proto:         []u8,
}

Fiber_Activity_Operation :: struct {
	// seq is the stable, language-runtime handle exposed through Activity_Future.
	// command_seq identifies the current Core command. Local Activity retries keep
	// the former and allocate a fresh latter after their backoff timer fires.
	seq:           u32,
	command_seq:   u32,
	activity_id:   string,
	activity_type: string,
	is_local:      bool,
	attempt:       u32,
	backoff_timer_seq: u32,
	has_original_schedule_time: bool,
	original_schedule_seconds:  i64,
	original_schedule_nanos:    i32,
	options:       Owned_Activity_Options,
	local_options: Owned_Local_Activity_Options,
	inputs:        [dynamic]codec.Payload,
	result:        codec.Payload,
	failure_message: string,
	failure_proto:   []u8,
	failure:         Activity_Failure,
	failure_detail_views: []codec.Payload_View,
	cancellation_detail_views: []codec.Payload_View,
	state:         Fiber_Operation_State,
	cancellation_command_emitted: bool,
	detached_from_root_cancellation: bool,
	cancellation_token_id: u32,
}

Fiber_Named_Payload :: struct {
	key:     string,
	payload: codec.Payload,
}

Fiber_Property_Command_Kind :: enum {
	Memo,
	Search_Attribute,
}

Fiber_Property_Command :: struct {
	kind:    Fiber_Property_Command_Kind,
	key:     string,
	payload: codec.Payload,
	emitted: bool,
}

Fiber_Timer_Operation :: struct {
	seq:         u32,
	duration:    time.Duration,
	summary:     codec.Payload,
	has_summary: bool,
	state:       Fiber_Operation_State,
	detached_from_root_cancellation: bool,
	cancellation_token_id: u32,
}

Fiber_Child_Workflow_Operation :: struct {
	seq:           u32,
	workflow_id:   string,
	workflow_type: string,
	options:       Owned_Child_Workflow_Options,
	inputs:        [dynamic]codec.Payload,
	run_id:        string,
	result:        codec.Payload,
	failure_message: string,
	cancellation_reason: string,
	failure_proto:   []u8,
	failure:         Child_Workflow_Failure,
	failure_detail_views:     []codec.Payload_View,
	cancellation_detail_views: []codec.Payload_View,
	command_state:   Fiber_Operation_State,
	start_state:     Child_Workflow_Start_State,
	completion_state: Child_Workflow_Completion_State,
	start_failure_cause: Child_Workflow_Start_Failure_Cause,
	detached_from_root_cancellation: bool,
	cancellation_token_id: u32,
}

Fiber_External_Signal_Operation :: struct {
	seq:                u32,
	child_workflow_seq: u32,
	namespace:           string,
	child_workflow_id:  string,
	run_id:              string,
	signal_name:        string,
	inputs:             [dynamic]codec.Payload,
	headers:            [dynamic]Fiber_Named_Payload,
	summary:            codec.Payload,
	has_summary:        bool,
	failure_message:    string,
	failure:            ^temporal_failure.Failure,
	cause:              Workflow_Signal_Failure_Cause,
	state:              Fiber_Operation_State,
	detached_from_root_cancellation: bool,
	cancellation_token_id: u32,
}

Fiber_External_Cancel_Operation :: struct {
	seq:             u32,
	namespace:       string,
	workflow_id:     string,
	run_id:          string,
	reason:          string,
	failure_message: string,
	failure:         ^temporal_failure.Failure,
	cause:           External_Workflow_Cancel_Failure_Cause,
	state:           Fiber_Operation_State,
}

Fiber_Continue_As_New_Operation :: struct {
	inputs:  [dynamic]codec.Payload,
	options: Owned_Continue_As_New_Options,
	present: bool,
}

Fiber_Patch_Decision :: struct {
	patch_id:     string,
	present:      bool,
	deprecated:   bool,
	emit_command: bool,
	command_sent: bool,
}

Fiber_Signal :: struct {
	name:                 string,
	identity:             string,
	headers:              [dynamic]Fiber_Named_Payload,
	originating_event_id: i64,
	inputs:               [dynamic]codec.Payload,
	input_views:          []codec.Payload_View,
	consumed:             bool,
}

Fiber_Signal_Routine :: struct {
	instance:             ^Fiber_Instance,
	name:                 string,
	inputs:               [dynamic]codec.Payload,
	input_views:          []codec.Payload_View,
	signal_context:       Signal_Context,
	callback_data:        rawptr,
	invoke:               Signal_Routine_Invoke_Proc,
	invoke_with_context:  Signal_Context_Routine_Invoke_Proc,
	invoke_payloads:      Signal_Payloads_Routine_Invoke_Proc,
	invoke_payloads_with_context: Signal_Context_Payloads_Routine_Invoke_Proc,
	ctx:                  Fiber_Context,
	result:               Payload_Result,
	waiting_activity_seq: u32,
	waiting_nexus_operation_seq: u32,
	waiting_child_workflow_seq: u32,
	waiting_external_signal_seq: u32,
	waiting_external_cancel_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	detached_scope_depth: int,
	current_cancellation_token_id: u32,
	unhandled_activity_seq: u32,
	unhandled_nexus_operation_seq: u32,
	unhandled_child_workflow_seq: u32,
	failure_message:        string,
	failure_type:           string,
	phase:                  Fiber_Phase,
	fiber:                  fiber_raw.Handle,
	completion_observed:    bool,
	unfinished_policy:      Handler_Unfinished_Policy,
}

Fiber_Update_Routine :: struct {
	instance:             ^Fiber_Instance,
	protocol_instance_id: string,
	inputs:               [dynamic]codec.Payload,
	input_views:          []codec.Payload_View,
	callback_data:        rawptr,
	invoke:               Update_Routine_Invoke_Proc,
	invoke_with_context:  Update_Context_Routine_Invoke_Proc,
	invoke_payloads:      Update_Payloads_Routine_Invoke_Proc,
	invoke_payloads_with_context: Update_Context_Payloads_Routine_Invoke_Proc,
	update_context:       Update_Context,
	ctx:                  Fiber_Context,
	result:               Payload_Result,
	waiting_activity_seq: u32,
	waiting_nexus_operation_seq: u32,
	waiting_child_workflow_seq: u32,
	waiting_external_signal_seq: u32,
	waiting_external_cancel_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	detached_scope_depth: int,
	current_cancellation_token_id: u32,
	unhandled_activity_seq: u32,
	unhandled_nexus_operation_seq: u32,
	unhandled_child_workflow_seq: u32,
	failure_message:        string,
	failure_type:           string,
	phase:                  Fiber_Phase,
	fiber:                  fiber_raw.Handle,
	response_emitted:       bool,
	unfinished_policy:      Handler_Unfinished_Policy,
}

Fiber_Instance :: struct {
	runtime:         ^Fiber_Runtime,
	instance_id:     u64,
	namespace:       string,
	run_id:          string,
	workflow_id:     string,
	workflow_type:   string,
	task_queue:      string,
	info:            Info,
	headers:         [dynamic]Fiber_Named_Payload,
	memo:            [dynamic]Fiber_Named_Payload,
	search_attributes: [dynamic]Fiber_Named_Payload,
	property_commands: [dynamic]Fiber_Property_Command,
	continued_failure: temporal_failure.Failure,
	last_completion_results: [dynamic]codec.Payload,
	last_completion_result_views: []codec.Payload_View,
	inputs:          [dynamic]codec.Payload,
	input_views:     []codec.Payload_View,
	output:          codec.Payload,
	callback_data:   rawptr,
	invoke:          Workflow_Invoke_Proc,
	workflow_context: Fiber_Context,
	activities:       [dynamic]Fiber_Activity_Operation,
	nexus_operations: [dynamic]Fiber_Nexus_Operation,
	child_workflows:  [dynamic]Fiber_Child_Workflow_Operation,
	external_signals: [dynamic]Fiber_External_Signal_Operation,
	external_cancels: [dynamic]Fiber_External_Cancel_Operation,
	continue_as_new: Fiber_Continue_As_New_Operation,
	patches:          [dynamic]Fiber_Patch_Decision,
	notified_patches: [dynamic]string,
	timers:           [dynamic]Fiber_Timer_Operation,
	signals:          [dynamic]Fiber_Signal,
	signal_handlers:  [dynamic]Fiber_Signal_Handler,
	signal_routines:  [dynamic]^Fiber_Signal_Routine,
	query_handlers:   [dynamic]Fiber_Query_Handler,
	update_handlers:  [dynamic]Fiber_Update_Handler,
	update_routines:  [dynamic]^Fiber_Update_Routine,
	waiting_activity_seq: u32,
	waiting_nexus_operation_seq: u32,
	waiting_child_workflow_seq: u32,
	waiting_external_signal_seq: u32,
	waiting_external_cancel_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	cancellation_reason:  string,
	cancellation_requested: bool,
	detached_scope_depth:   int,
	cancellation_nodes: [dynamic]Fiber_Cancellation_Node,
	current_cancellation_token_id: u32,
	next_cancellation_token_id: u32,
	terminal_command_sent: bool,
	terminal_failure_requested: bool,
	terminal_failure_message:   string,
	terminal_failure_type:      string,
	unhandled_activity_seq: u32,
	unhandled_nexus_operation_seq: u32,
	unhandled_child_workflow_seq: u32,
	failure_message:       string,
	failure_type:          string,
	next_activity_seq: u32,
	next_nexus_operation_seq: u32,
	next_child_workflow_seq: u32,
	next_external_signal_seq: u32,
	next_external_cancel_seq: u32,
	next_timer_seq:    u32,
	activation_is_replaying: bool,
	activation_is_replaying_history_events: bool,
	activation_history_length: u32,
	activation_history_size_bytes: u64,
	activation_continue_as_new_suggested: bool,
	activation_target_worker_deployment_version_changed: bool,
	current_deployment_version: Worker_Deployment_Version,
	has_current_deployment_version: bool,
	activation_time:         time.Time,
	has_activation_time:     bool,
	current_details:         string,
	activation_result:       ^Process_Result,
	activation_error_message: string,
	random_state:            u64,
	random_initialized:      bool,
	phase:           Fiber_Phase,
	fiber:           fiber_raw.Handle,
	arena:           virtual.Arena,
	odin_context:    allocator_runtime.Context,
	owner_allocator: allocator_runtime.Allocator,
}

Fiber_Runtime :: struct {
	namespace:              string,
	workflow_type:          string,
	task_queue:             string,
	callback_data:          rawptr,
	invoke:                 Workflow_Invoke_Proc,
	callback_destroy:       Workflow_Callback_Destroy_Proc,
	instances:              [dynamic]^Fiber_Instance,
	next_instance_id:       u64,
	inbound_interceptors:    [dynamic]Inbound_Interceptor,
	outbound_interceptors:   [dynamic]Outbound_Interceptor,
	activations:            int,
	replay_activations:     int,
	replay_initializations: int,
	evictions:              int,
	stack_options:          Stack_Options,
	logger:                 Logger,
	metric_emitter:         Metric_Emitter,
}

fiber_runtime_set_logger :: proc(runtime: ^Fiber_Runtime, logger: Logger) -> Error {
	if runtime == nil || runtime.callback_data == nil {
		return error_from_string("Workflow runtime is not initialized")
	}
	if len(runtime.instances) != 0 {
		return error_from_string("Workflow logger cannot be changed after execution starts")
	}
	runtime.logger = logger
	return {}
}

fiber_runtime_set_metric_emitter :: proc(
	runtime: ^Fiber_Runtime,
	emitter: Metric_Emitter,
) -> Error {
	if runtime == nil || runtime.callback_data == nil {
		return error_from_string("Workflow runtime is not initialized")
	}
	if len(runtime.instances) != 0 {
		return error_from_string("Workflow metric emitter cannot be changed after execution starts")
	}
	if runtime.metric_emitter.user_data != nil && runtime.metric_emitter.destroy != nil {
		runtime.metric_emitter.destroy(runtime.metric_emitter.user_data)
	}
	runtime.metric_emitter = emitter
	return {}
}

// Minicoro's compiled minimum is 32 KiB. Keeping the same limit here avoids
// silently accepting a smaller value which the C runtime would round up.
MIN_FIBER_STACK_SIZE :: 32 * 1024
DEFAULT_FIBER_STACK_SIZE :: 128 * 1024
MAX_FIBER_STACK_SIZE :: 16 * 1024 * 1024
FIBER_STACK_ALIGNMENT :: 16

// Stack_Options bounds the native memory reserved by every live Workflow and
// asynchronous handler fiber. Zero selects DEFAULT_FIBER_STACK_SIZE. Signal
// and Update handler sizes are independent because every concurrent handler
// owns its own stack.
Stack_Options :: struct {
	workflow:       int,
	signal_handler: int,
	update_handler: int,
}

resolve_stack_size :: proc(value: int) -> (int, bool) {
	if value == 0 {
		return DEFAULT_FIBER_STACK_SIZE, true
	}
	if value < MIN_FIBER_STACK_SIZE || value > MAX_FIBER_STACK_SIZE {
		return 0, false
	}
	return ((value + FIBER_STACK_ALIGNMENT - 1) / FIBER_STACK_ALIGNMENT) * FIBER_STACK_ALIGNMENT, true
}

stack_options_resolve :: proc(options: Stack_Options) -> (resolved: Stack_Options, error: Error) {
	workflow_size, workflow_ok := resolve_stack_size(options.workflow)
	signal_size, signal_ok := resolve_stack_size(options.signal_handler)
	update_size, update_ok := resolve_stack_size(options.update_handler)
	if !workflow_ok || !signal_ok || !update_ok {
		return {}, error_from_string("Workflow fiber stack sizes must be zero or between 32 KiB and 16 MiB")
	}
	resolved.workflow = workflow_size
	resolved.signal_handler = signal_size
	resolved.update_handler = update_size
	return
}

fiber_runtime_init_invoker :: proc(
	runtime: ^Fiber_Runtime,
	workflow_type, task_queue: string,
	callback_data: rawptr,
	invoke: Workflow_Invoke_Proc,
	callback_destroy: Workflow_Callback_Destroy_Proc,
	namespace := "default",
	stack_options := Stack_Options{},
) -> Error {
	if runtime == nil || len(namespace) == 0 || len(workflow_type) == 0 ||
	   len(task_queue) == 0 || callback_data == nil || invoke == nil ||
	   callback_destroy == nil {
		return error_from_string("namespace, workflow type, task queue, and workflow procedure are required")
	}
	resolved_stacks, stack_error := stack_options_resolve(stack_options)
	if stack_error.message != nil {
		return stack_error
	}
	owned_namespace, namespace_error := strings.clone(namespace)
	if namespace_error != nil {
		return error_from_string("could not allocate workflow namespace")
	}
	owned_type, type_error := strings.clone(workflow_type)
	if type_error != nil {
		delete(owned_namespace)
		return error_from_string("could not allocate workflow type")
	}
	owned_queue, queue_error := strings.clone(task_queue)
	if queue_error != nil {
		delete(owned_namespace)
		delete(owned_type)
		return error_from_string("could not allocate workflow task queue")
	}
	runtime.namespace = owned_namespace
	runtime.workflow_type = owned_type
	runtime.task_queue = owned_queue
	runtime.callback_data = callback_data
	runtime.invoke = invoke
	runtime.callback_destroy = callback_destroy
	runtime.instances = make([dynamic]^Fiber_Instance)
	runtime.inbound_interceptors = make([dynamic]Inbound_Interceptor)
	runtime.outbound_interceptors = make([dynamic]Outbound_Interceptor)
	runtime.stack_options = resolved_stacks
	return {}
}

fiber_runtime_add_inbound_interceptor :: proc(
	runtime: ^Fiber_Runtime,
	interceptor: Inbound_Interceptor,
) -> Error {
	if runtime == nil || runtime.callback_data == nil {
		return error_from_string("Workflow runtime is not initialized")
	}
	if len(runtime.instances) != 0 {
		return error_from_string("Workflow interceptors cannot be registered after execution starts")
	}
	if !inbound_interceptor_has_operation(interceptor) {
		return error_from_string("Workflow inbound interceptor must implement an operation")
	}
	append(&runtime.inbound_interceptors, interceptor)
	return {}
}

fiber_runtime_add_outbound_interceptor :: proc(
	runtime: ^Fiber_Runtime,
	interceptor: Outbound_Interceptor,
) -> Error {
	if runtime == nil || runtime.callback_data == nil {
		return error_from_string("Workflow runtime is not initialized")
	}
	if len(runtime.instances) != 0 {
		return error_from_string("Workflow interceptors cannot be registered after execution starts")
	}
	if !outbound_interceptor_has_operation(interceptor) {
		return error_from_string("Workflow outbound interceptor must implement an operation")
	}
	append(&runtime.outbound_interceptors, interceptor)
	return {}
}

fiber_runtime_init :: proc(
	runtime: ^Fiber_Runtime,
	workflow_type, task_queue: string,
	workflow: Fiber_Proc,
	namespace := "default",
	stack_options := Stack_Options{},
) -> Error {
	if workflow == nil {
		return error_from_string("workflow type, task queue, and workflow procedure are required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Fiber_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Workflow callback")
	}
	owned_callback^ = workflow
	init_error := fiber_runtime_init_invoker(
		runtime,
		workflow_type,
		task_queue,
		owned_callback,
		proc(data: rawptr, ctx: ^Fiber_Context, inputs: []codec.Payload_View) -> Payload_Result {
			if len(inputs) != 1 {
				return payload_fail(
					"string Workflow requires exactly one argument",
					"OdinWorkflowArgumentCountError",
				)
			}
			value: string
			if codec.decode_json(inputs[0], &value) != .None {
				return payload_fail(
					"could not decode Workflow input as json/plain string",
					"OdinWorkflowPayloadConversionError",
				)
			}
			defer delete(value)
			callback := cast(^Fiber_Proc)data
			output := callback^(ctx, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow result as json/plain string",
					"OdinWorkflowPayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
		proc(data: rawptr) {
			free(cast(^Fiber_Proc)data, allocator_runtime.heap_allocator())
		},
		namespace,
		stack_options,
	)
	if init_error.message != nil {
		free(owned_callback, allocator)
	}
	return init_error
}

fiber_runtime_init_payload :: proc(
	runtime: ^Fiber_Runtime,
	workflow_type, task_queue: string,
	workflow: Payload_Proc,
	namespace := "default",
	stack_options := Stack_Options{},
) -> Error {
	if workflow == nil {
		return error_from_string("workflow type, task queue, and workflow procedure are required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Payload_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Workflow callback")
	}
	owned_callback^ = workflow
	init_error := fiber_runtime_init_invoker(
		runtime,
		workflow_type,
		task_queue,
		owned_callback,
		proc(data: rawptr, ctx: ^Fiber_Context, inputs: []codec.Payload_View) -> Payload_Result {
			if len(inputs) != 1 {
				return payload_fail(
					"Payload Workflow requires exactly one argument",
					"OdinWorkflowArgumentCountError",
				)
			}
			callback := cast(^Payload_Proc)data
			return callback^(ctx, inputs[0])
		},
		proc(data: rawptr) {
			free(cast(^Payload_Proc)data, allocator_runtime.heap_allocator())
		},
		namespace,
		stack_options,
	)
	if init_error.message != nil {
		free(owned_callback, allocator)
	}
	return init_error
}

fiber_runtime_init_payloads :: proc(
	runtime: ^Fiber_Runtime,
	workflow_type, task_queue: string,
	workflow: Payloads_Proc,
	namespace := "default",
	stack_options := Stack_Options{},
) -> Error {
	if workflow == nil {
		return error_from_string("workflow type, task queue, and workflow procedure are required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Payloads_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Workflow callback")
	}
	owned_callback^ = workflow
	init_error := fiber_runtime_init_invoker(
		runtime,
		workflow_type,
		task_queue,
		owned_callback,
		proc(data: rawptr, ctx: ^Fiber_Context, inputs: []codec.Payload_View) -> Payload_Result {
			callback := cast(^Payloads_Proc)data
			return callback^(ctx, inputs)
		},
		proc(data: rawptr) {
			free(cast(^Payloads_Proc)data, allocator_runtime.heap_allocator())
		},
		namespace,
		stack_options,
	)
	if init_error.message != nil {
		free(owned_callback, allocator)
	}
	return init_error
}

fiber_result_error :: proc(operation: string, result: fiber_raw.Result) -> Error {
	description := fiber_raw.result_description(result)
	message, allocation_error := strings.concatenate({operation, ": ", string(description)})
	if allocation_error != nil {
		return error_from_string(operation)
	}
	defer delete(message)
	return error_from_string(message)
}

Fiber_Execution_State :: struct {
	waiting_activity_seq: u32,
	waiting_nexus_operation_seq: u32,
	waiting_child_workflow_seq: u32,
	waiting_external_signal_seq: u32,
	waiting_external_cancel_seq: u32,
	waiting_timer_seq:    u32,
	waiting_signal_name:  string,
	detached_scope_depth: int,
	current_cancellation_token_id: u32,
	unhandled_activity_seq: u32,
	unhandled_nexus_operation_seq: u32,
	unhandled_child_workflow_seq: u32,
	failure_message:        string,
	failure_type:           string,
	phase:                  Fiber_Phase,
	fiber:                  fiber_raw.Handle,
}

capture_instance_execution :: proc(instance: ^Fiber_Instance) -> Fiber_Execution_State {
	return {
		waiting_activity_seq = instance.waiting_activity_seq,
		waiting_nexus_operation_seq = instance.waiting_nexus_operation_seq,
		waiting_child_workflow_seq = instance.waiting_child_workflow_seq,
		waiting_external_signal_seq = instance.waiting_external_signal_seq,
		waiting_external_cancel_seq = instance.waiting_external_cancel_seq,
		waiting_timer_seq = instance.waiting_timer_seq,
		waiting_signal_name = instance.waiting_signal_name,
		detached_scope_depth = instance.detached_scope_depth,
		current_cancellation_token_id = instance.current_cancellation_token_id,
		unhandled_activity_seq = instance.unhandled_activity_seq,
		unhandled_nexus_operation_seq = instance.unhandled_nexus_operation_seq,
		unhandled_child_workflow_seq = instance.unhandled_child_workflow_seq,
		failure_message = instance.failure_message,
		failure_type = instance.failure_type,
		phase = instance.phase,
		fiber = instance.fiber,
	}
}

apply_instance_execution :: proc(instance: ^Fiber_Instance, state: Fiber_Execution_State) {
	instance.waiting_activity_seq = state.waiting_activity_seq
	instance.waiting_nexus_operation_seq = state.waiting_nexus_operation_seq
	instance.waiting_child_workflow_seq = state.waiting_child_workflow_seq
	instance.waiting_external_signal_seq = state.waiting_external_signal_seq
	instance.waiting_external_cancel_seq = state.waiting_external_cancel_seq
	instance.waiting_timer_seq = state.waiting_timer_seq
	instance.waiting_signal_name = state.waiting_signal_name
	instance.detached_scope_depth = state.detached_scope_depth
	instance.current_cancellation_token_id = state.current_cancellation_token_id
	instance.unhandled_activity_seq = state.unhandled_activity_seq
	instance.unhandled_nexus_operation_seq = state.unhandled_nexus_operation_seq
	instance.unhandled_child_workflow_seq = state.unhandled_child_workflow_seq
	instance.failure_message = state.failure_message
	instance.failure_type = state.failure_type
	instance.phase = state.phase
	instance.fiber = state.fiber
}

signal_routine_execution :: proc(routine: ^Fiber_Signal_Routine) -> Fiber_Execution_State {
	return {
		waiting_activity_seq = routine.waiting_activity_seq,
		waiting_nexus_operation_seq = routine.waiting_nexus_operation_seq,
		waiting_child_workflow_seq = routine.waiting_child_workflow_seq,
		waiting_external_signal_seq = routine.waiting_external_signal_seq,
		waiting_external_cancel_seq = routine.waiting_external_cancel_seq,
		waiting_timer_seq = routine.waiting_timer_seq,
		waiting_signal_name = routine.waiting_signal_name,
		detached_scope_depth = routine.detached_scope_depth,
		current_cancellation_token_id = routine.current_cancellation_token_id,
		unhandled_activity_seq = routine.unhandled_activity_seq,
		unhandled_nexus_operation_seq = routine.unhandled_nexus_operation_seq,
		unhandled_child_workflow_seq = routine.unhandled_child_workflow_seq,
		failure_message = routine.failure_message,
		failure_type = routine.failure_type,
		phase = routine.phase,
		fiber = routine.fiber,
	}
}

capture_signal_routine_execution :: proc(
	routine: ^Fiber_Signal_Routine,
	instance: ^Fiber_Instance,
) {
	routine.waiting_activity_seq = instance.waiting_activity_seq
	routine.waiting_nexus_operation_seq = instance.waiting_nexus_operation_seq
	routine.waiting_child_workflow_seq = instance.waiting_child_workflow_seq
	routine.waiting_external_signal_seq = instance.waiting_external_signal_seq
	routine.waiting_external_cancel_seq = instance.waiting_external_cancel_seq
	routine.waiting_timer_seq = instance.waiting_timer_seq
	routine.waiting_signal_name = instance.waiting_signal_name
	routine.detached_scope_depth = instance.detached_scope_depth
	routine.current_cancellation_token_id = instance.current_cancellation_token_id
	routine.unhandled_activity_seq = instance.unhandled_activity_seq
	routine.unhandled_nexus_operation_seq = instance.unhandled_nexus_operation_seq
	routine.unhandled_child_workflow_seq = instance.unhandled_child_workflow_seq
	routine.failure_message = instance.failure_message
	routine.failure_type = instance.failure_type
	routine.phase = instance.phase
	routine.fiber = instance.fiber
}

resume_signal_routine :: proc(routine: ^Fiber_Signal_Routine) -> Error {
	if routine == nil || routine.instance == nil || routine.fiber == nil {
		return error_from_string("Workflow Signal routine is not initialized")
	}
	instance := routine.instance
	main_execution := capture_instance_execution(instance)
	apply_instance_execution(instance, signal_routine_execution(routine))
	resume_result := fiber_raw.resume(routine.fiber)
	capture_signal_routine_execution(routine, instance)
	apply_instance_execution(instance, main_execution)
	if resume_result != .Success {
		return fiber_result_error("could not resume Workflow Signal routine", resume_result)
	}
	return {}
}

signal_routine_entry :: proc "c" (user_data: rawptr) {
	routine := cast(^Fiber_Signal_Routine)user_data
	instance := routine.instance
	context = instance.odin_context
	interceptor_input := Inbound_Handle_Signal_Input{
		ctx = &routine.ctx,
		workflow_type = instance.workflow_type,
		signal = &routine.signal_context,
		inputs = routine.input_views,
	}
	interceptor_state := Inbound_Handle_Signal_State{
		instance = instance,
		routine = routine,
	}
	result := inbound_handle_signal_chain(&interceptor_state, &interceptor_input)
	released_mutex := mutex_release_owned(&routine.ctx)
	if instance.phase == .Failed {
		codec.payload_free(&result.payload)
		message := instance.failure_message
		failure_type := instance.failure_type
		if instance.unhandled_activity_seq != 0 {
			operation, found := find_activity_operation(instance, instance.unhandled_activity_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowSignalActivityFailure"
			}
		}
		if instance.unhandled_nexus_operation_seq != 0 {
			operation, found := find_nexus_operation(instance, instance.unhandled_nexus_operation_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowSignalNexusOperationFailure"
			}
		}
		if instance.unhandled_child_workflow_seq != 0 {
			operation, found := find_child_workflow_operation(instance, instance.unhandled_child_workflow_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowSignalChildWorkflowFailure"
			}
		}
		if len(message) == 0 {
			message = "Workflow Signal routine failed"
		}
		routine.result = payload_fail(message, failure_type)
		return
	}
	if released_mutex && result.status == .Completed {
		codec.payload_free(&result.payload)
		routine.result = payload_fail(
			"Workflow Signal routine returned while owning a mutex",
			"OdinWorkflowMutexOwnershipError",
		)
		instance.phase = .Failed
		return
	}
	routine.result = result
	if result.status == .Failed {
		instance.phase = .Failed
		return
	}
	instance.phase = .Completed
}

update_routine_execution :: proc(routine: ^Fiber_Update_Routine) -> Fiber_Execution_State {
	return {
		waiting_activity_seq = routine.waiting_activity_seq,
		waiting_nexus_operation_seq = routine.waiting_nexus_operation_seq,
		waiting_child_workflow_seq = routine.waiting_child_workflow_seq,
		waiting_external_signal_seq = routine.waiting_external_signal_seq,
		waiting_external_cancel_seq = routine.waiting_external_cancel_seq,
		waiting_timer_seq = routine.waiting_timer_seq,
		waiting_signal_name = routine.waiting_signal_name,
		detached_scope_depth = routine.detached_scope_depth,
		current_cancellation_token_id = routine.current_cancellation_token_id,
		unhandled_activity_seq = routine.unhandled_activity_seq,
		unhandled_nexus_operation_seq = routine.unhandled_nexus_operation_seq,
		unhandled_child_workflow_seq = routine.unhandled_child_workflow_seq,
		failure_message = routine.failure_message,
		failure_type = routine.failure_type,
		phase = routine.phase,
		fiber = routine.fiber,
	}
}

capture_update_routine_execution :: proc(
	routine: ^Fiber_Update_Routine,
	instance: ^Fiber_Instance,
) {
	routine.waiting_activity_seq = instance.waiting_activity_seq
	routine.waiting_nexus_operation_seq = instance.waiting_nexus_operation_seq
	routine.waiting_child_workflow_seq = instance.waiting_child_workflow_seq
	routine.waiting_external_signal_seq = instance.waiting_external_signal_seq
	routine.waiting_external_cancel_seq = instance.waiting_external_cancel_seq
	routine.waiting_timer_seq = instance.waiting_timer_seq
	routine.waiting_signal_name = instance.waiting_signal_name
	routine.detached_scope_depth = instance.detached_scope_depth
	routine.current_cancellation_token_id = instance.current_cancellation_token_id
	routine.unhandled_activity_seq = instance.unhandled_activity_seq
	routine.unhandled_nexus_operation_seq = instance.unhandled_nexus_operation_seq
	routine.unhandled_child_workflow_seq = instance.unhandled_child_workflow_seq
	routine.failure_message = instance.failure_message
	routine.failure_type = instance.failure_type
	routine.phase = instance.phase
	routine.fiber = instance.fiber
}

resume_update_routine :: proc(routine: ^Fiber_Update_Routine) -> Error {
	if routine == nil || routine.instance == nil || routine.fiber == nil {
		return error_from_string("Workflow Update routine is not initialized")
	}
	instance := routine.instance
	main_execution := capture_instance_execution(instance)
	apply_instance_execution(instance, update_routine_execution(routine))
	resume_result := fiber_raw.resume(routine.fiber)
	capture_update_routine_execution(routine, instance)
	apply_instance_execution(instance, main_execution)
	if resume_result != .Success {
		return fiber_result_error("could not resume Workflow Update routine", resume_result)
	}
	return {}
}

update_routine_entry :: proc "c" (user_data: rawptr) {
	routine := cast(^Fiber_Update_Routine)user_data
	instance := routine.instance
	context = instance.odin_context
	interceptor_input := Inbound_Handle_Update_Input{
		ctx = &routine.ctx,
		workflow_type = instance.workflow_type,
		update = &routine.update_context,
		inputs = routine.input_views,
	}
	interceptor_state := Inbound_Handle_Update_Handler_State{
		instance = instance,
		routine = routine,
	}
	result := inbound_handle_update_handler_chain(&interceptor_state, &interceptor_input)
	released_mutex := mutex_release_owned(&routine.ctx)
	if instance.phase == .Failed {
		codec.payload_free(&result.payload)
		message := instance.failure_message
		failure_type := instance.failure_type
		if instance.unhandled_activity_seq != 0 {
			operation, found := find_activity_operation(instance, instance.unhandled_activity_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowUpdateActivityFailure"
			}
		}
		if instance.unhandled_nexus_operation_seq != 0 {
			operation, found := find_nexus_operation(instance, instance.unhandled_nexus_operation_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowUpdateNexusOperationFailure"
			}
		}
		if instance.unhandled_child_workflow_seq != 0 {
			operation, found := find_child_workflow_operation(instance, instance.unhandled_child_workflow_seq)
			if found {
				message = operation.failure_message
				failure_type = "OdinWorkflowUpdateChildWorkflowFailure"
			}
		}
		if len(message) == 0 {
			message = "Workflow Update routine failed"
		}
		routine.result = payload_fail(message, failure_type)
		return
	}
	if released_mutex && result.status == .Completed {
		codec.payload_free(&result.payload)
		routine.result = payload_fail(
			"Workflow Update routine returned while owning a mutex",
			"OdinWorkflowMutexOwnershipError",
		)
		instance.phase = .Failed
		return
	}
	routine.result = result
	if result.status == .Failed {
		instance.phase = .Failed
		return
	}
	instance.phase = .Completed
}

fiber_instance_destroy :: proc(instance: ^Fiber_Instance) -> Error {
	if instance == nil {
		return {}
	}
	for routine in instance.signal_routines {
		if routine.fiber == nil {
			continue
		}
		routine_result := fiber_raw.destroy(routine.fiber)
		if routine_result != .Success {
			return fiber_result_error("could not destroy Workflow Signal routine", routine_result)
		}
		routine.fiber = nil
	}
	for routine in instance.update_routines {
		if routine.fiber == nil {
			continue
		}
		routine_result := fiber_raw.destroy(routine.fiber)
		if routine_result != .Success {
			return fiber_result_error("could not destroy Workflow Update routine", routine_result)
		}
		routine.fiber = nil
	}
	fiber_result := fiber_raw.destroy(instance.fiber)
	if fiber_result != .Success {
		return fiber_result_error("could not destroy Workflow fiber", fiber_result)
	}
	instance.fiber = nil
	for &input in instance.inputs {
		codec.payload_free(&input)
	}
	delete(instance.inputs)
	codec.payload_free(&instance.output)
	temporal_failure.failure_free(&instance.continued_failure)
	for &payload in instance.last_completion_results {
		codec.payload_free(&payload)
	}
	delete(instance.last_completion_results)
	for &operation in instance.activities {
		for &input in operation.inputs {
			codec.payload_free(&input)
		}
		delete(operation.inputs)
		codec.payload_free(&operation.result)
	}
	for &operation in instance.nexus_operations {
		codec.payload_free(&operation.input)
		codec.payload_free(&operation.result)
	}
	for &operation in instance.child_workflows {
		for &input in operation.inputs {
			codec.payload_free(&input)
		}
		delete(operation.inputs)
		codec.payload_free(&operation.result)
	}
	for &operation in instance.external_signals {
		for &input in operation.inputs {
			codec.payload_free(&input)
		}
		delete(operation.inputs)
	}
	if instance.continue_as_new.present {
		for &input in instance.continue_as_new.inputs {
			codec.payload_free(&input)
		}
		delete(instance.continue_as_new.inputs)
	}
	for &signal in instance.signals {
		for &input in signal.inputs {
			codec.payload_free(&input)
		}
		delete(signal.inputs)
		for &entry in signal.headers {
			codec.payload_free(&entry.payload)
		}
	}
	delete(instance.current_details, instance.owner_allocator)
	virtual.arena_destroy(&instance.arena)
	free(instance, instance.owner_allocator)
	return {}
}

fiber_runtime_close :: proc(runtime: ^Fiber_Runtime) {
	if runtime == nil {
		return
	}
	for instance in runtime.instances {
		_ = fiber_instance_destroy(instance)
	}
	delete(runtime.instances)
	delete(runtime.inbound_interceptors)
	delete(runtime.outbound_interceptors)
	delete(runtime.namespace)
	delete(runtime.workflow_type)
	delete(runtime.task_queue)
	if runtime.callback_data != nil && runtime.callback_destroy != nil {
		runtime.callback_destroy(runtime.callback_data)
	}
	if runtime.metric_emitter.user_data != nil && runtime.metric_emitter.destroy != nil {
		runtime.metric_emitter.destroy(runtime.metric_emitter.user_data)
	}
	runtime^ = {}
}

find_fiber_instance :: proc(runtime: ^Fiber_Runtime, run_id: string) -> (^Fiber_Instance, bool) {
	for instance in runtime.instances {
		if instance.run_id == run_id {
			return instance, true
		}
	}
	return nil, false
}

fiber_runtime_has_run :: proc(runtime: ^Fiber_Runtime, run_id: string) -> bool {
	if runtime == nil {
		return false
	}
	_, found := find_fiber_instance(runtime, run_id)
	return found
}

remove_fiber_instance :: proc(runtime: ^Fiber_Runtime, run_id: string) -> Error {
	for index in 0..<len(runtime.instances) {
		if runtime.instances[index].run_id == run_id {
			instance := runtime.instances[index]
			destroy_error := fiber_instance_destroy(instance)
			if destroy_error.message != nil {
				return destroy_error
			}
			ordered_remove(&runtime.instances, index)
			return {}
		}
	}
	return {}
}

fiber_entry :: proc "c" (user_data: rawptr) {
	instance := (^Fiber_Instance)(user_data)
	context = instance.odin_context
	execute_input := Inbound_Execute_Workflow_Input{
		ctx = &instance.workflow_context,
		workflow_type = instance.workflow_type,
		inputs = instance.input_views,
	}
	execute_state := Inbound_Execute_Workflow_State{instance = instance}
	workflow_result := inbound_execute_workflow_chain(&execute_state, &execute_input)
	released_mutex := mutex_release_owned(&instance.workflow_context)
	if instance.phase == .Failed {
		codec.payload_free(&workflow_result.payload)
		return
	}
	if released_mutex && workflow_result.status == .Completed {
		codec.payload_free(&workflow_result.payload)
		instance.failure_message = "Workflow returned while owning a mutex"
		instance.failure_type = "OdinWorkflowMutexOwnershipError"
		instance.phase = .Failed
		return
	}
	if workflow_result.status == .Failed {
		instance.failure_message = workflow_result.failure_message
		instance.failure_type = workflow_result.failure_type
		instance.phase = .Failed
		return
	}
	if instance.cancellation_requested {
		codec.payload_free(&workflow_result.payload)
		instance.phase = .Canceled
		return
	}
	instance.output = workflow_result.payload
	instance.phase = .Completed
}

workflow_continue_initiator_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_ContinueAsNewInitiator,
) -> Continue_Initiator {
	switch value {
	case .CONTINUE_AS_NEW_INITIATOR_WORKFLOW:
		return .Workflow
	case .CONTINUE_AS_NEW_INITIATOR_RETRY:
		return .Retry
	case .CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE:
		return .Cron_Schedule
	case .CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED:
		return .Unspecified
	}
	return .Unspecified
}

workflow_info_duration :: proc(
	value: ^proto.Google_Protobuf_Duration,
) -> (duration: time.Duration, present, ok: bool) {
	if value == nil {
		return 0, false, true
	}
	if value.seconds < 0 || value.nanos < 0 || value.nanos >= 1_000_000_000 ||
	   value.seconds > 9_223_372_036 ||
	   (value.seconds == 9_223_372_036 && value.nanos > 854_775_807) {
		return 0, true, false
	}
	return time.Duration(value.seconds * i64(time.Second) + i64(value.nanos)), true, true
}

workflow_info_time :: proc(
	value: ^proto.Google_Protobuf_Timestamp,
) -> (result: time.Time, present, ok: bool) {
	if value == nil {
		return {}, false, true
	}
	if value.nanos < 0 || value.nanos >= 1_000_000_000 {
		return {}, true, false
	}
	return time.unix(value.seconds, i64(value.nanos)), true, true
}

clone_named_proto_payloads :: proc(
	entries: []proto.Temporal_Api_Common_V1_Memo_FieldsEntry,
	result: ^[dynamic]Fiber_Named_Payload,
) -> bool {
	for entry in entries {
		if len(entry.key) == 0 || entry.value == nil {
			return false
		}
		key, key_error := strings.clone(entry.key)
		payload, payload_error := proto.clone_payload(entry.value)
		if key_error != nil || payload_error != .None {
			if key_error == nil {
				delete(key)
			}
			codec.payload_free(&payload)
			return false
		}
		append(result, Fiber_Named_Payload{key = key, payload = payload})
	}
	return true
}

clone_named_header_payloads :: proc(
	entries: []proto.Coresdk_Workflow_Activation_InitializeWorkflow_HeadersEntry,
	result: ^[dynamic]Fiber_Named_Payload,
) -> bool {
	for entry in entries {
		if len(entry.key) == 0 || entry.value == nil {
			return false
		}
		key, key_error := strings.clone(entry.key)
		payload, payload_error := proto.clone_payload(entry.value)
		if key_error != nil || payload_error != .None {
			return false
		}
		append(result, Fiber_Named_Payload{key = key, payload = payload})
	}
	return true
}

clone_query_header_payloads :: proc(
	entries: []proto.Coresdk_Workflow_Activation_QueryWorkflow_HeadersEntry,
	result: ^[dynamic]Fiber_Named_Payload,
) -> bool {
	for entry in entries {
		if len(entry.key) == 0 || entry.value == nil {
			return false
		}
		key, key_error := strings.clone(entry.key)
		payload, payload_error := proto.clone_payload(entry.value)
		if key_error != nil || payload_error != .None {
			if key_error == nil {
				delete(key)
			}
			codec.payload_free(&payload)
			return false
		}
		append(result, Fiber_Named_Payload{key = key, payload = payload})
	}
	return true
}

clone_signal_header_payloads :: proc(
	entries: []proto.Coresdk_Workflow_Activation_SignalWorkflow_HeadersEntry,
	result: ^[dynamic]Fiber_Named_Payload,
) -> bool {
	for entry in entries {
		if len(entry.key) == 0 || entry.value == nil {
			return false
		}
		key, key_error := strings.clone(entry.key)
		payload, payload_error := proto.clone_payload(entry.value)
		if key_error != nil || payload_error != .None {
			if key_error == nil {
				delete(key)
			}
			codec.payload_free(&payload)
			return false
		}
		append(result, Fiber_Named_Payload{key = key, payload = payload})
	}
	return true
}

clone_workflow_headers :: proc(
	headers: ^Headers,
	result: ^[dynamic]Fiber_Named_Payload,
	allocator := context.allocator,
) -> bool {
	if headers == nil {
		return true
	}
	if headers.allocator.procedure == nil || result == nil {
		return false
	}
	result^ = make([dynamic]Fiber_Named_Payload, allocator)
	for &entry in headers.entries {
		key, key_error := strings.clone(entry.key, allocator)
		payload, payload_error := codec.payload_clone(codec.payload_view(&entry.payload), allocator)
		if key_error != nil || payload_error != .None {
			return false
		}
		append(result, Fiber_Named_Payload{key = key, payload = payload})
	}
	return true
}

clone_workflow_payload_map :: proc(
	payloads: ^Payload_Map,
	result: ^[dynamic]Fiber_Named_Payload,
	allocator := context.allocator,
) -> bool {
	return clone_workflow_headers(payloads, result, allocator)
}

clone_named_search_payloads :: proc(
	entries: []proto.Temporal_Api_Common_V1_SearchAttributes_IndexedFieldsEntry,
	result: ^[dynamic]Fiber_Named_Payload,
) -> bool {
	for entry in entries {
		if len(entry.key) == 0 || entry.value == nil {
			return false
		}
		key, key_error := strings.clone(entry.key)
		payload, payload_error := proto.clone_payload(entry.value)
		if key_error != nil || payload_error != .None {
			return false
		}
		append(result, Fiber_Named_Payload{key = key, payload = payload})
	}
	return true
}

clone_workflow_info_string :: proc(value: string, result: ^string) -> bool {
	cloned, clone_error := strings.clone(value)
	if clone_error != nil {
		return false
	}
	result^ = cloned
	return true
}

add_fiber_instance :: proc(
	runtime: ^Fiber_Runtime,
	run_id: string,
	initialize: ^proto.Coresdk_Workflow_Activation_InitializeWorkflow,
) -> (^Fiber_Instance, Error) {
	if initialize == nil {
		return nil, error_from_string("workflow initialization is missing")
	}

	instance := new(Fiber_Instance)
	instance.runtime = runtime
	runtime.next_instance_id += 1
	if runtime.next_instance_id == 0 {
		runtime.next_instance_id = 1
	}
	instance.instance_id = runtime.next_instance_id
	instance.owner_allocator = context.allocator
	arena_error := virtual.arena_init_growing(&instance.arena)
	if arena_error != nil {
		free(instance, instance.owner_allocator)
		return nil, error_from_string("could not initialize Workflow instance arena")
	}

	instance.odin_context = context
	instance.odin_context.allocator = virtual.arena_allocator(&instance.arena)
	previous_allocator := context.allocator
	context.allocator = instance.odin_context.allocator
	owned_namespace, namespace_error := strings.clone(runtime.namespace)
	instance.namespace = owned_namespace
	owned_run_id, run_id_error := strings.clone(run_id)
	instance.run_id = owned_run_id
	owned_workflow_id, workflow_id_error := strings.clone(initialize.workflow_id)
	instance.workflow_id = owned_workflow_id
	owned_workflow_type, workflow_type_error := strings.clone(runtime.workflow_type)
	instance.workflow_type = owned_workflow_type
	owned_task_queue, task_queue_error := strings.clone(runtime.task_queue)
	instance.task_queue = owned_task_queue
	instance.inputs = make([dynamic]codec.Payload)
	inputs_ok := true
	for &source in initialize.arguments {
		owned_input, clone_error := proto.clone_payload(&source)
		if clone_error != .None {
			inputs_ok = false
			break
		}
		append(&instance.inputs, owned_input)
	}
	instance.input_views = make([]codec.Payload_View, len(instance.inputs))
	for &input, index in instance.inputs {
		instance.input_views[index] = codec.payload_view(&input)
	}
	instance.headers = make([dynamic]Fiber_Named_Payload)
	instance.memo = make([dynamic]Fiber_Named_Payload)
	instance.search_attributes = make([dynamic]Fiber_Named_Payload)
	instance.property_commands = make([dynamic]Fiber_Property_Command)
	instance.last_completion_results = make([dynamic]codec.Payload)
	instance.activities = make([dynamic]Fiber_Activity_Operation)
	instance.nexus_operations = make([dynamic]Fiber_Nexus_Operation)
	instance.child_workflows = make([dynamic]Fiber_Child_Workflow_Operation)
	instance.external_signals = make([dynamic]Fiber_External_Signal_Operation)
	instance.external_cancels = make([dynamic]Fiber_External_Cancel_Operation)
	instance.patches = make([dynamic]Fiber_Patch_Decision)
	instance.notified_patches = make([dynamic]string)
	instance.timers = make([dynamic]Fiber_Timer_Operation)
	instance.signals = make([dynamic]Fiber_Signal)
	instance.signal_handlers = make([dynamic]Fiber_Signal_Handler)
	instance.signal_routines = make([dynamic]^Fiber_Signal_Routine)
	instance.query_handlers = make([dynamic]Fiber_Query_Handler)
	instance.update_handlers = make([dynamic]Fiber_Update_Handler)
	instance.update_routines = make([dynamic]^Fiber_Update_Routine)
	instance.cancellation_nodes = make([dynamic]Fiber_Cancellation_Node)
	instance.next_cancellation_token_id = 1
	instance.current_cancellation_token_id = 1
	append(
		&instance.cancellation_nodes,
		Fiber_Cancellation_Node{id = 1, linked_to_root = true},
	)

	info := &instance.info
	info.namespace = instance.namespace
	info.task_queue = instance.task_queue
	info.workflow_id = instance.workflow_id
	info.run_id = instance.run_id
	info.workflow_type = instance.workflow_type
	metadata_strings_ok := clone_workflow_info_string(initialize.identity, &info.identity)
	metadata_strings_ok = clone_workflow_info_string(
		initialize.first_execution_run_id,
		&info.first_execution_run_id,
	) && metadata_strings_ok
	metadata_strings_ok = clone_workflow_info_string(
		initialize.original_execution_run_id,
		&info.original_execution_run_id,
	) && metadata_strings_ok
	metadata_strings_ok = clone_workflow_info_string(
		initialize.continued_from_execution_run_id,
		&info.continued_from_run_id,
	) && metadata_strings_ok
	metadata_strings_ok = clone_workflow_info_string(
		initialize.cron_schedule,
		&info.cron_schedule,
	) && metadata_strings_ok
	info.attempt = initialize.attempt
	info.continued_initiator = workflow_continue_initiator_from_proto(initialize.continued_initiator)
	cron_interval, has_cron_interval, cron_interval_ok := workflow_info_duration(
		initialize.cron_schedule_to_schedule_interval,
	)
	info.cron_schedule_to_schedule_interval = cron_interval
	info.has_cron_schedule_to_schedule_interval = has_cron_interval
	continued_failure_ok := true
	if initialize.continued_failure != nil {
		continued_failure, failure_error := temporal_failure.clone_failure(
			initialize.continued_failure,
			instance.odin_context.allocator,
			0,
		)
		continued_failure_ok = failure_error == .None
		if continued_failure_ok {
			instance.continued_failure = continued_failure
			info.continued_failure = &instance.continued_failure
			info.has_continued_failure = true
		}
	}
	last_completion_results_ok := true
	if initialize.last_completion_result != nil {
		for &source in initialize.last_completion_result.payloads {
			payload, payload_error := proto.clone_payload(&source)
			if payload_error != .None {
				last_completion_results_ok = false
				break
			}
			append(&instance.last_completion_results, payload)
		}
	}
	if last_completion_results_ok {
		instance.last_completion_result_views = make(
			[]codec.Payload_View,
			len(instance.last_completion_results),
		)
		for &payload, index in instance.last_completion_results {
			instance.last_completion_result_views[index] = codec.payload_view(&payload)
		}
		info.last_completion_results = instance.last_completion_result_views
	}
	execution_timeout, has_execution_timeout, execution_timeout_ok := workflow_info_duration(
		initialize.workflow_execution_timeout,
	)
	info.execution_timeout = execution_timeout
	info.has_execution_timeout = has_execution_timeout
	run_timeout, has_run_timeout, run_timeout_ok := workflow_info_duration(
		initialize.workflow_run_timeout,
	)
	info.run_timeout = run_timeout
	info.has_run_timeout = has_run_timeout
	task_timeout, has_task_timeout, task_timeout_ok := workflow_info_duration(
		initialize.workflow_task_timeout,
	)
	info.task_timeout = task_timeout
	info.has_task_timeout = has_task_timeout
	start_time, has_start_time, start_time_ok := workflow_info_time(initialize.start_time)
	info.start_time = start_time
	info.has_start_time = has_start_time
	expiration_time, has_expiration_time, expiration_ok := workflow_info_time(
		initialize.workflow_execution_expiration_time,
	)
	info.execution_expiration_time = expiration_time
	info.has_execution_expiration_time = has_expiration_time
	if initialize.parent_workflow_info != nil {
		info.has_parent = true
		metadata_strings_ok = clone_workflow_info_string(
			initialize.parent_workflow_info.namespace,
			&info.parent.namespace,
		) && metadata_strings_ok
		metadata_strings_ok = clone_workflow_info_string(
			initialize.parent_workflow_info.workflow_id,
			&info.parent.workflow_id,
		) && metadata_strings_ok
		metadata_strings_ok = clone_workflow_info_string(
			initialize.parent_workflow_info.run_id,
			&info.parent.run_id,
		) && metadata_strings_ok
	}
	if initialize.root_workflow != nil {
		info.has_root = true
		metadata_strings_ok = clone_workflow_info_string(
			initialize.root_workflow.workflow_id,
			&info.root.workflow_id,
		) && metadata_strings_ok
		metadata_strings_ok = clone_workflow_info_string(
			initialize.root_workflow.run_id,
			&info.root.run_id,
		) && metadata_strings_ok
	}
	retry_durations_ok := true
	if initialize.retry_policy != nil {
		info.has_retry_policy = true
		policy := initialize.retry_policy
		retry_initial, _, retry_initial_ok := workflow_info_duration(policy.initial_interval)
		info.retry_policy.initial_interval = retry_initial
		info.retry_policy.backoff_coefficient = policy.backoff_coefficient
		retry_maximum, _, retry_maximum_ok := workflow_info_duration(policy.maximum_interval)
		info.retry_policy.maximum_interval = retry_maximum
		retry_durations_ok = retry_initial_ok && retry_maximum_ok
		info.retry_policy.maximum_attempts = policy.maximum_attempts
		info.retry_policy.non_retryable_error_types = make([]string, len(policy.non_retryable_error_types))
		for error_type, index in policy.non_retryable_error_types {
			metadata_strings_ok = clone_workflow_info_string(
				error_type,
				&info.retry_policy.non_retryable_error_types[index],
			) && metadata_strings_ok
		}
	}
	if initialize.priority != nil {
		info.priority.priority_key = initialize.priority.priority_key
		metadata_strings_ok = clone_workflow_info_string(
			initialize.priority.fairness_key,
			&info.priority.fairness_key,
		) && metadata_strings_ok
		info.priority.fairness_weight = initialize.priority.fairness_weight
	}
	headers_ok := clone_named_header_payloads(initialize.headers[:], &instance.headers)
	memo_ok := initialize.memo == nil || clone_named_proto_payloads(initialize.memo.fields[:], &instance.memo)
	search_ok := initialize.search_attributes == nil || clone_named_search_payloads(
		initialize.search_attributes.indexed_fields[:],
		&instance.search_attributes,
	)
	context.allocator = previous_allocator
	if namespace_error != nil || run_id_error != nil || workflow_id_error != nil ||
	   workflow_type_error != nil || task_queue_error != nil || !inputs_ok ||
	   !metadata_strings_ok || !retry_durations_ok || !cron_interval_ok ||
	   !continued_failure_ok || !last_completion_results_ok ||
	   !execution_timeout_ok || !run_timeout_ok || !task_timeout_ok || !start_time_ok ||
	   !expiration_ok || !headers_ok || !memo_ok || !search_ok {
		virtual.arena_destroy(&instance.arena)
		free(instance, instance.owner_allocator)
		return nil, error_from_string("could not preserve Workflow initialization")
	}

	instance.callback_data = runtime.callback_data
	instance.invoke = runtime.invoke
	instance.workflow_context.instance = instance
	instance.phase = .Ready
	fiber_result := fiber_raw.create(
		&instance.fiber,
		fiber_entry,
		instance,
		uint(runtime.stack_options.workflow),
	)
	if fiber_result != .Success {
		virtual.arena_destroy(&instance.arena)
		free(instance, instance.owner_allocator)
		return nil, fiber_result_error("could not create Workflow fiber", fiber_result)
	}
	append(&runtime.instances, instance)
	return instance, {}
}

find_activity_operation :: proc(instance: ^Fiber_Instance, seq: u32) -> (^Fiber_Activity_Operation, bool) {
	for index in 0..<len(instance.activities) {
		if instance.activities[index].seq == seq {
			return &instance.activities[index], true
		}
	}
	return nil, false
}

find_activity_operation_by_command_seq :: proc(
	instance: ^Fiber_Instance,
	command_seq: u32,
) -> (^Fiber_Activity_Operation, bool) {
	for index in 0..<len(instance.activities) {
		if instance.activities[index].command_seq == command_seq {
			return &instance.activities[index], true
		}
	}
	return nil, false
}

find_local_activity_by_backoff_timer :: proc(
	instance: ^Fiber_Instance,
	timer_seq: u32,
) -> (^Fiber_Activity_Operation, bool) {
	for index in 0..<len(instance.activities) {
		operation := &instance.activities[index]
		if operation.is_local && operation.backoff_timer_seq == timer_seq {
			return operation, true
		}
	}
	return nil, false
}

find_child_workflow_operation :: proc(
	instance: ^Fiber_Instance,
	seq: u32,
) -> (^Fiber_Child_Workflow_Operation, bool) {
	for index in 0..<len(instance.child_workflows) {
		if instance.child_workflows[index].seq == seq {
			return &instance.child_workflows[index], true
		}
	}
	return nil, false
}

find_external_signal_operation :: proc(
	instance: ^Fiber_Instance,
	seq: u32,
) -> (^Fiber_External_Signal_Operation, bool) {
	for index in 0..<len(instance.external_signals) {
		if instance.external_signals[index].seq == seq {
			return &instance.external_signals[index], true
		}
	}
	return nil, false
}

find_external_cancel_operation :: proc(
	instance: ^Fiber_Instance,
	seq: u32,
) -> (^Fiber_External_Cancel_Operation, bool) {
	for index in 0..<len(instance.external_cancels) {
		if instance.external_cancels[index].seq == seq {
			return &instance.external_cancels[index], true
		}
	}
	return nil, false
}

find_timer_operation :: proc(instance: ^Fiber_Instance, seq: u32) -> (^Fiber_Timer_Operation, bool) {
	for index in 0..<len(instance.timers) {
		if instance.timers[index].seq == seq {
			return &instance.timers[index], true
		}
	}
	return nil, false
}

find_buffered_signal :: proc(instance: ^Fiber_Instance, name: string) -> (^Fiber_Signal, bool) {
	for index in 0..<len(instance.signals) {
		signal := &instance.signals[index]
		if !signal.consumed && signal.name == name {
			return signal, true
		}
	}
	return nil, false
}

find_query_handler :: proc(
	instance: ^Fiber_Instance,
	name: string,
) -> (^Fiber_Query_Handler, bool) {
	if instance == nil {
		return nil, false
	}
	for index in 0..<len(instance.query_handlers) {
		handler := &instance.query_handlers[index]
		if handler.name == name {
			return handler, true
		}
	}
	return nil, false
}

WORKFLOW_METADATA_QUERY_TYPE :: "__temporal_workflow_metadata"

workflow_query_name_reserved :: proc(name: string) -> bool {
	return name == WORKFLOW_METADATA_QUERY_TYPE
}

query_registration_failed :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowQueryRegistrationError"
	instance.phase = .Failed
}

set_query_handler_payload :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Query_Invoke_Proc,
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || workflow_query_name_reserved(name) ||
	   callback_data == nil || callback == nil {
		if ctx != nil {
			query_registration_failed(ctx.instance, "invalid Workflow query handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_query_handler(instance, name)
	if duplicate {
		query_registration_failed(instance, "Workflow query handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		query_registration_failed(instance, "could not preserve Workflow query handler name")
		return false
	}
	append(
		&instance.query_handlers,
		Fiber_Query_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke = callback,
		},
	)
	return true
}

// set_query_handler_payloads exposes Temporal's complete ordered Query
// argument list to a synchronous, read-only callback.
set_query_handler_payloads :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Query_Payloads_Invoke_Proc,
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || workflow_query_name_reserved(name) ||
	   callback_data == nil || callback == nil {
		if ctx != nil {
			query_registration_failed(ctx.instance, "invalid repeated-input Workflow query handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_query_handler(instance, name)
	if duplicate {
		query_registration_failed(instance, "Workflow query handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		query_registration_failed(instance, "could not preserve Workflow query handler name")
		return false
	}
	append(
		&instance.query_handlers,
		Fiber_Query_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke_payloads = callback,
		},
	)
	return true
}

set_query_handler_payload_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Query_Context_Invoke_Proc,
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || workflow_query_name_reserved(name) ||
	   callback_data == nil || callback == nil {
		if ctx != nil {
			query_registration_failed(ctx.instance, "invalid contextual Workflow query handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_query_handler(instance, name)
	if duplicate {
		query_registration_failed(instance, "Workflow query handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		query_registration_failed(instance, "could not preserve Workflow query handler name")
		return false
	}
	append(
		&instance.query_handlers,
		Fiber_Query_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke_with_context = callback,
		},
	)
	return true
}

set_query_handler_payloads_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Query_Context_Payloads_Invoke_Proc,
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || workflow_query_name_reserved(name) ||
	   callback_data == nil || callback == nil {
		if ctx != nil {
			query_registration_failed(
				ctx.instance,
				"invalid contextual repeated-input Workflow query handler",
			)
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_query_handler(instance, name)
	if duplicate {
		query_registration_failed(instance, "Workflow query handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		query_registration_failed(instance, "could not preserve Workflow query handler name")
		return false
	}
	append(
		&instance.query_handlers,
		Fiber_Query_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke_payloads_with_context = callback,
		},
	)
	return true
}

// set_query_handler_json registers a synchronous, read-only query view over
// Workflow-owned state. The callback receives a top-level copy of State and no
// Fiber_Context, so it cannot schedule Temporal commands through the normal API.
set_query_handler_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(state: State, input: $Input) -> $Output,
) -> bool {
	Callback :: #type proc(state: State, input: Input) -> Output
	Data :: struct {
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			query_registration_failed(ctx.instance, "invalid typed Workflow query handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		query_registration_failed(ctx.instance, "could not allocate typed Workflow query handler")
		return false
	}
	data.state = state
	data.callback = callback
	return set_query_handler_payload(
		ctx,
		name,
		data,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow query input as json/plain",
					"OdinWorkflowQueryPayloadConversionError",
				)
			}
			output := owned.callback(owned.state^, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow query result as json/plain",
					"OdinWorkflowQueryPayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
	)
}

// set_query_handler_json_with_context adds read-only, invocation-scoped query
// metadata without exposing the command-capable Fiber_Context.
set_query_handler_json_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(query: ^Query_Context, state: State, input: $Input) -> $Output,
) -> bool {
	Callback :: #type proc(query: ^Query_Context, state: State, input: Input) -> Output
	Data :: struct {
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			query_registration_failed(ctx.instance, "invalid typed contextual Workflow query handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		query_registration_failed(ctx.instance, "could not allocate typed contextual Workflow query handler")
		return false
	}
	data.state = state
	data.callback = callback
	return set_query_handler_payload_with_context(
		ctx,
		name,
		data,
		proc(
			callback_data: rawptr,
			query: ^Query_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow query input as json/plain",
					"OdinWorkflowQueryPayloadConversionError",
				)
			}
			output := owned.callback(query, owned.state^, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow query result as json/plain",
					"OdinWorkflowQueryPayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
	)
}

find_signal_handler :: proc(
	instance: ^Fiber_Instance,
	name: string,
) -> (^Fiber_Signal_Handler, bool) {
	if instance == nil {
		return nil, false
	}
	for index in 0..<len(instance.signal_handlers) {
		handler := &instance.signal_handlers[index]
		if handler.name == name {
			return handler, true
		}
	}
	return nil, false
}

signal_registration_failed :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowSignalRegistrationError"
	instance.phase = .Failed
}

record_signal_activation_error :: proc(instance: ^Fiber_Instance, dispatch_error: ^Error) {
	if instance == nil || dispatch_error == nil || dispatch_error.message == nil ||
	   len(instance.activation_error_message) > 0 {
		return
	}
	message, allocation_error := strings.clone(
		string(dispatch_error.message),
		instance.odin_context.allocator,
	)
	if allocation_error != nil {
		instance.activation_error_message = "Workflow Signal handler failed"
		return
	}
	instance.activation_error_message = message
}

finish_signal_handler_registration :: proc(instance: ^Fiber_Instance, name: string) -> bool {
	dispatch_error := dispatch_buffered_signals_for_handler(
		instance,
		name,
		instance.activation_result,
	)
	if dispatch_error.message != nil {
		record_signal_activation_error(instance, &dispatch_error)
		error_free(&dispatch_error)
	}
	return true
}

set_signal_handler_payload :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Signal_Invoke_Proc,
	routine_callback: Signal_Routine_Invoke_Proc = nil,
	options := Signal_Handler_Options{},
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || (callback == nil) == (routine_callback == nil) {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid Workflow Signal handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_signal_handler(instance, name)
	if duplicate {
		signal_registration_failed(instance, "Workflow Signal handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(instance, "could not preserve Workflow Signal handler name")
		return false
	}
	append(
		&instance.signal_handlers,
		Fiber_Signal_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke = callback,
			invoke_routine = routine_callback,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return finish_signal_handler_registration(instance, owned_name)
}

// set_signal_handler_payloads exposes Temporal's repeated Signal argument
// boundary. Exactly one of callback and routine_callback must be provided.
set_signal_handler_payloads :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Signal_Payloads_Invoke_Proc,
	routine_callback: Signal_Payloads_Routine_Invoke_Proc = nil,
	options := Signal_Handler_Options{},
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || (callback == nil) == (routine_callback == nil) {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid repeated-input Workflow Signal handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_signal_handler(instance, name)
	if duplicate {
		signal_registration_failed(instance, "Workflow Signal handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(instance, "could not preserve Workflow Signal handler name")
		return false
	}
	append(
		&instance.signal_handlers,
		Fiber_Signal_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke_payloads = callback,
			invoke_payloads_routine = routine_callback,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return finish_signal_handler_registration(instance, owned_name)
}

set_signal_routine_payload_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Signal_Context_Routine_Invoke_Proc,
	options := Signal_Handler_Options{},
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid contextual Workflow Signal routine")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_signal_handler(instance, name)
	if duplicate {
		signal_registration_failed(instance, "Workflow Signal handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(instance, "could not preserve Workflow Signal handler name")
		return false
	}
	append(
		&instance.signal_handlers,
		Fiber_Signal_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke_routine_with_context = callback,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return finish_signal_handler_registration(instance, owned_name)
}

set_signal_routine_payloads_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Signal_Context_Payloads_Routine_Invoke_Proc,
	options := Signal_Handler_Options{},
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid contextual repeated-input Workflow Signal routine")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_signal_handler(instance, name)
	if duplicate {
		signal_registration_failed(instance, "Workflow Signal handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(instance, "could not preserve Workflow Signal handler name")
		return false
	}
	append(
		&instance.signal_handlers,
		Fiber_Signal_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke_payloads_routine_with_context = callback,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return finish_signal_handler_registration(instance, owned_name)
}

set_signal_handler_payload_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Signal_Context_Invoke_Proc,
	options := Signal_Handler_Options{},
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid contextual Workflow Signal handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_signal_handler(instance, name)
	if duplicate {
		signal_registration_failed(instance, "Workflow Signal handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(instance, "could not preserve Workflow Signal handler name")
		return false
	}
	append(
		&instance.signal_handlers,
		Fiber_Signal_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke_with_context = callback,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return finish_signal_handler_registration(instance, owned_name)
}

set_signal_handler_payloads_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	callback: Signal_Context_Payloads_Invoke_Proc,
	options := Signal_Handler_Options{},
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid contextual repeated-input Workflow Signal handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_signal_handler(instance, name)
	if duplicate {
		signal_registration_failed(instance, "Workflow Signal handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(instance, "could not preserve Workflow Signal handler name")
		return false
	}
	append(
		&instance.signal_handlers,
		Fiber_Signal_Handler{
			name = owned_name,
			callback_data = callback_data,
			invoke_payloads_with_context = callback,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return finish_signal_handler_registration(instance, owned_name)
}

// set_signal_handler_json registers an immediate deterministic Signal handler.
// It may mutate Workflow-owned state but cannot suspend or schedule operations.
set_signal_handler_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(state: ^State, input: $Input),
	options := Signal_Handler_Options{},
) -> bool {
	Callback :: #type proc(state: ^State, input: Input)
	Data :: struct {
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid typed Workflow Signal handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(ctx.instance, "could not allocate typed Workflow Signal handler")
		return false
	}
	data.state = state
	data.callback = callback
	return set_signal_handler_payload(
		ctx,
		name,
		data,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Signal input as json/plain",
					"OdinWorkflowSignalPayloadConversionError",
				)
			}
			owned.callback(owned.state, value)
			return payload_complete({})
		},
		nil,
		options,
	)
}

// set_signal_handler_json_with_context provides read-only invocation metadata
// while retaining the command-free semantics of a synchronous Signal handler.
set_signal_handler_json_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(signal: ^Signal_Context, state: ^State, input: $Input),
	options := Signal_Handler_Options{},
) -> bool {
	Callback :: #type proc(signal: ^Signal_Context, state: ^State, input: Input)
	Data :: struct {
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid typed contextual Workflow Signal handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(ctx.instance, "could not allocate typed contextual Workflow Signal handler")
		return false
	}
	data.state = state
	data.callback = callback
	return set_signal_handler_payload_with_context(
		ctx,
		name,
		data,
		proc(
			callback_data: rawptr,
			signal: ^Signal_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Signal input as json/plain",
					"OdinWorkflowSignalPayloadConversionError",
				)
			}
			owned.callback(signal, owned.state, value)
			return payload_complete({})
		},
		options,
	)
}

// set_signal_routine_json gives each Signal invocation an independent stackful
// routine that may await Activities, timers, or unhandled buffered signals.
set_signal_routine_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(ctx: ^Fiber_Context, state: ^State, input: $Input),
	options := Signal_Handler_Options{},
) -> bool {
	Callback :: #type proc(ctx: ^Fiber_Context, state: ^State, input: Input)
	Data :: struct {
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid typed Workflow Signal routine")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(ctx.instance, "could not allocate typed Workflow Signal routine")
		return false
	}
	data.state = state
	data.callback = callback
	return set_signal_handler_payload(
		ctx,
		name,
		data,
		nil,
		proc(
			callback_data: rawptr,
			routine_ctx: ^Fiber_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Signal input as json/plain",
					"OdinWorkflowSignalPayloadConversionError",
				)
			}
			owned.callback(routine_ctx, owned.state, value)
			return payload_complete({})
		},
		options,
	)
}

// set_signal_routine_json_with_context exposes run-owned Signal metadata to a
// stackful handler. The context remains valid across durable suspension and
// must not escape the Workflow run.
set_signal_routine_json_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(
		ctx: ^Fiber_Context,
		signal: ^Signal_Context,
		state: ^State,
		input: $Input,
	),
	options := Signal_Handler_Options{},
) -> bool {
	Callback :: #type proc(
		ctx: ^Fiber_Context,
		signal: ^Signal_Context,
		state: ^State,
		input: Input,
	)
	Data :: struct {
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			signal_registration_failed(ctx.instance, "invalid contextual Workflow Signal routine")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		signal_registration_failed(ctx.instance, "could not allocate contextual Workflow Signal routine")
		return false
	}
	data.state = state
	data.callback = callback
	return set_signal_routine_payload_with_context(
		ctx,
		name,
		data,
		proc(
			callback_data: rawptr,
			routine_ctx: ^Fiber_Context,
			signal: ^Signal_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Signal input as json/plain",
					"OdinWorkflowSignalPayloadConversionError",
				)
			}
			owned.callback(routine_ctx, signal, owned.state, value)
			return payload_complete({})
		},
		options,
	)
}

find_update_handler :: proc(
	instance: ^Fiber_Instance,
	name: string,
) -> (^Fiber_Update_Handler, bool) {
	if instance == nil {
		return nil, false
	}
	for index in 0..<len(instance.update_handlers) {
		handler := &instance.update_handlers[index]
		if handler.name == name {
			return handler, true
		}
	}
	return nil, false
}

update_registration_failed :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowUpdateRegistrationError"
	instance.phase = .Failed
}

set_update_handler_payload_internal :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	validator: Update_Validate_Proc,
	validator_with_context: Update_Context_Validate_Proc,
	callback: Update_Invoke_Proc,
	callback_with_context: Update_Context_Invoke_Proc,
	routine_callback: Update_Routine_Invoke_Proc,
	routine_callback_with_context: Update_Context_Routine_Invoke_Proc,
	options := Update_Handler_Options{},
) -> bool {
	callback_count := 0
	if callback != nil {
		callback_count += 1
	}
	if callback_with_context != nil {
		callback_count += 1
	}
	if routine_callback != nil {
		callback_count += 1
	}
	if routine_callback_with_context != nil {
		callback_count += 1
	}
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || callback_count != 1 ||
	   validator != nil && validator_with_context != nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid Workflow Update handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_update_handler(instance, name)
	if duplicate {
		update_registration_failed(instance, "Workflow Update handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(instance, "could not preserve Workflow Update handler name")
		return false
	}
	append(
		&instance.update_handlers,
		Fiber_Update_Handler{
			name = owned_name,
			callback_data = callback_data,
			validate = validator,
			validate_with_context = validator_with_context,
			invoke = callback,
			invoke_with_context = callback_with_context,
			invoke_routine = routine_callback,
			invoke_routine_with_context = routine_callback_with_context,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return true
}

set_update_handler_payload :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	validator: Update_Validate_Proc,
	callback: Update_Invoke_Proc,
	routine_callback: Update_Routine_Invoke_Proc = nil,
	options := Update_Handler_Options{},
) -> bool {
	return set_update_handler_payload_internal(
		ctx,
		name,
		callback_data,
		validator,
		nil,
		callback,
		nil,
		routine_callback,
		nil,
		options,
	)
}

set_update_handler_payloads_internal :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	validator: Update_Payloads_Validate_Proc,
	validator_with_context: Update_Context_Payloads_Validate_Proc,
	callback: Update_Payloads_Invoke_Proc,
	callback_with_context: Update_Context_Payloads_Invoke_Proc,
	routine_callback: Update_Payloads_Routine_Invoke_Proc,
	routine_callback_with_context: Update_Context_Payloads_Routine_Invoke_Proc,
	options := Update_Handler_Options{},
) -> bool {
	callback_count := 0
	if callback != nil {
		callback_count += 1
	}
	if callback_with_context != nil {
		callback_count += 1
	}
	if routine_callback != nil {
		callback_count += 1
	}
	if routine_callback_with_context != nil {
		callback_count += 1
	}
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(name) == 0 || callback_data == nil || callback_count != 1 ||
	   validator != nil && validator_with_context != nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid repeated-input Workflow Update handler")
		}
		return false
	}
	instance := ctx.instance
	_, duplicate := find_update_handler(instance, name)
	if duplicate {
		update_registration_failed(instance, "Workflow Update handler is already registered")
		return false
	}
	owned_name, allocation_error := strings.clone(name, instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(instance, "could not preserve Workflow Update handler name")
		return false
	}
	append(
		&instance.update_handlers,
		Fiber_Update_Handler{
			name = owned_name,
			callback_data = callback_data,
			validate_payloads = validator,
			validate_payloads_with_context = validator_with_context,
			invoke_payloads = callback,
			invoke_payloads_with_context = callback_with_context,
			invoke_payloads_routine = routine_callback,
			invoke_payloads_routine_with_context = routine_callback_with_context,
			unfinished_policy = options.unfinished_policy,
		},
	)
	return true
}

// set_update_handler_payloads registers a codec-neutral Update validator and
// handler over Temporal's complete ordered argument list. If routine_callback
// is supplied, it runs on a stackful routine and may suspend across activations.
set_update_handler_payloads :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	validator: Update_Payloads_Validate_Proc,
	callback: Update_Payloads_Invoke_Proc,
	routine_callback: Update_Payloads_Routine_Invoke_Proc = nil,
	options := Update_Handler_Options{},
) -> bool {
	return set_update_handler_payloads_internal(
		ctx,
		name,
		callback_data,
		validator,
		nil,
		callback,
		nil,
		routine_callback,
		nil,
		options,
	)
}

set_update_handler_payloads_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	callback_data: rawptr,
	validator: Update_Context_Payloads_Validate_Proc,
	callback: Update_Context_Payloads_Invoke_Proc,
	routine_callback: Update_Context_Payloads_Routine_Invoke_Proc = nil,
	options := Update_Handler_Options{},
) -> bool {
	return set_update_handler_payloads_internal(
		ctx,
		name,
		callback_data,
		nil,
		validator,
		nil,
		callback,
		nil,
		routine_callback,
		options,
	)
}

// set_validated_update_routine_json registers an Update handler on its own
// stackful routine. Unlike the synchronous form, the handler receives a
// Fiber_Context and may await Activities, timers, or signals.
set_validated_update_routine_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	validator: proc(state: State, input: $Input) -> Update_Validation,
	callback: proc(ctx: ^Fiber_Context, state: ^State, input: Input) -> $Output,
	options := Update_Handler_Options{},
) -> bool {
	Validator :: #type proc(state: State, input: Input) -> Update_Validation
	Callback :: #type proc(ctx: ^Fiber_Context, state: ^State, input: Input) -> Output
	Data :: struct {
		instance:  ^Fiber_Instance,
		state:     ^State,
		validator: Validator,
		callback:  Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || validator == nil || callback == nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid validated Workflow Update routine")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(ctx.instance, "could not allocate typed Workflow Update routine")
		return false
	}
	data.instance = ctx.instance
	data.state = state
	data.validator = validator
	data.callback = callback
	return set_update_handler_payload(
		ctx,
		name,
		data,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Update_Validation {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return update_reject(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return owned.validator(owned.state^, value)
		},
		nil,
		proc(
			callback_data: rawptr,
			routine_ctx: ^Fiber_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			output := owned.callback(routine_ctx, owned.state, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow Update result as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
		options,
	)
}

// set_validated_update_routine_json_with_context exposes the Update identity
// and headers to both validation and the stackful handler. The context is
// Workflow-run-owned and remains valid when the handler crosses activations.
set_validated_update_routine_json_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	validator: proc(update: ^Update_Context, state: State, input: $Input) -> Update_Validation,
	callback: proc(
		ctx: ^Fiber_Context,
		update: ^Update_Context,
		state: ^State,
		input: Input,
	) -> $Output,
	options := Update_Handler_Options{},
) -> bool {
	Validator :: #type proc(
		update: ^Update_Context,
		state: State,
		input: Input,
	) -> Update_Validation
	Callback :: #type proc(
		ctx: ^Fiber_Context,
		update: ^Update_Context,
		state: ^State,
		input: Input,
	) -> Output
	Data :: struct {
		state:     ^State,
		validator: Validator,
		callback:  Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || validator == nil || callback == nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid contextual Workflow Update routine")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(ctx.instance, "could not allocate contextual Workflow Update routine")
		return false
	}
	data.state = state
	data.validator = validator
	data.callback = callback
	return set_update_handler_payload_internal(
		ctx,
		name,
		data,
		nil,
		proc(
			callback_data: rawptr,
			update: ^Update_Context,
			input: codec.Payload_View,
		) -> Update_Validation {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return update_reject(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return owned.validator(update, owned.state^, value)
		},
		nil,
		nil,
		nil,
		proc(
			callback_data: rawptr,
			routine_ctx: ^Fiber_Context,
			update: ^Update_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			output := owned.callback(routine_ctx, update, owned.state, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow Update result as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
		options,
	)
}

set_update_routine_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(ctx: ^Fiber_Context, state: ^State, input: $Input) -> $Output,
	options := Update_Handler_Options{},
) -> bool {
	Callback :: #type proc(ctx: ^Fiber_Context, state: ^State, input: Input) -> Output
	Data :: struct {
		instance: ^Fiber_Instance,
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid Workflow Update routine")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(ctx.instance, "could not allocate typed Workflow Update routine")
		return false
	}
	data.instance = ctx.instance
	data.state = state
	data.callback = callback
	return set_update_handler_payload(
		ctx,
		name,
		data,
		nil,
		nil,
		proc(
			callback_data: rawptr,
			routine_ctx: ^Fiber_Context,
			input: codec.Payload_View,
		) -> Payload_Result {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			output := owned.callback(routine_ctx, owned.state, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow Update result as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
		options,
	)
}

// set_update_handler_json registers a synchronous state-mutating Update. The
// handler cannot suspend and receives no Fiber_Context in this first slice.
set_update_handler_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	callback: proc(state: ^State, input: $Input) -> $Output,
) -> bool {
	Callback :: #type proc(state: ^State, input: Input) -> Output
	Data :: struct {
		instance: ^Fiber_Instance,
		state:    ^State,
		callback: Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || callback == nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid typed Workflow Update handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(ctx.instance, "could not allocate typed Workflow Update handler")
		return false
	}
	data.instance = ctx.instance
	data.state = state
	data.callback = callback
	return set_update_handler_payload(
		ctx,
		name,
		data,
		nil,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Payload_Result {
			owned := cast(^Data)callback_data
			previous_allocator := context.allocator
			context.allocator = owned.instance.odin_context.allocator
			defer { context.allocator = previous_allocator }
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			output := owned.callback(owned.state, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow Update result as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
	)
}

// set_validated_update_handler_json runs a read-only validator on non-replay
// delivery, then invokes the synchronous state-mutating handler after acceptance.
set_validated_update_handler_json :: proc(
	ctx: ^Fiber_Context,
	name: string,
	state: ^$State,
	validator: proc(state: State, input: $Input) -> Update_Validation,
	callback: proc(state: ^State, input: Input) -> $Output,
) -> bool {
	Validator :: #type proc(state: State, input: Input) -> Update_Validation
	Callback :: #type proc(state: ^State, input: Input) -> Output
	Data :: struct {
		instance:  ^Fiber_Instance,
		state:     ^State,
		validator: Validator,
		callback:  Callback,
	}
	if ctx == nil || ctx.instance == nil || state == nil || validator == nil || callback == nil {
		if ctx != nil {
			update_registration_failed(ctx.instance, "invalid validated Workflow Update handler")
		}
		return false
	}
	data, allocation_error := new(Data, ctx.instance.odin_context.allocator)
	if allocation_error != nil {
		update_registration_failed(ctx.instance, "could not allocate validated Workflow Update handler")
		return false
	}
	data.instance = ctx.instance
	data.state = state
	data.validator = validator
	data.callback = callback
	return set_update_handler_payload(
		ctx,
		name,
		data,
		proc(callback_data: rawptr, input: codec.Payload_View) -> Update_Validation {
			owned := cast(^Data)callback_data
			value: Input
			if codec.decode_json(input, &value) != .None {
				return update_reject(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return owned.validator(owned.state^, value)
		},
		proc(callback_data: rawptr, input: codec.Payload_View) -> Payload_Result {
			owned := cast(^Data)callback_data
			previous_allocator := context.allocator
			context.allocator = owned.instance.odin_context.allocator
			defer { context.allocator = previous_allocator }
			value: Input
			if codec.decode_json(input, &value) != .None {
				return payload_fail(
					"could not decode Workflow Update input as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			output := owned.callback(owned.state, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return payload_fail(
					"could not encode Workflow Update result as json/plain",
					"OdinWorkflowUpdatePayloadConversionError",
				)
			}
			return payload_complete(payload)
		},
	)
}

find_cancellation_node :: proc(
	instance: ^Fiber_Instance,
	token_id: u32,
) -> (^Fiber_Cancellation_Node, bool) {
	if instance == nil || token_id == 0 {
		return nil, false
	}
	for index in 0..<len(instance.cancellation_nodes) {
		if instance.cancellation_nodes[index].id == token_id {
			return &instance.cancellation_nodes[index], true
		}
	}
	return nil, false
}

find_cancellation_node_by_deadline_timer :: proc(
	instance: ^Fiber_Instance,
	timer_seq: u32,
) -> (^Fiber_Cancellation_Node, bool) {
	if instance == nil || timer_seq == 0 {
		return nil, false
	}
	for index in 0..<len(instance.cancellation_nodes) {
		node := &instance.cancellation_nodes[index]
		if node.deadline_timer_seq == timer_seq {
			return node, true
		}
	}
	return nil, false
}

current_cancellation_token_id :: proc(instance: ^Fiber_Instance) -> u32 {
	if instance == nil {
		return 0
	}
	if instance.current_cancellation_token_id == 0 {
		return 1
	}
	return instance.current_cancellation_token_id
}

selected_cancellation_token_id :: proc(
	instance: ^Fiber_Instance,
	token: Cancellation_Token,
) -> (token_id: u32, valid: bool) {
	if instance == nil {
		return 0, false
	}
	if token.id == 0 {
		return current_cancellation_token_id(instance), true
	}
	if token.instance != instance {
		return 0, false
	}
	_, found := find_cancellation_node(instance, token.id)
	if !found {
		return 0, false
	}
	return token.id, true
}

cancellation_token_is_requested_id :: proc(instance: ^Fiber_Instance, token_id: u32) -> bool {
	node, found := find_cancellation_node(instance, token_id)
	return found && node.cancellation_requested
}

cancellation_token_reason_id :: proc(instance: ^Fiber_Instance, token_id: u32) -> string {
	node, found := find_cancellation_node(instance, token_id)
	if !found || !node.cancellation_requested {
		return ""
	}
	return node.reason
}

cancellation_token_linked_to_root :: proc(instance: ^Fiber_Instance, token_id: u32) -> bool {
	node, found := find_cancellation_node(instance, token_id)
	return found && node.linked_to_root
}

cancellation_token_descends_from :: proc(
	instance: ^Fiber_Instance,
	token_id, ancestor_id: u32,
) -> bool {
	if instance == nil || token_id == 0 || ancestor_id == 0 {
		return false
	}
	candidate_id := token_id
	for candidate_id != 0 {
		if candidate_id == ancestor_id {
			return true
		}
		node, found := find_cancellation_node(instance, candidate_id)
		if !found || node.parent_id == candidate_id {
			return false
		}
		candidate_id = node.parent_id
	}
	return false
}

apply_cancellation_to_operations :: proc(instance: ^Fiber_Instance) {
	if instance == nil {
		return
	}
	for index in 0..<len(instance.activities) {
		operation := &instance.activities[index]
		if !cancellation_token_is_requested_id(instance, operation.cancellation_token_id) {
			continue
		}
		operation.failure_message = cancellation_token_reason_id(
			instance,
			operation.cancellation_token_id,
		)
		switch operation.state {
		case .Scheduled:
			operation.state = .Canceled
		case .Commanded:
			if operation.is_local && operation.backoff_timer_seq != 0 {
				timer, found := find_timer_operation(instance, operation.backoff_timer_seq)
				if found {
					switch timer.state {
					case .Scheduled:
						timer.state = .Canceled
					case .Commanded:
						timer.state = .Cancel_Requested
					case .Resolved, .Failed, .Cancel_Requested, .Canceled:
					}
				}
				operation.state = .Canceled
			} else {
				operation.state = .Cancel_Requested
			}
		case .Resolved, .Failed, .Cancel_Requested, .Canceled:
		}
	}
	for index in 0..<len(instance.nexus_operations) {
		operation := &instance.nexus_operations[index]
		if !cancellation_token_is_requested_id(instance, operation.cancellation_token_id) {
			continue
		}
		reason := cancellation_token_reason_id(instance, operation.cancellation_token_id)
		operation.failure_message = reason
		switch operation.state {
		case .Scheduled:
			operation.state = .Canceled
			operation.start_state = .Failed
		case .Commanded:
			switch operation.options.cancellation_type {
			case .Abandon:
				operation.state = .Canceled
			case .Try_Cancel, .Wait_Cancellation_Completed, .Wait_Cancellation_Requested:
				operation.state = .Cancel_Requested
			}
		case .Resolved, .Failed, .Cancel_Requested, .Canceled, .Timed_Out:
		}
	}
	for index in 0..<len(instance.timers) {
		operation := &instance.timers[index]
		if !cancellation_token_is_requested_id(instance, operation.cancellation_token_id) {
			continue
		}
		switch operation.state {
		case .Scheduled:
			operation.state = .Canceled
		case .Commanded:
			operation.state = .Cancel_Requested
		case .Resolved, .Failed, .Cancel_Requested, .Canceled:
		}
	}
	for index in 0..<len(instance.child_workflows) {
		operation := &instance.child_workflows[index]
		if !cancellation_token_is_requested_id(instance, operation.cancellation_token_id) {
			continue
		}
		reason := cancellation_token_reason_id(instance, operation.cancellation_token_id)
		switch operation.command_state {
		case .Scheduled:
			operation.command_state = .Canceled
			operation.start_state = .Canceled
			operation.completion_state = .Canceled
			operation.cancellation_reason = reason
			operation.failure_message = reason
		case .Commanded:
			operation.cancellation_reason = reason
			switch operation.options.cancellation_type {
			case .Abandon:
				operation.command_state = .Resolved
				operation.start_state = .Canceled
				operation.completion_state = .Canceled
				operation.failure_message = reason
			case .Try_Cancel:
				operation.command_state = .Cancel_Requested
				operation.start_state = .Canceled
				operation.completion_state = .Canceled
				operation.failure_message = reason
			case .Wait_Cancellation_Completed, .Wait_Cancellation_Requested:
				operation.command_state = .Cancel_Requested
			}
		case .Resolved, .Failed, .Cancel_Requested, .Canceled:
		}
	}
	for index in 0..<len(instance.external_signals) {
		operation := &instance.external_signals[index]
		if !cancellation_token_is_requested_id(instance, operation.cancellation_token_id) {
			continue
		}
		reason := cancellation_token_reason_id(instance, operation.cancellation_token_id)
		switch operation.state {
		case .Scheduled:
			operation.state = .Canceled
			operation.failure_message = reason
		case .Commanded:
			operation.state = .Cancel_Requested
			operation.failure_message = reason
		case .Resolved, .Failed, .Cancel_Requested, .Canceled:
		}
	}
}

request_cancellation_token :: proc(
	instance: ^Fiber_Instance,
	token_id: u32,
	reason: string,
) -> bool {
	target, found := find_cancellation_node(instance, token_id)
	if !found {
		return false
	}
	if target.cancellation_requested {
		return true
	}
	owned_reason, allocation_error := strings.clone(reason, instance.odin_context.allocator)
	if allocation_error != nil {
		return false
	}
	for index in 0..<len(instance.cancellation_nodes) {
		node := &instance.cancellation_nodes[index]
		if node.cancellation_requested ||
		   !cancellation_token_descends_from(instance, node.id, token_id) {
			continue
		}
		node.cancellation_requested = true
		node.reason = owned_reason
	}
	apply_cancellation_to_operations(instance)
	return true
}

apply_fiber_cancellation :: proc(instance: ^Fiber_Instance, reason: string) {
	instance.cancellation_reason = reason
	instance.cancellation_requested = true
	_ = request_cancellation_token(instance, 1, reason)
}

is_cancellation_requested :: proc(ctx: ^Fiber_Context) -> bool {
	return ctx != nil && ctx.instance != nil && ctx.instance.cancellation_requested
}

cancellation_reason :: proc(ctx: ^Fiber_Context) -> string {
	if ctx == nil || ctx.instance == nil || !ctx.instance.cancellation_requested {
		return ""
	}
	return ctx.instance.cancellation_reason
}

is_replaying :: proc(ctx: ^Fiber_Context) -> bool {
	return ctx != nil && ctx.instance != nil && ctx.instance.activation_is_replaying
}

// is_replaying_history_events excludes replay activations which contain only
// Queries. It is the appropriate guard for suppressing duplicate external
// logging while still allowing query-time diagnostics.
is_replaying_history_events :: proc(ctx: ^Fiber_Context) -> bool {
	return ctx != nil && ctx.instance != nil &&
	       ctx.instance.activation_is_replaying_history_events
}

workflow_metric_attributes_valid :: proc(attributes: []Metric_Attribute) -> bool {
	for attribute in attributes {
		if len(attribute.key) == 0 {
			return false
		}
		if attribute.kind == .Float &&
		   (math.is_nan(attribute.float_value) || math.is_inf(attribute.float_value)) {
			return false
		}
	}
	return true
}

workflow_metric_emit :: proc(instance: ^Fiber_Instance, record: Metric_Record) {
	if instance == nil || instance.runtime == nil || len(record.name) == 0 ||
	   !workflow_metric_attributes_valid(record.attributes) {
		return
	}
	// History replay must never duplicate application metrics. Query-only
	// replay remains observable because it does not replay history events.
	if instance.activation_is_replaying_history_events {
		return
	}
	emitter := &instance.runtime.metric_emitter
	if emitter.emit == nil {
		return
	}
	emitted_record := record
	emitted_record.namespace = instance.namespace
	emitted_record.workflow_id = instance.workflow_id
	emitted_record.run_id = instance.run_id
	emitted_record.workflow_type = instance.workflow_type
	emitted_record.task_queue = instance.task_queue
	emitted_record.replaying = instance.activation_is_replaying
	emitted_record.replaying_history_events = instance.activation_is_replaying_history_events
	emitter.emit(emitter.user_data, &emitted_record)
}

metric_counter_add :: proc(
	ctx: ^Fiber_Context,
	name: string,
	value: u64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if ctx == nil do return
	workflow_metric_emit(ctx.instance, {
		name = name, kind = .Counter_Integer, integer_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

metric_histogram_record_integer :: proc(
	ctx: ^Fiber_Context,
	name: string,
	value: u64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if ctx == nil do return
	workflow_metric_emit(ctx.instance, {
		name = name, kind = .Histogram_Integer, integer_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

metric_histogram_record_float :: proc(
	ctx: ^Fiber_Context,
	name: string,
	value: f64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if ctx == nil || math.is_nan(value) || math.is_inf(value) || value < 0 do return
	workflow_metric_emit(ctx.instance, {
		name = name, kind = .Histogram_Float, float_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

metric_histogram_record_duration :: proc(
	ctx: ^Fiber_Context,
	name: string,
	value: time.Duration,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "duration",
) {
	if ctx == nil || value < 0 do return
	workflow_metric_emit(ctx.instance, {
		name = name, kind = .Histogram_Duration, duration_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

metric_gauge_record_integer :: proc(
	ctx: ^Fiber_Context,
	name: string,
	value: u64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if ctx == nil do return
	workflow_metric_emit(ctx.instance, {
		name = name, kind = .Gauge_Integer, integer_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

metric_gauge_record_float :: proc(
	ctx: ^Fiber_Context,
	name: string,
	value: f64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if ctx == nil || math.is_nan(value) || math.is_inf(value) do return
	workflow_metric_emit(ctx.instance, {
		name = name, kind = .Gauge_Float, float_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

metric_up_down_counter_add :: proc(
	ctx: ^Fiber_Context,
	name: string,
	value: i64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if ctx == nil do return
	workflow_metric_emit(ctx.instance, {
		name = name, kind = .Up_Down_Counter_Integer, signed_integer_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

query_metric_counter_add :: proc(
	query: ^Query_Context,
	name: string,
	value: u64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if query == nil do return
	workflow_metric_emit(query.instance, {
		name = name, kind = .Counter_Integer, integer_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

query_metric_histogram_record_integer :: proc(
	query: ^Query_Context,
	name: string,
	value: u64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if query == nil do return
	workflow_metric_emit(query.instance, {
		name = name, kind = .Histogram_Integer, integer_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

query_metric_histogram_record_float :: proc(
	query: ^Query_Context,
	name: string,
	value: f64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if query == nil || math.is_nan(value) || math.is_inf(value) || value < 0 do return
	workflow_metric_emit(query.instance, {
		name = name, kind = .Histogram_Float, float_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

query_metric_histogram_record_duration :: proc(
	query: ^Query_Context,
	name: string,
	value: time.Duration,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "duration",
) {
	if query == nil || value < 0 do return
	workflow_metric_emit(query.instance, {
		name = name, kind = .Histogram_Duration, duration_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

query_metric_gauge_record_integer :: proc(
	query: ^Query_Context,
	name: string,
	value: u64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if query == nil do return
	workflow_metric_emit(query.instance, {
		name = name, kind = .Gauge_Integer, integer_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

query_metric_gauge_record_float :: proc(
	query: ^Query_Context,
	name: string,
	value: f64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if query == nil || math.is_nan(value) || math.is_inf(value) do return
	workflow_metric_emit(query.instance, {
		name = name, kind = .Gauge_Float, float_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

query_metric_up_down_counter_add :: proc(
	query: ^Query_Context,
	name: string,
	value: i64,
	attributes: []Metric_Attribute = nil,
	description := "",
	unit := "",
) {
	if query == nil do return
	workflow_metric_emit(query.instance, {
		name = name, kind = .Up_Down_Counter_Integer, signed_integer_value = value,
		attributes = attributes, description = description, unit = unit,
	})
}

workflow_log_emit :: proc(
	instance: ^Fiber_Instance,
	level: Log_Level,
	message: string,
	fields: []Log_Field = nil,
) {
	if instance == nil || instance.runtime == nil {
		return
	}
	logger := &instance.runtime.logger
	if logger.emit == nil {
		return
	}
	if instance.activation_is_replaying_history_events &&
	   !logger.log_replay_history_events {
		return
	}
	record := Log_Record{
		level = level,
		message = message,
		fields = fields,
		namespace = instance.namespace,
		workflow_id = instance.workflow_id,
		run_id = instance.run_id,
		workflow_type = instance.workflow_type,
		task_queue = instance.task_queue,
		history_length = instance.activation_history_length,
		timestamp = instance.activation_time,
		has_timestamp = instance.has_activation_time,
		replaying = instance.activation_is_replaying,
		replaying_history_events = instance.activation_is_replaying_history_events,
	}
	logger.emit(logger.user_data, &record)
}

// log synchronously emits one application record through the Worker-owned
// logger. It performs no host I/O itself and emits no Workflow command. The
// callback must copy anything it retains after returning.
log :: proc(
	ctx: ^Fiber_Context,
	level: Log_Level,
	message: string,
	fields: []Log_Field = nil,
) {
	if ctx == nil {
		return
	}
	workflow_log_emit(ctx.instance, level, message, fields)
}

log_trace :: proc(ctx: ^Fiber_Context, message: string, fields: []Log_Field = nil) {
	log(ctx, .Trace, message, fields)
}

log_debug :: proc(ctx: ^Fiber_Context, message: string, fields: []Log_Field = nil) {
	log(ctx, .Debug, message, fields)
}

log_info :: proc(ctx: ^Fiber_Context, message: string, fields: []Log_Field = nil) {
	log(ctx, .Info, message, fields)
}

log_warn :: proc(ctx: ^Fiber_Context, message: string, fields: []Log_Field = nil) {
	log(ctx, .Warn, message, fields)
}

log_error :: proc(ctx: ^Fiber_Context, message: string, fields: []Log_Field = nil) {
	log(ctx, .Error, message, fields)
}

query_log :: proc(
	query: ^Query_Context,
	level: Log_Level,
	message: string,
	fields: []Log_Field = nil,
) {
	if query == nil {
		return
	}
	workflow_log_emit(query.instance, level, message, fields)
}

query_log_trace :: proc(query: ^Query_Context, message: string, fields: []Log_Field = nil) {
	query_log(query, .Trace, message, fields)
}

query_log_debug :: proc(query: ^Query_Context, message: string, fields: []Log_Field = nil) {
	query_log(query, .Debug, message, fields)
}

query_log_info :: proc(query: ^Query_Context, message: string, fields: []Log_Field = nil) {
	query_log(query, .Info, message, fields)
}

query_log_warn :: proc(query: ^Query_Context, message: string, fields: []Log_Field = nil) {
	query_log(query, .Warn, message, fields)
}

query_log_error :: proc(query: ^Query_Context, message: string, fields: []Log_Field = nil) {
	query_log(query, .Error, message, fields)
}

history_length :: proc(ctx: ^Fiber_Context) -> u32 {
	if ctx == nil || ctx.instance == nil {
		return 0
	}
	return ctx.instance.activation_history_length
}

history_size_bytes :: proc(ctx: ^Fiber_Context) -> u64 {
	if ctx == nil || ctx.instance == nil {
		return 0
	}
	return ctx.instance.activation_history_size_bytes
}

continue_as_new_suggested :: proc(ctx: ^Fiber_Context) -> bool {
	return ctx != nil && ctx.instance != nil &&
	       ctx.instance.activation_continue_as_new_suggested
}

target_worker_deployment_version_changed :: proc(ctx: ^Fiber_Context) -> bool {
	return ctx != nil && ctx.instance != nil &&
	       ctx.instance.activation_target_worker_deployment_version_changed
}

current_deployment_version :: proc(
	ctx: ^Fiber_Context,
) -> (Worker_Deployment_Version, bool) {
	if ctx == nil || ctx.instance == nil || !ctx.instance.has_current_deployment_version {
		return {}, false
	}
	return ctx.instance.current_deployment_version, true
}

// current_details returns a borrowed value which remains valid until the next
// successful set_current_details call or run eviction.
current_details :: proc(ctx: ^Fiber_Context) -> string {
	if ctx == nil || ctx.instance == nil {
		return ""
	}
	return ctx.instance.current_details
}

// set_current_details updates the value returned by Temporal's reserved
// __temporal_workflow_metadata Query. It is language-runtime metadata and does
// not emit a Workflow command or add an event to history.
set_current_details :: proc(ctx: ^Fiber_Context, details: string) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		return false
	}
	instance := ctx.instance
	owned, allocation_error := strings.clone(details, instance.owner_allocator)
	if allocation_error != nil {
		return false
	}
	delete(instance.current_details, instance.owner_allocator)
	instance.current_details = owned
	return true
}

info :: proc(ctx: ^Fiber_Context) -> (value: Info, present: bool) {
	if ctx == nil || ctx.instance == nil {
		return {}, false
	}
	return ctx.instance.info, true
}

continued_failure :: proc(
	ctx: ^Fiber_Context,
) -> (^temporal_failure.Failure, bool) {
	if ctx == nil || ctx.instance == nil || !ctx.instance.info.has_continued_failure ||
	   ctx.instance.info.continued_failure == nil {
		return nil, false
	}
	return ctx.instance.info.continued_failure, true
}

last_completion_result_payload :: proc(
	ctx: ^Fiber_Context,
	index: int,
) -> (codec.Payload_View, bool) {
	if ctx == nil || ctx.instance == nil || index < 0 ||
	   index >= len(ctx.instance.info.last_completion_results) {
		return {}, false
	}
	return ctx.instance.info.last_completion_results[index], true
}

last_completion_result_json :: proc(
	ctx: ^Fiber_Context,
	index: int,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := last_completion_result_payload(ctx, index)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

find_named_payload :: proc(
	entries: ^[dynamic]Fiber_Named_Payload,
	key: string,
) -> (^Fiber_Named_Payload, int, bool) {
	if entries == nil {
		return nil, -1, false
	}
	for index in 0..<len(entries^) {
		if entries^[index].key == key {
			return &entries^[index], index, true
		}
	}
	return nil, -1, false
}

header_payload :: proc(ctx: ^Fiber_Context, key: string) -> (codec.Payload_View, bool) {
	if ctx == nil || ctx.instance == nil {
		return {}, false
	}
	entry, _, found := find_named_payload(&ctx.instance.headers, key)
	if !found {
		return {}, false
	}
	return codec.payload_view(&entry.payload), true
}

header_json :: proc(
	ctx: ^Fiber_Context,
	key: string,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := header_payload(ctx, key)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

query_header_payload :: proc(query: ^Query_Context, key: string) -> (codec.Payload_View, bool) {
	if query == nil {
		return {}, false
	}
	entry, _, found := find_named_payload(&query.headers, key)
	if !found {
		return {}, false
	}
	return codec.payload_view(&entry.payload), true
}

query_header_json :: proc(
	query: ^Query_Context,
	key: string,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := query_header_payload(query, key)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

signal_header_payload :: proc(signal: ^Signal_Context, key: string) -> (codec.Payload_View, bool) {
	if signal == nil {
		return {}, false
	}
	entry, _, found := find_named_payload(&signal.headers, key)
	if !found {
		return {}, false
	}
	return codec.payload_view(&entry.payload), true
}

signal_header_json :: proc(
	signal: ^Signal_Context,
	key: string,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := signal_header_payload(signal, key)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

current_update :: proc(ctx: ^Fiber_Context) -> (^Update_Context, bool) {
	if ctx == nil || ctx.update_context == nil {
		return nil, false
	}
	return ctx.update_context, true
}

update_header_payload :: proc(
	update: ^Update_Context,
	key: string,
) -> (codec.Payload_View, bool) {
	if update == nil {
		return {}, false
	}
	entry, _, found := find_named_payload(&update.headers, key)
	if !found {
		return {}, false
	}
	return codec.payload_view(&entry.payload), true
}

update_header_json :: proc(
	update: ^Update_Context,
	key: string,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := update_header_payload(update, key)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

memo_payload :: proc(ctx: ^Fiber_Context, key: string) -> (codec.Payload_View, bool) {
	if ctx == nil || ctx.instance == nil {
		return {}, false
	}
	entry, _, found := find_named_payload(&ctx.instance.memo, key)
	if !found {
		return {}, false
	}
	return codec.payload_view(&entry.payload), true
}

memo_json :: proc(
	ctx: ^Fiber_Context,
	key: string,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := memo_payload(ctx, key)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

search_attribute_payload :: proc(
	ctx: ^Fiber_Context,
	key: string,
) -> (codec.Payload_View, bool) {
	if ctx == nil || ctx.instance == nil {
		return {}, false
	}
	entry, _, found := find_named_payload(&ctx.instance.search_attributes, key)
	if !found {
		return {}, false
	}
	return codec.payload_view(&entry.payload), true
}

search_attribute_json :: proc(
	ctx: ^Fiber_Context,
	key: string,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := search_attribute_payload(ctx, key)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

property_mutation_failure :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowPropertyError"
	instance.phase = .Failed
}

upsert_property_owned :: proc(
	ctx: ^Fiber_Context,
	kind: Fiber_Property_Command_Kind,
	key: string,
	payload: codec.Payload,
	remove: bool,
) -> bool {
	owned_payload := payload
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || len(key) == 0 {
		codec.payload_free(&owned_payload)
		if ctx != nil && ctx.instance != nil {
			property_mutation_failure(ctx.instance, "Workflow property mutation requires a ready context and non-empty key")
		}
		return false
	}
	instance := ctx.instance
	allocator := instance.odin_context.allocator
	command_key, command_key_error := strings.clone(key, allocator)
	if command_key_error != nil {
		codec.payload_free(&owned_payload)
		property_mutation_failure(instance, "could not preserve Workflow property key")
		return false
	}
	entries := &instance.memo
	if kind == .Search_Attribute {
		entries = &instance.search_attributes
	}
	entry, index, found := find_named_payload(entries, key)
	if remove {
		if found {
			codec.payload_free(&entry.payload)
			ordered_remove(entries, index)
		}
	} else {
		local_payload, clone_error := codec.payload_clone(
			codec.payload_view(&owned_payload),
			allocator,
		)
		if clone_error != .None {
			codec.payload_free(&owned_payload)
			property_mutation_failure(instance, "could not preserve Workflow property value")
			return false
		}
		if found {
			codec.payload_free(&entry.payload)
			entry.payload = local_payload
		} else {
			local_key, local_key_error := strings.clone(key, allocator)
			if local_key_error != nil {
				codec.payload_free(&owned_payload)
				codec.payload_free(&local_payload)
				property_mutation_failure(instance, "could not preserve Workflow property key")
				return false
			}
			append(entries, Fiber_Named_Payload{key = local_key, payload = local_payload})
		}
	}
	append(
		&instance.property_commands,
		Fiber_Property_Command{
			kind = kind,
			key = command_key,
			payload = owned_payload,
		},
	)
	return true
}

upsert_memo_payload :: proc(
	ctx: ^Fiber_Context,
	key: string,
	value: codec.Payload_View,
) -> bool {
	if ctx == nil || ctx.instance == nil {
		return false
	}
	payload, clone_error := codec.payload_clone(value, ctx.instance.odin_context.allocator)
	if clone_error != .None {
		property_mutation_failure(ctx.instance, "could not preserve Workflow memo value")
		return false
	}
	return upsert_property_owned(ctx, .Memo, key, payload, false)
}

upsert_memo_json :: proc(ctx: ^Fiber_Context, key: string, value: $T) -> bool {
	if ctx == nil || ctx.instance == nil {
		return false
	}
	payload, encode_error := codec.encode_json(value, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		property_mutation_failure(ctx.instance, "could not encode Workflow memo value")
		return false
	}
	return upsert_property_owned(ctx, .Memo, key, payload, false)
}

remove_memo :: proc(ctx: ^Fiber_Context, key: string) -> bool {
	return upsert_property_owned(ctx, .Memo, key, {}, true)
}

upsert_search_attribute_payload :: proc(
	ctx: ^Fiber_Context,
	key: string,
	attribute_type: Search_Attribute_Type,
	value: codec.Payload_View,
) -> bool {
	if ctx == nil || ctx.instance == nil {
		return false
	}
	payload, clone_error := codec.payload_clone(value, ctx.instance.odin_context.allocator)
	if clone_error != .None ||
	   codec.payload_set_search_attribute_type(&payload, attribute_type) != .None {
		codec.payload_free(&payload)
		property_mutation_failure(ctx.instance, "could not preserve Workflow Search Attribute")
		return false
	}
	return upsert_property_owned(ctx, .Search_Attribute, key, payload, false)
}

upsert_search_attribute_json :: proc(
	ctx: ^Fiber_Context,
	key: string,
	attribute_type: Search_Attribute_Type,
	value: $T,
) -> bool {
	if ctx == nil || ctx.instance == nil {
		return false
	}
	payload, encode_error := codec.encode_json(value, ctx.instance.odin_context.allocator)
	if encode_error != .None ||
	   codec.payload_set_search_attribute_type(&payload, attribute_type) != .None {
		codec.payload_free(&payload)
		property_mutation_failure(ctx.instance, "could not encode Workflow Search Attribute")
		return false
	}
	return upsert_property_owned(ctx, .Search_Attribute, key, payload, false)
}

remove_search_attribute :: proc(ctx: ^Fiber_Context, key: string) -> bool {
	return upsert_property_owned(ctx, .Search_Attribute, key, {}, true)
}

// now returns Temporal's activation timestamp. It never reads the host clock.
now :: proc(ctx: ^Fiber_Context) -> (value: time.Time, present: bool) {
	if ctx == nil || ctx.instance == nil || !ctx.instance.has_activation_time {
		return {}, false
	}
	return ctx.instance.activation_time, true
}

// random_u64 advances a deterministic per-run stream seeded by Core. The
// SplitMix64 algorithm and constants are part of Odin Workflow replay
// compatibility and must not change without versioning.
random_state_next :: proc(state: ^u64) -> u64 {
	if state == nil do return 0
	state^ += 0x9e3779b97f4a7c15
	value := state^
	value = (value ~ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ~ (value >> 27)) * 0x94d049bb133111eb
	return value ~ (value >> 31)
}

random_u64 :: proc(ctx: ^Fiber_Context) -> u64 {
	if ctx == nil || ctx.instance == nil || !ctx.instance.random_initialized {
		return 0
	}
	return random_state_next(&ctx.instance.random_state)
}

random_f64 :: proc(ctx: ^Fiber_Context) -> f64 {
	// Use the top 53 bits to produce a value in [0, 1).
	return f64(random_u64(ctx) >> 11) * (1.0 / 9007199254740992.0)
}

// Random_Stream is an independent deterministic generator created from the
// Workflow's Core-provided random stream. Creating one consumes exactly one
// draw from the parent stream. Copying the value intentionally snapshots it.
// It is Workflow-fiber state and must not be shared with ordinary threads.
Random_Stream :: struct {
	state:       u64,
	initialized: bool,
	instance:    ^Fiber_Instance,
}

random_stream_create :: proc(ctx: ^Fiber_Context) -> Random_Stream {
	if ctx == nil || ctx.instance == nil || !ctx.instance.random_initialized {
		return {}
	}
	return {
		state = random_u64(ctx),
		initialized = true,
		instance = ctx.instance,
	}
}

random_stream_u64 :: proc(stream: ^Random_Stream) -> u64 {
	if stream == nil || !stream.initialized do return 0
	return random_state_next(&stream.state)
}

random_stream_f64 :: proc(stream: ^Random_Stream) -> f64 {
	return f64(random_stream_u64(stream) >> 11) * (1.0 / 9007199254740992.0)
}

// random_stream_range returns an unbiased value in [minimum, maximum).
random_stream_range :: proc(
	stream: ^Random_Stream,
	minimum, maximum: u64,
) -> (value: u64, ok: bool) {
	if stream == nil || !stream.initialized || minimum >= maximum do return 0, false
	span := maximum - minimum
	// Values below the threshold would make modulo reduction biased. Unsigned
	// complement plus one is defined modulo 2^64 and avoids a wider integer.
	threshold := ((~span) + 1) % span
	for {
		draw := random_stream_u64(stream)
		if draw >= threshold do return minimum + draw % span, true
	}
}

random_stream_index :: proc(stream: ^Random_Stream, count: int) -> (index: int, ok: bool) {
	if count <= 0 do return 0, false
	value, range_ok := random_stream_range(stream, 0, u64(count))
	if !range_ok do return 0, false
	return int(value), true
}

random_uuid_v4_from_values :: proc(
	instance: ^Fiber_Instance,
	first, second: u64,
) -> string {
	if instance == nil do return ""
	id: uuid.Identifier
	for index in 0..<8 {
		id[index] = u8(first >> u64((7 - index) * 8))
		id[index + 8] = u8(second >> u64((7 - index) * 8))
	}
	id[uuid.VERSION_BYTE_INDEX] &= 0x0f
	id[uuid.VERSION_BYTE_INDEX] |= 0x40
	id[uuid.VARIANT_BYTE_INDEX] &= 0x3f
	id[uuid.VARIANT_BYTE_INDEX] |= 0x80
	result, allocation_error := uuid.to_string_allocated(
		id,
		instance.odin_context.allocator,
	)
	if allocation_error != nil {
		instance.failure_message = "could not allocate deterministic Workflow UUID"
		instance.failure_type = "OdinWorkflowRandomError"
		instance.phase = .Failed
		return ""
	}
	return result
}

random_stream_uuid_v4 :: proc(stream: ^Random_Stream) -> string {
	if stream == nil || !stream.initialized || stream.instance == nil do return ""
	return random_uuid_v4_from_values(
		stream.instance,
		random_stream_u64(stream),
		random_stream_u64(stream),
	)
}

uuid_v4 :: proc(ctx: ^Fiber_Context) -> string {
	if ctx == nil || ctx.instance == nil || !ctx.instance.random_initialized {
		return ""
	}
	return random_uuid_v4_from_values(ctx.instance, random_u64(ctx), random_u64(ctx))
}

find_patch_decision :: proc(
	instance: ^Fiber_Instance,
	patch_id: string,
) -> (^Fiber_Patch_Decision, bool) {
	if instance == nil {
		return nil, false
	}
	for index in 0..<len(instance.patches) {
		if instance.patches[index].patch_id == patch_id {
			return &instance.patches[index], true
		}
	}
	return nil, false
}

has_notified_patch :: proc(instance: ^Fiber_Instance, patch_id: string) -> bool {
	if instance == nil {
		return false
	}
	for notified in instance.notified_patches {
		if notified == patch_id {
			return true
		}
	}
	return false
}

notify_patch :: proc(instance: ^Fiber_Instance, patch_id: string) -> bool {
	if instance == nil || len(patch_id) == 0 {
		return false
	}
	if has_notified_patch(instance, patch_id) {
		return true
	}
	owned_id, allocation_error := strings.clone(patch_id, instance.odin_context.allocator)
	if allocation_error != nil {
		return false
	}
	append(&instance.notified_patches, owned_id)
	return true
}

patch_impl :: proc(ctx: ^Fiber_Context, patch_id: string, deprecated: bool) -> bool {
	if ctx == nil || ctx.instance == nil {
		return false
	}
	instance := ctx.instance
	if len(patch_id) == 0 {
		instance.failure_message = "Workflow patch ID must not be empty"
		instance.failure_type = "OdinWorkflowPatchError"
		instance.phase = .Failed
		return false
	}
	if decision, found := find_patch_decision(instance, patch_id); found {
		return decision.present
	}

	notified := has_notified_patch(instance, patch_id)
	present := true
	if instance.activation_is_replaying {
		present = notified
	}
	owned_id, allocation_error := strings.clone(patch_id, instance.odin_context.allocator)
	if allocation_error != nil {
		instance.failure_message = "could not preserve Workflow patch ID"
		instance.failure_type = "OdinWorkflowPatchError"
		instance.phase = .Failed
		return false
	}
	append(
		&instance.patches,
		Fiber_Patch_Decision{
			patch_id = owned_id,
			present = present,
			deprecated = deprecated,
			emit_command = present,
		},
	)
	return present
}

// patched gates a deterministic code change. Keep the call in both old and new
// branches until every open execution has recorded this patch ID.
patched :: proc(ctx: ^Fiber_Context, patch_id: string) -> bool {
	return patch_impl(ctx, patch_id, false)
}

// deprecate_patch records that the old branch has been removed. Once histories
// containing the non-deprecated marker can no longer exist, this call may also
// be removed.
deprecate_patch :: proc(ctx: ^Fiber_Context, patch_id: string) -> bool {
	return patch_impl(ctx, patch_id, true)
}

// cancellation_root returns the Workflow run's root token. The token is
// borrowed and remains valid only until the run is evicted.
cancellation_root :: proc(ctx: ^Fiber_Context) -> Cancellation_Token {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	return {instance = ctx.instance, id = 1}
}

// cancellation_current returns the token selected by the current main,
// Signal-handler, or Update-handler fiber.
cancellation_current :: proc(ctx: ^Fiber_Context) -> Cancellation_Token {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	return {
		instance = ctx.instance,
		id = current_cancellation_token_id(ctx.instance),
	}
}

new_cancellation_token :: proc(
	instance: ^Fiber_Instance,
	parent_id: u32,
	linked_to_root: bool,
) -> Cancellation_Token {
	if instance == nil || instance.phase != .Ready {
		return {}
	}
	root_linked := linked_to_root
	if parent_id != 0 {
		parent, found := find_cancellation_node(instance, parent_id)
		if !found {
			return {}
		}
		root_linked = parent.linked_to_root
	}
	instance.next_cancellation_token_id += 1
	node := Fiber_Cancellation_Node{
		id = instance.next_cancellation_token_id,
		parent_id = parent_id,
		linked_to_root = root_linked,
	}
	if parent_id != 0 {
		parent, _ := find_cancellation_node(instance, parent_id)
		node.has_deadline = parent.has_deadline
		node.deadline = parent.deadline
		if parent.cancellation_requested {
			node.cancellation_requested = true
			node.reason = parent.reason
		}
	}
	append(&instance.cancellation_nodes, node)
	return {instance = instance, id = node.id}
}

// cancellation_detached creates a token with no parent. Root Workflow
// cancellation does not reach it.
cancellation_detached :: proc(ctx: ^Fiber_Context) -> Cancellation_Token {
	if ctx == nil {
		return {}
	}
	return new_cancellation_token(ctx.instance, 0, false)
}

// cancellation_child creates a token that inherits cancellation from parent.
// Cancellation flows downward only; canceling the child does not affect parent.
cancellation_child :: proc(parent: Cancellation_Token) -> Cancellation_Token {
	if parent.instance == nil || parent.id == 0 {
		return {}
	}
	return new_cancellation_token(parent.instance, parent.id, false)
}

cancellation_deadline_reason :: proc() -> string {
	return "Workflow cancellation deadline exceeded"
}

set_cancellation_token_deadline :: proc(
	token: Cancellation_Token,
	deadline: time.Time,
) -> bool {
	instance := token.instance
	if instance == nil || token.id == 0 || instance.phase != .Ready ||
	   !instance.has_activation_time {
		return false
	}
	node, found := find_cancellation_node(instance, token.id)
	if !found {
		return false
	}
	deadline_ns := time.time_to_unix_nano(deadline)
	if node.has_deadline && time.time_to_unix_nano(node.deadline) <= deadline_ns {
		return true
	}
	now_ns := time.time_to_unix_nano(instance.activation_time)
	if deadline_ns > now_ns && now_ns < 0 && deadline_ns > max(i64) + now_ns {
		return false
	}
	node.has_deadline = true
	node.deadline = deadline
	if node.cancellation_requested {
		return true
	}
	remaining_ns := deadline_ns - now_ns
	if remaining_ns <= 0 {
		return request_cancellation_token(instance, token.id, cancellation_deadline_reason())
	}
	instance.next_timer_seq += 1
	node.deadline_timer_seq = instance.next_timer_seq
	append(
		&instance.timers,
		Fiber_Timer_Operation{
			seq = node.deadline_timer_seq,
			duration = time.Duration(remaining_ns),
			state = .Scheduled,
			detached_from_root_cancellation = !node.linked_to_root,
			cancellation_token_id = token.id,
		},
	)
	return true
}

// cancellation_child_with_deadline creates a child whose effective deadline
// is the earlier of its requested deadline and its parent's deadline. The
// deadline is driven by a durable Temporal timer and therefore replays.
cancellation_child_with_deadline :: proc(
	parent: Cancellation_Token,
	deadline: time.Time,
) -> Cancellation_Token {
	if parent.instance == nil || parent.id == 0 ||
	   !parent.instance.has_activation_time {
		return {}
	}
	token := cancellation_child(parent)
	if token.id == 0 || !set_cancellation_token_deadline(token, deadline) {
		return {}
	}
	return token
}

// cancellation_child_with_timeout derives an absolute deadline from the
// current Workflow activation timestamp. It never reads the host clock.
cancellation_child_with_timeout :: proc(
	parent: Cancellation_Token,
	timeout: time.Duration,
) -> Cancellation_Token {
	if parent.instance == nil || timeout <= 0 || !parent.instance.has_activation_time {
		return {}
	}
	now_ns := time.time_to_unix_nano(parent.instance.activation_time)
	if now_ns > max(i64) - i64(timeout) {
		return {}
	}
	return cancellation_child_with_deadline(
		parent,
		time.time_add(parent.instance.activation_time, timeout),
	)
}

// cancellation_detached_with_deadline creates an independent deadline tree
// that is not canceled by the Workflow root token.
cancellation_detached_with_deadline :: proc(
	ctx: ^Fiber_Context,
	deadline: time.Time,
) -> Cancellation_Token {
	if ctx == nil || ctx.instance == nil || !ctx.instance.has_activation_time {
		return {}
	}
	token := cancellation_detached(ctx)
	if token.id == 0 || !set_cancellation_token_deadline(token, deadline) {
		return {}
	}
	return token
}

cancellation_detached_with_timeout :: proc(
	ctx: ^Fiber_Context,
	timeout: time.Duration,
) -> Cancellation_Token {
	if ctx == nil || ctx.instance == nil || timeout <= 0 ||
	   !ctx.instance.has_activation_time {
		return {}
	}
	now_ns := time.time_to_unix_nano(ctx.instance.activation_time)
	if now_ns > max(i64) - i64(timeout) {
		return {}
	}
	return cancellation_detached_with_deadline(
		ctx,
		time.time_add(ctx.instance.activation_time, timeout),
	)
}

// cancellation_deadline returns the token's effective inherited deadline.
cancellation_deadline :: proc(token: Cancellation_Token) -> (time.Time, bool) {
	if token.instance == nil || token.id == 0 {
		return {}, false
	}
	node, found := find_cancellation_node(token.instance, token.id)
	if !found || !node.has_deadline {
		return {}, false
	}
	return node.deadline, true
}

cancellation_requested :: proc(token: Cancellation_Token) -> bool {
	return token.instance != nil &&
	       cancellation_token_is_requested_id(token.instance, token.id)
}

cancellation_token_reason :: proc(token: Cancellation_Token) -> string {
	if token.instance == nil {
		return ""
	}
	return cancellation_token_reason_id(token.instance, token.id)
}

// cancellation_request is deterministic language-runtime state. It emits no
// command by itself; linked pending operations emit their normal Core
// cancellation commands at the end of the activation.
cancellation_request :: proc(
	token: Cancellation_Token,
	reason := "Workflow cancellation token requested",
) -> bool {
	if token.instance == nil || token.id == 0 || token.instance.phase != .Ready {
		return false
	}
	return request_cancellation_token(token.instance, token.id, reason)
}

// cancellation_scope selects a token for operations created by the current
// fiber until cancellation_scope_end. Scopes must be ended in LIFO order.
cancellation_scope :: proc(
	ctx: ^Fiber_Context,
	token: Cancellation_Token,
) -> Cancellation_Scope {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   token.instance != ctx.instance || token.id == 0 {
		return {}
	}
	_, found := find_cancellation_node(ctx.instance, token.id)
	if !found {
		return {}
	}
	previous_id := current_cancellation_token_id(ctx.instance)
	previous_depth := ctx.instance.detached_scope_depth
	ctx.instance.current_cancellation_token_id = token.id
	ctx.instance.detached_scope_depth = token.id == 1 ||
		cancellation_token_linked_to_root(ctx.instance, token.id) ? 0 : 1
	return {
		instance = ctx.instance,
		previous_token_id = previous_id,
		selected_token_id = token.id,
		previous_detached_scope_depth = previous_depth,
		active = true,
	}
}

// detached_cancellation_scope is the compatibility convenience for creating
// and immediately selecting a detached token.
detached_cancellation_scope :: proc(ctx: ^Fiber_Context) -> Cancellation_Scope {
	token := cancellation_detached(ctx)
	return cancellation_scope(ctx, token)
}

cancellation_scope_end :: proc(scope: ^Cancellation_Scope) {
	if scope == nil || !scope.active || scope.instance == nil {
		return
	}
	// Preserve enough state for the caller to recover from an out-of-order end.
	// A later LIFO call can still close this scope after the nested one ends.
	if current_cancellation_token_id(scope.instance) != scope.selected_token_id {
		return
	}
	scope.instance.current_cancellation_token_id = scope.previous_token_id
	scope.instance.detached_scope_depth = scope.previous_detached_scope_depth
	scope.active = false
	scope.instance = nil
}

root_cancellation_applies :: proc(instance: ^Fiber_Instance) -> bool {
	return instance != nil && instance.cancellation_requested && instance.detached_scope_depth == 0
}

current_cancellation_applies :: proc(instance: ^Fiber_Instance) -> bool {
	return instance != nil && cancellation_token_is_requested_id(
		instance,
		current_cancellation_token_id(instance),
	)
}

activity_options_error :: proc(activity_type: string, options: Activity_Options) -> string {
	if len(activity_type) == 0 {
		return "Activity type must not be empty"
	}
	if options.schedule_to_close_timeout <= 0 && options.start_to_close_timeout <= 0 {
		return "Activity requires a positive Schedule-to-Close or Start-to-Close timeout"
	}
	if options.schedule_to_close_timeout < 0 ||
	   options.schedule_to_start_timeout < 0 ||
	   options.start_to_close_timeout < 0 ||
	   options.heartbeat_timeout < 0 {
		return "Activity timeouts must not be negative"
	}
	if options.schedule_to_close_timeout > 0 &&
	   options.start_to_close_timeout > options.schedule_to_close_timeout {
		return "Activity Start-to-Close timeout must not exceed Schedule-to-Close timeout"
	}
	if options.schedule_to_close_timeout > 0 &&
	   options.schedule_to_start_timeout > options.schedule_to_close_timeout {
		return "Activity Schedule-to-Start timeout must not exceed Schedule-to-Close timeout"
	}
	if options.retry_policy != nil {
		policy := options.retry_policy
		if policy.initial_interval < 0 || policy.maximum_interval < 0 {
			return "Activity Retry Policy intervals must not be negative"
		}
		if math.is_nan(policy.backoff_coefficient) || math.is_inf(policy.backoff_coefficient) {
			return "Activity Retry Policy backoff coefficient must be finite"
		}
		if policy.backoff_coefficient != 0 && policy.backoff_coefficient < 1 {
			return "Activity Retry Policy backoff coefficient must be zero or at least one"
		}
		if policy.maximum_interval > 0 &&
		   policy.initial_interval > 0 &&
		   policy.maximum_interval < policy.initial_interval {
			return "Activity Retry Policy maximum interval must not be shorter than its initial interval"
		}
		if policy.maximum_attempts < 0 {
			return "Activity Retry Policy maximum attempts must not be negative"
		}
	}
	if options.headers != nil && options.headers.allocator.procedure == nil {
		return "Activity headers are not initialized"
	}
	if int(options.versioning_intent) < int(Versioning_Intent.Unspecified) ||
	   int(options.versioning_intent) > int(Versioning_Intent.Default) {
		return "Activity versioning intent is invalid"
	}
	if options.priority != nil {
		priority := options.priority
		if priority.priority_key < 0 {
			return "Activity priority key must not be negative"
		}
		if len(priority.fairness_key) > 64 {
			return "Activity priority fairness key must not exceed 64 bytes"
		}
		if math.is_nan(priority.fairness_weight) || math.is_inf(priority.fairness_weight) ||
		   priority.fairness_weight < 0 {
			return "Activity priority fairness weight must be finite and non-negative"
		}
	}
	return ""
}

mark_activity_options_failure :: proc(ctx: ^Fiber_Context, message: string) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	ctx.instance.failure_message = message
	ctx.instance.failure_type = "OdinActivityOptionsError"
	ctx.instance.phase = .Failed
}

clone_activity_options :: proc(
	instance: ^Fiber_Instance,
	options: Activity_Options,
) -> (owned: Owned_Activity_Options, ok: bool) {
	allocator := instance.odin_context.allocator
	if len(options.task_queue) > 0 {
		queue, queue_error := strings.clone(options.task_queue, allocator)
		if queue_error != nil {
			return {}, false
		}
		owned.task_queue = queue
	}
	owned.schedule_to_close_timeout = options.schedule_to_close_timeout
	owned.schedule_to_start_timeout = options.schedule_to_start_timeout
	owned.start_to_close_timeout = options.start_to_close_timeout
	owned.heartbeat_timeout = options.heartbeat_timeout
	owned.cancellation_type = options.cancellation_type
	owned.disable_eager_execution = options.disable_eager_execution
	owned.versioning_intent = options.versioning_intent
	if !clone_workflow_headers(options.headers, &owned.headers, allocator) {
		return {}, false
	}
	if options.priority != nil {
		owned.has_priority = true
		owned.priority = options.priority^
		if len(options.priority.fairness_key) > 0 {
			owned.priority.fairness_key, _ = strings.clone(
				options.priority.fairness_key,
				allocator,
			)
			if owned.priority.fairness_key == "" {
				return {}, false
			}
		}
	}
	owned.has_summary = len(options.summary) > 0
	if owned.has_summary {
		encode_error: codec.Error
		owned.summary, encode_error = codec.encode_json(options.summary, allocator)
		if encode_error != .None {
			return {}, false
		}
	}
	if options.retry_policy != nil {
		owned.has_retry_policy = true
		owned.retry_policy = options.retry_policy^
		error_types := make(
			[]string,
			len(options.retry_policy.non_retryable_error_types),
			allocator,
		)
		owned.retry_policy.non_retryable_error_types = error_types
		for error_type, index in options.retry_policy.non_retryable_error_types {
			cloned, clone_error := strings.clone(error_type, allocator)
			if clone_error != nil {
				return {}, false
			}
			owned.retry_policy.non_retryable_error_types[index] = cloned
		}
	}
	return owned, true
}

activity_inputs_free :: proc(inputs: ^[dynamic]codec.Payload) {
	if inputs == nil {
		return
	}
	for &input in inputs^ {
		codec.payload_free(&input)
	}
	delete(inputs^)
	inputs^ = nil
}

clone_activity_inputs :: proc(
	instance: ^Fiber_Instance,
	inputs: []codec.Payload_View,
) -> (owned: [dynamic]codec.Payload, ok: bool) {
	if instance == nil {
		return nil, false
	}
	allocator := instance.odin_context.allocator
	owned = make([dynamic]codec.Payload, allocator)
	for input in inputs {
		payload, clone_error := codec.payload_clone(input, allocator)
		if clone_error != .None {
			activity_inputs_free(&owned)
			return nil, false
		}
		append(&owned, payload)
	}
	return owned, true
}

start_activity_owned :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: [dynamic]codec.Payload,
	options: Activity_Options,
) -> Activity_Future {
	owned_inputs := inputs
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		activity_inputs_free(&owned_inputs)
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {}
	}
	instance := ctx.instance
	token_id, token_valid := selected_cancellation_token_id(
		instance,
		options.cancellation_token,
	)
	if !token_valid {
		activity_inputs_free(&owned_inputs)
		mark_activity_options_failure(ctx, "Activity cancellation token does not belong to this Workflow run")
		return {}
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		activity_inputs_free(&owned_inputs)
		instance.next_activity_seq += 1
		append(
			&instance.activities,
			Fiber_Activity_Operation{
				seq = instance.next_activity_seq,
				command_seq = instance.next_activity_seq,
				state = .Canceled,
				cancellation_token_id = token_id,
				failure_message = cancellation_token_reason_id(
					instance,
					token_id,
				),
			},
		)
		return {seq = instance.next_activity_seq}
	}
	validation_error := activity_options_error(activity_type, options)
	if len(validation_error) > 0 {
		activity_inputs_free(&owned_inputs)
		mark_activity_options_failure(ctx, validation_error)
		return {}
	}
	owned_activity_type, type_error := strings.clone(
		activity_type,
		instance.odin_context.allocator,
	)
	if type_error != nil {
		activity_inputs_free(&owned_inputs)
		mark_activity_options_failure(ctx, "could not preserve Activity type")
		return {}
	}
	owned_options, options_ok := clone_activity_options(instance, options)
	if !options_ok {
		activity_inputs_free(&owned_inputs)
		mark_activity_options_failure(ctx, "could not preserve Activity options")
		return {}
	}
	instance.next_activity_seq += 1
	activity_id := options.activity_id
	if len(activity_id) == 0 {
		activity_id = fmt.aprintf("activity-%d", instance.next_activity_seq)
	} else {
		cloned_id, id_error := strings.clone(
			activity_id,
			instance.odin_context.allocator,
		)
		if id_error != nil {
			activity_inputs_free(&owned_inputs)
			mark_activity_options_failure(ctx, "could not preserve Activity ID")
			return {}
		}
		activity_id = cloned_id
	}
	operation := Fiber_Activity_Operation{
		seq = instance.next_activity_seq,
		command_seq = instance.next_activity_seq,
		activity_id = activity_id,
		activity_type = owned_activity_type,
		options = owned_options,
		inputs = owned_inputs,
		state = .Scheduled,
		detached_from_root_cancellation = !cancellation_token_linked_to_root(
			instance,
			token_id,
		),
		cancellation_token_id = token_id,
	}
	append(&instance.activities, operation)
	return Activity_Future{seq = operation.seq}
}

// start_activity_payload records a deterministic Activity command without
// suspending. The handle contains only a sequence number because the operation
// table can move. The borrowed inputs are copied into the Workflow instance arena.
start_activity_payload :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
) -> Activity_Future {
	inputs := [1]codec.Payload_View{input}
	return start_activity_payloads(ctx, activity_type, inputs[:])
}

start_activity_payloads :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: []codec.Payload_View,
) -> Activity_Future {
	retry_policy := Retry_Policy{maximum_attempts = 1}
	return start_activity_payloads_with_options(
		ctx,
		activity_type,
		inputs,
		{
			start_to_close_timeout = 20 * time.Second,
			retry_policy = &retry_policy,
		},
	)
}

start_activity_payload_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
	options: Activity_Options,
) -> Activity_Future {
	inputs := [1]codec.Payload_View{input}
	return start_activity_payloads_with_options(ctx, activity_type, inputs[:], options)
}

start_activity_payloads_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: []codec.Payload_View,
	options: Activity_Options,
) -> Activity_Future {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {}
	}
	headers := Headers{}
	if !outbound_headers_init(&headers, options.headers, ctx.instance) {
		ctx.instance.phase = .Failed
		ctx.instance.failure_message = "could not initialize Workflow Activity interceptor headers"
		ctx.instance.failure_type = "OdinWorkflowInterceptorError"
		return {}
	}
	defer headers_free(&headers)
	intercepted_options := options
	intercepted_options.headers = &headers
	interceptor_input := Outbound_Start_Activity_Input{
		ctx = ctx,
		activity_type = activity_type,
		inputs = inputs,
		options = intercepted_options,
		headers = &headers,
	}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	return outbound_start_activity_chain(&state, &interceptor_input)
}

start_activity_json :: proc(ctx: ^Fiber_Context, activity_type: string, input: $T) -> Activity_Future {
	retry_policy := Retry_Policy{maximum_attempts = 1}
	return start_activity_json_with_options(
		ctx,
		activity_type,
		input,
		{
			start_to_close_timeout = 20 * time.Second,
			retry_policy = &retry_policy,
		},
	)
}

start_activity_json_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $T,
	options: Activity_Options,
) -> Activity_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		ctx.instance.phase = .Failed
		ctx.instance.failure_message = "could not encode Activity input as json/plain"
		ctx.instance.failure_type = "OdinWorkflowPayloadConversionError"
		return {}
	}
	defer codec.payload_free(&payload)
	return start_activity_payload_with_options(
		ctx,
		activity_type,
		codec.payload_view(&payload),
		options,
	)
}

start_activity :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> Activity_Future {
	return start_activity_json(ctx, activity_type, input)
}

start_activity_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
	options: Activity_Options,
) -> Activity_Future {
	return start_activity_json_with_options(ctx, activity_type, input, options)
}

// cancel_activity requests deterministic cancellation of a started Activity.
// Awaiting a Wait_Cancellation_Completed Activity remains suspended until Core
// returns the Activity worker's cancellation failure and optional details.
cancel_activity :: proc(ctx: ^Fiber_Context, future: Activity_Future) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		return false
	}
	operation, found := find_activity_operation(ctx.instance, future.seq)
	if !found {
		return false
	}
	switch operation.state {
	case .Scheduled:
		operation.state = .Canceled
		return true
	case .Commanded:
		abandon := operation.options.cancellation_type == .Abandon
		if operation.is_local {
			abandon = operation.local_options.cancellation_type == .Abandon
		}
		if abandon {
			operation.state = .Canceled
		} else {
			operation.state = .Cancel_Requested
		}
		return true
	case .Cancel_Requested:
		return true
	case .Resolved, .Failed, .Canceled:
		return false
	}
	return false
}

local_activity_options_error :: proc(
	activity_type: string,
	options: Local_Activity_Options,
) -> string {
	if len(activity_type) == 0 {
		return "Local Activity type must not be empty"
	}
	if options.schedule_to_close_timeout <= 0 && options.start_to_close_timeout <= 0 {
		return "Local Activity requires a positive Schedule-to-Close or Start-to-Close timeout"
	}
	if options.schedule_to_close_timeout < 0 || options.schedule_to_start_timeout < 0 ||
	   options.start_to_close_timeout < 0 || options.local_retry_threshold < 0 {
		return "Local Activity timeouts must not be negative"
	}
	if options.schedule_to_close_timeout > 0 &&
	   options.start_to_close_timeout > options.schedule_to_close_timeout {
		return "Local Activity Start-to-Close timeout must not exceed Schedule-to-Close timeout"
	}
	if options.schedule_to_close_timeout > 0 &&
	   options.schedule_to_start_timeout > options.schedule_to_close_timeout {
		return "Local Activity Schedule-to-Start timeout must not exceed Schedule-to-Close timeout"
	}
	if options.retry_policy != nil {
		policy := options.retry_policy
		if policy.initial_interval < 0 || policy.maximum_interval < 0 ||
		   policy.maximum_attempts < 0 {
			return "Local Activity Retry Policy values must not be negative"
		}
		if math.is_nan(policy.backoff_coefficient) || math.is_inf(policy.backoff_coefficient) ||
		   (policy.backoff_coefficient != 0 && policy.backoff_coefficient < 1) {
			return "Local Activity Retry Policy backoff coefficient must be zero or at least one"
		}
		if policy.maximum_interval > 0 && policy.initial_interval > 0 &&
		   policy.maximum_interval < policy.initial_interval {
			return "Local Activity Retry Policy maximum interval must not be shorter than its initial interval"
		}
	}
	if options.headers != nil && options.headers.allocator.procedure == nil {
		return "Local Activity headers are not initialized"
	}
	return ""
}

clone_local_activity_options :: proc(
	instance: ^Fiber_Instance,
	options: Local_Activity_Options,
) -> (owned: Owned_Local_Activity_Options, ok: bool) {
	allocator := instance.odin_context.allocator
	owned.schedule_to_close_timeout = options.schedule_to_close_timeout
	owned.schedule_to_start_timeout = options.schedule_to_start_timeout
	owned.start_to_close_timeout = options.start_to_close_timeout
	owned.local_retry_threshold = options.local_retry_threshold
	owned.cancellation_type = options.cancellation_type
	owned.include_arguments_in_marker = options.include_arguments_in_marker
	owned.has_retry_policy = true
	if !clone_workflow_headers(options.headers, &owned.headers, allocator) {
		return {}, false
	}
	owned.has_summary = len(options.summary) > 0
	if owned.has_summary {
		encode_error: codec.Error
		owned.summary, encode_error = codec.encode_json(options.summary, allocator)
		if encode_error != .None {
			return {}, false
		}
	}
	if options.retry_policy == nil {
		owned.retry_policy.maximum_attempts = 1
		return owned, true
	}
	owned.retry_policy = options.retry_policy^
	owned.retry_policy.non_retryable_error_types = make(
		[]string,
		len(options.retry_policy.non_retryable_error_types),
		allocator,
	)
	for error_type, index in options.retry_policy.non_retryable_error_types {
		cloned, clone_error := strings.clone(error_type, allocator)
		if clone_error != nil {
			return {}, false
		}
		owned.retry_policy.non_retryable_error_types[index] = cloned
	}
	return owned, true
}

start_local_activity_owned :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: [dynamic]codec.Payload,
	options: Local_Activity_Options,
) -> Activity_Future {
	owned_inputs := inputs
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		activity_inputs_free(&owned_inputs)
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {}
	}
	instance := ctx.instance
	token_id, token_valid := selected_cancellation_token_id(
		instance,
		options.cancellation_token,
	)
	if !token_valid {
		activity_inputs_free(&owned_inputs)
		mark_activity_options_failure(
			ctx,
			"Local Activity cancellation token does not belong to this Workflow run",
		)
		return {}
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		activity_inputs_free(&owned_inputs)
		instance.next_activity_seq += 1
		append(
			&instance.activities,
			Fiber_Activity_Operation{
				seq = instance.next_activity_seq,
				command_seq = instance.next_activity_seq,
				is_local = true,
				attempt = 1,
				state = .Canceled,
				cancellation_token_id = token_id,
				failure_message = cancellation_token_reason_id(
					instance,
					token_id,
				),
			},
		)
		return {seq = instance.next_activity_seq}
	}
	validation_error := local_activity_options_error(activity_type, options)
	if len(validation_error) > 0 {
		activity_inputs_free(&owned_inputs)
		mark_activity_options_failure(ctx, validation_error)
		return {}
	}
	owned_activity_type, type_error := strings.clone(
		activity_type,
		instance.odin_context.allocator,
	)
	owned_options, options_ok := clone_local_activity_options(instance, options)
	if type_error != nil || !options_ok {
		activity_inputs_free(&owned_inputs)
		mark_activity_options_failure(ctx, "could not preserve Local Activity options")
		return {}
	}
	instance.next_activity_seq += 1
	activity_id := options.activity_id
	if len(activity_id) == 0 {
		activity_id = fmt.aprintf("local-activity-%d", instance.next_activity_seq)
	} else {
		cloned_id, id_error := strings.clone(activity_id, instance.odin_context.allocator)
		if id_error != nil {
			activity_inputs_free(&owned_inputs)
			mark_activity_options_failure(ctx, "could not preserve Local Activity ID")
			return {}
		}
		activity_id = cloned_id
	}
	operation := Fiber_Activity_Operation{
		seq = instance.next_activity_seq,
		command_seq = instance.next_activity_seq,
		activity_id = activity_id,
		activity_type = owned_activity_type,
		is_local = true,
		attempt = 1,
		local_options = owned_options,
		inputs = owned_inputs,
		state = .Scheduled,
		detached_from_root_cancellation = !cancellation_token_linked_to_root(
			instance,
			token_id,
		),
		cancellation_token_id = token_id,
	}
	append(&instance.activities, operation)
	return {seq = operation.seq}
}

start_local_activity_payload_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
	options: Local_Activity_Options,
) -> Activity_Future {
	inputs := [1]codec.Payload_View{input}
	return start_local_activity_payloads_with_options(ctx, activity_type, inputs[:], options)
}

start_local_activity_payloads_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: []codec.Payload_View,
	options: Local_Activity_Options,
) -> Activity_Future {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		return {}
	}
	headers := Headers{}
	if !outbound_headers_init(&headers, options.headers, ctx.instance) {
		ctx.instance.phase = .Failed
		ctx.instance.failure_message = "could not initialize Workflow Local Activity interceptor headers"
		ctx.instance.failure_type = "OdinWorkflowInterceptorError"
		return {}
	}
	defer headers_free(&headers)
	intercepted_options := options
	intercepted_options.headers = &headers
	interceptor_input := Outbound_Start_Local_Activity_Input{
		ctx = ctx,
		activity_type = activity_type,
		inputs = inputs,
		options = intercepted_options,
		headers = &headers,
	}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	return outbound_start_local_activity_chain(&state, &interceptor_input)
}

start_local_activity_payload :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
) -> Activity_Future {
	inputs := [1]codec.Payload_View{input}
	return start_local_activity_payloads(ctx, activity_type, inputs[:])
}

start_local_activity_payloads :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: []codec.Payload_View,
) -> Activity_Future {
	return start_local_activity_payloads_with_options(
		ctx,
		activity_type,
		inputs,
		{start_to_close_timeout = 20 * time.Second},
	)
}

start_local_activity_json_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $T,
	options: Local_Activity_Options,
) -> Activity_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		ctx.instance.phase = .Failed
		ctx.instance.failure_message = "could not encode Local Activity input as json/plain"
		ctx.instance.failure_type = "OdinWorkflowPayloadConversionError"
		return {}
	}
	defer codec.payload_free(&payload)
	return start_local_activity_payload_with_options(
		ctx,
		activity_type,
		codec.payload_view(&payload),
		options,
	)
}

start_local_activity_json :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $T,
) -> Activity_Future {
	return start_local_activity_json_with_options(
		ctx,
		activity_type,
		input,
		{start_to_close_timeout = 20 * time.Second},
	)
}

start_local_activity :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
) -> Activity_Future {
	return start_local_activity_json(ctx, activity_type, input)
}

start_local_activity_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
	options: Local_Activity_Options,
) -> Activity_Future {
	return start_local_activity_json_with_options(ctx, activity_type, input, options)
}

child_workflow_options_error :: proc(
	workflow_type: string,
	options: Child_Workflow_Options,
) -> string {
	if len(workflow_type) == 0 {
		return "Child Workflow type must not be empty"
	}
	if len(options.workflow_id) == 0 {
		return "Child Workflow ID must not be empty"
	}
	if options.workflow_execution_timeout < 0 || options.workflow_run_timeout < 0 ||
	   options.workflow_task_timeout < 0 {
		return "Child Workflow timeouts must not be negative"
	}
	if options.workflow_execution_timeout > 0 && options.workflow_run_timeout > options.workflow_execution_timeout {
		return "Child Workflow Run timeout must not exceed Execution timeout"
	}
	if options.retry_policy != nil {
		policy := options.retry_policy
		if policy.initial_interval < 0 || policy.maximum_interval < 0 || policy.maximum_attempts < 0 {
			return "Child Workflow Retry Policy values must not be negative"
		}
		if math.is_nan(policy.backoff_coefficient) || math.is_inf(policy.backoff_coefficient) ||
		   (policy.backoff_coefficient != 0 && policy.backoff_coefficient < 1) {
			return "Child Workflow Retry Policy backoff coefficient must be zero or at least one"
		}
		if policy.maximum_interval > 0 && policy.initial_interval > 0 &&
		   policy.maximum_interval < policy.initial_interval {
			return "Child Workflow Retry Policy maximum interval must not be shorter than its initial interval"
		}
	}
	if options.headers != nil && options.headers.allocator.procedure == nil {
		return "Child Workflow headers are not initialized"
	}
	if options.memo != nil && options.memo.allocator.procedure == nil {
		return "Child Workflow memo is not initialized"
	}
	if options.search_attributes != nil &&
	   options.search_attributes.allocator.procedure == nil {
		return "Child Workflow Search Attributes are not initialized"
	}
	if int(options.id_reuse_policy) < int(Workflow_Id_Reuse_Policy.Unspecified) ||
	   int(options.id_reuse_policy) > int(Workflow_Id_Reuse_Policy.Terminate_If_Running) {
		return "Child Workflow ID reuse policy is invalid"
	}
	if int(options.versioning_intent) < int(Versioning_Intent.Unspecified) ||
	   int(options.versioning_intent) > int(Versioning_Intent.Default) {
		return "Child Workflow versioning intent is invalid"
	}
	if options.priority != nil {
		priority := options.priority
		if priority.priority_key < 0 {
			return "Child Workflow priority key must not be negative"
		}
		if len(priority.fairness_key) > 64 {
			return "Child Workflow priority fairness key must not exceed 64 bytes"
		}
		if math.is_nan(priority.fairness_weight) || math.is_inf(priority.fairness_weight) ||
		   priority.fairness_weight < 0 {
			return "Child Workflow priority fairness weight must be finite and non-negative"
		}
	}
	return ""
}

clone_child_workflow_options :: proc(
	instance: ^Fiber_Instance,
	options: Child_Workflow_Options,
) -> (owned: Owned_Child_Workflow_Options, ok: bool) {
	allocator := instance.odin_context.allocator
	if len(options.task_queue) > 0 {
		owned.task_queue, _ = strings.clone(options.task_queue, allocator)
		if owned.task_queue == "" {
			return {}, false
		}
	}
	owned.workflow_execution_timeout = options.workflow_execution_timeout
	owned.workflow_run_timeout = options.workflow_run_timeout
	owned.workflow_task_timeout = options.workflow_task_timeout
	owned.cancellation_type = options.cancellation_type
	owned.parent_close_policy = options.parent_close_policy
	owned.id_reuse_policy = options.id_reuse_policy
	owned.versioning_intent = options.versioning_intent
	if len(options.cron_schedule) > 0 {
		owned.cron_schedule, _ = strings.clone(options.cron_schedule, allocator)
		if owned.cron_schedule == "" {
			return {}, false
		}
	}
	if !clone_workflow_headers(options.headers, &owned.headers, allocator) {
		return {}, false
	}
	if !clone_workflow_payload_map(options.memo, &owned.memo, allocator) {
		return {}, false
	}
	owned.has_search_attributes = options.search_attributes != nil
	if !clone_workflow_payload_map(
		options.search_attributes,
		&owned.search_attributes,
		allocator,
	) {
		return {}, false
	}
	if options.priority != nil {
		owned.has_priority = true
		owned.priority = options.priority^
		if len(options.priority.fairness_key) > 0 {
			owned.priority.fairness_key, _ = strings.clone(
				options.priority.fairness_key,
				allocator,
			)
			if owned.priority.fairness_key == "" {
				return {}, false
			}
		}
	}
	owned.has_static_summary = len(options.static_summary) > 0
	if owned.has_static_summary {
		encode_error: codec.Error
		owned.static_summary, encode_error = codec.encode_json(options.static_summary, allocator)
		if encode_error != .None {
			return {}, false
		}
	}
	owned.has_static_details = len(options.static_details) > 0
	if owned.has_static_details {
		encode_error: codec.Error
		owned.static_details, encode_error = codec.encode_json(options.static_details, allocator)
		if encode_error != .None {
			return {}, false
		}
	}
	if options.retry_policy != nil {
		owned.has_retry_policy = true
		owned.retry_policy = options.retry_policy^
		owned.retry_policy.non_retryable_error_types = make(
			[]string,
			len(options.retry_policy.non_retryable_error_types),
			allocator,
		)
		for error_type, index in options.retry_policy.non_retryable_error_types {
			cloned, clone_error := strings.clone(error_type, allocator)
			if clone_error != nil {
				return {}, false
			}
			owned.retry_policy.non_retryable_error_types[index] = cloned
		}
	}
	return owned, true
}

mark_child_workflow_failure :: proc(ctx: ^Fiber_Context, message, failure_type: string) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	ctx.instance.failure_message = message
	ctx.instance.failure_type = failure_type
	ctx.instance.phase = .Failed
}

start_child_workflow_owned :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	inputs: [dynamic]codec.Payload,
	options: Child_Workflow_Options,
) -> Child_Workflow_Future {
	owned_inputs := inputs
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready {
		activity_inputs_free(&owned_inputs)
		return {}
	}
	instance := ctx.instance
	token_id, token_valid := selected_cancellation_token_id(
		instance,
		options.cancellation_token,
	)
	if !token_valid {
		activity_inputs_free(&owned_inputs)
		mark_child_workflow_failure(
			ctx,
			"Child Workflow cancellation token does not belong to this Workflow run",
			"OdinChildWorkflowOptionsError",
		)
		return {}
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		activity_inputs_free(&owned_inputs)
		instance.next_child_workflow_seq += 1
		reason := cancellation_token_reason_id(
			instance,
			token_id,
		)
		append(
			&instance.child_workflows,
			Fiber_Child_Workflow_Operation{
				seq = instance.next_child_workflow_seq,
				command_state = .Canceled,
				start_state = .Canceled,
				completion_state = .Canceled,
				cancellation_reason = reason,
				failure_message = reason,
				cancellation_token_id = token_id,
			},
		)
		return {seq = instance.next_child_workflow_seq}
	}
	validation_error := child_workflow_options_error(workflow_type, options)
	if len(validation_error) > 0 {
		activity_inputs_free(&owned_inputs)
		mark_child_workflow_failure(ctx, validation_error, "OdinChildWorkflowOptionsError")
		return {}
	}
	owned_type, type_error := strings.clone(workflow_type, instance.odin_context.allocator)
	owned_id, id_error := strings.clone(options.workflow_id, instance.odin_context.allocator)
	owned_options, options_ok := clone_child_workflow_options(instance, options)
	if type_error != nil || id_error != nil || !options_ok {
		activity_inputs_free(&owned_inputs)
		mark_child_workflow_failure(ctx, "could not preserve Child Workflow options", "OdinChildWorkflowOptionsError")
		return {}
	}
	instance.next_child_workflow_seq += 1
	operation := Fiber_Child_Workflow_Operation{
		seq = instance.next_child_workflow_seq,
		workflow_id = owned_id,
		workflow_type = owned_type,
		options = owned_options,
		inputs = owned_inputs,
		command_state = .Scheduled,
		start_state = .Pending,
		completion_state = .Pending,
		detached_from_root_cancellation = !cancellation_token_linked_to_root(
			instance,
			token_id,
		),
		cancellation_token_id = token_id,
	}
	append(&instance.child_workflows, operation)
	return {seq = operation.seq}
}

start_child_workflow_payloads :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	inputs: []codec.Payload_View,
	options: Child_Workflow_Options,
) -> Child_Workflow_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	headers := Headers{}
	if !outbound_headers_init(&headers, options.headers, ctx.instance) {
		mark_child_workflow_failure(ctx, "could not initialize Child Workflow interceptor headers", "OdinWorkflowInterceptorError")
		return {}
	}
	defer headers_free(&headers)
	intercepted_options := options
	intercepted_options.headers = &headers
	interceptor_input := Outbound_Start_Child_Workflow_Input{
		ctx = ctx,
		workflow_type = workflow_type,
		inputs = inputs,
		options = intercepted_options,
		headers = &headers,
	}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	return outbound_start_child_workflow_chain(&state, &interceptor_input)
}

start_child_workflow_payload :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	input: codec.Payload_View,
	options: Child_Workflow_Options,
) -> Child_Workflow_Future {
	inputs := [?]codec.Payload_View{input}
	return start_child_workflow_payloads(ctx, workflow_type, inputs[:], options)
}

execute_child_workflow_payloads :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	inputs: []codec.Payload_View,
	options: Child_Workflow_Options,
) -> Payload_Child_Workflow_Result {
	return await_child_workflow_payload_result(
		ctx,
		start_child_workflow_payloads(ctx, workflow_type, inputs, options),
	)
}

start_child_workflow_json :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	input: $T,
	options: Child_Workflow_Options,
) -> Child_Workflow_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		mark_child_workflow_failure(ctx, "could not encode Child Workflow input as json/plain", "OdinWorkflowPayloadConversionError")
		return {}
	}
	defer codec.payload_free(&payload)
	return start_child_workflow_payload(
		ctx,
		workflow_type,
		codec.payload_view(&payload),
		options,
	)
}

child_workflow_start_result_from_operation :: proc(
	operation: ^Fiber_Child_Workflow_Operation,
) -> Child_Workflow_Start_Result {
	switch operation.start_state {
	case .Succeeded:
		return {status = .Succeeded, run_id = operation.run_id}
	case .Failed:
		return {
			status = .Failed,
			workflow_id = operation.workflow_id,
			workflow_type = operation.workflow_type,
			cause = operation.start_failure_cause,
			failure_message = operation.failure_message,
		}
	case .Canceled:
		return {status = .Canceled, failure_message = operation.failure_message}
	case .Pending:
	}
	return {status = .Failed, failure_message = "Child Workflow start has not resolved"}
}

child_workflow_start_failure_cause_from_proto :: proc(
	value: proto.Coresdk_Child_Workflow_StartChildWorkflowExecutionFailedCause,
) -> Child_Workflow_Start_Failure_Cause {
	switch value {
	case .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS:
		return .Workflow_Already_Exists
	case .START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED:
		return .Unspecified
	}
	return .Unspecified
}

await_child_workflow_start :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
) -> Child_Workflow_Start_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		return {status = .Failed, failure_message = "invalid Child Workflow start await"}
	}
	instance := ctx.instance
	operation, found := find_child_workflow_operation(instance, future.seq)
	if !found {
		mark_child_workflow_failure(ctx, "Child Workflow operation was not found", "OdinChildWorkflowRuntimeError")
		return {status = .Failed, failure_message = "Child Workflow operation was not found"}
	}
	if operation.start_state == .Pending {
		instance.waiting_child_workflow_seq = future.seq
		instance.phase = .Waiting_Child_Workflow_Start
		if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
			mark_child_workflow_failure(ctx, "could not resume Child Workflow start await", "OdinChildWorkflowRuntimeError")
			return {status = .Failed, failure_message = "could not resume Child Workflow start await"}
		}
		operation, found = find_child_workflow_operation(instance, future.seq)
	}
	if !found || operation.start_state == .Pending {
		mark_child_workflow_failure(ctx, "Child Workflow start resumed without a resolution", "OdinChildWorkflowRuntimeError")
		return {status = .Failed, failure_message = "Child Workflow start resumed without a resolution"}
	}
	return child_workflow_start_result_from_operation(operation)
}

payload_child_workflow_result_from_operation :: proc(
	operation: ^Fiber_Child_Workflow_Operation,
) -> Payload_Child_Workflow_Result {
	switch operation.completion_state {
	case .Succeeded:
		return {status = .Succeeded, value = codec.payload_view(&operation.result)}
	case .Failed:
		return {
			status = .Failed,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	case .Canceled:
		return {
			status = .Canceled,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	case .Pending:
	}
	return {status = .Failed, failure_message = "Child Workflow has not completed"}
}

await_child_workflow_payload_result :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
) -> Payload_Child_Workflow_Result {
	start_result := await_child_workflow_start(ctx, future)
	if start_result.status == .Failed {
		return {status = .Failed, failure_message = start_result.failure_message}
	}
	if start_result.status == .Canceled {
		return {status = .Canceled, failure_message = start_result.failure_message}
	}
	instance := ctx.instance
	operation, found := find_child_workflow_operation(instance, future.seq)
	if !found {
		return {status = .Failed, failure_message = "Child Workflow operation was not found"}
	}
	if operation.completion_state == .Pending {
		instance.waiting_child_workflow_seq = future.seq
		instance.phase = .Waiting_Child_Workflow
		if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
			mark_child_workflow_failure(ctx, "could not resume Child Workflow completion await", "OdinChildWorkflowRuntimeError")
			return {status = .Failed, failure_message = "could not resume Child Workflow completion await"}
		}
		operation, found = find_child_workflow_operation(instance, future.seq)
	}
	if !found || operation.completion_state == .Pending {
		mark_child_workflow_failure(ctx, "Child Workflow resumed without a completion", "OdinChildWorkflowRuntimeError")
		return {status = .Failed, failure_message = "Child Workflow resumed without a completion"}
	}
	return payload_child_workflow_result_from_operation(operation)
}

await_child_workflow_json_result :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	$T: typeid,
) -> Json_Child_Workflow_Result(T) {
	result := await_child_workflow_payload_result(ctx, future)
	if result.status != .Succeeded {
		return {
			status = result.status,
			failure_message = result.failure_message,
			failure = result.failure,
		}
	}
	value: T
	if codec.decode_json(result.value, &value) != .None {
		mark_child_workflow_failure(ctx, "could not decode Child Workflow result as json/plain", "OdinWorkflowPayloadConversionError")
		return {status = .Failed, failure_message = "could not decode Child Workflow result as json/plain"}
	}
	return {status = .Succeeded, value = value}
}

await_child_workflow_result :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
) -> Child_Workflow_Result {
	result := await_child_workflow_json_result(ctx, future, string)
	return {
		status = result.status,
		value = result.value,
		failure_message = result.failure_message,
		failure = result.failure,
	}
}

execute_child_workflow_json_result :: proc(
	ctx: ^Fiber_Context,
	workflow_type: string,
	input: $Input,
	options: Child_Workflow_Options,
	$Output: typeid,
) -> Json_Child_Workflow_Result(Output) {
	return await_child_workflow_json_result(
		ctx,
		start_child_workflow_json(ctx, workflow_type, input, options),
		Output,
	)
}

execute_child_workflow :: proc(
	ctx: ^Fiber_Context,
	workflow_type, input: string,
	options: Child_Workflow_Options,
) -> string {
	future := start_child_workflow_json(ctx, workflow_type, input, options)
	result := await_child_workflow_json_result(ctx, future, string)
	if result.status == .Failed ||
	   (result.status == .Canceled && ctx != nil && !root_cancellation_applies(ctx.instance)) {
		operation, found := find_child_workflow_operation(ctx.instance, future.seq)
		if found && len(operation.failure_proto) > 0 {
			ctx.instance.unhandled_child_workflow_seq = future.seq
			ctx.instance.phase = .Failed
		} else {
			mark_child_workflow_failure(ctx, result.failure_message, "OdinChildWorkflowExecutionError")
		}
		return ""
	}
	return result.value
}

// cancel_child_workflow suppresses a start command that has not been emitted,
// or requests cancellation through Core using the cancellation type selected
// when the child was started. The Future remains valid and can be awaited for
// the resulting terminal state.
cancel_child_workflow_terminal :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	reason := "Child Workflow cancellation requested",
) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		return false
	}
	instance := ctx.instance
	operation, found := find_child_workflow_operation(instance, future.seq)
	if !found || operation.completion_state != .Pending {
		return false
	}
	if operation.command_state == .Cancel_Requested {
		return true
	}
	if operation.command_state != .Scheduled && operation.command_state != .Commanded {
		return false
	}
	owned_reason, allocation_error := strings.clone(reason, instance.odin_context.allocator)
	if allocation_error != nil {
		mark_child_workflow_failure(
			ctx,
			"could not preserve Child Workflow cancellation reason",
			"OdinChildWorkflowCancellationError",
		)
		return false
	}
	operation.cancellation_reason = owned_reason
	operation.failure_message = owned_reason

	if operation.command_state == .Scheduled {
		operation.command_state = .Canceled
		operation.start_state = .Canceled
		operation.completion_state = .Canceled
		return true
	}

	switch operation.options.cancellation_type {
	case .Abandon:
		operation.command_state = .Resolved
		if operation.start_state == .Pending {
			operation.start_state = .Canceled
		}
		operation.completion_state = .Canceled
	case .Try_Cancel:
		operation.command_state = .Cancel_Requested
	case .Wait_Cancellation_Completed, .Wait_Cancellation_Requested:
		operation.command_state = .Cancel_Requested
	}
	return true
}

cancel_child_workflow :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	reason := "Child Workflow cancellation requested",
) -> bool {
	if ctx == nil || ctx.instance == nil {
		return false
	}
	input := Outbound_Cancel_Child_Workflow_Input{
		ctx = ctx,
		future = future,
		reason = reason,
	}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	return outbound_cancel_child_workflow_chain(&state, &input)
}

workflow_signal_failure_cause_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_SignalExternalWorkflowExecutionFailedCause,
) -> Workflow_Signal_Failure_Cause {
	switch value {
	case .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND:
		return .Workflow_Not_Found
	case .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND:
		return .Namespace_Not_Found
	case .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_SIGNAL_COUNT_LIMIT_EXCEEDED:
		return .Signal_Count_Limit_Exceeded
	case .SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED:
		return .Unspecified
	}
	return .Unspecified
}

workflow_signal_result_from_operation :: proc(
	operation: ^Fiber_External_Signal_Operation,
) -> Child_Workflow_Signal_Result {
	switch operation.state {
	case .Resolved:
		return {status = .Succeeded}
	case .Failed:
		return {
			status = .Failed,
			failure_message = operation.failure_message,
			cause = operation.cause,
			failure = operation.failure,
		}
	case .Canceled:
		return {
			status = .Canceled,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	case .Cancel_Requested:
		return {status = .Canceled, failure_message = operation.failure_message}
	case .Scheduled, .Commanded:
	}
	return {status = .Failed, failure_message = "Workflow signal has not resolved"}
}

schedule_external_signal_and_await :: proc(
	ctx: ^Fiber_Context,
	operation: Fiber_External_Signal_Operation,
	cancellation_token_id: u32,
) -> Workflow_Signal_Result {
	instance := ctx.instance
	owned_operation := operation
	instance.next_external_signal_seq += 1
	owned_operation.seq = instance.next_external_signal_seq
	owned_operation.state = .Scheduled
	owned_operation.detached_from_root_cancellation = !cancellation_token_linked_to_root(
		instance,
		cancellation_token_id,
	)
	owned_operation.cancellation_token_id = cancellation_token_id
	append(&instance.external_signals, owned_operation)
	instance.waiting_external_signal_seq = owned_operation.seq
	instance.phase = .Waiting_External_Signal
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		mark_child_workflow_failure(
			ctx,
			"could not resume external Workflow signal await",
			"OdinWorkflowSignalError",
		)
		return {status = .Failed, failure_message = "could not resume external Workflow signal await"}
	}
	operation_ptr, operation_found := find_external_signal_operation(instance, owned_operation.seq)
	if !operation_found {
		mark_child_workflow_failure(
			ctx,
			"external Workflow signal operation was not found",
			"OdinWorkflowSignalError",
		)
		return {status = .Failed, failure_message = "external Workflow signal operation was not found"}
	}
	return workflow_signal_result_from_operation(operation_ptr)
}

signal_child_workflow_payloads_owned :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	inputs: [dynamic]codec.Payload,
	options: Signal_Options,
) -> Child_Workflow_Signal_Result {
	owned_inputs := inputs
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   future.seq == 0 || len(signal_name) == 0 ||
	   options.headers != nil && options.headers.allocator.procedure == nil {
		activity_inputs_free(&owned_inputs)
		return {status = .Failed, failure_message = "invalid Child Workflow signal"}
	}
	instance := ctx.instance
	token_id, token_valid := selected_cancellation_token_id(
		instance,
		options.cancellation_token,
	)
	if !token_valid {
		activity_inputs_free(&owned_inputs)
		return {
			status = .Failed,
			failure_message = "Child Workflow signal cancellation token does not belong to this Workflow run",
		}
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		activity_inputs_free(&owned_inputs)
		return {
			status = .Canceled,
			failure_message = cancellation_token_reason_id(instance, token_id),
		}
	}
	start_result := await_child_workflow_start(ctx, future)
	if start_result.status != .Succeeded {
		activity_inputs_free(&owned_inputs)
		status := Child_Workflow_Signal_Status.Failed
		if start_result.status == .Canceled {
			status = .Canceled
		}
		return {status = status, failure_message = start_result.failure_message}
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		activity_inputs_free(&owned_inputs)
		return {
			status = .Canceled,
			failure_message = cancellation_token_reason_id(instance, token_id),
		}
	}
	child, found := find_child_workflow_operation(instance, future.seq)
	if !found || child.completion_state != .Pending {
		activity_inputs_free(&owned_inputs)
		return {status = .Failed, failure_message = "Child Workflow is not running"}
	}
	owned_name, allocation_error := strings.clone(signal_name, instance.odin_context.allocator)
	owned_headers: [dynamic]Fiber_Named_Payload
	headers_ok := clone_workflow_headers(
		options.headers,
		&owned_headers,
		instance.odin_context.allocator,
	)
	if allocation_error != nil || !headers_ok {
		activity_inputs_free(&owned_inputs)
		return {status = .Failed, failure_message = "could not preserve Child Workflow signal name or headers"}
	}
	owned_summary := codec.Payload{}
	has_summary := len(options.summary) > 0
	if has_summary {
		encode_error: codec.Error
		owned_summary, encode_error = codec.encode_json(
			options.summary,
			instance.odin_context.allocator,
		)
		if encode_error != .None {
			activity_inputs_free(&owned_inputs)
			return {status = .Failed, failure_message = "could not preserve Child Workflow signal summary"}
		}
	}
	operation := Fiber_External_Signal_Operation{
		child_workflow_seq = future.seq,
		child_workflow_id = child.workflow_id,
		signal_name = owned_name,
		inputs = owned_inputs,
		headers = owned_headers,
		summary = owned_summary,
		has_summary = has_summary,
	}
	return schedule_external_signal_and_await(ctx, operation, token_id)
}

signal_child_workflow_owned :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	input: codec.Payload,
	options: Signal_Options,
) -> Child_Workflow_Signal_Result {
	inputs := make([dynamic]codec.Payload)
	append(&inputs, input)
	return signal_child_workflow_payloads_owned(ctx, future, signal_name, inputs, options)
}

signal_child_workflow_payloads :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	inputs: []codec.Payload_View,
) -> Child_Workflow_Signal_Result {
	return signal_child_workflow_payloads_with_options(ctx, future, signal_name, inputs, {})
}

signal_child_workflow_payloads_with_options :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	inputs: []codec.Payload_View,
	options: Signal_Options,
) -> Child_Workflow_Signal_Result {
	if ctx == nil || ctx.instance == nil {
		return {status = .Failed, failure_message = "invalid Child Workflow signal context"}
	}
	headers := Headers{}
	if !outbound_headers_init(&headers, options.headers, ctx.instance) {
		return {status = .Failed, failure_message = "could not initialize Child Workflow Signal interceptor headers"}
	}
	defer headers_free(&headers)
	intercepted_options := options
	intercepted_options.headers = &headers
	interceptor_input := Outbound_Signal_Child_Workflow_Input{
		ctx = ctx,
		future = future,
		signal_name = signal_name,
		inputs = inputs,
		options = intercepted_options,
		headers = &headers,
	}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	return outbound_signal_child_workflow_chain(&state, &interceptor_input)
}

signal_child_workflow_payload :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	input: codec.Payload_View,
) -> Child_Workflow_Signal_Result {
	inputs := [?]codec.Payload_View{input}
	return signal_child_workflow_payloads(ctx, future, signal_name, inputs[:])
}

signal_child_workflow_payload_with_options :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	input: codec.Payload_View,
	options: Signal_Options,
) -> Child_Workflow_Signal_Result {
	inputs := [?]codec.Payload_View{input}
	return signal_child_workflow_payloads_with_options(ctx, future, signal_name, inputs[:], options)
}

signal_child_workflow_json :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	input: $T,
) -> Child_Workflow_Signal_Result {
	return signal_child_workflow_json_with_options(ctx, future, signal_name, input, {})
}

signal_child_workflow_json_with_options :: proc(
	ctx: ^Fiber_Context,
	future: Child_Workflow_Future,
	signal_name: string,
	input: $T,
	options: Signal_Options,
) -> Child_Workflow_Signal_Result {
	if ctx == nil || ctx.instance == nil {
		return {status = .Failed, failure_message = "invalid Child Workflow signal context"}
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		return {status = .Failed, failure_message = "could not encode Child Workflow signal as json/plain"}
	}
	defer codec.payload_free(&payload)
	return signal_child_workflow_payload_with_options(
		ctx,
		future,
		signal_name,
		codec.payload_view(&payload),
		options,
	)
}

signal_external_workflow_payloads_owned :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	signal_name: string,
	inputs: [dynamic]codec.Payload,
	options: Signal_Options,
) -> External_Workflow_Signal_Result {
	owned_inputs := inputs
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(execution.namespace) == 0 || len(execution.workflow_id) == 0 ||
	   len(signal_name) == 0 ||
	   options.headers != nil && options.headers.allocator.procedure == nil {
		activity_inputs_free(&owned_inputs)
		return {status = .Failed, failure_message = "invalid external Workflow signal"}
	}
	instance := ctx.instance
	token_id, token_valid := selected_cancellation_token_id(
		instance,
		options.cancellation_token,
	)
	if !token_valid {
		activity_inputs_free(&owned_inputs)
		return {
			status = .Failed,
			failure_message = "external Workflow signal cancellation token does not belong to this Workflow run",
		}
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		activity_inputs_free(&owned_inputs)
		return {
			status = .Canceled,
			failure_message = cancellation_token_reason_id(instance, token_id),
		}
	}
	owned_namespace, namespace_error := strings.clone(
		execution.namespace,
		instance.odin_context.allocator,
	)
	owned_workflow_id, workflow_id_error := strings.clone(
		execution.workflow_id,
		instance.odin_context.allocator,
	)
	owned_run_id, run_id_error := strings.clone(execution.run_id, instance.odin_context.allocator)
	owned_name, name_error := strings.clone(signal_name, instance.odin_context.allocator)
	owned_headers: [dynamic]Fiber_Named_Payload
	headers_ok := clone_workflow_headers(
		options.headers,
		&owned_headers,
		instance.odin_context.allocator,
	)
	if namespace_error != nil || workflow_id_error != nil || run_id_error != nil ||
	   name_error != nil || !headers_ok {
		activity_inputs_free(&owned_inputs)
		return {status = .Failed, failure_message = "could not preserve external Workflow signal"}
	}
	owned_summary := codec.Payload{}
	has_summary := len(options.summary) > 0
	if has_summary {
		encode_error: codec.Error
		owned_summary, encode_error = codec.encode_json(
			options.summary,
			instance.odin_context.allocator,
		)
		if encode_error != .None {
			activity_inputs_free(&owned_inputs)
			return {status = .Failed, failure_message = "could not preserve external Workflow signal summary"}
		}
	}
	operation := Fiber_External_Signal_Operation{
		namespace = owned_namespace,
		child_workflow_id = owned_workflow_id,
		run_id = owned_run_id,
		signal_name = owned_name,
		inputs = owned_inputs,
		headers = owned_headers,
		summary = owned_summary,
		has_summary = has_summary,
	}
	return schedule_external_signal_and_await(ctx, operation, token_id)
}

signal_external_workflow_owned :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	signal_name: string,
	input: codec.Payload,
	options: Signal_Options,
) -> External_Workflow_Signal_Result {
	inputs := make([dynamic]codec.Payload)
	append(&inputs, input)
	return signal_external_workflow_payloads_owned(ctx, execution, signal_name, inputs, options)
}

signal_external_workflow_payloads :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	signal_name: string,
	inputs: []codec.Payload_View,
) -> External_Workflow_Signal_Result {
	return signal_external_workflow_payloads_with_options(ctx, execution, signal_name, inputs, {})
}

signal_external_workflow_payloads_with_options :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	signal_name: string,
	inputs: []codec.Payload_View,
	options: Signal_Options,
) -> External_Workflow_Signal_Result {
	if ctx == nil || ctx.instance == nil {
		return {status = .Failed, failure_message = "invalid external Workflow signal context"}
	}
	headers := Headers{}
	if !outbound_headers_init(&headers, options.headers, ctx.instance) {
		return {status = .Failed, failure_message = "could not initialize external Workflow Signal interceptor headers"}
	}
	defer headers_free(&headers)
	intercepted_options := options
	intercepted_options.headers = &headers
	interceptor_input := Outbound_Signal_External_Workflow_Input{
		ctx = ctx,
		execution = execution,
		signal_name = signal_name,
		inputs = inputs,
		options = intercepted_options,
		headers = &headers,
	}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	return outbound_signal_external_workflow_chain(&state, &interceptor_input)
}

signal_external_workflow_payload :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	signal_name: string,
	input: codec.Payload_View,
) -> External_Workflow_Signal_Result {
	inputs := [?]codec.Payload_View{input}
	return signal_external_workflow_payloads(ctx, execution, signal_name, inputs[:])
}

signal_external_workflow_payload_with_options :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	signal_name: string,
	input: codec.Payload_View,
	options: Signal_Options,
) -> External_Workflow_Signal_Result {
	inputs := [?]codec.Payload_View{input}
	return signal_external_workflow_payloads_with_options(ctx, execution, signal_name, inputs[:], options)
}

signal_external_workflow_json :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	signal_name: string,
	input: $T,
) -> External_Workflow_Signal_Result {
	return signal_external_workflow_json_with_options(ctx, execution, signal_name, input, {})
}

signal_external_workflow_json_with_options :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	signal_name: string,
	input: $T,
	options: Signal_Options,
) -> External_Workflow_Signal_Result {
	if ctx == nil || ctx.instance == nil {
		return {status = .Failed, failure_message = "invalid external Workflow signal context"}
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		return {status = .Failed, failure_message = "could not encode external Workflow signal as json/plain"}
	}
	defer codec.payload_free(&payload)
	return signal_external_workflow_payload_with_options(
		ctx,
		execution,
		signal_name,
		codec.payload_view(&payload),
		options,
	)
}

external_workflow_cancel_failure_cause_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_CancelExternalWorkflowExecutionFailedCause,
) -> External_Workflow_Cancel_Failure_Cause {
	switch value {
	case .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND:
		return .Workflow_Not_Found
	case .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND:
		return .Namespace_Not_Found
	case .CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_UNSPECIFIED:
		return .Unspecified
	}
	return .Unspecified
}

request_cancel_external_workflow_terminal :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	reason := "external Workflow cancellation requested",
) -> External_Workflow_Cancel_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(execution.namespace) == 0 || len(execution.workflow_id) == 0 {
		return {status = .Failed, failure_message = "invalid external Workflow cancellation request"}
	}
	instance := ctx.instance
	owned_namespace, namespace_error := strings.clone(
		execution.namespace,
		instance.odin_context.allocator,
	)
	owned_workflow_id, workflow_id_error := strings.clone(
		execution.workflow_id,
		instance.odin_context.allocator,
	)
	owned_run_id, run_id_error := strings.clone(execution.run_id, instance.odin_context.allocator)
	owned_reason, reason_error := strings.clone(reason, instance.odin_context.allocator)
	if namespace_error != nil || workflow_id_error != nil || run_id_error != nil ||
	   reason_error != nil {
		return {status = .Failed, failure_message = "could not preserve external Workflow cancellation request"}
	}
	instance.next_external_cancel_seq += 1
	operation := Fiber_External_Cancel_Operation{
		seq = instance.next_external_cancel_seq,
		namespace = owned_namespace,
		workflow_id = owned_workflow_id,
		run_id = owned_run_id,
		reason = owned_reason,
		state = .Scheduled,
	}
	append(&instance.external_cancels, operation)
	instance.waiting_external_cancel_seq = operation.seq
	instance.phase = .Waiting_External_Cancel
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		mark_child_workflow_failure(
			ctx,
			"could not resume external Workflow cancellation await",
			"OdinExternalWorkflowCancellationError",
		)
		return {
			status = .Failed,
			failure_message = "could not resume external Workflow cancellation await",
		}
	}
	resolved, found := find_external_cancel_operation(instance, operation.seq)
	if !found {
		mark_child_workflow_failure(
			ctx,
			"external Workflow cancellation operation was not found",
			"OdinExternalWorkflowCancellationError",
		)
		return {
			status = .Failed,
			failure_message = "external Workflow cancellation operation was not found",
		}
	}
	if resolved.state == .Resolved {
		return {status = .Succeeded}
	}
	return {
		status = .Failed,
		failure_message = resolved.failure_message,
		cause = resolved.cause,
		failure = resolved.failure,
	}
}

request_cancel_external_workflow :: proc(
	ctx: ^Fiber_Context,
	execution: External_Workflow_Execution,
	reason := "external Workflow cancellation requested",
) -> External_Workflow_Cancel_Result {
	if ctx == nil || ctx.instance == nil {
		return {status = .Failed, failure_message = "invalid external Workflow cancellation context"}
	}
	input := Outbound_Cancel_External_Workflow_Input{
		ctx = ctx,
		execution = execution,
		reason = reason,
	}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	return outbound_cancel_external_workflow_chain(&state, &input)
}

continue_as_new_options_error :: proc(options: Continue_As_New_Options) -> string {
	if options.workflow_run_timeout < 0 || options.workflow_task_timeout < 0 ||
	   options.backoff_start_interval < 0 {
		return "Continue-As-New timeouts and backoff must not be negative"
	}
	if options.workflow_run_timeout > 0 &&
	   options.workflow_task_timeout > options.workflow_run_timeout {
		return "Continue-As-New Workflow Task timeout must not exceed Run timeout"
	}
	if options.retry_policy != nil {
		policy := options.retry_policy
		if policy.initial_interval < 0 || policy.maximum_interval < 0 ||
		   policy.maximum_attempts < 0 {
			return "Continue-As-New Retry Policy values must not be negative"
		}
		if math.is_nan(policy.backoff_coefficient) || math.is_inf(policy.backoff_coefficient) ||
		   (policy.backoff_coefficient != 0 && policy.backoff_coefficient < 1) {
			return "Continue-As-New Retry Policy backoff coefficient must be zero or at least one"
		}
		if policy.maximum_interval > 0 && policy.initial_interval > 0 &&
		   policy.maximum_interval < policy.initial_interval {
			return "Continue-As-New Retry Policy maximum interval must not be shorter than its initial interval"
		}
	}
	if options.headers != nil && options.headers.allocator.procedure == nil {
		return "Continue-As-New headers are not initialized"
	}
	if options.memo != nil && options.memo.allocator.procedure == nil {
		return "Continue-As-New memo is not initialized"
	}
	if options.search_attributes != nil &&
	   options.search_attributes.allocator.procedure == nil {
		return "Continue-As-New Search Attributes are not initialized"
	}
	if int(options.versioning_intent) < int(Continue_As_New_Versioning_Intent.Unspecified) ||
	   int(options.versioning_intent) > int(Continue_As_New_Versioning_Intent.Default) {
		return "Continue-As-New versioning intent is invalid"
	}
	if int(options.initial_versioning_behavior) <
	     int(Continue_As_New_Versioning_Behavior.Unspecified) ||
	   int(options.initial_versioning_behavior) >
	     int(Continue_As_New_Versioning_Behavior.Use_Ramping_Version) {
		return "Continue-As-New initial versioning behavior is invalid"
	}
	return ""
}

clone_continue_as_new_options :: proc(
	instance: ^Fiber_Instance,
	options: Continue_As_New_Options,
) -> (owned: Owned_Continue_As_New_Options, ok: bool) {
	allocator := instance.odin_context.allocator
	workflow_type := options.workflow_type
	if len(workflow_type) == 0 {
		workflow_type = instance.workflow_type
	}
	task_queue := options.task_queue
	if len(task_queue) == 0 {
		task_queue = instance.task_queue
	}
	owned.workflow_type, _ = strings.clone(workflow_type, allocator)
	owned.task_queue, _ = strings.clone(task_queue, allocator)
	if len(owned.workflow_type) == 0 || len(owned.task_queue) == 0 {
		return {}, false
	}
	owned.workflow_run_timeout = options.workflow_run_timeout
	owned.workflow_task_timeout = options.workflow_task_timeout
	owned.backoff_start_interval = options.backoff_start_interval
	owned.versioning_intent = options.versioning_intent
	owned.initial_versioning_behavior = options.initial_versioning_behavior
	if !clone_workflow_payload_map(options.memo, &owned.memo, allocator) {
		return {}, false
	}
	if !clone_workflow_headers(options.headers, &owned.headers, allocator) {
		return {}, false
	}
	owned.has_search_attributes = options.search_attributes != nil
	if !clone_workflow_payload_map(
		options.search_attributes,
		&owned.search_attributes,
		allocator,
	) {
		return {}, false
	}
	if options.retry_policy != nil {
		owned.has_retry_policy = true
		owned.retry_policy = options.retry_policy^
		owned.retry_policy.non_retryable_error_types = make(
			[]string,
			len(options.retry_policy.non_retryable_error_types),
			allocator,
		)
		for error_type, index in options.retry_policy.non_retryable_error_types {
			cloned, clone_error := strings.clone(error_type, allocator)
			if clone_error != nil {
				return {}, false
			}
			owned.retry_policy.non_retryable_error_types[index] = cloned
		}
	}
	return owned, true
}

continue_as_new_stop_with_failure :: proc(
	ctx: ^Fiber_Context,
	message, failure_type: string,
) -> ! {
	if ctx == nil || ctx.instance == nil {
		panic("Continue-As-New requires a Workflow context")
	}
	instance := ctx.instance
	instance.terminal_failure_requested = true
	instance.terminal_failure_message = message
	instance.terminal_failure_type = failure_type
	instance.failure_message = message
	instance.failure_type = failure_type
	instance.phase = .Failed
	if fiber_raw.yield(instance.fiber) != .Success {
		panic("could not yield failed Continue-As-New Workflow")
	}
	panic("failed Continue-As-New Workflow was resumed")
}

continue_as_new_payloads_owned :: proc(
	ctx: ^Fiber_Context,
	inputs: [dynamic]codec.Payload,
	options := Continue_As_New_Options{},
) -> ! {
	owned_inputs := inputs
	if ctx == nil || ctx.instance == nil || ctx != &ctx.instance.workflow_context ||
	   ctx.instance.phase != .Ready {
		activity_inputs_free(&owned_inputs)
		continue_as_new_stop_with_failure(
			ctx,
			"Continue-As-New requires the ready main Workflow context",
			"OdinContinueAsNewContextError",
		)
	}
	options_error := continue_as_new_options_error(options)
	if len(options_error) > 0 {
		activity_inputs_free(&owned_inputs)
		continue_as_new_stop_with_failure(ctx, options_error, "OdinContinueAsNewOptionsError")
	}
	instance := ctx.instance
	owned_options, options_cloned := clone_continue_as_new_options(instance, options)
	if !options_cloned {
		activity_inputs_free(&owned_inputs)
		continue_as_new_stop_with_failure(
			ctx,
			"could not preserve Continue-As-New options",
			"OdinContinueAsNewOptionsError",
		)
	}
	instance.continue_as_new = {
		inputs = owned_inputs,
		options = owned_options,
		present = true,
	}
	instance.phase = .Continued_As_New
	if fiber_raw.yield(instance.fiber) != .Success {
		panic("could not yield continued Workflow")
	}
	panic("continued Workflow was resumed")
}

continue_as_new_owned :: proc(
	ctx: ^Fiber_Context,
	input: codec.Payload,
	options := Continue_As_New_Options{},
) -> ! {
	inputs := make([dynamic]codec.Payload)
	append(&inputs, input)
	continue_as_new_payloads_owned(ctx, inputs, options)
}

continue_as_new_payloads :: proc(
	ctx: ^Fiber_Context,
	inputs: []codec.Payload_View,
	options := Continue_As_New_Options{},
) -> ! {
	if ctx == nil || ctx.instance == nil {
		continue_as_new_stop_with_failure(
			ctx,
			"Continue-As-New requires a Workflow context",
			"OdinContinueAsNewContextError",
		)
	}
	headers := Headers{}
	if !outbound_headers_init(&headers, options.headers, ctx.instance) {
		continue_as_new_stop_with_failure(
			ctx,
			"could not initialize Continue-As-New interceptor headers",
			"OdinWorkflowInterceptorError",
		)
	}
	intercepted_options := options
	intercepted_options.headers = &headers
	interceptor_input := Outbound_Continue_As_New_Input{
		ctx = ctx,
		inputs = inputs,
		options = intercepted_options,
		headers = &headers,
	}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	outbound_continue_as_new_chain(&state, &interceptor_input)
}

continue_as_new_payload :: proc(
	ctx: ^Fiber_Context,
	input: codec.Payload_View,
	options := Continue_As_New_Options{},
) -> ! {
	inputs := [?]codec.Payload_View{input}
	continue_as_new_payloads(ctx, inputs[:], options)
}

continue_as_new_json :: proc(
	ctx: ^Fiber_Context,
	input: $T,
	options := Continue_As_New_Options{},
) -> ! {
	if ctx == nil || ctx.instance == nil {
		continue_as_new_stop_with_failure(
			ctx,
			"Continue-As-New requires a Workflow context",
			"OdinContinueAsNewContextError",
		)
	}
	payload, encode_error := codec.encode_json(input, ctx.instance.odin_context.allocator)
	if encode_error != .None {
		continue_as_new_stop_with_failure(
			ctx,
			"could not encode Continue-As-New input as json/plain",
			"OdinContinueAsNewPayloadError",
		)
	}
	continue_as_new_payload(ctx, codec.payload_view(&payload), options)
}

activity_retry_state_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_RetryState,
) -> Activity_Retry_State {
	switch value {
	case .RETRY_STATE_IN_PROGRESS:
		return .In_Progress
	case .RETRY_STATE_NON_RETRYABLE_FAILURE:
		return .Non_Retryable_Failure
	case .RETRY_STATE_TIMEOUT:
		return .Timed_Out
	case .RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED:
		return .Maximum_Attempts_Reached
	case .RETRY_STATE_RETRY_POLICY_NOT_SET:
		return .Retry_Policy_Not_Set
	case .RETRY_STATE_INTERNAL_SERVER_ERROR:
		return .Internal_Server_Error
	case .RETRY_STATE_CANCEL_REQUESTED:
		return .Cancel_Requested
	case .RETRY_STATE_UNSPECIFIED:
		return .Unspecified
	}
	return .Unspecified
}

application_error_category_from_failure :: proc(
	value: temporal_failure.Application_Error_Category,
) -> Application_Error_Category {
	if value == .Benign {
		return .Benign
	}
	return .Unspecified
}

positive_duration_from_proto :: proc(
	value: ^proto.Google_Protobuf_Duration,
) -> (duration: time.Duration, ok: bool) {
	if value == nil || value.seconds < 0 || value.nanos < 0 || value.nanos >= 1_000_000_000 {
		return 0, false
	}
	// time.Duration is a signed nanosecond count. Reject malformed Core input
	// instead of allowing the multiplication to wrap.
	if value.seconds > 9_223_372_036 ||
	   (value.seconds == 9_223_372_036 && value.nanos > 854_775_807) {
		return 0, false
	}
	duration = time.Duration(value.seconds * i64(time.Second) + i64(value.nanos))
	return duration, duration > 0
}

activity_retry_state_from_failure :: proc(
	value: temporal_failure.Retry_State,
) -> Activity_Retry_State {
	switch value {
	case .In_Progress:              return .In_Progress
	case .Non_Retryable_Failure:    return .Non_Retryable_Failure
	case .Timeout:                  return .Timed_Out
	case .Maximum_Attempts_Reached: return .Maximum_Attempts_Reached
	case .Retry_Policy_Not_Set:     return .Retry_Policy_Not_Set
	case .Internal_Server_Error:    return .Internal_Server_Error
	case .Cancel_Requested:         return .Cancel_Requested
	case .Unspecified:              return .Unspecified
	}
	return .Unspecified
}

failure_timeout_type_from_failure :: proc(
	value: temporal_failure.Timeout_Type,
) -> Failure_Timeout_Type {
	switch value {
	case .Start_To_Close:    return .Start_To_Close
	case .Schedule_To_Start: return .Schedule_To_Start
	case .Schedule_To_Close: return .Schedule_To_Close
	case .Heartbeat:         return .Heartbeat
	case .Unspecified:       return .Unspecified
	}
	return .Unspecified
}

preserve_temporal_failure :: proc(data: []u8) -> (^temporal_failure.Failure, bool) {
	decoded, decode_error := temporal_failure.decode(data)
	if decode_error != .None {
		return nil, false
	}
	owned, allocation_error := new(temporal_failure.Failure)
	if allocation_error != nil {
		temporal_failure.failure_free(&decoded)
		return nil, false
	}
	owned^ = decoded
	return owned, true
}

failure_payload_views :: proc(
	values: []codec.Payload,
) -> []codec.Payload_View {
	result := make([]codec.Payload_View, len(values))
	for &value, index in values {
		result[index] = codec.payload_view(&value)
	}
	return result
}

preserve_activity_failure :: proc(
	operation: ^Fiber_Activity_Operation,
	data: []u8,
) -> bool {
	if operation == nil || len(data) == 0 {
		return false
	}
	tree, preserved := preserve_temporal_failure(data)
	if !preserved {
		return false
	}
	operation.failure.tree = tree
	operation.failure.message = tree.message
	operation.failure.source = tree.source
	operation.failure.stack_trace = tree.stack_trace
	activity := temporal_failure.find_kind(tree, .Activity)
	if activity != nil {
		operation.failure.activity_id = activity.activity.activity_id
		operation.failure.activity_type = activity.activity.activity_type
		operation.failure.identity = activity.activity.identity
		operation.failure.retry_state = activity_retry_state_from_failure(activity.activity.retry_state)
	}
	application_failure := temporal_failure.find_kind(tree, .Application)
	if application_failure != nil {
		info := &application_failure.application
		operation.failure_detail_views = failure_payload_views(info.details[:])
		operation.failure.application = {
			present = true,
			message = application_failure.message,
			type = info.type,
			non_retryable = info.non_retryable,
			category = application_error_category_from_failure(info.category),
			details = operation.failure_detail_views,
			next_retry_delay = info.next_retry_delay,
			has_next_retry_delay = info.has_next_retry_delay,
		}
	}
	canceled := temporal_failure.find_kind(tree, .Canceled)
	if canceled != nil {
		operation.failure.canceled = true
		operation.cancellation_detail_views = failure_payload_views(canceled.canceled.details[:])
		operation.failure.cancellation_details = operation.cancellation_detail_views
	}
	return true
}

activity_failure_detail_payload :: proc(
	failure: ^Activity_Failure,
	index: int,
) -> (payload: codec.Payload_View, found: bool) {
	if failure == nil || !failure.application.present || index < 0 ||
	   index >= len(failure.application.details) {
		return
	}
	return failure.application.details[index], true
}

// activity_failure_tree returns the complete recursive Temporal Failure. The
// pointer borrows the Workflow instance arena and is valid only for the current
// Workflow execution; callers must not pass it to failure_free.
activity_failure_tree :: proc(failure: ^Activity_Failure) -> ^temporal_failure.Failure {
	if failure == nil {
		return nil
	}
	return failure.tree
}

activity_failure_detail_json :: proc(
	failure: ^Activity_Failure,
	index: int,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := activity_failure_detail_payload(failure, index)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

activity_cancellation_detail_payload :: proc(
	failure: ^Activity_Failure,
	index: int,
) -> (payload: codec.Payload_View, found: bool) {
	if failure == nil || !failure.canceled || index < 0 ||
	   index >= len(failure.cancellation_details) {
		return
	}
	return failure.cancellation_details[index], true
}

activity_cancellation_detail_json :: proc(
	failure: ^Activity_Failure,
	index: int,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := activity_cancellation_detail_payload(failure, index)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

preserve_child_workflow_failure :: proc(
	operation: ^Fiber_Child_Workflow_Operation,
	data: []u8,
) -> bool {
	if operation == nil || len(data) == 0 {
		return false
	}
	tree, preserved := preserve_temporal_failure(data)
	if !preserved {
		return false
	}
	operation.failure.tree = tree
	operation.failure.message = tree.message
	operation.failure.source = tree.source
	operation.failure.stack_trace = tree.stack_trace
	child := temporal_failure.find_kind(tree, .Child_Workflow)
	if child != nil {
		operation.failure.namespace = child.child_workflow.namespace
		operation.failure.workflow_id = child.child_workflow.workflow_id
		operation.failure.run_id = child.child_workflow.run_id
		operation.failure.workflow_type = child.child_workflow.workflow_type
		operation.failure.initiated_event_id = child.child_workflow.initiated_event_id
		operation.failure.started_event_id = child.child_workflow.started_event_id
		operation.failure.retry_state = activity_retry_state_from_failure(child.child_workflow.retry_state)
	}
	application_failure := temporal_failure.find_kind(tree, .Application)
	if application_failure != nil {
		info := &application_failure.application
		operation.failure_detail_views = failure_payload_views(info.details[:])
		operation.failure.application = {
			present = true,
			message = application_failure.message,
			type = info.type,
			non_retryable = info.non_retryable,
			category = application_error_category_from_failure(info.category),
			details = operation.failure_detail_views,
			next_retry_delay = info.next_retry_delay,
			has_next_retry_delay = info.has_next_retry_delay,
		}
	}
	canceled := temporal_failure.find_kind(tree, .Canceled)
	if canceled != nil {
		operation.failure.canceled = true
		operation.cancellation_detail_views = failure_payload_views(canceled.canceled.details[:])
		operation.failure.cancellation_details = operation.cancellation_detail_views
	}
	timeout := temporal_failure.find_kind(tree, .Timeout)
	if timeout != nil {
		operation.failure.timed_out = true
		operation.failure.timeout_type = failure_timeout_type_from_failure(timeout.timeout.timeout_type)
	}
	operation.failure.terminated = temporal_failure.find_kind(tree, .Terminated) != nil
	return true
}

child_workflow_failure_detail_payload :: proc(
	failure: ^Child_Workflow_Failure,
	index: int,
) -> (payload: codec.Payload_View, found: bool) {
	if failure == nil || !failure.application.present || index < 0 ||
	   index >= len(failure.application.details) {
		return
	}
	return failure.application.details[index], true
}

// child_workflow_failure_tree returns the complete recursive Temporal Failure.
// It has the same borrowed Workflow-instance lifetime as the flattened view.
child_workflow_failure_tree :: proc(
	failure: ^Child_Workflow_Failure,
) -> ^temporal_failure.Failure {
	if failure == nil {
		return nil
	}
	return failure.tree
}

child_workflow_failure_detail_json :: proc(
	failure: ^Child_Workflow_Failure,
	index: int,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	payload, payload_found := child_workflow_failure_detail_payload(failure, index)
	if !payload_found {
		return false, .None
	}
	return true, codec.decode_json(payload, value)
}

child_workflow_cancellation_detail_json :: proc(
	failure: ^Child_Workflow_Failure,
	index: int,
	value: ^$T,
) -> (found: bool, error: codec.Error) {
	if failure == nil || !failure.canceled || index < 0 ||
	   index >= len(failure.cancellation_details) {
		return false, .None
	}
	return true, codec.decode_json(failure.cancellation_details[index], value)
}

activity_waits_for_cancellation_completion :: proc(operation: ^Fiber_Activity_Operation) -> bool {
	if operation == nil {
		return false
	}
	if operation.is_local {
		return operation.local_options.cancellation_type == .Wait_Cancellation_Completed
	}
	return operation.options.cancellation_type == .Wait_Cancellation_Completed
}

activity_operation_ready_for_waiter :: proc(
	instance: ^Fiber_Instance,
	operation: ^Fiber_Activity_Operation,
	waiter_token_id: u32,
) -> bool {
	if instance == nil || operation == nil {
		return false
	}
	switch operation.state {
	case .Resolved, .Failed, .Canceled:
		return true
	case .Cancel_Requested:
		// A language-side token cancellation must honor
		// Wait_Cancellation_Completed. Server root cancellation may resume the
		// linked waiter immediately so Workflow code can acknowledge shutdown.
		return !activity_waits_for_cancellation_completion(operation) ||
		       (instance.cancellation_requested &&
		        cancellation_token_linked_to_root(instance, waiter_token_id) &&
		        !operation.detached_from_root_cancellation)
	case .Scheduled, .Commanded:
	}
	return false
}

payload_activity_result_from_operation :: proc(operation: ^Fiber_Activity_Operation) -> Payload_Activity_Result {
	if operation.state == .Resolved {
		return {status = .Succeeded, value = codec.payload_view(&operation.result)}
	}
	if operation.state == .Failed {
		return {
			status = .Failed,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	}
	if operation.state == .Cancel_Requested || operation.state == .Canceled {
		return {
			status = .Canceled,
			failure_message = operation.failure_message,
			failure = operation.failure,
		}
	}
	return {status = .Failed, failure_message = "Activity has not resolved"}
}

// await_activity_payload_result suspends the whole Odin call stack only while the
// selected operation is unresolved. Success and failure applied earlier in the
// same activation are consumed synchronously.
await_activity_payload_result :: proc(
	ctx: ^Fiber_Context,
	future: Activity_Future,
) -> Payload_Activity_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {status = .Failed, failure_message = "invalid Activity await"}
	}
	instance := ctx.instance
	operation, found := find_activity_operation(instance, future.seq)
	if !found {
		instance.phase = .Failed
		return {status = .Failed, failure_message = "Activity operation was not found"}
	}
	cancellation_is_terminal := operation.state == .Cancel_Requested &&
		(!activity_waits_for_cancellation_completion(operation) || root_cancellation_applies(instance))
	if operation.state == .Resolved || operation.state == .Failed ||
	   cancellation_is_terminal || operation.state == .Canceled {
		return payload_activity_result_from_operation(operation)
	}
	instance.waiting_activity_seq = future.seq
	instance.phase = .Waiting_Activity
	yield_result := fiber_raw.yield(instance.fiber)
	if yield_result != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return {status = .Failed, failure_message = "could not resume Activity await"}
	}
	operation, found = find_activity_operation(instance, future.seq)
	cancellation_is_terminal = found && operation.state == .Cancel_Requested &&
		(!activity_waits_for_cancellation_completion(operation) || root_cancellation_applies(instance))
	if !found || (operation.state != .Resolved && operation.state != .Failed &&
	   !cancellation_is_terminal && operation.state != .Canceled) {
		instance.phase = .Failed
		return {status = .Failed, failure_message = "Activity resumed without a resolution"}
	}
	return payload_activity_result_from_operation(operation)
}

await_activity_json_result :: proc(
	ctx: ^Fiber_Context,
	future: Activity_Future,
	$T: typeid,
) -> Json_Activity_Result(T) {
	payload_result := await_activity_payload_result(ctx, future)
	if payload_result.status != .Succeeded {
		return {
			status = payload_result.status,
			failure_message = payload_result.failure_message,
			failure = payload_result.failure,
		}
	}
	value: T
	if codec.decode_json(payload_result.value, &value) != .None {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
			ctx.instance.failure_message = "could not decode Activity result as json/plain"
			ctx.instance.failure_type = "OdinWorkflowPayloadConversionError"
		}
		return {status = .Failed, failure_message = "could not decode Activity result as json/plain"}
	}
	return {status = .Succeeded, value = value}
}

await_activity_result :: proc(ctx: ^Fiber_Context, future: Activity_Future) -> Activity_Result {
	result := await_activity_json_result(ctx, future, string)
	return {
		status = result.status,
		value = result.value,
		failure_message = result.failure_message,
		failure = result.failure,
	}
}

await_activity_payload :: proc(
	ctx: ^Fiber_Context,
	future: Activity_Future,
) -> codec.Payload_View {
	result := await_activity_payload_result(ctx, future)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return {}
	}
	if result.status == .Canceled {
		return {}
	}
	return result.value
}

mark_activity_failure_unhandled :: proc(ctx: ^Fiber_Context, future: Activity_Future) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	if future.seq != 0 {
		operation, found := find_activity_operation(ctx.instance, future.seq)
		if found && operation.state == .Failed {
			ctx.instance.unhandled_activity_seq = future.seq
		}
	}
	ctx.instance.phase = .Failed
}

// await_activity is the convenience form for Workflows that want an unhandled
// Activity failure to fail the Workflow Execution.
await_activity :: proc(ctx: ^Fiber_Context, future: Activity_Future) -> string {
	result := await_activity_result(ctx, future)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return ""
	}
	if result.status == .Canceled {
		return ""
	}
	return result.value
}

// execute_activity is the sequential convenience form.
execute_activity :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> string {
	return await_activity(ctx, start_activity(ctx, activity_type, input))
}

execute_activity_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
	options: Activity_Options,
) -> string {
	return await_activity(
		ctx,
		start_activity_with_options(ctx, activity_type, input, options),
	)
}

execute_activity_result :: proc(ctx: ^Fiber_Context, activity_type, input: string) -> Activity_Result {
	return await_activity_result(ctx, start_activity(ctx, activity_type, input))
}

execute_activity_result_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
	options: Activity_Options,
) -> Activity_Result {
	return await_activity_result(
		ctx,
		start_activity_with_options(ctx, activity_type, input, options),
	)
}

execute_activity_payload :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_activity_payload(ctx, activity_type, input),
	)
}

execute_activity_payloads :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: []codec.Payload_View,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_activity_payloads(ctx, activity_type, inputs),
	)
}

execute_activity_payload_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
	options: Activity_Options,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_activity_payload_with_options(ctx, activity_type, input, options),
	)
}

execute_activity_payloads_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: []codec.Payload_View,
	options: Activity_Options,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_activity_payloads_with_options(ctx, activity_type, inputs, options),
	)
}

execute_activity_json_result :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
) -> Json_Activity_Result(Output) {
	return await_activity_json_result(
		ctx,
		start_activity_json(ctx, activity_type, input),
		Output,
	)
}

execute_activity_json_result_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
	options: Activity_Options,
) -> Json_Activity_Result(Output) {
	return await_activity_json_result(
		ctx,
		start_activity_json_with_options(ctx, activity_type, input, options),
		Output,
	)
}

execute_activity_json :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
) -> Output {
	future := start_activity_json(ctx, activity_type, input)
	result := await_activity_json_result(ctx, future, Output)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return {}
	}
	if result.status == .Canceled {
		return {}
	}
	return result.value
}

execute_activity_json_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
	options: Activity_Options,
) -> Output {
	future := start_activity_json_with_options(ctx, activity_type, input, options)
	result := await_activity_json_result(ctx, future, Output)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return {}
	}
	if result.status == .Canceled {
		return {}
	}
	return result.value
}

execute_local_activity :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
) -> string {
	return await_activity(ctx, start_local_activity(ctx, activity_type, input))
}

execute_local_activity_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
	options: Local_Activity_Options,
) -> string {
	return await_activity(
		ctx,
		start_local_activity_with_options(ctx, activity_type, input, options),
	)
}

execute_local_activity_result :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
) -> Activity_Result {
	return await_activity_result(ctx, start_local_activity(ctx, activity_type, input))
}

execute_local_activity_result_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type, input: string,
	options: Local_Activity_Options,
) -> Activity_Result {
	return await_activity_result(
		ctx,
		start_local_activity_with_options(ctx, activity_type, input, options),
	)
}

execute_local_activity_payload :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_local_activity_payload(ctx, activity_type, input),
	)
}

execute_local_activity_payloads :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: []codec.Payload_View,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_local_activity_payloads(ctx, activity_type, inputs),
	)
}

execute_local_activity_payload_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: codec.Payload_View,
	options: Local_Activity_Options,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_local_activity_payload_with_options(ctx, activity_type, input, options),
	)
}

execute_local_activity_payloads_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	inputs: []codec.Payload_View,
	options: Local_Activity_Options,
) -> codec.Payload_View {
	return await_activity_payload(
		ctx,
		start_local_activity_payloads_with_options(ctx, activity_type, inputs, options),
	)
}

execute_local_activity_json_result :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
) -> Json_Activity_Result(Output) {
	return await_activity_json_result(
		ctx,
		start_local_activity_json(ctx, activity_type, input),
		Output,
	)
}

execute_local_activity_json_result_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
	options: Local_Activity_Options,
) -> Json_Activity_Result(Output) {
	return await_activity_json_result(
		ctx,
		start_local_activity_json_with_options(ctx, activity_type, input, options),
		Output,
	)
}

execute_local_activity_json :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
) -> Output {
	future := start_local_activity_json(ctx, activity_type, input)
	result := await_activity_json_result(ctx, future, Output)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return {}
	}
	if result.status == .Canceled {
		return {}
	}
	return result.value
}

execute_local_activity_json_with_options :: proc(
	ctx: ^Fiber_Context,
	activity_type: string,
	input: $Input,
	$Output: typeid,
	options: Local_Activity_Options,
) -> Output {
	future := start_local_activity_json_with_options(ctx, activity_type, input, options)
	result := await_activity_json_result(ctx, future, Output)
	if result.status == .Failed {
		mark_activity_failure_unhandled(ctx, future)
		return {}
	}
	if result.status == .Canceled {
		return {}
	}
	return result.value
}

signal_payload_conversion_failed :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.phase = .Failed
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowPayloadConversionError"
}

signal_argument_count_failed :: proc(instance: ^Fiber_Instance, message: string) {
	if instance == nil {
		return
	}
	instance.phase = .Failed
	instance.failure_message = message
	instance.failure_type = "OdinWorkflowSignalArgumentCountError"
}

fiber_signal_context :: proc(signal: ^Fiber_Signal) -> Signal_Context {
	if signal == nil {
		return {}
	}
	return {
		name = signal.name,
		identity = signal.identity,
		headers = signal.headers,
		originating_event_id = signal.originating_event_id,
	}
}

// await_signal_payloads_with_context consumes the oldest buffered signal with
// the requested name, or suspends until Core delivers one. Returned Payload
// and metadata views are owned by the Workflow instance.
await_signal_payloads_with_context :: proc(ctx: ^Fiber_Context, name: string) -> Payloads_Signal {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || len(name) == 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {}
	}
	instance := ctx.instance
	if current_cancellation_applies(instance) {
		return {}
	}
	signal, found := find_buffered_signal(instance, name)
	if found {
		signal.consumed = true
		return {
			values = signal.input_views,
			metadata = fiber_signal_context(signal),
		}
	}
	waiting_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		instance.phase = .Failed
		return {}
	}
	instance.waiting_signal_name = waiting_name
	instance.phase = .Waiting_Signal
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return {}
	}
	if current_cancellation_applies(instance) {
		return {}
	}
	signal, found = find_buffered_signal(instance, name)
	if !found {
		instance.phase = .Failed
		return {}
	}
	signal.consumed = true
	return {
		values = signal.input_views,
		metadata = fiber_signal_context(signal),
	}
}

// await_signal_payload_with_context is the one-argument compatibility adapter.
await_signal_payload_with_context :: proc(ctx: ^Fiber_Context, name: string) -> Payload_Signal {
	result := await_signal_payloads_with_context(ctx, name)
	if ctx == nil || ctx.instance == nil || ctx.instance.phase == .Failed {
		return {metadata = result.metadata}
	}
	if current_cancellation_applies(ctx.instance) {
		return {metadata = result.metadata}
	}
	if len(result.values) != 1 {
		signal_argument_count_failed(
			ctx.instance,
			"single-Payload Workflow Signal await requires exactly one argument",
		)
		return {metadata = result.metadata}
	}
	return {value = result.values[0], metadata = result.metadata}
}

await_signal_payloads :: proc(ctx: ^Fiber_Context, name: string) -> []codec.Payload_View {
	return await_signal_payloads_with_context(ctx, name).values
}

// await_signal_payload is the compatibility adapter that discards invocation
// metadata. The returned view is owned by the Workflow instance.
await_signal_payload :: proc(ctx: ^Fiber_Context, name: string) -> codec.Payload_View {
	return await_signal_payload_with_context(ctx, name).value
}

// await_signal is the json/plain string compatibility adapter.
await_signal :: proc(ctx: ^Fiber_Context, name: string) -> string {
	payload := await_signal_payload(ctx, name)
	if ctx == nil || ctx.instance == nil || ctx.instance.phase == .Failed {
		return ""
	}
	if current_cancellation_applies(ctx.instance) {
		return ""
	}
	value, decode_error := codec.decode_string(payload.data, codec.encoding(payload))
	if decode_error != .None {
		signal_payload_conversion_failed(ctx.instance, "could not decode Workflow signal as json/plain string")
		return ""
	}
	return value
}

await_signal_json :: proc(ctx: ^Fiber_Context, name: string, $T: typeid) -> T {
	payload := await_signal_payload(ctx, name)
	value: T
	if ctx == nil || ctx.instance == nil || ctx.instance.phase == .Failed {
		return value
	}
	if current_cancellation_applies(ctx.instance) {
		return value
	}
	if codec.decode_json(payload, &value) != .None {
		signal_payload_conversion_failed(ctx.instance, "could not decode Workflow signal as json/plain")
		return {}
	}
	return value
}

await_signal_json_with_context :: proc(
	ctx: ^Fiber_Context,
	name: string,
	$T: typeid,
) -> Json_Signal(T) {
	result := await_signal_payload_with_context(ctx, name)
	value: T
	if ctx == nil || ctx.instance == nil || ctx.instance.phase == .Failed {
		return {metadata = result.metadata}
	}
	if current_cancellation_applies(ctx.instance) {
		return {metadata = result.metadata}
	}
	if codec.decode_json(result.value, &value) != .None {
		signal_payload_conversion_failed(ctx.instance, "could not decode Workflow signal as json/plain")
		return {metadata = result.metadata}
	}
	return {value = value, metadata = result.metadata}
}

// await_signal_or_timeout races a named signal against a durable Temporal
// timer. Core orders signal jobs before timer jobs in the same activation, so a
// buffered matching signal wins that tie without cancelling an already-fired
// timer.
await_signal_payloads_or_timeout :: proc(
	ctx: ^Fiber_Context,
	name: string,
	duration: time.Duration,
) -> Payloads_Signal_Wait_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || len(name) == 0 || duration <= 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return {status = .Timed_Out}
	}
	instance := ctx.instance
	if current_cancellation_applies(instance) {
		return {status = .Canceled}
	}
	signal, signal_found := find_buffered_signal(instance, name)
	if signal_found {
		signal.consumed = true
		return {
			status = .Received,
			values = signal.input_views,
			metadata = fiber_signal_context(signal),
		}
	}
	waiting_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		instance.phase = .Failed
		return {status = .Timed_Out}
	}
	instance.next_timer_seq += 1
	operation := Fiber_Timer_Operation{
		seq = instance.next_timer_seq,
		duration = duration,
		state = .Scheduled,
		detached_from_root_cancellation = !cancellation_token_linked_to_root(
			instance,
			current_cancellation_token_id(instance),
		),
		cancellation_token_id = current_cancellation_token_id(instance),
	}
	append(&instance.timers, operation)
	instance.waiting_signal_name = waiting_name
	instance.waiting_timer_seq = operation.seq
	instance.phase = .Waiting_Signal_Or_Timer
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.phase = .Failed
		return {status = .Timed_Out}
	}
	if current_cancellation_applies(instance) {
		return {status = .Canceled}
	}

	signal, signal_found = find_buffered_signal(instance, name)
	timer, timer_found := find_timer_operation(instance, operation.seq)
	if signal_found {
		signal.consumed = true
		if timer_found && timer.state == .Commanded {
			timer.state = .Cancel_Requested
		}
		return {
			status = .Received,
			values = signal.input_views,
			metadata = fiber_signal_context(signal),
		}
	}
	if timer_found && timer.state == .Resolved {
		return {status = .Timed_Out}
	}
	instance.phase = .Failed
	return {status = .Timed_Out}
}

await_signal_payload_or_timeout :: proc(
	ctx: ^Fiber_Context,
	name: string,
	duration: time.Duration,
) -> Payload_Signal_Wait_Result {
	result := await_signal_payloads_or_timeout(ctx, name, duration)
	if result.status != .Received {
		return {status = result.status, metadata = result.metadata}
	}
	if ctx == nil || ctx.instance == nil || len(result.values) != 1 {
		if ctx != nil {
			signal_argument_count_failed(
				ctx.instance,
				"single-Payload Workflow Signal await requires exactly one argument",
			)
		}
		return {status = .Received, metadata = result.metadata}
	}
	return {status = .Received, value = result.values[0], metadata = result.metadata}
}

await_signal_or_timeout :: proc(
	ctx: ^Fiber_Context,
	name: string,
	duration: time.Duration,
) -> Signal_Wait_Result {
	payload_result := await_signal_payload_or_timeout(ctx, name, duration)
	if payload_result.status != .Received {
		return {status = payload_result.status, metadata = payload_result.metadata}
	}
	value, decode_error := codec.decode_string(
		payload_result.value.data,
		codec.encoding(payload_result.value),
	)
	if decode_error != .None {
		if ctx != nil {
			signal_payload_conversion_failed(ctx.instance, "could not decode Workflow signal as json/plain string")
		}
		return {status = .Received, metadata = payload_result.metadata}
	}
	return {status = .Received, value = value, metadata = payload_result.metadata}
}

await_signal_json_or_timeout :: proc(
	ctx: ^Fiber_Context,
	name: string,
	duration: time.Duration,
	$T: typeid,
) -> Json_Signal_Wait_Result(T) {
	payload_result := await_signal_payload_or_timeout(ctx, name, duration)
	if payload_result.status != .Received {
		return {status = payload_result.status, metadata = payload_result.metadata}
	}
	value: T
	if codec.decode_json(payload_result.value, &value) != .None {
		if ctx != nil {
			signal_payload_conversion_failed(ctx.instance, "could not decode Workflow signal as json/plain")
		}
		return {status = .Received, metadata = payload_result.metadata}
	}
	return {status = .Received, value = value, metadata = payload_result.metadata}
}

selector_activity :: proc(future: Activity_Future) -> Selector_Case {
	return {kind = .Activity, activity = future}
}

selector_nexus_operation :: proc(future: Nexus_Operation_Future) -> Selector_Case {
	return {kind = .Nexus_Operation, nexus_operation = future}
}

selector_timer :: proc(future: Timer_Future) -> Selector_Case {
	return {kind = .Timer, timer = future}
}

selector_signal :: proc(name: string) -> Selector_Case {
	return {kind = .Signal, signal_name = name}
}

selector_child_workflow_start :: proc(future: Child_Workflow_Future) -> Selector_Case {
	return {kind = .Child_Workflow_Start, child_workflow = future}
}

selector_child_workflow :: proc(future: Child_Workflow_Future) -> Selector_Case {
	return {kind = .Child_Workflow, child_workflow = future}
}

workflow_wait_fail :: proc(ctx: ^Fiber_Context, message, failure_type: string) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	ctx.instance.failure_message = message
	ctx.instance.failure_type = failure_type
	ctx.instance.phase = .Failed
}

selector_fail :: proc(ctx: ^Fiber_Context, message: string) {
	workflow_wait_fail(ctx, message, "OdinWorkflowSelectorError")
}

join_fail :: proc(ctx: ^Fiber_Context, message: string) {
	workflow_wait_fail(ctx, message, "OdinWorkflowJoinError")
}

timer_fail :: proc(ctx: ^Fiber_Context, message: string) {
	workflow_wait_fail(ctx, message, "OdinWorkflowTimerError")
}

timer_operation_ready :: proc(operation: ^Fiber_Timer_Operation) -> bool {
	return operation != nil &&
	       (operation.state == .Resolved || operation.state == .Cancel_Requested ||
	        operation.state == .Canceled)
}

selector_case_ready :: proc(
	ctx: ^Fiber_Context,
	selector_case: ^Selector_Case,
	token_id: u32,
) -> bool {
	if ctx == nil || ctx.instance == nil || selector_case == nil {
		return false
	}
	instance := ctx.instance
	switch selector_case.kind {
	case .Activity:
		operation, found := find_activity_operation(instance, selector_case.activity.seq)
		return found && activity_operation_ready_for_waiter(instance, operation, token_id)
	case .Nexus_Operation:
		operation, found := find_nexus_operation(instance, selector_case.nexus_operation.seq)
		return found && nexus_operation_ready_for_waiter(instance, operation, token_id)
	case .Timer:
		operation, found := find_timer_operation(instance, selector_case.timer.seq)
		return found && timer_operation_ready(operation)
	case .Signal:
		_, found := find_buffered_signal(instance, selector_case.signal_name)
		return found
	case .Child_Workflow_Start:
		operation, found := find_child_workflow_operation(instance, selector_case.child_workflow.seq)
		return found && operation.start_state != .Pending
	case .Child_Workflow:
		operation, found := find_child_workflow_operation(instance, selector_case.child_workflow.seq)
		return found && operation.completion_state != .Pending
	}
	return false
}

selector_ready_index :: proc(
	ctx: ^Fiber_Context,
	cases: []Selector_Case,
	token_id: u32,
) -> (int, bool) {
	for &selector_case, index in cases {
		if selector_case_ready(ctx, &selector_case, token_id) {
			return index, true
		}
	}
	return -1, false
}

selector_should_resume :: proc(ctx: ^Fiber_Context) -> bool {
	if ctx == nil || ctx.instance == nil || len(ctx.waiting_selector_cases) == 0 {
		return false
	}
	_, ready := selector_ready_index(
		ctx,
		ctx.waiting_selector_cases,
		ctx.waiting_selector_cancellation_token_id,
	)
	return ready || cancellation_token_is_requested_id(
		ctx.instance,
		ctx.waiting_selector_cancellation_token_id,
	)
}

selector_cases_valid :: proc(ctx: ^Fiber_Context, cases: []Selector_Case) -> bool {
	if ctx == nil || ctx.instance == nil || len(cases) == 0 {
		return false
	}
	instance := ctx.instance
	for selector_case in cases {
		switch selector_case.kind {
		case .Activity:
			if selector_case.activity.seq == 0 {
				return false
			}
			_, found := find_activity_operation(instance, selector_case.activity.seq)
			if !found {
				return false
			}
		case .Nexus_Operation:
			if selector_case.nexus_operation.seq == 0 {
				return false
			}
			_, found := find_nexus_operation(instance, selector_case.nexus_operation.seq)
			if !found {
				return false
			}
		case .Timer:
			if selector_case.timer.seq == 0 {
				return false
			}
			_, found := find_timer_operation(instance, selector_case.timer.seq)
			if !found {
				return false
			}
		case .Signal:
			if len(selector_case.signal_name) == 0 {
				return false
			}
		case .Child_Workflow_Start, .Child_Workflow:
			if selector_case.child_workflow.seq == 0 {
				return false
			}
			_, found := find_child_workflow_operation(instance, selector_case.child_workflow.seq)
			if !found {
				return false
			}
		}
	}
	return true
}

join_cases_equal :: proc(left, right: ^Selector_Case) -> bool {
	if left == nil || right == nil || left.kind != right.kind {
		return false
	}
	switch left.kind {
	case .Activity:
		return left.activity.seq == right.activity.seq
	case .Nexus_Operation:
		return left.nexus_operation.seq == right.nexus_operation.seq
	case .Timer:
		return left.timer.seq == right.timer.seq
	case .Child_Workflow_Start, .Child_Workflow:
		return left.child_workflow.seq == right.child_workflow.seq
	case .Signal:
		return left.signal_name == right.signal_name
	}
	return false
}

join_cases_valid :: proc(ctx: ^Fiber_Context, cases: []Selector_Case) -> bool {
	if !selector_cases_valid(ctx, cases) {
		return false
	}
	for &join_case, index in cases {
		// Signal consumption needs an owned per-case result and is deliberately
		// deferred. Durable operation handles have stable result APIs already.
		if join_case.kind == .Signal {
			return false
		}
		for previous_index in 0..<index {
			if join_cases_equal(&join_case, &cases[previous_index]) {
				return false
			}
		}
	}
	return true
}

join_all_ready :: proc(ctx: ^Fiber_Context, cases: []Selector_Case, token_id: u32) -> bool {
	if len(cases) == 0 {
		return false
	}
	for &join_case in cases {
		if !selector_case_ready(ctx, &join_case, token_id) {
			return false
		}
	}
	return true
}

join_should_resume :: proc(ctx: ^Fiber_Context) -> bool {
	if ctx == nil || ctx.instance == nil || len(ctx.waiting_join_cases) == 0 {
		return false
	}
	return join_all_ready(
		ctx,
		ctx.waiting_join_cases,
		ctx.waiting_join_cancellation_token_id,
	) || cancellation_token_is_requested_id(
		ctx.instance,
		ctx.waiting_join_cancellation_token_id,
	)
}

// join_all waits until every durable case is ready. Cases are checked in slice
// order and their existing await APIs can then retrieve results synchronously.
// Failed or canceled operations are not hidden or implicitly discarded.
// Signal cases are intentionally unsupported until their consumed result can
// be returned without hidden allocation.
join_all :: proc(ctx: ^Fiber_Context, cases: []Selector_Case) -> Join_Status {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(ctx.waiting_join_cases) != 0 || !join_cases_valid(ctx, cases) {
		join_fail(ctx, "invalid Workflow join")
		return .Canceled
	}
	instance := ctx.instance
	token_id := current_cancellation_token_id(instance)
	if join_all_ready(ctx, cases, token_id) {
		return .Ready
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		return .Canceled
	}

	ctx.waiting_join_cases = cases
	ctx.waiting_join_cancellation_token_id = token_id
	defer {
		ctx.waiting_join_cases = nil
		ctx.waiting_join_cancellation_token_id = 0
	}
	instance.phase = .Waiting_Join
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		join_fail(ctx, "could not resume Workflow join")
		return .Canceled
	}
	if join_all_ready(ctx, cases, token_id) {
		return .Ready
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		return .Canceled
	}
	join_fail(ctx, "Workflow join resumed before every case was ready")
	return .Canceled
}

selector_result :: proc(ctx: ^Fiber_Context, cases: []Selector_Case, index: int) -> Selector_Result {
	selector_case := &cases[index]
	result := Selector_Result{status = .Selected, index = index, kind = selector_case.kind}
	if selector_case.kind == .Signal {
		signal, found := find_buffered_signal(ctx.instance, selector_case.signal_name)
		if !found {
			selector_fail(ctx, "selected Workflow Signal was not buffered")
			return {status = .Canceled, index = -1}
		}
		signal.consumed = true
		result.signal = {
			values = signal.input_views,
			metadata = fiber_signal_context(signal),
		}
	}
	return result
}

// selector_select polls cases in declaration order, matching the pinned Rust
// SDK's biased deterministic select. Losing operations remain live and must be
// awaited or canceled explicitly by Workflow code.
selector_select :: proc(ctx: ^Fiber_Context, cases: []Selector_Case) -> Selector_Result {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready ||
	   len(ctx.waiting_selector_cases) != 0 || !selector_cases_valid(ctx, cases) {
		selector_fail(ctx, "invalid Workflow selector")
		return {status = .Canceled, index = -1}
	}
	instance := ctx.instance
	token_id := current_cancellation_token_id(instance)
	if index, ready := selector_ready_index(ctx, cases, token_id); ready {
		return selector_result(ctx, cases, index)
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		return {status = .Canceled, index = -1}
	}

	ctx.waiting_selector_cases = cases
	ctx.waiting_selector_cancellation_token_id = token_id
	defer {
		ctx.waiting_selector_cases = nil
		ctx.waiting_selector_cancellation_token_id = 0
	}
	instance.phase = .Waiting_Selector
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		selector_fail(ctx, "could not resume Workflow selector")
		return {status = .Canceled, index = -1}
	}
	if index, ready := selector_ready_index(ctx, cases, token_id); ready {
		return selector_result(ctx, cases, index)
	}
	if cancellation_token_is_requested_id(instance, token_id) {
		return {status = .Canceled, index = -1}
	}
	selector_fail(ctx, "Workflow selector resumed without a ready case")
	return {status = .Canceled, index = -1}
}

start_timer_unintercepted :: proc(ctx: ^Fiber_Context, options: Timer_Options) -> Timer_Future {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || options.duration <= 0 {
		timer_fail(ctx, "invalid Workflow timer start")
		return {}
	}
	instance := ctx.instance
	token_id := current_cancellation_token_id(instance)
	if options.cancellation_token.id != 0 {
		if options.cancellation_token.instance != instance {
			timer_fail(ctx, "Workflow timer cancellation token belongs to another run")
			return {}
		}
		_, token_found := find_cancellation_node(instance, options.cancellation_token.id)
		if !token_found {
			timer_fail(ctx, "Workflow timer cancellation token was not found")
			return {}
		}
		token_id = options.cancellation_token.id
	}
	summary := codec.Payload{}
	has_summary := len(options.summary) > 0
	if has_summary {
		encode_error: codec.Error
		summary, encode_error = codec.encode_json(
			options.summary,
			instance.odin_context.allocator,
		)
		if encode_error != .None {
			timer_fail(ctx, "could not preserve Workflow timer summary")
			return {}
		}
	}
	instance.next_timer_seq += 1
	state := Fiber_Operation_State.Scheduled
	if cancellation_token_is_requested_id(instance, token_id) {
		state = .Canceled
	}
	operation := Fiber_Timer_Operation{
		seq = instance.next_timer_seq,
		duration = options.duration,
		summary = summary,
		has_summary = has_summary,
		state = state,
		detached_from_root_cancellation = !cancellation_token_linked_to_root(
			instance,
			token_id,
		),
		cancellation_token_id = token_id,
	}
	append(&instance.timers, operation)
	return {seq = operation.seq}
}

// start_timer_with_options creates a durable timer Future without suspending.
// Its options remain mutable throughout the synchronous outbound interceptor
// chain and are copied into Workflow-run ownership by the terminal continuation.
start_timer_with_options :: proc(ctx: ^Fiber_Context, options: Timer_Options) -> Timer_Future {
	if ctx == nil || ctx.instance == nil {
		return {}
	}
	input := Outbound_Start_Timer_Input{ctx = ctx, options = options}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	return outbound_start_timer_chain(&state, &input)
}

// start_timer creates a durable timer Future without suspending. Its outbound
// interceptor may inspect or replace Future creation but does not await it.
start_timer :: proc(ctx: ^Fiber_Context, duration: time.Duration) -> Timer_Future {
	return start_timer_with_options(ctx, {duration = duration})
}

cancel_timer :: proc(ctx: ^Fiber_Context, future: Timer_Future) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		return false
	}
	operation, found := find_timer_operation(ctx.instance, future.seq)
	if !found {
		return false
	}
	switch operation.state {
	case .Scheduled:
		operation.state = .Canceled
	case .Commanded:
		operation.state = .Cancel_Requested
	case .Cancel_Requested, .Canceled:
	case .Resolved, .Failed:
		return false
	}
	return true
}

await_timer :: proc(ctx: ^Fiber_Context, future: Timer_Future) -> Timer_Result_Status {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || future.seq == 0 {
		timer_fail(ctx, "invalid Workflow timer await")
		return .Canceled
	}
	instance := ctx.instance
	operation, found := find_timer_operation(instance, future.seq)
	if !found {
		timer_fail(ctx, "Workflow timer operation was not found")
		return .Canceled
	}
	if !timer_operation_ready(operation) {
		instance.waiting_timer_seq = future.seq
		instance.phase = .Waiting_Timer
		if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
			timer_fail(ctx, "could not resume Workflow timer await")
			return .Canceled
		}
		operation, found = find_timer_operation(instance, future.seq)
		if !found || !timer_operation_ready(operation) {
			timer_fail(ctx, "Workflow timer resumed without a resolution")
			return .Canceled
		}
	}
	if operation.state == .Resolved {
		return .Fired
	}
	return .Canceled
}

// sleep schedules a durable Temporal timer and suspends the Workflow fiber.
// It never reads wall-clock time in the language runtime.
sleep_terminal :: proc(ctx: ^Fiber_Context, options: Timer_Options) {
	if ctx == nil || ctx.instance == nil || ctx.instance.phase != .Ready || options.duration <= 0 {
		if ctx != nil && ctx.instance != nil {
			ctx.instance.phase = .Failed
		}
		return
	}
	// Sleep remains a suspending operation, while Timer creation traverses its
	// own non-suspending interceptor chain.
	future := start_timer_with_options(ctx, options)
	_ = await_timer(ctx, future)
}

sleep_with_options :: proc(ctx: ^Fiber_Context, options: Timer_Options) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	input := Outbound_Sleep_Input{ctx = ctx, options = options}
	state := Outbound_Interceptor_State{instance = ctx.instance}
	outbound_sleep_chain(&state, &input)
}

sleep :: proc(ctx: ^Fiber_Context, duration: time.Duration) {
	sleep_with_options(ctx, {duration = duration})
}

condition_fail :: proc(ctx: ^Fiber_Context, message: string) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	ctx.instance.failure_message = message
	ctx.instance.failure_type = "OdinWorkflowConditionError"
	ctx.instance.phase = .Failed
}

condition_should_resume :: proc(ctx: ^Fiber_Context) -> bool {
	if ctx == nil || ctx.instance == nil || ctx.waiting_condition_predicate == nil {
		return false
	}
	if ctx.waiting_condition_predicate(ctx.waiting_condition_data) {
		return true
	}
	return cancellation_token_is_requested_id(
		ctx.instance,
		ctx.waiting_condition_cancellation_token_id,
	)
}

await_condition_raw :: proc(
	ctx: ^Fiber_Context,
	data: rawptr,
	predicate: Condition_Predicate,
) -> bool {
	if ctx == nil || ctx.instance == nil || data == nil || predicate == nil ||
	   ctx.instance.phase != .Ready || ctx.waiting_condition_predicate != nil {
		condition_fail(ctx, "invalid Workflow condition wait")
		return false
	}
	instance := ctx.instance
	for {
		if predicate(data) {
			return true
		}
		if current_cancellation_applies(instance) {
			return false
		}
		ctx.waiting_condition_data = data
		ctx.waiting_condition_predicate = predicate
		ctx.waiting_condition_cancellation_token_id = current_cancellation_token_id(instance)
		instance.phase = .Waiting_Condition
		yield_result := fiber_raw.yield(instance.fiber)
		ctx.waiting_condition_data = nil
		ctx.waiting_condition_predicate = nil
		ctx.waiting_condition_cancellation_token_id = 0
		if yield_result != .Success || instance.phase != .Ready {
			condition_fail(ctx, "could not resume Workflow condition wait")
			return false
		}
	}
}

// await_condition suspends the current Workflow fiber until a deterministic,
// side-effect-free predicate over run-owned state becomes true. It returns false
// when the cancellation token selected at the start of the wait is requested.
// The state and predicate are borrowed for the stackful call and must not escape
// the Workflow run.
await_condition :: proc(
	ctx: ^Fiber_Context,
	state: ^$State,
	predicate: proc(state: ^State) -> bool,
) -> bool {
	Callback :: #type proc(state: ^State) -> bool
	Data :: struct {
		state:     ^State,
		predicate: Callback,
	}
	if state == nil || predicate == nil {
		condition_fail(ctx, "Workflow condition requires state and predicate")
		return false
	}
	data := Data{state = state, predicate = predicate}
	return await_condition_raw(
		ctx,
		&data,
		proc(condition_data: rawptr) -> bool {
			condition := cast(^Data)condition_data
			return condition.predicate(condition.state)
		},
	)
}

mutex_fail :: proc(ctx: ^Fiber_Context, message: string) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	ctx.instance.failure_message = message
	ctx.instance.failure_type = "OdinWorkflowMutexError"
	ctx.instance.phase = .Failed
}

mutex_remove_waiter :: proc(mutex: ^Mutex, ctx: ^Fiber_Context) {
	if mutex == nil || ctx == nil {
		return
	}
	for waiter, index in mutex.waiters {
		if waiter == ctx {
			ordered_remove(&mutex.waiters, index)
			return
		}
	}
}

mutex_record_owned :: proc(ctx: ^Fiber_Context, mutex: ^Mutex) {
	append(&ctx.owned_mutexes, mutex)
}

mutex_forget_owned :: proc(ctx: ^Fiber_Context, mutex: ^Mutex) -> bool {
	for owned, index in ctx.owned_mutexes {
		if owned == mutex {
			ordered_remove(&ctx.owned_mutexes, index)
			return true
		}
	}
	return false
}

mutex_grant_next :: proc(mutex: ^Mutex) {
	if mutex == nil || mutex.instance == nil || mutex.owner != nil {
		return
	}
	for len(mutex.waiters) > 0 {
		waiter := mutex.waiters[0]
		ordered_remove(&mutex.waiters, 0)
		if waiter == nil || waiter.instance != mutex.instance || waiter.waiting_mutex != mutex {
			continue
		}
		if cancellation_token_is_requested_id(
			mutex.instance,
			waiter.waiting_mutex_cancellation_token_id,
		) {
			continue
		}
		mutex.owner = waiter
		waiter.mutex_granted = true
		mutex_record_owned(waiter, mutex)
		return
	}
}

mutex_unlock_internal :: proc(ctx: ^Fiber_Context, mutex: ^Mutex) -> bool {
	if ctx == nil || mutex == nil || mutex.owner != ctx {
		return false
	}
	mutex.owner = nil
	_ = mutex_forget_owned(ctx, mutex)
	mutex_grant_next(mutex)
	return true
}

// mutex_try_lock acquires a free Workflow-local mutex without suspension.
// Reentrant acquisition is rejected as a deterministic execution failure.
mutex_try_lock :: proc(ctx: ^Fiber_Context, mutex: ^Mutex) -> bool {
	if ctx == nil || ctx.instance == nil || mutex == nil || ctx.instance.phase != .Ready {
		mutex_fail(ctx, "invalid Workflow mutex acquisition")
		return false
	}
	instance := ctx.instance
	if mutex.instance == nil {
		mutex.instance = instance
		mutex.instance_id = instance.instance_id
	} else if mutex.instance != instance || mutex.instance_id != instance.instance_id {
		mutex_fail(ctx, "Workflow mutex belongs to another Workflow run")
		return false
	}
	if mutex.owner == ctx {
		mutex_fail(ctx, "Workflow mutex is not reentrant")
		return false
	}
	if current_cancellation_applies(instance) || mutex.owner != nil || len(mutex.waiters) > 0 {
		return false
	}
	mutex.owner = ctx
	mutex_record_owned(ctx, mutex)
	return true
}

// mutex_lock acquires a Workflow-local FIFO mutex, suspending only the current
// Workflow fiber. False without a Workflow failure means its cancellation token
// was requested while it waited. Callers should normally pair success with
// `defer mutex_unlock(...)`.
mutex_lock :: proc(ctx: ^Fiber_Context, mutex: ^Mutex) -> bool {
	if ctx == nil || ctx.instance == nil || mutex == nil || ctx.instance.phase != .Ready {
		mutex_fail(ctx, "invalid Workflow mutex acquisition")
		return false
	}
	instance := ctx.instance
	if mutex.instance == nil {
		mutex.instance = instance
		mutex.instance_id = instance.instance_id
	} else if mutex.instance != instance || mutex.instance_id != instance.instance_id {
		mutex_fail(ctx, "Workflow mutex belongs to another Workflow run")
		return false
	}
	if mutex.owner == ctx || ctx.waiting_mutex != nil {
		mutex_fail(ctx, "Workflow mutex is not reentrant")
		return false
	}
	if current_cancellation_applies(instance) {
		return false
	}
	if mutex.owner == nil && len(mutex.waiters) == 0 {
		mutex.owner = ctx
		mutex_record_owned(ctx, mutex)
		return true
	}
	append(&mutex.waiters, ctx)
	ctx.waiting_mutex = mutex
	ctx.waiting_mutex_cancellation_token_id = current_cancellation_token_id(instance)
	ctx.mutex_granted = false
	instance.phase = .Waiting_Mutex
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		mutex_remove_waiter(mutex, ctx)
		ctx.waiting_mutex = nil
		ctx.waiting_mutex_cancellation_token_id = 0
		ctx.mutex_granted = false
		mutex_fail(ctx, "could not resume Workflow mutex wait")
		return false
	}
	granted := ctx.mutex_granted && mutex.owner == ctx
	if !granted {
		mutex_remove_waiter(mutex, ctx)
	}
	ctx.waiting_mutex = nil
	ctx.waiting_mutex_cancellation_token_id = 0
	ctx.mutex_granted = false
	return granted
}

// mutex_unlock transfers ownership to the oldest non-canceled waiter. Unlock
// by a non-owner is a deterministic execution failure.
mutex_unlock :: proc(ctx: ^Fiber_Context, mutex: ^Mutex) -> bool {
	if ctx == nil || ctx.instance == nil || mutex == nil ||
	   mutex.instance != ctx.instance || mutex.instance_id != ctx.instance.instance_id ||
	   !mutex_unlock_internal(ctx, mutex) {
		mutex_fail(ctx, "Workflow mutex unlock requires its current owner")
		return false
	}
	return true
}

// mutex_is_locked observes local ownership and never suspends.
mutex_is_locked :: proc(mutex: ^Mutex) -> bool {
	return mutex != nil && mutex.owner != nil
}

mutex_release_owned :: proc(ctx: ^Fiber_Context) -> bool {
	if ctx == nil || len(ctx.owned_mutexes) == 0 {
		return false
	}
	for len(ctx.owned_mutexes) > 0 {
		mutex := ctx.owned_mutexes[len(ctx.owned_mutexes) - 1]
		if !mutex_unlock_internal(ctx, mutex) {
			ordered_remove(&ctx.owned_mutexes, len(ctx.owned_mutexes) - 1)
		}
	}
	return true
}

// all_handlers_finished reports whether every concurrent Signal and accepted
// Update routine has finished.
all_handlers_finished :: proc(ctx: ^Fiber_Context) -> bool {
	if ctx == nil || ctx.instance == nil {
		return true
	}
	return !has_active_handlers(ctx.instance)
}

// await_all_handlers_finished suspends the main Workflow routine until every
// Signal and accepted Update routine has finished. Calling it from a handler
// routine would wait on itself, so that is rejected as a deterministic failure.
await_all_handlers_finished :: proc(ctx: ^Fiber_Context) {
	if ctx == nil || ctx.instance == nil {
		return
	}
	instance := ctx.instance
	if ctx != &instance.workflow_context || instance.phase != .Ready {
		instance.failure_message = "await_all_handlers_finished must be called by the main Workflow routine"
		instance.failure_type = "OdinWorkflowHandlerAwaitError"
		instance.phase = .Failed
		return
	}
	if !has_active_handlers(instance) {
		return
	}
	instance.phase = .Waiting_Handlers
	if fiber_raw.yield(instance.fiber) != .Success || instance.phase != .Ready {
		instance.failure_message = "could not resume Workflow handler await"
		instance.failure_type = "OdinWorkflowHandlerAwaitError"
		instance.phase = .Failed
	}
}

duration_to_proto :: proc(duration: time.Duration) -> ^proto.Google_Protobuf_Duration {
	if duration <= 0 {
		return nil
	}
	nanoseconds := i64(duration)
	value := new(proto.Google_Protobuf_Duration)
	value.seconds = nanoseconds / i64(time.Second)
	value.nanos = i32(nanoseconds % i64(time.Second))
	return value
}

activity_cancellation_type_to_proto :: proc(
	cancellation_type: Activity_Cancellation_Type,
) -> proto.Coresdk_Workflow_Commands_ActivityCancellationType {
	switch cancellation_type {
	case .Try_Cancel:
		return .TRY_CANCEL
	case .Wait_Cancellation_Completed:
		return .WAIT_CANCELLATION_COMPLETED
	case .Abandon:
		return .ABANDON
	}
	return .TRY_CANCEL
}

local_activity_cancellation_type_to_proto :: proc(
	cancellation_type: Local_Activity_Cancellation_Type,
) -> proto.Coresdk_Workflow_Commands_ActivityCancellationType {
	switch cancellation_type {
	case .Wait_Cancellation_Completed:
		return .WAIT_CANCELLATION_COMPLETED
	case .Try_Cancel:
		return .TRY_CANCEL
	case .Abandon:
		return .ABANDON
	}
	return .WAIT_CANCELLATION_COMPLETED
}

child_workflow_cancellation_type_to_proto :: proc(
	cancellation_type: Child_Workflow_Cancellation_Type,
) -> proto.Coresdk_Child_Workflow_ChildWorkflowCancellationType {
	switch cancellation_type {
	case .Wait_Cancellation_Completed:
		return .WAIT_CANCELLATION_COMPLETED
	case .Abandon:
		return .ABANDON
	case .Try_Cancel:
		return .TRY_CANCEL
	case .Wait_Cancellation_Requested:
		return .WAIT_CANCELLATION_REQUESTED
	}
	return .WAIT_CANCELLATION_COMPLETED
}

parent_close_policy_to_proto :: proc(
	policy: Parent_Close_Policy,
) -> proto.Coresdk_Child_Workflow_ParentClosePolicy {
	switch policy {
	case .Unspecified:
		return .PARENT_CLOSE_POLICY_UNSPECIFIED
	case .Terminate:
		return .PARENT_CLOSE_POLICY_TERMINATE
	case .Abandon:
		return .PARENT_CLOSE_POLICY_ABANDON
	case .Request_Cancel:
		return .PARENT_CLOSE_POLICY_REQUEST_CANCEL
	}
	return .PARENT_CLOSE_POLICY_UNSPECIFIED
}

workflow_id_reuse_policy_to_proto :: proc(
	policy: Workflow_Id_Reuse_Policy,
) -> proto.Temporal_Api_Enums_V1_WorkflowIdReusePolicy {
	switch policy {
	case .Allow_Duplicate_Failed_Only:
		return .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY
	case .Reject_Duplicate:
		return .WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	case .Terminate_If_Running:
		return .WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING
	case .Unspecified, .Allow_Duplicate:
		// Language SDKs normalize their zero-value option to Temporal's
		// documented Child Workflow default before crossing into Core.
		return .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
	return .WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
}

retry_policy_to_proto :: proc(
	policy: ^Retry_Policy,
) -> ^proto.Temporal_Api_Common_V1_RetryPolicy {
	if policy == nil {
		return nil
	}
	result := new(proto.Temporal_Api_Common_V1_RetryPolicy)
	result.initial_interval = duration_to_proto(policy.initial_interval)
	result.backoff_coefficient = policy.backoff_coefficient
	result.maximum_interval = duration_to_proto(policy.maximum_interval)
	result.maximum_attempts = policy.maximum_attempts
	for error_type in policy.non_retryable_error_types {
		append(&result.non_retryable_error_types, error_type)
	}
	return result
}

versioning_intent_to_proto :: proc(
	value: Versioning_Intent,
) -> proto.Coresdk_Common_VersioningIntent {
	switch value {
	case .Compatible: return .COMPATIBLE
	case .Default:    return .DEFAULT
	case .Unspecified:
	}
	return .UNSPECIFIED
}

continue_as_new_versioning_intent_to_proto :: proc(
	value: Continue_As_New_Versioning_Intent,
) -> proto.Coresdk_Common_VersioningIntent {
	return versioning_intent_to_proto(value)
}

continue_as_new_versioning_behavior_to_proto :: proc(
	value: Continue_As_New_Versioning_Behavior,
) -> proto.Temporal_Api_Enums_V1_ContinueAsNewVersioningBehavior {
	switch value {
	case .Auto_Upgrade:
		return .CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_AUTO_UPGRADE
	case .Use_Ramping_Version:
		return .CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_USE_RAMPING_VERSION
	case .Unspecified:
	}
	return .CONTINUE_AS_NEW_VERSIONING_BEHAVIOR_UNSPECIFIED
}

workflow_command_set_summary :: proc(
	command: ^proto.Coresdk_Workflow_Commands_WorkflowCommand,
	summary: ^codec.Payload,
) {
	if command == nil || summary == nil {
		return
	}
	metadata := new(proto.Temporal_Api_Sdk_V1_UserMetadata)
	metadata.summary = new(proto.Temporal_Api_Common_V1_Payload)
	metadata.summary^ = proto.encode_payload(codec.payload_view(summary))
	command.user_metadata = metadata
}

append_fiber_start_child_workflow_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Child_Workflow_Operation,
	default_task_queue: string,
) {
	task_queue := operation.options.task_queue
	if len(task_queue) == 0 {
		task_queue = default_task_queue
	}
	retry_policy: ^proto.Temporal_Api_Common_V1_RetryPolicy
	if operation.options.has_retry_policy {
		retry_policy = retry_policy_to_proto(&operation.options.retry_policy)
	}
	start := new(proto.Coresdk_Workflow_Commands_StartChildWorkflowExecution)
	start^ = {
		seq = operation.seq,
		workflow_id = operation.workflow_id,
		workflow_type = operation.workflow_type,
		task_queue = task_queue,
		workflow_execution_timeout = duration_to_proto(operation.options.workflow_execution_timeout),
		workflow_run_timeout = duration_to_proto(operation.options.workflow_run_timeout),
		workflow_task_timeout = duration_to_proto(operation.options.workflow_task_timeout),
		parent_close_policy = parent_close_policy_to_proto(operation.options.parent_close_policy),
		workflow_id_reuse_policy = workflow_id_reuse_policy_to_proto(
			operation.options.id_reuse_policy,
		),
		retry_policy = retry_policy,
		cron_schedule = operation.options.cron_schedule,
		cancellation_type = child_workflow_cancellation_type_to_proto(operation.options.cancellation_type),
		versioning_intent = versioning_intent_to_proto(operation.options.versioning_intent),
	}
	if operation.options.has_priority {
		start.priority = new(proto.Temporal_Api_Common_V1_Priority)
		start.priority^ = {
			priority_key = operation.options.priority.priority_key,
			fairness_key = operation.options.priority.fairness_key,
			fairness_weight = operation.options.priority.fairness_weight,
		}
	}
	for &entry in operation.options.headers {
		header_payload := new(proto.Temporal_Api_Common_V1_Payload)
		header_payload^ = proto.encode_payload(codec.payload_view(&entry.payload))
		append(
			&start.headers,
			proto.Coresdk_Workflow_Commands_StartChildWorkflowExecution_HeadersEntry{
				key = entry.key,
				value = header_payload,
			},
		)
	}
	for &entry in operation.options.memo {
		memo_payload := new(proto.Temporal_Api_Common_V1_Payload)
		memo_payload^ = proto.encode_payload(codec.payload_view(&entry.payload))
		append(
			&start.memo,
			proto.Coresdk_Workflow_Commands_StartChildWorkflowExecution_MemoEntry{
				key = entry.key,
				value = memo_payload,
			},
		)
	}
	if operation.options.has_search_attributes {
		start.search_attributes = new(proto.Temporal_Api_Common_V1_SearchAttributes)
		for &entry in operation.options.search_attributes {
			attribute_payload := new(proto.Temporal_Api_Common_V1_Payload)
			attribute_payload^ = proto.encode_payload(codec.payload_view(&entry.payload))
			append(
				&start.search_attributes.indexed_fields,
				proto.Temporal_Api_Common_V1_SearchAttributes_IndexedFieldsEntry{
					key = entry.key,
					value = attribute_payload,
				},
			)
		}
	}
	for &input in operation.inputs {
		append(&start.input, proto.encode_payload(codec.payload_view(&input)))
	}
	command := proto.Coresdk_Workflow_Commands_WorkflowCommand{
		variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Child_Workflow_Execution{
			value = start,
		},
	}
	if operation.options.has_static_summary || operation.options.has_static_details {
		metadata := new(proto.Temporal_Api_Sdk_V1_UserMetadata)
		if operation.options.has_static_summary {
			metadata.summary = new(proto.Temporal_Api_Common_V1_Payload)
			metadata.summary^ = proto.encode_payload(
				codec.payload_view(&operation.options.static_summary),
			)
		}
		if operation.options.has_static_details {
			metadata.details = new(proto.Temporal_Api_Common_V1_Payload)
			metadata.details^ = proto.encode_payload(
				codec.payload_view(&operation.options.static_details),
			)
		}
		command.user_metadata = metadata
	}
	append(
		commands,
		command,
	)
	operation.command_state = .Commanded
	result.started_child_workflow = true
	result.started_child_workflow_count += 1
	result.child_workflow_seq = operation.seq
}

append_fiber_schedule_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Activity_Operation,
	task_queue: string,
) {
	if operation.is_local {
		options := &operation.local_options
		schedule := new(proto.Coresdk_Workflow_Commands_ScheduleLocalActivity)
		original_schedule_time: ^proto.Google_Protobuf_Timestamp
		if operation.has_original_schedule_time {
			original_schedule_time = new(proto.Google_Protobuf_Timestamp)
			original_schedule_time.seconds = operation.original_schedule_seconds
			original_schedule_time.nanos = operation.original_schedule_nanos
		}
		schedule^ = {
			seq = operation.command_seq,
			activity_id = operation.activity_id,
			activity_type = operation.activity_type,
			attempt = operation.attempt,
			original_schedule_time = original_schedule_time,
			schedule_to_close_timeout = duration_to_proto(options.schedule_to_close_timeout),
			schedule_to_start_timeout = duration_to_proto(options.schedule_to_start_timeout),
			start_to_close_timeout = duration_to_proto(options.start_to_close_timeout),
			retry_policy = retry_policy_to_proto(&options.retry_policy),
			local_retry_threshold = duration_to_proto(options.local_retry_threshold),
			cancellation_type = local_activity_cancellation_type_to_proto(options.cancellation_type),
			include_arguments_in_marker = options.include_arguments_in_marker,
		}
		for &entry in options.headers {
			header_payload := new(proto.Temporal_Api_Common_V1_Payload)
			header_payload^ = proto.encode_payload(codec.payload_view(&entry.payload))
			append(
				&schedule.headers,
				proto.Coresdk_Workflow_Commands_ScheduleLocalActivity_HeadersEntry{
					key = entry.key,
					value = header_payload,
				},
			)
		}
		for &input in operation.inputs {
			append(&schedule.arguments, proto.encode_payload(codec.payload_view(&input)))
		}
		command := proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Local_Activity{
				value = schedule,
			},
		}
		if options.has_summary {
			workflow_command_set_summary(&command, &options.summary)
		}
		append(commands, command)
		operation.state = .Commanded
		result.scheduled_local_activity = true
		result.scheduled_local_activity_count += 1
		result.activity_seq = operation.command_seq
		return
	}
	activity_task_queue := operation.options.task_queue
	if len(activity_task_queue) == 0 {
		activity_task_queue = task_queue
	}
	retry_policy: ^proto.Temporal_Api_Common_V1_RetryPolicy
	if operation.options.has_retry_policy {
		retry_policy = retry_policy_to_proto(&operation.options.retry_policy)
	}
	schedule := new(proto.Coresdk_Workflow_Commands_ScheduleActivity)
	schedule^ = proto.Coresdk_Workflow_Commands_ScheduleActivity{
		seq = operation.command_seq,
		activity_id = operation.activity_id,
		activity_type = operation.activity_type,
		task_queue = activity_task_queue,
		schedule_to_close_timeout = duration_to_proto(operation.options.schedule_to_close_timeout),
		schedule_to_start_timeout = duration_to_proto(operation.options.schedule_to_start_timeout),
		start_to_close_timeout = duration_to_proto(operation.options.start_to_close_timeout),
		heartbeat_timeout = duration_to_proto(operation.options.heartbeat_timeout),
		retry_policy = retry_policy,
		cancellation_type = activity_cancellation_type_to_proto(operation.options.cancellation_type),
		do_not_eagerly_execute = operation.options.disable_eager_execution,
		versioning_intent = versioning_intent_to_proto(operation.options.versioning_intent),
	}
	if operation.options.has_priority {
		schedule.priority = new(proto.Temporal_Api_Common_V1_Priority)
		schedule.priority^ = {
			priority_key = operation.options.priority.priority_key,
			fairness_key = operation.options.priority.fairness_key,
			fairness_weight = operation.options.priority.fairness_weight,
		}
	}
	for &entry in operation.options.headers {
		header_payload := new(proto.Temporal_Api_Common_V1_Payload)
		header_payload^ = proto.encode_payload(codec.payload_view(&entry.payload))
		append(
			&schedule.headers,
			proto.Coresdk_Workflow_Commands_ScheduleActivity_HeadersEntry{
				key = entry.key,
				value = header_payload,
			},
		)
	}
	for &input in operation.inputs {
		append(&schedule.arguments, proto.encode_payload(codec.payload_view(&input)))
	}
	command := proto.Coresdk_Workflow_Commands_WorkflowCommand{
		variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Schedule_Activity{
			value = schedule,
		},
	}
	if operation.options.has_summary {
		workflow_command_set_summary(&command, &operation.options.summary)
	}
	append(commands, command)
	operation.state = .Commanded
	result.scheduled_activity = true
	result.scheduled_activity_count += 1
	result.activity_seq = operation.command_seq
}

append_fiber_timer_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Timer_Operation,
) {
	timeout := new(proto.Google_Protobuf_Duration)
	nanoseconds := i64(operation.duration)
	timeout.seconds = nanoseconds / i64(time.Second)
	timeout.nanos = i32(nanoseconds % i64(time.Second))
	start_timer := new(proto.Coresdk_Workflow_Commands_StartTimer)
	start_timer^ = proto.Coresdk_Workflow_Commands_StartTimer{
		seq = operation.seq,
		start_to_fire_timeout = timeout,
	}
	command := proto.Coresdk_Workflow_Commands_WorkflowCommand{
		variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Start_Timer{
			value = start_timer,
		},
	}
	if operation.has_summary {
		summary := new(proto.Temporal_Api_Common_V1_Payload)
		summary^ = proto.encode_payload(codec.payload_view(&operation.summary))
		user_metadata := new(proto.Temporal_Api_Sdk_V1_UserMetadata)
		user_metadata.summary = summary
		command.user_metadata = user_metadata
	}
	append(
		commands,
		command,
	)
	operation.state = .Commanded
	result.started_timer = true
	result.timer_seq = operation.seq
}

append_fiber_cancel_timer_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Timer_Operation,
) {
	cancel_timer := new(proto.Coresdk_Workflow_Commands_CancelTimer)
	cancel_timer.seq = operation.seq
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Timer{
				value = cancel_timer,
			},
		},
	)
	operation.state = .Canceled
	result.canceled_timer = true
	result.canceled_timer_seq = operation.seq
}

append_fiber_timer_cancellations :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.timers) {
		operation := &instance.timers[index]
		if operation.state == .Cancel_Requested {
			append_fiber_cancel_timer_command(result, commands, operation)
		}
	}
}

append_fiber_cancel_activity_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Activity_Operation,
) {
	if operation.cancellation_command_emitted {
		return
	}
	if operation.is_local {
		request := new(proto.Coresdk_Workflow_Commands_RequestCancelLocalActivity)
		request.seq = operation.command_seq
		append(
			commands,
			proto.Coresdk_Workflow_Commands_WorkflowCommand{
				variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Request_Cancel_Local_Activity{
					value = request,
				},
			},
		)
		operation.cancellation_command_emitted = true
		if operation.local_options.cancellation_type != .Wait_Cancellation_Completed {
			operation.state = .Canceled
		}
		result.requested_local_activity_cancellation = true
		result.requested_local_activity_cancellation_count += 1
		result.canceled_activity_seq = operation.command_seq
		return
	}
	request := new(proto.Coresdk_Workflow_Commands_RequestCancelActivity)
	request.seq = operation.command_seq
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Request_Cancel_Activity{
				value = request,
			},
		},
	)
	operation.cancellation_command_emitted = true
	if operation.options.cancellation_type != .Wait_Cancellation_Completed {
		operation.state = .Canceled
	}
	result.requested_activity_cancellation = true
	result.requested_activity_cancellation_count += 1
	result.canceled_activity_seq = operation.command_seq
}

append_fiber_activity_cancellations :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.activities) {
		operation := &instance.activities[index]
		if operation.state == .Cancel_Requested && !operation.cancellation_command_emitted {
			append_fiber_cancel_activity_command(result, commands, operation)
		}
	}
}

append_fiber_cancel_child_workflow_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_Child_Workflow_Operation,
) {
	request := new(proto.Coresdk_Workflow_Commands_CancelChildWorkflowExecution)
	request.child_workflow_seq = operation.seq
	request.reason = operation.cancellation_reason
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Child_Workflow_Execution{
				value = request,
			},
		},
	)
	operation.command_state = .Canceled
	result.requested_child_workflow_cancellation = true
	result.requested_child_workflow_cancellation_count += 1
	result.canceled_child_workflow_seq = operation.seq
}

append_fiber_signal_external_workflow_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_External_Signal_Operation,
) {
	request := new(proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution)
	request.seq = operation.seq
	request.signal_name = operation.signal_name
	for &entry in operation.headers {
		header_payload := new(proto.Temporal_Api_Common_V1_Payload)
		header_payload^ = proto.encode_payload(codec.payload_view(&entry.payload))
		append(
			&request.headers,
			proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution_HeadersEntry{
				key = entry.key,
				value = header_payload,
			},
		)
	}
	if operation.child_workflow_seq != 0 {
		request.target = proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution_Target_Child_Workflow_Id{
			value = operation.child_workflow_id,
		}
	} else {
		execution := new(proto.Coresdk_Common_NamespacedWorkflowExecution)
		execution^ = {
			namespace = operation.namespace,
			workflow_id = operation.child_workflow_id,
			run_id = operation.run_id,
		}
		request.target = proto.Coresdk_Workflow_Commands_SignalExternalWorkflowExecution_Target_Workflow_Execution{
			value = execution,
		}
	}
	for &input in operation.inputs {
		append(&request.args, proto.encode_payload(codec.payload_view(&input)))
	}
	command := proto.Coresdk_Workflow_Commands_WorkflowCommand{
		variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Signal_External_Workflow_Execution{
			value = request,
		},
	}
	if operation.has_summary {
		workflow_command_set_summary(&command, &operation.summary)
	}
	append(commands, command)
	operation.state = .Commanded
	if operation.child_workflow_seq != 0 {
		result.signaled_child_workflow = true
		result.signaled_child_workflow_count += 1
		result.child_workflow_signal_seq = operation.seq
	} else {
		result.signaled_external_workflow = true
		result.signaled_external_workflow_count += 1
		result.external_workflow_signal_seq = operation.seq
	}
}

append_fiber_request_cancel_external_workflow_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_External_Cancel_Operation,
) {
	execution := new(proto.Coresdk_Common_NamespacedWorkflowExecution)
	execution^ = {
		namespace = operation.namespace,
		workflow_id = operation.workflow_id,
		run_id = operation.run_id,
	}
	request := new(proto.Coresdk_Workflow_Commands_RequestCancelExternalWorkflowExecution)
	request^ = {
		seq = operation.seq,
		workflow_execution = execution,
		reason = operation.reason,
	}
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Request_Cancel_External_Workflow_Execution{
				value = request,
			},
		},
	)
	operation.state = .Commanded
	result.requested_external_workflow_cancellation = true
	result.requested_external_workflow_cancellation_count += 1
	result.external_workflow_cancellation_seq = operation.seq
}

append_fiber_cancel_external_signal_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	operation: ^Fiber_External_Signal_Operation,
) {
	request := new(proto.Coresdk_Workflow_Commands_CancelSignalWorkflow)
	request.seq = operation.seq
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Signal_Workflow{
				value = request,
			},
		},
	)
	operation.state = .Canceled
	if operation.child_workflow_seq != 0 {
		result.canceled_child_workflow_signal = true
	} else {
		result.canceled_external_workflow_signal = true
	}
}

append_fiber_external_signal_cancellations :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.external_signals) {
		operation := &instance.external_signals[index]
		if operation.state == .Cancel_Requested {
			append_fiber_cancel_external_signal_command(result, commands, operation)
		}
	}
}

append_fiber_child_workflow_cancellations :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.child_workflows) {
		operation := &instance.child_workflows[index]
		if operation.command_state == .Cancel_Requested {
			append_fiber_cancel_child_workflow_command(result, commands, operation)
		}
	}
}

Workflow_Command_State :: struct {
	phase:             Fiber_Phase,
	next_activity_seq: u32,
	next_nexus_operation_seq: u32,
	next_timer_seq:    u32,
	next_child_workflow_seq: u32,
	next_external_signal_seq: u32,
	next_external_cancel_seq: u32,
	activity_count:    int,
	nexus_operation_count: int,
	timer_count:       int,
	child_workflow_count: int,
	external_signal_count: int,
	external_cancel_count: int,
	patch_count:          int,
	property_command_count: int,
	memo_count:              int,
	search_attribute_count:  int,
	random_state:         u64,
}

workflow_command_state :: proc(instance: ^Fiber_Instance) -> Workflow_Command_State {
	return {
		phase = instance.phase,
		next_activity_seq = instance.next_activity_seq,
		next_nexus_operation_seq = instance.next_nexus_operation_seq,
		next_timer_seq = instance.next_timer_seq,
		next_child_workflow_seq = instance.next_child_workflow_seq,
		next_external_signal_seq = instance.next_external_signal_seq,
		next_external_cancel_seq = instance.next_external_cancel_seq,
		activity_count = len(instance.activities),
		nexus_operation_count = len(instance.nexus_operations),
		timer_count = len(instance.timers),
		child_workflow_count = len(instance.child_workflows),
		external_signal_count = len(instance.external_signals),
		external_cancel_count = len(instance.external_cancels),
		patch_count = len(instance.patches),
		property_command_count = len(instance.property_commands),
		memo_count = len(instance.memo),
		search_attribute_count = len(instance.search_attributes),
		random_state = instance.random_state,
	}
}

workflow_command_state_changed :: proc(
	instance: ^Fiber_Instance,
	before: Workflow_Command_State,
) -> bool {
	return instance.phase != before.phase ||
		instance.next_activity_seq != before.next_activity_seq ||
		instance.next_nexus_operation_seq != before.next_nexus_operation_seq ||
		instance.next_timer_seq != before.next_timer_seq ||
		instance.next_child_workflow_seq != before.next_child_workflow_seq ||
		instance.next_external_signal_seq != before.next_external_signal_seq ||
		instance.next_external_cancel_seq != before.next_external_cancel_seq ||
		len(instance.activities) != before.activity_count ||
		len(instance.nexus_operations) != before.nexus_operation_count ||
		len(instance.timers) != before.timer_count ||
		len(instance.child_workflows) != before.child_workflow_count ||
		len(instance.external_signals) != before.external_signal_count ||
		len(instance.external_cancels) != before.external_cancel_count ||
		len(instance.patches) != before.patch_count ||
		len(instance.property_commands) != before.property_command_count ||
		len(instance.memo) != before.memo_count ||
		len(instance.search_attributes) != before.search_attribute_count ||
		instance.random_state != before.random_state
}

restore_workflow_command_state :: proc(
	instance: ^Fiber_Instance,
	before: Workflow_Command_State,
) {
	instance.phase = before.phase
	instance.next_activity_seq = before.next_activity_seq
	instance.next_nexus_operation_seq = before.next_nexus_operation_seq
	instance.next_timer_seq = before.next_timer_seq
	instance.next_child_workflow_seq = before.next_child_workflow_seq
	instance.next_external_signal_seq = before.next_external_signal_seq
	instance.next_external_cancel_seq = before.next_external_cancel_seq
	instance.random_state = before.random_state
	for len(instance.activities) > before.activity_count {
		ordered_remove(&instance.activities, len(instance.activities) - 1)
	}
	for len(instance.nexus_operations) > before.nexus_operation_count {
		ordered_remove(&instance.nexus_operations, len(instance.nexus_operations) - 1)
	}
	for len(instance.timers) > before.timer_count {
		ordered_remove(&instance.timers, len(instance.timers) - 1)
	}
	for len(instance.child_workflows) > before.child_workflow_count {
		ordered_remove(&instance.child_workflows, len(instance.child_workflows) - 1)
	}
	for len(instance.external_signals) > before.external_signal_count {
		ordered_remove(&instance.external_signals, len(instance.external_signals) - 1)
	}
	for len(instance.external_cancels) > before.external_cancel_count {
		ordered_remove(&instance.external_cancels, len(instance.external_cancels) - 1)
	}
	for len(instance.patches) > before.patch_count {
		ordered_remove(&instance.patches, len(instance.patches) - 1)
	}
	for len(instance.property_commands) > before.property_command_count {
		ordered_remove(&instance.property_commands, len(instance.property_commands) - 1)
	}
	for len(instance.memo) > before.memo_count {
		ordered_remove(&instance.memo, len(instance.memo) - 1)
	}
	for len(instance.search_attributes) > before.search_attribute_count {
		ordered_remove(&instance.search_attributes, len(instance.search_attributes) - 1)
	}
}

Workflow_Metadata_Query_Response :: struct {
	current_details: string `json:"currentDetails,omitempty"`,
}

invoke_workflow_metadata_query :: proc(instance: ^Fiber_Instance) -> Payload_Result {
	if instance == nil {
		return payload_fail(
			"Workflow metadata query has no active Workflow",
			"OdinWorkflowQueryError",
		)
	}
	payload, encode_error := codec.encode_json(
		Workflow_Metadata_Query_Response{current_details = instance.current_details},
	)
	if encode_error != .None {
		return payload_fail(
			"could not encode Workflow metadata query result",
			"OdinWorkflowQueryPayloadConversionError",
		)
	}
	return payload_complete(payload)
}

invoke_workflow_query :: proc(
	instance: ^Fiber_Instance,
	query: ^proto.Coresdk_Workflow_Activation_QueryWorkflow,
) -> Payload_Result {
	if instance == nil || query == nil || len(query.query_id) == 0 || len(query.query_type) == 0 {
		return payload_fail("Workflow query is malformed", "OdinWorkflowQueryError")
	}
	if query.query_type == WORKFLOW_METADATA_QUERY_TYPE {
		return invoke_workflow_metadata_query(instance)
	}
	handler, found := find_query_handler(instance, query.query_type)
	if !found {
		message := fmt.aprintf("Workflow query handler %s is not registered", query.query_type)
		return payload_fail(message, "OdinWorkflowQueryNotFound")
	}
	command_state := workflow_command_state(instance)
	inputs := make([dynamic]codec.Payload)
	defer {
		for &input in inputs {
			codec.payload_free(&input)
		}
		delete(inputs)
	}
	for &source in query.arguments {
		input, clone_error := proto.clone_payload(&source)
		if clone_error != .None {
			return payload_fail(
				"could not preserve Workflow query inputs",
				"OdinWorkflowQueryPayloadConversionError",
			)
		}
		append(&inputs, input)
	}
	input_views := make([]codec.Payload_View, len(inputs))
	defer delete(input_views)
	for &input, index in inputs {
		input_views[index] = codec.payload_view(&input)
	}
	query_context := Query_Context{
		id = query.query_id,
		type = query.query_type,
		headers = make([dynamic]Fiber_Named_Payload),
		instance = instance,
	}
	defer {
		for &entry in query_context.headers {
			delete(entry.key)
			codec.payload_free(&entry.payload)
		}
		delete(query_context.headers)
	}
	if !clone_query_header_payloads(query.headers[:], &query_context.headers) {
		return payload_fail(
			"could not preserve Workflow query headers",
			"OdinWorkflowQueryHeaderConversionError",
		)
	}
	interceptor_input := Inbound_Handle_Query_Input{
		workflow_type = instance.workflow_type,
		query = &query_context,
		inputs = input_views,
	}
	interceptor_state := Inbound_Handle_Query_State{
		instance = instance,
		handler = handler,
	}
	result := inbound_handle_query_chain(&interceptor_state, &interceptor_input)
	if workflow_command_state_changed(instance, command_state) {
		codec.payload_free(&result.payload)
		restore_workflow_command_state(instance, command_state)
		return payload_fail(
			"Workflow query handler attempted to mutate runtime command state",
			"OdinWorkflowQueryMutationError",
		)
	}
	return result
}

append_query_response_command :: proc(
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	response: ^Pending_Query_Response,
) {
	query_result := new(proto.Coresdk_Workflow_Commands_QueryResult)
	query_result.query_id = response.query_id
	if response.result.status == .Completed {
		payload := new(proto.Temporal_Api_Common_V1_Payload)
		payload^ = proto.encode_payload(codec.payload_view(&response.result.payload))
		success := new(proto.Coresdk_Workflow_Commands_QuerySuccess)
		success.response = payload
		query_result.variant = proto.Coresdk_Workflow_Commands_QueryResult_Variant_Succeeded{
			value = success,
		}
	} else {
		failure_type := response.result.failure_type
		if len(failure_type) == 0 {
			failure_type = "OdinWorkflowQueryError"
		}
		application_info := new(proto.Temporal_Api_Failure_V1_ApplicationFailureInfo)
		application_info.type = failure_type
		application_info.non_retryable = true
		failure := new(proto.Temporal_Api_Failure_V1_Failure)
		failure.message = response.result.failure_message
		failure.source = "OdinSDK"
		failure.failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
			value = application_info,
		}
		query_result.variant = proto.Coresdk_Workflow_Commands_QueryResult_Variant_Failed{
			value = failure,
		}
	}
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Respond_To_Query{
				value = query_result,
			},
		},
	)
}

append_pending_update_response :: proc(
	responses: ^[dynamic]Pending_Update_Response,
	protocol_instance_id: string,
	kind: Update_Response_Kind,
	result: Payload_Result = {},
	failure_proto: []u8 = nil,
) {
	append(
		responses,
		Pending_Update_Response{
			protocol_instance_id = protocol_instance_id,
			kind = kind,
			result = result,
			failure_proto = failure_proto,
		},
	)
}

clone_signal_context_for_routine :: proc(
	instance: ^Fiber_Instance,
	source: ^Signal_Context,
) -> (result: Signal_Context, ok: bool) {
	if instance == nil || source == nil || len(source.name) == 0 {
		return {}, false
	}
	allocator := instance.odin_context.allocator
	name, name_error := strings.clone(source.name, allocator)
	identity, identity_error := strings.clone(source.identity, allocator)
	if name_error != nil || identity_error != nil {
		return {}, false
	}
	result = {
		name = name,
		identity = identity,
		headers = make([dynamic]Fiber_Named_Payload, allocator),
		originating_event_id = source.originating_event_id,
	}
	for &entry in source.headers {
		key, key_error := strings.clone(entry.key, allocator)
		payload, payload_error := codec.payload_clone(
			codec.payload_view(&entry.payload),
			allocator,
		)
		if key_error != nil || payload_error != .None {
			return {}, false
		}
		append(&result.headers, Fiber_Named_Payload{key = key, payload = payload})
	}
	return result, true
}

start_signal_routine :: proc(
	instance: ^Fiber_Instance,
	handler: ^Fiber_Signal_Handler,
	name: string,
	signal_context: ^Signal_Context,
	inputs: []codec.Payload_View,
) -> (^Fiber_Signal_Routine, Error) {
	if instance == nil || handler == nil ||
	   (handler.invoke_routine != nil || handler.invoke_routine_with_context != nil) ==
	   (handler.invoke_payloads_routine != nil || handler.invoke_payloads_routine_with_context != nil) ||
	   signal_context == nil || len(name) == 0 {
		return nil, error_from_string("invalid Workflow Signal routine")
	}
	allocator := instance.odin_context.allocator
	routine, allocation_error := new(Fiber_Signal_Routine, allocator)
	if allocation_error != nil {
		return nil, error_from_string("could not allocate Workflow Signal routine")
	}
	owned_name, name_error := strings.clone(name, allocator)
	routine.inputs = make([dynamic]codec.Payload, allocator)
	inputs_ok := true
	for input in inputs {
		owned_input, input_error := codec.payload_clone(input, allocator)
		if input_error != .None {
			inputs_ok = false
			break
		}
		append(&routine.inputs, owned_input)
	}
	routine.input_views = make([]codec.Payload_View, len(routine.inputs), allocator)
	for &input, index in routine.inputs {
		routine.input_views[index] = codec.payload_view(&input)
	}
	owned_context, context_ok := clone_signal_context_for_routine(instance, signal_context)
	if name_error != nil || !inputs_ok || !context_ok {
		return nil, error_from_string("could not preserve Workflow Signal routine input")
	}
	routine.instance = instance
	routine.name = owned_name
	routine.signal_context = owned_context
	routine.callback_data = handler.callback_data
	routine.invoke = handler.invoke_routine
	routine.invoke_with_context = handler.invoke_routine_with_context
	routine.invoke_payloads = handler.invoke_payloads_routine
	routine.invoke_payloads_with_context = handler.invoke_payloads_routine_with_context
	routine.unfinished_policy = handler.unfinished_policy
	routine.ctx.instance = instance
	routine.current_cancellation_token_id = 1
	routine.phase = .Ready
	create_result := fiber_raw.create(
		&routine.fiber,
		signal_routine_entry,
		routine,
		uint(instance.runtime.stack_options.signal_handler),
	)
	if create_result != .Success {
		return nil, fiber_result_error("could not create Workflow Signal routine", create_result)
	}
	resume_error := resume_signal_routine(routine)
	if resume_error.message != nil {
		if routine.fiber != nil {
			_ = fiber_raw.destroy(routine.fiber)
			routine.fiber = nil
		}
		return nil, resume_error
	}
	append(&instance.signal_routines, routine)
	return routine, {}
}

finish_signal_routine :: proc(
	routine: ^Fiber_Signal_Routine,
	result: ^Process_Result,
) -> Error {
	if routine == nil || routine.completion_observed || routine.fiber == nil ||
	   fiber_raw.status(routine.fiber) != .Dead {
		return {}
	}
	routine.completion_observed = true
	destroy_result := fiber_raw.destroy(routine.fiber)
	routine.fiber = nil
	if destroy_result != .Success {
		return fiber_result_error("could not destroy completed Workflow Signal routine", destroy_result)
	}
	if routine.result.status == .Failed || routine.phase == .Failed {
		message := routine.result.failure_message
		if len(message) == 0 {
			message = "Workflow Signal routine failed"
		}
		codec.payload_free(&routine.result.payload)
		return error_from_string(message)
	}
	codec.payload_free(&routine.result.payload)
	if result != nil {
		result.signal_handlers_completed += 1
	}
	return {}
}

finish_signal_routines :: proc(
	instance: ^Fiber_Instance,
	result: ^Process_Result,
) -> Error {
	for routine in instance.signal_routines {
		finish_error := finish_signal_routine(routine, result)
		if finish_error.message != nil {
			return finish_error
		}
	}
	return {}
}

signal_routine_should_resume :: proc(routine: ^Fiber_Signal_Routine) -> bool {
	if routine == nil || routine.instance == nil || routine.fiber == nil ||
	   fiber_raw.status(routine.fiber) != .Suspended {
		return false
	}
	instance := routine.instance
	cancellation_applies := cancellation_token_is_requested_id(
		instance,
		routine.current_cancellation_token_id,
	)
	switch routine.phase {
	case .Waiting_Condition:
		return condition_should_resume(&routine.ctx)
	case .Waiting_Mutex:
		return routine.ctx.mutex_granted || cancellation_applies
	case .Waiting_Join:
		return join_should_resume(&routine.ctx)
	case .Waiting_Selector:
		return selector_should_resume(&routine.ctx)
	case .Waiting_Activity:
		operation, found := find_activity_operation(instance, routine.waiting_activity_seq)
		return found && activity_operation_ready_for_waiter(
			instance,
			operation,
			routine.current_cancellation_token_id,
		)
	case .Waiting_Nexus_Operation:
		operation, found := find_nexus_operation(instance, routine.waiting_nexus_operation_seq)
		return found && nexus_operation_ready_for_waiter(
			instance,
			operation,
			routine.current_cancellation_token_id,
		)
	case .Waiting_Child_Workflow_Start:
		operation, found := find_child_workflow_operation(instance, routine.waiting_child_workflow_seq)
		return found && operation.start_state != .Pending
	case .Waiting_Child_Workflow:
		operation, found := find_child_workflow_operation(instance, routine.waiting_child_workflow_seq)
		return found && operation.completion_state != .Pending
	case .Waiting_External_Signal:
		operation, found := find_external_signal_operation(instance, routine.waiting_external_signal_seq)
		return found && (operation.state == .Resolved || operation.state == .Failed ||
			operation.state == .Cancel_Requested || operation.state == .Canceled)
	case .Waiting_External_Cancel:
		operation, found := find_external_cancel_operation(instance, routine.waiting_external_cancel_seq)
		return found && (operation.state == .Resolved || operation.state == .Failed)
	case .Waiting_Timer:
		operation, found := find_timer_operation(instance, routine.waiting_timer_seq)
		return found && (operation.state == .Resolved || operation.state == .Cancel_Requested ||
			operation.state == .Canceled)
	case .Waiting_Signal:
		_, found := find_buffered_signal(instance, routine.waiting_signal_name)
		return found || cancellation_applies
	case .Waiting_Signal_Or_Timer:
		_, signal_found := find_buffered_signal(instance, routine.waiting_signal_name)
		timer, timer_found := find_timer_operation(instance, routine.waiting_timer_seq)
		return signal_found || cancellation_applies ||
			(timer_found && (timer.state == .Resolved || timer.state == .Cancel_Requested ||
			timer.state == .Canceled))
	case .Waiting_Handlers, .Ready, .Continued_As_New, .Canceled, .Completed, .Failed:
	}
	return false
}

resume_ready_signal_routines :: proc(instance: ^Fiber_Instance) -> (resumed: bool, error: Error) {
	for routine in instance.signal_routines {
		if !signal_routine_should_resume(routine) {
			continue
		}
		resumed = true
		routine.phase = .Ready
		routine.waiting_activity_seq = 0
		routine.waiting_nexus_operation_seq = 0
		routine.waiting_child_workflow_seq = 0
		routine.waiting_external_signal_seq = 0
		routine.waiting_external_cancel_seq = 0
		routine.waiting_timer_seq = 0
		routine.waiting_signal_name = ""
		resume_error := resume_signal_routine(routine)
		if resume_error.message != nil {
			return resumed, resume_error
		}
	}
	return resumed, {}
}

dispatch_signal_handler :: proc(
	instance: ^Fiber_Instance,
	handler: ^Fiber_Signal_Handler,
	signal: ^Fiber_Signal,
	result: ^Process_Result,
) -> Error {
	if instance == nil || handler == nil || signal == nil {
		return error_from_string("invalid Workflow Signal handler dispatch")
	}
	signal_context := fiber_signal_context(signal)
	if result != nil {
		result.signal_handlers_started += 1
	}
	if handler.invoke_routine != nil || handler.invoke_routine_with_context != nil ||
	   handler.invoke_payloads_routine != nil || handler.invoke_payloads_routine_with_context != nil {
		routine, routine_error := start_signal_routine(
			instance,
			handler,
			signal.name,
			&signal_context,
			signal.input_views,
		)
		if routine_error.message != nil {
			return routine_error
		}
		return finish_signal_routine(routine, result)
	}
	command_state := workflow_command_state(instance)
	interceptor_input := Inbound_Handle_Signal_Input{
		workflow_type = instance.workflow_type,
		signal = &signal_context,
		inputs = signal.input_views,
	}
	interceptor_state := Inbound_Handle_Signal_State{
		instance = instance,
		handler = handler,
	}
	handler_result := inbound_handle_signal_chain(&interceptor_state, &interceptor_input)
	if workflow_command_state_changed(instance, command_state) {
		codec.payload_free(&handler_result.payload)
		restore_workflow_command_state(instance, command_state)
		return error_from_string("synchronous Workflow Signal handler attempted to mutate runtime command state")
	}
	if handler_result.status == .Failed {
		message := handler_result.failure_message
		if len(message) == 0 {
			message = "Workflow Signal handler failed"
		}
		codec.payload_free(&handler_result.payload)
		return error_from_string(message)
	}
	codec.payload_free(&handler_result.payload)
	if result != nil {
		result.signal_handlers_completed += 1
	}
	return {}
}

dispatch_buffered_signals_for_handler :: proc(
	instance: ^Fiber_Instance,
	name: string,
	result: ^Process_Result,
) -> Error {
	if instance == nil || len(name) == 0 {
		return error_from_string("invalid buffered Workflow Signal dispatch")
	}
	for {
		signal, found := find_buffered_signal(instance, name)
		if !found {
			return {}
		}
		handler, handler_found := find_signal_handler(instance, name)
		if !handler_found {
			return {}
		}
		signal.consumed = true
		dispatch_error := dispatch_signal_handler(instance, handler, signal, result)
		if dispatch_error.message != nil {
			return dispatch_error
		}
	}
}

invoke_workflow_signal :: proc(
	instance: ^Fiber_Instance,
	signal: ^proto.Coresdk_Workflow_Activation_SignalWorkflow,
	result: ^Process_Result,
) -> Error {
	if instance == nil || signal == nil || len(signal.signal_name) == 0 {
		return error_from_string("Workflow signal must have a name")
	}
	preserved := Fiber_Signal{
		headers = make([dynamic]Fiber_Named_Payload),
		inputs = make([dynamic]codec.Payload),
		originating_event_id = signal.originating_event_id,
	}
	buffered := false
	defer if !buffered {
		delete(preserved.name)
		delete(preserved.identity)
		for &entry in preserved.headers {
			delete(entry.key)
			codec.payload_free(&entry.payload)
		}
		delete(preserved.headers)
		for &input in preserved.inputs {
			codec.payload_free(&input)
		}
		delete(preserved.inputs)
		delete(preserved.input_views)
	}
	preserved.name, _ = strings.clone(signal.signal_name)
	preserved.identity, _ = strings.clone(signal.identity)
	inputs_ok := true
	for &source in signal.input {
		cloned_payload, clone_error := proto.clone_payload(&source)
		if clone_error != .None {
			inputs_ok = false
			break
		}
		append(&preserved.inputs, cloned_payload)
	}
	preserved.input_views = make([]codec.Payload_View, len(preserved.inputs))
	for &input, index in preserved.inputs {
		preserved.input_views[index] = codec.payload_view(&input)
	}
	if len(preserved.name) == 0 ||
	   (len(signal.identity) > 0 && len(preserved.identity) == 0) ||
	   !inputs_ok ||
	   !clone_signal_header_payloads(signal.headers[:], &preserved.headers) {
		return error_from_string("could not preserve Workflow signal Payloads or metadata")
	}
	handler, found := find_signal_handler(instance, signal.signal_name)
	if !found {
		append(&instance.signals, preserved)
		buffered = true
		return {}
	}
	return dispatch_signal_handler(instance, handler, &preserved, result)
}

find_update_routine :: proc(
	instance: ^Fiber_Instance,
	protocol_instance_id: string,
) -> (^Fiber_Update_Routine, bool) {
	for routine in instance.update_routines {
		if routine.protocol_instance_id == protocol_instance_id {
			return routine, true
		}
	}
	return nil, false
}

start_update_routine :: proc(
	instance: ^Fiber_Instance,
	handler: ^Fiber_Update_Handler,
	protocol_instance_id: string,
	update_context: ^Update_Context,
	inputs: []codec.Payload_View,
) -> (^Fiber_Update_Routine, Error) {
	if instance == nil || handler == nil ||
	   (handler.invoke_routine != nil || handler.invoke_routine_with_context != nil) ==
	   (handler.invoke_payloads_routine != nil || handler.invoke_payloads_routine_with_context != nil) ||
	   update_context == nil ||
	   len(protocol_instance_id) == 0 {
		return nil, error_from_string("invalid Workflow Update routine")
	}
	_, duplicate := find_update_routine(instance, protocol_instance_id)
	if duplicate {
		return nil, error_from_string("Workflow Update routine already exists")
	}
	allocator := instance.odin_context.allocator
	routine, allocation_error := new(Fiber_Update_Routine, allocator)
	if allocation_error != nil {
		return nil, error_from_string("could not allocate Workflow Update routine")
	}
	owned_protocol_id, id_error := strings.clone(protocol_instance_id, allocator)
	routine.inputs = make([dynamic]codec.Payload, allocator)
	inputs_ok := true
	for input in inputs {
		owned_input, input_error := codec.payload_clone(input, allocator)
		if input_error != .None {
			inputs_ok = false
			break
		}
		append(&routine.inputs, owned_input)
	}
	routine.input_views = make([]codec.Payload_View, len(routine.inputs), allocator)
	for &input, index in routine.inputs {
		routine.input_views[index] = codec.payload_view(&input)
	}
	if id_error != nil || !inputs_ok {
		return nil, error_from_string("could not preserve Workflow Update routine input")
	}
	routine.instance = instance
	routine.protocol_instance_id = owned_protocol_id
	routine.callback_data = handler.callback_data
	routine.invoke = handler.invoke_routine
	routine.invoke_with_context = handler.invoke_routine_with_context
	routine.invoke_payloads = handler.invoke_payloads_routine
	routine.invoke_payloads_with_context = handler.invoke_payloads_routine_with_context
	routine.update_context = update_context^
	routine.unfinished_policy = handler.unfinished_policy
	routine.ctx.instance = instance
	routine.ctx.update_context = &routine.update_context
	routine.current_cancellation_token_id = 1
	routine.phase = .Ready
	create_result := fiber_raw.create(
		&routine.fiber,
		update_routine_entry,
		routine,
		uint(instance.runtime.stack_options.update_handler),
	)
	if create_result != .Success {
		return nil, fiber_result_error("could not create Workflow Update routine", create_result)
	}
	resume_error := resume_update_routine(routine)
	if resume_error.message != nil {
		if routine.fiber != nil {
			_ = fiber_raw.destroy(routine.fiber)
			routine.fiber = nil
		}
		return nil, resume_error
	}
	append(&instance.update_routines, routine)
	return routine, {}
}

append_finished_update_routine_response :: proc(
	routine: ^Fiber_Update_Routine,
	responses: ^[dynamic]Pending_Update_Response,
) -> Error {
	if routine == nil || routine.response_emitted || routine.fiber == nil ||
	   fiber_raw.status(routine.fiber) != .Dead {
		return {}
	}
	kind := Update_Response_Kind.Completed
	if routine.result.status == .Failed || routine.phase == .Failed {
		kind = .Rejected
	}
	failure_proto: []u8
	if kind == .Rejected && routine.unhandled_activity_seq != 0 {
		operation, found := find_activity_operation(
			routine.instance,
			routine.unhandled_activity_seq,
		)
		if found {
			failure_proto = operation.failure_proto
		}
	} else if kind == .Rejected && routine.unhandled_nexus_operation_seq != 0 {
		operation, found := find_nexus_operation(
			routine.instance,
			routine.unhandled_nexus_operation_seq,
		)
		if found {
			failure_proto = operation.failure_proto
		}
	} else if kind == .Rejected && routine.unhandled_child_workflow_seq != 0 {
		operation, found := find_child_workflow_operation(
			routine.instance,
			routine.unhandled_child_workflow_seq,
		)
		if found {
			failure_proto = operation.failure_proto
		}
	}
	append_pending_update_response(
		responses,
		routine.protocol_instance_id,
		kind,
		routine.result,
		failure_proto,
	)
	routine.result = {}
	routine.response_emitted = true
	destroy_result := fiber_raw.destroy(routine.fiber)
	if destroy_result != .Success {
		return fiber_result_error("could not destroy completed Workflow Update routine", destroy_result)
	}
	routine.fiber = nil
	return {}
}

append_finished_update_routine_responses :: proc(
	instance: ^Fiber_Instance,
	responses: ^[dynamic]Pending_Update_Response,
) -> Error {
	for routine in instance.update_routines {
		append_error := append_finished_update_routine_response(routine, responses)
		if append_error.message != nil {
			return append_error
		}
	}
	return {}
}

update_routine_should_resume :: proc(routine: ^Fiber_Update_Routine) -> bool {
	if routine == nil || routine.instance == nil || routine.fiber == nil ||
	   fiber_raw.status(routine.fiber) != .Suspended {
		return false
	}
	instance := routine.instance
	cancellation_applies := cancellation_token_is_requested_id(
		instance,
		routine.current_cancellation_token_id,
	)
	switch routine.phase {
	case .Waiting_Condition:
		return condition_should_resume(&routine.ctx)
	case .Waiting_Mutex:
		return routine.ctx.mutex_granted || cancellation_applies
	case .Waiting_Join:
		return join_should_resume(&routine.ctx)
	case .Waiting_Selector:
		return selector_should_resume(&routine.ctx)
	case .Waiting_Activity:
		operation, found := find_activity_operation(instance, routine.waiting_activity_seq)
		return found && activity_operation_ready_for_waiter(
			instance,
			operation,
			routine.current_cancellation_token_id,
		)
	case .Waiting_Nexus_Operation:
		operation, found := find_nexus_operation(instance, routine.waiting_nexus_operation_seq)
		return found && nexus_operation_ready_for_waiter(
			instance,
			operation,
			routine.current_cancellation_token_id,
		)
	case .Waiting_Child_Workflow_Start:
		operation, found := find_child_workflow_operation(instance, routine.waiting_child_workflow_seq)
		return found && operation.start_state != .Pending
	case .Waiting_Child_Workflow:
		operation, found := find_child_workflow_operation(instance, routine.waiting_child_workflow_seq)
		return found && operation.completion_state != .Pending
	case .Waiting_External_Signal:
		operation, found := find_external_signal_operation(instance, routine.waiting_external_signal_seq)
		return found && (operation.state == .Resolved || operation.state == .Failed ||
			operation.state == .Cancel_Requested || operation.state == .Canceled)
	case .Waiting_External_Cancel:
		operation, found := find_external_cancel_operation(instance, routine.waiting_external_cancel_seq)
		return found && (operation.state == .Resolved || operation.state == .Failed)
	case .Waiting_Timer:
		operation, found := find_timer_operation(instance, routine.waiting_timer_seq)
		return found && (operation.state == .Resolved || operation.state == .Cancel_Requested ||
			operation.state == .Canceled)
	case .Waiting_Signal:
		_, found := find_buffered_signal(instance, routine.waiting_signal_name)
		return found || cancellation_applies
	case .Waiting_Signal_Or_Timer:
		_, signal_found := find_buffered_signal(instance, routine.waiting_signal_name)
		timer, timer_found := find_timer_operation(instance, routine.waiting_timer_seq)
		return signal_found || cancellation_applies ||
			(timer_found && (timer.state == .Resolved || timer.state == .Cancel_Requested ||
			timer.state == .Canceled))
	case .Waiting_Handlers, .Ready, .Continued_As_New, .Canceled, .Completed, .Failed:
	}
	return false
}

resume_ready_update_routines :: proc(instance: ^Fiber_Instance) -> (resumed: bool, error: Error) {
	for routine in instance.update_routines {
		if !update_routine_should_resume(routine) {
			continue
		}
		resumed = true
		routine.phase = .Ready
		routine.waiting_activity_seq = 0
		routine.waiting_nexus_operation_seq = 0
		routine.waiting_child_workflow_seq = 0
		routine.waiting_external_signal_seq = 0
		routine.waiting_external_cancel_seq = 0
		routine.waiting_timer_seq = 0
		routine.waiting_signal_name = ""
		resume_error := resume_update_routine(routine)
		if resume_error.message != nil {
			return resumed, resume_error
		}
	}
	return resumed, {}
}

resume_main_workflow_if_ready :: proc(
	instance: ^Fiber_Instance,
) -> (resumed: bool, error: Error) {
	if instance == nil || instance.fiber == nil ||
	   fiber_raw.status(instance.fiber) != .Suspended {
		return false, {}
	}
	should_resume := false
	switch instance.phase {
	case .Waiting_Condition:
		should_resume = condition_should_resume(&instance.workflow_context)
	case .Waiting_Mutex:
		should_resume = instance.workflow_context.mutex_granted ||
		                current_cancellation_applies(instance)
	case .Waiting_Join:
		should_resume = join_should_resume(&instance.workflow_context)
	case .Waiting_Selector:
		should_resume = selector_should_resume(&instance.workflow_context)
	case .Waiting_Activity:
		operation, found := find_activity_operation(instance, instance.waiting_activity_seq)
		should_resume = found && activity_operation_ready_for_waiter(
			instance,
			operation,
			current_cancellation_token_id(instance),
		)
	case .Waiting_Nexus_Operation:
		operation, found := find_nexus_operation(instance, instance.waiting_nexus_operation_seq)
		should_resume = found && nexus_operation_ready_for_waiter(
			instance,
			operation,
			current_cancellation_token_id(instance),
		)
	case .Waiting_Child_Workflow_Start:
		operation, found := find_child_workflow_operation(instance, instance.waiting_child_workflow_seq)
		should_resume = found && operation.start_state != .Pending
	case .Waiting_Child_Workflow:
		operation, found := find_child_workflow_operation(instance, instance.waiting_child_workflow_seq)
		should_resume = found && operation.completion_state != .Pending
	case .Waiting_External_Signal:
		operation, found := find_external_signal_operation(instance, instance.waiting_external_signal_seq)
		should_resume = found && (operation.state == .Resolved || operation.state == .Failed ||
			operation.state == .Cancel_Requested || operation.state == .Canceled)
	case .Waiting_External_Cancel:
		operation, found := find_external_cancel_operation(instance, instance.waiting_external_cancel_seq)
		should_resume = found && (operation.state == .Resolved || operation.state == .Failed)
	case .Waiting_Timer:
		operation, found := find_timer_operation(instance, instance.waiting_timer_seq)
		should_resume = found && (operation.state == .Resolved ||
			operation.state == .Cancel_Requested || operation.state == .Canceled)
	case .Waiting_Signal:
		_, found := find_buffered_signal(instance, instance.waiting_signal_name)
		should_resume = found || current_cancellation_applies(instance)
	case .Waiting_Signal_Or_Timer:
		_, signal_found := find_buffered_signal(instance, instance.waiting_signal_name)
		timer, timer_found := find_timer_operation(instance, instance.waiting_timer_seq)
		should_resume = signal_found || current_cancellation_applies(instance) ||
			(timer_found && (timer.state == .Resolved || timer.state == .Cancel_Requested ||
			timer.state == .Canceled))
	case .Waiting_Handlers:
		should_resume = !has_active_handlers(instance) || current_cancellation_applies(instance)
	case .Ready, .Continued_As_New, .Canceled, .Completed, .Failed:
	}
	if !should_resume {
		return false, {}
	}
	instance.phase = .Ready
	instance.waiting_activity_seq = 0
	instance.waiting_nexus_operation_seq = 0
	instance.waiting_child_workflow_seq = 0
	instance.waiting_external_signal_seq = 0
	instance.waiting_external_cancel_seq = 0
	instance.waiting_timer_seq = 0
	instance.waiting_signal_name = ""
	resume_result := fiber_raw.resume(instance.fiber)
	if resume_result != .Success {
		return true, fiber_result_error(
			"could not resume Workflow fiber after applying activation jobs",
			resume_result,
		)
	}
	return true, {}
}

clone_update_context :: proc(
	instance: ^Fiber_Instance,
	update: ^proto.Coresdk_Workflow_Activation_DoUpdate,
) -> (result: Update_Context, ok: bool) {
	if instance == nil || update == nil {
		return {}, false
	}
	allocator := instance.odin_context.allocator
	id, id_error := strings.clone(update.id, allocator)
	name, name_error := strings.clone(update.name, allocator)
	identity := ""
	if id_error != nil || name_error != nil {
		return {}, false
	}
	if update.meta != nil {
		owned_identity, identity_error := strings.clone(update.meta.identity, allocator)
		if identity_error != nil {
			return {}, false
		}
		identity = owned_identity
	}
	result = {
		id = id,
		name = name,
		identity = identity,
		headers = make([dynamic]Fiber_Named_Payload, allocator),
	}
	for &entry in update.headers {
		if len(entry.key) == 0 || entry.value == nil {
			return {}, false
		}
		key, key_error := strings.clone(entry.key, allocator)
		payload, payload_error := proto.clone_payload_view(
			proto.payload_view(entry.value),
			allocator,
		)
		if key_error != nil || payload_error != .None {
			return {}, false
		}
		append(&result.headers, Fiber_Named_Payload{key = key, payload = payload})
	}
	return result, true
}

invoke_workflow_update :: proc(
	instance: ^Fiber_Instance,
	update: ^proto.Coresdk_Workflow_Activation_DoUpdate,
	responses: ^[dynamic]Pending_Update_Response,
) {
	protocol_instance_id := update.protocol_instance_id
	if len(update.name) == 0 {
		append_pending_update_response(
			responses,
			protocol_instance_id,
			.Rejected,
			payload_fail(
				"Workflow Update requires a name",
				"OdinWorkflowUpdateArgumentError",
			),
		)
		return
	}
	handler, found := find_update_handler(instance, update.name)
	if !found {
		message := fmt.aprintf("Workflow Update handler %s is not registered", update.name)
		append_pending_update_response(
			responses,
			protocol_instance_id,
			.Rejected,
			payload_fail(message, "OdinWorkflowUpdateNotFound"),
		)
		return
	}
	inputs := make([dynamic]codec.Payload)
	defer {
		for &input in inputs {
			codec.payload_free(&input)
		}
		delete(inputs)
	}
	for &source in update.input {
		input, clone_error := proto.clone_payload(&source)
		if clone_error != .None {
			append_pending_update_response(
				responses,
				protocol_instance_id,
				.Rejected,
				payload_fail(
					"could not preserve Workflow Update inputs",
					"OdinWorkflowUpdatePayloadConversionError",
				),
			)
			return
		}
		append(&inputs, input)
	}
	input_views := make([]codec.Payload_View, len(inputs))
	defer delete(input_views)
	for &input, index in inputs {
		input_views[index] = codec.payload_view(&input)
	}
	update_context, context_ok := clone_update_context(instance, update)
	if !context_ok {
		append_pending_update_response(
			responses,
			protocol_instance_id,
			.Rejected,
			payload_fail(
				"could not preserve Workflow Update context",
				"OdinWorkflowUpdateContextError",
			),
		)
		return
	}

	if update.run_validator &&
	   (handler.validate != nil || handler.validate_with_context != nil ||
	    handler.validate_payloads != nil || handler.validate_payloads_with_context != nil) {
		command_state := workflow_command_state(instance)
		interceptor_input := Inbound_Handle_Update_Input{
			workflow_type = instance.workflow_type,
			update = &update_context,
			inputs = input_views,
		}
		interceptor_state := Inbound_Handle_Update_Validator_State{
			instance = instance,
			handler = handler,
		}
		validation := inbound_handle_update_validator_chain(
			&interceptor_state,
			&interceptor_input,
		)
		if workflow_command_state_changed(instance, command_state) {
			restore_workflow_command_state(instance, command_state)
			append_pending_update_response(
				responses,
				protocol_instance_id,
				.Rejected,
				payload_fail(
					"Workflow Update validator attempted to mutate runtime command state",
					"OdinWorkflowUpdateValidatorMutationError",
				),
			)
			return
		}
		if !validation.accepted {
			failure_type := validation.failure_type
			if len(failure_type) == 0 {
				failure_type = "OdinWorkflowUpdateRejected"
			}
			append_pending_update_response(
				responses,
				protocol_instance_id,
				.Rejected,
				payload_fail(validation.message, failure_type),
			)
			return
		}
	}

	append_pending_update_response(responses, protocol_instance_id, .Accepted)
	if handler.invoke_routine != nil || handler.invoke_routine_with_context != nil ||
	   handler.invoke_payloads_routine != nil || handler.invoke_payloads_routine_with_context != nil {
		routine, routine_error := start_update_routine(
			instance,
			handler,
			protocol_instance_id,
			&update_context,
			input_views,
		)
		if routine_error.message != nil {
			failure_message := fmt.aprintf("%s", string(routine_error.message))
			append_pending_update_response(
				responses,
				protocol_instance_id,
				.Rejected,
				payload_fail(
					failure_message,
					"OdinWorkflowUpdateRoutineError",
				),
			)
			error_free(&routine_error)
			return
		}
		append_error := append_finished_update_routine_response(routine, responses)
		if append_error.message != nil {
			failure_message := fmt.aprintf("%s", string(append_error.message))
			append_pending_update_response(
				responses,
				protocol_instance_id,
				.Rejected,
				payload_fail(
					failure_message,
					"OdinWorkflowUpdateRoutineError",
				),
			)
			error_free(&append_error)
		}
		return
	}
	command_state := workflow_command_state(instance)
	interceptor_input := Inbound_Handle_Update_Input{
		workflow_type = instance.workflow_type,
		update = &update_context,
		inputs = input_views,
	}
	interceptor_state := Inbound_Handle_Update_Handler_State{
		instance = instance,
		handler = handler,
	}
	result := inbound_handle_update_handler_chain(&interceptor_state, &interceptor_input)
	if workflow_command_state_changed(instance, command_state) {
		codec.payload_free(&result.payload)
		restore_workflow_command_state(instance, command_state)
		result = payload_fail(
			"synchronous Workflow Update handler attempted to mutate runtime command state",
			"OdinWorkflowUpdateMutationError",
		)
	}
	if result.status == .Completed {
		append_pending_update_response(responses, protocol_instance_id, .Completed, result)
	} else {
		append_pending_update_response(responses, protocol_instance_id, .Rejected, result)
	}
}

append_update_response_command :: proc(
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	response: ^Pending_Update_Response,
) {
	update_response := new(proto.Coresdk_Workflow_Commands_UpdateResponse)
	update_response.protocol_instance_id = response.protocol_instance_id
	switch response.kind {
	case .Accepted:
		accepted := new(proto.Google_Protobuf_Empty)
		update_response.response = proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Accepted{
			value = accepted,
		}
	case .Completed:
		payload := new(proto.Temporal_Api_Common_V1_Payload)
		payload^ = proto.encode_payload(codec.payload_view(&response.result.payload))
		update_response.response = proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Completed{
			value = payload,
		}
	case .Rejected:
		failure := new(proto.Temporal_Api_Failure_V1_Failure)
		restored := len(response.failure_proto) > 0 &&
		            proto.decode_Temporal_Api_Failure_V1_Failure(response.failure_proto, failure)
		if !restored {
			failure_type := response.result.failure_type
			if len(failure_type) == 0 {
				failure_type = "OdinWorkflowUpdateRejected"
			}
			application_info := new(proto.Temporal_Api_Failure_V1_ApplicationFailureInfo)
			application_info.type = failure_type
			application_info.non_retryable = true
			failure.message = response.result.failure_message
			failure.source = "OdinSDK"
			failure.failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
				value = application_info,
			}
		}
		update_response.response = proto.Coresdk_Workflow_Commands_UpdateResponse_Response_Rejected{
			value = failure,
		}
	}
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Update_Response{
				value = update_response,
			},
		},
	)
}

append_shared_operation_commands :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
) {
	for index in 0..<len(instance.activities) {
		operation := &instance.activities[index]
		if operation.state == .Scheduled {
			append_fiber_schedule_command(result, commands, operation, task_queue)
		}
	}
	for index in 0..<len(instance.nexus_operations) {
		operation := &instance.nexus_operations[index]
		if operation.state == .Scheduled {
			append_fiber_schedule_nexus_operation_command(result, commands, operation)
		}
	}
	for index in 0..<len(instance.timers) {
		operation := &instance.timers[index]
		if operation.state == .Scheduled {
			append_fiber_timer_command(result, commands, operation)
		}
	}
	for index in 0..<len(instance.child_workflows) {
		operation := &instance.child_workflows[index]
		if operation.command_state == .Scheduled {
			append_fiber_start_child_workflow_command(result, commands, operation, task_queue)
		}
	}
	for index in 0..<len(instance.external_signals) {
		operation := &instance.external_signals[index]
		if operation.state == .Scheduled {
			append_fiber_signal_external_workflow_command(result, commands, operation)
		}
	}
	for index in 0..<len(instance.external_cancels) {
		operation := &instance.external_cancels[index]
		if operation.state == .Scheduled {
			append_fiber_request_cancel_external_workflow_command(result, commands, operation)
		}
	}
	append_fiber_activity_cancellations(result, commands, instance)
	append_fiber_nexus_operation_cancellations(result, commands, instance)
	append_fiber_timer_cancellations(result, commands, instance)
	append_fiber_child_workflow_cancellations(result, commands, instance)
	append_fiber_external_signal_cancellations(result, commands, instance)
}

append_fiber_property_commands :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.property_commands) {
		property := &instance.property_commands[index]
		if property.emitted {
			continue
		}
		payload := new(proto.Temporal_Api_Common_V1_Payload)
		payload^ = proto.encode_payload(codec.payload_view(&property.payload))
		switch property.kind {
		case .Memo:
			memo := new(proto.Temporal_Api_Common_V1_Memo)
			append(
				&memo.fields,
				proto.Temporal_Api_Common_V1_Memo_FieldsEntry{
					key = property.key,
					value = payload,
				},
			)
			modify := new(proto.Coresdk_Workflow_Commands_ModifyWorkflowProperties)
			modify.upserted_memo = memo
			append(
				commands,
				proto.Coresdk_Workflow_Commands_WorkflowCommand{
					variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Modify_Workflow_Properties{
						value = modify,
					},
				},
			)
			result.memo_upserts += 1
		case .Search_Attribute:
			attributes := new(proto.Temporal_Api_Common_V1_SearchAttributes)
			append(
				&attributes.indexed_fields,
				proto.Temporal_Api_Common_V1_SearchAttributes_IndexedFieldsEntry{
					key = property.key,
					value = payload,
				},
			)
			upsert := new(proto.Coresdk_Workflow_Commands_UpsertWorkflowSearchAttributes)
			upsert.search_attributes = attributes
			append(
				commands,
				proto.Coresdk_Workflow_Commands_WorkflowCommand{
					variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Upsert_Workflow_Search_Attributes{
						value = upsert,
					},
				},
			)
			result.search_attribute_upserts += 1
		}
		property.emitted = true
	}
}

append_fiber_patch_commands :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) {
	for index in 0..<len(instance.patches) {
		decision := &instance.patches[index]
		if !decision.emit_command || decision.command_sent {
			continue
		}
		marker := new(proto.Coresdk_Workflow_Commands_SetPatchMarker)
		marker.patch_id = decision.patch_id
		marker.deprecated = decision.deprecated
		append(
			commands,
			proto.Coresdk_Workflow_Commands_WorkflowCommand{
				variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Set_Patch_Marker{
					value = marker,
				},
			},
		)
		decision.command_sent = true
		result.patch_markers_emitted += 1
		if decision.deprecated {
			result.deprecated_patch_markers_emitted += 1
		}
	}
}

append_fiber_pending_commands :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
) -> Error {
	append_shared_operation_commands(result, commands, instance, task_queue)
	switch instance.phase {
	case .Waiting_Condition:
		result.waiting_condition = true
	case .Waiting_Mutex:
	case .Waiting_Join:
		result.waiting_join = true
	case .Waiting_Selector:
		result.waiting_selector = true
	case .Waiting_Activity:
		result.waiting_activity_seq = instance.waiting_activity_seq
	case .Waiting_Nexus_Operation:
		result.waiting_nexus_operation_seq = instance.waiting_nexus_operation_seq
	case .Waiting_Child_Workflow_Start:
		result.waiting_child_workflow_start_seq = instance.waiting_child_workflow_seq
	case .Waiting_Child_Workflow:
		result.waiting_child_workflow_seq = instance.waiting_child_workflow_seq
	case .Waiting_External_Signal:
	case .Waiting_External_Cancel:
	case .Waiting_Timer:
	case .Waiting_Signal:
		result.waiting_signal = true
	case .Waiting_Signal_Or_Timer:
		result.waiting_signal = true
		result.waiting_timer_seq = instance.waiting_timer_seq
	case .Waiting_Handlers:
		result.waiting_handlers = true
	case .Ready, .Continued_As_New, .Canceled, .Completed, .Failed:
		return error_from_string("Workflow fiber suspended without a supported pending operation")
	}
	result.run_id = instance.run_id
	return {}
}

has_active_signal_routines :: proc(instance: ^Fiber_Instance) -> bool {
	for routine in instance.signal_routines {
		if !routine.completion_observed {
			return true
		}
	}
	return false
}

has_active_update_routines :: proc(instance: ^Fiber_Instance) -> bool {
	for routine in instance.update_routines {
		if !routine.response_emitted {
			return true
		}
	}
	return false
}

has_active_handlers :: proc(instance: ^Fiber_Instance) -> bool {
	return has_active_signal_routines(instance) || has_active_update_routines(instance)
}

observe_unfinished_signal_routines :: proc(
	result: ^Process_Result,
	instance: ^Fiber_Instance,
) {
	for routine in instance.signal_routines {
		if routine.completion_observed {
			continue
		}
		result.unfinished_signal_handlers += 1
		if routine.unfinished_policy == .Warn_And_Abandon {
			result.warned_unfinished_signal_handlers += 1
		}
	}
}

observe_unfinished_update_routines :: proc(
	result: ^Process_Result,
	instance: ^Fiber_Instance,
) {
	for routine in instance.update_routines {
		if routine.response_emitted {
			continue
		}
		result.unfinished_update_handlers += 1
		if routine.unfinished_policy == .Warn_And_Abandon {
			result.warned_unfinished_update_handlers += 1
		}
	}
}

commands_are_nonterminal_prefix :: proc(
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
) -> bool {
	for command in commands^ {
		allowed := false
		#partial switch variant in command.variant {
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Update_Response:
			allowed = variant.value != nil
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Set_Patch_Marker:
			allowed = variant.value != nil
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Modify_Workflow_Properties:
			allowed = variant.value != nil
		case proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Upsert_Workflow_Search_Attributes:
			allowed = variant.value != nil
		}
		if !allowed {
			return false
		}
	}
	return true
}

append_fiber_continue_as_new_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) -> Error {
	if !instance.continue_as_new.present {
		return error_from_string("Workflow requested Continue-As-New without preserved inputs")
	}
	if !commands_are_nonterminal_prefix(commands) {
		return error_from_string("fiber spike emitted Continue-As-New after another command")
	}
	append_fiber_timer_cancellations(result, commands, instance)
	operation := &instance.continue_as_new
	options := &operation.options
	request := new(proto.Coresdk_Workflow_Commands_ContinueAsNewWorkflowExecution)
	request.workflow_type = options.workflow_type
	request.task_queue = options.task_queue
	for &input in operation.inputs {
		append(&request.arguments, proto.encode_payload(codec.payload_view(&input)))
	}
	request.workflow_run_timeout = duration_to_proto(options.workflow_run_timeout)
	request.workflow_task_timeout = duration_to_proto(options.workflow_task_timeout)
	if options.has_retry_policy {
		request.retry_policy = retry_policy_to_proto(&options.retry_policy)
	}
	for &entry in options.memo {
		memo_payload := new(proto.Temporal_Api_Common_V1_Payload)
		memo_payload^ = proto.encode_payload(codec.payload_view(&entry.payload))
		append(
			&request.memo,
			proto.Coresdk_Workflow_Commands_ContinueAsNewWorkflowExecution_MemoEntry{
				key = entry.key,
				value = memo_payload,
			},
		)
	}
	request.backoff_start_interval = duration_to_proto(options.backoff_start_interval)
	for &entry in options.headers {
		header_payload := new(proto.Temporal_Api_Common_V1_Payload)
		header_payload^ = proto.encode_payload(codec.payload_view(&entry.payload))
		append(
			&request.headers,
			proto.Coresdk_Workflow_Commands_ContinueAsNewWorkflowExecution_HeadersEntry{
				key = entry.key,
				value = header_payload,
			},
		)
	}
	if options.has_search_attributes {
		request.search_attributes = new(proto.Temporal_Api_Common_V1_SearchAttributes)
		for &entry in options.search_attributes {
			attribute_payload := new(proto.Temporal_Api_Common_V1_Payload)
			attribute_payload^ = proto.encode_payload(codec.payload_view(&entry.payload))
			append(
				&request.search_attributes.indexed_fields,
				proto.Temporal_Api_Common_V1_SearchAttributes_IndexedFieldsEntry{
					key = entry.key,
					value = attribute_payload,
				},
			)
		}
	}
	request.versioning_intent = continue_as_new_versioning_intent_to_proto(
		options.versioning_intent,
	)
	request.initial_versioning_behavior = continue_as_new_versioning_behavior_to_proto(
		options.initial_versioning_behavior,
	)
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Continue_As_New_Workflow_Execution{
				value = request,
			},
		},
	)
	result.continued_as_new = true
	result.completed = true
	result.run_id = instance.run_id
	instance.terminal_command_sent = true
	return {}
}

append_fiber_completion_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) -> Error {
	if !commands_are_nonterminal_prefix(commands) {
		return error_from_string("fiber spike emitted completion after another command")
	}
	append_fiber_timer_cancellations(result, commands, instance)
	completion_payload := new(proto.Temporal_Api_Common_V1_Payload)
	completion_payload^ = proto.encode_payload(codec.payload_view(&instance.output))
	complete := new(proto.Coresdk_Workflow_Commands_CompleteWorkflowExecution)
	complete.result = completion_payload
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Complete_Workflow_Execution{
				value = complete,
			},
		},
	)
	result.completed = true
	result.run_id = instance.run_id
	instance.terminal_command_sent = true
	return {}
}

append_fiber_failure_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) -> Error {
	if !commands_are_nonterminal_prefix(commands) {
		return error_from_string("fiber spike emitted failure after another command")
	}
	failure := new(proto.Temporal_Api_Failure_V1_Failure)
	if instance.unhandled_activity_seq != 0 {
		operation, found := find_activity_operation(instance, instance.unhandled_activity_seq)
		if !found || operation.state != .Failed || len(operation.failure_proto) == 0 {
			return error_from_string("Workflow fiber failed without a preserved Activity failure")
		}
		if !proto.decode_Temporal_Api_Failure_V1_Failure(operation.failure_proto, failure) {
			return error_from_string("could not restore unhandled Activity failure")
		}
	} else if instance.unhandled_nexus_operation_seq != 0 {
		operation, found := find_nexus_operation(instance, instance.unhandled_nexus_operation_seq)
		if !found || (operation.state != .Failed && operation.state != .Timed_Out) ||
		   len(operation.failure_proto) == 0 {
			return error_from_string("Workflow fiber failed without a preserved Nexus operation failure")
		}
		if !proto.decode_Temporal_Api_Failure_V1_Failure(operation.failure_proto, failure) {
			return error_from_string("could not restore unhandled Nexus operation failure")
		}
	} else if instance.unhandled_child_workflow_seq != 0 {
		operation, found := find_child_workflow_operation(
			instance,
			instance.unhandled_child_workflow_seq,
		)
		if !found ||
		   (operation.completion_state != .Failed && operation.completion_state != .Canceled) ||
		   len(operation.failure_proto) == 0 {
			return error_from_string("Workflow fiber failed without a preserved Child Workflow failure")
		}
		if !proto.decode_Temporal_Api_Failure_V1_Failure(operation.failure_proto, failure) {
			return error_from_string("could not restore unhandled Child Workflow failure")
		}
	} else {
		if len(instance.failure_message) == 0 {
			return error_from_string("Workflow fiber failed without failure details")
		}
		failure_type := instance.failure_type
		if len(failure_type) == 0 {
			failure_type = "OdinWorkflowError"
		}
		application_info := new(proto.Temporal_Api_Failure_V1_ApplicationFailureInfo)
		application_info.type = failure_type
		application_info.non_retryable = true
		failure^ = proto.Temporal_Api_Failure_V1_Failure{
			message = instance.failure_message,
			source = "OdinSDK",
			failure_info = proto.Temporal_Api_Failure_V1_Failure_Failure_Info_Application_Failure_Info{
				value = application_info,
			},
		}
	}
	fail := new(proto.Coresdk_Workflow_Commands_FailWorkflowExecution)
	fail.failure = failure
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Fail_Workflow_Execution{
				value = fail,
			},
		},
	)
	result.completed = true
	result.run_id = instance.run_id
	instance.terminal_command_sent = true
	return {}
}

append_fiber_cancellation_commands :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
) -> Error {
	if !commands_are_nonterminal_prefix(commands) {
		return error_from_string("fiber spike emitted cancellation after another command")
	}
	append_fiber_activity_cancellations(result, commands, instance)
	append_fiber_nexus_operation_cancellations(result, commands, instance)
	append_fiber_timer_cancellations(result, commands, instance)
	append_fiber_child_workflow_cancellations(result, commands, instance)
	append_fiber_external_signal_cancellations(result, commands, instance)
	cancel := new(proto.Coresdk_Workflow_Commands_CancelWorkflowExecution)
	append(
		commands,
		proto.Coresdk_Workflow_Commands_WorkflowCommand{
			variant = proto.Coresdk_Workflow_Commands_WorkflowCommand_Variant_Cancel_Workflow_Execution{
				value = cancel,
			},
		},
	)
	result.canceled_workflow = true
	result.completed = true
	result.cancellation_reason = instance.cancellation_reason
	result.run_id = instance.run_id
	instance.terminal_command_sent = true
	return {}
}

append_fiber_progress_command :: proc(
	result: ^Process_Result,
	commands: ^[dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand,
	instance: ^Fiber_Instance,
	task_queue: string,
) -> Error {
	if instance.terminal_command_sent {
		result.run_id = instance.run_id
		return {}
	}
	append_fiber_patch_commands(result, commands, instance)
	append_fiber_property_commands(result, commands, instance)
	if instance.terminal_failure_requested {
		instance.failure_message = instance.terminal_failure_message
		instance.failure_type = instance.terminal_failure_type
		observe_unfinished_signal_routines(result, instance)
		observe_unfinished_update_routines(result, instance)
		return append_fiber_failure_command(result, commands, instance)
	}
	if instance.continue_as_new.present {
		observe_unfinished_signal_routines(result, instance)
		observe_unfinished_update_routines(result, instance)
		return append_fiber_continue_as_new_command(result, commands, instance)
	}
	if instance.phase == .Canceled {
		observe_unfinished_signal_routines(result, instance)
		observe_unfinished_update_routines(result, instance)
		return append_fiber_cancellation_commands(result, commands, instance)
	}
	main_status := fiber_raw.status(instance.fiber)
	if instance.phase == .Failed && main_status == .Suspended {
		observe_unfinished_signal_routines(result, instance)
		observe_unfinished_update_routines(result, instance)
		return append_fiber_failure_command(result, commands, instance)
	}
	switch main_status {
	case .Suspended:
		return append_fiber_pending_commands(result, commands, instance, task_queue)
	case .Dead:
		observe_unfinished_signal_routines(result, instance)
		observe_unfinished_update_routines(result, instance)
		if instance.phase == .Completed {
			return append_fiber_completion_command(result, commands, instance)
		}
		if instance.phase == .Failed {
			return append_fiber_failure_command(result, commands, instance)
		}
	case .Normal, .Running:
	}
	return error_from_string("Workflow fiber reached an invalid state after resolution")
}

apply_activation_patch_notifications :: proc(
	instance: ^Fiber_Instance,
	activation: ^proto.Coresdk_Workflow_Activation_WorkflowActivation,
) -> Error {
	if instance == nil || activation == nil {
		return error_from_string("Workflow patch notification has no active fiber")
	}
	for job in activation.jobs {
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Notify_Has_Patch:
			if variant.value == nil || len(variant.value.patch_id) == 0 {
				return error_from_string("Workflow patch notification has no patch ID")
			}
			if !notify_patch(instance, variant.value.patch_id) {
				return error_from_string("could not preserve Workflow patch notification")
			}
		}
	}
	return {}
}

apply_current_deployment_version :: proc(
	instance: ^Fiber_Instance,
	source: ^proto.Coresdk_Common_WorkerDeploymentVersion,
) -> bool {
	if instance == nil {
		return false
	}
	if source == nil {
		instance.has_current_deployment_version = false
		return true
	}
	if instance.has_current_deployment_version &&
	   instance.current_deployment_version.deployment_name == source.deployment_name &&
	   instance.current_deployment_version.build_id == source.build_id {
		return true
	}
	deployment_name, deployment_name_error := strings.clone(
		source.deployment_name,
		instance.odin_context.allocator,
	)
	build_id, build_id_error := strings.clone(
		source.build_id,
		instance.odin_context.allocator,
	)
	if deployment_name_error != nil || build_id_error != nil {
		return false
	}
	instance.current_deployment_version = {
		deployment_name = deployment_name,
		build_id = build_id,
	}
	instance.has_current_deployment_version = true
	return true
}

activation_replays_history_events :: proc(
	activation: ^proto.Coresdk_Workflow_Activation_WorkflowActivation,
) -> bool {
	if activation == nil || !activation.is_replaying {
		return false
	}
	for job in activation.jobs {
		is_query := false
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow:
			is_query = true
		}
		if !is_query {
			return true
		}
	}
	return false
}

apply_activation_metadata :: proc(
	instance: ^Fiber_Instance,
	activation: ^proto.Coresdk_Workflow_Activation_WorkflowActivation,
) -> Error {
	if instance == nil || activation == nil {
		return error_from_string("Workflow activation metadata has no active fiber")
	}
	instance.activation_is_replaying = activation.is_replaying
	instance.activation_is_replaying_history_events = activation_replays_history_events(activation)
	instance.activation_history_length = activation.history_length
	instance.activation_history_size_bytes = activation.history_size_bytes
	instance.activation_continue_as_new_suggested = activation.continue_as_new_suggested
	instance.activation_target_worker_deployment_version_changed =
		activation.target_worker_deployment_version_changed
	if !apply_current_deployment_version(instance, activation.deployment_version_for_current_task) {
		return error_from_string("could not preserve Workflow deployment version")
	}
	instance.has_activation_time = activation.timestamp != nil
	if activation.timestamp != nil {
		if activation.timestamp.nanos < 0 || activation.timestamp.nanos >= 1_000_000_000 {
			return error_from_string("Workflow activation timestamp is invalid")
		}
		instance.activation_time = time.unix(
			activation.timestamp.seconds,
			i64(activation.timestamp.nanos),
		)
	} else {
		instance.activation_time = {}
	}
	seed_updated := false
	for job in activation.jobs {
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Update_Random_Seed:
			if variant.value == nil {
				return error_from_string("Workflow random-seed update has no seed")
			}
			if !seed_updated {
				instance.random_state = variant.value.randomness_seed
				instance.random_initialized = true
				seed_updated = true
			}
		}
	}
	return {}
}

process_fiber_activation :: proc(runtime: ^Fiber_Runtime, data: []u8) -> (result: Process_Result) {
	if runtime == nil || runtime.invoke == nil || runtime.callback_data == nil {
		result.error = error_from_string("Workflow fiber runtime is not initialized")
		return
	}

	decode_arena: virtual.Arena
	arena_error := virtual.arena_init_growing(&decode_arena)
	if arena_error != nil {
		result.error = error_from_string("could not initialize Workflow decode arena")
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{}
	decode_ok := proto.decode_Coresdk_Workflow_Activation_WorkflowActivation(data, &activation)
	context.allocator = previous_allocator
	if !decode_ok || len(activation.run_id) == 0 {
		result.error = error_from_string("Core returned a malformed WorkflowActivation protobuf")
		return
	}

	runtime.activations += 1
	result.is_replaying = activation.is_replaying
	result.history_length = activation.history_length
	if activation.is_replaying {
		runtime.replay_activations += 1
	}

	context.allocator = virtual.arena_allocator(&decode_arena)
	pending_query_responses := make([dynamic]Pending_Query_Response)
	pending_update_responses := make([dynamic]Pending_Update_Response)
	context.allocator = previous_allocator
	defer {
		for &query_response in pending_query_responses {
			codec.payload_free(&query_response.result.payload)
		}
		for &update_response in pending_update_responses {
			codec.payload_free(&update_response.result.payload)
		}
	}
	instance, instance_found := find_fiber_instance(runtime, activation.run_id)
	defer {
		active_instance, active_found := find_fiber_instance(runtime, activation.run_id)
		if active_found {
			active_instance.activation_result = nil
		}
	}
	if instance_found {
		instance.activation_result = &result
		instance.activation_error_message = ""
		metadata_error := apply_activation_metadata(instance, &activation)
		if metadata_error.message != nil {
			message := string(metadata_error.message)
			error_free(&metadata_error)
			process_error(&result, activation.run_id, message)
			return
		}
		notification_error := apply_activation_patch_notifications(instance, &activation)
		if notification_error.message != nil {
			message := string(notification_error.message)
			error_free(&notification_error)
			process_error(&result, activation.run_id, message)
			return
		}
	}
	for job in activation.jobs {
		handled := false
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow:
			handled = true
			if variant.value == nil || variant.value.workflow_type != runtime.workflow_type || instance_found {
				process_error(&result, activation.run_id, "invalid Workflow fiber initialization")
				return
			}
			add_error: Error
			instance, add_error = add_fiber_instance(runtime, activation.run_id, variant.value)
			if add_error.message != nil {
				result.error = add_error
				result.completion = encode_activation_failure(activation.run_id, string(add_error.message))
				result.activation_failed = true
				return
			}
			instance_found = true
			instance.activation_result = &result
			instance.activation_error_message = ""
			instance.random_state = variant.value.randomness_seed
			instance.random_initialized = true
			metadata_error := apply_activation_metadata(instance, &activation)
			if metadata_error.message != nil {
				message := string(metadata_error.message)
				error_free(&metadata_error)
				process_error(&result, activation.run_id, message)
				return
			}
			notification_error := apply_activation_patch_notifications(instance, &activation)
			if notification_error.message != nil {
				message := string(notification_error.message)
				error_free(&notification_error)
				process_error(&result, activation.run_id, message)
				return
			}
			if activation.is_replaying {
				runtime.replay_initializations += 1
			}
			resume_result := fiber_raw.resume(instance.fiber)
			if resume_result != .Success {
				process_error(&result, activation.run_id, "could not start Workflow fiber")
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Notify_Has_Patch:
			handled = true
			if variant.value == nil || len(variant.value.patch_id) == 0 || !instance_found {
				process_error(&result, activation.run_id, "Workflow patch notification has no active fiber or patch ID")
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Update_Random_Seed:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Workflow random-seed update has no active fiber or seed")
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Activity:
			handled = true
			if variant.value == nil || variant.value.result == nil || !instance_found {
				process_error(&result, activation.run_id, "Activity resolution has no Workflow fiber")
				return
			}
			operation, operation_found := find_activity_operation_by_command_seq(
				instance,
				variant.value.seq,
			)
			if !operation_found {
				process_error(&result, activation.run_id, "Activity resolution does not match a pending Workflow operation")
				return
			}
			if operation.is_local != variant.value.is_local {
				process_error(&result, activation.run_id, "Activity resolution local/remote kind does not match its command")
				return
			}
			if operation.state == .Canceled {
				// Core may report the terminal resolution of a requested Activity
				// while detached cleanup is running or before cache eviction.
				result.resolved_activity_seq = operation.command_seq
				continue
			}
			if operation.state != .Commanded && operation.state != .Cancel_Requested {
				process_error(&result, activation.run_id, "Activity resolution does not match a pending Workflow operation")
				return
			}
			resolution_handled := false
			#partial switch status in variant.value.result.status {
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Completed:
				resolution_handled = true
				if status.value == nil || status.value.result == nil {
					process_error(&result, activation.run_id, "completed Activity has no result")
					return
				}
				context.allocator = instance.odin_context.allocator
				owned_result, clone_error := proto.clone_payload(status.value.result)
				context.allocator = previous_allocator
				if clone_error != .None {
					process_error(&result, activation.run_id, "could not preserve Activity result Payload")
					return
				}
				operation.result = owned_result
				operation.state = .Resolved
				result.resolved_activity_seq = operation.command_seq
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Failed:
				resolution_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "failed Activity has no failure")
					return
				}
				context.allocator = instance.odin_context.allocator
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
				failure_preserved := preserve_activity_failure(operation, failure_proto[:])
				context.allocator = previous_allocator
				if len(failure_proto) == 0 || !failure_preserved {
					process_error(&result, activation.run_id, "could not preserve Activity failure")
					return
				}
				operation.failure_message = operation.failure.message
				operation.failure_proto = failure_proto[:]
				operation.state = .Failed
				result.resolved_activity_seq = operation.command_seq
				result.resolved_activity_failed = true
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Cancelled:
				resolution_handled = true
				if status.value == nil {
					process_error(&result, activation.run_id, "canceled Activity has no result")
					return
				}
				if status.value.failure != nil {
					context.allocator = instance.odin_context.allocator
					failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
					failure_preserved := preserve_activity_failure(operation, failure_proto[:])
					context.allocator = previous_allocator
					if len(failure_proto) == 0 || !failure_preserved {
						process_error(&result, activation.run_id, "could not preserve Activity cancellation failure")
						return
					}
					operation.failure_message = operation.failure.message
					operation.failure_proto = failure_proto[:]
				}
				operation.state = .Canceled
				result.resolved_activity_seq = operation.command_seq
			case proto.Coresdk_Activity_Result_ActivityResolution_Status_Backoff:
				resolution_handled = true
				if operation.state == .Cancel_Requested {
					// Cancellation won the race with Core's backoff resolution. There
					// is no longer a running local attempt to cancel and no retry timer
					// should be created; resume the waiter as canceled.
					operation.state = .Canceled
					result.resolved_activity_seq = operation.command_seq
					continue
				}
				if !operation.is_local || operation.state != .Commanded ||
				   operation.backoff_timer_seq != 0 || status.value == nil ||
				   status.value.attempt <= operation.attempt ||
				   status.value.original_schedule_time == nil {
					process_error(&result, activation.run_id, "Local Activity backoff is invalid")
					return
				}
				backoff_duration, duration_ok := positive_duration_from_proto(
					status.value.backoff_duration,
				)
				timestamp := status.value.original_schedule_time
				if !duration_ok || timestamp.nanos < 0 || timestamp.nanos >= 1_000_000_000 {
					process_error(&result, activation.run_id, "Local Activity backoff metadata is invalid")
					return
				}
				instance.next_timer_seq += 1
				operation.attempt = status.value.attempt
				operation.backoff_timer_seq = instance.next_timer_seq
				operation.has_original_schedule_time = true
				operation.original_schedule_seconds = timestamp.seconds
				operation.original_schedule_nanos = timestamp.nanos
				append(
					&instance.timers,
					Fiber_Timer_Operation{
						seq = instance.next_timer_seq,
						duration = backoff_duration,
						state = .Scheduled,
						detached_from_root_cancellation = operation.detached_from_root_cancellation,
						cancellation_token_id = operation.cancellation_token_id,
					},
				)
				result.resolved_activity_seq = operation.command_seq
			}
			if !resolution_handled {
				process_error(&result, activation.run_id, "Activity result is unsupported by the Workflow fiber spike")
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Nexus_Operation_Start:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Nexus operation start resolution has no Workflow fiber")
				return
			}
			context.allocator = instance.odin_context.allocator
			resolution_error := apply_nexus_operation_start_resolution(instance, variant.value, &result)
			context.allocator = previous_allocator
			if len(resolution_error) > 0 {
				process_error(&result, activation.run_id, resolution_error)
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Nexus_Operation:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Nexus operation resolution has no Workflow fiber")
				return
			}
			context.allocator = instance.odin_context.allocator
			resolution_error := apply_nexus_operation_resolution(instance, variant.value, &result)
			context.allocator = previous_allocator
			if len(resolution_error) > 0 {
				process_error(&result, activation.run_id, resolution_error)
				return
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution_Start:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Child Workflow start resolution has no Workflow fiber")
				return
			}
			operation, operation_found := find_child_workflow_operation(instance, variant.value.seq)
			if !operation_found ||
			   (operation.command_state != .Commanded && operation.command_state != .Cancel_Requested && operation.command_state != .Canceled) {
				process_error(&result, activation.run_id, "Child Workflow start resolution does not match a pending operation")
				return
			}
			if operation.start_state != .Pending {
				result.resolved_child_workflow_start_seq = operation.seq
				continue
			}
			resolution_handled := false
			#partial switch status in variant.value.status {
			case proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Succeeded:
				resolution_handled = true
				if status.value == nil || len(status.value.run_id) == 0 {
					process_error(&result, activation.run_id, "started Child Workflow has no Run ID")
					return
				}
				context.allocator = instance.odin_context.allocator
				operation.run_id, _ = strings.clone(status.value.run_id)
				context.allocator = previous_allocator
				if len(operation.run_id) == 0 {
					process_error(&result, activation.run_id, "could not preserve Child Workflow Run ID")
					return
				}
				operation.start_state = .Succeeded
			case proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Failed:
				resolution_handled = true
				if status.value == nil {
					process_error(&result, activation.run_id, "failed Child Workflow start has no result")
					return
				}
				operation.start_state = .Failed
				operation.failure_message = "Child Workflow failed to start"
				operation.start_failure_cause = child_workflow_start_failure_cause_from_proto(
					status.value.cause,
				)
			case proto.Coresdk_Workflow_Activation_ResolveChildWorkflowExecutionStart_Status_Cancelled:
				resolution_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "canceled Child Workflow start has no failure")
					return
				}
				context.allocator = instance.odin_context.allocator
				operation.failure_message, _ = strings.clone(status.value.failure.message)
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
				operation.failure_proto = failure_proto[:]
				context.allocator = previous_allocator
				operation.start_state = .Canceled
			}
			if !resolution_handled {
				process_error(&result, activation.run_id, "Child Workflow start result is unsupported")
				return
			}
			result.resolved_child_workflow_start_seq = operation.seq

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Child_Workflow_Execution:
			handled = true
			if variant.value == nil || variant.value.result == nil || !instance_found {
				process_error(&result, activation.run_id, "Child Workflow completion has no Workflow fiber")
				return
			}
			operation, operation_found := find_child_workflow_operation(instance, variant.value.seq)
			if !operation_found ||
			   (operation.command_state != .Commanded && operation.command_state != .Cancel_Requested && operation.command_state != .Canceled) {
				process_error(&result, activation.run_id, "Child Workflow completion does not match a pending operation")
				return
			}
			if operation.completion_state != .Pending {
				result.resolved_child_workflow_seq = operation.seq
				continue
			}
			resolution_handled := false
			#partial switch status in variant.value.result.status {
			case proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Completed:
				resolution_handled = true
				if status.value == nil || status.value.result == nil {
					process_error(&result, activation.run_id, "completed Child Workflow has no result")
					return
				}
				context.allocator = instance.odin_context.allocator
				cloned_result, clone_error := proto.clone_payload(status.value.result)
				context.allocator = previous_allocator
				if clone_error != .None {
					process_error(&result, activation.run_id, "could not preserve Child Workflow result Payload")
					return
				}
				operation.result = cloned_result
				operation.completion_state = .Succeeded
			case proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Failed:
				resolution_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "failed Child Workflow has no failure")
					return
				}
				context.allocator = instance.odin_context.allocator
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
				failure_preserved := preserve_child_workflow_failure(operation, failure_proto[:])
				context.allocator = previous_allocator
				if len(failure_proto) == 0 || !failure_preserved {
					process_error(&result, activation.run_id, "could not preserve Child Workflow failure")
					return
				}
				operation.failure_message = operation.failure.message
				operation.failure_proto = failure_proto[:]
				operation.completion_state = .Failed
			case proto.Coresdk_Child_Workflow_ChildWorkflowResult_Status_Cancelled:
				resolution_handled = true
				if status.value == nil || status.value.failure == nil {
					process_error(&result, activation.run_id, "canceled Child Workflow has no failure")
					return
				}
				context.allocator = instance.odin_context.allocator
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(status.value.failure)
				failure_preserved := preserve_child_workflow_failure(operation, failure_proto[:])
				context.allocator = previous_allocator
				if len(failure_proto) == 0 || !failure_preserved {
					process_error(&result, activation.run_id, "could not preserve canceled Child Workflow failure")
					return
				}
				operation.failure_message = operation.failure.message
				operation.failure_proto = failure_proto[:]
				operation.completion_state = .Canceled
			}
			if !resolution_handled {
				process_error(&result, activation.run_id, "Child Workflow result is unsupported")
				return
			}
			result.resolved_child_workflow_seq = operation.seq

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Signal_External_Workflow:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "external Workflow signal resolution has no Workflow fiber")
				return
			}
			operation, operation_found := find_external_signal_operation(instance, variant.value.seq)
			if !operation_found {
				process_error(&result, activation.run_id, "external Workflow signal resolution does not match a pending operation")
				return
			}
			if operation.state == .Resolved || operation.state == .Failed {
				if operation.child_workflow_seq != 0 {
					result.resolved_child_workflow_signal_seq = operation.seq
				} else {
					result.resolved_external_workflow_signal_seq = operation.seq
				}
				continue
			}
			if operation.state != .Commanded && operation.state != .Cancel_Requested && operation.state != .Canceled {
				process_error(&result, activation.run_id, "external Workflow signal resolution does not match a commanded operation")
				return
			}
			was_canceled := operation.state == .Cancel_Requested || operation.state == .Canceled
			if variant.value.failure == nil {
				if was_canceled {
					operation.state = .Canceled
					if operation.child_workflow_seq != 0 {
						result.canceled_child_workflow_signal = true
					} else {
						result.canceled_external_workflow_signal = true
					}
				} else {
					operation.state = .Resolved
				}
			} else {
				context.allocator = instance.odin_context.allocator
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(variant.value.failure)
				failure, failure_preserved := preserve_temporal_failure(failure_proto[:])
				context.allocator = previous_allocator
				if len(failure_proto) == 0 || !failure_preserved {
					process_error(&result, activation.run_id, "could not preserve external Workflow signal failure")
					return
				}
				operation.failure_message = failure.message
				operation.failure = failure
				operation.cause = workflow_signal_failure_cause_from_proto(variant.value.cause)
				canceled_failure := temporal_failure.find_kind(failure, .Canceled) != nil
				if was_canceled || canceled_failure {
					operation.state = .Canceled
					if operation.child_workflow_seq != 0 {
						result.canceled_child_workflow_signal = true
					} else {
						result.canceled_external_workflow_signal = true
					}
				} else {
					operation.state = .Failed
					if operation.child_workflow_seq != 0 {
						result.resolved_child_workflow_signal_failed = true
					} else {
						result.resolved_external_workflow_signal_failed = true
					}
				}
			}
			if operation.child_workflow_seq != 0 {
				result.resolved_child_workflow_signal_seq = operation.seq
			} else {
				result.resolved_external_workflow_signal_seq = operation.seq
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Resolve_Request_Cancel_External_Workflow:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "external Workflow cancellation resolution has no Workflow fiber")
				return
			}
			operation, operation_found := find_external_cancel_operation(instance, variant.value.seq)
			if !operation_found || operation.state != .Commanded {
				process_error(&result, activation.run_id, "external Workflow cancellation resolution does not match a pending operation")
				return
			}
			if variant.value.failure == nil {
				operation.state = .Resolved
			} else {
				context.allocator = instance.odin_context.allocator
				failure_proto := proto.encode_Temporal_Api_Failure_V1_Failure(variant.value.failure)
				failure, failure_preserved := preserve_temporal_failure(failure_proto[:])
				context.allocator = previous_allocator
				if len(failure_proto) == 0 || !failure_preserved {
					process_error(&result, activation.run_id, "could not preserve external Workflow cancellation failure")
					return
				}
				operation.failure_message = failure.message
				operation.failure = failure
				operation.cause = external_workflow_cancel_failure_cause_from_proto(
					variant.value.cause,
				)
				operation.state = .Failed
				result.resolved_external_workflow_cancellation_failed = true
			}
			result.resolved_external_workflow_cancellation_seq = operation.seq

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Query_Workflow:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Workflow query has no active Workflow fiber")
				return
			}
			context.allocator = virtual.arena_allocator(&decode_arena)
			query_result := invoke_workflow_query(instance, variant.value)
			append(
				&pending_query_responses,
				Pending_Query_Response{
					query_id = variant.value.query_id,
					result = query_result,
				},
			)
			context.allocator = previous_allocator

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Do_Update:
			handled = true
			if variant.value == nil || !instance_found || len(variant.value.protocol_instance_id) == 0 {
				process_error(&result, activation.run_id, "Workflow Update has no active fiber or protocol instance ID")
				return
			}
			context.allocator = virtual.arena_allocator(&decode_arena)
			invoke_workflow_update(instance, variant.value, &pending_update_responses)
			context.allocator = previous_allocator

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Signal_Workflow:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "Workflow signal must have a name and one payload")
				return
			}
			context.allocator = instance.odin_context.allocator
			signal_error := invoke_workflow_signal(instance, variant.value, &result)
			context.allocator = previous_allocator
			if signal_error.message != nil {
				message := string(signal_error.message)
				process_error(&result, activation.run_id, message)
				error_free(&signal_error)
				return
			}
			result.received_signal = true
			result.received_signal_count += 1

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Cancel_Workflow:
			handled = true
			if variant.value == nil || !instance_found || instance.phase == .Canceled || instance.phase == .Completed || instance.phase == .Failed {
				process_error(&result, activation.run_id, "Workflow cancellation has no active Workflow fiber")
				return
			}
			context.allocator = instance.odin_context.allocator
			cancellation_reason, allocation_error := strings.clone(variant.value.reason)
			context.allocator = previous_allocator
			if allocation_error != nil {
				process_error(&result, activation.run_id, "could not preserve Workflow cancellation reason")
				return
			}
			apply_fiber_cancellation(instance, cancellation_reason)
			result.received_cancellation = true
			result.cancellation_reason = cancellation_reason

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Fire_Timer:
			handled = true
			if variant.value == nil || !instance_found {
				process_error(&result, activation.run_id, "timer firing has no Workflow fiber")
				return
			}
			timer_operation, timer_found := find_timer_operation(instance, variant.value.seq)
			if !timer_found {
				process_error(&result, activation.run_id, "timer firing does not match a pending Workflow operation")
				return
			}
			if timer_operation.state == .Canceled && instance.terminal_command_sent {
				continue
			}
			if timer_operation.state != .Commanded && timer_operation.state != .Cancel_Requested {
				process_error(&result, activation.run_id, "timer firing does not match a pending Workflow operation")
				return
			}
			timer_operation.state = .Resolved
			deadline_token, is_deadline_timer := find_cancellation_node_by_deadline_timer(
				instance,
				variant.value.seq,
			)
			if is_deadline_timer &&
			   !request_cancellation_token(
				instance,
				deadline_token.id,
				cancellation_deadline_reason(),
			   ) {
				process_error(&result, activation.run_id, "could not apply Workflow cancellation deadline")
				return
			}
			local_activity, is_backoff_timer := find_local_activity_by_backoff_timer(
				instance,
				variant.value.seq,
			)
			if is_backoff_timer && local_activity.state == .Commanded {
				instance.next_activity_seq += 1
				local_activity.command_seq = instance.next_activity_seq
				local_activity.backoff_timer_seq = 0
				local_activity.state = .Scheduled
			}

		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache:
			handled = true
			if len(activation.jobs) != 1 {
				process_error(&result, activation.run_id, "eviction activation contains additional jobs")
				return
			}
			destroy_error := remove_fiber_instance(runtime, activation.run_id)
			if destroy_error.message != nil {
				result.error = destroy_error
				result.completion = encode_activation_failure(activation.run_id, string(destroy_error.message))
				result.activation_failed = true
				return
			}
			instance = nil
			instance_found = false
			runtime.evictions += 1
			result.evicted = true
		}
		if !handled {
			process_error(&result, activation.run_id, "Workflow fiber spike received an unsupported activation job")
			return
		}
		if instance_found && len(instance.activation_error_message) > 0 {
			process_error(&result, activation.run_id, instance.activation_error_message)
			return
		}
	}

	if instance_found {
		// A routine resumed later in this order may make an earlier routine
		// ready (for example by canceling its token). Drain deterministically
		// until every main/Signal/Update fiber is blocked or terminal before
		// encoding the activation completion.
		for {
			made_progress := false

			resumed_signals, resume_signals_error := resume_ready_signal_routines(instance)
			made_progress = made_progress || resumed_signals
			if resume_signals_error.message != nil {
				message := string(resume_signals_error.message)
				process_error(&result, activation.run_id, message)
				error_free(&resume_signals_error)
				return
			}
			if len(instance.activation_error_message) > 0 {
				process_error(&result, activation.run_id, instance.activation_error_message)
				return
			}
			finished_signals_error := finish_signal_routines(instance, &result)
			if finished_signals_error.message != nil {
				message := string(finished_signals_error.message)
				process_error(&result, activation.run_id, message)
				error_free(&finished_signals_error)
				return
			}

			resumed_updates, resume_updates_error := resume_ready_update_routines(instance)
			made_progress = made_progress || resumed_updates
			if resume_updates_error.message != nil {
				message := string(resume_updates_error.message)
				process_error(&result, activation.run_id, message)
				error_free(&resume_updates_error)
				return
			}
			if len(instance.activation_error_message) > 0 {
				process_error(&result, activation.run_id, instance.activation_error_message)
				return
			}
			finished_updates_error := append_finished_update_routine_responses(
				instance,
				&pending_update_responses,
			)
			if finished_updates_error.message != nil {
				message := string(finished_updates_error.message)
				process_error(&result, activation.run_id, message)
				error_free(&finished_updates_error)
				return
			}

			resumed_main, resume_main_error := resume_main_workflow_if_ready(instance)
			made_progress = made_progress || resumed_main
			if resume_main_error.message != nil {
				message := string(resume_main_error.message)
				process_error(&result, activation.run_id, message)
				error_free(&resume_main_error)
				return
			}
			if len(instance.activation_error_message) > 0 {
				process_error(&result, activation.run_id, instance.activation_error_message)
				return
			}
			if !made_progress {
				break
			}
		}
	}

	context.allocator = virtual.arena_allocator(&decode_arena)
	commands := make([dynamic]proto.Coresdk_Workflow_Commands_WorkflowCommand)
	for &update_response in pending_update_responses {
		append_update_response_command(&commands, &update_response)
		switch update_response.kind {
		case .Accepted:
			result.updates_accepted += 1
		case .Rejected:
			result.updates_rejected += 1
		case .Completed:
			result.updates_completed += 1
		}
	}
	if instance_found {
		progress_error := append_fiber_progress_command(&result, &commands, instance, runtime.task_queue)
		if progress_error.message != nil {
			message := string(progress_error.message)
			context.allocator = previous_allocator
			result.error = error_from_string(message)
			result.completion = encode_activation_failure(activation.run_id, message)
			result.activation_failed = true
			return
		}
	}
	for &query_response in pending_query_responses {
		append_query_response_command(&commands, &query_response)
		result.queries_answered += 1
		if query_response.result.status == .Failed {
			result.queries_failed += 1
		}
	}
	context.allocator = previous_allocator

	success := proto.Coresdk_Workflow_Completion_Success{commands = commands}
	completion := proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion{
		run_id = activation.run_id,
		status = proto.Coresdk_Workflow_Completion_WorkflowActivationCompletion_Status_Successful{
			value = &success,
		},
	}
	result.completion = proto.encode_Coresdk_Workflow_Completion_WorkflowActivationCompletion(&completion)
	return
}

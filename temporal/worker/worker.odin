// Package worker owns registration, Core polling, dispatch, and lifecycle.
// The payload codecs and Workflow fiber runtime remain experimental.
package worker

import allocator_runtime "base:runtime"
import "core:bytes"
import "core:fmt"
import "core:mem/virtual"
import "core:strings"
import "core:sync"
import "core:thread"
import "core:time"
import activity "../activity"
import codec "../codec"
import temporal_core "../core"
import proto "../core/proto"
import nexus "../nexus"
import workflow "../workflow"
import workflow_isolate "../workflow/isolate"

String_Activity_Proc :: #type proc(ctx: ^activity.Context, input: string) -> string
Payload_Activity_Proc :: #type proc(
	ctx: ^activity.Context,
	input: codec.Payload_View,
) -> activity.Payload_Result
Payloads_Activity_Proc :: #type proc(
	ctx: ^activity.Context,
	inputs: []codec.Payload_View,
) -> activity.Payload_Result
Activity_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^activity.Context,
	inputs: []codec.Payload_View,
) -> activity.Payload_Result

Payload_Nexus_Operation_Proc :: #type proc(
	ctx: ^nexus.Context,
	input: codec.Payload_View,
) -> nexus.Payload_Result
Nexus_Operation_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^nexus.Context,
	input: codec.Payload_View,
) -> nexus.Payload_Result
Nexus_Cancel_Operation_Proc :: #type proc(
	ctx: ^nexus.Context,
	operation_token: string,
) -> nexus.Cancel_Result
Nexus_Cancel_Operation_Invoke_Proc :: #type proc(
	callback_data: rawptr,
	ctx: ^nexus.Context,
	operation_token: string,
) -> nexus.Cancel_Result

// Activity interceptors are registered on a Worker and form a real nested
// chain. Registration order is outermost-first. An interceptor may inspect or
// replace the input, decide whether to call next, and inspect or replace the
// result. User data is borrowed for the Worker lifetime. Inputs and next values
// are call-scoped and must not be retained.
Activity_Execute_Input :: struct {
	ctx:   ^activity.Context,
	inputs: []codec.Payload_View,
}

Activity_Execute_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Activity_Execute_Input,
) -> activity.Payload_Result

Activity_Execute_Next :: struct {
	data:   rawptr,
	invoke: Activity_Execute_Next_Proc,
}

Activity_Execute_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Activity_Execute_Input,
	next: Activity_Execute_Next,
) -> activity.Payload_Result

Activity_Heartbeat_Input :: struct {
	ctx:     ^activity.Context,
	details: codec.Payload_View,
}

Activity_Heartbeat_Next_Proc :: #type proc(
	data: rawptr,
	input: ^Activity_Heartbeat_Input,
) -> activity.Heartbeat_Status

Activity_Heartbeat_Next :: struct {
	data:   rawptr,
	invoke: Activity_Heartbeat_Next_Proc,
}

Activity_Heartbeat_Interceptor_Proc :: #type proc(
	user_data: rawptr,
	input: ^Activity_Heartbeat_Input,
	next: Activity_Heartbeat_Next,
) -> activity.Heartbeat_Status

Activity_Interceptor :: struct {
	user_data: rawptr,
	execute:   Activity_Execute_Interceptor_Proc,
	heartbeat: Activity_Heartbeat_Interceptor_Proc,
}

activity_execute_next :: proc(
	next: Activity_Execute_Next,
	input: ^Activity_Execute_Input,
) -> activity.Payload_Result {
	if next.invoke == nil || input == nil || input.ctx == nil {
		return activity.payload_fail(
			"invalid Activity execute interceptor continuation",
			failure_type = "OdinActivityInterceptorError",
		)
	}
	return next.invoke(next.data, input)
}

activity_heartbeat_next :: proc(
	next: Activity_Heartbeat_Next,
	input: ^Activity_Heartbeat_Input,
) -> activity.Heartbeat_Status {
	if next.invoke == nil || input == nil || input.ctx == nil {
		return .Invalid_Context
	}
	return next.invoke(next.data, input)
}

Activity_Registration :: struct {
	name:          string,
	callback_data: rawptr,
	invoke:        Activity_Invoke_Proc,
}

Nexus_Operation_Registration :: struct {
	service:              string,
	operation:            string,
	callback_data:        rawptr,
	invoke:               Nexus_Operation_Invoke_Proc,
	cancel_callback_data: rawptr,
	cancel_invoke:        Nexus_Cancel_Operation_Invoke_Proc,
}

Workflow_Registration :: struct {
	name:    string,
	runtime: workflow.Fiber_Runtime,
	external: bool,
}

Workflow_Metric_Instrument :: struct {
	name:   string,
	kind:   workflow.Metric_Kind,
	metric: temporal_core.Metric,
}

Workflow_Metric_Backend :: struct {
	meter:       temporal_core.Metric_Meter,
	instruments: [dynamic]Workflow_Metric_Instrument,
}

workflow_metric_kind_core :: proc(kind: workflow.Metric_Kind) -> temporal_core.Metric_Kind {
	switch kind {
	case .Counter_Integer:         return .Counter_Integer
	case .Histogram_Integer:       return .Histogram_Integer
	case .Histogram_Float:         return .Histogram_Float
	case .Histogram_Duration:      return .Histogram_Duration
	case .Gauge_Integer:           return .Gauge_Integer
	case .Gauge_Float:             return .Gauge_Float
	case .Up_Down_Counter_Integer: return .Up_Down_Counter_Integer
	}
	return .Counter_Integer
}

workflow_metric_instrument_find :: proc(
	backend: ^Workflow_Metric_Backend,
	name: string,
	kind: workflow.Metric_Kind,
) -> ^Workflow_Metric_Instrument {
	for &instrument in backend.instruments {
		if instrument.kind == kind && instrument.name == name {
			return &instrument
		}
	}
	return nil
}

workflow_metric_instrument_get :: proc(
	backend: ^Workflow_Metric_Backend,
	record: ^workflow.Metric_Record,
) -> ^Workflow_Metric_Instrument {
	instrument := workflow_metric_instrument_find(backend, record.name, record.kind)
	if instrument != nil do return instrument
	metric, metric_error := temporal_core.metric_create(
		&backend.meter,
		record.name,
		workflow_metric_kind_core(record.kind),
		record.description,
		record.unit,
	)
	if metric_error.message != nil {
		temporal_core.error_free(&metric_error)
		return nil
	}
	owned_name, allocation_error := strings.clone(record.name, allocator_runtime.heap_allocator())
	if allocation_error != nil {
		temporal_core.metric_close(&metric)
		return nil
	}
	append(&backend.instruments, Workflow_Metric_Instrument{
		name = owned_name,
		kind = record.kind,
		metric = metric,
	})
	return &backend.instruments[len(backend.instruments) - 1]
}

workflow_metric_emit_to_core :: proc(user_data: rawptr, record: ^workflow.Metric_Record) {
	backend := cast(^Workflow_Metric_Backend)user_data
	if backend == nil || record == nil do return
	instrument := workflow_metric_instrument_get(backend, record)
	if instrument == nil do return
	attributes := make([]temporal_core.Metric_Attribute, len(record.attributes))
	defer delete(attributes)
	for attribute, index in record.attributes {
		switch attribute.kind {
		case .String:
			attributes[index] = temporal_core.metric_attribute_string(attribute.key, attribute.string_value)
		case .Integer:
			attributes[index] = temporal_core.metric_attribute_i64(attribute.key, attribute.integer_value)
		case .Float:
			attributes[index] = temporal_core.metric_attribute_f64(attribute.key, attribute.float_value)
		case .Boolean:
			attributes[index] = temporal_core.metric_attribute_bool(attribute.key, attribute.boolean_value)
		}
	}
	record_error: temporal_core.Error
	switch record.kind {
	case .Counter_Integer:
		record_error = temporal_core.metric_counter_add(&instrument.metric, record.integer_value, attributes)
	case .Histogram_Integer:
		record_error = temporal_core.metric_histogram_record_integer(&instrument.metric, record.integer_value, attributes)
	case .Histogram_Float:
		record_error = temporal_core.metric_histogram_record_float(&instrument.metric, record.float_value, attributes)
	case .Histogram_Duration:
		record_error = temporal_core.metric_histogram_record_duration(&instrument.metric, record.duration_value, attributes)
	case .Gauge_Integer:
		record_error = temporal_core.metric_gauge_record_integer(&instrument.metric, record.integer_value, attributes)
	case .Gauge_Float:
		record_error = temporal_core.metric_gauge_record_float(&instrument.metric, record.float_value, attributes)
	case .Up_Down_Counter_Integer:
		record_error = temporal_core.metric_up_down_counter_add(&instrument.metric, record.signed_integer_value, attributes)
	}
	temporal_core.error_free(&record_error)
}

workflow_metric_backend_destroy :: proc(user_data: rawptr) {
	backend := cast(^Workflow_Metric_Backend)user_data
	if backend == nil do return
	for &instrument in backend.instruments {
		temporal_core.metric_close(&instrument.metric)
		delete(instrument.name, allocator_runtime.heap_allocator())
	}
	delete(backend.instruments)
	temporal_core.metric_meter_close(&backend.meter)
	free(backend, allocator_runtime.heap_allocator())
}

Workflow_Run :: struct {
	run_id:             string,
	registration_index: int,
}

Rejected_Workflow_Run :: struct {
	run_id: string,
}

Workflow_Activation_Observer :: #type proc(result: ^workflow.Process_Result)
Worker_Ready_Proc :: #type proc(user_data: rawptr) -> bool
// Workflow_Activation_Processor replaces the in-process Workflow dispatcher
// for online Worker activations. It is an experimental process-isolation seam:
// activation bytes are borrowed for this call, while the returned completion
// and Error follow the ordinary owned workflow.Process_Result contract. The
// callback runs serially on the Worker's Workflow poll thread.
Workflow_Activation_Processor_Proc :: #type proc(
	user_data: rawptr,
	activation: []u8,
) -> workflow.Process_Result

Callback_Drain_Kind :: enum {
	Activity,
	Nexus,
}

Callback_Drain_Reason :: enum {
	Worker_Shutdown,
	Bounded_Run,
	Worker_Error,
}

Callback_Drain_Info :: struct {
	kind:           Callback_Drain_Kind,
	reason:         Callback_Drain_Reason,
	outstanding:    int,
	warning_after: time.Duration,
}

Callback_Drain_Observer :: #type proc(
	user_data: rawptr,
	// info is borrowed only for this callback invocation.
	info: ^Callback_Drain_Info,
)

// Uncooperative_Callback_Policy observes an Activity/Nexus executor drain that
// exceeds warning_after. The observer runs once on a dedicated watchdog thread.
// It may log or terminate the process, but must return promptly and must not
// close/free the Worker. Odin callbacks cannot be preempted safely, so the
// default zero value waits without a watchdog and forced per-thread termination
// is deliberately unsupported.
Uncooperative_Callback_Policy :: struct {
	warning_after: time.Duration,
	observer:      Callback_Drain_Observer,
	user_data:     rawptr,
}

Run_Options :: struct {
	// Zero means no bound. These limits exist for tests and examples; production
	// workers normally run until request_shutdown is called.
	max_activity_tasks:       int,
	max_nexus_tasks:          int,
	max_workflow_executions: int,
	disable_activity_polling: bool,
	// Local Activity polling is opt-in while its cancellation/shutdown
	// interleavings are still being hardened. It requires both Workflow and
	// Activity registrations on this Worker.
	enable_local_activities:  bool,
	on_workflow_activation:  Workflow_Activation_Observer,
	workflow_activation_processor: Workflow_Activation_Processor_Proc,
	workflow_activation_processor_data: rawptr,
	// Called synchronously after Core validation and worker registration, but
	// before polling. This permits same-Connection eager Workflow starts.
	on_ready:                Worker_Ready_Proc,
	on_ready_data:           rawptr,
	uncooperative_callbacks: Uncooperative_Callback_Policy,
}

Run_Stats :: struct {
	activity_tasks_completed:             int,
	activity_cancellations_received:      int,
	// Highest number of started Activity executions simultaneously owned by
	// this language Worker. A completed callback remains owned until its pool
	// result is reaped, so completion handoff may briefly add one above the
	// configured Core slot count.
	activity_executions_peak_owned:        int,
	nexus_tasks_completed:                  int,
	// Core-local CancelTask notifications for an already running handler.
	nexus_cancellations_received:           int,
	// Protocol CancelOperation tasks dispatched to registered cancel handlers.
	nexus_cancel_operations_received:       int,
	nexus_executions_peak_owned:            int,
	workflow_activations_completed:       int,
	workflow_activation_failures:         int,
	workflow_executions_completed:        int,
	workflow_replay_activations:          int,
	workflow_evictions:                   int,
	workflow_signals_received:            int,
	workflow_signal_handlers_started:      int,
	workflow_signal_handlers_completed:    int,
	workflow_signal_handlers_abandoned:    int,
	workflow_signal_handler_warnings:      int,
	workflow_queries_answered:            int,
	workflow_queries_failed:              int,
	workflow_updates_accepted:             int,
	workflow_updates_rejected:             int,
	workflow_updates_completed:            int,
	workflow_update_handlers_abandoned:    int,
	workflow_update_handler_warnings:      int,
	workflow_cancellations_received:      int,
	timers_canceled:                      int,
	activity_cancellations_requested:     int,
}

Lifecycle :: enum {
	Uninitialized,
	Configuring,
	Running,
	Stopped,
	Closed,
}

Worker_Mode :: enum {
	Online,
	Replay,
}

Worker_Versioning_Mode :: temporal_core.Worker_Versioning_Mode
Worker_Versioning_Behavior :: temporal_core.Worker_Versioning_Behavior
Worker_Versioning_Options :: temporal_core.Worker_Versioning_Options
Nondeterminism_Policy :: temporal_core.Nondeterminism_Policy
Poller_Behavior_Kind :: temporal_core.Poller_Behavior_Kind
Poller_Behavior :: temporal_core.Poller_Behavior
Resource_Based_Slot_Options :: temporal_core.Resource_Based_Slot_Options
Resource_Based_Activity_Tuner_Options :: temporal_core.Resource_Based_Activity_Tuner_Options

poller_simple_maximum :: temporal_core.poller_simple_maximum
poller_autoscaling :: temporal_core.poller_autoscaling
resource_based_activity_tuner :: temporal_core.resource_based_activity_tuner
resource_based_activity_tuner_with_options :: temporal_core.resource_based_activity_tuner_with_options

Options :: struct {
	task_queue:                                      string,
	namespace:                                       string,
	identity_override:                               string,
	max_concurrent_activities:                       int,
	max_concurrent_nexus_tasks:                      int,
	max_cached_workflows:                            u32,
	sticky_queue_schedule_to_start_timeout:          time.Duration,
	max_heartbeat_throttle_interval:                  time.Duration,
	default_heartbeat_throttle_interval:              time.Duration,
	max_worker_activities_per_second:                 f64,
	max_task_queue_activities_per_second:             f64,
	graceful_shutdown_period:                         time.Duration,
	workflow_task_poller_behavior:                    Poller_Behavior,
	activity_task_poller_behavior:                    Poller_Behavior,
	nexus_task_poller_behavior:                       Poller_Behavior,
	nonsticky_to_sticky_poll_ratio:                   f32,
	max_eager_activity_reservations_per_workflow_task: u32,
	resource_based_activity_tuner:                    Resource_Based_Activity_Tuner_Options,
	versioning:                                      Worker_Versioning_Options,
	nondeterminism_policy:                           Nondeterminism_Policy,
	workflow_stacks:                                 workflow.Stack_Options,
	workflow_logger:                                 workflow.Logger,
}

options_default :: proc(task_queue: string, namespace := "default") -> Options {
	core_options := temporal_core.worker_options_default()
	return {
		task_queue = task_queue,
		namespace = namespace,
		max_concurrent_activities = core_options.max_activity_slots,
		max_concurrent_nexus_tasks = core_options.max_nexus_task_slots,
		max_cached_workflows = core_options.max_cached_workflows,
		sticky_queue_schedule_to_start_timeout =
			core_options.sticky_queue_schedule_to_start_timeout,
		max_heartbeat_throttle_interval = core_options.max_heartbeat_throttle_interval,
		default_heartbeat_throttle_interval =
			core_options.default_heartbeat_throttle_interval,
		max_worker_activities_per_second = core_options.max_worker_activities_per_second,
		max_task_queue_activities_per_second =
			core_options.max_task_queue_activities_per_second,
		graceful_shutdown_period = core_options.graceful_shutdown_period,
		workflow_task_poller_behavior = core_options.workflow_task_poller_behavior,
		activity_task_poller_behavior = core_options.activity_task_poller_behavior,
		nexus_task_poller_behavior = core_options.nexus_task_poller_behavior,
		nonsticky_to_sticky_poll_ratio = core_options.nonsticky_to_sticky_poll_ratio,
		max_eager_activity_reservations_per_workflow_task =
			core_options.max_eager_activity_reservations_per_workflow_task,
		resource_based_activity_tuner = core_options.resource_based_activity_tuner,
	}
}

Worker :: struct {
	core:                   temporal_core.Worker,
	client:                 ^temporal_core.Client,
	namespace:              string,
	task_queue:             string,
	activity_registrations: [dynamic]Activity_Registration,
	nexus_operation_registrations: [dynamic]Nexus_Operation_Registration,
	activity_interceptors:  [dynamic]Activity_Interceptor,
	workflow_inbound_interceptors: [dynamic]workflow.Inbound_Interceptor,
	workflow_interceptors:  [dynamic]workflow.Outbound_Interceptor,
	workflow_registrations: [dynamic]Workflow_Registration,
	workflow_runs:          [dynamic]Workflow_Run,
	rejected_workflow_runs: [dynamic]Rejected_Workflow_Run,
	lifecycle:              Lifecycle,
	max_concurrent_activities: int,
	max_concurrent_local_activities: int,
	max_concurrent_nexus_tasks: int,
	graceful_shutdown_period: time.Duration,
	core_options:          temporal_core.Worker_Options,
	versioning:             Worker_Versioning_Options,
	nondeterminism_policy:  Nondeterminism_Policy,
	workflow_stacks:        workflow.Stack_Options,
	workflow_logger:        workflow.Logger,
	mode:                   Worker_Mode,
	replay_runtime:         ^temporal_core.Runtime,
	uncooperative_callback_policy: Uncooperative_Callback_Policy,
	activity_callbacks_active: int,
	nexus_callbacks_active: int,
	shutdown_watch_mutex: sync.Mutex,
	shutdown_watch_done: sync.Sema,
	shutdown_watch_thread: ^thread.Thread,
}

Replay_Failure_Kind :: enum {
	None,
	Invalid_History,
	Nondeterminism,
	Workflow_Task_Failure,
	Internal,
}

Replay_Eviction_Reason :: proto.Coresdk_Workflow_Activation_RemoveFromCache_EvictionReason

Replay_Result :: struct {
	workflow_id:          string,
	failure:              Replay_Failure_Kind,
	eviction_reason:      Replay_Eviction_Reason,
	message:              []u8,
	activations_processed: int,
}

Replay_History :: struct {
	// Empty IDs are replaced with DEFAULT_REPLAY_WORKFLOW_ID. Both fields are
	// borrowed until replay_histories returns.
	workflow_id: string,
	protobuf:    []u8,
}

DEFAULT_REPLAY_WORKFLOW_ID :: "temporal-sdk-odin-replay-workflow"

replay_result_free :: proc(result: ^Replay_Result) {
	if result == nil {
		return
	}
	delete(result.workflow_id)
	delete(result.message)
	result^ = {}
}

replay_results_free :: proc(results: ^[dynamic]Replay_Result) {
	if results == nil {
		return
	}
	for &result in results^ {
		replay_result_free(&result)
	}
	delete(results^)
	results^ = nil
}

Error :: struct {
	message: []u8,
}

error_free :: proc(error: ^Error) {
	if error == nil || error.message == nil {
		return
	}
	delete(error.message)
	error.message = nil
}

error_from_string :: proc(message: string) -> Error {
	return Error{message = bytes.clone(transmute([]u8)message)}
}

error_from_core :: proc(core_error: ^temporal_core.Error) -> Error {
	if core_error == nil || core_error.message == nil {
		return {}
	}
	result := Error{message = core_error.message}
	core_error.message = nil
	return result
}

error_from_workflow :: proc(workflow_error: ^workflow.Error) -> Error {
	if workflow_error == nil || workflow_error.message == nil {
		return {}
	}
	result := Error{message = workflow_error.message}
	workflow_error.message = nil
	return result
}

owned_string_slice_free :: proc(values: []string) {
	for value in values {
		delete(value)
	}
	delete(values)
}

owned_string_slice_clone :: proc(values: []string) -> (result: []string, ok: bool) {
	result = make([]string, len(values))
	for value, index in values {
		cloned, clone_error := strings.clone(value)
		if clone_error != nil {
			for owned in result[:index] {
				delete(owned)
			}
			delete(result)
			return nil, false
		}
		result[index] = cloned
	}
	return result, true
}

create :: proc(
	client: ^temporal_core.Client,
	task_queue: string,
	namespace := "default",
	max_concurrent_activities := 10,
	versioning := Worker_Versioning_Options{},
	nondeterminism_policy := Nondeterminism_Policy{},
	workflow_stacks := workflow.Stack_Options{},
	graceful_shutdown_period := temporal_core.DEFAULT_GRACEFUL_SHUTDOWN_PERIOD,
	workflow_logger := workflow.Logger{},
) -> (worker: Worker, error: Error) {
	options := options_default(task_queue, namespace)
	options.max_concurrent_activities = max_concurrent_activities
	options.versioning = versioning
	options.nondeterminism_policy = nondeterminism_policy
	options.workflow_stacks = workflow_stacks
	options.graceful_shutdown_period = graceful_shutdown_period
	options.workflow_logger = workflow_logger
	return create_with_options(client, options)
}

create_with_options :: proc(
	client: ^temporal_core.Client,
	options: Options,
) -> (worker: Worker, error: Error) {
	if client == nil || client.handle == nil || client.runtime == nil {
		return {}, error_from_string("client is not initialized")
	}
	if len(options.task_queue) == 0 {
		return {}, error_from_string("task queue must not be empty")
	}
	if len(options.namespace) == 0 {
		return {}, error_from_string("namespace must not be empty")
	}
	core_options := temporal_core.Worker_Options{
		identity_override = options.identity_override,
		max_activity_slots = options.max_concurrent_activities,
		max_nexus_task_slots = options.max_concurrent_nexus_tasks,
		max_cached_workflows = options.max_cached_workflows,
		sticky_queue_schedule_to_start_timeout =
			options.sticky_queue_schedule_to_start_timeout,
		max_heartbeat_throttle_interval = options.max_heartbeat_throttle_interval,
		default_heartbeat_throttle_interval =
			options.default_heartbeat_throttle_interval,
		max_worker_activities_per_second = options.max_worker_activities_per_second,
		max_task_queue_activities_per_second =
			options.max_task_queue_activities_per_second,
		graceful_shutdown_period = options.graceful_shutdown_period,
		workflow_task_poller_behavior = options.workflow_task_poller_behavior,
		activity_task_poller_behavior = options.activity_task_poller_behavior,
		nexus_task_poller_behavior = options.nexus_task_poller_behavior,
		nonsticky_to_sticky_poll_ratio = options.nonsticky_to_sticky_poll_ratio,
		max_eager_activity_reservations_per_workflow_task =
			options.max_eager_activity_reservations_per_workflow_task,
		resource_based_activity_tuner = options.resource_based_activity_tuner,
	}
	core_options_error := temporal_core.worker_options_validate(core_options)
	if core_options_error.message != nil {
		return {}, error_from_core(&core_options_error)
	}
	versioning_error := temporal_core.worker_versioning_options_validate(options.versioning)
	if versioning_error.message != nil {
		return {}, error_from_core(&versioning_error)
	}
	nondeterminism_error := temporal_core.nondeterminism_policy_validate(
		options.nondeterminism_policy,
	)
	if nondeterminism_error.message != nil {
		return {}, error_from_core(&nondeterminism_error)
	}
	resolved_stacks, stack_error := workflow.stack_options_resolve(options.workflow_stacks)
	if stack_error.message != nil {
		return {}, error_from_workflow(&stack_error)
	}
	owned_task_queue, allocation_error := strings.clone(options.task_queue)
	if allocation_error != nil {
		return {}, error_from_string("could not allocate worker task queue")
	}
	owned_namespace, namespace_allocation_error := strings.clone(options.namespace)
	if namespace_allocation_error != nil {
		delete(owned_task_queue)
		return {}, error_from_string("could not allocate worker namespace")
	}
	owned_identity, identity_allocation_error := strings.clone(options.identity_override)
	if identity_allocation_error != nil {
		delete(owned_namespace)
		delete(owned_task_queue)
		return {}, error_from_string("could not allocate worker identity override")
	}
	owned_build_id, build_id_allocation_error := strings.clone(options.versioning.build_id)
	if build_id_allocation_error != nil {
		delete(owned_identity)
		delete(owned_namespace)
		delete(owned_task_queue)
		return {}, error_from_string("could not allocate worker Build ID")
	}
	owned_deployment_name, deployment_name_allocation_error := strings.clone(
		options.versioning.deployment_name,
	)
	if deployment_name_allocation_error != nil {
		delete(owned_build_id)
		delete(owned_identity)
		delete(owned_namespace)
		delete(owned_task_queue)
		return {}, error_from_string("could not allocate worker deployment name")
	}
	owned_failure_types, failure_types_ok := owned_string_slice_clone(
		options.nondeterminism_policy.fail_workflow_types,
	)
	if !failure_types_ok {
		delete(owned_deployment_name)
		delete(owned_build_id)
		delete(owned_identity)
		delete(owned_namespace)
		delete(owned_task_queue)
		return {}, error_from_string("could not allocate nondeterminism failure Workflow types")
	}
	worker.client = client
	worker.namespace = owned_namespace
	worker.task_queue = owned_task_queue
	worker.lifecycle = .Configuring
	worker.max_concurrent_activities = options.max_concurrent_activities
	worker.max_concurrent_local_activities = 1
	worker.max_concurrent_nexus_tasks = options.max_concurrent_nexus_tasks
	if options.resource_based_activity_tuner.enabled {
		worker.max_concurrent_activities =
			options.resource_based_activity_tuner.activity_slots.maximum_slots
		worker.max_concurrent_local_activities =
			options.resource_based_activity_tuner.local_activity_slots.maximum_slots
	}
	worker.graceful_shutdown_period = options.graceful_shutdown_period
	worker.core_options = core_options
	worker.core_options.identity_override = owned_identity
	worker.versioning = options.versioning
	worker.versioning.build_id = owned_build_id
	worker.versioning.deployment_name = owned_deployment_name
	worker.nondeterminism_policy = options.nondeterminism_policy
	worker.nondeterminism_policy.fail_workflow_types = owned_failure_types
	worker.workflow_stacks = resolved_stacks
	worker.workflow_logger = options.workflow_logger
	worker.activity_registrations = make([dynamic]Activity_Registration)
	worker.nexus_operation_registrations = make([dynamic]Nexus_Operation_Registration)
	worker.activity_interceptors = make([dynamic]Activity_Interceptor)
	worker.workflow_inbound_interceptors = make([dynamic]workflow.Inbound_Interceptor)
	worker.workflow_interceptors = make([dynamic]workflow.Outbound_Interceptor)
	worker.workflow_registrations = make([dynamic]Workflow_Registration)
	worker.workflow_runs = make([dynamic]Workflow_Run)
	worker.rejected_workflow_runs = make([dynamic]Rejected_Workflow_Run)
	worker.mode = .Online
	return
}

create_replayer :: proc(
	runtime: ^temporal_core.Runtime,
	namespace := "default",
	task_queue := "temporal-sdk-odin-replay",
	workflow_stacks := workflow.Stack_Options{},
	workflow_logger := workflow.Logger{},
) -> (worker: Worker, error: Error) {
	if runtime == nil || runtime.handle == nil {
		return {}, error_from_string("runtime is not initialized")
	}
	if len(namespace) == 0 {
		return {}, error_from_string("namespace must not be empty")
	}
	if len(task_queue) == 0 {
		return {}, error_from_string("task queue must not be empty")
	}
	resolved_stacks, stack_error := workflow.stack_options_resolve(workflow_stacks)
	if stack_error.message != nil {
		return {}, error_from_workflow(&stack_error)
	}
	owned_task_queue, task_queue_error := strings.clone(task_queue)
	if task_queue_error != nil {
		return {}, error_from_string("could not allocate replay task queue")
	}
	owned_namespace, namespace_error := strings.clone(namespace)
	if namespace_error != nil {
		delete(owned_task_queue)
		return {}, error_from_string("could not allocate replay namespace")
	}
	worker.namespace = owned_namespace
	worker.task_queue = owned_task_queue
	worker.lifecycle = .Configuring
	worker.max_concurrent_activities = 1
	worker.max_concurrent_local_activities = 1
	worker.max_concurrent_nexus_tasks = 1
	worker.workflow_stacks = resolved_stacks
	worker.workflow_logger = workflow_logger
	worker.mode = .Replay
	worker.replay_runtime = runtime
	worker.activity_registrations = make([dynamic]Activity_Registration)
	worker.nexus_operation_registrations = make([dynamic]Nexus_Operation_Registration)
	worker.activity_interceptors = make([dynamic]Activity_Interceptor)
	worker.workflow_inbound_interceptors = make([dynamic]workflow.Inbound_Interceptor)
	worker.workflow_interceptors = make([dynamic]workflow.Outbound_Interceptor)
	worker.workflow_registrations = make([dynamic]Workflow_Registration)
	worker.workflow_runs = make([dynamic]Workflow_Run)
	worker.rejected_workflow_runs = make([dynamic]Rejected_Workflow_Run)
	return
}

add_activity_interceptor :: proc(
	worker: ^Worker,
	interceptor: Activity_Interceptor,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("Activity interceptors cannot be registered after worker.run starts")
	}
	if interceptor.execute == nil && interceptor.heartbeat == nil {
		return error_from_string("Activity interceptor must implement execute or heartbeat")
	}
	append(&worker.activity_interceptors, interceptor)
	return {}
}

// add_workflow_interceptor installs deterministic outbound Workflow hooks.
// Registration order is outermost-first. The record is copied, while user_data
// remains application-owned and must outlive the Worker.
add_workflow_interceptor :: proc(
	worker: ^Worker,
	interceptor: workflow.Outbound_Interceptor,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("Workflow interceptors cannot be registered after worker.run starts")
	}
	if !workflow.outbound_interceptor_has_operation(interceptor) {
		return error_from_string("Workflow outbound interceptor must implement an operation")
	}
	for &registration in worker.workflow_registrations {
		if registration.external {
			continue
		}
		add_error := workflow.fiber_runtime_add_outbound_interceptor(
			&registration.runtime,
			interceptor,
		)
		if add_error.message != nil {
			result := error_from_string(string(add_error.message))
			workflow.error_free(&add_error)
			return result
		}
	}
	append(&worker.workflow_interceptors, interceptor)
	return {}
}

// add_workflow_inbound_interceptor wraps Workflow callback and Query handler
// execution. Registration order is outermost-first. The record is copied,
// while user_data remains application-owned and must outlive the Worker.
add_workflow_inbound_interceptor :: proc(
	worker: ^Worker,
	interceptor: workflow.Inbound_Interceptor,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("Workflow interceptors cannot be registered after worker.run starts")
	}
	if !workflow.inbound_interceptor_has_operation(interceptor) {
		return error_from_string("Workflow inbound interceptor must implement an operation")
	}
	for &registration in worker.workflow_registrations {
		if registration.external {
			continue
		}
		add_error := workflow.fiber_runtime_add_inbound_interceptor(
			&registration.runtime,
			interceptor,
		)
		if add_error.message != nil {
			result := error_from_string(string(add_error.message))
			workflow.error_free(&add_error)
			return result
		}
	}
	append(&worker.workflow_inbound_interceptors, interceptor)
	return {}
}

// set_workflow_logger replaces the replay-aware application logger while the
// Worker is still configuring. The record is copied; user_data remains
// application-owned and must outlive worker.close. A zero Logger disables it.
set_workflow_logger :: proc(worker: ^Worker, logger: workflow.Logger) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("Workflow logger cannot be changed after worker.run starts")
	}
	for &registration in worker.workflow_registrations {
		if registration.external {
			continue
		}
		set_error := workflow.fiber_runtime_set_logger(&registration.runtime, logger)
		if set_error.message != nil {
			result := error_from_string(string(set_error.message))
			workflow.error_free(&set_error)
			return result
		}
	}
	worker.workflow_logger = logger
	return {}
}

configure_workflow_runtime :: proc(worker: ^Worker, runtime: ^workflow.Fiber_Runtime) -> Error {
	logger_error := workflow.fiber_runtime_set_logger(runtime, worker.workflow_logger)
	if logger_error.message != nil {
		result := error_from_string(string(logger_error.message))
		workflow.error_free(&logger_error)
		return result
	}
	backend, backend_allocation_error := new(
		Workflow_Metric_Backend,
		allocator_runtime.heap_allocator(),
	)
	if backend_allocation_error != nil {
		return error_from_string("could not allocate Workflow metric backend")
	}
	backend.meter = temporal_core.metric_meter_noop()
	backend.instruments = make(
		[dynamic]Workflow_Metric_Instrument,
		allocator_runtime.heap_allocator(),
	)
	core_runtime := worker.replay_runtime
	if worker.mode == .Online && worker.client != nil {
		core_runtime = worker.client.runtime
	}
	if core_runtime != nil && core_runtime.metrics_enabled {
		base_attributes := [3]temporal_core.Metric_Attribute{
			temporal_core.metric_attribute_string("namespace", runtime.namespace),
			temporal_core.metric_attribute_string("task_queue", runtime.task_queue),
			temporal_core.metric_attribute_string("workflow_type", runtime.workflow_type),
		}
		meter, meter_error := temporal_core.metric_meter_create(core_runtime, base_attributes[:])
		if meter_error.message == nil {
			backend.meter = meter
		} else {
			temporal_core.error_free(&meter_error)
		}
	}
	emitter_error := workflow.fiber_runtime_set_metric_emitter(runtime, {
		user_data = backend,
		emit = workflow_metric_emit_to_core,
		destroy = workflow_metric_backend_destroy,
	})
	if emitter_error.message != nil {
		workflow_metric_backend_destroy(backend)
		result := error_from_string(string(emitter_error.message))
		workflow.error_free(&emitter_error)
		return result
	}
	for interceptor in worker.workflow_inbound_interceptors {
		add_error := workflow.fiber_runtime_add_inbound_interceptor(runtime, interceptor)
		if add_error.message != nil {
			result := error_from_string(string(add_error.message))
			workflow.error_free(&add_error)
			return result
		}
	}
	for interceptor in worker.workflow_interceptors {
		add_error := workflow.fiber_runtime_add_outbound_interceptor(runtime, interceptor)
		if add_error.message != nil {
			result := error_from_string(string(add_error.message))
			workflow.error_free(&add_error)
			return result
		}
	}
	return {}
}

register_activity_invoker :: proc(
	worker: ^Worker,
	name: string,
	callback_data: rawptr,
	invoke: Activity_Invoke_Proc,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("activities cannot be registered after worker.run starts")
	}
	if len(name) == 0 || callback_data == nil || invoke == nil {
		return error_from_string("activity name and callback are required")
	}
	for registration in worker.activity_registrations {
		if registration.name == name {
			return error_from_string("activity is already registered")
		}
	}
	owned_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		return error_from_string("could not allocate activity name")
	}
	append(
		&worker.activity_registrations,
		Activity_Registration{
			name = owned_name,
			callback_data = callback_data,
			invoke = invoke,
		},
	)
	return {}
}

register_payload_activity :: proc(
	worker: ^Worker,
	name: string,
	callback: Payload_Activity_Proc,
) -> Error {
	if callback == nil {
		return error_from_string("activity name and callback are required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Payload_Activity_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate activity callback")
	}
	owned_callback^ = callback
	registration_error := register_activity_invoker(
		worker,
		name,
		owned_callback,
		proc(data: rawptr, ctx: ^activity.Context, inputs: []codec.Payload_View) -> activity.Payload_Result {
			if len(inputs) != 1 {
				return activity.payload_fail(
					"Activity expected exactly one input Payload",
					failure_type = "OdinActivityArgumentCountError",
				)
			}
			callback := cast(^Payload_Activity_Proc)data
			return callback^(ctx, inputs[0])
		},
	)
	if registration_error.message != nil {
		free(owned_callback, allocator)
	}
	return registration_error
}

register_payloads_activity :: proc(
	worker: ^Worker,
	name: string,
	callback: Payloads_Activity_Proc,
) -> Error {
	if callback == nil {
		return error_from_string("activity name and callback are required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Payloads_Activity_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate activity callback")
	}
	owned_callback^ = callback
	registration_error := register_activity_invoker(
		worker,
		name,
		owned_callback,
		proc(data: rawptr, ctx: ^activity.Context, inputs: []codec.Payload_View) -> activity.Payload_Result {
			callback := cast(^Payloads_Activity_Proc)data
			return callback^(ctx, inputs)
		},
	)
	if registration_error.message != nil {
		free(owned_callback, allocator)
	}
	return registration_error
}

register_json_activity :: proc(
	worker: ^Worker,
	name: string,
	callback: proc(ctx: ^activity.Context, input: $Input) -> $Output,
) -> Error {
	Callback :: #type proc(ctx: ^activity.Context, input: Input) -> Output
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Callback, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate activity callback")
	}
	owned_callback^ = callback
	registration_error := register_activity_invoker(
		worker,
		name,
		owned_callback,
		proc(data: rawptr, ctx: ^activity.Context, inputs: []codec.Payload_View) -> activity.Payload_Result {
			if len(inputs) != 1 {
				return activity.payload_fail(
					"Activity expected exactly one input Payload",
					failure_type = "OdinActivityArgumentCountError",
				)
			}
			value: Input
			decode_error := codec.decode_json(inputs[0], &value)
			if decode_error != .None {
				return activity.payload_fail(
					"activity input is not valid json/plain for its Odin type",
					failure_type = "OdinPayloadConversionError",
				)
			}
			callback := cast(^Callback)data
			output := callback^(ctx, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return activity.payload_fail(
					"could not encode Activity result as json/plain",
					failure_type = "OdinPayloadConversionError",
				)
			}
			return activity.payload_complete(payload)
		},
	)
	if registration_error.message != nil {
		free(owned_callback, allocator)
	}
	return registration_error
}

register_json_activity_result :: proc(
	worker: ^Worker,
	name: string,
	callback: proc(ctx: ^activity.Context, input: $Input) -> activity.Result($Output),
) -> Error {
	Callback :: #type proc(ctx: ^activity.Context, input: Input) -> activity.Result(Output)
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Callback, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate activity callback")
	}
	owned_callback^ = callback
	registration_error := register_activity_invoker(
		worker,
		name,
		owned_callback,
		proc(data: rawptr, ctx: ^activity.Context, inputs: []codec.Payload_View) -> activity.Payload_Result {
			if len(inputs) != 1 {
				return activity.payload_fail(
					"Activity expected exactly one input Payload",
					failure_type = "OdinActivityArgumentCountError",
				)
			}
			value: Input
			decode_error := codec.decode_json(inputs[0], &value)
			if decode_error != .None {
				return activity.payload_fail(
					"activity input is not valid json/plain for its Odin type",
					failure_type = "OdinPayloadConversionError",
				)
			}
			callback := cast(^Callback)data
			result := callback^(ctx, value)
			if result.status == .Failed {
				return {
					status = .Failed,
					failure = result.failure,
				}
			}
			if result.status == .Canceled {
				return {
					status = .Canceled,
					cancellation = result.cancellation,
				}
			}
			if result.status == .Will_Complete_Async {
				return activity.payload_will_complete_async()
			}
			payload, encode_error := codec.encode_json(result.value)
			if encode_error != .None {
				return activity.payload_fail(
					"could not encode Activity result as json/plain",
					failure_type = "OdinPayloadConversionError",
				)
			}
			return activity.payload_complete(payload)
		},
	)
	if registration_error.message != nil {
		free(owned_callback, allocator)
	}
	return registration_error
}

register_activity :: proc(worker: ^Worker, name: string, callback: String_Activity_Proc) -> Error {
	return register_json_activity(
		worker,
		name,
		cast(proc(ctx: ^activity.Context, input: string) -> string)callback,
	)
}

register_nexus_operation_invoker :: proc(
	worker: ^Worker,
	service, operation: string,
	callback_data: rawptr,
	invoke: Nexus_Operation_Invoke_Proc,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("Nexus operations cannot be registered after worker.run starts")
	}
	if len(service) == 0 || len(operation) == 0 || callback_data == nil || invoke == nil {
		return error_from_string("Nexus service, operation, and callback are required")
	}
	for registration in worker.nexus_operation_registrations {
		if registration.service == service && registration.operation == operation {
			return error_from_string("Nexus operation is already registered")
		}
	}
	owned_service, service_error := strings.clone(service)
	if service_error != nil {
		return error_from_string("could not allocate Nexus service name")
	}
	owned_operation, operation_error := strings.clone(operation)
	if operation_error != nil {
		delete(owned_service)
		return error_from_string("could not allocate Nexus operation name")
	}
	append(
		&worker.nexus_operation_registrations,
		Nexus_Operation_Registration{
			service = owned_service,
			operation = owned_operation,
			callback_data = callback_data,
			invoke = invoke,
		},
	)
	return {}
}

register_payload_nexus_operation :: proc(
	worker: ^Worker,
	service, operation: string,
	callback: Payload_Nexus_Operation_Proc,
) -> Error {
	if callback == nil {
		return error_from_string("Nexus service, operation, and callback are required")
	}
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Payload_Nexus_Operation_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Nexus operation callback")
	}
	owned_callback^ = callback
	registration_error := register_nexus_operation_invoker(
		worker,
		service,
		operation,
		owned_callback,
		proc(data: rawptr, ctx: ^nexus.Context, input: codec.Payload_View) -> nexus.Payload_Result {
			callback := cast(^Payload_Nexus_Operation_Proc)data
			return callback^(ctx, input)
		},
	)
	if registration_error.message != nil {
		free(owned_callback, allocator)
	}
	return registration_error
}

register_json_nexus_operation :: proc(
	worker: ^Worker,
	service, operation: string,
	callback: proc(ctx: ^nexus.Context, input: $Input) -> $Output,
) -> Error {
	Callback :: #type proc(ctx: ^nexus.Context, input: Input) -> Output
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Callback, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Nexus operation callback")
	}
	owned_callback^ = callback
	registration_error := register_nexus_operation_invoker(
		worker,
		service,
		operation,
		owned_callback,
		proc(data: rawptr, ctx: ^nexus.Context, input: codec.Payload_View) -> nexus.Payload_Result {
			value: Input
			decode_error := codec.decode_json(input, &value)
			if decode_error != .None {
				return nexus.payload_handler_error(
					"Nexus operation input is not valid json/plain for its Odin type",
					error_type = "BAD_REQUEST",
					retry_behavior = .Non_Retryable,
				)
			}
			callback := cast(^Callback)data
			output := callback^(ctx, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return nexus.payload_handler_error(
					"could not encode Nexus operation result as json/plain",
					retry_behavior = .Non_Retryable,
				)
			}
			return nexus.payload_complete(payload)
		},
	)
	if registration_error.message != nil {
		free(owned_callback, allocator)
	}
	return registration_error
}

register_json_nexus_operation_result :: proc(
	worker: ^Worker,
	service, operation: string,
	callback: proc(ctx: ^nexus.Context, input: $Input) -> nexus.Result($Output),
) -> Error {
	Callback :: #type proc(ctx: ^nexus.Context, input: Input) -> nexus.Result(Output)
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Callback, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Nexus operation callback")
	}
	owned_callback^ = callback
	registration_error := register_nexus_operation_invoker(
		worker,
		service,
		operation,
		owned_callback,
		proc(data: rawptr, ctx: ^nexus.Context, input: codec.Payload_View) -> nexus.Payload_Result {
			value: Input
			decode_error := codec.decode_json(input, &value)
			if decode_error != .None {
				return nexus.payload_handler_error(
					"Nexus operation input is not valid json/plain for its Odin type",
					error_type = "BAD_REQUEST",
					retry_behavior = .Non_Retryable,
				)
			}
			callback := cast(^Callback)data
			result := callback^(ctx, value)
			defer nexus.result_free(&result)
			if result.status == .Handler_Error {
				return {
					status = .Handler_Error,
					handler_error = result.handler_error,
				}
			}
			if result.status == .Started_Async {
				payload_result := nexus.payload_async(result.operation_token)
				if result.operation_token_owned {
					payload_result.operation_token_owned = true
					result.operation_token = ""
					result.operation_token_owned = false
				}
				return payload_result
			}
			payload, encode_error := codec.encode_json(result.value)
			if encode_error != .None {
				return nexus.payload_handler_error(
					"could not encode Nexus operation result as json/plain",
					retry_behavior = .Non_Retryable,
				)
			}
			return nexus.payload_complete(payload)
		},
	)
	if registration_error.message != nil {
		free(owned_callback, allocator)
	}
	return registration_error
}

register_nexus_operation :: proc(
	worker: ^Worker,
	service, operation: string,
	callback: proc(ctx: ^nexus.Context, input: $Input) -> $Output,
) -> Error {
	return register_json_nexus_operation(worker, service, operation, callback)
}

register_nexus_cancel_handler :: proc(
	worker: ^Worker,
	service, operation: string,
	callback: Nexus_Cancel_Operation_Proc,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("Nexus cancel handlers cannot be registered after worker.run starts")
	}
	if callback == nil {
		return error_from_string("Nexus cancel callback is required")
	}
	registration, found := find_nexus_operation(worker, service, operation)
	if !found {
		return error_from_string("Nexus operation must be registered before its cancel handler")
	}
	if registration.cancel_invoke != nil {
		return error_from_string("Nexus cancel handler is already registered")
	}
	allocator := allocator_runtime.heap_allocator()
	owned_callback, allocation_error := new(Nexus_Cancel_Operation_Proc, allocator)
	if allocation_error != nil {
		return error_from_string("could not allocate Nexus cancel callback")
	}
	owned_callback^ = callback
	registration.cancel_callback_data = owned_callback
	registration.cancel_invoke = proc(
		data: rawptr,
		ctx: ^nexus.Context,
		operation_token: string,
	) -> nexus.Cancel_Result {
		registered := cast(^Nexus_Cancel_Operation_Proc)data
		return registered^(ctx, operation_token)
	}
	return {}
}

workflow_runtime_namespace :: proc(worker: ^Worker) -> string {
	if worker != nil && len(worker.namespace) > 0 {
		return worker.namespace
	}
	return "default"
}

// register_external_workflow registers only the Workflow type manifest needed
// for Core polling. Online activations must be handled by a configured
// workflow_activation_processor; no callback or fiber runtime exists in this
// parent Worker.
register_external_workflow :: proc(worker: ^Worker, name: string) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.mode != .Online || worker.lifecycle != .Configuring {
		return error_from_string("external workflows require a configuring online worker")
	}
	if len(name) == 0 {
		return error_from_string("external workflow name is required")
	}
	for registration in worker.workflow_registrations {
		if registration.name == name {
			return error_from_string("workflow is already registered")
		}
	}
	owned_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		return error_from_string("could not allocate external workflow name")
	}
	append(&worker.workflow_registrations, Workflow_Registration{
		name = owned_name,
		external = true,
	})
	return {}
}

// register_external_workflow_registry copies the registry's complete type
// manifest atomically. Procedure pointers remain process-local and are
// instantiated by workflow_isolate.Executor_Host in the child executable.
register_external_workflow_registry :: proc(
	worker: ^Worker,
	registry: ^workflow.Registry,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.mode != .Online || worker.lifecycle != .Configuring {
		return error_from_string("external Workflow registry requires a configuring online worker")
	}
	count := workflow.registry_count(registry)
	if count == 0 {
		return error_from_string("external Workflow registry is empty or uninitialized")
	}
	owned_names := make([dynamic]string, 0, count)
	for index in 0 ..< count {
		name, name_ok := workflow.registry_name(registry, index)
		if !name_ok || len(name) == 0 {
			for owned in owned_names {
				delete(owned)
			}
			delete(owned_names)
			return error_from_string("external Workflow registry contains an invalid name")
		}
		for registration in worker.workflow_registrations {
			if registration.name == name {
				for owned in owned_names {
					delete(owned)
				}
				delete(owned_names)
				return error_from_string("workflow is already registered")
			}
		}
		owned_name, clone_error := strings.clone(name)
		if clone_error != nil {
			for owned in owned_names {
				delete(owned)
			}
			delete(owned_names)
			return error_from_string("could not allocate external Workflow registry manifest")
		}
		append(&owned_names, owned_name)
	}
	for owned_name in owned_names {
		append(&worker.workflow_registrations, Workflow_Registration{
			name = owned_name,
			external = true,
		})
	}
	delete(owned_names)
	return {}
}

register_workflow :: proc(worker: ^Worker, name: string, callback: workflow.Fiber_Proc) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("workflows cannot be registered after worker.run starts")
	}
	if len(name) == 0 || callback == nil {
		return error_from_string("workflow name and callback are required")
	}
	for registration in worker.workflow_registrations {
		if registration.name == name {
			return error_from_string("workflow is already registered")
		}
	}
	owned_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		return error_from_string("could not allocate workflow name")
	}
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init(
		&runtime,
		name,
		worker.task_queue,
		callback,
		namespace = workflow_runtime_namespace(worker),
		stack_options = worker.workflow_stacks,
	)
	if init_error.message != nil {
		delete(owned_name)
		result := error_from_string(string(init_error.message))
		workflow.error_free(&init_error)
		return result
	}
	configuration_error := configure_workflow_runtime(worker, &runtime)
	if configuration_error.message != nil {
		delete(owned_name)
		workflow.fiber_runtime_close(&runtime)
		return configuration_error
	}
	append(
		&worker.workflow_registrations,
		Workflow_Registration{name = owned_name, runtime = runtime},
	)
	return {}
}

register_payload_workflow :: proc(
	worker: ^Worker,
	name: string,
	callback: workflow.Payload_Proc,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("workflows cannot be registered after worker.run starts")
	}
	if len(name) == 0 || callback == nil {
		return error_from_string("workflow name and callback are required")
	}
	for registration in worker.workflow_registrations {
		if registration.name == name {
			return error_from_string("workflow is already registered")
		}
	}
	owned_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		return error_from_string("could not allocate workflow name")
	}
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init_payload(
		&runtime,
		name,
		worker.task_queue,
		callback,
		namespace = workflow_runtime_namespace(worker),
		stack_options = worker.workflow_stacks,
	)
	if init_error.message != nil {
		delete(owned_name)
		result := error_from_string(string(init_error.message))
		workflow.error_free(&init_error)
		return result
	}
	configuration_error := configure_workflow_runtime(worker, &runtime)
	if configuration_error.message != nil {
		delete(owned_name)
		workflow.fiber_runtime_close(&runtime)
		return configuration_error
	}
	append(
		&worker.workflow_registrations,
		Workflow_Registration{name = owned_name, runtime = runtime},
	)
	return {}
}

// register_payloads_workflow exposes the codec-neutral repeated-argument
// boundary used by Temporal. The borrowed slice and all views are valid only
// for the callback invocation; the returned Payload_Result remains owned by
// the Workflow runtime.
register_payloads_workflow :: proc(
	worker: ^Worker,
	name: string,
	callback: workflow.Payloads_Proc,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("workflows cannot be registered after worker.run starts")
	}
	if len(name) == 0 || callback == nil {
		return error_from_string("workflow name and callback are required")
	}
	for registration in worker.workflow_registrations {
		if registration.name == name {
			return error_from_string("workflow is already registered")
		}
	}
	owned_name, allocation_error := strings.clone(name)
	if allocation_error != nil {
		return error_from_string("could not allocate workflow name")
	}
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init_payloads(
		&runtime,
		name,
		worker.task_queue,
		callback,
		namespace = workflow_runtime_namespace(worker),
		stack_options = worker.workflow_stacks,
	)
	if init_error.message != nil {
		delete(owned_name)
		result := error_from_string(string(init_error.message))
		workflow.error_free(&init_error)
		return result
	}
	configuration_error := configure_workflow_runtime(worker, &runtime)
	if configuration_error.message != nil {
		delete(owned_name)
		workflow.fiber_runtime_close(&runtime)
		return configuration_error
	}
	append(
		&worker.workflow_registrations,
		Workflow_Registration{name = owned_name, runtime = runtime},
	)
	return {}
}

register_json_workflow :: proc(
	worker: ^Worker,
	name: string,
	callback: proc(ctx: ^workflow.Fiber_Context, input: $Input) -> $Output,
) -> Error {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return error_from_string("worker is not initialized")
	}
	if worker.lifecycle != .Configuring {
		return error_from_string("workflows cannot be registered after worker.run starts")
	}
	if len(name) == 0 || callback == nil {
		return error_from_string("workflow name and callback are required")
	}
	for registration in worker.workflow_registrations {
		if registration.name == name {
			return error_from_string("workflow is already registered")
		}
	}
	owned_name, name_error := strings.clone(name)
	if name_error != nil {
		return error_from_string("could not allocate workflow name")
	}

	Callback :: #type proc(ctx: ^workflow.Fiber_Context, input: Input) -> Output
	allocator := allocator_runtime.heap_allocator()
	owned_callback, callback_error := new(Callback, allocator)
	if callback_error != nil {
		delete(owned_name)
		return error_from_string("could not allocate workflow callback")
	}
	owned_callback^ = callback
	runtime := workflow.Fiber_Runtime{}
	init_error := workflow.fiber_runtime_init_invoker(
		&runtime,
		name,
		worker.task_queue,
		owned_callback,
		proc(data: rawptr, ctx: ^workflow.Fiber_Context, inputs: []codec.Payload_View) -> workflow.Payload_Result {
			if len(inputs) != 1 {
				return workflow.payload_fail(
					"typed Workflow requires exactly one argument",
					"OdinWorkflowArgumentCountError",
				)
			}
			value: Input
			if codec.decode_json(inputs[0], &value) != .None {
				return workflow.payload_fail(
					"could not decode Workflow input as json/plain",
					"OdinWorkflowPayloadConversionError",
				)
			}
			callback := cast(^Callback)data
			output := callback^(ctx, value)
			payload, encode_error := codec.encode_json(output)
			if encode_error != .None {
				return workflow.payload_fail(
					"could not encode Workflow result as json/plain",
					"OdinWorkflowPayloadConversionError",
				)
			}
			return workflow.payload_complete(payload)
		},
		proc(data: rawptr) {
			free(cast(^Callback)data, allocator_runtime.heap_allocator())
		},
		namespace = workflow_runtime_namespace(worker),
		stack_options = worker.workflow_stacks,
	)
	if init_error.message != nil {
		delete(owned_name)
		free(owned_callback, allocator)
		result := error_from_string(string(init_error.message))
		workflow.error_free(&init_error)
		return result
	}
	configuration_error := configure_workflow_runtime(worker, &runtime)
	if configuration_error.message != nil {
		delete(owned_name)
		workflow.fiber_runtime_close(&runtime)
		return configuration_error
	}
	append(
		&worker.workflow_registrations,
		Workflow_Registration{name = owned_name, runtime = runtime},
	)
	return {}
}

find_activity :: proc(worker: ^Worker, name: string) -> (^Activity_Registration, bool) {
	for &registration in worker.activity_registrations {
		if registration.name == name {
			return &registration, true
		}
	}
	return nil, false
}

find_nexus_operation :: proc(
	worker: ^Worker,
	service, operation: string,
) -> (^Nexus_Operation_Registration, bool) {
	for &registration in worker.nexus_operation_registrations {
		if registration.service == service && registration.operation == operation {
			return &registration, true
		}
	}
	return nil, false
}

completion_failure :: proc(task_token: []u8, message: string) -> [dynamic]u8 {
	return proto.encode_activity_failure(task_token, message)
}

Activity_Execution :: struct {
	worker:        ^Worker,
	callback_data: rawptr,
	invoke:        Activity_Invoke_Proc,
	task_token:    []u8,
	inputs:        [dynamic]codec.Payload,
	input_views:   []codec.Payload_View,
	info_owner:    Activity_Info_Owner,
	metric_meter:  temporal_core.Metric_Meter,
	ctx:           activity.Context,
	error:         Error,
}

// Activity executions are routed by the exact opaque Core task-token bytes.
// Keys borrow each execution's owned task_token and must be removed before the
// execution is destroyed.
Activity_Execution_Routes :: map[string]^Activity_Execution

activity_execution_route_find :: proc(
	routes: Activity_Execution_Routes,
	task_token: []u8,
) -> (^Activity_Execution, bool) {
	execution, found := routes[string(task_token)]
	return execution, found
}

activity_execution_route_add :: proc(
	routes: ^Activity_Execution_Routes,
	execution: ^Activity_Execution,
) -> bool {
	if routes == nil || execution == nil || len(execution.task_token) == 0 {
		return false
	}
	key := string(execution.task_token)
	_, found := routes^[key]
	if found {
		return false
	}
	routes^[key] = execution
	return true
}

activity_execution_route_remove :: proc(
	routes: ^Activity_Execution_Routes,
	execution: ^Activity_Execution,
) -> bool {
	if routes == nil || execution == nil || len(execution.task_token) == 0 {
		return false
	}
	key := string(execution.task_token)
	routed, found := routes^[key]
	if !found || routed != execution {
		return false
	}
	delete_key(routes, key)
	return true
}

Activity_Info_Owner :: struct {
	info:                    activity.Info,
	owned_strings:           [dynamic]string,
	headers:                 [dynamic]codec.Payload,
	header_views:            [dynamic]activity.Header,
	heartbeat_details:       [dynamic]codec.Payload,
	heartbeat_detail_views:  [dynamic]codec.Payload_View,
	retry_error_type_views:  [dynamic]string,
}

activity_info_owner_clone_string :: proc(
	owner: ^Activity_Info_Owner,
	value: string,
) -> (owned: string, ok: bool) {
	if len(value) == 0 {
		return "", true
	}
	cloned, allocation_error := strings.clone(value, allocator_runtime.heap_allocator())
	if allocation_error != nil {
		return "", false
	}
	owned = cloned
	append(&owner.owned_strings, owned)
	return owned, true
}

activity_deadline :: proc(
	scheduled_time: proto.Timestamp_Value,
	started_time: proto.Timestamp_Value,
	schedule_to_close_timeout: proto.Duration_Value,
	start_to_close_timeout: proto.Duration_Value,
) -> (deadline: time.Time, present: bool) {
	if scheduled_time.present && schedule_to_close_timeout.present {
		deadline = time.time_add(scheduled_time.value, schedule_to_close_timeout.value)
		present = true
	}
	if started_time.present && start_to_close_timeout.present {
		attempt_deadline := time.time_add(started_time.value, start_to_close_timeout.value)
		if !present || time.to_unix_nanoseconds(attempt_deadline) < time.to_unix_nanoseconds(deadline) {
			deadline = attempt_deadline
			present = true
		}
	}
	return
}

activity_info_owner_destroy :: proc(owner: ^Activity_Info_Owner) {
	if owner == nil {
		return
	}
	for &payload in owner.heartbeat_details {
		codec.payload_free(&payload)
	}
	for &payload in owner.headers {
		codec.payload_free(&payload)
	}
	for value in owner.owned_strings {
		delete(value, allocator_runtime.heap_allocator())
	}
	delete(owner.heartbeat_details)
	delete(owner.heartbeat_detail_views)
	delete(owner.headers)
	delete(owner.header_views)
	delete(owner.retry_error_type_views)
	delete(owner.owned_strings)
	owner^ = {}
}

activity_info_owner_init :: proc(
	owner: ^Activity_Info_Owner,
	worker: ^Worker,
	task: ^proto.Activity_Task,
	task_token: []u8,
) -> Error {
	if owner == nil || worker == nil || task == nil {
		return error_from_string("could not initialize Activity info")
	}
	heap := allocator_runtime.heap_allocator()
	owner.owned_strings = make([dynamic]string, heap)
	owner.heartbeat_details = make([dynamic]codec.Payload, heap)
	owner.heartbeat_detail_views = make([dynamic]codec.Payload_View, heap)
	owner.headers = make([dynamic]codec.Payload, heap)
	owner.header_views = make([dynamic]activity.Header, heap)
	owner.retry_error_type_views = make([dynamic]string, heap)

	clone_ok: bool
	owner.info.activity_id, clone_ok = activity_info_owner_clone_string(owner, task.activity_id)
	if !clone_ok do return error_from_string("could not allocate Activity info")
	owner.info.activity_type, clone_ok = activity_info_owner_clone_string(owner, task.activity_type)
	if !clone_ok do return error_from_string("could not allocate Activity info")
	owner.info.activity_run_id, clone_ok = activity_info_owner_clone_string(owner, task.activity_run_id)
	if !clone_ok do return error_from_string("could not allocate Activity info")
	owner.info.namespace, clone_ok = activity_info_owner_clone_string(owner, task.workflow_namespace)
	if !clone_ok do return error_from_string("could not allocate Activity info")
	if len(owner.info.namespace) == 0 {
		owner.info.namespace, clone_ok = activity_info_owner_clone_string(owner, worker.namespace)
		if !clone_ok do return error_from_string("could not allocate Activity info")
	}
	owner.info.task_queue, clone_ok = activity_info_owner_clone_string(owner, worker.task_queue)
	if !clone_ok do return error_from_string("could not allocate Activity info")
	owner.info.workflow_id, clone_ok = activity_info_owner_clone_string(owner, task.workflow_id)
	if !clone_ok do return error_from_string("could not allocate Activity info")
	if len(owner.info.workflow_id) > 0 {
		owner.info.workflow_namespace, clone_ok = activity_info_owner_clone_string(
			owner,
			task.workflow_namespace,
		)
		if !clone_ok do return error_from_string("could not allocate Activity info")
		owner.info.workflow_run_id, clone_ok = activity_info_owner_clone_string(owner, task.workflow_run_id)
		if !clone_ok do return error_from_string("could not allocate Activity info")
		owner.info.workflow_type, clone_ok = activity_info_owner_clone_string(owner, task.workflow_type)
		if !clone_ok do return error_from_string("could not allocate Activity info")
	}
	owner.info.attempt = task.attempt
	owner.info.task_token = task_token
	owner.info.is_local = task.is_local
	owner.info.scheduled_time = task.scheduled_time.value
	owner.info.has_scheduled_time = task.scheduled_time.present
	owner.info.current_attempt_scheduled_time = task.current_attempt_scheduled_time.value
	owner.info.has_current_attempt_scheduled_time = task.current_attempt_scheduled_time.present
	owner.info.started_time = task.started_time.value
	owner.info.has_started_time = task.started_time.present
	owner.info.schedule_to_close_timeout = task.schedule_to_close_timeout.value
	owner.info.has_schedule_to_close_timeout = task.schedule_to_close_timeout.present
	owner.info.start_to_close_timeout = task.start_to_close_timeout.value
	owner.info.has_start_to_close_timeout = task.start_to_close_timeout.present
	owner.info.heartbeat_timeout = task.heartbeat_timeout.value
	owner.info.has_heartbeat_timeout = task.heartbeat_timeout.present
	owner.info.deadline, owner.info.has_deadline = activity_deadline(
		task.scheduled_time,
		task.started_time,
		task.schedule_to_close_timeout,
		task.start_to_close_timeout,
	)

	owner.info.has_retry_policy = task.retry_policy.present
	if task.retry_policy.present {
		owner.info.retry_policy = activity.Retry_Policy{
			initial_interval = task.retry_policy.initial_interval.value,
			has_initial_interval = task.retry_policy.initial_interval.present,
			backoff_coefficient = task.retry_policy.backoff_coefficient,
			maximum_interval = task.retry_policy.maximum_interval.value,
			has_maximum_interval = task.retry_policy.maximum_interval.present,
			maximum_attempts = task.retry_policy.maximum_attempts,
		}
		for error_type in task.retry_policy.non_retryable_error_types {
			owned_error_type, type_ok := activity_info_owner_clone_string(owner, error_type)
			if !type_ok do return error_from_string("could not allocate Activity Retry Policy")
			append(&owner.retry_error_type_views, owned_error_type)
		}
		owner.info.retry_policy.non_retryable_error_types = owner.retry_error_type_views[:]
	}
	owner.info.has_priority = task.priority.present
	if task.priority.present {
		fairness_key, fairness_ok := activity_info_owner_clone_string(owner, task.priority.fairness_key)
		if !fairness_ok do return error_from_string("could not allocate Activity priority")
		owner.info.priority = activity.Priority{
			priority_key = task.priority.priority_key,
			fairness_key = fairness_key,
			fairness_weight = task.priority.fairness_weight,
		}
	}

	for header in task.headers {
		owned_key, key_ok := activity_info_owner_clone_string(owner, header.key)
		if !key_ok {
			return error_from_string("could not allocate Activity header key")
		}
		owned_payload, clone_error := proto.clone_payload_view(header.payload, heap)
		if clone_error != .None {
			return error_from_string("could not allocate Activity header Payload")
		}
		append(&owner.headers, owned_payload)
		append(
			&owner.header_views,
			activity.Header{
				key = owned_key,
				payload = codec.payload_view(&owner.headers[len(owner.headers) - 1]),
			},
		)
	}
	owner.info.headers = owner.header_views[:]

	for detail in task.heartbeat_details {
		owned_detail, clone_error := proto.clone_payload_view(detail, heap)
		if clone_error != .None {
			return error_from_string("could not allocate Activity heartbeat details")
		}
		append(&owner.heartbeat_details, owned_detail)
	}
	for &detail in owner.heartbeat_details {
		append(&owner.heartbeat_detail_views, codec.payload_view(&detail))
	}
	owner.info.heartbeat_details = owner.heartbeat_detail_views[:]
	return {}
}

activity_cancel_reason :: proc(reason: proto.Coresdk_Activity_Task_ActivityCancelReason) -> activity.Cancel_Reason {
	switch reason {
	case .NOT_FOUND:
		return .Not_Found
	case .CANCELLED:
		return .Cancelled
	case .TIMED_OUT:
		return .Timed_Out
	case .WORKER_SHUTDOWN:
		return .Worker_Shutdown
	case .PAUSED:
		return .Paused
	case .RESET:
		return .Reset
	}
	return .Cancelled
}

activity_cancel_details :: proc(details: proto.Activity_Cancel_Details) -> activity.Cancellation_Details {
	return {
		is_not_found = details.is_not_found,
		is_cancelled = details.is_cancelled,
		is_paused = details.is_paused,
		is_timed_out = details.is_timed_out,
		is_worker_shutdown = details.is_worker_shutdown,
		is_reset = details.is_reset,
	}
}

activity_heartbeat_recorder :: proc(
	user_data: rawptr,
	details: codec.Payload_View,
) -> activity.Heartbeat_Status {
	execution := cast(^Activity_Execution)user_data
	if execution == nil || execution.worker == nil {
		return .Invalid_Context
	}
	input := Activity_Heartbeat_Input{ctx = &execution.ctx, details = details}
	state := Activity_Heartbeat_Chain_State{execution = execution}
	return activity_heartbeat_chain_invoke(&state, &input)
}

Activity_Execute_Chain_State :: struct {
	execution: ^Activity_Execution,
	index:     int,
}

activity_execute_chain_invoke :: proc(
	data: rawptr,
	input: ^Activity_Execute_Input,
) -> activity.Payload_Result {
	state := cast(^Activity_Execute_Chain_State)data
	if state == nil || state.execution == nil || state.execution.worker == nil ||
	   input == nil || input.ctx == nil {
		return activity.payload_fail(
			"invalid Activity execute interceptor state",
			failure_type = "OdinActivityInterceptorError",
		)
	}
	interceptors := state.execution.worker.activity_interceptors[:]
	for index := state.index; index < len(interceptors); index += 1 {
		interceptor := &interceptors[index]
		if interceptor.execute == nil {
			continue
		}
		next_state := Activity_Execute_Chain_State{
			execution = state.execution,
			index = index + 1,
		}
		return interceptor.execute(
			interceptor.user_data,
			input,
			Activity_Execute_Next{
				data = &next_state,
				invoke = activity_execute_chain_invoke,
			},
		)
	}
	return state.execution.invoke(
		state.execution.callback_data,
		input.ctx,
		input.inputs,
	)
}

Activity_Heartbeat_Chain_State :: struct {
	execution: ^Activity_Execution,
	index:     int,
}

activity_heartbeat_chain_invoke :: proc(
	data: rawptr,
	input: ^Activity_Heartbeat_Input,
) -> activity.Heartbeat_Status {
	state := cast(^Activity_Heartbeat_Chain_State)data
	if state == nil || state.execution == nil || state.execution.worker == nil ||
	   input == nil || input.ctx == nil {
		return .Invalid_Context
	}
	interceptors := state.execution.worker.activity_interceptors[:]
	for index := state.index; index < len(interceptors); index += 1 {
		interceptor := &interceptors[index]
		if interceptor.heartbeat == nil {
			continue
		}
		next_state := Activity_Heartbeat_Chain_State{
			execution = state.execution,
			index = index + 1,
		}
		return interceptor.heartbeat(
			interceptor.user_data,
			input,
			Activity_Heartbeat_Next{
				data = &next_state,
				invoke = activity_heartbeat_chain_invoke,
			},
		)
	}
	heartbeat := proto.encode_activity_heartbeat_payload(
		state.execution.task_token,
		input.details,
	)
	defer delete(heartbeat)
	core_error := temporal_core.worker_record_activity_heartbeat(
		&state.execution.worker.core,
		heartbeat[:],
	)
	if core_error.message != nil {
		temporal_core.error_free(&core_error)
		return .Core_Error
	}
	return .Recorded
}

activity_execution_main :: proc(execution: ^Activity_Execution) {
	// Activity-local allocations, including the returned payload, live only until
	// its payload has been copied into the completion.
	arena: virtual.Arena
	arena_error := virtual.arena_init_growing(&arena)
	if arena_error != nil {
		execution.error = error_from_string("could not initialize activity allocation arena")
		worker_initiate_shutdown(execution.worker)
		return
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	input := Activity_Execute_Input{
		ctx = &execution.ctx,
		inputs = execution.input_views,
	}
	chain_state := Activity_Execute_Chain_State{execution = execution}
	sync.atomic_add_explicit(&execution.worker.activity_callbacks_active, 1, .Acq_Rel)
	result := activity_execute_chain_invoke(&chain_state, &input)
	sync.atomic_sub_explicit(&execution.worker.activity_callbacks_active, 1, .Acq_Rel)
	context.allocator = previous_allocator
	defer codec.payload_free(&result.payload)
	defer activity.application_failure_free(&result.failure)
	defer activity.cancellation_free(&result.cancellation)

	completion: [dynamic]u8
	if result.status == .Will_Complete_Async {
		if execution.info_owner.info.is_local {
			completion = proto.encode_activity_failure(
				execution.task_token,
				"Local Activities cannot complete asynchronously",
				"OdinActivityFailureContractError",
				true,
			)
		} else {
			completion = proto.encode_activity_will_complete_async(execution.task_token)
		}
	} else if result.status == .Canceled || activity.is_cancellation_requested(&execution.ctx) {
		message := result.cancellation.message
		if len(message) == 0 {
			message = "Activity canceled"
		}
		completion = proto.encode_activity_cancellation(
			execution.task_token,
			message,
			codec.payload_view(&result.cancellation.details),
			result.status == .Canceled && result.cancellation.has_details,
		)
	} else if result.status == .Failed {
		failure_type := result.failure.type
		if len(failure_type) == 0 {
			failure_type = "OdinActivityError"
		}
		if result.failure.next_retry_delay < 0 {
			completion = proto.encode_activity_failure(
				execution.task_token,
				"Activity returned a negative next retry delay",
				"OdinActivityFailureContractError",
				true,
			)
		} else {
			completion = proto.encode_activity_failure(
				execution.task_token,
				result.failure.message,
				failure_type,
				result.failure.non_retryable,
				codec.payload_view(&result.failure.details),
				result.failure.has_details,
				result.failure.next_retry_delay,
			)
		}
	} else {
		completion = proto.encode_activity_success_payload(
			execution.task_token,
			codec.payload_view(&result.payload),
		)
	}
	defer delete(completion)
	core_error := temporal_core.worker_complete_activity(&execution.worker.core, completion[:])
	if core_error.message != nil {
		execution.error = error_from_core(&core_error)
		worker_initiate_shutdown(execution.worker)
	}
}

activity_execution_task :: proc(task: thread.Task) {
	activity_execution_main(cast(^Activity_Execution)task.data)
}

activity_execution_destroy :: proc(execution: ^Activity_Execution) {
	if execution == nil {
		return
	}
	delete(execution.task_token)
	for &input in execution.inputs {
		codec.payload_free(&input)
	}
	delete(execution.inputs)
	delete(execution.input_views)
	activity_info_owner_destroy(&execution.info_owner)
	temporal_core.metric_meter_close(&execution.metric_meter)
	error_free(&execution.error)
	free(execution)
}

activity_execution_start :: proc(
	worker: ^Worker,
	pool: ^thread.Pool,
	task: ^proto.Activity_Task,
) -> (execution: ^Activity_Execution, error: Error) {
	registration, found := find_activity(worker, task.activity_type)
	if !found {
		message := fmt.aprintf("activity %q is not registered", task.activity_type)
		defer delete(message)
		completion := completion_failure(task.task_token, message)
		defer delete(completion)
		core_error := temporal_core.worker_complete_activity(&worker.core, completion[:])
		return nil, error_from_core(&core_error)
	}

	execution = new(Activity_Execution)
	execution.worker = worker
	execution.callback_data = registration.callback_data
	execution.invoke = registration.invoke
	execution.task_token = bytes.clone(task.task_token)
	heap := allocator_runtime.heap_allocator()
	execution.inputs = make([dynamic]codec.Payload, heap)
	for input in task.inputs {
		owned, clone_error := proto.clone_payload_view(input, heap)
		if clone_error != .None {
			activity_execution_destroy(execution)
			return nil, error_from_string("could not allocate Activity input Payload")
		}
		append(&execution.inputs, owned)
	}
	execution.input_views = make([]codec.Payload_View, len(execution.inputs), heap)
	for &input, index in execution.inputs {
		execution.input_views[index] = codec.payload_view(&input)
	}
	info_error := activity_info_owner_init(
		&execution.info_owner,
		worker,
		task,
		execution.task_token,
	)
	if info_error.message != nil {
		activity_execution_destroy(execution)
		return nil, info_error
	}
	execution.metric_meter = temporal_core.metric_meter_noop()
	runtime := worker.client.runtime
	if runtime != nil && runtime.metrics_enabled {
		metric_attributes := [3]temporal_core.Metric_Attribute{
			temporal_core.metric_attribute_string("namespace", execution.info_owner.info.namespace),
			temporal_core.metric_attribute_string("task_queue", execution.info_owner.info.task_queue),
			temporal_core.metric_attribute_string("activity_type", execution.info_owner.info.activity_type),
		}
		meter, meter_error := temporal_core.metric_meter_create(runtime, metric_attributes[:])
		if meter_error.message == nil {
			execution.metric_meter = meter
		} else {
			temporal_core.error_free(&meter_error)
		}
	}
	activity.context_init_info(
		&execution.ctx,
		execution.info_owner.info,
		cast(rawptr)execution,
		activity_heartbeat_recorder,
		&execution.metric_meter,
	)
	thread.pool_add_task(pool, context.allocator, activity_execution_task, execution)
	return execution, {}
}

Workflow_Activation_Route :: workflow_isolate.Activation_Route
decode_workflow_activation_route :: workflow_isolate.decode_activation_route

find_workflow_registration :: proc(worker: ^Worker, name: string) -> (int, bool) {
	for registration, index in worker.workflow_registrations {
		if registration.name == name {
			return index, true
		}
	}
	return 0, false
}

find_workflow_run :: proc(worker: ^Worker, run_id: string) -> (int, bool) {
	for workflow_run, index in worker.workflow_runs {
		if workflow_run.run_id == run_id {
			return index, true
		}
	}
	return 0, false
}

remove_workflow_run :: proc(worker: ^Worker, index: int) {
	delete(worker.workflow_runs[index].run_id)
	ordered_remove(&worker.workflow_runs, index)
}

find_rejected_workflow_run :: proc(worker: ^Worker, run_id: string) -> (int, bool) {
	for rejected_run, index in worker.rejected_workflow_runs {
		if rejected_run.run_id == run_id {
			return index, true
		}
	}
	return 0, false
}

remove_rejected_workflow_run :: proc(worker: ^Worker, index: int) {
	delete(worker.rejected_workflow_runs[index].run_id)
	ordered_remove(&worker.rejected_workflow_runs, index)
}

process_workflow_activation :: proc(
	worker: ^Worker,
	data: []u8,
) -> (processed: workflow.Process_Result, error: Error) {
	route, route_ok := decode_workflow_activation_route(data)
	if !route_ok {
		return {}, error_from_string("Core returned a malformed WorkflowActivation protobuf")
	}

	registration_index := 0
	run_index, run_found := find_workflow_run(worker, route.run_id)
	rejected_index, rejected_found := find_rejected_workflow_run(worker, route.run_id)
	owned_run_id := ""
	if route.initializes {
		if run_found {
			return {}, error_from_string("Workflow run was initialized more than once without eviction")
		}
		registration_found: bool
		registration_index, registration_found = find_workflow_registration(worker, route.workflow_type)
		if !registration_found {
			message := fmt.aprintf("Workflow type %s not found", route.workflow_type)
			defer delete(message)
			if !rejected_found {
				cloned_run_id, allocation_error := strings.clone(route.run_id)
				if allocation_error != nil {
					return {}, error_from_string("could not allocate rejected Workflow run route")
				}
				append(
					&worker.rejected_workflow_runs,
					Rejected_Workflow_Run{run_id = cloned_run_id},
				)
			}
			return workflow.activation_failure_result(route.run_id, message), {}
		}
		cloned_run_id, allocation_error := strings.clone(route.run_id)
		if allocation_error != nil {
			return {}, error_from_string("could not allocate Workflow run route")
		}
		owned_run_id = cloned_run_id
	} else if rejected_found {
		if !route.evicts || route.job_count != 1 {
			return workflow.activation_failure_result(
				route.run_id,
				"rejected Workflow run received a non-eviction activation",
			), {}
		}
		remove_rejected_workflow_run(worker, rejected_index)
		processed.completion = workflow.encode_empty_activation_success(route.run_id)
		processed.evicted = true
		return processed, {}
	} else {
		if !run_found {
			return {}, error_from_string("Workflow activation does not match a registered run")
		}
		registration_index = worker.workflow_runs[run_index].registration_index
	}

	registration := &worker.workflow_registrations[registration_index]
	if registration.external {
		return {}, error_from_string(
			"external Workflow activation reached the in-process dispatcher",
		)
	}
	processed = workflow.process_fiber_activation(&registration.runtime, data)
	if route.initializes {
		if workflow.fiber_runtime_has_run(&registration.runtime, route.run_id) {
			append(
				&worker.workflow_runs,
				Workflow_Run{
					run_id = owned_run_id,
					registration_index = registration_index,
				},
			)
			owned_run_id = ""
		} else if processed.activation_failed {
			append(
				&worker.rejected_workflow_runs,
				Rejected_Workflow_Run{run_id = owned_run_id},
			)
			owned_run_id = ""
		}
		delete(owned_run_id)
	}
	if processed.evicted {
		run_index, run_found = find_workflow_run(worker, route.run_id)
		if run_found {
			remove_workflow_run(worker, run_index)
		}
	}
	return
}

dispatch_workflow_activation :: proc(
	worker: ^Worker,
	options: Run_Options,
	data: []u8,
) -> (processed: workflow.Process_Result, error: Error) {
	if options.workflow_activation_processor == nil {
		return process_workflow_activation(worker, data)
	}
	processed = options.workflow_activation_processor(
		options.workflow_activation_processor_data,
		data,
	)
	if processed.completion == nil {
		workflow.error_free(&processed.error)
		return {}, error_from_string(
			"external Workflow activation processor produced no completion",
		)
	}
	return processed, {}
}

Callback_Drain_Watch :: struct {
	done:     sync.Sema,
	policy:   Uncooperative_Callback_Policy,
	info:     Callback_Drain_Info,
}

uncooperative_callback_policy_validate :: proc(
	policy: Uncooperative_Callback_Policy,
) -> Error {
	if policy.warning_after < 0 {
		return error_from_string("uncooperative callback warning duration must not be negative")
	}
	if (policy.warning_after > 0) != (policy.observer != nil) {
		return error_from_string(
			"uncooperative callback warning duration and observer must be configured together",
		)
	}
	return {}
}

callback_drain_watch_main :: proc(state: ^Callback_Drain_Watch) {
	if state == nil || state.policy.observer == nil || state.policy.warning_after <= 0 {
		return
	}
	finished := sync.sema_wait_with_timeout(&state.done, state.policy.warning_after)
	if !finished {
		state.policy.observer(state.policy.user_data, &state.info)
	}
}

// callback_pool_finish preserves the safe wait-for-callback ownership rule.
// When configured it observes, but never forcibly terminates, an executor
// drain that exceeds the policy duration. The returned flag says whether a
// watchdog was required for this drain.
callback_pool_finish :: proc(
	pool: ^thread.Pool,
	policy: Uncooperative_Callback_Policy,
	kind: Callback_Drain_Kind,
	reason: Callback_Drain_Reason,
) -> (watch_started: bool) {
	if pool == nil {
		return false
	}
	outstanding := thread.pool_num_outstanding(pool)
	watch := Callback_Drain_Watch{
		policy = policy,
		info = {
			kind = kind,
			reason = reason,
			outstanding = outstanding,
			warning_after = policy.warning_after,
		},
	}
	watch_thread: ^thread.Thread
	if reason != .Worker_Shutdown && outstanding > 0 && policy.warning_after > 0 &&
	   policy.observer != nil {
		watch_thread = thread.create_and_start_with_poly_data(&watch, callback_drain_watch_main)
		watch_started = watch_thread != nil
	}
	thread.pool_finish(pool)
	if watch_thread != nil {
		sync.sema_post(&watch.done)
		thread.join(watch_thread)
		thread.destroy(watch_thread)
	}
	return
}

worker_shutdown_watch_main :: proc(worker: ^Worker) {
	if worker == nil {
		return
	}
	policy := worker.uncooperative_callback_policy
	if policy.observer == nil || policy.warning_after <= 0 {
		return
	}
	finished := sync.sema_wait_with_timeout(&worker.shutdown_watch_done, policy.warning_after)
	if finished {
		return
	}
	activity_count := sync.atomic_load_explicit(&worker.activity_callbacks_active, .Acquire)
	if activity_count > 0 {
		info := Callback_Drain_Info{
			kind = .Activity,
			reason = .Worker_Shutdown,
			outstanding = activity_count,
			warning_after = policy.warning_after,
		}
		policy.observer(policy.user_data, &info)
	}
	nexus_count := sync.atomic_load_explicit(&worker.nexus_callbacks_active, .Acquire)
	if nexus_count > 0 {
		info := Callback_Drain_Info{
			kind = .Nexus,
			reason = .Worker_Shutdown,
			outstanding = nexus_count,
			warning_after = policy.warning_after,
		}
		policy.observer(policy.user_data, &info)
	}
}

worker_shutdown_watch_start :: proc(worker: ^Worker) {
	if worker == nil || worker.uncooperative_callback_policy.observer == nil ||
	   worker.uncooperative_callback_policy.warning_after <= 0 {
		return
	}
	sync.mutex_lock(&worker.shutdown_watch_mutex)
	defer sync.mutex_unlock(&worker.shutdown_watch_mutex)
	if worker.shutdown_watch_thread != nil {
		return
	}
	worker.shutdown_watch_thread = thread.create_and_start_with_poly_data(
		worker,
		worker_shutdown_watch_main,
	)
}

worker_shutdown_watch_stop :: proc(worker: ^Worker) {
	if worker == nil {
		return
	}
	sync.mutex_lock(&worker.shutdown_watch_mutex)
	watch_thread := worker.shutdown_watch_thread
	if watch_thread != nil {
		sync.sema_post(&worker.shutdown_watch_done)
	}
	sync.mutex_unlock(&worker.shutdown_watch_mutex)
	if watch_thread == nil {
		return
	}
	thread.join(watch_thread)
	thread.destroy(watch_thread)
	sync.mutex_lock(&worker.shutdown_watch_mutex)
	if worker.shutdown_watch_thread == watch_thread {
		worker.shutdown_watch_thread = nil
	}
	sync.mutex_unlock(&worker.shutdown_watch_mutex)
}

worker_initiate_shutdown :: proc(worker: ^Worker) {
	if worker == nil {
		return
	}
	worker_shutdown_watch_start(worker)
	temporal_core.worker_initiate_shutdown(&worker.core)
}

reap_activity_executions :: proc(
	routes: ^Activity_Execution_Routes,
	pool: ^thread.Pool,
) -> (completed: int, error: Error) {
	for {
		task, done := thread.pool_pop_done(pool)
		if !done {
			break
		}
		execution := cast(^Activity_Execution)task.data
		if !activity_execution_route_remove(routes, execution) {
			activity_execution_destroy(execution)
			if error.message == nil {
				error = error_from_string("Activity executor returned an unknown task")
			}
			continue
		}
		if execution.error.message != nil {
			if error.message == nil {
				error = execution.error
				execution.error = {}
			}
		} else {
			completed += 1
		}
		activity_execution_destroy(execution)
	}
	return
}

run_activity_loop :: proc(
	worker: ^Worker,
	max_tasks: int,
	executor_threads: int,
	callback_policy: Uncooperative_Callback_Policy,
) -> (completed: int, cancellations: int, peak_in_flight: int, error: Error) {
	routes := make(
		Activity_Execution_Routes,
		executor_threads,
		allocator_runtime.heap_allocator(),
	)
	defer delete(routes)
	pool := thread.Pool{}
	thread.pool_init(
		&pool,
		allocator_runtime.heap_allocator(),
		executor_threads,
	)
	thread.pool_start(&pool)
	pool_finished := false
	drain_reason := Callback_Drain_Reason.Worker_Error
	defer {
		if !pool_finished {
			callback_pool_finish(&pool, callback_policy, .Activity, drain_reason)
			_, cleanup_error := reap_activity_executions(&routes, &pool)
			error_free(&cleanup_error)
		}
		thread.pool_destroy(&pool)
	}
	started := 0
	for max_tasks <= 0 || started < max_tasks {
		reaped, reap_error := reap_activity_executions(&routes, &pool)
		completed += reaped
		if reap_error.message != nil {
			return completed, cancellations, peak_in_flight, reap_error
		}
		poll := temporal_core.worker_poll_activity(&worker.core)
		if poll.error.message != nil {
			return completed, cancellations, peak_in_flight, error_from_core(&poll.error)
		}
		if poll.shutdown {
			drain_reason = .Worker_Shutdown
			break
		}

		decode_arena: virtual.Arena
		arena_error := virtual.arena_init_growing(&decode_arena)
		if arena_error != nil {
			delete(poll.protobuf)
			return completed, cancellations, peak_in_flight, error_from_string("could not initialize protobuf decode arena")
		}
		previous_allocator := context.allocator
		context.allocator = virtual.arena_allocator(&decode_arena)
		task, decode_error := proto.decode_activity_task(poll.protobuf)
		context.allocator = previous_allocator
		if decode_error != .None {
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			return completed, cancellations, peak_in_flight, error_from_string("Core returned a malformed ActivityTask protobuf")
		}
		if task.kind == .Cancel {
			execution, found := activity_execution_route_find(routes, task.task_token)
			if found {
				activity.context_request_cancellation(
					&execution.ctx,
					activity_cancel_reason(task.cancel_reason),
					activity_cancel_details(task.cancel_details),
				)
				cancellations += 1
			}
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			continue
		}
		if len(task.task_token) == 0 {
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			return completed, cancellations, peak_in_flight, error_from_string("Core returned an Activity start without a task token")
		}
		if _, duplicate := activity_execution_route_find(routes, task.task_token); duplicate {
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			return completed, cancellations, peak_in_flight, error_from_string("Core returned a duplicate Activity task token")
		}

		execution, execute_error := activity_execution_start(worker, &pool, &task)
		virtual.arena_destroy(&decode_arena)
		delete(poll.protobuf)
		if execute_error.message != nil {
			return completed, cancellations, peak_in_flight, execute_error
		}
		started += 1
		if execution == nil {
			completed += 1
		} else if !activity_execution_route_add(&routes, execution) {
			return completed, cancellations, peak_in_flight, error_from_string("could not route Activity execution task token")
		} else {
			peak_in_flight = max(peak_in_flight, len(routes))
		}
	}
	if drain_reason != .Worker_Shutdown {
		drain_reason = .Bounded_Run
	}
	callback_pool_finish(&pool, callback_policy, .Activity, drain_reason)
	pool_finished = true
	reaped, reap_error := reap_activity_executions(&routes, &pool)
	completed += reaped
	return completed, cancellations, peak_in_flight, reap_error
}

Nexus_Info_Owner :: struct {
	info:             nexus.Info,
	owned_strings:    [dynamic]string,
	headers:          [dynamic]nexus.Header,
	callback_headers: [dynamic]nexus.Header,
	links:            [dynamic]nexus.Link,
}

nexus_info_owner_clone_string :: proc(
	owner: ^Nexus_Info_Owner,
	value: string,
) -> (owned: string, ok: bool) {
	if len(value) == 0 {
		return "", true
	}
	cloned, allocation_error := strings.clone(value, allocator_runtime.heap_allocator())
	if allocation_error != nil {
		return "", false
	}
	append(&owner.owned_strings, cloned)
	return cloned, true
}

nexus_info_owner_destroy :: proc(owner: ^Nexus_Info_Owner) {
	if owner == nil {
		return
	}
	for value in owner.owned_strings {
		delete(value, allocator_runtime.heap_allocator())
	}
	delete(owner.owned_strings)
	delete(owner.headers)
	delete(owner.callback_headers)
	delete(owner.links)
	owner^ = {}
}

nexus_info_owner_init :: proc(
	owner: ^Nexus_Info_Owner,
	worker: ^Worker,
	task: ^proto.Nexus_Task,
	task_token: []u8,
) -> Error {
	if owner == nil || worker == nil || task == nil {
		return error_from_string("could not initialize Nexus operation info")
	}
	heap := allocator_runtime.heap_allocator()
	owner.owned_strings = make([dynamic]string, heap)
	owner.headers = make([dynamic]nexus.Header, heap)
	owner.callback_headers = make([dynamic]nexus.Header, heap)
	owner.links = make([dynamic]nexus.Link, heap)
	clone_ok: bool
	owner.info.service, clone_ok = nexus_info_owner_clone_string(owner, task.service)
	if !clone_ok do return error_from_string("could not allocate Nexus operation info")
	owner.info.operation, clone_ok = nexus_info_owner_clone_string(owner, task.operation)
	if !clone_ok do return error_from_string("could not allocate Nexus operation info")
	owner.info.request_id, clone_ok = nexus_info_owner_clone_string(owner, task.request_id)
	if !clone_ok do return error_from_string("could not allocate Nexus operation info")
	owner.info.operation_token, clone_ok = nexus_info_owner_clone_string(owner, task.operation_token)
	if !clone_ok do return error_from_string("could not allocate Nexus operation info")
	owner.info.callback_url, clone_ok = nexus_info_owner_clone_string(owner, task.callback_url)
	if !clone_ok do return error_from_string("could not allocate Nexus operation info")
	owner.info.endpoint, clone_ok = nexus_info_owner_clone_string(owner, task.endpoint)
	if !clone_ok do return error_from_string("could not allocate Nexus operation info")
	owner.info.namespace, clone_ok = nexus_info_owner_clone_string(owner, worker.namespace)
	if !clone_ok do return error_from_string("could not allocate Nexus operation info")
	owner.info.task_queue, clone_ok = nexus_info_owner_clone_string(owner, worker.task_queue)
	if !clone_ok do return error_from_string("could not allocate Nexus operation info")
	owner.info.task_token = task_token
	owner.info.scheduled_time = task.scheduled_time.value
	owner.info.has_scheduled_time = task.scheduled_time.present
	owner.info.deadline = task.request_deadline.value
	owner.info.has_deadline = task.request_deadline.present
	for entry in task.headers {
		key, key_ok := nexus_info_owner_clone_string(owner, entry.key)
		value, value_ok := nexus_info_owner_clone_string(owner, entry.value)
		if !key_ok || !value_ok {
			return error_from_string("could not allocate Nexus request header")
		}
		append(&owner.headers, nexus.Header{key = key, value = value})
	}
	for entry in task.callback_headers {
		key, key_ok := nexus_info_owner_clone_string(owner, entry.key)
		value, value_ok := nexus_info_owner_clone_string(owner, entry.value)
		if !key_ok || !value_ok {
			return error_from_string("could not allocate Nexus callback header")
		}
		append(&owner.callback_headers, nexus.Header{key = key, value = value})
	}
	for entry in task.links {
		url, url_ok := nexus_info_owner_clone_string(owner, entry.url)
		link_type, type_ok := nexus_info_owner_clone_string(owner, entry.type)
		if !url_ok || !type_ok {
			return error_from_string("could not allocate Nexus operation link")
		}
		append(&owner.links, nexus.Link{url = url, type = link_type})
	}
	owner.info.headers = owner.headers[:]
	owner.info.callback_headers = owner.callback_headers[:]
	owner.info.links = owner.links[:]
	return {}
}

Nexus_Execution :: struct {
	worker:               ^Worker,
	kind:                 proto.Nexus_Task_Kind,
	callback_data:        rawptr,
	invoke:               Nexus_Operation_Invoke_Proc,
	cancel_callback_data: rawptr,
	cancel_invoke:        Nexus_Cancel_Operation_Invoke_Proc,
	task_token:           []u8,
	operation_token:      string,
	input:                codec.Payload,
	info_owner:           Nexus_Info_Owner,
	metric_meter:         temporal_core.Metric_Meter,
	ctx:                  nexus.Context,
	error:                Error,
}

Nexus_Execution_Routes :: map[string]^Nexus_Execution

nexus_execution_route_find :: proc(
	routes: Nexus_Execution_Routes,
	task_token: []u8,
) -> (^Nexus_Execution, bool) {
	execution, found := routes[string(task_token)]
	return execution, found
}

nexus_execution_route_add :: proc(
	routes: ^Nexus_Execution_Routes,
	execution: ^Nexus_Execution,
) -> bool {
	if routes == nil || execution == nil || len(execution.task_token) == 0 {
		return false
	}
	key := string(execution.task_token)
	if _, found := routes^[key]; found {
		return false
	}
	routes^[key] = execution
	return true
}

nexus_execution_route_remove :: proc(
	routes: ^Nexus_Execution_Routes,
	execution: ^Nexus_Execution,
) -> bool {
	if routes == nil || execution == nil || len(execution.task_token) == 0 {
		return false
	}
	key := string(execution.task_token)
	routed, found := routes^[key]
	if !found || routed != execution {
		return false
	}
	delete_key(routes, key)
	return true
}

nexus_retry_behavior :: proc(
	value: nexus.Retry_Behavior,
) -> proto.Temporal_Api_Enums_V1_NexusHandlerErrorRetryBehavior {
	switch value {
	case .Retryable:
		return .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE
	case .Non_Retryable:
		return .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE
	case .Unspecified:
		return .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED
	}
	return .NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED
}

nexus_execution_main :: proc(execution: ^Nexus_Execution) {
	arena: virtual.Arena
	if virtual.arena_init_growing(&arena) != nil {
		execution.error = error_from_string("could not initialize Nexus operation allocation arena")
		worker_initiate_shutdown(execution.worker)
		return
	}
	defer virtual.arena_destroy(&arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&arena)
	if execution.kind == .Cancel_Operation {
		sync.atomic_add_explicit(&execution.worker.nexus_callbacks_active, 1, .Acq_Rel)
		result := execution.cancel_invoke(
			execution.cancel_callback_data,
			&execution.ctx,
			execution.operation_token,
		)
		sync.atomic_sub_explicit(&execution.worker.nexus_callbacks_active, 1, .Acq_Rel)
		context.allocator = previous_allocator
		completion: [dynamic]u8
		if nexus.is_cancellation_requested(&execution.ctx) {
			completion = proto.encode_nexus_ack_cancel(execution.task_token)
		} else if result.status == .Handler_Error {
			error_type := result.handler_error.type
			if len(error_type) == 0 do error_type = "INTERNAL"
			completion = proto.encode_nexus_handler_failure(
				execution.task_token,
				result.handler_error.message,
				error_type,
				nexus_retry_behavior(result.handler_error.retry_behavior),
			)
		} else {
			completion = proto.encode_nexus_cancel_success(execution.task_token)
		}
		defer delete(completion)
		core_error := temporal_core.worker_complete_nexus(&execution.worker.core, completion[:])
		if core_error.message != nil {
			execution.error = error_from_core(&core_error)
			worker_initiate_shutdown(execution.worker)
		}
		return
	}
	sync.atomic_add_explicit(&execution.worker.nexus_callbacks_active, 1, .Acq_Rel)
	result := execution.invoke(
		execution.callback_data,
		&execution.ctx,
		codec.payload_view(&execution.input),
	)
	sync.atomic_sub_explicit(&execution.worker.nexus_callbacks_active, 1, .Acq_Rel)
	context.allocator = previous_allocator
	defer nexus.payload_result_free(&result)

	completion: [dynamic]u8
	if nexus.is_cancellation_requested(&execution.ctx) {
		completion = proto.encode_nexus_ack_cancel(execution.task_token)
	} else if result.status == .Handler_Error {
		error_type := result.handler_error.type
		if len(error_type) == 0 {
			error_type = "INTERNAL"
		}
		completion = proto.encode_nexus_handler_failure(
			execution.task_token,
			result.handler_error.message,
			error_type,
			nexus_retry_behavior(result.handler_error.retry_behavior),
		)
	} else if result.status == .Started_Async {
		if len(result.operation_token) == 0 {
			completion = proto.encode_nexus_handler_failure(
				execution.task_token,
				"asynchronous Nexus operation returned an empty operation token",
				"INTERNAL",
				.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
			)
		} else {
			completion = proto.encode_nexus_async_success(
				execution.task_token,
				result.operation_token,
			)
		}
	} else {
		completion = proto.encode_nexus_sync_success(
			execution.task_token,
			codec.payload_view(&result.payload),
		)
	}
	defer delete(completion)
	core_error := temporal_core.worker_complete_nexus(&execution.worker.core, completion[:])
	if core_error.message != nil {
		execution.error = error_from_core(&core_error)
		worker_initiate_shutdown(execution.worker)
	}
}

nexus_execution_task :: proc(task: thread.Task) {
	nexus_execution_main(cast(^Nexus_Execution)task.data)
}

nexus_execution_destroy :: proc(execution: ^Nexus_Execution) {
	if execution == nil {
		return
	}
	delete(execution.task_token)
	delete(execution.operation_token, allocator_runtime.heap_allocator())
	codec.payload_free(&execution.input)
	nexus_info_owner_destroy(&execution.info_owner)
	temporal_core.metric_meter_close(&execution.metric_meter)
	error_free(&execution.error)
	free(execution)
}

nexus_complete_handler_failure :: proc(
	worker: ^Worker,
	task_token: []u8,
	message, error_type: string,
	retry_behavior := nexus.Retry_Behavior.Unspecified,
) -> Error {
	completion := proto.encode_nexus_handler_failure(
		task_token,
		message,
		error_type,
		nexus_retry_behavior(retry_behavior),
	)
	defer delete(completion)
	core_error := temporal_core.worker_complete_nexus(&worker.core, completion[:])
	return error_from_core(&core_error)
}

nexus_execution_start :: proc(
	worker: ^Worker,
	pool: ^thread.Pool,
	task: ^proto.Nexus_Task,
) -> (execution: ^Nexus_Execution, error: Error) {
	registration, found := find_nexus_operation(worker, task.service, task.operation)
	if !found {
		message := fmt.aprintf(
			"Nexus operation %q on service %q is not registered",
			task.operation,
			task.service,
		)
		defer delete(message)
		return nil, nexus_complete_handler_failure(
			worker,
			task.task_token,
			message,
			"NOT_FOUND",
			.Non_Retryable,
		)
	}

	execution = new(Nexus_Execution)
	execution.worker = worker
	execution.kind = task.kind
	execution.callback_data = registration.callback_data
	execution.invoke = registration.invoke
	execution.cancel_callback_data = registration.cancel_callback_data
	execution.cancel_invoke = registration.cancel_invoke
	execution.task_token = bytes.clone(task.task_token)
	if task.kind == .Cancel_Operation {
		if registration.cancel_invoke == nil {
			nexus_execution_destroy(execution)
			return nil, nexus_complete_handler_failure(
				worker,
				task.task_token,
				"Odin Nexus operation does not expose a cancel handler",
				"NOT_IMPLEMENTED",
				.Non_Retryable,
			)
		}
		operation_token, token_error := strings.clone(
			task.operation_token,
			allocator_runtime.heap_allocator(),
		)
		if token_error != nil {
			nexus_execution_destroy(execution)
			return nil, error_from_string("could not allocate Nexus operation token")
		}
		execution.operation_token = operation_token
	} else {
		owned_input, clone_error := proto.clone_payload_view(
			task.input,
			allocator_runtime.heap_allocator(),
		)
		if clone_error != .None {
			nexus_execution_destroy(execution)
			return nil, error_from_string("could not allocate Nexus operation input Payload")
		}
		execution.input = owned_input
	}
	info_error := nexus_info_owner_init(
		&execution.info_owner,
		worker,
		task,
		execution.task_token,
	)
	if info_error.message != nil {
		nexus_execution_destroy(execution)
		return nil, info_error
	}
	execution.metric_meter = temporal_core.metric_meter_noop()
	runtime := worker.client.runtime
	if runtime != nil && runtime.metrics_enabled {
		metric_attributes := [4]temporal_core.Metric_Attribute{
			temporal_core.metric_attribute_string("namespace", execution.info_owner.info.namespace),
			temporal_core.metric_attribute_string("task_queue", execution.info_owner.info.task_queue),
			temporal_core.metric_attribute_string("nexus_service", execution.info_owner.info.service),
			temporal_core.metric_attribute_string("nexus_operation", execution.info_owner.info.operation),
		}
		meter, meter_error := temporal_core.metric_meter_create(runtime, metric_attributes[:])
		if meter_error.message == nil {
			execution.metric_meter = meter
		} else {
			temporal_core.error_free(&meter_error)
		}
	}
	nexus.context_init(
		&execution.ctx,
		execution.info_owner.info,
		&execution.metric_meter,
	)
	thread.pool_add_task(pool, context.allocator, nexus_execution_task, execution)
	return execution, {}
}

reap_nexus_executions :: proc(
	routes: ^Nexus_Execution_Routes,
	pool: ^thread.Pool,
) -> (completed: int, error: Error) {
	for {
		task, done := thread.pool_pop_done(pool)
		if !done {
			break
		}
		execution := cast(^Nexus_Execution)task.data
		if !nexus_execution_route_remove(routes, execution) {
			nexus_execution_destroy(execution)
			if error.message == nil {
				error = error_from_string("Nexus executor returned an unknown task")
			}
			continue
		}
		if execution.error.message != nil {
			if error.message == nil {
				error = execution.error
				execution.error = {}
			}
		} else {
			completed += 1
		}
		nexus_execution_destroy(execution)
	}
	return
}

run_nexus_loop :: proc(
	worker: ^Worker,
	max_tasks, executor_threads: int,
	callback_policy: Uncooperative_Callback_Policy,
) -> (
	completed: int,
	cancellations: int,
	cancel_operations: int,
	peak_in_flight: int,
	error: Error,
) {
	routes := make(Nexus_Execution_Routes, executor_threads, allocator_runtime.heap_allocator())
	defer delete(routes)
	pool := thread.Pool{}
	thread.pool_init(&pool, allocator_runtime.heap_allocator(), executor_threads)
	thread.pool_start(&pool)
	pool_finished := false
	drain_reason := Callback_Drain_Reason.Worker_Error
	defer {
		if !pool_finished {
			callback_pool_finish(&pool, callback_policy, .Nexus, drain_reason)
			_, cleanup_error := reap_nexus_executions(&routes, &pool)
			error_free(&cleanup_error)
		}
		thread.pool_destroy(&pool)
	}
	started := 0
	for max_tasks <= 0 || started < max_tasks {
		reaped, reap_error := reap_nexus_executions(&routes, &pool)
		completed += reaped
		if reap_error.message != nil {
			return completed, cancellations, cancel_operations, peak_in_flight, reap_error
		}
		poll := temporal_core.worker_poll_nexus(&worker.core)
		if poll.error.message != nil {
			return completed, cancellations, cancel_operations, peak_in_flight, error_from_core(&poll.error)
		}
		if poll.shutdown {
			drain_reason = .Worker_Shutdown
			break
		}

		decode_arena: virtual.Arena
		if virtual.arena_init_growing(&decode_arena) != nil {
			delete(poll.protobuf)
			return completed, cancellations, cancel_operations, peak_in_flight, error_from_string("could not initialize Nexus protobuf decode arena")
		}
		previous_allocator := context.allocator
		context.allocator = virtual.arena_allocator(&decode_arena)
		task, decode_error := proto.decode_nexus_task(poll.protobuf)
		context.allocator = previous_allocator
		if decode_error != .None {
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			return completed, cancellations, cancel_operations, peak_in_flight, error_from_string("Core returned a malformed NexusTask protobuf")
		}
		if task.kind == .Cancel_Task {
			execution, found := nexus_execution_route_find(routes, task.task_token)
			if found {
				reason := nexus.Cancel_Reason.Timed_Out
				if task.cancel_reason == .WORKER_SHUTDOWN {
					reason = .Worker_Shutdown
				}
				nexus.context_request_cancellation(&execution.ctx, reason)
				cancellations += 1
			}
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			continue
		}
		if task.kind != .Start_Operation && task.kind != .Cancel_Operation {
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			return completed, cancellations, cancel_operations, peak_in_flight, error_from_string("Core returned an unsupported Nexus task")
		}
		if task.kind == .Cancel_Operation {
			cancel_operations += 1
		}
		if _, duplicate := nexus_execution_route_find(routes, task.task_token); duplicate {
			virtual.arena_destroy(&decode_arena)
			delete(poll.protobuf)
			return completed, cancellations, cancel_operations, peak_in_flight, error_from_string("Core returned a duplicate Nexus task token")
		}
		execution, execute_error := nexus_execution_start(worker, &pool, &task)
		virtual.arena_destroy(&decode_arena)
		delete(poll.protobuf)
		if execute_error.message != nil {
			return completed, cancellations, cancel_operations, peak_in_flight, execute_error
		}
		started += 1
		if execution == nil {
			completed += 1
		} else if !nexus_execution_route_add(&routes, execution) {
			return completed, cancellations, cancel_operations, peak_in_flight, error_from_string("could not route Nexus execution task token")
		} else {
			peak_in_flight = max(peak_in_flight, len(routes))
		}
	}
	if drain_reason != .Worker_Shutdown {
		drain_reason = .Bounded_Run
	}
	callback_pool_finish(&pool, callback_policy, .Nexus, drain_reason)
	pool_finished = true
	reaped, reap_error := reap_nexus_executions(&routes, &pool)
	completed += reaped
	return completed, cancellations, cancel_operations, peak_in_flight, reap_error
}

record_workflow_result :: proc(stats: ^Run_Stats, processed: ^workflow.Process_Result) {
	stats.workflow_activations_completed += 1
	if processed.activation_failed {
		stats.workflow_activation_failures += 1
	}
	if processed.completed {
		stats.workflow_executions_completed += 1
	}
	if processed.is_replaying {
		stats.workflow_replay_activations += 1
	}
	if processed.evicted {
		stats.workflow_evictions += 1
	}
	// Queries are ephemeral requests delivered once, even when Core marks the
	// activation replaying while reconstructing Workflow state for the query.
	stats.workflow_queries_answered += processed.queries_answered
	stats.workflow_queries_failed += processed.queries_failed
	if !processed.is_replaying {
		stats.workflow_signals_received += processed.received_signal_count
		stats.workflow_signal_handlers_started += processed.signal_handlers_started
		stats.workflow_signal_handlers_completed += processed.signal_handlers_completed
		stats.workflow_signal_handlers_abandoned += processed.unfinished_signal_handlers
		stats.workflow_signal_handler_warnings += processed.warned_unfinished_signal_handlers
		stats.workflow_updates_accepted += processed.updates_accepted
		stats.workflow_updates_rejected += processed.updates_rejected
		stats.workflow_updates_completed += processed.updates_completed
		stats.workflow_update_handlers_abandoned += processed.unfinished_update_handlers
		stats.workflow_update_handler_warnings += processed.warned_unfinished_update_handlers
		if processed.received_cancellation {
			stats.workflow_cancellations_received += 1
		}
		if processed.canceled_timer {
			stats.timers_canceled += 1
		}
		stats.activity_cancellations_requested += processed.requested_activity_cancellation_count +
			processed.requested_local_activity_cancellation_count
	}
}

run_workflow_loop :: proc(
	worker: ^Worker,
	options: Run_Options,
) -> (stats: Run_Stats, error: Error) {
	target_reached := false
	for {
		poll := temporal_core.worker_poll_workflow(&worker.core)
		if poll.error.message != nil {
			return stats, error_from_core(&poll.error)
		}
		if poll.shutdown {
			break
		}

		processed, dispatch_error := dispatch_workflow_activation(worker, options, poll.protobuf)
		delete(poll.protobuf)
		if dispatch_error.message != nil {
			return stats, dispatch_error
		}
		if processed.completion == nil {
			workflow.error_free(&processed.error)
			return stats, error_from_string("Workflow activation produced no completion")
		}
		complete_error := temporal_core.worker_complete_workflow(
			&worker.core,
			processed.completion[:],
		)
		if options.on_workflow_activation != nil {
			options.on_workflow_activation(&processed)
		}
		delete(processed.completion)
		processed.completion = nil
		if complete_error.message != nil {
			workflow.error_free(&processed.error)
			return stats, error_from_core(&complete_error)
		}
		record_workflow_result(&stats, &processed)
		if !processed.is_replaying && processed.warned_unfinished_update_handlers > 0 {
			fmt.eprintf(
				"Temporal warning: Workflow %s closed with %d unfinished Update handler(s); handlers were abandoned\n",
				processed.run_id,
				processed.warned_unfinished_update_handlers,
			)
		}
		if !processed.is_replaying && processed.warned_unfinished_signal_handlers > 0 {
			fmt.eprintf(
				"Temporal warning: Workflow %s closed with %d unfinished Signal handler(s); handlers were abandoned\n",
				processed.run_id,
				processed.warned_unfinished_signal_handlers,
			)
		}
		if processed.error.message != nil {
			if !processed.activation_failed {
				message := string(processed.error.message)
				result := error_from_string(message)
				workflow.error_free(&processed.error)
				return stats, result
			}
			workflow.error_free(&processed.error)
			continue
		}
		workflow.error_free(&processed.error)

		if options.max_workflow_executions > 0 &&
		   stats.workflow_executions_completed >= options.max_workflow_executions {
			target_reached = true
		}
		// Core owns cache eviction. A bounded run waits for the first eviction
		// after its terminal command so the language-owned fiber is destroyed.
		if target_reached && processed.evicted {
			break
		}
	}
	return
}

Replay_Eviction :: struct {
	found:   bool,
	reason:  Replay_Eviction_Reason,
	message: []u8,
}

replay_eviction_free :: proc(eviction: ^Replay_Eviction) {
	if eviction == nil {
		return
	}
	delete(eviction.message)
	eviction^ = {}
}

inspect_replay_eviction :: proc(data: []u8) -> (eviction: Replay_Eviction, ok: bool) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{}
	ok = proto.decode_Coresdk_Workflow_Activation_WorkflowActivation(data, &activation)
	context.allocator = previous_allocator
	if !ok {
		return
	}
	for job in activation.jobs {
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache:
			if variant.value == nil || eviction.found {
				ok = false
				return
			}
			eviction.found = true
			eviction.reason = variant.value.reason
			eviction.message = bytes.clone(transmute([]u8)variant.value.message)
		}
	}
	return
}

replay_failure_from_eviction :: proc(reason: Replay_Eviction_Reason) -> Replay_Failure_Kind {
	switch reason {
	case .CACHE_FULL, .LANG_REQUESTED:
		return .None
	case .NONDETERMINISM:
		return .Nondeterminism
	case .LANG_FAIL:
		return .Workflow_Task_Failure
	case .UNSPECIFIED,
	     .CACHE_MISS,
	     .TASK_NOT_FOUND,
	     .UNHANDLED_COMMAND,
	     .FATAL,
	     .PAGINATION_OR_HISTORY_FETCH,
	     .WORKFLOW_EXECUTION_ENDING:
		return .Internal
	}
	return .Internal
}

replay_accepted_history :: proc(
	worker: ^Worker,
	core_replayer: ^temporal_core.Replay_Worker,
	owned_workflow_id: string,
) -> (result: Replay_Result, error: Error) {
	result.workflow_id = owned_workflow_id

	for {
		poll := temporal_core.worker_poll_workflow(&core_replayer.worker)
		if poll.error.message != nil {
			return result, error_from_core(&poll.error)
		}
		if poll.shutdown {
			return result, error_from_string("replay worker stopped before reporting an outcome")
		}

		eviction, inspected := inspect_replay_eviction(poll.protobuf)
		if !inspected {
			delete(poll.protobuf)
			return result, error_from_string("Core returned a malformed replay WorkflowActivation")
		}
		processed, dispatch_error := process_workflow_activation(worker, poll.protobuf)
		delete(poll.protobuf)
		if dispatch_error.message != nil {
			replay_eviction_free(&eviction)
			return result, dispatch_error
		}
		if processed.completion == nil {
			replay_eviction_free(&eviction)
			workflow.error_free(&processed.error)
			return result, error_from_string("replay activation produced no completion")
		}
		complete_error := temporal_core.worker_complete_workflow(
			&core_replayer.worker,
			processed.completion[:],
		)
		delete(processed.completion)
		processed.completion = nil
		if complete_error.message != nil {
			replay_eviction_free(&eviction)
			workflow.error_free(&processed.error)
			return result, error_from_core(&complete_error)
		}
		result.activations_processed += 1
		if processed.error.message != nil && !processed.activation_failed {
			result_error := error_from_string(string(processed.error.message))
			workflow.error_free(&processed.error)
			replay_eviction_free(&eviction)
			return result, result_error
		}
		workflow.error_free(&processed.error)
		if !eviction.found {
			replay_eviction_free(&eviction)
			continue
		}
		result.failure = replay_failure_from_eviction(eviction.reason)
		result.eviction_reason = eviction.reason
		result.message = eviction.message
		eviction.message = nil
		replay_eviction_free(&eviction)
		return
	}
}

// replay_histories runs histories in input order through one Core replay
// worker. Per-history incompatibility and malformed protobuf are data in the
// corresponding Replay_Result; worker/lifecycle faults use Error. Input bytes
// and IDs are borrowed until this call returns. Returned results are owned.
replay_histories :: proc(
	worker: ^Worker,
	histories: []Replay_History,
) -> (results: [dynamic]Replay_Result, error: Error) {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return nil, error_from_string("replayer is not initialized")
	}
	if worker.mode != .Replay {
		return nil, error_from_string("worker was not created with create_replayer")
	}
	if worker.lifecycle == .Running {
		return nil, error_from_string("replayer is already running")
	}
	if worker.lifecycle == .Stopped {
		return nil, error_from_string("replayer cannot be reused after an operational failure")
	}
	if worker.replay_runtime == nil || worker.replay_runtime.handle == nil {
		return nil, error_from_string("runtime must remain open until replay returns")
	}
	if len(worker.workflow_registrations) == 0 {
		return nil, error_from_string("at least one Workflow must be registered for replay")
	}
	if len(worker.workflow_runs) != 0 || len(worker.rejected_workflow_runs) != 0 {
		return nil, error_from_string("previous replay did not fully evict its Workflow state")
	}
	results = make([dynamic]Replay_Result, 0, len(histories))
	if len(histories) == 0 {
		return
	}

	core_replayer, create_error := temporal_core.replay_worker_create(
		worker.replay_runtime,
		worker.namespace,
		worker.task_queue,
	)
	if create_error.message != nil {
		return results, error_from_core(&create_error)
	}
	worker.lifecycle = .Running
	defer {
		close_error := temporal_core.replay_worker_close(&core_replayer)
		if error.message == nil && close_error.message != nil {
			error = error_from_core(&close_error)
		} else {
			temporal_core.error_free(&close_error)
		}
		if error.message == nil {
			worker.lifecycle = .Configuring
		} else {
			worker.lifecycle = .Stopped
		}
	}

	for history in histories {
		workflow_id := history.workflow_id
		if len(workflow_id) == 0 {
			workflow_id = DEFAULT_REPLAY_WORKFLOW_ID
		}
		owned_workflow_id, allocation_error := strings.clone(workflow_id)
		if allocation_error != nil {
			return results, error_from_string("could not allocate replay Workflow ID")
		}
		if len(history.protobuf) == 0 {
			append(&results, Replay_Result{
				workflow_id = owned_workflow_id,
				failure = .Invalid_History,
				message = bytes.clone(transmute([]u8)string("replay history protobuf must not be empty")),
			})
			continue
		}
		push_error := temporal_core.replay_worker_push(
			&core_replayer,
			workflow_id,
			history.protobuf,
		)
		if push_error.message != nil {
			append(&results, Replay_Result{
				workflow_id = owned_workflow_id,
				failure = .Invalid_History,
				message = push_error.message,
			})
			push_error.message = nil
			temporal_core.error_free(&push_error)
			continue
		}
		result, replay_error := replay_accepted_history(
			worker,
			&core_replayer,
			owned_workflow_id,
		)
		if replay_error.message != nil {
			replay_result_free(&result)
			return results, replay_error
		}
		append(&results, result)
	}
	return
}

// replay_history is the single-history convenience form. Replay failures,
// including malformed history, are returned in Replay_Result.
replay_history :: proc(
	worker: ^Worker,
	workflow_id: string,
	history_protobuf: []u8,
) -> (result: Replay_Result, error: Error) {
	histories := [?]Replay_History{{
		workflow_id = workflow_id,
		protobuf = history_protobuf,
	}}
	results, batch_error := replay_histories(worker, histories[:])
	defer replay_results_free(&results)
	if batch_error.message != nil {
		return {}, batch_error
	}
	if len(results) != 1 {
		return {}, error_from_string("single-history replay produced an unexpected result count")
	}
	result = results[0]
	results[0] = {}
	return
}

Activity_Thread_State :: struct {
	worker:    ^Worker,
	max_tasks: int,
	executor_threads: int,
	callback_policy: Uncooperative_Callback_Policy,
	completed: int,
	cancellations: int,
	peak_in_flight: int,
	error:     Error,
}

Workflow_Thread_State :: struct {
	worker:  ^Worker,
	options: Run_Options,
	stats:   Run_Stats,
	error:   Error,
}

Nexus_Thread_State :: struct {
	worker:         ^Worker,
	max_tasks:      int,
	executor_threads: int,
	completed:      int,
	cancellations:  int,
	cancel_operations: int,
	peak_in_flight: int,
	callback_policy: Uncooperative_Callback_Policy,
	error:          Error,
}

activity_thread_main :: proc(state: ^Activity_Thread_State) {
	state.completed,
	state.cancellations,
	state.peak_in_flight,
	state.error = run_activity_loop(
		state.worker,
		state.max_tasks,
		state.executor_threads,
		state.callback_policy,
	)
	if state.error.message != nil {
		worker_initiate_shutdown(state.worker)
	}
}

workflow_thread_main :: proc(state: ^Workflow_Thread_State) {
	state.stats, state.error = run_workflow_loop(state.worker, state.options)
	if state.error.message != nil {
		worker_initiate_shutdown(state.worker)
	}
}

nexus_thread_main :: proc(state: ^Nexus_Thread_State) {
	state.completed,
	state.cancellations,
	state.cancel_operations,
	state.peak_in_flight,
	state.error = run_nexus_loop(
		state.worker,
		state.max_tasks,
		state.executor_threads,
		state.callback_policy,
	)
	if state.error.message != nil {
		worker_initiate_shutdown(state.worker)
	}
}

run :: proc(worker: ^Worker, options := Run_Options{}) -> (stats: Run_Stats, error: Error) {
	if worker == nil || worker.lifecycle == .Uninitialized || worker.lifecycle == .Closed {
		return {}, error_from_string("worker is not initialized")
	}
	if worker.lifecycle == .Running {
		return {}, error_from_string("worker is already running")
	}
	if worker.lifecycle == .Stopped {
		return {}, error_from_string("worker cannot be run more than once")
	}
	if worker.mode == .Replay {
		return {}, error_from_string("replay workers must use replay_history")
	}
	if options.max_activity_tasks < 0 || options.max_nexus_tasks < 0 ||
	   options.max_workflow_executions < 0 {
		return {}, error_from_string("worker run limits must not be negative")
	}
	callback_policy_error := uncooperative_callback_policy_validate(options.uncooperative_callbacks)
	if callback_policy_error.message != nil {
		return {}, callback_policy_error
	}
	poll_workflows := len(worker.workflow_registrations) > 0
	has_external_workflows := false
	has_in_process_workflows := false
	for registration in worker.workflow_registrations {
		if registration.external {
			has_external_workflows = true
		} else {
			has_in_process_workflows = true
		}
	}
	if has_external_workflows && has_in_process_workflows {
		return {}, error_from_string(
			"external and in-process Workflow registrations cannot be mixed",
		)
	}
	if has_external_workflows && options.workflow_activation_processor == nil {
		return {}, error_from_string(
			"external Workflow registrations require a Workflow activation processor",
		)
	}
	if has_external_workflows &&
	   (len(worker.workflow_inbound_interceptors) > 0 ||
	    len(worker.workflow_interceptors) > 0 ||
	    worker.workflow_logger.emit != nil) {
		return {}, error_from_string(
			"external Workflow logging and interceptors must be configured in the executor host",
		)
	}
	poll_remote_activities := len(worker.activity_registrations) > 0 && !options.disable_activity_polling
	poll_local_activities := len(worker.activity_registrations) > 0 && poll_workflows &&
		!options.disable_activity_polling && options.enable_local_activities
	poll_activities := poll_remote_activities || poll_local_activities
	poll_nexus := len(worker.nexus_operation_registrations) > 0
	activity_executor_threads := 0
	if poll_remote_activities {
		activity_executor_threads += worker.max_concurrent_activities
	}
	if poll_local_activities {
		activity_executor_threads += worker.max_concurrent_local_activities
	}
	if !poll_activities && !poll_workflows && !poll_nexus {
		return {}, error_from_string("worker has no enabled registered task types")
	}
	if worker.client == nil || worker.client.handle == nil || worker.client.runtime == nil {
		return {}, error_from_string("client must remain open until worker.close returns")
	}

	core_worker, create_error := temporal_core.worker_create(
		worker.client,
		worker.task_queue,
		worker.core_options,
		worker.namespace,
		enable_workflows = poll_workflows,
		enable_local_activities = poll_local_activities,
		enable_remote_activities = poll_remote_activities,
		enable_nexus = poll_nexus,
		versioning = worker.versioning,
		nondeterminism_policy = worker.nondeterminism_policy,
	)
	if create_error.message != nil {
		return {}, error_from_core(&create_error)
	}
	worker.core = core_worker

	validate_error := temporal_core.worker_validate(&worker.core)
	if validate_error.message != nil {
		result := error_from_core(&validate_error)
		close_error := temporal_core.worker_close(&worker.core)
		temporal_core.error_free(&close_error)
		return {}, result
	}
	worker.uncooperative_callback_policy = options.uncooperative_callbacks
	worker.lifecycle = .Running
	defer {
		worker_shutdown_watch_stop(worker)
		worker.uncooperative_callback_policy = {}
		worker.lifecycle = .Stopped
	}
	if options.on_ready != nil && !options.on_ready(options.on_ready_data) {
		return {}, error_from_string("worker ready callback failed")
	}

	if poll_activities && !poll_workflows && !poll_nexus {
		stats.activity_tasks_completed,
		stats.activity_cancellations_received,
		stats.activity_executions_peak_owned,
		error = run_activity_loop(
			worker,
			options.max_activity_tasks,
			activity_executor_threads,
			options.uncooperative_callbacks,
		)
		return
	}
	if poll_workflows && !poll_activities && !poll_nexus {
		return run_workflow_loop(worker, options)
	}
	if poll_nexus && !poll_activities && !poll_workflows {
		stats.nexus_tasks_completed,
		stats.nexus_cancellations_received,
		stats.nexus_cancel_operations_received,
		stats.nexus_executions_peak_owned,
		error = run_nexus_loop(
			worker,
			options.max_nexus_tasks,
			worker.max_concurrent_nexus_tasks,
			options.uncooperative_callbacks,
		)
		return
	}

	activity_state := Activity_Thread_State{
		worker = worker,
		max_tasks = options.max_activity_tasks,
		executor_threads = activity_executor_threads,
		callback_policy = options.uncooperative_callbacks,
	}
	workflow_state := Workflow_Thread_State{worker = worker, options = options}
	nexus_state := Nexus_Thread_State{
		worker = worker,
		max_tasks = options.max_nexus_tasks,
		executor_threads = worker.max_concurrent_nexus_tasks,
		callback_policy = options.uncooperative_callbacks,
	}
	workflow_thread: ^thread.Thread
	activity_thread: ^thread.Thread
	nexus_thread: ^thread.Thread
	if poll_workflows {
		workflow_thread = thread.create_and_start_with_poly_data(&workflow_state, workflow_thread_main)
		if workflow_thread == nil {
			return {}, error_from_string("could not start Workflow poll thread")
		}
	}
	if poll_activities {
		activity_thread = thread.create_and_start_with_poly_data(&activity_state, activity_thread_main)
		if activity_thread == nil {
			worker_initiate_shutdown(worker)
			if workflow_thread != nil {
				thread.join(workflow_thread)
				thread.destroy(workflow_thread)
				error_free(&workflow_state.error)
			}
			return {}, error_from_string("could not start Activity poll thread")
		}
	}
	if poll_nexus {
		nexus_thread = thread.create_and_start_with_poly_data(&nexus_state, nexus_thread_main)
		if nexus_thread == nil {
			worker_initiate_shutdown(worker)
			if workflow_thread != nil {
				thread.join(workflow_thread)
				thread.destroy(workflow_thread)
				error_free(&workflow_state.error)
			}
			if activity_thread != nil {
				thread.join(activity_thread)
				thread.destroy(activity_thread)
				error_free(&activity_state.error)
			}
			return {}, error_from_string("could not start Nexus poll thread")
		}
	}
	if workflow_thread != nil {
		thread.join(workflow_thread)
		// A bounded Workflow run is an example/test facility. Once its target
		// has evicted, stop otherwise-unbounded Activity and Nexus polls so all
		// enabled streams can join. Task-count limits only stop their own stream.
		if options.max_workflow_executions > 0 && workflow_state.error.message == nil {
			worker_initiate_shutdown(worker)
		}
	}
	if activity_thread != nil do thread.join(activity_thread)
	if nexus_thread != nil do thread.join(nexus_thread)
	if workflow_thread != nil do thread.destroy(workflow_thread)
	if activity_thread != nil do thread.destroy(activity_thread)
	if nexus_thread != nil do thread.destroy(nexus_thread)

	stats = workflow_state.stats
	stats.activity_tasks_completed = activity_state.completed
	stats.activity_cancellations_received = activity_state.cancellations
	stats.activity_executions_peak_owned = activity_state.peak_in_flight
	stats.nexus_tasks_completed = nexus_state.completed
	stats.nexus_cancellations_received = nexus_state.cancellations
	stats.nexus_cancel_operations_received = nexus_state.cancel_operations
	stats.nexus_executions_peak_owned = nexus_state.peak_in_flight
	if workflow_state.error.message != nil {
		error = workflow_state.error
		error_free(&activity_state.error)
		error_free(&nexus_state.error)
		return
	}
	if activity_state.error.message != nil {
		error = activity_state.error
		error_free(&nexus_state.error)
		return
	}
	if nexus_state.error.message != nil {
		error = nexus_state.error
		return
	}
	return
}

request_shutdown :: proc(worker: ^Worker) {
	if worker == nil || worker.lifecycle != .Running {
		return
	}
	worker_initiate_shutdown(worker)
}

close :: proc(worker: ^Worker) -> Error {
	if worker == nil {
		return {}
	}
	if worker.lifecycle == .Closed {
		return {}
	}
	if worker.lifecycle == .Running {
		return error_from_string("worker cannot be closed while worker.run is active")
	}
	core_error := temporal_core.worker_close(&worker.core)
	for registration in worker.activity_registrations {
		free(registration.callback_data, allocator_runtime.heap_allocator())
		delete(registration.name)
	}
	for registration in worker.nexus_operation_registrations {
		free(registration.callback_data, allocator_runtime.heap_allocator())
		if registration.cancel_callback_data != nil {
			free(registration.cancel_callback_data, allocator_runtime.heap_allocator())
		}
		delete(registration.service)
		delete(registration.operation)
	}
	for &registration in worker.workflow_registrations {
		if !registration.external {
			workflow.fiber_runtime_close(&registration.runtime)
		}
		delete(registration.name)
	}
	for workflow_run in worker.workflow_runs {
		delete(workflow_run.run_id)
	}
	for rejected_run in worker.rejected_workflow_runs {
		delete(rejected_run.run_id)
	}
	delete(worker.activity_registrations)
	delete(worker.nexus_operation_registrations)
	delete(worker.activity_interceptors)
	delete(worker.workflow_inbound_interceptors)
	delete(worker.workflow_interceptors)
	delete(worker.workflow_registrations)
	delete(worker.workflow_runs)
	delete(worker.rejected_workflow_runs)
	delete(worker.namespace)
	delete(worker.task_queue)
	delete(worker.core_options.identity_override)
	delete(worker.versioning.build_id)
	delete(worker.versioning.deployment_name)
	owned_string_slice_free(worker.nondeterminism_policy.fail_workflow_types)
	worker.activity_registrations = nil
	worker.nexus_operation_registrations = nil
	worker.activity_interceptors = nil
	worker.workflow_inbound_interceptors = nil
	worker.workflow_interceptors = nil
	worker.workflow_registrations = nil
	worker.workflow_runs = nil
	worker.rejected_workflow_runs = nil
	worker.client = nil
	worker.replay_runtime = nil
	worker.namespace = ""
	worker.task_queue = ""
	worker.lifecycle = .Closed
	worker.max_concurrent_activities = 0
	worker.max_concurrent_local_activities = 0
	worker.max_concurrent_nexus_tasks = 0
	worker.graceful_shutdown_period = 0
	worker.core_options = {}
	worker.versioning = {}
	worker.nondeterminism_policy = {}
	worker.workflow_stacks = {}
	worker.uncooperative_callback_policy = {}
	worker.activity_callbacks_active = 0
	worker.nexus_callbacks_active = 0
	worker.shutdown_watch_thread = nil
	worker.mode = .Online
	return error_from_core(&core_error)
}

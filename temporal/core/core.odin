// Package core owns the blocking lifecycle wrapper around the raw C ABI.
package core

import "core:c"
import "core:bytes"
import "core:fmt"
import "core:math"
import allocator_runtime "base:runtime"
import "core:strings"
import "core:sync"
import "core:time"
import raw "./raw"

Error :: struct {
	message:     []u8,
	status_code: u32,
	details:     []u8,
}

Runtime :: struct {
	handle:            ^raw.Runtime,
	forwards_core_logs: bool,
	metrics_enabled:   bool,
}

Client :: struct {
	handle:  ^raw.Connection,
	runtime: ^Runtime,
}

Worker :: struct {
	handle:                    ^raw.Worker,
	runtime:                   ^Runtime,
	custom_activity_supplier:  ^Custom_Slot_Supplier_Binding,
	workflows_enabled:         bool,
	local_activities_enabled:  bool,
	remote_activities_enabled: bool,
	nexus_enabled:             bool,
}

// Replay_Worker is an offline Core worker. The pusher feeds complete serialized
// temporal.api.history.v1.History messages into Core; the ordinary Worker handle
// then exposes WorkflowActivations and accepts WorkflowActivationCompletions.
Replay_Worker :: struct {
	worker: Worker,
	pusher: ^raw.Worker_Replay_Pusher,
}

DEFAULT_WORKER_BUILD_ID :: "temporal-sdk-odin-experimental"
DEFAULT_GRACEFUL_SHUTDOWN_PERIOD :: 5 * time.Second

Runtime_Key_Value :: struct {
	key:   string,
	value: string,
}

Runtime_Log_Level :: raw.Forwarded_Log_Level

// Runtime_Log owns its strings only for the duration of Runtime_Log_Callback.
// Clone fields inside the callback if they must outlive it.
Runtime_Log :: struct {
	level:            Runtime_Log_Level,
	target:           string,
	message:          string,
	timestamp_millis: u64,
	fields_json:      string,
}

Runtime_Log_Callback :: #type proc(user_data: rawptr, log: ^Runtime_Log)

Runtime_Logging_Options :: struct {
	// Uses Core's tracing filter syntax, for example "temporalio_sdk_core=debug".
	filter:    string,
	// The callback can run concurrently on arbitrary Core threads. It must not
	// call runtime_create/runtime_close and must return before user_data is freed.
	// Core's C ABI has no callback user-data slot, so only one forwarding Runtime
	// may exist in a process at a time.
	forward_to: Runtime_Log_Callback,
	user_data:  rawptr,
}

Open_Telemetry_Metric_Temporality :: raw.Open_Telemetry_Metric_Temporality
Open_Telemetry_Protocol :: raw.Open_Telemetry_Protocol

Runtime_Open_Telemetry_Options :: struct {
	url:                        string,
	headers:                    []Runtime_Key_Value,
	metric_periodicity:         time.Duration,
	metric_temporality:         Open_Telemetry_Metric_Temporality,
	durations_as_seconds:       bool,
	protocol:                   Open_Telemetry_Protocol,
	// Advanced bridge escape hatch: metric/newline/comma-separated-floats.
	histogram_bucket_overrides: string,
}

Runtime_Prometheus_Options :: struct {
	bind_address:               string,
	counters_total_suffix:      bool,
	unit_suffix:                bool,
	durations_as_seconds:       bool,
	// Advanced bridge escape hatch: metric/newline/comma-separated-floats.
	histogram_bucket_overrides: string,
}

Runtime_Metrics_Options :: struct {
	attach_service_name: bool,
	global_tags:         []Runtime_Key_Value,
	metric_prefix:       string,
}

Runtime_Type :: raw.Runtime_Type

Runtime_Info :: struct {
	runtime_type: Runtime_Type,
	version:      string,
}

// Runtime_Options is borrowed only during runtime_create_with_options. Exactly
// one metrics exporter may be selected. A nil exporter disables metrics.
Runtime_Options :: struct {
	logging:                   ^Runtime_Logging_Options,
	prometheus:                ^Runtime_Prometheus_Options,
	opentelemetry:             ^Runtime_Open_Telemetry_Options,
	metrics:                   Runtime_Metrics_Options,
	worker_heartbeat_interval: time.Duration,
	runtime_info:              []Runtime_Info,
	disable_environment_info:  bool,
}

runtime_options_default :: proc() -> Runtime_Options {
	return {
		metrics = {attach_service_name = true},
	}
}

runtime_prometheus_options_default :: proc(bind_address: string) -> Runtime_Prometheus_Options {
	return {bind_address = bind_address}
}

runtime_opentelemetry_options_default :: proc(url: string) -> Runtime_Open_Telemetry_Options {
	return {
		url = url,
		metric_periodicity = time.Second,
		metric_temporality = .Cumulative,
		protocol = .Grpc,
	}
}

// Runtime_Option_Bindings owns every temporary buffer and address-stable raw
// record borrowed by temporal_core_runtime_new. The bridge copies all option
// data synchronously; a configured logging callback remains a process-global
// function pointer for the Runtime lifetime.
Runtime_Option_Bindings :: struct {
	raw_options:       raw.Runtime_Options,
	telemetry:         raw.Telemetry_Options,
	logging:           raw.Logging_Options,
	metrics:           raw.Metrics_Options,
	prometheus:        raw.Prometheus_Options,
	opentelemetry:     raw.Open_Telemetry_Options,
	runtime_info:      [dynamic]raw.Runtime_Info,
	global_tags:       [dynamic]u8,
	opentelemetry_headers: [dynamic]u8,
}

Runtime_Log_Route :: struct {
	callback:  Runtime_Log_Callback,
	user_data: rawptr,
}

runtime_log_route_mutex: sync.Mutex
runtime_log_route: Runtime_Log_Route

Poller_Behavior_Kind :: enum {
	Default,
	Simple_Maximum,
	Autoscaling,
}

// Poller_Behavior is the owned Odin representation. Default leaves the C
// pointers null, allowing Core to choose its default and participate in
// server-directed poller autoscaling when available.
Poller_Behavior :: struct {
	kind:           Poller_Behavior_Kind,
	simple_maximum: int,
	minimum:        int,
	maximum:        int,
	initial:        int,
}

Resource_Based_Slot_Options :: struct {
	minimum_slots: int,
	maximum_slots: int,
	ramp_throttle: time.Duration,
}

Slot_Kind :: enum {
	Workflow,
	Activity,
	Local_Activity,
	Nexus,
}

// Slot_Reserve_Context fields are borrowed callback-scoped views. Clone a
// string before retaining it after the callback returns.
Slot_Reserve_Context :: struct {
	slot_kind:       Slot_Kind,
	task_queue:      string,
	worker_identity: string,
	worker_build_id: string,
	is_sticky:       bool,
}

Slot_Info :: struct {
	slot_kind:      Slot_Kind,
	workflow_type:  string,
	activity_type:  string,
	nexus_operation: string,
	nexus_service:   string,
	is_sticky:       bool,
}

Slot_Reservation :: struct {
	handle: ^raw.Slot_Reserve_Completion_Context,
}

Slot_Reserve_Callback :: #type proc(
	user_data:  rawptr,
	ctx:        ^Slot_Reserve_Context,
	reservation: Slot_Reservation,
)
Slot_Cancel_Reserve_Callback :: #type proc(
	user_data:   rawptr,
	reservation: Slot_Reservation,
)
Slot_Try_Reserve_Callback :: #type proc(
	user_data: rawptr,
	ctx:       ^Slot_Reserve_Context,
) -> uintptr
Slot_Mark_Used_Callback :: #type proc(
	user_data: rawptr,
	info:      ^Slot_Info,
	permit:    uintptr,
)
Slot_Release_Callback :: #type proc(
	user_data: rawptr,
	info:      ^Slot_Info,
	permit:    uintptr,
)
Slot_Available_Callback :: #type proc(user_data: rawptr) -> (available: uintptr, known: bool)
Slot_Supplier_Free_Callback :: #type proc(user_data: rawptr)

// Custom_Slot_Supplier callbacks may run concurrently on Core threads and
// must be thread-safe. Permit zero is reserved for "not available". Every
// asynchronous reservation must be completed exactly once with
// slot_reservation_complete or slot_reservation_cancel_complete.
Custom_Slot_Supplier :: struct {
	reserve:         Slot_Reserve_Callback,
	cancel_reserve:  Slot_Cancel_Reserve_Callback,
	try_reserve:     Slot_Try_Reserve_Callback,
	mark_used:       Slot_Mark_Used_Callback,
	release:         Slot_Release_Callback,
	available_slots: Slot_Available_Callback,
	free:            Slot_Supplier_Free_Callback,
	user_data:       rawptr,
}

Custom_Slot_Supplier_Binding :: struct {
	raw_callbacks:   raw.Custom_Slot_Supplier_Callbacks,
	supplier:        Custom_Slot_Supplier,
	released_by_core: bool,
}

#assert(offset_of(Custom_Slot_Supplier_Binding, raw_callbacks) == 0)

// Resource_Based_Activity_Tuner_Options controls the Activity and Local
// Activity suppliers that share one Core resource controller. Workflow and
// Nexus suppliers remain fixed until those Odin executors support the matching
// concurrency models. Zero-value options keep the fixed-size worker defaults.
Resource_Based_Activity_Tuner_Options :: struct {
	enabled:              bool,
	target_memory_usage:  f64,
	target_cpu_usage:     f64,
	activity_slots:       Resource_Based_Slot_Options,
	local_activity_slots: Resource_Based_Slot_Options,
}

// Resource_Based_Nexus_Tuner_Options controls the Nexus task supplier. When
// combined with the Activity tuner, both options must use identical targets
// because Core constructs one resource controller for the whole Worker.
Resource_Based_Nexus_Tuner_Options :: struct {
	enabled:             bool,
	target_memory_usage: f64,
	target_cpu_usage:    f64,
	nexus_task_slots:    Resource_Based_Slot_Options,
}

resource_based_activity_tuner_with_options :: proc(
	target_memory_usage: f64,
	target_cpu_usage: f64,
	activity_slots: Resource_Based_Slot_Options,
	local_activity_slots: Resource_Based_Slot_Options,
) -> Resource_Based_Activity_Tuner_Options {
	return {
		enabled = true,
		target_memory_usage = target_memory_usage,
		target_cpu_usage = target_cpu_usage,
		activity_slots = activity_slots,
		local_activity_slots = local_activity_slots,
	}
}

resource_based_activity_tuner :: proc(
	target_memory_usage: f64,
	target_cpu_usage: f64,
	maximum_activity_slots := 10,
	maximum_local_activity_slots := 10,
) -> Resource_Based_Activity_Tuner_Options {
	return resource_based_activity_tuner_with_options(
		target_memory_usage,
		target_cpu_usage,
		{
			minimum_slots = 1,
			maximum_slots = maximum_activity_slots,
			ramp_throttle = 50 * time.Millisecond,
		},
		{
			minimum_slots = 1,
			maximum_slots = maximum_local_activity_slots,
			ramp_throttle = 50 * time.Millisecond,
		},
	)
}

resource_based_nexus_tuner_with_options :: proc(
	target_memory_usage: f64,
	target_cpu_usage: f64,
	nexus_task_slots: Resource_Based_Slot_Options,
) -> Resource_Based_Nexus_Tuner_Options {
	return {
		enabled = true,
		target_memory_usage = target_memory_usage,
		target_cpu_usage = target_cpu_usage,
		nexus_task_slots = nexus_task_slots,
	}
}

resource_based_nexus_tuner :: proc(
	target_memory_usage: f64,
	target_cpu_usage: f64,
	maximum_nexus_task_slots := 10,
) -> Resource_Based_Nexus_Tuner_Options {
	return resource_based_nexus_tuner_with_options(
		target_memory_usage,
		target_cpu_usage,
		{
			minimum_slots = 1,
			maximum_slots = maximum_nexus_task_slots,
			ramp_throttle = 50 * time.Millisecond,
		},
	)
}

poller_simple_maximum :: proc(maximum: int) -> Poller_Behavior {
	return {kind = .Simple_Maximum, simple_maximum = maximum}
}

poller_autoscaling :: proc(minimum, maximum, initial: int) -> Poller_Behavior {
	return {
		kind = .Autoscaling,
		minimum = minimum,
		maximum = maximum,
		initial = initial,
	}
}

// Worker_Options contains the Core-owned worker controls the Odin runtime can
// currently honor. Strings are borrowed only for worker_create.
Worker_Options :: struct {
	identity_override:                              string,
	max_activity_slots:                             int,
	max_nexus_task_slots:                           int,
	// Zero selects one slot without caching and two with caching. A positive
	// value configures Core admission explicitly; cached workers require two.
	max_workflow_task_slots:                        int,
	max_cached_workflows:                           u32,
	sticky_queue_schedule_to_start_timeout:         time.Duration,
	max_heartbeat_throttle_interval:                 time.Duration,
	default_heartbeat_throttle_interval:             time.Duration,
	max_worker_activities_per_second:                f64,
	max_task_queue_activities_per_second:            f64,
	graceful_shutdown_period:                        time.Duration,
	workflow_task_poller_behavior:                   Poller_Behavior,
	activity_task_poller_behavior:                   Poller_Behavior,
	nexus_task_poller_behavior:                      Poller_Behavior,
	nonsticky_to_sticky_poll_ratio:                  f32,
	max_eager_activity_reservations_per_workflow_task: u32,
	resource_based_activity_tuner:                   Resource_Based_Activity_Tuner_Options,
	resource_based_nexus_tuner:                      Resource_Based_Nexus_Tuner_Options,
	custom_activity_slot_supplier:                   Custom_Slot_Supplier,
}

worker_options_default :: proc() -> Worker_Options {
	return {
		max_activity_slots = 10,
		max_nexus_task_slots = 1,
		max_cached_workflows = 0,
		sticky_queue_schedule_to_start_timeout = 10 * time.Second,
		max_heartbeat_throttle_interval = 60 * time.Second,
		default_heartbeat_throttle_interval = time.Second,
		graceful_shutdown_period = DEFAULT_GRACEFUL_SHUTDOWN_PERIOD,
		nonsticky_to_sticky_poll_ratio = 0.2,
		// Match Core's language-SDK default. Core still clears the request when
		// no Activity slot is available, and the Service may decline it.
		max_eager_activity_reservations_per_workflow_task = 3,
	}
}

// Worker_Option_Bindings owns the address-stable structs referenced by the raw
// C options. Core copies them synchronously during worker_new.
Worker_Option_Bindings :: struct {
	raw_options:         raw.Worker_Options,
	workflow_simple:     raw.Poller_Behavior_Simple_Maximum,
	workflow_autoscaling: raw.Poller_Behavior_Autoscaling,
	activity_simple:     raw.Poller_Behavior_Simple_Maximum,
	activity_autoscaling: raw.Poller_Behavior_Autoscaling,
	nexus_simple:        raw.Poller_Behavior_Simple_Maximum,
	nexus_autoscaling:   raw.Poller_Behavior_Autoscaling,
}

Worker_Versioning_Mode :: enum {
	Unversioned,
	Deployment_Based,
	Legacy_Build_Id_Based,
}

Worker_Versioning_Behavior :: enum i32 {
	Unspecified = 0,
	Pinned = 1,
	Auto_Upgrade = 2,
}

// Worker_Versioning_Options is borrowed only for worker_create. Unversioned
// workers use DEFAULT_WORKER_BUILD_ID when build_id is empty. Deployment-based
// routing is experimental upstream and requires both identity fields when
// use_worker_versioning is true.
Worker_Versioning_Options :: struct {
	mode:                        Worker_Versioning_Mode,
	build_id:                    string,
	deployment_name:             string,
	use_worker_versioning:       bool,
	default_versioning_behavior: Worker_Versioning_Behavior,
}

// Nondeterminism_Policy is borrowed only for worker_create. Core normally
// fails and retries the Workflow Task after a replay mismatch. These opt-ins
// instead make nondeterminism fail the Workflow Execution.
Nondeterminism_Policy :: struct {
	fail_all_workflows: bool,
	fail_workflow_types: []string,
}

Get_System_Info_Result :: struct {
	protobuf: []u8,
	error:    Error,
}

byte_ref :: proc(value: string) -> raw.Byte_Array_Ref {
	return raw.Byte_Array_Ref{
		data = raw_data(value),
		size = c.size_t(len(value)),
	}
}

bytes_ref :: proc(value: []u8) -> raw.Byte_Array_Ref {
	if len(value) == 0 {
		return {}
	}
	return raw.Byte_Array_Ref{
		data = raw_data(value),
		size = c.size_t(len(value)),
	}
}

copy_core_bytes :: proc(value: ^raw.Byte_Array) -> []u8 {
	if value == nil || value.size == 0 {
		return nil
	}
	result := make([]u8, int(value.size))
	copy(result, ([^]u8)(value.data)[:int(value.size)])
	return result
}

runtime_log_ref_view :: proc(value: raw.Byte_Array_Ref) -> string {
	if value.data == nil || value.size == 0 {
		return ""
	}
	return string(([^]u8)(value.data)[:int(value.size)])
}

slot_ref_view :: proc(value: raw.Byte_Array_Ref) -> string {
	if value.data == nil || value.size == 0 {
		return ""
	}
	return string(([^]u8)(value.data)[:int(value.size)])
}

slot_kind_from_raw :: proc(value: raw.Slot_Kind_Type) -> Slot_Kind {
	switch value {
	case .Workflow:       return .Workflow
	case .Activity:       return .Activity
	case .Local_Activity: return .Local_Activity
	case .Nexus:          return .Nexus
	}
	return .Activity
}

slot_reserve_context_from_raw :: proc(value: ^raw.Slot_Reserve_Context) -> Slot_Reserve_Context {
	if value == nil {
		return {}
	}
	return {
		slot_kind = slot_kind_from_raw(value.slot_type),
		task_queue = slot_ref_view(value.task_queue),
		worker_identity = slot_ref_view(value.worker_identity),
		worker_build_id = slot_ref_view(value.worker_build_id),
		is_sticky = value.is_sticky,
	}
}

slot_info_from_raw :: proc(value: ^raw.Slot_Info) -> Slot_Info {
	if value == nil {
		return {}
	}
	switch value.tag {
	case .Workflow:
		return {
			slot_kind = .Workflow,
			workflow_type = slot_ref_view(value.workflow.workflow_type),
			is_sticky = value.workflow.is_sticky,
		}
	case .Activity:
		return {
			slot_kind = .Activity,
			activity_type = slot_ref_view(value.activity.activity_type),
		}
	case .Local_Activity:
		return {
			slot_kind = .Local_Activity,
			activity_type = slot_ref_view(value.local_activity.activity_type),
		}
	case .Nexus:
		return {
			slot_kind = .Nexus,
			nexus_operation = slot_ref_view(value.nexus.operation),
			nexus_service = slot_ref_view(value.nexus.service),
		}
	}
	return {}
}

custom_slot_supplier_enabled :: proc(value: Custom_Slot_Supplier) -> bool {
	return value.reserve != nil || value.cancel_reserve != nil ||
	       value.try_reserve != nil || value.mark_used != nil ||
	       value.release != nil || value.available_slots != nil ||
	       value.free != nil || value.user_data != nil
}

custom_slot_supplier_validate :: proc(value: Custom_Slot_Supplier) -> Error {
	if !custom_slot_supplier_enabled(value) {
		return {}
	}
	if value.reserve == nil || value.cancel_reserve == nil ||
	   value.try_reserve == nil || value.mark_used == nil || value.release == nil {
		return client_option_error(
			"custom slot supplier requires reserve, cancel_reserve, try_reserve, mark_used, and release callbacks",
		)
	}
	return {}
}

custom_slot_supplier_binding :: proc(user_data: rawptr) -> ^Custom_Slot_Supplier_Binding {
	return cast(^Custom_Slot_Supplier_Binding)user_data
}

custom_slot_reserve_bridge :: proc "c" (
	raw_context: ^raw.Slot_Reserve_Context,
	completion_context: ^raw.Slot_Reserve_Completion_Context,
	user_data: rawptr,
) {
	context = allocator_runtime.default_context()
	binding := custom_slot_supplier_binding(user_data)
	if binding == nil || binding.supplier.reserve == nil {
		return
	}
	view := slot_reserve_context_from_raw(raw_context)
	binding.supplier.reserve(
		binding.supplier.user_data,
		&view,
		{handle = completion_context},
	)
}

custom_slot_cancel_reserve_bridge :: proc "c" (
	completion_context: ^raw.Slot_Reserve_Completion_Context,
	user_data: rawptr,
) {
	context = allocator_runtime.default_context()
	binding := custom_slot_supplier_binding(user_data)
	if binding == nil || binding.supplier.cancel_reserve == nil {
		return
	}
	binding.supplier.cancel_reserve(
		binding.supplier.user_data,
		{handle = completion_context},
	)
}

custom_slot_try_reserve_bridge :: proc "c" (
	raw_context: ^raw.Slot_Reserve_Context,
	user_data: rawptr,
) -> uintptr {
	context = allocator_runtime.default_context()
	binding := custom_slot_supplier_binding(user_data)
	if binding == nil || binding.supplier.try_reserve == nil {
		return 0
	}
	view := slot_reserve_context_from_raw(raw_context)
	return binding.supplier.try_reserve(binding.supplier.user_data, &view)
}

custom_slot_mark_used_bridge :: proc "c" (
	raw_context: ^raw.Slot_Mark_Used_Context,
	user_data: rawptr,
) {
	context = allocator_runtime.default_context()
	binding := custom_slot_supplier_binding(user_data)
	if binding == nil || binding.supplier.mark_used == nil || raw_context == nil {
		return
	}
	info := slot_info_from_raw(&raw_context.slot_info)
	binding.supplier.mark_used(
		binding.supplier.user_data,
		&info,
		raw_context.slot_permit,
	)
}

custom_slot_release_bridge :: proc "c" (
	raw_context: ^raw.Slot_Release_Context,
	user_data: rawptr,
) {
	context = allocator_runtime.default_context()
	binding := custom_slot_supplier_binding(user_data)
	if binding == nil || binding.supplier.release == nil || raw_context == nil {
		return
	}
	info: Slot_Info
	info_pointer: ^Slot_Info
	if raw_context.slot_info != nil {
		info = slot_info_from_raw(raw_context.slot_info)
		info_pointer = &info
	}
	binding.supplier.release(
		binding.supplier.user_data,
		info_pointer,
		raw_context.slot_permit,
	)
}

custom_slot_available_bridge :: proc "c" (
	available_slots: ^uintptr,
	user_data: rawptr,
) -> bool {
	context = allocator_runtime.default_context()
	binding := custom_slot_supplier_binding(user_data)
	if binding == nil || binding.supplier.available_slots == nil || available_slots == nil {
		return false
	}
	available, known := binding.supplier.available_slots(binding.supplier.user_data)
	if known {
		available_slots^ = available
	}
	return known
}

custom_slot_free_bridge :: proc "c" (callbacks: ^raw.Custom_Slot_Supplier_Callbacks) {
	context = allocator_runtime.default_context()
	if callbacks == nil {
		return
	}
	// raw_callbacks is the first field, so Core's pointer is also the binding.
	binding := cast(^Custom_Slot_Supplier_Binding)callbacks
	if binding.released_by_core {
		return
	}
	binding.released_by_core = true
	if binding.supplier.free != nil {
		binding.supplier.free(binding.supplier.user_data)
	}
}

custom_slot_supplier_binding_create :: proc(
	supplier: Custom_Slot_Supplier,
) -> (binding: ^Custom_Slot_Supplier_Binding, error: Error) {
	validation_error := custom_slot_supplier_validate(supplier)
	if validation_error.message != nil {
		return nil, validation_error
	}
	if !custom_slot_supplier_enabled(supplier) {
		return nil, {}
	}
	heap := allocator_runtime.heap_allocator()
	allocated_binding, allocation_error := new(Custom_Slot_Supplier_Binding, heap)
	if allocation_error != nil {
		return nil, client_option_error("could not allocate custom slot supplier binding")
	}
	binding = allocated_binding
	binding.supplier = supplier
	binding.raw_callbacks = {
		reserve = custom_slot_reserve_bridge,
		cancel_reserve = custom_slot_cancel_reserve_bridge,
		try_reserve = custom_slot_try_reserve_bridge,
		mark_used = custom_slot_mark_used_bridge,
		release = custom_slot_release_bridge,
		free = custom_slot_free_bridge,
		user_data = binding,
	}
	if supplier.available_slots != nil {
		binding.raw_callbacks.available_slots = custom_slot_available_bridge
	}
	return binding, {}
}

custom_slot_supplier_binding_destroy :: proc(binding: ^Custom_Slot_Supplier_Binding) {
	if binding == nil {
		return
	}
	if !binding.released_by_core && binding.supplier.free != nil {
		binding.supplier.free(binding.supplier.user_data)
	}
	binding^ = {}
	free(binding, allocator_runtime.heap_allocator())
}

custom_slot_supplier_raw :: proc(binding: ^Custom_Slot_Supplier_Binding) -> raw.Slot_Supplier {
	if binding == nil {
		return {}
	}
	return {
		tag = .Custom,
		custom = {callbacks = &binding.raw_callbacks},
	}
}

// A successful call consumes the reservation handle from Core's perspective.
// Calling either completion function again with the same handle is memory
// corruption in the upstream C ABI and is therefore forbidden.
slot_reservation_complete :: proc(reservation: Slot_Reservation, permit: uintptr) -> bool {
	if reservation.handle == nil || permit == 0 {
		return false
	}
	return raw.complete_async_reserve(reservation.handle, permit)
}

slot_reservation_cancel_complete :: proc(reservation: Slot_Reservation) -> bool {
	if reservation.handle == nil {
		return false
	}
	return raw.complete_async_cancel_reserve(reservation.handle)
}

runtime_log_clone_ref :: proc(value: raw.Byte_Array_Ref) -> (owned: string, ok: bool) {
	view := runtime_log_ref_view(value)
	if len(view) == 0 {
		return "", true
	}
	allocation_error: allocator_runtime.Allocator_Error
	owned, allocation_error = strings.clone(view, allocator_runtime.heap_allocator())
	return owned, allocation_error == nil
}

runtime_forwarded_log_callback :: proc "c" (level: raw.Forwarded_Log_Level, source: ^raw.Forwarded_Log) {
	context = allocator_runtime.default_context()
	if source == nil {
		return
	}
	sync.mutex_lock(&runtime_log_route_mutex)
	defer sync.mutex_unlock(&runtime_log_route_mutex)
	if runtime_log_route.callback == nil {
		return
	}

	heap := allocator_runtime.heap_allocator()
	log := Runtime_Log{
		level = level,
		timestamp_millis = raw.forwarded_log_timestamp_millis(source),
	}
	ok := true
	log.target, ok = runtime_log_clone_ref(raw.forwarded_log_target(source))
	if ok do log.message, ok = runtime_log_clone_ref(raw.forwarded_log_message(source))
	if ok do log.fields_json, ok = runtime_log_clone_ref(raw.forwarded_log_fields_json(source))
	defer delete(log.target, heap)
	defer delete(log.message, heap)
	defer delete(log.fields_json, heap)
	if !ok {
		return
	}
	runtime_log_route.callback(runtime_log_route.user_data, &log)
}

runtime_log_route_install :: proc(options: ^Runtime_Logging_Options) -> (installed: bool, error: Error) {
	if options == nil || options.forward_to == nil {
		return false, {}
	}
	sync.mutex_lock(&runtime_log_route_mutex)
	defer sync.mutex_unlock(&runtime_log_route_mutex)
	if runtime_log_route.callback != nil {
		return false, client_option_error("only one Core log-forwarding Runtime may exist in a process")
	}
	runtime_log_route = {callback = options.forward_to, user_data = options.user_data}
	return true, {}
}

runtime_log_route_remove :: proc() {
	sync.mutex_lock(&runtime_log_route_mutex)
	defer sync.mutex_unlock(&runtime_log_route_mutex)
	runtime_log_route = {}
}

error_free :: proc(error: ^Error) {
	if error == nil {
		return
	}
	delete(error.message)
	delete(error.details)
	error.message = nil
	error.details = nil
	error.status_code = 0
}

runtime_newline_map_encode :: proc(
	entries: []Runtime_Key_Value,
	name: string,
) -> (encoded: [dynamic]u8, error: Error) {
	for entry, index in entries {
		if len(entry.key) == 0 {
			return nil, client_option_error(fmt.tprintf("%s key must not be empty", name))
		}
		if strings.contains(entry.key, "\n") || strings.contains(entry.value, "\n") {
			return nil, client_option_error(fmt.tprintf("%s keys and values must not contain newlines", name))
		}
		if index > 0 {
			append(&encoded, '\n')
		}
		append(&encoded, ..transmute([]u8)entry.key)
		append(&encoded, '\n')
		append(&encoded, ..transmute([]u8)entry.value)
	}
	return
}

runtime_histogram_overrides_validate :: proc(value: string) -> Error {
	if len(value) == 0 {
		return {}
	}
	parts := strings.split(value, "\n")
	defer delete(parts)
	if len(parts) % 2 != 0 {
		return client_option_error("histogram bucket overrides must contain metric/value pairs")
	}
	for index := 0; index < len(parts); index += 2 {
		if len(parts[index]) == 0 || len(parts[index + 1]) == 0 {
			return client_option_error("histogram bucket override metric and bucket list must not be empty")
		}
	}
	return {}
}

runtime_options_validate :: proc(options: Runtime_Options) -> Error {
	if options.prometheus != nil && options.opentelemetry != nil {
		return client_option_error("exactly one Temporal metrics exporter may be configured")
	}
	if options.worker_heartbeat_interval < 0 {
		return client_option_error("worker heartbeat interval must not be negative")
	}
	if options.logging != nil && len(options.logging.filter) == 0 {
		return client_option_error("Core logging filter must not be empty")
	}
	if options.logging != nil && options.logging.forward_to == nil && options.logging.user_data != nil {
		return client_option_error("Core logging user data requires a forwarding callback")
	}
	if options.prometheus != nil {
		if len(options.prometheus.bind_address) == 0 {
			return client_option_error("Prometheus bind address must not be empty")
		}
		override_error := runtime_histogram_overrides_validate(
			options.prometheus.histogram_bucket_overrides,
		)
		if override_error.message != nil { return override_error }
	}
	if options.opentelemetry != nil {
		if len(options.opentelemetry.url) == 0 {
			return client_option_error("OpenTelemetry URL must not be empty")
		}
		if options.opentelemetry.metric_periodicity < 0 {
			return client_option_error("OpenTelemetry metric periodicity must not be negative")
		}
		if options.opentelemetry.metric_temporality != .Cumulative &&
		   options.opentelemetry.metric_temporality != .Delta {
			return client_option_error("OpenTelemetry metric temporality must be cumulative or delta")
		}
		if options.opentelemetry.protocol != .Grpc && options.opentelemetry.protocol != .Http {
			return client_option_error("OpenTelemetry protocol must be gRPC or HTTP")
		}
		override_error := runtime_histogram_overrides_validate(
			options.opentelemetry.histogram_bucket_overrides,
		)
		if override_error.message != nil { return override_error }
	}
	for info in options.runtime_info {
		if info.runtime_type == .Unspecified {
			return client_option_error("runtime info type must not be unspecified")
		}
	}
	return {}
}

runtime_option_bindings_free :: proc(bindings: ^Runtime_Option_Bindings) {
	if bindings == nil {
		return
	}
	delete(bindings.runtime_info)
	delete(bindings.global_tags)
	delete(bindings.opentelemetry_headers)
	bindings^ = {}
}

runtime_option_bindings_init :: proc(
	bindings: ^Runtime_Option_Bindings,
	options: Runtime_Options,
) -> Error {
	if bindings == nil {
		return client_option_error("runtime option bindings must not be nil")
	}
	bindings^ = {}
	validation_error := runtime_options_validate(options)
	if validation_error.message != nil {
		return validation_error
	}

	heartbeat_millis, heartbeat_error := worker_duration_millis(
		options.worker_heartbeat_interval,
		"worker heartbeat interval",
	)
	if heartbeat_error.message != nil { return heartbeat_error }
	bindings.raw_options.worker_heartbeat_interval_millis = heartbeat_millis
	bindings.raw_options.disable_environment_info = options.disable_environment_info
	for info in options.runtime_info {
		append(&bindings.runtime_info, raw.Runtime_Info{
			runtime_type = info.runtime_type,
			version = byte_ref(info.version),
		})
	}
	if len(bindings.runtime_info) > 0 {
		bindings.raw_options.runtime_info = {
			data = raw_data(bindings.runtime_info[:]),
			size = c.size_t(len(bindings.runtime_info)),
		}
	}

	if options.logging == nil && options.prometheus == nil && options.opentelemetry == nil {
		return {}
	}
	bindings.raw_options.telemetry = &bindings.telemetry
	if options.logging != nil {
		bindings.logging.filter = byte_ref(options.logging.filter)
		if options.logging.forward_to != nil {
			bindings.logging.forward_to = runtime_forwarded_log_callback
		}
		bindings.telemetry.logging = &bindings.logging
	}
	if options.prometheus == nil && options.opentelemetry == nil {
		return {}
	}

	global_tags, global_tags_error := runtime_newline_map_encode(
		options.metrics.global_tags,
		"metric tag",
	)
	if global_tags_error.message != nil { return global_tags_error }
	bindings.global_tags = global_tags
	bindings.metrics.attach_service_name = options.metrics.attach_service_name
	bindings.metrics.global_tags = bytes_ref(bindings.global_tags[:])
	bindings.metrics.metric_prefix = byte_ref(options.metrics.metric_prefix)
	bindings.telemetry.metrics = &bindings.metrics
	if options.prometheus != nil {
		prometheus := options.prometheus
		bindings.prometheus = {
			bind_address = byte_ref(prometheus.bind_address),
			counters_total_suffix = prometheus.counters_total_suffix,
			unit_suffix = prometheus.unit_suffix,
			durations_as_seconds = prometheus.durations_as_seconds,
			histogram_bucket_overrides = byte_ref(prometheus.histogram_bucket_overrides),
		}
		bindings.metrics.prometheus = &bindings.prometheus
		return {}
	}

	opentelemetry := options.opentelemetry
	headers, headers_error := runtime_newline_map_encode(opentelemetry.headers, "OpenTelemetry header")
	if headers_error.message != nil { return headers_error }
	bindings.opentelemetry_headers = headers
	periodicity_millis, periodicity_error := worker_duration_millis(
		opentelemetry.metric_periodicity,
		"OpenTelemetry metric periodicity",
	)
	if periodicity_error.message != nil { return periodicity_error }
	if periodicity_millis > u64(max(u32)) {
		return client_option_error("OpenTelemetry metric periodicity exceeds the Core ABI maximum")
	}
	bindings.opentelemetry = {
		url = byte_ref(opentelemetry.url),
		headers = bytes_ref(bindings.opentelemetry_headers[:]),
		metric_periodicity_millis = u32(periodicity_millis),
		metric_temporality = opentelemetry.metric_temporality,
		durations_as_seconds = opentelemetry.durations_as_seconds,
		protocol = opentelemetry.protocol,
		histogram_bucket_overrides = byte_ref(opentelemetry.histogram_bucket_overrides),
	}
	bindings.metrics.opentelemetry = &bindings.opentelemetry
	return {}
}

runtime_create :: proc() -> (runtime: Runtime, error: Error) {
	return runtime_create_with_options(runtime_options_default())
}

runtime_create_with_options :: proc(options: Runtime_Options) -> (runtime: Runtime, error: Error) {
	bindings := Runtime_Option_Bindings{}
	bindings_error := runtime_option_bindings_init(&bindings, options)
	if bindings_error.message != nil {
		runtime_option_bindings_free(&bindings)
		return {}, bindings_error
	}
	defer runtime_option_bindings_free(&bindings)
	forwarding_installed, forwarding_error := runtime_log_route_install(options.logging)
	if forwarding_error.message != nil {
		return {}, forwarding_error
	}
	result := raw.runtime_new(&bindings.raw_options)
	if result.fail != nil {
		error.message = copy_core_bytes(result.fail)
		raw.byte_array_free(result.runtime, result.fail)
		raw.runtime_free(result.runtime)
		if forwarding_installed do runtime_log_route_remove()
		return
	}
	runtime.handle = result.runtime
	runtime.forwards_core_logs = forwarding_installed
	runtime.metrics_enabled = options.prometheus != nil || options.opentelemetry != nil
	return
}

runtime_close :: proc(runtime: ^Runtime) {
	if runtime == nil || runtime.handle == nil {
		return
	}
	raw.runtime_free(runtime.handle)
	runtime.handle = nil
	runtime.metrics_enabled = false
	if runtime.forwards_core_logs {
		runtime_log_route_remove()
		runtime.forwards_core_logs = false
	}
}

Connect_State :: struct {
	done:    sync.One_Shot_Event,
	success: ^raw.Connection,
	fail:    ^raw.Byte_Array,
}

connect_callback :: proc "c" (user_data: rawptr, success: ^raw.Connection, fail: ^raw.Byte_Array) {
	state := cast(^Connect_State)user_data
	state.success = success
	state.fail = fail
	sync.one_shot_event_signal(&state.done)
}

wait_until :: proc(done: ^sync.One_Shot_Event) {
	sync.one_shot_event_wait(done)
}

client_connect :: proc(runtime: ^Runtime, target_url := "http://localhost:7233") -> (client: Client, error: Error) {
	return client_connect_with_options(runtime, {
		target_url = target_url,
		client_name = "temporal-sdk-odin",
		client_version = "0.0.1-experimental",
		identity = "temporal-sdk-odin@local",
		grpc_compression = .Gzip,
	})
}

client_connect_with_options :: proc(
	runtime: ^Runtime,
	options: Client_Connection_Options,
) -> (client: Client, error: Error) {
	if runtime == nil || runtime.handle == nil {
		error.message = bytes.clone(transmute([]u8)string("runtime is not initialized"))
		return
	}
	bindings := Client_Connection_Bindings{}
	bindings_error := client_connection_bindings_init(&bindings, options)
	if bindings_error.message != nil {
		return {}, bindings_error
	}
	defer client_connection_bindings_free(&bindings)
	state := Connect_State{}
	raw.client_connect(runtime.handle, &bindings.raw_options, &state, connect_callback)
	wait_until(&state.done)

	if state.fail != nil {
		error.message = copy_core_bytes(state.fail)
		raw.byte_array_free(runtime.handle, state.fail)
		return
	}
	client.handle = state.success
	client.runtime = runtime
	return
}

client_update_metadata :: proc(
	client: ^Client,
	metadata: []Client_Metadata_Entry,
	binary_metadata: []Client_Metadata_Entry = nil,
) -> Error {
	if client == nil || client.handle == nil {
		return client_option_error("Temporal Client is not connected")
	}
	bindings := Client_Connection_Bindings{}
	bindings_error := client_connection_bindings_init(&bindings, {
		target_url = "http://localhost:7233",
		client_name = "temporal-sdk-odin",
		client_version = "0.0.1-experimental",
		identity = "temporal-sdk-odin@local",
		metadata = metadata,
		binary_metadata = binary_metadata,
	})
	if bindings_error.message != nil {
		return bindings_error
	}
	defer client_connection_bindings_free(&bindings)
	raw.client_update_metadata(client.handle, bindings.raw_options.metadata)
	raw.client_update_binary_metadata(client.handle, bindings.raw_options.binary_metadata)
	return {}
}

client_update_api_key :: proc(client: ^Client, api_key: string) -> Error {
	if client == nil || client.handle == nil {
		return client_option_error("Temporal Client is not connected")
	}
	if len(api_key) > 0 && !client_text_metadata_value_valid(transmute([]u8)api_key) {
		return client_option_error("Temporal API key is not valid ASCII metadata")
	}
	raw.client_update_api_key(client.handle, byte_ref(api_key))
	return {}
}

client_close :: proc(client: ^Client) {
	if client == nil || client.handle == nil {
		return
	}
	raw.client_free(client.handle)
	client.handle = nil
	client.runtime = nil
}

fixed_slot_supplier :: proc(count: uintptr) -> raw.Slot_Supplier {
	return raw.Slot_Supplier{
		tag = .Fixed_Size,
		fixed_size = raw.Fixed_Size_Slot_Supplier{num_slots = count},
	}
}

worker_versioning_options_validate :: proc(options: Worker_Versioning_Options) -> Error {
	if int(options.mode) < int(Worker_Versioning_Mode.Unversioned) ||
	   int(options.mode) > int(Worker_Versioning_Mode.Legacy_Build_Id_Based) {
		return client_option_error("worker versioning mode is invalid")
	}
	if int(options.default_versioning_behavior) <
	     int(Worker_Versioning_Behavior.Unspecified) ||
	   int(options.default_versioning_behavior) >
	     int(Worker_Versioning_Behavior.Auto_Upgrade) {
		return client_option_error("worker default versioning behavior is invalid")
	}
	switch options.mode {
	case .Unversioned:
		if len(options.deployment_name) > 0 || options.use_worker_versioning ||
		   options.default_versioning_behavior != .Unspecified {
			return client_option_error(
				"unversioned workers cannot set deployment routing options",
			)
		}
	case .Deployment_Based:
		if len(options.build_id) == 0 {
			return client_option_error("deployment-based workers require a build ID")
		}
		if options.use_worker_versioning && len(options.deployment_name) == 0 {
			return client_option_error(
				"deployment-based routing requires a deployment name",
			)
		}
		if !options.use_worker_versioning &&
		   options.default_versioning_behavior != .Unspecified {
			return client_option_error(
				"default versioning behavior requires deployment-based routing",
			)
		}
	case .Legacy_Build_Id_Based:
		if len(options.build_id) == 0 {
			return client_option_error("legacy worker versioning requires a build ID")
		}
		if len(options.deployment_name) > 0 || options.use_worker_versioning ||
		   options.default_versioning_behavior != .Unspecified {
			return client_option_error(
				"legacy worker versioning cannot set deployment routing options",
			)
		}
	}
	return {}
}

worker_versioning_strategy_raw :: proc(
	options: Worker_Versioning_Options,
) -> raw.Worker_Versioning_Strategy {
	switch options.mode {
	case .Deployment_Based:
		return raw.Worker_Versioning_Strategy{
			tag = .Deployment_Based,
			deployment_based = raw.Worker_Deployment_Options{
				version = raw.Worker_Deployment_Version{
					deployment_name = byte_ref(options.deployment_name),
					build_id = byte_ref(options.build_id),
				},
				use_worker_versioning = options.use_worker_versioning,
				default_versioning_behavior = i32(options.default_versioning_behavior),
			},
		}
	case .Legacy_Build_Id_Based:
		return raw.Worker_Versioning_Strategy{
			tag = .Legacy_Build_Id_Based,
			legacy_build_id_based = raw.Legacy_Build_Id_Based_Strategy{
				build_id = byte_ref(options.build_id),
			},
		}
	case .Unversioned:
	}
	build_id := options.build_id
	if len(build_id) == 0 {
		build_id = DEFAULT_WORKER_BUILD_ID
	}
	return raw.Worker_Versioning_Strategy{
		tag = .None,
		none = raw.Worker_Versioning_None{build_id = byte_ref(build_id)},
	}
}

nondeterminism_policy_validate :: proc(policy: Nondeterminism_Policy) -> Error {
	for workflow_type in policy.fail_workflow_types {
		if len(workflow_type) == 0 {
			return client_option_error(
				"nondeterminism failure Workflow types must not be empty",
			)
		}
	}
	return {}
}

nondeterminism_policy_raw :: proc(
	policy: Nondeterminism_Policy,
	workflow_type_refs: []raw.Byte_Array_Ref,
) -> (bool, raw.Byte_Array_Ref_Array) {
	assert(len(workflow_type_refs) >= len(policy.fail_workflow_types))
	for workflow_type, index in policy.fail_workflow_types {
		workflow_type_refs[index] = byte_ref(workflow_type)
	}
	result := raw.Byte_Array_Ref_Array{size = c.size_t(len(policy.fail_workflow_types))}
	if len(policy.fail_workflow_types) > 0 {
		result.data = raw_data(workflow_type_refs)
	}
	return policy.fail_all_workflows, result
}

// The C bridge accepts whole milliseconds. Round positive fractions upward so
// a configured grace period never expires earlier than the Odin duration.
worker_graceful_shutdown_period_millis :: proc(
	period: time.Duration,
) -> (millis: u64, error: Error) {
	if period < 0 {
		return 0, client_option_error("graceful shutdown period must not be negative")
	}
	millis = u64(period / time.Millisecond)
	if period % time.Millisecond != 0 {
		millis += 1
	}
	return
}

worker_duration_millis :: proc(
	value: time.Duration,
	name: string,
) -> (millis: u64, error: Error) {
	if value < 0 {
		message := fmt.tprintf("%s must not be negative", name)
		return 0, client_option_error(message)
	}
	millis = u64(value / time.Millisecond)
	if value % time.Millisecond != 0 {
		millis += 1
	}
	return
}

poller_behavior_validate :: proc(value: Poller_Behavior, name: string) -> Error {
	switch value.kind {
	case .Default:
		return {}
	case .Simple_Maximum:
		if value.simple_maximum < 1 {
			return client_option_error(fmt.tprintf("%s simple maximum must be at least one", name))
		}
	case .Autoscaling:
		if value.minimum < 1 {
			return client_option_error(fmt.tprintf("%s autoscaling minimum must be at least one", name))
		}
		if value.maximum < value.minimum {
			return client_option_error(fmt.tprintf("%s autoscaling maximum must not be below minimum", name))
		}
		if value.initial < value.minimum || value.initial > value.maximum {
			return client_option_error(fmt.tprintf("%s autoscaling initial must be between minimum and maximum", name))
		}
	}
	return {}
}

resource_based_slot_options_validate :: proc(
	options: Resource_Based_Slot_Options,
	name: string,
) -> Error {
	if options.minimum_slots < 1 {
		return client_option_error(fmt.tprintf("%s minimum slots must be at least one", name))
	}
	if options.maximum_slots < options.minimum_slots {
		return client_option_error(fmt.tprintf("%s maximum slots must not be below minimum", name))
	}
	_, duration_error := worker_duration_millis(
		options.ramp_throttle,
		fmt.tprintf("%s ramp throttle", name),
	)
	return duration_error
}

resource_based_activity_tuner_validate :: proc(
	options: Resource_Based_Activity_Tuner_Options,
) -> Error {
	if !options.enabled {
		return {}
	}
	if math.is_nan(options.target_memory_usage) ||
	   math.is_inf(options.target_memory_usage) ||
	   options.target_memory_usage <= 0 || options.target_memory_usage > 1 {
		return client_option_error(
			"resource-based target memory usage must be finite and between zero (exclusive) and one",
		)
	}
	if math.is_nan(options.target_cpu_usage) || math.is_inf(options.target_cpu_usage) ||
	   options.target_cpu_usage <= 0 || options.target_cpu_usage > 1 {
		return client_option_error(
			"resource-based target CPU usage must be finite and between zero (exclusive) and one",
		)
	}
	activity_error := resource_based_slot_options_validate(
		options.activity_slots,
		"resource-based Activity supplier",
	)
	if activity_error.message != nil {
		return activity_error
	}
	return resource_based_slot_options_validate(
		options.local_activity_slots,
		"resource-based Local Activity supplier",
	)
}

resource_based_nexus_tuner_validate :: proc(
	options: Resource_Based_Nexus_Tuner_Options,
) -> Error {
	if !options.enabled {
		return {}
	}
	if math.is_nan(options.target_memory_usage) ||
	   math.is_inf(options.target_memory_usage) ||
	   options.target_memory_usage <= 0 || options.target_memory_usage > 1 {
		return client_option_error(
			"resource-based Nexus target memory usage must be finite and between zero (exclusive) and one",
		)
	}
	if math.is_nan(options.target_cpu_usage) || math.is_inf(options.target_cpu_usage) ||
	   options.target_cpu_usage <= 0 || options.target_cpu_usage > 1 {
		return client_option_error(
			"resource-based Nexus target CPU usage must be finite and between zero (exclusive) and one",
		)
	}
	return resource_based_slot_options_validate(
		options.nexus_task_slots,
		"resource-based Nexus task supplier",
	)
}

worker_options_validate :: proc(options: Worker_Options) -> Error {
	custom_activity_enabled := custom_slot_supplier_enabled(
		options.custom_activity_slot_supplier,
	)
	if !options.resource_based_activity_tuner.enabled && !custom_activity_enabled &&
	   options.max_activity_slots <= 0 {
		return client_option_error("max activity slots must be positive")
	}
	if !options.resource_based_nexus_tuner.enabled && options.max_nexus_task_slots <= 0 {
		return client_option_error("max Nexus task slots must be positive")
	}
	if options.max_workflow_task_slots < 0 {
		return client_option_error("max Workflow task slots must not be negative")
	}
	if options.max_cached_workflows > 0 && options.max_workflow_task_slots == 1 {
		return client_option_error(
			"max Workflow task slots must be at least two when Workflow caching is enabled",
		)
	}
	_, sticky_error := worker_duration_millis(
		options.sticky_queue_schedule_to_start_timeout,
		"sticky queue schedule-to-start timeout",
	)
	if sticky_error.message != nil {
		return sticky_error
	}
	_, max_heartbeat_error := worker_duration_millis(
		options.max_heartbeat_throttle_interval,
		"maximum heartbeat throttle interval",
	)
	if max_heartbeat_error.message != nil {
		return max_heartbeat_error
	}
	_, default_heartbeat_error := worker_duration_millis(
		options.default_heartbeat_throttle_interval,
		"default heartbeat throttle interval",
	)
	if default_heartbeat_error.message != nil {
		return default_heartbeat_error
	}
	_, shutdown_error := worker_graceful_shutdown_period_millis(
		options.graceful_shutdown_period,
	)
	if shutdown_error.message != nil {
		return shutdown_error
	}
	worker_rate := options.max_worker_activities_per_second
	if math.is_nan(worker_rate) || math.is_inf(worker_rate) || worker_rate < 0 ||
	   (worker_rate != 0 && worker_rate < math.F64_MIN) {
		return client_option_error("maximum worker Activities per second must be finite and non-negative")
	}
	task_queue_rate := options.max_task_queue_activities_per_second
	if math.is_nan(task_queue_rate) || math.is_inf(task_queue_rate) || task_queue_rate < 0 {
		return client_option_error("maximum task-queue Activities per second must be finite and non-negative")
	}
	ratio := options.nonsticky_to_sticky_poll_ratio
	if math.is_nan(ratio) || math.is_inf(ratio) || ratio < 0 || ratio > 1 {
		return client_option_error("nonsticky-to-sticky poll ratio must be finite and between zero and one")
	}
	workflow_poller_error := poller_behavior_validate(
		options.workflow_task_poller_behavior,
		"Workflow task poller",
	)
	if workflow_poller_error.message != nil {
		return workflow_poller_error
	}
	activity_poller_error := poller_behavior_validate(
		options.activity_task_poller_behavior,
		"Activity task poller",
	)
	if activity_poller_error.message != nil {
		return activity_poller_error
	}
	nexus_poller_error := poller_behavior_validate(
		options.nexus_task_poller_behavior,
		"Nexus task poller",
	)
	if nexus_poller_error.message != nil {
		return nexus_poller_error
	}
	if options.max_cached_workflows > 0 &&
	   options.workflow_task_poller_behavior.kind == .Simple_Maximum &&
	   options.workflow_task_poller_behavior.simple_maximum < 2 {
		return client_option_error("Workflow task simple poller must be at least two when Workflow caching is enabled")
	}
	activity_tuner_error :=
		resource_based_activity_tuner_validate(options.resource_based_activity_tuner)
	if activity_tuner_error.message != nil {
		return activity_tuner_error
	}
	nexus_tuner_error := resource_based_nexus_tuner_validate(options.resource_based_nexus_tuner)
	if nexus_tuner_error.message != nil {
		return nexus_tuner_error
	}
	custom_activity_error := custom_slot_supplier_validate(
		options.custom_activity_slot_supplier,
	)
	if custom_activity_error.message != nil {
		return custom_activity_error
	}
	if custom_activity_enabled && options.resource_based_activity_tuner.enabled {
		return client_option_error(
			"custom and resource-based Activity slot suppliers cannot both be enabled",
		)
	}
	if options.resource_based_activity_tuner.enabled &&
	   options.resource_based_nexus_tuner.enabled &&
	   (options.resource_based_activity_tuner.target_memory_usage !=
	      options.resource_based_nexus_tuner.target_memory_usage ||
	    options.resource_based_activity_tuner.target_cpu_usage !=
	      options.resource_based_nexus_tuner.target_cpu_usage) {
		return client_option_error(
			"all resource-based slot suppliers must use identical memory and CPU targets",
		)
	}
	return {}
}

resource_based_slot_supplier :: proc(
	options: Resource_Based_Slot_Options,
	tuner_options: raw.Resource_Based_Tuner_Options,
) -> (raw.Slot_Supplier, Error) {
	ramp_throttle_ms, duration_error := worker_duration_millis(
		options.ramp_throttle,
		"resource-based slot supplier ramp throttle",
	)
	if duration_error.message != nil {
		return {}, duration_error
	}
	return raw.Slot_Supplier{
		tag = .Resource_Based,
		resource_based = raw.Resource_Based_Slot_Supplier{
			minimum_slots = uintptr(options.minimum_slots),
			maximum_slots = uintptr(options.maximum_slots),
			ramp_throttle_ms = ramp_throttle_ms,
			tuner_options = tuner_options,
		},
	}, {}
}

poller_behavior_bind :: proc(
	value: Poller_Behavior,
	simple: ^raw.Poller_Behavior_Simple_Maximum,
	autoscaling: ^raw.Poller_Behavior_Autoscaling,
) -> raw.Poller_Behavior {
	switch value.kind {
	case .Default:
		return {}
	case .Simple_Maximum:
		simple.simple_maximum = uintptr(value.simple_maximum)
		return {simple_maximum = simple}
	case .Autoscaling:
		autoscaling.minimum = uintptr(value.minimum)
		autoscaling.maximum = uintptr(value.maximum)
		autoscaling.initial = uintptr(value.initial)
		return {autoscaling = autoscaling}
	}
	return {}
}

worker_option_bindings_init :: proc(
	bindings: ^Worker_Option_Bindings,
	options: Worker_Options,
) -> Error {
	if bindings == nil {
		return client_option_error("worker option bindings must not be nil")
	}
	bindings^ = {}
	validation_error := worker_options_validate(options)
	if validation_error.message != nil {
		return validation_error
	}
	sticky_ms, sticky_error := worker_duration_millis(
		options.sticky_queue_schedule_to_start_timeout,
		"sticky queue schedule-to-start timeout",
	)
	if sticky_error.message != nil { return sticky_error }
	max_heartbeat_ms, max_heartbeat_error := worker_duration_millis(
		options.max_heartbeat_throttle_interval,
		"maximum heartbeat throttle interval",
	)
	if max_heartbeat_error.message != nil { return max_heartbeat_error }
	default_heartbeat_ms, default_heartbeat_error := worker_duration_millis(
		options.default_heartbeat_throttle_interval,
		"default heartbeat throttle interval",
	)
	if default_heartbeat_error.message != nil { return default_heartbeat_error }
	shutdown_ms, shutdown_error := worker_graceful_shutdown_period_millis(
		options.graceful_shutdown_period,
	)
	if shutdown_error.message != nil { return shutdown_error }

	bindings.raw_options.identity_override = byte_ref(options.identity_override)
	bindings.raw_options.max_cached_workflows = options.max_cached_workflows
	workflow_slots := uintptr(1)
	if options.max_cached_workflows > 0 {
		// Cached workers need at least two Workflow slots so a cache eviction
		// cannot starve the Workflow poll stream.
		workflow_slots = 2
	}
	if options.max_workflow_task_slots > 0 {
		workflow_slots = uintptr(options.max_workflow_task_slots)
	}
	fixed_activity_slots := max(options.max_activity_slots, 1)
	bindings.raw_options.tuner = raw.Tuner_Holder{
		workflow_slot_supplier       = fixed_slot_supplier(workflow_slots),
		activity_slot_supplier       = fixed_slot_supplier(uintptr(fixed_activity_slots)),
		local_activity_slot_supplier = fixed_slot_supplier(1),
		nexus_task_slot_supplier     = fixed_slot_supplier(uintptr(options.max_nexus_task_slots)),
	}
	if options.resource_based_activity_tuner.enabled {
		tuner := options.resource_based_activity_tuner
		raw_tuner_options := raw.Resource_Based_Tuner_Options{
			target_memory_usage = tuner.target_memory_usage,
			target_cpu_usage = tuner.target_cpu_usage,
		}
		activity_supplier, activity_supplier_error := resource_based_slot_supplier(
			tuner.activity_slots,
			raw_tuner_options,
		)
		if activity_supplier_error.message != nil {
			return activity_supplier_error
		}
		local_activity_supplier, local_activity_supplier_error :=
			resource_based_slot_supplier(tuner.local_activity_slots, raw_tuner_options)
		if local_activity_supplier_error.message != nil {
			return local_activity_supplier_error
		}
		bindings.raw_options.tuner.activity_slot_supplier = activity_supplier
		bindings.raw_options.tuner.local_activity_slot_supplier = local_activity_supplier
	}
	if options.resource_based_nexus_tuner.enabled {
		tuner := options.resource_based_nexus_tuner
		raw_tuner_options := raw.Resource_Based_Tuner_Options{
			target_memory_usage = tuner.target_memory_usage,
			target_cpu_usage = tuner.target_cpu_usage,
		}
		nexus_supplier, nexus_supplier_error := resource_based_slot_supplier(
			tuner.nexus_task_slots,
			raw_tuner_options,
		)
		if nexus_supplier_error.message != nil {
			return nexus_supplier_error
		}
		bindings.raw_options.tuner.nexus_task_slot_supplier = nexus_supplier
	}
	bindings.raw_options.sticky_queue_schedule_to_start_timeout_ms = sticky_ms
	bindings.raw_options.max_heartbeat_throttle_interval_ms = max_heartbeat_ms
	bindings.raw_options.default_heartbeat_throttle_interval_ms = default_heartbeat_ms
	bindings.raw_options.max_activities_per_second = options.max_worker_activities_per_second
	bindings.raw_options.max_task_queue_activities_per_second = options.max_task_queue_activities_per_second
	bindings.raw_options.graceful_shutdown_period_ms = shutdown_ms
	bindings.raw_options.workflow_task_poller_behavior = poller_behavior_bind(
		options.workflow_task_poller_behavior,
		&bindings.workflow_simple,
		&bindings.workflow_autoscaling,
	)
	bindings.raw_options.nonsticky_to_sticky_poll_ratio = options.nonsticky_to_sticky_poll_ratio
	bindings.raw_options.activity_task_poller_behavior = poller_behavior_bind(
		options.activity_task_poller_behavior,
		&bindings.activity_simple,
		&bindings.activity_autoscaling,
	)
	bindings.raw_options.nexus_task_poller_behavior = poller_behavior_bind(
		options.nexus_task_poller_behavior,
		&bindings.nexus_simple,
		&bindings.nexus_autoscaling,
	)
	bindings.raw_options.max_eager_activity_reservations_per_wf_task =
		options.max_eager_activity_reservations_per_workflow_task
	return {}
}

worker_create :: proc(
	client: ^Client,
	task_queue: string,
	worker_options: Worker_Options,
	namespace := "default",
	enable_workflows := false,
	enable_local_activities := false,
	enable_remote_activities := true,
	enable_nexus := false,
	versioning := Worker_Versioning_Options{},
	nondeterminism_policy := Nondeterminism_Policy{},
) -> (worker: Worker, error: Error) {
	if client == nil || client.handle == nil || client.runtime == nil {
		error.message = bytes.clone(transmute([]u8)string("client is not initialized"))
		return
	}
	if len(task_queue) == 0 {
		error.message = bytes.clone(transmute([]u8)string("task queue must not be empty"))
		return
	}
	bindings := Worker_Option_Bindings{}
	bindings_error := worker_option_bindings_init(&bindings, worker_options)
	if bindings_error.message != nil {
		return {}, bindings_error
	}
	versioning_error := worker_versioning_options_validate(versioning)
	if versioning_error.message != nil {
		return {}, versioning_error
	}
	nondeterminism_error := nondeterminism_policy_validate(nondeterminism_policy)
	if nondeterminism_error.message != nil {
		return {}, nondeterminism_error
	}
	custom_activity_binding, custom_activity_error := custom_slot_supplier_binding_create(
		worker_options.custom_activity_slot_supplier,
	)
	if custom_activity_error.message != nil {
		return {}, custom_activity_error
	}
	workflow_type_refs := make(
		[]raw.Byte_Array_Ref,
		len(nondeterminism_policy.fail_workflow_types),
	)
	defer delete(workflow_type_refs)
	fail_all_workflows, fail_workflow_types := nondeterminism_policy_raw(
		nondeterminism_policy,
		workflow_type_refs,
	)

	options := &bindings.raw_options
	options.namespace = byte_ref(namespace)
	options.task_queue = byte_ref(task_queue)
	options.versioning_strategy = worker_versioning_strategy_raw(versioning)
	options.task_types = raw.Worker_Task_Types{
		enable_workflows = enable_workflows,
		enable_local_activities = enable_local_activities,
		enable_remote_activities = enable_remote_activities,
		enable_nexus = enable_nexus,
	}
	options.nondeterminism_as_workflow_fail = fail_all_workflows
	options.nondeterminism_as_workflow_fail_for_types = fail_workflow_types
	if custom_activity_binding != nil {
		options.tuner.activity_slot_supplier = custom_slot_supplier_raw(
			custom_activity_binding,
		)
	}
	result := raw.worker_new(client.handle, options)
	if result.fail != nil {
		error.message = copy_core_bytes(result.fail)
		raw.byte_array_free(client.runtime.handle, result.fail)
		custom_slot_supplier_binding_destroy(custom_activity_binding)
		return
	}
	worker.handle = result.worker
	worker.runtime = client.runtime
	worker.custom_activity_supplier = custom_activity_binding
	worker.workflows_enabled = enable_workflows
	worker.local_activities_enabled = enable_local_activities
	worker.remote_activities_enabled = enable_remote_activities
	worker.nexus_enabled = enable_nexus
	return
}

replay_worker_create :: proc(
	runtime: ^Runtime,
	namespace := "default",
	task_queue := "temporal-sdk-odin-replay",
) -> (replayer: Replay_Worker, error: Error) {
	if runtime == nil || runtime.handle == nil {
		error.message = bytes.clone(transmute([]u8)string("runtime is not initialized"))
		return
	}
	if len(namespace) == 0 {
		error.message = bytes.clone(transmute([]u8)string("namespace must not be empty"))
		return
	}
	if len(task_queue) == 0 {
		error.message = bytes.clone(transmute([]u8)string("task queue must not be empty"))
		return
	}
	options := raw.Worker_Options{
		namespace = byte_ref(namespace),
		task_queue = byte_ref(task_queue),
		versioning_strategy = worker_versioning_strategy_raw({}),
		// Upstream replay workers use a small cache and at least two Workflow
		// slots so eviction of one history cannot starve the replay stream.
		max_cached_workflows = 2,
		tuner = raw.Tuner_Holder{
			workflow_slot_supplier       = fixed_slot_supplier(2),
			activity_slot_supplier       = fixed_slot_supplier(1),
			local_activity_slot_supplier = fixed_slot_supplier(1),
			nexus_task_slot_supplier     = fixed_slot_supplier(1),
		},
		task_types = raw.Worker_Task_Types{enable_workflows = true},
		max_heartbeat_throttle_interval_ms     = 60_000,
		default_heartbeat_throttle_interval_ms = 1_000,
		graceful_shutdown_period_ms            = 5_000,
		nonsticky_to_sticky_poll_ratio         = 0.2,
	}
	result := raw.worker_replayer_new(runtime.handle, &options)
	if result.fail != nil {
		error.message = copy_core_bytes(result.fail)
		raw.byte_array_free(runtime.handle, result.fail)
		return
	}
	if result.worker == nil || result.worker_replay_pusher == nil {
		error.message = bytes.clone(transmute([]u8)string("Core returned an incomplete replay worker"))
		if result.worker != nil {
			raw.worker_free(result.worker)
		}
		if result.worker_replay_pusher != nil {
			raw.worker_replay_pusher_free(result.worker_replay_pusher)
		}
		return
	}
	replayer.worker = Worker{
		handle = result.worker,
		runtime = runtime,
		workflows_enabled = true,
	}
	replayer.pusher = result.worker_replay_pusher
	return
}

replay_worker_push :: proc(
	replayer: ^Replay_Worker,
	workflow_id: string,
	history_protobuf: []u8,
) -> Error {
	if replayer == nil || replayer.worker.handle == nil ||
	   replayer.worker.runtime == nil || replayer.pusher == nil {
		return Error{message = bytes.clone(transmute([]u8)string("replay worker is not initialized"))}
	}
	if len(workflow_id) == 0 {
		return Error{message = bytes.clone(transmute([]u8)string("replay workflow ID must not be empty"))}
	}
	if len(history_protobuf) == 0 {
		return Error{message = bytes.clone(transmute([]u8)string("replay history protobuf must not be empty"))}
	}
	result := raw.worker_replay_push(
		replayer.worker.handle,
		replayer.pusher,
		byte_ref(workflow_id),
		bytes_ref(history_protobuf),
	)
	if result.fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(result.fail)}
	raw.byte_array_free(replayer.worker.runtime.handle, result.fail)
	return error
}

replay_worker_close_pusher :: proc(replayer: ^Replay_Worker) {
	if replayer == nil || replayer.pusher == nil {
		return
	}
	raw.worker_replay_pusher_free(replayer.pusher)
	replayer.pusher = nil
}

replay_worker_close :: proc(replayer: ^Replay_Worker) -> Error {
	if replayer == nil {
		return {}
	}
	replay_worker_close_pusher(replayer)
	return worker_close(&replayer.worker)
}

Worker_Callback_State :: struct {
	done: sync.One_Shot_Event,
	fail: ^raw.Byte_Array,
}

worker_callback :: proc "c" (user_data: rawptr, fail: ^raw.Byte_Array) {
	state := cast(^Worker_Callback_State)user_data
	state.fail = fail
	sync.one_shot_event_signal(&state.done)
}

worker_wait_call :: proc(worker: ^Worker, invoke: proc(^raw.Worker, rawptr, raw.Worker_Callback)) -> Error {
	state := Worker_Callback_State{}
	invoke(worker.handle, &state, worker_callback)
	wait_until(&state.done)
	if state.fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(state.fail)}
	raw.byte_array_free(worker.runtime.handle, state.fail)
	return error
}

worker_validate_invoke :: proc(handle: ^raw.Worker, user_data: rawptr, callback: raw.Worker_Callback) {
	raw.worker_validate(handle, user_data, callback)
}

worker_validate :: proc(worker: ^Worker) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil {
		return Error{message = bytes.clone(transmute([]u8)string("worker is not initialized"))}
	}
	return worker_wait_call(worker, worker_validate_invoke)
}

// worker_replace_client synchronously asks Core to move future Worker traffic
// to a new connection. Core clones the connection, but the caller must keep
// both runtimes alive until the Worker is closed.
worker_replace_client :: proc(worker: ^Worker, client: ^Client) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil {
		return Error{message = bytes.clone(transmute([]u8)string("worker is not initialized"))}
	}
	if client == nil || client.handle == nil || client.runtime == nil {
		return Error{message = bytes.clone(transmute([]u8)string("replacement client is not initialized"))}
	}
	fail := raw.worker_replace_client(worker.handle, client.handle)
	if fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(fail)}
	// The bridge allocates this failure through the Worker's original Runtime.
	raw.byte_array_free(worker.runtime.handle, fail)
	return error
}

Poll_Result :: struct {
	protobuf: []u8,
	error:    Error,
	shutdown: bool,
}

Worker_Poll_State :: struct {
	done:    sync.One_Shot_Event,
	success: ^raw.Byte_Array,
	fail:    ^raw.Byte_Array,
}

worker_poll_callback :: proc "c" (user_data: rawptr, success, fail: ^raw.Byte_Array) {
	state := cast(^Worker_Poll_State)user_data
	state.success = success
	state.fail = fail
	sync.one_shot_event_signal(&state.done)
}

worker_poll_activity :: proc(worker: ^Worker) -> Poll_Result {
	result := Poll_Result{}
	if worker == nil || worker.handle == nil || worker.runtime == nil {
		result.error.message = bytes.clone(transmute([]u8)string("worker is not initialized"))
		return result
	}
	state := Worker_Poll_State{}
	raw.worker_poll_activity_task(worker.handle, &state, worker_poll_callback)
	wait_until(&state.done)
	if state.fail != nil {
		result.error.message = copy_core_bytes(state.fail)
		raw.byte_array_free(worker.runtime.handle, state.fail)
		return result
	}
	if state.success == nil {
		result.shutdown = true
		return result
	}
	result.protobuf = copy_core_bytes(state.success)
	raw.byte_array_free(worker.runtime.handle, state.success)
	return result
}

worker_poll_workflow :: proc(worker: ^Worker) -> Poll_Result {
	result := Poll_Result{}
	if worker == nil || worker.handle == nil || worker.runtime == nil || !worker.workflows_enabled {
		result.error.message = bytes.clone(transmute([]u8)string("workflow polling is not enabled"))
		return result
	}
	state := Worker_Poll_State{}
	raw.worker_poll_workflow_activation(worker.handle, &state, worker_poll_callback)
	wait_until(&state.done)
	if state.fail != nil {
		result.error.message = copy_core_bytes(state.fail)
		raw.byte_array_free(worker.runtime.handle, state.fail)
		return result
	}
	if state.success == nil {
		result.shutdown = true
		return result
	}
	result.protobuf = copy_core_bytes(state.success)
	raw.byte_array_free(worker.runtime.handle, state.success)
	return result
}

worker_poll_nexus :: proc(worker: ^Worker) -> Poll_Result {
	result := Poll_Result{}
	if worker == nil || worker.handle == nil || worker.runtime == nil || !worker.nexus_enabled {
		result.error.message = bytes.clone(transmute([]u8)string("Nexus polling is not enabled"))
		return result
	}
	state := Worker_Poll_State{}
	raw.worker_poll_nexus_task(worker.handle, &state, worker_poll_callback)
	wait_until(&state.done)
	if state.fail != nil {
		result.error.message = copy_core_bytes(state.fail)
		raw.byte_array_free(worker.runtime.handle, state.fail)
		return result
	}
	if state.success == nil {
		result.shutdown = true
		return result
	}
	result.protobuf = copy_core_bytes(state.success)
	raw.byte_array_free(worker.runtime.handle, state.success)
	return result
}

Complete_State :: struct {
	done: sync.One_Shot_Event,
	fail: ^raw.Byte_Array,
}

complete_callback :: proc "c" (user_data: rawptr, fail: ^raw.Byte_Array) {
	state := cast(^Complete_State)user_data
	state.fail = fail
	sync.one_shot_event_signal(&state.done)
}

worker_complete_activity :: proc(worker: ^Worker, completion: []u8) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil {
		return Error{message = bytes.clone(transmute([]u8)string("worker is not initialized"))}
	}
	state := Complete_State{}
	raw.worker_complete_activity_task(worker.handle, bytes_ref(completion), &state, complete_callback)
	wait_until(&state.done)
	if state.fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(state.fail)}
	raw.byte_array_free(worker.runtime.handle, state.fail)
	return error
}

worker_record_activity_heartbeat :: proc(worker: ^Worker, heartbeat: []u8) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil {
		return Error{message = bytes.clone(transmute([]u8)string("worker is not initialized"))}
	}
	fail := raw.worker_record_activity_heartbeat(worker.handle, bytes_ref(heartbeat))
	if fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(fail)}
	raw.byte_array_free(worker.runtime.handle, fail)
	return error
}

worker_complete_workflow :: proc(worker: ^Worker, completion: []u8) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil || !worker.workflows_enabled {
		return Error{message = bytes.clone(transmute([]u8)string("workflow completion is not enabled"))}
	}
	state := Complete_State{}
	raw.worker_complete_workflow_activation(worker.handle, bytes_ref(completion), &state, complete_callback)
	wait_until(&state.done)
	if state.fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(state.fail)}
	raw.byte_array_free(worker.runtime.handle, state.fail)
	return error
}

worker_complete_nexus :: proc(worker: ^Worker, completion: []u8) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil || !worker.nexus_enabled {
		return Error{message = bytes.clone(transmute([]u8)string("Nexus completion is not enabled"))}
	}
	state := Complete_State{}
	raw.worker_complete_nexus_task(worker.handle, bytes_ref(completion), &state, complete_callback)
	wait_until(&state.done)
	if state.fail == nil {
		return {}
	}
	error := Error{message = copy_core_bytes(state.fail)}
	raw.byte_array_free(worker.runtime.handle, state.fail)
	return error
}

worker_request_workflow_eviction :: proc(worker: ^Worker, run_id: string) -> Error {
	if worker == nil || worker.handle == nil || worker.runtime == nil || !worker.workflows_enabled {
		return Error{message = bytes.clone(transmute([]u8)string("workflow eviction is not enabled"))}
	}
	if len(run_id) == 0 {
		return Error{message = bytes.clone(transmute([]u8)string("workflow run id must not be empty"))}
	}
	raw.worker_request_workflow_eviction(worker.handle, byte_ref(run_id))
	return {}
}

worker_initiate_shutdown :: proc(worker: ^Worker) {
	if worker == nil || worker.handle == nil {
		return
	}
	raw.worker_initiate_shutdown(worker.handle)
}

finalize_shutdown_invoke :: proc(handle: ^raw.Worker, user_data: rawptr, callback: raw.Worker_Callback) {
	raw.worker_finalize_shutdown(handle, user_data, callback)
}

worker_close :: proc(worker: ^Worker) -> Error {
	if worker == nil {
		return {}
	}
	if worker.handle == nil {
		custom_slot_supplier_binding_destroy(worker.custom_activity_supplier)
		worker.custom_activity_supplier = nil
		return {}
	}
	worker_initiate_shutdown(worker)
	// Core shutdown completes only after every enabled poll stream observes
	// PollError::ShutDown. The synchronous wrapper has no outstanding polls, so
	// drain each enabled stream here before finalize_shutdown.
	error := Error{}
	if worker.workflows_enabled {
		poll := worker_poll_workflow(worker)
		if poll.protobuf != nil {
			delete(poll.protobuf)
		}
		error = poll.error
		if error.message == nil && !poll.shutdown {
			error.message = bytes.clone(transmute([]u8)string("workflow shutdown poll returned an unexpected activation"))
		}
	}
	if worker.local_activities_enabled || worker.remote_activities_enabled {
		poll := worker_poll_activity(worker)
		if poll.protobuf != nil {
			delete(poll.protobuf)
		}
		if error.message == nil {
			error = poll.error
		} else {
			error_free(&poll.error)
		}
		if error.message == nil && !poll.shutdown {
			error.message = bytes.clone(transmute([]u8)string("activity shutdown poll returned an unexpected task"))
		}
	}
	if worker.nexus_enabled {
		poll := worker_poll_nexus(worker)
		if poll.protobuf != nil {
			delete(poll.protobuf)
		}
		if error.message == nil {
			error = poll.error
		} else {
			error_free(&poll.error)
		}
		if error.message == nil && !poll.shutdown {
			error.message = bytes.clone(transmute([]u8)string("Nexus shutdown poll returned an unexpected task"))
		}
	}
	finalize_error := worker_wait_call(worker, finalize_shutdown_invoke)
	if error.message == nil {
		error = finalize_error
	} else {
		error_free(&finalize_error)
	}
	raw.worker_free(worker.handle)
	custom_slot_supplier_binding_destroy(worker.custom_activity_supplier)
	worker.handle = nil
	worker.runtime = nil
	worker.custom_activity_supplier = nil
	worker.workflows_enabled = false
	worker.local_activities_enabled = false
	worker.remote_activities_enabled = false
	worker.nexus_enabled = false
	return error
}

Rpc_State :: struct {
	done:            sync.One_Shot_Event,
	success:         ^raw.Byte_Array,
	status_code:     u32,
	failure_message: ^raw.Byte_Array,
	failure_details: ^raw.Byte_Array,
}

Rpc_Result :: struct {
	protobuf: []u8,
	error:    Error,
}

// Rpc_Cancellation_Token owns Core's thread-safe cancellation handle. It may be
// canceled from another thread, but must outlive every client_rpc_call that
// borrows it and must be closed exactly once after those calls return.
Rpc_Cancellation_Token :: struct {
	handle: ^raw.Cancellation_Token,
}

rpc_cancellation_token_create :: proc() -> Rpc_Cancellation_Token {
	return {handle = raw.cancellation_token_new()}
}

rpc_cancellation_token_cancel :: proc(token: ^Rpc_Cancellation_Token) {
	if token == nil || token.handle == nil do return
	raw.cancellation_token_cancel(token.handle)
}

rpc_cancellation_token_close :: proc(token: ^Rpc_Cancellation_Token) {
	if token == nil || token.handle == nil do return
	raw.cancellation_token_free(token.handle)
	token.handle = nil
}

rpc_callback :: proc "c" (
	user_data: rawptr,
	success: ^raw.Byte_Array,
	status_code: u32,
	failure_message: ^raw.Byte_Array,
	failure_details: ^raw.Byte_Array,
) {
	state := cast(^Rpc_State)user_data
	state.success = success
	state.status_code = status_code
	state.failure_message = failure_message
	state.failure_details = failure_details
	sync.one_shot_event_signal(&state.done)
}

// Core normally retries transport failures internally. Some tonic/h2
// cancellations lose their transport error source chain before Core's retry
// classifier sees them and arrive as gRPC Cancelled instead. Retry exactly once
// at the bridge boundary, and only when the caller already opted into Core
// retries. Caller cancellation and explicit no-retry/deadline probes do not
// match this predicate.
rpc_should_retry_lost_h2_transport :: proc(
	retry: bool,
	status_code: u32,
	message: string,
	attempt: int,
) -> bool {
	return retry && attempt == 0 && status_code == 1 &&
	       strings.contains(message, "h2 protocol error")
}

client_get_system_info :: proc(client: ^Client) -> Get_System_Info_Result {
	rpc_result := client_rpc_call(
		client,
		.Workflow,
		"GetSystemInfo",
		nil,
		retry = true,
		timeout_millis = 10_000,
	)
	return {protobuf = rpc_result.protobuf, error = rpc_result.error}
}

client_rpc_call :: proc(
	client: ^Client,
	service: raw.Rpc_Service,
	rpc: string,
	request: []u8,
	retry := true,
	timeout_millis: u32 = 10_000,
	cancellation_token: ^Rpc_Cancellation_Token = nil,
) -> Rpc_Result {
	result := Rpc_Result{}
	if client == nil || client.handle == nil || client.runtime == nil {
		result.error.message = bytes.clone(transmute([]u8)string("client is not initialized"))
		return result
	}
	if cancellation_token != nil && cancellation_token.handle == nil {
		result.error.message = bytes.clone(transmute([]u8)string("RPC cancellation token is not initialized"))
		return result
	}

	options := raw.Rpc_Call_Options{
		service = service,
		rpc = byte_ref(rpc),
		req = bytes_ref(request),
		retry = retry,
		timeout_millis = timeout_millis,
		cancellation_token = cancellation_token != nil ? cancellation_token.handle : nil,
	}
	runtime := client.runtime.handle
	for attempt in 0..<2 {
		state := Rpc_State{}
		raw.client_rpc_call(client.handle, &options, &state, rpc_callback)
		wait_until(&state.done)

		if state.failure_message != nil {
			failure_message := copy_core_bytes(state.failure_message)
			failure_details: []u8
			raw.byte_array_free(runtime, state.failure_message)
			if state.failure_details != nil {
				failure_details = copy_core_bytes(state.failure_details)
				raw.byte_array_free(runtime, state.failure_details)
			}
			if rpc_should_retry_lost_h2_transport(
				retry,
				state.status_code,
				string(failure_message),
				attempt,
			) {
				delete(failure_message)
				delete(failure_details)
				time.sleep(100 * time.Millisecond)
				continue
			}
			result.error.message = failure_message
			result.error.status_code = state.status_code
			result.error.details = failure_details
			return result
		}
		if state.success == nil {
			result.error.message = bytes.clone(transmute([]u8)string("Core RPC returned neither success nor failure"))
			return result
		}

		result.protobuf = copy_core_bytes(state.success)
		raw.byte_array_free(runtime, state.success)
		return result
	}
	return result
}

// Package core owns the blocking lifecycle wrapper around the raw C ABI.
package core

import "core:c"
import "core:bytes"
import "core:fmt"
import "core:math"
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
	handle: ^raw.Runtime,
}

Client :: struct {
	handle:  ^raw.Connection,
	runtime: ^Runtime,
}

Worker :: struct {
	handle:                    ^raw.Worker,
	runtime:                   ^Runtime,
	workflows_enabled:         bool,
	local_activities_enabled:  bool,
	remote_activities_enabled: bool,
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
	max_cached_workflows:                           u32,
	sticky_queue_schedule_to_start_timeout:         time.Duration,
	max_heartbeat_throttle_interval:                 time.Duration,
	default_heartbeat_throttle_interval:             time.Duration,
	max_worker_activities_per_second:                f64,
	max_task_queue_activities_per_second:            f64,
	graceful_shutdown_period:                        time.Duration,
	workflow_task_poller_behavior:                   Poller_Behavior,
	activity_task_poller_behavior:                   Poller_Behavior,
	nonsticky_to_sticky_poll_ratio:                  f32,
	max_eager_activity_reservations_per_workflow_task: u32,
}

worker_options_default :: proc() -> Worker_Options {
	return {
		max_activity_slots = 10,
		max_cached_workflows = 0,
		sticky_queue_schedule_to_start_timeout = 10 * time.Second,
		max_heartbeat_throttle_interval = 60 * time.Second,
		default_heartbeat_throttle_interval = time.Second,
		graceful_shutdown_period = DEFAULT_GRACEFUL_SHUTDOWN_PERIOD,
		nonsticky_to_sticky_poll_ratio = 0.2,
		// Keep eager dispatch opt-in until the Odin Activity/Workflow executor
		// has a dedicated eager-execution integration test.
		max_eager_activity_reservations_per_workflow_task = 0,
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

runtime_create :: proc() -> (runtime: Runtime, error: Error) {
	options := raw.Runtime_Options{}
	result := raw.runtime_new(&options)
	if result.fail != nil {
		error.message = copy_core_bytes(result.fail)
		raw.byte_array_free(result.runtime, result.fail)
		raw.runtime_free(result.runtime)
		return
	}
	runtime.handle = result.runtime
	return
}

runtime_close :: proc(runtime: ^Runtime) {
	if runtime == nil || runtime.handle == nil {
		return
	}
	raw.runtime_free(runtime.handle)
	runtime.handle = nil
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

worker_options_validate :: proc(options: Worker_Options) -> Error {
	if options.max_activity_slots <= 0 {
		return client_option_error("max activity slots must be positive")
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
	if options.max_cached_workflows > 0 &&
	   options.workflow_task_poller_behavior.kind == .Simple_Maximum &&
	   options.workflow_task_poller_behavior.simple_maximum < 2 {
		return client_option_error("Workflow task simple poller must be at least two when Workflow caching is enabled")
	}
	return {}
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
	bindings.raw_options.tuner = raw.Tuner_Holder{
		workflow_slot_supplier       = fixed_slot_supplier(workflow_slots),
		activity_slot_supplier       = fixed_slot_supplier(uintptr(options.max_activity_slots)),
		local_activity_slot_supplier = fixed_slot_supplier(1),
		nexus_task_slot_supplier     = fixed_slot_supplier(1),
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
	}
	options.nondeterminism_as_workflow_fail = fail_all_workflows
	options.nondeterminism_as_workflow_fail_for_types = fail_workflow_types
	result := raw.worker_new(client.handle, options)
	if result.fail != nil {
		error.message = copy_core_bytes(result.fail)
		raw.byte_array_free(client.runtime.handle, result.fail)
		return
	}
	worker.handle = result.worker
	worker.runtime = client.runtime
	worker.workflows_enabled = enable_workflows
	worker.local_activities_enabled = enable_local_activities
	worker.remote_activities_enabled = enable_remote_activities
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
	if worker == nil || worker.handle == nil {
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
	finalize_error := worker_wait_call(worker, finalize_shutdown_invoke)
	if error.message == nil {
		error = finalize_error
	} else {
		error_free(&finalize_error)
	}
	raw.worker_free(worker.handle)
	worker.handle = nil
	worker.runtime = nil
	worker.workflows_enabled = false
	worker.local_activities_enabled = false
	worker.remote_activities_enabled = false
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

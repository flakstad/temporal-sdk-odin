// Package raw is a deliberately thin subset of Temporal SDK Core's C ABI.
// Keep field order and widths synchronized with the pinned C header.
package raw

import "core:c"

when ODIN_OS == .Darwin || ODIN_OS == .Linux {
	foreign import temporal_core "system:temporalio_sdk_core_c_bridge"
} else {
	#panic("Temporal SDK Core spike currently supports macOS and Linux")
}

Runtime    :: struct { _opaque: u8 }
Connection :: struct { _opaque: u8 }
Worker     :: struct { _opaque: u8 }
Worker_Replay_Pusher :: struct { _opaque: u8 }
Cancellation_Token :: struct { _opaque: u8 }

Byte_Array_Ref :: struct {
	data: ^u8,
	size: c.size_t,
}

Byte_Array_Ref_Array :: struct {
	data: ^Byte_Array_Ref,
	size: c.size_t,
}

Metadata_Ref :: Byte_Array_Ref_Array

Byte_Array :: struct {
	data:         ^u8,
	size:         c.size_t,
	cap:          c.size_t,
	disable_free: bool,
}

Runtime_Type :: enum c.int {
	Unspecified     = 0,
	Jvm             = 1,
	Cpython         = 2,
	Node            = 3,
	Bun             = 4,
	Cruby           = 5,
	Go              = 6,
	Dotnet_Framework = 7,
	Dotnet_Core      = 8,
	Native           = 9,
	Roadrunner       = 10,
}

Runtime_Info :: struct {
	runtime_type: Runtime_Type,
	version:      Byte_Array_Ref,
}

Runtime_Info_Array :: struct {
	data: ^Runtime_Info,
	size: c.size_t,
}

Telemetry_Options :: struct {
	logging: rawptr,
	metrics: rawptr,
}

Runtime_Options :: struct {
	telemetry:                           ^Telemetry_Options,
	worker_heartbeat_interval_millis:    u64,
	runtime_info:                        Runtime_Info_Array,
	disable_environment_info:            bool,
}

Runtime_Or_Fail :: struct {
	runtime: ^Runtime,
	fail:    ^Byte_Array,
}

Client_Grpc_Compression :: enum c.int {
	Gzip = 0,
	None = 1,
}

Client_Grpc_Override_Callback :: #type proc "c" (request, user_data: rawptr)

Client_TLS_Options :: struct {
	server_root_ca_cert: Byte_Array_Ref,
	domain:              Byte_Array_Ref,
	client_cert:         Byte_Array_Ref,
	client_private_key:  Byte_Array_Ref,
}

Client_Retry_Options :: struct {
	initial_interval_millis: u64,
	randomization_factor:    f64,
	multiplier:              f64,
	max_interval_millis:     u64,
	max_elapsed_time_millis: u64,
	max_retries:             uintptr,
}

Client_Keep_Alive_Options :: struct {
	interval_millis: u64,
	timeout_millis:  u64,
}

Client_HTTP_Connect_Proxy_Options :: struct {
	target_host: Byte_Array_Ref,
	username:    Byte_Array_Ref,
	password:    Byte_Array_Ref,
}

Client_DNS_Load_Balancing_Options :: struct {
	resolution_interval_millis: u64,
}

Connection_Options :: struct {
	target_url:                       Byte_Array_Ref,
	client_name:                      Byte_Array_Ref,
	client_version:                   Byte_Array_Ref,
	metadata:                         Metadata_Ref,
	binary_metadata:                  Metadata_Ref,
	api_key:                          Byte_Array_Ref,
	identity:                         Byte_Array_Ref,
	tls_options:                      ^Client_TLS_Options,
	retry_options:                    ^Client_Retry_Options,
	keep_alive_options:               ^Client_Keep_Alive_Options,
	http_connect_proxy_options:       ^Client_HTTP_Connect_Proxy_Options,
	grpc_override_callback:           Client_Grpc_Override_Callback,
	grpc_override_callback_user_data: rawptr,
	dns_load_balancing_options:       ^Client_DNS_Load_Balancing_Options,
	grpc_compression:                 Client_Grpc_Compression,
	payloads_warn_size:               u64,
	memo_warn_size:                   u64,
}

Client_Connect_Callback :: #type proc "c" (
	user_data: rawptr,
	success:   ^Connection,
	fail:      ^Byte_Array,
)

Rpc_Service :: enum c.int {
	Workflow = 1,
	Operator,
	Cloud,
	Test,
	Health,
}

Rpc_Call_Options :: struct {
	service:            Rpc_Service,
	rpc:                Byte_Array_Ref,
	req:                Byte_Array_Ref,
	retry:              bool,
	metadata:           Metadata_Ref,
	binary_metadata:    Metadata_Ref,
	timeout_millis:      u32,
	cancellation_token: ^Cancellation_Token,
}

Client_Rpc_Call_Callback :: #type proc "c" (
	user_data:       rawptr,
	success:         ^Byte_Array,
	status_code:     u32,
	failure_message: ^Byte_Array,
	failure_details: ^Byte_Array,
)

Worker_Or_Fail :: struct {
	worker: ^Worker,
	fail:   ^Byte_Array,
}

Worker_Replayer_Or_Fail :: struct {
	worker:               ^Worker,
	worker_replay_pusher: ^Worker_Replay_Pusher,
	fail:                 ^Byte_Array,
}

Worker_Replay_Push_Result :: struct {
	fail: ^Byte_Array,
}

Worker_Versioning_Strategy_Tag :: enum c.int {
	None = 0,
	Deployment_Based,
	Legacy_Build_Id_Based,
}

Worker_Versioning_None :: struct {
	build_id: Byte_Array_Ref,
}

Worker_Deployment_Version :: struct {
	deployment_name: Byte_Array_Ref,
	build_id:        Byte_Array_Ref,
}

Worker_Deployment_Options :: struct {
	version:                     Worker_Deployment_Version,
	use_worker_versioning:       bool,
	default_versioning_behavior: i32,
}

Legacy_Build_Id_Based_Strategy :: struct {
	build_id: Byte_Array_Ref,
}

Worker_Versioning_Strategy :: struct {
	tag: Worker_Versioning_Strategy_Tag,
	using value: struct #raw_union {
		none:                  Worker_Versioning_None,
		deployment_based:      Worker_Deployment_Options,
		legacy_build_id_based: Legacy_Build_Id_Based_Strategy,
	},
}

Fixed_Size_Slot_Supplier :: struct {
	num_slots: uintptr,
}

Resource_Based_Tuner_Options :: struct {
	target_memory_usage: f64,
	target_cpu_usage:    f64,
}

Resource_Based_Slot_Supplier :: struct {
	minimum_slots:    uintptr,
	maximum_slots:    uintptr,
	ramp_throttle_ms: u64,
	tuner_options:    Resource_Based_Tuner_Options,
}

Custom_Slot_Supplier_Callbacks_Impl :: struct {
	callbacks: rawptr,
}

Slot_Supplier_Tag :: enum c.int {
	Fixed_Size = 0,
	Resource_Based,
	Custom,
}

Slot_Supplier :: struct {
	tag: Slot_Supplier_Tag,
	using value: struct #raw_union {
		fixed_size:     Fixed_Size_Slot_Supplier,
		resource_based: Resource_Based_Slot_Supplier,
		custom:         Custom_Slot_Supplier_Callbacks_Impl,
	},
}

Tuner_Holder :: struct {
	workflow_slot_supplier:       Slot_Supplier,
	activity_slot_supplier:       Slot_Supplier,
	local_activity_slot_supplier: Slot_Supplier,
	nexus_task_slot_supplier:     Slot_Supplier,
}

Worker_Task_Types :: struct {
	enable_workflows:         bool,
	enable_local_activities:  bool,
	enable_remote_activities: bool,
	enable_nexus:             bool,
}

Poller_Behavior_Simple_Maximum :: struct {
	simple_maximum: uintptr,
}

Poller_Behavior_Autoscaling :: struct {
	minimum: uintptr,
	maximum: uintptr,
	initial: uintptr,
}

Poller_Behavior :: struct {
	simple_maximum: ^Poller_Behavior_Simple_Maximum,
	autoscaling:    ^Poller_Behavior_Autoscaling,
}

Worker_Options :: struct {
	namespace:                                  Byte_Array_Ref,
	task_queue:                                 Byte_Array_Ref,
	versioning_strategy:                        Worker_Versioning_Strategy,
	identity_override:                          Byte_Array_Ref,
	max_cached_workflows:                       u32,
	tuner:                                      Tuner_Holder,
	task_types:                                 Worker_Task_Types,
	sticky_queue_schedule_to_start_timeout_ms:  u64,
	max_heartbeat_throttle_interval_ms:         u64,
	default_heartbeat_throttle_interval_ms:     u64,
	max_activities_per_second:                  f64,
	max_task_queue_activities_per_second:       f64,
	graceful_shutdown_period_ms:                u64,
	workflow_task_poller_behavior:              Poller_Behavior,
	nonsticky_to_sticky_poll_ratio:             f32,
	activity_task_poller_behavior:              Poller_Behavior,
	nexus_task_poller_behavior:                 Poller_Behavior,
	nondeterminism_as_workflow_fail:             bool,
	nondeterminism_as_workflow_fail_for_types:   Byte_Array_Ref_Array,
	plugins:                                    Byte_Array_Ref_Array,
	storage_drivers:                            Byte_Array_Ref_Array,
	disable_payload_error_limit:                 bool,
	max_eager_activity_reservations_per_wf_task: u32,
}

Worker_Callback :: #type proc "c" (user_data: rawptr, fail: ^Byte_Array)

Worker_Poll_Callback :: #type proc "c" (
	user_data: rawptr,
	success:   ^Byte_Array,
	fail:      ^Byte_Array,
)

#assert(size_of(bool) == 1)
#assert(size_of(Runtime_Type) == size_of(c.int))
#assert(size_of(Rpc_Service) == size_of(c.int))
#assert(size_of(Worker_Versioning_Strategy_Tag) == size_of(c.int))
#assert(size_of(Slot_Supplier_Tag) == size_of(c.int))
when size_of(uintptr) == 8 {
	// Values verified against Clang's layout of the pinned generated C header.
	#assert(size_of(Byte_Array_Ref) == 16)
	#assert(size_of(Byte_Array) == 32)
	#assert(offset_of(Byte_Array, disable_free) == 24)
	#assert(size_of(Runtime_Options) == 40)
	#assert(offset_of(Runtime_Options, disable_environment_info) == 32)
	#assert(size_of(Connection_Options) == 192)
	#assert(offset_of(Connection_Options, tls_options) == 112)
	#assert(offset_of(Connection_Options, grpc_compression) == 168)
	#assert(offset_of(Connection_Options, memo_warn_size) == 184)
	#assert(size_of(Client_TLS_Options) == 64)
	#assert(offset_of(Client_TLS_Options, client_private_key) == 48)
	#assert(size_of(Client_Retry_Options) == 48)
	#assert(offset_of(Client_Retry_Options, max_retries) == 40)
	#assert(size_of(Client_Keep_Alive_Options) == 16)
	#assert(size_of(Client_HTTP_Connect_Proxy_Options) == 48)
	#assert(size_of(Client_DNS_Load_Balancing_Options) == 8)
	#assert(size_of(Rpc_Call_Options) == 96)
	#assert(size_of(Worker_Replayer_Or_Fail) == 24)
	#assert(size_of(Worker_Replay_Push_Result) == 8)
	#assert(offset_of(Rpc_Call_Options, retry) == 40)
	#assert(offset_of(Rpc_Call_Options, cancellation_token) == 88)
	#assert(size_of(Worker_Versioning_Strategy) == 48)
	#assert(size_of(Slot_Supplier) == 48)
	#assert(size_of(Tuner_Holder) == 192)
	#assert(size_of(Worker_Task_Types) == 4)
	#assert(size_of(Poller_Behavior) == 16)
	#assert(size_of(Worker_Options) == 472)
	#assert(offset_of(Worker_Options, versioning_strategy) == 32)
	#assert(offset_of(Worker_Options, identity_override) == 80)
	#assert(offset_of(Worker_Options, max_cached_workflows) == 96)
	#assert(offset_of(Worker_Options, tuner) == 104)
	#assert(offset_of(Worker_Options, task_types) == 296)
	#assert(offset_of(Worker_Options, sticky_queue_schedule_to_start_timeout_ms) == 304)
	#assert(offset_of(Worker_Options, workflow_task_poller_behavior) == 352)
	#assert(offset_of(Worker_Options, nonsticky_to_sticky_poll_ratio) == 368)
	#assert(offset_of(Worker_Options, activity_task_poller_behavior) == 376)
	#assert(offset_of(Worker_Options, nondeterminism_as_workflow_fail) == 408)
	#assert(offset_of(Worker_Options, nondeterminism_as_workflow_fail_for_types) == 416)
	#assert(offset_of(Worker_Options, disable_payload_error_limit) == 464)
	#assert(offset_of(Worker_Options, max_eager_activity_reservations_per_wf_task) == 468)
}

@(default_calling_convention = "c", link_prefix = "temporal_core_")
foreign temporal_core {
	runtime_new       :: proc(options: ^Runtime_Options) -> Runtime_Or_Fail ---
	runtime_free      :: proc(runtime: ^Runtime) ---
	byte_array_free   :: proc(runtime: ^Runtime, bytes: ^Byte_Array) ---
	client_connect    :: proc(
		runtime: ^Runtime,
		options: ^Connection_Options,
		user_data: rawptr,
		callback: Client_Connect_Callback,
	) ---
	client_free       :: proc(client: ^Connection) ---
	client_update_metadata :: proc(client: ^Connection, metadata: Metadata_Ref) ---
	client_update_binary_metadata :: proc(client: ^Connection, metadata: Metadata_Ref) ---
	client_update_api_key :: proc(client: ^Connection, api_key: Byte_Array_Ref) ---
	cancellation_token_new :: proc() -> ^Cancellation_Token ---
	cancellation_token_cancel :: proc(token: ^Cancellation_Token) ---
	cancellation_token_free :: proc(token: ^Cancellation_Token) ---
	client_rpc_call   :: proc(
		client: ^Connection,
		options: ^Rpc_Call_Options,
		user_data: rawptr,
		callback: Client_Rpc_Call_Callback,
	) ---
	worker_new :: proc(connection: ^Connection, options: ^Worker_Options) -> Worker_Or_Fail ---
	worker_replayer_new :: proc(
		runtime: ^Runtime,
		options: ^Worker_Options,
	) -> Worker_Replayer_Or_Fail ---
	worker_replay_pusher_free :: proc(worker_replay_pusher: ^Worker_Replay_Pusher) ---
	worker_replay_push :: proc(
		worker: ^Worker,
		worker_replay_pusher: ^Worker_Replay_Pusher,
		workflow_id: Byte_Array_Ref,
		history: Byte_Array_Ref,
	) -> Worker_Replay_Push_Result ---
	worker_free :: proc(worker: ^Worker) ---
	worker_validate :: proc(worker: ^Worker, user_data: rawptr, callback: Worker_Callback) ---
	worker_poll_activity_task :: proc(
		worker: ^Worker,
		user_data: rawptr,
		callback: Worker_Poll_Callback,
	) ---
	worker_poll_workflow_activation :: proc(
		worker: ^Worker,
		user_data: rawptr,
		callback: Worker_Poll_Callback,
	) ---
	worker_complete_activity_task :: proc(
		worker: ^Worker,
		completion: Byte_Array_Ref,
		user_data: rawptr,
		callback: Worker_Callback,
	) ---
	worker_record_activity_heartbeat :: proc(
		worker: ^Worker,
		heartbeat: Byte_Array_Ref,
	) -> ^Byte_Array ---
	worker_complete_workflow_activation :: proc(
		worker: ^Worker,
		completion: Byte_Array_Ref,
		user_data: rawptr,
		callback: Worker_Callback,
	) ---
	worker_request_workflow_eviction :: proc(worker: ^Worker, run_id: Byte_Array_Ref) ---
	worker_initiate_shutdown :: proc(worker: ^Worker) ---
	worker_finalize_shutdown :: proc(worker: ^Worker, user_data: rawptr, callback: Worker_Callback) ---
}

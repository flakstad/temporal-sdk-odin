package main

import "core:fmt"
import "core:os"
import "core:sync"
import "core:time"
import temporal_core "../../temporal/core"
import proto "../../temporal/core/proto"

Forwarded_Log_State :: struct {
	mutex:       sync.Mutex,
	count:       int,
	well_formed: bool,
}

capture_core_log :: proc(user_data: rawptr, log: ^temporal_core.Runtime_Log) {
	state := cast(^Forwarded_Log_State)user_data
	if state == nil || log == nil {
		return
	}
	sync.mutex_lock(&state.mutex)
	defer sync.mutex_unlock(&state.mutex)
	state.count += 1
	if len(log.target) > 0 && len(log.message) > 0 && log.timestamp_millis > 0 {
		state.well_formed = true
	}
}

probe_standalone_nexus :: proc(client: ^temporal_core.Client) {
	timeout := proto.Google_Protobuf_Duration{seconds = 5}
	request := proto.Temporal_Api_Workflowservice_V1_StartNexusOperationExecutionRequest{
		namespace = "default",
		identity = "temporal-sdk-odin-capability-probe",
		request_id = "00000000-0000-4000-8000-000000000001",
		operation_id = "temporal-sdk-odin-capability-probe",
		endpoint = "odin-standalone-nexus-probe-missing",
		service = "odin-probe-service",
		operation = "probe",
		schedule_to_close_timeout = &timeout,
	}
	request_bytes := proto.encode_Temporal_Api_Workflowservice_V1_StartNexusOperationExecutionRequest(
		&request,
	)
	defer delete(request_bytes)

	result := temporal_core.client_rpc_call(
		client,
		.Workflow,
		"StartNexusOperationExecution",
		request_bytes[:],
		retry = false,
		timeout_millis = 5_000,
	)
	defer delete(result.protobuf)
	if result.error.message == nil {
		response := proto.Temporal_Api_Workflowservice_V1_StartNexusOperationExecutionResponse{}
		if !proto.decode_Temporal_Api_Workflowservice_V1_StartNexusOperationExecutionResponse(
			result.protobuf,
			&response,
		) {
			fmt.eprintln("Standalone Nexus operation RPC returned an undecodable response.")
			return
		}
		fmt.printf(
			"Standalone Nexus operation RPC succeeded unexpectedly (run_id=%s, started=%v).\n",
			response.run_id,
			response.started,
		)
		return
	}

	status_code := result.error.status_code
	message := string(result.error.message)
	if status_code == 12 {
		fmt.printf(
			"Standalone Nexus operation RPC is unavailable on this Temporal Service (status=%d): %s\n",
			status_code,
			message,
		)
	} else {
		fmt.printf(
			"Standalone Nexus operation RPC is recognized by the Temporal Service (status=%d): %s\n",
			status_code,
			message,
		)
	}
	temporal_core.error_free(&result.error)
}

main :: proc() {
	// On POSIX, os.get_env uses this buffer for both the key and the value.
	nexus_spike_buffer: [64]u8
	nexus_spike := os.get_env(nexus_spike_buffer[:], "TEMPORAL_CORE_NEXUS_POLL_SPIKE")
	prometheus_address_buffer: [128]u8
	prometheus_address := os.get_env(
		prometheus_address_buffer[:],
		"TEMPORAL_CORE_PROMETHEUS_BIND_ADDRESS",
	)
	log_filter_buffer: [256]u8
	log_filter := os.get_env(log_filter_buffer[:], "TEMPORAL_CORE_LOG_FILTER")
	forward_logs_buffer: [64]u8
	forward_logs := os.get_env(forward_logs_buffer[:], "TEMPORAL_CORE_FORWARD_LOGS")
	standalone_nexus_probe_buffer: [64]u8
	standalone_nexus_probe := os.get_env(
		standalone_nexus_probe_buffer[:],
		"TEMPORAL_CORE_STANDALONE_NEXUS_PROBE",
	)
	runtime_options := temporal_core.runtime_options_default()
	prometheus_options := temporal_core.Runtime_Prometheus_Options{}
	logging_options := temporal_core.Runtime_Logging_Options{}
	forwarded_logs := Forwarded_Log_State{}
	if len(prometheus_address) > 0 {
		prometheus_options.bind_address = prometheus_address
		prometheus_options.counters_total_suffix = true
		prometheus_options.unit_suffix = true
		runtime_options.prometheus = &prometheus_options
		runtime_options.metrics.global_tags = []temporal_core.Runtime_Key_Value{
			{key = "sdk_language", value = "odin"},
		}
	}
	if len(log_filter) > 0 || forward_logs == "1" {
		logging_options.filter = log_filter
		if len(logging_options.filter) == 0 do logging_options.filter = "info"
		if forward_logs == "1" {
			logging_options.forward_to = capture_core_log
			logging_options.user_data = &forwarded_logs
		}
		runtime_options.logging = &logging_options
	}
	runtime, runtime_error := temporal_core.runtime_create_with_options(runtime_options)
	if runtime_error.message != nil {
		fmt.eprintf("Core runtime initialization failed: %s\n", string(runtime_error.message))
		temporal_core.error_free(&runtime_error)
		os.exit(1)
	}
	defer temporal_core.runtime_close(&runtime)

	client, connect_error := temporal_core.client_connect(&runtime)
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
		temporal_core.error_free(&connect_error)
		os.exit(1)
	}
	defer temporal_core.client_close(&client)
	if standalone_nexus_probe == "1" {
		probe_standalone_nexus(&client)
	}

	if nexus_spike == "1" {
		options := temporal_core.worker_options_default()
		options.max_nexus_task_slots = 1
		options.nexus_task_poller_behavior = temporal_core.poller_simple_maximum(1)
		core_worker, create_error := temporal_core.worker_create(
			&client,
			"odin-core-nexus-spike",
			options,
			enable_remote_activities = false,
			enable_nexus = true,
		)
		if create_error.message != nil {
			fmt.eprintf("Core Nexus worker creation failed: %s\n", string(create_error.message))
			temporal_core.error_free(&create_error)
			os.exit(1)
		}
		validate_error := temporal_core.worker_validate(&core_worker)
		if validate_error.message != nil {
			fmt.eprintf("Core Nexus worker validation failed: %s\n", string(validate_error.message))
			temporal_core.error_free(&validate_error)
			close_error := temporal_core.worker_close(&core_worker)
			temporal_core.error_free(&close_error)
			os.exit(1)
		}
		temporal_core.error_free(&validate_error)
		close_error := temporal_core.worker_close(&core_worker)
		if close_error.message != nil {
			fmt.eprintf("Core Nexus worker shutdown failed: %s\n", string(close_error.message))
			temporal_core.error_free(&close_error)
			os.exit(1)
		}
		temporal_core.error_free(&close_error)
		fmt.println("Core Nexus poll stream validated and observed clean shutdown.")
	}

	info := temporal_core.client_get_system_info(&client)
	if info.error.message != nil {
		fmt.eprintf("GetSystemInfo failed: %s\n", string(info.error.message))
		temporal_core.error_free(&info.error)
		os.exit(1)
	}
	defer delete(info.protobuf)

	fmt.printf("Connected through Temporal SDK Core; GetSystemInfo returned %d protobuf bytes.\n", len(info.protobuf))
	if forward_logs == "1" {
		time.sleep(50 * time.Millisecond)
		sync.mutex_lock(&forwarded_logs.mutex)
		log_count := forwarded_logs.count
		well_formed := forwarded_logs.well_formed
		sync.mutex_unlock(&forwarded_logs.mutex)
		if log_count == 0 || !well_formed {
			fmt.eprintf(
				"Core log forwarding did not deliver a well-formed record (count=%d, valid=%v).\n",
				log_count,
				well_formed,
			)
			os.exit(1)
		}
		fmt.printf("Core log forwarding delivered %d callback-scoped records.\n", log_count)
	}
	if len(prometheus_address) > 0 {
		fmt.printf("Core Prometheus exporter is serving %s.\n", prometheus_address)
		// The integration script scrapes while this process owns the runtime.
		time.sleep(3 * time.Second)
	}
}

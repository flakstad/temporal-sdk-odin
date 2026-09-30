package main

import "core:fmt"
import "core:os"
import "core:time"
import temporal_core "../../temporal/core"

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
	runtime_options := temporal_core.runtime_options_default()
	prometheus_options := temporal_core.Runtime_Prometheus_Options{}
	logging_options := temporal_core.Runtime_Logging_Options{}
	if len(prometheus_address) > 0 {
		prometheus_options.bind_address = prometheus_address
		prometheus_options.counters_total_suffix = true
		prometheus_options.unit_suffix = true
		runtime_options.prometheus = &prometheus_options
		runtime_options.metrics.global_tags = []temporal_core.Runtime_Key_Value{
			{key = "sdk_language", value = "odin"},
		}
	}
	if len(log_filter) > 0 {
		logging_options.filter = log_filter
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
	if len(prometheus_address) > 0 {
		fmt.printf("Core Prometheus exporter is serving %s.\n", prometheus_address)
		// The integration script scrapes while this process owns the runtime.
		time.sleep(3 * time.Second)
	}
}

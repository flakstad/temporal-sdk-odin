package main

import "core:fmt"
import "core:os"
import temporal_client "../../temporal/client"

main :: proc() {
	path_buffer: [1024]u8
	config_path := os.get_env(path_buffer[:], "TEMPORAL_ODIN_CONFIG_PATH")
	if len(config_path) == 0 {
		fmt.eprintln("TEMPORAL_ODIN_CONFIG_PATH must name a Temporal TOML configuration file")
		os.exit(1)
	}

	profile, load_error := temporal_client.load_config_profile({
		profile = "dev",
		source = {path = config_path},
		disable_environment = true,
		config_file_strict = true,
	})
	if load_error.message != nil {
		fmt.eprintf("Temporal profile load failed: %s\n", string(load_error.message))
		temporal_client.error_free(&load_error)
		os.exit(1)
	}
	defer temporal_client.config_profile_free(&profile)

	client, connect_error := temporal_client.connect_with_config_profile(
		&profile,
		{identity = "temporal-sdk-odin-config-connect-example"},
	)
	if connect_error.message != nil {
		fmt.eprintf("Temporal profile connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		os.exit(1)
	}
	defer temporal_client.close(&client)

	info, info_error := temporal_client.get_system_info(&client)
	if info_error.message != nil {
		fmt.eprintf("GetSystemInfo failed: %s\n", string(info_error.message))
		temporal_client.error_free(&info_error)
		os.exit(1)
	}
	defer temporal_client.system_info_free(&info)
	if len(info.server_version) == 0 {
		fmt.eprintln("GetSystemInfo returned an empty Server version")
		os.exit(1)
	}

	fmt.printf(
		"Core-loaded profile %s connected to Temporal Server %s in namespace %s.\n",
		"dev",
		info.server_version,
		client.namespace,
	)
}

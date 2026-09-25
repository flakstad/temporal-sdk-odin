package main

import "core:fmt"
import "core:os"
import temporal_core "../../temporal/core"

main :: proc() {
	runtime, runtime_error := temporal_core.runtime_create()
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

	info := temporal_core.client_get_system_info(&client)
	if info.error.message != nil {
		fmt.eprintf("GetSystemInfo failed: %s\n", string(info.error.message))
		temporal_core.error_free(&info.error)
		os.exit(1)
	}
	defer delete(info.protobuf)

	fmt.printf("Connected through Temporal SDK Core; GetSystemInfo returned %d protobuf bytes.\n", len(info.protobuf))
}

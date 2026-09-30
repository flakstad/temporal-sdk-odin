package main

import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"

fail :: proc(message: string, error: ^temporal_client.Error = nil) {
	if error != nil && error.message != nil {
		fmt.eprintf("%s: %s (status=%d)\n", message, string(error.message), error.status_code)
		temporal_client.error_free(error)
	} else {
		fmt.eprintln(message)
	}
	os.exit(1)
}

main :: proc() {
	client, connect_error := temporal_client.connect()
	if connect_error.message != nil do fail("Temporal connection failed", &connect_error)
	defer temporal_client.close(&client)

	endpoint_buffer: [256]u8
	endpoint := os.get_env(endpoint_buffer[:], "NEXUS_ENDPOINT")
	if len(endpoint) == 0 do endpoint = "odin-standalone-nexus"
	cancel_buffer: [64]u8
	cancel_mode := os.get_env(cancel_buffer[:], "TEMPORAL_NEXUS_STANDALONE_CANCEL") == "1"
	operation_name := "hello"
	if cancel_mode do operation_name = "wait-for-cancel"
	operation_id := fmt.aprintf("odin-standalone-nexus-%d", time.now()._nsec)
	defer delete(operation_id)

	handle, start_error := temporal_client.start_nexus_operation_json(
		&client,
		"Odin",
		{
			id = operation_id,
			endpoint = endpoint,
			service = "odin-greeting-service",
			operation = operation_name,
			schedule_to_close_timeout = time.Minute,
			id_reuse_policy = .Reject_Duplicate,
			id_conflict_policy = .Fail,
			headers = []temporal_client.Nexus_Operation_Header{{
				key = "x-odin-sdk",
				value = "standalone-nexus",
			}},
		},
	)
	if start_error.message != nil do fail("Nexus operation start failed", &start_error)
	defer temporal_client.nexus_operation_handle_free(&handle)
	if !handle.started_by_request || len(handle.run_id) == 0 {
		fail("Temporal returned an invalid standalone Nexus operation handle")
	}

	if cancel_mode {
		started, poll_error := temporal_client.poll_nexus_operation(
			&handle,
			.Started,
			{timeout = 10 * time.Second},
		)
		if poll_error.message != nil do fail("Nexus operation start poll failed", &poll_error)
		if started.wait_stage == .Unspecified || len(started.operation_token) == 0 {
			temporal_client.nexus_operation_poll_result_free(&started)
			fail("Nexus operation start poll omitted its operation token")
		}
		temporal_client.nexus_operation_poll_result_free(&started)
		cancel_error := temporal_client.cancel_nexus_operation(
			&handle,
			"standalone Nexus integration cancellation",
		)
		if cancel_error.message != nil do fail("Nexus operation cancellation failed", &cancel_error)
		fmt.printf(
			"Standalone Nexus operation %s accepted cancellation through the Odin Client.\n",
			handle.id,
		)
		return
	}

	result: string
	result_error := temporal_client.get_nexus_operation_result_json(
		&handle,
		&result,
		{timeout = 10 * time.Second},
	)
	defer delete(result)
	if result_error.message != nil do fail("Nexus operation result failed", &result_error)
	if result != "Hello Odin" do fail("Standalone Nexus operation returned an unexpected result")
	fmt.printf(
		"Standalone Nexus operation %s/%s returned %q through the Odin Client.\n",
		handle.id,
		handle.run_id,
		result,
	)
}

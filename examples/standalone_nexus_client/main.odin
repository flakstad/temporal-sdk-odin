package main

import "core:bytes"
import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"
import codec "../../temporal/codec"

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
	lifecycle_buffer: [64]u8
	lifecycle_mode := os.get_env(lifecycle_buffer[:], "TEMPORAL_NEXUS_STANDALONE_LIFECYCLE") == "1"
	operation_name := "hello"
	if cancel_mode do operation_name = "wait-for-cancel"
	if lifecycle_mode do operation_name = "wait-for-terminate"
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
	if lifecycle_mode {
		started, poll_error := temporal_client.poll_nexus_operation(
			&handle,
			.Started,
			{timeout = 10 * time.Second},
		)
		if poll_error.message != nil do fail("Nexus operation start poll failed", &poll_error)
		if started.wait_stage != .Started || len(started.operation_token) == 0 {
			temporal_client.nexus_operation_poll_result_free(&started)
			fail("Nexus operation lifecycle start poll omitted its operation token")
		}
		temporal_client.nexus_operation_poll_result_free(&started)

		description, describe_error := temporal_client.describe_nexus_operation(
			&handle,
			{include_input = true, include_outcome = true},
		)
		if describe_error.message != nil do fail("Nexus operation Describe failed", &describe_error)
		if description.status != .Running || !description.has_input ||
		   description.identity != client.identity || len(description.headers) != 1 ||
		   description.headers[0].value != "standalone-nexus" ||
		   !bytes.equal(codec.payload_view(&description.input).data, transmute([]u8)string(`"Odin"`)) {
			temporal_client.nexus_operation_description_free(&description)
			fail("Nexus operation Describe returned unexpected running state")
		}
		temporal_client.nexus_operation_description_free(&description)

		terminate_error := temporal_client.terminate_nexus_operation(
			&handle,
			"standalone Nexus integration termination",
		)
		if terminate_error.message != nil do fail("Nexus operation termination failed", &terminate_error)
		closed, closed_error := temporal_client.poll_nexus_operation(
			&handle,
			.Closed,
			{timeout = 10 * time.Second},
		)
		temporal_client.nexus_operation_poll_result_free(&closed)
		if closed_error.kind != .Nexus_Operation_Terminated {
			fail("Nexus operation did not reach Terminated", &closed_error)
		}
		temporal_client.error_free(&closed_error)

		terminated, terminated_error := temporal_client.describe_nexus_operation(
			&handle,
			{include_outcome = true},
		)
		if terminated_error.message != nil do fail("Terminated Nexus operation Describe failed", &terminated_error)
		if terminated.status != .Terminated || !terminated.has_failure ||
		   terminated.failure_kind != .Nexus_Operation_Terminated {
			temporal_client.nexus_operation_description_free(&terminated)
			fail("Terminated Nexus operation Describe omitted its terminal outcome")
		}
		temporal_client.nexus_operation_description_free(&terminated)
		delete_error := temporal_client.delete_nexus_operation(&handle)
		if delete_error.message != nil do fail("Nexus operation deletion failed", &delete_error)
		_, missing_error := temporal_client.describe_nexus_operation(&handle)
		if missing_error.message == nil || missing_error.status_code != 5 {
			fail("Deleted Nexus operation remained visible", &missing_error)
		}
		temporal_client.error_free(&missing_error)
		fmt.printf(
			"Standalone Nexus operation %s was described, terminated, and deleted through the Odin Client.\n",
			handle.id,
		)
		return
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

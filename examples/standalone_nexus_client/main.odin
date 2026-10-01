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
	metadata := temporal_client.Start_Metadata{}
	if lifecycle_mode {
		metadata_error := temporal_client.start_metadata_init(&metadata)
		if metadata_error.message != nil do fail("Nexus operation metadata initialization failed", &metadata_error)
		summary_error := temporal_client.start_metadata_set_static_summary(&metadata, "Odin standalone lifecycle")
		if summary_error.message != nil do fail("Nexus operation summary failed", &summary_error)
		details_error := temporal_client.start_metadata_set_static_details(&metadata, "Live SDK integration proof")
		if details_error.message != nil do fail("Nexus operation details failed", &details_error)
		search_error := temporal_client.start_metadata_add_search_attribute_json(
			&metadata,
			"OdinNexusTest",
			.Keyword,
			operation_id,
		)
		if search_error.message != nil do fail("Nexus operation Search Attribute failed", &search_error)
	}
	defer temporal_client.start_metadata_free(&metadata)
	initial_links := [?]temporal_client.Link{{
		kind = .Workflow,
		workflow = {
			namespace = "default",
			workflow_id = "odin-initial-link",
			run_id = "00000000-0000-0000-0000-000000000001",
			reason = "standalone Nexus integration",
		},
	}}
	initial_callback_headers := [?]temporal_client.Nexus_Callback_Header{{
		key = "x-odin-callback",
		value = "initial",
	}}
	initial_callbacks := [?]temporal_client.Nexus_Completion_Callback{{
		url = "https://callbacks.invalid/odin-initial",
		headers = initial_callback_headers[:],
	}}
	start_options := temporal_client.Start_Nexus_Operation_Options{
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
	}
	if lifecycle_mode {
		start_options.metadata = &metadata
		start_options.links = initial_links[:]
		start_options.completion_callbacks = initial_callbacks[:]
	}

	handle, start_error := temporal_client.start_nexus_operation_json(
		&client,
		"Odin",
		start_options,
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
		conflict_links := [?]temporal_client.Link{{
			kind = .Workflow,
			workflow = {
				namespace = "default",
				workflow_id = "odin-conflict-link",
				run_id = "00000000-0000-0000-0000-000000000002",
				reason = "attached on conflict",
			},
		}}
		callback_headers := [?]temporal_client.Nexus_Callback_Header{{
			key = "x-odin-callback",
			value = "attached-on-conflict",
		}}
		conflict_callbacks := [?]temporal_client.Nexus_Completion_Callback{{
			url = "https://callbacks.invalid/odin-standalone",
			headers = callback_headers[:],
		}}
		on_conflict := temporal_client.Nexus_Operation_On_Conflict_Options{
			attach_request_id = true,
			attach_completion_callbacks = true,
			attach_links = true,
		}
		existing, existing_error := temporal_client.start_nexus_operation_json(
			&client,
			"Odin",
			{
				id = operation_id,
				endpoint = endpoint,
				service = "odin-greeting-service",
				operation = operation_name,
				schedule_to_close_timeout = time.Minute,
				id_reuse_policy = .Reject_Duplicate,
				id_conflict_policy = .Use_Existing,
				on_conflict_options = &on_conflict,
				links = conflict_links[:],
				completion_callbacks = conflict_callbacks[:],
			},
		)
		if existing_error.message != nil do fail("Nexus operation Use Existing failed", &existing_error)
		if existing.started_by_request || existing.run_id != handle.run_id {
			temporal_client.nexus_operation_handle_free(&existing)
			fail("Nexus operation Use Existing returned an unexpected execution")
		}
		temporal_client.nexus_operation_handle_free(&existing)

		description, describe_error := temporal_client.describe_nexus_operation(
			&handle,
			{include_input = true, include_outcome = true},
		)
		if describe_error.message != nil do fail("Nexus operation Describe failed", &describe_error)
		if description.status != .Running || !description.has_input ||
		   description.identity != client.identity || len(description.headers) != 1 ||
		   description.headers[0].value != "standalone-nexus" ||
		   !description.has_static_summary || description.static_summary != "Odin standalone lifecycle" ||
		   !description.has_static_details || description.static_details != "Live SDK integration proof" ||
		   len(description.search_attributes) != 1 || len(description.long_poll_token) == 0 ||
		   !bytes.equal(codec.payload_view(&description.input).data, transmute([]u8)string(`"Odin"`)) {
			fmt.eprintf(
				"Describe diagnostics: status=%v input=%v identity=%q headers=%d summary=%v/%q details=%v/%q search=%d links=%d callbacks=%d\n",
				description.status,
				description.has_input,
				description.identity,
				len(description.headers),
				description.has_static_summary,
				description.static_summary,
				description.has_static_details,
				description.static_details,
				len(description.search_attributes),
				len(description.links),
				len(description.completion_callbacks),
			)
			temporal_client.nexus_operation_description_free(&description)
			fail("Nexus operation Describe returned unexpected running state")
		}
		if len(description.links) > 0 && description.links[0].kind != .Workflow {
			temporal_client.nexus_operation_description_free(&description)
			fail("Nexus operation Describe returned an unexpected Link")
		}
		if len(description.completion_callbacks) > 0 &&
		   description.completion_callbacks[0].kind != .Nexus {
			temporal_client.nexus_operation_description_free(&description)
			fail("Nexus operation Describe returned an unexpected completion callback")
		}
		long_poll_token := bytes.clone(description.long_poll_token)
		defer delete(long_poll_token)
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
			{
				include_outcome = true,
				long_poll_token = long_poll_token,
				timeout = 10 * time.Second,
			},
		)
		if terminated_error.message != nil do fail("Terminated Nexus operation Describe failed", &terminated_error)
		if terminated.status != .Terminated || !terminated.has_failure ||
		   terminated.failure_kind != .Nexus_Operation_Terminated {
			temporal_client.nexus_operation_description_free(&terminated)
			fail("Terminated Nexus operation Describe omitted its terminal outcome")
		}
		temporal_client.nexus_operation_description_free(&terminated)
		visibility_query := fmt.aprintf(`OperationId = "%s"`, handle.id)
		defer delete(visibility_query)
		page, list_error := temporal_client.list_nexus_operations(
			&client,
			{page_size = 1, query = visibility_query},
		)
		if list_error.message != nil do fail("Nexus operation list failed", &list_error)
		if len(page.operations) != 1 || page.operations[0].operation_id != handle.id ||
		   page.operations[0].status != .Terminated {
			temporal_client.nexus_operation_list_page_free(&page)
			fail("Nexus operation list omitted its terminated execution")
		}
		temporal_client.nexus_operation_list_page_free(&page)
		count, count_error := temporal_client.count_nexus_operations(&client, visibility_query)
		if count_error.message != nil do fail("Nexus operation count failed", &count_error)
		if count.count != 1 {
			temporal_client.nexus_operation_count_free(&count)
			fail("Nexus operation count did not return one execution")
		}
		temporal_client.nexus_operation_count_free(&count)
		delete_error := temporal_client.delete_nexus_operation(&handle)
		if delete_error.message != nil do fail("Nexus operation deletion failed", &delete_error)
		_, missing_error := temporal_client.describe_nexus_operation(&handle)
		if missing_error.message == nil || missing_error.status_code != 5 {
			fail("Deleted Nexus operation remained visible", &missing_error)
		}
		temporal_client.error_free(&missing_error)
		fmt.printf(
			"Standalone Nexus operation %s was described, listed, counted, terminated, and deleted through the Odin Client.\n",
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

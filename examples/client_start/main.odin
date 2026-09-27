package main

import "core:encoding/uuid"
import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"

Typed_Greeting_Input :: struct {
	name:       string,
	salutation: string,
}

Typed_Greeting_Output :: struct {
	message:                        string,
	name_length:                    int,
	activity_id:                    string,
	activity_type:                  string,
	task_queue:                     string,
	namespace:                      string,
	workflow_id:                    string,
	workflow_type:                  string,
	attempt:                        u32,
	is_workflow_activity:           bool,
	has_deadline:                   bool,
	retry_maximum_attempts:         i32,
	heartbeat_timeout_milliseconds: i64,
}

run :: proc() -> int {
	connection_metadata := [?]temporal_client.RPC_Metadata_Entry{{
		key = "x-odin-sdk",
		value = transmute([]u8)string("client-start"),
	}}
	connection_binary_value := [?]u8{0x4f, 0x44, 0x49, 0x4e}
	connection_binary_metadata := [?]temporal_client.RPC_Metadata_Entry{{
		key = "x-odin-sdk-bin",
		value = connection_binary_value[:],
	}}
	connection_retry := temporal_client.Client_Retry_Options{
		initial_interval = 100 * time.Millisecond,
		randomization_factor = 0.2,
		multiplier = 1.7,
		maximum_interval = 5 * time.Second,
		maximum_elapsed_time = 10 * time.Second,
		maximum_retries = 10,
	}
	connection_keep_alive := temporal_client.Keep_Alive_Options{
		interval = 30 * time.Second,
		timeout = 15 * time.Second,
	}
	connection_dns := temporal_client.DNS_Load_Balancing_Options{
		resolution_interval = 30 * time.Second,
	}
	client, connect_error := temporal_client.connect_with_options({
		target_url = "http://localhost:7233",
		namespace = "default",
		identity = "temporal-sdk-odin-client-example",
		metadata = connection_metadata[:],
		binary_metadata = connection_binary_metadata[:],
		retry = &connection_retry,
		keep_alive = &connection_keep_alive,
		dns_load_balancing = &connection_dns,
		grpc_compression = .Gzip,
		payloads_warn_size = 512 * 1024,
		memo_warn_size = 2 * 1024,
	})
	if connect_error.message != nil {
		fmt.eprintf("Temporal client connection failed: %s\n", string(connect_error.message))
		temporal_client.error_free(&connect_error)
		return 1
	}
	defer temporal_client.close(&client)
	updated_metadata := [?]temporal_client.RPC_Metadata_Entry{{
		key = "x-odin-sdk",
		value = transmute([]u8)string("client-start-updated"),
	}}
	metadata_error := temporal_client.update_rpc_metadata(
		&client,
		updated_metadata[:],
		connection_binary_metadata[:],
	)
	if metadata_error.message != nil {
		fmt.eprintf("Temporal client metadata update failed: %s\n", string(metadata_error.message))
		temporal_client.error_free(&metadata_error)
		return 1
	}
	api_key_error := temporal_client.update_api_key(&client, "integration-smoke-key")
	if api_key_error.message != nil {
		fmt.eprintf("Temporal client API-key update failed: %s\n", string(api_key_error.message))
		temporal_client.error_free(&api_key_error)
		return 1
	}
	api_key_error = temporal_client.update_api_key(&client, "")
	if api_key_error.message != nil {
		fmt.eprintf("Temporal client API-key clear failed: %s\n", string(api_key_error.message))
		temporal_client.error_free(&api_key_error)
		return 1
	}

	env_buffer: [128]u8
	workflow_id := os.get_env(env_buffer[:], "TEMPORAL_CLIENT_WORKFLOW_ID")
	generated_id_buffer: [uuid.EXPECTED_LENGTH]u8
	if len(workflow_id) == 0 {
		workflow_id = uuid.to_string_buffer(uuid.generate_v4(), generated_id_buffer[:])
	}
	scenario_buffer: [64]u8
	scenario := os.get_env(scenario_buffer[:], "TEMPORAL_CLIENT_SCENARIO")
	if scenario == "failure-details" {
		handle, start_error := temporal_client.start_workflow_json(
			&client,
			"odin_failure_details_workflow",
			"Odin",
			{
				id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
		)
		if start_error.message != nil {
			fmt.eprintf(
				"Failure-details Workflow start failed (status=%d): %s\n",
				start_error.status_code,
				string(start_error.message),
			)
			temporal_client.error_free(&start_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&handle)
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		defer delete(result)
		if result_error.message != nil {
			fmt.eprintf(
				"Failure-details Workflow result failed (kind=%v status=%d): %s\n",
				result_error.kind,
				result_error.status_code,
				string(result_error.message),
			)
			temporal_client.error_free(&result_error)
			return 1
		}
		fmt.printf(
			"Odin client completed failure-details workflow_id=%s result=%q\n",
			workflow_id,
			result,
		)
		return 0
	}
	if scenario == "workflow-properties" {
		metadata := temporal_client.Start_Metadata{}
		metadata_init_error := temporal_client.start_metadata_init(&metadata)
		if metadata_init_error.message != nil {
			fmt.eprintf("Workflow metadata initialization failed: %s\n", string(metadata_init_error.message))
			temporal_client.error_free(&metadata_init_error)
			return 1
		}
		temporal_client.error_free(&metadata_init_error)
		defer temporal_client.start_metadata_free(&metadata)
		header_error := temporal_client.start_metadata_add_header_json(
			&metadata,
			"odin-header",
			"client-header",
		)
		if header_error.message != nil {
			fmt.eprintf("Workflow header encoding failed: %s\n", string(header_error.message))
			temporal_client.error_free(&header_error)
			return 1
		}
		temporal_client.error_free(&header_error)
		memo_error := temporal_client.start_metadata_add_memo_json(
			&metadata,
			"odin-start",
			"before",
		)
		if memo_error.message != nil {
			fmt.eprintf("Workflow memo encoding failed: %s\n", string(memo_error.message))
			temporal_client.error_free(&memo_error)
			return 1
		}
		temporal_client.error_free(&memo_error)
		search_error := temporal_client.start_metadata_add_search_attribute_json(
			&metadata,
			"OdinSdkKeyword",
			.Keyword,
			"before",
		)
		if search_error.message != nil {
			fmt.eprintf("Search Attribute encoding failed: %s\n", string(search_error.message))
			temporal_client.error_free(&search_error)
			return 1
		}
		temporal_client.error_free(&search_error)
		non_retryable_types := [?]string{"DoNotRetry"}
		retry_policy := temporal_client.Retry_Policy{
			initial_interval = 2 * time.Second,
			backoff_coefficient = 2,
			maximum_interval = 10 * time.Second,
			maximum_attempts = 4,
			non_retryable_error_types = non_retryable_types[:],
		}
		priority := temporal_client.Priority{
			priority_key = 2,
			fairness_key = "tenant-odin",
			fairness_weight = 3,
		}
		handle, start_error := temporal_client.start_workflow_json(
			&client,
			"odin_workflow_properties",
			workflow_id,
			{
				id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
				id_reuse_policy = .Allow_Duplicate,
				id_conflict_policy = .Fail,
				retry_policy = &retry_policy,
				start_delay = time.Second,
				priority = &priority,
				metadata = &metadata,
			},
		)
		if start_error.message != nil {
			fmt.eprintf(
				"Workflow-properties start failed (status=%d): %s\n",
				start_error.status_code,
				string(start_error.message),
			)
			temporal_client.error_free(&start_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&handle)
		existing, existing_error := temporal_client.start_workflow_json(
			&client,
			"odin_workflow_properties",
			workflow_id,
			{
				id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
				id_reuse_policy = .Allow_Duplicate,
				id_conflict_policy = .Use_Existing,
				retry_policy = &retry_policy,
				start_delay = time.Second,
				priority = &priority,
				metadata = &metadata,
			},
		)
		if existing_error.message != nil {
			fmt.eprintf(
				"Workflow-properties use-existing start failed (status=%d): %s\n",
				existing_error.status_code,
				string(existing_error.message),
			)
			temporal_client.error_free(&existing_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&existing)
		if existing.run_id != handle.run_id {
			fmt.eprintf(
				"Workflow-properties use-existing returned run %s instead of %s\n",
				existing.run_id,
				handle.run_id,
			)
			return 1
		}
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		defer delete(result)
		if result_error.message != nil {
			fmt.eprintf(
				"Workflow-properties result failed (kind=%v status=%d): %s\n",
				result_error.kind,
				result_error.status_code,
				string(result_error.message),
			)
			temporal_client.error_free(&result_error)
			return 1
		}
		description, describe_error := temporal_client.describe_workflow(&handle)
		defer temporal_client.workflow_description_free(&description)
		if describe_error.message != nil {
			fmt.eprintf(
				"Workflow-properties Describe failed (status=%d): %s\n",
				describe_error.status_code,
				string(describe_error.message),
			)
			temporal_client.error_free(&describe_error)
			return 1
		}
		if description.workflow_id != workflow_id ||
		   description.run_id != handle.run_id ||
		   description.workflow_type != "odin_workflow_properties" ||
		   description.task_queue != "odin-hello-workflow" ||
		   description.status != .Completed ||
		   description.history_length <= 0 {
			fmt.eprintf(
				"Workflow-properties Describe returned unexpected identity/status: id=%s run=%s type=%s queue=%s status=%v history=%d\n",
				description.workflow_id,
				description.run_id,
				description.workflow_type,
				description.task_queue,
				description.status,
				description.history_length,
			)
			return 1
		}
		memo_updated := false
		for entry in description.memo {
			if entry.key == "odin-status" {
				memo_updated = true
				break
			}
		}
		search_updated := false
		for entry in description.search_attributes {
			if entry.key == "OdinSdkKeyword" {
				search_updated = true
				break
			}
		}
		if !memo_updated || !search_updated {
			fmt.eprintf("Workflow-properties Describe omitted updated memo/Search Attributes\n")
			return 1
		}

		next_page_token: []u8
		defer delete(next_page_token)
		history_event_count := 0
		history_page_count := 0
		first_event_type := temporal_client.History_Event_Type.Unspecified
		last_event_type := temporal_client.History_Event_Type.Unspecified
		for {
			page, page_error := temporal_client.fetch_history_page(
				&handle,
				{
					maximum_page_size = 1,
					next_page_token = next_page_token,
					filter = .All,
					skip_archival = true,
				},
			)
			if page_error.message != nil {
				fmt.eprintf(
					"Workflow-properties history page failed (status=%d): %s\n",
					page_error.status_code,
					string(page_error.message),
				)
				temporal_client.error_free(&page_error)
				temporal_client.history_page_free(&page)
				return 1
			}
			history_page_count += 1
			for event in page.events {
				if history_event_count == 0 {
					first_event_type = event.event_type
				}
				last_event_type = event.event_type
				history_event_count += 1
			}
			delete(next_page_token)
			next_page_token = page.next_page_token
			page.next_page_token = nil
			finished := len(next_page_token) == 0
			temporal_client.history_page_free(&page)
			if finished {
				break
			}
			if history_page_count > 1_000 {
				fmt.eprintf("Workflow-properties history pagination did not terminate\n")
				return 1
			}
		}
		if history_event_count < 2 ||
		   first_event_type != .Workflow_Execution_Started ||
		   last_event_type != .Workflow_Execution_Completed {
			fmt.eprintf(
				"Workflow-properties history was incomplete: events=%d first=%v last=%v\n",
				history_event_count,
				first_event_type,
				last_event_type,
			)
			return 1
		}
		fmt.printf(
			"Odin client completed workflow-properties workflow_id=%s run_id=%s use_existing=true describe_status=%v history_events=%d history_pages=%d result=%q\n",
			workflow_id,
			handle.run_id,
			description.status,
			history_event_count,
			history_page_count,
			result,
		)
		return 0
	}
	handle, start_error := temporal_client.start_workflow_json(
		&client,
		"odin_typed_workflow",
		Typed_Greeting_Input{name = "Odin", salutation = "Hello"},
		{
			id = workflow_id,
			task_queue = "odin-hello-workflow",
			workflow_execution_timeout = time.Minute,
			workflow_run_timeout = time.Minute,
			workflow_task_timeout = 10 * time.Second,
		},
	)
	if start_error.message != nil {
		fmt.eprintf(
			"Workflow start failed (status=%d): %s\n",
			start_error.status_code,
			string(start_error.message),
		)
		temporal_client.error_free(&start_error)
		return 1
	}
	defer temporal_client.workflow_handle_free(&handle)

	result: Typed_Greeting_Output
	defer delete(result.message)
	defer delete(result.activity_id)
	defer delete(result.activity_type)
	defer delete(result.task_queue)
	defer delete(result.namespace)
	defer delete(result.workflow_id)
	defer delete(result.workflow_type)
	result_error := temporal_client.get_result_json(&handle, &result)
	if result_error.message != nil {
		fmt.eprintf(
			"Workflow result failed (kind=%v status=%d): %s\n",
			result_error.kind,
			result_error.status_code,
			string(result_error.message),
		)
		temporal_client.error_free(&result_error)
		return 1
	}

	fmt.printf(
		"Odin client completed workflow_id=%s run_id=%s message=%q activity_id=%s task_queue=%s retry_attempts=%d\n",
		result.workflow_id,
		handle.run_id,
		result.message,
		result.activity_id,
		result.task_queue,
		result.retry_maximum_attempts,
	)
	return 0
}

main :: proc() {
	exit_code := run()
	if exit_code != 0 {
		os.exit(exit_code)
	}
}

package main

import "core:encoding/uuid"
import "core:fmt"
import "core:os"
import "core:time"
import temporal_client "../../temporal/client"
import codec "../../temporal/codec"
import temporal_failure "../../temporal/failure"

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

start_workflow_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Start_Workflow_Input,
	next: temporal_client.Client_Start_Workflow_Next,
) -> (temporal_client.Workflow_Handle, temporal_client.Error) {
	if input.workflow_type == "odin_repeated_arguments_workflow" {
		if len(input.inputs) != 3 {
			return {}, temporal_client.error_from_string(
				.Invalid_Argument,
				"Client interceptor did not receive three Workflow arguments",
			)
		}
		fmt.println("Client start interceptor observed three Workflow arguments")
	}
	header_error := temporal_client.headers_add_json(
		input.headers,
		"odin-interceptor",
		"odin-client-start",
	)
	if header_error.message != nil {
		return {}, header_error
	}
	return temporal_client.client_start_workflow_next(next, input)
}

signal_with_start_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Signal_With_Start_Input,
	next: temporal_client.Client_Signal_With_Start_Next,
) -> (temporal_client.Workflow_Handle, temporal_client.Error) {
	fmt.println("Client Signal-With-Start interceptor entered before Core")
	header_error := temporal_client.headers_add_json(
		input.headers,
		"odin-signal-with-start-interceptor",
		"odin-client-signal-with-start",
	)
	if header_error.message != nil {
		return {}, header_error
	}
	handle, result_error := temporal_client.client_signal_with_start_next(next, input)
	if result_error.message == nil {
		fmt.println("Client Signal-With-Start interceptor exited after Core")
	}
	return handle, result_error
}

signal_workflow_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Signal_Workflow_Input,
	next: temporal_client.Client_Signal_Workflow_Next,
) -> temporal_client.Error {
	if input.signal_name == "repeated" {
		if len(input.inputs) != 3 {
			return temporal_client.error_from_string(
				.Invalid_Argument,
				"Client interceptor did not receive three Signal arguments",
			)
		}
		fmt.println("Client Signal interceptor observed three Signal arguments")
	}
	return temporal_client.client_signal_workflow_next(next, input)
}

query_workflow_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Query_Workflow_Input,
	next: temporal_client.Client_Query_Workflow_Next,
) -> (codec.Payload, temporal_client.Error) {
	if input.query_type == "repeated_status" {
		if len(input.inputs) != 3 {
			return {}, temporal_client.error_from_string(
				.Invalid_Argument,
				"Client interceptor did not receive three Query arguments",
			)
		}
		fmt.println("Client Query interceptor observed three Query arguments")
	}
	return temporal_client.client_query_workflow_next(next, input)
}

get_result_interceptor :: proc(
	_: rawptr,
	input: ^temporal_client.Client_Get_Result_Input,
	next: temporal_client.Client_Get_Result_Next,
) -> (codec.Payload, temporal_client.Error) {
	fmt.printf("Client result interceptor entered follow_runs=%v\n", input.follow_runs)
	result, result_error := temporal_client.client_get_result_next(next, input)
	if result_error.message == nil {
		fmt.println("Client result interceptor exited after terminal Workflow result")
	}
	return result, result_error
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
	interceptors := [?]temporal_client.Client_Interceptor{{
		start_workflow = start_workflow_interceptor,
		signal_with_start = signal_with_start_interceptor,
		signal_workflow = signal_workflow_interceptor,
		query_workflow = query_workflow_interceptor,
		get_result = get_result_interceptor,
	}}
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
		interceptors = interceptors[:],
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
	if scenario == "multiple-workflow-arguments" {
		name, name_error := codec.encode_json("Odin")
		count, count_error := codec.encode_json(42)
		enabled, enabled_error := codec.encode_json(true)
		if name_error != .None || count_error != .None || enabled_error != .None {
			fmt.eprintln("Could not encode repeated Workflow arguments")
			codec.payload_free(&name)
			codec.payload_free(&count)
			codec.payload_free(&enabled)
			return 1
		}
		defer codec.payload_free(&name)
		defer codec.payload_free(&count)
		defer codec.payload_free(&enabled)
		inputs := [3]codec.Payload_View{
			codec.payload_view(&name),
			codec.payload_view(&count),
			codec.payload_view(&enabled),
		}
		handle, start_error := temporal_client.start_workflow_payloads(
			&client,
			"odin_repeated_arguments_workflow",
			inputs[:],
			{
				id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
		)
		if start_error.message != nil {
			fmt.eprintf("Repeated-argument Workflow start failed: %s\n", string(start_error.message))
			temporal_client.error_free(&start_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&handle)
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		defer delete(result)
		if result_error.message != nil {
			fmt.eprintf("Repeated-argument Workflow result failed: %s\n", string(result_error.message))
			temporal_client.error_free(&result_error)
			return 1
		}
		if result != "Odin:42:true" {
			fmt.eprintf("Repeated-argument Workflow returned unexpected result: %q\n", result)
			return 1
		}
		fmt.printf(
			"Odin client completed repeated-argument workflow_id=%s run_id=%s result=%q\n",
			workflow_id,
			handle.run_id,
			result,
		)
		return 0
	}
	if scenario == "multiple-signal-arguments" {
		handle, start_error := temporal_client.start_workflow_json(
			&client,
			"odin_repeated_signal_workflow",
			"start",
			{
				id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
		)
		if start_error.message != nil {
			fmt.eprintf("Repeated-Signal Workflow start failed: %s\n", string(start_error.message))
			temporal_client.error_free(&start_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&handle)
		name, name_error := codec.encode_json("Odin")
		count, count_error := codec.encode_json(42)
		enabled, enabled_error := codec.encode_json(true)
		if name_error != .None || count_error != .None || enabled_error != .None {
			fmt.eprintln("Could not encode repeated Signal arguments")
			codec.payload_free(&name)
			codec.payload_free(&count)
			codec.payload_free(&enabled)
			return 1
		}
		defer codec.payload_free(&name)
		defer codec.payload_free(&count)
		defer codec.payload_free(&enabled)
		if codec.payload_add_metadata(
			&name,
			"messageType",
			transmute([]u8)string("odin.RepeatedSignal"),
		) != .None || codec.payload_add_external(&name, 3072) != .None {
			fmt.eprintln("Could not decorate repeated Signal argument")
			return 1
		}
		inputs := [3]codec.Payload_View{
			codec.payload_view(&name),
			codec.payload_view(&count),
			codec.payload_view(&enabled),
		}
		signal_error := temporal_client.signal_workflow_payloads(
			&handle,
			"repeated",
			inputs[:],
		)
		if signal_error.message != nil {
			fmt.eprintf("Repeated Signal failed: %s\n", string(signal_error.message))
			temporal_client.error_free(&signal_error)
			return 1
		}
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		defer delete(result)
		if result_error.message != nil {
			fmt.eprintf("Repeated-Signal Workflow result failed: %s\n", string(result_error.message))
			temporal_client.error_free(&result_error)
			return 1
		}
		if result != "Odin:42:true" {
			fmt.eprintf("Repeated-Signal Workflow returned unexpected result: %q\n", result)
			return 1
		}
		fmt.printf(
			"Odin client completed repeated-Signal workflow_id=%s run_id=%s result=%q\n",
			workflow_id,
			handle.run_id,
			result,
		)
		return 0
	}
	if scenario == "multiple-query-arguments" {
		handle, start_error := temporal_client.start_workflow_json(
			&client,
			"odin_repeated_query_workflow",
			"start",
			{
				id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
		)
		if start_error.message != nil {
			fmt.eprintf("Repeated-Query Workflow start failed: %s\n", string(start_error.message))
			temporal_client.error_free(&start_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&handle)
		name, name_error := codec.encode_json("Odin")
		count, count_error := codec.encode_json(42)
		enabled, enabled_error := codec.encode_json(true)
		if name_error != .None || count_error != .None || enabled_error != .None {
			fmt.eprintln("Could not encode repeated Query arguments")
			codec.payload_free(&name)
			codec.payload_free(&count)
			codec.payload_free(&enabled)
			return 1
		}
		defer codec.payload_free(&name)
		defer codec.payload_free(&count)
		defer codec.payload_free(&enabled)
		if codec.payload_add_metadata(
			&name,
			"messageType",
			transmute([]u8)string("odin.RepeatedQuery"),
		) != .None || codec.payload_add_external(&name, 4096) != .None {
			fmt.eprintln("Could not decorate repeated Query argument")
			return 1
		}
		inputs := [3]codec.Payload_View{
			codec.payload_view(&name),
			codec.payload_view(&count),
			codec.payload_view(&enabled),
		}
		headers := temporal_client.Headers{}
		headers_error := temporal_client.headers_init(&headers)
		if headers_error.message != nil {
			fmt.eprintf("Query headers initialization failed: %s\n", string(headers_error.message))
			temporal_client.error_free(&headers_error)
			return 1
		}
		temporal_client.error_free(&headers_error)
		defer temporal_client.headers_free(&headers)
		header_error := temporal_client.headers_add_json(&headers, "caller", "repeated-client")
		if header_error.message != nil {
			fmt.eprintf("Query header encoding failed: %s\n", string(header_error.message))
			temporal_client.error_free(&header_error)
			return 1
		}
		temporal_client.error_free(&header_error)
		query_result, query_error := temporal_client.query_workflow_payloads_with_options(
			&handle,
			"repeated_status",
			inputs[:],
			{headers = &headers},
		)
		defer codec.payload_free(&query_result)
		if query_error.message != nil {
			fmt.eprintf("Repeated Query failed: %s\n", string(query_error.message))
			temporal_client.error_free(&query_error)
			return 1
		}
		query_output: string
		defer delete(query_output)
		if codec.decode_json(codec.payload_view(&query_result), &query_output) != .None ||
		   query_output != "Odin:42:true/repeated-client" {
			fmt.eprintf("Repeated Query returned unexpected result: %q\n", query_output)
			return 1
		}
		signal_error := temporal_client.signal_workflow_json(&handle, "finish", "query finished")
		if signal_error.message != nil {
			fmt.eprintf("Repeated-Query finish Signal failed: %s\n", string(signal_error.message))
			temporal_client.error_free(&signal_error)
			return 1
		}
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		defer delete(result)
		if result_error.message != nil {
			fmt.eprintf("Repeated-Query Workflow result failed: %s\n", string(result_error.message))
			temporal_client.error_free(&result_error)
			return 1
		}
		if result != "query finished" {
			fmt.eprintf("Repeated-Query Workflow returned unexpected result: %q\n", result)
			return 1
		}
		fmt.printf(
			"Odin client completed repeated-Query workflow_id=%s run_id=%s query=%q\n",
			workflow_id,
			handle.run_id,
			query_output,
		)
		return 0
	}
	if scenario == "signal-with-start" {
		headers := temporal_client.Headers{}
		headers_init_error := temporal_client.headers_init(&headers)
		if headers_init_error.message != nil {
			fmt.eprintf("Signal header initialization failed: %s\n", string(headers_init_error.message))
			temporal_client.error_free(&headers_init_error)
			return 1
		}
		temporal_client.error_free(&headers_init_error)
		defer temporal_client.headers_free(&headers)
		first_header_error := temporal_client.headers_add_json(&headers, "caller", "first-client")
		if first_header_error.message != nil {
			fmt.eprintf("First Signal header encoding failed: %s\n", string(first_header_error.message))
			temporal_client.error_free(&first_header_error)
			return 1
		}
		temporal_client.error_free(&first_header_error)
		options := temporal_client.Signal_With_Start_Options{
			start_options = {
				id = workflow_id,
				task_queue = "odin-hello-workflow",
				workflow_execution_timeout = time.Minute,
				workflow_run_timeout = time.Minute,
				workflow_task_timeout = 10 * time.Second,
			},
			signal_headers = &headers,
		}
		first, first_error := temporal_client.signal_with_start_workflow_json(
			&client,
			"odin_signal_with_start_workflow",
			"seed",
			"append",
			"first",
			options,
		)
		if first_error.message != nil {
			fmt.eprintf("First Signal-With-Start failed: %s\n", string(first_error.message))
			temporal_client.error_free(&first_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&first)
		if !first.started_by_request {
			fmt.eprintln("First Signal-With-Start did not create the Workflow Execution")
			return 1
		}

		second_header_error := temporal_client.headers_add_json(&headers, "caller", "second-client")
		if second_header_error.message != nil {
			fmt.eprintf("Second Signal header encoding failed: %s\n", string(second_header_error.message))
			temporal_client.error_free(&second_header_error)
			return 1
		}
		temporal_client.error_free(&second_header_error)
		second, second_error := temporal_client.signal_with_start_workflow_json(
			&client,
			"odin_signal_with_start_workflow",
			"ignored-on-reuse",
			"append",
			"second",
			options,
		)
		if second_error.message != nil {
			fmt.eprintf("Second Signal-With-Start failed: %s\n", string(second_error.message))
			temporal_client.error_free(&second_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&second)
		if second.started_by_request || second.run_id != first.run_id ||
		   second.first_execution_run_id != first.first_execution_run_id {
			fmt.eprintln("Second Signal-With-Start did not reuse the original Workflow Execution")
			return 1
		}
		result: string
		result_error := temporal_client.get_result_json(&second, &result)
		defer delete(result)
		if result_error.message != nil {
			fmt.eprintf("Signal-With-Start result failed: %s\n", string(result_error.message))
			temporal_client.error_free(&result_error)
			return 1
		}
		if result != "seed:first/first-client:second/second-client" {
			fmt.eprintf("Signal-With-Start returned unexpected result: %q\n", result)
			return 1
		}
		fmt.printf(
			"Odin Signal-With-Start created and reused workflow_id=%s run_id=%s result=%q\n",
			workflow_id,
			second.run_id,
			result,
		)
		return 0
	}
	if scenario == "terminal-failure" {
		handle, start_error := temporal_client.start_workflow_json(
			&client,
			"odin_terminal_failure_workflow",
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
			fmt.eprintf("Terminal-failure Workflow start failed: %s\n", string(start_error.message))
			temporal_client.error_free(&start_error)
			return 1
		}
		defer temporal_client.workflow_handle_free(&handle)
		result: string
		result_error := temporal_client.get_result_json(&handle, &result)
		defer delete(result)
		if result_error.kind != .Workflow_Failed {
			fmt.eprintf(
				"Expected terminal Workflow failure, got kind=%v message=%s\n",
				result_error.kind,
				string(result_error.message),
			)
			temporal_client.error_free(&result_error)
			return 1
		}
		decoded, decode_error := temporal_client.decode_error_failure(&result_error)
		defer temporal_failure.failure_free(&decoded)
		if decode_error != .None {
			fmt.eprintf("Could not decode terminal Workflow Failure: %v\n", decode_error)
			temporal_client.error_free(&result_error)
			return 1
		}
		activity_failure := temporal_failure.find_kind(&decoded, .Activity)
		application_failure := temporal_failure.find_kind(&decoded, .Application)
		if activity_failure == nil || application_failure == nil ||
		   activity_failure.activity.activity_id != "terminal-failure-activity" ||
		   activity_failure.activity.activity_type != "failure_details" ||
		   activity_failure.activity.retry_state != .Maximum_Attempts_Reached ||
		   application_failure.message != "billing dependency rejected request" ||
		   application_failure.application.type != "DependencyRejected" ||
		   !application_failure.application.has_next_retry_delay ||
		   application_failure.application.next_retry_delay != 250 * time.Millisecond ||
		   len(application_failure.application.details) != 1 {
			fmt.eprintln("Terminal Workflow Failure did not preserve the Activity/Application chain")
			temporal_client.error_free(&result_error)
			return 1
		}
		fmt.printf(
			"Odin client decoded recursive terminal failure workflow_id=%s activity_id=%s application_type=%s\n",
			workflow_id,
			activity_failure.activity.activity_id,
			application_failure.application.type,
		)
		temporal_client.error_free(&result_error)
		return 0
	}
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
		summary_error := temporal_client.start_metadata_set_static_summary(
			&metadata,
			"Odin workflow properties",
		)
		if summary_error.message != nil {
			fmt.eprintf("Workflow static summary encoding failed: %s\n", string(summary_error.message))
			temporal_client.error_free(&summary_error)
			return 1
		}
		temporal_client.error_free(&summary_error)
		details_error := temporal_client.start_metadata_set_static_details(
			&metadata,
			"Exercises Workflow metadata through the experimental Odin SDK.",
		)
		if details_error.message != nil {
			fmt.eprintf("Workflow static details encoding failed: %s\n", string(details_error.message))
			temporal_client.error_free(&details_error)
			return 1
		}
		temporal_client.error_free(&details_error)
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
		rejected_query_result: string
		defer delete(rejected_query_result)
		rejected_query_error := temporal_client.query_workflow_json_with_options(
			&handle,
			"not_delivered_after_close",
			"query-input",
			&rejected_query_result,
			{reject_condition = .Not_Open},
		)
		if rejected_query_error.kind != .Workflow_Query_Rejected ||
		   !rejected_query_error.has_workflow_status ||
		   rejected_query_error.workflow_status != .Completed {
			fmt.eprintf(
				"Workflow-properties query was not rejected as completed: kind=%v has_status=%v status=%v message=%s\n",
				rejected_query_error.kind,
				rejected_query_error.has_workflow_status,
				rejected_query_error.workflow_status,
				string(rejected_query_error.message),
			)
			temporal_client.error_free(&rejected_query_error)
			return 1
		}
		temporal_client.error_free(&rejected_query_error)
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
		   !description.has_static_summary ||
		   description.static_summary != "Odin workflow properties" ||
		   !description.has_static_details ||
		   description.static_details != "Exercises Workflow metadata through the experimental Odin SDK." ||
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

		visibility_query := fmt.aprintf("WorkflowId = %q", workflow_id)
		defer delete(visibility_query)
		visibility_page, visibility_error := temporal_client.list_workflows(
			&client,
			{page_size = 1, query = visibility_query},
		)
		defer temporal_client.workflow_list_page_free(&visibility_page)
		if visibility_error.message != nil {
			fmt.eprintf(
				"Workflow-properties Visibility list failed (status=%d): %s\n",
				visibility_error.status_code,
				string(visibility_error.message),
			)
			temporal_client.error_free(&visibility_error)
			return 1
		}
		visibility_listed := false
		for summary in visibility_page.executions {
			if summary.workflow_id == workflow_id &&
			   summary.run_id == handle.run_id &&
			   summary.workflow_type == "odin_workflow_properties" &&
			   summary.status == .Completed {
				visibility_listed = true
				break
			}
		}
		if !visibility_listed {
			fmt.eprintf("Workflow-properties execution was absent from Visibility list\n")
			return 1
		}
		visibility_count, count_error := temporal_client.count_workflows(&client, visibility_query)
		defer temporal_client.workflow_count_free(&visibility_count)
		if count_error.message != nil {
			fmt.eprintf(
				"Workflow-properties Visibility count failed (status=%d): %s\n",
				count_error.status_code,
				string(count_error.message),
			)
			temporal_client.error_free(&count_error)
			return 1
		}
		if visibility_count.count < 1 {
			fmt.eprintf("Workflow-properties Visibility count was %d\n", visibility_count.count)
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
			"Odin client completed workflow-properties workflow_id=%s run_id=%s use_existing=true query_rejected_status=%v static_metadata=true describe_status=%v visibility_listed=%v visibility_count=%d history_events=%d history_pages=%d result=%q\n",
			workflow_id,
			handle.run_id,
			temporal_client.Workflow_Status.Completed,
			description.status,
			visibility_listed,
			visibility_count.count,
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

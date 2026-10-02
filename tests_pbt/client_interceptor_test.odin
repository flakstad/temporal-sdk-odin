package workflow_pbt_test

import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"
import schedule "../temporal/client/schedule"
import temporal_core "../temporal/core"
import raw "../temporal/core/raw"
import codec "../temporal/codec"

Client_Interceptor_Property_State :: struct {
	events:      [32]int,
	event_count: int,
}

Client_Interceptor_Property_Node :: struct {
	state:         ^Client_Interceptor_Property_State,
	id:            int,
	short_circuit: bool,
}

client_interceptor_property_signal :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Signal_Workflow_Input,
	next: temporal_client.Client_Signal_Workflow_Next,
) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	header_error := temporal_client.headers_add_json(input.headers, "generated-depth", node.id)
	if header_error.message != nil {
		return header_error
	}
	result: temporal_client.Error
	if node.short_circuit {
		result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result = temporal_client.client_signal_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

client_interceptor_property_signal_with_start :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Signal_With_Start_Input,
	next: temporal_client.Client_Signal_With_Start_Next,
) -> (temporal_client.Workflow_Handle, temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	header_error := temporal_client.headers_add_json(input.headers, "generated-depth", node.id)
	if header_error.message != nil {
		return {}, header_error
	}
	result: temporal_client.Workflow_Handle
	result_error: temporal_client.Error
	if node.short_circuit {
		result_error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result, result_error = temporal_client.client_signal_with_start_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result, result_error
}

client_interceptor_property_update_with_start :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Update_With_Start_Input,
	next: temporal_client.Client_Update_With_Start_Next,
) -> (result: temporal_client.Update_With_Start_Result, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	start_error := temporal_client.headers_add_json(input.start_headers, "generated-depth", node.id)
	if start_error.message != nil {
		return {}, start_error
	}
	update_error := temporal_client.headers_add_json(input.update_headers, "generated-depth", node.id)
	if update_error.message != nil {
		return {}, update_error
	}
	if node.short_circuit {
		error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result, error = temporal_client.client_update_with_start_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return
}

client_interceptor_property_get_result :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Get_Result_Input,
	next: temporal_client.Client_Get_Result_Next,
) -> (payload: codec.Payload, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	input.follow_runs = node.id % 2 == 0
	if node.short_circuit {
		error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		payload, error = temporal_client.client_get_result_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return
}

client_interceptor_property_wait_update :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Wait_Update_Input,
	next: temporal_client.Client_Wait_Update_Next,
) -> (payload: codec.Payload, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	if node.id > 1 && input.options.timeout != 2 * time.Second {
		return {}, temporal_client.error_from_string(.Invalid_Argument, "Update wait interceptor input mutation was not propagated")
	}
	input.options.timeout = 2 * time.Second
	if node.short_circuit {
		error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		payload, error = temporal_client.client_wait_update_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return
}

client_interceptor_property_cancel :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Cancel_Workflow_Input,
	next: temporal_client.Client_Cancel_Workflow_Next,
) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	input.reason = "generated cancellation"
	result: temporal_client.Error
	if node.short_circuit {
		result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result = temporal_client.client_cancel_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

client_interceptor_property_terminate :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Terminate_Workflow_Input,
	next: temporal_client.Client_Terminate_Workflow_Next,
) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	input.reason = "generated termination"
	result: temporal_client.Error
	if node.short_circuit {
		result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result = temporal_client.client_terminate_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

client_interceptor_property_delete :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Delete_Workflow_Input,
	next: temporal_client.Client_Delete_Workflow_Next,
) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	input.options.allow_latest_run = node.id % 2 == 0
	result: temporal_client.Error
	if node.short_circuit {
		result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result = temporal_client.client_delete_workflow_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return result
}

client_interceptor_property_list_workflows :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_List_Workflows_Input,
	next: temporal_client.Client_List_Workflows_Next,
) -> (page: temporal_client.Workflow_List_Page, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	input.options.query = "generated list query"
	input.options.page_size = i32(node.id)
	if node.short_circuit {
		error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		page, error = temporal_client.client_list_workflows_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return
}

client_interceptor_property_count_workflows :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Count_Workflows_Input,
	next: temporal_client.Client_Count_Workflows_Next,
) -> (result: temporal_client.Workflow_Count, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	input.query = "generated count query"
	if node.short_circuit {
		error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result, error = temporal_client.client_count_workflows_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return
}

client_interceptor_property_describe_schedule :: proc(user_data: rawptr, input: ^schedule.Client_Describe_Schedule_Input, next: schedule.Client_Describe_Schedule_Next) -> (result: schedule.Schedule_Description, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = schedule.client_describe_schedule_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_list_schedules :: proc(user_data: rawptr, input: ^schedule.Client_List_Schedules_Input, next: schedule.Client_List_Schedules_Next) -> (result: schedule.Schedule_List_Page, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	input.options.maximum_page_size = i32(node.id)
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = schedule.client_list_schedules_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_count_schedules :: proc(user_data: rawptr, input: ^schedule.Client_Count_Schedules_Input, next: schedule.Client_Count_Schedules_Next) -> (result: schedule.Schedule_Count, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	input.query = "generated Schedule count query"
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = schedule.client_count_schedules_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_matching_times :: proc(user_data: rawptr, input: ^schedule.Client_List_Schedule_Matching_Times_Input, next: schedule.Client_List_Schedule_Matching_Times_Next) -> (result: schedule.Schedule_Matching_Times, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = schedule.client_list_schedule_matching_times_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_create_schedule :: proc(user_data: rawptr, input: ^schedule.Client_Create_Schedule_Input, next: schedule.Client_Create_Schedule_Next) -> (result: schedule.Schedule_Handle, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = schedule.client_create_schedule_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_schedule_mutation :: proc(user_data: rawptr, input: ^schedule.Client_Schedule_Mutation_Input, next: schedule.Client_Schedule_Mutation_Next) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	result: temporal_client.Error
	if node.short_circuit do result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result = schedule.client_schedule_mutation_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return result
}

client_interceptor_property_connection_mutation :: proc(user_data: rawptr, input: ^temporal_client.Client_Connection_Mutation_Input, next: temporal_client.Client_Connection_Mutation_Next) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	if node.id > 1 && input.api_key != "generated API key" {
		return temporal_client.error_from_string(.Invalid_Argument, "connection interceptor input mutation was not propagated")
	}
	input.api_key = "generated API key"
	result: temporal_client.Error
	if node.short_circuit do result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result = temporal_client.client_connection_mutation_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return result
}

client_interceptor_property_start_nexus_operation :: proc(user_data: rawptr, input: ^temporal_client.Client_Start_Nexus_Operation_Input, next: temporal_client.Client_Start_Nexus_Operation_Next) -> (result: temporal_client.Nexus_Operation_Handle, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	if node.id > 1 && input.options.operation != "generated operation" {
		return {}, temporal_client.error_from_string(.Invalid_Argument, "Nexus start interceptor input mutation was not propagated")
	}
	input.options.operation = "generated operation"
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = temporal_client.client_start_nexus_operation_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_poll_nexus_operation :: proc(user_data: rawptr, input: ^temporal_client.Client_Poll_Nexus_Operation_Input, next: temporal_client.Client_Poll_Nexus_Operation_Next) -> (result: temporal_client.Nexus_Operation_Poll_Result, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	if node.id > 1 && input.wait_stage != .Started {
		return {}, temporal_client.error_from_string(.Invalid_Argument, "Nexus poll interceptor input mutation was not propagated")
	}
	input.wait_stage = .Started
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = temporal_client.client_poll_nexus_operation_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_describe_nexus_operation :: proc(user_data: rawptr, input: ^temporal_client.Client_Describe_Nexus_Operation_Input, next: temporal_client.Client_Describe_Nexus_Operation_Next) -> (result: temporal_client.Nexus_Operation_Description, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	if node.id > 1 && !input.options.include_input {
		return {}, temporal_client.error_from_string(.Invalid_Argument, "Nexus Describe interceptor input mutation was not propagated")
	}
	input.options.include_input = true
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = temporal_client.client_describe_nexus_operation_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_nexus_operation_mutation :: proc(user_data: rawptr, input: ^temporal_client.Client_Nexus_Operation_Mutation_Input, next: temporal_client.Client_Nexus_Operation_Mutation_Next) -> temporal_client.Error {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	if node.id > 1 && input.reason != "generated Nexus mutation" {
		return temporal_client.error_from_string(.Invalid_Argument, "Nexus mutation interceptor input mutation was not propagated")
	}
	input.reason = "generated Nexus mutation"
	result: temporal_client.Error
	if node.short_circuit do result = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result = temporal_client.client_nexus_operation_mutation_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return result
}

client_interceptor_property_list_nexus_operations :: proc(user_data: rawptr, input: ^temporal_client.Client_List_Nexus_Operations_Input, next: temporal_client.Client_List_Nexus_Operations_Next) -> (result: temporal_client.Nexus_Operation_List_Page, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	input.options.query = "generated Nexus list query"
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = temporal_client.client_list_nexus_operations_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_count_nexus_operations :: proc(user_data: rawptr, input: ^temporal_client.Client_Count_Nexus_Operations_Input, next: temporal_client.Client_Count_Nexus_Operations_Next) -> (result: temporal_client.Nexus_Operation_Count, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id; node.state.event_count += 1
	input.query = "generated Nexus count query"
	if node.short_circuit do error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	else do result, error = temporal_client.client_count_nexus_operations_next(next, input)
	node.state.events[node.state.event_count] = -node.id; node.state.event_count += 1
	return
}

client_interceptor_property_system_info :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Get_System_Info_Input,
	next: temporal_client.Client_Get_System_Info_Next,
) -> (result: temporal_client.System_Info, error: temporal_client.Error) {
	node := cast(^Client_Interceptor_Property_Node)user_data
	node.state.events[node.state.event_count] = node.id
	node.state.event_count += 1
	if node.short_circuit {
		error = temporal_client.error_from_string(.Rpc, "generated short circuit")
	} else {
		result, error = temporal_client.client_get_system_info_next(next, input)
	}
	node.state.events[node.state.event_count] = -node.id
	node.state.event_count += 1
	return
}

client_interceptor_chain_property :: proc(t: ^pbt.T) -> pbt.Result {
	depth := pbt.draw(t, pbt.int_range(1, 12))
	short_index := pbt.draw(t, pbt.int_range(0, depth - 1))
	operation := pbt.draw(t, pbt.int_range(0, 23))
	pbt.cover(t, short_index == 0, 5.0, "outermost short circuit")
	pbt.cover(t, short_index == depth - 1, 5.0, "innermost short circuit")
	pbt.cover(t, depth >= 9, 20.0, "deep chain")
	pbt.cover(t, operation == 0, 3.0, "Workflow Signal")
	pbt.cover(t, operation == 1, 3.0, "Workflow cancellation")
	pbt.cover(t, operation == 2, 3.0, "Workflow termination")
	pbt.cover(t, operation == 3, 3.0, "Signal-With-Start")
	pbt.cover(t, operation == 4, 3.0, "Workflow result")
	pbt.cover(t, operation == 5, 3.0, "Workflow Visibility list")
	pbt.cover(t, operation == 6, 3.0, "Workflow Visibility count")
	pbt.cover(t, operation >= 7 && operation <= 10, 12.0, "Schedule read operation")
	pbt.cover(t, operation == 11 || operation == 12, 6.0, "Schedule mutation operation")
	pbt.cover(t, operation == 13, 3.0, "connection mutation operation")
	pbt.cover(t, operation == 14, 3.0, "Update wait operation")
	pbt.cover(t, operation >= 15 && operation <= 18, 12.0, "Nexus lifecycle operation")
	pbt.cover(t, operation == 19 || operation == 20, 6.0, "Nexus Visibility operation")
	pbt.cover(t, operation == 21, 3.0, "Update-With-Start operation")
	pbt.cover(t, operation == 22, 3.0, "System Info operation")
	pbt.cover(t, operation == 23, 3.0, "Workflow deletion")

	state := Client_Interceptor_Property_State{}
	nodes: [12]Client_Interceptor_Property_Node
	client := temporal_client.Client{
		interceptors = make([dynamic]temporal_client.Client_Interceptor),
	}
	defer delete(client.interceptors)
	defer temporal_client.client_extensions_close(&client)
	connection := temporal_core.Client{handle = cast(^raw.Connection)(uintptr(1))}
	client.connection = &connection
	for index in 0..<depth {
		nodes[index] = {
			state = &state,
			id = index + 1,
			short_circuit = index == short_index,
		}
		interceptor := temporal_client.Client_Interceptor{user_data = &nodes[index]}
		schedule_interceptor := schedule.Interceptor{user_data = &nodes[index]}
		is_schedule_operation := operation >= 7 && operation <= 12
		switch operation {
		case 0:
			interceptor.signal_workflow = client_interceptor_property_signal
		case 1:
			interceptor.cancel_workflow = client_interceptor_property_cancel
		case 2:
			interceptor.terminate_workflow = client_interceptor_property_terminate
		case 3:
			interceptor.signal_with_start = client_interceptor_property_signal_with_start
		case 4:
			interceptor.get_result = client_interceptor_property_get_result
		case 5:
			interceptor.list_workflows = client_interceptor_property_list_workflows
		case 6:
			interceptor.count_workflows = client_interceptor_property_count_workflows
		case 7:
			schedule_interceptor.describe_schedule = client_interceptor_property_describe_schedule
		case 8:
			schedule_interceptor.list_schedules = client_interceptor_property_list_schedules
		case 9:
			schedule_interceptor.count_schedules = client_interceptor_property_count_schedules
		case 10:
			schedule_interceptor.list_schedule_matching_times = client_interceptor_property_matching_times
		case 11:
			schedule_interceptor.create_schedule = client_interceptor_property_create_schedule
		case 12:
			schedule_interceptor.schedule_mutation = client_interceptor_property_schedule_mutation
		case 13:
			interceptor.connection_mutation = client_interceptor_property_connection_mutation
		case 14:
			interceptor.wait_update = client_interceptor_property_wait_update
		case 15:
			interceptor.start_nexus_operation = client_interceptor_property_start_nexus_operation
		case 16:
			interceptor.poll_nexus_operation = client_interceptor_property_poll_nexus_operation
		case 17:
			interceptor.describe_nexus_operation = client_interceptor_property_describe_nexus_operation
		case 18:
			interceptor.nexus_operation_mutation = client_interceptor_property_nexus_operation_mutation
		case 19:
			interceptor.list_nexus_operations = client_interceptor_property_list_nexus_operations
		case 20:
			interceptor.count_nexus_operations = client_interceptor_property_count_nexus_operations
		case 21:
			interceptor.update_with_start = client_interceptor_property_update_with_start
		case 22:
			interceptor.get_system_info = client_interceptor_property_system_info
		case 23:
			interceptor.delete_workflow = client_interceptor_property_delete
		}
		if is_schedule_operation {
			add_error := schedule.add_interceptor(&client, schedule_interceptor)
			if add_error.message != nil {
				temporal_client.error_free(&add_error)
				return pbt.error("could not register generated Schedule interceptor")
			}
		} else {
			append(&client.interceptors, interceptor)
		}
	}

	headers := temporal_client.Headers{}
	init_error := temporal_client.headers_init(&headers)
	if init_error.message != nil {
		temporal_client.error_free(&init_error)
		return pbt.error("could not initialize generated caller headers")
	}
	temporal_client.error_free(&init_error)
	defer temporal_client.headers_free(&headers)
	caller_error := temporal_client.headers_add_json(&headers, "caller", "property")
	if caller_error.message != nil {
		temporal_client.error_free(&caller_error)
		return pbt.error("could not add generated caller header")
	}
	temporal_client.error_free(&caller_error)
	handle := temporal_client.Workflow_Handle{client = &client, id = "property-workflow"}
	schedule_handle := schedule.Schedule_Handle{client = &client, id = "property-schedule"}
	update_handle := temporal_client.Update_Handle{
		client = &client,
		workflow_id = "property-workflow",
		update_id = "property-update",
	}
	nexus_handle := temporal_client.Nexus_Operation_Handle{
		client = &client,
		id = "property-operation",
		run_id = "property-run",
	}
	operation_error: temporal_client.Error
	switch operation {
	case 0:
		operation_error = temporal_client.signal_workflow_with_options(
			&handle,
			"property-signal",
			{headers = &headers},
		)
	case 1:
		operation_error = temporal_client.cancel_workflow(&handle, "caller cancellation")
	case 2:
		operation_error = temporal_client.terminate_workflow(&handle, "caller termination")
	case 3:
		_, operation_error = temporal_client.signal_with_start_workflow_payload(
			&client,
			"property_workflow",
			{},
			"property-signal",
			{},
			{
				start_options = {id = "property-workflow", task_queue = "property-queue"},
				signal_headers = &headers,
			},
		)
	case 4:
		result: codec.Payload
		result, operation_error = temporal_client.get_result_payload(&handle)
		codec.payload_free(&result)
	case 5:
		page: temporal_client.Workflow_List_Page
		page, operation_error = temporal_client.list_workflows(
			&client,
			{page_size = 100, query = "caller list query"},
		)
		temporal_client.workflow_list_page_free(&page)
	case 6:
		count: temporal_client.Workflow_Count
		count, operation_error = temporal_client.count_workflows(&client, "caller count query")
		temporal_client.workflow_count_free(&count)
	case 7:
		description: schedule.Schedule_Description
		description, operation_error = schedule.describe_schedule(&schedule_handle)
		schedule.schedule_description_free(&description)
	case 8:
		page: schedule.Schedule_List_Page
		page, operation_error = schedule.list_schedules(&client, {maximum_page_size = 100})
		schedule.schedule_list_page_free(&page)
	case 9:
		count: schedule.Schedule_Count
		count, operation_error = schedule.count_schedules(&client, "caller Schedule query")
		schedule.schedule_count_free(&count)
	case 10:
		matching: schedule.Schedule_Matching_Times
		start := time.now()
		matching, operation_error = schedule.list_schedule_matching_times(
			&schedule_handle,
			start,
			time.time_add(start, time.Hour),
		)
		schedule.schedule_matching_times_free(&matching)
	case 11:
		created: schedule.Schedule_Handle
		created, operation_error = schedule.create_schedule_payload(
			&client,
			"property-schedule",
			"property-workflow",
			{},
			{},
		)
		schedule.schedule_handle_free(&created)
	case 12:
		operation_error = schedule.pause_schedule(&schedule_handle, "property pause")
	case 13:
		operation_error = temporal_client.update_api_key(&client, "caller API key")
	case 14:
		result: codec.Payload
		result, operation_error = temporal_client.wait_update_payload(&update_handle)
		codec.payload_free(&result)
	case 15:
		started: temporal_client.Nexus_Operation_Handle
		started, operation_error = temporal_client.start_nexus_operation_payload(
			&client,
			{},
			{
				id = "property-operation",
				endpoint = "property-endpoint",
				service = "property-service",
				operation = "caller-operation",
				schedule_to_close_timeout = time.Minute,
			},
		)
		temporal_client.nexus_operation_handle_free(&started)
	case 16:
		result: temporal_client.Nexus_Operation_Poll_Result
		result, operation_error = temporal_client.poll_nexus_operation(&nexus_handle)
		temporal_client.nexus_operation_poll_result_free(&result)
	case 17:
		result: temporal_client.Nexus_Operation_Description
		result, operation_error = temporal_client.describe_nexus_operation(&nexus_handle)
		temporal_client.nexus_operation_description_free(&result)
	case 18:
		operation_error = temporal_client.nexus_operation_mutation(
			&nexus_handle,
			temporal_client.Nexus_Operation_Mutation_Kind(pbt.draw(t, pbt.int_range(0, 2))),
			"caller reason",
		)
	case 19:
		page: temporal_client.Nexus_Operation_List_Page
		page, operation_error = temporal_client.list_nexus_operations(&client)
		temporal_client.nexus_operation_list_page_free(&page)
	case 20:
		count: temporal_client.Nexus_Operation_Count
		count, operation_error = temporal_client.count_nexus_operations(&client)
		temporal_client.nexus_operation_count_free(&count)
	case 21:
		result: temporal_client.Update_With_Start_Result
		result, operation_error = temporal_client.update_with_start_payload(
			&client,
			"property_workflow",
			{},
			"property-update",
			{},
			{
				start_options = {
					id = "property-workflow",
					task_queue = "property-queue",
					id_conflict_policy = .Use_Existing,
				},
				update_options = {headers = &headers},
			},
		)
		temporal_client.update_with_start_result_free(&result)
	case 22:
		info: temporal_client.System_Info
		info, operation_error = temporal_client.get_system_info(&client)
		temporal_client.system_info_free(&info)
	case 23:
		operation_error = temporal_client.delete_workflow(&handle)
	}
	defer temporal_client.error_free(&operation_error)
	if operation_error.kind != .Rpc || string(operation_error.message) != "generated short circuit" {
		return pbt.fail("generated Client interceptor did not short circuit")
	}
	reached := short_index + 1
	if state.event_count != reached * 2 {
		return pbt.fail("Client interceptor chain emitted the wrong event count")
	}
	for index in 0..<reached {
		if state.events[index] != index + 1 {
			return pbt.fail("Client interceptor entry order changed")
		}
		if state.events[reached + index] != -(reached - index) {
			return pbt.fail("Client interceptor unwind order changed")
		}
	}
	if len(headers.entries) != 1 || headers.entries[0].key != "caller" {
		return pbt.fail("Client interceptor mutated caller-owned headers")
	}
	return pbt.pass()
}

@(test)
client_interceptors_preserve_nested_next_and_header_isolation :: proc(t: ^testing.T) {
	result := pbt.check(
		"Client interceptors preserve nested next and header isolation",
		client_interceptor_chain_property,
		{num_tests = 1000, seed = 0x434c_4945_4e54_494e},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

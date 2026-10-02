package main

import "core:fmt"
import "core:os"
import temporal_client "../../temporal/client"
import schedule "../../temporal/client/schedule"
import standalone_nexus "../../temporal/client/standalone_nexus"

System_Info_Interceptor_State :: struct {
	entered: int,
	exited:  int,
}

system_info_interceptor :: proc(
	user_data: rawptr,
	input: ^temporal_client.Client_Get_System_Info_Input,
	next: temporal_client.Client_Get_System_Info_Next,
) -> (temporal_client.System_Info, temporal_client.Error) {
	state := cast(^System_Info_Interceptor_State)user_data
	state.entered += 1
	info, info_error := temporal_client.client_get_system_info_next(next, input)
	if info_error.message == nil {
		state.exited += 1
	}
	return info, info_error
}

main :: proc() {
	interceptor_state := System_Info_Interceptor_State{}
	interceptors := [?]temporal_client.Client_Interceptor{{
		user_data = &interceptor_state,
		get_system_info = system_info_interceptor,
	}}
	client, connect_error := temporal_client.connect_with_options({
		target_url = "http://localhost:7233",
		namespace = "default",
		identity = "temporal-sdk-odin-system-info-example",
		interceptors = interceptors[:],
	})
	if connect_error.message != nil {
		fmt.eprintf("Temporal connection failed: %s\n", string(connect_error.message))
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
	if interceptor_state.entered != 1 || interceptor_state.exited != 1 {
		fmt.eprintln("System Info interceptor did not surround the Core RPC")
		os.exit(1)
	}

	fmt.printf(
		"Temporal Server %s capabilities: schedules=%v nexus=%v eager_workflow_start=%v sdk_metadata=%v\n",
		info.server_version,
		info.capabilities.supports_schedules,
		info.capabilities.nexus,
		info.capabilities.eager_workflow_start,
		info.capabilities.sdk_metadata,
	)
	fmt.println("System Info interceptor surrounded the live Core RPC")

	features := [?]temporal_client.Server_Feature{
		.Schedules,
		.Schedule_Count,
		.Nexus,
		.Standalone_Nexus,
		.Workflow_Pause,
		.Standalone_Activity,
	}
	for feature in features {
		observation: temporal_client.Server_Feature_Observation
		probe_error: temporal_client.Error
		if feature == .Schedule_Count {
			observation, probe_error = schedule.probe_count_support(&client)
		} else if feature == .Standalone_Nexus {
			observation, probe_error = standalone_nexus.probe_support(&client)
		} else {
			observation, probe_error = temporal_client.probe_server_feature(&client, feature)
		}
		if probe_error.message != nil {
			fmt.eprintf(
				"Feature probe %s failed (status=%d): %s\n",
				temporal_client.server_feature_name(feature),
				probe_error.status_code,
				string(probe_error.message),
			)
			temporal_client.error_free(&probe_error)
			os.exit(1)
		}
		fmt.printf(
			"feature=%s sdk=%v support=%s evidence=%s status=%d\n",
			temporal_client.server_feature_name(feature),
			observation.sdk_implemented,
			temporal_client.server_feature_support_name(observation.support),
			temporal_client.server_feature_evidence_name(observation.evidence),
			observation.status_code,
		)
	}
}

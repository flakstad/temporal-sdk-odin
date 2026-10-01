package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"
import proto "../temporal/core/proto"

system_capabilities_equal :: proc(
	actual, expected: temporal_client.System_Capabilities,
) -> bool {
	return actual.signal_and_query_header == expected.signal_and_query_header &&
	       actual.internal_error_differentiation == expected.internal_error_differentiation &&
	       actual.activity_failure_include_heartbeat == expected.activity_failure_include_heartbeat &&
	       actual.supports_schedules == expected.supports_schedules &&
	       actual.encoded_failure_attributes == expected.encoded_failure_attributes &&
	       actual.build_id_based_versioning == expected.build_id_based_versioning &&
	       actual.upsert_memo == expected.upsert_memo &&
	       actual.eager_workflow_start == expected.eager_workflow_start &&
	       actual.sdk_metadata == expected.sdk_metadata &&
	       actual.count_group_by_execution_status == expected.count_group_by_execution_status &&
	       actual.nexus == expected.nexus &&
	       actual.server_scaled_deployments == expected.server_scaled_deployments &&
	       actual.server_scaled_provider_cloud_run == expected.server_scaled_provider_cloud_run
}

system_info_capabilities_property :: proc(t: ^pbt.T) -> pbt.Result {
	expected := temporal_client.System_Capabilities{
		signal_and_query_header = pbt.draw(t, pbt.boolean()),
		internal_error_differentiation = pbt.draw(t, pbt.boolean()),
		activity_failure_include_heartbeat = pbt.draw(t, pbt.boolean()),
		supports_schedules = pbt.draw(t, pbt.boolean()),
		encoded_failure_attributes = pbt.draw(t, pbt.boolean()),
		build_id_based_versioning = pbt.draw(t, pbt.boolean()),
		upsert_memo = pbt.draw(t, pbt.boolean()),
		eager_workflow_start = pbt.draw(t, pbt.boolean()),
		sdk_metadata = pbt.draw(t, pbt.boolean()),
		count_group_by_execution_status = pbt.draw(t, pbt.boolean()),
		nexus = pbt.draw(t, pbt.boolean()),
		server_scaled_deployments = pbt.draw(t, pbt.boolean()),
		server_scaled_provider_cloud_run = pbt.draw(t, pbt.boolean()),
	}
	pbt.cover(t, expected.supports_schedules, 30, "Schedules advertised")
	pbt.cover(t, expected.nexus, 30, "Nexus advertised")
	pbt.cover(t, expected.server_scaled_deployments, 30, "server-scaled deployments advertised")

	capabilities := proto.Temporal_Api_Workflowservice_V1_GetSystemInfoResponse_Capabilities{
		signal_and_query_header = expected.signal_and_query_header,
		internal_error_differentiation = expected.internal_error_differentiation,
		activity_failure_include_heartbeat = expected.activity_failure_include_heartbeat,
		supports_schedules = expected.supports_schedules,
		encoded_failure_attributes = expected.encoded_failure_attributes,
		build_id_based_versioning = expected.build_id_based_versioning,
		upsert_memo = expected.upsert_memo,
		eager_workflow_start = expected.eager_workflow_start,
		sdk_metadata = expected.sdk_metadata,
		count_group_by_execution_status = expected.count_group_by_execution_status,
		nexus = expected.nexus,
		server_scaled_deployments = expected.server_scaled_deployments,
		server_scaled_provider_cloud_run = expected.server_scaled_provider_cloud_run,
	}
	response := proto.Temporal_Api_Workflowservice_V1_GetSystemInfoResponse{
		server_version = "property-server-version",
		capabilities = &capabilities,
	}
	encoded := proto.encode_Temporal_Api_Workflowservice_V1_GetSystemInfoResponse(&response)
	actual, decode_error := temporal_client.decode_system_info(encoded[:])
	delete(encoded)
	defer temporal_client.system_info_free(&actual)
	defer temporal_client.error_free(&decode_error)
	if decode_error.message != nil {
		return pbt.fail("valid generated System Info response was rejected")
	}
	if actual.server_version != "property-server-version" ||
	   !system_capabilities_equal(actual.capabilities, expected) {
		return pbt.fail("generated System Info capabilities changed during round trip")
	}
	return pbt.pass()
}

@(test)
system_info_preserves_advertised_capabilities :: proc(t: ^testing.T) {
	result := pbt.check(
		"System Info preserves advertised server capabilities",
		system_info_capabilities_property,
		{num_tests = 500, seed = 0x5359_5354_494e_464f},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

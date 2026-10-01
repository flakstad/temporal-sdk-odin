package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"

server_feature_rpc_classification_property :: proc(t: ^pbt.T) -> pbt.Result {
	status_code := u32(pbt.draw(t, pbt.int_range(0, 16)))
	is_rpc_error := pbt.draw(t, pbt.boolean())
	feature := temporal_client.Server_Feature.Schedule_Count
	if pbt.draw(t, pbt.boolean()) {
		feature = .Standalone_Nexus
	}
	pbt.cover(t, status_code == temporal_client.GRPC_STATUS_UNIMPLEMENTED, 3, "gRPC UNIMPLEMENTED")
	pbt.cover(t, !is_rpc_error, 30, "local/non-RPC error")

	kind := temporal_client.Error_Kind.Invalid_Argument
	if is_rpc_error {
		kind = .Rpc
	}
	error := temporal_client.Error{
		kind = kind,
		message = transmute([]u8)string("generated probe failure"),
		status_code = status_code,
	}
	observation := temporal_client.server_feature_from_rpc_error(feature, &error)
	expected_unsupported := is_rpc_error && status_code == temporal_client.GRPC_STATUS_UNIMPLEMENTED
	if (observation.support == .Unsupported) != expected_unsupported {
		return pbt.fail("feature probe classified a status other than RPC UNIMPLEMENTED as unsupported")
	}
	if !expected_unsupported && observation.support != .Unknown {
		return pbt.fail("failed feature probe produced a conclusive result without UNIMPLEMENTED")
	}
	if observation.status_code != status_code || observation.evidence != .Safe_Rpc_Probe {
		return pbt.fail("feature probe discarded its status or evidence")
	}
	return pbt.pass()
}

@(test)
server_feature_probe_only_treats_rpc_unimplemented_as_unsupported :: proc(t: ^testing.T) {
	result := pbt.check(
		"Server feature probe only treats RPC UNIMPLEMENTED as unsupported",
		server_feature_rpc_classification_property,
		{num_tests = 500, seed = 0x4645_4154_5552_4553},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

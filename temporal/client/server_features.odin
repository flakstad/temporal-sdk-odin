package client

// Server_Feature names protocol surfaces whose availability matters to the
// Odin SDK. Some have an authoritative GetSystemInfo bit, while newer APIs
// require a focused RPC probe. This is deliberately not inferred from the
// Temporal Server version string.
Server_Feature :: enum {
	Schedules,
	Schedule_Count,
	Nexus,
	Standalone_Nexus,
	Workflow_Pause,
	Standalone_Activity,
}

// Server_Feature_Support is tri-state because an absent GetSystemInfo bit can
// mean either unsupported or disabled, and some APIs have no safe probe.
Server_Feature_Support :: enum {
	Unknown,
	Supported,
	Unsupported,
}

Server_Feature_Evidence :: enum {
	None,
	Get_System_Info,
	Safe_Rpc_Probe,
}

Server_Feature_Observation :: struct {
	feature:         Server_Feature,
	sdk_implemented: bool,
	support:         Server_Feature_Support,
	evidence:        Server_Feature_Evidence,
	// status_code is populated for a failed RPC probe. In particular, gRPC 12
	// means that the connected Service does not implement the RPC.
	status_code: u32,
}

GRPC_STATUS_UNIMPLEMENTED :: u32(12)

server_feature_name :: proc(feature: Server_Feature) -> string {
	switch feature {
	case .Schedules:
		return "schedules"
	case .Schedule_Count:
		return "schedule-count"
	case .Nexus:
		return "nexus"
	case .Standalone_Nexus:
		return "standalone-nexus"
	case .Workflow_Pause:
		return "workflow-pause"
	case .Standalone_Activity:
		return "standalone-activity"
	}
	return "unknown"
}

server_feature_support_name :: proc(support: Server_Feature_Support) -> string {
	switch support {
	case .Unknown:
		return "unknown"
	case .Supported:
		return "supported"
	case .Unsupported:
		return "unsupported"
	}
	return "unknown"
}

server_feature_evidence_name :: proc(evidence: Server_Feature_Evidence) -> string {
	switch evidence {
	case .None:
		return "none"
	case .Get_System_Info:
		return "get-system-info"
	case .Safe_Rpc_Probe:
		return "safe-rpc-probe"
	}
	return "none"
}

// server_feature_sdk_implemented describes the public Odin surface, not
// server support. The complete pinned standalone Activity lifecycle is now
// represented, including metadata, Visibility, operator controls, options,
// and interceptors.
server_feature_sdk_implemented :: proc(feature: Server_Feature) -> bool {
	switch feature {
	case .Schedules, .Schedule_Count, .Nexus, .Standalone_Nexus, .Workflow_Pause, .Standalone_Activity:
		return true
	}
	return false
}

server_feature_from_system_info :: proc(
	feature: Server_Feature,
	capabilities: System_Capabilities,
) -> Server_Feature_Observation {
	result := Server_Feature_Observation{
		feature = feature,
		sdk_implemented = server_feature_sdk_implemented(feature),
	}
	advertised := false
	switch feature {
	case .Schedules:
		advertised = capabilities.supports_schedules
	case .Nexus:
		advertised = capabilities.nexus
	case .Schedule_Count, .Standalone_Nexus, .Workflow_Pause, .Standalone_Activity:
		return result
	}
	result.evidence = .Get_System_Info
	if advertised {
		result.support = .Supported
	}
	// A false bit is intentionally Unknown, not Unsupported: the Service may
	// support the feature while disabling it through configuration.
	return result
}

server_feature_from_rpc_error :: proc(
	feature: Server_Feature,
	error: ^Error,
) -> Server_Feature_Observation {
	result := Server_Feature_Observation{
		feature = feature,
		sdk_implemented = server_feature_sdk_implemented(feature),
		evidence = .Safe_Rpc_Probe,
	}
	if error == nil || error.message == nil {
		result.support = .Supported
		return result
	}
	result.status_code = error.status_code
	if error.kind == .Rpc && error.status_code == GRPC_STATUS_UNIMPLEMENTED {
		result.support = .Unsupported
	}
	return result
}

// probe_server_feature performs only non-mutating probes. Unsupported is a
// successful probe result, not an SDK error. Other RPC failures remain errors
// because authentication, namespace, and transport failures do not establish
// whether the feature is supported.
//
// Optional packages own probes which require their RPC implementations. The
// base package therefore reports those features as Unknown without importing
// an extension solely to probe it.
probe_server_feature :: proc(
	client: ^Client,
	feature: Server_Feature,
) -> (Server_Feature_Observation, Error) {
	base := Server_Feature_Observation{
		feature = feature,
		sdk_implemented = server_feature_sdk_implemented(feature),
	}
	switch feature {
	case .Schedules, .Nexus:
		info, info_error := get_system_info(client)
		if info_error.message != nil {
			return base, info_error
		}
		defer system_info_free(&info)
		return server_feature_from_system_info(feature, info.capabilities), {}
	case .Schedule_Count, .Standalone_Nexus, .Workflow_Pause, .Standalone_Activity:
		return base, {}
	}
	return base, {}
}

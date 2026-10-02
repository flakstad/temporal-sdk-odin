package client

import proto "../core/proto"

// These Nexus state snapshots are part of the ordinary Workflow Describe
// response as well as the optional standalone Nexus Client surface. They stay
// in the base Client package so Workflow inspection does not depend on that
// optional package.
Pending_Nexus_Operation_State :: enum i32 {
	Unspecified = 0,
	Scheduled = 1,
	Backing_Off = 2,
	Started = 3,
	Blocked = 4,
}

Nexus_Operation_Cancellation_State :: enum i32 {
	Unspecified = 0,
	Scheduled = 1,
	Backing_Off = 2,
	Succeeded = 3,
	Failed = 4,
	Timed_Out = 5,
	Blocked = 6,
}

Nexus_Operation_Cancellation_Description :: struct {
	present:                    bool,
	requested_time:             Optional_Time,
	state:                      Nexus_Operation_Cancellation_State,
	attempt:                    i32,
	last_attempt_complete_time: Optional_Time,
	next_attempt_schedule_time: Optional_Time,
	last_failure_message:       string,
	last_failure_proto:         []u8,
	blocked_reason:             string,
	reason:                     string,
}

pending_nexus_operation_state_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_PendingNexusOperationState,
) -> (Pending_Nexus_Operation_State, bool) {
	numeric := i32(value)
	if numeric < i32(Pending_Nexus_Operation_State.Unspecified) ||
	   numeric > i32(Pending_Nexus_Operation_State.Blocked) {
		return {}, false
	}
	return Pending_Nexus_Operation_State(numeric), true
}

nexus_operation_cancellation_state_from_proto :: proc(
	value: proto.Temporal_Api_Enums_V1_NexusOperationCancellationState,
) -> (Nexus_Operation_Cancellation_State, bool) {
	numeric := i32(value)
	if numeric < i32(Nexus_Operation_Cancellation_State.Unspecified) ||
	   numeric > i32(Nexus_Operation_Cancellation_State.Blocked) {
		return {}, false
	}
	return Nexus_Operation_Cancellation_State(numeric), true
}

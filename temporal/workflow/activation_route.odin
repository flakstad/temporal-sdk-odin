package workflow

import "core:mem/virtual"
import proto "../core/proto"

// Activation_Route is the small borrowed view needed to route a Core
// WorkflowActivation before the full activation is processed. String fields
// borrow from data passed to decode_activation_route and must not outlive it.
Activation_Route :: struct {
	run_id:           string,
	workflow_type:    string,
	initializes:      bool,
	evicts:           bool,
	eviction_reason:  proto.Coresdk_Workflow_Activation_RemoveFromCache_EvictionReason,
	eviction_message: string,
	is_replaying:     bool,
	history_length:   u32,
	job_count:        int,
}

decode_activation_route :: proc(data: []u8) -> (route: Activation_Route, ok: bool) {
	decode_arena: virtual.Arena
	if virtual.arena_init_growing(&decode_arena) != nil {
		return
	}
	defer virtual.arena_destroy(&decode_arena)
	previous_allocator := context.allocator
	context.allocator = virtual.arena_allocator(&decode_arena)
	activation := proto.Coresdk_Workflow_Activation_WorkflowActivation{}
	ok = proto.decode_Coresdk_Workflow_Activation_WorkflowActivation(data, &activation)
	context.allocator = previous_allocator
	if !ok || len(activation.run_id) == 0 {
		ok = false
		return
	}
	route.run_id = activation.run_id
	route.is_replaying = activation.is_replaying
	route.history_length = activation.history_length
	route.job_count = len(activation.jobs)
	for job in activation.jobs {
		#partial switch variant in job.variant {
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Initialize_Workflow:
			if variant.value == nil || route.initializes {
				ok = false
				return
			}
			route.initializes = true
			route.workflow_type = variant.value.workflow_type
		case proto.Coresdk_Workflow_Activation_WorkflowActivationJob_Variant_Remove_From_Cache:
			route.evicts = variant.value != nil
			if variant.value != nil {
				route.eviction_reason = variant.value.reason
				route.eviction_message = variant.value.message
			}
		}
	}
	return
}

// activation_shard_index uses a process-independent FNV-1a hash so every
// activation for a run stays on the same executor shard.
activation_shard_index :: proc(run_id: string, shard_count: int) -> (index: int, ok: bool) {
	if len(run_id) == 0 || shard_count <= 0 {
		return
	}
	hash := u64(14695981039346656037)
	for value in transmute([]u8)run_id {
		hash = (hash ~ u64(value)) * u64(1099511628211)
	}
	return int(hash % u64(shard_count)), true
}

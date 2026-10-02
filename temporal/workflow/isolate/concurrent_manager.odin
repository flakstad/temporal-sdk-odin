package workflow_isolate

import "core:sync"
import workflow ".."

Concurrent_Manager_Options :: struct {
	// Every shard owns an independent Manager with this configuration. The
	// manager maximum executor count is enforced globally across every shard.
	// Concurrent calls must use Odin's process-safe heap allocator; custom
	// thread-local/arena contexts cannot own routes that a later executor thread
	// may evict.
	manager:     Manager_Options,
	shard_count: int,
}

Concurrent_Manager_Shard :: struct {
	mutex:   sync.Mutex,
	manager: Manager,
}

Concurrent_Manager :: struct {
	shards:            []Concurrent_Manager_Shard,
	stats_mutex:       sync.Mutex,
	active_routes:     int,
	peak_routes:       int,
	routes_admitting:  int,
	capacity_rejections: int,
	maximum_executors: int,
	active_activations: int,
	peak_activations:   int,
	initialized:       bool,
	shutting_down:     bool,
	shutdown_complete: bool,
}

// concurrent_capacity_allows_start admits reservations as well as already
// materialized routes without overflowing active+admitting.
concurrent_capacity_allows_start :: proc(
	active, admitting, maximum: int,
) -> bool {
	return active >= 0 && admitting >= 0 && maximum > 0 &&
	       active < maximum && admitting < maximum - active
}

// workflow_shard_index uses a process-independent FNV-1a hash. A run therefore
// keeps the same shard for every activation in this process generation,
// independently of which language executor thread handles it.
workflow_shard_index :: proc(run_id: string, shard_count: int) -> (index: int, ok: bool) {
	if len(run_id) == 0 || shard_count <= 0 {
		return
	}
	hash := u64(14695981039346656037)
	for value in transmute([]u8)run_id {
		hash = (hash ~ u64(value)) * u64(1099511628211)
	}
	return int(hash % u64(shard_count)), true
}

concurrent_manager_init :: proc(
	manager: ^Concurrent_Manager,
	options: Concurrent_Manager_Options,
) -> Manager_Error {
	if manager == nil || manager.initialized {
		return manager_error("concurrent Workflow executor manager is already initialized or missing")
	}
	if options.shard_count <= 0 {
		return manager_error("concurrent Workflow executor manager shard count must be positive")
	}
	if options.manager.maximum_executors <= 0 {
		return manager_error("concurrent Workflow executor capacity must be positive")
	}
	manager.shards = make([]Concurrent_Manager_Shard, options.shard_count)
	for shard_index in 0 ..< options.shard_count {
		init_error := manager_init(&manager.shards[shard_index].manager, options.manager)
		if init_error.message != nil {
			for initialized_index in 0 ..< shard_index {
				manager_close(&manager.shards[initialized_index].manager)
			}
			delete(manager.shards)
			manager.shards = nil
			return init_error
		}
	}
	manager.maximum_executors = options.manager.maximum_executors
	manager.initialized = true
	return {}
}

concurrent_manager_verify_host :: proc(manager: ^Concurrent_Manager) -> Manager_Error {
	if manager == nil || !manager.initialized || manager.shutting_down {
		return manager_error("concurrent Workflow executor manager is not available")
	}
	// All shards clone the same command and expected manifest. One preflight is
	// sufficient; every real generation still repeats Manager's handshake.
	return manager_verify_host(&manager.shards[0].manager)
}

concurrent_manager_process_activation :: proc(
	user_data: rawptr,
	activation: []u8,
) -> workflow.Process_Result {
	manager := cast(^Concurrent_Manager)user_data
	if manager == nil || !manager.initialized || manager.shutting_down {
		return failure_result(
			"unknown",
			"concurrent Workflow executor manager is not available",
		)
	}
	route, route_ok := decode_activation_route(activation)
	if !route_ok {
		return failure_result(
			"unknown",
			"concurrent Workflow executor manager received a malformed activation",
		)
	}
	shard_index, shard_ok := workflow_shard_index(route.run_id, len(manager.shards))
	if !shard_ok {
		return failure_result(
			route.run_id,
			"concurrent Workflow executor manager could not route activation",
		)
	}
	shard := &manager.shards[shard_index]
	sync.mutex_lock(&manager.stats_mutex)
	manager.active_activations += 1
	manager.peak_activations = max(
		manager.peak_activations,
		manager.active_activations,
	)
	sync.mutex_unlock(&manager.stats_mutex)
	sync.mutex_lock(&shard.mutex)
	before_routes := len(shard.manager.routes)
	_, route_found := find_route(&shard.manager, route.run_id)
	reserved_admission := route.initializes && !route_found
	if reserved_admission {
		sync.mutex_lock(&manager.stats_mutex)
		if !concurrent_capacity_allows_start(
			manager.active_routes,
			manager.routes_admitting,
			manager.maximum_executors,
		) {
			manager.capacity_rejections += 1
			manager.active_activations -= 1
			sync.mutex_unlock(&manager.stats_mutex)
			sync.mutex_unlock(&shard.mutex)
			return failure_result(
				route.run_id,
				route_start_failure_message(.Capacity),
			)
		}
		manager.routes_admitting += 1
		sync.mutex_unlock(&manager.stats_mutex)
	}
	result := manager_process_activation(&shard.manager, activation)
	after_routes := len(shard.manager.routes)
	sync.mutex_lock(&manager.stats_mutex)
	if reserved_admission {
		manager.routes_admitting -= 1
	}
	if before_routes != after_routes {
		manager.active_routes += after_routes - before_routes
		manager.peak_routes = max(manager.peak_routes, manager.active_routes)
	}
	manager.active_activations -= 1
	sync.mutex_unlock(&manager.stats_mutex)
	sync.mutex_unlock(&shard.mutex)
	return result
}

concurrent_manager_shutdown :: proc(manager: ^Concurrent_Manager) -> Manager_Error {
	if manager == nil || !manager.initialized {
		return manager_error("concurrent Workflow executor manager is not initialized")
	}
	if manager.shutdown_complete {
		return {}
	}
	manager.shutting_down = true
	had_error := false
	// Keep manager-owned allocation and release on the calling owner thread.
	// Each Manager still drains all of its child sessions concurrently. Moving
	// whole-shard teardown to arbitrary threads requires explicit allocator
	// provenance on every Manager-owned value first.
	for &shard in manager.shards {
		sync.mutex_lock(&shard.mutex)
		shutdown_error := manager_shutdown(&shard.manager)
		sync.mutex_unlock(&shard.mutex)
		if shutdown_error.message != nil {
			had_error = true
			manager_error_free(&shutdown_error)
		}
	}
	sync.mutex_lock(&manager.stats_mutex)
	manager.active_routes = 0
	manager.routes_admitting = 0
	sync.mutex_unlock(&manager.stats_mutex)
	manager.shutdown_complete = true
	if had_error {
		return manager_error(
			"one or more concurrent Workflow executor shards did not shut down cleanly",
		)
	}
	return {}
}

manager_stats_accumulate :: proc(total: ^Manager_Stats, value: Manager_Stats) {
	total.generations_started += value.generations_started
	total.child_exits += value.child_exits
	if value.child_exits > 0 {
		total.last_child_exit_code = value.last_child_exit_code
	}
	total.protocol_failures += value.protocol_failures
	total.activation_timeouts += value.activation_timeouts
	total.capacity_rejections += value.capacity_rejections
	total.evictions += value.evictions
	total.nondeterminism_evictions += value.nondeterminism_evictions
	total.language_failure_evictions += value.language_failure_evictions
	total.workflow_ending_evictions += value.workflow_ending_evictions
	total.graceful_shutdowns += value.graceful_shutdowns
	total.aborted_children += value.aborted_children
	total.manifest_handshakes += value.manifest_handshakes
	total.manifest_mismatches += value.manifest_mismatches
	total.shutdown_timeouts += value.shutdown_timeouts
	total.start_failures += value.start_failures
	total.crash_loop_rejections += value.crash_loop_rejections
	total.crash_loop_recoveries += value.crash_loop_recoveries
	total.incremental_activations += value.incremental_activations
	total.diagnostics_received += value.diagnostics_received
	total.diagnostic_budget_rejections += value.diagnostic_budget_rejections
}

concurrent_manager_stats :: proc(manager: ^Concurrent_Manager) -> Manager_Stats {
	if manager == nil || !manager.initialized {
		return {}
	}
	total := Manager_Stats{}
	for &shard in manager.shards {
		sync.mutex_lock(&shard.mutex)
		manager_stats_accumulate(&total, shard.manager.stats)
		sync.mutex_unlock(&shard.mutex)
	}
	sync.mutex_lock(&manager.stats_mutex)
	total.peak_routes = manager.peak_routes
	total.capacity_rejections += manager.capacity_rejections
	sync.mutex_unlock(&manager.stats_mutex)
	return total
}

concurrent_manager_route_count :: proc(manager: ^Concurrent_Manager) -> int {
	if manager == nil || !manager.initialized {
		return 0
	}
	sync.mutex_lock(&manager.stats_mutex)
	total := manager.active_routes
	sync.mutex_unlock(&manager.stats_mutex)
	return total
}

concurrent_manager_peak_activations :: proc(manager: ^Concurrent_Manager) -> int {
	if manager == nil || !manager.initialized {
		return 0
	}
	sync.mutex_lock(&manager.stats_mutex)
	peak := manager.peak_activations
	sync.mutex_unlock(&manager.stats_mutex)
	return peak
}

concurrent_manager_close :: proc(manager: ^Concurrent_Manager) {
	if manager == nil {
		return
	}
	if manager.initialized && !manager.shutdown_complete {
		shutdown_error := concurrent_manager_shutdown(manager)
		manager_error_free(&shutdown_error)
	}
	for &shard in manager.shards {
		manager_close(&shard.manager)
	}
	delete(manager.shards)
	manager^ = {}
}

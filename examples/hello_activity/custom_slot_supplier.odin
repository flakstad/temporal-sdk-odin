package main

import "core:sync"
import worker "../../temporal/worker"

Custom_Activity_Slot_State :: struct {
	mutex:                    sync.Mutex,
	capacity:                 uintptr,
	in_use:                   uintptr,
	next_permit:              uintptr,
	pending:                  [dynamic]worker.Slot_Reservation,
	reserve_calls:            int,
	cancel_calls:             int,
	try_calls:                int,
	mark_used_calls:          int,
	release_calls:            int,
	completed_reservations:   int,
	completed_cancellations:  int,
	pending_peak:             int,
	peak_in_use:              uintptr,
	saw_activity_reservation: bool,
	saw_hello_activity:       bool,
	freed:                    bool,
	protocol_error:           bool,
}

custom_activity_slot_state_init :: proc(
	state: ^Custom_Activity_Slot_State,
	capacity: uintptr,
) {
	state^ = {
		capacity = capacity,
		next_permit = 1,
		pending = make([dynamic]worker.Slot_Reservation),
	}
}

custom_activity_slot_state_destroy :: proc(state: ^Custom_Activity_Slot_State) {
	if state == nil {
		return
	}
	delete(state.pending)
	state^ = {}
}

custom_activity_slot_take_permit :: proc(
	state: ^Custom_Activity_Slot_State,
) -> uintptr {
	permit := state.next_permit
	state.next_permit += 1
	if state.next_permit == 0 {
		state.next_permit = 1
	}
	state.in_use += 1
	state.peak_in_use = max(state.peak_in_use, state.in_use)
	return permit
}

custom_activity_slot_complete_cancel :: proc(
	state: ^Custom_Activity_Slot_State,
	reservation: worker.Slot_Reservation,
) {
	if worker.slot_reservation_cancel_complete(reservation) {
		sync.mutex_lock(&state.mutex)
		state.completed_cancellations += 1
		sync.mutex_unlock(&state.mutex)
	} else {
		sync.mutex_lock(&state.mutex)
		state.protocol_error = true
		sync.mutex_unlock(&state.mutex)
	}
}

// Transfer one released permit to the oldest pending Core reservation. A
// cancellation may win after we remove it from the queue; Core reports that
// race by returning false, after which we complete cancellation and try the
// next waiter without consuming capacity.
custom_activity_slot_release_capacity :: proc(state: ^Custom_Activity_Slot_State) {
	for {
		reservation: worker.Slot_Reservation
		permit: uintptr
		sync.mutex_lock(&state.mutex)
		if len(state.pending) == 0 {
			if state.in_use == 0 {
				state.protocol_error = true
			} else {
				state.in_use -= 1
			}
			sync.mutex_unlock(&state.mutex)
			return
		}
		reservation = state.pending[0]
		ordered_remove(&state.pending, 0)
		permit = state.next_permit
		state.next_permit += 1
		if state.next_permit == 0 {
			state.next_permit = 1
		}
		sync.mutex_unlock(&state.mutex)

		if worker.slot_reservation_complete(reservation, permit) {
			sync.mutex_lock(&state.mutex)
			state.completed_reservations += 1
			sync.mutex_unlock(&state.mutex)
			return
		}
		custom_activity_slot_complete_cancel(state, reservation)
	}
}

custom_activity_slot_reserve :: proc(
	user_data: rawptr,
	ctx: ^worker.Slot_Reserve_Context,
	reservation: worker.Slot_Reservation,
) {
	state := cast(^Custom_Activity_Slot_State)user_data
	permit: uintptr
	sync.mutex_lock(&state.mutex)
	state.reserve_calls += 1
	state.saw_activity_reservation = state.saw_activity_reservation ||
		(ctx != nil && ctx.slot_kind == .Activity && len(ctx.task_queue) > 0)
	if state.in_use < state.capacity {
		permit = custom_activity_slot_take_permit(state)
	} else {
		append(&state.pending, reservation)
		state.pending_peak = max(state.pending_peak, len(state.pending))
	}
	sync.mutex_unlock(&state.mutex)

	if permit == 0 {
		return
	}
	completed := worker.slot_reservation_complete(reservation, permit)
	if completed {
		sync.mutex_lock(&state.mutex)
		state.completed_reservations += 1
		sync.mutex_unlock(&state.mutex)
		return
	}
	custom_activity_slot_complete_cancel(state, reservation)
	custom_activity_slot_release_capacity(state)
}

custom_activity_slot_cancel :: proc(
	user_data: rawptr,
	reservation: worker.Slot_Reservation,
) {
	state := cast(^Custom_Activity_Slot_State)user_data
	found := false
	sync.mutex_lock(&state.mutex)
	state.cancel_calls += 1
	for pending, index in state.pending {
		if pending.handle == reservation.handle {
			ordered_remove(&state.pending, index)
			found = true
			break
		}
	}
	sync.mutex_unlock(&state.mutex)
	if found {
		custom_activity_slot_complete_cancel(state, reservation)
	}
}

custom_activity_slot_try_reserve :: proc(
	user_data: rawptr,
	ctx: ^worker.Slot_Reserve_Context,
) -> uintptr {
	state := cast(^Custom_Activity_Slot_State)user_data
	sync.mutex_lock(&state.mutex)
	defer sync.mutex_unlock(&state.mutex)
	state.try_calls += 1
	state.saw_activity_reservation = state.saw_activity_reservation ||
		(ctx != nil && ctx.slot_kind == .Activity && len(ctx.task_queue) > 0)
	if state.in_use >= state.capacity {
		return 0
	}
	return custom_activity_slot_take_permit(state)
}

custom_activity_slot_mark_used :: proc(
	user_data: rawptr,
	info: ^worker.Slot_Info,
	permit: uintptr,
) {
	state := cast(^Custom_Activity_Slot_State)user_data
	sync.mutex_lock(&state.mutex)
	defer sync.mutex_unlock(&state.mutex)
	state.mark_used_calls += 1
	if permit == 0 || info == nil || info.slot_kind != .Activity {
		state.protocol_error = true
		return
	}
	state.saw_hello_activity = state.saw_hello_activity || info.activity_type == "hello"
}

custom_activity_slot_release :: proc(
	user_data: rawptr,
	_: ^worker.Slot_Info,
	permit: uintptr,
) {
	state := cast(^Custom_Activity_Slot_State)user_data
	sync.mutex_lock(&state.mutex)
	state.release_calls += 1
	if permit == 0 {
		state.protocol_error = true
		sync.mutex_unlock(&state.mutex)
		return
	}
	sync.mutex_unlock(&state.mutex)
	custom_activity_slot_release_capacity(state)
}

custom_activity_slot_available :: proc(user_data: rawptr) -> (uintptr, bool) {
	state := cast(^Custom_Activity_Slot_State)user_data
	sync.mutex_lock(&state.mutex)
	defer sync.mutex_unlock(&state.mutex)
	return state.capacity - state.in_use, true
}

custom_activity_slot_free :: proc(user_data: rawptr) {
	state := cast(^Custom_Activity_Slot_State)user_data
	sync.mutex_lock(&state.mutex)
	state.freed = true
	if state.in_use != 0 || len(state.pending) != 0 {
		state.protocol_error = true
	}
	sync.mutex_unlock(&state.mutex)
}

custom_activity_slot_supplier :: proc(
	state: ^Custom_Activity_Slot_State,
) -> worker.Custom_Slot_Supplier {
	return {
		reserve = custom_activity_slot_reserve,
		cancel_reserve = custom_activity_slot_cancel,
		try_reserve = custom_activity_slot_try_reserve,
		mark_used = custom_activity_slot_mark_used,
		release = custom_activity_slot_release,
		available_slots = custom_activity_slot_available,
		free = custom_activity_slot_free,
		user_data = state,
	}
}

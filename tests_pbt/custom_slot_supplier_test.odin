package workflow_pbt_test

import "core:testing"
import pbt "pbt:pbt"
import temporal_core "../temporal/core"

custom_slot_pbt_reserve :: proc(
	_: rawptr,
	_: ^temporal_core.Slot_Reserve_Context,
	_: temporal_core.Slot_Reservation,
) {}

custom_slot_pbt_cancel :: proc(
	_: rawptr,
	_: temporal_core.Slot_Reservation,
) {}

custom_slot_pbt_try :: proc(
	_: rawptr,
	_: ^temporal_core.Slot_Reserve_Context,
) -> uintptr {
	return 1
}

custom_slot_pbt_mark :: proc(
	_: rawptr,
	_: ^temporal_core.Slot_Info,
	_: uintptr,
) {}

custom_slot_pbt_release :: proc(
	_: rawptr,
	_: ^temporal_core.Slot_Info,
	_: uintptr,
) {}

custom_slot_pbt_available :: proc(_: rawptr) -> (uintptr, bool) {
	return 1, true
}

custom_slot_pbt_free :: proc(_: rawptr) {}

custom_slot_supplier_callback_set_property :: proc(t: ^pbt.T) -> pbt.Result {
	has_reserve := pbt.draw(t, pbt.boolean())
	has_cancel := pbt.draw(t, pbt.boolean())
	has_try := pbt.draw(t, pbt.boolean())
	has_mark := pbt.draw(t, pbt.boolean())
	has_release := pbt.draw(t, pbt.boolean())
	has_available := pbt.draw(t, pbt.boolean())
	has_free := pbt.draw(t, pbt.boolean())
	has_user_data := pbt.draw(t, pbt.boolean())
	resource_tuner := pbt.draw(t, pbt.boolean())

	required_count := 0
	required_callbacks := [?]bool{has_reserve, has_cancel, has_try, has_mark, has_release}
	for present in required_callbacks {
		if present {
			required_count += 1
		}
	}
	enabled := required_count > 0 || has_available || has_free || has_user_data
	complete := required_count == 5
	pbt.cover(t, !enabled, 0.1, "zero-value supplier")
	pbt.cover(t, complete, 2.0, "complete callback set")
	pbt.cover(t, enabled && !complete, 50.0, "partial callback set")
	pbt.cover(t, complete && resource_tuner, 1.0, "custom/resource conflict")

	sentinel: u8
	supplier := temporal_core.Custom_Slot_Supplier{}
	if has_reserve do supplier.reserve = custom_slot_pbt_reserve
	if has_cancel do supplier.cancel_reserve = custom_slot_pbt_cancel
	if has_try do supplier.try_reserve = custom_slot_pbt_try
	if has_mark do supplier.mark_used = custom_slot_pbt_mark
	if has_release do supplier.release = custom_slot_pbt_release
	if has_available do supplier.available_slots = custom_slot_pbt_available
	if has_free do supplier.free = custom_slot_pbt_free
	if has_user_data do supplier.user_data = &sentinel

	callback_error := temporal_core.custom_slot_supplier_validate(supplier)
	defer temporal_core.error_free(&callback_error)
	expected_callback_valid := !enabled || complete
	if (callback_error.message == nil) != expected_callback_valid {
		return pbt.fail("custom supplier callback completeness validation changed")
	}

	options := temporal_core.worker_options_default()
	options.custom_activity_slot_supplier = supplier
	if resource_tuner {
		options.resource_based_activity_tuner = temporal_core.resource_based_activity_tuner(
			0.8,
			0.8,
		)
	}
	options_error := temporal_core.worker_options_validate(options)
	defer temporal_core.error_free(&options_error)
	expected_options_valid := expected_callback_valid && !(complete && resource_tuner)
	if (options_error.message == nil) != expected_options_valid {
		return pbt.fail("custom supplier/resource tuner validation changed")
	}
	return pbt.pass()
}

@(test)
custom_slot_supplier_requires_a_complete_non_conflicting_callback_set :: proc(t: ^testing.T) {
	result := pbt.check(
		"Custom slot supplier callback sets are complete and non-conflicting",
		custom_slot_supplier_callback_set_property,
		{num_tests = 1_000, seed = 0x534c_4f54_5355_5050},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

package workflow_pbt_test

import "core:bytes"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import temporal_core "../temporal/core"
import raw "../temporal/core/raw"

connection_raw_slice :: proc(value: raw.Byte_Array_Ref) -> []u8 {
	if value.data == nil || value.size == 0 {
		return nil
	}
	return ([^]u8)(value.data)[:int(value.size)]
}

client_connection_options_property :: proc(t: ^pbt.T) -> pbt.Result {
	use_api_key := pbt.draw(t, pbt.boolean())
	use_explicit_tls := pbt.draw(t, pbt.boolean())
	initial_millis := pbt.draw(t, pbt.int_range(1, 2_000))
	maximum_millis := pbt.draw(t, pbt.int_range(2_001, 20_000))
	unbounded_elapsed_time := pbt.draw(t, pbt.boolean())
	maximum_elapsed_millis := 0
	if !unbounded_elapsed_time {
		maximum_elapsed_millis = pbt.draw(t, pbt.int_range(1, 60_000))
	}
	maximum_retries := pbt.draw(t, pbt.int_range(0, 100))
	binary_numbers := pbt.draw(t, pbt.array(pbt.int_range(0, 255), 0, 64))
	binary_value := make([]u8, len(binary_numbers))
	defer delete(binary_value)
	for number, index in binary_numbers {
		binary_value[index] = u8(number)
	}
	pbt.cover(t, use_api_key, 30.0, "API key enables TLS")
	pbt.cover(t, use_explicit_tls, 30.0, "explicit TLS")
	pbt.cover(t, len(binary_value) == 0, 1.0, "empty binary metadata")
	pbt.cover(t, unbounded_elapsed_time, 30.0, "unbounded elapsed retry time")

	metadata := [?]temporal_core.Client_Metadata_Entry{{
		key = "x-odin-property",
		value = transmute([]u8)string("property-value"),
	}}
	binary_metadata := [?]temporal_core.Client_Metadata_Entry{{
		key = "x-odin-property-bin",
		value = binary_value,
	}}
	tls := temporal_core.Client_TLS_Options{domain = "cloud.example.test"}
	retry := temporal_core.Client_Retry_Options{
		initial_interval = time.Duration(initial_millis) * time.Millisecond,
		randomization_factor = 0.2,
		multiplier = 2,
		maximum_interval = time.Duration(maximum_millis) * time.Millisecond,
		maximum_elapsed_time = time.Duration(maximum_elapsed_millis) * time.Millisecond,
		maximum_retries = uintptr(maximum_retries),
	}
	options := temporal_core.Client_Connection_Options{
		target_url = "https://namespace.tmprl.cloud:7233",
		client_name = "temporal-sdk-odin",
		client_version = "property-test",
		identity = "property-test-worker",
		metadata = metadata[:],
		binary_metadata = binary_metadata[:],
		retry = &retry,
	}
	if use_api_key {
		options.api_key = "property-api-key"
	}
	if use_explicit_tls {
		options.tls = &tls
	}
	bindings := temporal_core.Client_Connection_Bindings{}
	bindings_error := temporal_core.client_connection_bindings_init(&bindings, options)
	defer temporal_core.client_connection_bindings_free(&bindings)
	defer temporal_core.error_free(&bindings_error)
	if bindings_error.message != nil {
		return pbt.fail("valid generated Client connection options were rejected")
	}
	if (bindings.raw_options.tls_options != nil) != (use_api_key || use_explicit_tls) {
		return pbt.fail("API-key/explicit TLS enablement changed during binding")
	}
	if bindings.raw_options.retry_options.initial_interval_millis != u64(initial_millis) ||
	   bindings.raw_options.retry_options.max_interval_millis != u64(maximum_millis) ||
	   bindings.raw_options.retry_options.max_elapsed_time_millis != u64(maximum_elapsed_millis) ||
	   bindings.raw_options.retry_options.max_retries != uintptr(maximum_retries) {
		return pbt.fail("generated Client Retry Options changed during binding")
	}
	metadata_refs := ([^]raw.Byte_Array_Ref)(bindings.raw_options.metadata.data)[:int(bindings.raw_options.metadata.size)]
	if len(metadata_refs) != 1 || string(connection_raw_slice(metadata_refs[0])) != "x-odin-property\nproperty-value" {
		return pbt.fail("generated ASCII metadata changed during binding")
	}
	binary_refs := ([^]raw.Byte_Array_Ref)(bindings.raw_options.binary_metadata.data)[:int(bindings.raw_options.binary_metadata.size)]
	if len(binary_refs) != 1 {
		return pbt.fail("generated binary metadata was omitted")
	}
	encoded_binary := connection_raw_slice(binary_refs[0])
	prefix := transmute([]u8)string("x-odin-property-bin\n")
	if len(encoded_binary) != len(prefix) + len(binary_value) ||
	   !bytes.equal(encoded_binary[:len(prefix)], prefix) ||
	   !bytes.equal(encoded_binary[len(prefix):], binary_value) {
		return pbt.fail("generated binary metadata changed during binding")
	}
	return pbt.pass()
}

@(test)
client_connection_options_preserve_generated_transport_values :: proc(t: ^testing.T) {
	result := pbt.check(
		"Client connection options preserve transport values",
		client_connection_options_property,
		{num_tests = 500, seed = 0x434f_4e4e_4543_544f},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

worker_shutdown_duration_property :: proc(t: ^pbt.T) -> pbt.Result {
	sub_millisecond := pbt.draw(t, pbt.boolean())
	whole_milliseconds := 0
	if !sub_millisecond {
		whole_milliseconds = pbt.draw(t, pbt.int_range(1, 1_000_000))
	}
	whole_millisecond := pbt.draw(t, pbt.boolean())
	fractional_nanoseconds := 0
	if !whole_millisecond {
		fractional_nanoseconds = pbt.draw(t, pbt.int_range(1, 999_999))
	}
	negative := pbt.draw(t, pbt.boolean())
	pbt.cover(t, sub_millisecond, 30.0, "sub-millisecond grace period")
	pbt.cover(t, whole_millisecond, 30.0, "whole-millisecond grace period")
	pbt.cover(t, fractional_nanoseconds > 0, 30.0, "fractional millisecond rounds upward")
	pbt.cover(t, negative, 40.0, "negative grace period rejected")

	duration := time.Duration(whole_milliseconds) * time.Millisecond +
		time.Duration(fractional_nanoseconds) * time.Nanosecond
	if negative {
		duration = -(duration + time.Nanosecond)
	}
	actual, conversion_error := temporal_core.worker_graceful_shutdown_period_millis(duration)
	defer temporal_core.error_free(&conversion_error)
	if negative {
		if conversion_error.message == nil {
			return pbt.fail("negative generated Worker shutdown duration was accepted")
		}
		return pbt.pass()
	}
	if conversion_error.message != nil {
		return pbt.fail("valid generated Worker shutdown duration was rejected")
	}
	expected := u64(whole_milliseconds)
	if fractional_nanoseconds > 0 {
		expected += 1
	}
	if actual != expected {
		return pbt.fail("generated Worker shutdown duration changed at the C ABI boundary")
	}
	return pbt.pass()
}

@(test)
worker_shutdown_durations_preserve_generated_c_abi_contract :: proc(t: ^testing.T) {
	result := pbt.check(
		"Worker shutdown durations preserve the C ABI contract",
		worker_shutdown_duration_property,
		{num_tests = 500, seed = 0x5348_5554_444f_574e},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

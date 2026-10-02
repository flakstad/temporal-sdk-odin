package workflow_pbt_test

import "core:bytes"
import "core:testing"
import "core:time"
import pbt "pbt:pbt"
import temporal_client "../temporal/client"
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

client_configuration_profile_property :: proc(t: ^pbt.T) -> pbt.Result {
	select_beta := pbt.draw(t, pbt.boolean())
	override_address := pbt.draw(t, pbt.boolean())
	override_namespace := pbt.draw(t, pbt.boolean())
	override_api_key := pbt.draw(t, pbt.boolean())
	tls_enabled := pbt.draw(t, pbt.boolean())
	pbt.cover(t, select_beta, 30.0, "non-default profile")
	pbt.cover(t, override_address, 30.0, "environment address override")
	pbt.cover(t, override_namespace, 30.0, "environment namespace override")
	pbt.cover(t, override_api_key, 30.0, "environment API-key override")
	pbt.cover(t, !tls_enabled, 30.0, "environment disables TLS")

	toml_source := string(`
[profile.alpha]
address = "alpha.example.test:7233"
namespace = "alpha-namespace"
api_key = "alpha-key"

[profile.beta]
address = "beta.example.test:7233"
namespace = "beta-namespace"
api_key = "beta-key"
`)
	profile_name := "alpha"
	expected_address := "alpha.example.test:7233"
	expected_namespace := "alpha-namespace"
	expected_api_key := "alpha-key"
	if select_beta {
		profile_name = "beta"
		expected_address = "beta.example.test:7233"
		expected_namespace = "beta-namespace"
		expected_api_key = "beta-key"
	}
	environment_storage: [4]temporal_client.Config_Environment_Entry
	environment_count := 0
	if override_address {
		environment_storage[environment_count] = {
			key = "TEMPORAL_ADDRESS",
			value = "environment.example.test:8233",
		}
		environment_count += 1
		expected_address = "environment.example.test:8233"
	}
	if override_namespace {
		environment_storage[environment_count] = {
			key = "TEMPORAL_NAMESPACE",
			value = "environment-namespace",
		}
		environment_count += 1
		expected_namespace = "environment-namespace"
	}
	if override_api_key {
		environment_storage[environment_count] = {
			key = "TEMPORAL_API_KEY",
			value = "environment-key",
		}
		environment_count += 1
		expected_api_key = "environment-key"
	}
	environment_storage[environment_count] = {
		key = "TEMPORAL_TLS",
		value = tls_enabled ? "true" : "false",
	}
	environment_count += 1

	owned_source := bytes.clone(transmute([]u8)toml_source)
	profile, profile_error := temporal_client.load_config_profile({
		profile = profile_name,
		source = {data = owned_source},
		config_file_strict = true,
		use_environment_override = true,
		environment = environment_storage[:environment_count],
	})
	delete(owned_source)
	defer temporal_client.config_profile_free(&profile)
	defer temporal_client.error_free(&profile_error)
	if profile_error.message != nil {
		return pbt.fail("Core rejected a generated valid configuration profile")
	}
	if profile.address != expected_address ||
	   profile.namespace != expected_namespace ||
	   profile.api_key != expected_api_key {
		return pbt.fail("file/profile/environment precedence changed")
	}
	if !profile.tls.present || profile.tls.disabled == tls_enabled {
		return pbt.fail("generated TEMPORAL_TLS override changed")
	}
	return pbt.pass()
}

@(test)
client_configuration_profiles_preserve_core_resolution_precedence :: proc(t: ^testing.T) {
	result := pbt.check(
		"Client configuration profiles preserve Core resolution precedence",
		client_configuration_profile_property,
		{num_tests = 250, seed = 0x434f_4e46_4947_5552},
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

generated_poller_behavior :: proc(t: ^pbt.T, mode: int, cached: bool) -> temporal_core.Poller_Behavior {
	switch mode {
	case 0:
		return {}
	case 1:
		minimum := 1
		if cached {
			minimum = 2
		}
		return temporal_core.poller_simple_maximum(pbt.draw(t, pbt.int_range(minimum, 32)))
	case 2:
		minimum := pbt.draw(t, pbt.int_range(1, 8))
		maximum := pbt.draw(t, pbt.int_range(minimum, 32))
		initial := pbt.draw(t, pbt.int_range(minimum, maximum))
		return temporal_core.poller_autoscaling(minimum, maximum, initial)
	}
	return {}
}

worker_options_binding_property :: proc(t: ^pbt.T) -> pbt.Result {
	cache_enabled := pbt.draw(t, pbt.boolean())
	explicit_workflow_slots := pbt.draw(t, pbt.boolean())
	workflow_poller_mode := pbt.draw(t, pbt.int_range(0, 2))
	activity_poller_mode := pbt.draw(t, pbt.int_range(0, 2))
	nexus_poller_mode := pbt.draw(t, pbt.int_range(0, 2))
	slots := pbt.draw(t, pbt.int_range(1, 64))
	nexus_slots := pbt.draw(t, pbt.int_range(1, 64))
	resource_tuned := pbt.draw(t, pbt.boolean())
	nexus_resource_tuned := pbt.draw(t, pbt.boolean())
	activity_minimum := pbt.draw(t, pbt.int_range(1, 8))
	activity_maximum := pbt.draw(t, pbt.int_range(activity_minimum, 64))
	local_minimum := pbt.draw(t, pbt.int_range(1, 8))
	local_maximum := pbt.draw(t, pbt.int_range(local_minimum, 64))
	activity_ramp_millis := pbt.draw(t, pbt.int_range(0, 1_000))
	activity_ramp_fraction := pbt.draw(t, pbt.int_range(0, 999_999))
	local_ramp_millis := pbt.draw(t, pbt.int_range(0, 1_000))
	local_ramp_fraction := pbt.draw(t, pbt.int_range(0, 999_999))
	nexus_minimum := pbt.draw(t, pbt.int_range(1, 8))
	nexus_maximum := pbt.draw(t, pbt.int_range(nexus_minimum, 64))
	nexus_ramp_millis := pbt.draw(t, pbt.int_range(0, 1_000))
	nexus_ramp_fraction := pbt.draw(t, pbt.int_range(0, 999_999))
	target_memory_hundredths := pbt.draw(t, pbt.int_range(1, 100))
	target_cpu_hundredths := pbt.draw(t, pbt.int_range(1, 100))
	sticky_millis := pbt.draw(t, pbt.int_range(0, 60_000))
	sticky_fraction := pbt.draw(t, pbt.int_range(0, 999_999))
	worker_rate_unset := pbt.draw(t, pbt.boolean())
	worker_rate_tenths := 0
	if !worker_rate_unset {
		worker_rate_tenths = pbt.draw(t, pbt.int_range(1, 1_000))
	}
	task_queue_rate_tenths := pbt.draw(t, pbt.int_range(0, 1_000))
	ratio_hundredths := pbt.draw(t, pbt.int_range(0, 100))
	pbt.cover(t, cache_enabled, 30.0, "Workflow cache enabled")
	pbt.cover(t, explicit_workflow_slots, 30.0, "explicit Workflow slots")
	pbt.cover(t, workflow_poller_mode == 0, 20.0, "Core-default Workflow poller")
	pbt.cover(t, workflow_poller_mode == 1, 20.0, "simple Workflow poller")
	pbt.cover(t, workflow_poller_mode == 2, 20.0, "autoscaling Workflow poller")
	pbt.cover(t, activity_poller_mode == 2, 20.0, "autoscaling Activity poller")
	pbt.cover(t, nexus_poller_mode == 1, 20.0, "simple Nexus poller")
	pbt.cover(t, nexus_poller_mode == 2, 20.0, "autoscaling Nexus poller")
	pbt.cover(t, sticky_fraction > 0, 40.0, "fractional duration rounds upward")
	pbt.cover(t, worker_rate_unset, 30.0, "unset worker rate")
	pbt.cover(t, resource_tuned, 30.0, "resource-based Activity suppliers")
	pbt.cover(t, nexus_resource_tuned, 30.0, "resource-based Nexus supplier")
	pbt.cover(
		t,
		resource_tuned && nexus_resource_tuned,
		15.0,
		"shared Activity and Nexus resource controller",
	)

	options := temporal_core.worker_options_default()
	options.identity_override = "generated-worker"
	options.max_activity_slots = slots
	options.max_nexus_task_slots = nexus_slots
	if nexus_resource_tuned {
		// Prove the fixed capacity is not required when the resource supplier is enabled.
		options.max_nexus_task_slots = 0
	}
	if cache_enabled {
		options.max_cached_workflows = u32(pbt.draw(t, pbt.int_range(1, 500)))
	}
	if explicit_workflow_slots {
		minimum_workflow_slots := cache_enabled ? 2 : 1
		options.max_workflow_task_slots = pbt.draw(
			t,
			pbt.int_range(minimum_workflow_slots, 64),
		)
	}
	options.sticky_queue_schedule_to_start_timeout =
		time.Duration(sticky_millis) * time.Millisecond +
		time.Duration(sticky_fraction) * time.Nanosecond
	options.max_worker_activities_per_second = f64(worker_rate_tenths) / 10
	options.max_task_queue_activities_per_second = f64(task_queue_rate_tenths) / 10
	options.nonsticky_to_sticky_poll_ratio = f32(ratio_hundredths) / 100
	options.workflow_task_poller_behavior = generated_poller_behavior(
		t,
		workflow_poller_mode,
		cache_enabled,
	)
	options.activity_task_poller_behavior = generated_poller_behavior(
		t,
		activity_poller_mode,
		false,
	)
	options.nexus_task_poller_behavior = generated_poller_behavior(
		t,
		nexus_poller_mode,
		false,
	)
	if resource_tuned {
		options.resource_based_activity_tuner =
			temporal_core.resource_based_activity_tuner_with_options(
				f64(target_memory_hundredths) / 100,
				f64(target_cpu_hundredths) / 100,
				{
					minimum_slots = activity_minimum,
					maximum_slots = activity_maximum,
					ramp_throttle = time.Duration(activity_ramp_millis) * time.Millisecond +
						time.Duration(activity_ramp_fraction) * time.Nanosecond,
				},
				{
					minimum_slots = local_minimum,
					maximum_slots = local_maximum,
					ramp_throttle = time.Duration(local_ramp_millis) * time.Millisecond +
						time.Duration(local_ramp_fraction) * time.Nanosecond,
				},
			)
	}
	if nexus_resource_tuned {
		options.resource_based_nexus_tuner =
			temporal_core.resource_based_nexus_tuner_with_options(
				f64(target_memory_hundredths) / 100,
				f64(target_cpu_hundredths) / 100,
				{
					minimum_slots = nexus_minimum,
					maximum_slots = nexus_maximum,
					ramp_throttle = time.Duration(nexus_ramp_millis) * time.Millisecond +
						time.Duration(nexus_ramp_fraction) * time.Nanosecond,
				},
			)
	}

	bindings := temporal_core.Worker_Option_Bindings{}
	bindings_error := temporal_core.worker_option_bindings_init(&bindings, options)
	defer temporal_core.error_free(&bindings_error)
	if bindings_error.message != nil {
		return pbt.fail("valid generated Worker options were rejected")
	}
	raw_options := &bindings.raw_options
	if resource_tuned {
		activity_supplier := &raw_options.tuner.activity_slot_supplier
		local_supplier := &raw_options.tuner.local_activity_slot_supplier
		if activity_supplier.tag != .Resource_Based || local_supplier.tag != .Resource_Based {
			return pbt.fail("resource-based generated suppliers used the wrong C union arm")
		}
		expected_activity_ramp := u64(activity_ramp_millis)
		if activity_ramp_fraction > 0 {
			expected_activity_ramp += 1
		}
		expected_local_ramp := u64(local_ramp_millis)
		if local_ramp_fraction > 0 {
			expected_local_ramp += 1
		}
		if activity_supplier.resource_based.minimum_slots != uintptr(activity_minimum) ||
		   activity_supplier.resource_based.maximum_slots != uintptr(activity_maximum) ||
		   activity_supplier.resource_based.ramp_throttle_ms != expected_activity_ramp ||
		   local_supplier.resource_based.minimum_slots != uintptr(local_minimum) ||
		   local_supplier.resource_based.maximum_slots != uintptr(local_maximum) ||
		   local_supplier.resource_based.ramp_throttle_ms != expected_local_ramp {
			return pbt.fail("resource-based generated slot configuration changed during binding")
		}
		target_memory := f64(target_memory_hundredths) / 100
		target_cpu := f64(target_cpu_hundredths) / 100
		if activity_supplier.resource_based.tuner_options.target_memory_usage != target_memory ||
		   activity_supplier.resource_based.tuner_options.target_cpu_usage != target_cpu ||
		   local_supplier.resource_based.tuner_options.target_memory_usage != target_memory ||
		   local_supplier.resource_based.tuner_options.target_cpu_usage != target_cpu {
			return pbt.fail("resource-based generated targets changed during binding")
		}
	} else if raw_options.tuner.activity_slot_supplier.tag != .Fixed_Size ||
	          raw_options.tuner.activity_slot_supplier.fixed_size.num_slots != uintptr(slots) {
		return pbt.fail("generated fixed Activity slot count changed during binding")
	}
	expected_workflow_slots := uintptr(options.max_workflow_task_slots)
	if expected_workflow_slots == 0 {
		expected_workflow_slots = cache_enabled ? 2 : 1
	}
	if raw_options.tuner.workflow_slot_supplier.fixed_size.num_slots != expected_workflow_slots {
		return pbt.fail("Workflow slot admission changed during binding")
	}
	nexus_supplier := &raw_options.tuner.nexus_task_slot_supplier
	if nexus_resource_tuned {
		if nexus_supplier.tag != .Resource_Based {
			return pbt.fail("resource-based Nexus supplier used the wrong C union arm")
		}
		expected_nexus_ramp := u64(nexus_ramp_millis)
		if nexus_ramp_fraction > 0 {
			expected_nexus_ramp += 1
		}
		if nexus_supplier.resource_based.minimum_slots != uintptr(nexus_minimum) ||
		   nexus_supplier.resource_based.maximum_slots != uintptr(nexus_maximum) ||
		   nexus_supplier.resource_based.ramp_throttle_ms != expected_nexus_ramp ||
		   nexus_supplier.resource_based.tuner_options.target_memory_usage !=
		     f64(target_memory_hundredths) / 100 ||
		   nexus_supplier.resource_based.tuner_options.target_cpu_usage !=
		     f64(target_cpu_hundredths) / 100 {
			return pbt.fail("resource-based generated Nexus configuration changed during binding")
		}
	} else if nexus_supplier.tag != .Fixed_Size ||
	          nexus_supplier.fixed_size.num_slots != uintptr(nexus_slots) {
		return pbt.fail("generated fixed Nexus slot count changed during binding")
	}
	expected_sticky_millis := u64(sticky_millis)
	if sticky_fraction > 0 {
		expected_sticky_millis += 1
	}
	if raw_options.sticky_queue_schedule_to_start_timeout_ms != expected_sticky_millis {
		return pbt.fail("generated sticky timeout changed at the C ABI boundary")
	}
	if raw_options.max_activities_per_second != options.max_worker_activities_per_second ||
	   raw_options.max_task_queue_activities_per_second !=
	     options.max_task_queue_activities_per_second ||
	   raw_options.nonsticky_to_sticky_poll_ratio != options.nonsticky_to_sticky_poll_ratio {
		return pbt.fail("generated Worker rates changed during binding")
	}
	switch workflow_poller_mode {
	case 0:
		if raw_options.workflow_task_poller_behavior.simple_maximum != nil ||
		   raw_options.workflow_task_poller_behavior.autoscaling != nil {
			return pbt.fail("default generated Workflow poller was not encoded as null pointers")
		}
	case 1:
		if raw_options.workflow_task_poller_behavior.simple_maximum == nil ||
		   raw_options.workflow_task_poller_behavior.autoscaling != nil {
			return pbt.fail("simple generated Workflow poller used the wrong C union arm")
		}
	case 2:
		if raw_options.workflow_task_poller_behavior.simple_maximum != nil ||
		   raw_options.workflow_task_poller_behavior.autoscaling == nil {
			return pbt.fail("autoscaling generated Workflow poller used the wrong C union arm")
		}
	}
	switch nexus_poller_mode {
	case 0:
		if raw_options.nexus_task_poller_behavior.simple_maximum != nil ||
		   raw_options.nexus_task_poller_behavior.autoscaling != nil {
			return pbt.fail("default generated Nexus poller was not encoded as null pointers")
		}
	case 1:
		if raw_options.nexus_task_poller_behavior.simple_maximum == nil ||
		   raw_options.nexus_task_poller_behavior.autoscaling != nil {
			return pbt.fail("simple generated Nexus poller used the wrong C union arm")
		}
	case 2:
		if raw_options.nexus_task_poller_behavior.simple_maximum != nil ||
		   raw_options.nexus_task_poller_behavior.autoscaling == nil {
			return pbt.fail("autoscaling generated Nexus poller used the wrong C union arm")
		}
	}
	return pbt.pass()
}

@(test)
worker_options_preserve_generated_core_configuration :: proc(t: ^testing.T) {
	result := pbt.check(
		"Worker options preserve Core configuration",
		worker_options_binding_property,
		{num_tests = 500, seed = 0x574f_524b_4f50_5453},
	)
	defer pbt.destroy_check_result(&result)
	pbt.require_pass(t, result)
}

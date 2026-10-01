package core

import "core:c"
import "core:math"
import "core:time"
import raw "./raw"

Metric_Attribute_Kind :: enum {
	String,
	Integer,
	Float,
	Boolean,
}

// Metric_Attribute strings are borrowed only for the synchronous Core call.
Metric_Attribute :: struct {
	key:           string,
	kind:          Metric_Attribute_Kind,
	string_value:  string,
	integer_value: i64,
	float_value:   f64,
	boolean_value: bool,
}

metric_attribute_string :: proc(key, value: string) -> Metric_Attribute {
	return {key = key, kind = .String, string_value = value}
}

metric_attribute_i64 :: proc(key: string, value: i64) -> Metric_Attribute {
	return {key = key, kind = .Integer, integer_value = value}
}

metric_attribute_f64 :: proc(key: string, value: f64) -> Metric_Attribute {
	return {key = key, kind = .Float, float_value = value}
}

metric_attribute_bool :: proc(key: string, value: bool) -> Metric_Attribute {
	return {key = key, kind = .Boolean, boolean_value = value}
}

Metric_Kind :: raw.Metric_Kind

// Metric_Meter owns one Core meter and its immutable base-attribute set. Close
// it before its Runtime. Instruments clone this state and do not borrow it.
Metric_Meter :: struct {
	runtime:    ^Runtime,
	handle:     ^raw.Metric_Meter,
	attributes: ^raw.Metric_Attributes,
	noop:       bool,
}

// Metric owns an independent Core meter, attribute set, and instrument. It may
// outlive the Metric_Meter that created it, but not the Runtime.
Metric :: struct {
	runtime:    ^Runtime,
	meter:      ^raw.Metric_Meter,
	handle:     ^raw.Metric,
	attributes: ^raw.Metric_Attributes,
	kind:       Metric_Kind,
	noop:       bool,
}

// metric_meter_noop returns an initialized meter which validates instruments
// and values but performs no host I/O. Language-level Activity and Workflow
// contexts use this when their Runtime has no metrics exporter configured.
metric_meter_noop :: proc() -> Metric_Meter {
	return {noop = true}
}

metric_error :: proc(message: string) -> Error {
	return client_option_error(message)
}

metric_attributes_validate :: proc(attributes: []Metric_Attribute) -> Error {
	for attribute in attributes {
		if len(attribute.key) == 0 {
			return metric_error("metric attribute key must not be empty")
		}
		if int(attribute.kind) < int(Metric_Attribute_Kind.String) ||
		   int(attribute.kind) > int(Metric_Attribute_Kind.Boolean) {
			return metric_error("metric attribute kind is invalid")
		}
		if attribute.kind == .Float &&
		   (math.is_nan(attribute.float_value) || math.is_inf(attribute.float_value)) {
			return metric_error("metric float attribute must be finite")
		}
	}
	return {}
}

metric_raw_attributes :: proc(
	attributes: []Metric_Attribute,
) -> (result: [dynamic]raw.Metric_Attribute, error: Error) {
	validation_error := metric_attributes_validate(attributes)
	if validation_error.message != nil {
		return nil, validation_error
	}
	result = make([dynamic]raw.Metric_Attribute, 0, len(attributes))
	for attribute in attributes {
		raw_attribute := raw.Metric_Attribute{key = byte_ref(attribute.key)}
		switch attribute.kind {
		case .String:
			raw_attribute.value.string_value = byte_ref(attribute.string_value)
			raw_attribute.value_type = .String
		case .Integer:
			raw_attribute.value.int_value = attribute.integer_value
			raw_attribute.value_type = .Int
		case .Float:
			raw_attribute.value.float_value = attribute.float_value
			raw_attribute.value_type = .Float
		case .Boolean:
			raw_attribute.value.bool_value = attribute.boolean_value
			raw_attribute.value_type = .Bool
		}
		append(&result, raw_attribute)
	}
	return
}

metric_raw_attributes_pointer :: proc(
	attributes: []raw.Metric_Attribute,
	dummy: ^raw.Metric_Attribute,
) -> ^raw.Metric_Attribute {
	if len(attributes) == 0 {
		// Rust slice::from_raw_parts requires a non-null aligned pointer even
		// when the length is zero.
		return dummy
	}
	return raw_data(attributes)
}

metric_meter_create :: proc(
	runtime: ^Runtime,
	attributes: []Metric_Attribute = nil,
) -> (meter: Metric_Meter, error: Error) {
	if runtime == nil || runtime.handle == nil {
		return {}, metric_error("runtime is not initialized")
	}
	raw_attributes, raw_attributes_error := metric_raw_attributes(attributes)
	if raw_attributes_error.message != nil {
		return {}, raw_attributes_error
	}
	defer delete(raw_attributes)
	handle := raw.metric_meter_new(runtime.handle)
	if handle == nil {
		return {}, metric_error("runtime metrics are not configured")
	}
	dummy := raw.Metric_Attribute{}
	owned_attributes := raw.metric_attributes_new(
		handle,
		metric_raw_attributes_pointer(raw_attributes[:], &dummy),
		c.size_t(len(raw_attributes)),
	)
	if owned_attributes == nil {
		raw.metric_meter_free(handle)
		return {}, metric_error("Core did not create metric attributes")
	}
	return {runtime = runtime, handle = handle, attributes = owned_attributes}, {}
}

metric_meter_with_attributes :: proc(
	meter: ^Metric_Meter,
	attributes: []Metric_Attribute,
) -> (child: Metric_Meter, error: Error) {
	attributes_error := metric_attributes_validate(attributes)
	if attributes_error.message != nil {
		return {}, attributes_error
	}
	if meter != nil && meter.noop {
		return metric_meter_noop(), {}
	}
	if meter == nil || meter.handle == nil || meter.attributes == nil ||
	   meter.runtime == nil || meter.runtime.handle == nil {
		return {}, metric_error("metric meter is not initialized")
	}
	raw_attributes, raw_attributes_error := metric_raw_attributes(attributes)
	if raw_attributes_error.message != nil {
		return {}, raw_attributes_error
	}
	defer delete(raw_attributes)
	handle := raw.metric_meter_new(meter.runtime.handle)
	if handle == nil {
		return {}, metric_error("runtime metrics are not configured")
	}
	dummy := raw.Metric_Attribute{}
	owned_attributes := raw.metric_attributes_new_append(
		handle,
		meter.attributes,
		metric_raw_attributes_pointer(raw_attributes[:], &dummy),
		c.size_t(len(raw_attributes)),
	)
	if owned_attributes == nil {
		raw.metric_meter_free(handle)
		return {}, metric_error("Core did not extend metric attributes")
	}
	return {runtime = meter.runtime, handle = handle, attributes = owned_attributes}, {}
}

metric_meter_close :: proc(meter: ^Metric_Meter) {
	if meter == nil {
		return
	}
	if meter.attributes != nil {
		raw.metric_attributes_free(meter.attributes)
	}
	if meter.handle != nil {
		raw.metric_meter_free(meter.handle)
	}
	meter^ = {}
}

metric_kind_valid :: proc(kind: Metric_Kind) -> bool {
	return int(kind) >= int(Metric_Kind.Counter_Integer) &&
	       int(kind) <= int(Metric_Kind.Up_Down_Counter_Integer)
}

metric_create :: proc(
	meter: ^Metric_Meter,
	name: string,
	kind: Metric_Kind,
	description := "",
	unit := "",
) -> (metric: Metric, error: Error) {
	if len(name) == 0 {
		return {}, metric_error("metric name must not be empty")
	}
	if !metric_kind_valid(kind) {
		return {}, metric_error("metric kind is invalid")
	}
	if meter != nil && meter.noop {
		return {kind = kind, noop = true}, {}
	}
	if meter == nil || meter.handle == nil || meter.attributes == nil ||
	   meter.runtime == nil || meter.runtime.handle == nil {
		return {}, metric_error("metric meter is not initialized")
	}
	owned_meter := raw.metric_meter_new(meter.runtime.handle)
	if owned_meter == nil {
		return {}, metric_error("runtime metrics are not configured")
	}
	dummy := raw.Metric_Attribute{}
	owned_attributes := raw.metric_attributes_new_append(
		owned_meter,
		meter.attributes,
		&dummy,
		0,
	)
	if owned_attributes == nil {
		raw.metric_meter_free(owned_meter)
		return {}, metric_error("Core did not clone metric attributes")
	}
	raw_options := raw.Metric_Options{
		name = byte_ref(name),
		description = byte_ref(description),
		unit = byte_ref(unit),
		kind = kind,
	}
	handle := raw.metric_new(owned_meter, &raw_options)
	if handle == nil {
		raw.metric_attributes_free(owned_attributes)
		raw.metric_meter_free(owned_meter)
		return {}, metric_error("Core did not create metric instrument")
	}
	return {
		runtime = meter.runtime,
		meter = owned_meter,
		handle = handle,
		attributes = owned_attributes,
		kind = kind,
	}, {}
}

metric_counter_create :: proc(
	meter: ^Metric_Meter,
	name: string,
	description := "",
	unit := "",
) -> (Metric, Error) {
	return metric_create(meter, name, .Counter_Integer, description, unit)
}

metric_histogram_integer_create :: proc(
	meter: ^Metric_Meter,
	name: string,
	description := "",
	unit := "",
) -> (Metric, Error) {
	return metric_create(meter, name, .Histogram_Integer, description, unit)
}

metric_histogram_float_create :: proc(
	meter: ^Metric_Meter,
	name: string,
	description := "",
	unit := "",
) -> (Metric, Error) {
	return metric_create(meter, name, .Histogram_Float, description, unit)
}

metric_histogram_duration_create :: proc(
	meter: ^Metric_Meter,
	name: string,
	description := "",
	unit := "duration",
) -> (Metric, Error) {
	return metric_create(meter, name, .Histogram_Duration, description, unit)
}

metric_gauge_integer_create :: proc(
	meter: ^Metric_Meter,
	name: string,
	description := "",
	unit := "",
) -> (Metric, Error) {
	return metric_create(meter, name, .Gauge_Integer, description, unit)
}

metric_gauge_float_create :: proc(
	meter: ^Metric_Meter,
	name: string,
	description := "",
	unit := "",
) -> (Metric, Error) {
	return metric_create(meter, name, .Gauge_Float, description, unit)
}

metric_up_down_counter_create :: proc(
	meter: ^Metric_Meter,
	name: string,
	description := "",
	unit := "",
) -> (Metric, Error) {
	return metric_create(meter, name, .Up_Down_Counter_Integer, description, unit)
}

metric_close :: proc(metric: ^Metric) {
	if metric == nil {
		return
	}
	if metric.handle != nil {
		raw.metric_free(metric.handle)
	}
	if metric.attributes != nil {
		raw.metric_attributes_free(metric.attributes)
	}
	if metric.meter != nil {
		raw.metric_meter_free(metric.meter)
	}
	metric^ = {}
}

metric_record_attributes :: proc(
	metric: ^Metric,
	additional_attributes: []Metric_Attribute,
) -> (attributes: ^raw.Metric_Attributes, temporary: bool, error: Error) {
	if metric != nil && metric.noop {
		validation_error := metric_attributes_validate(additional_attributes)
		return nil, false, validation_error
	}
	if metric == nil || metric.handle == nil || metric.meter == nil ||
	   metric.attributes == nil || metric.runtime == nil || metric.runtime.handle == nil {
		return nil, false, metric_error("metric is not initialized")
	}
	if len(additional_attributes) == 0 {
		return metric.attributes, false, {}
	}
	raw_attributes, attributes_error := metric_raw_attributes(additional_attributes)
	if attributes_error.message != nil {
		return nil, false, attributes_error
	}
	defer delete(raw_attributes)
	dummy := raw.Metric_Attribute{}
	attributes = raw.metric_attributes_new_append(
		metric.meter,
		metric.attributes,
		metric_raw_attributes_pointer(raw_attributes[:], &dummy),
		c.size_t(len(raw_attributes)),
	)
	if attributes == nil {
		return nil, false, metric_error("Core did not extend metric attributes")
	}
	return attributes, true, {}
}

metric_counter_add :: proc(
	metric: ^Metric,
	value: u64,
	additional_attributes: []Metric_Attribute = nil,
) -> Error {
	if metric == nil || metric.kind != .Counter_Integer {
		return metric_error("metric is not an integer counter")
	}
	attributes, temporary, attributes_error := metric_record_attributes(
		metric,
		additional_attributes,
	)
	if attributes_error.message != nil {
		return attributes_error
	}
	if metric.noop do return {}
	raw.metric_record_integer(metric.handle, value, attributes)
	if temporary do raw.metric_attributes_free(attributes)
	return {}
}

metric_histogram_record_integer :: proc(
	metric: ^Metric,
	value: u64,
	additional_attributes: []Metric_Attribute = nil,
) -> Error {
	if metric == nil || metric.kind != .Histogram_Integer {
		return metric_error("metric is not an integer histogram")
	}
	attributes, temporary, attributes_error := metric_record_attributes(metric, additional_attributes)
	if attributes_error.message != nil do return attributes_error
	if metric.noop do return {}
	raw.metric_record_integer(metric.handle, value, attributes)
	if temporary do raw.metric_attributes_free(attributes)
	return {}
}

metric_histogram_record_float :: proc(
	metric: ^Metric,
	value: f64,
	additional_attributes: []Metric_Attribute = nil,
) -> Error {
	if metric == nil || metric.kind != .Histogram_Float {
		return metric_error("metric is not a float histogram")
	}
	if math.is_nan(value) || math.is_inf(value) || value < 0 {
		return metric_error("float histogram value must be finite and non-negative")
	}
	attributes, temporary, attributes_error := metric_record_attributes(metric, additional_attributes)
	if attributes_error.message != nil do return attributes_error
	if metric.noop do return {}
	raw.metric_record_float(metric.handle, value, attributes)
	if temporary do raw.metric_attributes_free(attributes)
	return {}
}

metric_histogram_record_duration :: proc(
	metric: ^Metric,
	value: time.Duration,
	additional_attributes: []Metric_Attribute = nil,
) -> Error {
	if metric == nil || metric.kind != .Histogram_Duration {
		return metric_error("metric is not a duration histogram")
	}
	if value < 0 {
		return metric_error("duration histogram value must not be negative")
	}
	millis := u64(value / time.Millisecond)
	if value % time.Millisecond != 0 do millis += 1
	attributes, temporary, attributes_error := metric_record_attributes(metric, additional_attributes)
	if attributes_error.message != nil do return attributes_error
	if metric.noop do return {}
	raw.metric_record_duration(metric.handle, millis, attributes)
	if temporary do raw.metric_attributes_free(attributes)
	return {}
}

metric_gauge_record_integer :: proc(
	metric: ^Metric,
	value: u64,
	additional_attributes: []Metric_Attribute = nil,
) -> Error {
	if metric == nil || metric.kind != .Gauge_Integer {
		return metric_error("metric is not an integer gauge")
	}
	attributes, temporary, attributes_error := metric_record_attributes(metric, additional_attributes)
	if attributes_error.message != nil do return attributes_error
	if metric.noop do return {}
	raw.metric_record_integer(metric.handle, value, attributes)
	if temporary do raw.metric_attributes_free(attributes)
	return {}
}

metric_gauge_record_float :: proc(
	metric: ^Metric,
	value: f64,
	additional_attributes: []Metric_Attribute = nil,
) -> Error {
	if metric == nil || metric.kind != .Gauge_Float {
		return metric_error("metric is not a float gauge")
	}
	if math.is_nan(value) || math.is_inf(value) {
		return metric_error("float gauge value must be finite")
	}
	attributes, temporary, attributes_error := metric_record_attributes(metric, additional_attributes)
	if attributes_error.message != nil do return attributes_error
	if metric.noop do return {}
	raw.metric_record_float(metric.handle, value, attributes)
	if temporary do raw.metric_attributes_free(attributes)
	return {}
}

metric_up_down_counter_add :: proc(
	metric: ^Metric,
	value: i64,
	additional_attributes: []Metric_Attribute = nil,
) -> Error {
	if metric == nil || metric.kind != .Up_Down_Counter_Integer {
		return metric_error("metric is not an integer up-down counter")
	}
	attributes, temporary, attributes_error := metric_record_attributes(metric, additional_attributes)
	if attributes_error.message != nil do return attributes_error
	if metric.noop do return {}
	raw.metric_record_integer_signed(metric.handle, value, attributes)
	if temporary do raw.metric_attributes_free(attributes)
	return {}
}

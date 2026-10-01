package core

import raw "./raw"

// Client_Env_Config_Source is borrowed for a synchronous Core call. Set at
// most one of path and data. With neither set, Core follows TEMPORAL_CONFIG_FILE
// and then its platform-specific default temporal.toml location.
Client_Env_Config_Source :: struct {
	path: string,
	data: []u8,
}

Client_Env_Config_Load_Options :: struct {
	source:             Client_Env_Config_Source,
	config_file_strict: bool,
	// Empty uses the process environment. A non-empty value must be a JSON
	// object containing the exact environment map Core should use.
	env_vars_json: string,
}

Client_Env_Config_Profile_Load_Options :: struct {
	profile:            string,
	source:             Client_Env_Config_Source,
	disable_file:       bool,
	disable_env:        bool,
	config_file_strict: bool,
	// Empty uses the process environment unless disable_env is true.
	env_vars_json: string,
}

client_env_config_source_validate :: proc(source: Client_Env_Config_Source) -> Error {
	if len(source.path) > 0 && len(source.data) > 0 {
		return client_option_error("Temporal client configuration source cannot set both path and data")
	}
	return {}
}

client_env_config_result :: proc(success, fail: ^raw.Byte_Array) -> ([]u8, Error) {
	if success != nil && fail == nil {
		result := copy_core_bytes(success)
		raw.byte_array_free(nil, success)
		return result, {}
	}
	if fail != nil && success == nil {
		error := Error{message = copy_core_bytes(fail)}
		raw.byte_array_free(nil, fail)
		return nil, error
	}
	if success != nil {
		raw.byte_array_free(nil, success)
	}
	if fail != nil {
		raw.byte_array_free(nil, fail)
	}
	return nil, client_option_error("Core returned an invalid client configuration result")
}

client_env_config_load_json :: proc(
	options: Client_Env_Config_Load_Options = {},
) -> ([]u8, Error) {
	validation_error := client_env_config_source_validate(options.source)
	if validation_error.message != nil {
		return nil, validation_error
	}
	raw_options := raw.Client_Env_Config_Load_Options{
		path = byte_ref(options.source.path),
		data = bytes_ref(options.source.data),
		config_file_strict = options.config_file_strict,
		env_vars = byte_ref(options.env_vars_json),
	}
	result := raw.client_env_config_load(&raw_options)
	return client_env_config_result(result.success, result.fail)
}

client_env_config_profile_load_json :: proc(
	options: Client_Env_Config_Profile_Load_Options = {},
) -> ([]u8, Error) {
	validation_error := client_env_config_source_validate(options.source)
	if validation_error.message != nil {
		return nil, validation_error
	}
	if options.disable_file && options.disable_env {
		return nil, client_option_error(
			"Temporal client configuration cannot disable both file and environment loading",
		)
	}
	raw_options := raw.Client_Env_Config_Profile_Load_Options{
		profile = byte_ref(options.profile),
		path = byte_ref(options.source.path),
		data = bytes_ref(options.source.data),
		disable_file = options.disable_file,
		disable_env = options.disable_env,
		config_file_strict = options.config_file_strict,
		env_vars = byte_ref(options.env_vars_json),
	}
	result := raw.client_env_config_profile_load(&raw_options)
	return client_env_config_result(result.success, result.fail)
}

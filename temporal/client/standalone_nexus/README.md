# Standalone Nexus Client extension

This optional package owns the pre-release standalone Nexus operation Client
surface and its typed outbound interceptors. Workflow-originated Nexus calls
remain in `temporal/workflow`; importing the base `temporal/client` package does
not compile this extension.

```odin
import temporal_client "path/to/temporal/client"
import standalone_nexus "path/to/temporal/client/standalone_nexus"

client, err := temporal_client.connect()
defer temporal_client.close(&client)

err = standalone_nexus.add_interceptor(&client, {
	describe_nexus_operation = observe_describe,
})

handle, err := standalone_nexus.start_nexus_operation_json(
	&client,
	input,
	options,
)
defer standalone_nexus.nexus_operation_handle_free(&handle)
```

Register interceptors during Client setup, before concurrent use. Interceptor
records are copied; `user_data` remains application-owned and must outlive the
Client. The base Client owns the package's opaque interceptor state and invokes
its destroy callback exactly once during `temporal_client.close`.

`standalone_nexus.probe_support` performs the non-mutating count-RPC probe. The
generic base Client probe returns `Unknown` because the base package
intentionally does not import this extension.

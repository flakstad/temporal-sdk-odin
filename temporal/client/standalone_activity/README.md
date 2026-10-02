# Standalone Activity Client extension

This optional package owns the standalone Activity Client lifecycle and its
typed outbound interceptors. Workflow-scheduled Activities remain in
`temporal/workflow`; importing the base `temporal/client` package does not
compile this extension.

```odin
import temporal_client "path/to/temporal/client"
import standalone_activity "path/to/temporal/client/standalone_activity"

client, err := temporal_client.connect()
defer temporal_client.close(&client)

err = standalone_activity.add_interceptor(&client, {
	describe_standalone_activity = observe_describe,
})

handle, err := standalone_activity.start_standalone_activity_json(
	&client,
	input,
	options,
)
defer standalone_activity.standalone_activity_handle_free(&handle)
```

Register interceptors during Client setup, before concurrent use. Interceptor
records are copied; `user_data` remains application-owned and must outlive the
Client. The base Client owns the package's opaque interceptor state and invokes
its destroy callback exactly once during `temporal_client.close`.

The normalized `Standalone_Activity_Options` type remains in the base Client
package because ordinary Workflow Describe snapshots use it as well. This
extension aliases that type and its matching free procedure; it does not copy
or fork the ownership model.

`standalone_activity.probe_support` performs the non-mutating count-RPC probe.
The generic base Client probe returns `Unknown` because the base package
intentionally does not import this extension.

# Schedule Client extension

This optional package owns the Temporal Schedule lifecycle and its typed
outbound interceptors. Importing `temporal/client` alone does not compile this
package.

```odin
import temporal_client "path/to/temporal/client"
import schedule "path/to/temporal/client/schedule"

client, err := temporal_client.connect()
defer temporal_client.close(&client)

err = schedule.add_interceptor(&client, {
	describe_schedule = observe_describe,
})

handle, err := schedule.create_schedule_json(
	&client,
	"daily-orders",
	"reconcile_orders",
	input,
	options,
)
defer schedule.schedule_handle_free(&handle)
```

Register interceptors during Client setup, before concurrent use. Interceptor
records are copied; `user_data` remains application-owned and must outlive the
Client. The base Client owns the package's opaque interceptor state and invokes
its destroy callback exactly once during `temporal_client.close`.

`schedule.probe_count_support` performs the non-mutating `CountSchedules`
feature probe. The generic base Client probe returns `Unknown` for Schedule
count support because the base package intentionally does not import this
extension.

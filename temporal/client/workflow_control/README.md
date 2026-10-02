# Workflow control Client extension

This optional package owns the experimental Workflow Pause/Unpause API and its
typed outbound interceptors. Importing `temporal/client` alone does not compile
this package.

```odin
import temporal_client "path/to/temporal/client"
import workflow_control "path/to/temporal/client/workflow_control"

client, err := temporal_client.connect()
defer temporal_client.close(&client)

err = workflow_control.add_interceptor(&client, {
    workflow_pause_mutation = observe_pause,
})
err = workflow_control.pause_workflow(&handle, {
    reason = "maintenance",
})
```

Register interceptors during Client setup, before concurrent use. Interceptor
records are copied; `user_data` remains application-owned and must outlive the
Client. The base Client owns the package's opaque interceptor state and invokes
its destroy callback exactly once during `temporal_client.close`.

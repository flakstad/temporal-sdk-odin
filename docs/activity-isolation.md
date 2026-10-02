# Per-invocation Activity process isolation

`temporal/activity/isolate` is an experimental execution boundary for Odin
Activities. SDK Core, its Worker, task polling, task-token routing, and final
completion remain in the parent process. Each callback invocation runs in a
fresh child process:

```text
Core ActivityTask protobuf
  -> parent worker route
  -> versioned bounded pipe frame
  -> Odin child callback
  -> ActivityTaskCompletion protobuf
  -> parent validation
  -> Core completion
```

The transport does not implement the Temporal Server protocol. Its payloads are
the exact pinned SDK Core `coresdk.activity_task.ActivityTask` and
`coresdk.ActivityTaskCompletion` protobuf bytes. The parent rejects malformed
frames, mismatched request IDs, missing completion results, and completions
whose opaque task token differs from the start task.

## Current API and ownership

The parent creates an `activity_isolate.Manager`, then registers each external
type with `worker.register_external_activity_processor`. The command and its
arguments are copied by `manager_init`; the manager pointer is borrowed by the
Worker until polling stops. `manager_close` must run only after `worker.run`
returns. External registration data is never freed by `worker.close`.

The child calls `activity_isolate.host_run` with one activity name and one
ordinary `worker.Activity_Invoke_Proc`. `worker.execute_activity_task_callback`
decodes the Core task, reconstructs the complete callback-visible
`activity.Info` (including attempt, workflow identity, headers, retry policy,
timeouts, priority, and previous heartbeat details), runs the callback in an
Activity-local arena, and encodes the standard completion variants.

Every invocation gets private stdin/stdout pipes and a fresh process. Child
stderr is inherited and never shares the framed stdout channel. Process starts
from the Worker supervisor, Workflow isolate manager, and Activity isolate
manager use one SDK-global lock because Odin's `os.process_start` handle
inheritance mutation is not thread-safe.

Processor results carry their allocator explicitly and are released after Core
copies them; a zero allocator means the synchronous call's context allocator.
A child exit, timeout, protocol violation, or explicit executor error becomes a
retryable `OdinExternalActivityExecutorError`/`OdinExternalActivityProtocolError`
completion. It does not stop the parent Worker or unrelated Activities.
Temporal Retry Policy remains authoritative; applications must retain normal
Activity idempotency because a child can fail after an external side effect.

## Verified path

Run the live proof against the recommended CLI dev server:

```bash
./scripts/activity-isolate-integration-test.sh
```

The script starts `temporal server start-dev` when needed, runs the Core-owning
Odin parent, and starts a Rust Workflow that calls the external `hello`
Activity. Attempt 1 invokes Odin `panic` in the per-invocation child. The child
dies, the parent converts that loss into a retryable Activity failure, and
Temporal delivers attempt 2. A fresh child returns `Recovered on Activity
attempt 2`; the parent then shuts Core down cleanly. Exact manager statistics
require two children, one failed child, one valid completion, one attempt-2
heartbeat acknowledged by Core, and a fully drained route set.

Unit tests additionally cover binary frame bytes, corrupt magic, truncation,
payload limits, full Activity metadata reconstruction, and exact/mismatched
completion task tokens. The ordinary property suite continues to cover opaque
binary task-token routing in the parent.

## Deliberate limitations

This is a correctness and crash-containment slice, not yet the default Activity
runtime:

- a fresh process is started per attempt; there is no warm bounded pool;
- child heartbeats use protocol v2 `Heartbeat`/`Heartbeat_Ack` frames and run
  through the parent's ordinary heartbeat/interceptor/Core chain;
- a Core cancel task kills the matching child and the parent returns a canceled
  completion, rather than first offering cooperative child cancellation;
- parent Activity execute interceptors are rejected for external registrations
  because callback execution occurs in the child. Parent heartbeat interceptors
  still wrap forwarded heartbeats; child-side execute interceptor registration
  is not yet exposed by `Host_Options`;
- child metrics use a no-op meter; structured log/metric forwarding is not yet
  part of protocol v2;
- shutdown currently assumes `worker.run` has drained all invocations before
  `manager_close`; forced manager teardown is best-effort;
- CPU/memory sandboxing, warm-pool admission, crash-loop policy, release
  signing, and all-target fault evidence remain production work.

The next increment should use the now-duplex protocol for cancellation: the
parent sends a cooperative cancel frame while the callback is live, followed by
a bounded TERM/KILL fallback. Only then should persistent warm children and
multi-activity manifests be added.

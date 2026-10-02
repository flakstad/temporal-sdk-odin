# Per-invocation Activity process isolation

`temporal/activity/isolate` is an opt-in execution boundary for Odin
Activities. SDK Core, its Worker, task polling, task-token routing, and final
completion remain in the parent process. Callbacks run either in fresh children
or serially in an opt-in bounded warm pool:

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

The parent creates an `activity_isolate.Manager`, then passes its application
manifest to `register_external_activity_manifest`. That helper copies only each
type name into the Core Worker; callback data and procedure pointers remain
process-local. The same ordinary Odin function can construct the manifest in
the child, where the callbacks are actually invoked. The command and its
arguments are copied by `manager_init`; the manager pointer is borrowed by the
Worker until polling stops. `manager_close` must run only after `worker.run`
returns. External registration data is never freed by `worker.close`.
`execution_timeout` bounds the complete invocation, including heartbeat
exchanges. `cancellation_grace` defaults to five seconds and bounds the interval
between a forwarded Core cancel task and termination of an unresponsive child.
`warm_pool_size = 0` retains one process per invocation; a positive value bounds
the number of persistent children. `startup_timeout` bounds the manifest
handshake for every newly started warm child. `shutdown_timeout` independently
bounds each child's protocol acknowledgement and process exit. Idle children
are drained concurrently, so those bounds do not multiply by pool size. The optional
`crash_loop` policy opens after a configured number of consecutive child/start
failures and rejects new starts until its cooldown admits one trial generation.
A successful Activity completion resets the count; a successful handshake alone
does not, because a callback may still crash immediately.

The child calls `activity_isolate.host_run` with either the single-callback
`host_options` convenience or an explicit `host_options_many` manifest of
`Host_Activity` values. The host decodes the requested Activity type and
dispatches its process-local `worker.Activity_Invoke_Proc`; procedure pointers
never cross the pipe. Empty names, nil callbacks, and duplicate manifest names
are rejected before the host reads a task. `worker.execute_activity_task_callback`
then decodes the Core task and reconstructs the complete callback-visible
`activity.Info` (including attempt, workflow identity, headers, retry policy,
timeouts, priority, and previous heartbeat details), runs the callback in an
Activity-local arena, and encodes the standard completion variants.

Every child gets private stdin/stdout pipes and runs at most one callback at a
time. `host_run` serves one invocation; `host_run_loop` serves serial requests
until protocol-v5 `Shutdown`. Pool admission blocks excess Activity execution
threads without exceeding the configured child bound. Successful children are
returned to the pool; crashed, timed-out, canceled-by-force, or
protocol-invalid children are retired and replaced on demand. Child stderr is
inherited and never shares the framed stdout channel. Process starts from the
Worker supervisor, Workflow isolate manager, and Activity isolate manager use
one SDK-global lock because Odin's `os.process_start` handle inheritance
mutation is not thread-safe.

Before a warm child enters the pool, protocol v5 requests its canonical,
length-prefixed Activity type manifest and requires byte-for-byte equality with
the parent manifest. Registration order is therefore part of the contract. A
missing, malformed, stale, reordered, or timed-out manifest retires the process
before any Core task or task token is dispatched to it.

Processor results carry their allocator explicitly and are released after Core
copies them; a zero allocator means the synchronous call's context allocator.
A child exit, timeout, protocol violation, or explicit executor error becomes a
retryable `OdinExternalActivityExecutorError`/`OdinExternalActivityProtocolError`
completion. It does not stop the parent Worker or unrelated Activities.
Temporal Retry Policy remains authoritative; applications must retain normal
Activity idempotency because a child can fail after an external side effect.
When a cooperative callback returns an encoded canceled completion, the parent
preserves it—including callback-authored details—instead of replacing it with a
generic cancellation.

As with ordinary remote Temporal Activities, server-requested cancellation is
observed through heartbeat traffic. A long-running isolated Activity must
heartbeat for the Service/Core cancel task to reach the parent; protocol v5 then
delivers it independently of any subsequent child heartbeat.

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
heartbeat acknowledged by Core, and a fully drained route set. Two additional
queues prove cooperative cancellation with zero kills and ignored cancellation
with exactly one grace-deadline kill. A fourth queue runs `hello` followed by
`goodbye` in one child and proves exact type dispatch plus reuse. A fifth starts
two Activities concurrently against pool size one and requires bounded
admission, serial execution, one process start, and one reuse. A sixth queue
runs the same concurrent Activities through a size-two pool and requires both
children to acknowledge the parallel shutdown drain. A seventh queue deliberately
starts an incompatible child manifest twice. The first
failure opens a one-failure breaker; the second task is rejected without a new
process, while callback dispatch, pool admission, and task-token routing remain
at zero.

Unit tests additionally cover binary frame bytes, corrupt magic, truncation,
payload limits, full Activity metadata reconstruction, and exact/mismatched
completion task tokens. The ordinary property suite continues to cover opaque
binary task-token routing in the parent.

## Deliberate limitations

This is a correctness and crash-containment slice, not yet the default Activity
runtime:

- warm pooling is opt-in; the default remains a fresh process per attempt;
- child heartbeats use protocol v5 `Heartbeat`/`Heartbeat_Ack` frames and run
  through the parent's ordinary heartbeat/interceptor/Core chain;
- Core cancel tasks use a serialized `Cancel` control frame and update the same
  child `activity.Context` observed by the callback. The manager starts a
  per-execution grace timer and kills only a child that does not return;
- parent Activity execute interceptors are rejected for external registrations
  because callback execution occurs in the child. Parent heartbeat interceptors
  still wrap forwarded heartbeats; child-side execute interceptor registration
  is not yet exposed by `Host_Options`;
- child metrics use a no-op meter; structured log/metric forwarding is not yet
  part of protocol v5;
- shutdown requires `worker.run` to have drained all invocations;
- prewarming/general readiness probes, CPU/memory sandboxing, release signing,
  and all-target fault evidence remain production work.

General readiness probes and child-side interceptor/diagnostic transport remain
the next hardening steps before treating the warm pool as a default.

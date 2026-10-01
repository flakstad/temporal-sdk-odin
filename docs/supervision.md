# Worker process supervision

The Odin SDK relies on Temporal Server for durable Workflow state and history,
not on a Worker process or an Odin fiber remaining alive. Production Worker
processes must nevertheless run behind a process boundary. The SDK now includes
an optional `temporal/supervisor` package for launching and restarting a whole
Worker executable. A platform supervisor such as systemd, launchd, Kubernetes,
or another service manager should still supervise that top-level process.

This is whole-Worker isolation. It is not yet a per-Workflow subprocess pool:
one fatal callback can still terminate every registration in that child, after
which Temporal retries unfinished tasks in the replacement process.
The separate [`workflow-isolation.md`](workflow-isolation.md) spike now proves
that a child can retain one suspended Workflow fiber across multiple framed
Core activations, but that transport is not yet wired into `worker.run`.

## SDK supervisor

`supervisor.run` is a blocking ownership boundary around one child generation
at a time. Options, command/environment slices, callbacks, user data, and file
handles are borrowed for the complete call. The default restart policy restarts
failed children, stops after a clean exit, applies bounded exponential backoff,
and caps consecutive restart attempts. A process that survives `stable_after`
resets the crash-loop counter and backoff.

```odin
command := []string{"./my-temporal-worker"}
options := supervisor.options_default(command)
options.readiness_probe = worker_is_polling
options.policy.readiness_timeout = 30 * time.Second

control := supervisor.Control{}
stats, err := supervisor.run(&control, options)
defer supervisor.error_free(&err)
```

`request_stop` is thread-safe. It asks the child to terminate, waits the
configured grace period, then kills and reaps it if necessary. Readiness is an
application callback because only the Worker executable knows whether all of
its registrations and pollers are usable. An unready child that exits or times
out counts as failed and follows the same restart policy. Observer callbacks
are synchronous and must return promptly; they receive generation, PID, exit,
readiness, and restart-delay events.

The package serializes its own calls to Odin's non-thread-safe `process_start`.
Applications that launch unrelated processes concurrently must coordinate
those external calls separately.

## Required supervisor contract

A replacement process must start with:

- the same Temporal namespace and task queue;
- compatible Workflow, Activity, and Nexus registrations;
- the SDK Core native library and generated protobuf bindings from the same
  pinned release unit; and
- a code version compatible with histories routed to that Worker. Use Temporal
  Worker Deployment Versioning when incompatible versions coexist.

The supervisor must restart a process after any non-zero exit or fatal signal,
apply bounded crash-loop backoff, and expose the process as ready only after the
Worker reports that it is polling. `temporal/supervisor` implements these child
lifecycle rules. Normal deployment should request graceful shutdown first. A
bounded grace period may then end in whole-process termination; individual Odin
executor threads must never be forcibly canceled.

An Odin panic, native crash, fixed Workflow-stack overflow, or externally
detected infinite Workflow callback can therefore terminate the whole process.
Temporal will retry an uncompleted Workflow Task. A replacement Worker rebuilds
the Workflow by replaying committed history on a fresh fiber. In-memory globals,
fiber stacks, allocator contents, and work performed after the last successful
Workflow Activation Completion are not durable.

This recovery model is why Workflow code must be deterministic and must not
perform direct filesystem, network, database, wall-clock, or random side
effects. Put those effects in Activities. An Activity may be delivered again
after a process crash, so its externally visible effects must be idempotent or
protected by an application-level idempotency key.

For the strongest current isolation, run Workflow and Activity registrations in
separate supervised Worker children. A panicking Activity then cannot destroy
cached Workflow fibers. Fine-grained SDK-managed per-execution subprocess pools
remain future work.

## Acceptance evidence

Run the focused crash/restart proof against the recommended Temporal CLI dev
server:

```bash
temporal server start-dev --headless --log-level error
./scripts/workflow-crash-integration-test.sh
```

The test starts `examples/worker_supervisor`, which launches a Workflow Worker
on a unique task queue and reports readiness only after the child log confirms
polling. After the first Workflow Task commits and the fiber is durably
suspended on a condition, the test sends `SIGKILL` to generation 1. The SDK
supervisor observes exit 9, applies backoff, starts generation 2, and reports it
ready. A Signal then makes the replacement replay committed history on a fresh
fiber and complete the same Workflow Execution. Final statistics require two
starts/readiness events, one failed exit, one restart, and one clean exit.

This proves automatic whole-Worker restart plus process-loss recovery for a
suspended deterministic Workflow. It does not make an individual process
crash-safe, recover from incompatible code, isolate registrations within one
child, or automatically detect infinite Workflow code.

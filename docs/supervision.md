# Worker process supervision

The Odin SDK relies on Temporal Server for durable Workflow state and history,
not on a Worker process or an Odin fiber remaining alive. Production Worker
processes must nevertheless run under an external supervisor. The SDK does not
currently create or manage child Worker processes.

## Required supervisor contract

A replacement process must start with:

- the same Temporal namespace and task queue;
- compatible Workflow, Activity, and Nexus registrations;
- the SDK Core native library and generated protobuf bindings from the same
  pinned release unit; and
- a code version compatible with histories routed to that Worker. Use Temporal
  Worker Deployment Versioning when incompatible versions coexist.

The supervisor should restart a process after any non-zero exit or fatal signal,
apply bounded crash-loop backoff, and expose the process as ready only after the
Worker reports that it is polling. Normal deployment should request graceful
shutdown first. A bounded grace period may then end in whole-process termination;
individual Odin executor threads must never be forcibly canceled.

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
separate Worker processes. A panicking Activity then cannot destroy cached
Workflow fibers. The SDK-managed subprocess pool discussed in the architecture
roadmap is not implemented yet.

## Acceptance evidence

Run the focused crash/restart proof against the recommended Temporal CLI dev
server:

```bash
temporal server start-dev --headless --log-level error
./scripts/workflow-crash-integration-test.sh
```

The test starts a Workflow on a unique task queue and waits until its first
Workflow Task has completed and the Odin fiber is durably suspended on a
condition. It sends `SIGKILL` to the Worker, confirms that the Workflow remains
open, starts an identical replacement Worker, and delivers a Signal. The new
process must replay the committed history on a fresh fiber and complete the
same Workflow Execution.

This proves process-loss recovery for a suspended deterministic Workflow. It
does not make an individual process crash-safe, recover from incompatible code,
or prove automatic detection of infinite Workflow code.

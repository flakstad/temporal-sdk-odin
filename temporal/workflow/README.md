# Workflow package (research only)

`spike.odin` is a bounded explicit state machine for one Workflow shape:

```text
initialize -> schedule hello Activity -> resolve Activity -> complete
```

`fiber_spike.odin` drives serial commands, multiple outstanding Activities,
handled Activity failures, buffered signals, a durable signal/timer race, and
Activity-awaiting Workflow Update routines through stackful coroutines. It also
implements Core's `NotifyHasPatch`/`SetPatchMarker` replay protocol through
`patched` and `deprecate_patch`. Workflow time, history length, replay status,
deterministic random values, and UUID v4 values come only from activation
metadata and Core-provided seeds. The
current live Workflow includes:

```odin
hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
    approval := workflow.await_signal_or_timeout(ctx, "approval", 30 * time.Second)
    if approval.status == .Timed_Out {
        return "Approval timed out"
    }
    return workflow.execute_activity(ctx, "hello", approval.value)
}
```

The C stack is only a cache. Eviction destroys it; replay starts the procedure
again on a new stack and must reproduce the same commands from history. A
per-instance Odin arena owns values that cross activation boundaries.

Accepted Update handlers use independent fiber stacks and wait state while
sharing the Workflow's deterministic command sequence and operation tables.
This permits `Accepted` plus an Activity command in one completion and the
terminal Update response after a later Activity resolution. Replay recreates
both stacks and requires byte-identical completions.

Both implementations exist to test Core activations, commands, ownership,
forced eviction, and replay. The fiber version has seq-keyed Activity/timer
operation tables, emits multiple commands in one completion, and applies every
resolution job before resuming Update routines and the main Workflow fiber in a
stable order. Activity failures are
arena-owned resolutions: `execute_activity_result` makes them handleable, while
the simpler `execute_activity` propagates an unhandled failure to Temporal.
Named string signals are queued in arrival order and do not resume a fiber that
is waiting on another operation. `await_signal_or_timeout` schedules a durable
timer and emits `CancelTimer` only when a signal wins before the timer fires.
`temporal/worker` now provides experimental name-based Workflow registration
and dispatch, but neither the fiber API nor its one-string codec is stable or a
complete Workflow runtime. Do not build production Workflows against them.

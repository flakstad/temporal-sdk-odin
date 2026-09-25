# Workflow package (research only)

`spike.odin` is a bounded explicit state machine for one Workflow shape:

```text
initialize -> schedule hello Activity -> resolve Activity -> complete
```

`fiber_spike.odin` drives serial commands, multiple outstanding Activities,
handled Activity failures, buffered signals, and a durable signal/timer race
through a stackful coroutine. The current live example is:

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

Both implementations exist to test Core activations, commands, ownership,
forced eviction, and replay. The fiber version has seq-keyed Activity/timer
operation tables, emits multiple commands in one completion, and applies every
resolution job before resuming the single Workflow fiber. Activity failures are
arena-owned resolutions: `execute_activity_result` makes them handleable, while
the simpler `execute_activity` propagates an unhandled failure to Temporal.
Named string signals are queued in arrival order and do not resume a fiber that
is waiting on another operation. `await_signal_or_timeout` schedules a durable
timer and emits `CancelTimer` only when a signal wins before the timer fires.
`temporal/worker` now provides experimental name-based Workflow registration
and dispatch, but neither the fiber API nor its one-string codec is stable or a
complete Workflow runtime. Do not build production Workflows against them.

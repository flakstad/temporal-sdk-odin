# Workflow package (research only)

`spike.odin` is a bounded explicit state machine for one Workflow shape:

```text
initialize -> schedule hello Activity -> resolve Activity -> complete
```

`fiber_spike.odin` drives serial commands, multiple outstanding Activities, and
handled Activity failures through a stackful coroutine. The current live
example is:

```odin
hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
    failed := workflow.execute_activity_result(ctx, "missing", name)
    if failed.status != .Failed {
        return "unexpected Activity success"
    }
    recovery_input, _ := strings.concatenate({"Recovered ", name})
    return workflow.execute_activity(ctx, "hello", recovery_input)
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
Neither
implementation is a stable registration API or a complete Workflow runtime. Do
not build application Workflows against them.

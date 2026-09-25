# Workflow package (research only)

`spike.odin` is a bounded explicit state machine for one Workflow shape:

```text
initialize -> schedule hello Activity -> resolve Activity -> complete
```

`fiber_spike.odin` drives both serial commands and two outstanding Activities
through a stackful coroutine. The current live example is:

```odin
hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
    first := workflow.start_activity(ctx, "hello", name)
    second := workflow.start_activity(ctx, "hello", "Temporal")
    a := workflow.await_activity(ctx, first)
    b := workflow.await_activity(ctx, second)
    result, _ := strings.concatenate({a, " & ", b})
    return result
}
```

The C stack is only a cache. Eviction destroys it; replay starts the procedure
again on a new stack and must reproduce the same commands from history. A
per-instance Odin arena owns values that cross activation boundaries.

Both implementations exist to test Core activations, commands, ownership,
forced eviction, and replay. The fiber version has seq-keyed Activity/timer
operation tables, emits multiple commands in one completion, and applies every
resolution job before resuming the single Workflow fiber. Neither
implementation is a stable registration API or a complete Workflow runtime. Do
not build application Workflows against them.

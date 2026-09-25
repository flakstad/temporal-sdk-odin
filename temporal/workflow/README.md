# Workflow package (research only)

`spike.odin` is a bounded explicit state machine for one Workflow shape:

```text
initialize -> schedule hello Activity -> resolve Activity -> complete
```

`fiber_spike.odin` drives the same shape through a stackful coroutine:

```odin
hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
    greeting := workflow.execute_activity(ctx, "hello", name)
    return greeting
}
```

The C stack is only a cache. Eviction destroys it; replay starts the procedure
again on a new stack and must reproduce the same commands from history. A
per-instance Odin arena owns values that cross activation boundaries.

Both implementations exist to test Core activations, commands, ownership,
forced eviction, and replay. The fiber version currently supports exactly one
string Activity await. Neither is a stable registration API or a complete
Workflow runtime. Do not build application Workflows against them.

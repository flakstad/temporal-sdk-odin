# Workflow package (research only)

`spike.odin` is a bounded explicit state machine for one Workflow shape:

```text
initialize -> schedule hello Activity -> resolve Activity -> complete
```

It exists to test Core activations, commands, forced eviction, and replay. It is
not a registration API or the intended sequential Odin Workflow runtime. Do not
build application Workflows against it.

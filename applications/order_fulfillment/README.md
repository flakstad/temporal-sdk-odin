# Order fulfillment application

This is a small but complete command-line application built against the public
Odin SDK packages. It imports the repository as an Odin collection; it does not
reach into SDK internals.

The application models an order that:

1. reserves inventory in an Activity;
2. waits durably for an operator approval Signal, with a five-minute timeout;
3. charges payment in an Activity;
4. waits on a durable five-second carrier timer; and
5. dispatches the order in an Activity.

The `status` Query observes Workflow-owned state without changing history.
`result` waits for the durable execution result. The Activities are deliberately
local stand-ins for inventory, payment, and shipping adapters; those are the
places where a real service would call external systems and enforce idempotency.

From the repository root:

```bash
temporal server start-dev
./scripts/build-order-fulfillment.sh

# Terminal 1
./native/target/release/temporal-odin-order-fulfillment worker

# Terminal 2
./native/target/release/temporal-odin-order-fulfillment start demo-1 "Odin mug" 2
./native/target/release/temporal-odin-order-fulfillment status demo-1
./native/target/release/temporal-odin-order-fulfillment approve demo-1 Andreas
./native/target/release/temporal-odin-order-fulfillment result demo-1
```

`TEMPORAL_ADDRESS` defaults to `http://localhost:7233` and
`TEMPORAL_NAMESPACE` defaults to `default`.

For a self-contained run that starts the CLI dev server when needed, builds the
application, launches a bounded Worker, and exercises the complete interaction.
The acceptance run deliberately kills that Worker while the durable timer is
pending, starts a replacement, and verifies replay and completion:

```bash
./scripts/order-fulfillment-integration-test.sh
```

`worker-once` is intended for that bounded acceptance run. `worker` is the
unbounded service mode and runs until its process is stopped.

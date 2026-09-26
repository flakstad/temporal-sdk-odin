# Temporal SDK for Odin — experimental

An experimental Odin language SDK built on [Temporal SDK Core](https://github.com/temporalio/sdk-rust),
not a reimplementation of Temporal Server or its gRPC protocol.

Current status: Milestones 0–2 plus bounded explicit-state-machine and stackful
fiber Workflow experiments. The repository builds the official Rust Core C
bridge, connects to a Temporal CLI dev server, executes a remote Activity in
Odin, and now starts a Workflow and obtains its typed result through a public
Odin Client over Core's generic WorkflowService RPC bridge. The Client also
sends typed signals, requests cancellation, and terminates executions. It proves
multiple outstanding Activities, catchable Activity failure,
buffered Workflow signals, and a durable signal/timer race across fresh-stack
replay. The live Workflow starts a timer, lets an `approval` signal win, emits
`CancelTimer` alongside an Activity command, and reproduces both commands after
eviction. Workflow cancellation now resumes suspended Odin code with a canceled
result, exposes the owned reason, and permits deterministic Activity/timer
cleanup in an explicit detached scope before `CancelWorkflowExecution`. A
reusable worker now registers both Activity and Workflow procedures, routes run IDs to the
correct fiber runtime, and polls both Core task streams concurrently. Its
bounded reusable Activity thread pool runs callbacks concurrently, exposes
cooperative cancellation and typed heartbeat payloads through
`activity.Context`, and has live tests for overlap and heartbeat-driven
cancellation. `activity.info(ctx)` now exposes the Core-delivered execution
identity, timestamps/timeouts, effective Retry Policy, priority, and previous
heartbeat details as callback-scoped borrowed values; a retry integration test
recovers typed heartbeat progress on attempt 2. Unknown Workflow types fail the retryable Workflow Task without
terminating the worker. Typed Activities can return explicit retryable or
non-retryable Application Failures with a failure type, JSON details, and an
optional next-retry delay; the live suite proves Core/Server redelivery from
attempt 1 to attempt 2. Workflow initialization, Activity arguments/results,
and Workflow completion now preserve generic Payloads; typed JSON Workflows run
end-to-end across eviction and fresh-stack replay. Workflow-scheduled Activities
now accept IDs, task queues, all four server timeouts, Retry Policies,
cancellation type, and eager-execution control. It does **not** provide general
nested/child cancellation-scope trees, signal handlers, queries/updates,
multiple Activity arguments, in-process panic containment, or a stable public
API.
Do not use it in production.

```text
Temporal CLI dev server
          ^ gRPC
          |
Temporal SDK Core
          ^ C ABI
          |
Odin raw bindings -> generated Core protobuf types -> worker registry/run loop
                                                   |-> Activity dispatch
                                                   `-> Workflow fibers
```

## Quick start

Prerequisites:

- Odin (tested with `dev-2026-05:ea5175d86`)
- Rust toolchain 1.94 and Cargo
- `protoc`
- Temporal CLI (tested with CLI 1.5.1 / Server 1.29.1)
- Clang/linker tooling for the target platform

The fiber experiment also builds pinned
[minicoro](https://github.com/edubart/minicoro) through a six-symbol local C
shim. It is a research dependency, not yet a frozen SDK choice.
The test-only [pbt](https://github.com/flakstad/pbt) submodule generates
payload/failure contracts and activation races, and verifies byte-identical
replay completions.

Clone submodules and build the pinned Core bridge:

```bash
git submodule update --init --recursive
./scripts/build-core.sh
```

Build all Odin examples in one pass with:

```bash
./scripts/build-examples.sh
```

In one terminal, start the recommended local development server:

```bash
temporal server start-dev
```

In another terminal, run the Activity Worker:

```bash
./scripts/run-hello-activity.sh
```

The worker waits for an Activity on task queue `odin-hello-activity`. The full
integration test starts a small workflow-only driver from the pinned upstream
Rust SDK, verifies both success and failure through the server, and exits:

```text
Workflow observed Odin Activity result: Hello Odin
one Activity Task completed and worker shut down cleanly
```

Run it with:

```bash
./scripts/integration-test.sh
```

With `examples/hello_workflow` polling, the public Client example performs
`StartWorkflowExecution`, long-polls `GetWorkflowExecutionHistory` for the
close event, and decodes the result Payload without Temporal CLI:

```bash
./scripts/run-client-start.sh
```

The core API is intentionally small:

```odin
client, err := temporal_client.connect()
defer temporal_client.close(&client)

handle, err := temporal_client.start_workflow_json(
    &client,
    "order_workflow",
    order,
    {id = "order-42", task_queue = "orders"},
)
defer temporal_client.workflow_handle_free(&handle)

result: Order_Result
err = temporal_client.get_result_json(&handle, &result)
```

The Client owns its Core runtime, connection, and namespace. A Workflow handle
owns its ID/run-ID strings but borrows the Client, so handles must be freed
before `client.close`. The Payload variants expose the codec-neutral boundary;
the JSON variants currently send and receive exactly one Payload.

Existing executions can be addressed without an RPC when creating the handle:

```odin
handle, err := temporal_client.get_workflow_handle(&client, "order-42")
defer temporal_client.workflow_handle_free(&handle)

err = temporal_client.signal_workflow_json(&handle, "approve", approval)
err = temporal_client.cancel_workflow(&handle, "customer canceled")
```

An empty run ID targets the current execution. Signal and cancellation calls
generate UUIDv4 request IDs unless the caller supplies one. Termination has
plain, Payload, and typed JSON detail variants.

The combined example now uses the reusable registration boundary:

```odin
temporal_worker, _ := worker.create(
    &client,
    "odin-hello-workflow",
)
_ = worker.register_activity(&temporal_worker, "hello", hello)
_ = worker.register_json_activity(&temporal_worker, "typed_greeting", typed_greeting)
_ = worker.register_workflow(&temporal_worker, "odin_hello_workflow", hello_workflow)
_ = worker.register_json_workflow(&temporal_worker, "odin_typed_workflow", typed_workflow)
_, _ = worker.run(&temporal_worker)
```

`register_json_activity` accepts ordinary typed Odin procedures such as
`proc(^activity.Context, Greeting_Input) -> Greeting_Output`. The generic
adapter encodes one `json/plain` Temporal Payload; the lower-level
`register_payload_activity` callback receives a borrowed `codec.Payload_View`
and can return any allocator-owned `codec.Payload`. Existing string Activities
remain source-compatible through `register_activity`.

Callbacks that need application failures use the result adapter:

```odin
charge :: proc(ctx: ^activity.Context, input: Charge) -> activity.Result(Receipt) {
    if input.amount <= 0 {
        return activity.result_failure(
            Receipt,
            "amount must be positive",
            failure_type = "InvalidCharge",
            non_retryable = true,
        )
    }
    return activity.result_success(Receipt{transaction_id = "example"})
}

_ = worker.register_json_activity_result(&temporal_worker, "charge", charge)
```

`result_failure_json` additionally attaches typed `json/plain` details and can
override the next retry delay. Retryable is the default for this explicit
result API. Payload-conversion failures are non-retryable SDK failures.

Activity execution metadata is available only while the callback is running:

```odin
charge :: proc(ctx: ^activity.Context, input: Charge) -> Receipt {
    info := activity.info(ctx)
    fmt.printf("%s attempt %d on %s\n", info.activity_id, info.attempt, info.task_queue)

    previous: Progress
    found, decode_error := activity.heartbeat_detail_json(ctx, 0, &previous)
    // `info`, its strings/slices, and the raw Payload view are borrowed from ctx.
    _ = found
    _ = decode_error
    return process_charge(input, previous)
}
```

Core supplies the Activity task fields, but the task queue comes from worker
configuration. The effective Retry Policy may differ from the scheduling
request because the Service can apply defaults or limits. `activity.deadline`
returns the earlier applicable Schedule-to-Close and Start-to-Close deadline.
Use `heartbeat_detail_payload` when a non-JSON codec owns the data.

Typed Workflows use the same codec boundary:

```odin
greeting_workflow :: proc(
    ctx: ^workflow.Fiber_Context,
    input: Greeting_Input,
) -> Greeting_Output {
    return workflow.execute_activity_json(
        ctx,
        "typed_greeting",
        input,
        Greeting_Output,
    )
}

_ = worker.register_json_workflow(&temporal_worker, "greeting", greeting_workflow)
```

The options form remains explicit and allocator-safe across suspension:

```odin
retry := workflow.Retry_Policy{
    initial_interval = time.Second,
    backoff_coefficient = 2,
    maximum_attempts = 3,
}
result := workflow.execute_activity_json_with_options(
    ctx,
    "typed_greeting",
    input,
    Greeting_Output,
    {
        activity_id = "greeting-for-order-42",
        start_to_close_timeout = 10 * time.Second,
        heartbeat_timeout = 2 * time.Second,
        retry_policy = &retry,
        cancellation_type = .Wait_Cancellation_Completed,
    },
)
```

`Activity_Options` borrows its strings, slices, and Retry Policy only for the
call. The runtime copies them into Workflow-owned storage before it can
suspend. At least one of Start-to-Close or Schedule-to-Close must be positive.
The compatibility helpers retain their experimental 20-second
Start-to-Close/one-attempt behavior.

`register_payload_workflow` is the lower-level form. String Workflows remain a
compatibility adapter over `json/plain`. Workflow signal buffering preserves a
generic Payload; `await_signal_payload` and `await_signal_json` expose raw and
typed forms, with equivalent signal-or-timeout variants. `await_signal` remains
the `json/plain` string compatibility adapter.

Catchable Activity failures expose both the outer Activity metadata and the
nested Application Failure:

```odin
result := workflow.execute_activity_result(ctx, "charge", order_id)
if result.status == .Failed {
    app := result.failure.application
    details: Charge_Failure
    found, decode_error := workflow.activity_failure_detail_json(
        &result.failure,
        0,
        &details,
    )
    // result.failure: Activity ID/type, identity, retry state
    // app: message, type, retryability, category, details, next retry delay
}
```

These values are borrowed from the Workflow instance and remain valid for that
run. Unhandled failures still propagate the preserved Temporal Failure protobuf.

Cancellation is cooperative in Workflow code. Result-returning waits report
`.Canceled`; cleanup that must outlive root cancellation runs in an explicit
detached scope:

```odin
result := workflow.execute_activity_result(ctx, "charge", order_id)
if result.status == .Canceled {
    reason := workflow.cancellation_reason(ctx)
    scope := workflow.detached_cancellation_scope(ctx)
    defer workflow.cancellation_scope_end(&scope)
    workflow.execute_activity(ctx, "release_reservation", order_id)
    _ = reason
}
```

Returning from the Workflow while root cancellation is pending acknowledges it
with `CancelWorkflowExecution`; the return value is not completed successfully.

`worker.create` owns only language-side configuration and registries. The first
`worker.run` constructs one Core worker whose enabled task streams match the
registered Activities and Workflows. A worker is intentionally single-run;
call `worker.close` after `run` returns and keep its borrowed client open until
that close completes.

The script starts `temporal server start-dev --headless` only when a healthy
service is not already running on `localhost:7233`; Docker is not involved. It
also starts `examples/hello_workflow`; the Odin Client delivers an `approval`
signal while the fiber is blocked on a durable timer, cancelling that timer,
evicting the fiber, and requiring replay to reproduce the timer cancellation
and following Activity command. Two executions use Odin Client cancellation
to verify direct pending-timer and pending-Activity cleanup through Core. A
third resumes Workflow code, requests cancellation of its in-flight Activity,
schedules a detached cleanup Activity, survives eviction/replay, and only then
acknowledges Workflow cancellation. Temporal CLI
still starts/waits on these older test executions and reads their history. The
Activity case waits for an Odin heartbeat, observes Core's cancel task in the
in-flight context, and replies with a cancellation completion. A separate fan-out case proves two Odin
Activity callbacks overlap rather than merely being queued concurrently. The
suite also starts an unregistered Workflow type, verifies a retryable
`WorkflowTaskFailed` event and Core eviction, then proves that the same Odin
worker can complete a registered Workflow afterward. Its Activity cases include
a structured JSON object round trip between a Rust Workflow and a typed Odin
Activity, plus a retryable typed failure whose 10 ms `next_retry_delay` is
honored before the same Activity succeeds with `activity.attempt(ctx) == 2`.
Attempt 1 also records typed heartbeat progress; the result from attempt 2 is
accepted only if the server-redelivered heartbeat detail decodes in Odin.
It also uses the new Odin Client to start a typed Odin Workflow with a
structured object, schedules a typed Odin Activity, observes Core eviction and
replay, and receives the structured
Workflow result through `GetWorkflowExecutionHistory`. Temporal CLI is used
only afterward for an independent history assertion. That live path uses
explicit Activity timeouts, retry/cancellation policy, task queue, and Activity ID; the Activity
returns the server-delivered Activity/Workflow identity, task queue, namespace,
deadline presence, timeout, attempt, and effective Retry Policy as part of the
result.

Unit, codec, generated-protobuf, and lifecycle tests:

```bash
./scripts/test.sh
```

See [architecture](docs/architecture.md) for the researched boundary and
[development](docs/development.md) for build details and troubleshooting.

## Repository layout

```text
docs/                     architecture and development notes
native/                   pinned sdk-core/minicoro and native bridge sources
temporal/core/raw/        near-1:1 subset of the C ABI
temporal/core/            blocking ownership/lifecycle wrapper
temporal/core/proto/      generated Core Activity and Workflow schema types
temporal/protobuf/        protobuf wire runtime used by generated code
temporal/activity/        Activity context
temporal/worker/          Activity/Workflow registry and concurrent Core polling
temporal/codec/           owned/borrowed Payload model and json/plain codec
temporal/workflow/        explicit-state and stackful-fiber experiments
temporal/client/          owned Client and Workflow handle/control APIs
examples/core_connect/    Odin -> Core -> Temporal proof
examples/client_start/    Odin Client start-and-result proof
examples/client_control/  Odin Client signal/cancel/terminate proof
examples/hello_activity/  Odin hello Activity Worker
examples/hello_workflow/  sequential suspension/replay/eviction proof
tests/                    lifecycle, codec, protobuf, replay, and fiber tests
tests_pbt/                generated activation-order and replay properties
test-deps/pbt/            pinned, test-only property-testing submodule
```

## License

MIT. The `native/sdk-core` submodule is Temporal's separate upstream project
and retains its own license.

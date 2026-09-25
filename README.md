# Temporal SDK for Odin — experimental

An experimental Odin language SDK built on [Temporal SDK Core](https://github.com/temporalio/sdk-rust),
not a reimplementation of Temporal Server or its gRPC protocol.

Current status: Milestones 0–2 plus bounded explicit-state-machine and stackful
fiber Workflow experiments. The repository builds the official Rust Core C
bridge, connects to a Temporal CLI dev server, executes a remote Activity in
Odin, and proves multiple outstanding Activities, catchable Activity failure,
buffered Workflow signals, and a durable signal/timer race across fresh-stack
replay. The live Workflow starts a timer, lets an `approval` signal win, emits
`CancelTimer` alongside an Activity command, and reproduces both commands after
eviction. It also translates Workflow cancellation into deterministic cleanup
of a pending timer or Activity and `CancelWorkflowExecution`. A reusable worker
now registers both Activity and Workflow procedures, routes run IDs to the
correct fiber runtime, and polls both Core task streams concurrently. Its
bounded reusable Activity thread pool runs callbacks concurrently, exposes
cooperative cancellation and string heartbeat details through
`activity.Context`, and has live tests for overlap and heartbeat-driven
cancellation. It does **not**
provide user Workflow cancellation scopes/cleanup, signal handlers,
queries/updates, generic payloads, panic containment, or a stable public API.
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
activation races and verifies byte-identical replay completions.

Clone submodules and build the pinned Core bridge:

```bash
git submodule update --init --recursive
./scripts/build-core.sh
```

Build all three Odin examples in one pass with:

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

The combined example now uses the reusable registration boundary:

```odin
temporal_worker, _ := worker.create(
    &client,
    "odin-hello-workflow",
)
_ = worker.register_activity(&temporal_worker, "hello", hello)
_ = worker.register_workflow(&temporal_worker, "odin_hello_workflow", hello_workflow)
_, _ = worker.run(&temporal_worker)
```

`worker.create` owns only language-side configuration and registries. The first
`worker.run` constructs one Core worker whose enabled task streams match the
registered Activities and Workflows. A worker is intentionally single-run;
call `worker.close` after `run` returns and keep its borrowed client open until
that close completes.

The script starts `temporal server start-dev --headless` only when a healthy
service is not already running on `localhost:7233`; Docker is not involved. It
also starts `examples/hello_workflow` and uses Temporal CLI to verify the Odin
Workflow result after delivering an `approval` signal while the fiber is blocked
on a durable timer, cancelling that timer, evicting the fiber, and requiring
replay to reproduce the timer cancellation and following Activity command.
Two more executions use `temporal workflow cancel` to verify pending-timer and
pending-Activity cleanup through Core. The Activity case waits for an Odin
heartbeat, observes Core's cancel task in the in-flight context, and replies
with a cancellation completion. A separate fan-out case proves two Odin
Activity callbacks overlap rather than merely being queued concurrently.

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
temporal/codec/           isolated json/plain user payload codec
temporal/workflow/        explicit-state and stackful-fiber experiments
temporal/client/          planned public client package
examples/core_connect/    Odin -> Core -> Temporal proof
examples/hello_activity/  Odin hello Activity Worker
examples/hello_workflow/  sequential suspension/replay/eviction proof
tests/                    lifecycle, codec, protobuf, replay, and fiber tests
tests_pbt/                generated activation-order and replay properties
test-deps/pbt/            pinned, test-only property-testing submodule
```

## License

MIT. The `native/sdk-core` submodule is Temporal's separate upstream project
and retains its own license.

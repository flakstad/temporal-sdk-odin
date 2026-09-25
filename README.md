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
eviction. It does **not** provide general Workflow registration, signal
handlers, queries/updates, Workflow cancellation, Activity
concurrency/cancellation/heartbeats, or a stable public API. Do not use it in
production.

```text
Temporal CLI dev server
          ^ gRPC
          |
Temporal SDK Core
          ^ C ABI
          |
Odin raw bindings -> generated Core protobuf types -> Activity dispatch
                                            `-----> experimental Workflow fiber
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

Clone submodules and build the pinned Core bridge:

```bash
git submodule update --init --recursive
./scripts/build-core.sh
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

The script starts `temporal server start-dev --headless` only when a healthy
service is not already running on `localhost:7233`; Docker is not involved. It
also starts `examples/hello_workflow` and uses Temporal CLI to verify the Odin
Workflow result after delivering an `approval` signal while the fiber is blocked
on a durable timer, cancelling that timer, evicting the fiber, and requiring
replay to reproduce the timer cancellation and following Activity command.

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
temporal/worker/          synchronous Activity registry and run loop
temporal/codec/           isolated json/plain user payload codec
temporal/workflow/        explicit-state and stackful-fiber experiments
temporal/client/          planned public client package
examples/core_connect/    Odin -> Core -> Temporal proof
examples/hello_activity/  Odin hello Activity Worker
examples/hello_workflow/  sequential suspension/replay/eviction proof
tests/                    lifecycle, codec, protobuf, replay, and fiber tests
```

## License

MIT. The `native/sdk-core` submodule is Temporal's separate upstream project
and retains its own license.

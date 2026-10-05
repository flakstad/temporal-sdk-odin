# Temporal SDK for Odin

A Temporal language SDK for [Odin](https://odin-lang.org/), built on the
official [Temporal SDK Core](https://github.com/temporalio/sdk-rust).

The SDK provides native Odin APIs for Clients, Workers, Activities, and
deterministic Workflows. Temporal Server owns Workflow history and durability;
SDK Core handles the Temporal protocol and state machines; this repository
provides the Odin runtime and API.

> **Status:** Pre-release. The SDK is usable and extensively tested, but its
> public API and native packaging may still change.

## Features

- Client connection, TLS, API keys, metadata, and configuration profiles
- Workflow start, result, Signal, Query, Update, cancellation, and termination
- Activity and Workflow Workers with concurrency and graceful shutdown
- Typed JSON and raw Temporal Payload APIs
- Stackful, sequential-looking Workflows
- Activities, Local Activities, timers, Signals, Queries, Updates, Child
  Workflows, cancellation scopes, Continue-As-New, and versioning patches
- Deterministic Workflow time, randomness, UUIDs, replay, and eviction
- Activity heartbeats, retries, cancellation, async completion, and failures
- Interceptors, Workflow logging, metrics, headers, and recursive Failures
- Offline history replay
- Schedules, Nexus, Visibility, standalone Activities, and standalone Nexus
  operations

Supported hosts:

- macOS arm64 and x86_64
- Linux arm64 and x86_64 with glibc

## Requirements

- Odin `dev-2026-09:a2fb372b7` or compatible
- Rust 1.94 and Cargo
- `protoc`
- Clang or another compatible C toolchain
- [Temporal CLI](https://docs.temporal.io/cli) for live examples and tests

## Quick start

Clone the repository with its pinned dependencies and build SDK Core:

```bash
git clone --recurse-submodules https://github.com/flakstad/temporal-sdk-odin.git
cd temporal-sdk-odin
./scripts/build-core.sh
```

Run the smallest complete Client → Worker → Workflow → Activity path:

```bash
./scripts/core-sdk-integration-test.sh
```

The integration script starts `temporal server start-dev` when no server is
already available at `localhost:7233`.

Run the complete local test suite with:

```bash
./scripts/test.sh
```

## Using the SDK

Expose the checkout as an Odin collection and link the native Core library:

```bash
SDK_ROOT=/path/to/temporal-sdk-odin
LIB_DIR="$SDK_ROOT/native/target/release"

odin build ./app \
  -collection:sdk="$SDK_ROOT" \
  -extra-linker-flags:"-L$LIB_DIR -Wl,-rpath,$LIB_DIR"
```

Import packages from that collection:

```odin
import "core:fmt"
import activity "sdk:temporal/activity"
import workflow "sdk:temporal/workflow"

hello_activity :: proc(_: ^activity.Context, name: string) -> string {
	return fmt.aprintf("Hello %s", name)
}

hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
	return workflow.execute_activity(ctx, "hello", name)
}
```

Errors contain owned messages and must be released with the corresponding
package or top-level facade function. See
[`examples/sdk_hello_workflow`](examples/sdk_hello_workflow) for complete error
handling, Client and Worker lifecycle, registration, and start/result code.

## Examples

- [`examples/sdk_hello_workflow`](examples/sdk_hello_workflow) — minimal Odin
  Client, Worker, Workflow, and Activity
- [`applications/order_fulfillment`](applications/order_fulfillment) —
  multi-process application with Activities, Signal, Query, durable timer, and
  Worker replacement
- [`applications/kvist_hello_workflow`](applications/kvist_hello_workflow) —
  complete [Kvist](https://kvist-lang.org/) consumer
- [`applications/kvist_replay_workflow`](applications/kvist_replay_workflow) —
  Kvist Workflow replay after complete Worker-process loss

Run the reference application acceptance test with:

```bash
./scripts/order-fulfillment-integration-test.sh
```

## Native libraries

SDK Core is linked dynamically. The Workflow fiber shim is linked statically.
Build host-specific archives with:

```bash
./scripts/package-native.sh
```

See [native distribution](docs/native-distribution.md) for archive contents,
verification, installation, and runtime linking.

## Documentation

- [Architecture](docs/architecture.md)
- [Development and testing](docs/development.md)
- [Native distribution](docs/native-distribution.md)

## License

MIT. Bundled upstream projects retain their own licenses.

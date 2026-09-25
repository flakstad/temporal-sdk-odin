# Development

## Toolchain and pinned dependency

The repository pins `temporalio/sdk-rust` as the `native/sdk-core` submodule at
commit `399cb7adb40f45eefbef90149e212743e474142c`. That upstream release commit
contains `sdk-core` crate 0.9.0 and client/common crates 1.0.0, and is pinned by
`sdk-dotnet` main as inspected on 2026-09-25.

`native/Cargo.toml` is a wrapper workspace. SDK Core deliberately does not
commit a workspace lockfile, while a language SDK needs reproducible native
artifacts. The wrapper therefore owns `native/Cargo.lock`, following the same
pattern as `sdk-dotnet/src/Temporalio/Bridge`.

When changing the Core pin:

1. Compare `native/sdk-core/Cargo.toml` with `native/Cargo.toml` and synchronize
   inherited workspace dependency and lint tables.
2. Regenerate `native/Cargo.lock` deliberately.
3. Diff the generated C header against `temporal/core/raw/raw.odin`.
4. Update ABI size/offset assertions and run all tests on every supported target.
5. Review Core protobuf schema changes before regenerating language types.

## Build

```bash
./scripts/build-core.sh
```

This builds the upstream `temporalio-sdk-core-c-bridge` Rust crate as a shared
library:

- macOS: `native/target/release/libtemporalio_sdk_core_c_bridge.dylib`
- Linux: `native/target/release/libtemporalio_sdk_core_c_bridge.so`

On macOS the script rewrites the dylib ID to an `@rpath` install name. Odin is
then linked with an explicit library search path and runtime path by the run/test
scripts. The current upstream crate declares only `cdylib`; static linking is
not an upstream-supported output of this bridge.

The first native build is substantial because it compiles Core and its Rust
dependency graph. Incremental builds are fast.

## Tests

Fast tests initialize and close a real Core runtime but do not require a server:

```bash
./scripts/test.sh
```

The raw package also contains compile-time ABI assertions for the structs used
by the spike. They are checked against the pinned generated header's 64-bit C
layout.

The integration test uses Temporal CLI directly—no Docker, PostgreSQL, or
Elasticsearch:

```bash
./scripts/integration-test.sh
```

It checks for a healthy service on `localhost:7233`. If none exists, it starts:

```bash
temporal server start-dev --headless --log-level error
```

It first builds and runs `examples/core_connect`, which performs:

```text
runtime_new -> client_connect -> GetSystemInfo RPC -> client_free -> runtime_free
```

The request protobuf is empty by definition. The response is intentionally kept
opaque; receiving and freeing it proves the network and ABI path.

The same script then runs `examples/hello_activity` twice. A small workflow-only
driver under `native/activity-test-driver` uses the pinned upstream Rust SDK to
schedule a real Activity because Temporal CLI Server 1.29.1 does not implement
the newer standalone-Activity start RPC. The first run verifies `"Odin"` ->
`"Hello Odin"`; the second schedules an unknown type and verifies that Odin's
application failure reaches the Workflow. The driver is test infrastructure,
not part of the distributed Odin SDK and not an Odin workflow runtime.

## Manual run

```bash
temporal server start-dev
./scripts/run-core-connect.sh
./scripts/run-hello-activity.sh
```

`run-hello-activity.sh` polls task queue `odin-hello-activity` and completes one
task before shutting down. Run `./scripts/integration-test.sh` instead when you
want the repository to schedule that task automatically.

The default endpoint is `http://localhost:7233` inside the Core connection
options. The wrapper accepts another URL, but TLS, API keys, metadata, and
configuration profiles are not exposed yet.

## Troubleshooting

- `library not found for -ltemporalio_sdk_core_c_bridge`: run
  `./scripts/build-core.sh` and use the provided run/test scripts.
- `Temporal connection failed`: confirm `temporal operator cluster health`
  succeeds and port 7233 is not occupied by another service.
- A worker hangs in shutdown: Core shutdown requires a final poll to observe
  its shutdown result before `finalize_shutdown`; preserve that lifecycle order.
- `StartActivityExecution` is unimplemented: this is expected from CLI Server
  1.29.1 even though the pinned client contains the newer API. Use the included
  workflow-only integration driver.
- Rust manifest inheritance errors after a Core bump: synchronize the workspace
  tables in `native/Cargo.toml` before regenerating the lockfile.
- An ABI assertion fails: do not weaken it. Compare the upstream generated C
  header and update the raw binding field-for-field.

## Current platform evidence

Milestones 1 and 2 were built and run end-to-end on macOS arm64. Linux
source/build paths are present but were not executed in this environment.
Cross-platform release packaging is intentionally deferred.

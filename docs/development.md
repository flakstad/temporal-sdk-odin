# Development

## Toolchain and pinned dependency

The repository pins `temporalio/sdk-rust` as the `native/sdk-core` submodule at
commit `399cb7adb40f45eefbef90149e212743e474142c`. That upstream release commit
contains `sdk-core` crate 0.9.0 and client/common crates 1.0.0, and is pinned by
`sdk-dotnet` main as inspected on 2026-09-25.

Property tests pin `flakstad/pbt` at
`3b465330bd3a11c45990ef2d7f44f1cf25a18145` as the `test-deps/pbt`
submodule. It is not linked into SDK or application artifacts.

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

The same command runs `scripts/build-fiber.sh`, which compiles the pinned
single-header minicoro dependency and the project-owned C shim into
`native/target/release/libtemporal_odin_fiber.a`. The shim is statically linked
into the Odin executable; it is separate from Core's dynamic library. Building
it directly requires `cc` and `ar`, normally supplied by Clang/Xcode tools on
macOS and the standard compiler toolchain on Linux.

The first native build is substantial because it compiles Core and its Rust
dependency graph. Incremental builds are fast.

## Protobuf generation

The C bridge exchanges Core messages as protobuf bytes. The repository-owned
generator reads a descriptor set produced from the exact schemas in the pinned
`native/sdk-core` submodule:

```bash
./scripts/generate-protos.sh
```

This currently generates the transitive type graph rooted at the Core Activity
task/completion and Workflow activation/completion messages into
`temporal/core/proto/generated.odin`. The generated file is committed. After a
Core pin or root-list change, regenerate it and review the diff; do not edit it
by hand. CI/local tests use:

```bash
./scripts/generate-protos.sh --check
```

The command needs `protoc` and Cargo, uses the pinned schemas plus the local
`protoc` well-known-type include directory, and has no separate runtime
dependency. Schema-independent wire operations live in `temporal/protobuf`.
Decoded nested objects and repeated fields use `context.allocator`; callers
must supply an arena or another allocator with a lifetime at least as long as
all decoded views. String and byte values borrow the input buffer.

## Tests

Fast tests check generated-code freshness, initialize and close a real Core
runtime, and exercise fiber suspension, destruction, fresh-stack replay, and
resumption without requiring a server:

```bash
./scripts/test.sh
```

The script runs the repository tests with Odin vetting, strict style, and
warnings promoted to errors. It then runs a separate property suite from
`tests_pbt` against the pinned `test-deps/pbt` submodule. That suite generates
500 signal/timer/cancellation activation batches across nine ordering classes,
forces eviction, reconstructs a fresh fiber, and requires the decisive
`WorkflowActivationCompletion` bytes to match replay exactly. PBT is a
test-only dependency. Its own package tests currently contain an upstream
unused-variable warning, so dependency warnings are not promoted to errors in
that second compilation; SDK-owned tests remain strict.

The tested Odin development compiler has intermittently asserted internally
while parallel type checking instantiated the standard-library
`queue.Queue(Task)` and `pbt.State_Model` types. Eight consecutive
single-threaded PBT compilations
passed after reproducing the parallel failure, so repository scripts default
to `-thread-count:1`. This affects compiler parallelism only, not the worker or
test runtime. Set `ODIN_THREAD_COUNT` to experiment with a newer compiler.

If the property dependency is absent, initialize it with:

```bash
git submodule update --init --recursive
```

The raw package also contains compile-time ABI assertions for the structs used
by the spike. They are checked against the pinned generated header's 64-bit C
layout.

## Worker registration and polling

`temporal/worker` owns the reusable language-side worker boundary. Activities
and Workflows must be registered before `worker.run` starts. Initialization
activations are routed by Workflow type; subsequent activations are routed by
run ID until Core sends `RemoveFromCache`. Eviction destroys the corresponding
fiber and removes that route.

`worker.create` clones the namespace and task-queue strings and initializes the
language registries, but deliberately does not create a Core worker. On the
first `run`, the registration set selects Core's `enable_workflows` and
`enable_remote_activities` task types; `disable_activity_polling` can suppress
the Activity type for that bounded run. This avoids polling unused streams and
removes task-type switches from the public construction API. The worker borrows
the client passed to `create`, so the stable client value, its connection, and
the Core runtime must outlive `worker.close`.

The C wrapper currently presents blocking poll calls. When both task types are
registered, `worker.run` creates one Workflow poll thread and one Activity poll
thread. Only the Workflow thread reads or mutates Workflow runtimes, so a fiber
never migrates between threads. The Activity poll thread instead creates one
heap-stable execution record per start task and submits it to a reusable Odin
standard-library thread pool. The pool and Core's fixed-size Activity slot
supplier both use `max_concurrent_activities` (10 by default), while the poll
thread remains free to deliver task-token-matched cancel tasks. On exit, the
pool finishes queued/running callbacks before its execution records are freed.
`worker.request_shutdown` is safe to call from a coordinating thread;
`worker.close` must only be called after `run` returns.

Activity cancellation is cooperative. `activity.is_cancellation_requested`
and `activity.cancellation_reason` expose the state Core delivered; returning
from the callback after a request makes the worker send a cancellation result
with `CanceledFailureInfo`. `activity.heartbeat(ctx, details)` sends one
`json/plain` string payload through
`temporal_core_worker_record_activity_heartbeat`. Core owns heartbeat
throttling, and any server cancellation response returns later through the
normal Activity poll stream.

`Run_Options.max_activity_tasks` and `max_workflow_executions` are bounded
test/example controls. Zero means unbounded. Normal workers call `run` without
bounds and arrange for another thread or process-lifecycle handler to call
`request_shutdown`.

The lifecycle is `Configuring -> Running -> Stopped -> Closed`. Registration is
accepted only while configuring, and a worker cannot be run twice. `close` is
also valid before `run`, in which case it only releases language-owned state;
it is idempotent after closing. If Core validation fails, `run` closes the
newly-created Core worker and leaves the language worker configurable so the
failure does not leak a native handle.

Workflow activation failures are distinct from worker-fatal failures. An
unknown Workflow type produces a failed `WorkflowActivationCompletion` with
`WorkflowWorkerUnhandledFailure`; Core reports a retryable Workflow Task
failure and evicts the rejected run. The language worker retains only that run
ID until `RemoveFromCache`, answers the eviction with an empty successful
completion, and keeps polling. Errors that cannot produce a valid completion—
such as malformed protobuf or an impossible run route—still stop `run`.
`Run_Stats.workflow_activation_failures` counts the nonfatal failed
completions.

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
not part of the distributed Odin SDK and not an Odin workflow runtime. This is
also the protobuf interoperability test: the Rust/Core side uses `prost` to
encode Activity Tasks and decode Odin's success/failure completions.

The script then schedules two Activities in one upstream-driver Workflow. Each
callback waits at an atomic barrier, so the expected results are possible only
if both Odin callbacks overlap. `integration-test.sh` builds all examples once,
then the run scripts `exec` those binaries without rebuilding. Standalone use
still builds by default. Keeping the process PID identical to the worker PID
lets integration cleanup avoid orphaning a task-queue poller.

Before the registered Workflow scenario, the live suite starts
`odin_missing_workflow` on the same queue. It requires both an
`EVENT_TYPE_WORKFLOW_TASK_FAILED` event in server history and a following Core
cache eviction, confirms that the worker process is still alive, terminates the
intentionally stuck execution, and then runs the registered Workflow. Temporal
[documents unknown Workflow Tasks](https://docs.temporal.io/task-queue) as
retryable Task failures rather than Workflow Execution failures, so the
explicit termination is test cleanup, not SDK behavior.

`./scripts/test.sh` uses the pinned `test-deps/pbt` submodule by default. During
local PBT library development, run `PBT_ROOT=~/Projects/pbt ./scripts/test.sh`
to compile the SDK properties against that checkout without changing the
reproducible repository pin. The Activity property suite generates in-flight
set sizes, target task tokens, cancel reasons, and heartbeat counts, then checks
that cancellation never leaks across tokens and no heartbeat is transported
after cancellation becomes visible.

Finally, the script starts `examples/hello_workflow` and invokes it directly
with `temporal workflow execute` in the background. After the Workflow has
started a durable timer and is visibly waiting for either that timer or an
`approval` signal, the script sends the signal through Temporal CLI. The signal
wins, so the Odin runtime emits `CancelTimer` and schedules an Activity in the
same completion. The reusable worker's Activity poll thread completes that
Activity while its Workflow poll thread handles Core-directed eviction and
fresh-stack replay. The final completion eviction is required before the test
succeeds. It then runs two cancellation cases through
`temporal workflow cancel`: a Workflow blocked on a timer must emit
`CancelTimer` plus `CancelWorkflowExecution`, and a Workflow blocked on an
Activity must emit `RequestCancelActivity` plus `CancelWorkflowExecution`.
For the Activity case, polling remains enabled: the callback first heartbeats,
the test cancels the Workflow, Core emits an Activity cancel task, and the Odin
callback returns a valid canceled completion. Core then delivers the terminal
Activity resolution in one final activation before eviction; Odin acknowledges
it with an empty successful completion and does not repeat the terminal command.

## Manual run

```bash
temporal server start-dev
./scripts/run-core-connect.sh
./scripts/run-hello-activity.sh
```

`run-hello-activity.sh` polls task queue `odin-hello-activity` and completes one
task before shutting down. Run `./scripts/integration-test.sh` instead when you
want the repository to schedule that task automatically.

For the explicit Workflow experiment, run the worker in one terminal:

```bash
./scripts/run-hello-workflow.sh
```

Then execute the Workflow from another:

```bash
temporal workflow execute \
  --workflow-id odin-workflow-manual \
  --type odin_hello_workflow \
  --task-queue odin-hello-workflow \
  --input '"Odin"'
```

This example is intentionally a signal/timer-race replay experiment, not a
stable or general Workflow API.

To manually exercise the runtime-owned cancellation policy, start the example
with `TEMPORAL_ODIN_SCENARIO=cancel-timer` or
`TEMPORAL_ODIN_SCENARIO=cancel-activity`, execute it as above (use input
`"cancel-activity"` for the latter), and then run:

```bash
temporal workflow cancel --workflow-id odin-workflow-manual
```

This narrow policy cancels pending Core operations and closes the Workflow. It
does not yet resume the user fiber to run cleanup code.

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

Milestones 1–3 and the stackful-fiber experiment were built and run end-to-end
on macOS arm64. Linux source/build paths and minicoro assembly targets are
present but were not executed in this environment. Cross-platform release
packaging is intentionally deferred.

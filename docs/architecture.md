# Architecture: Temporal SDK for Odin

Status: Milestone 0 research, Milestone 1 bridge spike, a narrow Milestone 2
Activity Worker slice, schema-derived Core protobuf bindings, a bounded
Milestone 3 explicit-state-machine Workflow spike, and stackful-fiber replay
spikes covering serial operations, two outstanding Activities, handled Activity
failure, buffered Workflow signals, a durable signal/timer race, and
runtime-owned Workflow cancellation, plus a reusable Activity/Workflow worker
registry and poll loop, 2026-09-25.

This document separates observed upstream behavior from project decisions. The
primary upstream snapshot used for implementation is
`temporalio/sdk-rust@399cb7adb40f45eefbef90149e212743e474142c`, the upstream
release commit pinned by the inspected `sdk-dotnet` main branch (`sdk-core`
crate 0.9.0; client/common crates 1.0.0). We also inspected
`sdk-rust` main at `0eb03f213079e539c18fb738c32a70bc191aadf4` and
`sdk-dotnet` at `f8b622e45ec553cbfeee8c508c4a7a989a1292c6` to detect newer
direction. The generated C header and C-bridge Cargo manifest were byte-for-byte
unchanged between the selected pin and that newer sdk-rust main snapshot.

## Decision

Use Temporal SDK Core through its existing C bridge as the foundation. Do not
implement Temporal Server's gRPC protocol in Odin.

The evidence supports the original architecture:

```text
Temporal Service
      ^ gRPC, retries, long polls, history
      |
Temporal SDK Core (Rust)
      ^ C ABI + protobuf byte buffers
      |
Temporal Odin SDK
      +-- typed client API
      +-- worker polling and lifecycle
      +-- activity dispatch
      +-- workflow language runtime
              ^
              |
          Odin user code
```

This is a qualified decision, not a claim that the C ABI is a stable public
standard. The C bridge is an unpublished (`publish = false`) Rust crate at
version `0.1.0`, and Temporal does not publish standalone C-bridge release
artifacts. We must pin, build, test, and package it as part of this SDK.

## What upstream says a language SDK does

Core's own [architecture document](https://github.com/temporalio/sdk-rust/blob/399cb7adb40f45eefbef90149e212743e474142c/ARCHITECTURE.md)
defines a Core-based SDK as two layers. Core communicates with the Temporal
Service, polls tasks, processes history, advances Temporal state machines, and
accepts completions. The language layer polls Core, invokes user code, returns
results/commands, and chooses language-appropriate execution and concurrency.

The boundary is not a server `WorkflowTask`. Core consumes server history and
exposes a language-facing `WorkflowActivation`. The language returns a
`WorkflowActivationCompletion`; Core turns its commands into the proper server
interaction and checks them against its state machines during replay.

```text
server Workflow Task + history
             |
             v
 Core history/state machines
             |
             v
 WorkflowActivation { run_id, is_replaying, jobs... }
             |
             v
 Odin workflow runtime and user code
             |
             v
 WorkflowActivationCompletion { run_id, commands... }
             |
             v
 Core state machines -> server commands / WFT completion
```

Core is therefore much more than a gRPC client, but less than a complete Odin
SDK. It does not call Odin procedures, choose an Odin payload codec, maintain an
Odin workflow call stack, or provide an Odin deterministic scheduler.

## The C bridge that exists today

The bridge source is
[`crates/sdk-core-c-bridge`](https://github.com/temporalio/sdk-rust/tree/399cb7adb40f45eefbef90149e212743e474142c/crates/sdk-core-c-bridge).
Its generated
[`temporal-sdk-core-c-bridge.h`](https://github.com/temporalio/sdk-rust/blob/399cb7adb40f45eefbef90149e212743e474142c/crates/sdk-core-c-bridge/include/temporal-sdk-core-c-bridge.h)
is the ABI source of truth.

The bridge exposes opaque handles plus C-layout options and callback APIs:

| Concern | C representation | Payload across boundary |
| --- | --- | --- |
| Runtime | `TemporalCoreRuntime*` | C option structs |
| Client connection | `TemporalCoreConnection*` | C connection option structs |
| Arbitrary client RPC | `TemporalCoreRpcCallOptions` | binary request/response protobuf |
| Worker | `TemporalCoreWorker*` | C worker option structs |
| Activity poll | async callback | binary `coresdk.activity_task.ActivityTask` |
| Activity completion | async callback | binary `coresdk.ActivityTaskCompletion` |
| Workflow poll | async callback | binary `coresdk.workflow_activation.WorkflowActivation` |
| Workflow completion | async callback | binary `coresdk.workflow_completion.WorkflowActivationCompletion` |
| Errors | `TemporalCoreByteArray*` | UTF-8 message, with gRPC status/details where applicable |

The asynchronous functions return through callbacks invoked by Core-managed
threads. Callers cannot assume a callback thread, and option buffers/user data
must remain alive until the callback. This affects every Odin wrapper: a stack
frame can be used only when the calling thread blocks until callback completion,
as the current spike does. A future async API needs heap-owned operation state
or a stable handle table.

### Client

`temporal_core_client_connect` creates the connection. Connecting normally
performs a `GetSystemInfo` exchange, so successful connection already exercises
the service. `temporal_core_client_rpc_call` additionally exposes Workflow,
Operator, Cloud, Test, and Health services by service enum and RPC name. The
caller supplies a serialized request message and decodes the returned response.

Core provides transport, TLS, metadata, retry/keepalive, API key, DNS balancing,
compression, and cancellation mechanics. The Odin layer must still provide
typed client methods, generate request IDs where required, encode the Temporal
API protobuf messages, convert user payloads, and map errors into an Odin API.

### Worker and activity tasks

`temporal_core_worker_new` takes a connection and a C `WorkerOptions` struct.
Separate poll calls exist for workflow activations, activity tasks, and Nexus
tasks. The language layer controls poll concurrency and dispatch.

An [`ActivityTask`](https://github.com/temporalio/sdk-rust/blob/399cb7adb40f45eefbef90149e212743e474142c/crates/protos/protos/local/temporal/sdk/core/activity_task/activity_task.proto)
contains a task token and a `oneof`: start or cancel. A start includes activity
type, workflow identity, inputs/headers as Temporal `Payload`s, attempt and
timeout data, heartbeat details, retry policy, priority, and local/remote state.
The language layer looks up the registered Odin procedure and executes it.

The result is an
[`ActivityTaskCompletion`](https://github.com/temporalio/sdk-rust/blob/399cb7adb40f45eefbef90149e212743e474142c/crates/protos/protos/local/temporal/sdk/core/core_interface.proto)
with the original task token and an `ActivityExecutionResult`: completed,
failed, cancelled, or asynchronously completing. Heartbeats are a separate
protobuf call. Core sends the correct server RPC and manages server-facing
activity mechanics; Odin owns procedure invocation, cancellation delivery,
heartbeat API surface, payload conversion, failures, and activity concurrency.

### Workflow activations and completions

[`WorkflowActivation`](https://github.com/temporalio/sdk-rust/blob/399cb7adb40f45eefbef90149e212743e474142c/crates/protos/protos/local/temporal/sdk/core/workflow_activation/workflow_activation.proto)
has a run ID, workflow time, replay flag, deterministic history length, and an
ordered list of jobs. Jobs include initialization, timer firing, activity
resolution, signal/query/update delivery, child/external workflow resolution,
random seed changes, patch notification, Nexus resolution, and cache eviction.

The language runtime consumes all jobs for an activation and returns a
[`WorkflowActivationCompletion`](https://github.com/temporalio/sdk-rust/blob/399cb7adb40f45eefbef90149e212743e474142c/crates/protos/protos/local/temporal/sdk/core/workflow_completion/workflow_completion.proto).
A success carries ordered `WorkflowCommand`s—start/cancel timer, schedule/cancel
activity, complete/fail/cancel workflow, child/external operations, queries,
updates, and more. A failure carries a Temporal `Failure`.

An eviction activation contains only `remove_from_cache`; language workflow
code must not be invoked and an empty successful completion is expected. The
language layer must cache live workflow runtimes by run ID, respect activation
ordering, and discard them on eviction.

## Exact Core/Odin responsibility boundary

| SDK Core owns | Odin SDK owns |
| --- | --- |
| gRPC channel and supported service stubs | Idiomatic client methods and options |
| TLS, metadata, API keys, retry and long polling | Odin error/result model |
| Worker task polling and server responses | Worker run loop, registration, dispatch and concurrency |
| History ingestion and Temporal state machines | Odin workflow instance/cache keyed by run ID |
| Converting history progress into activation jobs | Applying jobs to Odin futures/fibers/state |
| Matching commands against history during replay | Deterministic execution order and command sequencing |
| Sticky/non-sticky server mechanics and eviction requests | Dropping cached language state on eviction |
| Activity task delivery and completion transport | Calling activities and converting arguments/results/failures |
| Workflow time/random seeds/patch information delivery | Temporal-aware clock/random/patch APIs |
| Protocol-level heartbeat transport | Activity context and user-facing heartbeat/cancellation API |
| Service metrics hooks and internal telemetry | Odin logging/tracing integration and public configuration |

The Temporal Service owns durable history and workflow state. Neither Core nor
the Odin process serializes and stores an Odin stack in the service. A live
language workflow stack/coroutine may remain in memory as a sticky-cache
optimization. After eviction or worker loss, Odin must start the workflow code
again and deterministically replay activations until it reaches the same logical
suspension point. Core supplies replay jobs and validates command evolution; the
Odin runtime must make resuming sequential-looking code and scheduling its
continuations deterministic.

This finding narrows the future coroutine question: a fiber can preserve the
stack only between activations while cached. It cannot be the durability format.
The runtime must be able to construct a fresh fiber and drive it from the
beginning during replay.

## Serialization and protobuf consequence

The C boundary uses protobuf wire bytes, not JSON, for client RPCs and worker
messages. Core uses Rust `prost` to encode poll results and decode completions.
The relevant Core-local schemas import Temporal API schemas and Google well-known
types.

Odin therefore needs protobuf capability for complete workers. Milestone 1
avoids pretending otherwise: `GetSystemInfoRequest` is an empty message, and
the spike treats its response as opaque bytes. The generated Activity slice
now includes representations for:

- Core `ActivityTask`, `ActivityTaskCompletion`, and `ActivityExecutionResult`;
- Temporal `Payload`, `Failure`, `WorkflowExecution`, `RetryPolicy`, and
  `Priority`;
- Google `Timestamp` and `Duration`;
- all transitively used enums and `oneof`s.

User data is a separate codec layer inside `temporal.api.common.v1.Payload`.
JSON is acceptable as the first user codec, expressed through payload metadata
and data, but it does not replace the protobuf envelope. The codec belongs in
`temporal/codec`, not the Core binding.

### Odin protobuf survey and decision

Three projects were examined at their current revisions on 2026-09-25:

- [`lordhippo/odin-protobuf`](https://github.com/lordhippo/odin-protobuf) and
  its [`odin-protoc-plugin`](https://github.com/lordhippo/odin-protoc-plugin)
  (`ae5b9b5` and `cf37925`) are active and allocator-aware, but currently
  document missing `oneof` support, deterministic map serialization, field
  presence, and unknown-field retention. The plugin also lists dependency
  handling as incomplete. We built the plugin and ran it against the pinned
  Core descriptors rather than relying only on that documentation. Imported
  message names were emitted as invalid identifiers such as
  `_WorkflowExecution`; imported Google types were truncated to `amp` and `on`;
  and protobuf `oneof`s became raw Odin unions without a discriminator. It is
  therefore not usable unchanged for these schemas.
- [`lukasjarosch/protoc-gen-godin`](https://github.com/lukasjarosch/protoc-gen-godin)
  (`cec442a`, last changed in 2019) is explicitly work in progress and is not a
  sufficient runtime/codegen foundation.

The project now owns a small descriptor-driven generator in
`native/odin-proto-gen`. `protoc` resolves the pinned Core, Temporal API, and
Google imports into a `FileDescriptorSet`; the generator walks the transitive
graph from an explicit root-message list and emits one Odin package. Fully
qualified names are flattened predictably, protobuf `oneof`s become tagged Odin
unions, message presence remains explicit through pointers, and map entries are
represented as their generated repeated entry messages. Generated decoders use
`context.allocator`, borrow strings/bytes from the input buffer, skip valid
unknown fields, and reject malformed wire data. Encoders produce standard
protobuf wire data and omit default-valued singular fields.

The current roots are `coresdk.activity_task.ActivityTask`,
`coresdk.ActivityTaskCompletion`,
`coresdk.workflow_activation.WorkflowActivation`, and
`coresdk.workflow_completion.WorkflowActivationCompletion`. Their reachable
type graph covers the Activity slice and the complete Core-facing Workflow
activation/command boundary. The generated file is committed and
`./scripts/generate-protos.sh --check` makes schema drift visible in normal
tests. The live integration test is the interoperability oracle: Core encodes
poll results with Rust `prost`, Odin decodes them, Odin encodes completions,
and Core decodes those completions with `prost` before talking to the server.
Activity success/failure and Workflow schedule/complete paths all pass against
the CLI dev server.

This is intentionally a Core-schema tool, not a claimed general-purpose Odin
protobuf library. It does not retain unknown fields, emit a native Odin map, or
promise support for schema features not reachable from the selected roots.
Those limitations are acceptable at this boundary because Odin does not
decode-and-reencode the polled task, but they must be revisited when adding new
root messages. A custom Rust message shim remains a fallback; it would add a
second project-owned ABI and was not needed.

## How .NET binds Core

The .NET SDK is the closest precedent because it directly consumes this C ABI.
The inspected implementation:

1. Pins `sdk-rust` as a submodule under `src/Temporalio/Bridge/sdk-core`.
2. Uses a wrapper Cargo workspace and committed lockfile for reproducible native
   builds, because sdk-rust intentionally omits its own lockfile.
3. Generates C# P/Invoke declarations from the C header with ClangSharp.
4. Generates C# protobuf types and uses Google.Protobuf for Core-local and
   Temporal API messages.
5. Wraps opaque pointers in `SafeHandle` types.
6. Uses a scoped owner to pin options, byte references, delegates, cancellation
   tokens, and handles until asynchronous callbacks complete.
7. Converts callbacks to `TaskCompletionSource` completions and immediately
   wraps returned byte arrays in a disposer that calls Core's free function.
8. Polls workflow/activity tasks as protobufs and serializes protobuf
   completions back to Core.

Relevant source: [.NET Runtime](https://github.com/temporalio/sdk-dotnet/blob/f8b622e45ec553cbfeee8c508c4a7a989a1292c6/src/Temporalio/Bridge/Runtime.cs),
[Client](https://github.com/temporalio/sdk-dotnet/blob/f8b622e45ec553cbfeee8c508c4a7a989a1292c6/src/Temporalio/Bridge/Client.cs),
[Worker](https://github.com/temporalio/sdk-dotnet/blob/f8b622e45ec553cbfeee8c508c4a7a989a1292c6/src/Temporalio/Bridge/Worker.cs), and
[build notes](https://github.com/temporalio/sdk-dotnet/tree/f8b622e45ec553cbfeee8c508c4a7a989a1292c6/src/Temporalio/Bridge).

Python, TypeScript, and Ruby validate the overall Core/lang split but use
language-specific Rust bridges (PyO3, Neon/Node-API, and Ruby bindings) rather
than this generic C header directly. That does not invalidate C for Odin; it
shows that an extra Rust adapter is normal when a language runtime needs it.
Odin's strong C interop makes the .NET route the smaller initial dependency.

## Memory ownership and lifecycle

The following rules come from header comments and bridge source and are treated
as ABI requirements:

- `TemporalCoreByteArrayRef` is borrowed. Odin owns the referenced memory and
  must keep it stable for the duration documented by the receiving call. For
  asynchronous client calls, options, their referenced bytes, callback, and
  user data live until the callback.
- `TemporalCoreByteArray*` returned in success/failure callbacks is Core-owned
  but caller-managed. Copy or consume it, then call
  `temporal_core_byte_array_free(runtime, bytes)` exactly once. Do not call
  Odin `free` on it.
- Runtime creation failure is special: `RuntimeOrFail` still contains a runtime
  used to free the error. Free the error with that runtime, then free the
  runtime; never otherwise use the failed runtime.
- Free client handles with `temporal_core_client_free`, worker handles with
  `temporal_core_worker_free`, and the runtime with
  `temporal_core_runtime_free`.
- Normal teardown order is: initiate worker shutdown, drain/finish poll loops,
  finalize shutdown, free worker, free client, then free runtime. No callback or
  returned byte array may outlive the runtime.
- Worker poll callbacks return both pointers null for poll shutdown. Otherwise
  any non-null success/failure byte array must be freed.
- Forwarded-log references are valid only inside the log callback. Similar
  callback-borrowed data must be copied before returning when it must persist.
- Callback thread identity is unspecified. Odin callbacks must not assume they
  run on the main thread or use an uninitialized thread-local context.

The current Core wrapper blocks the caller on an Odin one-shot event. This keeps
stack-backed options and callback state alive, copies all returned bytes, and
frees them before returning. It deliberately exposes no general asynchronous
API yet. Generated protobuf decoding allocates nested messages and repeated
fields in a per-task virtual-memory arena while string and byte fields remain
views into the copied poll buffer. Both therefore stay alive through Activity
dispatch and completion encoding, then the arena and poll buffer are released.
Each Activity invocation has a separate temporary arena; allocations made by
the Activity, including its returned string, are released after result
encoding.

Shutdown exposed an important lifecycle detail in practice: after
`temporal_core_worker_initiate_shutdown`, an active poll must observe the
both-null shutdown callback before `temporal_core_worker_finalize_shutdown`.
The synchronous wrapper therefore performs one final Activity poll before
finalization.

## Native build and distribution

Upstream's bridge declares `crate-type = ["cdylib"]` and produces:

- `libtemporalio_sdk_core_c_bridge.dylib` on macOS;
- `libtemporalio_sdk_core_c_bridge.so` on Linux;
- `temporalio_sdk_core_c_bridge.dll` on Windows.

The requested first distribution matrix—macOS arm64/x86_64 and Linux
x86_64/arm64—is consistent with platforms already bundled by .NET. Its current
native packages additionally cover Linux musl and Windows variants. This does
not mean upstream publishes a reusable generic C binary; it builds and bundles
the bridge inside its NuGet artifacts.

For Odin, the near-term distribution plan should be:

1. Pin one tested sdk-rust commit and the wrapper `Cargo.lock`.
2. Build four CI artifacts from source with the pinned Rust toolchain.
3. Give each artifact a platform/architecture manifest and checksum.
4. Bundle or fetch exactly one matching dynamic library during SDK setup.
5. Set `@rpath` on macOS and `$ORIGIN`/rpath policy on Linux; verify with
   `otool -L`/`readelf -d`.
6. Run ABI, init/shutdown, and CLI-devserver smoke tests on every artifact.

The verified macOS arm64 release dylib is about 32 MiB and depends only on
system frameworks/libraries (IOKit, CoreFoundation, Security, iconv, and
libSystem). Linux still needs target-specific validation, including glibc vs
musl policy and certificate-store behavior. Static linking would require an
upstream/project modification and is not part of this spike.

The Workflow fiber experiment adds a separate, project-owned static archive,
`libtemporal_odin_fiber.a`. It contains a six-operation C shim over
[`minicoro`](https://github.com/edubart/minicoro) pinned at
`02dad0f8b7cbb12fe6e216ae7a76db15ca55cd7b`. The shim deliberately keeps
`mco_coro` and `mco_desc` private so their C layout does not become part of the
Odin ABI. `scripts/build-fiber.sh` compiles the single-header implementation
with the platform C compiler and archives it with `ar`; the Core bridge remains
a separate dynamic library. Minicoro is available under Unlicense or MIT No
Attribution terms.

## Milestones 1–3 implementation and evidence

Raw Odin declarations live in `temporal/core/raw` and mirror only the structs,
enums, callbacks, and symbols required for the spike. `temporal/core` adds a
small ownership wrapper; this is intentionally not the future public client
API.

Current symbol dependency:

```text
temporal_core_runtime_new
temporal_core_runtime_free
temporal_core_byte_array_free
temporal_core_client_connect
temporal_core_client_free
temporal_core_client_rpc_call
temporal_core_worker_new
temporal_core_worker_validate
temporal_core_worker_poll_activity_task
temporal_core_worker_complete_activity_task
temporal_core_worker_poll_workflow_activation
temporal_core_worker_complete_workflow_activation
temporal_core_worker_request_workflow_eviction
temporal_core_worker_initiate_shutdown
temporal_core_worker_finalize_shutdown
temporal_core_worker_free
```

The integration path actually executed on macOS arm64:

```text
Odin runtime_create
  -> temporal_core_runtime_new
Odin client_connect(http://localhost:7233)
  -> temporal_core_client_connect
  -> CLI dev server
Odin client_get_system_info
  -> temporal_core_client_rpc_call(Workflow, "GetSystemInfo", empty proto)
  -> CLI dev server
  <- 32-byte serialized response
copy + temporal_core_byte_array_free
client_free -> runtime_free
```

Verified environment:

- Odin `dev-2026-05:ea5175d86`;
- Temporal CLI 1.5.1, dev server 1.29.1;
- macOS arm64;
- Core C bridge pinned at `399cb7a`;
- runtime lifecycle tests green;
- end-to-end `GetSystemInfo` green through a directly started
  `temporal server start-dev --headless` process.

The Milestone 2 path, now using schema-generated bindings, also executed
end-to-end on the same environment:

```text
Rust workflow-only test driver -> Temporal CLI dev server
  -> Core Activity poll callback -> Odin worker
  -> decode prost ActivityTask with generated Odin types
  -> dispatch hello(ctx, "Odin")
  -> JSON Payload "Hello Odin"
  -> encode ActivityTaskCompletion -> Core prost decode -> server
  -> workflow observes "Hello Odin"
```

The integration suite repeats the path with an unregistered Activity type and
observes a workflow failure produced from Odin's non-retryable
`ApplicationFailure`. It also verifies validate, initiate-shutdown, shutdown
poll, finalize, and handle-free order. The driver uses the pinned upstream Rust
SDK only to create a Workflow that schedules the Activity; it is test
infrastructure and no Odin workflow runtime is implied.

An attempted smaller test using the pinned client's standalone-Activity API
failed because the CLI-bundled Temporal Server 1.29.1 returned `Unimplemented`
for `StartActivityExecution`. The API exists in the pinned client/protos but is
not available in this server version, so the workflow-only driver is required
for the requested CLI development loop today.

### Explicit Workflow state-machine experiment

The Milestone 3 experiment implements exactly one Workflow shape and does not
claim to be a user-facing Workflow runtime. Its explicit phases are:

```text
InitializeWorkflow
  -> ScheduleActivity(seq=1, type="hello")
  -> Waiting_Activity
ResolveActivity(seq=1)
  -> CompleteWorkflowExecution(result)
  -> Completed
```

The live test deliberately evicts the language state after the schedule
command and before executing the Activity. The observed activation sequence was:

| History | Replay | Job/result | Odin action |
| ---: | :---: | --- | --- |
| 3 | no | initialize | emit `ScheduleActivity(seq=1)` |
| 3 | no | requested eviction | delete the Odin instance |
| 3 | yes | initialize from history | recreate instance and emit the same command |
| 9 | no | activity resolved | emit `CompleteWorkflowExecution` |
| 9 | no | completion eviction | delete the completed instance |

Temporal CLI observed the final `"Hello Odin"` result. This demonstrates that
Core matches deterministic replay commands against history and that Odin state
can be discarded and reconstructed. The offline state-machine test additionally
asserts that the initial and replayed schedule completions are byte-for-byte
identical. This does not demonstrate sequential-looking Workflow code, timers,
multiple outstanding operations, signals, queries, updates, concurrency, or
nondeterminism detection in the language layer.

### Stackful Workflow fiber experiment

Odin `dev-2026-05` has no official portable fiber/coroutine package. The
standard `core:nbio` package is a callback-based non-blocking I/O layer; Odin's
[Q1 2026 newsletter](https://odin-lang.org/news/newsletter-2026-q1/) explicitly
describes native or Lua-style coroutines as something that can be built above
it, not functionality it already provides. Three small-runtime candidates were
checked before implementation:

- Laytan's [Odin minicoro port](https://gist.github.com/laytan/1c9887a3c9847ed7073dd632d9114229)
  is attractive because it is pure Odin, but the current target switch supports
  only Darwin arm64 and Linux amd64. That misses Darwin amd64 and Linux arm64
  from this project's initial matrix.
- [`foldcat/oasync`](https://github.com/foldcat/oasync) is a complete M:N task
  runtime. Its own documentation says it is incompatible with `core:sync`.
  Adopting a scheduler and replacement synchronization layer is too broad for
  the question this spike needs to answer.
- Upstream [minicoro](https://github.com/edubart/minicoro) is a single-header,
  dependency-free asymmetric stackful coroutine implementation. Its documented
  assembly targets include x86_64 and ARM64 on macOS and Linux, covering all
  four planned artifacts. It also exposes explicit create/resume/yield/destroy
  operations and configurable stack ownership.

Minicoro is therefore an experimental implementation dependency, not yet a
permanent public-SDK commitment. The Odin side sees only these project-owned C
symbols:

```text
temporal_odin_fiber_create
temporal_odin_fiber_resume
temporal_odin_fiber_yield
temporal_odin_fiber_status
temporal_odin_fiber_destroy
temporal_odin_fiber_result_description
```

Each cached Workflow instance owns a 128 KiB C-allocated fiber stack and an
Odin arena. The instance is heap-stable because the C trampoline retains its
address. The trampoline restores a captured Odin `runtime.Context`, with the
instance arena as allocator, before calling user code. Inputs, Activity request
payloads, resolved values, and arbitrary Workflow allocations can therefore
survive suspension without borrowing an activation decode arena. Command
storage itself remains alive for the full activation encode; an early version
of the spike found that pointers to helper-local command structs become invalid
before protobuf encoding, a concrete reason to keep this ownership rule
explicit.

The author-facing proof is genuinely sequential:

```odin
hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
    first := workflow.execute_activity(ctx, "hello", name)
    workflow.sleep(ctx, 250 * time.Millisecond)
    return workflow.execute_activity(ctx, "hello", first)
}
```

`execute_activity` and `sleep` each record a typed operation and yield from
inside the nested Odin call. Activity and timer sequence counters are separate;
the serial test emits `ScheduleActivity(seq=1)`, `StartTimer(seq=1)`, and
`ScheduleActivity(seq=2)`. Resolutions are matched against both operation type
and sequence before the fiber resumes.

The live acceptance test requests eviction at all three suspension points. The
observed Core boundary was:

| History | Replay | Odin command/job |
| ---: | :---: | --- |
| 3 | no | initialize -> Activity 1 |
| 3 | no | forced eviction |
| 3 | yes | initialize -> Activity 1 |
| 9 | no | resolve Activity 1 -> Timer 1 |
| 9 | no | forced eviction |
| 3 | yes | initialize -> Activity 1 |
| 9 | yes | resolve Activity 1 -> Timer 1 |
| 14 | no | fire Timer 1 -> Activity 2 |
| 14 | no | forced eviction |
| 3 | yes | initialize -> Activity 1 |
| 9 | yes | resolve Activity 1 -> Timer 1 |
| 14 | yes | fire Timer 1 -> Activity 2 |
| 20 | no | resolve Activity 2 -> complete Workflow |
| 20 | no | completion eviction |

Thus the third fresh fiber re-executed the Workflow from its original input,
consumed two prior resolutions, reproduced all three commands, and only then
continued at the live Activity 2 frontier. Temporal CLI returned
`"Hello Hello Odin"`; neither Activity was re-executed during replay. Offline
tests cover the individual eviction/recreation path and the complete serial
Activity/timer/Activity transition sequence under Odin's memory tracker.

The next bounded experiment replaced the single pending slot with per-instance,
seq-keyed Activity and timer operation tables. `start_activity` records without
yielding and returns a seq-only handle; `await_activity` yields only when that
operation is unresolved. Handles deliberately do not retain pointers into the
growable table. Command protobuf objects are activation-arena-owned until the
completion is encoded, allowing one completion to contain multiple commands.
This shape follows two current upstream boundaries: the pinned Rust Workflow
runtime [applies every activation job and returns their results before routines
are polled](https://github.com/temporalio/sdk-rust/blob/399cb7adb40f45eefbef90149e212743e474142c/crates/workflow/src/runtime/instance.rs),
while the TypeScript runtime keeps
[seq-keyed completion maps and an activation command buffer](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/internals.ts).

The live example schedules `Activity(seq=1)` and `Activity(seq=2)` in the
initial activation, then awaits them in source order. The observed boundary was:

| History | Replay | Odin command/job |
| ---: | :---: | --- |
| 3 | no | initialize -> schedule Activities 1 and 2 |
| 3 | no | forced eviction |
| 3 | yes | initialize -> reproduce both schedule commands |
| 10 | no | resolve Activity 2 -> remain waiting for Activity 1 |
| 10 | no | forced eviction |
| 3 | yes | initialize -> reproduce both schedule commands |
| 10 | yes | resolve Activity 2 -> remain waiting for Activity 1 |
| 15 | no | resolve Activity 1 -> consume both results and complete |
| 15 | no | completion eviction |

Activity 2 happened to finish first, which is useful evidence: completion order
need not equal await order. Temporal CLI returned
`"Hello Odin & Hello Temporal"`. An offline test also delivers both resolution
jobs in reverse order in one activation and asserts that the runtime stores all
jobs before resuming the fiber. This is the narrow `apply-all-before-resume`
rule required at the language/runtime boundary; it is not yet a general ready
queue or multi-fiber scheduler.

### Activity failure as a Workflow result

Core reports an exhausted Activity failure as the failed variant of
`ResolveActivity`; whether Workflow code catches it or lets it fail the
Workflow is language-runtime policy. This matches current SDK behavior:
TypeScript [rejects the seq-keyed Activity completion with a converted
failure](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/internals.ts),
and .NET documents that
[Activity failures throw and fail the Workflow only when uncaught](https://github.com/temporalio/sdk-dotnet#invoking-activities).

The Odin spike now records that resolution as `Fiber_Operation_State.Failed`.
It copies the human-readable message and re-encodes the complete Temporal
`Failure` protobuf into the Workflow instance arena before the activation
decode arena is released. `execute_activity_result` returns an experimental
`Activity_Result`, allowing Workflow code to branch. The existing
`execute_activity` convenience treats the same result as unhandled; the runtime
decodes the preserved protobuf and emits `FailWorkflowExecution`, retaining the
Temporal failure rather than synthesizing a new one.

The live failure-recovery sequence was:

| History | Replay | Odin command/job |
| ---: | :---: | --- |
| 3 | no | initialize -> schedule missing Activity 1 |
| 3 | no | forced eviction |
| 3 | yes | initialize -> reproduce Activity 1 command |
| 9 | no | Activity 1 fails -> catch -> schedule recovery Activity 2 |
| 9 | no | forced eviction |
| 3 | yes | initialize -> reproduce Activity 1 command |
| 9 | yes | replay failure -> take same catch branch -> reproduce Activity 2 command |
| 15 | no | Activity 2 resolves -> complete normally |
| 15 | no | completion eviction |

Temporal CLI returned `"Hello Recovered Odin"`. The offline replay test asserts
that the recovery command is byte-identical before and after eviction. A
separate test verifies that the non-result convenience still emits a
`FailWorkflowExecution` command containing the preserved failure message.

### Buffered Workflow signal

Core delivers external signals as `SignalWorkflow` activation jobs containing
the signal name and payloads. Buffering and dispatch are language-runtime
responsibilities: the TypeScript SDK has an explicit
[buffered signal queue](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/internals.ts),
and Temporal's Go documentation confirms that
[signals are retained until matching Workflow code receives them](https://github.com/temporalio/documentation/blob/main/docs/develop/go/workflows/message-passing.mdx#signal-channels).

The Odin spike accepts exactly one `json/plain` string argument per signal. It
copies name and payload into the Workflow instance arena and appends them to an
arrival-ordered queue. `await_signal(ctx, name)` consumes the oldest unconsumed
matching entry immediately, or changes the fiber phase to `Waiting_Signal` and
yields. A signal only resumes the fiber when that exact phase/name is active;
signals received while waiting on an Activity remain buffered.

The live acceptance sequence was:

| History | Replay | Odin command/job |
| ---: | :---: | --- |
| 3 | no | initialize -> schedule Activity 1; wait on Activity |
| 8 | no | receive `approval`; buffer it; remain waiting on Activity |
| 8 | no | forced eviction |
| 3 | yes | initialize -> reproduce Activity 1 command |
| 8 | yes | replay `approval`; buffer it; remain waiting on Activity |
| 13 | no | resolve Activity 1; consume buffered signal; complete |
| 13 | no | completion eviction |

Temporal CLI returned `"Hello Odin / Approved"`. The offline test asserts that
the original and replayed signal activations produce byte-identical empty
completions, proving the signal does not create a command or wake the Activity
await. A second test covers the direct path where a fiber already in
`Waiting_Signal` resumes when the matching signal arrives.

### Durable signal/timer race

The pinned Core schema already exposes `StartTimer`, `FireTimer`, and
`CancelTimer`; no C ABI addition is required. `CancelTimer.seq` must match the
language-assigned sequence from `StartTimer`. Core's activation preparation
[orders signals and updates before ordinary jobs such as `FireTimer`](https://github.com/temporalio/sdk-rust/blob/main/crates/sdk-core/src/worker/workflow/mod.rs),
while the TypeScript SDK only emits
[timer cancellation while its timer completion is still pending](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/workflow.ts).

The Odin spike now exposes
`await_signal_or_timeout(ctx, name, duration) -> Signal_Wait_Result`. With no
matching signal already buffered, it assigns a timer sequence, emits
`StartTimer`, and suspends in `Waiting_Signal_Or_Timer`. A later matching signal
wins and marks the still-commanded timer for cancellation. A `FireTimer` with no
matching signal wins the timeout branch. If Core supplies both jobs in one
activation, the signal wins according to Core's ordering, but the runtime sees
that the timer has also fired and does not emit an invalid late cancellation.

The live acceptance sequence was:

| History | Replay | Odin command/job |
| ---: | :---: | --- |
| 3 | no | initialize -> `StartTimer(seq=1)`; wait for signal or timer |
| 8 | no | receive `approval` -> `ScheduleActivity(seq=1)` + `CancelTimer(seq=1)` |
| 8 | no | forced eviction |
| 3 | yes | initialize -> reproduce `StartTimer(seq=1)` |
| 8 | yes | replay signal -> reproduce Activity and timer-cancel commands |
| 15 | no | resolve Activity 1 -> complete |
| 15 | no | completion eviction |

Temporal CLI returned `"Hello Approved"`. The offline replay test additionally
requires the initial timer completion and signal-winner completion to be
byte-identical before and after eviction. Separate tests prove the timeout
branch emits no cancellation and the same-activation signal/timer tie selects
the signal without attempting to cancel the fired timer.

### Workflow cancellation while suspended

Temporal cancellation is a cooperative Workflow event, not server-side
termination. The Service records a cancellation request and Core presents it as
`CancelWorkflow`; the language runtime decides how user execution and pending
operations react. This matches the [.NET cancellation contract](https://github.com/temporalio/documentation/blob/main/docs/develop/dotnet/workflows/cancellation.mdx)
and the TypeScript runtime's
[root cancellation-scope handling](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/internals.ts).

The current Odin policy is deliberately smaller than either public SDK:

1. Preserve the cancellation reason in the Workflow-owned arena.
2. Do not resume a suspended user fiber and do not run user cleanup.
3. Drop operations that were only locally scheduled.
4. Emit `RequestCancelActivity(seq)` and/or `CancelTimer(seq)` for commands
   already sent to Core.
5. Emit `CancelWorkflowExecution` in the same completion and mark the terminal
   command as sent.

This policy keeps the external semantics correct while postponing user-visible
cancellation tokens/scopes and cleanup syntax. It is not the intended final
API. In particular, Activity cancellation remains cooperative at the Activity
implementation boundary; the Workflow request does not forcibly stop arbitrary
Odin code.

The live timer path was:

| History | Odin command/job |
| ---: | --- |
| 3 | initialize -> `StartTimer(seq=1)` |
| 8 | `CancelWorkflow` -> `CancelTimer(seq=1)` + `CancelWorkflowExecution` |
| 8 | final cache eviction |

The live Activity path exposed an additional lifecycle rule:

| History | Odin command/job |
| ---: | --- |
| 3 | initialize -> `ScheduleActivity(seq=1)` |
| 8 | `CancelWorkflow` -> `RequestCancelActivity(seq=1)` + `CancelWorkflowExecution` |
| 8 | late terminal `ResolveActivity(seq=1)` -> empty successful completion |
| 8 | final cache eviction |

The late resolution is a Core-language-runtime handshake, not a second user
event. The Workflow instance must remain alive until eviction, accept the
terminal resolution idempotently, and never repeat `CancelWorkflowExecution`.
Offline tests cover both pending-operation kinds, cancellation before an
initial command is emitted, same-activation races, the late Activity
resolution, and byte-identical replay of both timer and Activity cancellation
sequences.

### Generated lifecycle testing

`tests_pbt` uses the pinned, test-only
[flakstad/pbt](https://github.com/flakstad/pbt) library to generate 500 batches
from nine signal/timer/cancellation presence and ordering classes. Each case is
checked against a small event-outcome model, then evicted and replayed from a
fresh fiber; the decisive completion must be byte-identical. Coverage
constraints ensure every class appears in a successful run.

The sibling `statecharts` library was evaluated as a possible independent
model oracle. We are not adding it yet: the present model has one wait state and
three event kinds, so a direct transition table is smaller and easier to audit.
Revisit a statechart model when cancellation scopes, multiple simultaneous
waits, or a real Workflow scheduler introduce orthogonal regions or nested
states. At that point it can be valuable precisely because the model should be
independent of the runtime implementation.

### Reusable worker registry and poll boundary

Core's architecture assigns task dispatch and concurrency to the language SDK:
the language layer continuously polls each enabled task kind, invokes user
code, and returns completions, using language-appropriate threads or
coroutines. The pinned
[Core architecture document](https://github.com/temporalio/sdk-rust/blob/399cb7adb40f45eefbef90149e212743e474142c/ARCHITECTURE.md)
is explicit that this concurrency is not supplied by Core.

The first reusable Odin worker implements this boundary:

```text
worker.run
    |-- Workflow poll thread
    |      decode route
    |      Workflow type --initialization--> registered Fiber_Runtime
    |      run ID --------subsequent-------> same Fiber_Runtime
    |      process activation -> complete activation
    |      RemoveFromCache -> destroy fiber and route
    |
    `-- Activity poll thread
           decode task -> registered Activity -> complete task
```

The blocking C calls can run concurrently against the same Core worker. The
Workflow registry itself is frozen while `run` is active, and only the Workflow
poll thread accesses its fiber runtimes and run-ID table. This preserves
single-threaded fiber affinity while allowing a Workflow-produced Activity to
execute without stopping Workflow polling. The Activity side is still one
poller executing one procedure synchronously; this is orchestration
concurrency, not yet concurrent Activity execution.

`register_workflow(name, proc)` owns one `Fiber_Runtime` per Workflow type. A
unit test initializes two types under different run IDs, verifies dispatch to
the correct procedure, evicts one, and proves that the other route remains.
The live signal/timer test now uses only this worker boundary: Workflow and
Activity streams run together, Core eviction causes fresh-stack replay, and
the Activity result resumes the correct Workflow.

Normal `worker.run` is unbounded. `request_shutdown` forwards Core's shutdown
request so outstanding poll calls can return; `close` is rejected while `run`
is active and finalizes only afterward. Bounded task/execution counts exist for
examples and tests. The current construction API still requires
`enable_workflows = true` before registration because the Core worker is
created too early; delayed construction is the clean fix.

This answers the narrow feasibility question: stackful suspension and
history-driven reconstruction work across the Odin/C boundary without a large
runtime. It does not yet answer scheduling at scale. A coroutine object is not
thread-safe, fixed stacks can overflow, and minicoro cautions against migrating
coroutines that use thread-local state. The production runtime should drive a
Workflow instance from one deterministic executor thread at a time, prohibit
concurrent resume/eviction, decide whether fixed or virtual-memory-backed stacks
are acceptable, and test sanitizers and all four target builds. Panics crossing
the C trampoline also need an explicit containment policy.

The blocking wrapper now drains both enabled poll streams during shutdown.
Core requires every enabled workflow/activity poll stream to observe shutdown
before finalization; draining only the Activity stream is insufficient once
Workflows are enabled.

## Risks and unresolved questions

1. **ABI stability.** The C bridge is generated from Rust structs, unpublished,
   and evolves with Core. Pinning plus ABI assertions is mandatory. We should
   ask upstream whether third-party language SDK consumption has compatibility
   guarantees or a preferred release channel.
2. **Protobuf generator scope.** The Activity and Workflow boundary graphs,
   including imports, maps, presence, and `oneof`s, are generated and
   wire-compatible with `prost`. Unknown fields are skipped rather than
   retained. Every new root graph still needs generation, compile, unit, and
   live `prost` interoperability checks.
3. **Worker concurrency.** Dedicated Workflow and Activity poll threads now
   integrate the blocking callbacks and preserve Workflow fiber affinity, but
   the Activity side remains synchronous. A production worker needs a bounded
   Activity executor, heap-stable in-flight state, cancellation delivery,
   heartbeats, and backpressure tests.
4. **Workflow runtime.** The fiber proves nested Odin suspension, durable
   timers, multiple outstanding Activities, multi-command completions,
   apply-all-before-resume, catchable Activity failures, buffered signals, and
   a durable timer/signal race, and runtime-owned cancellation across
   fresh-stack replay. It does not yet prove user cancellation scopes/cleanup,
   updates, panic containment, nondeterminism diagnostics, bounded stack policy,
   or a production deterministic scheduler.
5. **Cross-platform artifacts.** Only macOS arm64 was executed. Linux and all
   other architectures need CI proof before claiming support.
6. **Core version skew.** Server compatibility is broad, but Core-local
   protobuf schemas and C structs are coupled to the exact native library.
   Generated Odin types and the native artifact must ship as one versioned unit.
7. **Error/cancellation completeness.** The Workflow runtime preserves the raw
   Temporal failure protobuf for propagation, but the catchable experimental API
   exposes only status and message. A public API must model typed causes/details,
   Activity-side cancellation delivery, heartbeats, deadlines, panics, and
   codec failures.
8. **Client/server feature skew.** The pinned standalone-Activity client API is
   newer than CLI Server 1.29.1. Feature availability must be tested against
   the supported server matrix rather than inferred from generated APIs.

## Workflow runtime alternatives: final decision still deferred

| Option | What the evidence says now | First useful test |
| --- | --- | --- |
| Explicit state machine | Proven for Activity scheduling, forced eviction, replay, resolution, and completion; authoring API is unacceptable | Keep as a reference oracle, not the primary authoring direction |
| Stackful coroutine/fiber | Proven for serial operations, multiple outstanding Activities, handled/unhandled Activity failure, buffered signals, a durable signal/timer race, runtime-owned cancellation, registered dispatch, eviction, and replay on macOS arm64 | Add user cancellation scopes and test multiple live Workflow runs without moving fibers between threads |
| CPS/code generation | Can make state explicit while retaining source syntax; high tooling cost | Transform a two-await example and compare generated commands on replay |

Activity transport, protobuf, worker lifecycle, command matching, the replay
path, and the minimum fiber authoring boundary are now proven. This is enough
evidence to continue with fibers while retaining the explicit state machine as
a comparison point; it is not yet enough to freeze the public API.

## Recommended next vertical milestone

Workflow and Activity orchestration now lives in `temporal/worker`, and the live
example no longer polls Core or dispatches activations itself. The next vertical
milestone should make Activity execution genuinely asynchronous: one poll loop,
a small bounded executor, task-token keyed in-flight state, `ActivityTask`
cancellation delivery, and heartbeat support through `Activity_Context`. This
tests the next unproven Core/language lifecycle boundary and removes the largest
gap in the Activity worker without coupling it to Workflow cancellation-scope
syntax.

Alongside that slice, delay Core worker construction until `run` so registered
task types determine Core options and the public API no longer exposes
`enable_workflows`. Unknown Workflow types must also produce a language-facing
failed activation without terminating the worker. Generic payload APIs and
user Workflow cancellation scopes should follow as separate changes.

In parallel, add CI compile/smoke jobs
for macOS x86_64 and Linux x86_64/arm64 before treating minicoro as a committed
dependency. Keep queries and updates deferred until cancellation semantics are
grounded at the user-code boundary. Activity concurrency, heartbeats, and
Activity-side cancellation delivery remain the next worker-side production
work, but the Activity Worker vertical milestone itself is already complete.

The C bridge remains the recommended foundation. It now handles both Activity
Tasks and multi-activation Workflow execution end-to-end, including replay and
eviction. The findings add protobuf/tooling, language-runtime, and version-skew
obligations but give no reason to bypass Core or implement direct server gRPC.

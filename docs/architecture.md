# Architecture: Temporal SDK for Odin

Status: Milestone 0 research, Milestone 1 bridge spike, a narrow Milestone 2
Activity Worker slice, and schema-derived Activity protobuf bindings,
2026-09-25.

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

The first roots are `coresdk.activity_task.ActivityTask` and
`coresdk.ActivityTaskCompletion`. Their reachable type graph replaces the
hand-written Activity envelope codec. The generated file is committed and
`./scripts/generate-protos.sh --check` makes schema drift visible in normal
tests. The live integration test is the interoperability oracle: Core encodes
the polled task with Rust `prost`, Odin decodes it, Odin encodes success or
failure, and Core decodes the completion with `prost` before talking to the
server. Both paths pass against the CLI dev server.

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

The current wrapper blocks the caller on an Odin one-shot event. This keeps
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

## Milestone 1 and 2 implementation and evidence

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

## Risks and unresolved questions

1. **ABI stability.** The C bridge is generated from Rust structs, unpublished,
   and evolves with Core. Pinning plus ABI assertions is mandatory. We should
   ask upstream whether third-party language SDK consumption has compatibility
   guarantees or a preferred release channel.
2. **Protobuf generator scope.** The exact Activity graph and its imports,
   maps, presence, and `oneof`s are generated and wire-compatible with `prost`.
   Unknown fields are skipped rather than retained, and Workflow roots are not
   generated yet. Every new root graph needs generation, compile, unit, and
   live `prost` interoperability checks.
3. **Async callback integration.** The synchronous single-task worker is safe
   but not scalable. A real worker needs a callback-to-channel/event loop with
   heap-stable state, concurrent Activity execution, cancellation, and
   heartbeats.
4. **Workflow runtime.** Core does not solve Odin suspension, deterministic
   scheduling, or cached coroutine lifecycle. Fibers remain promising but must
   replay from a fresh stack after eviction.
5. **Cross-platform artifacts.** Only macOS arm64 was executed. Linux and all
   other architectures need CI proof before claiming support.
6. **Core version skew.** Server compatibility is broad, but Core-local
   protobuf schemas and C structs are coupled to the exact native library.
   Generated Odin types and the native artifact must ship as one versioned unit.
7. **Error/cancellation completeness.** The spike copies messages and frees all
   buffers and emits an application failure for dispatch/codec errors, but a
   public API must preserve gRPC status/details, Activity cancellation,
   heartbeats, deadlines, panics, and richer Temporal failures.
8. **Client/server feature skew.** The pinned standalone-Activity client API is
   newer than CLI Server 1.29.1. Feature availability must be tested against
   the supported server matrix rather than inferred from generated APIs.

## Workflow runtime alternatives: decision deliberately deferred

No workflow runtime was implemented.

| Option | What the evidence says now | First useful test |
| --- | --- | --- |
| Explicit state machine | Easiest replay model, worst desired authoring API | Complete one activation/timer cycle with hand-written states |
| Stackful coroutine/fiber | Best match for sequential Odin syntax; stack is cache-only, never durable | Start, suspend, evict, recreate, and replay a tiny workflow deterministically |
| CPS/code generation | Can make state explicit while retaining source syntax; high tooling cost | Transform a two-await example and compare generated commands on replay |

Revisit only after activity transport, protobuf, worker lifecycle, and error
handling are proven. That order prevents choosing a workflow runtime around an
incorrect understanding of Core.

## Recommended next vertical milestone

The bounded schema/tooling milestone is complete: Activity messages are
schema-derived and cross-checked with `prost` through the live Core boundary.
Next, start Milestone 3 with an explicit-state-machine workflow that
schedules this already-working `hello` Activity and completes after its
resolution. That should prove activation ordering, command encoding, replay,
and eviction before selecting fibers or code generation. Generate only the
Workflow activation/completion graph required by that experiment. Activity
concurrency, heartbeats, and cancellation remain important follow-up slices,
but they need not block the bounded Workflow activation experiment.

The C bridge remains the recommended foundation. Milestone 2 and the subsequent
schema milestone strengthened that decision: Core handled polling, tokens,
transport, retries, server completion, and shutdown as expected, while its
protobuf boundary proved practical to generate directly for Odin. The findings
add protobuf/tooling and version-skew obligations but give no reason to bypass
Core or implement direct server gRPC.

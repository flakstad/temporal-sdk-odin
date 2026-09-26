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

The tested Odin development compiler has intermittently asserted at
`src/check_type.cpp:598` or exited with signal 11 while type checking
`pbt.State_Model`, including with `-thread-count:1`. The smallest observed
command is the `odin test tests_pbt ...` invocation in `scripts/test.sh`; an
immediate identical retry succeeds, so this is compiler instability rather
than a shrinking/property failure. The property compilation therefore also
uses Odin's narrower `-no-threaded-checker` workaround. Repository scripts keep
`-thread-count:1` as the default for the separate `queue.Queue(Task)` parallel
compiler failure seen earlier. These flags affect compilation only, not worker
or test runtime concurrency. Set `ODIN_THREAD_COUNT` to experiment with a newer
compiler, but retain `-no-threaded-checker` until the `State_Model` regression
is fixed upstream.

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
with `CanceledFailureInfo`. `activity.heartbeat(ctx, details)` remains the
string convenience API; `activity.heartbeat_json` accepts any JSON-compatible
Odin value and `activity.heartbeat_payload` accepts a borrowed payload. All
three send one Temporal Payload through
`temporal_core_worker_record_activity_heartbeat`. Core owns heartbeat
throttling, and any server cancellation response returns later through the
normal Activity poll stream.

`activity.info(ctx)` returns callback-scoped execution metadata copied from
Core's `ActivityTask.Start`: Activity and Workflow identity, namespace, attempt,
timestamps, timeouts, effective Retry Policy, priority, local/remote state, and
previous heartbeat details. The worker supplies its configured task queue
because that value is not part of the Core start message. All strings, slices,
task-token bytes, and Payload views are borrowed from the context and must not
escape the callback.

```odin
info := activity.info(ctx)
deadline, has_deadline := activity.deadline(ctx)

progress: Progress
found, decode_error := activity.heartbeat_detail_json(ctx, 0, &progress)
raw_detail, raw_found := activity.heartbeat_detail_payload(ctx, 0)
```

Missing heartbeat indexes return `found = false` without a codec error. JSON
decode failures return `found = true` plus the codec error. The raw accessor is
the codec-neutral boundary. Deadline is derived from the earliest applicable
Schedule-to-Close and Start-to-Close endpoint; `has_deadline` is false only
when Core omitted the required timestamp/timeout pairs.

Activities can be registered at four levels:

```odin
worker.register_activity(&w, "hello", hello_string)
worker.register_json_activity(&w, "typed_greeting", typed_greeting)
worker.register_json_activity_result(&w, "charge", charge_with_failure)
worker.register_payload_activity(&w, "custom", custom_payload_activity)
```

The string form is a compatibility wrapper over the typed JSON adapter. JSON
callbacks receive one decoded Odin value and return one encoded value. Payload
callbacks borrow their input only for the callback and return an owned
`codec.Payload`; the executor frees that result with its recorded allocator
after Core completion bytes have been constructed.

The result adapter accepts
`proc(Context, Input) -> activity.Result(Output)`. A successful result is
encoded like the ordinary JSON adapter. A failure becomes a Temporal
`ApplicationFailureInfo` and preserves `message`, `type`, `non_retryable`, one
optional details Payload, and an optional positive `next_retry_delay`.
`result_failure` is retryable by default; use `non_retryable = true` only for a
permanent condition. `result_failure_json` owns its encoded details until the
executor has built the completion. A negative retry delay is rejected as a
non-retryable `OdinActivityFailureContractError` rather than emitting an
invalid protobuf duration.

Activity panics are not caught in-process. In the tested Odin toolchain,
`panic` delegates to `context.assertion_failure_proc`, whose type returns `!`;
there is no supported unwind/recover boundary. A `setjmp`/`longjmp` shim would
skip Odin `defer`s and allocator cleanup, so the SDK intentionally does not
present it as containment. Until subprocess-isolated Activity executors or
language/runtime support is designed, deploy workers under a process supervisor
and return `activity.Result(T)` for expected failures.

Workflows have the corresponding registration levels:

```odin
worker.register_workflow(&w, "string_workflow", string_workflow)
worker.register_json_workflow(&w, "typed_workflow", typed_workflow)
worker.register_payload_workflow(&w, "payload_workflow", payload_workflow)
```

The runtime stores Workflow input, scheduled Activity input, resolved Activity
output, and Workflow completion as allocator-owned `codec.Payload` values in
the Workflow instance arena. `register_json_workflow` generates a typed adapter
for `proc(Fiber_Context, Input) -> Output`; `register_workflow` is the existing
string compatibility adapter. `register_payload_workflow` receives a borrowed
view and returns an owned `workflow.Payload_Result`. Callback storage belongs to
the `Fiber_Runtime` and is released by `fiber_runtime_close`.

Workflow code can use `start_activity_payload`/
`await_activity_payload_result` at the codec boundary, or
`start_activity_json`, `execute_activity_json_result`, and
`execute_activity_json` for typed values. The output type is explicit at the
call site because an Activity name string cannot communicate an Odin return
type to the compiler. JSON conversion failures become non-retryable
`OdinWorkflowPayloadConversionError` Workflow failures. Signal payloads remain
on the string compatibility API for now.

Every Activity start/execute family has a `_with_options` form. The options map
to Core's `ScheduleActivity` fields:

```odin
non_retryable_types := [?]string{"InvalidOrder"}
retry := workflow.Retry_Policy{
    initial_interval = 500 * time.Millisecond,
    backoff_coefficient = 2,
    maximum_interval = 10 * time.Second,
    maximum_attempts = 5,
    non_retryable_error_types = non_retryable_types[:],
}
future := workflow.start_activity_json_with_options(
    ctx,
    "charge",
    order,
    {
        activity_id = "charge-order-42",
        task_queue = "payments",
        schedule_to_close_timeout = 2 * time.Minute,
        schedule_to_start_timeout = 10 * time.Second,
        start_to_close_timeout = 30 * time.Second,
        heartbeat_timeout = 5 * time.Second,
        retry_policy = &retry,
        cancellation_type = .Wait_Cancellation_Completed,
        disable_eager_execution = true,
    },
)
```

The options value, its Retry Policy pointer, and the non-retryable error-type
slice are borrowed only until `start_activity_*_with_options` returns. The
runtime clones all strings into the Workflow instance arena. A nil Retry Policy
leaves the field absent so the Service can apply its default policy. Zero
duration means unspecified; either Schedule-to-Close or Start-to-Close must be
positive. Invalid options fail the Workflow with non-retryable
`OdinActivityOptionsError`. The no-options compatibility calls still use the
bounded spike defaults: 20-second Start-to-Close, maximum attempts 1, and eager
execution disabled.

The raw Payload test uses `binary/custom`, an extra `messageType` metadata
entry, opaque bytes, and external-payload details. It verifies those fields are
unchanged in both the scheduled Activity argument and final Workflow result;
the generic runtime does not assume JSON.

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

## Client start/result slice

`temporal/client` now owns the high-level connection lifecycle:

```odin
client, connect_error := temporal_client.connect(
    target_url = "http://localhost:7233",
    namespace = "default",
)
defer temporal_client.close(&client)

handle, start_error := temporal_client.start_workflow_json(
    &client,
    "greeting_workflow",
    Greeting_Input{name = "Odin"},
    {id = "greeting-42", task_queue = "greetings"},
)
defer temporal_client.workflow_handle_free(&handle)

result: Greeting_Output
result_error := temporal_client.get_result_json(&handle, &result)
```

`connect` heap-owns a Core runtime, Core connection, and namespace. The handle
owns its Workflow ID, result run ID, and first execution run ID, while borrowing
the Client. Free handles before closing the Client. `start_workflow_payload`
and `get_result_payload` are the codec-neutral forms; JSON helpers currently
represent one Workflow argument and one result Payload.

Start uses a UUIDv4 request ID when the caller omits one. Zero duration means
unspecified; negative execution/run/task timeouts are rejected before RPC.
Result retrieval issues a blocking close-event long poll with no local timeout,
maps every terminal Workflow event, and follows retry/cron/Continue-As-New run
IDs by default. Set `follow_runs = false` to receive
`Workflow_Continued_As_New` instead. RPC failures retain the gRPC status code
and binary status-details protobuf.

The same handle supports control operations:

```odin
existing, handle_error := temporal_client.get_workflow_handle(&client, "greeting-42")
defer temporal_client.workflow_handle_free(&existing)

signal_error := temporal_client.signal_workflow_json(&existing, "approval", approval)
cancel_error := temporal_client.cancel_workflow(&existing, "no longer needed")
terminate_error := temporal_client.terminate_workflow(&existing, "operator decision")
```

Handle creation only clones identifiers; it does not contact the Service. An
empty run ID targets the current execution. Signal and cancellation requests
generate UUIDv4 deduplication IDs by default. Signal and termination have
codec-neutral Payload forms, plus typed JSON helpers. The live suite invokes
all three operations through `examples/client_control`.

Queries use the same borrowed handle and Core generic-RPC path:

```odin
status: Greeting_Query_Output
query_error := temporal_client.query_workflow_json(
    &existing,
    "greeting_status",
    Greeting_Query_Input{include_name = true},
    &status,
)
```

`query_workflow_payload` returns an owned Payload that the caller frees with
`codec.payload_free`; the JSON helper owns that intermediate value and decodes
it into the caller's output pointer. The request currently sends exactly one
argument Payload. A server `QueryRejected` response maps to
`.Workflow_Query_Rejected`; handler failures remain normal Core RPC failures
with the server's gRPC status/details.

Workflow code installs the matching typed handler with
`workflow.set_query_handler_json`. Its state pointer is borrowed from the
Workflow fiber and the callback receives a top-level state copy. The callback
must be read-only, must not retain the input/state values, and cannot suspend.
Unknown query names and codec failures become failed `RespondToQuery` commands
without failing the Workflow Task.

Workflow-side signal buffering is codec-neutral. Use
`workflow.await_signal_payload` for the borrowed Payload or
`workflow.await_signal_json(ctx, name, T)` for a typed value. The
`await_signal_payload_or_timeout`, `await_signal_json_or_timeout`, and string
compatibility forms use the same deterministic signal/timer race. Payload
conversion failures fail the Workflow with
`OdinWorkflowPayloadConversionError` rather than stopping the worker.

The integration test uses the Temporal CLI dev server directly—no Docker,
PostgreSQL, or Elasticsearch:

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

The same script then runs `examples/hello_activity` for several focused cases.
A small workflow-only driver under `native/activity-test-driver` uses the
pinned upstream Rust SDK to
schedule a real Activity because Temporal CLI Server 1.29.1 does not implement
the newer standalone-Activity start RPC. The first run verifies `"Odin"` ->
`"Hello Odin"`; the second schedules an unknown type and verifies that Odin's
application failure reaches the Workflow. The driver is test infrastructure,
not part of the distributed Odin SDK and not an Odin workflow runtime. This is
also the protobuf interoperability test: the Rust/Core side uses `prost` to
encode Activity Tasks and decode Odin's success/failure completions.

The typed retry case returns a retryable `TransientGreetingError` with JSON
details and `next_retry_delay = 10 ms` on attempt 1. Core and the dev server
schedule attempt 2; the Odin callback observes `activity.attempt(ctx) == 2`,
decodes the previous typed heartbeat detail, returns success, and the driver
receives the typed result. This proves heartbeat progress crossed Core and the
Service rather than remaining in Odin memory. The bounded test
worker therefore consumes two Activity Tasks before shutdown.

A separate live case uses `examples/client_start` to start
`odin_typed_workflow` with
`{name: "Odin", salutation: "Hello"}`. Odin preserves that Payload into a
typed Activity command, Core evicts the Workflow, fresh-stack replay reproduces
the same command, and the resolved object becomes the Workflow completion.
The Odin Client receives the greeting plus the Activity/Workflow identity, task
queue, namespace, deadline presence, heartbeat timeout, attempt, and effective
retry attempt limit observed by the Activity callback. Temporal CLI then reads
history independently from the Service and verifies the configured Activity ID, four timeouts, retry timing,
attempt limit, and non-retryable failure type.

Another Odin Client-started execution runs
`odin_failure_details_workflow`. Its Odin Activity returns a retryable
`DependencyRejected` Application Failure with typed JSON details and a 250 ms
next-retry override, while the Workflow configures one maximum attempt. After
Core eviction and replay, Workflow code observes the server wrapper's Activity
ID/type and `Maximum_Attempts_Reached` retry state, then decodes the nested
Application Failure message, type, retryability, details, and delay. It
completes with `"Observed structured Activity failure"`; this proves the fields
crossed both Core and the Service rather than only the local completion encoder.

The `cancel-cleanup` live case blocks in an Odin Activity, requests Workflow
cancellation through the public Odin Client, and requires the cancellation
activation to emit `RequestCancelActivity(seq=1)` plus
`ScheduleActivity(seq=2)` without a terminal command. Workflow code observes
`.Canceled` and the owned reason, opens a detached cancellation scope, and waits
for the cleanup Activity. Core evicts and replays both command batches. Only
after cleanup resolves does Odin emit `CancelWorkflowExecution`; Temporal CLI
must report the final execution as `CANCELED`.

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
intentionally stuck execution through the Odin Client, and then runs the
registered Workflow. Temporal [documents unknown Workflow Tasks](https://docs.temporal.io/task-queue) as
retryable Task failures rather than Workflow Execution failures, so the
explicit termination is test cleanup, not SDK behavior.

`./scripts/test.sh` uses the pinned `test-deps/pbt` submodule by default. During
local PBT library development, run `PBT_ROOT=~/Projects/pbt ./scripts/test.sh`
to compile the SDK properties against that checkout without changing the
reproducible repository pin. The Activity property suite generates in-flight
set sizes, target task tokens, cancel reasons, and heartbeat counts, then checks
that cancellation never leaks across tokens and no heartbeat is transported
after cancellation becomes visible. A second 500-case property generates
retryable/non-retryable Application Failures, type strings, JSON details, and
default/explicit retry delays, then round-trips every field through the Core
completion protobuf and the Workflow-facing structured failure adapter. Both
suites pass against the pin and `~/Projects/pbt`.
The Activity-context property adds 500 timestamp/timeout combinations and
requires the derived deadline to be the earliest applicable endpoint. A unit
test separately destroys the protobuf decode arena before reading every copied
context field and decoding a heartbeat detail.
The Workflow Payload property adds another 500 generated structs, creates and
evicts a typed fiber for each, and requires byte-identical completion after
fresh-stack replay. The Activity-options property generates 500 valid timeout,
retry, queue, ID, cancellation, and eager-execution combinations. It checks
every encoded `ScheduleActivity` field and requires byte-identical commands
after eviction and fresh-stack replay. The Client property generates another
500 typed Workflow results, wraps each in a real close-event history response,
destroys the encoded response, and requires the owned result Payload to retain
the generated value. The query property generates 500 success/failure
responses, destroys the command arena after encoding, and verifies query IDs,
typed success Payloads, and non-retryable Application Failure types after a
fresh decode.

Finally, the script starts `examples/hello_workflow` and invokes it with
`temporal workflow execute` in the background. After the Workflow has
started a durable timer and is visibly waiting for either that timer or an
`approval` signal, `examples/client_control` first sends a typed JSON query
through Core. The worker returns phase `waiting-for-approval` and name `Odin`
without resuming the fiber; the activation can be marked replaying while Core
reconstructs query state. The Client then sends the typed signal. The signal
wins, so the Odin runtime emits `CancelTimer` and schedules an
Activity in the
same completion. The reusable worker's Activity poll thread completes that
Activity while its Workflow poll thread handles Core-directed eviction and
fresh-stack replay. The final completion eviction is required before the test
succeeds. It then runs two cancellation cases through the Odin Client: a
Workflow blocked on a timer must emit
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

To manually exercise cancellation, start the example with
`TEMPORAL_ODIN_SCENARIO=cancel-timer`, `cancel-activity`, or `cancel-cleanup`.
Execute it as above, using the matching input string for either Activity case,
and then run:

```bash
temporal workflow cancel --workflow-id odin-workflow-manual
```

The cleanup case resumes the suspended user fiber, exposes `.Canceled` and the
request reason, runs a second Activity in `detached_cancellation_scope`, and
closes the Workflow as canceled only after that Activity resolves.

For the default scenario, query the running execution from another terminal
before sending the approval signal:

```bash
TEMPORAL_CLIENT_ACTION=query \
TEMPORAL_CLIENT_WORKFLOW_ID=odin-workflow-manual \
./scripts/run-client-control.sh
```

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

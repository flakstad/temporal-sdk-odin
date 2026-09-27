# Development

## Toolchain and pinned dependency

The repository pins `temporalio/sdk-rust` as the `native/sdk-core` submodule at
commit `399cb7adb40f45eefbef90149e212743e474142c`. That upstream release commit
contains `sdk-core` crate 0.9.0 and client/common crates 1.0.0, and is pinned by
`sdk-dotnet` main as inspected on 2026-09-25.

Property tests pin `flakstad/pbt` at
`3b465330bd3a11c45990ef2d7f44f1cf25a18145` as the `test-deps/pbt`
submodule. It is not linked into SDK or application artifacts.
They also pin `flakstad/statecharts` at
`9e82885f0ae4669061aa5505b30db43537e83d19` as
`test-deps/statecharts`. Both dependencies remain test-only.

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
`tests_pbt` against the pinned PBT and statechart submodules. That suite generates
500 signal/timer/cancellation activation batches across nine ordering classes,
forces eviction, reconstructs a fresh fiber, and requires the decisive
`WorkflowActivationCompletion` bytes to match replay exactly. Separate
500-case properties cover Child and external Workflow commands and exact
fresh-stack replay. Another 500 cases vary Continue-As-New identity,
timeouts, Retry Policy, backoff, Payload, and exact replay. A patch property
generates another 500 replay/notified/deprecated combinations, checks Core's
truth table, and requires duplicate calls to emit at most one marker. Another
500 activation contexts vary initial/reset seeds, timestamps, and history
lengths, then require exact random values and byte-identical fresh-stack replay.
PBT is a
test-only dependency. The Update handler suite additionally generates 1,000
close/await/cancel/drain/late-event traces and checks them against an independent
statechart lifecycle model. Its own package tests currently contain an upstream
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

Updates use the same Workflow handle:

```odin
result: Rename_Result
update, update_error := temporal_client.start_update_json(
	&existing,
	"rename_order",
	Rename_Input{name = "priority-order"},
)
defer temporal_client.update_handle_free(&update)
update_error = temporal_client.wait_update_json(&update, &result)
```

`start_update_payload` and `wait_update_payload` are the codec-neutral forms;
`execute_update_payload` composes them as a convenience. Start creates UUIDv4
Update and request IDs unless an explicit Update ID is supplied, asks
`UpdateWorkflowExecution` to wait for `ACCEPTED`, and returns an owned handle.
Wait long-polls `PollWorkflowExecutionUpdate` for `COMPLETED`. The successful
Payload is caller-owned. Update failure returns
`.Workflow_Update_Failed`; `Error.message` owns the Temporal failure message and
`Error.details` owns the encoded Failure protobuf.
If start returns an immediate completed outcome (notably validator rejection),
the handle owns that outcome and wait returns it without an extra poll. The raw
RPC protobuf buffer stays alive until all decoder-borrowed Update IDs, strings,
and Payload bytes have been cloned.

Workflow code uses `set_update_handler_json` or
`set_validated_update_handler_json` for immediate handlers. The
`set_update_routine_json` and `set_validated_update_routine_json` forms also
receive `^Fiber_Context` and may await Activities, timers, or signals.
Validators receive state by value, run only when Core sets `run_validator`, and
return `update_accept()` or `update_reject(message, type)`. Handlers receive
`^State`, run again during replay, and may mutate it deterministically. Typed
handler input is decoded in the Workflow arena so strings/slices retained in
state survive the activation; validator and query inputs retain their shorter
activation lifetime.

Each accepted routine has its own stack, wait state, and protocol ID, but shares
the Workflow instance's deterministic operation tables and sequence numbers.
The first completion contains `Accepted` and any commands scheduled before the
routine yields. A later activation resumes it and carries `Completed` or the
post-acceptance `Rejected` failure. Root Workflow cancellation and
unfinished-handler policy remain experimental. A main Workflow that returns now
emits its terminal command even when an accepted routine is unfinished. The registration default
is `Handler_Unfinished_Policy.Warn_And_Abandon`; `.Abandon` suppresses the
warning. Workflow code that requires handler results calls
`await_all_handlers_finished` before returning.
Multiple routines are driven in registration order, while only the routine
whose wait condition resolved resumes. The unit and live suites start two
Updates, resolve Activity sequence 2 before sequence 1, and require replay to
preserve both protocol IDs and the out-of-order completion sequence.

Workflow-side signal dispatch is codec-neutral. Registered names use
`set_signal_handler_json` for immediate state mutation or
`set_signal_routine_json` for an independent stack that may await Activities,
timers, or other unhandled signals. Each routine owns its execution state and
shares deterministic operation tables with main and Update routines. It counts
toward `all_handlers_finished`; closing first records the same default
warn-and-abandon policy used by Updates. Handler input is decoded in the
Workflow arena because user state may retain it after the activation.

Unregistered names keep the existing buffer behavior. Use
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

The `cancel-update` case opens a detached scope inside an Update handler before
it schedules its Activity. Operations capture that detached/root relationship
at creation time. The main Workflow receives root cancellation, cancels its own
timer/signal wait, then opens its own detached scope around
`await_all_handlers_finished`. The cancellation activation emits neither
`RequestCancelActivity` nor a terminal command. When the Activity resolves, the
runtime emits the Update's `Completed` response before
`CancelWorkflowExecution`; the Update caller receives its typed result and the
Workflow then reaches `CANCELED`. The unit test evicts the instance and requires
all four activation completions to be byte-identical on fresh-stack replay.

The `signal-handler` live case sends `set_greeting_name_async` through the
public Odin Client. The registered Signal routine schedules an Odin Activity
and suspends without waking main. After resolution it mutates Workflow state; a
typed Query observes the new name. A later unregistered `approval` signal takes
the buffered-await path, cancels main's timer, and completes the Workflow. Core
evicts between stages, and replay reconstructs both the handler stack and its
Activity command/resolution.

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
after eviction and fresh-stack replay. A separate 500-case Local Activity
options property covers
all three cancellation modes, timeouts, retry fields, local retry threshold,
marker-input inclusion, `ScheduleLocalActivity` encoding, and exact replay. The
Child Workflow options property
generates another 500 valid ID, queue, timeout, Retry Policy, cancellation-type,
and Parent Close Policy combinations, plus generated signal names and JSON
payloads. It checks every encoded `StartChildWorkflowExecution` and
`SignalExternalWorkflowExecution` field and requires both commands to be
byte-identical after eviction and fresh-stack replay. The external Workflow
property generates 500 namespace/Workflow ID/optional Run ID identities,
signal names, JSON payloads, and cancellation reasons. It checks exact
`NamespacedWorkflowExecution` targets for signal and cancellation commands and
requires both completions to be byte-identical after eviction and fresh-stack
replay. The Continue-As-New property generates 500 replacement Workflow types,
task queues, input Payloads, valid Run/Task timeout pairs, Retry Policies, and
backoffs. Empty identity options exercise reuse of the current registration;
every `ContinueAsNewWorkflowExecution` must encode exactly and remain
byte-identical after fresh-stack replay. The Client property generates another
500 typed Workflow results, wraps each in a real close-event history response,
destroys the encoded response, and requires the owned result Payload to retain
the generated value. The query property generates 500 success/failure
responses, destroys the command arena after encoding, and verifies query IDs,
typed success Payloads, and non-retryable Application Failure types after a
fresh decode.
The Update properties add 500 immediate protocol sequences across validator
rejection, accepted completion, and accepted handler rejection. It checks the
protocol instance ID, requires `Accepted` to precede the terminal response, and
round-trips typed result Payloads plus Application Failure types. A second 500
case property varies which of two accepted Updates finishes first and verifies
that each result stays attached to its own protocol instance ID.
The Update lifecycle property compiles a small pinned statechart and generates
1,000 traces through seven event kinds: handler start/finish, main return,
explicit await, immediate Workflow cancellation, cancellation with handler
draining, and late resolution. It requires at most one Workflow terminal
command, no state changes after close, no premature close during an explicit
await or cancellation drain, and identical observations when replaying the
same trace. Use
`STATECHARTS_ROOT=~/Projects/statecharts ./scripts/test.sh` to exercise a local
statecharts checkout without changing the pin; both root overrides can be set
together.

Finally, the script starts `examples/hello_workflow` and invokes it with
`temporal workflow execute` in the background. After the Workflow has
started a durable timer and is visibly waiting for either that timer or an
`approval` signal, `examples/client_control` first proves a validator rejection,
then starts two typed Updates using the public start/wait handle API. Their
independent routines wait for separate Odin Activities; the second Activity
finishes first and the first Update finishes last. A typed query returns phase
`waiting-for-approval` and the last-completed name without resuming the main
fiber; Core evicts and replays both accepted Updates, Activities, and terminal
responses with validation disabled while reconstructing query state. The Client
then sends the typed signal. The signal
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
The suite also cancels a Workflow with a detached, Activity-awaiting Update in
flight. It requires no Activity-cancel command, waits for the Update client to
receive its result, and only then accepts the Workflow's canceled close status.
In a separate execution it dispatches a typed Signal to an Activity-awaiting
handler, queries the handler's state mutation, and then completes main through
the ordinary buffered Signal API.

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

## Child, external, Continue-As-New, and patch acceptance test

`examples/child_workflow` registers successful, failing, and cancellation-aware
parent/child Workflows on task queue `odin-child-workflow`. The normal parent
assigns an explicit deterministic child ID, starts the child through SDK Core,
waits first for start and then completion, and returns the child's JSON result.
Run it with the CLI dev server:

```bash
temporal server start-dev
./scripts/run-child-workflow.sh

temporal workflow execute \
  --workflow-id odin-parent-manual \
  --type odin_parent_workflow \
  --task-queue odin-child-workflow \
  --input '"Odin"'
```

The example stops after two durable Workflow Executions reach terminal state
and Core evicts both cached fibers. `scripts/integration-test.sh` runs these
server-backed paths automatically:

- successful typed child result delivery;
- a failing child whose parent catches a structured Temporal child wrapper and
  typed Application Failure detail;
- cancellation of a waiting parent through the public Odin Client, which emits
  `CancelChildWorkflowExecution(seq=1)` and does not close the parent until Core
  resolves the child as canceled;
- signaling a child through its Workflow-local Future and waiting for Core's
  `ResolveSignalExternalWorkflow` acknowledgement;
- explicit child cancellation through the Future while the parent remains live
  and completes normally after observing the canceled child result;
- a second start with the same running child ID, producing Core's
  `WorkflowAlreadyExists` start resolution before the first child is canceled;
- two independent Workflows where one sends an external signal, waits for
  `ResolveSignalExternalWorkflow`, requests target cancellation, waits for
  `ResolveRequestCancelExternalWorkflow`, and completes while the target closes
  as canceled;
- a Workflow that emits `ContinueAsNewWorkflowExecution` with replacement
  input/timeouts, is evicted, starts a second run under the same Workflow ID,
  and returns the second run's result to Temporal CLI; and
- a Workflow that takes its new `patched` branch, emits `SetPatchMarker`, and
  leaves a server-side `core_patch` marker visible through `temporal workflow show`.

The unit suite additionally evicts the parent immediately after its start
command, replays onto a fresh stack, compares the completions byte for byte,
then applies separate start and completion resolutions. It verifies the exact
cancellation reason, structured failure ownership after the activation decode
arena is gone, and lossless propagation of an unhandled original Temporal
Failure wrapper. The 500-case Child Workflow property varies start options,
signal names, and signal Payloads, then requires exact fresh-stack replay of
both commands. A cancellation unit test also requires an in-flight child signal
to emit both `CancelSignalWorkflow` and `CancelChildWorkflowExecution` before
the parent closes as canceled. External-operation unit tests inspect namespace,
Workflow ID, Run ID, signal Payload, cancellation reason, success/failure cause,
and fresh-stack replay bytes. Continue-As-New unit tests additionally prove the
non-returning terminal path, default type/queue reuse, options encoding, invalid
option failure, second-run initialization, and exact replay. Patch unit tests
cover fresh history, replay with and without a preemptive notification, Core's
actual initialize-then-notify job order, memoization, and deprecated markers.

For the default scenario, query the running execution from another terminal
before sending the approval signal:

```bash
TEMPORAL_CLIENT_ACTION=query \
TEMPORAL_CLIENT_WORKFLOW_ID=odin-workflow-manual \
./scripts/run-client-control.sh
```

Execute the example's validated Activity-awaiting Update with:

```bash
TEMPORAL_CLIENT_ACTION=update \
TEMPORAL_CLIENT_WORKFLOW_ID=odin-workflow-manual \
TEMPORAL_CLIENT_UPDATE_INPUT=Temporal \
./scripts/run-client-control.sh
```

The default endpoint is `http://localhost:7233` inside the Core connection
options. The wrapper accepts another URL, but TLS, API keys, metadata, and
configuration profiles are not exposed yet.

### Local Activity live path

The main Workflow example registers the same `hello` procedure for remote and
Local Activity dispatch. To run only the Local Activity scenario manually:

```bash
temporal server start-dev

TEMPORAL_ODIN_SCENARIO=local-activity \
./scripts/run-hello-workflow.sh
```

In a second terminal:

```bash
temporal workflow execute \
  --workflow-id odin-local-manual \
  --type odin_local_activity_workflow \
  --task-queue odin-hello-workflow \
  --input '"Local"'
```

The result is `Hello Local`. `temporal workflow show --workflow-id
odin-local-manual --output json` contains a `core_local_activity` marker and an
`input` detail because the example opts into marker argument recording. The
full `./scripts/integration-test.sh` checks this path automatically.

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

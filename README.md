# Temporal SDK for Odin — experimental

An experimental Odin language SDK built on [Temporal SDK Core](https://github.com/temporalio/sdk-rust),
not a reimplementation of Temporal Server or its gRPC protocol.

Current status: Milestones 0–2 plus bounded explicit-state-machine and stackful
fiber Workflow experiments. The repository builds the official Rust Core C
bridge, connects to a Temporal CLI dev server, executes a remote Activity in
Odin, and now starts a Workflow and obtains its typed result through a public
Odin Client over Core's generic WorkflowService RPC bridge. The Client also
sends typed signals, requests cancellation, and terminates executions. It now
preserves terminal Workflow and Update Failure protobufs and decodes all ten
current Temporal Failure variants into an allocator-owned recursive Odin tree.
The live suite proves an unhandled `ActivityFailure -> ApplicationFailure`
chain from Odin Activity through Server history and back to the Odin Client. It
also
owns and types `DescribeWorkflowExecution` results—including identity, status,
timeouts, priority, memo/Search Attributes, and pending execution summaries—and
iterates full Workflow history with explicit pagination tokens. Each history
event includes stable metadata plus an owned canonical protobuf representation.
The live suite requests one-event pages against the CLI dev server and follows
every returned token. The Client also lists Workflow executions through
Temporal Visibility and counts raw or grouped queries. List pages own their
summaries, memo/Search Attribute Payloads, and opaque continuation token;
grouped count values remain codec-neutral Temporal Payloads. Client connection
options now expose Core's TLS/mTLS,
API-key, ASCII/binary RPC metadata, retry, keepalive, HTTP CONNECT proxy, DNS
load-balancing, compression, identity, and payload-warning controls. API-key
connections enable TLS automatically, and metadata/API keys can be replaced on
a live connection. It proves
Schedule lifecycle operations through the same Core RPC boundary: create a
cron/interval/structured-calendar Schedule with exclusions and optional TZif
data, replace it with an optimistic conflict token, describe/list/count it,
query matching times, backfill, pause/unpause, trigger it immediately, and
delete it. The CLI-devserver suite proves replacement, conflict-token rotation
and guarding, matching-time queries, and waits for the triggered Odin Workflow
result. On CLI Server 1.29.1 a stale guarded update is ignored by the legacy
scheduler even though the RPC itself succeeds; callers that require observed
application must Describe and verify the resulting definition.
`CountSchedules` is generated
and unit-tested but CLI Server 1.29.1 returns `UNIMPLEMENTED`; newer-server
acceptance remains outstanding. It also proves
typed read-only Workflow queries through the public Odin Client, including
query failure responses, eviction, and fresh-stack replay. Typed Workflow
Updates now validate, run on independent stackful routines, await Activities,
mutate Workflow-owned state, return a typed result in a later activation, and
replay without rerunning validators. Two accepted Updates can now remain live
at once, resolve their Activities out of order, and replay with stable protocol
identity. The Client exposes owned Update handles that separate acceptance from
waiting for the result. Concurrent handler close semantics now match the
established language SDKs: unfinished Update routines are abandoned with a
warning by default, registration can suppress that warning, and
`workflow.await_all_handlers_finished` explicitly keeps the main Workflow open.
Typed Signals may now be registered as immediate handlers or independent
stackful routines. An asynchronous Signal handler can await an Activity, mutate
Workflow-owned state, participate in handler draining, and replay on a fresh
stack; the CLI-devserver suite proves the state through a public Client Query.
Child Workflows now have a vertical start/result/failure/cancellation path:
Odin emits Core's `StartChildWorkflowExecution`, awaits its separate start and completion
resolutions, decodes typed JSON results and structured Child Workflow failures,
signals a started child through `SignalExternalWorkflowExecution`, and requests
explicit or inherited cancellation through `CancelChildWorkflowExecution`. The
signal API waits for Core's resolution and reports server failure causes. Unhandled
child failures preserve Core's original Temporal Failure wrapper and cause
chain when the parent fails. The CLI-devserver suite proves successful child
completion, a caught server-generated child failure, duplicate-ID start failure,
and parent cancellation that waits for child cancellation before closing.
Workflow code can also signal and request cancellation of arbitrary external
Workflow executions by namespace/Workflow ID/optional Run ID, suspending until
Core resolves each outbound operation. The live suite proves this against two
independent executions. `continue_as_new_json/payload/owned` now terminate the
current fiber run and carry new input plus timeout/retry/backoff options into a
new durable run; the CLI-devserver suite follows that chain to completion. The
Workflow runtime now implements Core's patch protocol through
`workflow.patched` and `workflow.deprecate_patch`. It consumes preemptive
`NotifyHasPatch` jobs, memoizes each decision per run, emits at most one
`SetPatchMarker`, and takes the legacy branch when replay encounters no marker.
The CLI-devserver suite verifies the resulting `core_patch` history marker. The
same activation-context layer now exposes `workflow.now`, `history_length`,
`is_replaying`, deterministic `random_u64`/`random_f64`, and `uuid_v4` without
consulting host time or OS entropy. Core-provided start/reset seeds are applied
before user code resumes. Local Activities now run through Core's dedicated
local task path: Workflow code emits `ScheduleLocalActivity`, the same Odin
Activity registry receives a callback with `activity.info(ctx).is_local`, and
`ResolveActivity(is_local=true)` resumes the fiber. The API exposes typed JSON
and generic Payload forms, timeout/cancellation options, marker-input control,
and exact eviction/replay. The live suite verifies the server-side
`core_local_activity` marker. Timer-backed Local Activity retry now follows
Core's `DoBackoff` contract: the runtime schedules a durable Workflow timer,
then emits a fresh Local Activity command carrying Core's attempt and original
schedule time while preserving the Odin Future. Both transitions replay to
byte-identical commands, and the CLI-devserver suite exercises a real failed
first attempt followed by timer-backed success.
Workflow initialization metadata is now exposed through borrowed
`workflow.Info`, including execution identity, attempt/run-chain fields,
timeouts, start/expiration time, parent/root identity, Retry Policy, priority,
cron schedule, and continuation cause. Initial headers plus initial and locally
updated memo/Search Attribute Payloads are readable through codec-neutral and
typed JSON helpers. The Odin Client supplies all three initial Payload maps
through an explicitly owned start-metadata builder. Workflow starts also expose
ID reuse/conflict policies, Retry Policy, cron schedule, start delay, and task
priority. The live suite proves delayed start plus `Use_Existing` by requiring
two starts against the CLI dev server to return the same run ID. Eager Workflow
start is also live-proven: a validated worker on the same Core connection
reserves a slot, Core dispatches the response task directly into that worker,
and history records `eagerExecutionAccepted`.
Signal-With-Start is live-proven as a separate atomic Client operation: the
first call creates an execution, the second reuses its run, and both Signal
headers reach Odin Workflow code. Temporal's request has one Signal header map,
not independent start and Signal headers; the Odin API makes that limitation
explicit and rejects eager-start intent on this RPC.
The same owned `Headers` builder now drives ordinary Client Signals and
Workflow-originated remote/Local Activities, Child Workflow starts, and
child/external Workflow Signals. Incoming Signal metadata remains borrowed
from the Workflow instance—including the signal/timer race helpers—and
Activity headers remain borrowed from `activity.Context`. Unit/property tests
cover wire encoding, ownership after Core decode arenas are destroyed, and
byte-identical replay; the CLI-devserver suite proves both Client Signal to
Workflow and Workflow command through Core to an Odin Activity.
Mutations emit Core's `ModifyWorkflowProperties` and
`UpsertWorkflowSearchAttributes` commands, update the local Workflow view
immediately, support explicit removal, and replay byte-for-byte. The live
CLI-devserver case verifies both history event types without adding another C
symbol: these features are entirely activation/completion protobuf behavior.
Workers must also set `Run_Options.enable_local_activities`; the opt-in keeps
the still-unhardened mixed remote-cancellation/local shutdown path out of
workers that do not use Local Activities.
The
offline suite requires
the start command to be byte-identical after eviction and fresh-stack replay.
It also proves
multiple outstanding Activities, catchable Activity failure,
buffered Workflow signals, and a durable signal/timer race across fresh-stack
replay. The live Workflow starts a timer, lets an `approval` signal win, emits
`CancelTimer` alongside an Activity command, and reproduces both commands after
eviction. Workflow cancellation now resumes suspended Odin code with a canceled
result, exposes the owned reason, and permits deterministic Activity/timer work
in an explicit detached scope before `CancelWorkflowExecution`. Detached
operations may be started before cancellation, or afterward for cleanup. A
canceling main Workflow can also drain a detached Update handler; the live
suite proves that its Update result reaches the caller before the Workflow is
acknowledged as canceled. A
reusable worker now registers both Activity and Workflow procedures, routes run IDs to the
correct fiber runtime, and polls both Core task streams concurrently. Its
bounded reusable Activity thread pool runs callbacks concurrently, exposes
cooperative cancellation and typed heartbeat payloads through
`activity.Context`, and has live tests for overlap and heartbeat-driven
cancellation. The context preserves Core's cancellation reason and all six
detail flags (`not_found`, `cancelled`, `paused`, `timed_out`,
`worker_shutdown`, and `reset`). Typed callbacks may return a canceled result
with a user Payload; a Workflow using `Wait_Cancellation_Completed` can request
the cancellation explicitly and decode that Payload from the resulting
`CanceledFailureInfo`. `activity.info(ctx)` now exposes the Core-delivered execution
identity, timestamps/timeouts, effective Retry Policy, priority, interceptor
headers, and previous heartbeat details as callback-scoped borrowed values; a retry integration test
recovers typed heartbeat progress on attempt 2. Unknown Workflow types fail the retryable Workflow Task without
terminating the worker. Typed Activities can return explicit retryable or
non-retryable Application Failures with a failure type, JSON details, and an
optional next-retry delay; the live suite proves Core/Server redelivery from
attempt 1 to attempt 2. Workflow initialization, Activity arguments/results,
and Workflow completion now preserve generic Payloads; typed JSON Workflows run
end-to-end across eviction and fresh-stack replay. Workflow-scheduled Activities
now accept IDs, task queues, all four server timeouts, Retry Policies,
cancellation type, and eager-execution control. It does **not** provide general
nested cancellation-token trees, client
cancellation of an accepted Update, multiple Activity arguments,
in-process panic containment, or a stable public API.
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
replay completions. A pinned [statecharts](https://github.com/flakstad/statecharts)
model independently checks Update close/await/late-event lifecycle traces.

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

The minimal parent/child sample can also be run independently. Start the worker,
then execute the parent from another terminal:

```bash
./scripts/run-child-workflow.sh

temporal workflow execute \
  --workflow-id odin-parent-manual \
  --type odin_parent_workflow \
  --task-queue odin-child-workflow \
  --input '"Odin"'
```

With `examples/hello_workflow` polling, the public Client example performs
`StartWorkflowExecution`, long-polls `GetWorkflowExecutionHistory` for the
close event, and decodes the result Payload without Temporal CLI:

```bash
./scripts/run-client-start.sh
```

The self-contained eager example creates a public Client and Worker on the same
Core connection and proves inline first-task dispatch:

```bash
./scripts/run-eager-workflow.sh
```

With `examples/hello_workflow` running in the `patch` scenario, exercise the
complete Schedule lifecycle without Temporal CLI acting as the client:

```bash
TEMPORAL_ODIN_SCENARIO=patch ./scripts/run-hello-workflow.sh

TEMPORAL_SCHEDULE_ID=odin-schedule-manual \
./scripts/run-schedule-lifecycle.sh
```

The public Client API remains explicit:

```odin
client, err := temporal_client.connect()
defer temporal_client.close(&client)

metadata := temporal_client.Start_Metadata{}
err = temporal_client.start_metadata_init(&metadata)
defer temporal_client.start_metadata_free(&metadata)
err = temporal_client.start_metadata_add_header_json(&metadata, "trace-id", trace_id)
err = temporal_client.start_metadata_add_memo_json(&metadata, "customer", customer_id)
err = temporal_client.start_metadata_add_search_attribute_json(
    &metadata,
    "OrderStatus",
    .Keyword,
    "pending",
)
err = temporal_client.start_metadata_set_static_summary(&metadata, "Order 42")
err = temporal_client.start_metadata_set_static_details(
    &metadata,
    "Tracks payment and fulfillment for order 42.",
)

retry := temporal_client.Retry_Policy{
    initial_interval = time.Second,
    backoff_coefficient = 2,
    maximum_interval = time.Minute,
    maximum_attempts = 5,
}
priority := temporal_client.Priority{priority_key = 2}

handle, err := temporal_client.start_workflow_json(
    &client,
    "order_workflow",
    order,
    {
        id = "order-42",
        task_queue = "orders",
        id_reuse_policy = .Allow_Duplicate,
        id_conflict_policy = .Fail,
        retry_policy = &retry,
        start_delay = 500 * time.Millisecond,
        priority = &priority,
        metadata = &metadata,
        enable_eager_workflow_start = true,
    },
)
defer temporal_client.workflow_handle_free(&handle)

signal_headers := temporal_client.Headers{}
err = temporal_client.headers_init(&signal_headers)
defer temporal_client.headers_free(&signal_headers)
err = temporal_client.headers_add_json(&signal_headers, "caller", "orders-api")

signaled, err := temporal_client.signal_with_start_workflow_json(
    &client,
    "order_workflow",
    order,
    "approve",
    approval,
    {
        start_options = {id = "order-42", task_queue = "orders"},
        signal_headers = &signal_headers,
    },
)
defer temporal_client.workflow_handle_free(&signaled)
// signaled.started_by_request distinguishes create from reuse.

result: Order_Result
err = temporal_client.get_result_json(&handle, &result)

description, err := temporal_client.describe_workflow(&handle)
defer temporal_client.workflow_description_free(&description)
// description.static_summary/static_details are owned strings when present.

page, err := temporal_client.fetch_history_page(
    &handle,
    {maximum_page_size = 100, filter = .All, skip_archival = true},
)
defer temporal_client.history_page_free(&page)

visibility_page, err := temporal_client.list_workflows(
    &client,
    {page_size = 100, query = `ExecutionStatus = "Running"`},
)
defer temporal_client.workflow_list_page_free(&visibility_page)

counts, err := temporal_client.count_workflows(
    &client,
    `ExecutionStatus = "Running" GROUP BY WorkflowType`,
)
defer temporal_client.workflow_count_free(&counts)

intervals := [?]temporal_client.Schedule_Interval{{every = 24 * time.Hour}}
schedule, err := temporal_client.create_schedule_json(
    &client,
    "daily-order-reconciliation",
    "reconcile_orders",
    Reconcile_Input{region = "no"},
    {
        spec = {intervals = intervals[:]},
        action = {
            workflow_id = "daily-reconciliation",
            task_queue = "orders",
        },
        overlap_policy = .Skip,
    },
)
defer temporal_client.schedule_handle_free(&schedule)

description, err = temporal_client.describe_schedule(&schedule)
defer temporal_client.schedule_description_free(&description)
err = temporal_client.update_schedule_json(
    &schedule,
    "reconcile_orders",
    Reconcile_Input{region = "no"},
    replacement_options,
    description.conflict_token, // guarded; empty means unconditional
)
matching, err := temporal_client.list_schedule_matching_times(
    &schedule,
    time.now(),
    time.time_add(time.now(), 7 * 24 * time.Hour),
)
defer temporal_client.schedule_matching_times_free(&matching)
err = temporal_client.pause_schedule(&schedule)
err = temporal_client.trigger_schedule(&schedule)
err = temporal_client.delete_schedule(&schedule)
```

Temporal Cloud API-key authentication uses the same Core connection:

```odin
rpc_metadata := [?]temporal_client.RPC_Metadata_Entry{{
    key = "x-application",
    value = transmute([]u8)string("orders"),
}}

client, err := temporal_client.connect_with_options({
    target_url = "https://my-namespace.account-id.tmprl.cloud:7233",
    namespace = "my-namespace.account-id",
    api_key = temporal_api_key,
    metadata = rpc_metadata[:],
})
defer temporal_client.close(&client)
```

Supplying an API key automatically supplies an empty TLS configuration, which
uses system root certificates and the target hostname. Self-hosted TLS can pass
`TLS_Options` with custom root CA bytes and an optional domain. mTLS additionally
requires both PEM client-certificate and private-key bytes. `update_rpc_metadata`
replaces connection-wide ASCII/binary headers; `update_api_key` replaces or,
with an empty string, clears the bearer token. Certificate files are deliberately
loaded by application code rather than hidden inside the SDK. Connection option
strings and byte slices are borrowed only until the blocking connect call
returns; Core owns its connection-state copies afterward.

The Client owns its Core runtime, connection, and namespace. A Workflow handle
owns its ID/run-ID strings but borrows the Client, so handles must be freed
before `client.close`. An Update handle follows the same rule and owns the
Workflow/run/Update IDs needed for later polling. The Payload variants expose the codec-neutral boundary;
the JSON variants currently send and receive exactly one Payload.
`Start_Metadata` explicitly owns cloned header, memo, Search Attribute, and
static summary/details Payloads. Static summary/details use Temporal's
`UserMetadata` fields and the required default `json/plain` string convention;
unlike ordinary user Payloads, Temporal defines these two UI fields as strings.
Its other `_payload` methods are codec-neutral, while JSON methods are
convenience adapters. Options borrow the builder only until the synchronous
start call returns, so it may be freed immediately afterward. Reusing a key
replaces its value, and custom Search Attribute names must already exist in the
Temporal namespace. Retry Policy and Priority are borrowed only during request
encoding. Zero values inside them preserve Temporal Service defaults. Start
delay and cron are mutually exclusive; invalid timeout, retry, priority, and ID
policy combinations are rejected before the RPC.
Eager start is only accepted when a validated Workflow worker for the
namespace/task queue is registered on the same Core connection;
`handle.eager_execution_accepted` reports whether the start response actually
contained the inline task.

`Workflow_Description`, `History_Page`, `Workflow_List_Page`, and
`Workflow_Count`, plus Schedule descriptions/list/counts/matching-times, are allocator-owned snapshots; they do not borrow Core
response buffers or protobuf decode arenas. Call their matching `_free`
procedures. List/count query strings are passed directly to Temporal Visibility;
the SDK does not yet add a query DSL. List and history `next_page_token` values are opaque and
must be passed unchanged to the next call. Event `protobuf` bytes are
canonically re-encoded from the pinned schema, so unknown fields from a newer
server are not retained. The typed history API currently rejects raw history
blobs rather than silently returning an incomplete page.

Existing executions can be addressed without an RPC when creating the handle:

```odin
handle, err := temporal_client.get_workflow_handle(&client, "order-42")
defer temporal_client.workflow_handle_free(&handle)

signal_headers := temporal_client.Headers{}
err = temporal_client.headers_init(&signal_headers)
defer temporal_client.headers_free(&signal_headers)
err = temporal_client.headers_add_json(&signal_headers, "caller", "orders-api")
err = temporal_client.signal_workflow_json_with_options(
    &handle,
    "approve",
    approval,
    {headers = &signal_headers},
)
status: Order_Status
err = temporal_client.query_workflow_json(
    &handle,
    "order_status",
    Order_Status_Query{},
    &status,
)
query_headers := temporal_client.Headers{}
err = temporal_client.headers_init(&query_headers)
defer temporal_client.headers_free(&query_headers)
err = temporal_client.headers_add_json(&query_headers, "caller", "orders-api")
err = temporal_client.query_workflow_json_with_options(
    &handle,
    "order_status",
    Order_Status_Query{},
    &status,
    {headers = &query_headers},
)
// Closed executions can be rejected before handler delivery.
err = temporal_client.query_workflow_json_with_options(
    &handle,
    "order_status",
    Order_Status_Query{},
    &status,
    {reject_condition = .Not_Open},
)
if err.kind == .Workflow_Query_Rejected && err.has_workflow_status {
    fmt.println("query rejected for", err.workflow_status)
}
updated: Rename_Result
update_headers := temporal_client.Headers{}
err = temporal_client.headers_init(&update_headers)
defer temporal_client.headers_free(&update_headers)
err = temporal_client.headers_add_json(&update_headers, "caller", "orders-api")
update, err := temporal_client.start_update_json_with_options(
    &handle,
    "rename_order",
    Rename_Input{name = "priority-order"},
    {headers = &update_headers},
)
defer temporal_client.update_handle_free(&update)
err = temporal_client.wait_update_json(&update, &updated)
err = temporal_client.cancel_workflow(&handle, "customer canceled")
```

`execute_update_json` remains the one-call convenience wrapper around
`start_update_json` plus `wait_update_json`; the codec-neutral and option-aware
variants are available when callers need custom Payloads or propagation headers.

Terminal Workflow and Update errors may carry a complete Temporal Failure:

```odin
result_error := temporal_client.get_result_json(&handle, &result)
if result_error.kind == .Workflow_Failed {
    failure, decode_error := temporal_client.decode_error_failure(&result_error)
    defer temporal_failure.failure_free(&failure)

    activity_failure := temporal_failure.find_kind(&failure, .Activity)
    application_failure := temporal_failure.find_kind(&failure, .Application)
}
temporal_client.error_free(&result_error)
```

The decoded tree owns every string, Payload, repeated field, and cause node
through the allocator passed to `decode_error_failure`. It remains valid after
the Client error is freed. `temporal/failure` represents Application, Timeout,
Canceled, Terminated, Server, Reset Workflow, Activity, Child Workflow, Nexus
Operation, and Nexus Handler failures; unknown/absent variants retain their
common fields as `.Unknown`.

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

Outbound operations on another Workflow use an explicit execution identity and
wait for Core's durable acknowledgement:

```odin
target := workflow.External_Workflow_Execution{
    namespace = "default",
    workflow_id = order_workflow_id,
    // Empty run_id targets the current execution.
}
signal := workflow.signal_external_workflow_json(
    ctx,
    target,
    "approve",
    approval,
)
if signal.status == .Succeeded {
    cancel := workflow.request_cancel_external_workflow(
        ctx,
        target,
        "superseded order",
    )
}
```

The identity and signal name are borrowed for the call and copied into the
Workflow arena before suspension. The `_payload` signal form clones a borrowed
Payload; `_owned` consumes its Payload. Failure results map Core's not-found,
namespace-not-found, and signal-count-limit causes. This remains a low-level
execution reference rather than a general client-style handle.

Continue-As-New is deliberately non-returning:

```odin
if state.events_processed >= 10_000 {
    workflow.continue_as_new_json(
        ctx,
        compacted_state,
        {
            workflow_run_timeout = 24 * time.Hour,
            workflow_task_timeout = 10 * time.Second,
        },
    )
}
```

Empty Workflow type and task queue options reuse the current registration.
The current slice also supports Retry Policy and first-task backoff.
Continue-As-New-specific memo, header, Search Attribute, and versioning
overrides are not exposed yet.
The primitive may only be called from the main Workflow context; misuse fails
the Workflow with an SDK Application Failure rather than returning past the
terminal command.

Version Workflow code with a stable patch ID:

```odin
if workflow.patched(ctx, "orders-use-v2") {
    // New deterministic command path.
} else {
    // Historical path for executions started before the change.
}
```

Repeated calls with the same ID return the memoized decision and emit no extra
command. On a new execution `patched` returns `true` and records the marker; on
replay it returns `true` only when Core supplied `NotifyHasPatch`. After all old
executions have passed the migration window, replace the branch with
`workflow.deprecate_patch(ctx, "orders-use-v2")` before eventually removing the
call. Patch IDs are part of durable history and must not be reused for a
different change. This is Workflow code versioning, not Worker Deployment
Versioning; deployment routing is not exposed yet.

Workflow time and random values must use Temporal-aware primitives:

```odin
current_time, available := workflow.now(ctx)
choice := workflow.random_u64(ctx)
ratio := workflow.random_f64(ctx)
request_id := workflow.uuid_v4(ctx)
```

`now` is constant within an activation and returns Core's history-derived
timestamp. The random stream is seeded by `InitializeWorkflow` and reset by a
pre-scanned `UpdateRandomSeed`; its SplitMix64 sequence is replay compatibility
state and is not cryptographically secure. `uuid_v4` consumes that stream and
returns a lowercase RFC 4122 variant/version string. Named isolated random
streams are not implemented yet.

Workflow identity and mutable properties use the same activation-owned state:

```odin
metadata, ok := workflow.info(ctx)
customer: string
found, decode_error := workflow.memo_json(ctx, "customer", &customer)

_ = workflow.upsert_memo_json(ctx, "status", "charged")
_ = workflow.remove_memo(ctx, "legacy-status")
_ = workflow.upsert_search_attribute_json(
    ctx,
    "OrderStatus",
    .Keyword,
    "charged",
)
_ = workflow.remove_search_attribute(ctx, "LegacyOrderStatus")
```

`Info`, `memo_payload`, and `search_attribute_payload` return borrowed values
valid until the run is evicted. Mutation functions clone their inputs into the
run arena and provide deterministic read-after-write behavior. The raw Payload
forms keep codecs isolated; JSON is only a convenience adapter. Search
Attribute types map to Temporal's exact `Bool`, `Int`, `Double`, `Keyword`,
`Text`, `Datetime`, and `KeywordList` Payload metadata. The caller must register
custom Search Attribute names with the Temporal Service and keep the declared
type consistent with the encoded value.

Typed query handlers are registered from inside Workflow code so replay builds
the same handler set and callback state:

```odin
Status_State :: struct { phase: string, order_id: string }
Status_Query :: struct {}
Status_Result :: struct { phase: string, order_id: string }

status_query :: proc(
    query: ^workflow.Query_Context,
    state: Status_State,
    _: Status_Query,
) -> Status_Result {
    caller: string
    _, _ = workflow.query_header_json(query, "caller", &caller)
    return {phase = state.phase, order_id = state.order_id}
}

state := Status_State{phase = "waiting", order_id = input.order_id}
if !workflow.set_query_handler_json_with_context(ctx, "order_status", &state, status_query) {
    return {}
}
```

The runtime answers a `QueryWorkflow` activation without resuming the suspended
fiber. Handler state is borrowed from the live Workflow stack, passed to the
callback by value, and must remain valid while the Workflow runs. The current
experimental boundary accepts exactly one input Payload and one result Payload.
The contextual registration additionally receives invocation-scoped query
identity and header accessors without exposing the command-capable
`Fiber_Context`; header Payload views must not escape the callback.
Unknown handlers and codec errors become failed query responses rather than
failed Workflow Tasks.

Typed Updates use a mutable state pointer plus an optional read-only validator.
The routine form receives a Workflow context and can suspend:

```odin
rename_validator :: proc(
    update: ^workflow.Update_Context,
    state: Status_State,
    input: Rename_Input,
) -> workflow.Update_Validation {
    if len(input.name) == 0 {
        return workflow.update_reject("name is required", "InvalidName")
    }
    return workflow.update_accept()
}

rename :: proc(
    ctx: ^workflow.Fiber_Context,
    update: ^workflow.Update_Context,
    state: ^Status_State,
    input: Rename_Input,
) -> Rename_Result {
    previous := state.order_id
    _ = workflow.execute_activity(ctx, "audit_rename", input.name)
    state.order_id = input.name
    return {previous = previous, current = state.order_id}
}

_ = workflow.set_validated_update_routine_json_with_context(
    ctx,
    "rename_order",
    &state,
    rename_validator,
    rename,
)
```

`Update_Context` exposes the server Update ID, name, client identity, and
headers. Its run-arena-owned values survive handler suspension, and
`workflow.current_update(ctx)` returns the same invocation while the routine is
running. They must not escape the Workflow run. The context-aware form is the
first propagation/interceptor seam; a composable interceptor chain is not yet
implemented.

The runtime emits `Accepted` with the Activity command, preserves the Update
routine by `protocol_instance_id`, and emits `Completed` after the Activity
resolution in a later activation. Validator rejection emits only `Rejected`;
validators are skipped during replay exactly as directed by Core. The original
synchronous handler APIs remain available for immediate Updates.

The main Workflow is allowed to close while a routine is unfinished. The
default handler option reports that abandonment through the worker; use
`.Abandon` only when silence is intentional. When completion depends on every
accepted handler result, wait explicitly before returning:

```odin
workflow.await_all_handlers_finished(ctx)
return result
```

Catchable Activity failures expose both the outer Activity metadata and the
nested Application Failure. The same result also exposes the complete recursive
Temporal Failure tree:

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

    tree := workflow.activity_failure_tree(&result.failure)
    timeout := temporal_failure.find_kind(tree, .Timeout)
    server := temporal_failure.find_kind(tree, .Server)
}
```

`child_workflow_failure_tree` provides the same boundary for Child Workflow
results. These pointers and the compatibility views borrow the Workflow
instance arena, remain valid for that run, and must not be passed to
`failure_free`. They include every current Failure variant, recursive causes,
encoded attributes, and detail/heartbeat Payloads. Unhandled failures still
propagate the preserved Temporal Failure protobuf byte-for-byte.
Failed Child/external Workflow signal results and failed external cancellation
results expose the same borrowed tree through their `failure` field, alongside
the operation-specific failure-cause enum.

Cancellation is cooperative in Workflow code. Result-returning waits report
`.Canceled`. An Activity or timer created inside a detached scope does not
inherit root Workflow cancellation; the scope may be opened proactively, or
after delivery for cleanup:

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
Detached scope state is local to the currently running main or Update fiber.
To let a detached handler finish, the main Workflow must itself use a detached
scope around `workflow.await_all_handlers_finished(ctx)` after observing
cancellation.

Signals can remain explicitly awaited, or dispatch to registered handlers.
Immediate handlers mutate state without a `Fiber_Context`; routine handlers
receive one and may suspend:

```odin
workflow.set_signal_routine_json(
    ctx,
    "rename",
    &state,
    proc(ctx: ^workflow.Fiber_Context, state: ^State, name: string) {
        _ = workflow.execute_activity(ctx, "audit_rename", name)
        state.name = name
    },
)
```

Registered names are dispatched to handlers; unregistered names retain the
existing deterministic buffer semantics used by `workflow.await_signal*`.
`await_all_handlers_finished` covers both Signal and Update routines.

`worker.create` owns only language-side configuration and registries. The first
`worker.run` constructs one Core worker whose enabled task streams match the
registered Activities and Workflows. A worker is intentionally single-run;
call `worker.close` after `run` returns and keep its borrowed client open until
that close completes.

The script starts `temporal server start-dev --headless` only when a healthy
service is not already running on `localhost:7233`; Docker is not involved. It
also starts `examples/hello_workflow`; before the signal, the Odin Client first
proves validator rejection, then performs a typed state-mutating Update whose
handler waits on an Odin Activity. A typed `greeting_status` query receives the
changed live Workflow state
without waking the fiber. The Client then delivers an `approval` signal while
the fiber is blocked on a durable timer, cancelling that timer, evicting the
fiber, and requiring replay to reproduce the timer cancellation
and following Activity command. Two executions use Odin Client cancellation
to verify direct pending-timer and pending-Activity cleanup through Core. A
third resumes Workflow code, requests cancellation of its in-flight Activity,
schedules a detached cleanup Activity, survives eviction/replay, and only then
acknowledges Workflow cancellation. A fourth starts an Activity-awaiting Update
inside a detached scope, cancels the Workflow, and proves the Update completes
before the main fiber emits `CancelWorkflowExecution`, without requesting
Activity cancellation. Temporal CLI
still starts/waits on these older test executions and reads their history. The
Activity case waits for an Odin heartbeat, observes Core's cancel task in the
in-flight context, and replies with a cancellation completion. A separate fan-out case proves two Odin
Activity callbacks overlap rather than merely being queued concurrently. The
suite also explicitly cancels an Activity with
`Wait_Cancellation_Completed`, verifies that Odin does not resume the Workflow
before Core resolves cancellation, and round-trips the callback's typed JSON
cancellation details back to the Workflow. The callback observes the exact six
Core cancellation flags, with only `is_cancelled` set for this server path. The
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
temporal/headers/         shared owned interceptor/context Payload map
temporal/workflow/        explicit-state and stackful-fiber experiments
temporal/client/          owned Client and Workflow handle/control APIs
temporal/failure/         owned recursive Temporal Failure model
examples/core_connect/    Odin -> Core -> Temporal proof
examples/client_start/    Odin Client start-and-result proof
examples/client_control/  Odin Client signal/cancel/terminate proof
examples/eager_workflow/  same-connection eager Workflow start proof
examples/schedule_lifecycle/ Schedule create/patch/trigger/delete proof
examples/hello_activity/  Odin hello Activity Worker
examples/hello_workflow/  sequential suspension/replay/eviction proof
examples/child_workflow/  child and external Workflow operation proofs
tests/                    lifecycle, codec, protobuf, replay, and fiber tests
tests_pbt/                generated activation-order and replay properties
test-deps/pbt/            pinned, test-only property-testing submodule
test-deps/statecharts/    pinned, test-only lifecycle-model submodule
```

## License

MIT. The `native/sdk-core` submodule is Temporal's separate upstream project
and retains its own license.

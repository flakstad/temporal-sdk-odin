# Temporal SDK for Odin — experimental

An experimental Odin language SDK built on [Temporal SDK Core](https://github.com/temporalio/sdk-rust),
not a reimplementation of Temporal Server or its gRPC protocol.

Current status: Milestones 0–2 plus bounded explicit-state-machine and stackful
fiber Workflow experiments. The repository builds the official Rust Core C
bridge, connects to a Temporal CLI dev server, executes a remote Activity in
Odin, and now starts a Workflow and obtains its typed result through a public
Odin Client over Core's generic WorkflowService RPC bridge. A thin top-level
`temporal` facade now exposes ordinary Client/Worker lifecycle and registration
without changing the concrete module types or hiding cleanup. The Client also
sends typed signals, requests cancellation, pauses/unpauses or terminates executions, and updates
versioning/priority/time-skipping execution options with explicit field masks. It now
deletes an explicitly run-scoped Workflow execution through Core, while an
empty Run ID is rejected unless the caller deliberately opts into Temporal's
broader latest-run semantics. It also
starts, polls, describes, requests cancellation of, terminates, and deletes
standalone Nexus operations through an owned Odin handle. Describe owns status,
pending/cancellation state, timing, input/outcome, headers, Search Attributes,
UI metadata, failures, and the opaque long-poll token. Paginated list and
group-capable count APIs expose the standalone-operation Visibility surface.
Start supports typed Search Attributes, static summary/details, generic Links,
completion callbacks, and explicit `Use_Existing` attachment options. Describe
owns all six Link variants and all three pinned callback variants with delivery
state, attempts, timing, and Failure bytes. The complete standalone lifecycle
also participates in the typed Client outbound-interceptor chain; its Describe
token is exercised against the live Service to observe a later state.
This pre-release server
feature is live-proven against Temporal Server 1.32.0 with
`nexusoperation.enableStandalone=true`; the
repository's established CLI 1.5.1/Server 1.29.1 baseline correctly
capability-probes as unsupported. The same Odin Nexus Worker handles both
Workflow-originated and standalone operations. The standalone Activity slice
now exposes typed Payload/JSON Start, an owned execution handle,
bounded/cancelable Poll/result waiting, typed terminal failures,
cancel/terminate/delete, owned Describe snapshots, and paginated Visibility
List/Count. Describe covers execution state, timing, timeouts, Retry Policy,
priority, input/outcome, heartbeat details, headers, Search Attributes, Links,
UI metadata, and canonical Failure/callback bytes. Server 1.32 dispatches the
resulting task through the unchanged Core Activity poll path to the existing
Odin Worker. Server 1.29 returns gRPC 12/`UNIMPLEMENTED`. Start now supports
Headers, Search Attributes, static summary/details, Links, completion callbacks,
and `Use_Existing` attachment options, and the complete slice runs through
typed Client interceptors. Pause, Unpause, Reset, and masked execution-option
updates are typed, intercepted, and live-proven. The Client preserves terminal Workflow and Update Failure protobufs and decodes all ten
current Temporal Failure variants into an allocator-owned recursive Odin tree.
The live suite proves an unhandled `ActivityFailure -> ApplicationFailure`
chain from Odin Activity through Server history and back to the Odin Client. It
also
owns and types `DescribeWorkflowExecution` results—including identity, status,
timeouts, priority, memo/Search Attributes, Workflow pause and deployment
versioning metadata, parent/root identity, auto-reset points, request IDs,
time-skipping state, effective pending-Activity options/routing, pending
child/Nexus summaries, and owned callback protobufs—and
iterates full Workflow history with explicit pagination tokens. Each history
event includes stable metadata plus an owned canonical protobuf representation.
The live suite requests one-event pages against the CLI dev server and follows
every returned token. The Client can now eagerly aggregate those pages into the
binary `temporal.api.history.v1.History` accepted by Core's offline replayer.
The optional `temporal/history_json` package converts canonical protobuf JSON
from `temporal workflow show --output json` into that same binary message using
the pinned Temporal descriptors and standard protobuf-JSON semantics. Its
small native sidecar is linked only by programs that import the package; the
ordinary Client and Worker still depend only on SDK Core and the fiber shim.
`worker.create_replayer` uses the dedicated C-bridge replay worker, drives the
same Odin Workflow registry with Core activations, and distinguishes compatible
history, nondeterminism, Workflow-task failure, and unexpected eviction. Unit
tests prove compatible and intentionally incompatible histories without a
server; the eager live example fetches a completed execution from the CLI dev
server and replays it offline. The Client also lists Workflow executions through
Temporal Visibility and counts raw or grouped queries. List pages own their
summaries, memo/Search Attribute Payloads, and opaque continuation token;
grouped count values remain codec-neutral Temporal Payloads. Client connection
options now expose Core's TLS/mTLS,
API-key, ASCII/binary RPC metadata, retry, keepalive, HTTP CONNECT proxy, DNS
load-balancing, compression, identity, and payload-warning controls. API-key
connections enable TLS automatically, and metadata/API keys can be replaced on
a live connection. Core's official TOML/environment profile loader is now
bound through the C ABI. `client.load_config_profile` returns an owned Odin
profile and `client.connect_with_config_profile` applies its address,
namespace, API key, TLS/mTLS sources, and gRPC metadata. Bare addresses receive
the same HTTP/HTTPS choice as upstream Core, including explicit TLS disablement
for local development. Codec settings are exposed but remote codec transport
is not implemented; the pinned bridge also omits Core's host-verification
disable flag from its JSON result. The public Client now exposes
`get_system_info`, returning an allocator-owned Server version plus all thirteen capability bits in the
pinned Temporal API. This gives applications an authoritative baseline for
Schedules, Nexus, eager Workflow start, SDK metadata, Build-ID versioning, and
related features; experimental RPCs without a capability bit still require a
focused probe. It proves
Schedule lifecycle operations through the same Core RPC boundary: create a
cron/interval/structured-calendar Schedule with exclusions and optional TZif
data, replace it with an optimistic conflict token, describe/list/count it,
query matching times, backfill, pause/unpause, trigger it immediately, and
delete it. The CLI-devserver suite proves replacement, conflict-token rotation
and guarding plus matching-time queries, then waits for two buffered backfill
executions and one manual trigger. On CLI Server 1.29.1 a stale guarded update
is ignored by the legacy
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
waiting for the result. Existing accepted Updates can be reattached by their
Workflow/run/Update IDs. Result waiting accepts a monotonic timeout and a
thread-safe Core cancellation token; either stops only the local long poll and
does not cancel the durable Update. If an unhandled Activity failure terminates
an accepted Update handler, the Update outcome preserves Core's complete
recursive `ActivityFailure -> ApplicationFailure` tree rather than flattening
it; the Workflow Execution remains open and can continue handling work.
Concurrent handler close semantics now match the
established language SDKs: unfinished Update routines are abandoned with a
warning by default, registration can suppress that warning, and
`workflow.await_all_handlers_finished` explicitly keeps the main Workflow open.
Typed Signals may now be registered as immediate handlers or independent
stackful routines. An asynchronous Signal handler can await an Activity, mutate
Workflow-owned state, participate in handler draining, and replay on a fresh
stack. `set_signal_routine_json_with_context` keeps Signal identity, event ID,
and headers available across those waits; the CLI-devserver suite proves the
state and propagated header through a public Client Query. Signals received
before their handler is registered are retained and synchronously dispatched in
arrival order when registration occurs; unmatched older names do not block
them. Offline replay, 500 generated traces, and the live dev-server path cover
late registration.
Signal-handler failures are now isolated as retryable Workflow Task failures:
Core evicts the failed Odin fiber, replay reconstructs the execution on a fresh
stack with byte-identical failure output, and the Worker continues serving
other executions. The CLI-devserver suite verifies the failed history event,
eviction, replay, and same-process recovery.
Child Workflows now have a vertical start/result/failure/cancellation path:
Odin emits Core's `StartChildWorkflowExecution`, awaits its separate start and completion
resolutions, decodes typed JSON results and structured Child Workflow failures,
signals a started child through `SignalExternalWorkflowExecution`, and requests
explicit or inherited cancellation through `CancelChildWorkflowExecution`. The
signal APIs preserve Core's repeated argument field, attach an optional
UI-facing summary to the enclosing command metadata, wait for Core's
resolution, and report server failure causes. Unhandled
child failures preserve Core's original Temporal Failure wrapper and cause
chain when the parent fails. `start_child_workflow_payloads` preserves Core's
repeated input field, including order, metadata, and external payload details;
the singular Payload and typed JSON helpers are one-argument adapters. The
start options now cover the language-SDK surface carried by the pinned Core
command: task queue, execution/run/task timeouts, Retry Policy, cancellation
and Parent Close policies, Workflow ID reuse, cron, headers, memo, typed Search
Attributes, versioning intent, priority, and UI-facing static summary/details.
Borrowed strings, maps, policies, priority, and metadata are copied into the
Workflow run arena before suspension. The zero-value ID-reuse option is
normalized to Temporal's `AllowDuplicate` Child Workflow default. Child
Workflows remain in the parent's namespace, matching the other Core-based
language SDKs; arbitrary cross-namespace operations use external Workflow
handles instead. The
CLI-devserver suite proves successful child
completion, a caught server-generated child failure, duplicate-ID start failure,
parent cancellation that waits for child cancellation before closing, and a
three-argument parent-to-child call handled by a codec-neutral child registration.
It also reads the parent history and verifies Child Workflow memo, typed Search
Attributes, priority, ID-reuse policy, and static metadata as stored by the
Temporal Service.
Workflow code can also signal and request cancellation of arbitrary external
Workflow executions by namespace/Workflow ID/optional Run ID, suspending until
Core resolves each outbound operation. The live suite proves this against two
independent executions. `continue_as_new_json/payload/owned` now terminate the
current fiber run and carry new input plus timeout/retry/backoff options into a
new durable run. Continue-As-New can also replace memo and typed Search
Attributes and select Core's compatible/default Build ID intent plus the
experimental auto-upgrade/ramping initial versioning behavior. The
CLI-devserver suite follows the chain to completion and reads the replacement
memo and Search Attribute from the new run. The
Workflow runtime now implements Core's patch protocol through
`workflow.patched` and `workflow.deprecate_patch`. It consumes preemptive
`NotifyHasPatch` jobs, memoizes each decision per run, emits at most one
`SetPatchMarker`, and takes the legacy branch when replay encounters no marker.
The CLI-devserver suite verifies the resulting `core_patch` history marker. The
Worker API also exposes Core's unversioned, deployment-based, and legacy Build
ID strategies. A live `Pinned` deployment test sets an Odin Worker version as
Current and proves the Service routes a Workflow to it. Workers can also opt
all or selected Workflow types into terminal failure on Core-detected
nondeterminism; the default remains a retryable Workflow Task failure. The
same activation-context layer now exposes `workflow.now`, `history_length`,
`history_size_bytes`, `is_replaying`, `is_replaying_history_events`, the
Continue-As-New suggestion and current Worker Deployment Version, plus
deterministic `random_u64`/`random_f64` and `uuid_v4` without consulting host
time or OS entropy. Independent `Random_Stream` values add unbiased ranges,
indexes, floating-point values, and UUID v4 generation while consuming only
the history-derived parent stream. `set_current_details` feeds Temporal's
reserved Workflow metadata Query without adding a history event. Core-provided
start/reset seeds are applied before user code resumes. Local Activities now run through Core's dedicated
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
Update-With-Start is also live-proven through Core's generic Client bridge and
the Service's atomic `ExecuteMultiOperation` RPC. A missing Workflow is created
and updated; a second call reuses the same run and applies another Update. The
Odin API requires `.Use_Existing` and rejects cron, eager start, and start delay
because those combinations are forbidden by the upstream wire contract.
`client.Connect_Options.identity` is now propagated consistently into every
supported Workflow, Update, Schedule, asynchronous Activity, and Nexus request
that carries a client identity. The focused Update-With-Start integration test
also verifies the configured identity in Server history for start, both Updates,
and the completion Signal.
The same owned `Headers` builder now drives ordinary Client Signals and
Workflow-originated remote/Local Activities, Child Workflow starts, and
child/external Workflow Signals. Incoming Signal metadata remains borrowed
from the Workflow instance—including the signal/timer race helpers—and
Activity headers remain borrowed from `activity.Context`. Unit/property tests
cover wire encoding, ownership after Core decode arenas are destroyed, and
byte-identical replay; the CLI-devserver suite proves both Client Signal to
Workflow and Workflow command through Core to an Odin Activity.
Ordinary Signals also preserve Temporal's repeated Payload field end-to-end.
Use `signal_workflow_payloads` on the Client and
`await_signal_payloads[_with_context]` or `set_signal_handler_payloads` in a
Workflow. The singular Payload/JSON/string helpers remain explicit one-argument
adapters and reject another arity with `OdinWorkflowSignalArgumentCountError`.
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
acknowledged as canceled. Workflow code can now build borrowed deterministic
`Cancellation_Token` trees: child cancellation flows downward without reaching
parents or siblings, detached roots ignore Workflow cancellation, and the first
reason wins. Activities, Local Activities, timers, Child Workflows, external
Signals, and Signal waits capture the token selected by their lexical
`cancellation_scope`. A
reusable worker now registers both Activity and Workflow procedures, routes run IDs to the
correct fiber runtime, and polls both Core task streams concurrently. Its
bounded reusable Activity thread pool runs callbacks concurrently, exposes
cooperative cancellation and typed heartbeat payloads through
`activity.Context`, and has live tests for overlap and heartbeat-driven
cancellation. A sustained-load case fans 24 Activities into three Core slots,
asserts a hard callback peak of three, and drains every execution before
shutdown. `worker.Run_Stats.activity_executions_peak_owned` reports the largest
task-token route set; it may briefly be one above the slot count while a
completed callback crosses the Core-to-pool handoff. Opaque binary task tokens
are indexed in an execution-owned hash
route, giving average constant-time cancellation delivery as concurrency grows;
duplicate tokens are rejected as a Core protocol invariant. The context
preserves Core's cancellation reason and all six
detail flags (`not_found`, `cancelled`, `paused`, `timed_out`,
`worker_shutdown`, and `reset`). Typed callbacks may return a canceled result
with a user Payload; a Workflow using `Wait_Cancellation_Completed` can request
the cancellation explicitly and decode that Payload from the resulting
`CanceledFailureInfo`. `worker.create` also accepts
`graceful_shutdown_period` (five seconds by default). After that delay Core
sends outstanding Activities a `.Worker_Shutdown` cancellation; the live
suite proves the reason/detail flags, cooperative return, completion flush, and
clean finalization.
This period delays cancellation—it is not a force-kill timeout, so an Activity
that ignores cancellation can still keep shutdown open.

Advanced Core worker controls use an explicit defaulted options value, while
the shorter `worker.create` call remains available:

```odin
options := worker.options_default("orders")
options.identity_override = "orders-worker-1"
options.max_concurrent_activities = 32
options.max_cached_workflows = 1_000
options.max_concurrent_workflow_tasks = 4 // fixed run-affine executors
options.max_workflow_task_slots = 8
options.sticky_queue_schedule_to_start_timeout = 10 * time.Second
options.max_worker_activities_per_second = 50
options.workflow_task_poller_behavior = worker.poller_simple_maximum(5)
options.activity_task_poller_behavior = worker.poller_autoscaling(1, 20, 5)
options.resource_based_activity_tuner = worker.resource_based_activity_tuner(
	0.8, // target memory utilization
	0.9, // target CPU utilization
)
options.resource_based_nexus_tuner = worker.resource_based_nexus_tuner(
	0.8, // must match the Activity tuner when both are enabled
	0.9,
)

sdk_worker, err := worker.create_with_options(&core_client, options)
```

Zero rate limits mean unset. Default poller behavior leaves both C pointers
null so SDK Core can use its default and accept server-directed poller
autoscaling. Durations are rounded upward to whole milliseconds at the C ABI;
zero Workflow Task slots safely derives one without caching or two with caching.
Larger explicit values raise Core admission capacity. Setting
`max_concurrent_workflow_tasks` above one enables concurrent language execution
for both in-process and external/process-isolated Workflows. Core is still
polled by exactly one thread, as upstream requires; activation callbacks and
completion calls run concurrently. Each in-process executor owns an independent
runtime set, and stable run-ID hashing keeps every suspended fiber on one OS
thread through timers, replay, and eviction. External processors receive a
bounded callback pool and must preserve their own per-run affinity. At least as
many Core task slots as executor threads are required. External concurrency
additionally requires a positive Workflow cache.
invalid rates, poller ranges, cache/poller combinations, resource targets/slot
ranges, and negative durations are rejected before `worker_new`. A
resource-based Activity tuner supersedes `max_concurrent_activities` and uses
conservative default maxima of ten remote and ten local Activities; use its
`_with_options` form to set exact minimum, maximum, and ramp-throttle values.
The corresponding Nexus tuner supersedes `max_concurrent_nexus_tasks` and
defaults to one-to-ten tasks. Core uses one resource controller per Worker, so
the CPU and memory targets must match when both tuners are enabled. Workflow
slots remain fixed. The identity and all other referenced strings are owned by
the Odin Worker until `worker.close`.

Advanced Activity Workers may instead provide
`options.custom_activity_slot_supplier`. Its thread-safe Odin callbacks receive
callback-scoped reservation/task views, issue nonzero permit IDs, and observe
mark-used/release events. Async reservations are explicitly completed through
`worker.slot_reservation_complete` or
`worker.slot_reservation_cancel_complete`. The SDK owns and pins the C callback
table through Core Worker shutdown, while the configured `user_data` remains
caller-owned until `worker.close`. Custom and resource-based Activity suppliers
are mutually exclusive; `max_concurrent_activities` still sizes the Odin
callback executor. A CLI-dev-server test proves capacity-one backpressure,
queued reservation cancellation, permit release, and supplier cleanup through
the real Core bridge.

A running online Worker can move future polling and Worker heartbeats to a new
Core connection with `worker.replace_client`. Both Client runtimes must remain
open until `worker.close`; registrations, Workflow cache, task queue, and
namespace remain attached to the same Core Worker. The two-server integration
test proves the Worker polls one CLI dev server, survives that server stopping,
and completes a Workflow from another after replacement. Replacement does not
cancel a poll that is already in flight; that poll must return or fail first.

The dev-server suite proves both cache modes: cache-disabled execution rebuilds
the Workflow from history, while a cached sticky execution resumes the same
suspended Odin fiber on a later Signal and evicts it cleanly at shutdown. It
also schedules 24 Activities against a four-per-second Worker limit and checks
that SDK Core throttles delivery before the Odin callback pool. Eager Activity
reservations and the server-side task-queue rate are mapped and validated but
remain opt-in pending equally direct behavioral tests.

Core runtime observability is configurable without bypassing the public
Client. `core.runtime_create_with_options` and
`client.Connect_Options.runtime_options` support Core console logging filters,
a Prometheus HTTP exporter, and OTLP metrics over gRPC or HTTP, including
global tags, metric prefixes, histogram overrides, exporter periodicity,
runtime heartbeat metadata, and environment privacy controls. The
CLI-devserver suite scrapes a live Prometheus endpoint and requires the tagged
`GetSystemInfo` request counter. Runtime option strings and typed key/value maps
are borrowed only for synchronous construction and copied by Core. Structured
log forwarding is exposed through a callback-scoped owned `Runtime_Log`: Odin
copies target, message, timestamp, and JSON fields before the raw Core record
expires. The pinned callback has no `user_data`, so the SDK provides one
synchronized process-wide route and rejects a second simultaneous forwarding
Runtime. The live suite receives real records from arbitrary Core threads and
the unit suite proves the route is reusable after shutdown.

The Runtime also exposes Core's application metric meter rather than requiring
a second metrics stack:

```odin
base := [?]core.Metric_Attribute{
    core.metric_attribute_string("component", "payments"),
}
meter, err := core.metric_meter_create(&runtime, base[:])
defer core.metric_meter_close(&meter)

charged, err := core.metric_counter_create(
    &meter,
    "orders_charged",
    description = "Successfully charged orders",
)
defer core.metric_close(&charged)

record_attributes := [?]core.Metric_Attribute{
    core.metric_attribute_string("currency", "NOK"),
}
err = core.metric_counter_add(&charged, 1, record_attributes[:])
```

All seven C-bridge instrument kinds are available: integer counter, integer/
float/duration histograms, integer/float gauges, and signed up-down counter.
Meters can derive immutable attribute scopes with
`metric_meter_with_attributes`; record calls can add temporary attributes.
Metric handles own their cloned meter/attribute state and must close before the
Runtime. An Activity callback receives a callback-scoped meter through
`activity.metric_meter(ctx)`. It automatically carries `namespace`,
`task_queue`, and `activity_type`; when no exporter is configured it remains a
valid, validating no-op meter. Nexus start and cancellation handlers receive
the same lifecycle through `nexus.metric_meter(ctx)`, scoped with `namespace`,
`task_queue`, `nexus_service`, and `nexus_operation`.

Workflow code uses replay-aware procedures instead of Core handles:

```odin
workflow.metric_counter_add(
    ctx,
    "orders_charged",
    1,
    []workflow.Metric_Attribute{
        workflow.metric_attribute_string("currency", "NOK"),
    },
)
```

The Worker owns and caches the Core instruments with automatic `namespace`,
`task_queue`, and `workflow_type` attributes. History-event replay is always
suppressed; query-only replay remains observable. Workflow metric procedures
return no status, so exporter state cannot influence deterministic control
flow. Query handlers have corresponding `query_metric_*` procedures. Do not
call the lower-level Core metric API directly from Workflow code.

`./scripts/scoped-metrics-integration-test.sh` starts the CLI dev server when
needed, runs a real Workflow and Activity through Core, forces cache eviction
and fresh-stack replay, and scrapes the live Prometheus endpoint while the
Workflow is suspended on a durable timer. It requires both scoped counters and
their automatic attributes to be present with value exactly one; a replayed
Workflow emission would make the test fail with value two or greater.

Server capability discovery uses the same public Client and generic Core RPC
bridge:

```odin
info, err := temporal_client.get_system_info(&client)
defer temporal_client.system_info_free(&info)

fmt.println(info.server_version)
fmt.println(info.capabilities.supports_schedules)
fmt.println(info.capabilities.nexus)
```

`System_Info` owns the version string through the requested allocator. The
capability booleans are the Service's `GetSystemInfo` response, not SDK guesses
based on version parsing. Run `./scripts/system-info-integration-test.sh`
against `temporal server start-dev` for the focused live proof.

Newer operations are represented separately by `Server_Feature`. The safe
probe API combines the Odin SDK surface with either authoritative System Info
evidence or a non-mutating RPC probe:

```odin
observed, err := temporal_client.probe_server_feature(
    &client,
    .Standalone_Nexus,
)
fmt.println(observed.sdk_implemented)
fmt.println(observed.support) // Supported, Unsupported, or Unknown
fmt.println(observed.evidence)
```

Only gRPC status 12/`UNIMPLEMENTED` is classified as `Unsupported`. Permission,
namespace, authentication, and transport failures remain `Unknown` and are
returned to the caller. A false System Info bit is also `Unknown`, because the
feature may merely be disabled. The default probes never mutate server state;
Workflow Pause therefore remains unknown. Standalone Activity reports
`sdk_implemented=true` because the complete pinned lifecycle exists, while
server support remains `Unknown`: proving it would require a mutating Start and
the Service exposes no authoritative capability bit. No result is inferred
from a parsed server version.

`activity.info(ctx)`
now exposes the Core-delivered execution
identity, timestamps/timeouts, effective Retry Policy, priority, interceptor
headers, and previous heartbeat details as callback-scoped borrowed values; a retry integration test
recovers typed heartbeat progress on attempt 2. Workers can register ordered
Activity interceptors that wrap both callback execution and heartbeat calls
with real `next` continuations. The first registered interceptor is outermost;
it may inspect or replace inputs/results or deliberately skip `next`. Interceptor
user data is borrowed until `worker.close`. Remote Activities may now return
`activity.result_will_complete_async(T)` (or the Payload equivalent). A public
Odin Client can subsequently complete, fail, report cancellation, or heartbeat
the Activity by copied task token, Workflow/Activity ID, or standalone Activity
ID. The live dev-server matrix defers in the worker, heartbeats and completes by
both task token and Workflow/Activity ID, fails by task token with typed details,
and acknowledges a requested cancellation by ID through Core's generic
WorkflowService RPC bridge from a separate Client. Local Activities
reject asynchronous completion because they have no server-side pending task.
Workers can also register deterministic
Workflow outbound interceptors for remote and Local Activity scheduling, Child
Workflow start/Signal, external Workflow Signal/cancellation, and
Continue-As-New, durable sleep, explicit Child Workflow cancellation, and
Nexus operation start.
These run inside the Workflow fiber on normal execution and replay, receive
isolated per-call headers where the command supports them, and emit through the
existing Core command path. Workflow inbound interceptors now wrap the complete
Workflow execution, Queries, Signals, Update validation, and Update handlers
with typed `next` continuations. Stackful Signal and Update continuations remain
open across their own durable suspensions.
Unknown Workflow types fail the retryable Workflow Task without
terminating the worker. Typed Activities can return explicit retryable or
non-retryable Application Failures with a failure type, JSON details, and an
optional next-retry delay; the live suite proves Core/Server redelivery from
attempt 1 to attempt 2. Workflow initialization, Activity arguments/results,
and Workflow completion now preserve generic Payloads; typed JSON Workflows run
end-to-end across eviction and fresh-stack replay. Workflow-scheduled Activities
now accept IDs, task queues, all four server timeouts, Retry Policies,
cancellation type, and eager-execution control. Workflow, Signal-handler, and
Update-handler fiber stacks now have an explicit Worker policy with validated
independent sizes, shared by online execution and offline replay. On macOS and
Linux, each fixed stack is now backed by demand-paged virtual memory with an
inaccessible guard page on its overflow edge. This turns silent adjacent-memory
corruption into a process fault; it does **not** provide a growable stack,
in-process panic/overflow recovery, or a stable public API. Activity panic
containment has a live-proven
deployment boundary: run Activities in an Activity-only Odin worker process
under a supervisor. `./scripts/activity-panic-integration-test.sh` deliberately
traps that process on attempt 1, starts a replacement Odin worker, and requires
Temporal to redeliver attempt 2 and complete the still-running Workflow. A
mixed Workflow/Activity process is not protected by this pattern. Suspended
Workflow recovery is also live-proven: `./scripts/workflow-crash-integration-test.sh`
runs the SDK's `temporal/supervisor`, kills child generation 1 after a committed
Workflow Task, and requires bounded-backoff restart, generation-2 readiness,
fresh-stack history replay, and Signal-driven completion. The whole-Worker
supervisor contract is documented in
[`docs/supervision.md`](docs/supervision.md). Fine-grained per-Workflow or
per-Activity production subprocess pools are not implemented. The experimental
[`docs/workflow-isolation.md`](docs/workflow-isolation.md) path is now connected
to online Core polling in zero-cache and sticky-cache modes. Its live test
deliberately crashes
the first per-Workflow executor, submits a failed activation completion,
acknowledges Core eviction, replays in fresh children, and obtains the durable
timer Workflow result from the CLI dev server. The current implementation is a
reusable bounded per-run manager with owned run-ID/generation routes,
failed-until-eviction fencing, correlated private pipes, and generated
statechart/PBT lifecycle coverage. A live hung-child test proves the optional
activation deadline kills/reaps the executor and recovers through Core replay.
Parent and child now share one application registration function: the parent
copies its Workflow type manifest into Core, while a generic child
`Executor_Host` independently instantiates and routes the registered callbacks.
One live Worker completes both string and typed-JSON Workflow types through the
same executor binary. Procedure pointers never cross the process boundary. It
now verifies a canonical type manifest in a pre-poll child and again in every
executor generation. Shutdown acknowledgement and process exit have an optional
deadline, full capacity rejects without blocking a run-affine manager shard,
the process limit is global across all shards and in-progress starts, and an
optional cooldown circuit breaker bounds consecutive executor failures.
`./scripts/workflow-concurrency-integration-test.sh` adds a live concurrency
proof: four language executor threads process slow independent Workflow runs on
distinct shards, the measured activation peak must reach at least two, and all
results and routes must drain cleanly. It also guards the upstream contract
that Workflow polling itself is never concurrent.
`./scripts/workflow-in-process-concurrency-integration-test.sh` proves the same
boundary without child processes: two typed Odin Workflows suspend on Temporal
timers, resume on their original executor threads, overlap after resumption,
complete through concurrent Core calls, and receive final eviction on those
same threads. An unknown Workflow type concurrently produces failed activation
completions and eviction without poisoning the valid shards or stopping the
Worker. A third Workflow remains suspended on a one-hour Temporal timer while a
Client cancellation arrives; it resumes on its original shard, observes the
exact reason, cancels the durable timer, closes as canceled, and is evicted.
The integration script runs a second history with explicit Core cache eviction
after the initial timer commands are accepted. The valid Workflow fibers are
destroyed, reconstructed from server history on their original hash shards,
and overlap again after timer resolution. That pass requires a positive
`workflow_replay_activations` count and zero thread migrations. The public
`worker.request_workflow_eviction` helper exposes Core's asynchronous
language-requested eviction for diagnostics and tests; ordinary applications
should leave cache management to Core.
The replay pass also runs one deliberately incompatible Workflow: its first
activation records a Timer, while its reconstructed fiber emits completion.
The type-scoped Core policy must turn the mismatch into terminal
`[TMPRL1100]` nondeterminism while the valid shards continue to completion.
A separate affine Workflow registers an immediate typed Signal handler,
receives two buffered Client Signals, mutates run-local state on its owner
thread, and completes through `await_condition`. The live pass requires two
started/completed handlers, the exact result, and no shard migration.
A second handler Workflow runs a stackful Signal routine and Update routine at
the same time. Both suspend on durable two-second timers, remain logically
active together, resume on the owner shard, and drain before the main fiber
closes. The Client receives the typed Update result and the Workflow requires
an in-run handler peak of two.
A fourth live pass starts at least two Workflow runs on every executor shard,
sends four independent stackful Signal routines to each run, and waits until
all routines have suspended on durable timers. It then requests Core eviction
for every live run. Fresh stacks must replay every Signal/timer command and
produce the exact aggregate result with a per-run concurrency peak of four,
complete every handler without warnings or abandonment, and report zero thread
migrations.
A fifth pass exercises the Workflow and Activity executors together. Every run
starts one Activity from its main fiber, one from a Signal routine, and one
from an Update routine. Once a full Activity-slot batch is executing, the test
evicts every Workflow cache entry. Activity completions must rebuild fresh
Workflow stacks from history, resume all three paths through durable timers,
and return exact Workflow and Update results. The proof covers every affine
shard and requires exact Activity/Signal/Update counts, positive replay and
eviction counts, bounded Activity concurrency, and zero thread migration.
`Run_Stats.workflow_activations_peak_owned` reports the largest number of
polled activations simultaneously owned by Odin, including queued and executing
work. `workflow_executor_queue_peak` reports the largest queue observed on one
fixed in-process shard; serial and external-processor execution report zero for
that shard-specific value. `workflow_executor_shards_used` reports how many
fixed shards received work. `workflow_cached_fibers_shutdown` counts cached
in-process fibers destroyed during teardown rather than by Core eviction.
`workflow_executor_shard_activations_min` and
`workflow_executor_shard_activations_max` expose the least and most activation
admissions among shards that received work, making sustained routing skew
observable without exposing an allocated per-shard result.
The live shutdown pass leaves runs suspended on one-hour durable timers across
all four shards and requests shutdown while one activation is executing on
each shard. It then requires every fiber to be destroyed on its owner thread.
Shutdown also verifies that activation ownership returns to zero.
With a non-zero Core cache, the two live timer Workflows retain one successful
child generation each across suspension and incremental resolution; crash and
timeout recovery still evict and replay safely. Protocol v3 carries the child's
complete language `Process_Result` in a separate diagnostic frame, validates
terminal flags against the unchanged Core completion, and restores normal
Worker statistics/observer visibility. Protocol v4 additionally carries
correlated, discriminated log/metric/application diagnostics to a borrowed
parent observer. The live proof checks timer sequence fields that the parent
cannot infer from the completion and exactly four structured diagnostic events.
Each activation has independent default limits of 256 observations, 1 MiB of
cumulative encoded diagnostic payload, and one monotonic response deadline.
An additional sticky-cache proof records a durable Timer with the ordinary
child, replays it with incompatible code behind the same type manifest, and
requires both Core's `NONDETERMINISM` eviction and terminal `[TMPRL1100]`
history from the CLI dev server.
The same manager now survives a real guarded Workflow-stack overflow: it
preserves the child's `SIGSEGV`/`SIGBUS` exit code, fails one Workflow Task,
waits for Core eviction, and completes both executions through fresh-process
replay.
It likewise contains a real Odin Workflow panic (`SIGTRAP`/`SIGILL`) without
attempting to unwind through C, then recovers through the same Core-owned
history path.
An actual non-yielding Workflow loop is bounded by the executor activation
deadline; the parent kills/reaps that child and uses Core eviction/replay to
continue serving the task queue.
It still lacks production pool hardening.
Replay-aware child logging, per-runtime metric-emitter creation, and
inbound/outbound interceptor configuration are now explicit host options; the
live proof requires exactly two non-replay interceptor logs and metrics in both
cache modes. A separate six-case fault matrix proves that stale, malformed,
mismatched, oversized-diagnostic, and late child responses fail only one
Workflow Task and recover through Core eviction and fresh-generation replay.
Canceling a Client wait does not cancel an already accepted durable Update; the
current Temporal Service exposes no such operation.

Activity and Nexus executor shutdown now has an explicit uncooperative-callback
policy. The zero value keeps the safe default—wait without a watchdog. A Worker
can instead request one notification after a deadline:

```odin
stats, err := worker.run(&w, {
    uncooperative_callbacks = {
        warning_after = 5 * time.Second,
        observer = callback_drain_observer,
        user_data = supervisor_state,
    },
})
```

The observer receives Activity/Nexus kind, shutdown/bounded/error reason, the
active callback count, and configured duration on a dedicated thread. It must
return promptly; it may log or terminate the whole process under a supervisor,
but must not close or free the still-running Worker. The SDK deliberately never uses Odin's unsafe
thread-termination primitive. `scripts/uncooperative-callback-integration-test.sh`
proves bounded drain, while the normal shutdown scenario proves notification
starts at `request_shutdown`, precedes Core's grace-period cancellation, and
still permits clean completion. An infinite Workflow callback remains a process
supervision concern because it occupies the deterministic Workflow executor
before a shutdown activation can be processed.

Synchronous Nexus operation handling and Workflow-side calling are now a
live-proven vertical slice. The
raw/Core packages bind the pinned bridge's separate Nexus poll and completion
functions, while `temporal/nexus` and the public Worker own operation context,
service/operation registration, typed JSON adapters, bounded concurrent
dispatch, cooperative Core cancellation, and exact handler-failure mapping.
The stackful Workflow runtime now emits `ScheduleNexusOperation`, consumes the
separate start/result activation jobs, preserves recursive failures, and offers
Payload, JSON, and string `start`/`await`/`execute` APIs. The integration suite
creates a real CLI-devserver endpoint, runs both caller Workflow and operation
handlers in Odin, preserves a non-retryable handler failure and a
schedule-to-close timeout as recursive Failure trees, then observes
`Hello Odin`, fetches the server history, and replays it through Core.
The deterministic outbound interceptor can replace the endpoint, service,
operation, input Payload, timeouts, cancellation policy, and Nexus headers
before the terminal continuation copies them into run ownership; nested order
and byte-identical fresh-stack replay are tested. Nexus operation Futures are
also valid `selector_select` and allocation-free `join_all` cases.
Start handlers can now return a durable async operation token with
`nexus.result_async`; use `nexus.result_async_copy` when the token is backed by
temporary storage. A separately registered cancel handler receives the same
owned token when Core dispatches `CancelOperation`. `Start_Workflow_Options`
can attach borrowed `nexus_completion_callbacks`, including callback headers,
to an ordinary Workflow start. The live suite starts an Odin completer
Workflow with the callback URL and headers supplied to the Nexus handler and
observes its terminal JSON result complete the original async Nexus operation.
All four cancellation policies are live-proven. `Abandon` resumes the caller
without sending `CancelOperation`; `Try_Cancel` sends it and resumes
immediately; `Wait_Cancellation_Requested` waits for cancellation acceptance;
and `Wait_Cancellation_Completed` waits for the backing Workflow's terminal
callback. The cancel handler addresses backing Workflows by operation token and
requests cancellation through the Odin Client. The handler also translates the
caller's Nexus `temporal://` link into the generic public Link model and attaches
it both to the callback and completer Workflow start, matching the upstream Core
integration pattern. The SDK encodes all six variants in the pinned common
schema (`WorkflowEvent`, `BatchJob`, `Activity`, `NexusOperation`, `Workflow`,
and `Callback`) and parses the four URL/type forms currently defined by upstream;
`BatchJob` and `Callback` have no upstream Nexus URL conversion. Parsed Link strings are owned
and released with `client.link_free`. Terminal Nexus outcomes are absorbing:
duplicate or late conflicting Core resolutions are acknowledged without
replacing the result already observed by Workflow code.

```odin
hello :: proc(ctx: ^nexus.Context, name: string) -> string {
    return fmt.aprintf("Hello %s", name)
}

_ = worker.register_nexus_operation(
    &sdk_worker,
    "odin-greeting-service",
    "hello",
    hello,
)
_, _ = worker.run(&sdk_worker)
```

```odin
result := workflow.execute_nexus_operation_json(
    ctx,
    endpoint,
    "odin-greeting-service",
    "hello",
    "Odin",
    string,
    {schedule_to_close_timeout = 20 * time.Second},
)
```

Do not use it in production.

```odin
_ = worker.add_activity_interceptor(
    &sdk_worker,
    {
        user_data = tracing_state,
        execute = trace_activity,
        heartbeat = trace_heartbeat,
    },
)

trace_activity :: proc(
    data: rawptr,
    input: ^worker.Activity_Execute_Input,
    next: worker.Activity_Execute_Next,
) -> activity.Payload_Result {
    // Start span / extract propagated headers.
    result := worker.activity_execute_next(next, input)
    // Finish span / inspect result.
    return result
}
```

Client connections accept an ordered `[]Client_Interceptor`. The current
typesafe outbound surface wraps Workflow start, Signal-With-Start,
Update-With-Start, Signal, Query, Workflow result waiting, Update start/result
waiting, cancellation,
termination, exact-run deletion, and Workflow
Visibility list/count, standalone Nexus start/poll/describe, cancel/terminate/
delete and list/count, standalone Activity start/poll/describe,
cancel/terminate/delete, pause/unpause/reset/options update and list/count, plus Schedule
Describe/List/Count/Matching-Times; each
operation has its own input, result, and `next` type. Schedule Create has a
handle-returning continuation; Update, Backfill, Pause, Unpause, Trigger, and
Delete share a typed mutation family distinguished by `Schedule_Mutation_Kind`.
Standalone Nexus cancel, terminate, and delete similarly share
`Nexus_Operation_Mutation_Kind`.
Live RPC-metadata and API-key replacement share a corresponding
`Connection_Mutation_Kind` input:

```odin
propagate_signal :: proc(
    data: rawptr,
    input: ^temporal_client.Client_Signal_Workflow_Input,
    next: temporal_client.Client_Signal_Workflow_Next,
) -> temporal_client.Error {
    err := temporal_client.headers_add_json(input.headers, "trace-id", "request-42")
    if err.message != nil do return err
    return temporal_client.client_signal_workflow_next(next, input)
}

interceptors := [?]temporal_client.Client_Interceptor{{
    signal_workflow = propagate_signal,
}}
client, err := temporal_client.connect_with_options({
    namespace = "default",
    interceptors = interceptors[:],
})
```

Interceptor records are copied at connect time, while `user_data` remains
application-owned until `client.close`. Headers are cloned per call: interceptor
mutations reach the RPC but never mutate the caller-owned builder. Shared
mutable `user_data` must be safe for concurrent Client calls. Connection
mutation inputs borrow their buffers only for the synchronous chain. API keys
are secrets and telemetry interceptors must not record their value.

Workflow deletion is intentionally stricter than the upstream wire request:
`delete_workflow` requires a handle with a non-empty Run ID. Use
`delete_workflow_with_options(handle, {allow_latest_run = true})` only when
selecting Temporal's latest execution—and terminating it first if running—is
the intended destructive scope.

Workflow outbound interception is registered on the Worker. The callbacks are
part of deterministic Workflow execution: do not read wall-clock time, perform
I/O, use process randomness, or mutate replay-sensitive external state. Remote
and Local Activities have separate typed inputs and continuations:

```odin
propagate_activity :: proc(
    data: rawptr,
    input: ^workflow.Outbound_Start_Activity_Input,
    next: workflow.Outbound_Start_Activity_Next,
) -> workflow.Activity_Future {
    if !workflow.headers_add_json(input.headers, "trace-id", "request-42") {
        return {}
    }
    return workflow.outbound_start_activity_next(next, input)
}

_ = worker.add_workflow_interceptor(
    &sdk_worker,
    {start_activity = propagate_activity},
)
```

Registration order is outermost-first. Interceptor records are copied into
each registered Workflow runtime, while `user_data` remains application-owned
until `worker.close`. Inputs, headers, and `next` continuations are scoped to
the synchronous scheduling call and must not be retained.

Inbound Workflow execution uses the same nesting contract, but its continuation
stays on the Workflow fiber stack across every durable suspension:

```odin
trace_workflow :: proc(
    data: rawptr,
    input: ^workflow.Inbound_Execute_Workflow_Input,
    next: workflow.Inbound_Execute_Workflow_Next,
) -> workflow.Payload_Result {
    result := workflow.inbound_execute_workflow_next(next, input)
    // Runs only when the Workflow returns, not at each activation boundary.
    return result
}

_ = worker.add_workflow_inbound_interceptor(
    &sdk_worker,
    {execute_workflow = trace_workflow},
)
```

`handle_query`, `handle_signal`, `handle_update_validator`, and
`handle_update_handler` wrap their corresponding language callbacks. Query,
synchronous Signal/Update, and validator chains retain their existing
command-state guards. Stackful Signal and Update handler continuations live on
the handler fiber and unwind only after the handler finishes. All five hooks
execute during replay and must be deterministic. Their input records and
continuations are call/fiber-stack scoped; `user_data` is borrowed until
`worker.close`.

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

- Odin (tested with `dev-2026-09:a2fb372b7`)
- Rust toolchain 1.94 and Cargo
- `protoc`
- Temporal CLI (tested with CLI 1.5.1 / Server 1.29.1)
- Clang/linker tooling for the target platform

The fiber experiment also builds pinned
[minicoro](https://github.com/edubart/minicoro) through a seven-symbol local C
shim. It is a research dependency, not yet a frozen SDK choice.
The test-only [pbt](https://github.com/flakstad/pbt) submodule generates
payload/failure contracts, activation races, all six orderings of
Signal-start/Update-start/token-cancel jobs, and 300 inherited deadline trees,
and verifies byte-identical replay completions. A 500-trace mixed-executor
property additionally combines main-, Signal-, and Update-owned Activity
awaits, four resolution partitions, all six resolution orders, and eviction
before or after partial resolution. A pinned
[statecharts](https://github.com/flakstad/statecharts)
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

The small top-level package covers ordinary Client/Worker lifecycle while
feature-specific APIs remain in their explicit packages:

```odin
import activity "temporal/activity"
import temporal "temporal"

hello :: proc(_: ^activity.Context, name: string) -> string {
    return "Hello " + name
}

client, client_error := temporal.connect()
assert(client_error.message == nil)
defer temporal.client_close(&client)

sdk_worker, worker_error := temporal.worker_create(&client, "hello")
assert(worker_error.message == nil)
defer {
    close_error := temporal.worker_close(&sdk_worker)
    temporal.worker_error_free(&close_error)
}

register_error := temporal.worker_register_activity(&sdk_worker, "hello", hello)
assert(register_error.message == nil)
_, run_error := temporal.worker_run(&sdk_worker)
assert(run_error.message == nil)
```

The facade keeps Client and Worker errors distinct and does not hide cleanup.
See `examples/sdk_hello_activity` for checked error handling and a bounded live
run against the Temporal CLI dev server.

Build a host-specific native runtime archive and verify that the packaged Core
ABI can initialize and shut down:

```bash
./scripts/package-native.sh
./scripts/verify-native-package.sh \
  dist/temporal-sdk-odin-native-macos-arm64.tar.gz --smoke
```

Use the archive matching the host: `macos-arm64`, `macos-x86_64`,
`linux-arm64`, or `linux-x86_64`. See
[native distribution](docs/native-distribution.md) for archive contents and
linking details. CI builds and smoke-tests all four target artifacts.

Install a matching archive into the revision-addressed local package cache and
build every example without rebuilding Rust Core:

```bash
native_lib_dir="$(./scripts/install-native-package.sh \
  dist/temporal-sdk-odin-native-macos-arm64.tar.gz)"
TEMPORAL_ODIN_NATIVE_LIB_DIR="$native_lib_dir" \
  ./scripts/build-examples.sh
```

Installation rejects another platform, another pinned Core revision, checksum
changes, and a C header that differs from the checked-out bridge. Reinstalling
the identical immutable package is idempotent; an incomplete or different
package at the same revision/target path is never overwritten.

Package verification also rejects a wrong native architecture, nonportable
Core install name/SONAME, embedded library search paths, unexpected shared
dependencies, and a mismatched fiber object. The final executable supplies the
runtime search path to the immutable package directory.

The isolated Workflow executor is application-specific, so it is deliberately
not included in that generic native archive. A relocatable parent/executor
example bundle and its full loader/process verification can be built with:

```bash
TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/package-workflow-isolate-example.sh dist
./scripts/verify-workflow-isolate-example-package.sh \
  dist/temporal-sdk-odin-workflow-isolate-example-<target>.tar.gz
```

CI performs this relocation proof on macOS and Linux for arm64 and x86_64. See
[the application-bundle contract](docs/workflow-isolate-example-package.md)
for why a real application must compile its own Workflow registry into the
executor.

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

The same worker also exposes the repeated-argument boundary:

```bash
temporal workflow execute \
  --workflow-id odin-repeated-parent-manual \
  --type odin_repeated_child_parent_workflow \
  --task-queue odin-child-workflow \
  --input '"Odin"'
```

With `examples/hello_workflow` polling, the public Client example performs
`StartWorkflowExecution`, long-polls `GetWorkflowExecutionHistory` for the
close event, and decodes the result Payload without Temporal CLI:

```bash
./scripts/run-client-start.sh
```

The self-contained eager Workflow example creates a public Client and Worker on the same
Core connection, proves inline first-task dispatch, fetches the resulting full
history, and replays it through an offline Core worker:

```bash
./scripts/run-eager-workflow.sh
```

Eager Activity dispatch is covered separately because merely completing an
Activity cannot distinguish inline delivery from an ordinary poll. Start the
CLI dev server with the self-hosted feature enabled, then run the focused
same-process proof:

```bash
temporal server start-dev \
  --dynamic-config-value 'system.enableActivityEagerExecution=true'
./scripts/run-eager-activity.sh
```

The example leaves Core's logging enabled so the integration suite can require
`Adding 1 activity tasks received from WFT complete`. The explicit server flag
keeps this reproducible across releases: CLI 1.5.1's bundled Server 1.29.1
declined the request without it, while current Temporal Server source defaults
the setting to true. If the Service declines, execution still succeeds through
normal polling.

The underlying replay boundary is explicit. The Client returns the exact binary
History protobuf; the replayer borrows it for the call and returns an owned
diagnostic result:

```odin
history, err := temporal_client.fetch_history_protobuf(&handle)
defer delete(history)

core_client := temporal_client.core_client(&client)
replayer, err := worker.create_replayer(core_client.runtime)
defer worker.close(&replayer)

err = worker.register_workflow(&replayer, "orders", order_workflow)
replay_result, err := worker.replay_history(
    &replayer,
    handle.id,
    history,
)
defer worker.replay_result_free(&replay_result)

if replay_result.failure == .Nondeterminism {
    // replay_result.message contains Core's TMPRL1100 diagnostic
}
```

The C bridge requires protobuf wire bytes. `fetch_history_protobuf` is the
direct in-process path from Service history to replay; `temporal/history_json`
is an explicit optional conversion step for canonical Temporal CLI JSON.
`worker.replay_histories` accepts a borrowed slice of histories and runs all of
them through one Core worker. Results remain in input order; malformed protobuf,
nondeterminism, and Workflow-task failures are per-history results so a corpus
run can continue, while worker/lifecycle failures abort the batch.

The eager example can persist its fetched History and the replay example can
use one or many such files as a CI compatibility corpus:

```bash
temporal server start-dev

TEMPORAL_REPLAY_HISTORY_OUT=/tmp/eager.pb \
  ./scripts/run-eager-workflow.sh

./scripts/run-replay-history.sh /tmp/eager.pb
```

The same replay tool recognizes Temporal CLI JSON by its first non-whitespace
byte and converts it before calling Core:

```bash
temporal workflow show --workflow-id my-workflow --output json >/tmp/history.json
./scripts/run-replay-history.sh /tmp/history.json
```

JSON fields absent from the pinned schema are rejected instead of silently
dropped. `./scripts/history-json-integration-test.sh` proves the complete
CLI-export → conversion → Core replay path against `temporal server start-dev`.

`examples/replay_history` intentionally registers the eager sample Workflow.
Applications should copy its small driver and replace that registration with
their own Workflow definitions. The command exits non-zero if any history is
incompatible or invalid.

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

update_headers := temporal_client.Headers{}
err = temporal_client.headers_init(&update_headers)
defer temporal_client.headers_free(&update_headers)
err = temporal_client.headers_add_json(&update_headers, "trace-id", trace_id)

atomic, err := temporal_client.update_with_start_json(
    &client,
    "order_workflow",
    order,
    "set_reference",
    Set_Reference_Input{reference = "external-42"},
    {
        start_options = {
            id = "order-42",
            task_queue = "orders",
            id_conflict_policy = .Use_Existing,
        },
        update_options = {
            update_id = "set-reference-42",
            headers = &update_headers,
        },
    },
)
defer temporal_client.update_with_start_result_free(&atomic)
// atomic.workflow.started_by_request distinguishes creation from attachment.

update_result: Set_Reference_Output
err = temporal_client.wait_update_json(&atomic.update, &update_result)

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
signal_args := [?]codec.Payload_View{
    codec.payload_view(&order),
    codec.payload_view(&approval),
}
err = temporal_client.signal_workflow_payloads(
    &handle,
    "approve_order",
    signal_args[:],
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
query_args := [?]codec.Payload_View{
    codec.payload_view(&order),
    codec.payload_view(&filters),
}
raw_status, err := temporal_client.query_workflow_payloads_with_options(
    &handle,
    "order_status_raw",
    query_args[:],
    {headers = &query_headers},
)
defer codec.payload_free(&raw_status)
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
wait_token := temporal_client.client_cancellation_token_create()
defer temporal_client.client_cancellation_token_close(&wait_token)
err = temporal_client.wait_update_json_with_options(
    &update,
    &updated,
    {timeout = 30 * time.Second, cancellation_token = &wait_token},
)
update_args := [?]codec.Payload_View{
    codec.payload_view(&name),
    codec.payload_view(&options),
}
raw_update, err := temporal_client.start_update_payloads_with_options(
    &handle,
    "rename_order_raw",
    update_args[:],
    {headers = &update_headers},
)
defer temporal_client.update_handle_free(&raw_update)
err = temporal_client.cancel_workflow(&handle, "customer canceled")

err = temporal_client.pause_workflow(
    &handle,
    {reason = "downstream dependency outage"},
)
err = temporal_client.unpause_workflow(
    &handle,
    {reason = "downstream dependency recovered"},
)

priority := temporal_client.Priority{priority_key = 2}
priority_path := [?]string{"priority"}
post_reset := [?]temporal_client.Post_Reset_Operation{{
    kind = .Update_Workflow_Options,
    update_workflow_options = {
        update_mask = priority_path[:],
        priority = &priority,
    },
}}
reset_handle, err := temporal_client.reset_workflow(
    &handle,
    {
        workflow_task_finish_event_id = 4,
        reason = "deploy replay-safe Workflow code",
        reapply_exclude_types = []temporal_client.Reset_Reapply_Exclude_Type{
            .Updates,
            .Nexus,
        },
        post_reset_operations = post_reset[:],
    },
)
defer temporal_client.workflow_handle_free(&reset_handle)

updated_options, err := temporal_client.update_workflow_execution_options(
    &handle,
    {
        update_mask = priority_path[:],
        priority = &priority,
    },
)
defer temporal_client.workflow_execution_options_free(&updated_options)
```

`execute_update_json` remains the one-call convenience wrapper around
`start_update_json` plus `wait_update_json`; the codec-neutral and option-aware
variants are available when callers need custom Payloads or propagation headers.
The plural `start_update_payloads[_with_options]` and
`execute_update_payloads` forms preserve Temporal's complete ordered argument
list. `get_update_handle` reconstructs an owned handle after reconnect.
`Update_Wait_Options` bounds or locally cancels only the result wait; a
`.Wait_Timed_Out` or `.Wait_Canceled` error does not cancel the accepted Update.
The cancellation token may be triggered from another thread, must outlive the
wait call, and is released explicitly with `client_cancellation_token_close`.

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

An empty run ID targets the current execution. Signal, cancellation, and reset
calls generate UUIDv4 request IDs unless the caller supplies one. A successful
reset returns a new owned Workflow handle containing the new Run ID; it does not
mutate the source handle. The reset point must be a Workflow Task completion,
timeout, failure, or start event. The current surface supports the modern
signal/Update/Nexus reapply-exclusion list and both pinned
`PostResetOperation` variants. Post-reset Signals carry ordered codec-neutral
Payloads, Headers, and all six Link variants. Atomic Workflow-option updates
use an explicit field mask and can set or clear versioning override, priority,
and time-skipping configuration. It deliberately leaves the deprecated positive
reapply mode unset. With an empty exclusion list, the pinned Temporal API
documents signal reapplication as the compatibility default. Server 1.32
live-proves post-reset priority updates but rejects the schema-present Signal
variant as unsupported; its wire contract is unit- and property-tested until
the Service implements it. Termination has plain, Payload, and typed JSON
detail variants.

The same validated option model drives the ordinary
`UpdateWorkflowExecutionOptions` RPC for an open execution. Its field mask can
set or explicitly clear versioning override, priority, and time-skipping
configuration. The returned snapshot owns deployment IDs, fairness key, and
fast-forward ID and must be freed explicitly. It also exposes Server's optional
update timestamp. Server 1.32 live-proves priority write, canonical response,
history/Describe read-back, and the outbound interceptor path; that server does
not yet populate the newer response timestamp.

Experimental Workflow Pause is also exposed as explicit Client operations.
`pause_workflow` and `unpause_workflow` borrow a handle, accept reason plus an
optional idempotence ID, generate UUIDv4 IDs by default, and share one typed
outbound-interceptor family. Server 1.32 live-proves Paused status, Query
rejection, accepted-but-deferred Signal delivery, Unpause, and both history
events. Self-hosted use requires Server 1.30 or newer with
`frontend.WorkflowPauseEnabled=true`; it is not silently advertised as a stable
capability on older or unconfigured Services.

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

Every live Workflow and asynchronous Signal/Update handler owns a native fiber
stack. The default is 128 KiB per fiber. Workers can choose independent bounded
sizes when their call depth or concurrency profile needs a different tradeoff:

```odin
temporal_worker, err := worker.create(
    &client,
    "orders",
    workflow_stacks = {
        workflow = 256 * 1024,
        signal_handler = 128 * 1024,
        update_handler = 192 * 1024,
    },
)
```

Zero selects the default. Explicit values must be between 32 KiB and 16 MiB
and are aligned to the native runtime's 16-byte boundary. The same option is
available on `worker.create_replayer`, so replay uses the production stack
policy. These are fixed-size, demand-paged virtual-memory stacks on macOS and
Linux. A `PROT_NONE` page immediately below the downward-growing stack traps
overflow before it reaches minicoro metadata. The fault terminates the process;
workers still need process supervision, and stacks do not grow automatically.

`register_json_activity` accepts ordinary typed Odin procedures such as
`proc(^activity.Context, Greeting_Input) -> Greeting_Output`. The generic
adapter encodes one `json/plain` Temporal Payload; the lower-level
`register_payload_activity` callback receives a borrowed `codec.Payload_View`
and can return any allocator-owned `codec.Payload`. Existing string Activities
remain source-compatible through `register_activity`.

The codec-neutral boundary also supports Temporal's repeated Activity argument
field. Use `register_payloads_activity` for a callback-scoped borrowed
`[]codec.Payload_View`, and schedule it with `start_activity_payloads` or
`execute_activity_payloads` (plus their `_with_options` forms). Remote and Local
Activity commands preserve argument order, metadata, and external-payload
details. The typed JSON and legacy string adapters intentionally remain
single-argument APIs; receiving any other arity fails the Activity with
`OdinActivityArgumentCountError` instead of silently discarding arguments.

Workflow starts preserve the same repeated Payload contract. Codec-neutral
callers use `temporal_client.start_workflow_payloads`, and workers register
`register_payloads_workflow` callbacks that receive the ordered borrowed
`[]codec.Payload_View`. The Client interceptor sees that same slice. The
single-Payload, typed JSON, and string Workflow adapters remain convenient
one-argument APIs and deterministically fail with
`OdinWorkflowArgumentCountError` on any other arity. Unit replay, a 500-case
property over 0–12 arguments, and a CLI-devserver scenario cover the full
Client → Core → Server → Workflow path.

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
priority := workflow.Workflow_Priority{
    priority_key = 3,
    fairness_key = "orders",
    fairness_weight = 1.25,
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
        cancellation_token = workflow.cancellation_current(ctx),
        versioning_intent = .Default,
        priority = &priority,
        summary = "Create order greeting",
    },
)
```

`Activity_Options` borrows its strings, slices, Retry Policy, priority, summary,
and cancellation token only for the call. The runtime copies retained values
into Workflow-owned storage before it can suspend. Priority is carried on
Core's `ScheduleActivity`; summary is encoded as
`WorkflowCommand.user_metadata.summary`. `versioning_intent`
retains Core's legacy Build-ID routing control, while new applications should
prefer Worker Deployment Versioning. Local Activity options expose the same
summary metadata on Core's persisted Local Activity marker. At least one of
Start-to-Close or Schedule-to-Close must be positive.
The compatibility helpers retain their experimental 20-second
Start-to-Close/one-attempt behavior.

`register_payload_workflow` is the single-argument lower-level form;
`register_payloads_workflow` exposes Temporal's repeated Workflow argument
field. String Workflows remain a compatibility adapter over `json/plain`.
Child Workflow starts follow the same shape:
`start_child_workflow_payloads` accepts a borrowed ordered slice and clones all
Payloads into Workflow-owned storage before suspension, while
`start_child_workflow_payload` and `start_child_workflow_json` preserve the
single-argument convenience API. `execute_child_workflow_payloads` combines
start and await while retaining the codec-neutral result.
Workflow signal buffering preserves a
complete ordered Payload slice. `await_signal_payloads` exposes the repeated
codec-neutral boundary; `await_signal_payload` and `await_signal_json` are
one-argument raw and typed forms, with equivalent signal-or-timeout variants.
`await_signal` remains the `json/plain` string compatibility adapter.

Outbound operations on another Workflow use an explicit execution identity and
wait for Core's durable acknowledgement:

```odin
target := workflow.External_Workflow_Execution{
    namespace = "default",
    workflow_id = order_workflow_id,
    // Empty run_id targets the current execution.
}
signal := workflow.signal_external_workflow_json_with_options(
    ctx,
    target,
    "approve",
    approval,
    {summary = "Approve order"},
)
if signal.status == .Succeeded {
    cancel := workflow.request_cancel_external_workflow(
        ctx,
        target,
        "superseded order",
    )
}
```

The identity, signal name, and optional summary are borrowed for the call and
copied into the Workflow arena before suspension. The summary is encoded as
`WorkflowCommand.user_metadata.summary` for both child and external targets.
`signal_external_workflow_payloads` and the Child Workflow equivalent clone
every borrowed Payload in order; their singular Payload and JSON forms remain
one-argument adapters. The singular `_owned` form consumes its Payload. Failure
results map Core's not-found,
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
`continue_as_new_payloads` preserves Core's ordered repeated argument field;
the singular Payload and JSON forms remain one-argument adapters. The current
slice also supports Retry Policy, first-task backoff, typed Payload headers,
memo replacement, typed Search Attribute replacement, versioning intent, and
initial versioning behavior. Build maps with `workflow.payload_map_init`, add
memo through `payload_map_add_json/payload`, and add Search Attributes through
the typed `payload_map_add_search_attribute_json/payload` helpers. Nil maps
inherit the current run. An initialized empty Search Attribute map explicitly
clears it; Core's proto3 map cannot distinguish an empty memo override from an
unset memo, so an empty memo map inherits too.
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
different change. This Workflow code versioning is separate from Worker
Deployment Versioning, which is configured when the Worker is created:

```odin
sdk_worker, create_error := worker.create(
    &client,
    "orders",
    versioning = {
        mode = .Deployment_Based,
        deployment_name = "orders-service",
        build_id = "2026-09-29.1",
        use_worker_versioning = true,
        default_versioning_behavior = .Pinned,
    },
)
```

The zero value remains an unversioned Worker with the SDK's experimental
default Build ID. Deployment routing requires non-empty deployment/build IDs;
its default behavior may be `.Pinned`, `.Auto_Upgrade`, or `.Unspecified`.
Current/ramping version administration remains a Temporal Service operation,
normally performed with `temporal worker deployment` or deployment APIs. The
legacy Build-ID strategy is exposed for existing installations but should not
be selected for new deployments.

Core-detected nondeterminism normally fails the Workflow Task so a compatible
code deployment can repair the execution. Applications that deliberately want
terminal failure can opt in globally or by Workflow type:

```odin
sdk_worker, create_error := worker.create(
    &client,
    "orders",
    nondeterminism_policy = {
        fail_workflow_types = []string{"order_workflow"},
    },
)
```

Set `fail_all_workflows = true` only when every Workflow on the Worker should
use that policy. This changes failure semantics and remains experimental in
upstream SDKs; patching or Worker Deployment Versioning is normally preferable
for compatible code evolution.

Workflow time and random values must use Temporal-aware primitives:

```odin
current_time, available := workflow.now(ctx)
choice := workflow.random_u64(ctx)
ratio := workflow.random_f64(ctx)
request_id := workflow.uuid_v4(ctx)

random := workflow.random_stream_create(ctx)
shard, ok := workflow.random_stream_range(&random, 0, 16)
index, index_ok := workflow.random_stream_index(&random, len(candidates))
correlation_id := workflow.random_stream_uuid_v4(&random)
```

`now` is constant within an activation and returns Core's history-derived
timestamp. The random stream is seeded by `InitializeWorkflow` and reset by a
pre-scanned `UpdateRandomSeed`; its SplitMix64 sequence is replay compatibility
state and is not cryptographically secure. `uuid_v4` consumes that stream and
returns a lowercase RFC 4122 variant/version string. Creating a `Random_Stream`
consumes exactly one draw from the parent and then advances independently.
Copying it intentionally snapshots its state. Existing streams retain their
state across later activations; a pre-scanned Core seed update affects parent
draws and streams created after that update. Range reduction uses rejection
sampling rather than biased modulo reduction. These values are Workflow-only
and are not safe for sharing with ordinary threads.

Per-activation introspection is also history-derived:

```odin
history_bytes := workflow.history_size_bytes(ctx)
should_roll_over := workflow.continue_as_new_suggested(ctx)
deployment, versioned := workflow.current_deployment_version(ctx)

_ = workflow.set_current_details(ctx, "Waiting for payment")
```

`is_replaying_history_events` differs from `is_replaying` for query-only replay
activations, allowing diagnostic logging during a Query without repeating
side effects for replayed history. Deployment-version strings are borrowed from
the Workflow run. `current_details` is SDK-runtime metadata rather than durable
Workflow state: Workflow code deterministically reconstructs it during replay,
and the reserved `__temporal_workflow_metadata` Query returns it as Temporal's
`{"currentDetails":"..."}` JSON payload.

Workflow application logging is separate from Core runtime telemetry. Install
one callback while the Worker is still configuring:

```odin
worker_options.workflow_logger = {
    user_data = &log_sink,
    emit = emit_workflow_log,
}

fields := [?]workflow.Log_Field{
    workflow.log_field_string("order_id", order.id),
    workflow.log_field_i64("attempt", 1),
}
workflow.log_info(ctx, "order started", fields[:])
```

`log_trace`, `log_debug`, `log_info`, `log_warn`, and `log_error` synchronously
deliver a borrowed `Log_Record` containing Workflow identity, type, task queue,
history length, activation timestamp, replay flags, and typed string/integer/
float/boolean fields. Query handlers use the corresponding `query_log_*`
procedures. The SDK performs no host I/O and logging emits no Workflow command.
The callback must copy strings or fields it keeps after returning, and its
`user_data` remains application-owned through `worker.close`.

Logs from activations replaying history events are suppressed by default, so a
normal log does not appear again merely because Core rebuilt a Workflow on a
fresh stack. Query-only replay remains visible. Set
`log_replay_history_events = true` only for replay diagnostics where duplicate
records are intentional. Logging procedures deliberately return no value:
whether a sink is configured or a replay record is suppressed cannot influence
deterministic Workflow control flow.

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

Initialization also preserves Core's previous-run state. `workflow.Info`
contains the cron schedule-to-schedule interval, a borrowed recursive
`failure.Failure` when this run continued after a failure, and every generic
last-completion-result Payload. `workflow.continued_failure`,
`last_completion_result_payload`, and `last_completion_result_json` provide
checked accessors. Their values are owned by the Workflow run arena and must not
escape the run.

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
callback by value, and must remain valid while the Workflow runs. Codec-neutral
handlers registered with `set_query_handler_payloads[_with_context]` receive
Temporal's complete ordered argument slice. The Payload/JSON helpers remain
one-argument adapters and reject another arity with
`OdinWorkflowQueryArgumentCountError`. Query results remain exactly one Payload.
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

Codec-neutral Updates may instead use
`set_update_handler_payloads[_with_context]`; validator, synchronous handler,
and optional stackful routine callback all receive the same ordered Payload
slice. Singular Payload/JSON registrations reject any arity other than one with
`OdinWorkflowUpdateArgumentCountError`.

`Update_Context` exposes the server Update ID, name, client identity, and
headers. Its run-arena-owned values survive handler suspension, and
`workflow.current_update(ctx)` returns the same invocation while the routine is
running. They must not escape the Workflow run. Workflow execution and Query
have composable inbound interceptor chains, as do Signal handling, Update
validation, and Update handling. Stackful Signal and Update chains are created
on their handler fibers, so their interceptor continuations remain live across
durable suspension rather than being limited to one activation.

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

For selective cancellation, create child or detached tokens and pass one
directly to an operation. A zero token inherits the token currently selected
by the calling Workflow fiber:

```odin
root := workflow.cancellation_root(ctx)
branch := workflow.cancellation_child(root)
future := workflow.start_activity_with_options(ctx, "charge", order_id, {
    start_to_close_timeout = 30 * time.Second,
    cancellation_token = branch,
})

_ = workflow.cancellation_request(branch, "order superseded")
result := workflow.await_activity_result(ctx, future)
```

Tokens are borrowed run-local handles and must not survive Workflow eviction.
Cancellation is downward-only, repeated requests preserve the first reason,
and `cancellation_detached` creates a new root unrelated to Workflow
cancellation. `Activity_Options`, `Local_Activity_Options`,
`Child_Workflow_Options`, `Nexus_Operation_Options`, `Signal_Options`, and
`Timer_Options` all accept this explicit run-local selection. Foreign or stale
tokens fail deterministically before emitting a command. The older
`detached_cancellation_scope` helper creates and
selects such a token as a compatibility convenience. Nested scopes must be
ended in LIFO order, normally with `defer workflow.cancellation_scope_end(&scope)`.
`cancellation_request` cancels associated operations but does not itself cancel
the Workflow Execution; only a server-delivered root cancellation is
acknowledged with `CancelWorkflowExecution`.

Odin's Child Signal convenience accepts a `Child_Workflow_Future`, so it may
internally wait for the Child start before a signal command can exist. The
Signal token covers that prerequisite wait as well: cancellation resumes only
the Signal call and emits no `SignalExternalWorkflowExecution`. It does not
change a direct `await_child_workflow_start`. Once either a Child or external
Signal has been commanded, the same token emits Core's
`CancelSignalWorkflow` and returns `.Canceled`.
The CLI-devserver suite drives the pre-start case through an atomic
Signal-With-Start request. The cancellation Signal is therefore present in the
initial activation; history proves that the Child starts and completes while
the canceled Child Signal never becomes a Core command.

After applying every Core job, the Workflow scheduler drains ready Signal,
Update, and main fibers to a deterministic fixed point. Cancellation in a
later-scheduled fiber therefore wakes earlier Signal/timer waiters, completes
their handler responses, and can release `await_all_handlers_finished` in the
same activation. Activity options using `Wait_Cancellation_Completed` remain
suspended until Core delivers the terminal Activity resolution.

Shared state across the main Workflow, Signal routines, and Update routines can
be guarded by the zero-value `workflow.Mutex`. It is a deterministic FIFO
primitive implemented entirely by the Odin Workflow scheduler: locking
suspends only the current fiber, produces no server command, and honors the
cancellation token selected at the start of the wait.

```odin
if !workflow.mutex_lock(ctx, &state.mutex) {
    return "canceled"
}
defer workflow.mutex_unlock(ctx, &state.mutex)
state.balance += delta
```

Keep a mutex in state owned by one Workflow run and do not copy it after first
use. `mutex_try_lock` respects queued waiters; `mutex_is_locked` only observes
the current state. Reentrant use, cross-run use, non-owner unlock, and returning
while holding a mutex fail deterministically. Main/Signal failures fail the
Workflow; an Update-handler failure rejects that Update. OS mutexes and other
host synchronization are not safe substitutes in replaying Workflow code.

Workflow-state predicates can be awaited directly from the main Workflow or a
stackful Signal/Update handler:

```odin
ready := workflow.await_condition(
    ctx,
    &state,
    proc(state: ^Order_State) -> bool {
        return state.payment_received && state.inventory_reserved
    },
)
```

`await_condition` emits no command and does not poll host time. The predicate
must be deterministic, side-effect-free, and non-blocking. It is checked after
activation jobs and handler progress in the scheduler's stable order. The wait
inherits the selected Workflow cancellation token; a simultaneous true
predicate takes precedence over cancellation, as in the pinned Rust runtime.
External Signals/Updates or another durable operation must still provide an
activation capable of changing the state—conditions are not server-side events.
The CLI-devserver suite starts a Workflow blocked on such a predicate, delivers
an approval Signal, and observes completion through Core and Temporal Server.

Several durable operations can be raced with the declaration-biased Workflow
selector. Operations are started explicitly, then represented as borrowed
cases:

```odin
activity_future := workflow.start_activity(ctx, "charge", order)
timer_future := workflow.start_timer(ctx, 30 * time.Second)
cases := [?]workflow.Selector_Case{
    workflow.selector_activity(activity_future),
    workflow.selector_timer(timer_future),
}
selected := workflow.selector_select(ctx, cases[:])
if selected.index == 0 {
    _ = workflow.cancel_timer(ctx, timer_future)
    payment := workflow.await_activity(ctx, activity_future)
}
```

Ready cases are checked in slice order, independent of the order of jobs inside
the Core activation. The selector also supports Nexus operation Futures,
Signals, and Child Workflow start/completion futures. It does not silently
cancel losing operations;
callers retain their handles and must await or cancel them explicitly. The case
slice and Signal names are borrowed only until `selector_select` returns, while
the selected Signal payload/metadata views remain borrowed from the Workflow
run. The CLI-devserver suite live-proves an Activity/Timer race, fresh-stack
replay, and explicit timer cancellation.

`start_timer` traverses the distinct outbound `start_timer` interceptor and
returns its `Timer_Future` without suspending. `start_timer_with_options`
additionally accepts an explicit Workflow cancellation token and a UI-facing
summary:

```odin
timeout := workflow.cancellation_child(workflow.cancellation_root(ctx))
timer := workflow.start_timer_with_options(ctx, {
    duration = 30 * time.Second,
    cancellation_token = timeout,
    summary = "Wait for payment",
})
```

A zero token inherits the token selected by the current Workflow fiber. The
summary is copied into run ownership and encoded as Temporal command
`UserMetadata`. An interceptor may replace any option before calling
`workflow.outbound_start_timer_next`. `sleep_with_options` exposes the same
options for the sequential, suspending form. `sleep` first
traverses the stackful `sleep` chain and then the timer-start chain: code after
a timer-start continuation runs as soon as the Future is created, while code
after a sleep continuation runs only after the timer fires or is canceled.

Already-started durable operations can also be joined without hidden result
allocations:

```odin
first := workflow.start_activity(ctx, "charge", first_order)
second := workflow.start_activity(ctx, "charge", second_order)
cases := [?]workflow.Selector_Case{
    workflow.selector_activity(first),
    workflow.selector_activity(second),
}
if workflow.join_all(ctx, cases[:]) == .Ready {
    first_result := workflow.await_activity(ctx, first)
    second_result := workflow.await_activity(ctx, second)
}
```

`join_all` suspends until every declared Activity, Nexus operation, Timer, Child
Workflow start, or Child Workflow completion is ready. The ordinary typed
`await_*` operations then return synchronously and preserve declaration order.
`join_all_into` additionally accepts unique named Signals and writes their
consumed Payload/metadata views into a caller-provided `[]Join_Case_Result`.
This keeps the join allocation-free and makes Signal ownership explicit; the
views borrow the Workflow run. Exact duplicate cases are rejected. Cancellation
returns `.Canceled` if all cases have not already become ready, clears the
caller-provided result slots (`present=false`), and leaves partially ready Signals buffered.
Signal cases can be mixed with durable operation cases; those Futures retain
their typed `await_*` APIs after `.Ready`.

A 300-trace property composes both APIs around three Child Workflows. It varies
start and completion declaration order, Core job order, and the explicitly
canceled child; every intermediate completion must remain byte-identical after
eviction and fresh-stack replay. This exercises Child start/completion futures
as scheduler inputs rather than only testing their direct `await_*` paths.
Another 180-trace property varies two Signal case orders, both Core delivery
orders, and one/two-activation delivery. The CLI-devserver path sends `beta`
before `alpha`, observes a partial joined wait, and completes with values in
case-slot order after Core replay. A third property does the same for a mixed
Signal/Activity join. The live path delivers the Signal while a two-second Odin
Activity is still running, remains suspended, and completes only after Core
resolves the Activity. A focused deadline test proves cancellation does not
consume a partially ready Signal and reproduces all three completions on replay.

Deadline tokens use Temporal timers and the activation timestamp rather than
the host clock. Children inherit the earlier parent deadline; a shorter child
deadline adds its own timer, while a later one adds no redundant command.
Detached deadline trees remain independent of root Workflow cancellation:

```odin
branch := workflow.cancellation_child_with_timeout(
    workflow.cancellation_root(ctx),
    30 * time.Second,
)
deadline, has_deadline := workflow.cancellation_deadline(branch)
scope := workflow.cancellation_scope(ctx, branch)
defer workflow.cancellation_scope_end(&scope)
workflow.sleep(ctx, 24 * time.Hour) // returns when the branch deadline fires
```

Absolute variants are `cancellation_child_with_deadline` and
`cancellation_detached_with_deadline`; detached timeouts use
`cancellation_detached_with_timeout`. Expired deadlines cancel immediately and
emit no timer command. Deadline state and commands are replay-identical.

Returning from the Workflow while root cancellation is pending acknowledges it
with `CancelWorkflowExecution`; the return value is not completed successfully.
If Core includes `InitializeWorkflow` and `CancelWorkflow` in one initial
activation, the root cancellation is installed before Odin first resumes the
new stackful fiber, so the same rule holds even for an immediately returning
procedure.
The in-process Workflow Worker always has one deterministic Core poll thread.
With the default concurrency of one, that thread also executes Workflows. With
a larger value, it hashes each run ID onto a fixed executor thread. Every shard
owns independent `Fiber_Runtime` instances created from the process-local
registration factories, plus its own active/rejected run routes. A runtime
rejects overlapping or cross-thread entry, and shard shutdown destroys cached
fibers on the owning executor before releasing callback factories. Logger,
metric-emitter, interceptor, and activation-observer user data can be invoked
concurrently in this mode and must be synchronized by the application.
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
Registering a handler drains older matching buffered Signals before the
registering fiber continues.
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
temporal/history_json/    optional Temporal CLI History JSON replay converter
temporal/protobuf/        protobuf wire runtime used by generated code
temporal/activity/        Activity context
temporal/nexus/           sync/async Nexus operation context and results
temporal/worker/          Activity/Workflow/Nexus registry and concurrent polling
temporal/supervisor/      whole-Worker child process lifecycle and restart policy
temporal/codec/           owned/borrowed Payload model and json/plain codec
temporal/headers/         shared owned interceptor/context Payload map
temporal/workflow/        explicit-state and stackful-fiber experiments
temporal/workflow/isolate versioned activation/completion process framing
temporal/client/          owned Client and Workflow handle/control APIs
temporal/failure/         owned recursive Temporal Failure model
examples/sdk_hello_activity/ top-level Client/Worker lifecycle facade
examples/core_connect/    Odin -> Core -> Temporal proof
examples/config_connect/  Core TOML profile -> public Client proof
examples/worker_replace_client/ live Core Worker connection failover proof
examples/client_start/    Odin Client start-and-result proof
examples/client_control/  Odin Client signal/cancel/terminate proof
examples/workflow_concurrency/ single-Core-poller/external-executor overlap proof
examples/workflow_in_process_concurrency/ run-affine fiber overlap/migration proof
examples/update_with_start/ atomic create-or-attach Update proof
examples/eager_workflow/  same-connection eager Workflow start proof
examples/eager_activity/  same-worker eager Activity dispatch proof
examples/replay_history/  offline binary/CLI-JSON compatibility corpus runner
examples/worker_supervisor/ whole-Worker readiness/restart proof
examples/workflow_isolate/ persistent per-run Workflow process spike
examples/workflow_isolate_app/ shared parent/child Workflow registry
examples/workflow_isolate_core/ Core-polled crash/replay/nondeterminism proof
examples/schedule_lifecycle/ Schedule create/patch/trigger/delete proof
examples/hello_activity/  Odin hello Activity Worker
examples/hello_nexus/     synchronous Odin Nexus operation Worker
examples/standalone_nexus_client/ standalone Nexus Client lifecycle proof
examples/hello_nexus_workflow/ all-Odin Workflow-to-Nexus and replay proof
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

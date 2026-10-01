# Architecture: Temporal SDK for Odin

Status: Milestone 0 research, Milestone 1 bridge spike, a narrow Milestone 2
Activity Worker slice, schema-derived Core protobuf bindings, a bounded
Milestone 3 explicit-state-machine Workflow spike, and stackful-fiber replay
spikes covering serial operations, two outstanding Activities, handled Activity
failure, buffered Workflow signals, a durable signal/timer race, and
cooperative Workflow cancellation with detached cleanup, plus a reusable
Activity/Workflow/Nexus worker registry and poll loop, updated 2026-09-30. The Activity slice now also has a
generic Temporal Payload boundary, typed JSON Activity registration, and
structured Application Failures proven through a live retry. The Workflow fiber
now preserves generic Payloads across initialization, Activity
commands/results, completion, eviction, and replay, with typed JSON adapters
above that boundary. Workflow Activity options are represented explicitly and
preserved across fresh-stack replay. Activity context now carries heap-stable
execution identity, timestamps/timeouts, effective Retry Policy, priority, and
generic heartbeat details copied from Core before the poll decode arena dies.
The public Client owns Core connection lifecycle, starts or obtains a Workflow
handle, long-polls its close event, follows new runs, returns a generic or typed
Payload result, sends typed signals, requests cancellation, and terminates
executions. The Workflow runtime now preserves signal Payloads generically and
offers raw, typed JSON, and string await adapters.
Catchable Activity and Child Workflow results expose both ergonomic flattened
views and the complete recursive Temporal Failure tree, including every pinned
variant, encoded attributes, and Payload details.
Workflow initialization metadata, memo, and Search Attributes are also mapped
into run-owned state. Workflow code can read them through borrowed views and
emit deterministic memo/Search Attribute upserts or removals; unit, generated
property, replay, and live Service-history tests cover this boundary.

This document separates observed upstream behavior from project decisions. The
primary upstream snapshot used for implementation is
`temporalio/sdk-rust@e163abd6dc19040064986a63c8b8cf4756ffd320`, upstream main
as inspected on 2026-09-30 and the revision pinned by `sdk-python` commit
`5bb461265e6c1af29c5f25dbb871a7e67448c61e`. It contains `sdk-core` crate
0.9.0 and client/common crates 1.0.0, ten commits after the `core-v0.9.0`
release baseline `399cb7adb40f45eefbef90149e212743e474142c`. The generated C
header and C-bridge Cargo manifest are byte-for-byte unchanged from that
baseline; the upgrade changes Core behavior, Rust dependencies, and protobuf
schemas without changing Odin's raw ABI layout.

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

Core's own [architecture document](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/ARCHITECTURE.md)
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

### Offline history replay boundary

The pinned C bridge contains a dedicated replay path; it is not necessary to
simulate a Service or feed server Workflow Tasks directly. Odin calls
`temporal_core_worker_replayer_new(runtime, worker_options)`, which creates a
Core worker over an in-process history stream. Each
`temporal_core_worker_replay_push(worker, pusher, workflow_id, history)` call
synchronously decodes a binary `temporal.api.history.v1.History` and then
queues the owned decoded history onto Core's async stream. Malformed protobuf is
returned immediately as a Core-allocated error buffer.

After a successful push the normal language boundary applies:

```text
History protobuf -> Core state machines -> WorkflowActivation
                                      Odin Workflow runtime
WorkflowActivationCompletion <- deterministic commands
```

Core reports the outcome through a final `RemoveFromCache` activation. Following
the upstream Rust SDK's `ReplayOutcomeInterceptor`, Odin classifies `CACHE_FULL`
and `LANG_REQUESTED` as compatible replay, `NONDETERMINISM` as an incompatible
Workflow definition, `LANG_FAIL` as a language Workflow-task failure, and every
other eviction reason as an internal replay failure. Odin still completes that
final activation so Core can finish its lifecycle.

The language API retains registered Odin Workflow definitions across calls.
`replay_history` creates one Core worker for its single input;
`replay_histories` creates one Core worker for the entire batch, pushes the next
history only after the previous final eviction, and returns outcomes in input
order. Malformed protobuf, nondeterminism, and Workflow-task failures are
per-history results, while worker/lifecycle failures abort the batch. This
sequential use of the C bridge's capacity-one async channel keeps pusher
lifetime and result correlation explicit. Concurrent streaming replay remains
deferred until cancellation and backpressure have dedicated tests.

`client.fetch_history_protobuf` follows all Service pagination tokens, retains
each canonical encoded event until aggregation is finished, and returns the
single History message Core expects. Retaining event buffers matters: generated
protobuf decoders allocate message structure in the supplied arena but borrow
strings and bytes from their input. CLI `workflow show --output json` is not an
input to this ABI and is not part of the current replay path.

## The C bridge that exists today

The bridge source is
[`crates/sdk-core-c-bridge`](https://github.com/temporalio/sdk-rust/tree/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/sdk-core-c-bridge).
Its generated
[`temporal-sdk-core-c-bridge.h`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/sdk-core-c-bridge/include/temporal-sdk-core-c-bridge.h)
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
| Activity heartbeat | synchronous error return | binary `coresdk.ActivityHeartbeat` |
| Workflow poll | async callback | binary `coresdk.workflow_activation.WorkflowActivation` |
| Workflow completion | async callback | binary `coresdk.workflow_completion.WorkflowActivationCompletion` |
| Replay construction | `temporal_core_worker_replayer_new` | worker options, without a Client connection |
| Replay history input | `temporal_core_worker_replay_push` | binary `temporal.api.history.v1.History` plus Workflow ID |
| Replay input close | `temporal_core_worker_replay_pusher_free` | closes Core's history stream |
| Application metric meter | `temporal_core_metric_*` | opaque meters, instruments, typed attributes, and scalar records |
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

Core normally also classifies and retries retryable tonic transport failures.
The pinned bridge can lose tonic's nested transport-error source while turning
one HTTP/2 failure into a gRPC `Cancelled` status; in that form Core's retry
classifier no longer recognizes it. The Odin generic-RPC wrapper has one narrow
fallback for the observed `status = 1` plus `h2 protocol error` shape. It waits
100 ms and retries once, only when the caller already enabled Core retries. It
does not match explicit no-retry calls, ordinary application cancellation,
deadlines, other status codes, or a second failure. Every Core success/failure
buffer is still copied and freed before retrying or returning.

`client.Connect_Options` now maps the complete stable connection subset exposed
by the pinned C bridge: TLS/mTLS byte material and domain override, API key,
ASCII and binary gRPC metadata, retry and keepalive timing, HTTP CONNECT proxy,
DNS refresh, gzip selection, identity, and experimental payload/memo warning
thresholds. It intentionally does not expose the bridge's custom gRPC transport
override because that callback has a separate asynchronous lifetime contract.

The configured identity crosses two related but distinct boundaries. Odin sends
it to Core in `ClientOptions.identity`, where Core uses it as the connection's
SDK identity, and heap-clones the same value into `client.Client`. Every typed
Client request that has an identity field then copies that owned value into the
request protobuf: Workflow start, Signal/Signal-With-Start, Update and polling,
atomic Update-With-Start, cancel/reset/terminate, Workflow option and pause
mutations, Schedules, asynchronous Activity completion, and Nexus operations.
Low-level request encoders retain `temporal-sdk-odin@local` as a compatibility
default when called directly without an identity; connected Clients always use
their configured non-empty value. Exact-wire tests cover each request family,
and the Update-With-Start live proof reads Server history and observes the
configured example identity on start, both Updates, and the final Signal.

The raw metadata ABI is an array where every element is `key + "\n" + value`.
Odin validates lowercase gRPC key syntax, `-bin` placement, printable ASCII
values, duplicate keys, certificate/key pairing, and millisecond duration
constraints before creating a runtime. It owns the joined metadata buffers
through the connect callback; other option bytes are borrowed from the blocking
call and therefore remain live for the same interval. Core copies all options
into Rust-owned connection state before completing the callback. An API key
implicitly installs empty TLS options, matching upstream's configuration-profile
semantics and system-root behavior. Dynamic metadata and API-key replacement
use `temporal_core_client_update_metadata`,
`temporal_core_client_update_binary_metadata`, and
`temporal_core_client_update_api_key`; those calls synchronously copy their
inputs. Updating an API key cannot retrofit TLS onto an already-open plaintext
connection.

The implemented Client slice calls `StartWorkflowExecution` and
`GetWorkflowExecutionHistory` through `temporal_core_client_rpc_call`. Odin
constructs the WorkflowService request protobufs, generates a UUIDv4 request
ID, and uses `HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT` with `wait_new_event =
true`. Core owns gRPC transport and retries. The handle result loop maps
completed, failed, canceled, terminated, timed-out, and Continue-As-New close
events, following the next run by default as the current .NET SDK does.

`SignalWithStartWorkflowExecution` uses that same generic RPC symbol; the C ABI
adds no operation-specific function. The pinned Service request has exactly one
`Header` field. Core's Rust client builds the combined request from ordinary
start options and places that header on the Signal event, and Core's own
integration test observes it through the Workflow Signal interceptor. It is
therefore not possible to encode independent Workflow-start and Signal header
maps in this RPC. Odin exposes the field as `signal_headers`, rejects non-empty
`Start_Metadata.headers` for this operation, and continues to project memo,
Search Attributes, UserMetadata, timeouts, Retry Policy, start delay, priority,
and ID policies from `Start_Workflow_Options`. Eager start is rejected because
the combined request has no eager-task field. The Service rejects conflict
policy `Fail`; zero/unspecified retains Signal-With-Start's `UseExisting`
default. The response's `started` bit becomes
`Workflow_Handle.started_by_request`, allowing callers to distinguish creation
from reuse without comparing histories.

Update-With-Start uses WorkflowService `ExecuteMultiOperation`, again through
the existing generic `temporal_core_client_rpc_call` C symbol. The pinned API
currently accepts exactly two operations, in this order:
`[StartWorkflowExecutionRequest, UpdateWorkflowExecutionRequest]`. Core owns
transport, retry, authentication, and gRPC status propagation; the Service
starts both operations atomically. Odin owns the language-level composition,
Payload/header codecs, request IDs, response validation, and the two returned
handles. This matches Temporal's documented semantics: a missing Workflow is
started and receives the Update, while a running Workflow is reused and
receives the same Update atomically.

The public `update_with_start_*` family deliberately requires
`Workflow_Id_Conflict_Policy.Use_Existing`. It also rejects cron, eager start,
and start delay because the upstream multi-operation request explicitly
forbids those Start fields. The nested Update must omit both Run ID selectors;
Odin constructs it from the Start Workflow ID and waits for at least Accepted.
Independent start and Update Header builders are possible here because the
operations retain their ordinary request messages. A dedicated outbound
interceptor exposes both isolated working maps and both ordered argument
slices; ordinary Start and Update interceptors do not run a second time.

Implementation reuses the established request encoders, decodes their bytes
into one temporary arena, nests those messages, and immediately encodes the
outer request. No arena pointer escapes the call. The C response buffer remains
alive while the outer response and both nested messages are decoded. Odin then
heap-clones Workflow/Run/Update IDs and any immediate Update outcome before
freeing that buffer. `Update_With_Start_Result` owns one `Workflow_Handle` and
one `Update_Handle`; `update_with_start_result_free` releases both, while the
Client remains borrowed. The response must contain exactly Start then Update,
and both returned references must identify the same execution.

This slice adds generated roots for `ExecuteMultiOperationRequest/Response`,
but no raw C binding or new native symbol. It is covered by exact wire and
ownership tests, interceptor nesting, a 500-case property over both repeated
argument lists, and a CLI-devserver create-then-attach test. That live test also
requires the configured Client identity to occur on at least four history
events, proving it is not merely retained in the Odin handle. The live proof ran
against CLI 1.5.1 / Server 1.29.1 with
`frontend.enableExecuteMultiOperation=true`; current Temporal guidance
recommends Server 1.28 or newer for Update-With-Start.

The same generic RPC boundary now implements five read APIs without adding C
symbols:

- `GetSystemInfo` sends the empty pinned request through Core and returns an
  allocator-owned Server version plus all thirteen advertised capability bits.
  Odin does not infer support from version strings. A false bit may mean either
  unsupported or disabled by dynamic configuration, while newer experimental
  RPCs with no corresponding bit still need operation-specific probes.
- `DescribeWorkflowExecution` decodes the API response in a temporary arena,
  then copies the public snapshot into the caller's allocator. The current
  typed surface owns Workflow identity/type/task queue, status and timing,
  execution timeouts and counters, priority, memo/Search Attribute Payloads,
  pending Activity/child/Nexus summaries, Workflow pause metadata, deployment
  versioning/transition state, parent/root execution identity, auto-reset
  points, request-ID event mappings, time-skipping state, expiration/reset
  timestamps, and the pending Workflow Task. This covers every field in the
  pinned `WorkflowExecutionInfo` and `WorkflowExecutionExtendedInfo` messages.
  Pending Activity snapshots own legacy/new worker routing identity,
  manual/rule pause metadata, and effective Activity options. Callback schemas
  are preserved as allocator-owned canonical protobufs while their upstream
  language-level model continues to evolve.
- `GetWorkflowExecutionHistory` accepts the Service's opaque next-page token,
  page size, wait/filter, and archival flags. It returns owned event metadata
  and a canonical protobuf encoding of each decoded event. The re-encoding
  preserves every field known by the pinned Temporal API schema, but not
  unknown future fields. A response containing only `raw_history` is rejected;
  supporting encoded history blobs will require explicit codec/decompression
  handling instead of pretending that the typed event list is complete.
- `ListWorkflowExecutions` accepts Temporal's Visibility query string, requested
  page size, and opaque continuation token. Odin copies the limited
  `WorkflowExecutionInfo` visibility projection into owned summaries, including
  identity, status/timing/counters, parent/root identity, priority, memo, and
  Search Attributes. Experimental versioning details remain available only in
  the generated schema for now.
- `CountWorkflowExecutions` returns the Service's approximate total plus any
  aggregation groups. Group values are ordered according to the query's
  `GROUP BY` fields and remain codec-neutral owned Payloads; interpreting them
  as strings, integers, dates, or other Search Attribute types belongs above
  this transport-level API.

Schedules use the same generic client bridge and add no C ABI. The implemented
surface encodes `CreateSchedule`, `DescribeSchedule`, `ListSchedules`,
`CountSchedules`, `UpdateSchedule`, `ListScheduleMatchingTimes`,
`PatchSchedule`, and `DeleteSchedule`. Create and full replacement accept cron
strings, interval specs, structured/excluded calendars, timezone names and TZif
data, one Payload-backed Workflow action,
Workflow headers/memo/Search Attributes, Schedule memo/Search Attributes,
timeouts/retry/priority, overlap/catchup policies, and state. Create additionally
supports an optional immediate trigger. `Schedule_Handle` owns its ID and create conflict token while
borrowing the Client. Describe owns the normalized interval/cron projection,
action inputs, state/policies, running/recent/future execution information,
memo/Search Attributes, and the current conflict token. Patch powers
pause/unpause, manual trigger, and one or more backfill ranges; delete leaves
handle lifetime explicit. Matching-time results are allocator-owned. Full
update replaces all four Schedule fields. Its conflict token is explicit:
callers pass a token from Describe for optimistic concurrency, or an empty token
for an unconditional update. It is intentionally not read implicitly from the
handle because any intervening patch can make that token stale. Nil top-level
metadata preserves memo/Search Attributes; initialized empty metadata clears
them. Calendar input supplies useful high-level defaults for omitted fields,
while Describe exposes the Service-normalized structured form.

The pinned CLI Server 1.29.1 rotates the token and guards the mutation, but its
legacy scheduler implements update as an asynchronous signal: a mismatched token
is logged and the update is ignored after `UpdateSchedule` has already returned
success. This matches the upstream legacy scheduler's `processUpdate` /
`checkConflict` path and explains why current .NET and Rust clients do not expose
a reliable conflict-specific result. Odin therefore treats RPC success as
transport acceptance, not proof of application; callers needing that guarantee
must Describe and verify. The live test sends a deliberately different stale
definition after an intervening patch and proves it was not applied.

`CountSchedules` is present in the pinned
Core descriptors, but CLI Server 1.29.1 reports gRPC `UNIMPLEMENTED`; its wire
and owned-response tests pass, while live acceptance requires a newer server.

The ownership rule is the same for all of these calls: Core's callback response buffer
is copied by `temporal/core`, generated decoding borrows a short-lived virtual
arena, and the language-facing result records its own allocator. Its matching
free procedure releases nested strings, Payload metadata/data, repeated lists,
pagination tokens, and per-event protobuf buffers. The Workflow handle and
Client are only borrowed for the blocking call. The CLI-devserver acceptance
case requests a maximum page size of one and observes eight events over five
Service pages, demonstrating that the token is opaque and page size is a
request rather than an exact event-count promise.

Start requests now accept an explicitly owned `Start_Metadata` builder.
Codec-neutral and JSON helpers clone named header, memo, and Search Attribute
Payloads through the builder's recorded allocator; duplicate names replace the
previous owned value. Dedicated string setters encode static summary/details
through the same owned builder. Those values map
to `temporal.api.sdk.v1.UserMetadata` on `StartWorkflowExecutionRequest` and
use the default `json/plain` string representation expected by Temporal UIs.
`Start_Workflow_Options` borrows the builder only while
the blocking RPC request is encoded. Search Attribute typing shares the codec
layer's canonical Temporal metadata mapping with the Workflow runtime. A wire
unit test and 500 generated start-request cases decode the resulting
`StartWorkflowExecutionRequest` and verify every key, byte, type label, policy,
duration, retry field, priority field, and UserMetadata value.
The same request boundary maps Odin-native ID reuse/conflict enums, a borrowed
Retry Policy, cron schedule, start delay, and borrowed Priority onto the
corresponding Temporal API fields. Zero-valued optional policy fields retain
the Service defaults; a nil Retry Policy still means that the Workflow does not
retry. Odin rejects invalid duration, backoff, priority, cron/delay, and legacy
terminate-if-running/conflict combinations before calling Core. This remains
protobuf work above the generic Client RPC and adds no C symbol.

Eager Workflow start uses that same generic C call, but not as a raw boolean
shortcut. Core's connection tracks registered workers. For a request with
`request_eager_execution`, Core attempts to reserve a Workflow slot for the
same namespace/task queue, populates deployment options when applicable, and
clears the request if no slot exists. A returned `eager_workflow_task` is then
scheduled directly into the reserved worker before the response is returned to
Odin. `Workflow_Handle.eager_execution_accepted` reflects presence of that
response task. The live example starts from the worker's post-validation
`on_ready` hook, completes through the ordinary activation loop, and verifies
the history's `eagerExecutionAccepted` bit. No new C ABI is required, but a
worker on a different Client connection cannot participate in eager dispatch.

The same generic bridge now carries `SignalWorkflowExecution`,
`RequestCancelWorkflowExecution`, `ResetWorkflowExecution`, and
`TerminateWorkflowExecution`. Signals
support zero or one generic/typed Payload and generate a UUIDv4 request ID for
deduplication unless the caller supplies one. Cancellation does the same.
Cancellation and termination forward a started handle's first execution run ID
as a chain guard. `get_workflow_handle` performs no RPC and allows an empty run
ID to address the current execution by Workflow ID.

Reset remains a Client/Service operation rather than a Workflow command. Odin
encodes the selected Workflow/run, positive Workflow Task boundary event ID,
reason, request ID, client identity, and the modern repeated
`ResetReapplyExcludeType` list. It leaves the deprecated `reset_reapply_type`
field unset; the pinned schema documents signal reapplication as that field's
compatibility default. The language-facing `Post_Reset_Operation` covers both
pinned union arms. Signal operations borrow an ordered Payload list, Headers,
and generic Links. Workflow-option operations pair an explicit `FieldMask` with
nullable versioning override, priority, and time-skipping values; a masked nil
pointer requests that Server clear the field. Pinned, Auto-Upgrade, and one-time
deployment routing overrides encode through the modern oneof rather than its
deprecated compatibility fields. The Service
terminates the current run, creates the replacement run, and returns its Run
ID. Odin decodes that response in a temporary arena and clones the IDs into a
new owned `Workflow_Handle`; the input handle is borrowed and unchanged. The
live CLI-devserver case resets a completed repeated-argument Workflow at event
4, runs the replacement through the ordinary Core worker path, and requires the
same typed result from the new Run ID. A focused Server 1.32.0 run additionally
sets priority 2 atomically, observes
`WORKFLOW_EXECUTION_OPTIONS_UPDATED` in the new history before its first new
Workflow Task, and reads the priority back through Describe. The same server
rejects the schema-present Signal arm with `INVALID_ARGUMENT` and
`unsupported post reset operation`; that arm therefore has exact wire/order
coverage but is not advertised as live-capable. No C ABI symbol was added: this is exact
WorkflowService protobuf generation above Core's generic RPC dispatcher.

The ordinary `UpdateWorkflowExecutionOptions` path reuses that exact validation
and encoding model without manufacturing a reset. Odin sends namespace,
Workflow/run identity, client identity, the complete `WorkflowExecutionOptions`
message, and its explicit `FieldMask` through Core's generic WorkflowService
dispatcher. A successful response becomes an owned
`Workflow_Execution_Options` snapshot; callers explicitly free cloned
deployment/build IDs, priority fairness key, and fast-forward ID. The update
timestamp remains optional because Server 1.32 accepts and persists priority,
returns the canonical priority, and exposes it through Describe while omitting
that newer response field. The operation participates in the typed Client
outbound-interceptor chain. The only generated-code expansion is adding the
request/response pair to the existing deterministic proto roots; the C ABI is
unchanged and Core already dispatches the `UpdateWorkflowExecutionOptions`
symbol.

Workflow Pause follows the same Service-owned boundary. Odin does not suspend
or serialize a fiber itself: `PauseWorkflowExecution` changes the open
execution's server status and suppresses new Workflow/Activity dispatch until
`UnpauseWorkflowExecution`. The language layer encodes namespace, Workflow/run
ID, Core Client identity, reason, and an explicit or generated UUIDv4 request
ID, then uses the existing generic RPC C symbol. Both operations share one
typed outbound-interceptor family. A feature-enabled Server 1.32 run proves
status 8/Paused, typed Query rejection, acceptance of a Signal without an Odin
activation, subsequent delivery only after Unpause, and history event types 58
and 59. This is deliberately marked experimental: [Temporal's current Pause
documentation](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/workflow/workflow-pause.mdx)
requires self-hosted Server 1.30+ with `frontend.WorkflowPauseEnabled` and notes
that the API may change. Generated request/response roots and the pinned Core
dispatcher are versioned together; no C ABI extension is needed.

`client.Client` heap-owns a stable Core runtime and connection because
`core.Client` retains a pointer to its runtime. It also owns its namespace.
`Workflow_Handle` owns Workflow/run ID strings and borrows the Client. Closing
the handle before the Client is therefore an explicit lifetime requirement.
RPC success/failure buffers are copied out of Core and freed through
`temporal_core_byte_array_free`; gRPC status code and binary status details are
preserved in the Odin error.

Ordinary Workflow start additionally exposes the Service protobuf's repeated
`completion_callbacks` field as borrowed `Nexus_Completion_Callback` values.
Each entry maps exactly to `temporal.api.common.v1.Callback.Nexus`: URL plus
string header map and optional Temporal Links. Ordinary start options can
attach the same links to the new Workflow. The public tagged `Link` model maps
all six variants in the pinned common schema: `WorkflowEvent`, `BatchJob`,
`Activity`, `NexusOperation`, `Workflow`, and `Callback`. Callback links include
an execution kind and identity, optional component path, and dispatch request
ID. `WorkflowEvent` supports both event-ID and request-ID references. Odin validates each variant, constructs the
temporary protobuf graph, and discards it immediately after synchronous
encoding. The Service, not Core or the Odin process, invokes that URL when the
started Workflow reaches a terminal result. `SignalWithStartWorkflowExecution`
has no equivalent field in the pinned schema, so that API rejects completion
callbacks instead of silently dropping them.

Nexus Link conversion follows the
[current upstream TypeScript converter](https://github.com/temporalio/sdk-typescript/blob/2ed151670cd91ed3377ce6c9b6cb0c0de878348c/packages/nexus/src/link-converter.ts)
rather than inferring a URL vocabulary from protobuf names. `link_from_nexus`
accepts the Link type string plus its `temporal:` URL and parses these identities:

- `WorkflowEvent`: `/namespaces/{namespace}/workflows/{workflow}/{run}/history`,
  with either `EventReference` or `RequestIdReference` query fields;
- `NexusOperation`: `/namespaces/{namespace}/nexus-operations/{operation}/{run}/details`;
- `Activity`: `/namespaces/{namespace}/activities/{activity}/{run}/details`;
- `Workflow`: `/namespaces/{namespace}/workflows/{workflow}/{run}`, with an
  optional reason.

Components use URL percent decoding. Query `+` is decoded as a space. The
parser also accepts the legacy Core/server Workflow-event URL observed by the
live suite, whose authority is nonempty and whose event reference omits the
`referenceType` field. Parsed strings are allocated with the caller-selected
allocator and must be released once with `link_free`; caller-constructed Link
strings remain borrowed for synchronous request encoding. The pinned schema's
`BatchJob` and `Callback` variants are encoding-only because upstream defines
no Nexus URL mapping for them. The current Python SDK's generated common API at commit
[`5bb4612`](https://github.com/temporalio/sdk-python/blob/5bb461265e6c1af29c5f25dbb871a7e67448c61e/temporalio/api/common/v1/message_pb2.pyi)
and the pinned Core schema are the source for the `Callback` shape; the Odin
SDK does not invent a URL form that the upstream converter does not define.

### Worker and activity tasks

`temporal_core_worker_new` takes a connection and a C `WorkerOptions` struct.
Separate poll calls exist for workflow activations, activity tasks, and Nexus
tasks. The language layer controls poll concurrency and dispatch.

An [`ActivityTask`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/protos/protos/local/temporal/sdk/core/activity_task/activity_task.proto)
contains a task token and a `oneof`: start or cancel. A start includes activity
type, workflow identity, inputs/headers as Temporal `Payload`s, attempt and
timeout data, heartbeat details, retry policy, priority, and local/remote state.
The language layer looks up the registered Odin procedure and executes it.

The result is an
[`ActivityTaskCompletion`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/protos/protos/local/temporal/sdk/core/core_interface.proto)
with the original task token and an `ActivityExecutionResult`: completed,
failed, cancelled, or asynchronously completing. Heartbeats are a separate
protobuf call. Core sends the correct server RPC and manages server-facing
activity mechanics; Odin owns procedure invocation, cancellation delivery,
heartbeat API surface, payload conversion, failures, and activity concurrency.

### Nexus task boundary

The pinned bridge exposes Nexus as a fourth independently enabled Worker task
type, not as a Workflow activation or an Activity task. Core polls the endpoint
task queue and returns one serialized `coresdk.nexus.NexusTask`; the language
SDK must decode it, dispatch the named service and operation, then return one
serialized `coresdk.nexus.NexusTaskCompletion` with the same task token.

`NexusTask` contains the original `PollNexusTaskQueueResponse` or a Core-local
cancel request, plus Core's deadline and endpoint name. A start request carries
the service, operation, request ID, callback URL/headers, input Payload, and
operation headers. Its completion is one of a Nexus response, cancellation
acknowledgement, or Temporal Failure. A synchronous successful operation
returns exactly one Temporal Payload; an asynchronous start returns the durable
operation token through the upstream Nexus response union.

The implementation now crosses that boundary for synchronous results, async
token starts, protocol cancellation, and Core-local task cancellation:

- `temporal/core/raw` binds `temporal_core_worker_poll_nexus_task` and
  `temporal_core_worker_complete_nexus_task`;
- the repository generator roots both Core Nexus task messages and their exact
  transitive API/Nexus protobuf graph;
- `temporal/core` owns blocking poll/completion wrappers, validates fixed Nexus
  slots and poller behavior, and drains the enabled stream before worker
  finalization;
- `temporal/nexus` defines a callback-scoped borrowed context, header/deadline
  views, atomic cooperative cancellation, synchronous results, async start
  tokens, cancel results, and handler errors;
- `temporal/worker` owns cloned service/operation registrations, enables Nexus
  only when that registry is nonempty, copies every value that outlives the
  decode arena, routes opaque binary task tokens, and executes handlers through
  a bounded pool sized by `max_concurrent_nexus_tasks`; async operations may add
  a distinct cancel handler to the same registration;
- `examples/core_connect` validates a Nexus-enabled worker against the CLI dev
  server, begins the real Core poll path, initiates shutdown, observes the
  shutdown sentinel, and finalizes cleanly.
- `examples/hello_nexus` is called through a real CLI-devserver endpoint by a
  Workflow caller and returns a typed JSON string through Core.
- `temporal/workflow/nexus_operation.odin` owns a replay-stable operation table,
  emits `ScheduleNexusOperation`/`RequestCancelNexusOperation`, consumes Core's
  distinct `ResolveNexusOperationStart` and `ResolveNexusOperation` jobs, and
  exposes Payload, typed JSON, and string `start`/`await`/`execute` forms.
- `examples/hello_nexus_workflow` runs the caller Workflow and Nexus handlers in
  one Odin Worker. It live-proves success, a non-retryable handler
  failure, schedule-to-close timeout/cancellation, and two concurrent successful
  calls composed with selector/join, then fetches the server history and passes
  every branch through Core's offline replayer. An async start returns a
  durable token, then routes `CancelOperation` to the registered Odin cancel
  handler with that identical token. Separate live branches cover all four
  cancellation policies: `Abandon` detaches without a cancel request,
  `Try_Cancel` requests cancellation and resumes immediately,
  `Wait_Cancellation_Requested` waits for request acceptance, and
  `Wait_Cancellation_Completed` waits for the backing Workflow's terminal
  callback. Another async operation starts an Odin completer Workflow with the
  request's callback URL and headers; the Service delivers that Workflow's
  terminal JSON Payload to the waiting caller. Workflow-backed cancel handlers
  use their operation token as the backing Workflow ID and request cancellation
  through the Odin Client.

This proves the complete synchronous route from a Temporal Workflow through
Service, Core, the C ABI, and an Odin operation callback and back. Unknown
operations become non-retryable `NOT_FOUND` Nexus handler failures. Callback
errors use a Temporal `Failure` containing `NexusHandlerFailureInfo`, which the
pinned Core explicitly requires; the deprecated Nexus `HandlerError` message is
not used. A Core-local cancel task atomically marks the matching callback
context, and a cooperative callback is acknowledged with `AckCancel`.

Workflow-side ownership follows the other stackful operations. Endpoint,
service, operation, string headers, input Payload, result Payload, async
operation token, encoded recursive Failure, and cancellation metadata live in
the Workflow-run arena. Futures contain only a stable sequence number because
the dynamic operation table may move. On eviction, result/input Payload buffers
are released before the arena; replay reconstructs the table by re-executing
the Workflow and reproduces the exact command bytes. Core remains responsible
for endpoint routing, Nexus state machines, history interpretation, timeouts,
and translating server events into the two activation-job forms.

The remaining boundary is intentionally explicit. Start handlers may return a
durable operation token, and `CancelOperation` routes an owned copy through a
separately registered handler before emitting the exact cancel response.
Registrations without that handler retain a non-retryable `NOT_IMPLEMENTED`
response. Async success through a Workflow completion callback and all four
cancellation policies are live-proven. The handler also
preserves Core's raw Nexus links, parses all four upstream URL/type forms into
the generic pinned Link model, and attaches those values to callbacks and
ordinary Workflow starts. All six pinned protobuf variants encode; `BatchJob`
and `Callback` are encoding-only because upstream has no Nexus URL converter
for them. Outbound metrics are not implemented. The
outbound interceptor and selector/join integration are implemented.

### Standalone Nexus operation Client boundary

The upgraded pinned API and C bridge also carry the pre-release standalone
Nexus operation RPC family. This is a Client-side execution model: no caller
Workflow is required, but the operation is still routed through the same Nexus
endpoint, task queue, Core Nexus poll, and Odin handler registry described
above. It adds no raw C symbol. Odin serializes the exact WorkflowService
request and invokes Core's existing generic `temporal_core_client_rpc_call`
with these verified method names:

- `StartNexusOperationExecution`;
- `PollNexusOperationExecution`;
- `RequestCancelNexusOperationExecution`;
- `DescribeNexusOperationExecution`;
- `TerminateNexusOperationExecution`;
- `DeleteNexusOperationExecution`;
- `ListNexusOperationExecutions`;
- `CountNexusOperationExecutions`.

`temporal/client/nexus_operation.odin` now exposes an owned
`Nexus_Operation_Handle`, borrowed start options, codec-neutral and typed JSON
start/result helpers, Started/Closed polling, monotonic local wait timeouts,
Core cancellation tokens, cancellation requests, an allocator-owned typed
description, termination, and deletion. The description preserves execution
and pending status, all server timing/attempt fields, cancellation state,
headers, Search Attributes, static summary/details, optional input/result or
encoded terminal Failure, and the opaque long-poll token. The list API owns its
limited execution summaries, typed status/times, Search Attribute Payloads,
and pagination token. Count preserves both the approximate total and optional
group values as codec-neutral Payloads. Both participate in the public Client
outbound-interceptor chain. Start, Poll/result waiting, Describe, and the
cancel/terminate/delete mutation family participate in the same chain, giving
the complete standalone lifecycle typed nested continuations. Starts carry all three operation timeouts, ID
reuse/conflict policies, Nexus string headers, typed Search Attributes, static
summary/details, generic Links, completion callbacks, and the three upstream
`Use_Existing` attachment flags. Describe owns and decodes all six common Link
variants plus Nexus, Internal, and NexusHandler callbacks, including delivery
state, attempt timing, headers, source context, Links, and encoded failure.
The shared Link owner uses a dynamic string collection rather than a fixed
four-string ceiling because callback component paths are unbounded.

The server compatibility boundary was measured rather than inferred:

| Temporal dev Server | Probe result | Meaning |
| --- | --- | --- |
| 1.29.1 from CLI 1.5.1 | gRPC 12 `UNIMPLEMENTED`, unknown method | Standalone Nexus is unavailable |
| 1.32.0 from CLI 1.9.1 with `nexusoperation.enableStandalone=true` | gRPC 5 `NOT_FOUND` for a deliberately absent endpoint | RPC is registered and the request reached endpoint lookup |

The complete established integration matrix remains green on Server 1.29.1.
When run wholesale against 1.32.0, an older Workflow-originated Nexus timeout
case observes 11 tasks/no Core-local handler cancellation instead of the
1.29.1 baseline's 12 tasks/one cancellation. The standalone-focused suite is
green on 1.32.0, but the broader Nexus behavior change must be understood
before 1.32.0 replaces 1.29.1 as the repository-wide baseline.

Against Server 1.32.0 the live slice creates a real endpoint, starts the
existing Odin `hello` Nexus handler directly from the Odin Client, waits with
`PollNexusOperationExecution`, and owns/decodes the JSON result after Core's
callback buffer and protobuf arena are gone. A second async handler returns an
operation token; the Client requests cancellation, Server dispatches
`CancelOperation`, and the registered Odin cancel handler verifies the same
token. `RequestCancelNexusOperationExecution` means that cancellation was
requested, not that the asynchronous operation is already in a terminal
canceled state. Terminal outcome still comes from the handler/callback path.
The third path starts an unfinished asynchronous operation, describes its
owned `Running` state and input, terminates it, observes the typed
`Terminated` outcome through both Poll and Describe, finds and counts it through
an exact Visibility query, deletes it, and requires a subsequent Describe to
return gRPC status 5/`NOT_FOUND`. It also round-trips a custom typed Search
Attribute and static summary/details and proves that `Use_Existing` returns the
same Run ID without dispatching another handler task. The initial Describe
returns an opaque token which is sent on the later Describe; Server then returns
the changed terminal state and outcome. Contrary to the pinned proto comment
that a token is absent once complete, Server 1.32.0 still returns a 132-byte
token with that terminal response. Odin therefore keeps the token opaque and
does not infer liveness from its presence.

The pinned API schema includes operation Links, completion callbacks, and the
three on-conflict attachment flags. Exact wire and allocator-ownership tests
cover those fields and every Link/callback variant. The current CLI 1.9.1 dev
Server 1.32.0 accepts the fields but returns zero operation Links and callbacks
from Describe, including fields supplied on the original Start. Therefore the
focused live test does not claim persistence of those still-experimental
fields; a newer server must be rechecked before that capability is advertised
as live-proven.

The poll RPC may return a successful empty response when a server long poll
expires. Odin treats that as a repoll signal, recomputes the remaining
monotonic client deadline, and returns `Wait_Timed_Out` only when that complete
budget is exhausted. Result Payloads, operation tokens, IDs, and encoded
recursive Failures are cloned before the decode arena is destroyed. The handle
owns its IDs using the heap and must be freed before its borrowed Client is
closed.

### Standalone Activity client boundary

The pinned upstream API now has a standalone Activity lifecycle separate from
Workflow-scheduled Activity commands:

```text
Odin Client StartActivityExecution
  -> Core generic WorkflowService RPC
  -> Server persists standalone execution
  -> ordinary Core Activity task poll
  -> existing Odin Activity callback/completion path
  -> Odin Client PollActivityExecution
```

The public slice lives in `temporal/client/standalone_activity.odin`,
`standalone_activity_options.odin`, and `standalone_activity_visibility.odin`.
It exposes codec-neutral/JSON Start, an
owned ID/Run-ID handle, monotonic bounded or Core-cancelable result waiting,
recursive terminal Failure bytes, cancel/terminate/delete, owned Describe
snapshots, and paginated Visibility List/Count. Start validates both
close-timeout alternatives, Retry Policy, priority, and ID reuse/conflict
policy, and encodes Header Payloads, Search Attributes, static summary/details,
Links, Nexus completion callbacks, and `Use_Existing` attachment options. Poll clones the Run ID and result Payload before destroying its
protobuf arena. Describe additionally owns execution state, timing, optional
input/outcome/heartbeat data, headers, Search Attributes, Links, Retry Policy,
priority, UI metadata, and canonical Failure bytes. Callback variants remain
owned canonical protobuf bytes until their still-evolving API deserves a
stable Odin model. List pages own every summary and continuation token; Count
owns codec-neutral group Payloads. No new C symbol is required: client
operations use `temporal_core_client_rpc_call`, while task execution uses the
established Core Activity Worker ABI.

This was measured on both repository server baselines. CLI 1.5.1/Server 1.29.1
returns gRPC 12/`UNIMPLEMENTED` for `StartActivityExecution`. CLI 1.9.1/Server
1.32.0 live-proves successful Start/Poll/Describe/result through an Odin Worker,
Visibility List/Count with an exact Activity-ID query, and scheduled-state
cancellation, termination, and deletion. The entire pinned slice has typed
Client outbound interceptors. Pause, Unpause, Reset, and
`UpdateActivityExecutionOptions` use the generic Core RPC bridge as well and
are live-proven through a workerless scheduled execution on Server 1.32. The
options API accepts only the canonical field-mask paths that Server's
`MergeActivityOptions` implements: `task_queue.name`, the four timeouts,
`start_delay`, whole `priority`/`retry_policy` messages, and their writable
subfields. Task-queue kind/normalized name and the retry policy's repeated
non-retryable-error field are deliberately unavailable as leaf updates. The
owned response clones task queue, duration presence, Retry Policy, and Priority
before its protobuf decode arena is destroyed. The SDK capability is therefore
true, independently of the connected Service's support.

Memory ownership follows the existing poll boundary. The C callback lends a
Core `ByteArray`; `temporal/core` copies it into an Odin allocation and calls
`temporal_core_byte_array_free` immediately. Generated decoding then borrows
strings and bytes from that copy and allocates nested collections from the
caller's arena. The dispatcher clones the token, Payload, strings, and both
header maps into one execution owner before destroying that arena. Completion bytes are Odin-owned for the duration of the
blocking completion call; the bridge decodes them synchronously before its
callback returns. Core owns Service polling, protocol/state-machine behavior,
deadlines, and transport. Odin owns handler lookup/execution, Payload codecs,
language cancellation, handler errors, and the completion it produces.
`nexus.result_async` borrows its token through the immediate typed adapter;
`nexus.result_async_copy` heap-copies temporary tokens, transfers that ownership
to the internal Payload result, and frees it only after completion encoding.
This distinction is regression-tested because freeing an owned Workflow handle
inside a handler otherwise corrupts a borrowed token before Worker encoding.

Odin now implements the fourth result variant for remote Activities. A callback
returning `WillCompleteAsync` releases the worker task without resolving the
server-side Activity. The callback's task token is borrowed, so the application
must copy it before returning. The public Client addresses the pending Activity
by task token, Workflow/Activity ID, or standalone Activity ID and calls the
pinned bridge's generic WorkflowService dispatcher with:

- `RespondActivityTaskCompleted[ById]`;
- `RespondActivityTaskFailed[ById]`;
- `RespondActivityTaskCanceled[ById]`;
- `RecordActivityTaskHeartbeat[ById]`.

This adds no C ABI symbol. It adds the exact WorkflowService request/heartbeat
response schemas to the generated Odin protobuf roots. The live path is Odin
worker → Core `WillCompleteAsync` → Service pending Activity → separate Odin
Client → Core RPC bridge → Service terminal operation → waiting Workflow. The
CLI-devserver matrix proves task-token and Workflow/Activity-ID heartbeats and
completion, task-token failure with typed JSON details, and By-ID cancellation
acknowledgement. Cancellation is not an unsolicited terminal operation: the
Workflow first requests cancellation, the Odin Client observes
`cancel_requested` in a heartbeat response, and only then calls
`RespondActivityTaskCanceledById`. Temporal history is decoded to verify the
failure type/details and cancellation details. Local Activities explicitly
reject asynchronous completion because no server task remains to complete.

### Activity execution context boundary

Core's `coresdk.activity_task.Start` is also the source of Activity execution
metadata. The Odin adapter now maps it as follows:

| Odin `activity.Info` value | Source |
| --- | --- |
| Activity ID/type/attempt and standalone Activity run ID | `Start` |
| Workflow ID/run ID/type/namespace | `Start.workflow_execution` and adjacent fields |
| Scheduled/current-attempt-scheduled/started timestamps | protobuf timestamps in `Start` |
| Schedule-to-Close, Start-to-Close, and heartbeat timeouts | protobuf durations in `Start` |
| Retry Policy | `Start.retry_policy`; this is the Service's effective policy, not necessarily the request |
| Priority and local/remote flag | `Start.priority` and `Start.is_local` |
| Previous heartbeat details | repeated generic Temporal Payloads in `Start.heartbeat_details` |
| Task queue | language worker configuration; it is not present in `Start` |
| Namespace fallback | worker/client namespace when Core sends an empty workflow namespace |

This matches the construction boundary used by the current
[.NET](https://github.com/temporalio/sdk-dotnet/blob/f8b622e45ec553cbfeee8c508c4a7a989a1292c6/src/Temporalio/Worker/ActivityWorker.cs)
and [Python](https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_activity.py)
SDKs: they decode Core's start message and add the worker task queue. Both also
preserve heartbeat details as generic payloads and surface the effective Retry
Policy. Core does not send one explicit deadline. Odin derives one when enough
data is present by taking the earlier of `scheduled_time +
schedule_to_close_timeout` and `started_time + start_to_close_timeout`. The
`started_time` field is explicitly defined by Core as the instant Core received
the attempt through polling.

All generated protobuf strings, repeated fields, and payload bytes initially
borrow the per-poll decode arena. Before that arena is destroyed, the worker
copies every value used by the callback into a heap-stable Activity execution
record and its `Activity_Info_Owner`. `activity.Info`, its nested slices, `task_token`, and
values returned by `heartbeat_detail_payload` are borrowed from that owner and
are valid only until the callback returns. Destruction frees heartbeat Payload
metadata/data, retry-type strings, identity strings, and the execution-owned
task token exactly once. A unit test destroys the decode arena before reading the full context to
exercise this ownership boundary.

`heartbeat_detail_json` is only a convenience over the generic Payload view.
The live retry case records JSON progress on attempt 1, fails retryably, and
requires attempt 2 to recover the server-redelivered detail. Thus the tested
path is Odin → Core → Service → Core → a fresh Odin Activity context, rather
than only an in-process codec round trip.

### Workflow activations and completions

[`WorkflowActivation`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/protos/protos/local/temporal/sdk/core/workflow_activation/workflow_activation.proto)
has a run ID, workflow time, replay flag, deterministic history length, and an
ordered list of jobs. Jobs include initialization, timer firing, activity
resolution, signal/query/update delivery, child/external workflow resolution,
random seed changes, patch notification, Nexus resolution, and cache eviction.

The language runtime consumes all jobs for an activation and returns a
[`WorkflowActivationCompletion`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/protos/protos/local/temporal/sdk/core/workflow_completion/workflow_completion.proto).
A success carries ordered `WorkflowCommand`s—start/cancel timer, schedule/cancel
activity, complete/fail/cancel workflow, child/external operations, queries,
updates, memo property modifications, Search Attribute upserts, and more. A
failure carries a Temporal `Failure`.

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
| Initial Workflow metadata, memo, and Search Attribute delivery | Run-owned views, codecs, mutation API, and deterministic property command order |
| Protocol-level heartbeat transport | Activity context and user-facing heartbeat/cancellation API |
| Logging subscribers and Prometheus/OTLP metric export | Selecting Core telemetry, Odin-facing configuration, and safe callback routing |

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
  `Priority`, plus `Memo` and `SearchAttributes` maps;
- Google `Timestamp` and `Duration`;
- all transitively used enums and `oneof`s.

User data is a separate codec layer inside `temporal.api.common.v1.Payload`.
JSON is acceptable as the first user codec, expressed through payload metadata
and data, but it does not replace the protobuf envelope. The codec belongs in
`temporal/codec`, not the Core binding.

The implemented language boundary distinguishes borrowed and owned values:

- `codec.Payload_View` borrows metadata, data, and external-payload details for
  the duration of a callback;
- `codec.Payload` owns cloned fields through a recorded Odin allocator and must
  be released with `codec.payload_free`;
- `codec.encode_json` and `decode_json` operate on arbitrary Odin values while
  requiring the Temporal `encoding=json/plain` metadata contract;
- `worker.register_json_activity` specializes a compile-time trampoline from
  `proc(Context, Input) -> Output` to the worker's uniform payload callback;
- `worker.register_json_activity_result` maps
  `proc(Context, Input) -> activity.Result(Output)` to success or a structured
  Temporal Application Failure;
- `worker.register_payload_activity` exposes the lower-level payload boundary
  without coupling custom codecs to Core protobuf types;
- `worker.register_payloads_activity` exposes all repeated Activity arguments
  as an invocation-scoped borrowed slice, while the Worker owns cloned
  Payloads until callback completion.

Metadata is represented as owned entries rather than only an `encoding` string;
lookup follows protobuf map last-value-wins semantics and SDK construction
replaces duplicate keys. This preserves codec-specific keys such as
`messageType`, and the bridge also copies the schema's external-payload size
details. Core's repeated `Start.input`, `ScheduleActivity.arguments`, and
`ScheduleLocalActivity.arguments` fields remain repeated across the Odin
boundary. Workflow instances own cloned argument Payloads in their run arena;
the Worker clones polled arguments to heap storage before releasing the Core
poll buffer. Codec-neutral `*_activity_payloads*` APIs expose the full list.
Existing string and typed JSON adapters accept exactly one Payload and return a
non-retryable `OdinActivityArgumentCountError` for zero or multiple arguments.
The singular decoded `Activity_Task.input` field is retained only as a borrowed
alias of `inputs[0]` for source compatibility.

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

The current roots are `coresdk.ActivityHeartbeat`,
`coresdk.activity_task.ActivityTask`,
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
- `temporal_core_worker_record_activity_heartbeat` borrows its protobuf bytes
  only for the synchronous call. A non-null return is a Core-allocated UTF-8
  error and must be freed exactly once with the worker runtime.
- `temporal_core_worker_replay_push` borrows Workflow ID and History bytes for
  the synchronous call. Its non-null `fail` member is Core-allocated and is
  freed with `temporal_core_byte_array_free`. A successful call schedules a send
  through the pusher; the pusher must remain alive while that history is being
  consumed.
- `temporal_core_worker_replay_pusher_free` owns and drops the opaque pusher and
  closes Core's history stream. Odin calls it after the final replay eviction
  and before normal worker shutdown/finalization. It must be called exactly
  once and before freeing the runtime.
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
Each Activity invocation has a heap-stable execution record plus a separate
temporary arena; allocations made by the Activity, including its returned
string, are released after result encoding. Its owned task token, input, and
activity ID remain alive until the callback thread is joined, which also keeps
heartbeat byte references and cancel routing valid.

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

The distribution plan, with its current status, is:

1. Done: pin one tested sdk-rust commit and the wrapper `Cargo.lock`.
2. Defined in CI: build four artifacts from source with the pinned toolchain;
   both arm64 targets have also been executed locally.
3. Done: give each artifact a platform/architecture manifest and checksum.
4. Pending: fetch/select exactly one matching dynamic library during SDK setup.
5. Partial: set `@rpath` on macOS and supply an application rpath on Linux;
   complete `otool -L`/`readelf -d` policy checks on all hosted targets.
6. Defined in CI: run ABI and init/shutdown smoke tests on every artifact;
   retain the deeper CLI-devserver suite as a separate integration gate.

`scripts/package-native.sh` emits one target-named archive containing Core's
dynamic library, the static fiber shim, the exact C header, licenses, and a
version/checksum manifest. `scripts/verify-native-package.sh --smoke` extracts
that archive and links the real Core runtime lifecycle test against its library
directory. The four-platform GitHub Actions matrix performs the build, full
unit/property suite, example compilation, packaging, and packaged-ABI smoke.
The explicit `native/sdk-core-revision.txt` pin also lets the build work from a
complete source archive where Git submodule metadata is unavailable.

The verified macOS arm64 release dylib is about 32 MiB and depends only on
system frameworks/libraries (IOKit, CoreFoundation, Security, iconv, and
libSystem). The first Linux packages target glibc; musl and certificate-store
policy remain separate validation work. Static linking would require an
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
temporal_core_forwarded_log_target              (raw-only)
temporal_core_forwarded_log_message             (raw-only)
temporal_core_forwarded_log_timestamp_millis    (raw-only)
temporal_core_forwarded_log_fields_json         (raw-only)
temporal_core_client_connect
temporal_core_client_free
temporal_core_client_update_metadata
temporal_core_client_update_binary_metadata
temporal_core_client_update_api_key
temporal_core_cancellation_token_new
temporal_core_cancellation_token_cancel
temporal_core_cancellation_token_free
temporal_core_client_rpc_call
temporal_core_worker_new
temporal_core_worker_validate
temporal_core_worker_poll_activity_task
temporal_core_worker_complete_activity_task
temporal_core_worker_record_activity_heartbeat
temporal_core_worker_poll_nexus_task
temporal_core_worker_complete_nexus_task
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

Original live-server verification environment:

- Odin `dev-2026-05:ea5175d86` (the current build matrix uses
  `dev-2026-09:a2fb372b7`);
- Temporal CLI 1.5.1, dev server 1.29.1;
- macOS arm64;
- Core C bridge pinned at `e163abd`;
- runtime lifecycle tests green;
- end-to-end `GetSystemInfo` green through a directly started
  `temporal server start-dev --headless` process.

### Runtime telemetry boundary

The pinned bridge represents telemetry entirely in the synchronous
`TemporalCoreRuntimeOptions -> TemporalCoreTelemetryOptions` construction call.
Logging is a filter plus an optional callback. Metrics select exactly one of
Prometheus, OpenTelemetry, or a custom meter; shared fields contain global
tags, metric prefix, and service-name attachment. Prometheus binds its own HTTP
listener inside Core. OpenTelemetry builds an OTLP exporter inside Core and
supports gRPC/HTTP, headers, temporality, periodicity, duration units, and
histogram overrides. Odin does not implement either protocol.

`Runtime_Option_Bindings` mirrors this pointer graph with address-stable raw
records. It encodes typed tag/header pairs to the bridge's newline-delimited
map, points every `ByteArrayRef` at caller-owned memory, calls
`temporal_core_runtime_new`, and destroys all temporary arrays immediately
after that call returns. This is safe because the bridge converts those values
to owned Rust strings/maps during `Runtime::new`. The resulting Core runtime
owns exporter tasks and listener state until `temporal_core_runtime_free`.
The public Client accepts the same configuration while retaining sole
ownership of the created runtime.

The bridge also exposes a separate application-metric API over the exporter
owned by the Runtime. Odin now binds all eleven functions:
`temporal_core_metric_meter_new/free`,
`temporal_core_metric_attributes_new/new_append/free`,
`temporal_core_metric_new/free`, and the four integer, float, signed-integer,
and duration record calls. The three C option/attribute records and both enums
are mirrored with compile-time size/offset assertions against the generated
header. This path is not protobuf-serialized and never reaches Temporal Server.

`Metric_Meter` owns one Core meter and immutable base-attribute handle. A
`Metric` clones both into independent Core handles when it is created, so the
factory meter can close before recording. Per-record attributes create one
temporary Core-owned attribute handle, record synchronously, and free it only
after the C call returns. Input names, descriptions, units, keys, and string
values are borrowed for construction; Rust copies them immediately. Instruments
and meters must close before their Runtime. Since Odin records are copyable,
their owning handle values must be moved by convention rather than duplicated.

The C record functions panic on a mismatched instrument kind. The high-level
wrapper retains the kind and rejects every wrong record operation before FFI;
it also rejects invalid kinds, empty attribute keys, non-finite floats,
negative float-histogram values, and negative durations. Duration values use
the bridge's integer-millisecond contract and round a positive fraction upward.
Counters and integer histograms/gauges accept `u64`; up-down counters accept
`i64`. Typed attributes carry string, integer, float, or boolean values.
Core's `get_metric_meter` deliberately bypasses the Temporal-only metric-prefix
wrapper, so application names are exported exactly as supplied while global
tags and optional `service_name` still apply.

Activities receive an execution-owned `Metric_Meter` through their callback
Context. The Worker creates it with namespace, task queue, and Activity type
base attributes after decoding Core's Activity task, and closes it only after
the callback result has been handed off. A Runtime without an exporter supplies
a validating no-op meter, keeping application code independent of deployment
observability settings.

Nexus start and cancellation handlers now receive an execution-owned meter by
the same rule. Its base attributes are namespace, task queue, Nexus service,
and Nexus operation. The Worker constructs it only after the task's owned
`Nexus_Info` has been built, passes a callback-scoped pointer through
`nexus.Context`, and closes it when the routed execution is reaped. The no-op
path is identical when the Runtime has no exporter. A live Prometheus scrape
observes both start- and cancellation-handler counters with every automatic
attribute before Worker shutdown.

Workflow code never receives the Core handle. Its `metric_*` procedures build a
borrowed synchronous record and return no status. The language runtime drops
records for activations replaying history events, while allowing query-only
replay diagnostics. The Worker-owned backend adds namespace, task queue, and
Workflow type attributes, lazily caches typed Core instruments, and destroys
the cache with the registered Workflow runtime. Consequently a native handle
never lives on a fiber stack that Core eviction can discard without running an
Odin `defer`, and exporter state cannot be used for deterministic branching.
Calling the lower-level Runtime meter directly from Workflow code remains an
unsupported replaying side effect.

The focused live proof configures the hello Worker with Core's Prometheus
exporter, emits one Workflow counter before scheduling an Activity, and emits
one Activity counter through `activity.metric_meter`. Cache-disabled execution
then evicts the first fiber, replays the Workflow on a fresh stack, resolves the
Activity, and suspends on a durable timer. A scrape at that point observes both
series with their namespace/task-queue/type scope and value exactly one. This
joins replay classification, language suppression, backend instrument caching,
Core export, Activity-context ownership, and shutdown in one server-backed
test rather than inferring the language path from the lower-level meter test.

Logging is deliberately asymmetric. A nil `forward_to` makes Core write to its
console subscriber. A non-nil callback may be invoked from any Core thread and
the raw ABI provides no user-data pointer. Odin therefore installs one
synchronized process-wide route, rejects a second simultaneous forwarding
Runtime, and associates the public callback/user-data pair with the one active
route. The C trampoline calls all four borrowed accessors (`target`, `message`,
`timestamp_millis`, `fields_json`) and heap-copies every string before invoking
the Odin callback. `Runtime_Log` is callback-scoped and its fields must be
cloned again if retained. `runtime_close` first frees Core—which deactivates its
forwarder—then takes the route lock, thereby waiting for any callback already
inside the Odin dispatcher before clearing user data. Callbacks may originate
on concurrent Core threads but are serialized by that lock; they must not call
Runtime create/close or panic across the C boundary.

The live acceptance test first configures forwarding at trace level, performs
`GetSystemInfo`, and requires at least one record with non-empty target/message
and a positive timestamp. A lifecycle unit test proves exclusivity and route
reuse after close. A separate live path configures the Prometheus exporter on loopback,
performs `GetSystemInfo`, scrapes `/metrics` while the runtime remains alive,
and observes `temporal_request_total` with Core's service tag and an Odin global
tag. The same scrape requires an Odin-created counter with base and per-record
string/integer/float/boolean attributes and the exact value three. This proves
Core's listener, application-meter ABI, instrumentation, option ownership, and
shutdown path together. OTLP has ABI/unit coverage but awaits a collector-backed
integration test.

### Workflow application logging boundary

Core runtime logs describe transport, polling, state-machine, and other native
SDK behavior. Application logs emitted by Odin Workflow code are a language
runtime concern: Core does not execute that code and the C bridge exposes no
Workflow-application logger. The Odin Worker therefore copies a `Logger` record
into every registered Workflow runtime. `worker.set_workflow_logger` may replace
it only during configuration and updates already registered runtimes; the
callback's `user_data` remains caller-owned until `worker.close`.

Each call builds a stack-local, callback-scoped `Log_Record`. Message, field
keys and string values remain borrowed from Workflow code. Namespace, Workflow
ID, Run ID, Workflow type, and task queue borrow run-arena strings; history
length, timestamp, and replay state come from the current Core activation. The
callback is synchronous and must copy any data retained afterward. The SDK does
not allocate for or perform host I/O on the Workflow fiber, emit a Workflow
command, or feed callback state back into execution. Public logging procedures
return no value, so sink presence and replay suppression cannot become a source
of nondeterministic branching.

The suppression predicate mirrors the pinned Rust runtime's activation
classification: suppress only when `is_replaying` is true and the activation
contains at least one non-Query job. A query-only replay activation is therefore
observable, while history reconstruction does not duplicate ordinary
application logs. `log_replay_history_events` is an explicit diagnostic opt-in
that permits duplicates. This policy is implemented in the Odin layer because
only it knows where user logging calls occur; Core still owns replay detection
and supplies the authoritative activation flag/jobs.

Unit coverage checks every typed field, activation metadata, all five levels,
query-only replay, default suppression, opt-in replay emission, Worker
propagation/replacement, and byte-identical activation completions. A 500-case
property varies replay classification, opt-in policy, field values, and levels.
The CLI-devserver selector path forces Core eviction and fresh-stack replay: it
observes one start record rather than a duplicate and then one terminal record,
while the Workflow result and command history remain unchanged.

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

A second live path sends `{name, salutation}` from the Rust Workflow as
`json/plain`, invokes an Odin
`proc(^activity.Context, Greeting_Input) -> Greeting_Output`, and observes the
structured `{message, name_length}` result after it crosses Odin -> Core ->
Server -> Rust. This proves that typed Activity values are an Odin-side adapter
over Temporal Payloads rather than a change to the C bridge.

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
temporal_odin_fiber_stack_size
temporal_odin_fiber_has_guard_page
temporal_odin_fiber_guard_page_size
temporal_odin_fiber_reserved_size
temporal_odin_fiber_destroy
temporal_odin_fiber_result_description
```

Each cached Workflow instance and asynchronous Signal/Update handler owns a
C-allocated fiber stack and the Workflow instance owns an Odin arena. The
default is 128 KiB per fiber. `workflow.Stack_Options`, accepted by online and
replay Worker creation, independently configures the three stack classes from
32 KiB through 16 MiB and aligns values to 16 bytes before they cross the C
ABI. The project-owned stack-size diagnostic proves that each value reaches the
corresponding native coroutine. On macOS and Linux the bridge allocates each
coroutine from a private, demand-paged mapping and places one inaccessible OS
page between minicoro metadata/storage and the downward-growing stack. The
stack remains fixed-size. Crossing its lower bound faults the process instead
of silently overwriting coroutine metadata; it is detection/containment at the
address-space boundary, not recoverable Workflow failure semantics.
The instance is heap-stable because the C trampoline retains its address. The
trampoline restores a captured Odin `runtime.Context`, with the instance arena
as allocator, before calling user code. Inputs, Activity request payloads,
resolved values, and arbitrary Workflow allocations can therefore survive
suspension without borrowing an activation decode arena. Command storage
itself remains alive for the full activation encode; an early version of the
spike found that pointers to helper-local command structs become invalid before
protobuf encoding, a concrete reason to keep this ownership rule explicit.

The author-facing proof is genuinely sequential:

```odin
hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
    first := workflow.execute_activity(ctx, "hello", name)
    workflow.sleep(ctx, 250 * time.Millisecond)
    return workflow.execute_activity(ctx, "hello", first)
}
```

The same runtime now has a typed path without changing the C or protobuf
boundary:

```odin
typed_workflow :: proc(ctx: ^workflow.Fiber_Context, input: Greeting_Input) -> Greeting_Output {
    return workflow.execute_activity_json(
        ctx,
        "typed_greeting",
        input,
        Greeting_Output,
    )
}
```

`Fiber_Instance` owns every `InitializeWorkflow.arguments` `codec.Payload`, an
ordered borrowed-view slice over those owned values, and the Workflow output.
Each Activity operation likewise owns its arguments and resolved result. The
raw `register_payloads_workflow` boundary receives the complete borrowed slice
and returns an owned `workflow.Payload_Result`; `register_payload_workflow`,
`register_json_workflow`, and `register_workflow` are explicit one-argument
adapters. They fail deterministically with `OdinWorkflowArgumentCountError`
rather than dropping extra history input. Metadata and external-payload details
are copied rather than reconstructed, so custom codecs can use the same
runtime.

The offline typed test forces eviction after scheduling the Activity and
requires the fresh fiber to emit byte-identical command bytes. A live Temporal
CLI case sends `{name, salutation}`, Core evicts and replays the Workflow, Odin
runs a typed Activity, and the Service returns `{message, name_length,
activity_id}`. A
500-case PBT property repeats structured Workflow input/output plus
eviction/replay with generated values. Incompatible typed input deterministically
produces a non-retryable `OdinWorkflowPayloadConversionError`, not a worker
shutdown. A raw Payload test separately carries custom `encoding`,
`messageType`, opaque bytes, and external-payload size details through Workflow
input -> Activity command -> Activity resolution -> Workflow completion. Typed
signals use the same Payload boundary.

The public Client mirrors that boundary with
`encode_start_workflow_request_payloads` and `start_workflow_payloads`.
`Client_Start_Workflow_Input.inputs` lets outbound interceptors inspect or
replace the repeated views before the terminal Core RPC. The original singular
functions construct a one-element slice and remain source-compatible.
Signal-With-Start still exposes one Workflow input and one Signal input in its
current high-level API; widening those independently is a separate API decision
even though its underlying start request uses protobuf `Payloads`.

### Workflow information, memo, and Search Attributes

No C-bridge expansion was necessary for this slice. Core already sends the
language-facing state in `InitializeWorkflow` and accepts mutations as ordinary
completion commands:

```text
InitializeWorkflow
  +-- identity, Workflow/run/type/attempt and run-chain metadata
  +-- timeouts, start/expiration time, parent/root, Retry Policy, priority
  +-- continued Failure, last completion Payloads, cron scheduling interval
  +-- Headers<string, Payload>
  +-- Memo<string, Payload>
  `-- SearchAttributes<string, Payload>

Odin Workflow code
  +-- workflow.info / header_* / memo_* / search_attribute_*
  +-- ModifyWorkflowProperties(upserted_memo)
  `-- UpsertWorkflowSearchAttributes(search_attributes)
```

`workflow.Info` is copied into the per-run arena during initialization. Its
strings and slices are borrowed by user code and remain valid until Core asks
the Worker to evict that run. The root execution protobuf does not carry a
namespace, so `Info.root.namespace` is empty; parent namespace is preserved
from Core's `NamespacedWorkflowExecution`. Invalid protobuf durations or
timestamps fail initialization instead of reaching user code.

The prior-run fields follow the same ownership rule. Odin clones
`continued_failure` into the shared recursive Failure model, clones every
`last_completion_result` Payload, and validates
`cron_schedule_to_schedule_interval` before user code starts. Checked accessors
return borrowed pointers/views into the run arena. Forced eviction and replay
rebuild the same values and retain byte-identical Workflow commands.

Headers, memo, and Search Attribute values remain generic `codec.Payload`
objects. The runtime clones all initial maps into the run arena, so they outlive
the activation decode arena. Raw accessors return borrowed `Payload_View`s and
JSON accessors are adapters above that boundary. Headers are read-only Workflow
inputs. Every successful memo/Search Attribute mutation records an owned
command value and immediately updates the local map, giving deterministic
read-after-write behavior in the same Workflow task. Removal uses the upstream
protobuf contract: the named entry is sent with a default/empty Payload.

Temporal's Search Attribute type is encoded in Payload metadata under `type`.
The Odin enum maps exactly to the upstream strings `Bool`, `Int`, `Double`,
`Keyword`, `Text`, `Datetime`, and `KeywordList`. This is distinct from the
user codec's `encoding` metadata. The current generic API does not statically
prove that an Odin value matches the selected Search Attribute type; the
Service schema and application tests remain the enforcement boundary. Custom
field names must be registered with the Service before use.

Commands retain call order and are emitted before a terminal Workflow command.
Fresh-stack replay reconstructs the initial maps and executes the same calls,
therefore producing byte-identical completion bytes. A 500-case property varies
keys, values, Keyword/Text selection, upsert/removal sequences, local
read-after-write, eviction, and replay. The Odin Client/devserver test observes both
`EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED` and
`EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES` in real Service history.

Activity scheduling is no longer hardcoded. `Activity_Options` maps directly
onto the pinned Core `ScheduleActivity` command: Activity ID, task queue,
Schedule-to-Close, Schedule-to-Start, Start-to-Close, heartbeat timeout, Retry
Policy, cancellation type, eager-execution control, legacy Build-ID versioning
intent, and priority. The enclosing `WorkflowCommand.user_metadata.summary`
carries the UI-facing summary. Options are borrowed at the author-facing call
and copied into the fiber instance arena before the Workflow can yield. This
includes each non-retryable error type, the priority fairness key, and an
encoded owned summary Payload. Empty task queue and Activity ID select the
Workflow queue and the deterministic sequence-derived ID respectively.

`Local_Activity_Options.summary` uses the same command metadata envelope. Core
persists it on the resulting `core_local_activity` marker, including after a
timer-backed local retry. Local Activities have no server task-queue priority or
versioning intent, so those fields remain remote-Activity-only. Versioning
intent is retained for cross-SDK and pinned-Core parity but represents legacy
Build-ID routing; Worker Deployment Versioning is preferred for new systems.

The options API validates that at least one close timeout is positive, rejects
negative durations/attempt counts, invalid retry coefficients, invalid
versioning values, negative/non-finite priority values, and fairness keys over
64 bytes, and rejects Start-to-Close or Schedule-to-Start values larger than an
explicit Schedule-to-Close value. Invalid options become a non-retryable
`OdinActivityOptionsError` Workflow failure rather than a malformed Core
command. The original convenience functions remain compatibility wrappers with
the spike's 20-second Start-to-Close timeout, one attempt, and disabled eager
execution. Options-specific offline and 500-case PBT tests require exact
command fields, priority and metadata envelopes, and byte-identical fresh-stack
replay. The live test additionally
reads Service history and checks the Activity ID, all four timeouts, retry
intervals/coefficient/attempt limit, non-retryable type, priority, and summary.
The Local Activity live path checks the marker summary separately.

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
are polled](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/workflow/src/runtime/instance.rs),
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
It copies the outer message/source/stack, Activity ID/type/identity/retry state,
and the first nested Application Failure's message, type, retryability,
category, Payload details, and next-retry delay into the Workflow instance
arena before the activation decode arena is released. It also re-encodes the
complete Temporal `Failure` protobuf for lossless propagation.
`execute_activity_result` returns an experimental `Activity_Result`, allowing
Workflow code to branch. `activity_failure_detail_payload/json` provides raw or
typed access to borrowed details. The existing
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

A live Core/Server case schedules an Odin Activity with one maximum attempt.
The Activity returns retryable `DependencyRejected` details and a 250 ms
next-retry override. The Workflow observes the server-produced Activity wrapper
with `Maximum_Attempts_Reached`, decodes `{dependency: "billing", code: 409}`
from the nested Application Failure, and completes normally. Core eviction and
fresh-stack replay occur before that resolution.

### Shared recursive Failure boundary

`temporal/failure` is the language-facing, allocator-owned representation of
Temporal's recursive `Failure` protobuf. It is deliberately independent of
Client, Worker, and Workflow packages so every boundary can converge on one
model without introducing dependency cycles. It currently covers the complete
pinned union: Application, Timeout, Canceled, Terminated, Server, Reset
Workflow, Activity, Child Workflow, Nexus Operation, and Nexus Handler.
Common message/source/stack fields, encoded attributes, all variant strings and
Payload lists, enum values, and recursive causes are cloned before the
protobuf decode arena is destroyed.

The ownership path for a terminal Workflow failure is:

```text
Core RPC response bytes (Core-owned until rpc result release)
  -> GetWorkflowExecutionHistory decode arena (borrowed fields)
  -> canonically encoded Failure in Result_Page (Odin-owned bytes)
  -> Error.details (ownership transferred, no copy)
  -> failure.decode (allocator-owned recursive tree)
```

`result_page_free`, `client.error_free`, and `failure.failure_free` each release
their own layer. A decoded tree therefore survives freeing both the raw RPC
response and `Error.details`. Update failures already used `Error.details`; the
same decoder now serves both result paths. No Core C-ABI change or new symbol is
involved—the generic WorkflowService RPC and generated protobuf graph are
sufficient.

Workflow activation failures use the same decoder with a different lifetime:

```text
ResolveActivity / ResolveChildWorkflowExecution Failure (activation arena)
  -> preserved encoded Failure bytes (Workflow instance arena)
  -> recursive failure tree (Workflow instance arena)
  -> borrowed Activity_Failure / Child_Workflow_Failure compatibility views
```

`activity_failure_tree` and `child_workflow_failure_tree` return the tree root.
Workflow code must not free it: eviction destroys the containing instance arena
and replay reconstructs a fresh tree from history. The old direct fields now
borrow strings and Payload views from this tree rather than maintaining a
second ownership graph. This removes duplicate conversion logic while retaining
the existing Activity/Application and Child convenience API.

The decoder limits a cause chain to 128 nodes, validates protobuf durations
before converting to `time.Duration`, and treats malformed present union
pointers as errors. A valid protobuf with no known union member becomes
`.Unknown`, preserving its common fields and cause. Unit coverage constructs a
single chain containing every current variant; a 500-case PBT suite varies
every node kind and depths 1–32. The CLI-devserver test proves an unhandled Odin
Activity failure crosses Odin Worker -> Core -> Service close history -> Core
Client RPC and returns as `ActivityFailure -> ApplicationFailure` with owned
details and next-retry delay.

### Buffered Workflow signal

Core delivers external signals as `SignalWorkflow` activation jobs containing
the signal name and payloads. Buffering and dispatch are language-runtime
responsibilities: the TypeScript SDK has an explicit
[buffered signal queue](https://github.com/temporalio/sdk-typescript/blob/main/packages/workflow/src/internals.ts),
and Temporal's Go documentation confirms that
[signals are retained until matching Workflow code receives them](https://github.com/temporalio/documentation/blob/main/docs/develop/go/workflows/message-passing.mdx#signal-channels).

The Odin runtime accepts Core's complete repeated Payload field per signal. It
copies the name and every Payload—including metadata and external-payload
details—into the Workflow instance arena and appends the invocation to an
arrival-ordered queue. `await_signal_payloads(ctx, name)` consumes the oldest
unconsumed matching entry immediately, or changes the fiber phase to
`Waiting_Signal` and yields. The contextual form also exposes identity, headers,
and originating event ID. `await_signal_payload` and `await_signal_json` are
explicit one-argument adapters; another arity deterministically fails with
`OdinWorkflowSignalArgumentCountError` rather than dropping values.
`await_signal` remains the `json/plain` string adapter. Generic, typed, and
string signal-or-timeout variants share the same buffer and race logic. A signal
only resumes the fiber when that exact phase/name is active; signals received
while waiting on an Activity remain buffered.

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

### Read-only Workflow queries

Queries cross two distinct protobuf boundaries. The public Client sends
[`QueryWorkflowRequest`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/protos/protos/api_upstream/temporal/api/workflowservice/v1/request_response.proto#L1185)
through the C bridge's generic WorkflowService RPC call. Core then delivers a
[`QueryWorkflow`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/protos/protos/local/temporal/sdk/core/workflow_activation/workflow_activation.proto#L282)
activation job with query ID/type, repeated argument Payloads, and headers. The
language SDK must return a
[`QueryResult`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/protos/protos/local/temporal/sdk/core/workflow_commands/workflow_commands.proto#L179)
command with the same ID and either one success Payload or a Temporal Failure.
Core owns query transport and server coordination; handler registration,
dispatch, codec conversion, state access, and failure construction belong to
Odin.

The fiber runtime now registers handlers while Workflow code executes. A typed
handler receives a top-level copy of caller-selected state and cannot receive a
`Fiber_Context`, which removes the normal command-scheduling API from its
surface. A query activation invokes the handler synchronously without resuming
the suspended Workflow fiber, appends `RespondToQuery`, and leaves the Workflow
at the same suspension point. The runtime snapshots phase, sequence counters,
and operation counts around the callback; an observed Temporal-command-state
mutation becomes a failed query with `OdinWorkflowQueryMutationError`. This is
runtime defense, not a general memory-purity proof: a handler can still mutate
state reachable through pointers embedded inside its copied value. The API and
determinism documentation therefore continue to define handlers as read-only.

Query serialization now preserves the protocol's repeated argument field and
single result Payload. The public `query_workflow_payloads` method and
`set_query_handler_payloads` registration are codec-neutral; contextual variants
add headers and query identity. Singular Payload/JSON methods remain explicit
one-argument adapters and fail with `OdinWorkflowQueryArgumentCountError` for
another arity rather than discarding data. Unknown handlers, argument-count
mismatches, and codec errors are returned as failed query results; they do not fail the
Workflow Task or terminate the worker. Query state pointers are borrowed from
the live Workflow stack and remain valid because handlers execute only while
that fiber instance is cached. Eviction destroys both together; replay creates
a fresh stack and deterministically registers a fresh handler.

Client query options now borrow an explicitly owned `Headers` map. The request
encodes it into `WorkflowQuery.header`; Core delivers the corresponding map on
the activation job. `set_query_handler_json_with_context` receives a narrow
`Query_Context` containing query identity and owned-for-the-invocation header
copies, exposed to user code as borrowed Payload views/typed JSON. It still
does not expose `Fiber_Context`, so adding propagation data does not also expose
Temporal command primitives. The map and views die when the synchronous query
callback returns.

The live CLI-devserver run exposed an important detail: Core may mark the
activation containing the externally requested query as `is_replaying=true`
while reconstructing Workflow state for that query. Query accounting therefore
cannot use the ordinary "ignore replay work" stats filter. Query jobs are
ephemeral deliveries and are counted once when Odin answers them, even on such
an activation. The test then observes Core eviction, reconstructs a new fiber,
and continues normally after the subsequent signal. Offline tests require
byte-identical query completions across forced eviction/replay and cover a
query delivered alongside a signal-producing completion. A separate live case
sends three Payloads through the Odin Client, Core, and Service into the
codec-neutral handler; a 500-case property varies 0–12 arguments while checking
order, metadata, external details, headers, and deterministic request bytes.

Core reserves `legacy_query` as `LEGACY_QUERY_ID`: a legacy query completion
may contain no other commands. The current runtime produces the correct
query-only completion when Core sends such an activation, but it does not yet
reject a malformed mixed legacy activation itself. Client-side reject-condition selection is exposed independently of
Workflow delivery: Odin maps all four pinned conditions onto
`QueryWorkflowRequest.query_reject_condition`. A `QueryRejected` response is a
typed Client error that retains the rejecting Workflow execution status.

Static summary/details illustrate a separate Core boundary. They are persisted
by the Service and returned in
`DescribeWorkflowExecutionResponse.execution_config.user_metadata`, so the
Client exposes owned decoded strings on `Workflow_Description`. The pinned
`InitializeWorkflow` activation has no UserMetadata field. Consequently the
Workflow runtime cannot surface start static metadata through `workflow.info`
without a future upstream activation change or an extra Service RPC, which
would be the wrong determinism boundary. Odin therefore exposes these values
only at Client start/Describe today.

### Workflow Update routines

Core delivers durable Updates as
[`DoUpdate`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/protos/protos/local/temporal/sdk/core/workflow_activation/workflow_activation.proto#L343)
jobs. The user-visible Update ID and Core's `protocol_instance_id` are distinct;
every language response must echo the protocol instance ID. The completion
contract is stricter than queries:

- A validator rejection produces one `UpdateResponse.Rejected` in the same
  activation.
- An accepted Update always produces `Accepted` in that activation.
- The handler later produces either `Completed` with a Payload or `Rejected`
  with a Failure. A synchronous handler may place this terminal response
  immediately after `Accepted` in the same completion.
- On replay Core sets `run_validator=false`. Odin skips validation, reruns the
  state mutation, and still emits the same `Accepted` plus terminal sequence.

These rules come directly from
[`UpdateResponse`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/protos/protos/local/temporal/sdk/core/workflow_commands/workflow_commands.proto#L355)
and Core's Update state machine, which explicitly supports immediate
accept-and-complete. Update responses are emitted before ordinary Workflow
commands so an activation that also closes the Workflow cannot place its
terminal command ahead of the Update protocol messages.

The synchronous handler form remains useful for immediate state mutation. Its
validator receives a top-level state copy and no `Fiber_Context`; command
sequence counters and operation-table sizes are guarded around that callback.
This is not deep memory-purity enforcement.

The routine form gives each accepted Update an independent minicoro stack and
execution state keyed by `protocol_instance_id`. It shares the enclosing
Workflow's deterministic Activity/timer sequence counters, operation tables,
signals, state arena, and cancellation request, but owns its phase, wait target,
failure state, and fiber handle. Before a routine is resumed, the small
execution-state record is swapped into the instance; after yield or completion
it is copied back and the main Workflow execution state is restored. This lets
the existing Temporal-aware primitives operate unchanged while preventing an
Update wait from replacing the main Workflow's wait.

This matches the pinned Rust language runtime's separation between
[`RoutineKind::Update` and `UpdateRoutineCompletion`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/workflow/src/runtime/types.rs)
and its creation of a separately polled routine after inline validation in
[`start_update_routine`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/workflow/src/runtime/instance.rs).

Activation jobs are applied first. Ready Update routines are then resumed in
registration order before the main Workflow fiber, matching Core's documented
handler-before-main ordering. `Accepted` is emitted in the `DoUpdate`
activation. A routine that schedules an Activity can yield with only that
acceptance and `ScheduleActivity` in the completion; its later resolution
resumes the same routine and produces the terminal Update response. Completed
native coroutine resources are destroyed immediately, while routing metadata
and arena-backed result bytes remain valid through completion serialization.
If the main Workflow routine returns while an accepted Update is still active,
the terminal Workflow command is emitted and that handler cannot continue.
This matches the established SDK contract: the default
`Handler_Unfinished_Policy.Warn_And_Abandon` records a worker warning, while
`.Abandon` deliberately suppresses it. `await_all_handlers_finished` is the
explicit Workflow primitive for applications whose terminal result depends on
all accepted handlers. It suspends only the main fiber; Update completions are
emitted before the main fiber resumes and closes. Calling it from an Update
handler is rejected because the handler would wait on itself. This follows
Core's Rust
[`WorkflowContext::all_handlers_finished`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/workflow/src/workflow_context.rs#L2302-L2312)
condition and the same opt-in waiting model documented for
[Python message handlers](https://docs.temporal.io/develop/python/message-passing#ensure-your-handlers-finish-before-the-workflow-completes)
and Go's
[`HandlerUnfinishedPolicy`](https://pkg.go.dev/go.temporal.io/sdk/workflow#HandlerUnfinishedPolicy).

Concurrency is now exercised rather than only implied by the routine table.
Two accepted routines can schedule Activity sequences 1 and 2, sequence 2 can
resolve and complete its Update first, and sequence 1 can complete later. Only
the routine whose own wait condition is ready resumes; registration order is
the deterministic tie-breaker when several become ready in one activation.
Forced eviction and replay reproduce every completion byte-for-byte.

The same cooperative scheduler now owns a small synchronization primitive for
shared language state. `workflow.Mutex` is a zero-value FIFO lock bound to one
`Fiber_Instance` on first use. Its owner and waiter queue contain only
Workflow-fiber contexts allocated within that run; it does not cross the Core C
ABI, create a command, block a host thread, or claim durable state outside
history. Unlock marks the oldest non-canceled waiter ready, after which the
ordinary Signal -> Update -> main fixed-point scheduler resumes it. A waiter
captures its current Workflow cancellation-token ID, so cancellation removes
it without violating FIFO order for the remaining waiters. Eviction discards
the queue with the fiber arena, and replay reconstructs the queue from the same
handler ordering and suspension points.

This is explicitly a language-runtime responsibility. Core supplies activation
ordering, replay inputs, and durable operation resolutions, but it neither sees
nor coordinates mutations to Odin values between concurrently live handler
fibers. The Odin runtime therefore rejects reentrant locking, cross-run mutex
reuse, and non-owner unlock as deterministic execution failures. The usual
handler boundary applies: a main/Signal failure fails the Workflow, while an
Update failure rejects that Update. The runtime also tracks locks per fiber,
releases them during routine teardown, and converts a successful return while
owning one into `OdinWorkflowMutexOwnershipError` so a forgotten unlock cannot
strand the scheduler. User code should still pair each successful lock with
`defer mutex_unlock`; ordinary OS locks would block the worker thread and have
no replay semantics.

The related `workflow.await_condition` primitive follows the pinned Rust
runtime's `WorkflowContext::wait_condition` boundary. Core has no condition
command or durable condition object. Odin stores a type-erased predicate plus a
borrowed state pointer in the suspended fiber context. The typed public wrapper
keeps both in a stack-local adapter whose lifetime naturally spans the stackful
call. The predicate receives no `Fiber_Context`, so it cannot intentionally
schedule Temporal operations.

The predicate runs once before suspension and is reevaluated by the stable
Signal -> Update -> main scheduler after activation jobs or another routine may
have mutated shared state. A condition that becomes true resumes in the same
activation fixed point. Its captured cancellation-token ID is checked only
after the predicate, matching upstream's condition-before-cancellation
priority. Returning `false` therefore means cancellation won while the
predicate remained false. No explicit language-side notification object is
needed: Workflow state can only make deterministic progress while activation
work or a ready fiber is being applied, and the scheduler already drains after
each such step.

Predicates must be deterministic, side-effect-free, non-blocking, and must not
retain their borrowed pointer. Eviction destroys the predicate adapter with the
fiber stack; replay reexecutes the Workflow and reconstructs it. A false
condition does not itself arrange a future activation, so application code must
still have an external Signal/Update or a durable Activity/timer/child operation
that can cause relevant state to change. This entire feature remains above the
C ABI and adds no protobuf or Core symbol. The live acceptance case blocks the
main Odin fiber on an approval predicate, delivers a real Temporal Signal, and
requires the same activation to report the Signal handler and terminal Workflow
completion through Core.

The Workflow selector is another language-runtime-only boundary. The pinned
Rust SDK documents its `select!` as deterministic and biased, polling branches
in declaration order through `futures_util::select_biased!`. Odin mirrors that
contract with `Selector_Case` values for Activity, Nexus operation, Timer,
Signal, Child Workflow start, and Child Workflow completion futures. Core still
supplies the durable resolutions; it does not select among language futures and
no C ABI or protobuf addition is involved.

All jobs in one activation are applied before the stable Workflow scheduler is
drained. Consequently, when several cases become ready in the same activation,
the case slice order wins regardless of Core job order. Cancellation is an
implicit fallback after ready cases, matching the existing condition-first
rule. Losing operations remain live and are never canceled implicitly: their
handles continue to belong to Workflow code and must be awaited or explicitly
canceled. Case slices and Signal-name strings are borrowed across the stackful
call only; selected Signal payload and metadata views borrow the run arena.
Eviction drops the suspended selector with the fiber, and replay reconstructs
it from the same command sequence.

The unit suite covers Activity/Activity and Nexus/Nexus ordering plus a
Signal/Timer tie. The Nexus case resolves both operations in one activation,
chooses by declaration order rather than sequence/job order, and reproduces the
two schedule commands byte-for-byte after eviction. A dedicated 180-trace
Nexus property varies forward/reverse declaration order and every valid
interleaving of the two start/result job pairs, exercises `join_all`, and
requires byte-identical initial and terminal replay completions. The existing
180-trace property crosses all six case permutations with all six Core-job
permutations and requires byte-identical fresh-stack replay. The CLI dev-server
case starts a real Activity and Timer, observes `waiting_selector=true`, lets
the Activity win, emits `CancelTimer`, and completes through Core and Temporal
Server after Core has also forced a replay on a fresh Odin stack.

A second 300-trace composition property starts three Child Workflows, selects
between two start futures, joins all three start futures, explicitly cancels one
of the children, selects between two completion futures, and finally joins all
three completions across a partial activation boundary. It varies declaration
and Core-job order plus all cancellation targets, then reproduces all four
activation completions byte-for-byte on fresh-stack replay.

Odin's first `join_all` boundary reuses the same durable case descriptors but
waits for every case instead of choosing one. Upstream Rust has both a
heterogeneous `join!` macro and a homogeneous generic `join_all`; Odin has
neither variadic macros nor one generic Future result type. The current API is
therefore a heterogeneous, allocation-free readiness barrier. After `.Ready`,
the existing typed Activity, Nexus operation, Timer, or Child Workflow
`await_*` call reads each result synchronously in application-selected order.

Exact duplicate cases are rejected because Odin handles are copyable whereas
Rust futures are normally moved. Signal cases are also rejected for now:
consuming several buffered Signals needs an explicit owned result collection,
and silently allocating one would conflict with the SDK's ownership goals. The
case slice is borrowed across suspension. If the selected cancellation token is
requested before every operation is ready, join returns `.Canceled` and leaves
the operation handles under caller ownership. As with selector, this scheduler
state never crosses the C ABI and adds no protobuf type or Core symbol.

A focused unit trace resolves the second Activity first, verifies the join
remains suspended, then resolves the first Activity and Timer in reverse job
order before checking three byte-identical replay completions. A 180-trace PBT
suite varies all six Core job orders and both one/two-result activation splits.
The live dev-server case runs a fast and a two-second Activity concurrently,
observes a partial `waiting_join=true` activation, and completes in declaration
order after Core eviction and replay.

An unhandled Activity or Child Workflow failure is terminal for its Update
routine, not for the enclosing Workflow Execution. The routine records the
failed operation sequence. Its terminal `UpdateResponse.Rejected` restores the
original encoded Temporal Failure supplied by Core, preserving the recursive
wrapper/cause tree, retry state, typed details, and retry delay. The pending
response borrows those bytes from the Workflow instance only until activation
completion encoding; the operation remains instance-owned throughout that
window. Explicit result APIs still let handler code inspect and recover from a
failure without rejecting the Update.

Typed handler input is decoded in the Workflow instance arena—not the
activation decode arena—because a state mutation may retain strings or slices
from that input beyond the current activation. It remains valid until eviction.
Validator input and rejected-result strings need only activation lifetime.
Successful result Payloads are logically released after completion
serialization through their recorded allocator. Because typed handlers allocate
in the instance arena, their backing storage is reclaimed in bulk at eviction.
Replay then constructs a new arena and reapplies accepted Updates to rebuild
state.

The protocol boundary itself is not singular: Service `Input.args` and Core
`DoUpdate.input` are repeated Payload fields. Odin therefore carries 0..N
ordered Payloads through Client interceptors, protobuf encoding, validation,
handler interceptors, synchronous handlers, and stackful routines. The
codec-neutral `set_update_handler_payloads[_with_context]` registrations accept
that full slice; singular Payload/JSON adapters require exactly one and return
`OdinWorkflowUpdateArgumentCountError` otherwise. Routine-owned clones and
views live in the Workflow arena, so suspension does not retain activation
decode memory.

The public Client uses the C bridge's existing generic RPC symbol for
`UpdateWorkflowExecution`. `start_update_payload/json` requests lifecycle stage
`ACCEPTED` and returns an owned `Update_Handle` containing the server's Workflow,
run, and Update IDs. `wait_update_payload/json` uses
`PollWorkflowExecutionUpdate` with lifecycle stage `COMPLETED`.
`execute_update_payloads` and the singular `execute_update_payload/json`
adapters compose those operations for callers that prefer
a blocking one-call API.
`get_update_handle` reconstructs an owned reference from stable IDs for a
caller reconnecting after acceptance. The option-aware waits pass the pinned C
bridge's native `CancellationToken` into `RpcCallOptions` and can also apply a
monotonic total timeout. Timeout disables Core retries for that bounded poll;
an empty long-poll response recalculates the remaining budget. Cancellation
selects against the entire Core RPC future, including retries. Both outcomes
end only the local wait and map to `.Wait_Timed_Out`/`.Wait_Canceled`; the
accepted server-side Update continues.
The pinned transport currently reports its request-deadline expiry as gRPC
status 1 with `Timeout expired` rather than status 4; Odin normalizes both that
observed form and standard `DEADLINE_EXCEEDED` to `.Wait_Timed_Out`.
Successful outcomes are cloned out of the protobuf decode arena. Failure
outcomes become `.Workflow_Update_Failed`, with an owned message and encoded
Temporal Failure bytes in `Error.details`. No custom native ABI extension is
needed: polling uses the generic RPC symbol and cancellation uses the three
existing upstream token lifecycle symbols.
The generated decoder borrows protobuf strings and byte fields directly from
the C-bridge RPC response buffer. That buffer therefore remains alive until the
Update reference and any immediate outcome have been cloned. An outcome already
returned by the start RPC is owned by `Update_Handle` and served locally by
`wait_update_*`; this is required for validator rejections, whose completed
reference is not guaranteed to be pollable as a later accepted Update.
The Odin token owns exactly one Core allocation. `RpcCallOptions` borrows its
pointer only for the blocking call; Core clones the internal Rust token before
spawning the RPC future. The wrapper still requires callers to keep the Odin
handle alive until the call returns, then close it exactly once (close is
nil-safe).

The option-aware start variants project the shared owned `Headers` builder into
`Temporal_Api_Update_V1_Input.header`. Core later exposes that map together with
Update ID/name and `Request.meta.identity` on `DoUpdate`. Odin clones those
borrowed activation fields into the Workflow run arena before invoking either
the validator or handler. This ownership transfer is necessary because a
stackful handler may suspend for arbitrarily many activations; its
`Update_Context` remains stable until eviction, then replay reconstructs it
from history. The client RPC and worker paths both use existing generic C
bridge symbols, so this feature adds no ABI surface.

This shape follows the upstream language boundary rather than inventing an
Odin-only header channel: Python's
[`HandleUpdateInput`](https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_interceptor.py)
carries headers through its Workflow inbound interceptor, while the outbound
interceptor is responsible for commands issued by handler code. .NET's
[OpenTelemetry interceptor](https://github.com/temporalio/sdk-dotnet/blob/main/src/Temporalio.Extensions.OpenTelemetry/TracingInterceptor.cs)
demonstrates the same client/workflow/activity interception split. Odin
uses that split rather than one universal hook. The Activity family is now the
first complete chain: Worker-level interceptors wrap inbound callback execution
and outbound heartbeat calls through explicit `next` continuations. Client
outbound now uses the same nesting contract with separate types for Workflow
start, Signal-With-Start, Signal, Query, Update start/result waiting,
cancellation, and
termination, plus the complete Workflow-result wait. Workflow outbound now
applies that same contract to remote and Local Activity scheduling, Child
Workflow start/Signal,
external Workflow Signal/cancellation, Continue-As-New, durable sleep, and
explicit Child Workflow cancellation. Workflow inbound interception now wraps
the complete Workflow execution, Queries, Signals, Update validation, and
Update handlers, including independently suspended handler fibers.

Activity interceptors live wholly above Core. Core still owns polling,
cancellation delivery, heartbeat transport, and task completion; the chain
wraps the language callback and the existing heartbeat recorder. Registration
order is outermost-first and unwind order is reversed. The Worker copies only
the interceptor records; `user_data` remains application-owned and borrowed
until close. The array becomes immutable when polling begins, so concurrent
Activity executions can read it without chain-level locking. Interceptor code
runs in ordinary Activity threads and may perform side effects. This rule must
not be reused unchanged for Workflow interceptors, which must be deterministic
and replay-aware.

No C ABI or protobuf change is involved. Unit coverage proves nested execution,
short-circuiting, and heartbeat wrapping; a 500-case PBT property varies chain
depth and rejection position; the CLI-devserver scenario sends one heartbeat
through Core and returns the Activity result through the same interceptor set.

Client interception also remains entirely above the C bridge. Each public call
constructs a mutable semantic input and clones the caller's headers before
entering the chain. The terminal continuation performs the existing protobuf
encoding and generic Client RPC. Per-operation continuation types preserve
`Workflow_Handle`, `Update_Handle`, Payload, and Error results without an opaque
result union. Workflow-start adapts the older `Start_Metadata.headers` storage
into the shared working map; memo, Search Attributes, static metadata, policies,
and timeouts remain in the mutable start options. This is an allocation per
intercepted call, chosen to make ownership and caller isolation unambiguous.

Signal-With-Start carries both user Payloads and mutable start options, but only
one isolated Header builder: the pinned Service RPC assigns that map to the
Signal event and cannot represent an independent Workflow-start Header.
Cancellation provides mutable reason/request-ID fields, while termination
provides a mutable reason and optional borrowed Payload view. Visibility
list/count and the complete Schedule surface expose their semantic inputs and
preserve the existing allocator-owned result contract through typed
continuations. Schedule Create returns its owned handle directly; the six
Error-returning mutations share a discriminated semantic input while retaining
operation-specific fields. Dynamic RPC-metadata and API-key replacement share
a connection-mutation input and remain direct Core ABI calls at the terminal;
all buffers are borrowed for that synchronous chain.
Their terminal continuations encode only after the complete chain has run. The
Client owns a copy of interceptor records, but callback `user_data` is borrowed.
A 500-case property checks twenty-two nested operation families and arbitrary
short-circuit depth, including all six standalone Nexus lifecycle/Visibility
interceptor groups. `get_result` surrounds the whole logical
wait, including any Core history long polls and followed Continue-As-New/retry
runs; its Payload result is owned. The Update wait interceptor likewise wraps
the complete result long poll and returns an owned Payload. Live start,
Signal-With-Start, Signal, Query, result, Update, cancellation, termination,
Visibility list/count, and every
Schedule path cross Service/Core. The live Client example also proves metadata
replacement plus API-key set/clear interception before later RPCs. A
pre-canceled Update poll proves interceptor unwind through Core; the concurrent
Update scenario times out a slow poll, reattaches by stable IDs, and receives
the durable result later.

Workflow outbound interception lives inside the deterministic fiber boundary.
The Worker copies configured `workflow.Outbound_Interceptor` records into every
Workflow runtime, whether the interceptor or Workflow was registered first.
Once polling begins the arrays are immutable. Each intercepted command clones
caller headers, when supported by the Core command, into a run-arena working
map, invokes typed nested continuations outermost-first, and makes the terminal
continuation clone the possibly changed semantic input into the operation
table. The existing command encoder then serializes the operation. External
cancellation exposes its reason instead of a header map. Continue-As-New
preserves its non-returning `-> !` contract through every continuation. The
caller-owned builder is never mutated, and call-scoped inputs/continuations
cannot safely be retained.

Nexus operation start is now part of the same ordered outbound chain, but its
headers are Nexus protocol strings rather than Temporal Payload Headers. Its
semantic input exposes endpoint, service, operation, one Payload view, the
three timeouts, cancellation policy, and header slice. The terminal continuation
clones every retained value into the Workflow run arena before returning the
non-suspending `Nexus_Operation_Future`. Thus an interceptor can use a temporary
replacement Payload/header slice during `next`, while neither it nor the
continuation may escape the call. Offline eviction/replay tests require nested
entry/unwind order and byte-identical `ScheduleNexusOperation` command bytes.

Sleep interception deliberately wraps the entire stackful wait: code after
`next` resumes only when the timer fires or cancellation unwinds the wait.
Timer creation has a second, non-suspending `start_timer` chain. Its terminal
continuation creates the durable operation and immediately returns a
`Timer_Future`; `sleep` reaches it only after the outer sleep interceptors have
mutated their input. Thus a sleep call composes both boundaries without making
direct `start_timer` pretend to suspend.

This distinction follows pinned Core revision
`e163abd6dc19040064986a63c8b8cf4756ffd320`: Rust
`WorkflowInterceptor::start_timer` accepts `StartTimerInput`/`TimerOptions` and
returns `CancellableWorkflowOutboundFuture<TimerResult>`, while
`WorkflowContext::timer` calls that chain before preparing the Future. Odin's
source evidence is in
`crates/workflow/src/workflow_interceptors.rs` and
`crates/workflow/src/workflow_context.rs`. Odin now mirrors the stable options
that cross this language boundary: duration, an optional explicit Workflow
cancellation token, and summary. A zero token inherits the token selected by
the current main/Signal/Update fiber. The terminal continuation validates that
an explicit token belongs to the same run, copies summary as `json/plain` into
the run arena, and places it in `WorkflowCommand.user_metadata.summary`. The
whole mutable `Timer_Options` record traverses each interceptor. Core's
experimental Event Group markers remain intentionally out of the public API.
`start_timer_with_options` returns the non-suspending Future;
`sleep_with_options` composes the stackful sleep and timer-start chains around
the same record.

This differs from .NET's stackful outbound `DelayAsync` boundary, which is the
closest analogue for Odin `sleep`. Explicit Child cancellation is a separate
Odin hook because Odin exposes a procedural Future API rather than an
extensible child handle object; its continuation owns the same reason mutation
and command-state transition as `cancel_child_workflow`.

The chain intentionally executes again during replay. Its code must therefore
obey the same determinism rules as Workflow code; logging may duplicate and
ordinary I/O, wall-clock reads, and process randomness are unsafe. Header and
option mutations become part of the command bytes and must be reproduced on
fresh-stack replay. Offline tests require byte-identical command completions
after eviction. CLI-devserver scenarios prove interceptor headers cross Core
into Activity, Child Workflow, Signal, and continued-run contexts. Live sleep
and Child-cancellation callbacks also execute across Core timer/cancellation
delivery. This feature adds no C symbol or protobuf type: it is a language-layer semantic hook
immediately before the already implemented command construction. Its shape
follows the corresponding outbound operations in the current
[Python](https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_interceptor.py)
and
[.NET](https://github.com/temporalio/sdk-dotnet/blob/main/src/Temporalio/Worker/Interceptors/WorkflowOutboundInterceptor.cs)
SDK interceptor APIs.

Workflow inbound interception also remains entirely above Core and adds no ABI
or protobuf surface. `execute_workflow` is entered from the main stackful fiber,
before the registered Workflow invoker. Its nested continuation therefore stays
on that same fiber stack through every activation suspension and unwinds only
when user Workflow code reaches a terminal return. This is the semantic unit
exposed by current Python
[`WorkflowInboundInterceptor.execute_workflow`](https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_interceptor.py)
and .NET
[`WorkflowInboundInterceptor.ExecuteWorkflowAsync`](https://github.com/temporalio/sdk-dotnet/blob/main/src/Temporalio/Worker/Interceptors/WorkflowInboundInterceptor.cs),
not one callback per Core activation.

`handle_query` surrounds the synchronous Query handler after Core has delivered
`QueryWorkflow` and Odin has cloned the borrowed argument and headers. The
runtime's command-state snapshot surrounds both interceptor and terminal
handler, preserving Query read-only semantics. Query input/context remain valid
only for the synchronous chain; returned Payload ownership follows the existing
handler contract.

`handle_signal` selects the synchronous terminal for immediate handlers and the
routine terminal when the handler has its own fiber. The routine creates its
interceptor chain inside `signal_routine_entry`; consequently `next` remains on
that fiber stack across durable suspension. Odin now clones Signal identity,
originating event ID, headers, and Payload into the run arena before starting
the fiber. This is both the ownership boundary required by the interceptor and
an improvement over the earlier routine path, which did not retain contextual
Signal metadata.
The typed `set_signal_routine_json_with_context` API exposes this run-owned
context directly to user code and applies the same non-escaping lifetime rule.

Update validation and handling use distinct continuation/result types.
`handle_update_validator` is invoked only when Core sets `run_validator` and is
enclosed by the existing command-state mutation guard. Replay activations that
skip validation also skip this interceptor. `handle_update_handler` chooses the
synchronous callback terminal or is created inside `update_routine_entry` on
the Update fiber. In the latter case its stack and the run-arena-owned
`Update_Context` survive until the handler returns a result.

The runtime and Worker copy interceptor records, but borrow `user_data` until
close. Inputs and continuations are stack-scoped. Registration becomes
immutable before polling. All inbound callbacks execute during replay and must
be deterministic. Live CLI-devserver coverage crosses Core for Workflow
execution, Query, Signal routines, Update validation, and Update routines.
Offline tests prove nested order, delayed fiber-specific unwind, context
ownership after suspension, command-state guards, and fresh-stack replay
command equality. This feature remains entirely language-side and adds no C
symbol or protobuf type.

The live CLI-devserver test first rejects an invalid name through the validator,
then holds two typed rename Updates concurrently. Their independent routines
schedule Activity sequences 1 and 2; sequence 2 resolves first, then sequence 1.
Both Client handles receive the correct typed result, and a query observes the
last-completed mutation. Core repeatedly evicts the instance and replay
reconstructs the main fiber, both Update routines, both scheduled Activities,
both resolutions, and both terminal Update responses with validation disabled.
Offline tests require byte-identical completions for the same out-of-order
sequence. Property tests generate both completion orderings in addition to
validator rejection, immediate completion, and accepted-then-handler-rejection.

The deterministic mutex provides an explicit answer for state that must span a
suspension, but the runtime cannot infer which user fields require it or detect
every logical race/deadlock. Stack/resource limits and panic containment also
remain before the routine API can be considered stable.

### Workflow Signal handlers

The pinned Rust runtime represents every asynchronous Signal invocation as a
[`RoutineKind::Signal`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/workflow/src/runtime/types.rs#L70-L82).
`start_signal_routine` decodes the input, starts a separately polled handler,
and tracks it in the same active-handler count used by
`all_handlers_finished`. Unlike an Update, a Signal has no accepted/completed
protocol response; successful completion is language-runtime state only, while
a handler failure fails the Workflow Task.

Odin now follows that boundary:

- `set_signal_handler_json` runs an immediate handler without exposing a
  `Fiber_Context`, so it can mutate Workflow-owned state but cannot suspend or
  intentionally schedule commands.
- `set_signal_routine_json` creates one minicoro stack per invocation. Its
  execution state is swapped through the same Temporal-aware primitives used by
  main and Update routines, while Activity/timer sequence tables remain shared.
- Registered names dispatch to handlers. Unregistered names continue into the
  existing codec-neutral buffer for `await_signal_payload/json`.
- Registration synchronously drains every older buffered invocation with that
  name in arrival order. Older signals for names that still lack handlers do
  not block matching entries. Dispatch happens before the registering Workflow
  fiber continues, so handler state is visible to the very next statement.
- Ready Signal routines run before Update routines and main. This preserves the
  upstream handler-before-main rule and gives deterministic registration order
  as the tie-breaker among ready Signal routines.
- Signal and Update routines both participate in `all_handlers_finished` and
  `await_all_handlers_finished`. Closing main abandons either kind; the default
  emits a worker warning and `.Abandon` suppresses it.
- Inputs are cloned into the Workflow arena before the handler starts. Routine
  stacks and routing records live until completion or instance eviction.
- Codec-neutral `set_signal_handler_payloads` and contextual/stackful variants
  receive the full ordered borrowed slice. The singular handler registrations
  enforce exactly one argument.

The offline test starts an Activity-awaiting Signal handler, delivers main's
finish signal while that handler is suspended, and proves main remains in
`Waiting_Handlers`. Activity resolution completes the handler and then main in
the same activation. All four completion byte sequences match after eviction
and fresh-stack replay. A second test invokes an immediate handler and main's
buffered signal in one activation, proving handler state mutation is visible
before main resumes. A late-registration regression sends an unknown Signal,
then a matching Signal, then the Signal that lets main register the handler;
the matching invocation runs synchronously before main returns, while the
unknown invocation stays buffered. Eviction and fresh-stack replay produce
byte-identical completions. A 500-case property varies 1–12 matching buffered
Signals and 1–4 earlier unknown Signals and checks dispatch count, arrival
order, and replay bytes. The live CLI-devserver test sends the asynchronous
Signal before its handler exists, then lets the Workflow register it, await an
Activity, query the mutated state, and replay after Core eviction.

Handler failure now has an explicit Core/language lifecycle test. A handler
failure produces a failed `WorkflowActivationCompletion` with
`WorkflowWorkerUnhandledFailure`; it does not directly fail the Workflow
Execution. Pinned Core
[`failed_completion`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/sdk-core/src/worker/workflow/managed_run.rs#L598-L680)
requests language cache eviction and reports the Workflow Task failure only for
the first task attempt. Odin keeps the failed fiber routable only until the
corresponding `RemoveFromCache(LANG_FAIL)`, then destroys it. The retry rebuilds
the run from history on a fresh stack and reproduces the same failed completion
bytes. The CLI-devserver acceptance case observes the Workflow Task failure in
server history, observes eviction plus replay in Odin, terminates the
intentionally retrying execution, and proves that the same Worker can complete
a different Workflow afterward.

Remaining Signal risks are concurrent shared-state policy and timer/cancellation
coverage for Signal routines.

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

The pinned Rust Workflow crate confirms the intended split. It owns a root
[`WorkflowCancellationToken`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/workflow/src/cancellation.rs),
cancels it from `notify_cancel`, and lets operation options override the root
with another token. A newly created token is detached; a child token inherits
its parent. This policy lives above the Core C bridge: Core delivers
`CancelWorkflow` and consumes cancellation commands, while the language runtime
must associate user operations and handler routines with cancellation scopes.

The Odin runtime now models the Workflow's root cancellation state explicitly:

1. Preserve the cancellation reason in the Workflow-owned arena and mark the
   root scope canceled.
2. Mark pending linked Activities/timers for cancellation and resume the
   suspended user fiber.
3. Result-returning Activity and signal/timer waits expose `.Canceled`.
   Convenience waits return their zero value; returning from the Workflow while
   root cancellation remains pending acknowledges cancellation rather than
   completing successfully.
4. New operations in the canceled root scope resolve locally as canceled.
5. `detached_cancellation_scope` temporarily unlinks newly created operations
   from root cancellation. It may be opened proactively or after cancellation
   delivery and must be paired with `cancellation_scope_end` (normally through
   `defer`). Every Activity/timer captures the relationship when scheduled, so
   later root cancellation skips detached operations even after the lexical
   scope has closed.
6. The runtime emits `CancelWorkflowExecution` only after user code returns,
   so detached Activities/timers may finish first.
7. Scope depth is part of the currently selected execution state. Swapping
   between main and Update fibers keeps their scopes independent even though
   their deterministic operation tables are shared.

The runtime now also exposes borrowed run-local `Cancellation_Token` values.
The root token is linked to `CancelWorkflow`; `cancellation_child` creates an
arbitrary downward-only relationship; and `cancellation_detached` creates an
unrelated root. Token IDs are assigned deterministically by Workflow execution.
The first cancellation reason wins, descendants inherit it unless already
canceled independently, and parents/siblings are untouched. A lexical
`cancellation_scope` selects the token captured by newly created Activities,
Local Activities, timers, Child Workflows, external Signals, and Signal waits.
The older detached-scope API is implemented as a compatibility adapter over a
new detached token. Tokens, nodes, and cancellation reasons are owned by the
Workflow run arena; public token values are borrowed IDs and become invalid at
cache eviction. Scope state belongs to the currently running fiber and nested
scopes must close in LIFO order.

Every operation options record that corresponds to an upstream Rust
`WorkflowCancellationToken` override now accepts an explicit token:
`Activity_Options`, `Local_Activity_Options`, `Child_Workflow_Options`,
`Nexus_Operation_Options`, `Signal_Options`, and `Timer_Options`. Zero inherits
the current fiber token. A nonzero handle must reference a live node in the
same Workflow instance; foreign and stale handles fail before a Core command is
created. The selected numeric ID is stored beside the language-runtime
operation, not inside the owned wire options: cancellation-token graphs are
deterministic Odin scheduler state and never cross the C ABI or protobuf
boundary. Operations continue to emit their existing Core cancellation
commands.

`cancellation_request` changes deterministic language-runtime state. It emits
only the appropriate operation cancellation commands and never synthesizes
`CancelWorkflowExecution`. Core's `CancelWorkflow` activation remains the sole
source of server-requested root cancellation, which the Odin runtime
acknowledges after Workflow code and any explicitly drained handlers return.

The offline acceptance test builds root/child/grandchild/sibling/detached
branches and schedules five Activities. Canceling the child emits exactly
`RequestCancelActivity(seq=2)` and `RequestCancelActivity(seq=3)`; seq 1, 4,
and 5 remain resolvable. An Activity, Child Workflow, external Signal, and
timer created later under a new descendant of that canceled child all resolve
locally without adding commands. After eviction, fresh-stack replay reproduces
the initial and cancellation completion protobufs byte-for-byte. The live
dev-server scenario schedules child/sibling/detached Activities, receives a
public client Signal, observes one cooperative Activity cancellation, lets both
unrelated branches finish, and completes the Workflow successfully.

A second focused options trace binds one detached token directly to a remote
Activity, Local Activity, Child Workflow, and Nexus operation. It asserts that
all four commanded operations retain the same run-local ID, emit their normal
Core cancellation requests, and complete on the following activation.
Generated properties select explicit tokens for all of those operation
families plus child and external Workflow Signals and require byte-identical
fresh-stack replay. An already-canceled Signal token resolves locally without
sending `SignalExternalWorkflowExecution`.

Pinned Rust exposes Child signaling only from `StartedChildWorkflow`; its
`SignalWorkflowOptions` token therefore wraps a Signal command future that
already has a Child handle. Odin also exposes a Future-based convenience that
can be called before `ResolveChildWorkflowExecutionStart`. The runtime models
that extra composition with a private waiter token ID. Token cancellation may
wake this internal prerequisite wait without making the public
`await_child_workflow_start` cancellation-sensitive. If cancellation and Child
start resolution share an activation, cancellation wins and no Signal command
is created. Once a Signal command exists, cancellation follows Rust/Core and
emits `CancelSignalWorkflow(seq)`. The waiter ID is part of the per-fiber
execution snapshot so main, Signal, and Update fibers remain isolated; it is
never serialized.

The CLI-devserver acceptance path now proves the same pre-start transition
across the public Odin Client, Core bridge, Server, and Workflow worker. Client
Signal-With-Start places the cancellation Signal in the initial activation.
The parent starts a Child and enters the internal Signal prerequisite wait;
the Signal routine cancels the explicit token before Core resolves Child
start. Parent history contains the Child start/completion but no
`SignalExternalWorkflowExecutionInitiated` event. This is deliberately based
on activation ordering rather than host timing.

A separate acceptance trace starts a Signal routine waiting on an unhandled
Signal and an Update routine waiting on a durable timer under the same child
token. Main cancels that token only after both routines have already had their
scheduler turn. One activation then cancels the timer, completes both handlers,
emits the Update response, releases main, and completes the Workflow. All three
activation completions reproduce byte-for-byte after eviction. A separate
Activity trace proves that `Wait_Cancellation_Completed` emits the cancellation
request but does not resume user code until `ResolveActivity(Cancelled)` arrives.

Cancellation tokens now support absolute deadlines and relative timeouts. The
runtime derives relative deadlines from Core's deterministic activation
timestamp, represents the winning deadline as a durable Temporal timer, and
propagates the earlier effective deadline to descendants. A later child
deadline emits no redundant timer; an earlier child deadline owns a separate
timer and cancels only its branch when fired. Detached deadline roots remain
independent of root Workflow cancellation. Expired deadlines cancel
immediately without emitting a command. Unit and generated-value tests evict
the run and require byte-identical deadline timer/completion commands on a
fresh-stack replay.

There is no separate protocol for canceling an already accepted Update. Root
Workflow cancellation and the handler's operation scope determine whether that
routine can finish before close.

The post-job scheduler drains Signal routines, Update routines, and the main
Workflow fiber in that stable order until no fiber can advance. A later routine
can therefore cancel a token owned by a waiter that already had its turn: the
next deterministic pass resumes that waiter within the same activation.
Handler completion can then release a main fiber suspended in
`await_all_handlers_finished`, also before completion encoding. A
`Wait_Cancellation_Completed` Activity is deliberately excluded from readiness
for language-side cancellation until Core supplies its terminal resolution;
server root cancellation may still preempt a linked waiter so the Workflow can
acknowledge shutdown.
Activity cancellation remains cooperative at the Activity implementation
boundary; the Workflow request does not forcibly stop arbitrary Odin code.

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

The live detached-cleanup path adds a non-terminal cancellation activation:

| History | Odin command/job |
| ---: | --- |
| 3 | initialize -> `ScheduleActivity(seq=1)` |
| 8 | `CancelWorkflow` -> `RequestCancelActivity(seq=1)` + `ScheduleActivity(seq=2)` |
| 8 | terminal resolution of seq 1 is accepted while cleanup remains pending |
| 3/8 | eviction -> fresh-stack replay reproduces both command batches |
| 15 | `ResolveActivity(seq=2)` -> `CancelWorkflowExecution` |
| 15 | final cache eviction |

The live detached-Update path proves handler draining across cancellation:

| History phase | Odin command/job |
| ---: | --- |
| initialize | main starts a timer/signal race |
| Update | handler accepts and schedules detached `Activity(seq=1)` |
| cancel | main timer is canceled; detached Activity is not; no terminal command |
| Activity resolution | `UpdateResponse.Completed`, then `CancelWorkflowExecution` |
| close | Update caller has its result; Workflow status becomes `CANCELED` |

The late resolution is a Core-language-runtime handshake, not a second user
event. The Workflow instance must remain alive until eviction, accept the
terminal resolution idempotently, and never repeat `CancelWorkflowExecution`.
Offline tests cover both pending-operation kinds, cancellation before an
initial command is emitted, same-activation races, the late Activity
resolution, and byte-identical replay of both timer and Activity cancellation
sequences. A separate test proves cancellation delivery into user code,
detached Activity cleanup, delayed acknowledgement, and byte-identical
fresh-stack replay. Additional tests open the detached scope before cancellation
and prove both a main-fiber Activity and an Update-handler Activity survive root
cancellation. The Update case drains the handler, emits its protocol response
before the terminal command, and reproduces four activation completions
byte-for-byte after eviction. The live CLI-devserver case proves the same command ordering
through Core and the Service and finishes with Temporal status `CANCELED`.

### Generated lifecycle testing

`tests_pbt` uses the pinned, test-only
[flakstad/pbt](https://github.com/flakstad/pbt) library to generate 500 batches
from nine signal/timer/cancellation presence and ordering classes. Each case is
checked against a small event-outcome model, then evicted and replayed from a
fresh fiber; the decisive completion must be byte-identical. Coverage
constraints ensure every class appears in a successful run.

The cancellation scheduler property additionally generates all six
permutations of Signal-handler start, Update-handler start, and cancellation
Signal jobs inside one activation. Every ordering must drain both handler
fibers and main to completion without emitting the timer that was created and
canceled before command encoding. Eviction and fresh-stack replay must produce
the same initialization and terminal completion bytes.

The deadline property generates 300 parent/child timeout pairs. It covers the
parent, child, and equal-deadline cases; requires only the minimal one or two
durable timer commands; fires the winning timer; and checks branch-local
cancellation plus exact completion bytes after eviction and replay.

The pinned, test-only
[flakstad/statecharts](https://github.com/flakstad/statecharts) library now
provides an independent Update lifecycle oracle. PBT generates 1,000 traces
across handler start/finish, main return, explicit handler await, immediate
cancellation, cancellation with a detached handler drain, and late resolution.
The model asserts one-shot terminal emission, no changes after close,
await/drain blocking, a combined response-and-close transition, and
deterministic re-execution of the same trace. `PBT_ROOT=~/Projects/pbt` and
`STATECHARTS_ROOT=~/Projects/statecharts` allow deeper testing against the
sibling checkouts without weakening the reproducible submodule pins.

### Reusable worker registry and poll boundary

Core's architecture assigns task dispatch and concurrency to the language SDK:
the language layer continuously polls each enabled task kind, invokes user
code, and returns completions, using language-appropriate threads or
coroutines. The pinned
[Core architecture document](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/ARCHITECTURE.md)
is explicit that this concurrency is not supplied by Core.

The first reusable Odin worker implements this boundary:

```text
worker.create -> language configuration + registries only

worker.run -> derive Core task types -> worker_new + worker_validate
    |-- Workflow poll thread
    |      decode route
    |      Workflow type --initialization--> registered Fiber_Runtime
    |      run ID --------subsequent-------> same Fiber_Runtime
    |      process activation -> complete activation
    |      RemoveFromCache -> destroy fiber and route
    |
    `-- Activity poll thread
           decode task -> task-token route -> bounded callback thread
                                      |-> heartbeat Core synchronously
                                      `-> complete success/failure/cancel
```

The blocking C calls can run concurrently against the same Core worker. The
Workflow registry itself is frozen while `run` is active, and only the Workflow
poll thread accesses its fiber runtimes and run-ID table. This preserves
single-threaded fiber affinity while allowing a Workflow-produced Activity to
execute without stopping Workflow polling. Activity start tasks own
heap-stable execution/context state until their callback thread joins. Core's
fixed or resource-based Activity slot supplier bounds concurrency; the single
poll thread continues to
receive prioritized cancel tasks and atomically marks the matching context.
The live backpressure case submits 24 Activities to a three-slot Worker. The
first three callbacks form a barrier, proving that all slots are used; the
callback high-water mark must remain exactly three while every task completes.
The language Worker also reports its execution-ownership high-water mark in
`Run_Stats.activity_executions_peak_owned`. That route count may transiently be
four: Core can accept one completion and dispatch replacement work before the
Odin poll loop reaps the completed pool task. This is a completion-handoff
window, not a fourth executing callback, and remains bounded under the full
fan-out.
The poll thread indexes each execution by the exact opaque binary task token;
the map key borrows the execution-owned token and is removed before execution
destruction. This makes cancellation routing average constant-time and rejects
duplicate start tokens instead of overwriting a live route. Core explicitly
permits Activity completions to be called concurrently.

Cancellation cannot forcibly unwind arbitrary Odin code. User code must return
after observing `activity.is_cancellation_requested`; the worker then emits an
`ActivityExecutionResult.cancelled` carrying `CanceledFailureInfo`. Heartbeat
calls are synchronous only at the C decode/queue boundary. Core throttles the
server heartbeat RPC, and a server cancellation response later interrupts the
Activity poll as a separate cancel task. The live suite verifies this exact
sequence and preserves Core's reason, including the valid `NotFound` race after
the Workflow has already closed.

The cancellation boundary is now lossless for the pinned Core schema in both
directions. A Core `ActivityTask.Cancel` is routed by task token and atomically
publishes its primary `ActivityCancelReason` plus all six independent
`ActivityCancellationDetails` flags to the callback context. A typed callback
can return `.Canceled` with a message and one codec-isolated Payload; the worker
copies it into `CanceledFailureInfo.details` before freeing the callback-owned
result. Core subsequently resolves a Workflow Activity with a recursive
Failure tree. The Workflow runtime owns that decoded tree in its run arena and
exposes cancellation-detail Payloads as borrowed views for the lifetime of the
result/runtime.

`workflow.cancel_activity` records the intent in deterministic Workflow state
and emits at most one `RequestCancelActivity` command. `TryCancel` changes the
language Future to canceled immediately, while
`WaitCancellationCompleted` remains in `CancelRequested` until Core supplies
`ResolveActivity`; this distinction is required for callback-authored
cancellation details to be observable. `Abandon` never emits the command.
Root-Workflow cancellation still follows the existing scope policy and may
resume the root wait without waiting for a remote callback, so callers needing
the acknowledgement/details contract must use explicit cancellation with
`Wait_Cancellation_Completed`.

Expected Activity failures are values, not panics. Temporal's
[Application Failure contract](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/application-failures.mdx)
defines these wire fields, and its
[Retry Policy contract](https://github.com/temporalio/documentation/blob/main/docs/encyclopedia/retry-policies.mdx)
makes Activities retryable by default. `activity.Result(T)` carries
either a typed success value or an `Application_Failure` with message, type,
retryability, one optional codec Payload of details, and an optional next retry
delay. The worker maps that directly to the generated
`ApplicationFailureInfo` protobuf inside `ActivityExecutionResult.failed`.
Core and the Service—not the callback thread—apply the Activity Retry Policy.
The live test returns retryable `TransientGreetingError` plus typed details and
a 10 ms delay on attempt 1; the next task reaches Odin with attempt 2 and
completes successfully. A 500-case property test varies type, retryability,
details, and delay and requires exact protobuf preservation.

This is deliberately separate from panic containment. The tested Odin runtime
defines both [`panic`](https://github.com/odin-lang/Odin/blob/ea5175d86/base/runtime/core_builtin.odin#L1573)
and [`Assertion_Failure_Proc`](https://github.com/odin-lang/Odin/blob/ea5175d86/base/runtime/core.odin#L299)
as non-returning (`-> !`) and offers no supported unwind/recover mechanism.
Jumping out through C would skip
Odin `defer`s and allocator cleanup. Consequently an unexpected callback panic
still terminates the worker process; honest containment requires a process
boundary or future Odin runtime support, not a `longjmp` shim.

That process boundary is now live-proven rather than merely recommended. The
panic integration test runs an Activity-only Odin worker separately from the
Workflow worker, deliberately traps attempt 1, verifies the process exits
non-zero without an Activity completion, and starts a replacement Odin worker
on the same task queue. Temporal's Start-To-Close timeout and Retry Policy
redeliver the durable Activity as attempt 2; the replacement returns its
observed attempt number and the unchanged Workflow completes. This is the
minimum safe production policy under a process supervisor. It does not make a
mixed Workflow/Activity process crash-safe and is not an SDK-managed
per-Activity subprocess executor.

`register_workflow(name, proc)` owns one `Fiber_Runtime` per Workflow type. A
unit test initializes two types under different run IDs, verifies dispatch to
the correct procedure, evicts one, and proves that the other route remains.
The live signal/timer test now uses only this worker boundary: Workflow and
Activity streams run together, Core eviction causes fresh-stack replay, and
the Activity result resumes the correct Workflow.

Normal `worker.run` is unbounded. `request_shutdown` forwards Core's shutdown
request so outstanding poll calls can return; `close` is rejected while `run`
is active and finalizes only afterward. Bounded task/execution counts exist for
examples and tests. Core worker construction is delayed until `run`, so
registered task types select `WorkerTaskTypes.enable_workflows` and
`enable_remote_activities` exactly. The public API no longer exposes either
Core capability flag.

The Worker-owned `graceful_shutdown_period` maps directly to the C bridge's
`graceful_shutdown_period_millis`. Odin defaults it to five seconds, rejects
negative durations, preserves explicit zero, and rounds positive fractional
milliseconds upward. Upstream Core defines this as a delay before issuing
`WorkerShutdown` cancellations to every outstanding Activity and Nexus task;
it is not a deadline that forcibly kills language callbacks. The live test
keeps an Activity heartbeating, initiates shutdown from a coordinating Odin
thread, receives the exact `Worker_Shutdown` primary reason and sole detail
flag after 200 ms, returns cooperatively, and verifies Core flushes its terminal
completion before finalization. An uncooperative callback can therefore still
hold shutdown open. `Run_Options.uncooperative_callbacks` makes that boundary
observable without claiming unsafe recovery: `request_shutdown` starts a
watchdog immediately, active Activity/Nexus callbacks are counted atomically,
and one observer call per still-active kind fires after the configured duration.
Bounded/error drains use the same observer around thread-pool finish. The zero
value waits silently. No executor thread is killed, and observer code must not
free the Worker; a deployment may use the notification to terminate the whole
process under a supervisor.

### Worker configuration boundary

`worker.create_with_options` now exposes the subset of the pinned Core worker
builder that the Odin executor can honor without introducing another runtime:
worker identity override, fixed or resource-based Activity capacity, Workflow
cache size and sticky timeout, heartbeat throttling, worker-local and task-queue
Activity rate limits, graceful shutdown, Workflow/Activity poller behavior,
sticky/nonsticky poll ratio, and eager Activity reservation count. The original
`worker.create` is a compatibility convenience that fills the same
`worker.Options` defaults.
Resource-based Workflow/Nexus suppliers, custom slot suppliers, async Nexus
operations, plugins, storage drivers, and the experimental payload-error limit
remain deliberately unexposed. Fixed Nexus slots and poller behavior configure
the public synchronous executor and its raw poll/completion lifecycle.

The public `Poller_Behavior` is an owned tagged Odin value. Immediately before
`temporal_core_worker_new`, `Worker_Option_Bindings` translates it to the C
bridge's two-pointer representation and owns the pointed-to simple/autoscaling
records on the calling stack. Both null means `None`, allowing Core's default
`SimpleMaximum(5)` behavior and server-directed autoscaling enrollment; exactly
one non-null pointer selects the corresponding policy. The bridge calls
`to_core()` and copies all values synchronously, so neither pointer escapes
`worker_new`. Odin validates minimum/maximum/initial ordering before the FFI.

`identity_override` differs from those temporary numeric records because Worker
construction is intentionally delayed until registrations determine the task
types. The language Worker therefore clones the identity at
`create_with_options`, retains it alongside namespace, task queue, versioning
identity, and nondeterminism type names, and deletes it in `worker.close`. The C
bridge converts its borrowed `ByteArrayRef` to an owned Rust `String` during
`worker_new`.

The C bridge accepts four independent supplier union values but constructs one
shared resource controller; consequently every resource-based arm must repeat
the same CPU and memory targets. Odin exposes this invariant as one
`Resource_Based_Activity_Tuner_Options` containing independent remote and Local
Activity slot ranges. Targets must be finite in `(0, 1]`, slot minima must be
positive, maxima cannot be below minima, and ramp durations are non-negative.
The convenience constructor deliberately defaults both maxima to ten because
the current Odin executor uses one OS thread per possible callback. When the
tuner is enabled it supersedes `max_concurrent_activities`; `worker.run` sums
the maximums of only the enabled remote/Local suppliers to size the shared
callback pool. Workflow activation execution remains serial. The Nexus executor
is concurrent but currently honors only the fixed supplier; resource-based
Nexus tuning stays unexposed until the callback pool can follow dynamic slot
capacity safely.

The two rate-limit fields use zero as the C bridge's `None` sentinel. Odin
rejects negative, NaN, and infinite values; positive values cross unchanged.
Durations use non-negative Odin `time.Duration` values and round positive
sub-millisecond fractions upward because the ABI stores `u64` milliseconds.
The sticky poll ratio is restricted to `[0, 1]`. A cached Worker receives two
fixed Workflow slots instead of the uncached default of one, and an explicit
simple Workflow poller below two is rejected, matching Core's cache/poller
constraint. Eager Activity reservations now match Core's language-SDK default
of three per Workflow Task. The ordinary `workflow.execute_activity` and
`workflow.start_activity` helpers allow eager execution; explicit options can
still set `disable_eager_execution=true` per command.

The cache path is now verified against the CLI dev server rather than only the
C layout. A condition Workflow with `max_cached_workflows=10`, a ten-second
sticky timeout, and `SimpleMaximum(2)` suspends after its initial activation.
The later Signal activation resumes the same Odin fiber with `is_replaying`
false, completes the Workflow, and then observes Core's terminal cache eviction
during bounded Worker shutdown. The paired cache-disabled scenario still
requires fresh-stack replay, so the suite proves both execution modes rather
than merely accepting a sticky configuration.

The per-Worker Activity rate limit is also behavior-tested. Twenty-four remote
Activities are offered to a Worker with three execution slots and
`max_worker_activities_per_second=4`. Core spaces the polls so the run takes at
least five seconds (7.57 seconds in the first local run), while Odin completes
all tasks and drains its callback routes. This demonstrates that throttling is
performed before Activity dispatch by Core; the Odin thread pool remains a
capacity bound, not a second rate limiter. The server-side task-queue rate still
has exact ABI coverage but needs dedicated behavioral acceptance.

Eager Activity dispatch has a dedicated same-process acceptance path. The test
uses one Core Client/Worker, ten fixed Activity slots, one Activity poller, and
one permitted eager reservation. Core's debug event confirms that the Service
returned an Activity task directly in `RespondWorkflowTaskCompletedResponse`
and that Core inserted it into the Activity task stream; successful Activity
completion alone is not accepted as proof because an ordinary poll produces the
same language-level task. The CLI 1.5.1 dev server bundled with Server 1.29.1
declined eager Activity execution in the first live run unless
`system.enableActivityEagerExecution=true` was supplied. Current Temporal Server
`main` defines that setting with a `true` default, so this is a version/dev-server
difference rather than a general self-hosted default. The integration harness
sets it explicitly to remain reproducible. With the flag absent in the tested
version, Core correctly reserved a slot and requested eager execution, then
logged that the Service returned one fewer task and released the reservation
before normal polling handled it. This fallback is transparent to Odin code.

Upstream references: [Worker performance and eager task
behavior](https://docs.temporal.io/develop/worker-performance), [runtime and
resource tuning](https://docs.temporal.io/develop/worker-performance/runtime-tuning),
and the current
[`EnableActivityEagerExecution` server
default](https://github.com/temporalio/temporal/blob/main/common/dynamicconfig/constants.go).

This adds no protobuf messages and no C symbols. It populates the existing
`TemporalCoreWorkerOptions` passed to `temporal_core_worker_new`; all polling,
rate limiting, sticky routing, heartbeat coalescing, cache management, and
server negotiation remain Core responsibilities. Odin still owns callback
capacity, registration, dispatch, Workflow fibers, and completion production.
The ABI mapping has exact unit coverage plus a 500-case generated property over
cache, fixed/resource slots, shared resource targets, durations, rates, and both
poller union arms. A live CLI-dev-server case also creates the resource
controller, polls one Activity through it, and returns the completion.

This creates an explicit ownership boundary. The Odin worker owns cloned
namespace/task-queue strings, registrations, Workflow runtimes, and eventually
the Core worker handle. It only borrows the client pointer; the client and Core
runtime must remain open until `worker.close` has finalized and freed the Core
worker. The C bridge converts all string refs into owned Rust configuration
during synchronous `temporal_core_worker_new`, so no Odin string pointer is
retained across that call. Registration is limited to `Configuring`; a
successful run is one-shot (`Running -> Stopped`) because Core shutdown and
poll-stream state are not reusable. Closing before `run` releases only the
language-owned configuration and is safe.

### Activation failure is not worker failure

The language boundary has three materially different failure outcomes:

| Outcome | C/Core representation | Odin worker action |
| --- | --- | --- |
| Workflow code completes unsuccessfully | Successful activation containing `FailWorkflowExecution` | Keep polling; Workflow Execution becomes failed |
| Activation cannot be executed, including an unknown Workflow type | Failed `WorkflowActivationCompletion` with `WorkflowWorkerUnhandledFailure` | Keep polling; retain the rejected run until Core eviction |
| Bridge/protocol invariant prevents any valid completion | Poll/decode/routing/Core completion error | Stop `worker.run` and initiate shutdown |

The second row matches the pinned Rust SDK's
[`worker_handles_unknown_workflow_types_gracefully`](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/sdk-core/tests/integ_tests/worker_tests.rs)
behavior. It is intentionally a retryable Workflow Task failure, not a terminal
Workflow Execution failure. Core follows the failed completion with
`RemoveFromCache`; Odin keeps a small rejected-run table so it can acknowledge
that exact eviction without weakening routing checks for arbitrary unknown run
IDs. The live suite verifies the server history event, eviction, and subsequent
execution of a registered Workflow on the same worker.

This answers the narrow feasibility question: stackful suspension and
history-driven reconstruction work across the Odin/C boundary without a large
runtime. It does not yet answer scheduling at scale. A coroutine object is not
thread-safe, fixed stacks can overflow, and minicoro cautions against migrating
coroutines that use thread-local state. The Worker now validates and propagates
an explicit per-fiber resource policy, but the production runtime should still
drive a Workflow instance from one deterministic executor thread at a time and
prohibit concurrent resume/eviction. The chosen fixed, virtual-memory-backed
guard-page stack must still be exercised by sanitizers and all four target
builds, and stack sizing remains an operator concern because it does not grow.
Panics crossing the C trampoline also need an explicit containment policy.

The blocking wrapper now drains both enabled poll streams during shutdown.
Core requires every enabled workflow/activity poll stream to observe shutdown
before finalization; draining only the Activity stream is insufficient once
Workflows are enabled.

## Risks and unresolved questions

1. **ABI stability.** The C bridge is generated from Rust structs, unpublished,
   and evolves with Core. Pinning plus ABI assertions is mandatory. We should
   ask upstream whether third-party language SDK consumption has compatibility
   guarantees or a preferred release channel.
2. **Protobuf generator scope.** The Activity, Workflow activation, and first
   WorkflowService client graphs,
   including imports, maps, presence, and `oneof`s, are generated and
   wire-compatible with `prost`. Unknown fields are skipped rather than
   retained. Every new root graph still needs generation, compile, unit, and
   live `prost` interoperability checks.
3. **Worker concurrency.** The bounded Activity executor, heap-stable
   hash-indexed task-token routes, cooperative cancellation, and heartbeat
   transport pass live tests. It reuses a standard-library thread pool sized to
   the combined maxima of Core's enabled remote and Local Activity suppliers.
   Structured
   retryable/non-retryable failures pass generated and live retry tests. A
   500-case route property exercises 1–64 opaque binary tokens, embedded zero
   bytes, duplicate rejection, and forward/reverse drain order. A live 24-task
   fan-out saturates three Core slots, verifies a hard callback peak of three,
   bounds the completion-handoff route set to four, and drains cleanly. A live
   shutdown case also validates the configurable grace delay, concurrent
   heartbeats, exact Core cancellation flags, completion flush, and clean
   finalization. Activity-only process crash/replacement and suspended Workflow
   process loss/replay on an identical replacement Worker are live-proven. The
   remaining concurrency risks are mixed-process panic containment, infinite
   Workflow code, and process-level recovery after an uncooperative callback
   notification.
4. **Workflow runtime.** The fiber proves nested Odin suspension, durable
   timers, multiple outstanding Activities, multi-command completions,
   apply-all-before-resume, catchable Activity failures, buffered signals, and
   a durable timer/signal race, generic/typed Payload flow, read-only queries,
   synchronous and Activity-awaiting validated Updates, default/suppressed
   unfinished-handler warnings, explicit handler draining, root cancellation,
   synchronous/asynchronous Signal handlers, preemptively detached operations,
   detached Update/Activity cleanup, Child Workflows, external Workflow
   signal/cancel operations, Continue-As-New, public Timer futures, and a
   declaration-biased multi-operation selector across fresh-stack replay. It
   now proves downward-only nested cancellation-token trees across Activities,
   timers, Signal waits, main/Signal/Update fibers, propagated durable
   deadlines, and replay. Client Update result waits are now bounded and locally
   cancellable without implying durable Update cancellation. It does not yet prove
   integrated per-execution panic containment, replay-history JSON import,
   richer nondeterminism diagnostics, growable stacks, recoverable stack
   overflow, or
   a production deterministic scheduler.
5. **Cross-platform artifacts.** macOS arm64 and a clean Ubuntu 24.04 arm64
   container are locally executed through unit/property tests, all example
   builds, packaging, and packaged-runtime smoke. The repository defines the
   same jobs for macOS and Linux glibc x86_64, but those targets still require a
   hosted green run before all four can be claimed as supported. Musl and
   Windows are out of the initial scope.
   Public binary releases additionally need complete transitive Rust license
   notices/SBOM and a macOS signing/notarization policy.
6. **Core version skew.** Server compatibility is broad, but Core-local
   protobuf schemas and C structs are coupled to the exact native library.
   Generated Odin types and the native artifact must ship as one versioned unit.
7. **Error/cancellation completeness.** The shared Client/Workflow failure model
   converts every current pinned Failure variant, recursive cause, Payload list,
   and encoded attribute. The Workflow runtime retains older flattened
   Activity/Child fields as compatibility views over that model while preserving
   raw bytes for lossless propagation. External/Child signal and external
   cancellation results also retain a borrowed tree when Core supplies a
   Failure, in addition to their operation-specific cause enum.
   Activity cancellation now preserves Core's primary reason, all six detail
   flags, and callback-authored cancellation Payloads through a
   `Wait_Cancellation_Completed` Workflow result. Nested token/scope trees now
   isolate child, sibling, and detached Activity branches; fixed-point handler
   scheduling now covers token-driven timer and Signal wakeups, and inherited
   deadline timers cancel their branch deterministically. The Activity API now
   models typed success, retryability, failure type,
   details, next-retry delay, deadline/execution metadata, effective Retry
   Policy, and prior heartbeat details. Its Activity-only process-isolation
   policy is now live-proven. The SDK now manages optional whole-Worker child
   restart/readiness/backoff. A versioned bounded pipe protocol and a persistent
   two-activation Workflow executor child are proven independently, while
   Worker/Core routing and a per-Activity subprocess executor remain
   unimplemented. Typed JSON decode/encode errors
   become non-retryable Activity failures instead of worker-fatal errors.
8. **Client/server feature skew.** Workflow start/result, signals, queries,
   Activity-awaiting Updates, cancellation, termination, Describe/history, and
   Visibility list/count, System Info discovery, Workflow execution-option updates, and feature-gated
   Workflow Pause/Unpause are live-proven. Query reject conditions and static
   summary/details, query headers, and eager Workflow start are now wire- and
   live-proven. The public Client now exposes the Server's official thirteen
   `GetSystemInfo` capability bits. `Server_Feature_Observation` now separates
   whether the Odin API is implemented from tri-state server support and its
   evidence source. Safe non-mutating probes cover `CountSchedules` and
   standalone Nexus Visibility; only RPC `UNIMPLEMENTED` is conclusive
   negative evidence. Experimental mutating APIs without a bit remain unknown.
   `CountSchedules` is not implemented by the current CLI dev Server 1.29.1
   despite being present in the pinned upstream protobuf schema. Start headers,
   memo, and typed Search Attributes are implemented through an explicitly owned
   metadata builder. ID reuse/conflict, Retry Policy, cron, start delay, and
   Priority are implemented and wire/live tested.
   Signal-With-Start is now wire-, property-, and CLI-devserver-tested for both
   create and reuse paths, including its single Signal-header map and response
   `started` bit. The absence of separate start headers and eager dispatch is a
   protocol constraint, not an Odin wrapper omission.
   Update-With-Start is wire-, property-, interceptor-, and
   CLI-devserver-tested for both atomic creation and attachment to a running
   execution. It depends only on Core's generic RPC bridge and the pinned
   `ExecuteMultiOperation` protobufs; the public API enforces every current
   operation-specific Service restriction before transport.
   The pinned standalone-Activity client API is
   newer than CLI Server 1.29.1. Feature availability must be tested against
   the supported server matrix rather than inferred from generated APIs.
9. **Nexus language semantics.** Core's transport, exact task/completion schema,
   public synchronous/async-start Odin handler paths, registered cancel handlers,
   and the stackful Workflow caller
   are pinned, generated, replay-tested, and live-tested against a real
   endpoint. Handler errors have the exact
   `NexusHandlerFailureInfo` envelope, and all callback-visible data is owned
   beyond the poll arena. Workflow operation state, results, recursive failures,
   and optional operation tokens are owned by the run arena. Callback
   completion and generic pinned-schema Link encoding are implemented; four
   upstream Nexus URL/type forms parse with explicit ownership. All six pinned
   variants encode, while `BatchJob` and `Callback` have no upstream URL
   mapping. Handler application metrics are exposed through a callback-scoped
   Core meter; automatic outbound operation metrics remain. Async token
   handoff, Workflow-backed callback
   completion, caller/completer linking, and every cancellation policy are
   live-proven. Runtime state transitions for those policies are also
   property-tested. All four terminal operation states absorb duplicate or
   conflicting result delivery; in particular, late success cannot replace the
   immediate canceled result produced by `Abandon` or `Try_Cancel`.
   The Workflow outbound interceptor is implemented
   and fresh-stack replay-tested. Schedule-to-close deadline enforcement and Core's
   cooperative timeout cancellation of a running synchronous Odin handler are
   live-proven. A bounded run
   stops polling after its requested task count, so cancellation of that final
   in-flight task is only meaningful for unbounded production runs; tests must
   not confuse the bounded example control with production cancellation.

## Workflow runtime alternatives: final decision still deferred

| Option | What the evidence says now | First useful test |
| --- | --- | --- |
| Explicit state machine | Proven for Activity scheduling, forced eviction, replay, resolution, and completion; authoring API is unacceptable | Keep as a reference oracle, not the primary authoring direction |
| Stackful coroutine/fiber | Proven for serial operations, multiple outstanding Activities, handled/unhandled Activity failure, buffered and handler-dispatched signals, a durable signal/timer race, read-only queries, concurrent Activity-awaiting Updates, shared handler draining/default abandonment, detached main/Update cancellation work, Child Workflow start/result/failure/signal/cancel, external Workflow signal/cancel, Continue-As-New, patch/versioning, registered dispatch, eviction, and replay on macOS arm64 | Stress shared-state scheduling without moving fibers between threads |
| CPS/code generation | Can make state explicit while retaining source syntax; high tooling cost | Transform a two-await example and compare generated commands on replay |

Activity transport, protobuf, worker lifecycle, command matching, the replay
path, and the minimum fiber authoring boundary are now proven. This is enough
evidence to continue with fibers while retaining the explicit state machine as
a comparison point; it is not yet enough to freeze the public API.

## Recommended next vertical milestone

The complete pinned standalone Nexus RPC family used by this SDK—start, poll,
cancel, describe, terminate, delete, list, and count—is now wired through Core,
live-proven, and covered by typed Client outbound interceptors. Its typed
Start/Describe data surface is also implemented; the Server 1.32 persistence
and terminal-token limitations above remain explicit. Core log forwarding is
now exposed with copied callback-scoped records and a lifecycle-tested
process-wide route. Workflow reset now exposes both `PostResetOperation` arms,
with atomic priority update proven on Server 1.32 and the unsupported Signal arm
called out explicitly. Ordinary `UpdateWorkflowExecutionOptions` now reuses the
same validated model, is intercepted, and is live-proven through canonical
response plus Describe. The supported-server inventory identified experimental
Workflow Pause as a coherent control gap; Pause/Unpause is now intercepted and
live-proven with deferred Signal delivery. Atomic Update-With-Start is now also
implemented and live-proven on both create and attach paths. Workflow deletion
is intentionally deprioritized: current language SDK handles generally treat
it as an operator/CLI concern, and exposing a destructive call needs explicit
chain/run targeting and safety semantics. Activity-only process
crash/replacement is now live-proven with a real panic, Start-To-Close timeout,
retry, and replacement Odin worker. Fixed Workflow stacks now have a
live-tested native guard page on macOS/Linux; they do not grow, and overflow is
a process fault. Activity/Nexus shutdown now has a live-proven watchdog policy
that observes, but never unsafely terminates, callbacks beyond a configured
deadline. Standalone Activity Start/Poll/Describe/result,
cancel/terminate/delete, Visibility List/Count, Pause/Unpause/Reset, and
masked execution-option updates are now implemented, intercepted,
wire-tested, and live-proven against Server 1.32. This completes the pinned
standalone Activity client family. The next vertical milestone should continue
production hardening of the Workflow runtime: isolate an uncooperative or
panicking Workflow execution at a process boundary while preserving Core's
task/completion lifecycle. The SDK now provides the smaller whole-Worker
supervisor itself, including readiness, bounded crash-loop backoff, thread-safe
stop, TERM/grace/KILL, lifecycle events, unit tests, and a live abrupt-crash/
replay proof. The fine-grained IPC path now carries opaque
activation/completion protobufs through bounded, versioned, correlated frames
and is connected to Core through a reusable bounded per-run manager. Child
death is live-proven as activation failure, eviction, and fresh-process replay;
generated statechart traces check failed-generation fencing and multi-route
capacity. A positive activation deadline now kills and reaps an unresponsive
executor, joins its pipe reader, and drives the same failed-completion/eviction/
replay path against the dev server. Generic registry hosting is now proven with
one shared parent/child registration function and two Workflow types, including
a typed-JSON result. A canonical registry handshake is now checked once before
polling and again for each child generation. Shutdown response and process exit
are now bounded independently from activation execution; capacity admission is
immediate rather than deadlocking the serial poll thread; and a cooldown circuit
breaker bounds consecutive executor failures. Sticky per-run routing with a
non-zero Core cache is now live-proven: two
successful children retain two timer Workflows across incremental activations,
while an initial crash or timeout still evicts and recovers. Protocol v3 now
carries the complete language `Process_Result` separately from the authoritative
opaque Core completion, with live observer evidence for timer sequence fields.
Child runtimes can now receive a replay-aware logger plus ordered inbound and
outbound interceptors through explicit host options; the live proof requires
replay-suppressed interceptor logs on child stderr. The next step is a
per-runtime metric-emitter factory and structured diagnostics. Production
hardening remains process-boundary work. The
capability inventory is now a public, tested runtime model plus the maintained
evidence table below; it must grow whenever a new protocol surface is added.
Activity Worker is not the next milestone: it is already implemented and
extensively live-tested.

### Maintained client/server capability evidence

This table records observed behavior; it is not a version-number heuristic in
the runtime. `probe_server_feature` asks the connected Service directly wherever
a non-mutating probe exists.

| Feature | Odin surface | Discovery | CLI 1.5.1 / Server 1.29.1 | Server 1.32 evidence |
| --- | --- | --- | --- | --- |
| Schedules | Implemented | `GetSystemInfo.supports_schedules` | Advertised and lifecycle live-proven | Not separately regressed |
| `CountSchedules` | Implemented | Safe count RPC | gRPC 12 `UNIMPLEMENTED` | Not yet recorded |
| Nexus from Workflows | Implemented | `GetSystemInfo.nexus` | Advertised and live-proven | Live suite exposed a timeout behavior change still under classification |
| Standalone Nexus | Implemented | Safe count RPC | gRPC 12 `UNIMPLEMENTED` | Live-proven on 1.32.0 when `nexusoperation.enableStandalone=true` |
| Workflow Pause/Unpause | Implemented | No safe probe or System Info bit | Unknown; no mutation is attempted | Live-proven on 1.32.0 |
| Standalone Activity lifecycle | Implemented: complete pinned Start/Poll/Describe/result/control/options/Visibility surface and interceptors | No safe probe; SDK=true, server support remains unknown | Start returns gRPC 12 `UNIMPLEMENTED` | Complete lifecycle and masked options update live-proven through an Odin Worker on 1.32.0 |

The matrix deliberately distinguishes “SDK surface absent”, “server said
UNIMPLEMENTED”, and “not safely observed”. Authentication or namespace errors
never become unsupported-feature claims.

The selector/join branch, async
cancel-token handoff, and terminal-result idempotence are unit-, live-, replay-,
and/or property-tested as applicable; selector ordering and all four
cancellation-policy transitions are also property-tested.
The Workflow outbound
interceptor and handler failure/pre-start timeout classification are now
wire-/unit-tested as applicable and fresh-stack replay-tested; handler failure
and timeout also run through the live server. Async callback completion now
runs through a linked Workflow started by the Odin handler. Activity Worker is
no longer the next milestone: it is already implemented and extensively
live-tested.

Activity concurrency, task-token routing, cooperative cancel delivery,
heartbeat transport, typed JSON Payloads, structured user failures with a real
server-directed retry, generic Workflow Payloads, and replay-safe Activity
options are now proven live. Activity context identity, timeouts, effective
Retry Policy, deadline derivation, and heartbeat-detail redelivery are also
proven. The PBT suites generate 500 Payload values, 500
Application Failure contracts, 500 typed Workflow replay cases, 500 remote
Activity-option command/replay cases, and 500 Local Activity-option
command/replay cases, plus 500 Activity deadline combinations and 300 Workflow
cancellation-deadline trees,
500 Client close-event/result round trips, 500 recursive Failure chains,
500 Client start request (including static UserMetadata and eager-start intent)
round trips, 500 Signal-With-Start request round trips, 500 ordinary repeated
Signal request/header round trips over 0–12 arguments, 500 paginated Visibility
list/count round trips, 500 repeated Query request/header round trips over 0–12
arguments, 500 repeated Update request/header round trips over 0–12 arguments,
500 Update-With-Start request round trips over two independent 0–12 argument
lists with exact operation order and deterministic bytes,
500 Worker versioning mappings across every C-union strategy,
500 global/per-type nondeterminism policy mappings,
500 query response round trips, and
500 single-Update protocol response sequences, 500 two-Update completion-order sequences,
500 Child Workflow option/repeated-argument/repeated-signal/replay cases, 500 external Workflow
repeated-signal/cancel/replay cases, 500 Continue-As-New repeated-argument/metadata/versioning/replay cases, 500 patch
truth-table cases, 500 deterministic time/random activation contexts, 500 late
Signal-handler buffer/order/replay cases, and 1,000
statechart-backed Update lifecycle traces,
against both the pinned library and `~/Projects/pbt`.

The declaration-biased Workflow selector, allocation-free `join_all`, public
Timer future, and distinct non-suspending timer-start interceptor are now
implemented and live-proven. Two independent
180-trace properties cover selector case/job order and join job/split order with
fresh-stack replay; the selector property also carries generated Timer summaries
through command metadata and replay. Timer duration, summary, and explicit
cancellation-token options are implemented. A 300-trace Child composition
property now covers start/completion selectors, start/completion joins,
explicit cancellation, partial readiness, and four byte-identical replay
completions. Nexus operation Futures now participate in the same selector/join
readiness model, with a separate 180-trace biased-order/job-interleaving and
fresh-stack replay property. Two additional 240-case properties cover all four
cancellation-policy transitions and require every terminal state to absorb
duplicate or conflicting result resolution.
None of these scheduler features requires a new C ABI symbol.

The Child Workflow start/result/failure/signal/cancellation slice is now
implemented. The Workflow-local Future acts as the public child handle for
await, signal, and explicit cancellation. A live duplicate-ID case proves the
Core-local `WorkflowAlreadyExists` start failure. External Workflow signal and
cancellation are also implemented and live-proven. Continue-As-New now provides
the next-run Payloads, timeout/retry/backoff options, memo, typed Search
Attributes, headers, and Core versioning choices through a non-returning
terminal fiber primitive. Core's patch/versioning protocol is now
implemented and live-proven. The first Local Activity
command/dispatch/resolution path and timer-backed `DoBackoff` retries are now
live-proven. Workflow information, headers, memo, and Search Attribute commands
are implemented, and the Odin Client supplies all three initial Payload maps
end-to-end. Client start-policy completeness (ID reuse/conflict, Retry Policy,
priority, delay, and cron) is now implemented and live-proven, including
`Use_Existing`. Typed Describe, paginated history, Visibility list/count, and
Schedule create/describe/list/pause/unpause/trigger/delete are now implemented
and live-proven without new C symbols. Full replace-update with optimistic
conflict tokens and matching-time queries are also live-proven. Backfill now has
an additional live `BUFFER_ALL` slice that executes two historical actions plus
one manual trigger on the bounded Odin Worker; structured/excluded calendars
and TZif data retain exact wire/property coverage.
Recursive Client and Workflow failure inspection is now implemented and
live-proven. Query reject conditions plus static summary/details are now
implemented and live-proven against the CLI dev server, including the typed
status on a rejected closed-execution query and persisted Describe metadata.
Activity cancellation detail completeness is now implemented: all Core flags,
an explicit Workflow cancel primitive, correct
`Wait_Cancellation_Completed` suspension, recursive canceled Failure
preservation, and a typed user-detail round trip are unit-, property-, and
live-proven. Query headers, Signal-With-Start, and same-connection eager
Workflow start are now owned, wire-tested, and live-proven; start headers were
already covered by `Start_Metadata`. Signal-With-Start proved that the protocol
exposes one Signal header rather than separable start/signal maps. Normal
`SignalWorkflowExecution` now accepts the same explicitly owned header builder,
and Signal metadata is retained by both direct and signal/timer-race awaits.
The Workflow runtime clones headers into its run arena and emits Core's header
maps for remote/Local Activities, Child Workflow starts, and child/external
Workflow Signals. Core delivers Activity headers back on `ActivityTask.Start`;
the worker moves them into the callback owner before destroying its protobuf
decode arena. Generated-value properties require byte-identical replay for all
four outgoing command families, and the CLI-devserver suite proves the two
main interceptor paths: Client Signal to Workflow and Workflow command to Odin
Activity. This adds no C ABI symbol—the boundary remains protobufs over the
generic Client RPC plus poll/completion calls. Backfill should still gain a dedicated multi-execution
live case without overloading the one-shot Schedule example. Remaining failure work includes deciding whether start failures
that only carry Core cause enums need a synthetic language-level Failure.

Asynchronous Signal handlers are now implemented by generalizing the handler
scheduler. Immediate and stackful registrations, independent suspension,
deterministic handler-before-main ordering, shared handler draining,
warn/abandon close policy, public Client delivery, Query-observed state mutation,
Core eviction, and fresh-stack replay are tested. Dynamic registration and
late buffered dispatch are now covered offline, generatively, and through the
live server. Handler failure is also covered through failed completion, Core
`LANG_FAIL` eviction, fresh-stack replay, server history, and continued Worker
service. A documented shared-state policy remains before this API can be called
stable.

Update cancellation through root Workflow cancellation is now proven while a
handler awaits an Activity: the detached operation survives, explicit draining
prevents early close, the Update response precedes cancellation acknowledgement,
and late resolution/replay are deterministic. The public token/tree abstraction
now covers deterministic child/detached scopes and Activity cancellation.
Client-side Update waits can now be canceled through Core's RPC token, but this
deliberately does not cancel an accepted Update. Any future policy for canceling
the durable operation itself depends on a server protocol capability; the
current Temporal protocol does not expose that as a language-token operation.

Activity panic containment is defined as an Activity-only supervised process
and its crash/replacement retry path is live-proven. The SDK can now supervise
and restart that whole Worker child; a per-Activity subprocess executor remains
future work unless Odin gains supported recovery. Activity and Nexus callback
drains now have deadline observation without forced thread termination; truly
stuck callbacks still require the supervisor's parent process to detect and
replace the child. Infinite Workflow code cannot reach the activation boundary
needed for cooperative shutdown and has the same process-level limitation. A
statechart oracle can later expand the now-live graceful-shutdown path to
retry/failure races.

In parallel, add CI compile/smoke jobs
for macOS x86_64 and Linux x86_64/arm64 before treating minicoro as a committed
dependency. The Update routine table now has explicit ownership, replay tests,
a defined Workflow-close policy, and a live cancellation/drain path. The public
cancellation model remains experimental while API naming and accepted-Update
policy are unresolved.

The C bridge remains the recommended foundation. It now handles Activity Tasks,
multi-activation Workflow execution including replay/eviction, and synchronous
Nexus operations end-to-end. The findings add protobuf/tooling,
language-runtime, and version-skew obligations but give no reason to bypass
Core or implement direct server gRPC.

### Child Workflow boundary

Child Workflows required no new native ABI surface. They are entirely expressed
through the existing serialized Workflow Activation and Activation Completion
messages:

- Odin emits `StartChildWorkflowExecution`, including one language-assigned
  `seq`, explicit Workflow ID/type, task queue, an ordered repeated input
  Payload field, timeouts, Retry Policy, Parent Close Policy, Child
  Cancellation Type, Workflow ID reuse, cron schedule, headers, memo, typed
  Search Attributes, versioning intent, and priority. Static summary/details
  travel on the enclosing `WorkflowCommand.user_metadata`, not on the start
  message itself.
- Core later emits `ResolveChildWorkflowExecutionStart(seq, status)`. Success
  carries the child Run ID; failure carries Workflow ID/type and
  `WorkflowAlreadyExists`; cancellation carries a Failure.
- Independently, Core emits `ResolveChildWorkflowExecution(seq, result)` with a
  completed Payload, Failure, or cancellation Failure.
- Odin emits `CancelChildWorkflowExecution(child_workflow_seq, reason)` when an
  inherited parent cancellation reaches a commanded child. Core applies the
  cancellation type carried by the original start command.
- Odin emits `SignalExternalWorkflowExecution` with the child Workflow ID as
  its target and an ordered repeated argument field. An optional UI summary is
  encoded on the enclosing `WorkflowCommand.user_metadata`. Core later emits
  `ResolveSignalExternalWorkflow(seq, failure,
  cause)`, which Odin maps back to the waiting fiber.

This means one child sequence has two independent readiness milestones. The
Odin operation table registers both conceptually at start time, even when user
code awaits only completion. `await_child_workflow_start` suspends on the first;
`await_child_workflow_payload_result` and the typed JSON wrapper then suspend on
the second. A completion delivered in the same activation as the start
resolution is retained before either fiber resume, so activation ordering does
not lose it.

Owned Workflow ID/type/options/input Payloads, option strings, map keys and
Payloads, Retry Policy strings, priority fairness key, encoded static metadata,
Run ID, output Payload, structured failure fields/details, and original encoded
Failure bytes live in the Workflow instance arena. Public options borrow their
inputs only until the start call returns. Activation decode values remain
borrowed only for that callback and are cloned before the decode arena is
destroyed.
Eviction destroys the entire operation table; replay recreates it by rerunning
deterministic Odin code. The offline acceptance test evicts immediately after
the start command and requires a byte-identical command from a fresh stack.

The raw Core message retains a `namespace` string, but current TypeScript,
Python, .NET, and Rust language surfaces do not offer cross-namespace Child
Workflow starts. Odin therefore leaves it at Core's same-namespace default
instead of exposing a misleading option. Cross-namespace signal/cancel remains
available through `External_Workflow_Execution`. The zero-value Odin
`Workflow_Id_Reuse_Policy` is normalized to `ALLOW_DUPLICATE`, matching Core's
documented Child Workflow default and the normalization performed by upstream
language runtimes. Nil and initialized-empty memo/Search Attribute maps both
mean no starting values because a new child has no inherited map state.

`start_child_workflow_payloads` is the codec-neutral public boundary. Its
borrowed slice, Payload bytes, metadata entries, and external payload details
are synchronously cloned into the run arena. The interceptor sees the complete
slice and may replace it before the terminal clone. The singular Payload and
typed JSON helpers delegate as one-element slices. Core's protobuf already
models `StartChildWorkflowExecution.input` as repeated, so this required no C
ABI change and no native bridge symbol. `execute_child_workflow_payloads` is
the start-plus-await convenience form and returns the codec-neutral child
result without imposing a JSON result type.

The default Odin cancellation enum intentionally maps its zero value to Core's
`WAIT_CANCELLATION_COMPLETED`, matching the safe Core SDK default rather than
the protobuf enum's zero-valued `ABANDON`. `ABANDON` and `TRY_CANCEL` can resume
the parent locally; the wait policies remain suspended until Core supplies the
corresponding resolution. Unit tests cover cancellation while waiting for the
start resolution and exact reason transport. The CLI-devserver test cancels a
parent with a running, timer-blocked child and verifies that Odin emits the
cancel command, receives the child resolution, and only then acknowledges the
parent cancellation.

On failure, Odin builds the shared recursive Failure tree in the Workflow
instance arena. The direct child identity, Application, cancellation, timeout,
and termination fields are borrowed compatibility views over that tree. Typed
JSON helpers decode its owned detail Payloads through borrowed views.
For an unhandled child failure, the parent completion re-emits the original
Temporal Failure protobuf rather than synthesizing a generic Application
Failure. This preserves server-generated wrapper/cause semantics for callers
and higher-level parent Workflows. The public structure currently summarizes
the first recognized cause instead of exposing an arbitrary recursive typed
Failure tree.

`Child_Workflow_Future` is also the Workflow-local operation handle. Explicit
`cancel_child_workflow` suppresses a still-local start or, once the start was
commanded, uses Core's child-specific cancel command and preserves the caller's
reason. Typed JSON or raw Payload signals and their optional summary metadata
are copied into the Workflow arena, assigned an independent external-signal
sequence, and suspended until Core acknowledges the command. Signal failures
expose Core's not-found, namespace-not-found, and
signal-count-limit causes. Both signal command bytes and their generated input
Payloads are covered by eviction/fresh-stack replay properties. The
codec-neutral child/external signal APIs clone zero or more Payloads—including
metadata and external payload details—into the run arena; singular Payload,
owned-Payload, and typed JSON helpers adapt to one element.

The API enum for `StartChildWorkflowExecutionFailedCause` has additional
namespace/versioning values, but the pinned Core activation schema deliberately
uses its narrower Core-local enum containing only `Unspecified` and
`WorkflowAlreadyExists`. Odin therefore does not expose causes that Core cannot
deliver across this boundary.

The CLI-devserver acceptance test registers parent and child Workflow types on
one Odin worker and task queue. Temporal starts the child as a distinct durable
execution. Success returns the child's typed result, failure produces a real
server-authored child wrapper decoded by the parent, and cancellation traverses
the Core command/resolution protocol. This confirms the intended boundary:

```text
Odin parent fiber -> StartChild command -> Core -> Temporal Service
Temporal Service -> child activation -> Core -> Odin child fiber
Odin child completion -> Core -> Temporal Service
Temporal Service -> child completion resolution -> Core -> Odin parent fiber
```

### External Workflow operation boundary

External Workflow signal and cancellation likewise require no new C symbol.
They use the existing activation/completion protobuf channel:

- `SignalExternalWorkflowExecution` targets a
  `NamespacedWorkflowExecution { namespace, workflow_id, run_id }`, carries one
  language-assigned signal sequence and generic Payload arguments, may carry a
  UI summary on the enclosing command metadata, and resolves through
  `ResolveSignalExternalWorkflow`.
- `RequestCancelExternalWorkflowExecution` targets the same identity, uses its
  own language-assigned cancellation sequence and optional reason, and resolves
  through `ResolveRequestCancelExternalWorkflow`.
- A missing run ID means the current execution for that Workflow ID. Namespace
  and Workflow ID are required by the Odin boundary.
- Signals and cancellation requests have independent sequence spaces because
  Core resolves them through different activation job types.

The execution identity, signal name, optional encoded summary, reason, ordered
Payloads, failure text, and encoded failure bytes are copied into the Workflow
instance arena before the user fiber can suspend.
`signal_external_workflow_payloads` clones its borrowed Payload slice; the
singular Payload and JSON helpers delegate with one element, while
`signal_external_workflow_owned` consumes one owned Payload. Returned strings
are Workflow-owned views and must not escape the execution. Eviction destroys
this state; deterministic replay recreates it from Workflow code and history.

Core owns server delivery and maps history events back to the matching `seq`.
Odin owns validation, sequence allocation, ordered argument cloning, command construction, suspension,
resolution lookup, failure-cause mapping, and fiber resumption. Signal failures
map `WorkflowNotFound`, `NamespaceNotFound`, and `SignalCountLimitExceeded`;
cancellation failures map the first two. Root Workflow cancellation can cancel
an in-flight signal through `CancelSignalWorkflow`. An already-issued external
cancellation request is itself cleanup work and remains awaited until Core
resolves it rather than being canceled locally.

The acceptance test starts two independent Odin Workflow executions. The caller
signals the target, waits for acknowledgement, requests target cancellation,
waits for that acknowledgement, and completes. The target observes the signal
and then closes as canceled. The test reads both child- and external-target
signal summaries from real
`EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED` history events. Unit
and 500-case property tests additionally require both outbound command
completions to be byte-identical after eviction and fresh-stack replay.

### Continue-As-New boundary

Continue-As-New is another existing Core completion command and adds no C ABI:
Odin emits `ContinueAsNewWorkflowExecution`; Core and the Service close the
current run, create the next run in the same Workflow execution chain, and later
deliver an ordinary `InitializeWorkflow` activation for it. There is no
resolution job for the old run because the command is terminal.

The Odin primitive is typed `-> !`. It copies/owns the new ordered input Payloads and
options, marks the active fiber run terminal, yields to the language scheduler,
and is never resumed. The completion builder emits exactly one terminal command
after any Update responses. Empty Workflow type or task queue values reuse the
current registered values. The initial slice maps:

- zero or more ordered generic input Payloads;
- Workflow type and task queue;
- Workflow Run and Workflow Task timeouts;
- Retry Policy;
- first-task backoff;
- replacement memo and typed Search Attributes;
- Build ID versioning intent and experimental initial versioning behavior; and
- generic Payload headers, including interceptor mutations.

Nil memo/Search Attribute maps preserve the current run's values. An initialized
empty Search Attribute map is encoded as a present empty message and therefore
requests replacement with no attributes. The proto3 memo field is a map rather
than an optional message, so Core's wire format cannot distinguish an empty
replacement from unset; an empty memo map therefore preserves the current memo.
Search Attribute builders attach Temporal's canonical `type` Payload metadata.
`Continue_As_New_Versioning_Intent` maps exactly to Core's `UNSPECIFIED`,
`COMPATIBLE`, and `DEFAULT`; `Continue_As_New_Versioning_Behavior` maps to the
Service enum's unspecified, auto-upgrade, and use-ramping-version values.
Negative/contradictory durations, invalid enum values, and invalid Retry Policies
fail the Workflow with a non-retryable SDK Application Failure. Calls outside
the main Workflow context follow the same failure path. This restriction avoids
letting a concurrent Signal or Update handler silently terminate main while the
handler scheduler still owns a different execution-state snapshot.

Ownership mirrors other commands: `continue_as_new_payloads` clones a borrowed
slice including metadata and external payload details, the singular `_payload`
form clones one view, the `_owned` form consumes one Payload, and JSON encodes directly into the
Workflow allocator. Options, Retry Policy strings, memo, headers, and Search
Attributes are cloned
before the fiber yields. The interceptor working headers also live in this
arena because the terminal call cannot return to a defer. Eviction frees the
suspended terminal fiber and its arena; replay reruns user code and must
reconstruct identical command bytes. Unit and 500-case property tests enforce
that invariant while varying map presence, all seven Search Attribute metadata
types, and every versioning enum value. The live dev-server case proves the
Service creates run two, applies replacement memo/Search Attributes, and that
the same Odin worker completes it under the original Workflow ID.

### Workflow patch/versioning boundary

Workflow code patching also uses the existing protobuf completion boundary and
requires no new C ABI symbol. The pinned Core schema exposes
`WorkflowActivationJob.notify_has_patch` (field 9) and
`WorkflowCommand.set_patch_marker` (field 10). `NotifyHasPatch` contains the
patch ID; `SetPatchMarker` contains the same ID plus `deprecated`.

Core owns history lookahead and marker state machines. While replaying, it scans
through the next Workflow Task for `core_patch` marker events and sends
`NotifyHasPatch` preemptively. The language runtime owns the synchronous API
decision and must return before waiting for any resolution. The exact pinned
Core behavior, verified in `crates/workflow/src/workflow_context.rs` and
`crates/sdk-core/src/worker/workflow/machines/patch_state_machine.rs`, is:

| Activation state | `patched(id)` result | Odin completion |
| --- | --- | --- |
| Not replaying | `true` | one non-deprecated `SetPatchMarker` |
| Replaying, notified by Core | `true` | matching `SetPatchMarker` |
| Replaying, no notification | `false` | no patch command |

`deprecate_patch` uses the same decision table and sets `deprecated=true` when
it emits a marker. A patch ID is memoized on first use, so subsequent calls in
the run return the same decision and emit no duplicate command.

Core's first replay activation is observably ordered as `InitializeWorkflow`
followed by `NotifyHasPatch`. Rust can construct its async Workflow before
polling it; Odin's stackful fiber begins executing immediately when resumed for
initialization. The Odin runtime therefore pre-scans and arena-copies every
patch notification before it starts or resumes user code. This is a real
language-runtime adaptation, not a change to Core semantics. On later
activations the same pre-scan happens before signals, Updates, resolutions, or
the main fiber can run.

Patch IDs and memoized decisions live in the per-run arena. Activation strings
borrow the decode arena only until copied. Completion command strings then
borrow the run arena through encoding; eviction destroys them with the fiber.
Read-only query and synchronous Update validation snapshots include the patch
decision count, so an attempted first patch decision is rolled back and
reported as a mutation.

The unit suite covers fresh history, replay with and without notification,
Core's initialize-then-notify order, duplicate calls, and deprecated markers.
A 500-case property generates the replay/notified/deprecated truth table. The
CLI-devserver test executes the new branch, observes one emitted marker, and
requires `core_patch` in server history. This API concerns deterministic code
changes inside a Workflow and remains separate from Worker Deployment
Versioning.

### Worker Deployment Versioning boundary

The pinned C bridge already exposes `TemporalCoreWorkerVersioningStrategy` as
a tagged union with `None`, `DeploymentBased`, and `LegacyBuildIdBased`
variants. Odin mirrors it with `Worker_Versioning_Mode` and maps the Service's
`VersioningBehavior` values to `.Unspecified`, `.Pinned`, and `.Auto_Upgrade`.
No new C symbol or protobuf graph is involved; these options are inputs to the
existing `temporal_core_worker_new` call.

`worker.create` remains language-only and therefore clones deployment/build
identity together with namespace/task queue. `worker.run` later passes borrowed
byte references to `core.worker_create`; the C bridge synchronously converts
them to Rust-owned `String` values while constructing `WorkerConfig`. The Odin
copies remain valid until `worker.close`, making delayed Core creation explicit
and safe.

The modern deployment mode validates the same important invariants as Core:
Build ID is required, deployment name is required when routing is enabled, and
a default behavior is invalid when routing is disabled. Unversioned mode keeps
the prior default Build ID and rejects deployment-only fields. Legacy mode
requires its Build ID and rejects modern routing fields.

The CLI-devserver acceptance test starts a `.Pinned` Odin Worker, makes its
deployment version Current, starts a Workflow on an isolated task queue, and
requires Describe output to contain the exact deployment name and Build ID.
This proves Service -> matching -> Core -> Odin routing rather than only C-union
encoding. CLI 1.5.1 exposes `--unversioned` cleanup, but bundled Server 1.29.1
rejects that transition with `BuildID cannot be empty`; cleanup is consequently
best-effort and uniquely names every test deployment. Core's experimental
rollout callback remains unexposed.

### Nondeterminism failure policy boundary

Core—not the Odin scheduler—detects a mismatch between replay history and an
Odin `WorkflowActivationCompletion`. By default Core fails the Workflow Task,
leaving the Workflow Execution running so a compatible deployment can repair
it, matching Temporal's
[execution-failure guidance](https://github.com/temporalio/documentation/blob/main/docs/troubleshooting/execution-failures.mdx).
The pinned C bridge's
[`WorkerOptions` conversion](https://github.com/temporalio/sdk-rust/blob/e163abd6dc19040064986a63c8b8cf4756ffd320/crates/sdk-core-c-bridge/src/worker.rs)
exposes two inputs on `TemporalCoreWorkerOptions`:
`nondeterminism_as_workflow_fail` applies terminal failure globally, while
`nondeterminism_as_workflow_fail_for_types` applies it to named Workflow types.

Odin exposes these as `Nondeterminism_Policy`. `worker.create` validates and
clones the type names; `worker.run` builds a temporary
`TemporalCoreByteArrayRefArray` for the synchronous `temporal_core_worker_new`
call. The bridge immediately creates Rust-owned strings and Core converts the
list to its per-type failure map. No protobuf type or additional C symbol is
required.

The live test records a Timer command with one Worker process, replaces the
same Workflow type with code that emits `CompleteWorkflowExecution`, and sends
a Signal to force fresh replay. Core reports `[TMPRL1100]`, transforms the
mismatch into `FailWorkflow`, and the Service history closes as failed. This
proves detection, per-type selection, process-loss replay, and terminal policy.
It does not replace patching/versioned deployments, nor does it provide an
offline replay command or richer Odin-originated mismatch diagnostics.

### Workflow time and randomness boundary

Temporal-aware time is activation state, not a host-clock read. Core places the
history-derived time in `WorkflowActivation.timestamp`; Odin copies it to the
run instance before any job can resume code. `workflow.now` returns that stable
value plus an availability flag, and every call within the activation observes
the same instant. `workflow.history_length` and `workflow.is_replaying` expose
the corresponding activation fields without introducing commands.

Core also derives a seed from the run identity. The first seed arrives in
`InitializeWorkflow.randomness_seed`; reset boundaries arrive as the
preemptive `UpdateRandomSeed` job. The Rust reference runtime scans seed updates
in `apply_activation_context` before dispatching jobs. Odin mirrors that order,
including when `InitializeWorkflow` precedes the update job in the serialized
activation. This keeps Signal/Update routines and the main fiber on one shared,
deterministically ordered stream.

The Odin API provides `random_u64`, `random_f64`, and `uuid_v4` on the shared
per-run stream. `random_stream_create` consumes exactly one shared draw and
returns an independent value-state SplitMix64 generator. Its u64/f64 and UUID
operations use the same pinned algorithm; range/index helpers use rejection
sampling so arbitrary non-power-of-two bounds are unbiased. Copying a stream
snapshots it intentionally. Existing stream values are not mutated by a later
`UpdateRandomSeed`; because Core seed updates are pre-scanned, streams created
after that activation context is applied derive from the new parent state.
SplitMix64 was selected as a tiny, locally implemented generator so replay does
not depend on an Odin standard-library algorithm changing. Its algorithm and
constants are now compatibility state. `random_f64` uses the high 53 bits and
returns `[0, 1)`; UUID generation consumes two draws and stamps RFC 4122 version
4 and variant bits. None is cryptographically secure. Query and synchronous
Update-validation snapshots include the random state, so drawing there is
detected as mutation and rolled back.

One unit test fixes a Core seed, timestamp, and history length, then requires an
identical terminal completion after eviction and replay. A 500-case property
varies all three plus optional `UpdateRandomSeed`, non-power-of-two range and
index bounds, checks every exact shared/independent SplitMix64 draw, and again
requires byte-identical completion. The live patch example also reads Workflow
time and produces shared and independent deterministic UUID/range/index values
using the real CLI dev server. All information fields in the pinned
`InitializeWorkflow` schema are preserved, including ordered repeated Payload
arguments.

### Per-activation metadata and current details

Core's `WorkflowActivation` also carries `history_size_bytes`,
`continue_as_new_suggested`, `deployment_version_for_current_task`, and the
experimental `target_worker_deployment_version_changed` flag. Odin copies the
scalars before dispatch and preserves deployment/build strings in the run
arena only when their value changes. Public accessors therefore borrow stable
run-owned strings without retaining pointers into the short-lived protobuf
decode arena. `history_size_bytes` is a Core estimate and may be zero on the
initial activation; callers must not interpret zero as a missing field.
For `WorkerVersioningStrategy::None`, Core may still expose a version with an
empty deployment name and the worker Build ID; the optional is absent only
when Core's computed version is empty. Odin preserves that distinction rather
than equating an empty deployment name with no version.

`is_replaying_history_events` is derived exactly like the pinned Rust runtime:
it is true only when `is_replaying` is true and the activation contains at
least one non-Query job. Query-only replay can consequently expose live
diagnostics without being mistaken for replay of history events.

`set_current_details` is not a Core command. It replaces one heap-owned string
on the Workflow instance, and the reserved
`__temporal_workflow_metadata` Query serializes it as proto-JSON-compatible
`currentDetails`. Repeated updates free the prior string; eviction frees the
last value, and replay reconstructs it by rerunning Workflow code. The query is
handled before the user registry, and registration rejects its reserved name.
No C symbol or generated protobuf change is involved. Unit coverage verifies
all activation fields, query encoding, ownership through eviction, query-only
replay classification, and byte-identical fresh-stack replay. The live
properties Workflow queries `currentDetails` before a Signal permits it to
close; the deployment acceptance Workflow also requires a non-empty version
delivered by Core.

## Core-connected Workflow process isolation

The language boundary can now be replaced for online Workflow activations
without moving any Temporal protocol responsibility out of Core. `worker.run`
normally calls its in-process fiber dispatcher. When the experimental
`Run_Options.workflow_activation_processor` callback is configured, the same
borrowed `WorkflowActivation` bytes are handed to that callback serially and
its owned `WorkflowActivationCompletion` bytes are passed unchanged to Core.
Offline replay intentionally continues to use the ordinary in-process registry.

No new C ABI is required. This path depends on the already-pinned bridge calls:

- `temporal_core_worker_poll_workflow_activation` supplies the activation;
- `temporal_core_worker_complete_workflow_activation` accepts success or
  failed completion bytes;
- `temporal_core_worker_initiate_shutdown` and the final shutdown poll/finalize
  sequence retain ownership of Worker teardown; and
- the existing generic WorkflowService RPC bridge starts the execution and
  waits for its result through the public Odin Client.

The child protocol carries the Core protobufs opaquely. The parent decodes only
the routing fields (`run_id`, initialization, eviction, replay flag, history
length, and Workflow type) and terminal completion structure needed to validate
child observation metadata. Protocol v3 sends a separate JSON
`Completion_Metadata` frame containing the complete language
`workflow.Process_Result`, excluding its completion bytes and duplicate run ID.
The protobuf remains authoritative: inconsistent terminal flags reject the
session, and metadata never alters commands. Core remains responsible for
history application and state machines; the child owns only the Odin registry,
fiber stacks, and language-deterministic state for its cache lifetime.

Native procedure pointers are process-local and are never serialized. The
application exposes one ordinary Odin registration function and compiles it into
both executables. The parent calls it to build `workflow.Registry`, then copies
only its type-name manifest into the Worker with
`register_external_workflow_registry`. The child calls the same function and
passes its Registry to `isolate.Executor_Host`, which instantiates one
independent `Fiber_Runtime` per type and selects it from
`InitializeWorkflow.workflow_type`. Subsequent activations route by run ID
until eviction. The Registry supports string, one-Payload, repeated-Payload,
and typed-JSON callbacks; the live proof completes both string and typed-JSON
types through one executor binary.

External registrations are metadata-only in the parent and cannot be mixed
with in-process registrations. An external activation processor is mandatory.
Parent-side Workflow loggers and interceptors are rejected because the parent
has no executing runtime. `Executor_Host_Options` now copies a replay-aware
logger and ordered inbound/outbound interceptors into each child runtime before
execution; hook `user_data` remains child-application-owned. The live host emits
interceptor logs on inherited stderr, leaving framed stdout exclusive to the
protocol, and zero-cache replay proves default log suppression. Protocol version
2 adds an ordered, length-prefixed type manifest;
version 3 adds the correlated observation-metadata frame.
`manager_verify_host` compares it in a temporary child before polling, and every
executor generation repeats the correlated handshake before activation. A
mismatch is therefore rejected and reaped without running Workflow code.

The first verified mode sets `max_cached_workflows = 0`. A child therefore
serves one Workflow Task and is shut down only after the corresponding
`RemoveFromCache` activation. A forced generation-1 exit becomes a failed
activation completion. Core then emits language-failure eviction, and the
Server retries the Workflow Task; fresh child generations rebuild Odin state
from the history carried by later Core activations. Against the CLI dev server,
the two timer Workflows currently complete after five generations: one crashed
initial attempt and four successful zero-cache tasks across scheduling and
replay/timer resolution. Exact batching and generation counts are diagnostic,
not an SDK contract.

The second verified mode sets `max_cached_workflows = 100`. After one initial
crash or timeout, the two Workflow runs each retain one successful child across
timer suspension and resolution. Core delivers two incremental activations to
those existing run routes. The live path therefore uses three generations and
three evictions rather than zero-cache's five, with three graceful shutdowns
including preflight. Both exact results and all failure-containment assertions
remain unchanged.

The cache/language-lifetime invariant is strict:

```text
Core cache entry exists  <=>  exactly one live Odin executor generation owns it
```

Zero-cache satisfies this with one process per successful Workflow Task. Sticky
mode retains a run-routed child until Core eviction and is now live-proven with
non-zero caching. Starting a new child for an incremental activation while Core
still considers the old language cache entry live remains invalid because the
new process lacks the suspended fiber.

The external processor itself is an Odin-only API seam and is still marked
experimental. A reusable manager now provides bounded admission, owned run-ID
routes, monotonically increasing generations, private per-generation pipes,
correlated request IDs, failed-until-eviction state, and complete child reaping.
Its transition function is checked against an independent statechart over
generated traces, while a second generated model checks multi-route capacity
and generation fencing. Activation deadlines use a joinable reader thread and
kill/reap on expiry; the hung-child live path proves retry and replay. Startup
and per-generation manifest verification now fence executable/registry skew.
Shutdown deadlines now kill/reap both missing acknowledgements and processes
that acknowledge but do not exit. Capacity exhaustion fails immediately because
waiting on the serial activation thread would prevent eviction from releasing a
slot. A configurable consecutive-failure breaker prevents unbounded respawn and
admits a trial after cooldown. Complete observation metadata is now preserved
through the process manager and fed to ordinary Worker statistics/observers;
the live proof checks child-only timer sequence values in zero- and sticky-cache
modes. Executor-host log, metric, and interceptor hooks remain language-runtime
work, not additional Temporal protocol work. Logger and interceptor
configuration is implemented; per-runtime metric-emitter creation and optional
structured event transport remain. See
`docs/workflow-isolation.md` for the executed recovery trace and remaining
safety work.

### Local Activity boundary

Local Activities do not bypass SDK Core. Odin emits the existing
`WorkflowCommand.schedule_local_activity` variant and Core dispatches a normal
`ActivityTask` through `temporal_core_worker_poll_activity_task`, with
`Start.is_local=true`. The registered Odin Activity callback and completion ABI
are therefore shared with remote Activities. Core records the result as a
`core_local_activity` marker and later delivers
`ResolveActivity(is_local=true)` to the Workflow runtime. No new C symbol is
required.

The worker task-type configuration is significant. The C bridge exposes four
independent booleans: Workflows, Local Activities, remote Activities, and
Nexus. Enabling Workflows and polling Activity tasks is insufficient unless
`TemporalCoreWorkerTaskTypes.enable_local_activities` is also true. The first
live attempt exposed this exact failure mode: Core waited inside Workflow Task
completion for a language-side local poll that could never produce a task, and
the Workflow Task timed out. The high-level Worker enables Local Activities
only when it owns both a Workflow poller and an Activity registry and
`Run_Options.enable_local_activities` is set. This opt-in is temporary while
mixed remote Try-Cancel/local shutdown interleavings are hardened; enabling the
Core task type for workers that did not use it exposed a bounded shutdown join
stall. The Core lifecycle wrapper includes an enabled local stream in its final
shutdown poll.

The Odin runtime reuses its Activity future/result/failure machinery while
emitting the distinct `ScheduleLocalActivity` and
`RequestCancelLocalActivity` commands. It validates
`ResolveActivity.is_local` against the scheduled operation so a mismatched Core
resolution becomes an activation failure rather than corrupting a waiting
fiber. The public options currently map Activity ID, Schedule-to-Close,
Schedule-to-Start, Start-to-Close, Retry Policy, local retry threshold,
cancellation type, and marker-input inclusion. Local Activities have neither a
task queue nor a heartbeat timeout.

Core may resolve a retrying Local Activity with `DoBackoff` when the retry delay
should become a durable timer. Correct language handling must schedule that
timer and then emit a new Local Activity command with Core's next attempt and
original schedule time. The Odin runtime now keeps a stable logical operation
sequence for the public Future and a separate Core command sequence. On
`DoBackoff` it records Core's next attempt/original timestamp and emits a normal
`StartTimer`; `FireTimer` allocates a fresh Core sequence and re-emits
`ScheduleLocalActivity`, while the suspended fiber continues waiting on the
original logical sequence. Cancellation during backoff cancels that timer.
A unit test covers the full transition and fresh-stack replay, one 500-case PBT
varies every exposed option, a second 500-case PBT varies retry metadata and
requires byte-identical timer/retry commands, and the CLI dev-server suite
proves a failed first attempt followed by the durable timer and successful
second local attempt.

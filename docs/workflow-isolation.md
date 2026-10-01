# Workflow process isolation

Status: experimental per-run process boundary, connected to `worker.run` and
live-proven in both zero-cache and sticky-cache modes against the Temporal CLI
dev server. It is not yet a production executor pool.

## Decision and evidence

The executor lifetime must align with Core's language-cache lifetime. There are
two valid modes:

- **zero-cache isolation:** use a fresh child for one Workflow Task, complete
  it, consume/acknowledge Core's eviction, and replay history in a new child for
  the next task;
- **sticky isolation:** retain one child for the cached lifetime of a Workflow
  run and route every incremental activation for that run to the same child.

Starting a fresh process for an arbitrary incremental activation without first
coordinating Core eviction is invalid: the new child does not own the suspended
Workflow fiber expected by that activation. Zero-cache execution is a correct,
simple first implementation at the cost of replaying history for every task.
Both modes now use the same manager and protocol; Core cache configuration
selects the lifetime.

The pinned Core implementation gives the parent a recovery path. A failed
`WorkflowActivationCompletion` is handled as a Workflow Task failure and a
non-autocomplete language failure requests eviction with `LangFail`. The next
Workflow Task can then reconstruct language state from history. This behavior
is exercised by Core's `fail_wf_task` integration test and implemented in
`workflow_stream.rs`/`workflow/mod.rs` in the pinned `native/sdk-core` tree.

Therefore the sticky-mode boundary is:

```text
Temporal Server
      │
SDK Core + Odin Worker parent
      │  owns polling, task completion, routing, child lifecycle
      │
      ├── run A executor ── owns run A fiber and deterministic state
      ├── run B executor ── owns run B fiber and deterministic state
      └── ...
```

If an executor dies while processing an activation, the parent must submit a
failed completion for that run, consume Core's eviction activation, discard the
dead route, and let retry/replay create a fresh executor. The parent must never
invent or persist Workflow state itself.

## Versioned frame protocol

`temporal/workflow/isolate` now defines the first transport layer. It carries
opaque Core protobuf bytes rather than translating them into a second command
model.

Every frame has this big-endian header:

| Offset | Width | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `TOIS` |
| 4 | 2 | protocol version, currently `4` |
| 6 | 2 | message kind |
| 8 | 8 | request/correlation ID |
| 16 | 4 | run-ID byte length |
| 20 | 4 | payload byte length |

The body is the run ID followed by the payload. `Activation`,
`Completion_Metadata`, and `Completion` frames require both fields. Shutdown
frames require neither. The default
decoder rejects run IDs over 1 KiB, payloads over 32 MiB, unknown kinds,
unsupported versions, truncation, and trailing bytes before allocating or
dispatching runtime work. Binary payloads and embedded zero bytes are preserved.
`read` and `decode` return owned frames allocated by the supplied allocator;
callers release them with `owned_frame_free`. `write` only borrows its frame for
the synchronous call.

One writer owns each pipe direction. Concurrent writers must be serialized by
the future process-pool layer; the protocol does not rely on a frame fitting in
the platform's atomic pipe-write size.

## Executed transport spike

Build and run the process proof without a Temporal Server:

```bash
./scripts/build-examples.sh
TEMPORAL_ODIN_SKIP_BUILD=1 ./scripts/workflow-isolate-integration-test.sh
```

`examples/workflow_isolate` starts a child copy of itself with stdin/stdout
pipes. The parent sends an initialization activation. The child runs ordinary
sequential Odin Workflow code until `workflow.sleep`, returns a `StartTimer`
completion, and retains the suspended fiber. The same child then receives a
`FireTimer` activation and returns `CompleteWorkflowExecution`. A correlated
shutdown handshake proves clean framing and process teardown.

Unit tests cover exact binary roundtrip, pipe transport, corruption, truncation,
trailing data, limits, and invalid control frames. A 500-case property test
generates binary run IDs, payloads up to 8 KiB, both data message kinds, and
64-bit correlation IDs.

## Shared registry and executor host

Workflow procedure pointers cannot be serialized or transferred safely across
an `exec` boundary. The implemented boundary instead compiles one application
registration function into both processes and calls it independently:

```text
workflow_isolate_app.register
        │
        ├── parent Registry -> Workflow type-name manifest -> Core Worker
        └── child Registry  -> independent Fiber_Runtime callbacks -> Executor_Host
```

`workflow.Registry` owns cloned type names and process-local callback factories.
It supports string, generic Payload, repeated Payload, and typed JSON Workflow
adapters. `registry_instantiate` creates an independent runtime for one type;
the host owns that runtime after creation, so the source Registry can be closed.

The parent calls `worker.register_external_workflow_registry`. This copies only
the complete type-name manifest; those registrations contain no callback or
fiber runtime. `worker.run` requires an external activation processor, rejects
mixed in-process/external registrations, and rejects parent-side Workflow
logging or interceptors because those hooks must execute in the child where the
Workflow code runs. `Executor_Host_Options` installs a replay-aware logger and
ordered inbound/outbound interceptors into every child runtime before execution.
Its metric-emitter factory is called once per registered Workflow runtime with
that type's name, task queue, and namespace; each successful result transfers
one independently owned emitter to that runtime.
The option slices are borrowed only during initialization; hook records are
copied, while their `user_data` remains child-application-owned through host
close.

The child initializes `workflow_isolate.Executor_Host` from its independently
constructed Registry. On `InitializeWorkflow`, the host selects the registered
type, creates the run route, and sends the activation to that type's runtime.
Later activations use the run-ID route until `RemoveFromCache`, which destroys
the runtime instance and route. Unknown types become failed activation
completions; they are never dispatched to an arbitrary callback.

`examples/workflow_isolate_app` is the shared application package used by both
parent and child. The live proof runs two types through one executor binary: a
string Workflow and a typed-JSON Workflow. This proves the executable is an
application host rather than a child hard-coded to one callback.

Protocol version 2 adds a canonical registry-manifest handshake. The payload is
an ordered count followed by length-prefixed UTF-8 type names. With
`expected_registry` configured, `manager_verify_host` starts the ordinary child,
compares its manifest, and shuts it down before `worker.run` begins polling.
Every per-task child repeats the correlated handshake before receiving its first
activation, covering a replaced or inconsistent executable after preflight.
Malformed or mismatched manifests are protocol failures and the child is reaped.

Protocol version 3 adds one `Completion_Metadata` frame immediately before each
opaque Core `Completion`. Its payload is an internal JSON encoding of the
child's complete `workflow.Process_Result`, except that `completion` and
`run_id` are deliberately omitted: the following protobuf completion and the
correlated frame run ID remain authoritative. The parent decodes the metadata,
checks its activation-failure and terminal-command flags against the protobuf,
then supplies all language-runtime counters, wait state, cancellation detail,
and owned error text to the ordinary Worker observer/statistics path. A
metadata mismatch is a protocol failure; metadata can never change commands
submitted to Core.

Protocol version 4 permits zero or more correlated `Diagnostic` frames before
`Completion_Metadata`. The first payload byte is a stable discriminator
(`Application`, `Log`, or `Metric`); remaining bytes are codec-neutral and
borrowed by the parent observer only for its synchronous callback. Diagnostic
frames must match both request ID and run ID. They are never interpreted as
commands and cannot replace completion metadata or the Core protobuf.
The manager accepts at most 256 diagnostics per activation by default, and one
activation deadline covers the complete diagnostic/metadata/completion stream;
a child cannot keep resetting the deadline by sending observations.

`executor_host_emit_diagnostic` clones payloads onto the process heap even when
called from Workflow code with its arena allocator active. The host owns and
frees those clones after synchronous frame writes. Invalid events are dropped
and increment `diagnostics_dropped`; they cannot fail or change the Workflow
activation.

## Core-integrated cache proofs

The online Worker now has an experimental `workflow_activation_processor`
callback. It replaces only language-side activation dispatch: Core still owns
polling, history processing, Workflow state machines, completion submission,
retry, and eviction. The callback borrows the opaque activation bytes for one
serial invocation and returns an ordinarily owned `workflow.Process_Result`.
The existing in-process dispatcher remains the default, and offline replay
deliberately remains in-process.

`temporal/workflow/isolate.Manager` is the reusable per-run implementation of
that callback. Initialization copies both child command vectors, requires a
positive route capacity, and owns a dynamic run-ID table. Each accepted route
gets a monotonically increasing generation and its own process and pipe pair.
Capacity counts both live routes and failed routes awaiting eviction, so a
crash cannot free a slot and permit an unsafe replacement before Core confirms
`RemoveFromCache`. An optional first-generation command exists solely for
fault-injection and bootstrap tests; ordinary generations use the main command.
`activation_timeout` is zero for an unbounded wait or a positive monotonic
deadline. The bounded path reads on one joinable helper thread; on expiry it
kills the child, joins the reader after pipe EOF, reports exactly one activation
failure, and retains the failed route until eviction.

`shutdown_timeout` independently bounds both the correlated shutdown response
and the child's exit after acknowledgement. Expiry kills and reaps the process;
no reader thread or zombie is left behind. A real-process unit test starts a
child that never reads the request and proves the 20 ms deadline returns well
before that child would exit naturally.

Admission is deliberately non-waiting. Workflow activations are processed on
one serial Core poll thread, including the evictions that release capacity, so
waiting there for a slot would deadlock. When `maximum_executors` is full, the
manager returns a failed activation immediately and increments
`capacity_rejections`; Core remains responsible for eviction and retry.

An optional global consecutive-failure circuit breaker prevents unlimited
process respawn when the executable repeatedly exits, times out, or violates
the protocol. `maximum_consecutive_failures` opens the breaker and `cooldown`
admits one new trial after a monotonic deadline. Any successful activation
resets the streak. Rejected activations start no process. This is containment,
not a per-Workflow-type error budget: healthy activations on the same manager
reset the global consecutive count.

Run the complete proof with no pre-existing infrastructure:

```bash
./scripts/workflow-isolate-core-integration-test.sh
```

The script uses `temporal server start-dev --headless` when port 7233 is not
already healthy. It does not use Docker or an external database. The example
creates a Worker with `max_cached_workflows = 0`, registers the shared two-type
manifest, and then:

1. generation 1 exits with status 86 before answering its first activation;
2. the parent turns EOF into a failed `WorkflowActivationCompletion` rather
   than crashing the Worker;
3. Core emits `RemoveFromCache`, which the parent acknowledges even though the
   failed child is already gone;
4. later generations receive replay history, suspend at durable timers, answer
   successfully, consume their zero-cache evictions, and exit cleanly;
5. fresh generations replay history, receive timer resolutions, complete both
   Workflows, consume eviction, and exit cleanly; and
6. the public Odin Client obtains the exact string result
   `"Hello from an isolated Workflow, Odin"` and decodes the typed JSON result
   `{"message":"Goodbye from the shared registry, Odin","code":42}` from the
   Temporal Service.

The parent observer also requires a non-zero child-reported `timer_seq` for
every `StartTimer`. That sequence is not reconstructed from the Core completion
by the parent, so this is live evidence that v3 observation metadata crosses
the process boundary. The zero-cache path observes four such commands because
both timer schedules are replayed; sticky mode observes two.

The example host also installs an inbound execution interceptor which emits one
Workflow log and one counter metric. The logger and example metric emitter write
to inherited child
`stderr`; framed protocol data has exclusive ownership of child `stdout`. Both
zero-cache and sticky live paths require exactly two log and two metric records.
The two replay initializations in zero-cache emit none because Workflow logging
and metrics suppress history replay by default. This proves that hooks run in
the executor and observability survives the process boundary without changing
commands.
They also enqueue discriminated log/metric diagnostics. The manager's borrowed
observer requires exactly two of each kind and `diagnostics_received=4` in both
cache modes, proving structured child observations reach the parent separately
from completion metadata.

The live assertion requires one activation failure, one child exit, two
completed Workflows, no owned route at return, both exact results, and no
capacity, shutdown, crash-loop, or protocol failures. The observed
CLI-dev-server path currently has
five generations, five evictions, five graceful shutdowns (including preflight),
six successful manifest handshakes, and one aborted crashed child. Those exact
counts are diagnostic rather than an API promise;
Core may legitimately change activation batching.

The same script accepts
`TEMPORAL_ODIN_ISOLATE_FIRST_MODE=hang`. That child deliberately stops before
answering its first activation. With a 250 ms activation deadline, the live
path observes exactly one timeout, kills and reaps the child, submits the failed
completion, consumes eviction, replays in later generations, and returns the
same durable result. No pipe-reader thread remains at shutdown.

Run the protocol-fault matrix with:

```bash
TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/workflow-isolate-protocol-fault-integration-test.sh
```

Against the CLI dev server, the first executor generation independently injects
five faults: a stale request ID, invalid metadata JSON, invalid completion
protobuf, metadata whose terminal flags disagree with the completion, and a
response delayed beyond the activation deadline. Each protocol fault increments
exactly one protocol failure; the delayed response increments exactly one
timeout. The parent aborts and reaps that generation, submits one failed
activation completion, waits for Core eviction, and completes both Workflows in
fresh generations. Private pipes and generation-owned routes prevent a stale or
late response from being consumed by the replacement.

Set `TEMPORAL_ODIN_ISOLATE_CACHE_MODE=sticky` to use
`max_cached_workflows=100`. After one deliberately failed generation, the two
Workflow runs each keep one child across timer suspension and resolution. The
live path observes three generations and three evictions instead of the
zero-cache path's five, while still processing both timer resolutions as
incremental activations and returning the same exact results. The crash and hang
variants both pass. This proves the invariant in the non-zero-cache direction:
incremental activations remain routed to the generation that owns the suspended
fiber until Core explicitly evicts it.

This is evidence for the central recovery contract: the parent can survive a
language executor crash, fail only that Workflow Task, and let Core/Server
history drive a fresh Odin runtime. Temporal remains the durable state owner.

### Real nondeterminism across the process boundary

Run the incompatible-replay proof with:

```bash
TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/workflow-isolate-nondeterminism-integration-test.sh
```

The seed Worker runs the ordinary executor until Core and the CLI dev server
have recorded a one-hour Timer, then stops and reaps its child. A second Worker uses
the same Workflow type manifest but starts one deliberately incompatible child
whose implementation completes immediately instead of scheduling that Timer.
The activation is a real replay activation built from server history; no Core
protobuf bytes are forged by the test.

The replay Worker uses a sticky Core cache so the semantic eviction reason is
observable. Core rejects the incompatible command with
`RemoveFromCache(NONDETERMINISM)`, and the manager records exactly one
`nondeterminism_evictions` event. The type-scoped Core policy converts that
mismatch into a terminal Workflow failure. The test independently requires
`WORKFLOW_EXECUTION_STATUS_FAILED` and `[TMPRL1100]`/nondeterminism evidence in
the server history. With a zero-sized cache, Core may emit the cache-lifecycle
eviction first, so that mode is deliberately not used for this diagnostic.

### Guard-page stack overflow containment

Run an actual overflowing Workflow fiber through the same boundary with:

```bash
TEMPORAL_ODIN_ISOLATE_FIRST_MODE=stack-overflow \
  ./scripts/workflow-isolate-core-integration-test.sh
```

The first child uses the ordinary registry manifest but a Workflow callback
that recursively consumes its fixed minicoro stack. The native guard page
terminates only that executor with `SIGSEGV` or `SIGBUS`. The manager retains
the OS exit code, reports one failed activation, consumes Core's language-fail
eviction, and starts no replacement until that eviction arrives. Ordinary code
in fresh generations then replays the server history and both Workflows finish
with their exact expected results. The preexisting native guard test separately
requires the same signals, so this is not simulated with `exit(86)`.

Child stderr contains the logger/metric hook emitted before the fatal signal,
but its queued structured diagnostic frames cannot be delivered after the
process dies. The live assertions intentionally distinguish those two paths.

### Ownership at the experimental callback

- Core's poll result owns the activation bytes. `worker.run` borrows them to the
  callback and frees them immediately after it returns.
- Route strings decoded from an activation also borrow those bytes. The manager
  clones a run ID for every owned route and separately keeps a reporting
  copy alive until `worker.run` has consumed the returned result.
- The child protocol decoder returns owned run-ID/payload buffers; callers use
  `owned_frame_free`.
- The callback returns an owned completion buffer. `worker.run` submits it to
  Core and deletes it after the synchronous bridge call returns.
- The child metadata encoder borrows `Process_Result` only for synchronous JSON
  encoding. The parent decoder owns its error bytes and cancellation-reason
  string. The returned Worker result owns the error bytes; the manager retains
  the cancellation-reason allocation until the next serial activation or
  `manager_close`, matching the callback's borrowed-field lifetime.
- Replay flag, history length, eviction status, and reported run ID continue to
  come from the parent-decoded activation/route, not child metadata.
- Every started child is reaped. Normal eviction uses a correlated shutdown
  handshake; EOF/protocol failure closes pipes, kills only when necessary, and
  waits for the process.

### Lifecycle model and generated traces

The implementation exposes and uses one pure lifecycle transition function:

```text
Absent + Initialize       -> Live(g), StartExecutor
Live + Success            -> Live(g), CompleteActivation
Live + Failure/Initialize -> Failed(g), FailAndAbort
Failed + non-eviction     -> Failed(g), FailAwaitingEviction
Live/Failed + Eviction    -> Absent, shutdown-or-ack
```

A `statecharts` model independently declares the same transition graph. A
1,000-case property test feeds up to 64 generated events through both and
requires identical state and effects after every event. A second 1,000-case
property generates up to 128 events across eight run slots and capacities from
one through eight. It requires route count never to exceed capacity, every
owned route to have a generation, and a failed generation never to change
before eviction. A third 1,000-case property varies breaker threshold, failure
count, cooldown, and elapsed time; it requires rejection before cooldown and
exactly one reset/admission decision at or after the boundary. The existing
500-case framing property remains independent. A process-backed unit test then
proves two real child exits open the breaker and the third initialization starts
no process.

Generation fencing currently comes from route ownership as well as the model:
each generation has private pipes and monotonically correlated request IDs.
Closing and reaping an old session removes the only handles through which its
response could be read; a new generation never reuses those pipes.

## Not implemented yet

This slice deliberately does not claim production isolation. The following are
still required:

- validate macOS/Linux arm64/x86_64 pipe and process behavior.

The next discriminating vertical slice is cross-platform pipe/process
validation and packaging of the executor binary on the four supported target
triples. Production isolation still needs the same containment proof for Odin
panics and other fatal native child exits.

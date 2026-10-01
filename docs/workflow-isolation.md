# Workflow process isolation

Status: experimental zero-cache process boundary, connected to `worker.run`
and live-proven against the Temporal CLI dev server. It is not yet a production
executor pool.

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
Sticky per-run children are the later performance mode.

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
| 4 | 2 | protocol version, currently `2` |
| 6 | 2 | message kind |
| 8 | 8 | request/correlation ID |
| 16 | 4 | run-ID byte length |
| 20 | 4 | payload byte length |

The body is the run ID followed by the payload. `Activation` and `Completion`
frames require both fields. Shutdown frames require neither. The default
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
Workflow code runs.

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

## Core-integrated zero-cache proof

The online Worker now has an experimental `workflow_activation_processor`
callback. It replaces only language-side activation dispatch: Core still owns
polling, history processing, Workflow state machines, completion submission,
retry, and eviction. The callback borrows the opaque activation bytes for one
serial invocation and returns an ordinarily owned `workflow.Process_Result`.
The existing in-process dispatcher remains the default, and offline replay
deliberately remains in-process.

`temporal/workflow/isolate.Manager` is the reusable zero-cache implementation
of that callback. Initialization copies both child command vectors, requires a
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

This is evidence for the central recovery contract: the parent can survive a
language executor crash, fail only that Workflow Task, and let Core/Server
history drive a fresh Odin runtime. Temporal remains the durable state owner.

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

- configure replay-aware logs, metrics, interceptors, and diagnostics in the
  executor host and carry observations back without letting them affect
  commands;
- preserve the complete `Process_Result` observation metadata across the
  boundary (the current parent reconstructs only terminal commands, timer
  starts, replay, history length, activation failure, and eviction);
- prove nondeterminism and malformed/late child completions against
  `temporal server start-dev`;
- validate macOS/Linux arm64/x86_64 pipe and process behavior.

The next discriminating vertical slice is sticky per-run routing with a non-zero
Core cache. It can reuse the manager, registry handshake, bounded shutdown, and
wire protocol, but must prove that every incremental activation stays on the
same generation until Core eviction. Executor-host observability should follow
without changing command determinism.

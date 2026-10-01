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
| 4 | 2 | protocol version, currently `1` |
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

## Core-integrated zero-cache proof

The online Worker now has an experimental `workflow_activation_processor`
callback. It replaces only language-side activation dispatch: Core still owns
polling, history processing, Workflow state machines, completion submission,
retry, and eviction. The callback borrows the opaque activation bytes for one
serial invocation and returns an ordinarily owned `workflow.Process_Result`.
The existing in-process dispatcher remains the default, and offline replay
deliberately remains in-process.

Run the complete proof with no pre-existing infrastructure:

```bash
./scripts/workflow-isolate-core-integration-test.sh
```

The script uses `temporal server start-dev --headless` when port 7233 is not
already healthy. It does not use Docker or an external database. The example
creates a Worker with `max_cached_workflows = 0` and one Workflow poller, then:

1. generation 1 exits with status 86 before answering its first activation;
2. the parent turns EOF into a failed `WorkflowActivationCompletion` rather
   than crashing the Worker;
3. Core emits `RemoveFromCache`, which the parent acknowledges even though the
   failed child is already gone;
4. generation 2 receives replay history, suspends at a durable timer, answers
   successfully, consumes its zero-cache eviction, and exits cleanly;
5. generation 3 replays history, receives the timer resolution, completes the
   Workflow, consumes eviction, and exits cleanly; and
6. the public Odin Client obtains `"Hello from an isolated Workflow, Odin"`
   from the Temporal Service.

The live assertion requires one activation failure, one child crash, one
completed Workflow, no live child at return, and at least two child
generations. The observed CLI-dev-server path currently has exactly three
generations and three evictions. Those exact counts are diagnostic rather than
an API promise; Core may legitimately change activation batching.

This is evidence for the central recovery contract: the parent can survive a
language executor crash, fail only that Workflow Task, and let Core/Server
history drive a fresh Odin runtime. Temporal remains the durable state owner.

### Ownership at the experimental callback

- Core's poll result owns the activation bytes. `worker.run` borrows them to the
  callback and frees them immediately after it returns.
- Route strings decoded from an activation also borrow those bytes. The example
  clones a run ID for every live child route and separately keeps a reporting
  copy alive until `worker.run` has consumed the returned result.
- The child protocol decoder returns owned run-ID/payload buffers; callers use
  `owned_frame_free`.
- The callback returns an owned completion buffer. `worker.run` submits it to
  Core and deletes it after the synchronous bridge call returns.
- Every started child is reaped. Normal eviction uses a correlated shutdown
  handshake; EOF/protocol failure closes pipes, kills only when necessary, and
  waits for the process.

## Not implemented yet

This slice deliberately does not claim production isolation. The following are
still required:

- move the single-session example into a reusable bounded process-pool package
  with a run-ID/generation route table;
- transfer Workflow registration metadata to independently launched executors
  instead of compiling a dedicated child executable for this one Workflow;
- reject late responses from an old generation before they can complete a newer
  activation;
- bound child count, queueing, activation time, shutdown, and crash-loop rate;
- carry replay-aware logs, metrics, interceptors, and diagnostics across the
  process boundary without letting them affect commands;
- preserve the complete `Process_Result` observation metadata across the
  boundary (the current parent reconstructs only terminal commands, timer
  starts, replay, history length, activation failure, and eviction);
- prove nondeterminism and malformed/late child completions against
  `temporal server start-dev`;
- validate macOS/Linux arm64/x86_64 pipe and process behavior.

The next discriminating vertical slice is the reusable bounded zero-cache
executor manager: extract lifecycle/routing from the example, add activation
deadlines and generation fencing, and model crash/eviction/shutdown traces with
the pinned PBT/statecharts dependencies. Sticky per-run routing should follow
only after that manager is correct; it can reuse the wire protocol while
changing child lifetime to match Core's non-zero cache.

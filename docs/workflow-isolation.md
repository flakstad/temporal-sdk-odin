# Workflow process isolation

Status: experimental process-boundary spike; not connected to `worker.run`.

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

## Executed spike

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

## Not implemented yet

This spike deliberately does not claim production isolation. The following are
still required:

- separate the Workflow registry/runtime from the Core-owning `Worker` so the
  same application registrations can initialize executor children;
- route each polled activation to exactly one live executor for its run;
- turn child exit/protocol failure into a failed Core completion, then consume
  and acknowledge the resulting eviction before replacing the route;
- destroy a child after `RemoveFromCache` and prevent late responses from an old
  generation from completing a newer activation;
- bound child count, queueing, activation time, shutdown, and crash-loop rate;
- carry replay-aware logs, metrics, interceptors, and diagnostics across the
  process boundary without letting them affect commands;
- prove normal execution, forced child crash, replay, and nondeterminism against
  `temporal server start-dev`;
- validate macOS/Linux arm64/x86_64 pipe and process behavior.

The next discriminating vertical slice is a real Core-polled Workflow in
zero-cache mode: route one Workflow Task through a child, acknowledge eviction,
then deliberately kill a later child and require failed completion, eviction,
fresh-process history replay, and successful completion. Sticky per-run routing
can then reuse the same protocol without making the first safety proof depend
on a concurrent process table.

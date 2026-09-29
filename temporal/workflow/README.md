# Workflow package (research only)

`spike.odin` is a bounded explicit state machine for one Workflow shape:

```text
initialize -> schedule hello Activity -> resolve Activity -> complete
```

`fiber_spike.odin` drives serial commands, multiple outstanding Activities,
handled Activity failures, buffered signals, a durable signal/timer race, and
Activity-awaiting Workflow Update routines through stackful coroutines. It also
implements Core's `NotifyHasPatch`/`SetPatchMarker` replay protocol through
`patched` and `deprecate_patch`. Workflow time, history length, replay status,
deterministic random values, and UUID v4 values come only from activation
metadata and Core-provided seeds. The
current live Workflow includes:

```odin
hello_workflow :: proc(ctx: ^workflow.Fiber_Context, name: string) -> string {
    approval := workflow.await_signal_or_timeout(ctx, "approval", 30 * time.Second)
    if approval.status == .Timed_Out {
        return "Approval timed out"
    }
    return workflow.execute_activity(ctx, "hello", approval.value)
}
```

The C stack is only a cache. Eviction destroys it; replay starts the procedure
again on a new stack and must reproduce the same commands from history. A
per-instance Odin arena owns values that cross activation boundaries.

Accepted Update handlers use independent fiber stacks and wait state while
sharing the Workflow's deterministic command sequence and operation tables.
This permits `Accepted` plus an Activity command in one completion and the
terminal Update response after a later Activity resolution. Replay recreates
both stacks and requires byte-identical completions.

Signal and Update routines may overlap at Temporal suspension points, so shared
Workflow state sometimes needs explicit coordination. `Mutex` is a zero-value,
Workflow-local FIFO mutex for that purpose:

```odin
state: struct {
	mutex: workflow.Mutex,
	value: int,
}

if !workflow.mutex_lock(ctx, &state.mutex) {
	return // the selected cancellation token was requested while waiting
}
defer workflow.mutex_unlock(ctx, &state.mutex)
state.value += 1
```

The mutex suspends only the current Workflow fiber; it never blocks an OS
thread and emits no Temporal command. A waiter uses the cancellation token
selected when it starts waiting, and ownership passes to non-canceled waiters
in arrival order. `mutex_try_lock` never jumps ahead of queued waiters, and
`mutex_is_locked` is only an observation helper. Mutexes are bound to one
Workflow run on first use: keep them in Workflow-owned state, do not copy them
after first use, and never retain them across eviction. Reentrant locking,
cross-run use, non-owner unlock, or returning while still owning a mutex becomes
a deterministic execution failure. In the main or a Signal routine that fails
the Workflow; in an Update routine it rejects that Update. Use
`defer mutex_unlock` after every successful acquisition. Host synchronization
primitives remain invalid inside Workflow code because they are not replay-aware.

`await_condition` waits for a deterministic predicate over Workflow-owned
state without polling wall time or emitting a Temporal command:

```odin
if !workflow.await_condition(
	ctx,
	&state,
	proc(state: ^Order_State) -> bool {
		return state.approved || state.rejected
	},
) {
	return .Canceled
}
```

The predicate is evaluated immediately and again after activation jobs or other
ready handlers may have changed state. It must be deterministic,
side-effect-free, non-blocking, and must not retain the borrowed state pointer.
The wait inherits the cancellation token selected by the current cancellation
scope and returns `false` when that token is requested. If the predicate becomes
true in the same scheduler pass as cancellation, the condition wins, matching
the pinned Rust runtime. Conditions are local scheduler state: eviction drops
the suspended stack and replay reconstructs the wait from history.

`workflow.Stack_Options` controls Workflow, Signal-handler, and Update-handler
stack sizes independently when the Worker is created. Zero selects the 128 KiB
default; explicit sizes are validated between 32 KiB and 16 MiB. Replay Workers
accept the same policy. Minicoro allocates the full fixed stack for every live
fiber, so larger handler stacks directly multiply concurrent-handler memory.

Both implementations exist to test Core activations, commands, ownership,
forced eviction, and replay. The fiber version has seq-keyed Activity/timer
operation tables, emits multiple commands in one completion, and applies every
resolution job before resuming Update routines and the main Workflow fiber in a
stable order. Activity failures are
arena-owned resolutions: `execute_activity_result` makes them handleable, while
the simpler `execute_activity` propagates an unhandled failure to Temporal.
Named string signals are queued in arrival order and do not resume a fiber that
is waiting on another operation. `await_signal_or_timeout` schedules a durable
timer and emits `CancelTimer` only when a signal wins before the timer fires.
Its result retains the same borrowed Signal metadata as the non-timeout await.
`await_signal_payload_with_context` and `await_signal_json_with_context`
preserve Core's Signal identity, headers, and originating history event ID;
these are borrowed Workflow-instance views. Synchronous handlers can request
the same metadata with `set_signal_handler_json_with_context`. Header helpers
remain codec-neutral with an optional typed JSON adapter.
`set_validated_update_routine_json_with_context` similarly exposes an
arena-owned `Update_Context` with Update ID/name, client identity, and headers
to both validation and the stackful handler. The same context remains available
through `current_update` after suspension and resume; it must not escape the
Workflow run.
The shared owned `Headers` builder can also be attached to remote/Local
Activity options, Child Workflow options, and child/external Signal options.
Each operation clones it into the Workflow run arena before suspension so the
caller may free the builder after the scheduling call returns.
Continue-As-New options accept the same builder; its terminal call never
returns, and the run arena releases its working copy when Core evicts the old
run.
`temporal/worker` now provides experimental name-based Workflow registration
and dispatch, but neither the fiber API nor its one-string codec is stable or a
complete Workflow runtime. Do not build production Workflows against them.

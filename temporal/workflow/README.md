# Workflow package (research only)

`spike.odin` is a bounded explicit state machine for one Workflow shape:

```text
initialize -> schedule hello Activity -> resolve Activity -> complete
```

`fiber_spike.odin` drives serial commands, multiple outstanding Activities,
handled Activity failures, buffered signals, a durable signal/timer race, and
Activity-awaiting Workflow Update routines through stackful coroutines. It also
implements Core's `NotifyHasPatch`/`SetPatchMarker` replay protocol through
`patched` and `deprecate_patch`. Workflow time, history length/size, replay and
history-replay status, Continue-As-New suggestions, the current Worker
Deployment Version, deterministic random values, and UUID v4 values come only
from activation metadata and Core-provided seeds. `set_current_details` updates
the response to Temporal's reserved Workflow metadata Query without emitting a
command. The
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

`start_timer` exposes the same durable timer operation used by `sleep` as a
`Timer_Future`. It can be awaited, canceled, or combined with other operations
in a declaration-biased selector:

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
	return workflow.await_activity(ctx, activity_future)
}
return "timed out"
```

The selector supports Activity and Timer futures, named Signals, and Child
Workflow start/completion futures. Every activation job is applied before the
scheduler chooses the first ready case, so declaration order—not Core job
order—is the deterministic tie-breaker. Cancellation is considered only after
ready cases. Losing operations are left live; callers must explicitly await or
cancel them. The case slice and its Signal names are borrowed until selection
returns. Selected Signal values and metadata are borrowed run-arena views.

Direct `start_timer` traverses the outbound `start_timer` interceptor without
suspending. Use `start_timer_with_options` to select a specific Workflow
cancellation token or attach a UI-facing summary:

```odin
timer := workflow.start_timer_with_options(ctx, {
	duration = 30 * time.Second,
	cancellation_token = timeout_token,
	summary = "Wait for approval",
})
```

A zero token inherits the current fiber token. Empty summary means absent; a
non-empty value is copied into Workflow-run ownership and serialized as
`json/plain` command `UserMetadata.summary`. The interceptor input exposes the
complete mutable options record and its continuation returns the created
`Timer_Future`, so it can deterministically replace options or the Future:

```odin
timer_interceptor :: proc(
	_: rawptr,
	input: ^workflow.Outbound_Start_Timer_Input,
	next: workflow.Outbound_Start_Timer_Next,
) -> workflow.Timer_Future {
	input.options.duration *= 2
	input.options.summary = "intercepted timer"
	return workflow.outbound_start_timer_next(next, input)
}
```

`sleep_with_options(ctx, options)` exposes the same record for sequential code;
plain `sleep(ctx, duration)` remains the compact convenience.
`sleep` has a separate stackful interceptor because it models the complete
suspending call. It traverses the sleep chain first and the timer-start chain
when its terminal continuation creates the timer. Code after timer `next`
runs immediately after Future creation; code after sleep `next` runs only when
the wait resolves.

`join_all` waits for every already-started durable operation in a borrowed case
slice. It allocates no result collection: after `.Ready`, the existing typed
await operations retrieve their values synchronously in caller-controlled
order.

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

Activity, Timer, Child Workflow start, and Child Workflow completion cases are
supported. Exact duplicate cases are invalid. Signal cases are deliberately
rejected until join can return consumed Signal payloads with explicit ownership.
Cancellation is checked after the all-ready condition, and unfinished
operations remain owned by the Workflow when join returns `.Canceled`.
Selector/join composition over Child start and completion futures, including an
explicitly canceled child and partial readiness, is covered by a 300-trace
fresh-stack replay property.

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

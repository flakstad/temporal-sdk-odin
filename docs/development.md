# Development

## Toolchain and pinned dependency

The repository pins `temporalio/sdk-rust` as the `native/sdk-core` submodule at
commit `e163abd6dc19040064986a63c8b8cf4756ffd320`. This immutable upstream-main
snapshot was inspected on 2026-09-30 and is also the Core revision pinned by
`sdk-python` commit `5bb461265e6c1af29c5f25dbb871a7e67448c61e`. It contains
`sdk-core` crate 0.9.0 and client/common crates 1.0.0 plus ten commits after the
`core-v0.9.0` release baseline.
The same commit is recorded in `native/sdk-core-revision.txt` so source archives
without Git metadata retain a machine-readable native dependency identity.

Property tests pin `flakstad/pbt` at
`3b465330bd3a11c45990ef2d7f44f1cf25a18145` as the `test-deps/pbt`
submodule. It is not linked into SDK or application artifacts.
They also pin `flakstad/statecharts` at
`9e82885f0ae4669061aa5505b30db43537e83d19` as
`test-deps/statecharts`. Both dependencies remain test-only.

The Workflow logging property generates 500 combinations of Core replay state,
history-event presence, explicit replay-log opt-in, all five log levels, and
typed integer/float/boolean fields. It requires the suppression decision to
match the activation classification and validates every callback-scoped field.
The focused unit test additionally evicts the run, replays on a fresh fiber,
and requires the completion bytes to remain identical with logging disabled,
suppressed, and explicitly enabled. This guards the stronger invariant that
observability cannot change Workflow commands.

The Workflow metric property generates another 500 combinations across all
seven instrument kinds, fresh execution, history replay, query-only replay,
invalid names/attributes, and negative histogram values. It requires exactly
the non-history, valid records to reach the host emitter and checks that
activation identity is attached without becoming visible to Workflow control
flow.

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

### Core upgrade record: 2026-09-30

The current pin advances ten upstream commits from release baseline
`399cb7adb40f45eefbef90149e212743e474142c` to
`e163abd6dc19040064986a63c8b8cf4756ffd320`. The latter was `sdk-rust/main`
when selected and is also consumed by the inspected `sdk-python` revision.
The comparison produced these concrete results:

- the generated C header has the same SHA-256 at both revisions,
  `d0f7f6a53c7d8dd0e55ce1006b3fb2bc69ba0ee7b2328f714d4b0b3ba6ee2b25`,
  so no Odin raw binding, size, offset, symbol, or ownership contract changed;
- the generated Odin protobuf graph grows additively with Activity failure
  causes, execution identity, `Callback.NexusHandler`, `Link.Callback`, and
  related callback/request metadata;
- the wrapper lockfile records upstream's `wit-bindgen` 0.61.1 update;
- Core fixes transient poll-target reduction, non-sticky polling under sticky
  saturation, repeated oversized Workflow-task failure reporting, and Local
  Activity resolution ordering during replay.

The repository generator tests, three Rust generator tests, 179 Odin unit
tests, and 57 property tests pass on the new pin. The complete CLI-devserver
integration suite also exits successfully, covering Core lifecycle, Client
RPCs, Nexus, Activity and Workflow workers, cache/eviction, Local Activities,
Child Workflows, updates/signals/queries, deployment versioning, and offline
replay. The `macos-arm64` native archive builds with the new revision in its
manifest and passes the packaged ABI init/shutdown smoke test. This is therefore
a tested pin upgrade, not schema-only adoption.

The upgrade exposed and fixed a build-integrity bug in `build-core.sh`. Its old
check compared the revision file with the superproject's committed `HEAD`, even
when a different indexed and checked-out submodule was being compiled. It now
requires the revision file, indexed gitlink, and actual submodule `HEAD` to be
identical before Cargo runs.

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

## Top-level lifecycle facade

`temporal/temporal.odin` exposes a deliberately small convenience surface for
Client connection, Worker creation, registration, running, shutdown, and
cleanup. `worker_create` inherits the connected Client's namespace;
`worker_create_with_options` does the same only when `options.namespace` is
empty. Both paths still copy Worker configuration into Worker ownership.

The facade aliases the underlying concrete Client/Worker types and their
distinct error types. It does not wrap handles in another allocation or merge
error ownership. Advanced Client operations and deterministic Workflow
primitives remain under `temporal/client` and `temporal/workflow`.

Build the compile-checked facade example with all other examples, or run its
one-task Activity Worker against the CLI dev server:

```bash
./scripts/build-examples.sh
./native/target/release/temporal-odin-sdk-hello-activity
```

## Native packages and platform CI

Create the package for the current host with:

```bash
./scripts/package-native.sh
```

This builds Core unless `TEMPORAL_ODIN_SKIP_BUILD=1`, then writes a target-named
archive and archive checksum under `dist/`. The archive contains the Core
shared library, static fiber shim, exact generated C header, license texts,
linking notes, and a manifest with the Core revision, crate versions, build
toolchains, and per-file SHA-256 checksums. It intentionally does not duplicate
the Odin source packages: generated protobuf code, raw bindings, and this
native artifact must come from the same SDK repository revision.

Verify both archive structure and the packaged ABI with:

```bash
./scripts/verify-native-package.sh \
  dist/temporal-sdk-odin-native-macos-arm64.tar.gz --smoke
```

Use the archive name for the current host. Smoke mode links only the runtime
init/shutdown test against the unpacked archive, rather than the build-tree
libraries. Verification rejects target mismatch, unsafe archive paths, missing
files, checksum changes, a Core commit different from
`native/sdk-core-revision.txt`, and—when the submodule is present—a mismatched C
header.

Install the verified package and consume it without compiling Rust Core:

```bash
native_lib_dir="$(./scripts/install-native-package.sh \
  dist/temporal-sdk-odin-native-macos-arm64.tar.gz)"
TEMPORAL_ODIN_NATIVE_LIB_DIR="$native_lib_dir" \
  ./scripts/build-examples.sh
```

The default immutable location is
`native/prebuilt/<sdk-core-commit>/<target>/`; it is ignored by Git. Pass an
explicit second argument to select another install root. The installer is
idempotent for byte-identical content and refuses to overwrite an existing
partial or different package. `scripts/native-package-integration-test.sh`
proves install/reinstall, the full unit/property suites, all example links, and
rejection of a deliberately wrong Core pin. It also mutates or mismatches a
packaged library to prove that the loader-policy gate fails closed. CI runs it
for every target.

Every verified package is inspected by
`scripts/verify-native-link-policy.sh`. On macOS, both dynamic bridges must be
thin for the declared architecture, identify themselves through their exact
`@rpath` names, contain no `LC_RPATH`, retain valid linker signatures, and
depend only on Apple system libraries/frameworks. On Linux, both must have the
declared ELF machine and exact SONAME, contain no RPATH/RUNPATH, and use only
the allowlisted glibc-era system libraries. The single real object in the
static fiber archive must match the same architecture on both platforms. The
application—not the artifact—chooses its runtime search path.

`.github/workflows/ci.yml` runs the full unit/property suite, builds all
examples, packages, and smoke-tests on macOS arm64/x86_64 and Linux glibc
arm64/x86_64. Actions are commit-pinned; Odin `dev-2026-09`, Rust 1.94, and
protoc 36.0 are explicit inputs. The build scripts also accept a complete
source archive without `.git`, while a Git checkout verifies
`native/sdk-core-revision.txt` against its sdk-core gitlink before compiling.
Before publishing these artifacts as releases, generate and audit third-party
license notices for Core's complete Rust dependency graph and decide the macOS
code-signing/notarization policy. The current archives are CI/development
artifacts, not a completed binary-release pipeline.

## Protobuf generation

The C bridge exchanges Core messages as protobuf bytes. The repository-owned
generator reads a descriptor set produced from the exact schemas in the pinned
`native/sdk-core` submodule:

```bash
./scripts/generate-protos.sh
```

This currently generates the transitive type graph rooted at the Core Activity
task/completion, Nexus task/completion, and Workflow activation/completion
messages into `temporal/core/proto/generated.odin`. The generated file is
committed. After a Core pin or root-list change, regenerate it and review the
diff; do not edit it by hand. CI/local tests use:

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

Before the Odin tests, the script runs `scripts/fiber-guard-test.sh`. It first
proves normal guarded-fiber create/resume/destroy, then forks an isolated child,
fills a minimum-size stack in 1 KiB frames, and requires `SIGSEGV` or `SIGBUS`
at the inaccessible page. This is intentionally a subprocess test: stack
overflow is a fatal process boundary, not an Odin error that can safely unwind
through the C coroutine trampoline. The bridge reports guard-page and reserved
mapping sizes through diagnostic-only raw symbols, and an Odin unit test checks
the same lifecycle through the actual binding.

The fast suite also creates Core's dedicated offline replay worker through the
C ABI. It replays a complete synthetic `temporal.api.history.v1.History`, then
runs an incompatible Workflow definition against the same history and requires
Core's `NONDETERMINISM` eviction with a `TMPRL1100` diagnostic. A malformed
History verifies immediate Core error ownership and shutdown. A batch test then
feeds compatible, nondeterministic, malformed, and compatible histories through
one Core worker, requiring ordered per-history outcomes and successful recovery.
These tests use a real Core runtime but no Temporal Service.

The script runs the repository tests with Odin vetting, strict style, and
warnings promoted to errors. It then runs a separate property suite from
`tests_pbt` against the pinned PBT and statechart submodules. That suite generates
500 signal/timer/cancellation activation batches across nine ordering classes,
forces eviction, reconstructs a fresh fiber, and requires the decisive
`WorkflowActivationCompletion` bytes to match replay exactly. Separate
500-case properties cover Child and external Workflow commands and exact
fresh-stack replay. Another 500 cases vary Continue-As-New identity, timeouts,
Retry Policy, backoff, zero to twelve ordered Payloads, memo/Search Attribute
presence, all typed Search Attribute metadata variants, and both versioning
enum families with exact replay. A patch property
generates another 500 replay/notified/deprecated combinations, checks Core's
truth table, and requires duplicate calls to emit at most one marker. Another
500 cases generate all three Worker versioning strategies and require exact
preservation of deployment/build identity, routing intent, and default behavior
across the Odin-to-C ABI mapping. Another 500 generate global and per-type
nondeterminism policies containing zero to twelve Workflow types and require
exact C-array preservation. Another
500 activation contexts vary initial/reset seeds, timestamps, history lengths,
and non-power-of-two range/index bounds. They require exact shared-stream and
independent-stream SplitMix64 values, unbiased rejection-sampling results, UUID
shape, and byte-identical fresh-stack replay.
Another 500 generated stack policies cover default, minimum, aligned custom,
below-minimum, and above-maximum sizes independently for Workflow, Signal, and
Update fibers. Unit tests inspect the actual native coroutine sizes, not only
the Odin configuration records. Another 500 generated late-handler traces put
one to twelve matching Signals behind one to four unknown names, then register
the handler and require arrival-ordered dispatch plus byte-identical replay.
The Workflow-mutex property generates all six arrival permutations for three
accepted Update waiters behind a Signal routine that suspends while owning the
lock. It requires FIFO handoff and byte-identical initialize, queue, handoff,
and terminal completions after eviction and fresh-stack replay. A focused unit
test separately cancels a queued waiter through its selected Workflow
cancellation token and proves that the owner remains live and can release the
lock normally.
The Workflow-condition property varies four threshold partitions and both
Signal/Update waiter-start orders. Main, Signal, and Update fibers suspend on
typed state predicates; each pre-threshold activation must leave the waiters
blocked, the threshold activation must resume both handlers in the deterministic
fixed point, and eviction/fresh-stack replay must reproduce every completion
byte-for-byte. A focused unit test separately proves cancellation and the
upstream-compatible rule that a true predicate wins over cancellation observed
in the same scheduler pass.
The Workflow-selector property crosses all six declaration orders for two
Activities and one Timer with all six Core activation-job orders. Across 180
generated traces the first declared ready case must win, and initialize plus
terminal completions must be byte-identical after eviction and fresh-stack
replay.
The Workflow-join property generates all six resolution orders for two
Activities and one Timer, split after either one or two early jobs. Every early
activation must retain `waiting_join=true`; only the final operation may resume
the Workflow. Result order and all three completions must survive fresh-stack
replay byte-for-byte across 180 traces.
A separate 180-trace Signal-join property varies both case and delivery orders
plus one/two-activation delivery. `join_all_into` must consume each unique
Signal into the matching caller-provided slot and reproduce every completion
byte-for-byte after eviction. A focused negative test requires duplicate Signal
names to fail as `OdinWorkflowJoinError`.
A second 180-trace property mixes one Signal with one Activity and varies their
case order, Core job order, and activation split. A deadline-driven unit trace
proves a canceled partial join clears its output slots without consuming the
already-buffered Signal (`present=false`), which an outer non-canceled scope
then reads normally;
initial, partial, and terminal completions are replay-identical.
A 300-trace Child composition property then combines both primitives. Three
children resolve start jobs in every generated order; a start selector and
start join run before one generated child is explicitly canceled. Completion
selection receives two jobs in either order, completion join must remain blocked
until the third child resolves, and initialize/start/partial/terminal completion
bytes must all survive eviction and fresh-stack replay.
The CLI-devserver suite also starts `odin_condition_workflow`, waits until the
Worker reports `waiting_condition=true`, sends an `approve` Signal through the
Temporal CLI, and requires the predicate-resumed result plus clean eviction.
It then runs `TEMPORAL_ODIN_SCENARIO=selector`: the Workflow starts one Activity
and one durable Timer, waits with `waiting_selector=true`, selects the Activity,
explicitly cancels the losing Timer, and returns `Hello selector`. The worker
log must show both Core-driven fresh-stack replay and the terminal
`CancelTimer` command. The same run requires the distinct `start_timer`
interceptor to enter and unwind around non-suspending Future creation, replace
the Timer summary, and persist that summary in Server history.
Offline, a separate 180-trace Nexus selector property varies declaration order
and all valid interleavings of two Core start/result job pairs, passes both
resolved Futures through `join_all`, and reproduces initial and terminal
completion bytes after eviction.
The following `TEMPORAL_ODIN_SCENARIO=join-all` case starts a fast and a
two-second Activity with two callback slots. It requires a partial joined wait,
Core eviction/replay, two completed Activity Tasks, and the declaration-ordered
result `Hello join-fast & Hello Temporal-slow`.
The following `signal-join` case sends `beta` before `alpha`, requires the first
Signal to leave `waiting_join=true`, then returns `A & B` from the declared
case slots after Core-driven replay.
The `mixed-join` case delivers `approval` while a two-second Activity is still
running, requires the Workflow to remain joined, then returns
`Hello Temporal-slow & approved` only after Core resolves the Activity.
The activation-metadata unit test varies history size, Continue-As-New and
target-version flags, deployment/build identity, full replay, and query-only
replay. It queries `__temporal_workflow_metadata`, evicts the run, and requires
initialize, metadata-query, and terminal completions to be byte-identical on a
fresh stack. The CLI-devserver properties scenario additionally queries live
`currentDetails` before signaling the Workflow to finish, while the pinned
deployment scenario requires Core to expose its routed Worker version.
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

`worker.create` clones the namespace, task queue, deployment name, and Build ID
strings and initializes the language registries, but deliberately does not
create a Core worker. On the first `run`, the registration set selects Core's
`enable_workflows` and
`enable_remote_activities` task types; `disable_activity_polling` can suppress
the Activity type for that bounded run. This avoids polling unused streams and
removes task-type switches from the public construction API. The worker borrows
the client passed to `create`, so the stable client value, its connection, and
the Core runtime must outlive `worker.close`.

The optional `versioning` argument maps one-to-one onto the pinned C bridge's
`TemporalCoreWorkerVersioningStrategy` union. The zero value selects
unversioned operation. `.Deployment_Based` supports the modern experimental
deployment model with `.Pinned` or `.Auto_Upgrade` defaults;
`.Legacy_Build_Id_Based` exists for migration compatibility. Odin owns cloned
identity strings until `worker.close`; the C bridge converts their borrowed
byte references into Rust-owned strings synchronously during `worker_new`.
Validation rejects ambiguous combinations before the Core worker is created.

The optional `nondeterminism_policy` maps to Core's two existing Worker config
fields. The zero value preserves Temporal's repairable default: Core reports a
nondeterministic completion as a failed Workflow Task. `fail_all_workflows`
changes that behavior globally, while `fail_workflow_types` changes it only for
the named types. The language Worker clones every type name because Core worker
creation is delayed until `run`; `core.worker_create` builds a temporary C
reference array, and the bridge synchronously copies it into Rust `String`
keys during `worker_new`.

The C wrapper currently presents blocking poll calls. When both task types are
registered, `worker.run` creates one Workflow poll thread and one Activity poll
thread. Only the Workflow thread reads or mutates Workflow runtimes, so a fiber
never migrates between threads. The Activity poll thread instead creates one
heap-stable execution record per start task and submits it to a reusable Odin
standard-library thread pool. The pool and Core's fixed-size Activity slot
supplier both use `max_concurrent_activities` (10 by default), while the poll
thread remains free to deliver task-token-matched cancel tasks. Active
executions are hash-indexed by exact opaque binary task token; the key borrows
the execution-owned token and is deleted before that execution is freed. A
500-case property stresses 1–64 routes, embedded NUL bytes, duplicate tokens,
lookup locality, and forward/reverse drain order. On exit, the pool finishes
queued/running callbacks before its execution records are freed.
The dev-server suite also schedules 24 Activities at once against a Worker with
three slots. It requires an exact callback peak of three, all 24 completions,
an empty callback set at shutdown, and a bounded task-token route peak of three
or four. Four is valid because Core releases a slot when completion is accepted
before the poll loop reaps that callback's finished pool task. The observed
route high-water mark is returned as
`Run_Stats.activity_executions_peak_owned`; it is an Odin ownership metric, not
a second Core slot count.
`worker.request_shutdown` is safe to call from a coordinating thread;
`worker.close` must only be called after `run` returns.
`worker.create(..., graceful_shutdown_period = ...)` controls Core's delay
between shutdown initiation and cooperative cancellation of outstanding
Activities. The default is five seconds and zero requests cancellation
immediately. The C bridge accepts milliseconds, so positive fractional
milliseconds round upward; negative values are rejected during language Worker
construction. A 500-case property covers this conversion in the PBT layer. The
dev-server scenario uses 200 ms, keeps one callback alive
with heartbeats, initiates shutdown from another Odin thread, and verifies that
the callback receives only `.Worker_Shutdown` after the grace period. Its
cancellation completion reaches the server before `run` and `close` finish.
Core does not terminate callback code after another timeout: an Activity that
ignores cooperative cancellation can keep Worker shutdown waiting indefinitely.

For the complete supported Core configuration surface, construct
`worker.options_default(task_queue)` and call `worker.create_with_options`.
Do not start from `worker.Options{}` unless every field is intentionally filled:
the helper preserves the SDK's explicit heartbeat, poll-ratio, slot, shutdown,
and eager-reservation defaults. The language Worker clones `identity_override`
because Core construction remains delayed until `run`; all numeric values are
stored by value.

The Core adapter uses `Worker_Option_Bindings` to keep the raw simple/autoscaling
poller records address-stable for the synchronous `temporal_core_worker_new`
call. Null pointers select Core's default poller policy. Durations are converted
to milliseconds with positive fractions rounded upward, zero rates map to
Core's `None`, and nonzero rates must be finite and positive. Enabling Workflow
caching raises the fixed Workflow slot supplier from one to two; an explicitly
configured simple Workflow poller must likewise be at least two.

Activity capacity may instead use Core's resource-based supplier:

```odin
options.resource_based_activity_tuner = worker.resource_based_activity_tuner(
    0.8, // target memory utilization
    0.9, // target CPU utilization
)
options.resource_based_nexus_tuner = worker.resource_based_nexus_tuner(
    0.8, // shared controller: use the same targets
    0.9,
)
```

The convenience constructor uses one-to-ten slots and a 50 ms ramp throttle
for both remote and local Activities. The `_with_options` form exposes their
independent slot ranges. Nexus capacity has a matching tuner and `_with_options`
constructor. Every enabled resource supplier must carry identical CPU/memory
targets because the C bridge creates one shared resource controller and rejects
mixed resource options. The Odin callback pools are sized from the suppliers'
maximums so Core never grants work without matching executor capacity. Workflow
slots remain fixed-size. The public Nexus executor enables polling only when an
operation is registered and uses the configured Nexus poller behavior.

Activity slots may instead be supplied by Odin callbacks:

```odin
options.custom_activity_slot_supplier = {
    reserve = reserve_slot,
    cancel_reserve = cancel_reservation,
    try_reserve = try_reserve_slot,
    mark_used = mark_slot_used,
    release = release_slot,
    available_slots = available_slots, // optional
    free = supplier_free,              // optional
    user_data = &supplier_state,
}
```

Callbacks can be concurrent and must return promptly. `reserve` retains the
opaque `worker.Slot_Reservation` and later calls exactly one of
`worker.slot_reservation_complete(reservation, nonzero_permit)` or
`worker.slot_reservation_cancel_complete(reservation)`. A successful completion
consumes the handle; using it again is memory corruption in the upstream ABI.
All strings in `Slot_Reserve_Context` and `Slot_Info` are callback-scoped views.
Clone them before retaining them. Permit IDs must be unique among live permits,
and `release` may receive nil task info when a reserved slot was never used.

The callback table is copied into SDK-owned address-stable memory and remains
pinned until Core has dropped the supplier. `user_data` is borrowed and must
remain valid until `worker.close` returns; the SDK never deallocates it. The
optional `free` hook runs exactly once for every binding successfully created,
including when Core Worker construction fails. The pinned Rust adapter does
not currently invoke the C header's documented supplier-drop callback, so Odin
also runs the hook after `temporal_core_worker_free`. An exactly-once flag keeps
this compatible with a future upstream fix. A custom Activity supplier cannot
be combined with the resource-based Activity tuner, and
`max_concurrent_activities` continues to size the Odin executor pool.

Run the focused capacity/cancellation proof against the CLI dev server:

```bash
temporal server start-dev --headless
./scripts/custom-slot-supplier-integration-test.sh
```

It uses one custom permit with multiple Core pollers, completes an Activity,
and requires queued reservation, cancellation, mark-used, release, and free
callbacks to drain without leaked permits. The focused proof and every Activity
case in the full integration matrix use a unique task queue, so a stale task or
poller from another case cannot satisfy or perturb the supplier assertions.

A 1,000-case PBT property generates partial and complete callback tables plus
resource-tuner combinations, proving that incomplete or conflicting policies
are rejected before `worker_new`.
Prefer fixed slots for ordinary Workers; Temporal recommends resource-based
tuning when resource protection or workload-specific scaling warrants it.
Pairing it with `worker.poller_autoscaling` avoids excessive pollers at low
utilization.

Unit tests assert every populated C field and pointer/union arm. A 500-case
property test varies cache state, fixed/resource slots, shared targets, all
poller forms for Workflow, Activity, and Nexus, duration rounding, rates, and
sticky poll ratio. It runs against both the pinned PBT submodule and, during
local library development, `PBT_ROOT=~/Projects/pbt`.

The integration matrix exercises two complementary Workflow configurations.
The ordinary condition scenario leaves caching disabled and requires a
fresh-stack replay before its Signal can complete the Workflow. The
`workflow-cache` scenario sets a cache of ten and a two-poller Workflow policy;
it rejects any replay activation, requires the Signal to resume the existing
fiber, and still requires Core's final eviction during shutdown. This is the
minimum sticky-queue regression test for changes to Worker options, run routing,
fiber ownership, or shutdown.

`ACTIVITY_EXPECT_RATE_LIMIT=1` configures the hello Activity Worker for four
Activities per second and runs the existing 24-Activity load driver. The Worker
itself asserts a run time between five and thirty seconds and complete callback
and task drainage. Keep the lower bound generous: Core permits the initial
request and the test measures Worker startup as well as polling. A run that
forgets to populate the C rate field completes in well under that bound.

`ACTIVITY_RESOURCE_TUNER=1` configures the same Worker with resource-based
remote/Local Activity suppliers and autoscaling Activity pollers. The
integration harness completes a real task through the CLI dev server, proving
that Core accepts the tuner and that Odin's executor returns its completion.

`TEMPORAL_NEXUS_RESOURCE_TUNER=1` similarly configures both Nexus examples with
a resource-based supplier. The Workflow-to-Nexus suite exercises it on the
baseline CLI dev server; the standalone suite also enables it for synchronous,
cancellation, and lifecycle operations on servers that expose the newer RPCs.
Together they prove that Core accepts the supplier and that the bounded Odin
Nexus executor returns every completion.

### Replacing a running Worker's Client

Core can move an already constructed Worker to another connection without
rebuilding its registrations or language runtime:

```odin
replace_error := worker.replace_client(
    &sdk_worker,
    client.core_client(&replacement_client),
)
```

Call this from `Run_Options.on_ready` or from a coordinating thread while
`worker.run` is active. It is rejected before run, after run, and on a replay
Worker. Keep both the original and replacement Clients open until
`worker.close` returns; the Worker retains its original Runtime while Core
clones the replacement transport. The Worker's namespace and task queue do not
change.

The call does not cancel a Core long poll that is already in flight. The next
poll uses the replacement connection after that outstanding call returns or
fails. Treat a returned replacement error as requiring Worker recreation; Core
does not promise to roll the old registration back.

The focused integration test starts a second CLI dev server itself and expects
the ordinary development server on `localhost:7233`:

```bash
temporal server start-dev --headless
./scripts/worker-replace-client-integration-test.sh
```

It validates and starts polling against the temporary server on port 17233,
stops that server to fail the outstanding poll, switches Core to the primary
server, and completes a Workflow that exists only there.

### Runtime observability

Use `core.runtime_options_default()` and pass the result to
`core.runtime_create_with_options`. The same value can be attached by pointer
to `client.Connect_Options.runtime_options`; it is borrowed only while the
public Client constructs its owned Core runtime.

```odin
prometheus := core.Runtime_Prometheus_Options{
    bind_address = "127.0.0.1:9464",
    counters_total_suffix = true,
    unit_suffix = true,
}
runtime_options := core.runtime_options_default()
runtime_options.prometheus = &prometheus
runtime_options.metrics.global_tags = []core.Runtime_Key_Value{
    {key = "sdk_language", value = "odin"},
}

runtime, err := core.runtime_create_with_options(runtime_options)
```

Once metrics are configured, application code can use the same Core exporter:

```odin
base := [?]core.Metric_Attribute{
    core.metric_attribute_string("component", "billing"),
    core.metric_attribute_i64("shard", 7),
}
meter, err := core.metric_meter_create(&runtime, base[:])
defer core.metric_meter_close(&meter)

counter, err := core.metric_counter_create(&meter, "billing_requests")
defer core.metric_close(&counter)

err = core.metric_counter_add(&counter, 1)
```

Activity code obtains the same exporter with automatic execution scope:

```odin
meter := activity.metric_meter(ctx)
counter, err := core.metric_counter_create(meter, "activity_items")
defer core.metric_close(&counter)
err = core.metric_counter_add(&counter, 1)
```

Nexus handlers use the same explicit pattern through
`nexus.metric_meter(ctx)`. Their base attributes are `namespace`, `task_queue`,
`nexus_service`, and `nexus_operation`; Activity meters use `activity_type`
instead of the last two. The meter pointer is valid only during the callback.
It is a validating no-op when metrics are disabled, so handler code does not
need exporter conditionals.
Workflow code must instead use `workflow.metric_counter_add` and its typed
siblings. Those calls are replay-filtered and return no status; history replay
cannot duplicate a record or branch on host observability state. The Worker
owns their lazily cached Core instruments until `worker.close`.

`Metric` clones a private Core meter and base-attribute handle, so it remains
valid if its factory `Metric_Meter` closes first. It must still close before the
Runtime. Do not copy owning meter/instrument records. The wrapper validates the
instrument kind before calling Core because the raw ABI treats a mismatch as a
panic. Per-record attributes allocate one short-lived Core attribute handle;
derive a child meter with `metric_meter_with_attributes` when a label set will
be reused. Positive sub-millisecond duration observations round upward to one
millisecond.

The main CLI-devserver suite configures Prometheus on the Workflow-to-Nexus
example and scrapes start- and cancellation-handler counters before shutdown.
It requires the four automatic Nexus attributes, a record-specific attribute,
the Runtime's global tag, value two for the example's two concurrent start
operations, and value one for its registered cancel handler. This also tests
that each execution-owned meter stays alive for the callback and is released
during worker cleanup.

Prometheus and OpenTelemetry are mutually exclusive because the Core C bridge
accepts exactly one exporter. OTLP supports gRPC/HTTP, headers, cumulative or
delta temporality, and a configurable export period. Durations round positive
sub-milliseconds upward at the integer-millisecond ABI. Typed tags and headers
are encoded as Core's `key\nvalue` map only inside an address-stable binding
owner, and keys/values containing newlines are rejected before FFI. Histogram
bucket overrides intentionally remain the bridge's compact
`metric\n1,2.5,10` escape hatch for now.

Setting `Runtime_Logging_Options.filter` with no forwarding callback selects
Core's console subscriber. Set `forward_to` and optional `user_data` to receive
structured `Runtime_Log` records instead. The callback can arrive from an
arbitrary Core thread; target, message, and JSON fields are owned only until it
returns and must be cloned to retain. The C ABI supplies no per-runtime pointer,
so only one forwarding Runtime may exist per process. The SDK serializes the
route and rejects another until `runtime_close` has deactivated Core and removed
the route. A callback must not call Runtime create/close or panic across C.

```odin
on_core_log :: proc(data: rawptr, log: ^core.Runtime_Log) {
    // Synchronize application state here; this is a Core runtime thread.
    fmt.printf("[%v] %s: %s\n", log.level, log.target, log.message)
}

logging := core.Runtime_Logging_Options{
    filter = "temporalio_sdk_core=debug",
    forward_to = on_core_log,
    user_data = tracing_state,
}
runtime_options.logging = &logging
```

The integration suite starts `examples/core_connect` with a loopback
Prometheus address, scrapes `/metrics` while the runtime is alive, and requires
`temporal_request_total` for `GetSystemInfo` with both Core's `service_name` and
the Odin global tag. It also creates `odin_application_events` through the raw
meter ABI and requires value `3` plus base/per-record string, integer, float,
and boolean labels. This proves the exporter and application record path rather
than only their C layouts. Two lifecycle tests create every pinned instrument
kind against a real Core Runtime on an ephemeral port, validate failure without
an exporter, close factory meters before recording, and reject kind/value/
attribute misuse before FFI.
It also runs `examples/core_connect` with
`TEMPORAL_CORE_FORWARD_LOGS=1 TEMPORAL_CORE_LOG_FILTER=trace` and requires a
well-formed callback record. The unit suite proves a second simultaneous route
is rejected and that a fresh forwarding Runtime succeeds after close.

Run the scoped language-metric proof independently with:

```bash
./scripts/scoped-metrics-integration-test.sh
```

It uses `temporal server start-dev` directly when no healthy local Service is
already present. The hello Worker exports Prometheus on a unique loopback port,
emits a Workflow counter and an Activity-context counter, and deliberately
waits on a durable timer after Core has evicted and replayed the Workflow. The
scrape requires automatic `namespace`, `task_queue`, and Workflow/Activity type
labels, the per-record `scenario` label, and exactly one observation for each
counter. `scripts/integration-test.sh` includes this focused proof in the full
live matrix.

Activity cancellation is cooperative. `activity.is_cancellation_requested`
and `activity.cancellation_reason` expose the primary state Core delivered.
`activity.cancellation_details` returns the associated snapshot of Core's six
independent detail flags: not found, cancelled, paused, timed out, worker
shutdown, and reset. More than one flag may be true. Returning from a plain
callback after a request makes the worker send a cancellation result with an
empty `CanceledFailureInfo`. A result-returning callback may instead use
`activity.result_canceled` or `activity.result_canceled_json` to control the
message and attach one user Payload. The worker copies that Payload into the
completion before releasing the callback-owned result.

At Workflow level, `workflow.cancel_activity` deterministically emits at most
one cancel command for a started Activity. `Try_Cancel` may resume immediately;
`Wait_Cancellation_Completed` remains suspended until Core delivers
`ResolveActivity`, at which point
`workflow.activity_cancellation_detail_payload` and
`workflow.activity_cancellation_detail_json` expose callback-supplied details
as views borrowed from the Workflow result. `Abandon` emits no cancellation
command. Only `Wait_Cancellation_Completed` promises the Activity callback had
an opportunity to supply cancellation details.

`activity.heartbeat(ctx, details)` remains the
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

For remote Activities, `activity.result_will_complete_async(T)` and
`activity.payload_will_complete_async()` emit Core's `WillCompleteAsync`
completion. The callback must copy `activity.info(ctx).task_token` before it
returns if another process will address the Activity by token; all `Info` data
is callback-borrowed. `temporal/client/async_activity.odin` then exposes
completion, Application Failure, cancellation, and heartbeat RPCs by task token
or by IDs. Heartbeats return all three current Service flags: cancellation,
pause, and reset. ID forms also cover standalone Activities by leaving Workflow
ID empty. Local Activities convert an attempted asynchronous result into a
non-retryable contract failure.

The live dev-server matrix schedules `async_hello`, lets the Odin worker emit
`WillCompleteAsync` and stop after one task, then uses a separately connected
Odin Client for four paths: heartbeat plus completion by copied task token,
heartbeat plus completion by Workflow/Activity ID, heartbeat plus typed
Application Failure by task token, and heartbeat plus cancellation reporting by
ID. The cancellation driver requests Activity cancellation with
`WaitCancellationCompleted`; Odin waits until the heartbeat response exposes
`cancel_requested` before acknowledging it. The integration test reads the
completed histories and decodes their Payload data with `jq`, proving the
failure message/type/non-retryable flag/details and the cancellation details.
Standalone-Activity ID addressing uses the same By-ID requests but is not yet a
live test because the pinned CLI Server does not implement
`StartActivityExecution`.

Activity panics are not caught in-process. In the tested Odin toolchain,
`panic` delegates to `context.assertion_failure_proc`, whose type returns `!`;
there is no supported unwind/recover boundary. A `setjmp`/`longjmp` shim would
skip Odin `defer`s and allocator cleanup, so the SDK intentionally does not
present it as containment. Until subprocess-isolated Activity executors or
language/runtime support is designed, deploy workers under a process supervisor
and return `activity.Result(T)` for expected failures.

The supported containment pattern is now exercised end-to-end:

```bash
./scripts/activity-panic-integration-test.sh
```

The test starts an Activity-only Odin worker on a unique queue and a separate
Rust Workflow worker. Attempt 1 prints a marker and invokes Odin `panic`, so the
Activity worker terminates via `Trace/BPT trap` without returning a completion.
The Service retains the durable Activity state; after the one-second
Start-To-Close timeout and retry backoff, a newly started Odin Activity worker
receives attempt 2. The callback returns its observed attempt number and the
original Workflow completes with `Recovered on Activity attempt 2`. This proves
the recommended supervisor/replacement process boundary, not in-process
recovery. Co-locating Workflow and Activity registrations still makes an
Activity panic fatal to that entire worker process.

### Uncooperative callback shutdown policy

`Run_Options.uncooperative_callbacks` configures a duration, observer, and
borrowed user pointer. Duration and observer must either both be set or both be
zero; negative durations are rejected before Core Worker creation. On explicit
or SDK-initiated shutdown, the Worker starts a watchdog before forwarding the
request to Core. Activity and Nexus callback entry/exit update atomic counters.
After the deadline, the watchdog invokes the observer once for each still-active
kind with a `Callback_Drain_Info`. Bounded and error drains use the same model
around `thread.pool_finish`.

The observer is diagnostic/process-policy code. It runs on a dedicated thread,
must return promptly, and may record telemetry or terminate the process, but it
must not call `worker.close`, release registrations, or otherwise free callback-owned state.
The SDK continues waiting because Odin's standard pool termination uses OS
thread cancellation and explicitly warns that resources may remain owned.

Two live paths cover the timing boundary. The existing Worker-shutdown scenario
starts its 50 ms watchdog when `request_shutdown` is called, reports one active
Activity, then receives Core's `Worker_Shutdown` cancellation after the 200 ms
grace period and finalizes cleanly. The focused bounded-drain proof is:

```bash
./scripts/uncooperative-callback-integration-test.sh
```

Its Activity ignores cancellation for 250 ms. The 50 ms observer fires with
`kind=Activity`, `reason=Bounded_Run`, and `outstanding=1`; the callback later
returns normally, proving observation does not kill the executor. Unit tests
also cover incomplete policy rejection and a semaphore-controlled pool drain.
Workflow callbacks are excluded: an infinite Workflow invocation cannot yield
to Core, so process supervision remains the only sound containment boundary.

### Workflow process crash and replacement

The whole-Worker process contract is specified in
[`supervision.md`](supervision.md). Its focused acceptance test is:

```bash
./scripts/workflow-crash-integration-test.sh
```

The SDK `temporal/supervisor` starts the first Odin Worker, waits for an
application readiness probe confirming its poller, and observes the initial
Workflow Task suspend on a condition. The test then sends `SIGKILL` to child
generation 1 and verifies that the CLI result wait remains open. The supervisor
must observe the failed exit, apply its backoff, start generation 2, and report
the replacement ready on the same unique task queue. After a Signal, that child
logs replay of the waiting condition and completes. Exact lifecycle statistics
require two starts/readiness events, one restart, one failed exit, and one clean
exit. This proves SDK-managed whole-Worker replacement plus Server-owned
durability; it does not provide in-process panic recovery or a per-execution
subprocess pool.

Unit tests cover invalid policies, pre-requested stop, clean exit, readiness
timeout with child termination, and bounded restart exhaustion. A 500-case PBT
property generates initial/maximum backoff, multipliers, and transition counts,
requiring every delay to remain monotonic, overflow-safe in the generated
domain, and exactly saturated at the configured maximum.

### Per-run Workflow process boundary

The fine-grained executor transport and its zero/sticky-cache Core integration are
documented in [`workflow-isolation.md`](workflow-isolation.md). The transport
can be run without a Temporal Server:

```bash
./scripts/build-examples.sh
TEMPORAL_ODIN_SKIP_BUILD=1 ./scripts/workflow-isolate-integration-test.sh
```

Build and verify the relocatable, application-specific parent/executor bundle:

```bash
TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/package-workflow-isolate-example.sh dist
./scripts/verify-workflow-isolate-example-package.sh \
  dist/temporal-sdk-odin-workflow-isolate-example-<target>.tar.gz
```

Unlike the generic native runtime archive, this executor contains the example
Workflow registry. The verifier moves the unpacked bundle, checks its relative
loader paths and pinned Core/protocol versions, then runs the serverless
process proof. CI repeats the same proof on all four supported targets.

The parent sends two correlated binary Core activations over bounded versioned
frames to one child process. The child suspends a sequential Odin Workflow on a
timer after the first activation, retains its fiber, resumes it on `FireTimer`,
and returns the terminal Core completion. Unit and PBT coverage validates
framing independently of the process example.

The complete Worker/Core/Server recovery proof is:

```bash
./scripts/workflow-isolate-core-integration-test.sh
```

It starts `temporal server start-dev --headless` when needed, deliberately
crashes the first Workflow executor, reports that as an activation failure,
consumes Core eviction, and requires a fresh child to replay and ultimately
complete the Workflow. `max_cached_workflows = 0` intentionally aligns every
child lifetime with one Core Workflow Task/cache lifetime.

The parent now uses the reusable `temporal/workflow/isolate.Manager`, not
example-local process code. `manager_init` copies the ordinary and optional
first-generation command vectors and sets a hard maximum for owned routes.
Failed routes retain their generation and consume capacity until Core eviction;
normal eviction exchanges the final activation, performs a correlated shutdown
handshake, and reaps the child. `manager_shutdown` concurrently performs that
correlated handshake for every remaining cached child, rejects new activations,
and kills/reaps failures under the configured deadline; `manager_close` invokes
it automatically. This avoids a route-count-multiplied shutdown delay while
preserving explicit error reporting for callers that invoke `manager_shutdown`
directly. Generated `statecharts`/PBT traces cover lifecycle equivalence,
capacity, and generation fencing. A positive `activation_timeout` adds a
joinable pipe-reader thread; expiry kills and reaps the child before returning
the activation failure. Run the real hung-child recovery path with:

```bash
TEMPORAL_ODIN_ISOLATE_FIRST_MODE=hang \
  ./scripts/workflow-isolate-core-integration-test.sh
```

Run stale/malformed/mismatched/late executor responses through the same Core
eviction and replay path with:

```bash
TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/workflow-isolate-protocol-fault-integration-test.sh
```

The five cases require either one protocol failure or one activation timeout,
one failed Workflow activation, complete child reaping, and successful durable
results from replacement generations. A stale response never shares a pipe or
route with its replacement generation.

Run a real history/command mismatch through the executor boundary with:

```bash
TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/workflow-isolate-nondeterminism-integration-test.sh
```

This first records a durable one-hour Timer using the ordinary child, then
replays the same execution using an incompatible implementation behind the
same type manifest. The sticky-cache replay must produce exactly one Core
`NONDETERMINISM` eviction; the type-scoped Core policy must then fail the
Workflow, and the CLI history must contain `[TMPRL1100]` or equivalent
nondeterminism diagnostics.

Exercise real guard-page stack-overflow recovery with:

```bash
TEMPORAL_ODIN_ISOLATE_FIRST_MODE=stack-overflow \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/workflow-isolate-core-integration-test.sh
```

The first Workflow callback recursively exhausts the minicoro stack. The child
must terminate with the platform's `SIGSEGV` or `SIGBUS` value, the manager must
preserve that exit code, and Core eviction/replay must complete both executions
in replacement children. This complements `scripts/fiber-guard-test.sh`, which
proves the guard mapping and signal in a smaller native fork test.

Exercise an actual Odin Workflow panic with:

```bash
TEMPORAL_ODIN_ISOLATE_FIRST_MODE=panic \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/workflow-isolate-core-integration-test.sh
```

The child must print the intentional panic and terminate with `SIGTRAP` or
`SIGILL`; Core eviction/replay must then produce both ordinary Workflow
results. This deliberately tests process recovery rather than installing a
custom assertion handler or unwinding through the native fiber trampoline.

Exercise a Workflow callback that never reaches a suspension boundary with:

```bash
TEMPORAL_ODIN_ISOLATE_FIRST_MODE=infinite-loop \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/workflow-isolate-core-integration-test.sh
```

The executor enters a real infinite Odin loop. The 250 ms activation deadline
must kill and reap it, increment exactly one timeout rather than a child-exit
counter, and recover both executions through Core eviction and replay.

Parent and child now both call the same application-level
`workflow_isolate_app.register` function. In the parent,
`register_external_workflow_registry` copies only the Workflow type-name
manifest into the Core Worker. In each child, `Executor_Host` instantiates
independent callback runtimes from the Registry and routes initialization by
Workflow type plus later activations by run ID. Procedure pointers never cross
the pipe. The live test completes both a string Workflow and a typed-JSON
Workflow through the same executor binary, after either a deliberate first
child crash or timeout.

Run the non-zero-cache path with:

```bash
TEMPORAL_ODIN_ISOLATE_CACHE_MODE=sticky \
  ./scripts/workflow-isolate-core-integration-test.sh
```

After one failed generation, two timer Workflows use exactly two successful
children across suspension and resolution: three generations/evictions total,
versus five in zero-cache mode. Both timer resolutions are incremental
activations routed to the already-owned generation. The hang variant passes
with the same counts.

External and in-process Workflow registrations cannot be mixed on one Worker.
Workflow loggers and interceptors for external registrations must be configured
inside the executor host; the parent rejects them rather than silently applying
them to runtimes that will never execute. Pass `Executor_Host_Options` to
`executor_host_init_with_options` to install one replay-aware logger and ordered
inbound/outbound interceptor lists in every child runtime before execution. The
example logger uses inherited stderr because stdout is the binary isolation
protocol. The optional metric-emitter factory receives Workflow type, task
queue, and namespace once per registered runtime; a successful returned emitter
is owned and destroyed by exactly that runtime. Protocol version 2 encodes the
ordered type-name manifest with explicit lengths. Protocol version 3 adds a
correlated JSON `Completion_Metadata` frame before the opaque protobuf
completion. It
preserves the complete child `Process_Result` for Worker statistics and
observers; the parent validates terminal flags against the authoritative
completion and uses activation routing as the source of replay/history/eviction
fields. `manager_verify_host` verifies the
ordinary child before `worker.run`, and every task child repeats the handshake
before its first activation. The zero-cache crash and hang paths each require
six successful handshakes; sticky mode requires four. All require zero
mismatches. The live observer requires four non-zero child timer sequences in
zero-cache replay and two in sticky mode. Both paths also require exactly two
inbound-interceptor log records; zero-cache replay records are suppressed.
They require exactly two counter records as well; zero-cache replay metrics are
also suppressed.

Protocol version 4 adds zero or more `Diagnostic` frames before metadata. Child
code calls `executor_host_emit_diagnostic` with an application/log/metric kind
and opaque non-empty payload. The host heap-clones it, avoiding the Workflow
arena allocator active inside logger/metric callbacks. `Manager_Options` accepts
a synchronous borrowed `diagnostic_observer`; the live proof requires two log
and two metric events with non-zero generation IDs. Zero
`maximum_diagnostics_per_activation` selects the bounded default of 256.
`maximum_diagnostic_payload_bytes_per_activation` selects a separate cumulative
encoded-payload budget whose default is 1 MiB. Admission is overflow-safe and a
rejected frame is never delivered to the observer. One monotonic activation
deadline spans all diagnostic, metadata, and completion reads rather than
restarting per frame. The `diagnostic-budget` live fault sends one correctly
correlated oversized frame, requires one protocol/budget rejection, Core
eviction, and successful replay in a fresh child.

The manager now has independent activation and shutdown deadlines. The latter
bounds both acknowledgement and child exit, then kills/reaps on expiry. Manager
teardown drains cached child sessions in parallel and aggregates their results
after joining, so four hung children consume one concurrent deadline window
rather than four serial windows. Tests also verify clean multi-child handshake,
idempotence, post-shutdown activation rejection, and complete route release.
Capacity admission never waits because the same serial poll thread must receive
the eviction that frees a route; exhaustion returns a failed activation
immediately.

`Manager_Options.eviction_observer` exposes Core's complete borrowed eviction
diagnostic at the parent boundary: run ID, reason, message, route presence, and
generation. The callback is synchronous and must copy data it retains. This
keeps nondeterminism explanations visible even when the affected executor has
already crashed or been killed.

An optional consecutive-failure circuit breaker opens after a configured number
of child/session failures and admits one new trial after a monotonic cooldown.
Unit tests use real sleeping/exiting subprocesses for shutdown and breaker
behavior, while a 1,000-case property test checks the breaker boundary. The
CLI-dev-server crash and hang paths enable these policies and require zero
shutdown timeouts or breaker rejections during successful recovery.

Workflows have the corresponding registration levels:

```odin
worker.register_workflow(&w, "string_workflow", string_workflow)
worker.register_json_workflow(&w, "typed_workflow", typed_workflow)
worker.register_payload_workflow(&w, "payload_workflow", payload_workflow)
worker.register_payloads_workflow(&w, "payloads_workflow", payloads_workflow)
```

The runtime stores Workflow inputs, scheduled Activity inputs, resolved Activity
output, and Workflow completion as allocator-owned `codec.Payload` values in
the Workflow instance arena. `register_json_workflow` generates a typed adapter
for `proc(Fiber_Context, Input) -> Output`; `register_workflow` is the existing
string compatibility adapter. `register_payload_workflow` receives one borrowed
view, while `register_payloads_workflow` receives the complete ordered borrowed
slice from `InitializeWorkflow.arguments`; both return an owned
`workflow.Payload_Result`. The one-argument adapters reject zero or multiple
arguments as a non-retryable `OdinWorkflowArgumentCountError`. Callback storage
belongs to the `Fiber_Runtime` and is released by `fiber_runtime_close`.

Workflow code can use `start_activity_payload`/
`await_activity_payload_result` at the codec boundary, or
`start_activity_json`, `execute_activity_json_result`, and
`execute_activity_json` for typed values. The output type is explicit at the
call site because an Activity name string cannot communicate an Odin return
type to the compiler. JSON conversion failures become non-retryable
`OdinWorkflowPayloadConversionError` Workflow failures. Signal payloads remain
on the string compatibility API for now.

For more than one codec-neutral argument, use
`start_activity_payloads`/`execute_activity_payloads` or the corresponding Local
Activity functions. The slice is borrowed only for the scheduling call; the
Workflow runtime clones every Payload into the run arena before it can suspend.
Register the callback with `worker.register_payloads_activity`; its input slice
and views are borrowed until the callback returns. `register_activity`,
`register_json_activity`, `register_json_activity_result`, and
`register_payload_activity` are deliberately single-argument adapters and fail
other arities with `OdinActivityArgumentCountError`.

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

The same rule applies when user Workflow code fails inside a Signal handler.
Core first records the failed Workflow Task, then delivers
`RemoveFromCache(LANG_FAIL)`. Odin retains the failed fiber only long enough to
acknowledge that eviction; the retry initializes a fresh stack and reconstructs
state from history. The unit regression compares both initial and failed
completion bytes across that fresh-stack replay, then routes a healthy run
through the same Worker. The `signal-handler-failure` integration scenario
observes exactly one reported Workflow Task failure in CLI-devserver history,
terminates the intentionally retrying Workflow, and completes a second Workflow
without restarting the Worker.

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
the Client. Free handles before closing the Client. `start_workflow_payloads`
is the repeated codec-neutral start form; `start_workflow_payload` and
`start_workflow_json` are one-argument adapters. `get_result_payload` remains
codec-neutral because a Workflow completion contains at most one result
Payload.

The combined start/signal path reuses the same start projection but has a
dedicated option wrapper because Temporal exposes only one header map and it is
delivered on the Signal event:

```odin
headers := temporal_client.Headers{}
_ = temporal_client.headers_init(&headers)
defer temporal_client.headers_free(&headers)
_ = temporal_client.headers_add_json(&headers, "caller", "odin-client")

handle, err := temporal_client.signal_with_start_workflow_json(
    &client,
    "greeting_workflow",
    "initial input",
    "append",
    "signal input",
    {
        start_options = {id = "greeting-42", task_queue = "greetings"},
        signal_headers = &headers,
    },
)
```

`started_by_request` reports whether that call created the execution. Start
headers and eager start are rejected for this operation because the upstream
request cannot encode them separately. To run its create-then-reuse acceptance
case manually, start the worker with
`TEMPORAL_ODIN_SCENARIO=signal-with-start`, then run
`examples/client_start` with `TEMPORAL_CLIENT_SCENARIO=signal-with-start` and a
shared `TEMPORAL_CLIENT_WORKFLOW_ID`. The full integration script does this and
requires exactly two `WORKFLOW_EXECUTION_SIGNALED` history events.

Initial Payload maps use a separate owned builder:

```odin
metadata := temporal_client.Start_Metadata{}
_ = temporal_client.start_metadata_init(&metadata)
defer temporal_client.start_metadata_free(&metadata)

_ = temporal_client.start_metadata_add_header_json(&metadata, "trace-id", trace_id)
_ = temporal_client.start_metadata_add_memo_json(&metadata, "customer", customer_id)
_ = temporal_client.start_metadata_add_search_attribute_json(
    &metadata,
    "OrderStatus",
    .Keyword,
    "pending",
)
_ = temporal_client.start_metadata_set_static_summary(&metadata, "Greeting 42")
_ = temporal_client.start_metadata_set_static_details(
    &metadata,
    "Started by the Odin SDK development example.",
)

retry_policy := temporal_client.Retry_Policy{
    initial_interval = time.Second,
    backoff_coefficient = 2,
    maximum_attempts = 5,
}
priority := temporal_client.Priority{priority_key = 2}

handle, start_error := temporal_client.start_workflow_json(
    &client,
    "greeting_workflow",
    input,
    {
        id = "greeting-42",
        task_queue = "greetings",
        id_reuse_policy = .Allow_Duplicate,
        id_conflict_policy = .Fail,
        retry_policy = &retry_policy,
        start_delay = 500 * time.Millisecond,
        priority = &priority,
        metadata = &metadata,
        enable_eager_workflow_start = true,
    },
)
```

The builder records its allocator and owns cloned keys/Payloads. Payload helpers
accept borrowed codec views, JSON helpers encode directly into builder-owned
storage, and adding the same key again frees and replaces the previous value.
`Start_Workflow_Options` borrows the builder only for request encoding. Search
Attribute helpers replace/add Temporal's canonical `type` Payload metadata;
the custom field itself must already be registered in the namespace.

Workflow start policies are plain borrowed option values. Nil `retry_policy`
and `priority` pointers omit those messages. Within an explicit policy, zero
durations, coefficient, priority key, and fairness weight preserve Service
defaults. Zero maximum attempts means unlimited attempts up to Workflow
timeouts. `Unspecified` ID policies preserve the Service defaults:
`Allow_Duplicate` for a closed execution and `Fail` for a running execution.
The SDK retains deprecated `Terminate_If_Running` for compatibility, but new
code should combine `.Allow_Duplicate` with `.Terminate_Existing`.

Validation happens before request encoding: durations must be non-negative,
Run timeout cannot exceed an explicit Execution timeout, start delay cannot
accompany cron, a nonzero backoff coefficient must be finite and at least one,
maximum retry interval cannot be shorter than a nonzero initial interval,
fairness keys are at most 64 bytes, and priority values cannot be
negative/non-finite.

Start uses a UUIDv4 request ID when the caller omits one. Zero duration means
unspecified; invalid options are rejected before RPC.
When eager start is requested, the matching Workflow worker must already be
validated on the same public Client/Core connection. Core reserves the worker
slot, may clear the request if no slot is available, and dispatches a returned
task before the RPC result reaches Odin. The resulting Workflow handle records
whether eager execution was actually accepted. `worker.Run_Options.on_ready`
is the synchronous post-validation/pre-poll hook used by the self-contained
live example to establish this ordering without a race.
Result retrieval issues a blocking close-event long poll with no local timeout,
maps every terminal Workflow event, and follows retry/cron/Continue-As-New run
IDs by default. Set `follow_runs = false` to receive
`Workflow_Continued_As_New` instead. RPC failures retain the gRPC status code
and binary status-details protobuf.

The generic Core RPC adapter has a deliberately bounded workaround for a
transport edge observed under the live suite: tonic can surface a lost HTTP/2
connection as status 1 with `h2 protocol error` after its nested transport
cause has disappeared, preventing Core's own retry classifier from recognizing
it. Retry-enabled calls repeat that exact failure once after 100 ms. No-retry
deadline probes, ordinary cancellation, different status/message combinations,
and the second attempt return normally to the caller. The unit predicate test
keeps this fallback from expanding into a general application-level retry loop.
Bounded Update result waits deliberately disable retries. If their monotonic
deadline has actually elapsed and tonic reports that same status-1 HTTP/2
shape, the Update client maps it to `Wait_Timed_Out`; an identical early
transport failure remains an RPC error. This preserves the local-wait contract
without issuing a second long poll or affecting the durable Update.

Failed Workflow close events and failed Update outcomes retain the encoded
Temporal `Failure` in `Error.details`. Decode it through the shared owned model:

```odin
failure, decode_error := temporal_client.decode_error_failure(&result_error)
defer temporal_failure.failure_free(&failure)

activity_failure := temporal_failure.find_kind(&failure, .Activity)
application_failure := temporal_failure.find_kind(&failure, .Application)
```

`temporal/failure.decode` accepts an explicit allocator. The resulting tree
owns cloned common fields, variant fields, Payload metadata/data/external
references, and recursively allocated cause nodes. Free it with
`failure.failure_free`; it does not borrow `Error.details`, a Core response, or
the protobuf decode arena. The decoder covers all current pinned Failure
variants, rejects malformed protobuf/duration values, and caps cause depth at
128. A decoded `.Unknown` node still preserves common fields and its cause.

The same handle supports control operations:

```odin
existing, handle_error := temporal_client.get_workflow_handle(&client, "greeting-42")
defer temporal_client.workflow_handle_free(&existing)

signal_error := temporal_client.signal_workflow_json(&existing, "approval", approval)
cancel_error := temporal_client.cancel_workflow(&existing, "no longer needed")
terminate_error := temporal_client.terminate_workflow(&existing, "operator decision")
pause_error := temporal_client.pause_workflow(
    &existing,
    {reason = "downstream dependency outage"},
)
unpause_error := temporal_client.unpause_workflow(
    &existing,
    {reason = "downstream dependency recovered"},
)

reset, reset_error := temporal_client.reset_workflow(
    &existing,
    {
        workflow_task_finish_event_id = 4,
        reason = "deploy replay-safe Workflow code",
    },
)
defer temporal_client.workflow_handle_free(&reset)

priority := temporal_client.Priority{priority_key = 5}
priority_path := [?]string{"priority"}
updated_options, update_options_error := temporal_client.update_workflow_execution_options(
    &existing,
    {update_mask = priority_path[:], priority = &priority},
)
defer temporal_client.workflow_execution_options_free(&updated_options)
```

Handle creation only clones identifiers; it does not contact the Service. An
empty run ID targets the current execution. Signal, cancellation, and reset
requests generate UUIDv4 deduplication IDs by default. Reset requires a positive
Workflow Task completed/timed-out/failed/started event ID and returns a separate
owned handle for the new run without changing the source handle. Its current
options expose signal, Update, and Nexus reapply exclusions plus ordered
post-reset operations. The post-reset Signal arm carries repeated Payloads,
Headers, and Links. The Workflow-options arm uses explicit mask paths to set or
clear versioning override, priority, and time-skipping config. The deprecated
positive reapply enum remains unset; an empty exclusion list preserves Service
behavior, whose pinned compatibility default reapplies signals. Signal and termination have codec-neutral Payload
forms, plus typed JSON helpers. The live suite invokes all four operations
through `examples/client_control`.

Run the focused newer-server proof with:

```bash
./scripts/post-reset-integration-test.sh
```

It resets a completed repeated-argument Workflow, atomically sets priority 2,
completes the new run, requires `WORKFLOW_EXECUTION_OPTIONS_UPDATED` in history,
and reads priority 2 back through Describe. CLI 1.9.1/Server 1.32.0 rejects the
other schema-present arm with `unsupported post reset operation:
*workflow.PostResetOperation_SignalWorkflow_`. Signal therefore remains exact
wire/unit/PBT coverage rather than a live capability claim. The 500-case reset
property generates 0–12 ordered Signal/option operations and preserves their
kind, order, names, masks, and priorities through protobuf encoding.

Run the ordinary open-execution update proof with:

```bash
./scripts/workflow-options-integration-test.sh
```

It waits until the Workflow is durably suspended, sends priority 5 through the
public intercepted Client API and Core's generic RPC bridge, checks the
canonical response, reads priority 5 back through CLI Describe, then signals the
Workflow to completion. CLI 1.9.1/Server 1.32.0 omits the response's newer
`update_time` field, so the owned result models it as optional. The request and
response have direct wire/ownership tests; the 500-case reset property exercises
the shared option encoder across arbitrary ordered operations.

Run the focused atomic Update-With-Start proof with a local dev server that has
the multi-operation endpoint enabled:

```bash
temporal server start-dev \
  --headless \
  --dynamic-config-value frontend.enableExecuteMultiOperation=true

./scripts/update-with-start-integration-test.sh
```

The test starts the Odin Workflow/Activity Worker, calls
`ExecuteMultiOperation` through Core against a missing Workflow ID, waits for
the Update result, repeats the call against the running execution, and requires
the same Run ID with `started_by_request=false`. It then signals the Workflow
to completion. The example supplies a non-default `Connect_Options.identity`;
the script fetches JSON history and requires that identity on at least four
events, covering Workflow start, both Updates, and the final Signal rather than
only checking Odin-side request construction. `scripts/integration-test.sh`
enables the same dev-server flag
and runs this proof automatically. Exact wire/ownership tests cover the two
nested responses, and a 500-case property varies both 0–12 argument lists.

Workflow Pause is a feature-gated experimental Service API. Start a compatible
local server explicitly:

```bash
temporal server start-dev \
  --headless \
  --dynamic-config-value frontend.WorkflowPauseEnabled=true

TEMPORAL_ODIN_WORKFLOW_PAUSE_ENABLED=1 ./scripts/integration-test.sh
```

Or, with that server already running, execute only the focused proof:

```bash
./scripts/workflow-pause-integration-test.sh
```

The focused test reaches a durable Signal wait, pauses the Workflow through the
public intercepted Client API, verifies the owned pause identity/reason/time
through Odin `DescribeWorkflowExecution`, then checks Paused status and Query
rejection,
sends a Signal, and proves no Odin activation processes it before Unpause. It
then unpauses, completes normally, and requires both pause history events. The
request wire contract and interceptor nesting have direct unit coverage; a
500-case property preserves operation kind, optional run targeting, identity,
reason, and idempotence ID. The test accepts symbolic event/status names and
their numeric values because older CLIs do not know the newer enums even when
connected to Server 1.32.

Queries use the same borrowed handle and Core generic-RPC path:

```odin
status: Greeting_Query_Output
query_error := temporal_client.query_workflow_json(
    &existing,
    "greeting_status",
    Greeting_Query_Input{include_name = true},
    &status,
)

closed_query_error := temporal_client.query_workflow_json_with_options(
    &existing,
    "greeting_status",
    Greeting_Query_Input{},
    &status,
    {reject_condition = .Not_Open},
)
```

`query_workflow_payloads[_with_options]` sends the complete ordered argument
slice and returns an owned Payload that the caller frees with
`codec.payload_free`; the singular Payload and JSON helpers are one-argument
adapters. Metadata and external-payload details are retained for every
argument. A server `QueryRejected` response maps to
`.Workflow_Query_Rejected` and sets `has_workflow_status` plus the typed
`workflow_status` that caused rejection. The available conditions are
`Unspecified`, `None`, `Not_Open`, and `Not_Completed_Cleanly`. Handler failures
remain normal Core RPC failures with the server's gRPC status/details.

Workflow code installs the matching typed handler with
`workflow.set_query_handler_json`. Its state pointer is borrowed from the
Workflow fiber and the callback receives a top-level state copy. The callback
must be read-only, must not retain the input/state values, and cannot suspend.
Unknown query names and codec failures become failed `RespondToQuery` commands
without failing the Workflow Task.

Core reserves the query ID `legacy_query` for its older query transport and
requires that activation to contain exactly one Query job. Odin checks this
shape immediately after protobuf decoding, before metadata application or
fiber dispatch. A focused unit test proves a valid query-only completion and
that a rejected mixed Query/Signal activation leaves the Signal available. A
500-case property varies one to twelve companion jobs, first/middle/last query
placement, and replay state while requiring identical pre-dispatch rejection.

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

`start_update_payloads[_with_options]` and `wait_update_payload` are the
codec-neutral forms; `execute_update_payloads` composes them as a convenience.
The singular Payload and JSON APIs remain one-argument adapters. Start creates UUIDv4
Update and request IDs unless an explicit Update ID is supplied, asks
`UpdateWorkflowExecution` to wait for `ACCEPTED`, and returns an owned handle.
Wait long-polls `PollWorkflowExecutionUpdate` for `COMPLETED`. The successful
Payload is caller-owned. Update failure returns
`.Workflow_Update_Failed`; `Error.message` owns the Temporal failure message and
`Error.details` owns the encoded Failure protobuf.
When an accepted stackful Update terminates through an unhandled Activity or
Child Workflow failure, the runtime uses Core's original encoded Failure for
`UpdateResponse.Rejected`. The Client therefore decodes the same recursive
wrapper/cause tree—including operation identity, retry state, typed details,
and retry delay—while the enclosing Workflow remains live. Handlers that use
the explicit `*_result` Activity/Child APIs can still catch the failure and
complete the Update successfully.
`wait_update_payload_with_options` and `wait_update_json_with_options` accept a
monotonic total timeout plus an optional Core-backed cancellation token. They
return `.Wait_Timed_Out` or `.Wait_Canceled` without changing the durable
Update. A token can be canceled from another thread, is borrowed until the
blocking wait returns, and must then be explicitly closed. `get_update_handle`
clones Workflow/run/Update IDs so an accepted Update can be polled after a
process reconnect.
If start returns an immediate completed outcome (notably validator rejection),
the handle owns that outcome and wait returns it without an extra poll. The raw
RPC protobuf buffer stays alive until all decoder-borrowed Update IDs, strings,
and Payload bytes have been cloned.

The `client_start` dev-server probe pre-cancels a Core token, enters
`PollWorkflowExecutionUpdate` through an attached handle, requires
`.Wait_Canceled`, and then completes an ordinary Workflow RPC on the same
connection. Unit tests cover token cancel/close idempotence, invalid lifetime
use, negative timeouts, and identifier ownership. The concurrent-Update live
scenario bounds the slow Update wait to 20 ms, receives `.Wait_Timed_Out`,
reattaches from copied stable IDs, and then obtains the eventual result—proving
that local timeout did not cancel the durable Update.

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

For codec-neutral multi-argument Updates,
`set_update_handler_payloads[_with_context]` exposes the complete ordered slice
to validation and handling, including the optional stackful routine callback.
The runtime clones every Core `DoUpdate.input` Payload before invocation and
keeps routine arguments in the Workflow arena across suspension. Singular
registrations reject zero or multiple arguments with
`OdinWorkflowUpdateArgumentCountError`.

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

The Nexus application-metric probe distinguishes an exporter that never
started from counters that have not yet reached their expected values. It waits
within the scenario's 60-second Workflow timeout because retries, operation
timeouts, async completion, and cancellation all precede the final sample.

It first builds and runs `examples/core_connect`, which performs:

```text
runtime_new -> client_connect -> GetSystemInfo RPC -> client_free -> runtime_free
```

The request protobuf is empty by definition. The response is intentionally kept
opaque; receiving and freeing it proves the network and ABI path.

The suite immediately follows that raw bridge proof with the public, typed
Client path:

```bash
./scripts/system-info-integration-test.sh
```

It requires a non-empty live Server version and the advertised Schedule bit.
The response is decoded in a temporary arena and copied into an owned
`client.System_Info`; unit tests destroy the source bytes before reading the
result, a 500-case property varies all thirteen capability bits, and the Client
interceptor property includes the result-bearing operation. The live example
also requires its interceptor to enter before and unwind after the Core RPC.

### Standalone Activities

Standalone Activities require a newer Service than the repository's original
CLI 1.5.1/Server 1.29.1 baseline. The old baseline returns gRPC status 12 for
`StartActivityExecution`. Temporal CLI 1.9.1/Server 1.32.0 accepts the first
Odin client slice with its default dev-server configuration:

```bash
temporal server start-dev --headless
./scripts/standalone-activity-integration-test.sh
```

The focused script uses a unique task queue. It starts an Activity directly
from `examples/standalone_activity`, reads an owned description, lets the
existing `hello_activity` Worker receive it through Core's ordinary Activity
task poll, and waits for the typed result through `PollActivityExecution`. It
then describes the terminal input/outcome, retries an exact Activity-ID
Visibility List/Count query until the eventually-consistent record appears,
and deletes the execution. Two workerless scheduled executions prove
request-cancel and immediate termination, typed terminal errors, and deletion.

The public handle owns Activity/Run IDs and borrows the Client. Poll results
own their Run ID, result Payload, and encoded recursive Failure. Describe,
List, and Count return allocator-owned snapshots and require their matching
free procedures. Describe's callback entries are currently canonical protobuf
bytes so no unstable callback union is exposed prematurely. Start options,
Describe's long-poll token, list/count queries, and input Payload views are
borrowed only for the synchronous call. Start supports Header Payloads, Search
Attributes, static summary/details, Links, completion callbacks, and
`Use_Existing` attachment options. Start, Poll, Describe, the three mutations,
List, Count, and options update all participate in the typed Client interceptor
chain. Pause,
Unpause, and Reset share a typed control interceptor and are live-proven by a
workerless scheduled execution that remains paused across Reset, resumes, and
is then terminated and deleted. Before those controls, the same execution is
moved to a new task queue and receives a new Start-To-Close timeout and
Priority through `UpdateActivityExecutionOptions`; both the owned RPC response
and a subsequent Describe must observe the update. Update masks use exact
protobuf paths such as `task_queue.name`, with exported Odin constants for all
Server-supported paths. The response owns its copied strings and nested Retry
Policy/Priority values and must be released with
`standalone_activity_options_free`. This completes the four newer standalone
Activity request/response pairs in the pinned API.

The suite then repeats that executable with the Core-only Nexus lifecycle
spike:

```bash
TEMPORAL_CORE_NEXUS_POLL_SPIKE=1 ./scripts/run-core-connect.sh
```

It creates and validates a Nexus-enabled Core worker, enters the actual Nexus
poll path, initiates shutdown, requires the poll callback's shutdown sentinel,
and finalizes the worker. No endpoint is needed because this test proves the
bridge symbol, Worker options, Service validation, poll stream, and memory-safe
shutdown independently of handler dispatch.

It also runs an opt-in standalone Nexus capability probe:

```bash
TEMPORAL_CORE_STANDALONE_NEXUS_PROBE=1 ./scripts/run-core-connect.sh
```

The probe sends a valid `StartNexusOperationExecution` request to a deliberately
missing endpoint. Server 1.29.1 reports gRPC status 12/`UNIMPLEMENTED`; Server
1.32.0 recognizes the RPC and reports status 5/`NOT_FOUND`. The latter is the
expected positive capability result. Standalone Nexus is pre-release in Server
1.32.0 and is disabled by default. A local compatible server can be started
with Temporal CLI 1.9.1 or later:

```bash
temporal server start-dev \
  --headless \
  --dynamic-config-value 'nexusoperation.enableStandalone=true'
```

Run the focused 1.32 integration separately with:

```bash
./scripts/standalone-nexus-integration-test.sh
```

When the connected Service recognizes the RPC, the main integration script
also invokes this focused script. It creates an endpoint targeting
`odin-hello-nexus` and exercises three paths:

```text
Odin Client start -> Server 1.32 -> Core Nexus poll -> Odin hello handler
Odin Client poll  <- durable result <- Core completion <- JSON Payload

Odin Client cancel -> Server 1.32 -> Core CancelOperation -> Odin cancel handler

Odin Client Describe running state -> terminate -> Poll/Describe terminal state
Odin Client list/count exact Visibility query -> delete -> Describe NOT_FOUND
```

The lifecycle start registers the custom `OdinNexusTest` Keyword Search
Attribute, stores static summary/details, and makes a second
`Use_Existing` start that must return the original Run ID. The pinned request
also sends Links, completion callbacks, and on-conflict attachment flags. CLI
1.9.1/Server 1.32.0 currently returns zero Links/callbacks from Describe even
when they were sent on the original Start; exact unit wire/ownership tests cover
those experimental fields until a server release persists and returns them.

The lifecycle path exercises the exact generic Core bridge RPC names
`DescribeNexusOperationExecution`, `TerminateNexusOperationExecution`, and
`DeleteNexusOperationExecution`, then `ListNexusOperationExecutions` and
`CountNexusOperationExecutions`. No additional raw C ABI symbol is involved.
Every field returned from Describe is cloned out of the temporary protobuf
decode arena and released with `nexus_operation_description_free`.

The running Describe's opaque token is supplied to the terminal Describe so
the Service returns the changed state. CLI 1.9.1/Server 1.32.0 returns a
132-byte token even with the terminal response, contrary to the pinned proto
comment that it is absent when complete. Treat the token only as opaque input
for a follow-on Describe; its presence is not a liveness test. All six public
operation groups enter typed Client outbound interceptors before their existing
protobuf/RPC terminal. Unit and 500-case PBT tests require nested entry/unwind
order, short-circuiting, and mutable-input propagation.

On older Services the exact capability probe remains required and this focused
branch is skipped; all existing Workflow-to-Nexus integration still runs.
`RequestCancelNexusOperationExecution` acknowledges the request, not terminal
operation cancellation. The async integration therefore asserts handler-side
cancel dispatch/token identity rather than claiming an immediate terminal
outcome.

The established complete suite is green on Server 1.29.1. A wholesale run on
1.32.0 currently exposes a separate Workflow-originated Nexus timeout behavior
change before the conditional standalone branch: the worker sees 11 Nexus
tasks/no Core-local cancellation instead of 12/one. Keep the focused standalone
test and the 1.29.1 full-regression run as distinct matrix entries until that
upstream behavior is classified.

The suite next creates a uniquely named Nexus endpoint targeting
`odin-hello-nexus-workflow` and starts `examples/hello_nexus_workflow`. The same
Odin Worker runs the stackful caller Workflow, three completer Workflows, and
eight registered operation types across twelve Nexus tasks.
It first observes a non-retryable handler failure, then forces a one-second
schedule-to-close timeout and verifies that Core cooperatively cancels the
running Odin callback. It then starts an asynchronous completer Workflow with
Temporal's callback URL/headers and requires the Workflow result to resolve the
waiting Nexus call. The handler requires the caller Workflow-event link,
translates its Nexus URL/type pair through the generic Link parser, and attaches
the resulting common Link to both the callback and completer start. A second async operation is canceled; its
registered cancel handler uses the durable operation token as the backing
Workflow ID and calls the Odin Client cancellation API. Separate branches then
exercise every cancellation policy: `Abandon` detaches without dispatching a
protocol cancellation, `Try_Cancel` dispatches it while resuming the caller
immediately, `Wait_Cancellation_Requested` waits for Service acceptance, and
`Wait_Cancellation_Completed` waits for the backing Workflow's terminal
callback. Finally it starts two concurrent
`odin-greeting-service/hello` calls, wakes through the Nexus selector, joins
both Futures, and requires both to return the JSON string `"Hello Odin"`. The
example fetches the resulting server history and replays all branches offline
through Core before the endpoint is deleted. This proves synchronous, async
callback-completion, and async-cancel routes without a second language SDK:

```text
Workflow -> Temporal endpoint -> Core Nexus poll -> Odin callback
         <- Temporal result   <- Core completion <- Odin Payload
```

To exercise the worker manually after building the examples:

```bash
temporal operator nexus endpoint create \
  --name odin-nexus-local \
  --target-namespace default \
  --target-task-queue odin-hello-nexus-workflow

NEXUS_ENDPOINT=odin-nexus-local \
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  ./scripts/run-hello-nexus-workflow.sh
```

The example starts one caller Workflow plus three backing/completer Workflows,
handles the operation matrix, validates the result,
replays the fetched history, and exits. `scripts/integration-test.sh` is the
canonical repeatable harness. Endpoint names permit letters, digits, and
hyphens, but not underscores. Core cancel tasks are routed cooperatively.
Protocol `CancelOperation` requests invoke an explicitly registered handler;
registrations without one return a non-retryable `NOT_IMPLEMENTED` response.
Worker statistics distinguish these paths:
`nexus_cancellations_received` counts Core-local `CancelTask` notifications for
an in-flight handler, while `nexus_cancel_operations_received` counts protocol
`CancelOperation` tasks. The live matrix requires exactly one local timeout
cancellation and three protocol cancel operations; the latter count also proves
that `Abandon` did not dispatch a fourth request.
`nexus.result_async(Output, token)` populates both the modern operation token
and deprecated operation ID for compatibility when token storage already
outlives the handler adapter. `nexus.result_async_copy(Output, token)` preserves
a temporary token until completion encoding. `Start_Workflow_Options` accepts
borrowed `nexus_completion_callbacks`; the Service uses those URL/header values
to deliver the completer Workflow's eventual terminal result to the Nexus
operation. `client.link_from_nexus` converts a callback-scoped raw Nexus
URL/type pair to the generic pinned-schema Link model. It owns all decoded
strings, so the example calls `client.link_free` after the synchronous
Workflow-start encoding; the callback and ordinary Workflow-start options only
borrow those values. The converter accepts upstream's WorkflowEvent,
NexusOperation, Activity, and Workflow URL forms and the legacy WorkflowEvent
form observed from the pinned Core/server combination. BatchJob can be
constructed and encoded directly but has no upstream Nexus URL mapping.
Signal-With-Start rejects completion callbacks because its pinned request
schema has no completion-callback field.

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

The Workflow sequence also creates a uniquely named deployment-based Worker on
an isolated task queue,
marks its `.Pinned` version Current through Temporal CLI, and executes a real
Workflow through it. Describe output must report the exact deployment name and
Build ID. Cleanup is best-effort because CLI 1.5.1 exposes an unversioned
transition that its bundled dev Server 1.29.1 currently rejects.

A separate isolated-queue case starts revision one of a Workflow and records a
one-hour Timer. It then stops that Worker, starts revision two—which emits an
incompatible completion—with per-type terminal nondeterminism enabled, and
signals the execution to force replay. The Service history must close with
`WORKFLOW_EXECUTION_STATUS_FAILED` and Core's `[TMPRL1100]` nondeterminism
message. This proves the policy changes Service-visible behavior rather than
only filling C fields.

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
The result also exposes the complete tree through
`workflow.activity_failure_tree`; `workflow.child_workflow_failure_tree` does
the same for Child results. Unlike Client-decoded failures these pointers are
borrowed from the Workflow instance arena: inspect them synchronously and never
call `failure_free`. The older direct Activity/Application and Child fields are
compatibility views over this tree, not separately cloned ownership graphs.
Signal-external-Workflow and request-cancel-external-Workflow failures expose a
borrowed `result.failure` pointer with the same lifetime; their `cause` enum
remains the concise operation-specific classification.

The immediately following `terminal-failure` case deliberately leaves the same
Activity failure unhandled. Odin emits `FailWorkflowExecution`, Temporal stores
the failed close event, and `examples/client_start` retrieves and decodes its
owned `ActivityFailure -> ApplicationFailure` chain. The assertion covers the
Activity ID/type/retry state and Application message/type/detail/delay after the
original Core RPC buffer has been released.

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
public Odin Client before the Workflow has registered that handler. A separate
`register_signal_handler` Signal resumes main; registration synchronously drains
the earlier invocation, and the Signal routine schedules an Odin Activity and
suspends. After resolution it mutates Workflow state; a typed Query observes
the new name. A later unregistered `approval` signal takes the buffered-await
path, cancels main's timer, and completes the Workflow. Core evicts between
stages, and replay reconstructs the late registration, handler stack, and its
Activity command/resolution.

The following `signal-handler-failure` case registers a handler that returns an
intentional language failure. The script requires a failed activation, a Core
cache eviction, replay initialization on a new Odin stack, a repeated handler
failure, and an `EVENT_TYPE_WORKFLOW_TASK_FAILED` in Temporal history. Core
marks history reconstruction as replay; the Signal job in the new Workflow Task
attempt is not itself marked replay. Because this class of failure is retryable
by design, the script terminates that execution and starts a command-only
Workflow on the still-running Worker to prove failure isolation.

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
The Client connection property adds 500 generated binary metadata values,
retry durations/counts, API-key states, and explicit TLS states. It checks the
exact C ABI buffers and requires API-key-only configurations to enable TLS.
Focused validation tests cover all connection option layouts, invalid metadata,
one-sided mTLS material, sub-millisecond retry values, and incomplete proxy
credentials.
The Workflow Payload property adds another 500 generated structs, creates and
evicts a typed fiber for each, and requires byte-identical completion after
fresh-stack replay. The Activity-options property generates 500 valid timeout,
retry, queue, ID, cancellation, eager-execution, versioning-intent, priority,
and summary combinations. It checks every encoded `ScheduleActivity` field plus
the command metadata envelope and requires byte-identical commands after
eviction and fresh-stack replay. A separate 500-case Local Activity options
property covers
all three cancellation modes, timeouts, retry fields, local retry threshold,
marker-input inclusion, summary metadata, `ScheduleLocalActivity` encoding, and
exact replay. Live CLI-devserver cases additionally require remote Activity
priority/summary on `ActivityTaskScheduled` and Local Activity summary on the
persisted `core_local_activity` marker. The
Workflow-properties property generates 500 memo/Search Attribute keys and
values, Keyword/Text types, and independent removals. Each case verifies local
read-after-write, command counts, eviction, and byte-identical fresh-stack
replay. The focused unit test additionally checks every exposed `workflow.Info`
field—including the recursive continued Failure, repeated last-completion
Payloads, and cron scheduling interval—and decodes the exact `ModifyWorkflowProperties` and
`UpsertWorkflowSearchAttributes` payloads and type metadata.
Another 500-case property generates Client start headers, memo, Search
Attributes, raw Payload bytes, keys, all seven Temporal type labels, ID
policies, Retry Policies, delays, priorities, and eager-start intent. It
frees each source Payload before request encoding, then decodes the complete
`StartWorkflowExecutionRequest` to prove builder ownership and wire fidelity.
An additional 500 cases generate zero to twelve ordered Query arguments with
arbitrary Payload bytes, per-argument metadata and external payload details,
plus query headers, query types, and reject conditions. The source Payloads
remain caller-owned, and repeated encoding must remain byte-identical.
Another 500 cases do the same for zero to twelve ordered Update arguments while
also checking Workflow/run identity, Update/request IDs, headers, and
byte-identical request encoding.
Configured Client identity is additionally checked on exact protobuf requests
for Workflow start, Signal-With-Start, Signal, Update, Update-With-Start,
cancel, reset, terminate, Schedule create/update/backfill/patch/delete,
Workflow options/pause, asynchronous Activity completion, and Nexus operations.
The request encoders fall back to the documented default only when invoked
directly with an empty identity.
Another 500 generated Workflow Reset requests vary event IDs, optional Run IDs,
and every signal/Update/Nexus reapply exclusion. They decode the exact pinned
request type and require namespace, client identity, target, reason, request ID,
and exclusion order to survive encoding. Focused unit tests reject a nonpositive
event ID and verify Client interceptor nesting and option mutation.
Another 500 cases generate Workflow-side Nexus schedule commands across opaque
Payload bytes, endpoint/service/operation strings, string headers, all four
cancellation policies, and independent schedule-to-close, schedule-to-start,
and start-to-close timeouts. The focused runtime test additionally forces cache
eviction and requires byte-identical replay before applying both the synchronous
start and completed-result jobs in one activation. Separate 240-case properties
exercise every cancellation-policy transition and every terminal operation
state. The latter applies duplicate/conflicting late success results twice and
requires the first terminal outcome to remain unchanged.
The
Child Workflow options property
generates another 500 valid ID, queue, timeout, Retry Policy, cancellation-type,
Parent Close Policy, ID-reuse, cron, versioning-intent, priority, memo, typed
Search Attribute, static summary, and static details combinations, plus generated
signal names, signal summaries, and zero to twelve decorated JSON Payloads. It
checks every encoded `StartChildWorkflowExecution` field, including the
enclosing command metadata,
and every `SignalExternalWorkflowExecution` field and metadata summary, and
requires both commands to be byte-identical after eviction and fresh-stack
replay. These generated Workflow-operation cases now also create run-local
cancellation tokens and select them directly through Activity, Local Activity,
Child Workflow, Nexus, and child/external Signal options. A focused unit trace
checks the stored token IDs, all four Core cancellation-command families,
foreign/stale-token rejection, and terminal completion on the following
activation. A 240-case property varies whether Child start stays pending or
resolves in the token-cancellation activation, covers both Core job orders,
and requires zero Child Signal commands plus byte-identical fresh-stack replay.
The direct Child-start waiter regression remains pending until Core supplies a
start resolution, proving that only the Signal convenience gained this wakeup.
The live CLI-devserver case uses Client Signal-With-Start to place the
cancellation Signal in the initial Workflow activation. It requires one
started/completed Child Workflow and zero
`SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED` history events, avoiding a
wall-clock race between a normal Client Signal and Child-start resolution.
The external Workflow
property generates 500 namespace/Workflow ID/optional Run ID identities,
signal names, summaries, JSON payloads, and cancellation reasons. It checks exact
`NamespacedWorkflowExecution` targets for signal and cancellation commands and
requires both completions to be byte-identical after eviction and fresh-stack
replay. The Continue-As-New property generates 500 replacement Workflow types,
task queues, zero to twelve ordered input Payloads, valid Run/Task timeout
pairs, Retry Policies, backoffs, memo/Search Attribute overrides, and
versioning choices. Empty identity options exercise reuse of the current
registration; absent maps exercise inherited state; all public versioning
values must map to the pinned Core enums;
every `ContinueAsNewWorkflowExecution` must encode exactly and remain
byte-identical after fresh-stack replay. The Client property generates another
500 typed Workflow results, wraps each in a real close-event history response,
destroys the encoded response, and requires the owned result Payload to retain
the generated value. A separate 500-case Client history property varies event
counts, IDs, event types, versions, task IDs, ignore flags, archival state, and
opaque pagination tokens. It destroys the source response and requires every
owned event plus its canonical protobuf encoding to remain readable. Focused
unit tests perform the same arena-lifetime check for rich Describe results,
including memo/Search Attribute Payloads, Workflow pause metadata, pending
Activity/child/Nexus summaries, recursive Nexus failures, and owned canonical
callback protobufs. The same ownership test covers legacy and current Worker
Deployment identity, transitions (including a nil target meaning a transition
to unversioned workers), execution overrides, Activity pause rules, and
effective Activity options. It also destroys the decode buffer before checking
owned parent/root identities, auto-reset points, expiration/reset timestamps,
request-ID event mappings, and nested time-skipping configuration/state. The
live deployment scenario additionally requires the Odin Describe path to return
the Deployment name and Build ID. The
Visibility property adds 500 pages with zero to sixteen execution summaries,
all current Workflow status values, and opaque terminal/non-terminal tokens,
plus zero to eight grouped count results with arbitrary ASCII Payload bytes.
It destroys each encoded response before inspecting the owned Odin value;
focused tests additionally cover memo/Search Attribute Payloads, parent/root
identity, priority, deployment name, timing, counters, and invalid Client use.
The Schedule property generates another 500 valid interval/phase, overlap,
paused/limited-action, and immediate-trigger combinations, decodes each
`CreateScheduleRequest`, and checks the exact Workflow action and policy wire
contract. A second 500-case Schedule property generates structured calendar
ranges and full replacements, preserving every range and optimistic conflict
token through `UpdateScheduleRequest`. Focused ownership tests destroy encoded Describe/List/Count responses
before reading action inputs, recent executions, memo, pagination tokens, and
grouped count Payloads.
The query property generates 500 success/failure
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
then starts an accepted Update whose Activity returns a structured application
failure. The Client requires the resulting `ActivityFailure ->
ApplicationFailure` tree and the Workflow continues. It then starts two typed
Updates using the public start/wait handle API. Their
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
It then starts another worker for the already completed repeated-argument
Workflow, calls `examples/client_control` with `TEMPORAL_CLIENT_ACTION=reset`
and event ID 4, and requires the returned new Run ID to complete with the same
typed `Odin:42:true` result. This verifies Odin request/response ownership,
Core's generic RPC dispatcher, Server reset history construction, and ordinary
activation/completion execution in one path.
In a separate execution it dispatches a typed Signal to an Activity-awaiting
handler, queries the handler's state mutation, and then completes main through
the ordinary buffered Signal API.

## Manual run

```bash
temporal server start-dev \
  --dynamic-config-value 'system.enableActivityEagerExecution=true'
./scripts/run-core-connect.sh
./scripts/run-hello-activity.sh
```

The dynamic value makes eager Activity delivery reproducible across server
versions. CLI 1.5.1's bundled Server 1.29.1 declined the eager request without
it in the first live run; current Temporal Server source defaults the setting to
true. Either way, Core safely falls back to the normal Activity task queue when
the Service declines. Run the focused proof with
`./scripts/run-eager-activity.sh`. Its success line proves the Workflow and
Activity completed, while the integration suite additionally requires Core's
`Adding 1 activity tasks received from WFT complete` event.

`./scripts/run-eager-workflow.sh` is self-contained and now also demonstrates
the Service-to-offline-replay loop. It starts and completes a Workflow, calls
`client.fetch_history_protobuf` to follow every history page, creates a replay
worker without a Client connection, registers the same Workflow definition,
and requires a compatible `LANG_REQUESTED` eviction. The full integration suite
asserts this output.

Set `TEMPORAL_REPLAY_HISTORY_OUT` to make the eager example persist the exact
binary History it fetched. `./scripts/run-replay-history.sh` accepts one or more
binary History files or canonical JSON files from
`temporal workflow show --output json`, runs them as one Core replay batch,
prints one PASS/FAIL line per file, and exits non-zero if any result is
incompatible or invalid. JSON is converted with the optional pinned-descriptor
native bridge before the unchanged binary replay API is called. The example
registers `odin_eager_workflow`; application repositories should use the driver
with their own complete Workflow registry.

Run `./scripts/history-json-integration-test.sh` for the focused JSON proof. It
starts `temporal server start-dev` when needed, completes the eager Workflow,
exports its real CLI JSON, converts it, and replays the result through Core.
The unit suite checks canonical field names, enum names, timestamps, base64
Payload bytes, oneof selection, malformed input, and unknown-field rejection.
A 1,000-case PBT property sends arbitrary binary data—including embedded zero
bytes—through the raw ABI and requires exactly one valid, freeable owned result.

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

`TEMPORAL_ODIN_SCENARIO=cancellation-tree` exercises selective language-side
cancellation instead of canceling the Workflow Execution. It starts child,
sibling, and detached Activity branches by passing their tokens directly in
`Activity_Options`, receives `cancel-child-token`, emits a
Core Activity-cancellation command only for the child branch, and requires the
sibling and detached branches to complete. The Workflow itself completes
successfully and replay rebuilds the same token IDs and commands.

`TEMPORAL_ODIN_SCENARIO=cross-routine-cancellation` starts a stackful Signal
handler waiting for a Signal and an accepted Update handler waiting for a
durable timer under one child token. A public client Signal makes main cancel
that token. The same Workflow Activation must cancel the timer, finish both
handlers, return the Update result, release main's handler drain, and complete
the Workflow. This is the live fixed-point scheduler acceptance case.

`TEMPORAL_ODIN_SCENARIO=cancellation-deadline` creates a child token with a
one-second timeout, then starts a 24-hour durable sleep inside that scope. Core
fires the deadline timer; the same activation cancels the sleep timer, resumes
the main fiber, and completes successfully. The test proves the deadline uses
Temporal history rather than a host-clock/background-thread shortcut.

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

```bash
./scripts/child-cancellation-integration-test.sh
```

The focused script runs the explicit-cancellation and duplicate-child cleanup
cases without the rest of the integration matrix.

- successful typed child result delivery;
- a codec-neutral three-argument Child Workflow start that preserves argument
  order, custom metadata, external payload details, and interceptor headers;
- a failing child whose parent catches a structured Temporal child wrapper and
  typed Application Failure detail;
- cancellation of a waiting parent through the public Odin Client, which emits
  `CancelChildWorkflowExecution(seq=1)` and does not close the parent until Core
  resolves the child as canceled;
- signaling a child through its Workflow-local Future and waiting for Core's
  `ResolveSignalExternalWorkflow` acknowledgement, using three ordered Payloads
  with custom metadata and external payload details; the initiated event must
  retain the command's UI summary;
- explicit child cancellation through the Future while the parent remains live
  and completes normally after observing the canceled child result; the child
  is deliberately suspended before option-metadata assertions so cancellation
  cannot race an unrelated early return;
- a second start with the same running child ID, producing Core's
  `WorkflowAlreadyExists` start resolution before the first child is canceled;
- two independent Workflows where one sends an external signal, waits for
  `ResolveSignalExternalWorkflow`, requests target cancellation, waits for
  `ResolveRequestCancelExternalWorkflow`, and completes while the target closes
  as canceled; the signal carries three ordered Payloads and its UI summary
  across both workers;
- a Workflow that emits `ContinueAsNewWorkflowExecution` with replacement
  input/timeouts/memo/typed Search Attribute, is evicted, starts a second run
  under the same Workflow ID, reads the replacements there, and returns the
  second run's result to Temporal CLI;
- a codec-neutral Continue-As-New Workflow that carries three ordered Payloads,
  metadata, and external payload details into its second run; and
- a Workflow that takes its new `patched` branch, emits `SetPatchMarker`, and
  leaves a server-side `core_patch` marker visible through `temporal workflow show`;
- a Workflow that reads its Core-supplied identity, starting memo, and starting
  Search Attribute, then upserts/removes memo fields and upserts a typed Search
  Attribute. The test observes `EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED` and
  `EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES` in Service history.

The unit suite additionally starts a child with three ordered Payloads, evicts
the parent immediately after its start
command, replays onto a fresh stack, compares the completions byte for byte,
then applies separate start and completion resolutions. It verifies the exact
cancellation reason, structured failure ownership after the activation decode
arena is gone, and lossless propagation of an unhandled original Temporal
Failure wrapper. The 500-case Child Workflow property varies every public start
option listed above, signal names, and zero to twelve ordered start/signal
Payloads with metadata and external details, then requires exact fresh-stack
replay of both commands. The CLI-devserver path additionally has the child read
its memo, typed Search Attribute, and priority, then checks the parent's Service
history for ID reuse, memo, Search Attributes, priority, and static UserMetadata.
A cancellation unit test also requires an in-flight child signal
to emit both `CancelSignalWorkflow` and `CancelChildWorkflowExecution` before
the parent closes as canceled. Another unit activation combines
`InitializeWorkflow` and `CancelWorkflow` around an immediately returning
procedure and requires cancellation to be installed before its first fiber
resume. This covers the job batch observed live when a Child cancellation
arrives before the Child's initial Workflow Task is handled.
External-operation unit tests inspect namespace, Workflow ID, Run ID, signal
Payload, cancellation reason, success/failure cause, and fresh-stack replay
bytes. The 500-case external Workflow property likewise varies its signal from
zero through twelve ordered Payloads. The live and
property suites carry three and zero-to-twelve Continue-As-New arguments,
respectively. Continue-As-New unit and property tests also cover replacement
memo, typed Search Attributes, every versioning value, and exact
presence/absence encoding. Continue-As-New unit tests additionally prove the
non-returning terminal path, default type/queue reuse, options encoding, invalid
option failure, second-run initialization, and exact replay. Patch unit tests
cover fresh history, replay with and without a preemptive notification, Core's
actual initialize-then-notify job order, memoization, and deprecated markers.

Run only the Workflow-properties live path by registering its custom Search
Attribute once, starting the worker, and executing the command-only Workflow:

```bash
temporal operator search-attribute create \
  --name OdinSdkKeyword \
  --type Keyword

TEMPORAL_ODIN_SCENARIO=workflow-properties \
./scripts/run-hello-workflow.sh

TEMPORAL_CLIENT_SCENARIO=workflow-properties \
TEMPORAL_CLIENT_WORKFLOW_ID=odin-properties-manual \
./scripts/run-client-start.sh
```

The registration command is only needed for a new dev server. The Workflow
reads the Odin Client's header, memo, and Search Attribute and returns
`Workflow properties updated`; its history contains two memo-property events
(one upsert and one empty-Payload removal) and one Search Attribute upsert
event. The same client invocation now calls `DescribeWorkflowExecution`, checks
the completed identity/status plus updated memo/Search Attribute keys, then
walks `GetWorkflowExecutionHistory` with a requested page size of one until the
opaque next-page token is empty. It also executes
`ListWorkflowExecutions` and `CountWorkflowExecutions` with an exact
`WorkflowId` Visibility query, requiring the completed typed summary and a
non-zero count. This is the live ownership, Visibility, and pagination
acceptance path for the typed read APIs. Counts are approximate by Temporal's
contract; the integration assertion therefore only requires the unique test
execution to be visible, not an exact global total.

The normal query/update live paths add an owned `Headers` value to each query.
The Service copies it into Core's Query activation; the contextual Odin handler
decodes `caller = "odin-client-control"` and returns it in the result. `Headers`
owns cloned keys and Payloads until `headers_free`; query options only borrow it
for the synchronous RPC.

### Schedule lifecycle live path

Run the Schedule client against the patch Workflow worker:

```bash
TEMPORAL_ODIN_SCENARIO=schedule-backfill ./scripts/run-hello-workflow.sh

TEMPORAL_SCHEDULE_ID=odin-schedule-manual \
./scripts/run-schedule-lifecycle.sh
```

The client creates a paused 24-hour interval Schedule, describes it, replaces it
with a 12-hour Schedule using the Describe conflict token, proves that a
deliberately different update using a stale token is not applied, queries the
next two days of matching times, lists it, and unpauses and pauses it through
`PatchSchedule`. It then backfills the previous 24-hour window with
`BUFFER_ALL`, waits for exactly two Workflow executions, triggers a third
execution manually, waits for its result, and deletes the Schedule. This
exercises Create/Describe/Update/List/ListScheduleMatchingTimes/Patch,
multi-execution backfill, Trigger, and Delete through SDK Core and the CLI dev
server. The dedicated worker scenario has a three-execution bound so shutdown
also proves that all backfill and trigger work was dispatched.
CLI Server 1.29.1's legacy scheduler acknowledges the stale update RPC before it
checks and ignores the conflict asynchronously, so the example reports both
`stale_conflict_guarded=true` and `stale_rpc_rejected=false`.
`CountSchedules` is also called; CLI Server 1.29.1 returns gRPC status 12
(`UNIMPLEMENTED`), so the example reports `count_supported=false` and continues.
The generated request/response and ownership tests still cover Count, and a
newer-server acceptance run is required before claiming its live support.

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
options. `client.connect_with_options` exposes TLS/mTLS, API keys, connection
metadata, retry/keepalive, proxy, DNS load balancing, compression, identity,
and warning thresholds. The checked-in example exercises every non-TLS network
option against the CLI dev server and calls the live metadata/API-key update
symbols (the smoke API key is immediately cleared before a Workflow RPC).
Cloud and mTLS credentials are not stored in this repository, so certificate
verification and Temporal Cloud authentication still require an external
credentialed acceptance environment.

The public Client can also use Core's Temporal TOML/environment profile loader:

```odin
profile, load_error := client.load_config_profile({profile = "dev"})
defer client.error_free(&load_error)
assert(load_error.message == nil)
defer client.config_profile_free(&profile)

temporal_client, connect_error := client.connect_with_config_profile(&profile)
defer client.error_free(&connect_error)
assert(connect_error.message == nil)
defer client.close(&temporal_client)
```

With no explicit source, Core checks `TEMPORAL_CONFIG_FILE` and its default
`temporal.toml` location; `TEMPORAL_PROFILE` selects a profile and other
`TEMPORAL_*` values override it. Tests can pass inline bytes and an exact
environment map instead. `Config_Profile` owns its decoded strings, TLS source
data, codec settings, and gRPC metadata until `config_profile_free`.

For the focused live proof, start the CLI dev server and run:

```bash
temporal server start-dev --headless
./scripts/config-profile-integration-test.sh
```

The script loads `examples/config_connect/temporal.toml` through the pinned C
bridge, connects through the public Client, and requires a non-empty
`GetSystemInfo` server version. Remote codec endpoint/auth values are currently
configuration data only. The pinned bridge also omits Core's
`disable_host_verification` field from its result JSON, so that setting cannot
yet be applied by Odin.

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

To exercise Core's timer-backed Local Activity retry contract manually, run
the worker with `TEMPORAL_ODIN_SCENARIO=local-activity-retry` and execute
`odin_local_activity_retry_workflow` on the same task queue. Its first local
attempt returns a retryable failure with a 250 ms next-retry delay. Because the
Workflow sets a 10 ms local retry threshold, Core returns `DoBackoff`; Odin
emits a durable Workflow timer and schedules attempt 2 after it fires. The
history contains both `core_local_activity` and `EVENT_TYPE_TIMER_STARTED`.

### Interceptor headers

`temporal/headers` is the shared ownership layer for interceptor-style Temporal
Payload maps. A builder owns cloned keys and Payloads until `headers_free`; a
Client RPC or Workflow operation only borrows the builder during the call.
Workflow operations clone it into the run arena before they can suspend.

Ordinary Client Signals use `signal_workflow_json_with_options`, the singular
Payload variant, or `signal_workflow_payloads[_with_options]` with
`Signal_Workflow_Options.headers`. The repeated form preserves all ordered
Payloads, metadata, and external-payload details. Workflow code receives them
through `await_signal_payloads[_with_context]` or
`set_signal_handler_payloads`; singular adapters reject zero or multiple
arguments with `OdinWorkflowSignalArgumentCountError`. Client Updates use
`start_update_json_with_options`, `start_update_payload_with_options`, or
`start_update_payloads_with_options` with
`Update_Workflow_Options.headers`. Workflow Activity,
Local Activity, Child Workflow, and child/external Signal options expose the
same `workflow.Headers` alias. Incoming values are borrowed: use
`workflow.signal_header_*` while the Signal context is valid, and
`activity.header_*` during the Activity callback.

Context-aware Workflow Update routines additionally receive
`^workflow.Update_Context`. Core activation data is cloned into the Workflow
run arena before validation or handler start, so Update ID, name, client
identity, and headers remain valid across Activity/timer/signal suspension and
fresh-stack replay. `workflow.update_header_*` reads the map, and
`workflow.current_update(ctx)` identifies the currently executing Update
routine. Do not retain these views after run eviction.

The offline properties generate arbitrary header strings for ordinary Client
Signals, Client Updates, and every implemented outgoing Workflow command family, evict the
Workflow, and require byte-identical commands after fresh-stack replay. The
full integration script additionally runs `activity-headers` and
`signal-headers` scenarios against `temporal server start-dev`; the normal
Update scenario proves its header before validation and again after an Activity
resume. Together these cover both directions across Core rather than only
protobuf round trips.

### Activity interceptor chain

Register Activity interceptors while the Worker is still configuring with
`worker.add_activity_interceptor`. Each entry may implement `execute`,
`heartbeat`, or both. Registration order is outermost-first: execute callbacks
enter in forward order, the registered Activity runs at the terminal, and
callbacks unwind in reverse order. Calling `next` is explicit, so an
interceptor can reject or synthesize a result without invoking later entries.
Heartbeat calls use the same nesting model and terminate at Core's existing
`record_activity_heartbeat` operation.

The SDK borrows `Activity_Interceptor.user_data` until `worker.close`; it never
frees application state. Execute inputs, Payload views, `activity.Context`, and
`next` continuations are invocation-scoped and must not be retained. Results
retain their normal callback lifetime. Interceptor registration is
rejected after `worker.run` starts, which keeps the chain immutable while the
Activity pool invokes it concurrently.

A unit test covers nested execute order, short-circuiting, and heartbeat order.
A 500-case property varies chain depth and the short-circuit position. The
`activity-interceptor` integration scenario proves execute-before/after and
heartbeat-before/after across a real Core worker and CLI dev server.
The `multiple-activity-arguments` scenario separately sends three JSON Payloads
through Workflow command encoding, SDK Core, Temporal Server, Activity polling,
and the codec-neutral Odin callback. Unit replay checks byte-identical commands,
and a 500-case property covers 0–12 ordered arguments for remote and Local
Activities while preserving metadata and external-payload details.
The `multiple-workflow-arguments` scenario starts a Workflow through the Odin
Client with three JSON Payloads, carries them through SDK Core and the Temporal
CLI dev server, and returns `Odin:42:true` from a codec-neutral Odin Workflow.
An offline eviction/replay test preserves order, metadata, and external-payload
details, while a separate 500-case property covers deterministic Client request
encoding for 0–12 arguments.
The `multiple-signal-arguments` scenario carries three Signal Payloads from the
Odin Client interceptor through Core and the CLI dev server into a suspended
Odin Workflow. Its unit test evicts and replays the run byte-for-byte, and the
500-case Signal property varies 0–12 arguments while retaining ordering,
metadata, external details, headers, and deterministic request bytes.

### Client outbound interceptor chain

`Connect_Options.interceptors` installs ordered, typesafe outbound interceptors
for Workflow start, Signal-With-Start, Signal, Query, Update start/result waiting,
cancellation, termination, Workflow result waiting, Visibility list/count,
standalone Nexus start/poll/describe/mutation/list/count, and Schedule
Describe/List/Count/Matching-Times, plus System Info discovery. Each operation has a dedicated
mutable input and result-bearing `next`; no result passes through an untyped
pointer. Visibility and Schedule list/count interceptors may replace query and
pagination options. Schedule matching-time interceptors may replace the time
range. Every read continuation returns its allocator-owned public result type
under the same free-procedure contract as the direct call. Schedule Create has
its own handle-returning continuation. The six Error-returning mutations share
one semantic input with a `Schedule_Mutation_Kind`; operation-specific fields
remain borrowed for the synchronous chain. Runtime replacement of ASCII/binary
RPC metadata and the API key uses a typed connection-mutation continuation;
its slices and string are likewise borrowed only during the call. Interceptors
must treat the API-key field as secret material.
Signal-With-Start exposes both user Payloads, mutable start options, and the
single Header map the upstream RPC assigns to its Signal event. Cancellation
exposes its reason and request ID. Termination exposes its reason plus an
optional codec-neutral details Payload. Registration order is outermost-first
and unwind order is reversed. Skipping `next` deliberately short-circuits the
RPC, so a successful start/update/query short circuit must return a valid
operation-specific handle or Payload.

`get_result` wraps the complete semantic wait rather than each internal history
long poll. Its mutable `follow_runs` field controls whether the terminal follows
Continue-As-New and retry runs. A successful continuation returns an owned
Payload; an interceptor that replaces it must follow the same ownership rule.
`wait_update` similarly surrounds the complete Update result long poll. Its
mutable `Update_Wait_Options` can narrow the timeout or replace the borrowed
cancellation token, and its successful continuation returns an owned Payload.

Before the chain runs, the SDK clones existing headers into a per-call owned
`Headers` value. Workflow-start headers are adapted from `Start_Metadata`; the
other operations clone their option header builder. Interceptors mutate
`input.headers`, and the terminal encoder uses exactly that working copy. It is
freed after the synchronous chain returns, while the caller's builder remains
unchanged and reusable. Interceptor inputs and `next` values must not be
retained. The Client copies interceptor records during connect but only borrows
their `user_data` until `close`.
Client calls may run concurrently; mutable interceptor user data therefore needs
application-level synchronization.

The unit and 500-case PBT tests cover nested order, short-circuiting, header
isolation, Signal-With-Start, mutable cancellation/termination inputs,
Visibility queries, the complete standalone Nexus surface, and the complete
Schedule surface. The generative chain now varies twenty-two operation
families. The `workflow-properties` live scenario
wraps both Visibility RPCs; the Schedule lifecycle wraps every read RPC and
all seven create/mutation paths through Core. Every `client_start` live scenario
also replaces RPC metadata, sets an API key, clears it, and requires the
connection-mutation continuation to observe all three Core calls.
The same example routes its pre-canceled Update wait through the result-bearing
interceptor and requires unwind after Core returns `.Wait_Canceled`.
`client_start` injects start and Signal-With-Start headers solely through its
interceptors; `client_control` does the same for Signal, Query, and Update. The
full dev-server suite requires those headers to arrive in Workflow code,
including Update context after Activity suspension, and requires
Signal-With-Start, cancellation, and termination chains to unwind only after
their real Core RPC succeeds. The result hook remains entered across the Core
long poll and exits only after a terminal Workflow result is owned by Odin.

### Workflow outbound interceptor chain

Register `workflow.Outbound_Interceptor` values with
`worker.add_workflow_interceptor` while the Worker is configuring. Interceptors
registered before a Workflow are copied when its runtime is created;
interceptors registered afterward are copied into every existing runtime.
Registration after `worker.run` starts is rejected. The current typed hooks are
`start_activity`, `start_local_activity`, `start_child_workflow`,
`start_nexus_operation`,
`signal_child_workflow`, `signal_external_workflow`,
`cancel_external_workflow`, `continue_as_new`, `start_timer`, `sleep`, and
`cancel_child_workflow`. Each has a semantic input, its natural result type,
and an explicit `next` continuation. Continue-As-New and its continuation are
`-> !`, preserving the terminal control-flow contract. A sleep interceptor's
`next` remains on the Workflow fiber stack while the durable timer is pending,
so code after `next` runs only after timer resolution or cancellation. In
contrast, `start_timer` returns its `Timer_Future` and unwinds immediately.
The terminal sleep continuation calls through the timer-start chain after all
sleep interceptors have applied their duration changes. The mutable Timer input
contains `Timer_Options`: duration, optional explicit cancellation token, and
summary. Empty token inherits the current fiber token; non-empty summary is
encoded as command `UserMetadata.summary`. `start_timer_with_options` returns a
Future; `sleep_with_options` applies the same record to the suspending form.

The SDK clones the caller's command headers into a Workflow-run-arena working
map before invoking the chain. Every interceptor sees the same mutable working
map, the terminal continuation persists it into the operation, and the original
builder remains unchanged. External cancellation has no header field, but its
identity and reason remain mutable semantic input. The record itself is copied;
child/external signal records include their mutable summary, which the terminal
continuation JSON-encodes into owned command metadata before suspension.
`user_data` remains borrowed until `worker.close`. Inputs, header pointers, and
`next` values are valid only during that call. Continue-As-New's temporary
working map is reclaimed with the terminal run arena because the call cannot
return to execute a defer.

Nexus uses its protocol-specific UTF-8 header slice rather than the generic
Payload Header builder. The `start_nexus_operation` input exposes endpoint,
service, operation, one Payload view, and the complete options record. All are
call-scoped and mutable. Its terminal continuation clones the Payload, strings,
timeouts, cancellation policy, and headers into run-arena ownership before
returning the `Nexus_Operation_Future`; interceptor-owned replacement slices
therefore need remain valid only until `next` returns.

These callbacks run on the Workflow fiber and execute again during history
replay. They must be deterministic: use no network/filesystem access, wall
clock, process randomness, or external mutable state that affects the result.
Use Activities or Temporal-aware Workflow primitives for those operations.
Use the Worker-owned `workflow.Logger` for diagnostics; it suppresses replayed
history events by default. Interceptors must not call host loggers directly.

The unit suite verifies nested entry/unwind order, caller-header isolation,
command headers, timer-duration and cancellation-reason replacement, delayed
sleep-interceptor unwind until timer resolution, immediate timer-start unwind,
summary metadata, and explicit-token cancellation,
Continue-As-New header merging, Nexus semantic/header replacement and nested
unwind, and byte-identical completions after
fresh-stack replay. A 500-case PBT property varies seven operations, chain
depth, and short-circuit position on the outbound side; the shared property now
varies thirteen operations after including all five inbound hooks.
CLI-devserver scenarios read interceptor-injected
headers from real Activity, Child Workflow, Signal, and next-run Workflow
contexts after they cross SDK Core. The selector scenario proves the
timer-start hook and interceptor-replaced summary in real Server history around
Core command creation and cancellation. Its 180-trace PBT property also varies
summary values and requires byte-identical replay. The Child
Workflow scenario also proves
sleep suspension/unwind and explicit Child cancellation hooks around real Core
commands.

### Workflow inbound interceptor chain

Register `workflow.Inbound_Interceptor` with
`worker.add_workflow_inbound_interceptor` while the Worker is configuring. The
current hooks are `execute_workflow`, `handle_query`, `handle_signal`,
`handle_update_validator`, and `handle_update_handler`; registration order is
outermost-first, and registration works both before and after Workflow
registration. The Worker copies each record into every Workflow runtime.
`user_data` remains application-owned and borrowed until `worker.close`.

`execute_workflow` runs on the main Workflow fiber. Its `next` continuation
contains the complete user Workflow call, so it remains suspended on that fiber
across Activities, timers, Signals, and any other durable waits. Code after
`inbound_execute_workflow_next` runs only when the Workflow returns with a
result or failure. The call-scoped input and continuation remain valid on the
suspended fiber stack, but must never be retained elsewhere.

`handle_query` wraps the synchronous registered Query handler. It receives the
borrowed `Query_Context`, Workflow type, and complete Payload-view slice. The existing
command-state snapshot still encloses the whole interceptor chain, so an
interceptor or handler that tries to schedule Workflow commands produces
`OdinWorkflowQueryMutationError` and has those mutations rolled back.

`handle_signal` wraps both immediate handlers and independent stackful Signal
routines. Immediate handlers retain their command-state guard. A routine chain
is constructed inside `signal_routine_entry`, so its `next` and every
interceptor stack frame remain suspended with the handler fiber. Signal name,
identity, originating event ID, headers, and Payload are cloned into the
Workflow run arena before that fiber starts; the context therefore remains
valid after arbitrarily many activation boundaries.
`set_signal_routine_json_with_context` exposes that same run-owned context to
user handler code; the pointer must not escape the Workflow run.

Update validation and handling are separate hooks, matching Core's
`run_validator` contract. `handle_update_validator` runs synchronously only
when Core requests validation and remains command-free.
`handle_update_handler` wraps either the immediate handler or the independent
Update fiber. For a routine, the chain stays open across Activities, timers,
and Signals and unwinds only when the Update result is ready. Replayed Updates
with `run_validator=false` correctly skip only the validator chain.

All hooks execute inside deterministic Workflow code and again during replay.
The unit suite proves nested execute entry remains open across multiple
activations and unwinds only at completion; Query entry/unwind completes while
the main fiber remains suspended. Signal and Update tests prove nested handler
chains stay open across Activity resolution and that their invocation contexts
remain valid afterward. The 500-case Workflow-interceptor property now varies
twelve inbound/outbound operations and arbitrary short-circuit depth.
The CLI-devserver suite proves execute wrapping around an Activity-suspending
Workflow, Query wrapping across Core's Query activation path, validator
entry/exit around rejection and acceptance, and Signal/Update handler unwind
after real Activity round trips.

## Server feature inventory

Run the live, non-mutating inventory against the Temporal CLI dev server:

```bash
temporal server start-dev --headless --log-level error
./scripts/system-info-integration-test.sh
```

The example first reads all official `GetSystemInfo` fields and then prints one
machine-readable row per higher-level feature. On the pinned CLI 1.5.1 / Server
1.29.1 baseline, the important rows are:

```text
feature=schedules sdk=true support=supported evidence=get-system-info status=0
feature=schedule-count sdk=true support=unsupported evidence=safe-rpc-probe status=12
feature=nexus sdk=true support=supported evidence=get-system-info status=0
feature=standalone-nexus sdk=true support=unsupported evidence=safe-rpc-probe status=12
feature=workflow-pause sdk=true support=unknown evidence=none status=0
feature=standalone-activity sdk=true support=unknown evidence=none status=0
```

The count probes are read-only. Do not turn a failed authentication, namespace,
or connection attempt into a support decision: only RPC status 12 is classified
as unsupported. Workflow Pause has no read-only equivalent and is intentionally
not probed. Unit tests cover every evidence branch, while a 500-case PBT varies
status and error kind to require that only an RPC `UNIMPLEMENTED` result becomes
conclusive negative evidence.

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
on macOS arm64. Native packaging and packaged-runtime smoke verification also
run locally there. A clean Ubuntu 24.04 arm64 container has also built Core from
a Git-free complete source archive, run the then-current 135 unit tests and 42
property tests, built every example, and passed runtime init/shutdown against the
packaged Linux arm64 artifact. The repository now has a four-target hosted CI
matrix for macOS arm64/x86_64 and Linux glibc arm64/x86_64. The two x86_64
targets still require a green hosted run before support is claimed; musl and
Windows are not part of this first distribution set.

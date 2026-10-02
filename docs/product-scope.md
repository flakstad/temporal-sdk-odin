# Product scope

Status: active project direction, 2026-10-02.

## Goal

Build a usable, production-capable Temporal language SDK for Odin on top of
Temporal SDK Core. The project is not a protocol experiment or a disposable
proof of concept. It should let an Odin application own ordinary
Temporal Clients, Workers, Activities, and deterministic Workflows with explicit
Odin ownership and error handling.

Production-capable does not mean implementing every WorkflowService RPC before
the SDK can ship. Temporal adds server APIs continuously, and complete parity is
a moving target. The first stable release is defined by a dependable application
programming core, not by the cardinality of generated request types.

## Stable core

These capabilities form one release surface and may block the first stable
release:

1. **Native foundation:** pinned SDK Core ABI, correct memory ownership,
   reproducible native artifacts, clean initialization/shutdown, and verified
   macOS/Linux arm64/x86_64 packages.
2. **Client:** connect/configuration/TLS, start and obtain Workflow handles,
   result waiting, Signals, Queries, Updates, cancellation, termination,
   Describe, history, and the failure model needed by those operations.
3. **Worker:** Activity and Workflow registration, bounded concurrency,
   lifecycle, graceful shutdown, polling/completion error propagation, and
   replay workers.
4. **Activities:** Payload and typed JSON boundaries, context and metadata,
   retries, heartbeats, cancellation, Local Activities, structured failures,
   and interceptors.
5. **Workflow runtime:** deterministic stackful execution, Activities, timers,
   Signals, Queries, Updates, Child Workflows, external Workflow operations,
   cancellation scopes, Continue-As-New, patching, deterministic time/random,
   eviction, and fresh-stack replay.
6. **Application boundary:** replaceable codecs, recursive Temporal Failures,
   headers and common metadata, logs/metrics/interceptors, deterministic replay
   tests, and a concise set of runnable examples.

The core is complete only when these paths are coherent together. A feature
that works only on the first execution but not after eviction/replay is not a
completed Workflow feature.

## Extended surface

The repository already implements valuable capabilities outside the stable
core:

- Schedules and Visibility;
- Nexus Workers and Workflow Nexus operations;
- standalone Activity and Nexus operation APIs;
- Workflow Pause/Unpause, Reset, deletion, and execution-option mutation;
- custom slot suppliers and advanced deployment/versioning controls;
- Worker supervision and opt-in Activity/Workflow process isolation.

These modules remain supported development work; they are not discarded or
hidden. They should have explicit capability/version guards and their own tests,
but a server-version-specific limitation in one of them does not block the core
SDK release unless the feature is promoted into the stable core. The standard
Worker depends only on the Workflow routing contract; importing it does not
compile the opt-in Activity or Workflow subprocess implementations. Workflow
Pause/Unpause, Schedules, standalone Nexus operations, and standalone
Activities are Client extensions physically extracted from the base package.
Their typed interceptors
are stored through owned opaque extension slots, so base Client lifecycle
remains responsible for cleanup without a reverse dependency on an optional
package. The base retains only shared Workflow Describe snapshots, including
the normalized Activity options also returned by the standalone Activity API.
Other extended Client families should migrate through the same boundary when
their shared types have similarly been separated.

## Deliberately deferred

The following do not belong on the critical path without a demonstrated Odin
application need:

- exhaustive wrappers for every administrative or experimental Service RPC;
- a bespoke Temporal transport or replacement for SDK Core;
- static enforcement of all Workflow determinism rules;
- Windows, musl, or additional packaging targets;
- remote payload codec transport and framework-specific integrations;
- API parity whose only justification is that another language SDK exposes it.

Deferral means “add from evidence”, not “never”. A concrete application need,
an official SDK compatibility requirement, or repeated user demand is enough to
reconsider an item.

## Release gates

The SDK should be described as production-capable only after all of these are
true:

- the public core API and ownership rules have been reviewed and frozen for a
  documented compatibility window;
- the Workflow runtime is split into maintainable subsystems with stable,
  actionable diagnostics;
- deterministic replay, eviction, cancellation, concurrency, nondeterminism,
  and process-loss suites pass under longer mixed-operation soak/fault runs;
- native packages and packaged consumer smoke tests are green on all four
  initial targets;
- the Core pin upgrade procedure, compatibility matrix, license/SBOM material,
  and release process are reproducible;
- the primary examples exercise only public API and document operational limits
  honestly.

Until then the project is under active development toward that bar. Individual
optional facilities may still be labelled experimental when their contracts are
not ready; that label no longer describes the purpose of the whole SDK.

## Consequences

This direction prioritizes consolidation over further breadth. The superseded
explicit state-machine runtime has been removed after its shared result/error
contract moved to the active runtime. New core work
should close a release gate or a correctness gap. New extended APIs should be
small vertical slices justified by an actual consumer and must not destabilize
Workflow replay or Worker lifecycle.

The current breadth is useful evidence and should be retained. It does not make
all modules equally stable, and line count or RPC count is not a completion
metric. The decisive signals are deployability, deterministic behavior,
ownership safety, compatibility, and recovery under faults.

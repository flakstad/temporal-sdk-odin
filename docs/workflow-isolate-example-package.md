# Workflow isolation application bundle

This archive is a relocation proof for an application-specific isolated
Workflow Worker. It is not a generic SDK runtime or a production release.

Contents:

- `bin/temporal-odin-workflow-isolate-core` — the Core-owning Worker parent;
- `bin/temporal-odin-workflow-isolate` — the executor containing the example's
  Workflow registry and callbacks;
- `lib/libtemporalio_sdk_core_c_bridge.{dylib,so}` — the pinned Core bridge;
- `manifest.txt` — target, Core revision, isolation protocol version, and
  checksums;
- `licenses/` — licenses for the Odin SDK, SDK Core, and minicoro.

The parent resolves Core relative to itself through
`@loader_path/../lib` on macOS or `$ORIGIN/../lib` on Linux. The executor uses
the project-owned fiber shim statically and contains the application's Workflow
registry. Moving the complete directory therefore preserves the loader and
parent/child process contract.

Verify and run the serverless process proof from a source checkout with:

```bash
./scripts/verify-workflow-isolate-example-package.sh \
  path/to/temporal-sdk-odin-workflow-isolate-example-<target>.tar.gz
```

The verifier checks archive safety, the host architecture, the exact pinned
Core revision and protocol version, binary checksums, relative loader paths,
and code signatures where applicable. It then moves the unpacked directory,
loads the parent binary without contacting a server, and runs the child process
transport proof through suspension, resumption, and clean shutdown.

Real applications need their own equivalent executor build because procedure
pointers and Workflow registration cannot be loaded from the generic native
runtime archive. The complete Core-connected example additionally needs
`TEMPORAL_ODIN_ISOLATE_CHILD_COMMAND` to point at the executor in this bundle
and a Temporal CLI dev server on `localhost:7233`.

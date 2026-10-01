# Native runtime package

This archive contains the platform-specific part of the experimental Temporal
SDK for Odin. It is built from the exact SDK Core revision recorded in
`manifest.txt`; do not combine it with generated Odin protobuf or raw C ABI
bindings from another repository revision.

Contents:

- `lib/libtemporalio_sdk_core_c_bridge.{dylib,so}` — Temporal SDK Core's C
  bridge;
- `lib/libtemporal_odin_fiber.a` — the project-owned minicoro shim used by the
  Workflow experiment;
- `include/temporal-sdk-core-c-bridge.h` — the matching upstream C header;
- `manifest.txt` — target, toolchain, upstream version, and file checksums;
- `licenses/` — licenses for the Odin SDK, SDK Core, and minicoro.

The Odin packages are distributed from the source repository. Point the linker
at this archive's `lib` directory and provide a runtime search path to the same
directory. The repository scripts do this with:

```text
-L/path/to/archive/lib -Wl,-rpath,/path/to/archive/lib
```

The Core bridge is dynamic; the fiber shim is static. macOS uses an `@rpath`
install name. Linux packages currently target glibc, not musl. The package is
host-targeted and is not a cross-compilation sysroot.

On the packaged macOS and Linux targets, the fiber shim uses `mmap`/`mprotect`
to reserve each fixed Workflow stack lazily and place an inaccessible page at
its overflow edge. This is part of the native shim ABI and is exercised by
`scripts/fiber-guard-test.sh`; it is not supplied by SDK Core. Overflow remains
a fatal process signal, so deployment supervision and explicit stack sizing are
still required when Workflows execute in-process. The experimental per-run
executor manager now live-proves that the same signal can fail one Workflow
Task and recover through Core eviction/replay without terminating the parent
Worker.

These are experimental CI artifacts, not final release bundles. Public binary
distribution still needs an audited notice/SBOM for Core's transitive Rust
dependencies, and macOS release artifacts need an explicit code-signing and
notarization policy.

Verify an archive from the source checkout before using it:

```bash
./scripts/verify-native-package.sh path/to/archive.tar.gz --smoke
```

The smoke mode links the repository's runtime lifecycle test against the
unpacked libraries and exercises Core initialization and shutdown without a
Temporal Server.

# Temporal SDK for Odin — experimental

An experimental Odin language SDK built on [Temporal SDK Core](https://github.com/temporalio/sdk-rust),
not a reimplementation of Temporal Server or its gRPC protocol.

Current status: Milestone 0 research and a Milestone 1 Core bridge spike. The
repository can build the official Rust Core C bridge, initialize it from Odin,
connect to a local Temporal CLI dev server, call `GetSystemInfo`, and shut down
cleanly. It does **not** yet provide workers, activities, workflows, payload
codecs, or a stable public API. Do not use it in production.

```text
Temporal CLI dev server
          ^ gRPC
          |
Temporal SDK Core
          ^ C ABI
          |
Odin raw bindings -> minimal lifecycle wrapper -> core_connect example
```

## Quick start

Prerequisites:

- Odin (tested with `dev-2026-05:ea5175d86`)
- Rust toolchain 1.94 and Cargo
- `protoc`
- Temporal CLI (tested with CLI 1.5.1 / Server 1.29.1)
- Clang/linker tooling for the target platform

Clone submodules and build the pinned Core bridge:

```bash
git submodule update --init --recursive
./scripts/build-core.sh
```

In one terminal, start the recommended local development server:

```bash
temporal server start-dev
```

In another terminal, run the Odin spike:

```bash
./scripts/run-core-connect.sh
```

Expected output resembles:

```text
Connected through Temporal SDK Core; GetSystemInfo returned 32 protobuf bytes.
```

The self-contained integration test starts a headless CLI dev server only when
one is not already healthy on `localhost:7233`:

```bash
./scripts/integration-test.sh
```

Unit/lifecycle tests:

```bash
./scripts/test.sh
```

See [architecture](docs/architecture.md) for the researched boundary and
[development](docs/development.md) for build details and troubleshooting.

## Repository layout

```text
docs/                     architecture and development notes
native/                   pinned sdk-core submodule and Cargo lockfile
temporal/core/raw/        near-1:1 subset of the C ABI
temporal/core/            minimal Odin ownership/lifecycle wrapper
temporal/{client,...}/    planned public packages
examples/core_connect/    Odin -> Core -> Temporal proof
tests/                    lifecycle and ownership tests
```

## License

MIT. The `native/sdk-core` submodule is Temporal's separate upstream project
and retains its own license.

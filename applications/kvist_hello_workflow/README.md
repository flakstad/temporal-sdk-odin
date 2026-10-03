# Kvist hello Workflow

This application is written in Kvist and imports the public Odin Temporal SDK
packages directly. Kvist lowers the source to Odin; the resulting native
program links the same SDK Core bridge and fiber shim as an Odin application.

Run the complete path against a Temporal CLI dev server:

```bash
./scripts/kvist-hello-workflow-integration-test.sh
```

The script uses `../kvist/kvist` by default. Override it with `KVIST_BIN` and
override the native SDK library directory with `TEMPORAL_ODIN_NATIVE_LIB_DIR`.

The program deliberately covers both sides of the language SDK boundary:

- a Kvist Activity callback;
- a sequential-looking Kvist Workflow that suspends on
  `workflow.execute_activity`;
- public Client and Worker lifecycle calls; and
- a typed result returned through Temporal Server.

Kvist's current CLI does not expose arbitrary Odin linker flags. The build
script therefore supplies the native-library search path through the platform
linker environment, and the run script supplies the dynamic-loader path. No raw
Odin is used in the Kvist source.

One current interop wrinkle is visible in the imports: the facade's
`temporal.Worker_Run_Options` is an Odin alias of `worker.Run_Options`. Kvist
currently treats a qualified imported alias constructor as a conversion, so
the application imports `temporal/worker` and constructs the underlying struct
directly. The resulting value is the exact type expected by `worker_run`.

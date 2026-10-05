# Kvist durable replay Workflow

This application proves that a Workflow written in Kvist survives the complete
loss of its Worker process. It uses only the public Odin Temporal SDK packages;
there is no raw Odin adapter in the application.

The Workflow executes one Activity, waits on a five-second durable Temporal
timer, and returns the Activity result. The acceptance test kills the first
Worker with `SIGKILL` while that timer is pending. A new Worker process then:

1. receives the persisted history from Temporal;
2. reconstructs the Workflow on a fresh stack;
3. replays the already completed Activity without executing it again;
4. resumes when the durable timer fires; and
5. completes the Workflow with the original result.

Run the complete proof from the repository root:

```bash
./scripts/kvist-replay-workflow-integration-test.sh
```

The script uses `../kvist/kvist` by default. Override it with `KVIST_BIN`, or
set `TEMPORAL_ODIN_NATIVE_LIB_DIR` to use an existing native Core build.

For a manual run, start `temporal server start-dev`, build the application, and
use separate terminals:

```bash
./scripts/build-kvist-replay-workflow.sh

# Terminal 1: terminate this after it reports started_timer=true.
./native/target/release/temporal-kvist-replay worker

# Terminal 2:
./native/target/release/temporal-kvist-replay start my-replay

# Terminal 1 replacement:
./native/target/release/temporal-kvist-replay worker-once

# Terminal 2:
./native/target/release/temporal-kvist-replay result my-replay
```

The activation diagnostics come from a Worker observer outside deterministic
Workflow code. `replay=true` in the replacement process is direct evidence
that Core supplied replay history to a newly instantiated Kvist Workflow.

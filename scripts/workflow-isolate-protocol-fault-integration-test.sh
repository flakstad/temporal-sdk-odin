#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-examples.sh"
fi

for fault_mode in \
  stale \
  malformed-metadata \
  malformed-completion \
  metadata-mismatch \
  late; do
  TEMPORAL_ODIN_SKIP_BUILD=1 \
  TEMPORAL_ODIN_ISOLATE_CACHE_MODE=sticky \
  TEMPORAL_ODIN_ISOLATE_FIRST_MODE="$fault_mode" \
    "$repo_root/scripts/workflow-isolate-core-integration-test.sh"
done

echo "Workflow executor protocol faults were contained and recovered through Core eviction/replay."

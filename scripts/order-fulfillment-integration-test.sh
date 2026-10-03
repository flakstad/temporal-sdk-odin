#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="$repo_root/native/target/release/temporal-odin-order-fulfillment"
server_pid=""
worker_pid=""
order_id="acceptance-${RANDOM}-$(date +%s)"
server_log="$(mktemp "${TMPDIR:-/tmp}/temporal-odin-order-server.XXXXXX")"
worker_log="$(mktemp "${TMPDIR:-/tmp}/temporal-odin-order-worker.XXXXXX")"

cleanup() {
  if [[ -n "$worker_pid" ]]; then
    "$binary" cancel "$order_id" >/dev/null 2>&1 || true
    kill "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$server_log" "$worker_log"
}
trap cleanup EXIT INT TERM

if [[ "${TEMPORAL_ODIN_SKIP_BUILD:-}" != "1" ]]; then
  "$repo_root/scripts/build-order-fulfillment.sh" >/dev/null
elif [[ ! -x "$binary" ]]; then
  echo "Order application binary is missing" >&2
  exit 1
fi

if ! temporal operator cluster health \
  --address localhost:7233 \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  temporal server start-dev --headless --log-level error >"$server_log" 2>&1 &
  server_pid=$!
  for _ in {1..30}; do
    if temporal operator cluster health \
      --address localhost:7233 \
      --client-connect-timeout 1s \
      --command-timeout 1s \
      --output none >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

if ! temporal operator cluster health \
  --address localhost:7233 \
  --client-connect-timeout 1s \
  --command-timeout 1s \
  --output none >/dev/null 2>&1; then
  echo "Temporal CLI dev server did not become ready; see $server_log" >&2
  exit 1
fi

"$binary" worker-once >"$worker_log" 2>&1 &
worker_pid=$!
worker_ready=false
for _ in {1..100}; do
  if grep -q '^worker ready task_queue=odin-order-fulfillment$' "$worker_log"; then
    worker_ready=true
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Order Worker exited before becoming ready" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
if [[ "$worker_ready" != true ]]; then
  echo "Order Worker did not become ready" >&2
  cat "$worker_log" >&2
  exit 1
fi

start_output="$("$binary" start "$order_id" "Odin mug" 2)"
echo "$start_output"
grep -Eq "^started workflow_id=order-${order_id} run_id=[^ ]+$" <<<"$start_output"

status_output=""
for _ in {1..100}; do
  if status_output="$("$binary" status "$order_id" 2>/dev/null)" &&
    grep -q 'phase="awaiting_approval"' <<<"$status_output"; then
    break
  fi
  sleep 0.1
done
echo "$status_output"
grep -q 'phase="awaiting_approval"' <<<"$status_output"
grep -q 'reservation="reserved"' <<<"$status_output"

approve_output="$("$binary" approve "$order_id" Andreas)"
echo "$approve_output"
grep -Eq "^approved workflow_id=order-${order_id} approved_by=\"Andreas\"$" \
  <<<"$approve_output"

carrier_output=""
for _ in {1..100}; do
  if carrier_output="$("$binary" status "$order_id" 2>/dev/null)" &&
    grep -q 'phase="waiting_for_carrier"' <<<"$carrier_output"; then
    break
  fi
  sleep 0.1
done
echo "$carrier_output"
grep -q 'phase="waiting_for_carrier"' <<<"$carrier_output"
grep -q 'payment="charged"' <<<"$carrier_output"
grep -q 'total_cents=2598' <<<"$carrier_output"

# Prove that the application depends on Temporal history rather than the Odin
# process. The replacement Worker must replay the run and finish the pending
# durable timer without re-running already completed Activities.
kill -KILL "$worker_pid"
wait "$worker_pid" 2>/dev/null || true
worker_pid=""
"$binary" worker-once >>"$worker_log" 2>&1 &
worker_pid=$!
replacement_ready=false
for _ in {1..100}; do
  ready_count="$(grep -c '^worker ready task_queue=odin-order-fulfillment$' "$worker_log" || true)"
  if [[ "$ready_count" -ge 2 ]]; then
    replacement_ready=true
    break
  fi
  if ! kill -0 "$worker_pid" 2>/dev/null; then
    echo "Replacement Order Worker exited before becoming ready" >&2
    cat "$worker_log" >&2
    exit 1
  fi
  sleep 0.1
done
if [[ "$replacement_ready" != true ]]; then
  echo "Replacement Order Worker did not become ready" >&2
  cat "$worker_log" >&2
  exit 1
fi

result_output="$("$binary" result "$order_id")"
echo "$result_output"
grep -q 'phase="completed"' <<<"$result_output"
grep -q 'reservation="reserved"' <<<"$result_output"
grep -q 'approved_by="Andreas"' <<<"$result_output"
grep -q 'payment="charged"' <<<"$result_output"
grep -q 'total_cents=2598' <<<"$result_output"
grep -q 'shipment="carrier-accepted"' <<<"$result_output"

wait "$worker_pid"
worker_pid=""
cat "$worker_log"
grep -q '^activity=reserve_inventory ' "$worker_log"
grep -q '^activity=charge_payment ' "$worker_log"
grep -q '^activity=dispatch_order ' "$worker_log"
[[ "$(grep -c '^activity=reserve_inventory ' "$worker_log")" -eq 1 ]]
[[ "$(grep -c '^activity=charge_payment ' "$worker_log")" -eq 1 ]]
[[ "$(grep -c '^activity=dispatch_order ' "$worker_log")" -eq 1 ]]
grep -Eq '^worker stopped workflows=1 activities=1 activations=[1-9][0-9]*$' "$worker_log"

echo "Order fulfillment application replayed after Worker loss and completed through Temporal SDK Core."

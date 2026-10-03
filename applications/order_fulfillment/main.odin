package main

import "core:fmt"
import "core:os"
import "core:strconv"
import "core:time"
import temporal "sdk:temporal"
import activity "sdk:temporal/activity"
import temporal_client "sdk:temporal/client"
import workflow "sdk:temporal/workflow"

TASK_QUEUE :: "odin-order-fulfillment"
WORKFLOW_TYPE :: "odin_order_fulfillment"
APPROVAL_SIGNAL :: "approve"
STATUS_QUERY :: "status"

Order_Input :: struct {
	order_id: string,
	item:     string,
	quantity: int,
}

Approval :: struct {
	approved_by: string,
}

Order_Status :: struct {
	order_id:          string,
	item:              string,
	quantity:          int,
	phase:             string,
	reservation_state: string,
	approved_by:       string,
	payment_state:     string,
	total_cents:       int,
	shipment_state:    string,
}

Reserve_Request :: struct {
	order_id: string,
	item:     string,
	quantity: int,
}

Reservation :: struct {
	state: string,
}

Charge_Request :: struct {
	order_id:    string,
	quantity:    int,
	unit_cents:  int,
}

Payment :: struct {
	state:       string,
	total_cents: int,
}

Dispatch_Request :: struct {
	order_id: string,
	item:     string,
	quantity: int,
}

Shipment :: struct {
	state: string,
}

client_error :: proc(operation: string, err: ^temporal_client.Error) -> int {
	fmt.eprintf(
		"%s failed (kind=%v, status=%d): %s\n",
		operation,
		err.kind,
		err.status_code,
		string(err.message),
	)
	temporal_client.error_free(err)
	return 1
}

worker_error :: proc(operation: string, err: ^temporal.Worker_Error) -> int {
	fmt.eprintf("%s failed: %s\n", operation, string(err.message))
	temporal.worker_error_free(err)
	return 1
}

connect :: proc() -> (temporal.Client, temporal.Client_Error) {
	address_buffer: [512]u8
	namespace_buffer: [256]u8
	address := os.get_env(address_buffer[:], "TEMPORAL_ADDRESS")
	namespace := os.get_env(namespace_buffer[:], "TEMPORAL_NAMESPACE")
	if len(address) == 0 do address = "http://localhost:7233"
	if len(namespace) == 0 do namespace = "default"
	return temporal.connect_with_options({
		target_url = address,
		namespace = namespace,
		identity = "odin-order-fulfillment",
	})
}

workflow_id_for :: proc(order_id: string) -> string {
	return fmt.aprintf("order-%s", order_id)
}

reserve_inventory :: proc(
	ctx: ^activity.Context,
	request: Reserve_Request,
) -> Reservation {
	info := activity.info(ctx)
	fmt.printf(
		"activity=%s order=%s item=%q quantity=%d attempt=%d\n",
		info.activity_type,
		request.order_id,
		request.item,
		request.quantity,
		info.attempt,
	)
	_ = os.flush(os.stdout)
	return {state = "reserved"}
}

charge_payment :: proc(ctx: ^activity.Context, request: Charge_Request) -> Payment {
	info := activity.info(ctx)
	total := request.quantity * request.unit_cents
	fmt.printf(
		"activity=%s order=%s total_cents=%d attempt=%d\n",
		info.activity_type,
		request.order_id,
		total,
		info.attempt,
	)
	_ = os.flush(os.stdout)
	return {state = "charged", total_cents = total}
}

dispatch_order :: proc(ctx: ^activity.Context, request: Dispatch_Request) -> Shipment {
	info := activity.info(ctx)
	fmt.printf(
		"activity=%s order=%s item=%q quantity=%d attempt=%d\n",
		info.activity_type,
		request.order_id,
		request.item,
		request.quantity,
		info.attempt,
	)
	_ = os.flush(os.stdout)
	return {state = "carrier-accepted"}
}

query_status :: proc(state: Order_Status, _: string) -> Order_Status {
	return state
}

order_workflow :: proc(ctx: ^workflow.Fiber_Context, input: Order_Input) -> Order_Status {
	state := Order_Status{
		order_id = input.order_id,
		item = input.item,
		quantity = input.quantity,
		phase = "reserving_inventory",
	}
	if !workflow.set_query_handler_json(ctx, STATUS_QUERY, &state, query_status) {
		state.phase = "query_registration_failed"
		return state
	}

	retry := workflow.Retry_Policy{
		initial_interval = time.Second,
		backoff_coefficient = 2,
		maximum_interval = 5 * time.Second,
		maximum_attempts = 3,
	}
	activity_options := workflow.Activity_Options{
		schedule_to_close_timeout = 30 * time.Second,
		start_to_close_timeout = 10 * time.Second,
		retry_policy = &retry,
	}

	reservation := workflow.execute_activity_json_with_options(
		ctx,
		"reserve_inventory",
		Reserve_Request{
			order_id = input.order_id,
			item = input.item,
			quantity = input.quantity,
		},
		Reservation,
		activity_options,
	)
	state.reservation_state = reservation.state
	state.phase = "awaiting_approval"

	approval := workflow.await_signal_json_or_timeout(
		ctx,
		APPROVAL_SIGNAL,
		5 * time.Minute,
		Approval,
	)
	switch approval.status {
	case .Timed_Out:
		state.phase = "approval_timed_out"
		return state
	case .Canceled:
		state.phase = "canceled"
		return state
	case .Received:
		state.approved_by = approval.value.approved_by
	}

	state.phase = "charging_payment"
	payment := workflow.execute_activity_json_with_options(
		ctx,
		"charge_payment",
		Charge_Request{
			order_id = input.order_id,
			quantity = input.quantity,
			unit_cents = 1299,
		},
		Payment,
		activity_options,
	)
	state.payment_state = payment.state
	state.total_cents = payment.total_cents

	// This is a durable Temporal timer, not a process sleep. The Worker can be
	// restarted while it is pending and Core will replay the Workflow safely.
	state.phase = "waiting_for_carrier"
	workflow.sleep(ctx, 5 * time.Second)

	state.phase = "dispatching"
	shipment := workflow.execute_activity_json_with_options(
		ctx,
		"dispatch_order",
		Dispatch_Request{
			order_id = input.order_id,
			item = input.item,
			quantity = input.quantity,
		},
		Shipment,
		activity_options,
	)
	state.shipment_state = shipment.state
	state.phase = "completed"
	return state
}

worker_ready :: proc(_: rawptr) -> bool {
	fmt.printf("worker ready task_queue=%s\n", TASK_QUEUE)
	_ = os.flush(os.stdout)
	return true
}

run_worker :: proc(max_workflows: int) -> int {
	client, connect_err := connect()
	if connect_err.message != nil do return client_error("connect", &connect_err)
	defer temporal.client_close(&client)

	sdk_worker, create_err := temporal.worker_create(&client, TASK_QUEUE)
	if create_err.message != nil do return worker_error("create worker", &create_err)
	defer {
		close_err := temporal.worker_close(&sdk_worker)
		if close_err.message != nil {
			_ = worker_error("close worker", &close_err)
		}
	}

	registration_err := temporal.worker_register_json_activity(
		&sdk_worker,
		"reserve_inventory",
		reserve_inventory,
	)
	if registration_err.message != nil do return worker_error("register inventory Activity", &registration_err)
	registration_err = temporal.worker_register_json_activity(
		&sdk_worker,
		"charge_payment",
		charge_payment,
	)
	if registration_err.message != nil do return worker_error("register payment Activity", &registration_err)
	registration_err = temporal.worker_register_json_activity(
		&sdk_worker,
		"dispatch_order",
		dispatch_order,
	)
	if registration_err.message != nil do return worker_error("register dispatch Activity", &registration_err)
	registration_err = temporal.worker_register_json_workflow(
		&sdk_worker,
		WORKFLOW_TYPE,
		order_workflow,
	)
	if registration_err.message != nil do return worker_error("register order Workflow", &registration_err)

	stats, run_err := temporal.worker_run(&sdk_worker, {
		max_workflow_executions = max_workflows,
		on_ready = worker_ready,
	})
	if run_err.message != nil do return worker_error("run worker", &run_err)
	fmt.printf(
		"worker stopped workflows=%d activities=%d activations=%d\n",
		stats.workflow_executions_completed,
		stats.activity_tasks_completed,
		stats.workflow_activations_completed,
	)
	return 0
}

start_order :: proc(order_id, item: string, quantity: int) -> int {
	client, connect_err := connect()
	if connect_err.message != nil do return client_error("connect", &connect_err)
	defer temporal.client_close(&client)

	workflow_id := workflow_id_for(order_id)
	defer delete(workflow_id)
	handle, start_err := temporal_client.start_workflow_json(
		&client,
		WORKFLOW_TYPE,
		Order_Input{order_id = order_id, item = item, quantity = quantity},
		{
			id = workflow_id,
			task_queue = TASK_QUEUE,
			workflow_execution_timeout = 10 * time.Minute,
			workflow_run_timeout = 10 * time.Minute,
			workflow_task_timeout = 10 * time.Second,
			id_reuse_policy = .Reject_Duplicate,
			id_conflict_policy = .Fail,
		},
	)
	if start_err.message != nil do return client_error("start order", &start_err)
	defer temporal_client.workflow_handle_free(&handle)
	fmt.printf("started workflow_id=%s run_id=%s\n", handle.id, handle.run_id)
	return 0
}

get_handle :: proc(
	client: ^temporal.Client,
	order_id: string,
) -> (temporal_client.Workflow_Handle, temporal_client.Error) {
	workflow_id := workflow_id_for(order_id)
	defer delete(workflow_id)
	return temporal_client.get_workflow_handle(client, workflow_id)
}

print_status :: proc(prefix: string, status: Order_Status) {
	fmt.printf(
		"%s order=%s item=%q quantity=%d phase=%q reservation=%q approved_by=%q payment=%q total_cents=%d shipment=%q\n",
		prefix,
		status.order_id,
		status.item,
		status.quantity,
		status.phase,
		status.reservation_state,
		status.approved_by,
		status.payment_state,
		status.total_cents,
		status.shipment_state,
	)
}

status_free :: proc(status: ^Order_Status) {
	if status == nil do return
	delete(status.order_id)
	delete(status.item)
	delete(status.phase)
	delete(status.reservation_state)
	delete(status.approved_by)
	delete(status.payment_state)
	delete(status.shipment_state)
	status^ = {}
}

query_order :: proc(order_id: string) -> int {
	client, connect_err := connect()
	if connect_err.message != nil do return client_error("connect", &connect_err)
	defer temporal.client_close(&client)
	handle, handle_err := get_handle(&client, order_id)
	if handle_err.message != nil do return client_error("get order handle", &handle_err)
	defer temporal_client.workflow_handle_free(&handle)

	status: Order_Status
	query_err := temporal_client.query_workflow_json(&handle, STATUS_QUERY, "", &status)
	if query_err.message != nil do return client_error("query order", &query_err)
	defer status_free(&status)
	print_status("status", status)
	return 0
}

approve_order :: proc(order_id, approved_by: string) -> int {
	client, connect_err := connect()
	if connect_err.message != nil do return client_error("connect", &connect_err)
	defer temporal.client_close(&client)
	handle, handle_err := get_handle(&client, order_id)
	if handle_err.message != nil do return client_error("get order handle", &handle_err)
	defer temporal_client.workflow_handle_free(&handle)

	signal_err := temporal_client.signal_workflow_json(
		&handle,
		APPROVAL_SIGNAL,
		Approval{approved_by = approved_by},
	)
	if signal_err.message != nil do return client_error("approve order", &signal_err)
	fmt.printf("approved workflow_id=%s approved_by=%q\n", handle.id, approved_by)
	return 0
}

wait_for_order :: proc(order_id: string) -> int {
	client, connect_err := connect()
	if connect_err.message != nil do return client_error("connect", &connect_err)
	defer temporal.client_close(&client)
	handle, handle_err := get_handle(&client, order_id)
	if handle_err.message != nil do return client_error("get order handle", &handle_err)
	defer temporal_client.workflow_handle_free(&handle)

	result: Order_Status
	result_err := temporal_client.get_result_json(&handle, &result)
	if result_err.message != nil do return client_error("wait for order", &result_err)
	defer status_free(&result)
	print_status("result", result)
	return 0
}

cancel_order :: proc(order_id: string) -> int {
	client, connect_err := connect()
	if connect_err.message != nil do return client_error("connect", &connect_err)
	defer temporal.client_close(&client)
	handle, handle_err := get_handle(&client, order_id)
	if handle_err.message != nil do return client_error("get order handle", &handle_err)
	defer temporal_client.workflow_handle_free(&handle)

	cancel_err := temporal_client.cancel_workflow(&handle, "order canceled by operator")
	if cancel_err.message != nil do return client_error("cancel order", &cancel_err)
	fmt.printf("cancellation requested workflow_id=%s\n", handle.id)
	return 0
}

usage :: proc() {
	fmt.eprintln("usage:")
	fmt.eprintln("  order-fulfillment worker")
	fmt.eprintln("  order-fulfillment worker-once")
	fmt.eprintln("  order-fulfillment start <order-id> <item> <quantity>")
	fmt.eprintln("  order-fulfillment status <order-id>")
	fmt.eprintln("  order-fulfillment approve <order-id> <approved-by>")
	fmt.eprintln("  order-fulfillment result <order-id>")
	fmt.eprintln("  order-fulfillment cancel <order-id>")
}

run :: proc() -> int {
	if len(os.args) < 2 {
		usage()
		return 2
	}
	switch os.args[1] {
	case "worker":
		return run_worker(0)
	case "worker-once":
		return run_worker(1)
	case "start":
		if len(os.args) != 5 {
			usage()
			return 2
		}
		quantity, ok := strconv.parse_int(os.args[4])
		if !ok || quantity <= 0 {
			fmt.eprintln("quantity must be a positive integer")
			return 2
		}
		return start_order(os.args[2], os.args[3], quantity)
	case "status":
		if len(os.args) != 3 {
			usage()
			return 2
		}
		return query_order(os.args[2])
	case "approve":
		if len(os.args) != 4 {
			usage()
			return 2
		}
		return approve_order(os.args[2], os.args[3])
	case "result":
		if len(os.args) != 3 {
			usage()
			return 2
		}
		return wait_for_order(os.args[2])
	case "cancel":
		if len(os.args) != 3 {
			usage()
			return 2
		}
		return cancel_order(os.args[2])
	case:
		usage()
		return 2
	}
}

main :: proc() {
	os.exit(run())
}

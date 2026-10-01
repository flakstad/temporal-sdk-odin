// Package temporal is the small convenience entry point for SDK lifecycle.
// Feature-specific APIs remain in temporal/client, temporal/worker,
// temporal/activity, and temporal/workflow so ownership and error types stay
// explicit while the public surface is experimental.
package temporal

import temporal_client "./client"
import temporal_worker "./worker"

EXPERIMENTAL :: true

Client :: temporal_client.Client
Client_Connect_Options :: temporal_client.Connect_Options
Client_Error :: temporal_client.Error
Client_Error_Kind :: temporal_client.Error_Kind

Worker :: temporal_worker.Worker
Worker_Options :: temporal_worker.Options
Worker_Run_Options :: temporal_worker.Run_Options
Worker_Run_Stats :: temporal_worker.Run_Stats
Worker_Error :: temporal_worker.Error

connect :: temporal_client.connect
connect_with_options :: temporal_client.connect_with_options
client_close :: temporal_client.close
client_error_free :: temporal_client.error_free

// worker_options_default inherits the Client namespace. Returned strings are
// borrowed from the arguments until worker_create_with_options copies them.
worker_options_default :: proc(client: ^Client, task_queue: string) -> Worker_Options {
	namespace := "default"
	if client != nil && len(client.namespace) > 0 {
		namespace = client.namespace
	}
	return temporal_worker.options_default(task_queue, namespace)
}

worker_create :: proc(
	client: ^Client,
	task_queue: string,
) -> (worker: Worker, error: Worker_Error) {
	options := worker_options_default(client, task_queue)
	return temporal_worker.create_with_options(temporal_client.core_client(client), options)
}

// An empty namespace inherits the Client namespace. An explicit namespace is
// passed through for parity with the lower-level Worker options.
worker_create_with_options :: proc(
	client: ^Client,
	options: Worker_Options,
) -> (worker: Worker, error: Worker_Error) {
	resolved := options
	if len(resolved.namespace) == 0 && client != nil {
		resolved.namespace = client.namespace
	}
	return temporal_worker.create_with_options(temporal_client.core_client(client), resolved)
}

worker_register_activity :: temporal_worker.register_activity
worker_register_payload_activity :: temporal_worker.register_payload_activity
worker_register_payloads_activity :: temporal_worker.register_payloads_activity
worker_register_json_activity :: temporal_worker.register_json_activity
worker_register_json_activity_result :: temporal_worker.register_json_activity_result
worker_register_nexus_operation :: temporal_worker.register_nexus_operation
worker_register_payload_nexus_operation :: temporal_worker.register_payload_nexus_operation
worker_register_json_nexus_operation :: temporal_worker.register_json_nexus_operation
worker_register_json_nexus_operation_result :: temporal_worker.register_json_nexus_operation_result
worker_register_nexus_cancel_handler :: temporal_worker.register_nexus_cancel_handler
worker_register_workflow :: temporal_worker.register_workflow
worker_register_payload_workflow :: temporal_worker.register_payload_workflow
worker_register_payloads_workflow :: temporal_worker.register_payloads_workflow
worker_register_json_workflow :: temporal_worker.register_json_workflow
worker_register_external_workflow :: temporal_worker.register_external_workflow
worker_register_external_workflow_registry :: temporal_worker.register_external_workflow_registry
worker_add_activity_interceptor :: temporal_worker.add_activity_interceptor
worker_add_workflow_interceptor :: temporal_worker.add_workflow_interceptor
worker_add_workflow_inbound_interceptor :: temporal_worker.add_workflow_inbound_interceptor
worker_run :: temporal_worker.run
worker_request_shutdown :: temporal_worker.request_shutdown
worker_close :: temporal_worker.close
worker_error_free :: temporal_worker.error_free

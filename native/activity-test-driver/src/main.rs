use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, Url, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_common::{ActivityDefinition, RetryPolicy};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityCancellationType, ActivityOptions, CancellableFuture, Runtime, Worker, WorkerOptions,
    WorkflowContext, WorkflowResult,
};

struct HelloActivity;

impl ActivityDefinition for HelloActivity {
    type Input = String;
    type Output = String;

    fn name(&self) -> &str {
        "hello"
    }
}

struct MissingActivity;

impl ActivityDefinition for MissingActivity {
    type Input = String;
    type Output = String;

    fn name(&self) -> &str {
        "missing_activity"
    }
}

struct TypedGreetingActivity;

impl ActivityDefinition for TypedGreetingActivity {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    fn name(&self) -> &str {
        "typed_greeting"
    }
}

struct RetryGreetingActivity;

impl ActivityDefinition for RetryGreetingActivity {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    fn name(&self) -> &str {
        "retry_greeting"
    }
}

struct AsyncHelloActivity;

impl ActivityDefinition for AsyncHelloActivity {
    type Input = String;
    type Output = String;

    fn name(&self) -> &str {
        "async_hello"
    }
}

#[workflow]
#[derive(Default)]
struct ActivityDriverWorkflow;

#[workflow_methods]
impl ActivityDriverWorkflow {
    #[run(name = "odin_activity_driver")]
    async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let result = ctx
            .execute_activity(
                HelloActivity,
                name,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(20)),
            )
            .await?;
        Ok(result)
    }
}

#[workflow]
#[derive(Default)]
struct FailureDriverWorkflow;

#[workflow_methods]
impl FailureDriverWorkflow {
    #[run(name = "odin_activity_failure_driver")]
    async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let result = ctx
            .execute_activity(
                MissingActivity,
                name,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(20)),
            )
            .await?;
        Ok(result)
    }
}

#[workflow]
#[derive(Default)]
struct ConcurrentActivityDriverWorkflow;

#[workflow_methods]
impl ConcurrentActivityDriverWorkflow {
    #[run(name = "odin_concurrent_activity_driver")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let activities = ["concurrent-a", "concurrent-b"].map(|name| {
            ctx.execute_activity(
                HelloActivity,
                name.to_string(),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(20)),
            )
        });
        let results = temporalio_sdk::workflows::join_all(activities).await;
        let mut outputs = Vec::with_capacity(results.len());
        for result in results {
            outputs.push(result?);
        }
        Ok(outputs.join(" & "))
    }
}

const ACTIVITY_LOAD_TASKS: usize = 24;

#[workflow]
#[derive(Default)]
struct ActivityLoadDriverWorkflow;

#[workflow_methods]
impl ActivityLoadDriverWorkflow {
    #[run(name = "odin_activity_load_driver")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let activities = (0..ACTIVITY_LOAD_TASKS).map(|index| {
            ctx.execute_activity(
                HelloActivity,
                format!("load-{index:02}"),
                ActivityOptions::start_to_close_timeout(Duration::from_secs(20)),
            )
        });
        let results = temporalio_sdk::workflows::join_all(activities).await;
        for result in results {
            result?;
        }
        Ok(format!("completed {ACTIVITY_LOAD_TASKS} load Activities"))
    }
}

#[workflow]
#[derive(Default)]
struct TypedActivityDriverWorkflow;

#[workflow_methods]
impl TypedActivityDriverWorkflow {
    #[run(name = "odin_typed_activity_driver")]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: serde_json::Value,
    ) -> WorkflowResult<serde_json::Value> {
        let result = ctx
            .execute_activity(
                TypedGreetingActivity,
                input,
                ActivityOptions::start_to_close_timeout(Duration::from_secs(20)),
            )
            .await?;
        Ok(result)
    }
}

#[workflow]
#[derive(Default)]
struct RetryActivityDriverWorkflow;

#[workflow_methods]
impl RetryActivityDriverWorkflow {
    #[run(name = "odin_retry_activity_driver")]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: serde_json::Value,
    ) -> WorkflowResult<serde_json::Value> {
        let result = ctx
            .execute_activity(
                RetryGreetingActivity,
                input,
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(20))
                    .heartbeat_timeout(Duration::from_secs(5))
                    .retry_policy(RetryPolicy::builder().maximum_attempts(2).build())
                    .build(),
            )
            .await?;
        Ok(result)
    }
}

#[workflow]
#[derive(Default)]
struct AsyncActivityDriverWorkflow;

#[workflow_methods]
impl AsyncActivityDriverWorkflow {
    #[run(name = "odin_async_activity_driver")]
    async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let cancel_activity = name == "__cancel__";
        let activity = ctx.execute_activity(
            AsyncHelloActivity,
            name,
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(20))
                .cancellation_type(ActivityCancellationType::WaitCancellationCompleted)
                .build(),
        );
        if cancel_activity {
            ctx.timer(Duration::from_millis(10)).await;
            activity.cancel();
        }
        let result = activity.await?;
        Ok(result)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target =
        std::env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| "http://localhost:7233".to_string());
    let queue =
        std::env::var("TEMPORAL_TASK_QUEUE").unwrap_or_else(|_| "odin-hello-activity".to_string());
    let input = std::env::var("ACTIVITY_INPUT").unwrap_or_else(|_| "Odin".to_string());
    let expect_failure = std::env::var_os("ACTIVITY_EXPECT_FAILURE").is_some();
    let concurrent = std::env::var_os("ACTIVITY_CONCURRENT").is_some();
    let load = std::env::var_os("ACTIVITY_LOAD").is_some();
    let typed = std::env::var_os("ACTIVITY_TYPED").is_some();
    let retry = std::env::var_os("ACTIVITY_RETRY").is_some();
    let async_mode = std::env::var("ACTIVITY_ASYNC_MODE").ok();
    let expected = if load {
        format!("completed {ACTIVITY_LOAD_TASKS} load Activities")
    } else if concurrent {
        "Hello concurrent-a & Hello concurrent-b".to_string()
    } else {
        format!("Hello {input}")
    };

    let runtime = Runtime::from_current_tokio(Default::default())?;
    let connection =
        Connection::connect(ConnectionOptions::new(Url::from_str(&target)?).build()).await?;
    let client = Client::new(connection, ClientOptions::new("default").build())?;
    let options = WorkerOptions::new(queue.clone())
        .register_workflow::<ActivityDriverWorkflow>()?
        .register_workflow::<FailureDriverWorkflow>()?
        .register_workflow::<ConcurrentActivityDriverWorkflow>()?
        .register_workflow::<ActivityLoadDriverWorkflow>()?
        .register_workflow::<TypedActivityDriverWorkflow>()?
        .register_workflow::<RetryActivityDriverWorkflow>()?
        .register_workflow::<AsyncActivityDriverWorkflow>()?
        .build();
    let mut worker = Worker::new(&runtime, client.clone(), options)?;
    let shutdown = worker.shutdown_handle();

    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let workflow_id = std::env::var("ACTIVITY_WORKFLOW_ID")
        .unwrap_or_else(|_| format!("odin-sdk-activity-spike-{}-{unique}", std::process::id()));
    let driver = async move {
        if let Some(async_mode) = async_mode {
            let activity_input = if async_mode == "id-cancel" {
                "__cancel__".to_string()
            } else {
                input
            };
            let handle = client
                .start_workflow(
                    AsyncActivityDriverWorkflow::run,
                    activity_input,
                    WorkflowStartOptions::new(queue, workflow_id).build(),
                )
                .await?;
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await?;
            shutdown();
            match async_mode.as_str() {
                "token-complete" => {
                    let value = result?;
                    if value != "Hello async Odin" {
                        return Err(
                            format!("unexpected token-completed Activity result: {value}").into(),
                        );
                    }
                    println!("Workflow observed token-completed Odin Activity result: {value}");
                }
                "id-complete" => {
                    let value = result?;
                    if value != "Hello async by ID Odin" {
                        return Err(
                            format!("unexpected ID-completed Activity result: {value}").into()
                        );
                    }
                    println!("Workflow observed ID-completed Odin Activity result: {value}");
                }
                "token-failure" => match result {
                    Ok(value) => {
                        return Err(
                            format!("asynchronously failed Activity returned: {value}").into()
                        );
                    }
                    Err(error) => {
                        println!("Workflow observed expected async Activity failure: {error}");
                    }
                },
                "id-cancel" => match result {
                    Ok(value) => {
                        return Err(
                            format!("asynchronously canceled Activity returned: {value}").into(),
                        );
                    }
                    Err(error) => {
                        println!("Workflow observed expected async Activity cancellation: {error}");
                    }
                },
                value => return Err(format!("unsupported ACTIVITY_ASYNC_MODE: {value}").into()),
            }
            return Ok::<(), Box<dyn std::error::Error>>(());
        }
        if retry {
            let retry_input = serde_json::json!({"name": "Odin"});
            let expected = serde_json::json!({
                "message": "Hello after retry, Odin!",
                "attempt": 2,
                "heartbeat_completed": 17,
            });
            let handle = client
                .start_workflow(
                    RetryActivityDriverWorkflow::run,
                    retry_input,
                    WorkflowStartOptions::new(queue, workflow_id).build(),
                )
                .await?;
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await??;
            shutdown();
            if result != expected {
                return Err(format!(
                    "unexpected retried Activity result: {result}; expected {expected}"
                )
                .into());
            }
            println!("Workflow observed retried Odin Activity result: {result}");
            return Ok::<(), Box<dyn std::error::Error>>(());
        }

        if typed {
            let typed_input = serde_json::json!({
                "name": "Odin",
                "salutation": "Hello",
            });
            let expected = serde_json::json!({
                "message": "Hello, Odin!",
                "name_length": 4,
            });
            let handle = client
                .start_workflow(
                    TypedActivityDriverWorkflow::run,
                    typed_input,
                    WorkflowStartOptions::new(queue, workflow_id).build(),
                )
                .await?;
            let result = tokio::time::timeout(
                Duration::from_secs(30),
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await??;
            shutdown();
            if result != expected {
                return Err(format!(
                    "unexpected typed Activity result: {result}; expected {expected}"
                )
                .into());
            }
            println!("Workflow observed typed Odin Activity result: {result}");
            return Ok::<(), Box<dyn std::error::Error>>(());
        }

        let result = if expect_failure {
            let handle = client
                .start_workflow(
                    FailureDriverWorkflow::run,
                    input,
                    WorkflowStartOptions::new(queue, workflow_id).build(),
                )
                .await?;
            tokio::time::timeout(
                Duration::from_secs(30),
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await?
        } else if load {
            let handle = client
                .start_workflow(
                    ActivityLoadDriverWorkflow::run,
                    (),
                    WorkflowStartOptions::new(queue, workflow_id).build(),
                )
                .await?;
            tokio::time::timeout(
                Duration::from_secs(30),
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await?
        } else if concurrent {
            let handle = client
                .start_workflow(
                    ConcurrentActivityDriverWorkflow::run,
                    (),
                    WorkflowStartOptions::new(queue, workflow_id).build(),
                )
                .await?;
            tokio::time::timeout(
                Duration::from_secs(30),
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await?
        } else {
            let handle = client
                .start_workflow(
                    ActivityDriverWorkflow::run,
                    input,
                    WorkflowStartOptions::new(queue, workflow_id).build(),
                )
                .await?;
            tokio::time::timeout(
                Duration::from_secs(30),
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await?
        };
        shutdown();

        if expect_failure {
            match result {
                Ok(value) => {
                    return Err(
                        format!("unknown Activity unexpectedly completed: {value:?}").into(),
                    );
                }
                Err(error) => {
                    println!("Workflow observed expected Odin Activity failure: {error}");
                    return Ok::<(), Box<dyn std::error::Error>>(());
                }
            }
        }

        let result = result?;
        if result != expected {
            return Err(
                format!("unexpected Activity result: {result:?}; expected {expected:?}").into(),
            );
        }
        println!("Workflow observed Odin Activity result: {result}");
        Ok::<(), Box<dyn std::error::Error>>(())
    };

    let worker_future = worker.run();
    tokio::pin!(worker_future);
    tokio::pin!(driver);
    tokio::select! {
        result = &mut driver => {
            result?;
            worker_future.await?;
        }
        result = &mut worker_future => {
            result?;
            driver.await?;
        }
    }
    Ok(())
}

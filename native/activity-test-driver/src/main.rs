use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, Url, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_common::ActivityDefinition;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, Runtime, Worker, WorkerOptions, WorkflowContext, WorkflowResult,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target =
        std::env::var("TEMPORAL_ADDRESS").unwrap_or_else(|_| "http://localhost:7233".to_string());
    let queue =
        std::env::var("TEMPORAL_TASK_QUEUE").unwrap_or_else(|_| "odin-hello-activity".to_string());
    let input = std::env::var("ACTIVITY_INPUT").unwrap_or_else(|_| "Odin".to_string());
    let expect_failure = std::env::var_os("ACTIVITY_EXPECT_FAILURE").is_some();
    let expected = format!("Hello {input}");

    let runtime = Runtime::from_current_tokio(Default::default())?;
    let connection =
        Connection::connect(ConnectionOptions::new(Url::from_str(&target)?).build()).await?;
    let client = Client::new(connection, ClientOptions::new("default").build())?;
    let options = WorkerOptions::new(queue.clone())
        .register_workflow::<ActivityDriverWorkflow>()?
        .register_workflow::<FailureDriverWorkflow>()?
        .build();
    let mut worker = Worker::new(&runtime, client.clone(), options)?;
    let shutdown = worker.shutdown_handle();

    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let workflow_id = format!("odin-sdk-activity-spike-{}-{unique}", std::process::id());
    let driver = async move {
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

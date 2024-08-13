use crate::{
    traits::{
        processable::RunnableStepType, IntoRunnableStep, NamedStep, Processable, RunnableStep,
    },
    types::transaction_context::TransactionContext,
    utils::{
        errors::ProcessorError,
        step_metrics::{StepMetricLabels, StepMetricsBuilder},
    },
};
use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::Zero;
use instrumented_channel::{
    instrumented_bounded_channel, InstrumentedAsyncReceiver, InstrumentedAsyncSender,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, task::JoinHandle};
use tracing::{error, info, warn};

#[async_trait]
pub trait PollableAsyncStep
where
    Self: Processable + NamedStep + Send + Sized + Sync + 'static,
{
    /// Returns the duration between poll attempts.
    fn poll_interval(&self) -> Duration;

    /// Polls the internal state and returns a batch of output items if available.
    async fn poll(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError>;

    async fn should_continue_polling(&mut self) -> bool {
        // By default, we always continue polling
        true
    }
}

pub struct RunnablePollableStep<Step: PollableAsyncStep> {
    pub step: Step,
}

impl<Step: PollableAsyncStep> RunnablePollableStep<Step> {
    pub fn new(step: Step) -> Self {
        Self { step }
    }
}

pub struct PollableAsyncRunType;

impl RunnableStepType for PollableAsyncRunType {}

impl<Step: PollableAsyncStep> NamedStep for RunnablePollableStep<Step> {
    fn name(&self) -> String {
        self.step.name()
    }
}

impl<Step> IntoRunnableStep<Step::Input, Step::Output, Step, PollableAsyncRunType> for Step
where
    Step: PollableAsyncStep<RunType = PollableAsyncRunType> + Send + Sized + 'static,
{
    fn into_runnable_step(self) -> impl RunnableStep<Step::Input, Step::Output> {
        RunnablePollableStep::new(self)
    }
}

impl<Step> From<Step> for RunnablePollableStep<Step>
where
    Step: PollableAsyncStep<RunType = PollableAsyncRunType> + Send + Sized + 'static,
{
    fn from(step: Step) -> Self {
        RunnablePollableStep::new(step)
    }
}

impl<PollableStep> RunnableStep<PollableStep::Input, PollableStep::Output>
    for RunnablePollableStep<PollableStep>
where
    PollableStep: PollableAsyncStep + Send + Sized + Sync + 'static,
{
    fn spawn(
        self,
        input_receiver: Option<InstrumentedAsyncReceiver<TransactionContext<PollableStep::Input>>>,
        output_channel_size: usize,
        _input_sender: Option<InstrumentedAsyncSender<TransactionContext<PollableStep::Input>>>,
    ) -> (
        InstrumentedAsyncReceiver<TransactionContext<PollableStep::Output>>,
        JoinHandle<()>,
    ) {
        let mut step = self.step;
        let step_name = step.name();
        let input_receiver = input_receiver.expect("Input receiver must be set");

        let (output_sender, output_receiver) =
            instrumented_bounded_channel(&format!("{} Output", step_name), output_channel_size);

        let handle = tokio::spawn(async move {
            // This should only be used for the inputless first step to keep the async sender in scope so the channel stays alive.
            let _input_sender = _input_sender.clone();
            let step_name = step.name();

            step.init().await;

            let arc_step = Arc::new(Mutex::new(step));

            // Spawn polling task
            info!(step_name = step_name, "Spawning polling task");
            let poll_step = Arc::clone(&arc_step);
            let poll_step_name = step_name.clone();
            let poll_output_sender = output_sender.clone();
            let mut polling_task = tokio::spawn(async move {
                let mut last_poll = tokio::time::Instant::now();
                let poll_duration = poll_step.lock().await.poll_interval();

                while poll_step.lock().await.should_continue_polling().await {
                    // It's possible that the channel always has items, so we need to ensure we call `poll` manually if we need to
                    if last_poll.elapsed() >= poll_duration {
                        let polling_duration_for_logging = Instant::now();
                        let result = match poll_step.lock().await.poll().await {
                            Ok(result) => result,
                            Err(e) => {
                                error!(
                                    step_name = poll_step_name,
                                    error = e.to_string(),
                                    "Failed to poll"
                                );
                                break;
                            },
                        };
                        match StepMetricsBuilder::default()
                            .labels(StepMetricLabels {
                                step_name: poll_step_name.clone(),
                            })
                            .polling_duration_in_secs(
                                polling_duration_for_logging.elapsed().as_secs_f64(),
                            )
                            .build()
                        {
                            Ok(mut metrics) => metrics.log_metrics(),
                            Err(e) => {
                                error!(
                                    step_name = poll_step_name,
                                    error = e.to_string(),
                                    "Failed to log metrics"
                                );
                                break;
                            },
                        }
                        if let Some(outputs_with_context) = result {
                            for output_with_context in outputs_with_context {
                                match StepMetricsBuilder::default()
                                    .labels(StepMetricLabels {
                                        step_name: poll_step_name.clone(),
                                    })
                                    .latest_polled_version(output_with_context.end_version)
                                    .latest_polled_transaction_timestamp(
                                        output_with_context.get_start_transaction_timestamp_unix(),
                                    )
                                    .num_polled_transactions_count(
                                        output_with_context.get_num_transactions(),
                                    )
                                    .polled_size_in_bytes(output_with_context.total_size_in_bytes)
                                    .build()
                                {
                                    Ok(mut metrics) => metrics.log_metrics(),
                                    Err(e) => {
                                        error!(
                                            step_name = poll_step_name,
                                            error = e.to_string(),
                                            "Failed to log metrics"
                                        );
                                        break;
                                    },
                                }
                                match poll_output_sender.send(output_with_context).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        error!(
                                            step_name = poll_step_name,
                                            error = e.to_string(),
                                            "Error sending output to channel"
                                        );
                                        break;
                                    },
                                }
                            }
                        };
                        last_poll = tokio::time::Instant::now();
                    }
                }
            });

            // Spawn processing task
            info!(step_name = step_name, "Spawning processing task");
            let process_step = Arc::clone(&arc_step);
            let process_step_name = step_name.clone();
            let process_output_sender = output_sender.clone();
            let mut processing_task = tokio::spawn(async move {
                loop {
                    let input_with_context = match input_receiver.recv().await {
                        Ok(input_with_context) => input_with_context,
                        Err(e) => {
                            // If the previous steps have finished and the channels have closed , we should break out of the loop
                            warn!(
                                step_name = process_step_name,
                                error = e.to_string(),
                                "No input received from channel"
                            );
                            break;
                        },
                    };
                    let processing_duration = Instant::now();
                    let output_with_context =
                        match process_step.lock().await.process(input_with_context).await {
                            Ok(output_with_context) => output_with_context,
                            Err(e) => {
                                error!(
                                    step_name = process_step_name,
                                    error = e.to_string(),
                                    "Failed to process input"
                                );
                                break;
                            },
                        };
                    if let Some(output_with_context) = output_with_context {
                        match StepMetricsBuilder::default()
                            .labels(StepMetricLabels {
                                step_name: process_step_name.clone(),
                            })
                            .latest_processed_version(output_with_context.end_version)
                            .latest_transaction_timestamp(
                                output_with_context.get_start_transaction_timestamp_unix(),
                            )
                            .num_transactions_processed_count(
                                output_with_context.get_num_transactions(),
                            )
                            .processing_duration_in_secs(
                                processing_duration.elapsed().as_secs_f64(),
                            )
                            .processed_size_in_bytes(output_with_context.total_size_in_bytes)
                            .build()
                        {
                            Ok(mut metrics) => metrics.log_metrics(),
                            Err(e) => {
                                error!(
                                    step_name = process_step_name,
                                    error = e.to_string(),
                                    "Failed to log metrics"
                                );
                                break;
                            },
                        }
                        match process_output_sender.send(output_with_context).await {
                            Ok(_) => (),
                            Err(e) => {
                                error!(
                                    step_name = process_step_name,
                                    error = e.to_string(),
                                    "Error sending output to channel"
                                );
                                break;
                            },
                        }
                    }
                }
            });

            // If either polling or processing task ends, we should stop the other one.
            tokio::select! {
                _ = &mut polling_task => {
                    info!(step_name = step_name, "Polling task has ended. Stopping processing task.");
                    processing_task.abort();
                },
                _ = &mut processing_task => {
                    info!(step_name = step_name, "Processing task has ended. Stopping polling task.");
                    polling_task.abort();
                },
            }

            info!(step_name = step_name, "Cleaning up step");
            // Do any additional cleanup
            let res = arc_step.lock().await.cleanup().await;
            match res {
                Ok(Some(outputs_with_context)) => {
                    for output_with_context in outputs_with_context {
                        match StepMetricsBuilder::default()
                            .labels(StepMetricLabels {
                                step_name: step_name.clone(),
                            })
                            .latest_polled_version(output_with_context.end_version)
                            .latest_polled_transaction_timestamp(
                                output_with_context.get_start_transaction_timestamp_unix(),
                            )
                            .num_polled_transactions_count(
                                output_with_context.get_num_transactions(),
                            )
                            .polled_size_in_bytes(output_with_context.total_size_in_bytes)
                            .build()
                        {
                            Ok(mut metrics) => metrics.log_metrics(),
                            Err(e) => {
                                error!(
                                    step_name = step_name,
                                    error = e.to_string(),
                                    "Failed to log metrics"
                                );
                                return;
                            },
                        }

                        match output_sender.send(output_with_context).await {
                            Ok(_) => {},
                            Err(e) => {
                                error!(
                                    step_name = step_name,
                                    error = e.to_string(),
                                    "Error sending output to channel"
                                );
                                return;
                            },
                        }
                    }
                },
                Ok(None) => (),
                Err(e) => {
                    error!(
                        step_name = step_name,
                        error = e.to_string(),
                        "Error cleaning up step"
                    );
                    return;
                },
            }

            // Wait for output channel to be empty before ending the task and closing the send channel
            loop {
                let channel_size = output_sender.len();
                info!(
                    step_name = step_name,
                    channel_size = channel_size,
                    "Waiting for output channel to be empty"
                );
                if channel_size.is_zero() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!(
                step_name = step_name,
                "Output channel is empty. Closing send channel."
            );
        });

        (output_receiver, handle)
    }
}

use crate::{
    steps::step_metrics::{StepMetricLabels, StepMetricsBuilder},
    traits::{
        processable::RunnableStepType, IntoRunnableStep, NamedStep, Processable, RunnableStep,
    },
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use anyhow::Result;
use async_trait::async_trait;
use bigdecimal::Zero;
use instrumented_channel::{
    instrumented_bounded_channel, InstrumentedAsyncReceiver, InstrumentedAsyncSender,
};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

#[async_trait]
pub trait PollableAsyncStep
where
    Self: Processable + NamedStep + Send + Sized + 'static,
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
    PollableStep: PollableAsyncStep + Send + Sized + 'static,
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
            let poll_duration = step.poll_interval();

            let mut last_poll = tokio::time::Instant::now();

            step.init().await;

            while step.should_continue_polling().await {
                // It's possible that the channel always has items, so we need to ensure we call `poll` manually if we need to
                if last_poll.elapsed() >= poll_duration {
                    let polling_duration_for_logging = Instant::now();
                    let result = match step.poll().await {
                        Ok(result) => result,
                        Err(e) => {
                            error!(step_name, "Failed to poll: {:?}", e);
                            break;
                        },
                    };
                    StepMetricsBuilder::default()
                        .labels(StepMetricLabels {
                            step_name: step.name(),
                        })
                        .polling_duration_in_secs(
                            polling_duration_for_logging.elapsed().as_secs_f64(),
                        )
                        .build()
                        .unwrap()
                        .log_metrics();
                    if let Some(outputs_with_context) = result {
                        for output_with_context in outputs_with_context {
                            StepMetricsBuilder::default()
                                .labels(StepMetricLabels {
                                    step_name: step.name(),
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
                                .unwrap()
                                .log_metrics();
                            match output_sender.send(output_with_context).await {
                                Ok(_) => {},
                                Err(e) => {
                                    error!(step_name, "Error sending output to channel: {:?}", e);
                                    break;
                                },
                            }
                        }
                    };
                    last_poll = tokio::time::Instant::now();
                }

                // Since we just polled, we should check if we need to continue polling
                if !step.should_continue_polling().await {
                    break;
                }

                let elapsed = last_poll.elapsed();
                let time_to_next_poll = if elapsed >= poll_duration {
                    Duration::from_secs(0)
                } else {
                    poll_duration - elapsed
                };

                tokio::select! {
                    _ = tokio::time::sleep(time_to_next_poll) => {
                        let polling_duration = Instant::now();
                        let result = match step.poll().await {
                            Ok(result) => result,
                            Err(e) => {
                                error!(step_name, "Failed to poll: {:?}", e);
                                break;
                            }
                        };
                        StepMetricsBuilder::default()
                            .labels(StepMetricLabels {
                                step_name: step.name(),
                            })
                            .polling_duration_in_secs(polling_duration.elapsed().as_secs_f64())
                            .build()
                            .unwrap()
                            .log_metrics();
                        if let Some(outputs_with_context) = result {
                            for output_with_context in outputs_with_context {
                                StepMetricsBuilder::default()
                                    .labels(StepMetricLabels {
                                        step_name: step.name(),
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
                                    .unwrap()
                                    .log_metrics();

                                match output_sender.send(output_with_context).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        error!(step_name, "Error sending output to channel: {:?}", e);
                                        break;
                                    }
                                }
                            }
                        };
                        last_poll = tokio::time::Instant::now();
                    }
                    input_with_context_res = input_receiver.recv() => {
                        match input_with_context_res {
                            Ok(input_with_context) => {
                                let processing_duration = Instant::now();
                                let output_with_context = match step.process(input_with_context).await {
                                    Ok(output_with_context) => output_with_context,
                                    Err(e) => {
                                        error!(step_name, "Failed to process input: {:?}", e);
                                        break;
                                    }
                                };
                                if let Some(output_with_context) = output_with_context {
                                    StepMetricsBuilder::default()
                                    .labels(StepMetricLabels {
                                        step_name: step.name(),
                                    })
                                    .latest_processed_version(output_with_context.end_version)
                                    .latest_transaction_timestamp(
                                        output_with_context.get_start_transaction_timestamp_unix(),
                                    )
                                    .num_transactions_processed_count(
                                        output_with_context.get_num_transactions(),
                                    )
                                    .processing_duration_in_secs(processing_duration.elapsed().as_secs_f64())
                                    .processed_size_in_bytes(output_with_context.total_size_in_bytes)
                                    .build()
                                    .unwrap()
                                    .log_metrics();

                                    match output_sender.send(output_with_context).await {
                                        Ok(_) => {},
                                        Err(e) => {
                                            error!(step_name, "Error sending output to channel: {:?}", e);
                                            break;
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                // If the previous steps have finished and the channels have closed , we should break out of the loop
                                warn!(step_name, "Error receiving input from channel: {:?}", e);
                                break;
                            }
                        }
                    }
                }
            }

            info!(step_name, "Polling has ended.");

            // Do any additional cleanup
            let res = step.cleanup().await;
            match res {
                Ok(Some(outputs_with_context)) => {
                    for output_with_context in outputs_with_context {
                        StepMetricsBuilder::default()
                            .labels(StepMetricLabels {
                                step_name: step.name(),
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
                            .unwrap()
                            .log_metrics();

                        match output_sender.send(output_with_context).await {
                            Ok(_) => {},
                            Err(e) => {
                                error!(step_name, "Error sending output to channel: {:?}", e);
                                break;
                            },
                        }
                    }
                },
                Ok(None) => (),
                Err(e) => {
                    error!(step_name, "Error cleaning up step: {:?}", e);
                },
            }

            // Wait for output channel to be empty before ending the task and closing the send channel
            loop {
                let channel_size = output_sender.len();
                info!(
                    step_name,
                    channel_size, "Waiting for output channel to be empty"
                );
                if channel_size.is_zero() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!(step_name, "Output channel is empty. Closing send channel.");
        });

        (output_receiver, handle)
    }
}

use crate::{
    traits::{
        processable::RunnableStepType, IntoRunnableStep, NamedStep, Processable, RunnableStep,
    },
    types::transaction_context::TransactionContext,
};
use async_trait::async_trait;
use instrumented_channel::{instrumented_bounded_channel, InstrumentedAsyncReceiver};
use std::time::Duration;
use tokio::task::JoinHandle;

#[async_trait]
pub trait PollableAsyncStep
where
    Self: Processable + NamedStep + Send + Sized + 'static,
{
    /// Returns the duration between poll attempts.
    fn poll_interval(&self) -> Option<Duration> {
        None
    }

    /// Polls the internal state and returns a batch of output items if available.
    async fn poll(&mut self) -> Option<Vec<TransactionContext<Self::Output>>>;

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
            let poll_duration = step.poll_interval();

            let mut last_poll = tokio::time::Instant::now();

            step.init().await;

            while step.should_continue_polling().await {
                // It's possible that the channel always has items, so we need to ensure we call `poll` manually if we need to
                match poll_duration {
                    Some(poll_duration) => {
                        if last_poll.elapsed() >= poll_duration {
                            let result = step.poll().await;
                            if let Some(outputs) = result {
                                for output in outputs {
                                    output_sender
                                        .send(output)
                                        .await
                                        .expect("Failed to send output");
                                }
                            };
                            last_poll = tokio::time::Instant::now();
                        }
                    },
                    None => {
                        let result = step.poll().await;
                        if let Some(outputs) = result {
                            for output in outputs {
                                output_sender
                                    .send(output)
                                    .await
                                    .expect("Failed to send output");
                            }
                        };
                    },
                }

                let time_to_next_poll = match poll_duration {
                    Some(poll_duration) => poll_duration - last_poll.elapsed(),
                    None => Duration::from_secs(0),
                };

                tokio::select! {
                    _ = tokio::time::sleep(time_to_next_poll) => {
                        let result = step.poll().await;
                        if let Some(outputs) = result {
                            for output in outputs {
                                output_sender.send(output).await.expect("Failed to send output");
                            }
                        };
                        last_poll = tokio::time::Instant::now();
                    }
                    input_with_context_res = input_receiver.recv() => {
                        match input_with_context_res {
                            Ok(input_with_context) => {
                                let output_with_context = step.process(input_with_context).await;
                                if let Some(output_with_context) = output_with_context {
                                    let output_send_res = output_sender.send(output_with_context).await;

                                    match output_send_res {
                                        Ok(_) => {},
                                        Err(e) => {
                                            panic!("Failed to send output for {}: {:?}", step_name, e);
                                        }
                                    }
                                }
                            },
                            Err(e) => {
                                panic!("Failed to receive input for {}: {:?}", step_name, e);
                            }
                        }
                    }
                }
            }

            // TODO: Wait for channel to be empty before ending the task
            step.cleanup().await;
        });

        (output_receiver, handle)
    }
}

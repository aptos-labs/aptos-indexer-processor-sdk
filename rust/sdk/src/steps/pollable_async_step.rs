use crate::traits::processable::RunnableStepType;
use crate::traits::{IntoRunnableStep, NamedStep, Processable, RunnableStep};
use async_trait::async_trait;
use kanal::AsyncReceiver;
use std::time::Duration;
use tokio::task::JoinHandle;

#[async_trait]
pub trait PollableAsyncStep
where
    Self: Processable + NamedStep + Send + Sized + 'static,
{
    /// Returns the duration between poll attempts.
    fn poll_interval(&self) -> Duration {
        Duration::from_secs(0)
    }

    /// Polls the internal state and returns a batch of output items if available.
    async fn poll(&mut self) -> Option<Vec<Self::Output>>;

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
        input_receiver: Option<AsyncReceiver<Vec<PollableStep::Input>>>,
        channel_size: usize,
    ) -> (AsyncReceiver<Vec<PollableStep::Output>>, JoinHandle<()>) {
        let (output_sender, output_receiver) = kanal::bounded_async(channel_size);

        let mut step = self.step;
        let input_receiver = input_receiver.expect("Input receiver must be set");

        let handle = tokio::spawn(async move {
            let poll_duration = step.poll_interval();

            let mut last_poll = tokio::time::Instant::now();

            step.init().await;

            while step.should_continue_polling().await {
                // It's possible that the channel always has items, so we need to ensure we call `poll` manually if we need to
                if last_poll.elapsed() >= poll_duration {
                    let result = step.poll().await;
                    if let Some(output) = result {
                        output_sender
                            .send(output)
                            .await
                            .expect("Failed to send output");
                    };
                    last_poll = tokio::time::Instant::now();
                }

                let time_to_next_poll = poll_duration - last_poll.elapsed();

                tokio::select! {
                    _ = tokio::time::sleep(time_to_next_poll) => {
                        let result = step.poll().await;
                        if let Some(output) = result {
                            output_sender.send(output).await.expect("Failed to send output");
                        };
                        last_poll = tokio::time::Instant::now();
                    }
                    input = input_receiver.recv() => {
                        let input = input.expect("Failed to receive input");
                        let output = step.process(input).await;
                        if !output.is_empty() {
                            output_sender.send(output).await.expect("Failed to send output");
                        }
                    }
                }
            }

            step.cleanup().await;
        });

        (output_receiver, handle)
    }
}

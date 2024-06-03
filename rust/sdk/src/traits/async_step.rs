use std::fmt::Debug;
use crate::traits::instrumentation::NamedStep;
use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::instrument;

#[async_trait]
pub trait AsyncStep: NamedStep
    where
        Self: Debug + Sized + Send + 'static,
{
    type Input: Debug + Send + 'static;
    type Output: Send + 'static;

    /// Processes a batch of input items and returns a batch of output items.
    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output>;

    /// Returns the input channel for receiving input items.
    fn input_receiver(&mut self) -> &AsyncReceiver<Vec<Self::Input>>;

    /// Returns the output channel for sending output items.
    fn output_sender(&mut self) -> &AsyncSender<Vec<Self::Output>>;
}

#[async_trait]
#[allow(dead_code)]
pub trait PollableAsyncStep: AsyncStep
    where
        Self: Sized + Send + 'static,
{
    /// Returns the duration between poll attempts.
    fn poll_interval(&self) -> Duration;

    /// Polls the internal state and returns a batch of output items if available.
    async fn poll(&mut self) -> Option<Vec<<Self as AsyncStep>::Output>>;
}

// TODO: Implement this for everything we can automatically?
pub trait SpawnsPollable: PollableAsyncStep {
    #[instrument]
    fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let input_receiver = self.input_receiver().clone();
            let output_sender = self.output_sender().clone();
            let poll_duration = self.poll_interval();

            let mut last_poll = tokio::time::Instant::now();

            loop {
                // It's possible that the channel always has items, so we need to ensure we call `poll` manually if we need to
                if last_poll.elapsed() >= poll_duration {
                    let result = self.poll().await;
                    if let Some(output) = result {
                        output_sender
                            .send(output)
                            .await
                            .expect("Failed to send output");
                    };
                    last_poll = tokio::time::Instant::now();
                }

                tokio::select! {
                    _ = tokio::time::sleep(poll_duration) => {
                        let result = self.poll().await;
                        if let Some(output) = result {
                            output_sender.send(output).await.expect("Failed to send output");
                        };
                        last_poll = tokio::time::Instant::now();
                    }
                    input = input_receiver.recv() => {
                        let input = input.expect("Failed to receive input");
                        let output = self.process(input).await;
                        if !output.is_empty() {
                            output_sender.send(output).await.expect("Failed to send output");
                        }
                    }
                }
            }
        })
    }
}

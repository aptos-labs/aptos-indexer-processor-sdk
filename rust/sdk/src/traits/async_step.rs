use crate::{connectors::AsyncStepConnector, traits::instrumentation::NamedStep};
use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};
use std::time::Duration;
use tokio::task::JoinHandle;
use crate::connectors::AsyncStepChannelWrapper;
use crate::stream::Transaction;

#[async_trait]
pub trait AsyncStep
    where
        Self: NamedStep + Sized + Send + 'static,
{
    type Input: Send + 'static;
    type Output: Send + 'static;

    /// Processes a batch of input items and returns a batch of output items.
    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output>;

    fn connect<NextStep>(self, next_step: NextStep) -> AsyncStepConnector<Self, NextStep>
        where
            NextStep: AsyncStep<Input=Self::Output>,
    {
        AsyncStepConnector {
            left_step: self,
            right_step: next_step,
        }
    }

    /**
    InputSender -> InputReceiver => Step => OutputSender -> OutputReceiver
    **/

    fn input_sender(&mut self) -> Option<&AsyncSender<Vec<Self::Input>>> {
        None
    }

    fn input_receiver(&mut self) -> Option<&AsyncReceiver<Vec<Self::Input>>> {
        None
    }

    fn output_sender(&mut self) -> Option<&AsyncSender<Vec<Self::Output>>> {
        None
    }

    fn output_receiver(&mut self) -> Option<&AsyncReceiver<Vec<Self::Output>>> {
        None
    }

    // setters for above
    fn set_input_sender(&mut self, _sender: Option<AsyncSender<Vec<Self::Input>>>) {
        panic!("Can not set input sender for this step: {}", self.name());
    }

    fn set_input_receiver(&mut self, _receiver: Option<AsyncReceiver<Vec<Self::Input>>>) {
        panic!("Can not set input receiver for this step: {}", self.name());
    }

    fn set_output_sender(&mut self, _sender: Option<AsyncSender<Vec<Self::Output>>>) {
        panic!("Can not set output sender for this step: {}", self.name());
    }

    fn set_output_receiver(&mut self, _receiver: Option<AsyncReceiver<Vec<Self::Output>>>) {
        panic!("Can not set output receiver for this step: {}", self.name());
    }

    fn connect_channeled<NextStep>(self, next_step: NextStep, channel_size: usize) -> AsyncStepChannelWrapper<AsyncStepConnector<Self, NextStep>>
        where
            NextStep: AsyncStep<Input=Self::Output>,
    {
        let mut left_step = self;
        let mut right_step = AsyncStepChannelWrapper::new(next_step, None, None);

        if left_step.output_sender().is_none() && right_step.input_receiver().is_none() {
            let (left_output_sender, right_input_receiver) = kanal::bounded_async::<Vec<Self::Input>>(channel_size);
            left_step.set_output_sender(Some(left_output_sender));
            right_step.set_input_receiver(Some(right_input_receiver));
        } else {
            if left_step.output_sender().is_some() ^ right_step.input_receiver().is_some() {
                panic!("Both output sender and input receiver must be present or absent");
            }
        }



        let (output_sender, output_receiver) = kanal::bounded_async::<Vec<<NextStep as AsyncStep>::Output>>(channel_size);

        AsyncStepChannelWrapper {
            step: connected,
            input_sender: Some(input_sender),
            input_receiver: Some(input_receiver),
            output_sender: Some(output_sender),
            output_receiver: Some(output_receiver),
        }
    }
}


#[async_trait]
#[allow(dead_code)]
pub trait PollableAsyncStep
    where
        Self: Sized + Send + 'static + AsyncStep,
{
    /// Returns the duration between poll attempts.
    fn poll_interval(&self) -> Duration;

    /// Polls the internal state and returns a batch of output items if available.
    async fn poll(&mut self) -> Option<Vec<<Self as AsyncStep>::Output>>;
}

// TODO: Implement this for everything we can automatically?
pub trait SpawnsPollable: PollableAsyncStep {
    fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let input_receiver = self.input_receiver().unwrap().clone();
            let output_sender = self.output_sender().unwrap().clone();
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

/// Spawns without polling
pub trait SpawnsAsync: AsyncStep {
    fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let input_receiver = self.input_receiver().unwrap().clone();
            let output_sender = self.output_sender().unwrap().clone();

            loop {
                tokio::select! {
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

/// Spawns pollable with only output sender
pub trait SpawnsPollableWithOutput: PollableAsyncStep {
    fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let output_sender = self.output_sender().unwrap().clone();
            let poll_duration = self.poll_interval();
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(poll_duration) => {
                        let result = self.poll().await;
                        if let Some(output) = result {
                            output_sender.send(output).await.expect("Failed to send output");
                        };
                    }
                }
            }
        })
    }
}


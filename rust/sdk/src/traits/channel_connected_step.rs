use std::time::Duration;

use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};
use tokio::task::JoinHandle;

use super::instrumentation::NamedStep;

#[async_trait]
pub trait ChannelConnectableStep
where
    Self: NamedStep + Sized + Send + 'static,
{
    type Input: Send + 'static;
    type Output: Send + 'static;

    async fn init(self) {}

    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output> {
        Vec::new()
    }

    fn connect_channel<NextStep>(self, next_step: NextStep) -> ChannelConnector<Self, NextStep>
    where
        NextStep: ChannelConnectableStep,
    {
        ChannelConnector {
            left_step: self,
            right_step: next_step,
        }
    }
}

pub struct ChannelConnectableStepManager<Step>
where
    Step: ChannelConnectableStep,
{
    pub step: Step,
    pub input_receiver: Option<AsyncReceiver<Vec<Step::Input>>>,
    pub output_sender: Option<AsyncSender<Vec<Step::Output>>>,
}

#[async_trait]
pub trait ChannelConnectableStepWithInput: ChannelConnectableStep
where
    Self: ChannelConnectableStep + Sized + Send + 'static,
{
    /// Returns the input channel for receiving input items.
    fn input_receiver(&mut self) -> &AsyncReceiver<Vec<Self::Input>>;

    // async fn get_next_input(&mut self) -> Vec<Self::Input>;
}

#[async_trait]
pub trait ChannelConnectableStepWithOutput: ChannelConnectableStep
where
    Self: ChannelConnectableStep + Sized + Send + 'static,
{
    /// Returns the output channel for sending output items.
    fn output_sender(&mut self) -> &AsyncSender<Vec<Self::Output>>;

    // async fn send_output(&mut self);
}

pub struct ChannelConnector<LeftStep, RightStep>
where
    LeftStep: ChannelConnectableStep,
    RightStep: ChannelConnectableStep,
{
    pub left_step: LeftStep,
    pub right_step: RightStep,
    // pub left_receiver: AsyncReceiver<Input>,
    // pub output_sender: AsyncSender<Output>,
}

impl<LeftStep, RightStep> ChannelConnectableStep for ChannelConnector<LeftStep, RightStep>
where
    LeftStep: ChannelConnectableStep,
    RightStep: ChannelConnectableStep,
{
    type Input = LeftStep::Input;
    type Output = RightStep::Output;
}

impl<LeftStep, RightStep> NamedStep for ChannelConnector<LeftStep, RightStep>
where
    LeftStep: ChannelConnectableStep,
    RightStep: ChannelConnectableStep,
{
    fn name(&self) -> String {
        format!("{} -> {}", self.left_step.name(), self.right_step.name())
    }
}

#[async_trait]
#[allow(dead_code)]
pub trait PollableStep
where
    Self: Sized + Send + 'static + ChannelConnectableStep,
{
    /// Returns the duration between poll attempts.
    fn poll_interval(&self) -> Duration;

    /// Polls the internal state and returns a batch of output items if available.
    async fn poll(&mut self) -> Option<Vec<<Self as ChannelConnectableStep>::Output>>;
}

// TODO: Implement this for everything we can automatically?
pub trait SpawnsPollable:
    PollableStep + ChannelConnectableStepWithInput + ChannelConnectableStepWithOutput
{
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

/// Spawns without polling
pub trait SpawnsNonPollable:
    ChannelConnectableStep + ChannelConnectableStepWithInput + ChannelConnectableStepWithOutput
{
    fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let input_receiver = self.input_receiver().clone();
            let output_sender = self.output_sender().clone();

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
pub trait SpawnsPollableWithOutput: PollableStep + ChannelConnectableStepWithOutput {
    fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let output_sender = self.output_sender().clone();
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

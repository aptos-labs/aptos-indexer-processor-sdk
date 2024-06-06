use super::channel_connected_step::{
    ChannelConnectableStep, ChannelConnectableStepWithInput, ChannelConnectableStepWithOutput,
    SpawnsNonPollable,
};
use crate::traits::instrumentation::NamedStep;
use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};

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
        NextStep: AsyncStep<Input = Self::Output>,
    {
        AsyncStepConnector {
            left_step: self,
            right_step: next_step,
        }
    }
}

/// Connects the output of LeftStep -> input of RightStep
pub struct AsyncStepConnector<LeftStep, RightStep>
where
    LeftStep: AsyncStep,
    RightStep: AsyncStep,
{
    pub left_step: LeftStep,
    pub right_step: RightStep,
}

impl<LeftStep, RightStep> AsyncStepConnector<LeftStep, RightStep>
where
    LeftStep: AsyncStep,
    RightStep: AsyncStep,
{
    pub fn new(left_step: LeftStep, right_step: RightStep) -> Self {
        Self {
            left_step,
            right_step,
        }
    }
}

#[async_trait]
impl<LeftStep, RightStep> AsyncStep for AsyncStepConnector<LeftStep, RightStep>
where
    LeftStep: AsyncStep,
    RightStep: AsyncStep<Input = LeftStep::Output>,
{
    type Input = LeftStep::Input;
    type Output = RightStep::Output;

    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output> {
        let left_step_output = self.left_step.process(items).await;
        self.right_step.process(left_step_output).await
    }
}

impl<LeftStep, RightStep> NamedStep for AsyncStepConnector<LeftStep, RightStep>
where
    LeftStep: AsyncStep,
    RightStep: AsyncStep,
{
    fn name(&self) -> String {
        format!("{} -> {}", self.left_step.name(), self.right_step.name())
    }
}

/// Wraps an async step with input receiver and output sender
pub struct AsyncStepChannelWrapper<Step>
where
    Step: AsyncStep,
{
    pub step: Step,
    pub input_receiver: AsyncReceiver<Vec<Step::Input>>,
    pub output_sender: AsyncSender<Vec<Step::Output>>,
}

impl<Step> AsyncStepChannelWrapper<Step>
where
    Step: AsyncStep,
{
    pub fn new(
        step: Step,
        input_receiver: AsyncReceiver<Vec<Step::Input>>,
        output_sender: AsyncSender<Vec<Step::Output>>,
    ) -> Self {
        Self {
            step,
            input_receiver,
            output_sender,
        }
    }
}

#[async_trait]
impl<Step> ChannelConnectableStep for AsyncStepChannelWrapper<Step>
where
    Step: AsyncStep,
{
    type Input = Step::Input;
    type Output = Step::Output;

    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output> {
        self.step.process(items).await
    }
}

#[async_trait]
impl<Step> ChannelConnectableStepWithOutput for AsyncStepChannelWrapper<Step>
where
    Step: AsyncStep,
{
    fn output_sender(&mut self) -> &AsyncSender<Vec<Step::Output>> {
        &self.output_sender
    }
}

#[async_trait]
impl<Step> ChannelConnectableStepWithInput for AsyncStepChannelWrapper<Step>
where
    Step: AsyncStep,
{
    fn input_receiver(&mut self) -> &AsyncReceiver<Vec<Step::Input>> {
        &self.input_receiver
    }
}

impl<Step> SpawnsNonPollable for AsyncStepChannelWrapper<Step> where Step: AsyncStep {}

impl<Step> NamedStep for AsyncStepChannelWrapper<Step>
where
    Step: AsyncStep,
{
    fn name(&self) -> String {
        self.step.name()
    }
}

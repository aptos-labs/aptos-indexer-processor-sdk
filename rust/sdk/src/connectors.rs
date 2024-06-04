use std::time::Duration;

use crate::traits::{
    async_step::{
        AsyncStep, AsyncStepWithInput, AsyncStepWithOutput, PollableAsyncStep, SpawnsAsync,
    },
    instrumentation::NamedStep,
};
use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};

/// Connects the output of FirstStep -> input of SecondStep
pub struct AsyncStepConnector<FirstStep, SecondStep>
where
    FirstStep: AsyncStep,
    SecondStep: AsyncStep,
{
    pub first_step: FirstStep,
    pub second_step: SecondStep,
}

impl<FirstStep, SecondStep> AsyncStepConnector<FirstStep, SecondStep>
where
    FirstStep: AsyncStep,
    SecondStep: AsyncStep,
{
    pub fn new(first_step: FirstStep, second_step: SecondStep) -> Self {
        Self {
            first_step,
            second_step,
        }
    }
}

#[async_trait]
impl<FirstStep, SecondStep> AsyncStep for AsyncStepConnector<FirstStep, SecondStep>
where
    FirstStep: AsyncStep,
    SecondStep: AsyncStep<Input = FirstStep::Output>,
{
    type Input = FirstStep::Input;
    type Output = SecondStep::Output;

    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output> {
        let first_step_output = self.first_step.process(items).await;
        self.second_step.process(first_step_output).await
    }
}

impl<FirstStep, SecondStep> NamedStep for AsyncStepConnector<FirstStep, SecondStep>
where
    FirstStep: AsyncStep,
    SecondStep: AsyncStep,
{
    fn name(&self) -> String {
        format!("{} -> {}", self.first_step.name(), self.second_step.name())
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
impl<Step> AsyncStep for AsyncStepChannelWrapper<Step>
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
impl<Step> AsyncStepWithOutput for AsyncStepChannelWrapper<Step>
where
    Step: AsyncStep,
{
    fn output_sender(&mut self) -> &AsyncSender<Vec<Step::Output>> {
        &self.output_sender
    }
}

#[async_trait]
impl<Step> AsyncStepWithInput for AsyncStepChannelWrapper<Step>
where
    Step: AsyncStep,
{
    fn input_receiver(&mut self) -> &AsyncReceiver<Vec<Step::Input>> {
        &self.input_receiver
    }
}

impl<Step> SpawnsAsync for AsyncStepChannelWrapper<Step> where Step: AsyncStep {}

impl<Step> NamedStep for AsyncStepChannelWrapper<Step>
where
    Step: AsyncStep,
{
    fn name(&self) -> String {
        self.step.name()
    }
}

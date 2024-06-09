use crate::traits::{
    async_step::{AsyncStep, SpawnsAsync},
    instrumentation::NamedStep,
};
use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};

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
        RightStep: AsyncStep<Input=LeftStep::Output>,
{
    type Input = LeftStep::Input;
    type Output = RightStep::Output;

    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output> {
        let left_step_output = self.left_step.process(items).await;
        self.right_step.process(left_step_output).await
    }

    fn input_receiver(&mut self) -> Option<&AsyncReceiver<Vec<Self::Input>>> {
        self.left_step.input_receiver()
    }

    fn output_sender(&mut self) -> Option<&AsyncSender<Vec<Self::Output>>> {
        self.right_step.output_sender()
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
#[derive(Clone, Debug)]
pub struct AsyncStepChannelWrapper<Step>
    where
        Step: AsyncStep,
{
    pub step: Step,
    pub input_sender: Option<AsyncSender<Vec<Step::Input>>>,
    pub input_receiver: Option<AsyncReceiver<Vec<Step::Input>>>,
    pub output_sender: Option<AsyncSender<Vec<Step::Output>>>,
    pub output_receiver: Option<AsyncReceiver<Vec<Step::Output>>>,
}

impl<Step> AsyncStepChannelWrapper<Step>
    where
        Step: AsyncStep,
{
    pub fn new(
        step: Step,
        input_receiver: Option<AsyncReceiver<Vec<Step::Input>>>,
        output_sender: Option<AsyncSender<Vec<Step::Output>>>,
    ) -> Self {
        Self {
            step,
            input_sender: None,
            input_receiver,
            output_sender,
            output_receiver: None,
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

    fn input_receiver(&mut self) -> Option<&AsyncReceiver<Vec<Self::Input>>> {
        self.input_receiver.as_ref()
    }

    fn output_sender(&mut self) -> Option<&AsyncSender<Vec<Self::Output>>> {
        self.output_sender.as_ref()
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

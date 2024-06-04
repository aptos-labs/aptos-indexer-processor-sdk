use crate::traits::{async_step::AsyncStep, instrumentation::NamedStep};
use async_trait::async_trait;

pub struct AsyncStepConnector<FirstStep, SecondStep>
where
    FirstStep: AsyncStep,
    SecondStep: AsyncStep,
{
    pub first_step: FirstStep,
    pub second_step: SecondStep,
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

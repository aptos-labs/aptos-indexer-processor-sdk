// pub struct Pipeline<FirstStep>
// where
//     FirstStep: Step,
// {
//     first_step: FirstStep,
// }

// impl<FirstStep> Pipeline<FirstStep>
// where
//     FirstStep: Step,
// {
//     pub fn new(first_step: FirstStep) -> Self {
//         Pipeline { first_step }
//     }

//     pub async fn execute(
//         &self,
//         input: FirstStep::Input,
//     ) -> Result<FirstStep::Output, Box<dyn std::error::Error + Send + Sync + 'static>> {
//         // Go through the dag and initialize all the steps
//         self.first_step.process(input).await
//     }
// }

pub struct Pipeline<InitialStep>
where
    InitialStep: InitialAsyncStep,
{
    firstStep: AsyncStep,
}

impl<InitialStep> Pipeline<InitialStep>
where
    InitialStep: AsyncStep,
{
    pub fn new(firstStep: InitialStep) -> Self {
        Pipeline { firstStep }
    }

    pub fn connect<NextStep>(
        &self,
        nextStep: NextStep,
    ) -> Pipeline<AsyncStepChannelConnector<InitialStep, NextStep>>
    where
        NextStep: AsyncStep,
    {
        Pipeline {
            firstStep: AsyncStepChannelConnector {
                first_step: self.firstStep,
                second_step: nextStep,
            },
        }
    }

    pub fn run(&self) {}
}

use crate::traits::async_step::{AsyncStep, InitialAsyncStep};

pub struct AsyncStepChannelConnector<FirstStep, SecondStep>
where
    FirstStep: AsyncStep,
    SecondStep: AsyncStep,
{
    first_step: FirstStep,
    second_step: SecondStep,
}

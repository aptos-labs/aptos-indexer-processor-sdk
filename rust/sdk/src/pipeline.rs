pub struct Pipeline<FirstStep>
where
    FirstStep: Step,
{
    first_step: FirstStep,
}

impl<FirstStep> Pipeline<FirstStep>
where
    FirstStep: Step,
{
    pub fn new(first_step: FirstStep) -> Self {
        Pipeline { first_step }
    }

    pub async fn execute(
        &self,
        input: FirstStep::Input,
    ) -> Result<FirstStep::Output, Box<dyn std::error::Error + Send + Sync + 'static>> {
        // Go through the dag and initialize all the steps
        self.first_step.process(input).await
    }
}

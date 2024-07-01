use super::metrics::METRICS_PREFIX;
use crate::traits::{
    processable::RunnableStepType, IntoRunnableStep, NamedStep, Processable, RunnableStep,
};
use async_trait::async_trait;
use kanal::AsyncReceiver;
use tokio::task::JoinHandle;

#[async_trait]
pub trait AsyncStep
where
    Self: Processable + Send + Sized + 'static,
{
}

pub struct AsyncRunType;

impl RunnableStepType for AsyncRunType {}

pub struct RunnableAsyncStep<Step>
where
    Step: AsyncStep,
{
    pub step: Step,
}

impl<Step> RunnableAsyncStep<Step>
where
    Step: AsyncStep,
{
    pub fn new(step: Step) -> Self {
        // latest version metric
        // latest timestamp metric
        // number of transactions metric
        // processing duration
        // transaction size
        // processing error count
        Self { step }
    }
}

impl<Step> NamedStep for RunnableAsyncStep<Step>
where
    Step: 'static + AsyncStep + Send + Sized,
{
    fn name(&self) -> String {
        self.step.name()
    }

    fn type_name(&self) -> String {
        let step_type = std::any::type_name::<Step>().to_string();
        format!("{} (via RunnableAsyncStep)", step_type)
    }
}

impl<Step> IntoRunnableStep<Step::Input, Step::Output, Step, AsyncRunType> for Step
where
    Step: AsyncStep<RunType = AsyncRunType> + Send + Sized + 'static,
{
    fn into_runnable_step(self) -> impl RunnableStep<Step::Input, Step::Output> {
        RunnableAsyncStep::new(self)
    }
}

impl<Step> RunnableStep<Step::Input, Step::Output> for RunnableAsyncStep<Step>
where
    Step: AsyncStep + Send + Sized + 'static,
{
    fn spawn(
        self,
        input_receiver: Option<AsyncReceiver<Vec<Step::Input>>>,
        output_channel_size: usize,
    ) -> (AsyncReceiver<Vec<Step::Output>>, JoinHandle<()>) {
        let (output_sender, output_receiver) = kanal::bounded_async(output_channel_size);
        let input_receiver = input_receiver.expect("Input receiver must be set");

        let mut step = self.step;
        let handle = tokio::spawn(async move {
            loop {
                let input = input_receiver
                    .recv()
                    .await
                    .expect("Failed to receive input");
                let output = step.process(input).await;
                if !output.is_empty() {
                    output_sender
                        .send(output)
                        .await
                        .expect("Failed to send output");
                }
            }
        });

        (output_receiver, handle)
    }
}

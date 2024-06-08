use crate::traits::{runnable_step::Async, IntoRunnableStep, Processable, RunnableStep};
use async_trait::async_trait;
use kanal::AsyncReceiver;
use tokio::task::JoinHandle;

#[async_trait]
pub trait AsyncStep
where
    Self: Processable + Send + Sized + 'static,
{
}

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
        Self { step }
    }
}

impl<Step> RunnableStep<Step::Input, Step::Output> for RunnableAsyncStep<Step>
where
    Step: AsyncStep + Send + Sized + 'static,
{
    fn spawn(
        self,
        input_receiver: Option<AsyncReceiver<Vec<Step::Input>>>,
        channel_size: usize,
    ) -> (AsyncReceiver<Vec<Step::Output>>, JoinHandle<()>) {
        let (output_sender, output_receiver) = kanal::bounded_async(channel_size);
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

impl<Step> IntoRunnableStep<Step::Input, Step::Output, Step, Async> for Step
where
    Step: AsyncStep<RunType = Async> + Send + Sized + 'static,
{
    fn into_runnable_step(self) -> impl RunnableStep<Step::Input, Step::Output> {
        RunnableAsyncStep::new(self)
    }
}

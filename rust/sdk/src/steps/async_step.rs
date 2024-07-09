use std::time::Instant;

use crate::{
    metrics::{
        step_metrics::{StepMetricLabels, StepMetricsBuilder},
        transaction_context::TransactionContext,
    },
    traits::{
        processable::RunnableStepType, IntoRunnableStep, NamedStep, Processable, RunnableStep,
    },
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
        input_receiver: Option<AsyncReceiver<TransactionContext<Step::Input>>>,
        output_channel_size: usize,
    ) -> (
        AsyncReceiver<TransactionContext<Step::Output>>,
        JoinHandle<()>,
    ) {
        let (output_sender, output_receiver) = kanal::bounded_async(output_channel_size);
        let input_receiver = input_receiver.expect("Input receiver must be set");

        let mut step = self.step;
        let handle = tokio::spawn(async move {
            loop {
                let input_with_context = input_receiver
                    .recv()
                    .await
                    .expect("Failed to receive input");
                let processing_duration = Instant::now();
                let output_with_context = step.process(input_with_context).await;
                StepMetricsBuilder::default()
                    .labels(StepMetricLabels {
                        step_name: step.name(),
                    })
                    .latest_processed_version(output_with_context.end_version)
                    .latest_transaction_timestamp(
                        output_with_context.get_start_transaction_timestamp_unix(),
                    )
                    .num_transactions_processed_count(output_with_context.get_num_transactions())
                    .processing_duration_in_secs(processing_duration.elapsed().as_secs_f64())
                    .transaction_size(output_with_context.total_size_in_bytes)
                    .build()
                    .unwrap()
                    .log_metrics();
                output_sender
                    .send(output_with_context)
                    .await
                    .expect("Failed to send output");
            }
        });

        (output_receiver, handle)
    }
}

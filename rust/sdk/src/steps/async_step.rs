use super::step_metrics::{StepMetricLabels, StepMetricsBuilder};
use crate::{
    traits::{
        processable::RunnableStepType, IntoRunnableStep, NamedStep, Processable, RunnableStep,
    },
    types::transaction_context::TransactionContext,
};
use async_trait::async_trait;
use instrumented_channel::{instrumented_bounded_channel, InstrumentedAsyncReceiver};
use std::time::Instant;
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
        input_receiver: Option<InstrumentedAsyncReceiver<TransactionContext<Step::Input>>>,
        output_channel_size: usize,
    ) -> (
        InstrumentedAsyncReceiver<TransactionContext<Step::Output>>,
        JoinHandle<()>,
    ) {
        let mut step = self.step;
        let step_name = step.name();
        let input_receiver = input_receiver.expect("Input receiver must be set");

        let (output_sender, output_receiver) =
            instrumented_bounded_channel(&format!("{}: Output", step_name), output_channel_size);

        let handle = tokio::spawn(async move {
            loop {
                let input_with_context = input_receiver
                    .recv()
                    .await
                    .expect("Failed to receive input");
                let processing_duration = Instant::now();
                let output_with_context = step.process(input_with_context).await;
                if let Some(output_with_context) = output_with_context {
                    StepMetricsBuilder::default()
                        .labels(StepMetricLabels {
                            step_name: step.name(),
                        })
                        .latest_processed_version(output_with_context.end_version)
                        .latest_transaction_timestamp(
                            output_with_context.get_start_transaction_timestamp_unix(),
                        )
                        .num_transactions_processed_count(
                            output_with_context.get_num_transactions(),
                        )
                        .processing_duration_in_secs(processing_duration.elapsed().as_secs_f64())
                        .processed_size_in_bytes(output_with_context.total_size_in_bytes)
                        .build()
                        .unwrap()
                        .log_metrics();
                    output_sender
                        .send(output_with_context)
                        .await
                        .expect(format!("Failed to send output for {}", step.name()).as_str());
                }
            }
        });

        (output_receiver, handle)
    }
}

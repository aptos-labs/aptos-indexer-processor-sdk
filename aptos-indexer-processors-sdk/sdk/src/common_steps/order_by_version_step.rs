use crate::{
    traits::{
        pollable_async_step::PollableAsyncRunType, NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use ahash::AHashMap;
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;

pub struct OrderByVersionStep<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + 'static,
{
    pub ordered_versions: Vec<TransactionContext<Input>>,
    pub unordered_versions: AHashMap<u64, TransactionContext<Input>>,
    pub expected_next_version: u64,
    // pub versions: BTreeSet<TransactionContext<Input>>,
    // Duration to poll and return the ordered versions
    pub poll_interval: Duration,
}

impl<Input> OrderByVersionStep<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + 'static,
{
    pub fn new(starting_version: u64, poll_interval: Duration) -> Self {
        Self {
            ordered_versions: Vec::new(),
            unordered_versions: AHashMap::new(),
            expected_next_version: starting_version,
            poll_interval,
        }
    }

    fn update_ordered_versions(&mut self) {
        // While there are batches in unordered_versions that are in order, add them to ordered_versions
        while let Some(batch) = self
            .unordered_versions
            .remove(&(self.expected_next_version))
        {
            self.expected_next_version = batch.metadata.end_version + 1;
            self.ordered_versions.push(batch);
        }
    }
}

#[async_trait]
impl<Input> Processable for OrderByVersionStep<Input>
where
    Input: Send + Sync + 'static,
{
    type Input = Input;
    type Output = Input;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        current_batch: TransactionContext<Input>,
    ) -> Result<Option<TransactionContext<Input>>, ProcessorError> {
        // If there's a gap in the expected_next_version and current_version
        // ave the current_version to unordered_versions for later processing.
        if self.expected_next_version != current_batch.metadata.start_version {
            tracing::debug!(
                next_version = self.expected_next_version,
                step = self.name(),
                "Gap detected starting from version: {}",
                current_batch.metadata.start_version
            );
            self.unordered_versions
                .insert(current_batch.metadata.start_version, current_batch);
        } else {
            tracing::debug!("No gap detected");
            self.expected_next_version = current_batch.metadata.end_version + 1;
            self.ordered_versions.push(current_batch);

            // If the current_versions is the expected_next_version, update the ordered_versions
            self.update_ordered_versions();
        }
        // Pass through
        Ok(None) // No immediate output
    }

    // Once polling ends, release the remaining ordered items in buffer
    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        Ok(Some(std::mem::take(&mut self.ordered_versions)))
    }
}

#[async_trait]
impl<Input: Send + Sync + 'static> PollableAsyncStep for OrderByVersionStep<Input> {
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(&mut self) -> Result<Option<Vec<TransactionContext<Input>>>, ProcessorError> {
        Ok(Some(std::mem::take(&mut self.ordered_versions)))
    }
}

impl<Input: Send + 'static> NamedStep for OrderByVersionStep<Input> {
    // TODO: oncecell this somehow? Likely in wrapper struct...
    fn name(&self) -> String {
        format!("OrderByVersionStep: {}", std::any::type_name::<Input>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        builder::ProcessorBuilder,
        test::{steps::pass_through_step::PassThroughStep, utils::receive_with_timeout},
        traits::{IntoRunnableStep, RunnableStepWithInputReceiver},
        types::transaction_context::TransactionMetadata,
    };
    use instrumented_channel::instrumented_bounded_channel;

    fn generate_unordered_transaction_contexts() -> Vec<TransactionContext<()>> {
        vec![
            TransactionContext {
                data: (),
                metadata: TransactionMetadata {
                    start_version: 100,
                    end_version: 199,
                    start_transaction_timestamp: None,
                    end_transaction_timestamp: None,
                    total_size_in_bytes: 0,
                },
            },
            TransactionContext {
                data: (),
                metadata: TransactionMetadata {
                    start_version: 0,
                    end_version: 99,
                    start_transaction_timestamp: None,
                    end_transaction_timestamp: None,
                    total_size_in_bytes: 0,
                },
            },
        ]
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::needless_return)]
    async fn test_order_step() {
        // Setup
        let (input_sender, input_receiver) = instrumented_bounded_channel("input", 1);
        let input_step = RunnableStepWithInputReceiver::new(
            input_receiver,
            PassThroughStep::default().into_runnable_step(),
        );
        let order_step = OrderByVersionStep::<()>::new(0, Duration::from_millis(250));

        let (_pb, mut output_receiver) =
            ProcessorBuilder::new_with_runnable_input_receiver_first_step(input_step)
                .connect_to(order_step.into_runnable_step(), 5)
                .end_and_return_output_receiver(5);

        let unordered_transaction_contexts = generate_unordered_transaction_contexts();
        let mut ordered_transaction_contexts = unordered_transaction_contexts.clone();
        ordered_transaction_contexts.sort();

        for transaction_context in unordered_transaction_contexts {
            input_sender.send(transaction_context).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        for ordered_transaction_context in ordered_transaction_contexts {
            let result = receive_with_timeout(&mut output_receiver, 100)
                .await
                .unwrap();
            assert_eq!(
                result.metadata.start_version,
                ordered_transaction_context.metadata.start_version
            );
        }
    }
}

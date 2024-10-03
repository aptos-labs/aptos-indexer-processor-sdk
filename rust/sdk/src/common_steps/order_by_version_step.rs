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

pub struct OrderByStartingVersionStep<Input>
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

impl<Input> OrderByStartingVersionStep<Input>
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
            self.expected_next_version = batch.end_version + 1;
            self.ordered_versions.push(batch);
        }
    }
}

#[async_trait]
impl<Input> Processable for OrderByStartingVersionStep<Input>
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
        if self.expected_next_version != current_batch.start_version {
            tracing::debug!(
                next_version = self.expected_next_version,
                step = self.name(),
                "Gap detected starting from version: {}",
                current_batch.start_version
            );
            self.unordered_versions
                .insert(current_batch.start_version, current_batch);
        } else {
            tracing::debug!("No gap detected");
            self.expected_next_version = current_batch.end_version + 1;
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
impl<Input: Send + Sync + 'static> PollableAsyncStep for OrderByStartingVersionStep<Input> {
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(&mut self) -> Result<Option<Vec<TransactionContext<Input>>>, ProcessorError> {
        Ok(Some(std::mem::take(&mut self.ordered_versions)))
    }
}

impl<Input: Send + 'static> NamedStep for OrderByStartingVersionStep<Input> {
    // TODO: oncecell this somehow? Likely in wrapper struct...
    fn name(&self) -> String {
        format!(
            "OrderByStartingVersionStep: {}",
            std::any::type_name::<Input>()
        )
    }
}

use crate::{
    traits::{PollableAsyncRunType, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use ahash::AHashMap;
use anyhow::Result;
use async_trait::async_trait;
#[async_trait]
pub trait VersionTrackerStep<T>:
    Processable<Input = T, Output = T, RunType = PollableAsyncRunType>
where
    T: Send + 'static,
{
    // Required methods to be implemented by a tracker.
    async fn save_processor_status(&mut self) -> Result<(), ProcessorError>;
    fn get_next_version(&self) -> u64;
    fn get_seen_versions(&mut self) -> &mut AHashMap<u64, TransactionContext<T>>;
    fn get_last_success_batch(&self) -> &Option<TransactionContext<T>>;
    fn set_next_version(&mut self, next_version: u64);
    fn set_last_success_batch(&mut self, last_success_batch: Option<TransactionContext<T>>);

    fn update_last_success_batch(&mut self, current_batch: TransactionContext<T>) {
        let mut new_prev_batch = current_batch;
        // While there are batches in seen_versions that are in order, update the new_prev_batch to the next batch.
        while let Some(next_version) = self
            .get_seen_versions()
            .remove(&(new_prev_batch.end_version + 1))
        {
            new_prev_batch = next_version;
        }
        self.set_next_version(new_prev_batch.end_version + 1);
        self.set_last_success_batch(Some(new_prev_batch));
    }

    // Default implementation for `process` from `Processable` trait
    async fn process(
        &mut self,
        current_batch: TransactionContext<T>,
    ) -> Result<Option<TransactionContext<T>>, ProcessorError> {
        // If there's a gap in the next_version and current_version, save the current_version to seen_versions for
        // later processing.
        if self.get_next_version() != current_batch.start_version {
            tracing::debug!(
                next_version = self.get_next_version(),
                step = self.name(),
                "Gap detected starting from version: {}",
                current_batch.start_version
            );
            self.get_seen_versions()
                .insert(current_batch.start_version, TransactionContext {
                    data: vec![], // No data is needed for tracking. This is to avoid clone.
                    start_version: current_batch.start_version,
                    end_version: current_batch.end_version,
                    start_transaction_timestamp: current_batch.start_transaction_timestamp.clone(),
                    end_transaction_timestamp: current_batch.end_transaction_timestamp.clone(),
                    total_size_in_bytes: current_batch.total_size_in_bytes,
                });
        } else {
            tracing::debug!("No gap detected");
            // If the current_batch is the next expected version, update the last success batch
            self.update_last_success_batch(TransactionContext {
                data: vec![], // No data is needed for tracking. This is to avoid clone.
                start_version: current_batch.start_version,
                end_version: current_batch.end_version,
                start_transaction_timestamp: current_batch.start_transaction_timestamp.clone(),
                end_transaction_timestamp: current_batch.end_transaction_timestamp.clone(),
                total_size_in_bytes: current_batch.total_size_in_bytes,
            });
        }
        // Pass through
        Ok(Some(current_batch))
    }

    /// Default implementation for `cleanup` from the `Processable` trait
    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        self.save_processor_status().await?;
        Ok(None)
    }
}

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
use tracing::info;

const UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

pub trait ProcessorStatusSaver {
    async fn save_processor_status<T>(
        &self,
        tracker_name: &str,
        last_success_batch: &TransactionContext<T>,
    ) -> Result<(), ProcessorError>
    where
        T: Send + Sync + 'static,
        Self: Send + Sync + 'static,
}

pub struct LatestVersionProcessedTracker<T, S>
where
    Self: Sized + Send + 'static,
    T: Send + Sync + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
{
    tracker_name: String,
    // Next version to process that we expect.
    next_version: u64,
    // Last successful batch of sequentially processed transactions. Includes metadata to write to storage.
    last_success_batch: Option<TransactionContext<T>>,
    // Tracks all the versions that have been processed out of order.
    seen_versions: AHashMap<u64, TransactionContext<T>>,
    processor_status_saver: S,
}

impl<T, S> LatestVersionProcessedTracker<T, S>
where
    Self: Sized + Send + 'static,
    T: Send + Sync + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
{
    pub fn new(starting_version: u64, tracker_name: String, processor_status_saver: S) -> Self {
        Self {
            tracker_name,
            next_version: starting_version,
            last_success_batch: None,
            seen_versions: AHashMap::new(),
            processor_status_saver,
        }
    }

    fn update_last_success_batch(&mut self, current_batch: TransactionContext<T>) {
        let mut new_prev_batch = current_batch;
        // While there are batches in seen_versions that are in order, update the new_prev_batch to the next batch.
        while let Some(next_version) = self
            .seen_versions
            .remove(&(new_prev_batch.metadata.end_version + 1))
        {
            new_prev_batch = next_version;
        }
        self.next_version = new_prev_batch.metadata.end_version + 1;
        self.last_success_batch = Some(new_prev_batch);
    }

    async fn save_processor_status(&mut self) -> Result<(), ProcessorError> {
        if let Some(last_success_batch) = self.last_success_batch.as_ref() {
            self.processor_status_saver
                .save_processor_status(&self.tracker_name.clone(), last_success_batch)
                .await
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<T, S> Processable for LatestVersionProcessedTracker<T, S>
where
    Self: Sized + Send + 'static,
    T: Send + Sync + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
{
    type Input = T;
    type Output = T;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        current_batch: TransactionContext<T>,
    ) -> Result<Option<TransactionContext<T>>, ProcessorError> {
        // info!(
        //     start_version = current_batch.start_version,
        //     end_version = current_batch.end_version,
        //     step_name = self.name(),
        //     "Processing versions"
        // );
        // If there's a gap in the next_version and current_version, save the current_version to seen_versions for
        // later processing.
        if self.next_version != current_batch.metadata.start_version {
            info!(
                expected_next_version = self.next_version,
                step = self.name(),
                batch_version = current_batch.metadata.start_version,
                "Gap detected",
            );
            self.seen_versions.insert(
                current_batch.metadata.start_version,
                TransactionContext {
                    data: vec![], // No data is needed for tracking. This is to avoid clone.
                    metadata: current_batch.metadata.clone(),
                },
            );
        } else {
            // info!("No gap detected");
            // If the current_batch is the next expected version, update the last success batch
            self.update_last_success_batch(TransactionContext {
                data: vec![], // No data is needed for tracking. This is to avoid clone.
                metadata: current_batch.metadata.clone(),
            });
        }
        // Pass through
        Ok(Some(current_batch))
    }

    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        // If processing or polling ends, save the last successful batch to the database.
        self.save_processor_status().await?;
        Ok(None)
    }
}

#[async_trait]
impl<T: Send + 'static, S> PollableAsyncStep for LatestVersionProcessedTracker<T, S>
where
    Self: Sized + Send + Sync + 'static,
    T: Send + Sync + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
{
    fn poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(UPDATE_PROCESSOR_STATUS_SECS)
    }

    async fn poll(&mut self) -> Result<Option<Vec<TransactionContext<T>>>, ProcessorError> {
        // TODO: Add metrics for gap count
        self.save_processor_status().await?;
        // Nothing should be returned
        Ok(None)
    }
}

impl<T, S> NamedStep for LatestVersionProcessedTracker<T, S>
where
    Self: Sized + Send + 'static,
    T: Send + Sync + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
{
    fn name(&self) -> String {
        format!(
            "LatestVersionProcessedTracker: {}",
            std::any::type_name::<T>()
        )
    }
}

use crate::{
    traits::{
        pollable_async_step::PollableAsyncRunType, NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use anyhow::Result;
use async_trait::async_trait;
use std::marker::PhantomData;

pub const DEFAULT_UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

/// The `ProcessorStatusSaver` trait object should be implemented in order to save the latest successfully
/// processed transaction versino to storage. I.e., persisting the `processor_status` to storage.
#[async_trait]
pub trait ProcessorStatusSaver {
    // T represents the transaction type that the processor is tracking.
    async fn save_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError>;
}

/// Tracks the versioned processing of sequential transactions, ensuring no gaps
/// occur between them.
///
/// Important: this step assumes ordered transactions. Please use the `OrederByVersionStep` before this step
/// if the transactions are not ordered.
pub struct VersionTrackerStep<T, S>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
    S: ProcessorStatusSaver + Send + 'static,
{
    /// Last successful batch of sequentially processed transactions. Includes metadata to write to
    /// storage.
    last_success_batch: Option<TransactionContext<()>>,
    /// Last batch that was saved to storage. Used to avoid redundant saves.
    last_saved_batch: Option<TransactionContext<()>>,
    polling_interval_secs: u64,
    processor_status_saver: S,
    _marker: PhantomData<T>,
}

impl<T, S> VersionTrackerStep<T, S>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
    S: ProcessorStatusSaver + Send + 'static,
{
    pub fn new(processor_status_saver: S, polling_interval_secs: u64) -> Self {
        Self {
            last_success_batch: None,
            last_saved_batch: None,
            processor_status_saver,
            polling_interval_secs,
            _marker: PhantomData,
        }
    }

    async fn save_processor_status(&mut self) -> Result<(), ProcessorError> {
        if let Some(last_success_batch) = self.last_success_batch.as_ref() {
            // Only save if the batch has changed since last save
            if self.last_saved_batch.as_ref() != Some(last_success_batch) {
                self.processor_status_saver
                    .save_processor_status(last_success_batch)
                    .await?;
                self.last_saved_batch = Some(last_success_batch.clone());
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<T, S> Processable for VersionTrackerStep<T, S>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
    S: ProcessorStatusSaver + Send + 'static,
{
    type Input = T;
    type Output = T;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        current_batch: TransactionContext<T>,
    ) -> Result<Option<TransactionContext<T>>, ProcessorError> {
        // If there's a gap in version, return an error
        if let Some(last_success_batch) = self.last_success_batch.as_ref() {
            if last_success_batch.metadata.end_version + 1 != current_batch.metadata.start_version {
                return Err(ProcessorError::ProcessError {
                    message: format!(
                        "Gap detected starting from version: {}",
                        current_batch.metadata.start_version
                    ),
                });
            }
        }

        // Update the last success batch
        self.last_success_batch = Some(TransactionContext {
            data: (),
            metadata: current_batch.metadata.clone(),
        });

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
impl<T: Send + 'static, S> PollableAsyncStep for VersionTrackerStep<T, S>
where
    Self: Sized + Send + Sync + 'static,
    T: Send + Sync + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
{
    fn poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.polling_interval_secs)
    }

    async fn poll(&mut self) -> Result<Option<Vec<TransactionContext<T>>>, ProcessorError> {
        // TODO: Add metrics for gap count
        self.save_processor_status().await?;
        // Nothing should be returned
        Ok(None)
    }
}

impl<T, S> NamedStep for VersionTrackerStep<T, S>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
    S: ProcessorStatusSaver + Send + 'static,
{
    fn name(&self) -> String {
        format!("VersionTrackerStep: {}", std::any::type_name::<T>())
    }
}

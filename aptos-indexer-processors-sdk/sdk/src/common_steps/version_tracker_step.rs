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

#[async_trait]
pub trait ProcessorStatusSaver {
    // T represents the transaction type that the processor is tracking.
    async fn save_processor_status<T>(
        &self,
        tracker_name: &str,
        last_success_batch: &TransactionContext<T>,
    ) -> Result<(), ProcessorError>;
}

pub struct VersionTrackerStep<T, S>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
    S: ProcessorStatusSaver + Send + 'static,
{
    tracker_name: String,
    // Last successful batch of sequentially processed transactions. Includes metadata to write to storage.
    last_success_batch: Option<TransactionContext<()>>,
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
    pub fn new(
        tracker_name: String,
        processor_status_saver: S,
        polling_interval_secs: u64,
    ) -> Self {
        Self {
            tracker_name,
            last_success_batch: None,
            processor_status_saver,
            polling_interval_secs,
            _marker: PhantomData,
        }
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

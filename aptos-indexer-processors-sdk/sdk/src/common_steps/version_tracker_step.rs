use crate::{
    config::{
        indexer_processor_config::{DbConfig, IndexerProcessorConfig, ProcessorMode},
        processor_status_saver::{
            BackfillProgress, BackfillStatus, ProcessorStatus, ProcessorStatusSaver,
        },
    },
    traits::{
        pollable_async_step::PollableAsyncRunType, NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use anyhow::Result;
use aptos_indexer_transaction_stream::utils::parse_timestamp;
use async_trait::async_trait;
use std::marker::PhantomData;

pub const DEFAULT_UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

/// Tracks the versioned processing of sequential transactions, ensuring no gaps
/// occur between them.
///
/// Important: this step assumes ordered transactions. Please use the `OrederByVersionStep` before this step
/// if the transactions are not ordered.
pub struct VersionTrackerStep<T, S, D>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
    D: DbConfig + Send + Sync + Clone + 'static,
{
    // Last successful batch of sequentially processed transactions. Includes metadata to write to storage.
    last_success_batch: Option<TransactionContext<()>>,
    polling_interval_secs: u64,
    processor_status_saver: S,
    processor_id: String,
    processor_config: IndexerProcessorConfig<D>,
    _marker: PhantomData<T>,
}

impl<T, S, D> VersionTrackerStep<T, S, D>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
    D: DbConfig + Send + Sync + Clone + 'static,
{
    pub fn new(
        processor_status_saver: S,
        polling_interval_secs: u64,
        processor_id: &str,
        processor_config: IndexerProcessorConfig<D>,
    ) -> Self {
        Self {
            last_success_batch: None,
            processor_status_saver,
            polling_interval_secs,
            processor_id: processor_id.to_string(),
            processor_config,
            _marker: PhantomData,
        }
    }

    async fn save_processor_status(&mut self) -> Result<(), ProcessorError> {
        if let Some(last_success_batch) = self.last_success_batch.as_ref() {
            match self.processor_config.mode {
                ProcessorMode::Default => {
                    self.processor_status_saver
                        .save_processor_status(ProcessorStatus {
                            processor_id: self.processor_id.clone(),
                            last_success_version: last_success_batch.metadata.end_version as i64,
                            last_transaction_timestamp: last_success_batch
                                .metadata
                                .end_transaction_timestamp
                                .as_ref()
                                .map(|t| {
                                    parse_timestamp(
                                        t,
                                        last_success_batch.metadata.end_version as i64,
                                    )
                                    .naive_utc()
                                }),
                        })
                        .await
                },
                ProcessorMode::Backfill => {
                    let backfill_config = self.processor_config.backfill_config.as_ref().unwrap();
                    let lst_success_version = last_success_batch.metadata.end_version as i64;
                    let backfill_status =
                        if lst_success_version >= backfill_config.ending_version as i64 {
                            BackfillStatus::Complete
                        } else {
                            BackfillStatus::InProgress
                        };
                    let backfill_progress = BackfillProgress {
                        backfill_id: backfill_config.backfill_id.clone(),
                        backfill_status,
                        backfill_start_version: backfill_config.initial_starting_version,
                        backfill_end_version: backfill_config.ending_version,
                        last_success_version: lst_success_version,
                        last_transaction_timestamp: last_success_batch
                            .metadata
                            .end_transaction_timestamp
                            .as_ref()
                            .map(|t| parse_timestamp(t, lst_success_version).naive_utc()),
                    };
                    self.processor_status_saver
                        .save_backfill_processor_status(backfill_progress)
                        .await
                },
                ProcessorMode::Testing => Ok(()),
            }
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl<T, S, D> Processable for VersionTrackerStep<T, S, D>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
    D: DbConfig + Send + Sync + Clone + 'static,
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
impl<T: Send + 'static, S, D> PollableAsyncStep for VersionTrackerStep<T, S, D>
where
    Self: Sized + Send + Sync + 'static,
    T: Send + Sync + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
    D: DbConfig + Send + Sync + Clone + 'static,
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

impl<T, S, D> NamedStep for VersionTrackerStep<T, S, D>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
    S: ProcessorStatusSaver + Send + Sync + 'static,
    D: DbConfig + Send + Sync + Clone + 'static,
{
    fn name(&self) -> String {
        format!("VersionTrackerStep: {}", std::any::type_name::<T>())
    }
}

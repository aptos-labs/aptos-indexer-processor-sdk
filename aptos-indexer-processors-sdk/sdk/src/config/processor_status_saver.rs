use crate::utils::errors::ProcessorError;
use anyhow::Result;
use async_trait::async_trait;

/// The `ProcessorStatusSaver` trait object should be implemented in order to save the latest successfully
/// processed transaction version to storage. I.e., persisting the `processor_status` to storage.
// This is important to ensure that the processor can resume from the last successfully processed transaction version.
// This trait is also extended to include methods for saving the chain id and backfill progress.
#[async_trait]
pub trait ProcessorStatusSaver {
    // T represents the transaction type that the processor is tracking.
    async fn save_processor_status(
        &self,
        processor_status: ProcessorStatus,
    ) -> Result<(), ProcessorError>;

    async fn get_processor_status(
        &self,
        processor_id: &str,
    ) -> Result<Option<ProcessorStatus>, ProcessorError>;

    /// Save the chain ID to storage. This is used to track the chain ID that's being processed
    /// and prevents the processor from processing the wrong chain.
    async fn save_chain_id(&self, chain_id: u64) -> Result<(), ProcessorError>;

    /// Get the chain ID from storage. This is used to track the chain ID that's being processed
    /// and prevents the processor from processing the wrong chain.
    async fn get_chain_id(&self) -> Result<Option<u64>, ProcessorError>;

    /// Save the backfill processor status to storage. This is used to track the status of a data backfill
    /// By default, this method is unimplemented.
    async fn save_backfill_processor_status(
        &self,
        _backfill_progress: BackfillProgress,
    ) -> Result<(), ProcessorError> {
        unimplemented!()
    }

    /// Get the backfill processor status from storage. This is used to track the status of a data backfill
    /// By default, this method is unimplemented.
    async fn get_backfill_processor_status(
        &self,
        _backfill_id: &str,
    ) -> Result<Option<BackfillProgress>, ProcessorError> {
        unimplemented!()
    }
}

pub struct ProcessorStatus {
    pub processor_id: String,
    pub last_success_version: i64,
    pub last_transaction_timestamp: Option<chrono::NaiveDateTime>,
}

#[derive(PartialEq, Eq, Debug)]
pub enum BackfillStatus {
    InProgress,
    Complete,
}

pub struct BackfillProgress {
    pub backfill_id: String,
    pub backfill_status: BackfillStatus,
    pub backfill_start_version: u64,
    pub backfill_end_version: u64,
    pub last_success_version: i64,
    pub last_transaction_timestamp: Option<chrono::NaiveDateTime>,
}

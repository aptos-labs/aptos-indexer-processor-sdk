use anyhow::{Context, Result};
use aptos_indexer_transaction_stream::TransactionStreamConfig;
use async_trait::async_trait;

#[async_trait]
pub trait LastSuccessVersionRetriever {
    async fn get_last_success_version(&self, processor_id: &str) -> Result<Option<u64>>;
}

pub async fn get_starting_version<T>(
    transaction_stream_config: &TransactionStreamConfig,
    last_success_version_retriever: &T,
    processor_id: &str,
) -> Result<u64>
where
    T: LastSuccessVersionRetriever,
{
    // Check if there's a checkpoint in the appropriate processor status table.
    let latest_processed_version = last_success_version_retriever
        .get_last_success_version(processor_id)
        .await
        .context("Failed to get latest processed version from DB")?;

    // If nothing checkpointed, return the `starting_version` from the config, or 0 if not set.
    Ok(latest_processed_version.unwrap_or(transaction_stream_config.starting_version.unwrap_or(0)))
}

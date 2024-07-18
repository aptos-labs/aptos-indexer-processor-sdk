use super::database::new_db_pool;
use crate::{
    config::indexer_processor_config::IndexerProcessorConfig,
    db::models::processor_status::ProcessorStatusQuery,
};
use anyhow::{Context, Result};

pub async fn get_starting_version(
    indexer_processor_config: &IndexerProcessorConfig,
) -> Result<u64> {
    // If starting_version is set in TransactionStreamConfig, use that
    if indexer_processor_config
        .transaction_stream_config
        .starting_version
        .is_some()
    {
        return Ok(indexer_processor_config
            .transaction_stream_config
            .starting_version
            .unwrap());
    }

    // If it's not set, check if the DB has latest_processed_version set and use that
    let latest_processed_version_from_db =
        get_latest_processed_version_from_db(indexer_processor_config)
            .await
            .context("Failed to get latest processed version from DB")?;
    if let Some(latest_processed_version_tracker) = latest_processed_version_from_db {
        return Ok(latest_processed_version_tracker);
    }

    // If latest_processed_version is not stored in DB, return the default 0
    Ok(0)
}

/// Gets the start version for the processor. If not found, start from 0.
pub async fn get_latest_processed_version_from_db(
    indexer_processor_config: &IndexerProcessorConfig,
) -> Result<Option<u64>> {
    // let db_config = indexer_processor_config.db_config;
    let conn_pool = new_db_pool(
        &indexer_processor_config
            .db_config
            .postgres_connection_string,
        Some(indexer_processor_config.db_config.db_pool_size),
    )
    .await
    .context("Failed to create connection pool")?;
    let mut conn = conn_pool.get().await?;

    match ProcessorStatusQuery::get_by_processor(
        indexer_processor_config.processor_config.name(),
        &mut conn,
    )
    .await?
    {
        Some(status) => Ok(Some(status.last_success_version as u64 + 1)),
        None => Ok(None),
    }
}

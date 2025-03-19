use super::database::{execute_with_better_error, execute_with_better_error_conn, ArcDbPool};
use crate::{
    aptos_indexer_transaction_stream::{utils::time::parse_timestamp, TransactionStreamConfig},
    common_steps::ProcessorStatusSaver,
    postgres::{
        models::{
            ledger_info::LedgerInfo,
            processor_status::{ProcessorStatus, ProcessorStatusQuery},
        },
        processor_metadata_schema::processor_metadata::{ledger_infos, processor_status},
    },
    types::transaction_context::TransactionContext,
    utils::{chain_id_check::ChainIdChecker, errors::ProcessorError},
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use diesel::{query_dsl::methods::FilterDsl, upsert::excluded, ExpressionMethods};

/// A trait implementation of ChainIdChecker for Postgres.
pub struct PostgresChainIdChecker {
    pub db_pool: ArcDbPool,
}

impl PostgresChainIdChecker {
    pub fn new(db_pool: ArcDbPool) -> Self {
        Self { db_pool }
    }
}

#[async_trait]
impl ChainIdChecker for PostgresChainIdChecker {
    async fn save_chain_id(&self, chain_id: u64) -> Result<()> {
        let mut conn = self
            .db_pool
            .get()
            .await
            .context("Error getting db connection")?;
        execute_with_better_error_conn(
            &mut conn,
            diesel::insert_into(ledger_infos::table)
                .values(LedgerInfo {
                    chain_id: chain_id as i64,
                })
                .on_conflict_do_nothing(),
        )
        .await
        .context("Error updating chain_id!")?;
        Ok(())
    }

    async fn get_chain_id(&self) -> Result<Option<u64>> {
        let mut conn = self.db_pool.get().await?;
        let maybe_existing_chain_id = LedgerInfo::get(&mut conn)
            .await?
            .map(|li| li.chain_id as u64);
        Ok(maybe_existing_chain_id)
    }
}

/// A trait implementation of ProcessorStatusSaver for Postgres.
pub struct PostgresProcessorStatusSaver {
    pub db_pool: ArcDbPool,
    pub processor_name: String,
}

impl PostgresProcessorStatusSaver {
    pub fn new(processor_name: &str, db_pool: ArcDbPool) -> Self {
        Self {
            db_pool,
            processor_name: processor_name.to_string(),
        }
    }
}

#[async_trait]
impl ProcessorStatusSaver for PostgresProcessorStatusSaver {
    async fn save_processor_status(
        &self,
        last_success_batch: &TransactionContext<()>,
    ) -> Result<(), ProcessorError> {
        let last_success_version = last_success_batch.metadata.end_version as i64;
        let last_transaction_timestamp = last_success_batch
            .metadata
            .end_transaction_timestamp
            .as_ref()
            .map(|t| parse_timestamp(t, last_success_batch.metadata.end_version as i64))
            .map(|t| t.naive_utc());
        let status = ProcessorStatus {
            processor: self.processor_name.clone(),
            last_success_version,
            last_transaction_timestamp,
        };

        // Save regular processor status to the database
        execute_with_better_error(
            self.db_pool.clone(),
            diesel::insert_into(processor_status::table)
                .values(&status)
                .on_conflict(processor_status::processor)
                .do_update()
                .set((
                    processor_status::last_success_version
                        .eq(excluded(processor_status::last_success_version)),
                    processor_status::last_updated.eq(excluded(processor_status::last_updated)),
                    processor_status::last_transaction_timestamp
                        .eq(excluded(processor_status::last_transaction_timestamp)),
                ))
                .filter(
                    processor_status::last_success_version
                        .le(excluded(processor_status::last_success_version)),
                ),
        )
        .await?;
        Ok(())
    }
}

pub async fn get_starting_version(
    processor_name: &str,
    transaction_stream_config: TransactionStreamConfig,
    conn_pool: ArcDbPool,
) -> Result<u64> {
    let mut conn = conn_pool.get().await?;
    let latest_processed_version =
        ProcessorStatusQuery::get_by_processor(processor_name, &mut conn)
            .await?
            .map(|ps| ps.last_success_version as u64);
    // If nothing checkpointed, return the `starting_version` from the config, or 0 if not set.
    Ok(latest_processed_version.unwrap_or(transaction_stream_config.starting_version.unwrap_or(0)))
}

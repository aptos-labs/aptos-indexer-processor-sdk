use super::database::{execute_with_better_error, execute_with_better_error_conn, ArcDbPool};
use crate::{
    models::{
        backfill_processor_status::{
            BackfillProcessorStatus, BackfillProcessorStatusQuery, BackfillStatus,
        },
        ledger_info::LedgerInfo,
        processor_status::{ProcessorStatus, ProcessorStatusQuery},
    },
    schema::{backfill_processor_status, ledger_infos, processor_status},
};
use anyhow::{Context, Result};
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::utils::time::parse_timestamp,
    common_steps::ProcessorStatusSaver,
    config::processor_mode::{BackfillConfig, BootStrapConfig, ProcessorMode, TestingConfig},
    types::transaction_context::TransactionContext,
    utils::{
        chain_id_check::ChainIdChecker, errors::ProcessorError,
        starting_version::LastSuccessVersionRetriever,
    },
};
use async_trait::async_trait;
use diesel::{upsert::excluded, ExpressionMethods};

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
            None,
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
    pub processor_mode: ProcessorMode,
    pub processor_name: String,
}

impl PostgresProcessorStatusSaver {
    pub fn new(processor_name: &str, processor_mode: ProcessorMode, db_pool: ArcDbPool) -> Self {
        Self {
            db_pool,
            processor_mode,
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

        match &self.processor_mode {
            ProcessorMode::Default(BootStrapConfig {
                initial_starting_version: _,
            }) => {
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
                        )),
                    Some(" WHERE processor_status.last_success_version <= EXCLUDED.last_success_version "),
                )
                    .await?;
            },
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id,
                initial_starting_version,
                ending_version,
                overwrite_checkpoint,
            }) => {
                let backfill_alias = format!("{}_{}", self.processor_name, backfill_id);
                let backfill_status = if last_success_version >= *ending_version as i64 {
                    BackfillStatus::Complete
                } else {
                    BackfillStatus::InProgress
                };
                let status = BackfillProcessorStatus {
                    backfill_alias,
                    backfill_status,
                    last_success_version,
                    last_transaction_timestamp,
                    backfill_start_version: *initial_starting_version as i64,
                    backfill_end_version: *ending_version as i64,
                };

                // If overwrite_checkpoint is true, then always update the backfill status.
                let where_clause = match overwrite_checkpoint {
                    true => None,
                    false => Some(" WHERE backfill_processor_status.last_success_version <= EXCLUDED.last_success_version "),
                };

                execute_with_better_error(
                    self.db_pool.clone(),
                    diesel::insert_into(backfill_processor_status::table)
                        .values(&status)
                        .on_conflict(backfill_processor_status::backfill_alias)
                        .do_update()
                        .set((
                            backfill_processor_status::backfill_status
                                .eq(excluded(backfill_processor_status::backfill_status)),
                            backfill_processor_status::last_success_version
                                .eq(excluded(backfill_processor_status::last_success_version)),
                            backfill_processor_status::last_updated
                                .eq(excluded(backfill_processor_status::last_updated)),
                            backfill_processor_status::last_transaction_timestamp.eq(excluded(
                                backfill_processor_status::last_transaction_timestamp,
                            )),
                            backfill_processor_status::backfill_start_version
                                .eq(excluded(backfill_processor_status::backfill_start_version)),
                            backfill_processor_status::backfill_end_version
                                .eq(excluded(backfill_processor_status::backfill_end_version)),
                        )),
                    where_clause,
                )
                .await?;
            },
            ProcessorMode::Testing(_) => {
                // In testing mode, the last success version is not stored.
            },
        }
        Ok(())
    }
}

/// A trait implementation of StartingVersionRetriever for Postgres.
pub struct PostgresStartingVersionRetriever {
    pub db_pool: ArcDbPool,
    pub processor_mode: ProcessorMode,
    pub processor_name: String,
}

impl PostgresStartingVersionRetriever {
    pub fn new(processor_name: &str, processor_mode: ProcessorMode, db_pool: ArcDbPool) -> Self {
        Self {
            db_pool,
            processor_mode,
            processor_name: processor_name.to_string(),
        }
    }
}

#[async_trait]
impl LastSuccessVersionRetriever for PostgresStartingVersionRetriever {
    async fn get_last_success_version(&self, _processor_id: &str) -> Result<Option<u64>> {
        let mut conn = self.db_pool.get().await?;

        match &self.processor_mode {
            ProcessorMode::Default(BootStrapConfig {
                initial_starting_version,
            }) => {
                let status =
                    ProcessorStatusQuery::get_by_processor(&self.processor_name, &mut conn)
                        .await
                        .context("Failed to query processor_status table.")?;

                // If there's no last success version saved, start with the version from config
                Ok(Some(status.map_or(*initial_starting_version, |status| {
                    std::cmp::max(
                        status.last_success_version as u64,
                        *initial_starting_version,
                    )
                })))
            },
            ProcessorMode::Backfill(BackfillConfig {
                backfill_id,
                initial_starting_version,
                ending_version,
                overwrite_checkpoint,
            }) => {
                let backfill_status_option = BackfillProcessorStatusQuery::get_by_processor(
                    &self.processor_name,
                    backfill_id,
                    &mut conn,
                )
                .await
                .context("Failed to query backfill_processor_status table.")?;

                // Return None if there is no checkpoint, if the backfill is old (complete), or if overwrite_checkpoint is true.
                // Otherwise, return the checkpointed version + 1.
                if let Some(status) = backfill_status_option {
                    // If the backfill is complete and overwrite_checkpoint is false, return the ending_version to end the backfill.
                    if status.backfill_status == BackfillStatus::Complete && !overwrite_checkpoint {
                        return Ok(Some(*ending_version));
                    }
                    // If status is Complete or overwrite_checkpoint is true, this is the start of a new backfill job.
                    if *overwrite_checkpoint {
                        let backfill_alias = status.backfill_alias.clone();
                        let status = BackfillProcessorStatus {
                            backfill_alias,
                            backfill_status: BackfillStatus::InProgress,
                            last_success_version: 0,
                            last_transaction_timestamp: None,
                            backfill_start_version: *initial_starting_version as i64,
                            backfill_end_version: *ending_version as i64,
                        };
                        execute_with_better_error(
                            self.db_pool.clone(),
                            diesel::insert_into(backfill_processor_status::table)
                                .values(&status)
                                .on_conflict(backfill_processor_status::backfill_alias)
                                .do_update()
                                .set((
                                    backfill_processor_status::backfill_status
                                        .eq(excluded(backfill_processor_status::backfill_status)),
                                    backfill_processor_status::last_success_version.eq(excluded(
                                        backfill_processor_status::last_success_version,
                                    )),
                                    backfill_processor_status::last_updated
                                        .eq(excluded(backfill_processor_status::last_updated)),
                                    backfill_processor_status::last_transaction_timestamp.eq(
                                        excluded(
                                            backfill_processor_status::last_transaction_timestamp,
                                        ),
                                    ),
                                    backfill_processor_status::backfill_start_version.eq(excluded(
                                        backfill_processor_status::backfill_start_version,
                                    )),
                                    backfill_processor_status::backfill_end_version.eq(excluded(
                                        backfill_processor_status::backfill_end_version,
                                    )),
                                )),
                            None,
                        )
                        .await?;
                        return Ok(Some(*initial_starting_version));
                    }

                    // `backfill_config.initial_starting_version` is NOT respected.
                    // Return the last success version + 1.
                    let starting_version = status.last_success_version as u64 + 1;
                    log_ascii_warning(starting_version);
                    Ok(Some(starting_version))
                } else {
                    Ok(Some(*initial_starting_version))
                }
            },
            ProcessorMode::Testing(TestingConfig {
                override_starting_version,
                ending_version: _,
            }) => {
                // Always start from the override_starting_version.
                Ok(Some(*override_starting_version))
            },
        }
    }
}

fn log_ascii_warning(version: u64) {
    println!(
        r#"
 ██╗    ██╗ █████╗ ██████╗ ███╗   ██╗██╗███╗   ██╗ ██████╗ ██╗
 ██║    ██║██╔══██╗██╔══██╗████╗  ██║██║████╗  ██║██╔════╝ ██║
 ██║ █╗ ██║███████║██████╔╝██╔██╗ ██║██║██╔██╗ ██║██║  ███╗██║
 ██║███╗██║██╔══██║██╔══██╗██║╚██╗██║██║██║╚██╗██║██║   ██║╚═╝
 ╚███╔███╔╝██║  ██║██║  ██║██║ ╚████║██║██║ ╚████║╚██████╔╝██╗
  ╚══╝╚══╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝
                                                               
=================================================================
   This backfill job is resuming progress at version {}
=================================================================
"#,
        version
    );
}

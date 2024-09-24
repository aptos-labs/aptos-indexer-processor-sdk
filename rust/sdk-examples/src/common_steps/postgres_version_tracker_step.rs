use crate::{
    config::indexer_processor_config::DbConfig,
    db::common::models::processor_status::ProcessorStatus,
    schema::processor_status,
    utils::database::{execute_with_better_error, new_db_pool, ArcDbPool},
};
use ahash::AHashMap;
use anyhow::{Context, Result};
use aptos_indexer_processor_sdk::{
    traits::{NamedStep, PollableAsyncRunType, PollableAsyncStep, Processable, VersionTrackerStep},
    types::transaction_context::TransactionContext,
    utils::{errors::ProcessorError, time::parse_timestamp},
};
use async_trait::async_trait;
use diesel::{upsert::excluded, ExpressionMethods};

const UPDATE_PROCESSOR_STATUS_SECS: u64 = 1;

pub struct PostgresLatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
{
    conn_pool: ArcDbPool,
    tracker_name: String,
    // Next version to process that we expect.
    next_version: u64,
    // Last successful batch of sequentially processed transactions. Includes metadata to write to storage.
    last_success_batch: Option<TransactionContext<T>>,
    // Tracks all the versions that have been processed out of order.
    seen_versions: AHashMap<u64, TransactionContext<T>>,
}

impl<T> PostgresLatestVersionProcessedTracker<T>
where
    T: Send + 'static,
{
    pub async fn new(
        db_config: DbConfig,
        starting_version: u64,
        tracker_name: String,
    ) -> Result<Self> {
        let conn_pool = new_db_pool(
            &db_config.postgres_connection_string,
            Some(db_config.db_pool_size),
        )
        .await
        .context("Failed to create connection pool")?;
        Ok(Self {
            conn_pool,
            tracker_name,
            next_version: starting_version,
            last_success_batch: None,
            seen_versions: AHashMap::new(),
        })
    }
}

#[async_trait]
impl<T> VersionTrackerStep<T> for PostgresLatestVersionProcessedTracker<T>
where
    T: Send + 'static,
{
    fn get_next_version(&self) -> u64 {
        self.next_version
    }

    fn get_seen_versions(&mut self) -> &mut AHashMap<u64, TransactionContext<T>> {
        &mut self.seen_versions
    }

    fn get_last_success_batch(&self) -> &Option<TransactionContext<T>> {
        &self.last_success_batch
    }

    fn set_next_version(&mut self, next_version: u64) {
        self.next_version = next_version;
    }

    fn set_last_success_batch(&mut self, last_success_batch: Option<TransactionContext<T>>) {
        self.last_success_batch = last_success_batch;
    }

    async fn save_processor_status(&mut self) -> Result<(), ProcessorError> {
        if let Some(last_success_batch) = self.last_success_batch.as_ref() {
            let end_timestamp = last_success_batch
                .end_transaction_timestamp
                .as_ref()
                .map(|t| parse_timestamp(t, last_success_batch.end_version as i64))
                .map(|t| t.naive_utc());
            let status = ProcessorStatus {
                processor: self.tracker_name.clone(),
                last_success_version: last_success_batch.end_version as i64,
                last_transaction_timestamp: end_timestamp,
            };
            execute_with_better_error(
                self.conn_pool.clone(),
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
            ).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl<T> Processable for PostgresLatestVersionProcessedTracker<T>
where
    T: Send + 'static,
{
    type Input = T;
    type Output = T;
    type RunType = PollableAsyncRunType;

    // Use the default implementation from LatestVersionProcessedTracker
    async fn process(
        &mut self,
        current_batch: TransactionContext<Self::Input>,
    ) -> Result<Option<TransactionContext<Self::Output>>, ProcessorError> {
        VersionTrackerStep::<T>::process(self, current_batch).await
    }

    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        VersionTrackerStep::<T>::cleanup(self).await
    }
}

impl<T> NamedStep for PostgresLatestVersionProcessedTracker<T>
where
    Self: Sized + Send + 'static,
    T: Send + 'static,
{
    fn name(&self) -> String {
        format!(
            "PostgresLatestVersionProcessedTracker: {}",
            std::any::type_name::<T>()
        )
    }
}

#[async_trait]
impl<T: Send + 'static> PollableAsyncStep for PostgresLatestVersionProcessedTracker<T>
where
    Self: Sized + Send + Sync + 'static,
    T: Send + 'static,
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

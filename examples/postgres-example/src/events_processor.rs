// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::events_step::EventsStep;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    builder::ProcessorBuilder,
    common_steps::{
        TransactionStreamStep, VersionTrackerStep, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
    },
    postgres::{
        subconfigs::postgres_config::PostgresConfig,
        utils::{
            checkpoint::{
                get_starting_version, PostgresChainIdChecker, PostgresProcessorStatusSaver,
            },
            database::{new_db_pool, run_migrations, ArcDbPool},
        },
    },
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
    utils::chain_id_check::check_or_update_chain_id,
};
use aptos_indexer_processor_sdk_server_framework::RunnableConfig;
use async_trait::async_trait;
use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use serde::{Deserialize, Serialize};
use tracing::info;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("src/db/migrations");
pub const PROCESSOR_NAME: &str = "events_processor";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventsProcessorConfig {
    pub transaction_stream_config: TransactionStreamConfig,
    pub postgres_config: PostgresConfig,
}

#[async_trait::async_trait]
impl RunnableConfig for EventsProcessorConfig {
    async fn run(&self) -> Result<()> {
        let processor = EventsProcessor::new(self.clone()).await?;
        processor.run_processor().await?;
        Ok(())
    }

    fn get_server_name(&self) -> String {
        PROCESSOR_NAME.to_string()
    }
}

pub struct EventsProcessor {
    pub config: EventsProcessorConfig,
    pub db_pool: ArcDbPool,
}

impl EventsProcessor {
    pub async fn new(config: EventsProcessorConfig) -> Result<Self> {
        let conn_pool = new_db_pool(
            &config.postgres_config.connection_string,
            Some(config.postgres_config.db_pool_size),
        )
        .await
        .expect("Failed to create connection pool");

        Ok(Self {
            config,
            db_pool: conn_pool,
        })
    }
}

#[async_trait]
impl ProcessorTrait for EventsProcessor {
    fn name(&self) -> &'static str {
        PROCESSOR_NAME
    }

    async fn run_processor(&self) -> Result<()> {
        // Run migrations
        run_migrations(
            self.config.postgres_config.connection_string.clone(),
            self.db_pool.clone(),
            MIGRATIONS,
        )
        .await;

        check_or_update_chain_id(
            &self.config.transaction_stream_config,
            &PostgresChainIdChecker::new(self.db_pool.clone()),
        )
        .await?;

        // Merge the starting version from config and the latest processed version from the DB
        let starting_version = get_starting_version(
            self.name(),
            self.config.transaction_stream_config.clone(),
            self.db_pool.clone(),
        )
        .await?;

        // Define processor steps
        let transaction_stream_config = self.config.transaction_stream_config.clone();
        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..transaction_stream_config
        })
        .await?;
        let events_step = EventsStep {
            conn_pool: self.db_pool.clone(),
        };
        let processor_status_saver =
            PostgresProcessorStatusSaver::new(self.name(), self.db_pool.clone());
        let version_tracker =
            VersionTrackerStep::new(processor_status_saver, DEFAULT_UPDATE_PROCESSOR_STATUS_SECS);

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(events_step.into_runnable_step(), 10)
        .connect_to(version_tracker.into_runnable_step(), 10)
        .end_and_return_output_receiver(10);

        // (Optional) Parse the results
        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    if txn_context.data.is_empty() {
                        continue;
                    }
                    info!(
                        "Finished processing events from versions [{:?}, {:?}]",
                        txn_context.metadata.start_version, txn_context.metadata.end_version,
                    );
                },
                Err(_) => {
                    info!("Channel is closed");
                    return Ok(());
                },
            }
        }
    }
}

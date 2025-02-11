// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    aptos_indexer_transaction_stream::TransactionStreamConfig, common_steps::ProcessorStatusSaver,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::any::Any;

pub const QUERY_DEFAULT_RETRIES: u32 = 5;
pub const QUERY_DEFAULT_RETRY_DELAY_MS: u64 = 500;

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub enum ProcessorMode {
    #[serde(rename = "default")]
    #[default]
    Default,
    #[serde(rename = "backfill")]
    Backfill,
    #[serde(rename = "testing")]
    Testing,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerProcessorConfig<D> {
    pub processor_name: String,
    pub transaction_stream_config: TransactionStreamConfig,
    pub db_config: D,
    #[serde(default)]
    pub mode: ProcessorMode,
    pub bootstrap_config: Option<BootStrapConfig>,
    pub testing_config: Option<TestingConfig>,
    pub backfill_config: Option<BackfillConfig>,
}

impl<D> IndexerProcessorConfig<D>
where
    Self: Send + Sync + 'static,
    D: DbConfig + Send + Sync + Clone + 'static,
{
    fn validate(&self) -> Result<(), String> {
        match self.mode {
            ProcessorMode::Testing => {
                if self.testing_config.is_none() {
                    return Err("testing_config must be present when mode is 'testing'".to_string());
                }
            },
            ProcessorMode::Backfill => {
                if self.backfill_config.is_none() {
                    return Err(
                        "backfill_config must be present when mode is 'backfill'".to_string()
                    );
                }
            },
            ProcessorMode::Default => {},
        }
        Ok(())
    }

    pub async fn get_starting_version(&self) -> Result<u64> {
        // Check if there's a checkpoint in the approrpiate processor status table.
        let db_config = self.db_config.clone();
        let _latest_processed_version_from_db = db_config
            .into_runnable_config()
            .await
            .context("Failed to initialize DB config")?
            .get_latest_processed_version(&self.processor_name)
            .await
            .context("Failed to get latest processed version from DB")?;

        // If nothing checkpointed, return the `starting_version` from the config, or 0 if not set.
        // Ok(latest_processed_version_from_db
        //     .unwrap_or(self.transaction_stream_config.starting_version.unwrap_or(0)))
        Ok(0)
    }
}

// Note: You'll need to create a concrete implementation of this trait
// for your specific database configuration
#[async_trait]
pub trait DbConfig
where
    Self: Any + Clone + Serialize + DeserializeOwned + Sync + Send + 'static,
{
    type Runnable: RunnableDbConfig;

    async fn into_runnable_config(self) -> Result<Self::Runnable>;
}

#[async_trait]
pub trait RunnableDbConfig
where
    Self: ProcessorStatusSaver + Send + Sync + 'static,
{
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillConfig {
    pub backfill_id: String,
    pub initial_starting_version: u64,
    pub ending_version: u64,
    pub overwrite_checkpoint: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
/// Initial starting version for non-backfill processors. Processors will pick up where it left off
/// if restarted. Read more in `starting_version.rs`
pub struct BootStrapConfig {
    pub initial_starting_version: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
/// Use this config for testing. Processors will not use checkpoint and will
/// always start from `override_starting_version`.
pub struct TestingConfig {
    pub override_starting_version: u64,
    pub ending_version: u64,
}

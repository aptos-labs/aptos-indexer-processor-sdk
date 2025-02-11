// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::processor_status_saver::BackfillStatus;
use crate::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    config::processor_status_saver::ProcessorStatusSaver,
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

#[derive(Clone, Deserialize, Debug, Serialize)]
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
    D: DbConfig + DeserializeOwned + Send + Sync + Clone + 'static,
{
    // pub fn validate(&self) -> Result<(), String> {
    //     match self.mode {
    //         ProcessorMode::Testing => {
    //             if self.testing_config.is_none() {
    //                 return Err("testing_config must be present when mode is 'testing'".to_string());
    //             }
    //         },
    //         ProcessorMode::Backfill => {
    //             if self.backfill_config.is_none() {
    //                 return Err(
    //                     "backfill_config must be present when mode is 'backfill'".to_string()
    //                 );
    //             }
    //         },
    //         ProcessorMode::Default => {},
    //     }
    //     Ok(())
    // }

    pub async fn get_starting_version(&self) -> Result<u64> {
        match self.mode {
            ProcessorMode::Testing => {
                let testing_config = self
                    .testing_config
                    .clone()
                    .context("testing_config must be present when mode is 'testing'")?;
                Ok(testing_config.override_starting_version)
            },
            ProcessorMode::Default => {
                // Check if there's a checkpoint in the approrpiate processor status table.
                let processor_status = self
                    .db_config
                    .clone()
                    .into_runnable_config()
                    .await
                    .context("Failed to initialize DB config")?
                    .get_processor_status(&self.processor_name)
                    .await
                    .context("Failed to get latest processed version from DB")?;

                let default_starting_version = self
                    .bootstrap_config
                    .clone()
                    .map_or(0, |config| config.initial_starting_version);

                Ok(processor_status.map_or(default_starting_version, |status| {
                    std::cmp::max(status.last_success_version as u64, default_starting_version)
                }))
            },
            ProcessorMode::Backfill => {
                let backfill_config = self
                    .backfill_config
                    .clone()
                    .context("backfill_config must be present when mode is 'backfill'")?;
                let backfill_status_option = self
                    .db_config
                    .clone()
                    .into_runnable_config()
                    .await
                    .context("Failed to initialize DB config")?
                    .get_backfill_processor_status(&backfill_config.backfill_id)
                    .await
                    .context("Failed to query backfill_processor_status table.")?;

                // Return None if there is no checkpoint, if the backfill is old (complete), or if overwrite_checkpoint is true.
                // Otherwise, return the checkpointed version + 1.
                if let Some(status) = backfill_status_option {
                    // If the backfill is complete and overwrite_checkpoint is false, return the ending_version to end the backfill.
                    if status.backfill_status == BackfillStatus::Complete
                        && !backfill_config.overwrite_checkpoint
                    {
                        return Ok(backfill_config.ending_version);
                    }
                    // If status is Complete or overwrite_checkpoint is true, this is the start of a new backfill job.
                    if backfill_config.overwrite_checkpoint {
                        return Ok(backfill_config.initial_starting_version);
                    }

                    // `backfill_config.initial_starting_version` is NOT respected.
                    // Return the last success version + 1.
                    let starting_version = status.last_success_version as u64 + 1;
                    log_ascii_warning(starting_version);
                    Ok(starting_version)
                } else {
                    Ok(backfill_config.initial_starting_version)
                }
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

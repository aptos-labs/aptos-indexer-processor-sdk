// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    aptos_indexer_transaction_stream::TransactionStreamConfig, common_steps::ProcessorStatusSaver,
};
use anyhow::Result;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::any::Any;

pub const QUERY_DEFAULT_RETRIES: u32 = 5;
pub const QUERY_DEFAULT_RETRY_DELAY_MS: u64 = 500;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerProcessorConfig<D> {
    pub processor_name: String,
    pub transaction_stream_config: TransactionStreamConfig,
    pub db_config: D,
    pub backfill_config: Option<BackfillConfig>,
}

// #[async_trait::async_trait]
// impl RunnableConfig for IndexerProcessorConfig {
//     async fn run(&self) -> Result<()> {
//         match self.processor_config {
//             ProcessorConfig::EventsProcessor => {
//                 let events_processor = EventsProcessor::new(self.clone()).await?;
//                 events_processor.run_processor().await
//             },
//         }
//     }

//     fn get_server_name(&self) -> String {
//         // Get the part before the first _ and trim to 12 characters.
//         let before_underscore = self
//             .processor_config
//             .name()
//             .split('_')
//             .next()
//             .unwrap_or("unknown");
//         before_underscore[..before_underscore.len().min(12)].to_string()
//     }
// }

// Note: You'll need to create a concrete implementation of this trait
// for your specific database configuration
pub trait DbConfig:
    Any + Clone + Send + DeserializeOwned + ProcessorStatusSaver + Serialize
{
}

// #[derive(Clone, Debug, Deserialize, Serialize)]
// #[serde(deny_unknown_fields)]
// pub struct PostgresConfig {
//     pub postgres_connection_string: String,
//     // Size of the pool for writes/reads to the DB. Limits maximum number of queries in flight
//     #[serde(default = "PostgresConfig::default_db_pool_size")]
//     pub db_pool_size: u32,
// }

// impl DbConfig for PostgresConfig {}

// impl PostgresConfig {
//     pub const fn default_db_pool_size() -> u32 {
//         150
//     }
// }

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackfillConfig {
    pub backfill_alias: String,
}

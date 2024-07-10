// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::events_processor::events_processor::EventsProcessor;
use ahash::AHashMap;
use anyhow::{Context, Result};
use aptos_indexer_transaction_stream::TransactionStreamConfig;
use serde::{Deserialize, Serialize};
use server_framework::RunnableConfig;
use std::{collections::HashSet, time::Duration};
use url::Url;

pub const QUERY_DEFAULT_RETRIES: u32 = 5;
pub const QUERY_DEFAULT_RETRY_DELAY_MS: u64 = 500;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerProcessorConfig {
    pub processor_config: ProcessorConfig,
    pub transaction_stream_config: TransactionStreamConfig,
}

#[async_trait::async_trait]
impl RunnableConfig for IndexerProcessorConfig {
    async fn run(&self) -> Result<()> {
        match self.processor_config {
            ProcessorConfig::EventsProcessor => {
                EventsProcessor::new(self.transaction_stream_config.clone())
                    .run_processor()
                    .await
                    .context("Error running EventsProcessor")
            },
        }
    }

    fn get_server_name(&self) -> String {
        // Get the part before the first _ and trim to 12 characters.
        let before_underscore = self
            .processor_config
            .name()
            .split('_')
            .next()
            .unwrap_or("unknown");
        before_underscore[..before_underscore.len().min(12)].to_string()
    }
}

/// This enum captures the configs for all the different processors that are defined.
/// The configs for each processor should only contain configuration specific to that
/// processor. For configuration that is common to all processors, put it in
/// IndexerGrpcProcessorConfig.
#[derive(Clone, Debug, Deserialize, Serialize, strum::IntoStaticStr, strum::EnumDiscriminants)]
#[serde(tag = "type", rename_all = "snake_case")]
// What is all this strum stuff? Let me explain.
//
// Previously we had consts called NAME in each module and a function called `name` on
// the ProcessorTrait. As such it was possible for this name to not match the snake case
// representation of the struct name. By using strum we can have a single source for
// processor names derived from the enum variants themselves.
//
// That's what this strum_discriminants stuff is, it uses macro magic to generate the
// ProcessorName enum based on ProcessorConfig. The rest of the derives configure this
// generation logic, e.g. to make sure we use snake_case.
#[strum(serialize_all = "snake_case")]
#[strum_discriminants(
    derive(
        Deserialize,
        Serialize,
        strum::EnumVariantNames,
        strum::IntoStaticStr,
        strum::Display,
        clap::ValueEnum
    ),
    name(ProcessorName),
    clap(rename_all = "snake_case"),
    serde(rename_all = "snake_case"),
    strum(serialize_all = "snake_case")
)]
pub enum ProcessorConfig {
    EventsProcessor,
}

impl ProcessorConfig {
    /// Get the name of the processor config as a static str. This is a convenience
    /// method to access the derived functionality implemented by strum::IntoStaticStr.
    pub fn name(&self) -> &'static str {
        self.into()
    }
}
#[derive(Debug)]
// To ensure that the variants of ProcessorConfig and Processor line up, in the testing
// build path we derive EnumDiscriminants on this enum as well and make sure the two
// sets of variants match up in `test_processor_names_complete`.
#[cfg_attr(
    test,
    derive(strum::EnumDiscriminants),
    strum_discriminants(
        derive(strum::EnumVariantNames),
        name(ProcessorDiscriminants),
        strum(serialize_all = "snake_case")
    )
)]
pub enum Processor {
    EventsProcessor,
}

#[cfg(test)]
mod test {
    use super::*;
    use strum::VariantNames;

    /// This test exists to make sure that when a new processor is added, it is added
    /// to both Processor and ProcessorConfig. To make sure this passes, make sure the
    /// variants are in the same order (lexicographical) and the names match.
    #[test]
    fn test_processor_names_complete() {
        assert_eq!(ProcessorName::VARIANTS, ProcessorDiscriminants::VARIANTS);
    }
}

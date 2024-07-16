use super::{events_extractor::EventsExtractor, events_storer::EventsStorer};
use crate::{
    common_steps::latest_processed_version_tracker::LatestVersionProcessedTracker,
    config::indexer_processor_config::{DbConfig, IndexerProcessorConfig},
    utils::starting_version::get_starting_version,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    builder::ProcessorBuilder,
    steps::{TimedBuffer, TransactionStreamStep},
    traits::{IntoRunnableStep, RunnableStepWithInputReceiver},
};
use aptos_indexer_transaction_stream::TransactionStreamConfig;
use instrumented_channel::instrumented_bounded_channel;
use std::time::Duration;

pub struct EventsProcessor {
    pub config: IndexerProcessorConfig,
}

impl EventsProcessor {
    pub fn new(config: IndexerProcessorConfig) -> Self {
        Self { config }
    }

    pub async fn run_processor(self) -> Result<()> {
        let starting_version = get_starting_version(self.config.clone()).await?;
        let (_input_sender, input_receiver) = instrumented_bounded_channel("input", 1);

        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version: Some(starting_version),
            ..self.config.transaction_stream_config
        })
        .await?;
        let transaction_stream_with_input = RunnableStepWithInputReceiver::new(
            input_receiver,
            transaction_stream.into_runnable_step(),
        );
        let events_extractor = EventsExtractor {};
        let events_storer = EventsStorer::new(self.config.db_config.clone()).await?;
        let timed_buffer = TimedBuffer::new(Duration::from_secs(1));
        let version_tracker = LatestVersionProcessedTracker::new(
            self.config.db_config,
            starting_version,
            self.config.processor_config.name().to_string(),
        )
        .await?;

        let (_, buffer_receiver) = ProcessorBuilder::new_with_runnable_input_receiver_first_step(
            transaction_stream_with_input,
        )
        .connect_to(events_extractor.into_runnable_step(), 10)
        .connect_to(timed_buffer.into_runnable_step(), 10)
        .connect_to(events_storer.into_runnable_step(), 10)
        .connect_to(version_tracker.into_runnable_step(), 10)
        .end_and_return_output_receiver(10);

        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    if txn_context.data.is_empty() {
                        println!("Received no transactions");
                        continue;
                    }
                    println!(
                        "Received events versions: {:?} to {:?}",
                        txn_context.start_version, txn_context.end_version,
                    );
                },
                Err(e) => {
                    println!("Error receiving transactions: {:?}", e);
                },
            }
        }
    }
}

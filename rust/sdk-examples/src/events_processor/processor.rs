use super::{events_extractor::EventsExtractor, events_storer::EventsStorer};
use crate::config::indexer_processor_config::DbConfig;
use anyhow::Result;
use aptos_indexer_transaction_stream::config::TransactionStreamConfig;
use instrumented_channel::instrumented_bounded_channel;
use sdk::{
    builder::ProcessorBuilder,
    steps::{TimedBuffer, TransactionStreamStep},
    traits::{IntoRunnableStep, RunnableStepWithInputReceiver},
};
use std::time::Duration;

pub struct EventsProcessor {
    pub transaction_stream_config: TransactionStreamConfig,
    pub db_config: DbConfig,
}

impl EventsProcessor {
    pub fn new(transaction_stream_config: TransactionStreamConfig, db_config: DbConfig) -> Self {
        Self {
            transaction_stream_config,
            db_config,
        }
    }

    pub async fn run_processor(self) -> Result<()> {
        let (_, input_receiver) = instrumented_bounded_channel("input", 1);

        let transaction_stream = TransactionStreamStep::new(self.transaction_stream_config).await?;
        let transaction_stream_with_input = RunnableStepWithInputReceiver::new(
            input_receiver,
            transaction_stream.into_runnable_step(),
        );
        let events_extractor = EventsExtractor {};
        let events_storer = EventsStorer::new(self.db_config.clone()).await?;
        let timed_buffer = TimedBuffer::new(Duration::from_secs(1));

        let (_, buffer_receiver) = ProcessorBuilder::new_with_runnable_input_receiver_first_step(
            transaction_stream_with_input,
        )
        .connect_to(events_extractor.into_runnable_step(), 10)
        .connect_to(timed_buffer.into_runnable_step(), 10)
        .connect_to(events_storer.into_runnable_step(), 10)
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

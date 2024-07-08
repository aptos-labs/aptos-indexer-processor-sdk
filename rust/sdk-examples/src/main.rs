use anyhow::Result;
use aptos_indexer_transaction_stream::config::TransactionStreamConfig;
use sdk::{
    builder::ProcessorBuilder,
    steps::{TimedBuffer, TransactionStreamStep},
    traits::{IntoRunnableStep, RunnableStepWithInputReceiver},
};
use sdk_examples::events_processor::{
    events_extractor::EventsExtractor, events_storer::EventsStorer,
};
use std::time::Duration;
use url::Url;

const RUNTIME_WORKER_MULTIPLIER: usize = 2;

fn main() {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus * RUNTIME_WORKER_MULTIPLIER).max(16);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .disable_lifo_slot()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap()
        .block_on(async {
            // TODO: actually launch something here
            run_processor().await.unwrap();
        })
}

async fn run_processor() -> Result<()> {
    let (input_sender, input_receiver) = kanal::bounded_async(1);
    let transaction_stream_config = TransactionStreamConfig {
        indexer_grpc_data_service_address: Url::parse("https://grpc.devnet.aptoslabs.com:443")?,
        starting_version: 0,
        request_ending_version: None,
        auth_token: String::from("aptoslabs_TJs4NQU8Xf5_EJMNnZFPXRH6YNpWM7bCcurMBEUtZtRb6"),
        request_name_header: String::from("sdk_events_processor"),
        indexer_grpc_http2_ping_interval_secs: 30,
        indexer_grpc_http2_ping_timeout_secs: 10,
        indexer_grpc_reconnection_timeout_secs: 5,
        indexer_grpc_response_item_timeout_secs: 60,
    };

    let transaction_stream = TransactionStreamStep::new(transaction_stream_config).await?;
    let transaction_stream_with_input =
        RunnableStepWithInputReceiver::new(input_receiver, transaction_stream.into_runnable_step());
    let events_extractor = EventsExtractor {};
    let events_storer = EventsStorer::new(
        "postgresql://postgres:@localhost:5432/example".to_string(),
        None,
    )
    .await?;
    let timed_buffer = TimedBuffer::new(Duration::from_secs(1));

    let (processor_builder, buffer_receiver) =
        ProcessorBuilder::new_with_runnable_input_receiver_first_step(
            transaction_stream_with_input,
        )
        .connect_to(events_extractor.into_runnable_step(), 10)
        .connect_to(timed_buffer.into_runnable_step(), 10)
        .connect_to(events_storer.into_runnable_step(), 10)
        .end_and_return_output_receiver(10);

    loop {
        match buffer_receiver.recv().await {
            Ok(txn_context) => {
                if txn_context.data.len() == 0 {
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

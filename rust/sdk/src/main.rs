use anyhow::Result;
use sdk::simple_step::SimpleStep;
use sdk::stream::{Transaction, TransactionStream};
use sdk::timed_buffer::TimedBuffer;
use sdk::traits::async_step::{AsyncStep, AsyncStepChannelWrapper};
use sdk::traits::channel_connected_step::{
    ChannelConnectableStep, SpawnsNonPollable, SpawnsPollable, SpawnsPollableWithOutput,
};
use std::time::Duration;

const RUNTIME_WORKER_MULTIPLIER: usize = 2;

async fn processor() -> Result<()> {
    // create channels
    let (stream_sender, stream_receiver) = kanal::bounded_async::<Vec<Transaction>>(10);
    let (dag_sender, dag_receiver) = kanal::bounded_async::<Vec<Transaction>>(10);
    let (buffer_sender, buffer_receiver) = kanal::bounded_async::<Vec<Transaction>>(10);

    let stream = TransactionStream::new(stream_sender);

    let simple_step_1 = SimpleStep;
    let simple_step_2 = SimpleStep;
    let dag = simple_step_1.connect(simple_step_2);
    let simple_dag = AsyncStepChannelWrapper::new(dag, stream_receiver, dag_sender);

    let buffer = TimedBuffer::new(dag_receiver, buffer_sender, Duration::from_secs(1));

    let dag = stream.connect_channel(simple_dag).connect_channel(buffer);

    loop {
        match buffer_receiver.recv().await {
            Ok(transactions) => {
                println!("Received transactions: {:?}", transactions);
            },
            Err(e) => {
                println!("Error receiving transactions: {:?}", e);
            },
        }
    }
}

fn main() -> Result<()> {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus * RUNTIME_WORKER_MULTIPLIER).max(16);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .disable_lifo_slot()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap()
        .block_on(async { processor().await })
}

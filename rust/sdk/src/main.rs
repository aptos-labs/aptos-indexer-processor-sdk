use std::time::Duration;

use kanal::{AsyncReceiver, AsyncSender};
use sdk::connectors::AsyncStepChannelWrapper;
use sdk::simple_step::SimpleStep;
use sdk::stream::{Transaction, TransactionStream};
use sdk::timed_buffer::TimedBuffer;
use sdk::traits::async_step::{AsyncStep, SpawnsAsync, SpawnsPollable, SpawnsPollableWithOutput};

fn main() {
    let (stream_sender, stream_receiver) = kanal::bounded_async::<Vec<Transaction>>(10);
    let (buffer_sender, buffer_receiver) = kanal::bounded_async::<Vec<Transaction>>(10);
    let stream = TransactionStream::new(stream_sender);
    let buffer = TimedBuffer::new(stream_receiver, buffer_sender, Duration::from_secs(1));

    let simple_step_1 = SimpleStep;
    let simple_step_2 = SimpleStep;
    let dag = simple_step_1.connect(simple_step_2);
    let (simple_sender, _) = kanal::bounded_async::<Vec<i64>>(10);
    let (_, simple_receiver) = kanal::bounded_async::<Vec<i64>>(10);
    let simple_dag = AsyncStepChannelWrapper::new(dag, simple_receiver, simple_sender);
    simple_dag.spawn();

    buffer.spawn();
    println!("Hello, world!");
}

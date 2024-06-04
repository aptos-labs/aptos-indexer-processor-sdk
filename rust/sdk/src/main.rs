use std::time::Duration;

use kanal::{AsyncReceiver, AsyncSender};
// use sdk::pipeline::Pipeline;
use sdk::stream::{Transaction, TransactionStream};
use sdk::timed_buffer::TimedBuffer;
use sdk::traits::async_step::{SpawnsPollable, SpawnsPollableWithOutput};

fn main() {
    let (stream_sender, stream_receiver) = kanal::bounded_async::<Vec<Transaction>>(10);
    let (buffer_sender, buffer_receiver) = kanal::bounded_async::<Vec<Transaction>>(10);
    let stream = TransactionStream::new(stream_sender);
    let buffer = TimedBuffer::new(stream_receiver, buffer_sender, Duration::from_secs(1));

    stream.spawn();
    buffer.spawn();
    println!("Hello, world!");
}

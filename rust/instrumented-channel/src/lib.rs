pub mod channel_metrics;

use channel_metrics::ChannelMetrics;
use delegate::delegate;
/**

# Instrumented Channel
This is a wrapper and abstraction over the kanal channel (for now), but it can be extended to support other channels as well.

The main purpose of this crate is to provide a way to instrument the channel, so that we can track the number of messages sent and received, and the time taken to send and receive messages.

## Example
```rust
use instrumented_channel::instrumented_bounded_channel;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    let (sender, receiver) = instrumented_bounded_channel("channel_name", 10);
    sender.send(42).await.unwrap();
    assert_eq!(receiver.recv().await.unwrap(), 42);
}
```
 **/
use kanal::{AsyncReceiver, AsyncSender, ReceiveError, SendError};

pub struct InstrumentedAsyncSender<T> {
    pub(crate) sender: AsyncSender<T>,
    // Metrics
    pub(crate) channel_metrics: channel_metrics::ChannelMetrics,
}

impl<T> InstrumentedAsyncSender<T> {
    // shared_send_impl methods
    delegate! {
        to self.sender {
            pub fn is_disconnected(&self) -> bool;
            pub fn len(&self) -> usize;
            pub fn is_empty(&self) -> bool;
            pub fn is_full(&self) -> bool;
            pub fn capacity(&self);
            pub fn receiver_count(&self) -> u32;
            pub fn sender_count(&self) -> u32;
            pub fn close(&self) -> bool;
            pub fn is_closed(&self) -> bool;
        }
    }

    pub fn new(sender: AsyncSender<T>, name: &str) -> Self {
        let channel_metrics = ChannelMetrics::new(name.to_string());

        Self {
            sender,
            channel_metrics,
        }
    }

    pub async fn send(&'_ self, data: T) -> Result<(), SendError> {
        let send_start = std::time::Instant::now();
        let res = self.sender.send(data).await;
        let send_duration = send_start.elapsed();

        if res.is_err() {
            self.channel_metrics
                .log_send_duration(send_duration.as_secs_f64())
                .inc_failed_sends_count();
        } else {
            self.channel_metrics
                .log_send_duration(send_duration.as_secs_f64())
                .inc_sent_messages_count();
        }

        res
    }
}

impl<T> Clone for InstrumentedAsyncSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            channel_metrics: self.channel_metrics.clone(),
        }
    }
}

pub struct InstrumentedAsyncReceiver<T> {
    pub(crate) receiver: AsyncReceiver<T>,
    // Metrics
    pub(crate) channel_metrics: ChannelMetrics,
}

impl<T> InstrumentedAsyncReceiver<T> {
    // shared_recv_impl methods
    delegate! {
        to self.receiver {
            pub fn is_disconnected(&self) -> bool;
            pub fn len(&self) -> usize;
            pub fn is_empty(&self) -> bool;
            pub fn is_full(&self) -> bool;
            pub fn capacity(&self);
            pub fn receiver_count(&self) -> u32;
            pub fn sender_count(&self) -> u32;
            pub fn close(&self) -> bool;
            pub fn is_closed(&self) -> bool;
        }
    }

    pub fn new(receiver: AsyncReceiver<T>, name: &str) -> Self {
        let channel_metrics = ChannelMetrics::new(name.to_string());
        Self {
            receiver,
            channel_metrics,
        }
    }

    pub async fn recv(&'_ self) -> Result<T, ReceiveError> {
        let receive_start = std::time::Instant::now();
        let result = self.receiver.recv().await;
        let receive_duration = receive_start.elapsed();

        if result.is_err() {
            self.channel_metrics
                .log_receive_duration(receive_duration.as_secs_f64())
                .inc_failed_receives_count();
        } else {
            self.channel_metrics
                .log_receive_duration(receive_duration.as_secs_f64())
                .inc_received_messages_count();
        }

        result
    }
}

impl<T> Clone for InstrumentedAsyncReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
            channel_metrics: self.channel_metrics.clone(),
        }
    }
}

pub fn instrumented_bounded_channel<T>(
    channel_name: &str,
    size: usize,
) -> (InstrumentedAsyncSender<T>, InstrumentedAsyncReceiver<T>) {
    let (sender, receiver) = kanal::bounded_async(size);
    (
        InstrumentedAsyncSender::new(sender, channel_name),
        InstrumentedAsyncReceiver::new(receiver, channel_name),
    )
}

pub fn instrumented_unbounded_channel<T>(
    channel_name: &str,
) -> (InstrumentedAsyncSender<T>, InstrumentedAsyncReceiver<T>) {
    let (sender, receiver) = kanal::unbounded_async();
    (
        InstrumentedAsyncSender::new(sender, channel_name),
        InstrumentedAsyncReceiver::new(receiver, channel_name),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Encoder;

    fn gather_metrics_to_string() -> String {
        let metrics = prometheus::gather();
        let mut buffer = vec![];
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&metrics, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
    #[tokio::test]
    async fn test_instrumented_channel() {
        let (sender, receiver) = instrumented_bounded_channel("my_channel", 10);
        sender.send(42).await.unwrap();
        sender.send(999).await.unwrap();
        sender.send(3).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 42);
        // TODO: check prometheus metrics
        let metrics = gather_metrics_to_string();
        println!("{}", metrics);
    }
}

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

// Double __ on purpose
const METRICS_PREFIX: &str = "aptos_procsdk_channel_";

pub struct InstrumentedAsyncSender<T> {
    pub(crate) sender: AsyncSender<T>,
    // Metrics
    pub(crate) sent_messages: prometheus::IntCounterVec,
    pub(crate) send_duration: prometheus::HistogramVec,
    pub(crate) failed_sends: prometheus::IntCounterVec,
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
        let sent_messages = prometheus::register_int_counter_vec!(
            // TODO: better to make them separate series, or use labels?
            format!("{}_{}_sent_messages", METRICS_PREFIX, name),
            "Number of messages sent",
            &[],
        )
        .unwrap();

        let send_duration = prometheus::register_histogram_vec!(
            format!("{}_{}_message_send_await_duration_ms", METRICS_PREFIX, name),
            "Time taken to complete awaiting a message send in milliseconds",
            &[],
            // TODO: better buckets?
            vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0],
        )
        .unwrap();

        let failed_sends = prometheus::register_int_counter_vec!(
            format!("{}_{}_failed_message_sends", METRICS_PREFIX, name),
            "Number of failed message sends",
            &[],
        )
        .unwrap();

        Self {
            sender,
            sent_messages,
            send_duration,
            failed_sends,
        }
    }

    pub async fn send(&'_ self, data: T) -> Result<(), SendError> {
        let send_start = std::time::Instant::now();
        let res = self.sender.send(data).await;
        let send_duration = send_start.elapsed();
        self.send_duration
            .with_label_values(&[])
            .observe(send_duration.as_millis() as f64);
        self.sent_messages.with_label_values(&[]).inc();
        if res.is_err() {
            self.failed_sends.with_label_values(&[]).inc();
        }
        res
    }
}

impl<T> Clone for InstrumentedAsyncSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            sent_messages: self.sent_messages.clone(),
            send_duration: self.send_duration.clone(),
            failed_sends: self.failed_sends.clone(),
        }
    }
}

pub struct InstrumentedAsyncReceiver<T> {
    pub(crate) receiver: AsyncReceiver<T>,
    // Metrics
    pub(crate) received_messages: prometheus::IntCounterVec,
    pub(crate) receive_duration: prometheus::HistogramVec,
    pub(crate) _failed_receives: prometheus::IntCounterVec,
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
        // TODO: channel size
        let received_messages = prometheus::register_int_counter_vec!(
            format!("{}_{}_received_messages", METRICS_PREFIX, name),
            "Number of messages received",
            &[],
        )
        .unwrap();
        let receive_duration = prometheus::register_histogram_vec!(
            format!(
                "{}_{}_message_receive_await_duration_ms",
                METRICS_PREFIX, name
            ),
            "Time taken to complete awaiting a message receive in milliseconds",
            &[],
        )
        .unwrap();
        let _failed_receives = prometheus::register_int_counter_vec!(
            format!("{}_{}_failed_message_receives", METRICS_PREFIX, name),
            "Number of failed message receives",
            &[],
        )
        .unwrap();
        Self {
            receiver,
            received_messages,
            receive_duration,
            _failed_receives,
        }
    }

    pub async fn recv(&'_ self) -> Result<T, ReceiveError> {
        let receive_start = std::time::Instant::now();
        let result = self.receiver.recv().await;
        let receive_duration = receive_start.elapsed();
        self.receive_duration
            .with_label_values(&[])
            .observe(receive_duration.as_millis() as f64);
        self.received_messages.with_label_values(&[]).inc();
        result
    }
}

impl<T> Clone for InstrumentedAsyncReceiver<T> {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
            received_messages: self.received_messages.clone(),
            receive_duration: self.receive_duration.clone(),
            _failed_receives: self._failed_receives.clone(),
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

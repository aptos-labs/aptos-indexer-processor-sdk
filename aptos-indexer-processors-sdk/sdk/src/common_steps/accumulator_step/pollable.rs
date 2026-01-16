use super::{Accumulatable, Accumulator};
use crate::{
    traits::{NamedStep, RunnableStep},
    types::transaction_context::TransactionContext,
    utils::step_metrics::{StepMetricLabels, StepMetricsBuilder},
};
use bigdecimal::Zero;
use instrumented_channel::{
    instrumented_bounded_channel, InstrumentedAsyncReceiver, InstrumentedAsyncSender,
};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// A step that accumulates input batches for a fixed time interval before flushing.
///
/// Unlike [`AccumulatorStep`](super::AccumulatorStep) which only accumulates when the output
/// channel is full (backpressure-based), this step *always* accumulates incoming batches for the
/// configured `flush_interval` before sending them downstream as a single merged batch.
///
/// This is useful when you want to batch multiple small inputs into larger chunks regardless of
/// downstream pressure, for example to reduce the frequency of database writes or API calls.
///
/// In more detail, each iteration of the loop does the following:
/// 1. Receive input from the input channel
/// 2. Start accumulation with the received input and begin the flush deadline timer
/// 3. Accumulate more inputs until flush conditions are met:
///    a. If the flush deadline is reached: flush the accumulator
///    b. If `max_accumulator_size_bytes` is reached: wait for the flush deadline, then flush
///    c. Otherwise: receive and accumulate more input, or flush on timeout
/// 4. Instrument the output and send it to the output channel
pub struct PollableAccumulatorStep {
    /// Duration to accumulate batches before flushing.
    flush_interval: Duration,
    /// Maximum size of accumulated bytes.
    ///
    /// If the accumulator size exceeds this limit, the step will stop accepting new inputs
    /// until the next scheduled flush.
    max_accumulator_size_bytes: usize,
}

impl PollableAccumulatorStep {
    /// Creates a new PollableAccumulatorStep.
    pub fn new(flush_interval_ms: u64, max_accumulator_size_bytes: usize) -> Self {
        Self {
            flush_interval: Duration::from_millis(flush_interval_ms),
            max_accumulator_size_bytes,
        }
    }
}

impl NamedStep for PollableAccumulatorStep {
    fn name(&self) -> String {
        "PollableAccumulatorStep".to_string()
    }
}

impl<T: Accumulatable + Sync + Send + 'static> RunnableStep<T, T> for PollableAccumulatorStep {
    fn spawn(
        self,
        input_receiver: Option<InstrumentedAsyncReceiver<TransactionContext<T>>>,
        output_channel_size: usize,
        _input_sender: Option<InstrumentedAsyncSender<TransactionContext<T>>>,
    ) -> (
        InstrumentedAsyncReceiver<TransactionContext<T>>,
        JoinHandle<()>,
    ) {
        let step = self;
        let step_name = step.name();
        let input_receiver = input_receiver.expect("Input receiver must be set");

        let (output_sender, output_receiver) =
            instrumented_bounded_channel(&step_name, output_channel_size);

        info!(
            step_name = step_name,
            flush_interval_ms = step.flush_interval.as_millis(),
            max_accumulator_size_bytes = step.max_accumulator_size_bytes,
            "Spawning pollable accumulator task"
        );

        let handle = tokio::spawn(async move {
            loop {
                // 1. Receive input
                let input_with_context = match input_receiver.recv().await {
                    Ok(input) => input,
                    Err(e) => {
                        warn!(
                            step_name = step_name,
                            error = e.to_string(),
                            "Input channel closed"
                        );
                        break;
                    },
                };

                let processing_start = Instant::now();

                // 2. Start accumulation and begin flush deadline timer
                let flush_deadline = processing_start + step.flush_interval;
                let mut accumulator = Accumulator::new(input_with_context);

                // 3. Accumulate more inputs until the flush deadline or max buffer size is reached
                loop {
                    let time_until_flush = flush_deadline.saturating_duration_since(Instant::now());

                    // 3a. If the flush deadline is reached, flush
                    if time_until_flush.is_zero() {
                        break;
                    }

                    // 3b. If max_accumulator_size_bytes is reached, wait for the flush deadline
                    // then flush
                    if accumulator.total_size_in_bytes() >= step.max_accumulator_size_bytes {
                        warn!(
                            step_name,
                            accumulator_size_bytes = accumulator.total_size_in_bytes(),
                            max_accumulator_size_bytes = step.max_accumulator_size_bytes,
                            time_until_flush_ms = time_until_flush.as_millis(),
                            "Accumulator full, waiting for flush deadline..."
                        );
                        tokio::time::sleep(time_until_flush).await;
                        break;
                    }

                    // 3c. Otherwise, receive and accumulate more input until flushu deadline
                    match tokio::time::timeout(time_until_flush, input_receiver.recv()).await {
                        Ok(Ok(input)) => {
                            accumulator.accumulate(input);
                        },
                        Ok(Err(e)) => {
                            warn!(
                                step_name = step_name,
                                error = e.to_string(),
                                "Input channel closed during accumulation"
                            );
                            break;
                        },
                        Err(_) => {
                            // Flush deadline reached.
                            break;
                        },
                    }
                }

                // 4. Instrument the output and send it to the output channel
                let output_with_context = TransactionContext::from(accumulator);
                match StepMetricsBuilder::default()
                    .labels(StepMetricLabels {
                        step_name: step_name.clone(),
                    })
                    .latest_processed_version(output_with_context.metadata.end_version)
                    .processed_transaction_latency(output_with_context.get_transaction_latency())
                    .latest_transaction_timestamp(
                        output_with_context.get_start_transaction_timestamp_unix(),
                    )
                    .num_transactions_processed_count(output_with_context.get_num_transactions())
                    .processing_duration_in_secs(processing_start.elapsed().as_secs_f64())
                    .processed_size_in_bytes(output_with_context.metadata.total_size_in_bytes)
                    .build()
                {
                    Ok(mut metrics) => metrics.log_metrics(),
                    Err(e) => {
                        error!(
                            step_name = step_name,
                            error = e.to_string(),
                            "Failed to log metrics"
                        );
                        break;
                    },
                }
                match output_sender.send(output_with_context).await {
                    Ok(_) => (),
                    Err(e) => {
                        error!(
                            step_name = step_name,
                            error = e.to_string(),
                            "Error sending output to channel"
                        );
                        break;
                    },
                }
            }

            // Wait for output channel to be empty before ending the task and closing the send channel
            loop {
                let channel_size = output_sender.len();
                info!(
                    step_name = step_name,
                    channel_size = channel_size,
                    "Waiting for output channel to be empty"
                );
                if channel_size.is_zero() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            info!(
                step_name = step_name,
                "Output channel is empty. Closing send channel."
            );
        });

        (output_receiver, handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::transaction_context::TransactionMetadata;
    use tokio::time::timeout;

    #[derive(Clone, Debug, PartialEq)]
    struct TestData {
        items: Vec<u64>,
    }

    impl Accumulatable for TestData {
        fn accumulate(&mut self, other: Self) {
            self.items.extend(other.items);
        }
    }

    fn make_test_context(
        items: Vec<u64>,
        start_version: u64,
        end_version: u64,
        size_in_bytes: u64,
    ) -> TransactionContext<TestData> {
        TransactionContext {
            data: TestData { items },
            metadata: TransactionMetadata {
                start_version,
                end_version,
                start_transaction_timestamp: None,
                end_transaction_timestamp: None,
                total_size_in_bytes: size_in_bytes,
            },
        }
    }

    #[tokio::test]
    async fn test_pollable_accumulator_flushes_on_timeout() {
        let step = PollableAccumulatorStep::new(100, 1_000_000);
        let (input_sender, input_receiver) = instrumented_bounded_channel("test_input", 10);

        let (output_receiver, _handle) =
            RunnableStep::<TestData, TestData>::spawn(step, Some(input_receiver), 10, None);

        // Send two items
        input_sender
            .send(make_test_context(vec![1, 2], 0, 1, 50))
            .await
            .unwrap();
        input_sender
            .send(make_test_context(vec![3, 4], 2, 3, 50))
            .await
            .unwrap();

        // Wait for flush (timeout + some buffer)
        let result = timeout(Duration::from_millis(200), output_receiver.recv()).await;
        assert!(result.is_ok(), "Should receive flushed output");

        let output = result.unwrap().unwrap();
        assert_eq!(output.data.items, vec![1, 2, 3, 4]);
        assert_eq!(output.metadata.start_version, 0);
        assert_eq!(output.metadata.end_version, 3);
        assert_eq!(output.metadata.total_size_in_bytes, 100);
    }

    #[tokio::test]
    async fn test_pollable_accumulator_flushes_on_channel_close() {
        let step = PollableAccumulatorStep::new(10_000, 1_000_000);
        let (input_sender, input_receiver) = instrumented_bounded_channel("test_input", 10);

        let (output_receiver, _handle) =
            RunnableStep::<TestData, TestData>::spawn(step, Some(input_receiver), 10, None);

        // Send one item then close
        input_sender
            .send(make_test_context(vec![1, 2], 0, 1, 50))
            .await
            .unwrap();
        drop(input_sender);

        // Should receive the accumulated data even though timeout hasn't elapsed
        let result = timeout(Duration::from_millis(500), output_receiver.recv()).await;
        assert!(result.is_ok(), "Should receive flushed output on close");

        let output = result.unwrap().unwrap();
        assert_eq!(output.data.items, vec![1, 2]);
    }
}

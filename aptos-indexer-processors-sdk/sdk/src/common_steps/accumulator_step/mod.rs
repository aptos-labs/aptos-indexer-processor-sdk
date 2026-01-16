use crate::{
    traits::{NamedStep, RunnableStep},
    types::transaction_context::TransactionContext,
    utils::step_metrics::{StepMetricLabels, StepMetricsBuilder},
};
use aptos_protos::transaction::v1::Transaction;
use bigdecimal::Zero;
use instrumented_channel::{
    instrumented_bounded_channel, InstrumentedAsyncReceiver, InstrumentedAsyncSender,
};
pub use pollable::PollableAccumulatorStep;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

mod pollable;

const FLUSH_POLL_INTERVAL_MS: Duration = Duration::from_millis(100);

/// Trait for types that can be accumulated.
pub trait Accumulatable {
    fn accumulate(&mut self, other: Self);
}

impl Accumulatable for Vec<Transaction> {
    fn accumulate(&mut self, other: Self) {
        self.extend(other);
    }
}

/// A struct that accumulates transaction contexts.
pub(crate) struct Accumulator<T: Accumulatable + Sync + Send + 'static>(TransactionContext<T>);

impl<T: Accumulatable + Sync + Send + 'static> Accumulator<T> {
    pub(crate) fn new(ctx: TransactionContext<T>) -> Self {
        Self(ctx)
    }

    /// Adds the transaction context to the accumulator.
    ///
    /// The new context is added to the accumulator by updating the metadata and accumulating the
    /// data.
    pub(crate) fn accumulate(&mut self, ctx: TransactionContext<T>) {
        self.0.metadata.end_version = ctx.metadata.end_version;
        self.0.metadata.end_transaction_timestamp = ctx.metadata.end_transaction_timestamp;
        self.0.metadata.total_size_in_bytes += ctx.metadata.total_size_in_bytes;
        self.0.data.accumulate(ctx.data);
    }

    /// Returns the total size of the accumulator in bytes.
    pub(crate) fn total_size_in_bytes(&self) -> usize {
        self.0.metadata.total_size_in_bytes as usize
    }
}

impl<T: Accumulatable + Sync + Send + 'static> From<Accumulator<T>> for TransactionContext<T> {
    fn from(accumulator: Accumulator<T>) -> Self {
        accumulator.0
    }
}

/// A step that accumulates input batches when the output channel is full.
///
/// When downstream steps are slow and the output channel becomes full, this step will accumulate
/// incoming batches and into a single, larger batch. When the output channel has space again, it
/// flushes the accumulated batch.
///
/// In more detail, each iteration of the loop does the following:
/// 1. Receive input from the input channel
/// 2. Determine the output based on output channel state:
///    a. If the output channel is NOT full: pass the input through as is
///    b. If the output channel is full:
///       - Accumulate the input
///       - Keep receiving and accumulating inputs until the output channel has space
///       - Flush the accumulator
/// 3. Instrument the output and send it to the output channel
///
/// If downstream steps are limited by IO bottlenecks, the accumulator step is especially useful
/// when tailing the head of the transaction gRPC stream. Due to the behavior of the transaction
/// stream, even if the processor falls behind, the stream will continue to produce small batches of
/// transactions as opposed to combining the remaining transactions into a single, larger batch. The
/// accumulator step will serve as a buffer for merging the small batches of transactions to reduce
/// the number of IOPS for the downstream steps.
///
/// # Example
/// Accumulating batches of transactions between the TransactionStreamStep and a slow DbWriteStep.
/// The accumulation not only decreases the number of database IOPS by increasing the number of
/// transactions processed per write but also allows the upstream TransactionStreamStep to continue
/// polling the gRPC stream.
///
/// ```text
/// ┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────────┐
/// │ TransactionStreamStep │ --> │    AccumulatorStep    │ --> │      DbWriteStep      │
/// └─────────────────────┘     └─────────────────────┘     └─────────────────────┘
/// ```
pub struct AccumulatorStep {
    /// Maximum size of accummulated bytes.
    ///
    /// If the accumulator size exceeds the max buffer size, the step will wait for a flush before
    /// accumulating more.
    max_accumulator_size_bytes: usize,
}

impl AccumulatorStep {
    /// Creates a new AccumulatorStep with the given max buffer size in bytes.
    pub fn new(max_accumulator_size_bytes: usize) -> Self {
        Self {
            max_accumulator_size_bytes,
        }
    }
}

impl NamedStep for AccumulatorStep {
    fn name(&self) -> String {
        "AccumulatorStep".to_string()
    }
}

impl<T: Accumulatable + Sync + Send + 'static> RunnableStep<T, T> for AccumulatorStep {
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
            max_accumulator_size_bytes = step.max_accumulator_size_bytes,
            "Spawning accumulating task"
        );

        let handle = tokio::spawn(async move {
            loop {
                // 1. Receive input
                let input_with_context = match input_receiver.recv().await {
                    Ok(input_with_context) => input_with_context,
                    Err(e) => {
                        // If the previous steps have finished and the channels have closed, we
                        // should break out of the loop.
                        warn!(
                            step_name = step_name,
                            error = e.to_string(),
                            "No input received from channel"
                        );
                        break;
                    },
                };

                let processing_duration = Instant::now();

                // 2. Determine the output
                // 2a. If the output channel is not full, pass the input through as is.
                let output_with_context = if !output_sender.is_full() {
                    input_with_context
                }
                // 2b. If the output channel is full, start the accumulation process.
                else {
                    info!(
                        step_name,
                        max_accumulator_size_bytes = step.max_accumulator_size_bytes,
                        channel_size = output_sender.len(),
                        "Output channel is full, accumulating..."
                    );
                    let mut accumulator = Accumulator::new(input_with_context);

                    // Loop untill the accumulator can be flushed.
                    loop {
                        // If the output channel is not full, flush.
                        if !output_sender.is_full() {
                            break;
                        }

                        // If the accumulator size exceeds the max buffer size, wait for space in
                        // the output channel, then flush.
                        if accumulator.total_size_in_bytes() >= step.max_accumulator_size_bytes {
                            while output_sender.is_full() {
                                warn!(
                                    step_name,
                                    accumulator_size_bytes = accumulator.total_size_in_bytes(),
                                    max_buffer_size_bytes = step.max_accumulator_size_bytes,
                                    "Accumulator buffer max size exceeded, waiting for output channel space..."
                                );
                                tokio::time::sleep(FLUSH_POLL_INTERVAL_MS).await;
                            }
                            break;
                        }

                        // Try to accumulate more input or loop to run checks again.
                        tokio::select! {
                            biased;
                            rx_result = input_receiver.recv() => {
                                let Ok(input) = rx_result else {
                                    warn!(step_name, "Input channel closed during accumulation");
                                    break;
                                };
                                accumulator.accumulate(input);
                            },
                            _ = tokio::time::sleep(FLUSH_POLL_INTERVAL_MS) => {},
                        }
                    }

                    // Flush the accumulator
                    TransactionContext::from(accumulator)
                };

                // 3. Instrument the output and send it to the output channel
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
                    .processing_duration_in_secs(processing_duration.elapsed().as_secs_f64())
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

    #[test]
    fn test_accumulator_new() {
        let ctx = make_test_context(vec![1, 2, 3], 0, 2, 100);
        let accumulator: Accumulator<TestData> = Accumulator::new(ctx);

        assert_eq!(accumulator.0.data.items, vec![1, 2, 3]);
        assert_eq!(accumulator.0.metadata.start_version, 0);
        assert_eq!(accumulator.0.metadata.end_version, 2);
        assert_eq!(accumulator.0.metadata.total_size_in_bytes, 100);
    }

    #[test]
    fn test_accumulator_accumulate() {
        let ctx1 = make_test_context(vec![1, 2], 0, 1, 50);
        let ctx2 = make_test_context(vec![3, 4], 2, 3, 60);
        let ctx3 = make_test_context(vec![5], 4, 4, 40);

        let mut accumulator = Accumulator::new(ctx1);
        accumulator.accumulate(ctx2);
        accumulator.accumulate(ctx3);

        assert_eq!(accumulator.0.data.items, vec![1, 2, 3, 4, 5]);
        assert_eq!(accumulator.0.metadata.start_version, 0);
        assert_eq!(accumulator.0.metadata.end_version, 4);
        assert_eq!(accumulator.0.metadata.total_size_in_bytes, 150);
    }

    #[test]
    fn test_accumulator_total_size_in_bytes() {
        let ctx = make_test_context(vec![1, 2], 0, 1, 100);
        let accumulator = Accumulator::new(ctx);

        assert_eq!(accumulator.total_size_in_bytes(), 100);
    }

    #[test]
    fn test_accumulator_into_transaction_context() {
        let ctx1 = make_test_context(vec![1, 2], 0, 1, 100);
        let ctx2 = make_test_context(vec![3, 4], 2, 3, 200);

        let mut accumulator = Accumulator::new(ctx1);
        accumulator.accumulate(ctx2);

        let result: TransactionContext<TestData> = accumulator.into();

        assert_eq!(result.data.items, vec![1, 2, 3, 4]);
        assert_eq!(result.metadata.start_version, 0);
        assert_eq!(result.metadata.end_version, 3);
        assert_eq!(result.metadata.total_size_in_bytes, 300);
    }
}

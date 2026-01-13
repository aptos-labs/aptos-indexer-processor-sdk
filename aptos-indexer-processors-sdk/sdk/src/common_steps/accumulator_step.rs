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
use std::time::{Duration, Instant};
use tokio::{sync::Notify, task::JoinHandle};
use tracing::{error, info, warn};

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

/// A step that accumulates input batches when the output channel is full.
///
/// When downstream steps are slow and the output channel becomes full, this step will accumulate
/// incoming batches and into a single, larger batch. When the output channel has space again, it
/// flushes the accumulated batch.
///
/// In more detail, each iteration of the loop does the following:
/// 1. Receive input from the input channel
/// 2. Determine the output based on output channel state:
///    a. If the output channel is NOT full:
///       - If the accumulator has data, accumulate the input and flush
///       - Otherwise, pass the input through as is
///    b. If the output channel is full:
///       - Accumulate the input
///       - Keep receiving and accumulating inputs until the output channel has space
///       - Flush the accumulator
/// 3. Instrument the output and send it to the output channel
///
/// The accumulator step is especially useful when tailing the head of the transaction gRPC stream
/// because it does not support client-side backpressure handling. Even if the processor falls
/// behind the transaction stream, the transaction stream will continue to produce small batches of
/// transactions as opposed to batching the remaining transactions into a single, larger batch. This
/// is a common occurence when downstream steps are limited by IO bottlenecks. The accumulator step
/// will serve as a buffer to accumulate transactions from the transaction stream and reduce the
/// number of IOPS.
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
pub struct AccumulatorStep<T: Accumulatable + Sync + Send + 'static> {
    /// Maximum size of accummulated bytes.
    ///
    /// If the accumulator size exceeds the max buffer size, the step will wait for a flush before
    /// accumulating more.
    max_buffer_size_bytes: usize,
    flush_notify: Notify,
    accumulator: Option<TransactionContext<T>>,
}

impl<T: Accumulatable + Sync + Send + 'static> AccumulatorStep<T> {
    /// Creates a new AccumulatorStep with the given max buffer size in bytes.
    pub fn new(max_buffer_size_bytes: usize) -> Self {
        Self {
            max_buffer_size_bytes,
            flush_notify: Notify::new(),
            accumulator: None,
        }
    }

    /// Adds the transaction context to the accumulator.
    ///
    /// If the accumulator is empty, the new context is simply stored in the accumulator. Otherwise
    /// if the accumulator is not empty, the new context is added to the accumulator by updating the
    /// metadata and accumulating the data. Finally if the accumulator size exceeds the max buffer
    /// size, this function will wait for a flush to be completed before continuing to accumulate.
    pub async fn accumulate(&mut self, ctx: TransactionContext<T>) {
        // Wait for flush if the accumulator size exceeds the max buffer size
        let accumulator_size_bytes = self.get_accumulator_size_bytes();
        if accumulator_size_bytes >= self.max_buffer_size_bytes {
            warn!(
                accumulator_size_bytes = accumulator_size_bytes,
                max_buffer_size_bytes = self.max_buffer_size_bytes,
                "Accumulator buffer max size exceeded, waiting for flush..."
            );
            self.flush_notify.notified().await;
        }

        self.accumulate_impl(ctx);
    }

    /// Accumulates the new context into the accumulator.
    fn accumulate_impl(&mut self, ctx: TransactionContext<T>) {
        if let Some(accumulator) = self.accumulator.as_mut() {
            accumulator.metadata.end_version = ctx.metadata.end_version;
            accumulator.metadata.end_transaction_timestamp = ctx.metadata.end_transaction_timestamp;
            accumulator.metadata.total_size_in_bytes += ctx.metadata.total_size_in_bytes;
            accumulator.data.accumulate(ctx.data);
        } else {
            self.accumulator = Some(ctx);
        }
    }

    /// Flushes the accumulator by returning the accumulated transaction context.
    ///
    /// This function will also notify any waiting tasks that the accumulator has been flushed.
    pub fn flush(&mut self) -> Option<TransactionContext<T>> {
        let accumulator = self.accumulator.take();
        if accumulator.is_some() {
            self.flush_notify.notify_waiters();
        }
        accumulator
    }

    /// Returns the total size of the accumulator in bytes.
    pub fn get_accumulator_size_bytes(&self) -> usize {
        self.accumulator
            .as_ref()
            .map_or(0, |data| data.metadata.total_size_in_bytes as usize)
    }
}

impl<T: Accumulatable + Sync + Send + 'static> NamedStep for AccumulatorStep<T> {
    fn name(&self) -> String {
        "AccumulatorStep".to_string()
    }
}

impl<T: Accumulatable + Sync + Send + 'static> RunnableStep<T, T> for AccumulatorStep<T> {
    fn spawn(
        self,
        input_receiver: Option<InstrumentedAsyncReceiver<TransactionContext<T>>>,
        output_channel_size: usize,
        _input_sender: Option<InstrumentedAsyncSender<TransactionContext<T>>>,
    ) -> (
        InstrumentedAsyncReceiver<TransactionContext<T>>,
        JoinHandle<()>,
    ) {
        let mut step = self;
        let step_name = step.name();
        let input_receiver = input_receiver.expect("Input receiver must be set");

        let (output_sender, output_receiver) =
            instrumented_bounded_channel(&step_name, output_channel_size);

        info!(step_name = step_name, "Spawning accumulating task");
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
                // 2a. If the output channel is not full, flush if accumulator is some or pass
                // through.
                let output_with_context = if !output_sender.is_full() {
                    // If the accumulator is not empty, flush it and add the input to the
                    // accumulator.
                    if step.accumulator.is_some() {
                        // Accumulate the final input without waiting for a flush. This is to avoid
                        // the deadlock that occurs if adding this final input would cause the
                        // accumulator to exceed the max buffer size and wait for a flush, which
                        // happens immediately afterwards.
                        step.accumulate_impl(input_with_context);
                        step.flush()
                    }
                    // 2b. Otherwise, return the input as is
                    else {
                        Some(input_with_context)
                    }
                }
                // 2b. If the output channel is full, start the accumulation process.
                else {
                    info!(
                        step_name,
                        accumulator_size_bytes = step.get_accumulator_size_bytes(),
                        channel_size = output_sender.len(),
                        "Output channel is full, accumulating..."
                    );
                    step.accumulate(input_with_context).await;

                    // Keep accumulating until the output channel has space.
                    loop {
                        tokio::select! {
                            input_with_context = input_receiver.recv() => {
                                if let Ok(input_with_context) = input_with_context {
                                    step.accumulate(input_with_context).await;
                                }
                            },
                            _ = tokio::time::sleep(FLUSH_POLL_INTERVAL_MS) => {
                                if !output_sender.is_full() {
                                    break;
                                }
                            },
                        }
                    }

                    // Flush the accumulator
                    step.flush()
                };

                // 3. Instrument the output and send it to the output channel
                if let Some(output_with_context) = output_with_context {
                    match StepMetricsBuilder::default()
                        .labels(StepMetricLabels {
                            step_name: step_name.clone(),
                        })
                        .latest_processed_version(output_with_context.metadata.end_version)
                        .processed_transaction_latency(
                            output_with_context.get_transaction_latency(),
                        )
                        .latest_transaction_timestamp(
                            output_with_context.get_start_transaction_timestamp_unix(),
                        )
                        .num_transactions_processed_count(
                            output_with_context.get_num_transactions(),
                        )
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

    #[tokio::test]
    async fn test_accumulate_first_item() {
        let mut step: AccumulatorStep<TestData> = AccumulatorStep::new(1000);
        let ctx = make_test_context(vec![1, 2, 3], 0, 2, 100);

        step.accumulate(ctx).await;

        assert!(step.accumulator.is_some());
        let acc = step.accumulator.as_ref().unwrap();
        assert_eq!(acc.data.items, vec![1, 2, 3]);
        assert_eq!(acc.metadata.start_version, 0);
        assert_eq!(acc.metadata.end_version, 2);
        assert_eq!(acc.metadata.total_size_in_bytes, 100);
    }

    #[tokio::test]
    async fn test_accumulate_multiple_items() {
        let mut step: AccumulatorStep<TestData> = AccumulatorStep::new(10000);

        let ctx1 = make_test_context(vec![1, 2], 0, 1, 50);
        let ctx2 = make_test_context(vec![3, 4], 2, 3, 60);
        let ctx3 = make_test_context(vec![5], 4, 4, 40);

        step.accumulate(ctx1).await;
        step.accumulate(ctx2).await;
        step.accumulate(ctx3).await;

        let acc = step.accumulator.as_ref().unwrap();
        assert_eq!(acc.data.items, vec![1, 2, 3, 4, 5]);
        assert_eq!(acc.metadata.start_version, 0);
        assert_eq!(acc.metadata.end_version, 4);
        assert_eq!(acc.metadata.total_size_in_bytes, 150);
    }

    #[tokio::test]
    async fn test_flush_empty_accumulator() {
        let mut step: AccumulatorStep<TestData> = AccumulatorStep::new(1000);

        let result = step.flush();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_flush_with_data() {
        let mut step: AccumulatorStep<TestData> = AccumulatorStep::new(1000);

        let ctx1 = make_test_context(vec![1, 2], 0, 1, 100);
        let ctx2 = make_test_context(vec![3, 4], 2, 3, 200);

        step.accumulate(ctx1).await;
        step.accumulate(ctx2).await;

        let result = step.flush();
        assert!(result.is_some());

        let flushed = result.unwrap();
        assert_eq!(flushed.data.items, vec![1, 2, 3, 4]);
        assert_eq!(flushed.metadata.start_version, 0);
        assert_eq!(flushed.metadata.end_version, 3);
        assert_eq!(flushed.metadata.total_size_in_bytes, 300);

        assert!(step.accumulator.is_none());
        assert_eq!(step.get_accumulator_size_bytes(), 0);
    }

    #[tokio::test]
    async fn test_accumulate_impl() {
        let mut step: AccumulatorStep<TestData> = AccumulatorStep::new(1000);

        let ctx1 = make_test_context(vec![1], 0, 0, 50);
        let ctx2 = make_test_context(vec![2], 1, 1, 50);

        step.accumulate_impl(ctx1);
        step.accumulate_impl(ctx2);

        let acc = step.accumulator.as_ref().unwrap();
        assert_eq!(acc.data.items, vec![1, 2]);
        assert_eq!(acc.metadata.total_size_in_bytes, 100);
    }

    #[tokio::test]
    async fn test_multiple_flush_cycles() {
        let mut step: AccumulatorStep<TestData> = AccumulatorStep::new(1000);

        step.accumulate(make_test_context(vec![1, 2], 0, 1, 100))
            .await;
        let result1 = step.flush();
        assert!(result1.is_some());
        assert_eq!(result1.unwrap().data.items, vec![1, 2]);

        step.accumulate(make_test_context(vec![3, 4], 2, 3, 100))
            .await;
        let result2 = step.flush();
        assert!(result2.is_some());
        assert_eq!(result2.unwrap().data.items, vec![3, 4]);

        let result3 = step.flush();
        assert!(result3.is_none());
    }
}

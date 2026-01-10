use aptos_indexer_processor_sdk::{
    common_steps::{Accumulatable, AccumulatorStep},
    instrumented_channel::instrumented_bounded_channel,
    traits::RunnableStep,
    types::transaction_context::{TransactionContext, TransactionMetadata},
};
use async_trait::async_trait;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Default)]
struct TestBatch {
    items: Vec<u64>,
}

#[async_trait]
impl Accumulatable for TestBatch {
    async fn accumulate(&mut self, other: Self) {
        self.items.extend(other.items);
    }
}

fn make_test_context(
    items: Vec<u64>,
    start_version: u64,
    end_version: u64,
    size_in_bytes: u64,
) -> TransactionContext<TestBatch> {
    TransactionContext {
        data: TestBatch { items },
        metadata: TransactionMetadata {
            start_version,
            end_version,
            start_transaction_timestamp: None,
            end_transaction_timestamp: None,
            total_size_in_bytes: size_in_bytes,
        },
    }
}

async fn receive_with_timeout<T>(
    receiver: &aptos_indexer_processor_sdk::instrumented_channel::InstrumentedAsyncReceiver<T>,
    timeout_ms: u64,
) -> Option<T> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        receiver.recv().await
    })
    .await
    .ok()
    .and_then(|r| r.ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_passthrough_when_channel_not_full() {
    let step: AccumulatorStep<TestBatch> = AccumulatorStep::new(Some(10000));

    let (input_sender, input_receiver) = instrumented_bounded_channel("input", 10);
    let (output_receiver, _handle) = step.spawn(Some(input_receiver), 10, None);

    let batch1 = make_test_context(vec![1, 2, 3], 0, 2, 100);
    input_sender.send(batch1.clone()).await.unwrap();

    let received = receive_with_timeout(&output_receiver, 1000).await;
    assert!(received.is_some(), "Should receive batch");
    let received = received.unwrap();
    assert_eq!(received.data.items, vec![1, 2, 3]);
    assert_eq!(received.metadata.start_version, 0);
    assert_eq!(received.metadata.end_version, 2);

    let batch2 = make_test_context(vec![4, 5], 3, 4, 100);
    input_sender.send(batch2).await.unwrap();

    let received2 = receive_with_timeout(&output_receiver, 1000).await;
    assert!(received2.is_some(), "Should receive second batch");
    assert_eq!(received2.unwrap().data.items, vec![4, 5]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_backpressure_at_max_buffer_size() {
    let step: AccumulatorStep<TestBatch> = AccumulatorStep::new(Some(1));

    let (input_sender, input_receiver) = instrumented_bounded_channel("input", 10);
    // Don't clone - the step owns the receiver
    let (output_receiver, _handle) = step.spawn(Some(input_receiver), 1, None);

    // batch1: passes through to output channel (output now full)
    let batch1 = make_test_context(vec![1], 0, 0, 0);
    input_sender.send(batch1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(output_receiver.len(), 1);

    // batch2: gets accumulated with size=2 (accumulator size becomes 2, exceeds max=1)
    // This maxes out the buffer and triggers backpressure for the next accumulate call
    let batch2 = make_test_context(vec![2], 1, 1, 2);
    input_sender.send(batch2).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // batch3: pulled from input, then accumulate() blocks because accumulator_size (2) >= max (1)
    let batch3 = make_test_context(vec![3], 2, 2, 1);
    input_sender.send(batch3).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // batch4: should stay in input channel because the step is blocked in accumulate()
    let batch4 = make_test_context(vec![4], 3, 3, 1);
    input_sender.send(batch4).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // batch4 should remain in input channel (backpressure in effect)
    // The step is blocked waiting for flush_notify, so it cannot pull batch4 from input
    assert_eq!(input_sender.len(), 1,);

    // Output channel still has just the first batch (we never consumed it)
    assert_eq!(output_receiver.len(), 1,);
    assert_eq!(output_receiver.recv().await.unwrap().data.items, vec![1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_accumulation_when_output_full() {
    let step: AccumulatorStep<TestBatch> = AccumulatorStep::new(None);
    let (input_sender, input_receiver) = instrumented_bounded_channel("input", 10);
    let (output_receiver, _handle) = step.spawn(Some(input_receiver), 1, None);

    // batch1: passes through to output channel (output now full)
    let batch1 = make_test_context(vec![1], 0, 0, 100);
    input_sender.send(batch1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Output channel should have batch1
    assert_eq!(output_receiver.len(), 1,);

    // batch2, batch3, batch4: all get accumulated (output channel is full)
    let batch2 = make_test_context(vec![2], 1, 1, 100);
    let batch3 = make_test_context(vec![3], 2, 2, 100);
    let batch4 = make_test_context(vec![4], 3, 3, 100);
    input_sender.send(batch2).await.unwrap();
    input_sender.send(batch3).await.unwrap();
    input_sender.send(batch4).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Output channel should still only have batch1
    assert_eq!(output_receiver.len(), 1,);

    // Consume batch1 from output - this makes output channel not full
    let received1 = output_receiver.recv().await.unwrap();
    assert_eq!(received1.data.items, vec![1],);
    assert_eq!(received1.metadata.start_version, 0);
    assert_eq!(received1.metadata.end_version, 0);
    assert_eq!(received1.metadata.total_size_in_bytes, 100);

    // TODO: add loop to cehck if should flush so doesn't have to be triggered by new input
    //////////////////////////////!@rierwjfoidsajfkjsadkl;fjasd;ofkjdsakl;fjadsl;kjfklas
    //////////////////////////////!@rierwjfoidsajfkjsadkl;fjasd;ofkjdsakl;fjadsl;kjfklas
    //////////////////////////////!@rierwjfoidsajfkjsadkl;fjasd;ofkjdsakl;fjadsl;kjfklas
    //////////////////////////////!@rierwjfoidsajfkjsadkl;fjasd;ofkjdsakl;fjadsl;kjfklas
    //////////////////////////////!@rierwjfoidsajfkjsadkl;fjasd;ofkjdsakl;fjadsl;kjfklas
    //////////////////////////////!@rierwjfoidsajfkjsadkl;fjasd;ofkjdsakl;fjadsl;kjfklas
    //////////////////////////////!@rierwjfoidsajfkjsadkl;fjasd;ofkjdsakl;fjadsl;kjfklas
    //////////////////////////////!@rierwjfoidsajfkjsadkl;fjasd;ofkjdsakl;fjadsl;kjfklas

    // The step only flushes accumulated data when it receives a new input and finds
    // the output channel is not full. Send batch5 to trigger the flush.
    let batch5 = make_test_context(vec![5], 4, 4, 100);
    input_sender.send(batch5).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now the accumulated batch (2+3+4+5) should be flushed to output
    assert_eq!(
        output_receiver.len(),
        1,
        "Output channel should have the accumulated batch"
    );

    // Consume the accumulated batch and verify it contains batch2 + batch3 + batch4 + batch5
    let received2 = output_receiver.recv().await.unwrap();
    assert_eq!(
        received2.data.items,
        vec![2, 3, 4, 5],
        "Accumulated batch should contain items from batch2, batch3, batch4, batch5"
    );
    // Metadata should span from batch2's start to batch5's end
    assert_eq!(received2.metadata.start_version, 1);
    assert_eq!(received2.metadata.end_version, 4);
    // Total size should be sum of all accumulated batches
    assert_eq!(received2.metadata.total_size_in_bytes, 400);
}

use aptos_indexer_processor_sdk::{
    common_steps::{Accumulatable, AccumulatorStep, PollableAccumulatorStep},
    instrumented_channel::instrumented_bounded_channel,
    traits::RunnableStep,
    types::transaction_context::{TransactionContext, TransactionMetadata},
};
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Default)]
struct TestBatch {
    items: Vec<u64>,
}

impl Accumulatable for TestBatch {
    fn accumulate(&mut self, other: Self) {
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
    let step = AccumulatorStep::new(10000);

    let (input_sender, input_receiver) = instrumented_bounded_channel("input", 10);
    let (output_receiver, _handle) =
        RunnableStep::<TestBatch, TestBatch>::spawn(step, Some(input_receiver), 10, None);

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
async fn test_multiple_accumulation_cycles() {
    // Test that the step correctly handles multiple accumulation and flush cycles
    let step = AccumulatorStep::new(10000);

    let (input_sender, input_receiver) = instrumented_bounded_channel("input", 10);
    let (output_receiver, _handle) =
        RunnableStep::<TestBatch, TestBatch>::spawn(step, Some(input_receiver), 1, None);

    // batch1: passes through to output channel (output now full)
    let batch1 = make_test_context(vec![1], 0, 0, 100);
    input_sender.send(batch1).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(output_receiver.len(), 1);

    // batch2, batch3: get accumulated while output is full
    let batch2 = make_test_context(vec![2], 1, 1, 100);
    let batch3 = make_test_context(vec![3], 2, 2, 100);
    input_sender.send(batch2).await.unwrap();
    input_sender.send(batch3).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Output channel should still only have batch1
    assert_eq!(output_receiver.len(), 1);

    // Consume batch1 to make space
    let received1 = output_receiver.recv().await.unwrap();
    assert_eq!(received1.data.items, vec![1]);

    // Wait for polling loop to detect output has space and flush
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Accumulated batch2+batch3 should be flushed
    assert_eq!(output_receiver.len(), 1);
    let received2 = output_receiver.recv().await.unwrap();
    assert_eq!(received2.data.items, vec![2, 3]);
    assert_eq!(received2.metadata.start_version, 1);
    assert_eq!(received2.metadata.end_version, 2);
    assert_eq!(received2.metadata.total_size_in_bytes, 200);

    // After consuming, output is empty again. Send batch4 - it will pass through
    // because output is not full. Then send batch5, batch6 which get accumulated.
    let batch4 = make_test_context(vec![4], 3, 3, 100);
    input_sender.send(batch4).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // batch4 should pass through (output was not full)
    assert_eq!(output_receiver.len(), 1);

    // Now output is full again with batch4. Send batch5, batch6 to accumulate.
    let batch5 = make_test_context(vec![5], 4, 4, 100);
    let batch6 = make_test_context(vec![6], 5, 5, 100);
    input_sender.send(batch5).await.unwrap();
    input_sender.send(batch6).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Output should still have just batch4
    assert_eq!(output_receiver.len(), 1);

    // Consume batch4
    let received3 = output_receiver.recv().await.unwrap();
    assert_eq!(received3.data.items, vec![4]);

    // Wait for polling loop to flush accumulated batch5+batch6
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Consume accumulated batch5+batch6
    let received4 = output_receiver.recv().await.unwrap();
    assert_eq!(received4.data.items, vec![5, 6]);
    assert_eq!(received4.metadata.start_version, 4);
    assert_eq!(received4.metadata.end_version, 5);
    assert_eq!(received4.metadata.total_size_in_bytes, 200);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_accumulation_when_output_full() {
    // Use a large buffer size so we don't hit backpressure during this test
    let step = AccumulatorStep::new(10000);
    let (input_sender, input_receiver) = instrumented_bounded_channel("input", 10);
    let (output_receiver, _handle) =
        RunnableStep::<TestBatch, TestBatch>::spawn(step, Some(input_receiver), 1, None);

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

    tokio::time::sleep(Duration::from_millis(200)).await;

    // The accumulated batch (2+3+4) should be automatically flushed to output
    assert_eq!(output_receiver.len(), 1,);

    // Consume the accumulated batch and verify it contains batch2 + batch3 + batch4
    let received2 = output_receiver.recv().await.unwrap();
    assert_eq!(received2.data.items, vec![2, 3, 4],);
    // Metadata should span from batch2's start to batch4's end
    assert_eq!(received2.metadata.start_version, 1);
    assert_eq!(received2.metadata.end_version, 3);
    // Total size should be sum of all accumulated batches
    assert_eq!(received2.metadata.total_size_in_bytes, 300);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_pollable_accumulator_flushes_on_timeout() {
    let step = PollableAccumulatorStep::new(100, 1_000_000);
    let (input_sender, input_receiver) = instrumented_bounded_channel("test_input", 10);

    let (output_receiver, _handle) =
        RunnableStep::<TestBatch, TestBatch>::spawn(step, Some(input_receiver), 10, None);

    // Send two items
    input_sender
        .send(make_test_context(vec![1, 2], 0, 1, 50))
        .await
        .unwrap();
    input_sender
        .send(make_test_context(vec![3, 4], 2, 3, 50))
        .await
        .unwrap();

    // Test to see accumulator does not flush before interval
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(output_receiver.len(), 0);

    // Wait for flush (timeout + some buffer)
    let result = tokio::time::timeout(Duration::from_millis(100), output_receiver.recv()).await;
    assert!(result.is_ok(), "Should receive flushed output");

    let output = result.unwrap().unwrap();
    assert_eq!(output.data.items, vec![1, 2, 3, 4]);
    assert_eq!(output.metadata.start_version, 0);
    assert_eq!(output.metadata.end_version, 3);
    assert_eq!(output.metadata.total_size_in_bytes, 100);
}

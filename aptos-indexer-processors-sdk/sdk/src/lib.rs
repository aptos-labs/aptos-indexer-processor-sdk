pub mod builder;
pub mod common_steps; // TODO: Feature gate this?
pub mod test;
pub mod traits;
pub mod types;
pub mod utils;

// Re-exporting crates to provide a cohesive SDK interface
pub use aptos_indexer_transaction_stream;
pub use aptos_protos;
pub use bcs;
pub use instrumented_channel;

#[cfg(test)]
mod tests {
    use crate::{
        builder::ProcessorBuilder,
        common_steps::{
            timed_size_buffer_step::{BufferHandler, TableConfig, TimedSizeBufferStep},
            TimedBufferStep,
        },
        test::{steps::pass_through_step::PassThroughStep, utils::receive_with_timeout},
        traits::{
            AsyncStep, IntoRunnableStep, NamedStep, Processable, RunnableAsyncStep,
            RunnableStepWithInputReceiver,
        },
        types::transaction_context::{TransactionContext, TransactionMetadata},
        utils::errors::ProcessorError,
    };
    use allocative_derive::Allocative;
    use anyhow::Result;
    use async_trait::async_trait;
    use google_cloud_storage::client::{Client as GCSClient, ClientConfig};
    use instrumented_channel::instrumented_bounded_channel;
    use parquet_derive::ParquetRecordWriter;
    use serde::{Deserialize, Serialize};
    use std::{sync::Arc, time::Duration};

    #[derive(Clone, Debug, PartialEq)]
    pub struct TestStruct {
        pub i: usize,
    }

    fn make_test_structs(num: usize) -> Vec<TestStruct> {
        (1..(num + 1)).map(|i| TestStruct { i }).collect()
    }

    pub struct TestStep;

    impl AsyncStep for TestStep {}

    impl NamedStep for TestStep {
        fn name(&self) -> String {
            "TestStep".to_string()
        }
    }

    #[async_trait]
    impl Processable for TestStep {
        type Input = Vec<usize>;
        type Output = Vec<TestStruct>;
        type RunType = ();

        async fn process(
            &mut self,
            item: TransactionContext<Vec<usize>>,
        ) -> Result<Option<TransactionContext<Vec<TestStruct>>>, ProcessorError> {
            let processed = item.data.into_iter().map(|i| TestStruct { i }).collect();
            Ok(Some(TransactionContext {
                data: processed,
                metadata: item.metadata,
            }))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::needless_return)]
    async fn test_connect_two_steps() {
        let (input_sender, input_receiver) = instrumented_bounded_channel("input", 1);

        let input_step = RunnableStepWithInputReceiver::new(
            input_receiver,
            RunnableAsyncStep::new(PassThroughStep::default()),
        );

        // Create a timed buffer that outputs the input after 1 second
        let timed_buffer_step = TimedBufferStep::<Vec<usize>>::new(Duration::from_millis(200));
        let first_step = timed_buffer_step;

        let second_step = TestStep;
        let second_step = RunnableAsyncStep::new(second_step);

        let builder = ProcessorBuilder::new_with_runnable_input_receiver_first_step(input_step)
            .connect_to(first_step.into_runnable_step(), 5)
            .connect_to(second_step, 3);

        let mut fanout_builder = builder.fanout_broadcast(2);
        let (_, first_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(RunnableAsyncStep::new(PassThroughStep::default()), 1)
            .end_and_return_output_receiver(1);

        let (second_builder, second_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(
                RunnableAsyncStep::new(PassThroughStep::new_named("MaxStep".to_string())),
                2,
            )
            .connect_to(RunnableAsyncStep::new(PassThroughStep::default()), 5)
            .end_and_return_output_receiver(5);

        let mut output_receivers = [first_output_receiver, second_output_receiver];

        output_receivers.iter().for_each(|output_receiver| {
            assert_eq!(output_receiver.len(), 0, "Output should be empty");
        });

        let left_input = TransactionContext {
            data: vec![1, 2, 3],
            metadata: TransactionMetadata {
                start_version: 0,
                end_version: 1,
                start_transaction_timestamp: None,
                end_transaction_timestamp: None,
                total_size_in_bytes: 0,
            },
        };
        input_sender.send(left_input.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;

        output_receivers.iter().for_each(|output_receiver| {
            assert_eq!(output_receiver.len(), 1, "Output should have 1 item");
        });

        for output_receiver in output_receivers.iter_mut() {
            let result = receive_with_timeout(output_receiver, 100).await.unwrap();

            assert_eq!(
                result.data,
                make_test_structs(3),
                "Output should be the same as input"
            );
        }

        let graph = second_builder.graph;
        let dot = graph.dot();
        println!("{:}", dot);
        // first_handle.abort();
        // second_handle.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::needless_return)]
    async fn test_fanin() {
        let (input_sender, input_receiver) = instrumented_bounded_channel("input", 1);

        let input_step = RunnableStepWithInputReceiver::new(
            input_receiver,
            RunnableAsyncStep::new(PassThroughStep::default()),
        );

        let mut fanout_builder =
            ProcessorBuilder::new_with_runnable_input_receiver_first_step(input_step)
                .fanout_broadcast(2);

        let (first_builder, first_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(
                RunnableAsyncStep::new(PassThroughStep::new_named("FanoutStep1".to_string())),
                5,
            )
            .end_and_return_output_receiver(5);

        let (second_builder, second_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(
                RunnableAsyncStep::new(PassThroughStep::new_named("FanoutStep2".to_string())),
                5,
            )
            .end_and_return_output_receiver(5);

        let test_step = TestStep;
        let test_step = RunnableAsyncStep::new(test_step);

        let (_, mut fanin_output_receiver) = ProcessorBuilder::new_with_fanin_step_with_receivers(
            vec![
                (first_output_receiver, first_builder.graph),
                (second_output_receiver, second_builder.graph),
            ],
            RunnableAsyncStep::new(PassThroughStep::new_named("FaninStep".to_string())),
            3,
        )
        .connect_to(test_step, 10)
        .end_and_return_output_receiver(6);

        assert_eq!(fanin_output_receiver.len(), 0, "Output should be empty");

        let left_input = TransactionContext {
            data: vec![1, 2, 3],
            metadata: TransactionMetadata {
                start_version: 0,
                end_version: 1,
                start_transaction_timestamp: None,
                end_transaction_timestamp: None,
                total_size_in_bytes: 0,
            },
        };
        input_sender.send(left_input.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;

        assert_eq!(fanin_output_receiver.len(), 2, "Output should have 2 items");

        for _ in 0..2 {
            let result = receive_with_timeout(&mut fanin_output_receiver, 100)
                .await
                .unwrap();

            assert_eq!(
                result.data,
                make_test_structs(3),
                "Output should be the same as input"
            );
        }

        let graph = fanout_builder.graph;
        let dot = graph.dot();
        println!("{:}", dot);
        // first_handle.abort();
        // second_handle.abort();
    }

    use crate::traits::parquet_extract_trait::{
        ExtractResources, GetTimeStamp, HasVersion, NamedTable,
    };

    // Mock ParquetBufferHandler to simulate the buffer handling.
    pub struct ParquetBufferHandler {}
    #[async_trait]
    impl<P> BufferHandler<P> for ParquetBufferHandler
    where
        P: Send + Sync + 'static,
    {
        async fn handle_buffer(
            &mut self,
            _gcs_client: &GCSClient,
            buffer: Vec<P>,
            _metadata: &mut TransactionMetadata,
        ) -> Result<(), ProcessorError> {
            println!("Handling buffer with {} items", buffer.len());
            Ok(())
        }
    }
    use super::*;
    use tokio::sync::Mutex;

    // Define a mock implementation of ParquetType for testing.
    #[derive(Allocative, Clone, Debug, Default, Deserialize, ParquetRecordWriter, Serialize)]
    struct MockParquetType;
    impl NamedTable for MockParquetType {
        const TABLE_NAME: &'static str = "MockParquetType";
    }
    impl GetTimeStamp for MockParquetType {
        fn get_timestamp(&self) -> chrono::NaiveDateTime {
            chrono::Utc::now().naive_utc()
        }
    }
    impl HasVersion for MockParquetType {
        fn version(&self) -> i64 {
            1
        }
    }

    // Mock BufferHandler that keeps track of the handle_buffer calls.
    struct MockBufferHandler {
        call_count: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl BufferHandler<MockParquetType> for MockBufferHandler {
        async fn handle_buffer(
            &mut self,
            _gcs_client: &GCSClient,
            _buffer: Vec<MockParquetType>,
            _metadata: &mut TransactionMetadata,
        ) -> Result<(), ProcessorError> {
            let mut count = self.call_count.lock().await;
            *count += 1;
            Ok(())
        }
    }
    impl ExtractResources<MockParquetType> for MockParquetType {
        fn extract(self) -> Vec<MockParquetType> {
            vec![self.clone()]
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[allow(clippy::needless_return)]
    async fn test_size_buffer() {
        // Step 1: Create input sender and receiver channels.
        let (input_sender, input_receiver) = instrumented_bounded_channel("input", 1);
        let call_count = Arc::new(Mutex::new(0));
        let buffer_handler = MockBufferHandler {
            call_count: call_count.clone(),
        };
        let config = ClientConfig::default().with_auth().await.unwrap();

        // Step 2: Initialize the TimedSizeBufferStep with mocked components.
        let time_size_buffer_step =
            TimedSizeBufferStep::<MockParquetType, MockParquetType, MockBufferHandler>::new(
                Duration::from_millis(200),
                TableConfig {
                    table_name: "test_table".to_string(),
                    bucket_name: "test_bucket".to_string(),
                    bucket_root: "test_root".to_string(),
                    max_size: 1024, // Set a small buffer size for testing.
                },
                Arc::new(GCSClient::new(config)), // Mocked or default GCS client
                "test_processor",
                buffer_handler,
            )
            .into_runnable_step();

        // Step 3: Create a processor builder with fan-out and fan-in steps.
        let input_step = RunnableStepWithInputReceiver::new(input_receiver, time_size_buffer_step);
        let mut fanout_builder =
            ProcessorBuilder::new_with_runnable_input_receiver_first_step(input_step)
                .fanout_broadcast(2);

        // Step 4: Connect fan-out steps and gather their outputs.
        let (first_builder, first_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(
                RunnableAsyncStep::new(PassThroughStep::new_named("FanoutStep1".to_string())),
                5,
            )
            .end_and_return_output_receiver(5);

        let (second_builder, second_output_receiver) = fanout_builder
            .get_processor_builder()
            .unwrap()
            .connect_to(
                RunnableAsyncStep::new(PassThroughStep::new_named("FanoutStep2".to_string())),
                5,
            )
            .end_and_return_output_receiver(5);

        let (_, mut fanin_output_receiver) = ProcessorBuilder::new_with_fanin_step_with_receivers(
            vec![
                (first_output_receiver, first_builder.graph),
                (second_output_receiver, second_builder.graph),
            ],
            RunnableAsyncStep::new(PassThroughStep::new_named("FaninStep".to_string())),
            3,
        )
        .end_and_return_output_receiver(6);

        // Step 6: Verify initial state of fan-in output.
        assert_eq!(fanin_output_receiver.len(), 0, "Output should be empty");

        // Step 7: Send input data and validate fan-in processing.
        let left_input = TransactionContext {
            data: MockParquetType {},
            metadata: TransactionMetadata {
                start_version: 0,
                end_version: 1,
                start_transaction_timestamp: None,
                end_transaction_timestamp: None,
                total_size_in_bytes: 0,
            },
        };
        input_sender.send(left_input.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;

        assert_eq!(fanin_output_receiver.len(), 2, "Output should have 2 items");

        for _ in 0..2 {
            let result = receive_with_timeout(&mut fanin_output_receiver, 100)
                .await
                .unwrap();

            println!("Received unit value as expected: {:?}", result.data);
        }

        let graph = fanout_builder.graph;
        let dot = graph.dot();
        println!("{:}", dot);
    }
}

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
        common_steps::TimedBufferStep,
        test::{steps::pass_through_step::PassThroughStep, utils::receive_with_timeout},
        traits::{
            AsyncStep, IntoRunnableStep, NamedStep, Processable, RunnableAsyncStep,
            RunnableStepWithInputReceiver,
        },
        types::transaction_context::TransactionContext,
        utils::errors::ProcessorError,
    };
    use anyhow::Result;
    use async_trait::async_trait;
    use instrumented_channel::instrumented_bounded_channel;
    use std::time::Duration;

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
        type Input = usize;
        type Output = TestStruct;
        type RunType = ();

        async fn process(
            &mut self,
            item: TransactionContext<usize>,
        ) -> Result<Option<TransactionContext<TestStruct>>, ProcessorError> {
            let processed = item.data.into_iter().map(|i| TestStruct { i }).collect();
            Ok(Some(TransactionContext {
                data: processed,
                start_version: item.start_version,
                end_version: item.end_version,
                start_transaction_timestamp: item.start_transaction_timestamp,
                end_transaction_timestamp: item.end_transaction_timestamp,
                total_size_in_bytes: item.total_size_in_bytes,
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
        let timed_buffer_step = TimedBufferStep::<usize>::new(Duration::from_millis(200));
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
            start_version: 0,
            end_version: 1,
            start_transaction_timestamp: None,
            end_transaction_timestamp: None,
            total_size_in_bytes: 0,
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
        //first_handle.abort();
        //second_handle.abort();
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
            start_version: 0,
            end_version: 1,
            start_transaction_timestamp: None,
            end_transaction_timestamp: None,
            total_size_in_bytes: 0,
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
        //first_handle.abort();
        //second_handle.abort();
    }
}

use anyhow::Result;
use aptos_indexer_transaction_stream::config::TransactionStreamConfig;
use sdk::{
    builder::ProcessorBuilder,
    steps::{TimedBuffer, TransactionStreamStep},
    traits::IntoRunnableStep,
};
use std::time::Duration;
use url::Url;

const RUNTIME_WORKER_MULTIPLIER: usize = 2;

fn main() {
    let num_cpus = num_cpus::get();
    let worker_threads = (num_cpus * RUNTIME_WORKER_MULTIPLIER).max(16);

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .disable_lifo_slot()
        .enable_all()
        .worker_threads(worker_threads)
        .build()
        .unwrap()
        .block_on(async {
            // TODO: actually launch something here
            run_processor().await.unwrap();
        })
}

async fn run_processor() -> Result<()> {
    // let (input_sender, input_receiver) = kanal::bounded_async(1);
    let transaction_stream_config = TransactionStreamConfig {
        indexer_grpc_data_service_address: Url::parse("https://grpc.devnet.aptoslabs.com:443")?,
        starting_version: 0,
        request_ending_version: None,
        auth_token: String::from("aptoslabs_TJs4NQU8Xf5_EJMNnZFPXRH6YNpWM7bCcurMBEUtZtRb6"),
        request_name_header: String::from("sdk_processor"),
        indexer_grpc_http2_ping_interval_secs: 30,
        indexer_grpc_http2_ping_timeout_secs: 10,
        indexer_grpc_reconnection_timeout_secs: 5,
        indexer_grpc_response_item_timeout_secs: 60,
    };

    let transaction_stream = TransactionStreamStep::new(transaction_stream_config).await?;

    // let transaction_stream_with_input =
    //     RunnableStepWithInputReceiver::new(input_receiver, transaction_stream.into_runnable_step());

    let timed_buffer = TimedBuffer::new(Duration::from_secs(1));

    let (_, buffer_receiver) =
        ProcessorBuilder::new_with_inputless_first_step(transaction_stream.into_runnable_step())
            .connect_to(timed_buffer.into_runnable_step(), 10)
            .end_and_return_output_receiver(10);

    loop {
        match buffer_receiver.recv().await {
            Ok(txn_context) => {
                if txn_context.data.is_empty() {
                    println!("Received no transactions");
                    continue;
                }
                println!(
                    "Received transactions: {:?} to {:?}",
                    txn_context.start_version, txn_context.end_version,
                );
            },
            Err(e) => {
                println!("Error receiving transactions: {:?}", e);
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use instrumented_channel::instrumented_bounded_channel;
    use sdk::{
        builder::ProcessorBuilder,
        steps::{AsyncStep, RunnableAsyncStep, TimedBuffer},
        test::{steps::pass_through_step::PassThroughStep, utils::receive_with_timeout},
        traits::{IntoRunnableStep, NamedStep, Processable, RunnableStepWithInputReceiver},
        types::transaction_context::TransactionContext,
    };
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
        ) -> TransactionContext<TestStruct> {
            let processed = item.data.into_iter().map(|i| TestStruct { i }).collect();
            TransactionContext {
                data: processed,
                start_version: item.start_version,
                end_version: item.end_version,
                start_transaction_timestamp: item.start_transaction_timestamp,
                end_transaction_timestamp: item.end_transaction_timestamp,
                total_size_in_bytes: item.total_size_in_bytes,
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connect_two_steps() {
        let (input_sender, input_receiver) = instrumented_bounded_channel("input", 1);

        let input_step = RunnableStepWithInputReceiver::new(
            input_receiver,
            RunnableAsyncStep::new(PassThroughStep::default()),
        );

        // Create a timed buffer that outputs the input after 1 second
        let timed_buffer_step = TimedBuffer::<usize>::new(Duration::from_millis(200));
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
    async fn test_fanin() {
        let (input_sender, input_receiver) = instrumented_bounded_channel("channel_name", 1);

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

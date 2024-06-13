use anyhow::Result;
use sdk::{
    builder::ProcessorBuilder,
    steps::{TimedBuffer, TransactionStreamStep},
    traits::{IntoRunnableStep, RunnableStepWithInputReceiver},
};
use std::time::Duration;
use transaction_filter::transaction_filter::TransactionFilter;
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
    let (input_sender, input_receiver) = kanal::bounded_async(1);

    let transaction_stream = TransactionStreamStep::new(
        Url::parse("https://grpc.devnet.aptoslabs.com:443").unwrap(),
        Duration::from_secs(30),
        Duration::from_secs(10),
        Duration::from_secs(5),
        Duration::from_secs(60),
        0,
        None,
        String::from("aptoslabs_TJs4NQU8Xf5_EJMNnZFPXRH6YNpWM7bCcurMBEUtZtRb6"),
        String::from("sdk_processor"),
        TransactionFilter::default(),
        100_000,
    );

    let transaction_stream_with_input =
        RunnableStepWithInputReceiver::new(input_receiver, transaction_stream.into_runnable_step());

    let timed_buffer = TimedBuffer::new(Duration::from_secs(1));

    let (processor_builder, buffer_receiver) =
        ProcessorBuilder::new_with_runnable_input_receiver_first_step(
            transaction_stream_with_input,
        )
        .end_with_and_return_output_receiver(timed_buffer.into_runnable_step(), 10);

    loop {
        match buffer_receiver.recv().await {
            Ok(txn_pb) => {
                if txn_pb.len() == 0 {
                    println!("Received no transactions");
                    continue;
                }
                println!(
                    "Received transactions: {:?} to {:?}",
                    txn_pb
                        .first()
                        .unwrap()
                        .transactions
                        .first()
                        .unwrap()
                        .version,
                    txn_pb.last().unwrap().transactions.last().unwrap().version
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
    use sdk::{
        builder::ProcessorBuilder,
        steps::{AsyncStep, RunnableAsyncStep, TimedBuffer},
        test::{steps::pass_through_step::PassThroughStep, utils::receive_with_timeout},
        traits::{IntoRunnableStep, NamedStep, Processable, RunnableStepWithInputReceiver},
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

        async fn process(&mut self, item: Vec<usize>) -> Vec<TestStruct> {
            item.into_iter().map(|i| TestStruct { i }).collect()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_connect_two_steps() {
        let (input_sender, input_receiver) = kanal::bounded_async(1);

        let input_step = RunnableStepWithInputReceiver::new(
            input_receiver,
            RunnableAsyncStep::new(PassThroughStep::new()),
        );

        // Create a timed buffer that outputs the input after 1 second
        let timed_buffer_step = TimedBuffer::<usize>::new(Duration::from_millis(200));
        let first_step = timed_buffer_step;

        let second_step = TestStep;
        let second_step = RunnableAsyncStep::new(second_step);

        let (builder, mut output_receiver) =
            ProcessorBuilder::new_with_runnable_input_receiver_first_step(input_step)
                .connect_to(first_step.into_runnable_step(), 5)
                .connect_to(second_step, 3)
                .end_with_and_return_output_receiver(
                    RunnableAsyncStep::new(PassThroughStep::new()),
                    1,
                );

        assert_eq!(output_receiver.len(), 0, "Output should be empty");

        let left_input = vec![1, 2, 3];
        input_sender.send(left_input.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;

        assert_eq!(output_receiver.len(), 1, "Output should have 1 item");

        let result = receive_with_timeout(&mut output_receiver, 100)
            .await
            .unwrap();

        assert_eq!(
            result,
            make_test_structs(3),
            "Output should be the same as input"
        );

        let graph = builder.graph;
        let dot = graph.dot();
        println!("{:}", dot);
        //first_handle.abort();
        //second_handle.abort();
    }
}

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
        })
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use kanal::AsyncReceiver;
    use sdk::{
        builder::ProcessorBuilder,
        steps::{AsyncStep, RunnableAsyncStep, TimedBuffer},
        traits::{
            processable::RunnableStepType, IntoRunnableStep, NamedStep, Processable,
            RunnableStepWithInputReceiver,
        },
    };
    use std::{marker::PhantomData, time::Duration};

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

    pub struct PassThroughStep<Input: Send + 'static> {
        name: Option<String>,
        _input: PhantomData<Input>,
    }

    impl<Input: Send + 'static> PassThroughStep<Input> {
        pub fn new() -> Self {
            Self {
                name: None,
                _input: PhantomData,
            }
        }

        pub fn new_named(name: String) -> Self {
            Self {
                name: Some(name),
                _input: PhantomData,
            }
        }
    }

    impl<Input: Send + 'static> AsyncStep for PassThroughStep<Input> {}

    impl<Input: Send + 'static> NamedStep for PassThroughStep<Input> {
        fn name(&self) -> String {
            self.name
                .clone()
                .unwrap_or_else(|| "PassThroughStep".to_string())
        }
    }

    pub struct PassThroughStepType;

    impl RunnableStepType for PassThroughStepType {}

    #[async_trait]
    impl<Input: Send + 'static> Processable for PassThroughStep<Input> {
        type Input = Input;
        type Output = Input;
        type RunType = PassThroughStepType;

        async fn process(&mut self, item: Vec<Input>) -> Vec<Input> {
            item
        }
    }

    async fn receive_with_timeout<T>(
        receiver: &mut AsyncReceiver<T>,
        timeout_ms: u64,
    ) -> Option<T> {
        tokio::time::timeout(Duration::from_millis(timeout_ms), async {
            receiver.recv().await
        })
            .await
            .unwrap()
            .ok()
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

        let builder = ProcessorBuilder::new_with_runnable_input_receiver_first_step(input_step)
            .connect_to(first_step.into_runnable_step(), 5)
            .connect_to(second_step, 3);

        let mut builders = builder.fanout_broadcast(2);
        let (first_builder, first_output_receiver) = builders.pop().unwrap().end_with_and_return_output_receiver(
            RunnableAsyncStep::new(PassThroughStep::new()),
            1,
        );

        let (second_builder, second_output_receiver) = builders.pop().unwrap().connect_to(
            RunnableAsyncStep::new(PassThroughStep::new_named("MaxStep".to_string())),
            2,
        ).end_with_and_return_output_receiver(
            RunnableAsyncStep::new(PassThroughStep::new()),
            5,
        );

        let mut output_receivers = [first_output_receiver, second_output_receiver];

        output_receivers.iter().for_each(|output_receiver| {
            assert_eq!(output_receiver.len(), 0, "Output should be empty");
        });

        let left_input = vec![1, 2, 3];
        input_sender.send(left_input.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;

        output_receivers.iter().for_each(|output_receiver| {
            assert_eq!(output_receiver.len(), 1, "Output should have 1 item");
        });

        for output_receiver in output_receivers.iter_mut() {
            let result = receive_with_timeout(output_receiver, 100).await.unwrap();

            assert_eq!(
                result,
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
}

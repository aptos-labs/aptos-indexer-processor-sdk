use crate::traits::{RunnableStep, RunnableStepWithInputReceiver};
use tokio::task::JoinHandle;

pub fn connect_two_steps<LeftInput, LeftOutput, RightOutput, LeftStep, RightStep>(
    left_step: RunnableStepWithInputReceiver<LeftInput, LeftOutput, LeftStep>,
    right_step: RightStep,
    channel_size: usize,
) -> (
    JoinHandle<()>,
    RunnableStepWithInputReceiver<LeftOutput, RightOutput, RightStep>,
)
where
    LeftInput: Send + 'static,
    LeftOutput: Send + 'static,
    RightOutput: Send + 'static,
    LeftStep: RunnableStep<LeftInput, LeftOutput> + Send + Sized + 'static,
    RightStep: RunnableStep<LeftOutput, RightOutput> + Send + Sized + 'static,
{
    let RunnableStepWithInputReceiver {
        input_receiver: left_input_receiver,
        step: left_step,
        ..
    } = left_step;

    let (left_output_receiver, left_handle) =
        left_step.spawn(Some(left_input_receiver), channel_size);

    let right_step_with_input_receiver =
        RunnableStepWithInputReceiver::new(left_output_receiver, right_step);

    (left_handle, right_step_with_input_receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        steps::{AsyncStep, RunnableAsyncStep, RunnablePollableStep, TimedBuffer},
        traits::{NamedStep, Processable},
    };
    use async_trait::async_trait;
    use kanal::AsyncReceiver;
    use std::time::Duration;

    #[derive(Clone, Debug, PartialEq)]
    pub struct TestStruct {
        pub i: usize,
    }

    fn make_test_structs(num: usize) -> Vec<TestStruct> {
        (1..(num+1)).map(|i| TestStruct { i }).collect()
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

        async fn process(&mut self, item: Vec<usize>) -> Vec<TestStruct> {
            item.into_iter().map(|i| TestStruct { i }).collect()
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

        // Create a timed buffer that outputs the input after 1 second
        let timed_buffer_step = TimedBuffer::<usize>::new(Duration::from_millis(200));
        let first_step = RunnablePollableStep::new(timed_buffer_step);
        let first_step = first_step.add_input_receiver(input_receiver);

        let second_step = TestStep;
        let second_step = RunnableAsyncStep::new(second_step);

        let (first_handle, second_step) = connect_two_steps(first_step, second_step, 1);

        let (mut output_receiver, second_handle) = second_step.spawn(None, 1);

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
        first_handle.abort();
        second_handle.abort();
    }
}

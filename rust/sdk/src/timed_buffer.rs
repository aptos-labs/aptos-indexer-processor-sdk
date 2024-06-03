use crate::traits::{
    async_step::{AsyncStep, PollableAsyncStep, SpawnsPollable},
    instrumentation::NamedStep,
};
use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};
use std::time::Duration;

pub struct TimedBuffer<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + 'static,
{
    pub input_receiver: AsyncReceiver<Vec<Input>>,
    pub output_sender: AsyncSender<Vec<Input>>,
    pub internal_buffer: Vec<Input>,
    pub poll_interval: Duration,
}

impl<Input> TimedBuffer<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + 'static,
{
    #[allow(dead_code)]
    pub fn new(
        input_receiver: AsyncReceiver<Vec<Input>>,
        output_sender: AsyncSender<Vec<Input>>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            input_receiver,
            output_sender,
            internal_buffer: Vec::new(),
            poll_interval,
        }
    }
}

#[async_trait]
impl<Input> AsyncStep for TimedBuffer<Input>
where
    Input: Send + 'static,
{
    type Input = Input;
    type Output = Input;

    async fn process(&mut self, item: Vec<Input>) -> Vec<Input> {
        self.internal_buffer.extend(item);
        Vec::new() // No immediate output
    }

    fn input_receiver(&mut self) -> &AsyncReceiver<Vec<Input>> {
        &self.input_receiver
    }

    fn output_sender(&mut self) -> &AsyncSender<Vec<Input>> {
        &self.output_sender
    }
}

#[async_trait]
impl<Input: Send + 'static> PollableAsyncStep for TimedBuffer<Input> {
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(&mut self) -> Option<Vec<Input>> {
        Some(std::mem::take(&mut self.internal_buffer))
    }
}

impl<Input: Send + 'static> NamedStep for TimedBuffer<Input> {
    // TODO: oncecell this somehow? Likely in wrapper struct...
    fn name(&self) -> String {
        format!("TimedBuffer: {}", std::any::type_name::<Input>())
    }
}

impl<Input: Send + 'static> SpawnsPollable for TimedBuffer<Input> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    pub struct TestStruct {
        pub i: usize,
    }

    fn make_test_structs(num: usize) -> Vec<TestStruct> {
        (1..num).map(|i| TestStruct { i }).collect()
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
    async fn test_timed_buffer() {
        let (input_sender, input_receiver) = kanal::bounded_async::<Vec<TestStruct>>(10);

        let (output_sender, mut output_receiver) = kanal::bounded_async::<Vec<TestStruct>>(10);

        let buffer = TimedBuffer::new(input_receiver, output_sender, Duration::from_millis(100));

        let handle = buffer.spawn();

        let input1 = make_test_structs(3);
        input_sender.send(input1.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        let input2 = make_test_structs(4);
        input_sender.send(input2.clone()).await.unwrap();

        assert_eq!(output_receiver.len(), 0, "Output should be empty");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(output_receiver.len(), 1, "Output should have 1 item");

        let result = receive_with_timeout(&mut output_receiver, 100)
            .await
            .unwrap();
        let mut expected = input1.clone();
        expected.extend(input2.clone());
        assert_eq!(result, expected, "Poll should return all items");
        handle.abort();
    }
}

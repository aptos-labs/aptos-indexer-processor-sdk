use async_trait::async_trait;
use kanal::{AsyncReceiver, AsyncSender};
use std::time::{Duration};
use tokio::task::JoinHandle;


#[async_trait]
pub trait AsyncStep where Self: Sized + Send + 'static {
    type Input: Send + 'static;
    type Output: Send + 'static;

    /// Processes a batch of input items and returns a batch of output items.
    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output>;

    /// Returns the input channel for receiving input items.
    fn input_receiver(&mut self) -> &AsyncReceiver<Vec<Self::Input>>;

    /// Returns the output channel for sending output items.
    fn output_sender(&mut self) -> &AsyncSender<Vec<Self::Output>>;
}

#[async_trait]
#[allow(dead_code)]
pub trait PollableAsyncStep: where Self: Sized + Send + 'static + AsyncStep {
    /// Returns the duration between poll attempts.
    fn poll_interval(&self) -> Duration;

    /// Polls the internal state and returns a batch of output items if available.
    async fn poll(&mut self) -> Option<Vec<<Self as AsyncStep>::Output>>;
}

pub struct TimedBuffer<Input> where Self: Sized + Send + 'static, Input: Send + 'static {
    pub input_receiver: AsyncReceiver<Vec<Input>>,
    pub output_sender: AsyncSender<Vec<Input>>,
    pub internal_buffer: Vec<Input>,
    pub poll_interval: Duration,
}

impl<Input> TimedBuffer<Input> where Self: Sized + Send + 'static, Input: Send + 'static {
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
impl<Input> AsyncStep for TimedBuffer<Input> where Input: Send + 'static {
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

// TODO: Implement this for everything we can automatically?
pub trait SpawnsPollable: PollableAsyncStep {
    fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let input_receiver = self.input_receiver().clone();
            let output_sender = self.output_sender().clone();
            let poll_duration = self.poll_interval();

            let mut last_poll = tokio::time::Instant::now();

            loop {
                // It's possible that the channel always has items, so we need to ensure we call `poll` manually if we need to
                if last_poll.elapsed() >= poll_duration {
                    let result = self.poll().await;
                    if let Some(output) = result {
                        output_sender.send(output).await.expect("Failed to send output");
                    };
                    last_poll = tokio::time::Instant::now();
                }

                tokio::select! {
                    _ = tokio::time::sleep(poll_duration) => {
                        let result = self.poll().await;
                        if let Some(output) = result {
                            output_sender.send(output).await.expect("Failed to send output");
                        };
                        last_poll = tokio::time::Instant::now();
                    }
                    input = input_receiver.recv() => {
                        let input = input.expect("Failed to receive input");
                        let output = self.process(input).await;
                        if !output.is_empty() {
                            output_sender.send(output).await.expect("Failed to send output");
                        }
                    }
                }
            }
        })
    }
}

impl<Input: Send + 'static> SpawnsPollable for TimedBuffer<Input> {}

#[async_trait]
impl<Input: Send + 'static> PollableAsyncStep for TimedBuffer<Input> {
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(&mut self) -> Option<Vec<Input>> {
        Some(std::mem::take(&mut self.internal_buffer))
    }
}

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
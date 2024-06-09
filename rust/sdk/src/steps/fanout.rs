use kanal::{AsyncReceiver, AsyncSender};
use tokio::task::JoinHandle;
use crate::steps::async_step::AsyncRunType;
use crate::traits::Processable;

/// 1 to N fanout step
pub struct AsyncFanoutStep<Input>
where
    Self: Sized + Send + 'static,
    Input: Clone + Send + 'static,
{
    pub input_receiver: AsyncReceiver<Vec<Input>>,
    pub output_senders: Vec<AsyncSender<Vec<Input>>>,
}

impl<Input> AsyncFanoutStep<Input>
where
    Self: Sized + Send + 'static,
    Input: Clone + Send + 'static,
{
    pub fn new(
        input_receiver: AsyncReceiver<Vec<Input>>,
        output_senders: Vec<AsyncSender<Vec<Input>>>,
    ) -> Self {
        Self {
            input_receiver,
            output_senders,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let input = self.input_receiver.recv().await.unwrap();
                for output_sender in &self.output_senders {
                    output_sender.send(input.clone()).await.unwrap();
                }
            }
        })
    }
}

impl<Input> Processable for AsyncFanoutStep<Input> {
    type Input = Input;
    type Output = Input;
    type RunType = AsyncRunType;

    async fn process(&mut self, item: Vec<Input>) -> Vec<Vec<usize>> {
        item
    }
}

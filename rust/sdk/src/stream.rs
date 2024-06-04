use crate::traits::{
    async_step::{
        AsyncStep, AsyncStepWithOutput, PollableAsyncStep, SpawnsPollable, SpawnsPollableWithOutput,
    },
    instrumentation::NamedStep,
};
use async_trait::async_trait;
use kanal::AsyncSender;

#[derive(Copy, Clone)]
pub struct Transaction {
    pub transaction_version: u64,
}

pub struct TransactionStream
where
    Self: Sized + Send + 'static,
    Transaction: Send + 'static,
{
    pub output_sender: AsyncSender<Vec<Transaction>>,
}

impl TransactionStream
where
    Self: Sized + Send + 'static,
    Transaction: Send + 'static,
{
    pub fn new(output_sender: AsyncSender<Vec<Transaction>>) -> Self {
        Self { output_sender }
    }
}

#[async_trait]
impl AsyncStep for TransactionStream {
    type Input = ();
    type Output = Transaction;

    async fn process(&mut self, item: Vec<()>) -> Vec<Transaction> {
        // Gets the transactions from grpc
        vec![Transaction {
            transaction_version: 0,
        }]
    }
}

#[async_trait]
impl AsyncStepWithOutput for TransactionStream {
    fn output_sender(&mut self) -> &AsyncSender<Vec<Transaction>> {
        &self.output_sender
    }
}

impl NamedStep for TransactionStream {
    fn name(&self) -> String {
        "TransactionStream".to_string()
    }
}

#[async_trait]
impl PollableAsyncStep for TransactionStream {
    fn poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_secs(1)
    }

    async fn poll(&mut self) -> Option<Vec<Transaction>> {
        None
    }
}

impl SpawnsPollableWithOutput for TransactionStream {}

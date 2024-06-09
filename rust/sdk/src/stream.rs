use crate::traits::{instrumentation::NamedStep, Processable};
use crate::steps::PollableAsyncStep;
use async_trait::async_trait;
use kanal::AsyncSender;
use crate::steps::pollable_async_step::PollableAsyncRunType;

#[derive(Copy, Clone, Debug)]
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
        Some(vec![Transaction {
            transaction_version: 1,
        }])
    }
}

#[async_trait]
impl Processable for TransactionStream {
    type Input = ();
    type Output = Transaction;
    type RunType = PollableAsyncRunType;
    async fn process(&mut self, _item: Vec<()>) -> Vec<Transaction> {
        vec![Transaction {
            transaction_version: 1,
        }]
    }
}
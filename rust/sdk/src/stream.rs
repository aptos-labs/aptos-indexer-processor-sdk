use crate::traits::{async_step::InitialAsyncStep, instrumentation::NamedStep};
use async_trait::async_trait;
use kanal::AsyncSender;

#[derive(Copy, Clone)]
pub struct Transaction {
    pub transaction_version: u64,
}

pub struct GrpcStream<Transaction>
where
    Self: Sized + Send + 'static,
    Transaction: Send + 'static,
{
    pub output_sender: AsyncSender<Vec<Transaction>>,
}

impl<Transaction> GrpcStream<Transaction>
where
    Self: Sized + Send + 'static,
    Transaction: Send + 'static,
{
    pub fn new(output_sender: AsyncSender<Vec<Transaction>>) -> Self {
        Self { output_sender }
    }
}

#[async_trait]
impl InitialAsyncStep for GrpcStream<Transaction> {
    type Output = Transaction;

    async fn process(&mut self) -> Vec<Transaction> {
        vec![Transaction {
            transaction_version: 0,
        }]
    }

    fn output_sender(&mut self) -> &AsyncSender<Vec<Transaction>> {
        &self.output_sender
    }
}

impl NamedStep for GrpcStream<Transaction> {
    fn name(&self) -> String {
        "TransactionStream".to_string()
    }
}

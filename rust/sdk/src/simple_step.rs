use crate::stream::Transaction;
use crate::traits::{async_step::AsyncStep, instrumentation::NamedStep};
use async_trait::async_trait;

#[derive(Clone, Debug)]
pub struct SimpleStep;

#[async_trait]
impl AsyncStep for SimpleStep {
    type Input = Transaction;
    type Output = Transaction;

    async fn process(&mut self, item: Vec<Transaction>) -> Vec<Transaction> {
        item.into_iter()
            .map(|txn| Transaction {
                transaction_version: txn.transaction_version * 2,
            })
            .collect()
    }
}

impl NamedStep for SimpleStep {
    fn name(&self) -> String {
        "SimpleStep".to_string()
    }
}

use super::{pollable_async_step::PollableAsyncRunType, PollableAsyncStep};
use crate::{
    traits::{NamedStep, Processable},
    types::transaction_context::TransactionContext,
};
use anyhow::Result;
use aptos_indexer_transaction_stream::{
    config::TransactionStreamConfig,
    transaction_stream::TransactionStream as TransactionStreamInternal,
};
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use mockall::mock;

pub struct TransactionStreamStep
where
    Self: Sized + Send + 'static,
{
    pub transaction_stream: TransactionStreamInternal,
}

impl TransactionStreamStep
where
    Self: Sized + Send + 'static,
{
    pub async fn new(transaction_stream_config: TransactionStreamConfig) -> Result<Self> {
        let transaction_stream = TransactionStreamInternal::new(transaction_stream_config).await?;
        Ok(Self { transaction_stream })
    }
}

#[async_trait]
impl Processable for TransactionStreamStep
where
    Self: Sized + Send + 'static,
{
    type Input = ();
    type Output = Transaction;
    type RunType = PollableAsyncRunType;

    async fn process(&mut self, _item: TransactionContext<()>) -> TransactionContext<Transaction> {
        TransactionContext::default()
    }
}

#[async_trait]
impl PollableAsyncStep for TransactionStreamStep
where
    Self: Sized + Send + 'static,
{
    async fn poll(&mut self) -> Option<Vec<TransactionContext<Transaction>>> {
        let txn_pb_response_res = self.transaction_stream.get_next_transaction_batch().await;
        match txn_pb_response_res {
            Ok(txn_pb_response) => {
                let transactions_with_context = TransactionContext {
                    data: txn_pb_response.transactions,
                    start_version: txn_pb_response.start_version,
                    end_version: txn_pb_response.end_version,
                    start_transaction_timestamp: txn_pb_response.start_txn_timestamp,
                    end_transaction_timestamp: txn_pb_response.end_txn_timestamp,
                    total_size_in_bytes: txn_pb_response.size_in_bytes,
                };
                Some(vec![transactions_with_context])
            },
            Err(e) => {
                println!("Error getting transactions: {:?}", e);
                None
            },
        }
    }
}

impl NamedStep for TransactionStreamStep {
    fn name(&self) -> String {
        "TransactionStream".to_string()
    }
}

mock! {
    pub TransactionStreamStep {}

    #[async_trait]
    impl Processable for TransactionStreamStep
    where Self: Sized + Send + 'static,
    {
        type Input = ();
        type Output = Transaction;
        type RunType = PollableAsyncRunType;

        async fn init(&mut self);

        async fn process(&mut self, _item: TransactionContext<()> ) -> TransactionContext<Transaction>;
    }

    #[async_trait]
    impl PollableAsyncStep for TransactionStreamStep
    where
        Self: Sized + Send + 'static,
    {
        // async fn poll(&mut self) -> Option<Vec<TransactionsPBResponse>> {
        //     // Testing framework can provide mocked transactions here
        //     Some(vec![TransactionsPBResponse {
        //         transactions: vec![],
        //         chain_id: 0,
        //         start_version: 0,
        //         end_version: 100,
        //         start_txn_timestamp: None,
        //         end_txn_timestamp: None,
        //         size_in_bytes: 10,
        //     }])
        // }
        async fn poll(&mut self) -> Option<Vec<TransactionContext<Transaction>>>;
    }

    impl NamedStep for TransactionStreamStep {
        fn name(&self) -> String;
    }
}

#[cfg(test)]
mod tests {
    use instrumented_channel::instrumented_bounded_channel;

    use super::*;
    use crate::{
        builder::ProcessorBuilder,
        test::{steps::pass_through_step::PassThroughStep, utils::receive_with_timeout},
        traits::{IntoRunnableStep, RunnableStepWithInputReceiver},
    };
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_transaction_stream() {
        let (_, input_receiver) = instrumented_bounded_channel("input", 1);

        let mut mock_transaction_stream = MockTransactionStreamStep::new();
        // Testing framework can provide mocked transactions here
        mock_transaction_stream.expect_poll().returning(|| {
            Some(vec![TransactionContext {
                data: vec![],
                start_version: 0,
                end_version: 100,
                start_transaction_timestamp: None,
                end_transaction_timestamp: None,
                total_size_in_bytes: 10,
            }])
        });
        mock_transaction_stream.expect_init().returning(|| {
            // Do nothing
        });
        mock_transaction_stream
            .expect_name()
            .returning(|| "MockTransactionStream".to_string());

        let pass_through_step = PassThroughStep::default();

        let transaction_stream_with_input = RunnableStepWithInputReceiver::new(
            input_receiver,
            mock_transaction_stream.into_runnable_step(),
        );

        let (_, mut output_receiver) =
            ProcessorBuilder::new_with_runnable_input_receiver_first_step(
                transaction_stream_with_input,
            )
            .connect_to(pass_through_step.into_runnable_step(), 5)
            .end_and_return_output_receiver(5);

        tokio::time::sleep(Duration::from_millis(250)).await;
        let result = receive_with_timeout(&mut output_receiver, 100)
            .await
            .unwrap();

        assert_eq!(result.data.len(), 1);
    }
}

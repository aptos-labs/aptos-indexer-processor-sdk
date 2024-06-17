use super::{pollable_async_step::PollableAsyncRunType, PollableAsyncStep};
use crate::traits::{NamedStep, Processable};
use anyhow::Result;
use aptos_indexer_transaction_stream::config::TransactionStreamConfig;
use aptos_indexer_transaction_stream::transaction_stream::{
    TransactionStream as TransactionStreamInternal, TransactionsPBResponse,
};
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
    type Output = TransactionsPBResponse;
    type RunType = PollableAsyncRunType;

    async fn process(&mut self, _item: Vec<()>) -> Vec<TransactionsPBResponse> {
        Vec::new()
    }
}

#[async_trait]
impl PollableAsyncStep for TransactionStreamStep
where
    Self: Sized + Send + 'static,
{
    async fn poll(&mut self) -> Option<Vec<TransactionsPBResponse>> {
        let transactions_output = self.transaction_stream.get_next_transaction_batch().await;
        match transactions_output {
            Ok(transactions) => Some(vec![transactions]),
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
        type Output = TransactionsPBResponse;
        type RunType = PollableAsyncRunType;

        async fn init(&mut self);

        async fn process(&mut self, _item: Vec<()>) -> Vec<TransactionsPBResponse>;
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
        async fn poll(&mut self) -> Option<Vec<TransactionsPBResponse>>;
    }

    impl NamedStep for TransactionStreamStep {
        fn name(&self) -> String;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        builder::ProcessorBuilder,
        test::{steps::pass_through_step::PassThroughStep, utils::receive_with_timeout},
        traits::{IntoRunnableStep, RunnableStepWithInputReceiver},
    };
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_transaction_stream() {
        let (input_sender, input_receiver) = kanal::bounded_async(1);

        let mut mock_transaction_stream = MockTransactionStreamStep::new();
        // Testing framework can provide mocked transactions here
        mock_transaction_stream.expect_poll().returning(|| {
            Some(vec![TransactionsPBResponse {
                transactions: vec![],
                chain_id: 0,
                start_version: 0,
                end_version: 100,
                start_txn_timestamp: None,
                end_txn_timestamp: None,
                size_in_bytes: 10,
            }])
        });
        mock_transaction_stream.expect_init().returning(|| {
            // Do nothing
        });
        mock_transaction_stream
            .expect_name()
            .returning(|| "MockTransactionStream".to_string());

        let pass_through_step = PassThroughStep::new();

        let transaction_stream_with_input = RunnableStepWithInputReceiver::new(
            input_receiver,
            mock_transaction_stream.into_runnable_step(),
        );

        let (builder, mut output_receiver) =
            ProcessorBuilder::new_with_runnable_input_receiver_first_step(
                transaction_stream_with_input,
            )
            .end_with_and_return_output_receiver(pass_through_step.into_runnable_step(), 5);

        tokio::time::sleep(Duration::from_millis(250)).await;
        let result = receive_with_timeout(&mut output_receiver, 100)
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
    }
}

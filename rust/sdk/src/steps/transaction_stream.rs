use super::PollableAsyncStep;
use crate::traits::{NamedStep, Processable};
use aptos_indexer_transaction_stream::transaction_stream::{
    TransactionStream as TransactionStreamInternal, TransactionsPBResponse,
};
use async_trait::async_trait;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use transaction_filter::transaction_filter::TransactionFilter;
use url::Url;

pub struct TransactionStream
where
    Self: Sized + Send + 'static,
{
    pub transaction_stream: TransactionStreamInternal,
}

impl TransactionStream
where
    Self: Sized + Send + 'static,
{
    pub fn new(
        indexer_grpc_data_service_address: Url,
        indexer_grpc_http2_ping_interval: Duration,
        indexer_grpc_http2_ping_timeout: Duration,
        indexer_grpc_reconnection_timeout_secs: Duration,
        indexer_grpc_response_item_timeout_secs: Duration,
        starting_version: u64,
        request_ending_version: Option<u64>,
        auth_token: String,
        processor_name: String,
        transaction_filter: TransactionFilter,
        pb_channel_txn_chunk_size: usize,
    ) -> Self {
        let transaction_stream = TransactionStreamInternal::new(
            indexer_grpc_data_service_address,
            indexer_grpc_http2_ping_interval,
            indexer_grpc_http2_ping_timeout,
            indexer_grpc_reconnection_timeout_secs,
            indexer_grpc_response_item_timeout_secs,
            starting_version,
            request_ending_version,
            auth_token,
            processor_name,
            transaction_filter,
            pb_channel_txn_chunk_size,
        );
        Self { transaction_stream }
    }
}

#[async_trait]
impl Processable for TransactionStream
where
    Self: Sized + Send + 'static,
{
    type Input = ();
    type Output = TransactionsPBResponse;
    type RunType = ();

    async fn init(&mut self) {
        self.transaction_stream.init_stream().await;
    }

    async fn cleanup(&mut self) {}

    async fn process(&mut self, _item: Vec<()>) -> Vec<TransactionsPBResponse> {
        Vec::new()
    }
}

#[async_trait]
impl PollableAsyncStep for TransactionStream
where
    Self: Sized + Send + 'static,
{
    async fn poll(&mut self) -> Option<Vec<TransactionsPBResponse>> {
        let transactions_output = self.transaction_stream.get_next_transaction_batch().await;
        Some(transactions_output.transactions)
    }
}

impl NamedStep for TransactionStream {
    fn name(&self) -> String {
        "TransactionStream".to_string()
    }
}

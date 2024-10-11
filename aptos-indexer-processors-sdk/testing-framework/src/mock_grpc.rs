use aptos_protos::indexer::v1::{
    raw_data_server::{RawData, RawDataServer},
    GetTransactionsRequest, TransactionsResponse,
};
use futures::Stream;
use std::pin::Pin;
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct MockGrpcServer {
    pub transactions_response: Vec<TransactionsResponse>,
    pub chain_id: u64,
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<TransactionsResponse, Status>> + Send>>;

#[tonic::async_trait]
impl RawData for MockGrpcServer {
    type GetTransactionsStream = ResponseStream;

    async fn get_transactions(
        &self,
        req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        let version = req.into_inner().starting_version.unwrap();

        // Find the specific transaction that matches the version
        let transaction = self.transactions_response.iter().flat_map(|transactions_response| {
            transactions_response.transactions.iter()
        })
            .find(|tx| {
                // println!("Checking transaction version: {}", tx.version);
                tx.version == version // Return the transaction that matches the version
            });

        let result = match transaction {
            Some(tx) => {
                // Build a new TransactionResponse with this matching transaction
                let mut new_transaction_response = TransactionsResponse {
                    transactions: vec![tx.clone()],
                    ..Default::default()
                };
                new_transaction_response.chain_id = Some(self.chain_id); // Set the chain_id field in the response
                new_transaction_response
            },
            None => {
                // No matching transaction found, return a default response with the first transaction
                let mut default_transaction_response = self.transactions_response[0].clone();
                default_transaction_response.chain_id = Some(self.chain_id); // Set the chain_id field
                default_transaction_response
            },
        };

// Create a stream and return the response
        let stream = futures::stream::iter(vec![Ok(result)]);
        Ok(Response::new(Box::pin(stream)))
    }
}

impl MockGrpcServer {
    pub async fn run(self) {
        tonic::transport::Server::builder()
            .add_service(
                RawDataServer::new(self)
                    .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                    .send_compressed(tonic::codec::CompressionEncoding::Zstd),
            )
            .serve("127.0.0.1:51254".parse().unwrap())
            .await
            .unwrap();
    }
}
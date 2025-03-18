use aptos_protos::indexer::v1::{
    raw_data_server::{RawData, RawDataServer},
    GetTransactionsRequest, TransactionsResponse,
};
use futures::Stream;
use std::{collections::HashMap, pin::Pin};
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};

// Bind to port 0 to get a random available port
const GRPC_ADDRESS: &str = "127.0.0.1:0";

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
        let request = req.into_inner();
        let starting_version = request.starting_version.unwrap_or(0); // Default to 0 if starting_version is not provided
        let transactions_count = request.transactions_count.unwrap_or(1); // Default to 1 if transactions_count is not provided
        let mut collected_transactions = Vec::new();

        let mut transaction_map = HashMap::new();
        for transaction_response in &self.transactions_response {
            for tx in &transaction_response.transactions {
                transaction_map.insert(tx.version, tx.clone());
            }
        }

        let mut sorted_transactions: Vec<_> = transaction_map
            .iter()
            .filter(|(&version, _)| version >= starting_version)
            .map(|(_, tx)| tx.clone())
            .collect();
        sorted_transactions.sort_by_key(|tx| tx.version);

        collected_transactions.extend(
            sorted_transactions
                .into_iter()
                .take(transactions_count as usize),
        );

        let result = if !collected_transactions.is_empty() {
            TransactionsResponse {
                transactions: collected_transactions,
                chain_id: Some(self.chain_id),
            }
        } else {
            // Return a default response with chain_id if no transactions are found
            let mut default_transaction_response = self.transactions_response[0].clone();
            default_transaction_response.chain_id = Some(self.chain_id);
            default_transaction_response
        };

        let stream = futures::stream::iter(vec![Ok(result)]);
        Ok(Response::new(Box::pin(stream)))
    }
}

impl MockGrpcServer {
    pub async fn run(self) -> anyhow::Result<u16> {
        let listener = tokio::net::TcpListener::bind(GRPC_ADDRESS).await?;
        let bound_addr = listener.local_addr()?; // Get the actual bound address

        // Convert the TcpListener into a TcpListenerStream (wrapping it with `?` to handle potential errors)
        let stream = TcpListenerStream::new(listener);

        // Build and start the gRPC server without graceful shutdown
        let server = Server::builder().add_service(
            RawDataServer::new(self)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd) // Enable compression for incoming requests
                .send_compressed(tonic::codec::CompressionEncoding::Zstd), // Compress outgoing responses
        );

        tokio::spawn(async move {
            // This server will run until the process is killed or the task is stopped
            let server_timeout = Duration::from_secs(60);

            match timeout(server_timeout, server.serve_with_incoming(stream)).await {
                Ok(result) => match result {
                    Ok(_) => {
                        println!("Server stopped successfully.");
                    },
                    Err(e) => {
                        eprintln!("Failed to run gRPC server: {:?}", e);
                    },
                },
                Err(_) => {
                    eprintln!("Server timed out and was stopped.");
                },
            }
        });

        // Return the port number so it can be used by other parts of the program
        let port = bound_addr.port();
        println!("Server is running on port {}", port);

        Ok(port)
    }
}

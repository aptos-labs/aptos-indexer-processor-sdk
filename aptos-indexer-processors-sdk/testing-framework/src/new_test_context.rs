use aptos_protos::indexer::v1::TransactionsResponse;
use aptos_indexer_processor_sdk::traits::processor_trait::ProcessorTrait;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use url::Url;
use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig;
use crate::mock_grpc::MockGrpcServer;

pub struct SdkTestContext<D: TestDatabase> {
    pub transaction_batches: Vec<Transaction>,
    pub database: D, // Holds the database setup (user-defined)
}

impl<D: TestDatabase> SdkTestContext<D> {
    pub async fn new(txn_bytes: &[&[u8]], mut database: D) -> anyhow::Result<Self> {
        let transaction_batches = txn_bytes
            .iter()
            .map(|txn| serde_json::from_slice(txn).unwrap())
            .collect::<Vec<Transaction>>();

        // Set up the database using the user-defined method
        database.setup().await?;

        Ok(SdkTestContext {
            transaction_batches,
            database,
        })
    }

    /// Run the processor and pass user-defined validation logic
    pub async fn run<F>(
        &self,
        processor: &impl ProcessorTrait,
        verification_f: F,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(&str) -> anyhow::Result<()> + Send + Sync + 'static,
    {
        let transactions = self.transaction_batches.clone();
        let transactions_response = vec![TransactionsResponse {
            transactions,
            ..TransactionsResponse::default()
        }];

        self.setup_mock_grpc(transactions_response, 1).await;

        // Run the processor
        processor.run_processor().await?;

        // Pass the DB URL for user-defined validation
        let db_url = self.database.get_db_url();
        verification_f(&db_url)?;

        Ok(())
    }

    /// Helper function to set up and run the mock GRPC server.
    async fn setup_mock_grpc(&self, transactions: Vec<TransactionsResponse>, chain_id: u64) {
        println!("received transactions size: {:?}", transactions.len());
        let mock_grpc_server = MockGrpcServer {
            transactions,
            chain_id,
        };

        // Start the Mock GRPC server
        tokio::spawn(async move {
            println!("Starting Mock GRPC server");
            mock_grpc_server.run().await;
        });
    }

    pub fn create_transaction_stream_config(&self,
        starting_version: u64,
        ending_version: u64,
    ) -> TransactionStreamConfig {
        TransactionStreamConfig {
            indexer_grpc_data_service_address: Url::parse("http://localhost:51254")
                .expect("Could not parse database url"),
            starting_version: Some(starting_version), // dynamically pass the starting version
            request_ending_version: Some(ending_version), // dynamically pass the ending version
            auth_token: "".to_string(),
            request_name_header: "sdk testing".to_string(),
            indexer_grpc_http2_ping_interval_secs: 30,
            indexer_grpc_http2_ping_timeout_secs: 10,
            indexer_grpc_reconnection_timeout_secs: 10,
            indexer_grpc_response_item_timeout_secs: 60,
        }
    }

}

#[async_trait]
pub trait TestDatabase: Send + Sync {
    /// Set up the test container using user-defined code.
    async fn setup<'a>(&'a mut self) -> anyhow::Result<()>;

    /// Retrieve the database connection URL after setup.
    fn get_db_url(&self) -> String;
}

pub struct PostgresTestDatabase {
    connection_string: String,
    postgres_container: Option<ContainerAsync<GenericImage>>,
}

impl PostgresTestDatabase {
    pub fn new() -> Self {
        PostgresTestDatabase {
            postgres_container: None,
            connection_string: String::new(),
        }
    }
}

#[async_trait]
impl TestDatabase for PostgresTestDatabase {
    /// Set up the Postgres container and get the database connection URL.
    async fn setup<'a>(&'a mut self) -> anyhow::Result<()> {
        self.postgres_container = Some(
            GenericImage::new("postgres", "14")
                .with_exposed_port(5432.tcp())
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_DB", "postgres")
                .with_env_var("POSTGRES_USER", "postgres")
                .with_env_var("POSTGRES_PASSWORD", "postgres")
                .start()
                .await
                .expect("Postgres started"),
        );

        // Retrieve the host and port of the container for the connection string
        let host = self
            .postgres_container
            .as_ref()
            .unwrap()
            .get_host()
            .await
            .expect("Failed to get host");

        let port = self
            .postgres_container
            .as_ref()
            .unwrap()
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get port");

        // Create the Postgres connection string
        self.connection_string = format!("postgres://postgres:postgres@{}:{}/postgres", host, port);

        Ok(())
    }

    /// Retrieve the Postgres connection URL after the container has been set up.
    fn get_db_url(&self) -> String {
        self.connection_string.clone()
    }
}

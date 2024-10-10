use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use anyhow::Context;
use aptos_protos::indexer::v1::TransactionsResponse;
use aptos_indexer_processor_sdk::traits::processor_trait::ProcessorTrait;
use aptos_protos::transaction::v1::Transaction;
use serde_json::to_string_pretty;

use url::Url;
use aptos_indexer_processor_sdk::aptos_indexer_transaction_stream::TransactionStreamConfig;
use crate::mock_grpc::MockGrpcServer;

const DEFAULT_OUTPUT_FOLDER: &str = "expected_db_output_files/";
const INDEXER_GRPC_DATA_SERVICE_URL: &str = "http://localhost:51254";

pub struct SdkTestContext{
    pub transaction_batches: Vec<Transaction>,
}

impl SdkTestContext {
    pub async fn new(txn_bytes: &[&[u8]]) -> anyhow::Result<Self> {
        let transaction_batches = txn_bytes
            .iter()
            .map(|txn| serde_json::from_slice(txn).unwrap())
            .collect::<Vec<Transaction>>();

        Ok(SdkTestContext {
            transaction_batches,
        })
    }

    /// Run the processor and pass user-defined validation logic
    pub async fn run<F>(
        &self,
        processor: &impl ProcessorTrait,
        db_url: &str,
        txn_version: u64,
        generate_files: bool, // flag to control file generation
        output_path: Option<String>, // Optional custom output path
        verification_f: F,
    ) -> anyhow::Result<serde_json::Value>
    where
        F: FnOnce(&str) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static,
    {
        let transactions = self.transaction_batches.clone();
        let transactions_response = vec![TransactionsResponse {
            transactions,
            ..TransactionsResponse::default()
        }];

        self.setup_mock_grpc(transactions_response, 1).await;

        // Run the processor
        processor.run_processor().await?;

        let db_values = verification_f(db_url)?;

        // Conditionally generate output files if the `generate_files` flag is true
        if generate_files {
            generate_output_file(
                processor.name(),
                &txn_version.to_string(),
                &db_values,
                output_path
            )?;
        } else {
            println!("[INFO] Skipping file generation as requested.");
        }

        Ok(db_values)
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
            indexer_grpc_data_service_address: Url::parse(INDEXER_GRPC_DATA_SERVICE_URL)
                .expect("Could not parse database url"),
            starting_version: Some(starting_version), // dynamically pass the starting version
            request_ending_version: Some(ending_version), // dynamically pass the ending version
            auth_token: "".to_string(),
            request_name_header: "sdk-testing".to_string(),
            indexer_grpc_http2_ping_interval_secs: 30,
            indexer_grpc_http2_ping_timeout_secs: 10,
            indexer_grpc_reconnection_timeout_secs: 10,
            indexer_grpc_response_item_timeout_secs: 60,
        }
    }

}

// Helper function to construct the output file path
fn construct_file_path(output_dir: &str, processor_name: &str, txn_version: &str) -> PathBuf {
    Path::new(output_dir)
        .join(processor_name)
        .join(format!("{}_{}.json", processor_name, txn_version))
}

// Helper function to ensure the directory exists
fn ensure_directory_exists(path: &Path) -> anyhow::Result<()> {
    if let Some(parent_dir) = path.parent() {
        fs::create_dir_all(parent_dir).expect("Failed to create directory");
    }
    Ok(())
}

// Helper function to generate output files
fn generate_output_file(
    processor_name: &str,
    txn_version: &str,
    db_values: &serde_json::Value,
    output_path: Option<String>,
) -> anyhow::Result<()> {
        let output_dir = output_path.unwrap_or_else(|| DEFAULT_OUTPUT_FOLDER.to_string());
        let file_path = construct_file_path(&output_dir, processor_name, txn_version);

        ensure_directory_exists(&file_path)?;

        fs::write(&file_path, to_string_pretty(db_values)?)
            .context(format!("Failed to write file to {:?}", file_path))?;
        println!("[INFO] Generated output file at: {}", file_path.display());
        Ok(())
}
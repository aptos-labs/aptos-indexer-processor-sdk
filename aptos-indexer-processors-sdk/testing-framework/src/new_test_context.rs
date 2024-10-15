use crate::mock_grpc::MockGrpcServer;
use anyhow::Context;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    traits::processor_trait::ProcessorTrait,
};
use aptos_protos::{indexer::v1::TransactionsResponse, transaction::v1::Transaction};
use serde_json::{to_string_pretty, Error as SerdeError};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::sync::Mutex;
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use url::Url;

const INDEXER_GRPC_DATA_SERVICE_URL: &str = "http://localhost:51254";

pub struct SdkTestContext {
    pub transaction_batches: Vec<Transaction>,
}

impl SdkTestContext {
    pub async fn new(txn_bytes: &[&[u8]]) -> anyhow::Result<Self> {
        let transaction_batches = txn_bytes
            .iter()
            .enumerate()
            .map(|(idx, txn)| {
                serde_json::from_slice::<Transaction>(txn).map_err(|err| {
                    anyhow::anyhow!(
                        "Failed to parse transaction at index {}: {}",
                        idx,
                        format_serde_error(err)
                    )
                })
            })
            .collect::<Result<Vec<Transaction>, _>>()?;

        let context = SdkTestContext {
            transaction_batches,
        };

        // Create mock GRPC transactions and setup the server
        let transactions = context.transaction_batches.clone();
        let transactions_response = vec![TransactionsResponse {
            transactions,
            ..TransactionsResponse::default()
        }];

        // Call setup_mock_grpc to start the server and get the port
        context.setup_mock_grpc(transactions_response, 1).await;

        Ok(context)
    }

    /// Run the processor and pass user-defined validation logic
    pub async fn run<F>(
        &mut self,
        processor: &impl ProcessorTrait,
        db_url: &str,
        txn_version: u64,
        generate_files: bool, // flag to control file generation
        output_path: String,  // output path
        verification_f: F,    // Modified to return a HashMap for multi-table data
    ) -> anyhow::Result<HashMap<String, serde_json::Value>>
    // Return HashMap for multi-table results
    where
        F: FnOnce(&str) -> anyhow::Result<HashMap<String, serde_json::Value>>
            + Send
            + Sync
            + 'static, // Modified for multi-table verification
    {
        let retry_strategy = ExponentialBackoff::from_millis(100).map(jitter).take(5); // Retry up to 5 times

        let result = Retry::spawn(retry_strategy, || async {
            processor
                .run_processor()
                .await
                .context("Failed to run processor")
        })
        .await;

        // Handle failure after retries
        match result {
            Ok(_) => {
                println!("[INFO] Processor run succeeded");
            },
            Err(e) => {
                eprintln!("[ERROR] Processor failed after retries: {:?}", e);
                return Err(anyhow::anyhow!(
                    "Failed to run processor after multiple retries: {}",
                    e
                ));
            },
        }

        // Small delay to ensure all data is processed before verification
        tokio::time::sleep(Duration::from_millis(250)).await;

        // Retrieve data from multiple tables using verification function
        let db_values = verification_f(db_url).context("Verification function failed")?;

        // Conditionally generate output files for each table
        if generate_files {
            println!("[TEST] Generating output files for all tables.");

            // Iterate over each table's data in the HashMap and generate an output file
            for (table_name, table_data) in &db_values {
                generate_output_file(
                    processor.name(),
                    table_name,
                    &format!("{}", txn_version),
                    table_data,
                    output_path.clone(),
                )?;
            }
        } else {
            println!("[TEST] Skipping file generation as requested.");
        }

        // Return the HashMap containing the data from all queried tables
        Ok(db_values)
    }

    /// Helper function to set up and run the mock GRPC server.
    async fn setup_mock_grpc(
        &self,
        transactions_response: Vec<TransactionsResponse>,
        chain_id: u64,
    ) {
        let mock_grpc_server = MockGrpcServer {
            transactions_response: Mutex::new(transactions_response),
            chain_id,
        };

        tokio::spawn(async move {
            println!("Starting Mock GRPC server");
            let _ = mock_grpc_server.run().await;
        });
    }

    // TODO: follow up on txn_version whether it should be a vec or not.
    pub fn create_transaction_stream_config(&self, txn_version: u64) -> TransactionStreamConfig {
        TransactionStreamConfig {
            indexer_grpc_data_service_address: Url::parse(INDEXER_GRPC_DATA_SERVICE_URL)
                .expect("Could not parse database url"),
            starting_version: Some(txn_version), // dynamically pass the starting version
            request_ending_version: Some(txn_version), // dynamically pass the ending version
            auth_token: "".to_string(),
            request_name_header: "sdk-testing".to_string(),
            indexer_grpc_http2_ping_interval_secs: 30,
            indexer_grpc_http2_ping_timeout_secs: 10,
            indexer_grpc_reconnection_timeout_secs: 10,
            indexer_grpc_response_item_timeout_secs: 60,
        }
    }
}

/// Helper function to format serde_json errors for better readability.
fn format_serde_error(err: SerdeError) -> String {
    match err.classify() {
        serde_json::error::Category::Io => format!("I/O error: {}", err),
        serde_json::error::Category::Syntax => format!("Syntax error: {}", err),
        serde_json::error::Category::Data => format!("Data error: {}", err),
        serde_json::error::Category::Eof => format!("Unexpected end of input: {}", err),
    }
}

// Helper function to construct the output file path with the table name
fn construct_file_path(
    output_dir: &str,
    processor_name: &str,
    table_name: &str,
    txn_version: &str,
) -> PathBuf {
    Path::new(output_dir)
        .join(processor_name)
        .join(txn_version)
        .join(format!("{}.json", table_name)) // Including table_name in the format
}

// Helper function to ensure the directory exists
fn ensure_directory_exists(path: &Path) -> anyhow::Result<()> {
    if let Some(parent_dir) = path.parent() {
        fs::create_dir_all(parent_dir).context("Failed to create directory")?;
    }
    Ok(())
}

// Helper function to generate output files for each table
fn generate_output_file(
    processor_name: &str,
    table_name: &str,
    txn_version: &str,
    db_values: &serde_json::Value,
    output_dir: String,
) -> anyhow::Result<()> {
    let file_path = construct_file_path(&output_dir, processor_name, table_name, txn_version); // Pass table_name here

    ensure_directory_exists(&file_path)?;

    fs::write(&file_path, to_string_pretty(db_values)?)
        .context(format!("Failed to write file to {:?}", file_path))?;
    println!("[TEST] Generated output file at: {}", file_path.display());
    Ok(())
}

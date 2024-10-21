use crate::mock_grpc::MockGrpcServer;
use anyhow::Context;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    traits::processor_trait::ProcessorTrait,
};
use aptos_protos::{indexer::v1::TransactionsResponse, transaction::v1::Transaction};
use serde_json::{to_string_pretty, Error as SerdeError, Value};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time::{self, Duration as TokioDuration};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use url::Url;

pub struct SdkTestContext {
    pub transaction_batches: Vec<Transaction>,
    pub port: Option<String>,
}

const SLEEP_DURATION: Duration = Duration::from_millis(250);

impl SdkTestContext {
    pub async fn new(txn_bytes: &[&[u8]]) -> anyhow::Result<Self> {
        let transaction_batches = txn_bytes
            .iter()
            .enumerate()
            .map(|(idx, txn)| {
                // Deserialize the transaction
                let mut transaction =
                    serde_json::from_slice::<Transaction>(txn).map_err(|err| {
                        anyhow::anyhow!(
                            "Failed to parse transaction at index {}: {}",
                            idx,
                            format_serde_error(err)
                        )
                    })?;

                // Update the transaction version to enforce ordering (txn1, txn2, txn3, ...)
                // This ensures that the mock gRPC returns consecutive transaction versions.
                transaction.version = idx as u64 + 1;

                Ok::<Transaction, anyhow::Error>(transaction) // Explicit type annotation
            })
            .collect::<Result<Vec<Transaction>, _>>()?;

        let mut context = SdkTestContext {
            transaction_batches,
            port: None,
        };

        // Create mock GRPC transactions and setup the server
        let transactions = context.transaction_batches.clone();
        let transactions_response = vec![TransactionsResponse {
            transactions,
            ..TransactionsResponse::default()
        }];

        // Call setup_mock_grpc to start the server and get the port
        let port = context.setup_mock_grpc(transactions_response, 1).await;
        context.port = Some(port.to_string());
        Ok(context)
    }

    /// Run the processor and pass user-defined validation logic
    pub async fn run<F>(
        &mut self,
        processor: &impl ProcessorTrait,
        txn_version: u64,
        generate_files: bool,             // flag to control file generation
        output_path: String,              // output path
        custom_file_name: Option<String>, // custom file name when testing multiple txns
        verification_f: F,                // Modified to return a HashMap for multi-table data
    ) -> anyhow::Result<HashMap<String, Value>>
    where
        F: FnOnce() -> anyhow::Result<HashMap<String, Value>> + Send + Sync + 'static, // Modified for multi-table verification
    {
        let retry_strategy = ExponentialBackoff::from_millis(100).map(jitter).take(5); // Retry up to 5 times

        let timeout_duration = TokioDuration::from_secs(10); // e.g., 5 seconds timeout for each retry
        let result = Retry::spawn(retry_strategy, || async {
            // Wrap processor call with a timeout
            match time::timeout(timeout_duration, processor.run_processor()).await {
                Ok(result) => result.context("Processor run failed"),
                Err(_) => Err(anyhow::anyhow!("Processor run timed out")),
            }
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
        tokio::time::sleep(SLEEP_DURATION).await;
        // Retrieve data from multiple tables using verification function
        let mut db_values = verification_f().context("Verification function failed")?;

        // Conditionally generate output files for each table
        if generate_files {
            println!(
                "[TEST] Generating output files for all {} tables",
                db_values.len()
            );
            // Iterate over each table's data in the HashMap and generate an output file
            for (table_name, table_data) in db_values.iter_mut() {
                remove_inserted_at(table_data);

                generate_output_file(
                    processor.name(),
                    table_name,
                    &format!("{}", txn_version),
                    table_data,
                    output_path.clone(),
                    custom_file_name.clone(),
                )?;
            }
        } else {
            println!("[TEST] Skipping file generation as requested.");
        }

        Ok(db_values)
    }

    /// Helper function to set up and run the mock GRPC server.
    async fn setup_mock_grpc(
        &self,
        transactions_response: Vec<TransactionsResponse>,
        chain_id: u64,
    ) -> u16 {
        let mock_grpc_server = MockGrpcServer {
            transactions_response,
            chain_id,
        };

        let port = tokio::spawn(async move {
            println!("Starting Mock GRPC server");
            mock_grpc_server.run().await.unwrap() // Get the port returned by `run`
        })
        .await
        .unwrap();

        println!("Mock GRPC server is running on port {}", port);
        port
    }

    pub fn create_transaction_stream_config(&self, starting_version: u64, txn_count: u64) -> TransactionStreamConfig {
        let data_service_address = format!(
            "http://localhost:{}",
            self.port.as_ref().expect("Port is not set")
        );
        TransactionStreamConfig {
            indexer_grpc_data_service_address: Url::parse(&data_service_address)
                .expect("Could not parse database url"),
            starting_version: Some(starting_version),
            request_ending_version: Some(txn_count),
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
pub fn generate_output_file(
    processor_name: &str,
    table_name: &str,
    txn_version: &str,
    db_values: &Value,
    output_dir: String,
    custom_file_name: Option<String>,
) -> anyhow::Result<()> {
    let file_path = match custom_file_name {
        Some(custom_name) => {
            // If custom_file_name is present, build the file path using it
            PathBuf::from(&output_dir)
                .join(processor_name)
                .join(custom_name)
                .join(
                    format!("{}.json", table_name),
                )
        },
        None => {
            // Default case: use table_name and txn_version to construct file name
            construct_file_path(&output_dir, processor_name, table_name, txn_version)
        },
    };

    ensure_directory_exists(&file_path)?;
    fs::write(&file_path, to_string_pretty(db_values)?)
        .context(format!("Failed to write file to {:?}", file_path))?;
    println!("[TEST] Generated output file at: {}", file_path.display());
    Ok(())
}

#[allow(dead_code)]
pub fn remove_inserted_at(value: &mut Value) {
    if let Some(array) = value.as_array_mut() {
        for item in array.iter_mut() {
            if let Some(obj) = item.as_object_mut() {
                obj.remove("inserted_at");
            }
        }
    }
}

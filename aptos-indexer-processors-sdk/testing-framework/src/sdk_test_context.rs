use crate::mock_grpc::MockGrpcServer;
use anyhow::Context;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    config::indexer_processor_config::{DbConfig, IndexerProcessorConfig},
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
use tokio::time::{timeout, Duration as TokioDuration};
use tokio_retry::{
    strategy::{jitter, ExponentialBackoff},
    Retry,
};
use url::Url;

pub struct SdkTestContext {
    transaction_batches: Vec<Transaction>,
    test_transaction_versions: Vec<u64>,
    port: Option<String>,
    request_start_version: u64,
    transactions_count: u64,
}

const SLEEP_DURATION: Duration = Duration::from_millis(250);

/// SdkTestContext is a helper struct to set up and run indexer processor tests.
/// It is initialized with a slice of generated transaction bytes
/// and provides methods to run a mock GRPC server that returns these transactions,
/// construct a TransactionStreamConfig, and runs the processor for testing.
impl SdkTestContext {
    pub fn new(txn_bytes: &[&[u8]]) -> Self {
        let mut transaction_batches = match txn_bytes
            .iter()
            .enumerate()
            .map(|(idx, txn)| {
                let transaction = serde_json::from_slice::<Transaction>(txn).map_err(|err| {
                    anyhow::anyhow!(
                        "Failed to parse transaction at index {}: {}",
                        idx,
                        format_serde_error(err)
                    )
                })?;

                Ok::<Transaction, anyhow::Error>(transaction) // Explicit type annotation
            })
            .collect::<Result<Vec<Transaction>, _>>()
        {
            Ok(txns) => txns,
            Err(e) => panic!("Failed to parse transactions: {}", e),
        };

        if transaction_batches.is_empty() {
            panic!("SdkTestContext must be initialized with at least one transaction");
        }

        transaction_batches.sort_by_key(|tx| tx.version);

        let request_start_version = transaction_batches[0].version;
        let transactions_count = transaction_batches.len() as u64;
        let test_versions = transaction_batches.iter().map(|tx| tx.version).collect();

        // Check if the provided transaction bytes contains a transaction with version 1.
        // This is required for the chain_id_check to pass.
        let version_1_exists = transaction_batches.iter().any(|tx| tx.version == 1);

        // Append the dummy transaction with version 1 if it doesn't exist to pass chain_id_check
        if !version_1_exists {
            let dummy_transaction = Transaction {
                version: 1,
                ..Transaction::default()
            };
            transaction_batches.insert(0, dummy_transaction);
        }

        SdkTestContext {
            transaction_batches,
            test_transaction_versions: test_versions,
            request_start_version,
            transactions_count,
            port: None,
        }
    }

    // Return the start version of the transactions in this test.
    // This is used to construct the TransactionStreamConfig.
    pub fn get_request_start_version(&self) -> u64 {
        self.request_start_version
    }

    // Return the number of transactions in this test.
    // This is used to construct the TransactionStreamConfig.
    pub fn get_transactions_count(&self) -> u64 {
        self.transactions_count
    }

    // Return the transaction versions that will be included in this test.
    // This does not include the dummy transaction with version 1.
    // The purpose of this is to fetch the versions for the data validation step of the test.
    pub fn get_test_transaction_versions(&self) -> Vec<u64> {
        self.test_transaction_versions.clone()
    }

    /// Run the processor and pass user-defined validation logic
    pub async fn run<F, D>(
        &mut self,
        processor: &impl ProcessorTrait,
        config: IndexerProcessorConfig<D>,
        generate_files: bool,             // flag to control file generation
        output_path: String,              // output path
        custom_file_name: Option<String>, // custom file name when testing multiple txns
        verification_f: F,                // Modified to return a HashMap for multi-table data
    ) -> anyhow::Result<HashMap<String, Value>>
    where
        F: FnOnce() -> anyhow::Result<HashMap<String, Value>> + Send + Sync + 'static, // Modified for multi-table verification
        D: DbConfig,
    {
        let retry_strategy = ExponentialBackoff::from_millis(100).map(jitter).take(5); // Retry up to 5 times

        let timeout_duration = TokioDuration::from_secs(10); // e.g., 5 seconds timeout for each retry
        let result = Retry::spawn(retry_strategy, || async {
            // Wrap processor call with a timeout
            match timeout(timeout_duration, processor.run_processor(config.clone())).await {
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
                    &format!("{}", self.request_start_version),
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
    pub async fn init_mock_grpc(&mut self) -> anyhow::Result<()> {
        // Create mock GRPC transactions and setup the server
        let transactions = self.transaction_batches.clone();
        let transactions_response = vec![TransactionsResponse {
            transactions,
            ..TransactionsResponse::default()
        }];

        // Call setup_mock_grpc to start the server and get the port
        let mock_grpc_server = MockGrpcServer {
            transactions_response,
            chain_id: 1,
        };

        let port = tokio::spawn(async move {
            println!("Starting Mock GRPC server");
            mock_grpc_server.run().await.unwrap() // Get the port returned by `run`
        })
        .await
        .unwrap();

        println!("Mock GRPC server is running on port {}", port);
        self.port = Some(port.to_string());

        Ok(())
    }

    pub fn create_transaction_stream_config(&self) -> TransactionStreamConfig {
        let data_service_address = format!(
            "http://localhost:{}",
            self.port.as_ref().expect(
                "Port must be set before creating TransactionStreamConfig. Did you init_mock_grpc?"
            )
        );
        // Even if the transactions are not consecutive, for the mock GRPC to return the correct number of transactions,
        // we set request_ending_version to (start version + transactions count - 1)
        let request_ending_version = Some(self.request_start_version + self.transactions_count - 1);
        TransactionStreamConfig {
            indexer_grpc_data_service_address: Url::parse(&data_service_address)
                .expect("Could not parse database url"),
            starting_version: Some(self.request_start_version),
            request_ending_version,
            auth_token: "".to_string(),
            request_name_header: "sdk-testing".to_string(),
            additional_headers: Default::default(),
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
                .join(format!("{}.json", table_name))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_sdk_test_context() {
        let txn = Transaction {
            version: 100,
            ..Transaction::default()
        };

        let txn_bytes = serde_json::to_vec(&txn).unwrap();

        let mut sdk_test_context = SdkTestContext::new(&[&txn_bytes]);
        assert_eq!(sdk_test_context.get_request_start_version(), 100);
        assert_eq!(sdk_test_context.get_transactions_count(), 1);
        assert_eq!(sdk_test_context.transaction_batches.len(), 2);
        assert_eq!(sdk_test_context.get_test_transaction_versions(), vec![100]);

        assert!(sdk_test_context.init_mock_grpc().await.is_ok());
        let transaction_stream_config = sdk_test_context.create_transaction_stream_config();
        assert_eq!(transaction_stream_config.starting_version, Some(100));
        assert_eq!(transaction_stream_config.request_ending_version, Some(100));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_sdk_test_context_genesis() {
        let txn = Transaction {
            version: 1,
            ..Transaction::default()
        };

        let txn_bytes = serde_json::to_vec(&txn).unwrap();

        let mut sdk_test_context = SdkTestContext::new(&[&txn_bytes]);
        assert_eq!(sdk_test_context.get_request_start_version(), 1);
        assert_eq!(sdk_test_context.get_transactions_count(), 1);
        assert_eq!(sdk_test_context.transaction_batches.len(), 1);
        assert_eq!(sdk_test_context.get_test_transaction_versions(), vec![1]);

        assert!(sdk_test_context.init_mock_grpc().await.is_ok());
        let transaction_stream_config = sdk_test_context.create_transaction_stream_config();
        assert_eq!(transaction_stream_config.starting_version, Some(1));
        assert_eq!(transaction_stream_config.request_ending_version, Some(1));
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_sdk_test_context_multiple_txns() {
        let txn1 = Transaction {
            version: 100,
            ..Transaction::default()
        };
        let txn2 = Transaction {
            version: 200,
            ..Transaction::default()
        };

        let mut sdk_test_context = SdkTestContext::new(&[
            &serde_json::to_vec(&txn1).unwrap(),
            &serde_json::to_vec(&txn2).unwrap(),
        ]);
        assert_eq!(sdk_test_context.get_request_start_version(), 100);
        assert_eq!(sdk_test_context.get_transactions_count(), 2);
        assert_eq!(sdk_test_context.transaction_batches.len(), 3);
        assert_eq!(sdk_test_context.get_test_transaction_versions(), vec![
            100, 200
        ]);

        assert!(sdk_test_context.init_mock_grpc().await.is_ok());
        let transaction_stream_config = sdk_test_context.create_transaction_stream_config();
        assert_eq!(transaction_stream_config.starting_version, Some(100));
        assert_eq!(transaction_stream_config.request_ending_version, Some(101));
    }
}

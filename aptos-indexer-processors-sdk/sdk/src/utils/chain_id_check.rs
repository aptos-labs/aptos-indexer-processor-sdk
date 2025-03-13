use super::errors::ProcessorError;
use anyhow::Result;
use aptos_indexer_transaction_stream::{TransactionStream, TransactionStreamConfig};
use async_trait::async_trait;
use tracing::info;

#[async_trait]
pub trait ChainIdChecker {
    /// Save the chain ID to storage. This is used to track the chain ID that's being processed
    /// and prevents the processor from processing the wrong chain.
    async fn save_chain_id(&self, chain_id: u64) -> Result<()>;

    /// Get the chain ID from storage. This is used to track the chain ID that's being processed
    /// and prevents the processor from processing the wrong chain.
    async fn get_chain_id(&self) -> Result<Option<u64>>;
}

/// Verify the chain id from TransactionStream against the database.
pub async fn check_or_update_chain_id<T>(
    transaction_stream_config: &TransactionStreamConfig,
    chain_id_checker: &T,
) -> Result<u64, ProcessorError>
where
    T: ChainIdChecker,
{
    info!("Checking if chain id is correct");
    let maybe_existing_chain_id =
        chain_id_checker
            .get_chain_id()
            .await
            .map_err(|e| ProcessorError::ChainIdCheckError {
                message: format!("Error getting chain id from db: {:?}", e),
            })?;

    let transaction_stream = TransactionStream::new(transaction_stream_config.clone())
        .await
        .map_err(|e| ProcessorError::ChainIdCheckError {
            message: format!("Error initializing transaction stream: {:?}", e),
        })?;
    let grpc_chain_id =
        transaction_stream
            .get_chain_id()
            .await
            .map_err(|e| ProcessorError::ChainIdCheckError {
                message: format!("Error getting chain id from transaction stream: {:?}", e),
            })?;

    match maybe_existing_chain_id {
        Some(chain_id) => {
            if chain_id != grpc_chain_id {
                return Err(ProcessorError::ChainIdCheckError {
                    message: format!(
                        "Wrong chain id detected! Trying to index chain {} now but existing data is for chain {}",
                        grpc_chain_id, chain_id
                    ),
                });
            }

            info!(
                chain_id = chain_id,
                "Chain id matches! Continue to index...",
            );
            Ok(chain_id)
        },
        None => {
            info!(
                chain_id = grpc_chain_id,
                "Saving chain id to db, continue to index..."
            );
            chain_id_checker
                .save_chain_id(grpc_chain_id)
                .await
                .map_err(|e| ProcessorError::ChainIdCheckError {
                    message: format!("Error saving chain id to db: {:?}", e),
                })?;
            Ok(grpc_chain_id)
        },
    }
}

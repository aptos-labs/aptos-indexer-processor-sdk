use crate::{
    postgres::utils::database::ArcDbPool,
    traits::{AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use anyhow::Result;
use aptos_protos::transaction::v1::Transaction;
use async_trait::async_trait;

// Basic process step that runs a process function on each transaction
pub struct BasicProcessorStep<F, Fut>
where
    F: FnMut(Vec<Transaction>, ArcDbPool) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), ProcessorError>> + Send + 'static,
{
    pub process_function: F,
    pub conn_pool: ArcDbPool,
}

#[async_trait]
impl<F, Fut> Processable for BasicProcessorStep<F, Fut>
where
    F: FnMut(Vec<Transaction>, ArcDbPool) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), ProcessorError>> + Send + 'static,
{
    type Input = Vec<Transaction>;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        (self.process_function)(transactions.data, self.conn_pool.clone())
            .await
            .map_err(|e| ProcessorError::ProcessError {
                message: format!("Processing transactionsfailed: {:?}", e),
            })?;
        Ok(Some(TransactionContext {
            data: (), // Stub out data since it's not used in the next step
            metadata: transactions.metadata,
        }))
    }
}

impl<F, Fut> AsyncStep for BasicProcessorStep<F, Fut>
where
    F: FnMut(Vec<Transaction>, ArcDbPool) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), ProcessorError>> + Send + 'static,
{
}

impl<F, Fut> NamedStep for BasicProcessorStep<F, Fut>
where
    F: FnMut(Vec<Transaction>, ArcDbPool) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), ProcessorError>> + Send + 'static,
{
    fn name(&self) -> String {
        "BasicProcessorStep".to_string()
    }
}

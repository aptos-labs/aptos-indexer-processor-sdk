use crate::{
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use std::{marker::PhantomData, sync::Arc};

pub struct ArcifyStep<T: Send + Sync + 'static>
where
    Self: Sized + Send + 'static,
{
    _marker: PhantomData<T>,
}

#[async_trait::async_trait]
impl<T> Processable for ArcifyStep<T>
where
    T: Send + Sync + 'static,
{
    type Input = T;
    type Output = Arc<T>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<T>,
    ) -> Result<Option<TransactionContext<Arc<T>>>, ProcessorError> {
        Ok(Some(TransactionContext {
            data: item.data.into_iter().map(Arc::new).collect(),
            start_version: item.start_version,
            end_version: item.end_version,
            start_transaction_timestamp: item.start_transaction_timestamp,
            end_transaction_timestamp: item.end_transaction_timestamp,
            total_size_in_bytes: item.total_size_in_bytes,
        }))
    }
}

impl<T> AsyncStep for ArcifyStep<T> where T: Send + Sync + 'static {}

impl<T> NamedStep for ArcifyStep<T>
where
    T: Send + Sync + 'static,
{
    fn name(&self) -> String {
        "Arcify".to_string()
    }
}

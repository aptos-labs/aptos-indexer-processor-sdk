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

impl<T: Send + Sync + 'static> ArcifyStep<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T: Send + Sync + 'static> Default for ArcifyStep<T> {
    fn default() -> Self {
        Self::new()
    }
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
        format!("Arcify<{}>", std::any::type_name::<T>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_transaction_context() -> TransactionContext<usize> {
        TransactionContext {
            data: vec![1, 2, 3],
            start_version: 0,
            end_version: 0,
            start_transaction_timestamp: None,
            end_transaction_timestamp: None,
            total_size_in_bytes: 0,
        }
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_arcify_step_process() {
        let mut step = ArcifyStep::<usize>::new();
        let input = generate_transaction_context();

        let result = step.process(input).await.unwrap().unwrap();
        assert_eq!(result.data.len(), 3);
        assert_eq!(*result.data[0], 1);
        assert_eq!(*result.data[1], 2);
        assert_eq!(*result.data[2], 3);
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_arcify_strong_count() {
        let mut step = ArcifyStep::<usize>::new();
        let input = generate_transaction_context();

        let result = step.process(input).await.unwrap().unwrap();
        assert_eq!(Arc::strong_count(&result.data[0]), 1);

        let arc_clone = result.data[0].clone();
        assert_eq!(Arc::strong_count(&arc_clone), 2);

        drop(arc_clone);
        assert_eq!(Arc::strong_count(&result.data[0]), 1);
    }

    #[tokio::test]
    #[allow(clippy::needless_return)]
    async fn test_arcify_ptr_eq() {
        let mut step = ArcifyStep::<usize>::new();
        let input = generate_transaction_context();

        let result = step.process(input).await.unwrap().unwrap();
        let arc_clone = result.data[0].clone();
        assert!(Arc::ptr_eq(&result.data[0], &arc_clone));
    }
}

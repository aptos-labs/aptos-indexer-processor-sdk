use crate::{
    steps::{async_step::AsyncRunType, AsyncStep},
    traits::{NamedStep, Processable},
    types::transaction_context::TransactionContext,
};
use async_trait::async_trait;
use std::marker::PhantomData;

pub struct PassThroughStep<Input: Send + 'static> {
    name: Option<String>,
    _input: PhantomData<Input>,
}

impl<Input: Send + 'static> PassThroughStep<Input> {
    pub fn new() -> Self {
        Self {
            name: None,
            _input: PhantomData,
        }
    }

    pub fn new_named(name: String) -> Self {
        Self {
            name: Some(name),
            _input: PhantomData,
        }
    }
}

impl<Input: Send + 'static> AsyncStep for PassThroughStep<Input> {}

impl<Input: Send + 'static> NamedStep for PassThroughStep<Input> {
    fn name(&self) -> String {
        self.name
            .clone()
            .unwrap_or_else(|| "PassThroughStep".to_string())
    }
}

#[async_trait]
impl<Input: Send + 'static> Processable for PassThroughStep<Input> {
    type Input = Input;
    type Output = Input;
    type RunType = AsyncRunType;

    async fn process(&mut self, item: TransactionContext<Input>) -> TransactionContext<Input> {
        item
    }
}

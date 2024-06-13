use crate::{
    steps::{async_step::AsyncRunType, AsyncStep},
    traits::{NamedStep, Processable},
};
use async_trait::async_trait;
use std::marker::PhantomData;

pub struct PassThroughStep<Input: Send + 'static> {
    _input: PhantomData<Input>,
}

impl<Input: Send + 'static> PassThroughStep<Input> {
    pub fn new() -> Self {
        Self {
            _input: PhantomData,
        }
    }
}

impl<Input: Send + 'static> AsyncStep for PassThroughStep<Input> {}

impl<Input: Send + 'static> NamedStep for PassThroughStep<Input> {
    fn name(&self) -> String {
        "PassThroughStep".to_string()
    }
}

#[async_trait]
impl<Input: Send + 'static> Processable for PassThroughStep<Input> {
    type Input = Input;
    type Output = Input;
    type RunType = AsyncRunType;

    async fn process(&mut self, item: Vec<Input>) -> Vec<Input> {
        item
    }
}

use crate::{
    steps::{pollable_async_step::PollableAsyncRunType, PollableAsyncStep},
    traits::{NamedStep, Processable},
    types::transaction_context::TransactionContext,
};
use async_trait::async_trait;
use std::time::Duration;

pub struct TimedBuffer<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + 'static,
{
    pub internal_buffer: Vec<TransactionContext<Input>>,
    pub poll_interval: Duration,
}

impl<Input> TimedBuffer<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + 'static,
{
    #[allow(dead_code)]
    pub fn new(poll_interval: Duration) -> Self {
        Self {
            internal_buffer: Vec::new(),
            poll_interval,
        }
    }
}

#[async_trait]
impl<Input> Processable for TimedBuffer<Input>
where
    Input: Send + 'static,
{
    type Input = Input;
    type Output = Input;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Input>,
    ) -> Option<TransactionContext<Input>> {
        self.internal_buffer.push(item);
        None // No immediate output
    }
}

#[async_trait]
impl<Input: Send + 'static> PollableAsyncStep for TimedBuffer<Input> {
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(&mut self) -> Option<Vec<TransactionContext<Input>>> {
        Some(std::mem::take(&mut self.internal_buffer))
    }
}

impl<Input: Send + 'static> NamedStep for TimedBuffer<Input> {
    // TODO: oncecell this somehow? Likely in wrapper struct...
    fn name(&self) -> String {
        format!("TimedBuffer: {}", std::any::type_name::<Input>())
    }
}

use crate::{
    traits::{
        pollable_async_step::PollableAsyncRunType, NamedStep, PollableAsyncStep, Processable,
    },
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use anyhow::Result;
use async_trait::async_trait;
use std::time::Duration;

pub struct TimedBufferStep<Input>
where
    Self: Sized + Send + 'static,
    Input: Send + 'static,
{
    pub internal_buffer: Vec<TransactionContext<Input>>,
    pub poll_interval: Duration,
}

impl<Input> TimedBufferStep<Input>
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
impl<Input> Processable for TimedBufferStep<Input>
where
    Input: Send + Sync + 'static,
{
    type Input = Input;
    type Output = Input;
    type RunType = PollableAsyncRunType;

    async fn process(
        &mut self,
        item: TransactionContext<Input>,
    ) -> Result<Option<TransactionContext<Input>>, ProcessorError> {
        self.internal_buffer.push(item);
        Ok(None) // No immediate output
    }

    // Once polling ends, release the remaining items in buffer
    async fn cleanup(
        &mut self,
    ) -> Result<Option<Vec<TransactionContext<Self::Output>>>, ProcessorError> {
        Ok(Some(std::mem::take(&mut self.internal_buffer)))
    }
}

#[async_trait]
impl<Input: Send + Sync + 'static> PollableAsyncStep for TimedBufferStep<Input> {
    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    async fn poll(&mut self) -> Result<Option<Vec<TransactionContext<Input>>>, ProcessorError> {
        Ok(Some(std::mem::take(&mut self.internal_buffer)))
    }
}

impl<Input: Send + 'static> NamedStep for TimedBufferStep<Input> {
    // TODO: oncecell this somehow? Likely in wrapper struct...
    fn name(&self) -> String {
        format!("TimedBuffer: {}", std::any::type_name::<Input>())
    }
}

use crate::traits::runnable_step::RunnableStepType;
use crate::traits::NamedStep;
use async_trait::async_trait;

#[async_trait]
pub trait Processable
where
    Self: NamedStep + Send + Sized + 'static,
{
    type Input: Send + 'static;
    type Output: Send + 'static;
    type RunType: RunnableStepType;

    /// Processes a batch of input items and returns a batch of output items.
    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output>;
}

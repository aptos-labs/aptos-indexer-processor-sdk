use crate::traits::NamedStep;
use async_trait::async_trait;

/// A trait to convince the compiler that different step types are mutually exclusive
pub trait RunnableStepType {}

// This is a dummy implementation for the unit type
impl RunnableStepType for () {}

#[async_trait]
pub trait Processable
where
    Self: NamedStep + Send + Sized + 'static,
{
    type Input: Send + 'static;
    type Output: Send + 'static;
    // This is to convince the compiler of mutual exclusivity of different step impls
    type RunType: RunnableStepType;

    /// Lifecycle methods
    async fn init(&mut self) {}
    async fn cleanup(&mut self) {}

    /// Processes a batch of input items and returns a batch of output items.
    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output>;
}

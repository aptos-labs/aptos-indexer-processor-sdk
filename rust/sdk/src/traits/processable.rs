use crate::traits::NamedStep;
use async_trait::async_trait;

// This is a marker trait to force rust to mutually exclude Processable implementations
pub trait ProcessableStepExclusivityMarker {}
impl ProcessableStepExclusivityMarker for () {}

#[async_trait]
pub trait Processable
    where
        Self: NamedStep + Send + Sized + 'static,
{
    type Input: Send + 'static;
    type Output: Send + 'static;

    type ExclusivityMarker: ProcessableStepExclusivityMarker;

    /// Processes a batch of input items and returns a batch of output items.
    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output>;
}

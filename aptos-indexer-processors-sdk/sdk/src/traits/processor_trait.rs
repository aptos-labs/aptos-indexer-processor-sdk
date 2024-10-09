use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait ProcessorTrait: Send + Sync {
    async fn run_processor(&self) -> anyhow::Result<()>;
}

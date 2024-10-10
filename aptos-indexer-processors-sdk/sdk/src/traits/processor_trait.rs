use async_trait::async_trait;

#[async_trait]
pub trait ProcessorTrait: Send + Sync {
    fn name(&self) -> &'static str;
    async fn run_processor(&self) -> anyhow::Result<()>;
}

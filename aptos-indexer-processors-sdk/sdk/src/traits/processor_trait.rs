use crate::config::indexer_processor_config::{DbConfig, IndexerProcessorConfig};
use async_trait::async_trait;

#[async_trait]
pub trait ProcessorTrait: Send + Sync {
    fn name(&self) -> &'static str;
    async fn run_processor<D: DbConfig>(
        &self,
        config: IndexerProcessorConfig<D>,
    ) -> anyhow::Result<()>;
}

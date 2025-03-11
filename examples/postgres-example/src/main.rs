use anyhow::Result;
use aptos_indexer_processor_sdk::server_framework::ServerArgs;
use clap::Parser;
use events_processor::EventsProcessorConfig;

pub mod events_model;
pub mod events_processor;
pub mod events_step;
#[path = "db/schema.rs"]
pub mod schema;

#[tokio::main]
async fn main() -> Result<()> {
    let args = ServerArgs::parse();
    args.run::<EventsProcessorConfig>(tokio::runtime::Handle::current())
        .await
}

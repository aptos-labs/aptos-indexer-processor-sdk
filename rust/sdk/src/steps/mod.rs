pub mod async_step;
pub mod event_extractor;
pub mod lambda_steps;
pub mod log;
pub mod pollable_async_step;
pub mod step_metrics;
pub mod timed_buffer;
pub mod transaction_stream_step;

use crate::traits::{IntoRunnableStep, RunnableStep};
use aptos_indexer_transaction_stream::{config, TransactionsPBResponse};
// use aptos_indexer_transaction_stream::TransactionStreamConfig;
// Re-export the steps
pub use async_step::{AsyncStep, RunnableAsyncStep};
use bevy_reflect::Reflect;
pub use event_extractor::{EventExtractor, EventExtractorConfig};
pub use log::{Log, LogConfig};
pub use pollable_async_step::{PollableAsyncStep, RunnablePollableStep};
use serde::{Deserialize, Serialize};
pub use timed_buffer::{TimedBuffer, TimedBufferConfig};
pub use transaction_stream_step::TransactionStreamStep;

/*
// Collect all the known step configs into a single enum. This is useful for
// serialization and deserialization.
#[derive(Clone, Debug, Deserialize, Reflect, Serialize)]
pub enum StepConfig {
    EventExtractor(EventExtractorConfig),
    Log(LogConfig),
    // TransactionStream(TransactionStreamConfig),
    TimedBuffer(TimedBufferConfig),
}

pub enum StepEnum {
    EventExtractor(EventExtractor),
    Log(Log),
    // TransactionStream(TransactionStreamStep),
    TimedBuffer(TimedBuffer<TransactionsPBResponse>),
}

impl From<StepConfig> for StepEnum {
    fn from(config: StepConfig) -> Self {
        match config {
            StepConfig::EventExtractor(config) => StepEnum::EventExtractor(EventExtractor::new(config)),
            StepConfig::Log(config) => StepEnum::Log(Log::new(config)),
            // StepConfig::TransactionStream(config) => Step::TransactionStream(TransactionStreamStep::new(config)),
            StepConfig::TimedBuffer(config) => StepEnum::TimedBuffer(TimedBuffer::new(config)),
        }
    }
}
*/

use super::{async_step::AsyncRunType, AsyncStep};
use crate::traits::{NamedStep, Processable};
use aptos_indexer_transaction_stream::TransactionsPBResponse;
use async_trait::async_trait;
use bevy_reflect::Reflect;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Reflect, Serialize)]
pub struct LogConfig {
    pub prefix: String,
}

pub struct Log<T: Send>
where
    Self: Sized + Send + 'static,
{
    config: LogConfig,
    f: Option<fn(&T) -> String>,
}

impl<T: Send> Log<T> {
    #[allow(dead_code)]
    pub fn new(config: LogConfig) -> Self {
        Self { config, f: None }
    }

    pub fn with_lambda(&mut self, f: fn(&T) -> String) -> &mut Self {
        self.f = Some(f);
        self
    }
}

#[async_trait]
impl<T: Send + 'static> Processable for Log<T> {
    type Input = T;
    type Output = T;
    type RunType = AsyncRunType;

    async fn process(&mut self, items: Vec<T>) -> Vec<T> {
        for (index, item) in items.iter().enumerate() {
            let extra = match &self.f {
                Some(f) => format!(": {}", f(item)),
                None => "".to_string(),
            };
            println!("[{}] {}{}", index, self.config.prefix, extra);
        }
        items
    }
}

impl<T: Send> AsyncStep for Log<T> {}

impl<T: Send> NamedStep for Log<T> {
    fn name(&self) -> String {
        "Log".to_string()
    }
}

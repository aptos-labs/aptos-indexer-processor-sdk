use async_trait::async_trait;

use crate::traits::{async_step::AsyncStep, instrumentation::NamedStep};

pub struct SimpleStep;

#[async_trait]
impl AsyncStep for SimpleStep {
    type Input = i64;
    type Output = i64;

    async fn process(&mut self, item: Vec<i64>) -> Vec<i64> {
        item.into_iter().map(|i| i * 2).collect()
    }
}

impl NamedStep for SimpleStep {
    fn name(&self) -> String {
        "SimpleStep".to_string()
    }
}

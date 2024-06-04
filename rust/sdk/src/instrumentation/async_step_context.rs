use crate::traits::{async_step::AsyncStep, instrumentation::NamedStep};
use async_trait::async_trait;
use delegate::delegate;
use kanal::{AsyncReceiver, AsyncSender};

#[derive(Clone, Debug)]
pub struct AsyncStepContext<Step: AsyncStep> {
    pub async_step: Step,
    pub step_name: String,
    /** Named ordering of steps in the pipeline??? Topological sort?
             *          /-> 3-a
             *   1 -> 2 - > 3-b
             *         \-> 3-c  ->  4-c-a
             *                  \-> 4-c-b
             **/
    pub order_name: String,

    pub input_queue_metric: None
}

impl<Step: AsyncStep> AsyncStepContext<Step> {
    pub fn new(async_step: Step, order_name: String) -> Self {
        let step_name = async_step.name();
        Self {
            async_step,
            step_name,
            order_name,
        }
    }
}

impl<Step: AsyncStep> NamedStep for AsyncStepContext<Step> {
    delegate! {
        to self.async_step {
            fn name(&self) -> String;
        }
    }
}

#[async_trait]
impl<Step: AsyncStep> AsyncStep for AsyncStepContext<Step> {
    type Input = Step::Input;
    type Output = Step::Output;

    delegate! {
        to self.async_step {
            fn input_receiver(&mut self) -> &AsyncReceiver<Vec<Step::Input>>;
            fn output_sender(&mut self) -> &AsyncSender<Vec<Step::Output>>;
        }
    }

    async fn process(&mut self, items: Vec<Self::Input>) -> Vec<Self::Output> {
        self.async_step.process(items).await
    }
}

/*
//TODO: error[E0210]: type parameter `Step` must be covered by another type when it appears before the first local type (`AsyncStepContext<Step>`)
impl<Step: AsyncStep> Into<AsyncStepContext<Step>> for Step {
    fn into(self) -> AsyncStepContext<Step> {
        AsyncStepContext::new(self)
    }
}
*/
use crate::{traits::NamedStep, types::transaction_context::TransactionContext};
use instrumented_channel::InstrumentedAsyncReceiver;
use std::marker::PhantomData;
use tokio::task::JoinHandle;

pub trait RunnableStep<Input, Output>: NamedStep
where
    Self: Send + Sized + 'static,
    Input: Send + 'static,
    Output: Send + 'static,
{
    /// Runs the step, forever, with the given input receiver and returns the output receiver and the join handle.
    fn spawn(
        self,
        input_receiver: Option<InstrumentedAsyncReceiver<TransactionContext<Input>>>,
        output_channel_size: usize,
    ) -> (
        InstrumentedAsyncReceiver<TransactionContext<Output>>,
        JoinHandle<()>,
    );

    fn add_input_receiver(
        self,
        input_receiver: InstrumentedAsyncReceiver<TransactionContext<Input>>,
    ) -> RunnableStepWithInputReceiver<Input, Output, Self> {
        RunnableStepWithInputReceiver::new(input_receiver, self)
    }

    fn type_name(&self) -> String {
        <Self as NamedStep>::type_name(self)
    }
}

pub struct RunnableStepWithInputReceiver<Input, Output, Step>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output>,
{
    pub input_receiver: InstrumentedAsyncReceiver<TransactionContext<Input>>,
    pub step: Step,
    _output: PhantomData<Output>,
}

impl<Input, Output, Step> RunnableStepWithInputReceiver<Input, Output, Step>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output>,
{
    pub fn new(
        input_receiver: InstrumentedAsyncReceiver<TransactionContext<Input>>,
        step: Step,
    ) -> Self {
        Self {
            input_receiver,
            step,
            _output: Default::default(),
        }
    }
}

impl<Input, Output, Step> NamedStep for RunnableStepWithInputReceiver<Input, Output, Step>
where
    Input: 'static + Send,
    Output: 'static + Send,
    Step: RunnableStep<Input, Output>,
{
    fn name(&self) -> String {
        self.step.name()
    }

    fn type_name(&self) -> String {
        format!(
            "{} (via RunnableStepWithInputReceiver)",
            RunnableStep::type_name(&self.step)
        )
    }
}

impl<Input, Output, Step> RunnableStep<Input, Output>
    for RunnableStepWithInputReceiver<Input, Output, Step>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output>,
{
    fn spawn(
        self,
        input_receiver: Option<InstrumentedAsyncReceiver<TransactionContext<Input>>>,
        channel_size: usize,
    ) -> (
        InstrumentedAsyncReceiver<TransactionContext<Output>>,
        JoinHandle<()>,
    ) {
        if input_receiver.is_some() {
            panic!("Input receiver already set for {:?}", self.name());
        }
        self.step.spawn(Some(self.input_receiver), channel_size)
    }

    fn add_input_receiver(
        self,
        _input_receiver: InstrumentedAsyncReceiver<TransactionContext<Input>>,
    ) -> RunnableStepWithInputReceiver<Input, Output, Self> {
        panic!("Input receiver already set for {:?}", self.name());
    }
}

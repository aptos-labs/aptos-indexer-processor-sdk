use kanal::AsyncReceiver;
use std::marker::PhantomData;
use tokio::task::JoinHandle;

pub trait RunnableStep<Input, Output>
where
    Self: Send + Sized + 'static,
    Input: Send + 'static,
    Output: Send + 'static,
{
    /// Runs the step, forever, with the given input receiver and returns the output receiver and the join handle.
    fn spawn(
        self,
        input_receiver: Option<AsyncReceiver<Vec<Input>>>,
        channel_size: usize,
    ) -> (AsyncReceiver<Vec<Output>>, JoinHandle<()>);

    fn add_input_receiver(
        self,
        input_receiver: AsyncReceiver<Vec<Input>>,
    ) -> RunnableStepWithInputReceiver<Input, Output, Self> {
        RunnableStepWithInputReceiver::new(input_receiver, self)
    }
}

pub struct RunnableStepWithInputReceiver<Input, Output, Step>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output>,
{
    pub input_receiver: AsyncReceiver<Vec<Input>>,
    pub step: Step,
    _output: PhantomData<Output>,
}

impl<Input, Output, Step> RunnableStepWithInputReceiver<Input, Output, Step>
where
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output>,
{
    pub fn new(input_receiver: AsyncReceiver<Vec<Input>>, step: Step) -> Self {
        Self {
            input_receiver,
            step,
            _output: Default::default(),
        }
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
        input_receiver: Option<AsyncReceiver<Vec<Input>>>,
        channel_size: usize,
    ) -> (AsyncReceiver<Vec<Output>>, JoinHandle<()>) {
        if input_receiver.is_some() {
            panic!("Input receiver already set");
        }
        self.step.spawn(Some(self.input_receiver), channel_size)
    }

    fn add_input_receiver(
        self,
        _input_receiver: AsyncReceiver<Vec<Input>>,
    ) -> RunnableStepWithInputReceiver<Input, Output, Self> {
        panic!("Input receiver already set");
    }
}

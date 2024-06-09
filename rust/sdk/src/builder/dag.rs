use crate::traits::{RunnableStep, RunnableStepWithInputReceiver};
use tokio::task::JoinHandle;


pub fn connect_two_steps<LeftInput, LeftOutput, RightOutput, LeftStep, RightStep>(
    left_step: RunnableStepWithInputReceiver<LeftInput, LeftOutput, LeftStep>,
    right_step: RightStep,
    channel_size: usize,
) -> (
    JoinHandle<()>,
    RunnableStepWithInputReceiver<LeftOutput, RightOutput, RightStep>,
)
    where
        LeftInput: Send + 'static,
        LeftOutput: Send + 'static,
        RightOutput: Send + 'static,
        LeftStep: RunnableStep<LeftInput, LeftOutput> + Send + Sized + 'static,
        RightStep: RunnableStep<LeftOutput, RightOutput> + Send + Sized + 'static,
{
    let RunnableStepWithInputReceiver {
        input_receiver: left_input_receiver,
        step: left_step,
        ..
    } = left_step;

    let (left_output_receiver, left_handle) =
        left_step.spawn(Some(left_input_receiver), channel_size);

    let right_step_with_input_receiver =
        RunnableStepWithInputReceiver::new(left_output_receiver, right_step);

    (left_handle, right_step_with_input_receiver)
}

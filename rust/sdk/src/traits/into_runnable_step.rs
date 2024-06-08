use crate::traits::RunnableStep;

use super::Processable;

pub trait IntoRunnableStep<
    Input,
    Output,
    Step: Processable,
    RunnableType = <Step as Processable>::RunType,
> where
    Self: Send + Sized + 'static,
    Input: Send + 'static,
    Output: Send + 'static,
{
    fn into_runnable_step(self) -> impl RunnableStep<Input, Output>;
}

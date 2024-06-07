use crate::traits::runnable_step::RunnableStep;

pub trait IntoRunnableStep<Input, Output>
    where
        Self: Send + Sized + 'static,
        Input: Send + 'static,
        Output: Send + 'static,
{
    fn into_runnable_step(self) -> impl RunnableStep<Input, Output>;
}
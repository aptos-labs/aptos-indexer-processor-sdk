use crate::traits::RunnableStep;

pub trait IntoRunnableStep<Input, Output>
where
    Self: Send + Sized + 'static,
    Input: Send + 'static,
    Output: Send + 'static,
{
    fn into_runnable_step(self) -> impl RunnableStep<Input, Output>;
}

impl<Input, Output, Step> IntoRunnableStep<Input, Output> for Step
where
    Self: Send + Sized + 'static,
    Input: Send + 'static,
    Output: Send + 'static,
    Step: RunnableStep<Input, Output> + Send + Sized + 'static,
{
    fn into_runnable_step(self) -> impl RunnableStep<Input, Output> {
        self
    }
}

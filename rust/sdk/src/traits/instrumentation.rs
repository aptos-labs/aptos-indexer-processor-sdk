use std::marker::PhantomData;

pub trait NamedStep {
    fn name(&self) -> String;

    fn type_name(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }
}

pub struct StepInstrumentor<Step>
where
    Step: NamedStep + Send + Sized + 'static,
{
    _step: PhantomData<Step>,
}

impl<Step> Default for StepInstrumentor<Step>
where
    Step: NamedStep + Send + Sized + 'static,
{
    fn default() -> Self {
        Self {
            _step: Default::default(),
        }
    }
}

use std::marker::PhantomData;

pub trait NamedStep {
    fn name(&self) -> String;
}


pub struct StepInstrumentor<Step>
    where Step: NamedStep + Send + Sized + 'static, {
    _step: PhantomData<Step>,
}

impl<Step> StepInstrumentor<Step>
    where Step: NamedStep + Send + Sized + 'static, {
    pub fn new() -> Self {
        Self {
            _step: Default::default()
        }
    }
}
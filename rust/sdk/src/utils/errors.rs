use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProcessorError {
    #[error("Step Init Error: {message}")]
    StepInitError { message: String },
    #[error("Process Error: {message}")]
    ProcessError { message: String },
    #[error("Poll Error: {message}")]
    PollError { message: String },
    #[error("DB Store Error: {message}")]
    DBStoreError {
        message: String,
        query: Option<String>,
    },
}

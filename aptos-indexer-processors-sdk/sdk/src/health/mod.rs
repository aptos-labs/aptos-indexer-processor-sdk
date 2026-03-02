//! Health checking utilities for processors.

pub mod core;
pub mod progress;

// Re-export commonly used types.
pub use core::HealthCheck;
pub use progress::{
    ProgressHealthChecker, ProgressHealthConfig, ProgressStatusProvider,
    default_no_progress_threshold_secs,
};

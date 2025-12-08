//! Progress health checking for processors.

use super::core::ReadinessCheck;
use async_trait::async_trait;
use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Configuration for progress health checking.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ProgressHealthConfig {
    /// The number of seconds the processor is allowed to make no progress before it's
    /// considered unhealthy.
    #[serde(default = "default_no_progress_threshold_secs")]
    pub no_progress_threshold_secs: u64,
}

pub const fn default_no_progress_threshold_secs() -> u64 {
    45
}

impl Default for ProgressHealthConfig {
    fn default() -> Self {
        Self {
            no_progress_threshold_secs: default_no_progress_threshold_secs(),
        }
    }
}

/// A trait for providing the processor's last updated timestamp.
///
/// Implement this trait to provide a custom backend for progress health checking.
/// The SDK provides `PostgresProgressStatusProvider` for postgres-backed processors.
#[async_trait]
pub trait ProgressStatusProvider: Send + Sync {
    /// Get the last updated timestamp for the processor.
    /// Returns `None` if the processor hasn't written status yet (e.g., during startup).
    async fn get_last_updated(&self) -> Result<Option<NaiveDateTime>, String>;
}

/// A readiness check that verifies the processor is making forward progress.
///
/// This is generic over the status provider, allowing different backends (postgres, etc.).
pub struct ProgressHealthChecker {
    processor_name: String,
    status_provider: Box<dyn ProgressStatusProvider>,
    no_progress_threshold_secs: u64,
}

impl ProgressHealthChecker {
    pub fn new(
        processor_name: String,
        status_provider: Box<dyn ProgressStatusProvider>,
        config: ProgressHealthConfig,
    ) -> Self {
        Self {
            processor_name,
            status_provider,
            no_progress_threshold_secs: config.no_progress_threshold_secs,
        }
    }
}

#[async_trait]
impl ReadinessCheck for ProgressHealthChecker {
    fn name(&self) -> &str {
        "ProgressHealth"
    }

    async fn is_ready(&self) -> Result<(), String> {
        let last_updated = self.status_provider.get_last_updated().await?;

        match last_updated {
            Some(last_updated) => {
                let now = Utc::now().naive_utc();
                let seconds_since_update = (now - last_updated).num_seconds();
                let timeout = self.no_progress_threshold_secs as i64;

                if seconds_since_update > timeout {
                    warn!(
                        processor = %self.processor_name,
                        seconds_since_update,
                        timeout,
                        "Processor has not made progress within timeout"
                    );
                    Err(format!(
                        "Last updated {} seconds ago (threshold: {} seconds)",
                        seconds_since_update, timeout
                    ))
                } else {
                    Ok(())
                }
            },
            None => {
                // The processor hasn't written to the status table yet.
                // This is okay during startup, so we return healthy.
                Ok(())
            },
        }
    }
}

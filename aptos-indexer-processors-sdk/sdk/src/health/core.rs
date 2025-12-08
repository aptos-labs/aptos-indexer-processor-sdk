use async_trait::async_trait;

/// A trait for implementing custom health checks.
///
/// Implementations can be passed to `register_probes_and_metrics_handler` to add
/// custom health checks to the `/healthz` endpoint.
#[async_trait]
pub trait HealthCheck: Send + Sync {
    /// Returns the name of this health check (used in error messages).
    fn name(&self) -> &str;

    /// Check if this component is healthy.
    /// Returns `Ok(())` if healthy, or `Err(reason)` if not healthy.
    async fn is_healthy(&self) -> Result<(), String>;
}

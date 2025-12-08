use async_trait::async_trait;

/// A trait for implementing custom readiness checks.
///
/// Implementations can be passed to `register_probes_and_metrics_handler` to add
/// custom health checks to the `/healthz` endpoint.
#[async_trait]
pub trait ReadinessCheck: Send + Sync {
    /// Returns the name of this readiness check (used in error messages).
    fn name(&self) -> &str;

    /// Check if this component is ready.
    /// Returns `Ok(())` if ready, or `Err(reason)` if not ready.
    async fn is_ready(&self) -> Result<(), String>;
}

use crate::utils::additional_headers::AdditionalHeaders;
use aptos_transaction_filter::BooleanTransactionFilter;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_retry::strategy::ExponentialBackoff;
use url::Url;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub address: Url,
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Whether this is the primary endpoint. Set programmatically, not from config.
    #[serde(skip, default)]
    pub is_primary: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionStreamConfig {
    pub indexer_grpc_data_service_address: Url,
    pub starting_version: Option<u64>,
    pub request_ending_version: Option<u64>,
    #[serde(default)]
    pub auth_token: Option<String>,
    pub request_name_header: String,
    #[serde(default)]
    pub additional_headers: AdditionalHeaders,
    #[serde(default = "TransactionStreamConfig::default_indexer_grpc_http2_ping_interval")]
    pub indexer_grpc_http2_ping_interval_secs: u64,
    #[serde(default = "TransactionStreamConfig::default_indexer_grpc_http2_ping_timeout")]
    pub indexer_grpc_http2_ping_timeout_secs: u64,
    #[serde(default = "TransactionStreamConfig::default_indexer_grpc_reconnection_timeout")]
    pub indexer_grpc_reconnection_timeout_secs: u64,
    #[serde(default = "TransactionStreamConfig::default_indexer_grpc_response_item_timeout")]
    pub indexer_grpc_response_item_timeout_secs: u64,
    #[serde(default = "TransactionStreamConfig::default_indexer_grpc_reconnection_max_retries")]
    pub indexer_grpc_reconnection_max_retries: u64,
    /// Initial delay (in ms) for exponential backoff on reconnection. Each subsequent retry
    /// doubles this value, up to `reconnection_max_delay_ms`.
    #[serde(
        default = "TransactionStreamConfig::default_indexer_grpc_reconnection_initial_delay_ms"
    )]
    pub indexer_grpc_reconnection_initial_delay_ms: u64,
    /// Maximum delay (in ms) that the exponential backoff can grow to.
    #[serde(
        default = "TransactionStreamConfig::default_indexer_grpc_reconnection_max_delay_ms"
    )]
    pub indexer_grpc_reconnection_max_delay_ms: u64,
    /// Random jitter applied to each backoff delay, expressed as a ± percentage (e.g. 20 means
    /// the actual delay will be within ±20% of the computed backoff).
    #[serde(
        default = "TransactionStreamConfig::default_indexer_grpc_reconnection_jitter_percent"
    )]
    pub indexer_grpc_reconnection_jitter_percent: u64,
    #[serde(default)]
    pub transaction_filter: Option<BooleanTransactionFilter>,
    /// Backup gRPC endpoints for failover. Tried in order after primary fails.
    #[serde(default)]
    pub backup_endpoints: Vec<Endpoint>,
}

impl TransactionStreamConfig {
    pub const fn indexer_grpc_http2_ping_interval(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_interval_secs)
    }

    pub const fn indexer_grpc_http2_ping_timeout(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_http2_ping_timeout_secs)
    }

    pub const fn indexer_grpc_reconnection_timeout(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_reconnection_timeout_secs)
    }

    pub const fn indexer_grpc_response_item_timeout(&self) -> Duration {
        Duration::from_secs(self.indexer_grpc_response_item_timeout_secs)
    }

    /// Indexer GRPC http2 ping interval in seconds. Defaults to 30.
    /// Tonic ref: https://docs.rs/tonic/latest/tonic/transport/channel/struct.Endpoint.html#method.http2_keep_alive_interval
    pub const fn default_indexer_grpc_http2_ping_interval() -> u64 {
        30
    }

    /// Indexer GRPC http2 ping timeout in seconds. Defaults to 10.
    pub const fn default_indexer_grpc_http2_ping_timeout() -> u64 {
        10
    }

    /// Default timeout for establishing a grpc connection. Defaults to 5 seconds.
    pub const fn default_indexer_grpc_reconnection_timeout() -> u64 {
        5
    }

    /// Default timeout for receiving an item from grpc stream. Defaults to 60 seconds.
    pub const fn default_indexer_grpc_response_item_timeout() -> u64 {
        60
    }

    /// Default max retries for reconnecting to grpc. Defaults to 10.
    pub const fn default_indexer_grpc_reconnection_max_retries() -> u64 {
        10
    }

    /// Default initial delay for reconnection backoff in milliseconds. Defaults to 1000ms.
    pub const fn default_indexer_grpc_reconnection_initial_delay_ms() -> u64 {
        1000
    }

    /// Default maximum delay cap for reconnection backoff in milliseconds. Defaults to 30s.
    pub const fn default_indexer_grpc_reconnection_max_delay_ms() -> u64 {
        30_000
    }

    /// Default jitter as a percentage (±) applied to each backoff delay. Defaults to 20%.
    pub const fn default_indexer_grpc_reconnection_jitter_percent() -> u64 {
        20
    }

    /// Build an iterator of backoff delays using `tokio-retry`'s `ExponentialBackoff`,
    /// with configurable ±jitter applied on top. Each successive delay doubles the
    /// previous one (capped at `max_delay_ms`), then jitter is added.
    pub fn reconnection_backoff_iter(&self) -> impl Iterator<Item = Duration> {
        let jitter_pct = self.indexer_grpc_reconnection_jitter_percent;
        ExponentialBackoff::from_millis(self.indexer_grpc_reconnection_initial_delay_ms)
            .max_delay(Duration::from_millis(self.indexer_grpc_reconnection_max_delay_ms))
            .map(move |d| apply_jitter(d, jitter_pct))
    }

    /// Total number of endpoints (primary + backups)
    pub fn total_endpoints(&self) -> usize {
        1 + self.backup_endpoints.len()
    }

    /// Returns all endpoints in order: primary first, then backups.
    pub fn get_endpoints(&self) -> Vec<Endpoint> {
        let mut endpoints = Vec::with_capacity(1 + self.backup_endpoints.len());
        endpoints.push(Endpoint {
            address: self.indexer_grpc_data_service_address.clone(),
            auth_token: self.auth_token.clone(),
            is_primary: true,
        });
        for backup in &self.backup_endpoints {
            endpoints.push(Endpoint {
                address: backup.address.clone(),
                auth_token: backup.auth_token.clone(),
                is_primary: false,
            });
        }
        endpoints
    }
}

/// Apply ±jitter_percent jitter to a duration. Uses subsecond nanoseconds from the
/// system clock as a lightweight entropy source so we avoid adding a `rand` dependency.
fn apply_jitter(d: Duration, jitter_percent: u64) -> Duration {
    if jitter_percent == 0 {
        return d;
    }
    let base_ms = d.as_millis() as f64;
    let fraction = jitter_percent as f64 / 100.0;
    let jitter_range = base_ms * fraction;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as f64;
    // Map nanos (0..1e9) to [-1.0, 1.0).
    let jitter = (nanos / 500_000_000.0 - 1.0) * jitter_range;
    Duration::from_millis((base_ms + jitter).max(0.0) as u64)
}

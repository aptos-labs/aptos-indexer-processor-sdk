use crate::utils::additional_headers::AdditionalHeaders;
use aptos_transaction_filter::BooleanTransactionFilter;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
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
    /// Whether to apply random jitter to backoff delays. Jitter adds a random duration
    /// between 0 and the computed delay, spreading out reconnection attempts across
    /// processors ("full jitter" strategy). Defaults to true.
    #[serde(
        default = "TransactionStreamConfig::default_indexer_grpc_reconnection_enable_jitter"
    )]
    pub indexer_grpc_reconnection_enable_jitter: bool,
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

    /// Whether to apply jitter to backoff delays. Defaults to true.
    pub const fn default_indexer_grpc_reconnection_enable_jitter() -> bool {
        true
    }

    /// Build an iterator of backoff delays using `tokio-retry`'s `ExponentialBackoff`.
    /// Each successive delay doubles the previous one (capped at `max_delay_ms`).
    /// When jitter is enabled, each delay is randomized to a value between 0 and the
    /// computed delay (the "full jitter" strategy from `tokio-retry`).
    pub fn reconnection_backoff_iter(&self) -> Box<dyn Iterator<Item = Duration> + Send> {
        let backoff = ExponentialBackoff::from_millis(
            self.indexer_grpc_reconnection_initial_delay_ms,
        )
        .max_delay(Duration::from_millis(self.indexer_grpc_reconnection_max_delay_ms));

        if self.indexer_grpc_reconnection_enable_jitter {
            Box::new(backoff.map(jitter))
        } else {
            Box::new(backoff)
        }
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

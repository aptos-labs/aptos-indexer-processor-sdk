use crate::utils::additional_headers::AdditionalHeaders;
use aptos_transaction_filter::BooleanTransactionFilter;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use url::Url;

/// Configuration for a backup gRPC endpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackupEndpoint {
    /// The gRPC endpoint URL
    pub address: Url,
    /// Optional auth token for this endpoint. If None, uses the primary endpoint's token.
    #[serde(default)]
    pub auth_token: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionStreamConfig {
    pub indexer_grpc_data_service_address: Url,
    pub starting_version: Option<u64>,
    pub request_ending_version: Option<u64>,
    pub auth_token: String,
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
    #[serde(default)]
    pub transaction_filter: Option<BooleanTransactionFilter>,
    /// Backup gRPC endpoints for failover. Tried in order after primary fails.
    #[serde(default)]
    pub backup_endpoints: Vec<BackupEndpoint>,
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

    /// Default max retries for reconnecting to grpc. Defaults to 5.
    pub const fn default_indexer_grpc_reconnection_max_retries() -> u64 {
        5
    }

    /// Total number of endpoints (primary + backups)
    pub fn total_endpoints(&self) -> usize {
        1 + self.backup_endpoints.len()
    }

    /// Get address for endpoint at index (0 = primary, 1+ = backup)
    pub fn endpoint_address(&self, index: usize) -> Option<&Url> {
        if index == 0 {
            Some(&self.indexer_grpc_data_service_address)
        } else {
            self.backup_endpoints.get(index - 1).map(|b| &b.address)
        }
    }

    /// Get auth token for endpoint at index (0 = primary, 1+ = backup).
    /// Backup endpoints inherit primary token if not specified.
    pub fn endpoint_auth_token(&self, index: usize) -> Option<&str> {
        if index == 0 {
            Some(&self.auth_token)
        } else {
            self.backup_endpoints.get(index - 1).map(|b| {
                b.auth_token.as_deref().unwrap_or(&self.auth_token)
            })
        }
    }
}

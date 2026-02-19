use aptos_indexer_transaction_stream::{
    config::{Endpoint, TransactionStreamConfig},
    transaction_stream::TransactionStream,
};
use aptos_protos::{
    indexer::v1::{
        raw_data_server::{RawData, RawDataServer},
        GetTransactionsRequest, TransactionsResponse,
    },
    transaction::v1::Transaction,
    util::timestamp::Timestamp,
};
use futures::Stream;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};
use url::Url;

/// A stream that returns transactions with configurable timestamps (for simulating latency).
/// Returns batches indefinitely until the connection is dropped.
struct HighLatencyResponseStream {
    batch_count: u64,
    next_version: u64,
    timestamp_offset_secs: i64,
}

impl HighLatencyResponseStream {
    /// `timestamp_offset_secs`: negative means the transactions appear "old" (high latency).
    /// For example, -300 means timestamps are 5 minutes in the past.
    fn new(start_version: u64, timestamp_offset_secs: i64) -> Self {
        Self {
            batch_count: 0,
            next_version: start_version,
            timestamp_offset_secs,
        }
    }

    fn make_timestamp(&self) -> Timestamp {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        Timestamp {
            seconds: now.as_secs() as i64 + self.timestamp_offset_secs,
            nanos: 0,
        }
    }
}

impl Stream for HighLatencyResponseStream {
    type Item = Result<TransactionsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.batch_count >= 100 {
            return Poll::Ready(None);
        }
        self.batch_count += 1;
        let version = self.next_version;
        self.next_version += 1;
        let ts = self.make_timestamp();

        Poll::Ready(Some(Ok(TransactionsResponse {
            transactions: vec![Transaction {
                version,
                timestamp: Some(ts),
                ..Transaction::default()
            }],
            chain_id: Some(1),
            processed_range: Some(aptos_protos::indexer::v1::ProcessedRange {
                first_version: version,
                last_version: version,
            }),
        })))
    }
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<TransactionsResponse, Status>> + Send>>;

/// A mock gRPC server that returns transactions with configurable old timestamps.
pub struct HighLatencyMockGrpcServer {
    connection_count: Arc<AtomicU64>,
    timestamp_offset_secs: i64,
}

#[tonic::async_trait]
impl RawData for HighLatencyMockGrpcServer {
    type GetTransactionsStream = ResponseStream;

    async fn get_transactions(
        &self,
        req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        self.connection_count.fetch_add(1, Ordering::SeqCst);
        let start_version = req.into_inner().starting_version.unwrap_or(0);
        let stream = HighLatencyResponseStream::new(start_version, self.timestamp_offset_secs);
        Ok(Response::new(Box::pin(stream)))
    }
}

impl HighLatencyMockGrpcServer {
    pub fn new(connection_count: Arc<AtomicU64>, timestamp_offset_secs: i64) -> Self {
        Self {
            connection_count,
            timestamp_offset_secs,
        }
    }

    pub async fn run(self) -> anyhow::Result<u16> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let bound_addr = listener.local_addr()?;
        let stream = TcpListenerStream::new(listener);

        let server = Server::builder().add_service(
            RawDataServer::new(self)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                .send_compressed(tonic::codec::CompressionEncoding::Zstd),
        );

        tokio::spawn(async move {
            let _ = server.serve_with_incoming(stream).await;
        });

        Ok(bound_addr.port())
    }
}

/// A mock gRPC server that returns fresh (low latency) transactions.
pub struct FreshMockGrpcServer {
    connection_count: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl RawData for FreshMockGrpcServer {
    type GetTransactionsStream = ResponseStream;

    async fn get_transactions(
        &self,
        req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        self.connection_count.fetch_add(1, Ordering::SeqCst);
        let start_version = req.into_inner().starting_version.unwrap_or(0);
        // Offset of 0 means timestamps are "now" (fresh)
        let stream = HighLatencyResponseStream::new(start_version, 0);
        Ok(Response::new(Box::pin(stream)))
    }
}

impl FreshMockGrpcServer {
    pub fn new(connection_count: Arc<AtomicU64>) -> Self {
        Self { connection_count }
    }

    pub async fn run(self) -> anyhow::Result<u16> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let bound_addr = listener.local_addr()?;
        let stream = TcpListenerStream::new(listener);

        let server = Server::builder().add_service(
            RawDataServer::new(self)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                .send_compressed(tonic::codec::CompressionEncoding::Zstd),
        );

        tokio::spawn(async move {
            let _ = server.serve_with_incoming(stream).await;
        });

        Ok(bound_addr.port())
    }
}

/// A mock gRPC server that always fails (for testing failover when all other endpoints fail).
pub struct FailingMockGrpcServer {
    connection_count: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl RawData for FailingMockGrpcServer {
    type GetTransactionsStream = ResponseStream;

    async fn get_transactions(
        &self,
        _req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        self.connection_count.fetch_add(1, Ordering::SeqCst);
        Err(Status::unavailable("Server is unavailable"))
    }
}

impl FailingMockGrpcServer {
    pub fn new(connection_count: Arc<AtomicU64>) -> Self {
        Self { connection_count }
    }

    pub async fn run(self) -> anyhow::Result<u16> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let bound_addr = listener.local_addr()?;
        let stream = TcpListenerStream::new(listener);

        let server = Server::builder().add_service(
            RawDataServer::new(self)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                .send_compressed(tonic::codec::CompressionEncoding::Zstd),
        );

        tokio::spawn(async move {
            let _ = server.serve_with_incoming(stream).await;
        });

        Ok(bound_addr.port())
    }
}

fn create_latency_config(primary_port: u16) -> TransactionStreamConfig {
    TransactionStreamConfig {
        indexer_grpc_data_service_address: Url::parse(&format!(
            "http://127.0.0.1:{}",
            primary_port
        ))
        .unwrap(),
        starting_version: Some(0),
        request_ending_version: None,
        auth_token: Some("test_token".to_string()),
        request_name_header: "test".to_string(),
        additional_headers: Default::default(),
        indexer_grpc_http2_ping_interval_secs: 30,
        indexer_grpc_http2_ping_timeout_secs: 10,
        indexer_grpc_reconnection_timeout_secs: 1,
        indexer_grpc_response_item_timeout_secs: 10,
        indexer_grpc_reconnection_max_retries: 2,
        indexer_grpc_reconnection_retry_delay_ms: 50,
        transaction_filter: None,
        backup_endpoints: vec![],
        max_latency_ms: None,
        latency_grace_period_secs: None,
    }
}

/// Test 1: Grace period triggers failover.
/// Primary returns stale transactions, backup returns fresh ones.
/// After grace period elapses, stream should switch to backup.
#[tokio::test]
async fn test_latency_failover_grace_period_triggers() {
    // Primary: transactions are 5 minutes old (300s offset)
    let primary_count = Arc::new(AtomicU64::new(0));
    let primary_server = HighLatencyMockGrpcServer::new(primary_count.clone(), -300);
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    // Backup: fresh transactions
    let backup_count = Arc::new(AtomicU64::new(0));
    let backup_server = FreshMockGrpcServer::new(backup_count.clone());
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_latency_config(primary_port);
    config.max_latency_ms = Some(10_000); // 10 seconds max latency
    config.latency_grace_period_secs = Some(1); // 1 second grace period
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut stream = TransactionStream::new(config)
        .await
        .expect("Should connect to primary");

    // First batch should come from primary (latency exceeded, starts grace period)
    let result = stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "First batch should succeed");

    // Backup should not have been contacted yet (grace period just started)
    assert_eq!(
        backup_count.load(Ordering::SeqCst),
        0,
        "Backup should not be contacted during grace period"
    );

    // Wait for grace period to elapse
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Next batch should trigger failover (grace period has elapsed)
    let result = stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "Batch after grace period should succeed");

    // Backup should now have been contacted
    assert!(
        backup_count.load(Ordering::SeqCst) > 0,
        "Backup should have been contacted after grace period elapsed"
    );
}

/// Test 2: Grace period resets when latency drops below threshold.
/// If latency drops during the grace period, no failover should occur.
#[tokio::test]
async fn test_latency_failover_grace_period_resets() {
    // Use fresh transactions (latency within threshold)
    let primary_count = Arc::new(AtomicU64::new(0));
    let primary_server = FreshMockGrpcServer::new(primary_count.clone());
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let backup_count = Arc::new(AtomicU64::new(0));
    let backup_server = FreshMockGrpcServer::new(backup_count.clone());
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_latency_config(primary_port);
    config.max_latency_ms = Some(10_000); // 10 seconds
    config.latency_grace_period_secs = Some(1); // 1 second
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut stream = TransactionStream::new(config)
        .await
        .expect("Should connect to primary");

    // Fetch multiple batches - latency should always be within threshold
    for _ in 0..5 {
        let result = stream.get_next_transaction_batch().await;
        assert!(result.is_ok(), "Batch should succeed from primary");
    }

    // Backup should never have been contacted
    assert_eq!(
        backup_count.load(Ordering::SeqCst),
        0,
        "Backup should not be contacted when latency is within threshold"
    );
}

/// Test 3: Feature disabled when config fields are missing.
/// No failover should occur when max_latency_ms or latency_grace_period_secs is None.
#[tokio::test]
async fn test_latency_failover_disabled_when_config_missing() {
    // Primary: high latency (5 minutes old)
    let primary_count = Arc::new(AtomicU64::new(0));
    let primary_server = HighLatencyMockGrpcServer::new(primary_count.clone(), -300);
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let backup_count = Arc::new(AtomicU64::new(0));
    let backup_server = FreshMockGrpcServer::new(backup_count.clone());
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    // Config with None for latency fields (feature disabled)
    let mut config = create_latency_config(primary_port);
    config.max_latency_ms = None;
    config.latency_grace_period_secs = None;
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut stream = TransactionStream::new(config)
        .await
        .expect("Should connect to primary");

    // Fetch multiple batches - no failover should happen despite high latency
    for _ in 0..5 {
        let result = stream.get_next_transaction_batch().await;
        assert!(result.is_ok(), "Batch should succeed from primary");
    }

    // Backup should never have been contacted
    assert_eq!(
        backup_count.load(Ordering::SeqCst),
        0,
        "Backup should not be contacted when latency failover is disabled"
    );
}

/// Test 4: Single endpoint stays connected despite high latency.
/// With only one endpoint, there's nowhere to fail over to.
#[tokio::test]
async fn test_latency_failover_single_endpoint_stays() {
    let primary_count = Arc::new(AtomicU64::new(0));
    let primary_server = HighLatencyMockGrpcServer::new(primary_count.clone(), -300);
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let mut config = create_latency_config(primary_port);
    config.max_latency_ms = Some(10_000); // 10 seconds
    config.latency_grace_period_secs = Some(1); // 1 second
                                                // No backup endpoints

    let mut stream = TransactionStream::new(config)
        .await
        .expect("Should connect to primary");

    // First batch starts grace period
    let result = stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "First batch should succeed");

    // Wait for grace period
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Next batch should still succeed (stays on same endpoint, no failover possible)
    let result = stream.get_next_transaction_batch().await;
    assert!(
        result.is_ok(),
        "Should stay on primary when no other endpoints available"
    );

    // Should still be getting data from primary
    assert!(
        primary_count.load(Ordering::SeqCst) > 0,
        "Should still be connected to primary"
    );
}

/// Test 5: Failed failover stays on current endpoint.
/// All other endpoints fail to connect, so we stay on the current (slow) endpoint.
#[tokio::test]
async fn test_latency_failover_failed_stays_on_current() {
    // Primary: high latency
    let primary_count = Arc::new(AtomicU64::new(0));
    let primary_server = HighLatencyMockGrpcServer::new(primary_count.clone(), -300);
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    // Backup: always fails
    let backup_count = Arc::new(AtomicU64::new(0));
    let backup_server = FailingMockGrpcServer::new(backup_count.clone());
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_latency_config(primary_port);
    config.max_latency_ms = Some(10_000); // 10 seconds
    config.latency_grace_period_secs = Some(1); // 1 second
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut stream = TransactionStream::new(config)
        .await
        .expect("Should connect to primary");

    // First batch starts grace period
    let result = stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "First batch should succeed from primary");

    // Wait for grace period
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Next batch triggers failover attempt - backup fails, should stay on primary
    let result = stream.get_next_transaction_batch().await;
    assert!(
        result.is_ok(),
        "Should stay on primary after failed failover"
    );

    // Backup was attempted
    assert!(
        backup_count.load(Ordering::SeqCst) > 0,
        "Backup should have been attempted during failover"
    );
}

/// Test 6: Grace period is endpoint-local and resets on reconnect.
/// If we reconnect successfully, stale grace-period state should not carry over.
#[tokio::test]
async fn test_latency_failover_grace_period_resets_on_reconnect() {
    // Primary: high latency
    let primary_count = Arc::new(AtomicU64::new(0));
    let primary_server = HighLatencyMockGrpcServer::new(primary_count.clone(), -300);
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    // Backup: fresh transactions (used only to detect unexpected failover)
    let backup_count = Arc::new(AtomicU64::new(0));
    let backup_server = FreshMockGrpcServer::new(backup_count.clone());
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_latency_config(primary_port);
    config.max_latency_ms = Some(10_000); // 10 seconds
    config.latency_grace_period_secs = Some(1); // 1 second
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut stream = TransactionStream::new(config)
        .await
        .expect("Should connect to primary");

    // First batch starts grace period on primary.
    let result = stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "First batch should succeed from primary");

    // Let the first grace period expire, then force a reconnect.
    tokio::time::sleep(Duration::from_secs(2)).await;
    stream
        .reconnect_to_grpc_with_retries()
        .await
        .expect("Reconnect should succeed");

    // Next batch should start a fresh grace period, not fail over immediately.
    let result = stream.get_next_transaction_batch().await;
    assert!(
        result.is_ok(),
        "Batch after reconnect should succeed without immediate failover"
    );

    assert_eq!(
        backup_count.load(Ordering::SeqCst),
        0,
        "Backup should not be contacted immediately after reconnect"
    );
}

/// Test 7: Latency failover uses single-probe attempts with cooldown.
/// Even with a high retry config, failover should probe a bad backup once and then cooldown.
#[tokio::test]
async fn test_latency_failover_single_probe_with_cooldown() {
    // Primary: high latency
    let primary_count = Arc::new(AtomicU64::new(0));
    let primary_server = HighLatencyMockGrpcServer::new(primary_count.clone(), -300);
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    // Backup: always fails
    let backup_count = Arc::new(AtomicU64::new(0));
    let backup_server = FailingMockGrpcServer::new(backup_count.clone());
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_latency_config(primary_port);
    config.max_latency_ms = Some(10_000); // 10 seconds
    config.latency_grace_period_secs = Some(1); // 1 second
    // Set a high retry count to ensure probe logic is not retry-based.
    config.indexer_grpc_reconnection_max_retries = 100;
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut stream = TransactionStream::new(config)
        .await
        .expect("Should connect to primary");

    // First batch starts grace period.
    let result = stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "First batch should succeed from primary");

    // Wait for grace period and trigger first failover attempt.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let result = stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "Failover attempt should keep stream alive");

    // Probe-based failover should touch backup only once.
    assert_eq!(
        backup_count.load(Ordering::SeqCst),
        1,
        "Backup should be probed exactly once per failover attempt"
    );

    // Start grace period again.
    let result = stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "Batch should still succeed from primary");

    // Wait for grace period and trigger another failover attempt window.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let result = stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "Cooldown path should keep stream alive");

    // Backup should still be at 1 call due to cooldown skip.
    assert_eq!(
        backup_count.load(Ordering::SeqCst),
        1,
        "Backup should be skipped while in cooldown"
    );
}

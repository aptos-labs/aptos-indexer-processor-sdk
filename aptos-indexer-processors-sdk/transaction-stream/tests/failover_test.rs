use aptos_indexer_transaction_stream::{
    config::{Endpoint, TransactionStreamConfig},
    transaction_stream::TransactionStream,
};
use aptos_protos::indexer::v1::{
    raw_data_server::{RawData, RawDataServer},
    GetTransactionsRequest, TransactionsResponse,
};
use futures::Stream;
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};
use url::Url;

/// A stream that returns transactions successfully
struct WorkingResponseStream {
    done: bool,
    start_version: u64,
    end_version: u64,
}

impl WorkingResponseStream {
    fn new(start_version: u64, end_version: u64) -> Self {
        Self {
            done: false,
            start_version,
            end_version,
        }
    }
}

impl Stream for WorkingResponseStream {
    type Item = Result<TransactionsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        self.done = true;
        Poll::Ready(Some(Ok(TransactionsResponse {
            transactions: vec![],
            chain_id: Some(1),
            processed_range: Some(aptos_protos::indexer::v1::ProcessedRange {
                first_version: self.start_version,
                last_version: self.end_version,
            }),
        })))
    }
}

type ResponseStream = Pin<Box<dyn Stream<Item = Result<TransactionsResponse, Status>> + Send>>;

/// A mock gRPC server that always fails
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

/// A mock gRPC server that works after N connections (simulating a backup that works)
pub struct WorkingMockGrpcServer {
    connection_count: Arc<AtomicU64>,
    start_version: u64,
}

#[tonic::async_trait]
impl RawData for WorkingMockGrpcServer {
    type GetTransactionsStream = ResponseStream;

    async fn get_transactions(
        &self,
        _req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        self.connection_count.fetch_add(1, Ordering::SeqCst);
        let stream = WorkingResponseStream::new(self.start_version, self.start_version + 99);
        Ok(Response::new(Box::pin(stream)))
    }
}

impl WorkingMockGrpcServer {
    pub fn new(connection_count: Arc<AtomicU64>, start_version: u64) -> Self {
        Self {
            connection_count,
            start_version,
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

fn create_base_config(primary_port: u16) -> TransactionStreamConfig {
    TransactionStreamConfig {
        indexer_grpc_data_service_address: Url::parse(&format!("http://127.0.0.1:{}", primary_port))
            .unwrap(),
        starting_version: Some(0),
        request_ending_version: None,
        auth_token: Some("primary_token".to_string()),
        request_name_header: "test".to_string(),
        additional_headers: Default::default(),
        indexer_grpc_http2_ping_interval_secs: 30,
        indexer_grpc_http2_ping_timeout_secs: 10,
        indexer_grpc_reconnection_timeout_secs: 1,
        indexer_grpc_response_item_timeout_secs: 2,
        indexer_grpc_reconnection_max_retries: 2,
        indexer_grpc_reconnection_initial_delay_ms: 50,
        indexer_grpc_reconnection_max_delay_ms: 30_000,
        indexer_grpc_reconnection_enable_jitter: false,
        transaction_filter: None,
        backup_endpoints: vec![],
    }
}

#[tokio::test]
async fn test_failover_to_backup_endpoint() {
    // Primary server always fails
    let primary_connection_count = Arc::new(AtomicU64::new(0));
    let primary_server = FailingMockGrpcServer::new(primary_connection_count.clone());
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    // Backup server works
    let backup_connection_count = Arc::new(AtomicU64::new(0));
    let backup_server = WorkingMockGrpcServer::new(backup_connection_count.clone(), 0);
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    // Create config with failing primary and working backup
    let mut config = create_base_config(primary_port);
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    // TransactionStream::new tries primary (fails), then backup (works)
    let mut transaction_stream = TransactionStream::new(config)
        .await
        .expect("Should connect via backup after primary fails");

    // Verify primary was attempted and failed
    let primary_attempts = primary_connection_count.load(Ordering::SeqCst);
    assert!(
        primary_attempts > 0,
        "Primary should have been attempted: {}",
        primary_attempts
    );

    // Verify backup was used
    let backup_attempts = backup_connection_count.load(Ordering::SeqCst);
    assert!(
        backup_attempts > 0,
        "Backup should have been used after primary failed: {}",
        backup_attempts
    );

    // First batch should succeed from backup
    let result = transaction_stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "First batch should succeed from backup");

    println!(
        "Test completed. Primary attempts: {}, Backup attempts: {}",
        primary_attempts, backup_attempts
    );
}

#[tokio::test]
async fn test_all_endpoints_exhausted() {
    // Both primary and backup servers fail
    let primary_connection_count = Arc::new(AtomicU64::new(0));
    let primary_server = FailingMockGrpcServer::new(primary_connection_count.clone());
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let backup_connection_count = Arc::new(AtomicU64::new(0));
    let backup_server = FailingMockGrpcServer::new(backup_connection_count.clone());
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    // Create config with failing backup endpoint
    let mut config = create_base_config(primary_port);
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    // Attempt to create stream should fail after exhausting all endpoints
    let result = TransactionStream::new(config).await;
    assert!(result.is_err(), "Should fail when all endpoints exhausted");

    // Both servers should have been tried
    let primary_attempts = primary_connection_count.load(Ordering::SeqCst);
    let backup_attempts = backup_connection_count.load(Ordering::SeqCst);

    println!(
        "Primary attempts: {}, Backup attempts: {}",
        primary_attempts, backup_attempts
    );

    assert!(
        primary_attempts > 0,
        "Primary should have been attempted: {}",
        primary_attempts
    );
    assert!(
        backup_attempts > 0,
        "Backup should have been attempted after primary failed: {}",
        backup_attempts
    );
}

#[tokio::test]
async fn test_config_endpoint_helpers() {
    let config = TransactionStreamConfig {
        indexer_grpc_data_service_address: Url::parse("http://primary.example.com").unwrap(),
        starting_version: Some(0),
        request_ending_version: None,
        auth_token: Some("primary_token".to_string()),
        request_name_header: "test".to_string(),
        additional_headers: Default::default(),
        indexer_grpc_http2_ping_interval_secs: 30,
        indexer_grpc_http2_ping_timeout_secs: 10,
        indexer_grpc_reconnection_timeout_secs: 5,
        indexer_grpc_response_item_timeout_secs: 60,
        indexer_grpc_reconnection_max_retries: 5,
        indexer_grpc_reconnection_initial_delay_ms: 1000,
        indexer_grpc_reconnection_max_delay_ms: 30_000,
        indexer_grpc_reconnection_enable_jitter: true,
        transaction_filter: None,
        backup_endpoints: vec![
            Endpoint {
                address: Url::parse("http://backup1.example.com").unwrap(),
                auth_token: None,
                is_primary: false,
            },
            Endpoint {
                address: Url::parse("http://backup2.example.com").unwrap(),
                auth_token: Some("backup2_token".to_string()),
                is_primary: false,
            },
        ],
    };

    // Test get_endpoints
    let endpoints = config.get_endpoints();
    assert_eq!(endpoints.len(), 3);

    assert_eq!(endpoints[0].address.as_str(), "http://primary.example.com/");
    assert_eq!(endpoints[0].auth_token.as_deref(), Some("primary_token"));

    assert_eq!(endpoints[1].address.as_str(), "http://backup1.example.com/");
    assert_eq!(endpoints[1].auth_token, None); // No auth token specified, no auth required

    assert_eq!(endpoints[2].address.as_str(), "http://backup2.example.com/");
    assert_eq!(endpoints[2].auth_token.as_deref(), Some("backup2_token")); // Own token
}

#[tokio::test]
async fn test_reconnect_tries_primary_first() {
    // Primary always fails, backup always works.
    // After initial failover to backup, reconnection should still try primary first.
    let primary_connection_count = Arc::new(AtomicU64::new(0));
    let primary_server = FailingMockGrpcServer::new(primary_connection_count.clone());
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let backup_connection_count = Arc::new(AtomicU64::new(0));
    let backup_server = WorkingMockGrpcServer::new(backup_connection_count.clone(), 0);
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    let mut config = create_base_config(primary_port);
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    // Initial connection: primary fails, falls to backup
    let mut transaction_stream = TransactionStream::new(config)
        .await
        .expect("Should connect via backup");

    let primary_count_after_init = primary_connection_count.load(Ordering::SeqCst);
    assert!(
        primary_count_after_init > 0,
        "Primary should have been attempted during init"
    );

    // Reset primary counter to track reconnection attempts
    primary_connection_count.store(0, Ordering::SeqCst);

    // Force a reconnection by calling reconnect_to_grpc_with_retries directly.
    // This should reset to primary (fails), then fall to backup (succeeds).
    let result = transaction_stream.reconnect_to_grpc_with_retries().await;
    assert!(result.is_ok(), "Should reconnect via backup");

    let primary_count_after_reconnect = primary_connection_count.load(Ordering::SeqCst);
    assert!(
        primary_count_after_reconnect > 0,
        "Primary should have been retried during reconnection, but got {} attempts",
        primary_count_after_reconnect
    );

    println!(
        "Primary attempts after reconnect: {}, Backup total: {}",
        primary_count_after_reconnect,
        backup_connection_count.load(Ordering::SeqCst)
    );
}

#[tokio::test]
async fn test_backward_compatibility_no_backup_endpoints() {
    // Test that config without backup_endpoints works (backward compatibility)
    let working_connection_count = Arc::new(AtomicU64::new(0));
    let working_server = WorkingMockGrpcServer::new(working_connection_count.clone(), 0);
    let working_port = working_server
        .run()
        .await
        .expect("Failed to start working server");

    let config = create_base_config(working_port);
    assert!(
        config.backup_endpoints.is_empty(),
        "Default should have no backup endpoints"
    );
    assert_eq!(config.total_endpoints(), 1);

    let mut transaction_stream = TransactionStream::new(config)
        .await
        .expect("Failed to create transaction stream");

    let result = transaction_stream.get_next_transaction_batch().await;
    assert!(result.is_ok(), "Should work with no backup endpoints");
}

#[tokio::test]
async fn test_exponential_backoff_reconnection() {
    // Verify that reconnection uses exponential backoff by measuring that a second
    // reconnect attempt takes longer than a first one (delays double each time).
    let primary_connection_count = Arc::new(AtomicU64::new(0));
    let primary_server = FailingMockGrpcServer::new(primary_connection_count.clone());
    let primary_port = primary_server
        .run()
        .await
        .expect("Failed to start primary server");

    let backup_connection_count = Arc::new(AtomicU64::new(0));
    let backup_server = WorkingMockGrpcServer::new(backup_connection_count.clone(), 0);
    let backup_port = backup_server
        .run()
        .await
        .expect("Failed to start backup server");

    // Small delays to keep the test fast but large enough to measure growth.
    let mut config = create_base_config(primary_port);
    config.indexer_grpc_reconnection_initial_delay_ms = 100;
    config.indexer_grpc_reconnection_enable_jitter = false;
    config.indexer_grpc_reconnection_max_retries = 1;
    config.backup_endpoints = vec![Endpoint {
        address: Url::parse(&format!("http://127.0.0.1:{}", backup_port)).unwrap(),
        auth_token: None,
        is_primary: false,
    }];

    let mut transaction_stream = TransactionStream::new(config)
        .await
        .expect("Should connect via backup");

    // Consume the initial batch.
    let _ = transaction_stream.get_next_transaction_batch().await;

    primary_connection_count.store(0, Ordering::SeqCst);
    backup_connection_count.store(0, Ordering::SeqCst);

    let start = std::time::Instant::now();
    let result = transaction_stream.reconnect_to_grpc_with_retries().await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Should reconnect via backup");

    // With 1 retry per endpoint: primary sleeps 100ms then fails, backup sleeps 200ms
    // then succeeds. Total reconnect delay >= 300ms.
    assert!(
        elapsed >= std::time::Duration::from_millis(250),
        "Expected at least ~300ms from exponential backoff (100+200), but got {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_backoff_default_values() {
    assert_eq!(
        TransactionStreamConfig::default_indexer_grpc_reconnection_initial_delay_ms(),
        1000
    );
    assert_eq!(
        TransactionStreamConfig::default_indexer_grpc_reconnection_max_delay_ms(),
        30_000
    );
    assert!(TransactionStreamConfig::default_indexer_grpc_reconnection_enable_jitter());
}

use aptos_indexer_transaction_stream::{
    config::{ReconnectionConfig, StalenessConfig, TransactionStreamConfig},
    transaction_stream::{STALENESS_RECONNECTS, TransactionStream},
};
use aptos_protos::{
    indexer::v1::{
        GetTransactionsRequest, TransactionsResponse,
        raw_data_server::{RawData, RawDataServer},
    },
    transaction::v1::Transaction,
    util::timestamp::Timestamp,
};
use futures::Stream;
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};
use url::Url;

/// Emits one batch per poll, each carrying a single transaction whose timestamp is
/// `initial_staleness_secs` behind now, reduced by `catch_up_secs_per_batch` each time.
///
/// `catch_up_secs_per_batch: 0` models a backend pinned behind the chain; a positive value
/// models a consumer draining a backlog, which must never trigger a reconnect.
struct StaleResponseStream {
    next_version: u64,
    staleness_secs: i64,
    catch_up_secs_per_batch: i64,
}

impl Stream for StaleResponseStream {
    type Item = Result<TransactionsResponse, Status>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let version = self.next_version;
        self.next_version += 1;

        let staleness = self.staleness_secs;
        self.staleness_secs = (staleness - self.catch_up_secs_per_batch).max(0);

        let txn = Transaction {
            version,
            timestamp: Some(Timestamp {
                seconds: chrono::Utc::now().timestamp() - staleness,
                nanos: 0,
            }),
            ..Default::default()
        };

        Poll::Ready(Some(Ok(TransactionsResponse {
            transactions: vec![txn],
            chain_id: Some(1),
            processed_range: None,
        })))
    }
}

struct StaleMockGrpcServer {
    connection_count: Arc<AtomicU64>,
    initial_staleness_secs: i64,
    catch_up_secs_per_batch: i64,
}

#[tonic::async_trait]
impl RawData for StaleMockGrpcServer {
    type GetTransactionsStream =
        Pin<Box<dyn Stream<Item = Result<TransactionsResponse, Status>> + Send>>;

    async fn get_transactions(
        &self,
        _req: Request<GetTransactionsRequest>,
    ) -> Result<Response<Self::GetTransactionsStream>, Status> {
        let connection = self.connection_count.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(Box::pin(StaleResponseStream {
            // Each connection resumes at a distinct version so the gap check stays quiet.
            next_version: connection * 1_000_000,
            staleness_secs: self.initial_staleness_secs,
            catch_up_secs_per_batch: self.catch_up_secs_per_batch,
        })))
    }
}

impl StaleMockGrpcServer {
    async fn run(self) -> anyhow::Result<u16> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = Server::builder().add_service(
            RawDataServer::new(self)
                .accept_compressed(tonic::codec::CompressionEncoding::Zstd)
                .send_compressed(tonic::codec::CompressionEncoding::Zstd),
        );
        tokio::spawn(async move {
            let _ = server
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        Ok(port)
    }
}

fn config_for(port: u16, staleness_config: StalenessConfig) -> TransactionStreamConfig {
    TransactionStreamConfig {
        indexer_grpc_data_service_address: Url::parse(&format!("http://127.0.0.1:{}", port))
            .unwrap(),
        starting_version: Some(0),
        request_ending_version: None,
        auth_token: Some("test_token".to_string()),
        request_name_header: "test".to_string(),
        additional_headers: Default::default(),
        indexer_grpc_http2_ping_interval_secs: 30,
        indexer_grpc_http2_ping_timeout_secs: 10,
        indexer_grpc_response_item_timeout_secs: 60,
        reconnection_config: ReconnectionConfig {
            timeout_secs: 5,
            max_retries: 2,
            initial_delay_ms: 10,
            max_delay_ms: 100,
            enable_jitter: false,
        },
        transaction_filter: None,
        backup_endpoints: vec![],
        primary_failback_interval_secs: 0,
        staleness_config,
    }
}

/// A stream stuck behind the chain reconnects, even though it never stops delivering and so
/// never trips `indexer_grpc_response_item_timeout_secs`.
#[tokio::test]
async fn test_reconnects_when_stream_stays_behind() {
    let connection_count = Arc::new(AtomicU64::new(0));
    let port = StaleMockGrpcServer {
        connection_count: connection_count.clone(),
        initial_staleness_secs: 120,
        catch_up_secs_per_batch: 0,
    }
    .run()
    .await
    .expect("failed to start mock server");

    let config = config_for(
        port,
        StalenessConfig {
            max_staleness_secs: 5,
            sustained_secs: 1,
            reconnect_cooldown_secs: 0,
        },
    );

    let mut stream = TransactionStream::new(config)
        .await
        .expect("failed to create transaction stream");

    assert_eq!(connection_count.load(Ordering::SeqCst), 1);
    let reconnects_before = STALENESS_RECONNECTS.get();

    // Batches arrive continuously, so the item timeout (60s) can never fire. Only the
    // staleness check can force a reconnect here.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        if stream.get_next_transaction_batch().await.is_err() {
            break;
        }
        if connection_count.load(Ordering::SeqCst) > 1 {
            break;
        }
    }

    assert!(
        connection_count.load(Ordering::SeqCst) > 1,
        "a stream stuck 120s behind should have reconnected"
    );
    // Distinguishes a staleness reconnect from one the error path happened to cause.
    assert!(
        STALENESS_RECONNECTS.get() > reconnects_before,
        "the reconnect should have come from the staleness check"
    );
}

/// A consumer draining a backlog is stale by definition. It must not reconnect, or it would
/// restart the drain every time and never catch up.
#[tokio::test]
async fn test_does_not_reconnect_while_catching_up() {
    let connection_count = Arc::new(AtomicU64::new(0));
    let port = StaleMockGrpcServer {
        connection_count: connection_count.clone(),
        initial_staleness_secs: 120,
        catch_up_secs_per_batch: 1,
    }
    .run()
    .await
    .expect("failed to start mock server");

    let config = config_for(
        port,
        StalenessConfig {
            max_staleness_secs: 5,
            sustained_secs: 1,
            reconnect_cooldown_secs: 0,
        },
    );

    let mut stream = TransactionStream::new(config)
        .await
        .expect("failed to create transaction stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if stream.get_next_transaction_batch().await.is_err() {
            break;
        }
    }

    assert_eq!(
        connection_count.load(Ordering::SeqCst),
        1,
        "staleness that is improving every batch must not trigger a reconnect"
    );
}

/// `max_staleness_secs: 0` leaves behaviour exactly as it was before the check existed.
#[tokio::test]
async fn test_disabled_by_zero_threshold() {
    let connection_count = Arc::new(AtomicU64::new(0));
    let port = StaleMockGrpcServer {
        connection_count: connection_count.clone(),
        initial_staleness_secs: 3600,
        catch_up_secs_per_batch: 0,
    }
    .run()
    .await
    .expect("failed to start mock server");

    let config = config_for(
        port,
        StalenessConfig {
            max_staleness_secs: 0,
            sustained_secs: 1,
            reconnect_cooldown_secs: 0,
        },
    );

    let mut stream = TransactionStream::new(config)
        .await
        .expect("failed to create transaction stream");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if stream.get_next_transaction_batch().await.is_err() {
            break;
        }
    }

    assert_eq!(
        connection_count.load(Ordering::SeqCst),
        1,
        "a zero threshold must disable the check entirely"
    );
}

use instrumented_channel::InstrumentedAsyncReceiver;
use std::time::Duration;

pub async fn receive_with_timeout<T>(
    receiver: &mut InstrumentedAsyncReceiver<T>,
    timeout_ms: u64,
) -> Option<T> {
    tokio::time::timeout(Duration::from_millis(timeout_ms), async {
        receiver.recv().await
    })
    .await
    .unwrap()
    .ok()
}

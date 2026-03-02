# Aptos TypeScript SDK Changelog

All notable changes to the Aptos TypeScript SDK will be captured in this file. This changelog is written by hand for now. It adheres to the format set out by [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

- Added exponential backoff with configurable jitter for transaction stream reconnects, using `tokio-retry`'s `ExponentialBackoff`. This mitigates GRPC rate limiting and prevents processor crash-loops during reconnection storms.
- **Breaking**: Reconnection settings (`timeout_secs`, `max_retries`, `initial_delay_ms`, `max_delay_ms`, `enable_jitter`) are now grouped under a `reconnection_config` field in `TransactionStreamConfig` instead of being top-level fields. The previous flat fields (`indexer_grpc_reconnection_timeout_secs`, `indexer_grpc_reconnection_max_retries`, `indexer_grpc_reconnection_retry_delay_ms`) are removed. Configs that omit the new section entirely will get sensible defaults.
- Added backoff delays to the inner retry loops in `get_stream_for_endpoint`, which previously had no delay between retries.

## 0.2.0 (2025-12-09)

- Renamed `/readiness` endpoint to `/healthz`.
- Added support for the health endpoint for checking processors are making forward progress.
- `ServerArgs::run` now supports additional health checks via the new `run_with_health_checks` method.
- **Breaking**: `run_server_with_config` requires an additional `health_checks` argument now.

## 0.1.0

- Initial version at start of changelog.

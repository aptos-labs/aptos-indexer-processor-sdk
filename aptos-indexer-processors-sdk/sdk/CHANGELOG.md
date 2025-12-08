# Aptos TypeScript SDK Changelog

All notable changes to the Aptos TypeScript SDK will be captured in this file. This changelog is written by hand for now. It adheres to the format set out by [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

## 0.2.0 (2025-12-09)

- Renamed `/readiness` endpoint to `/health`.
- Added support for the health endpoint for checking processors are making forward progress.
- `ServerArgs::run` now supports additional health checks via the new `run_with_health_checks` method.
- **Breaking**: `run_server_with_config` requires an additional `health_checks` argument now.

## 0.1.0

- Initial version at start of changelog.

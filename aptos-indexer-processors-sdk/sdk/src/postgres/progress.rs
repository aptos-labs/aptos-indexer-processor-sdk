//! Postgres-specific progress health checking.
//!
//! This module provides `PostgresProgressStatusProvider`, which implements the
//! `ProgressStatusProvider` trait for postgres-backed processors.

use crate::{
    health::ProgressStatusProvider,
    postgres::{models::processor_status::ProcessorStatusQuery, utils::database::ArcDbPool},
};
use async_trait::async_trait;
use chrono::NaiveDateTime;

/// A postgres-backed implementation of `ProgressStatusProvider`.
///
/// This queries the `processor_status` table to get the last updated timestamp.
pub struct PostgresProgressStatusProvider {
    processor_name: String,
    db_pool: ArcDbPool,
}

impl PostgresProgressStatusProvider {
    pub fn new(processor_name: String, db_pool: ArcDbPool) -> Self {
        Self {
            processor_name,
            db_pool,
        }
    }
}

#[async_trait]
impl ProgressStatusProvider for PostgresProgressStatusProvider {
    async fn get_last_updated(&self) -> Result<Option<NaiveDateTime>, String> {
        let mut conn = self
            .db_pool
            .get()
            .await
            .map_err(|e| format!("Failed to get DB connection: {}", e))?;

        let status = ProcessorStatusQuery::get_by_processor(&self.processor_name, &mut conn)
            .await
            .map_err(|e| format!("Failed to query processor status: {}", e))?;

        Ok(status.map(|s| s.last_updated))
    }
}

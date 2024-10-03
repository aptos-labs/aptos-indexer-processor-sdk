// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

#![allow(clippy::extra_unused_lifetimes)]

use crate::{schema::backfill_processor_status, utils::database::DbPoolConnection};
use diesel::{AsChangeset, ExpressionMethods, Insertable, OptionalExtension, QueryDsl, Queryable};
use diesel_async::RunQueryDsl;

#[derive(AsChangeset, Debug, Insertable)]
#[diesel(table_name = backfill_processor_status)]
/// Only tracking the latest version successfully processed
pub struct BackfillProcessorStatus {
    pub processor_name: String,
    pub last_success_version: i64,
    pub last_transaction_timestamp: Option<chrono::NaiveDateTime>,
    pub backfill_start_version: i64,
    pub backfill_end_version: i64,
}

#[derive(AsChangeset, Debug, Queryable)]
#[diesel(table_name = backfill_processor_status)]
/// Only tracking the latest version successfully processed
pub struct BackfillProcessorStatusQuery {
    pub processor_name: String,
    pub last_success_version: i64,
    pub last_updated: chrono::NaiveDateTime,
    pub last_transaction_timestamp: Option<chrono::NaiveDateTime>,
    pub backfill_start_version: i64,
    pub backfill_end_version: i64,
}

impl BackfillProcessorStatusQuery {
    pub async fn get_by_processor(
        processor_name: &str,
        conn: &mut DbPoolConnection<'_>,
    ) -> diesel::QueryResult<Option<Self>> {
        backfill_processor_status::table
            .filter(backfill_processor_status::processor_name.eq(processor_name))
            .first::<Self>(conn)
            .await
            .optional()
    }
}

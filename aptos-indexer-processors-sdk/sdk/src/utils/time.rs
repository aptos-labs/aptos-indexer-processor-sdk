// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! Helpers related to dealing with dates and times.

use aptos_protos::util::timestamp::Timestamp;
use chrono::Utc;

/// 9999-12-31 23:59:59, this is the max supported by Google BigQuery.
pub const MAX_TIMESTAMP_SECS: i64 = 253_402_300_799;

pub fn parse_timestamp(ts: &Timestamp, version: i64) -> chrono::DateTime<Utc> {
    let final_ts = if ts.seconds >= MAX_TIMESTAMP_SECS {
        Timestamp {
            seconds: MAX_TIMESTAMP_SECS,
            nanos: 0,
        }
    } else {
        ts.clone()
    };
    chrono::DateTime::from_timestamp(final_ts.seconds, final_ts.nanos as u32)
        .unwrap_or_else(|| panic!("Could not parse timestamp {:?} for version {}", ts, version))
}

pub fn parse_timestamp_secs(ts: u64, version: i64) -> chrono::DateTime<Utc> {
    chrono::DateTime::from_timestamp(std::cmp::min(ts, MAX_TIMESTAMP_SECS as u64) as i64, 0)
        .unwrap_or_else(|| panic!("Could not parse timestamp {:?} for version {}", ts, version))
}

/// Convert the protobuf Timestamp to epcoh time in seconds.
pub fn time_diff_since_pb_timestamp_in_secs(timestamp: &Timestamp) -> f64 {
    let current_timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("SystemTime before UNIX EPOCH!")
        .as_secs_f64();
    let transaction_time = timestamp.seconds as f64 + timestamp.nanos as f64 * 1e-9;
    current_timestamp - transaction_time
}

/// Convert the protobuf timestamp to ISO format
pub fn timestamp_to_iso(timestamp: &Timestamp) -> String {
    let dt = parse_timestamp(timestamp, 0);
    dt.format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string()
}

/// Convert the protobuf timestamp to unixtime
pub fn timestamp_to_unixtime(timestamp: &Timestamp) -> f64 {
    timestamp.seconds as f64 + timestamp.nanos as f64 * 1e-9
}

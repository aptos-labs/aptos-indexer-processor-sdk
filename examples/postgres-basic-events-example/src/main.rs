use std::sync::Arc;

use crate::events_model::EventModel;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::transaction::TxnData,
    postgres::{
        basic_processor::process,
        utils::database::{execute_in_chunks, MAX_DIESEL_PARAM_SIZE},
    }, utils::errors::ProcessorError,
};
use diesel::{pg::Pg, query_builder::QueryFragment};
use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use field_count::FieldCount;
use rayon::prelude::*;
use tracing::{error, info, warn};
use clickhouse::Client;
use serde::Serialize;
pub mod events_model;
#[path = "db/schema.rs"]
pub mod schema;

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("src/db/migrations");

fn insert_events_query(
    items_to_insert: Vec<EventModel>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use crate::schema::events::dsl::*;
    diesel::insert_into(crate::schema::events::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_nothing()
}

async fn insert_clickhouse<T>(
    // client: &Arc<Client>,
    table_name: String,
    data: &[T],
    start_version: u64,
    end_version: u64,
) -> Result<(), ProcessorError> where T: clickhouse::Row + Serialize + Send + Sync,
{
    let client = Arc::new(
    Client::default()
        .with_url("https://l456jrzvli.us-central1.gcp.clickhouse.cloud:8443")
        .with_user("default")
        .with_password("I0hvRoe~sLbDO")
        .with_database("default"),
        );

    client.query("CREATE TABLE IF NOT EXISTS events (
        transaction_version Int64,
        event_index Int64,
        account_address String,
        transaction_block_height Int64,
        type String,
    ) ENGINE = MergeTree() ORDER BY (transaction_version, event_index);").execute().await.map_err(|e| ProcessorError::DBStoreError {
        message: e.to_string(),
        query: None,
    })?;
    

    let mut insert = client.insert(table_name.as_str()).map_err(|e| ProcessorError::DBStoreError {
        message: e.to_string(),
        query: None,
    })?;
    
    for row in data {
        insert
            .write(row)
            .await.map_err(|e| ProcessorError::DBStoreError {
                message: e.to_string(),
                query: None,
            })?;
    }

    match insert.end().await {
        Ok(_) => {
            info!(
                "Events version [{}, {}] stored successfully",
                start_version,
                end_version
            );
            Ok(())
        },
        Err(e) => {
            error!("Failed to store events: {:?}", e);
            Err(ProcessorError::DBStoreError {
                message: e.to_string(),
                query: None,
            })
        },
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    process(
        "events_processor".to_string(),
        MIGRATIONS,
        async |transactions, conn_pool| {
            let events = transactions
                .par_iter()
                .map(|txn| {
                    let txn_version = txn.version as i64;
                    let block_height = txn.block_height as i64;
                    let txn_data = match txn.txn_data.as_ref() {
                        Some(data) => data,
                        None => {
                            warn!(
                                transaction_version = txn_version,
                                "Transaction data doesn't exist"
                            );
                            return vec![];
                        },
                    };
                    let default = vec![];
                    let raw_events = match txn_data {
                        TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                        TxnData::Genesis(tx_inner) => &tx_inner.events,
                        TxnData::User(tx_inner) => &tx_inner.events,
                        _ => &default,
                    };

                    EventModel::from_events(raw_events, txn_version, block_height)
                })
                .flatten()
                .collect::<Vec<EventModel>>();


        let execute_res = insert_clickhouse("events".to_string(), &events, transactions.first().unwrap().version, transactions.last().unwrap().version).await;
        
        match execute_res {
            Ok(_) => {
                info!(
                    "Events version [{}, {}] stored successfully",
                    transactions.first().unwrap().version,
                    transactions.last().unwrap().version
                );
                Ok(())
            },
            Err(e) => {
                error!("Failed to store events: {:?}", e);
                Err(e)
            },
        }
        },
    )
    .await?;
    Ok(())
}

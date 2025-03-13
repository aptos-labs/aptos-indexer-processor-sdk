use crate::events_model::EventModel;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::transaction::TxnData,
    postgres::{
        basic_processor::process,
        utils::database::{execute_in_chunks, MAX_DIESEL_PARAM_SIZE},
    },
};
use diesel::{pg::Pg, query_builder::QueryFragment};
use diesel_migrations::{embed_migrations, EmbeddedMigrations};
use field_count::FieldCount;
use rayon::prelude::*;
use tracing::{error, info, warn};

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

            // Store events in the database
            let execute_res = execute_in_chunks(
                conn_pool.clone(),
                insert_events_query,
                &events,
                MAX_DIESEL_PARAM_SIZE / EventModel::field_count(),
            )
            .await;
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

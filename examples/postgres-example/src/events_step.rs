use crate::events_model::EventModel;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{transaction::TxnData, Transaction},
    postgres::utils::database::{execute_in_chunks, ArcDbPool, MAX_DIESEL_PARAM_SIZE},
    traits::{async_step::AsyncRunType, AsyncStep, NamedStep, Processable},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use async_trait::async_trait;
use diesel::{pg::Pg, query_builder::QueryFragment};
use field_count::FieldCount;
use rayon::prelude::*;
use tracing::{error, info, warn};

/// EventsStep extracts events data from transactions and store it in the database.
pub struct EventsStep {
    pub conn_pool: ArcDbPool,
}

#[async_trait]
impl Processable for EventsStep {
    type Input = Vec<Transaction>;
    type Output = Vec<EventModel>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        transactions: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<Vec<EventModel>>>, ProcessorError> {
        // Extract events from transactions
        let events = transactions
            .data
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
            self.conn_pool.clone(),
            insert_events_query,
            &events,
            MAX_DIESEL_PARAM_SIZE / EventModel::field_count(),
        )
        .await;
        match execute_res {
            Ok(_) => {
                info!(
                    "Events version [{}, {}] stored successfully",
                    transactions.metadata.start_version, transactions.metadata.end_version
                );
            },
            Err(e) => {
                error!("Failed to store events: {:?}", e);
            },
        }

        Ok(Some(TransactionContext {
            data: events,
            metadata: transactions.metadata,
        }))
    }
}

impl AsyncStep for EventsStep {}

impl NamedStep for EventsStep {
    fn name(&self) -> String {
        "EventsStep".to_string()
    }
}

fn insert_events_query(
    items_to_insert: Vec<EventModel>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use crate::schema::events::dsl::*;
    diesel::insert_into(crate::schema::events::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_nothing()
}

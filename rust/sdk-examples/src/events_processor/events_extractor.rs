use super::events_models::EventModel;
use aptos_indexer_transaction_stream::TransactionsPBResponse;
use aptos_protos::transaction::v1::transaction::TxnData;
use async_trait::async_trait;
use rayon::prelude::*;
use sdk::{
    steps::{async_step::AsyncRunType, AsyncStep},
    traits::{NamedStep, Processable},
};

pub struct EventsExtractor
where
    Self: Sized + Send + 'static, {}

#[async_trait]
impl Processable for EventsExtractor {
    type Input = TransactionsPBResponse;
    type Output = EventModel;
    type RunType = AsyncRunType;

    async fn process(&mut self, item: Vec<TransactionsPBResponse>) -> Vec<EventModel> {
        item.par_iter()
            .map(|txn_pb| {
                let mut events = vec![];
                for txn in &txn_pb.transactions {
                    let txn_version = txn.version as i64;
                    let block_height = txn.block_height as i64;
                    let txn_data = match txn.txn_data.as_ref() {
                        Some(data) => data,
                        None => {
                            tracing::warn!(
                                transaction_version = txn_version,
                                "Transaction data doesn't exist"
                            );
                            // PROCESSOR_UNKNOWN_TYPE_COUNT
                            //     .with_label_values(&["EventsProcessor"])
                            //     .inc();
                            continue;
                        },
                    };
                    let default = vec![];
                    let raw_events = match txn_data {
                        TxnData::BlockMetadata(tx_inner) => &tx_inner.events,
                        TxnData::Genesis(tx_inner) => &tx_inner.events,
                        TxnData::User(tx_inner) => &tx_inner.events,
                        _ => &default,
                    };

                    let txn_events = EventModel::from_events(raw_events, txn_version, block_height);
                    events.extend(txn_events);
                }
                events
            })
            .flatten()
            .collect::<Vec<EventModel>>()
    }
}

impl AsyncStep for EventsExtractor {}

impl NamedStep for EventsExtractor {
    fn name(&self) -> String {
        "EventsExtractor".to_string()
    }
}

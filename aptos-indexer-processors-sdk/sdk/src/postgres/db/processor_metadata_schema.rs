// @generated automatically by Diesel CLI.

pub mod processor_metadata {
    diesel::table! {
        processor_metadata.ledger_infos (chain_id) {
            chain_id -> Int8,
        }
    }

    diesel::table! {
        processor_metadata.processor_status (processor) {
            #[max_length = 100]
            processor -> Varchar,
            last_success_version -> Int8,
            last_updated -> Timestamp,
            last_transaction_timestamp -> Nullable<Timestamp>,
        }
    }

    diesel::allow_tables_to_appear_in_same_query!(
        ledger_infos,
        processor_status,
    );
}

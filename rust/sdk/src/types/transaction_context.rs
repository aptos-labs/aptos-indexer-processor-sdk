use aptos_indexer_transaction_stream::utils::timestamp::timestamp_to_unixtime;

/// TransactionContext is a struct that holds data processed from a set of transactions
/// and includes metadata about the transactions that the data is associated with.
/// The metadata is used for metrics and logging purposes.
#[derive(Clone, Default)]
pub struct TransactionContext<T> {
    pub data: Vec<T>,

    // Metadata about the transactions that the data is associated with
    pub start_version: u64,
    pub end_version: u64,
    pub start_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub end_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub total_size_in_bytes: u64,
}

impl<T> TransactionContext<T> {
    pub fn get_num_transactions(&self) -> u64 {
        self.end_version - self.start_version + 1
    }

    pub fn get_start_transaction_timestamp_unix(&self) -> Option<f64> {
        self.start_transaction_timestamp
            .as_ref()
            .map(timestamp_to_unixtime)
    }
}

#[derive(Clone, Default)]
pub struct TransactionContextMultipleBatch<T> {
    pub data: Vec<T>,
    // Metadata about the transactions that the data is associated with
    // TODO: Implement this
}

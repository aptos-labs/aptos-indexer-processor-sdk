use aptos_indexer_transaction_stream::utils::timestamp_to_unixtime;

/// Contains processed data and associated transaction metadata.
///
/// The processed data is extracted from transactions and the
/// TransactionContext contains additional metadata about which transactions the extracted
/// data originated from. The metadata is used for metrics and logging purposes.
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

impl<T> Ord for TransactionContext<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.start_version.cmp(&other.start_version)
    }
}

impl<T> PartialOrd for TransactionContext<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Eq for TransactionContext<T> {}

impl<T> PartialEq for TransactionContext<T> {
    fn eq(&self, other: &Self) -> bool {
        self.start_version == other.start_version
    }
}

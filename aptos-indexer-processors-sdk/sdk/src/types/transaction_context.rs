use aptos_indexer_transaction_stream::utils::timestamp_to_unixtime;

/// Contains processed data and associated transaction metadata.
///
/// The processed data is extracted from transactions and the
/// TransactionContext contains additional metadata about which transactions the extracted
/// data originated from. The metadata is used for metrics and logging purposes.
#[derive(Clone, Default)]
pub struct TransactionContext<T> {
    pub data: T,
    pub metadata: TransactionMetadata,
}

impl<T> TransactionContext<T> {
    pub fn get_num_transactions(&self) -> u64 {
        self.metadata.end_version - self.metadata.start_version + 1
    }

    pub fn get_start_transaction_timestamp_unix(&self) -> Option<f64> {
        self.metadata
            .start_transaction_timestamp
            .as_ref()
            .map(timestamp_to_unixtime)
    }
}

impl<T> Ord for TransactionContext<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.metadata
            .start_version
            .cmp(&other.metadata.start_version)
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
        self.metadata.start_version == other.metadata.start_version
    }
}

// Metadata about a batch of transactions
#[derive(Clone, Default)]
pub struct TransactionMetadata {
    pub start_version: u64,
    pub end_version: u64,
    pub start_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub end_transaction_timestamp: Option<aptos_protos::util::timestamp::Timestamp>,
    pub total_size_in_bytes: u64,
}

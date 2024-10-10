use derive_builder::Builder;
use once_cell::sync::Lazy;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use std::sync::atomic::AtomicU64;

pub const METRICS_PREFIX: &str = "aptos_procsdk_step_";

pub fn init_step_metrics_registry(registry: &mut Registry) {
    // AsyncStep metrics
    registry.register(
        format!("{}_{}", METRICS_PREFIX, "latest_processed_version"),
        "Latest processed version",
        LATEST_PROCESSED_VERSION.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "latest_transaction_timestamp"),
        "Latest transaction timestamp",
        LATEST_PROCESSED_TRANSACTION_TIMESTAMP.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "processed_transaction_latency"),
        "Latency of the polled transactions, computed by (txn timestamp - current time)",
        PROCESSED_TRANSACTION_LATENCY.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "num_transactions_processed_count"),
        "Number of transactions processed",
        NUM_TRANSACTIONS_PROCESSED_COUNT.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "processing_duration_in_secs"),
        "Processing duration in seconds",
        PROCESSING_DURATION_IN_SECS.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "processed_size_in_bytes"),
        "Transaction size",
        PROCESSED_SIZE_IN_BYTES.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "processing_error_count"),
        "Processing error count",
        PROCESSING_ERROR_COUNT.clone(),
    );

    // PollableAsyncStep metrics
    registry.register(
        format!("{}_{}", METRICS_PREFIX, "latest_polled_version"),
        "Latest polled version",
        LATEST_POLLED_VERSION.clone(),
    );

    registry.register(
        format!(
            "{}_{}",
            METRICS_PREFIX, "latest_polled_transaction_timestamp"
        ),
        "Latest polled transaction timestamp",
        LATEST_POLLED_TRANSACTION_TIMESTAMP.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "polled_transaction_latency"),
        "Latency of the polled transactions, computed by (txn timestamp - current time)",
        POLLED_TRANSACTION_LATENCY.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "num_polled_transactions_count"),
        "Number of transactions polled",
        NUM_POLLED_TRANSACTIONS_COUNT.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "polling_duration_in_secs"),
        "Polling duration in seconds",
        POLLING_DURATION_IN_SECS.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "polled_size_in_bytes"),
        "Polled transaction size",
        POLLED_SIZE_IN_BYTES.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "polling_error_count"),
        "Polling error count",
        POLLING_ERROR_COUNT.clone(),
    );
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StepMetricLabels {
    pub step_name: String,
}

// AsyncStep metrics
pub static LATEST_PROCESSED_VERSION: Lazy<Family<StepMetricLabels, Gauge>> =
    Lazy::new(Family::<StepMetricLabels, Gauge>::default);

pub static LATEST_PROCESSED_TRANSACTION_TIMESTAMP: Lazy<
    Family<StepMetricLabels, Gauge<f64, AtomicU64>>,
> = Lazy::new(Family::<StepMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static PROCESSED_TRANSACTION_LATENCY: Lazy<Family<StepMetricLabels, Gauge<f64, AtomicU64>>> =
    Lazy::new(Family::<StepMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static NUM_TRANSACTIONS_PROCESSED_COUNT: Lazy<Family<StepMetricLabels, Counter>> =
    Lazy::new(Family::<StepMetricLabels, Counter>::default);

pub static PROCESSING_DURATION_IN_SECS: Lazy<Family<StepMetricLabels, Gauge<f64, AtomicU64>>> =
    Lazy::new(Family::<StepMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static PROCESSED_SIZE_IN_BYTES: Lazy<Family<StepMetricLabels, Counter>> =
    Lazy::new(Family::<StepMetricLabels, Counter>::default);

pub static PROCESSING_ERROR_COUNT: Lazy<Family<StepMetricLabels, Counter>> =
    Lazy::new(Family::<StepMetricLabels, Counter>::default);

// PollableAsyncStep metrics
pub static LATEST_POLLED_VERSION: Lazy<Family<StepMetricLabels, Gauge>> =
    Lazy::new(Family::<StepMetricLabels, Gauge>::default);

pub static LATEST_POLLED_TRANSACTION_TIMESTAMP: Lazy<
    Family<StepMetricLabels, Gauge<f64, AtomicU64>>,
> = Lazy::new(Family::<StepMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static POLLED_TRANSACTION_LATENCY: Lazy<Family<StepMetricLabels, Gauge<f64, AtomicU64>>> =
    Lazy::new(Family::<StepMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static NUM_POLLED_TRANSACTIONS_COUNT: Lazy<Family<StepMetricLabels, Counter>> =
    Lazy::new(Family::<StepMetricLabels, Counter>::default);

pub static POLLING_DURATION_IN_SECS: Lazy<Family<StepMetricLabels, Gauge<f64, AtomicU64>>> =
    Lazy::new(Family::<StepMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static POLLED_SIZE_IN_BYTES: Lazy<Family<StepMetricLabels, Counter>> =
    Lazy::new(Family::<StepMetricLabels, Counter>::default);

pub static POLLING_ERROR_COUNT: Lazy<Family<StepMetricLabels, Counter>> =
    Lazy::new(Family::<StepMetricLabels, Counter>::default);

#[derive(Builder)]
pub struct StepMetrics {
    pub labels: StepMetricLabels,
    // AsyncStep metrics
    #[builder(default, setter(strip_option))]
    latest_processed_version: Option<u64>,
    #[builder(default)]
    latest_transaction_timestamp: Option<f64>,
    #[builder(default)]
    processed_transaction_latency: Option<f64>,
    #[builder(default, setter(strip_option))]
    num_transactions_processed_count: Option<u64>,
    #[builder(default, setter(strip_option))]
    processing_duration_in_secs: Option<f64>,
    #[builder(default, setter(strip_option))]
    processed_size_in_bytes: Option<u64>,

    // PollableAsyncStep metrics
    #[builder(default, setter(strip_option))]
    latest_polled_version: Option<u64>,
    #[builder(default)]
    latest_polled_transaction_timestamp: Option<f64>,
    #[builder(default)]
    polled_transaction_latency: Option<f64>,
    #[builder(default, setter(strip_option))]
    num_polled_transactions_count: Option<u64>,
    #[builder(default, setter(strip_option))]
    polling_duration_in_secs: Option<f64>,
    #[builder(default, setter(strip_option))]
    polled_size_in_bytes: Option<u64>,
}

impl StepMetrics {
    pub fn log_metrics(&mut self) {
        // AsyncStep metrics
        if let Some(version) = self.latest_processed_version {
            LATEST_PROCESSED_VERSION
                .get_or_create(&self.labels)
                .set(version as i64);
        }
        if let Some(timestamp) = self.latest_transaction_timestamp {
            LATEST_PROCESSED_TRANSACTION_TIMESTAMP
                .get_or_create(&self.labels)
                .set(timestamp);
        }
        if let Some(processed_latency) = self.processed_transaction_latency {
            PROCESSED_TRANSACTION_LATENCY
                .get_or_create(&self.labels)
                .set(processed_latency);
        }
        if let Some(count) = self.num_transactions_processed_count {
            NUM_TRANSACTIONS_PROCESSED_COUNT
                .get_or_create(&self.labels)
                .inc_by(count);
        }
        if let Some(duration) = self.processing_duration_in_secs {
            PROCESSING_DURATION_IN_SECS
                .get_or_create(&self.labels)
                .set(duration);
        }
        if let Some(size_in_bytes) = self.processed_size_in_bytes {
            PROCESSED_SIZE_IN_BYTES
                .get_or_create(&self.labels)
                .inc_by(size_in_bytes);
        }

        // PollableAsyncStep metrics
        if let Some(version) = self.latest_polled_version {
            LATEST_POLLED_VERSION
                .get_or_create(&self.labels)
                .set(version as i64);
        }
        if let Some(timestamp) = self.latest_polled_transaction_timestamp {
            LATEST_POLLED_TRANSACTION_TIMESTAMP
                .get_or_create(&self.labels)
                .set(timestamp);
        }
        if let Some(polled_latency) = self.polled_transaction_latency {
            POLLED_TRANSACTION_LATENCY
                .get_or_create(&self.labels)
                .set(polled_latency);
        }
        if let Some(count) = self.num_polled_transactions_count {
            NUM_POLLED_TRANSACTIONS_COUNT
                .get_or_create(&self.labels)
                .inc_by(count);
        }
        if let Some(duration) = self.polling_duration_in_secs {
            POLLING_DURATION_IN_SECS
                .get_or_create(&self.labels)
                .set(duration);
        }
        if let Some(size_in_bytes) = self.polled_size_in_bytes {
            POLLED_SIZE_IN_BYTES
                .get_or_create(&self.labels)
                .inc_by(size_in_bytes);
        }
    }

    pub fn inc_processing_error_count(&self) {
        PROCESSING_ERROR_COUNT.get_or_create(&self.labels).inc();
    }

    pub fn inc_polling_error_count(&self) {
        POLLING_ERROR_COUNT.get_or_create(&self.labels).inc();
    }
}

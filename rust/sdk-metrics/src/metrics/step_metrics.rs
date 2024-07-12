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
    registry.register(
        format!("{}_{}", METRICS_PREFIX, "latest_processed_version"),
        "Latest processed version",
        LATEST_PROCESSED_VERSION.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "latest_transaction_timestamp"),
        "Latest transaction timestamp",
        LATEST_TRANSACTION_TIMESTAMP.clone(),
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
        format!("{}_{}", METRICS_PREFIX, "transaction_size"),
        "Transaction size",
        TRANSACTION_SIZE.clone(),
    );

    registry.register(
        format!("{}_{}", METRICS_PREFIX, "processing_error_count"),
        "Processing error count",
        PROCESSING_ERROR_COUNT.clone(),
    );
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StepMetricLabels {
    pub step_name: String,
}

pub static LATEST_PROCESSED_VERSION: Lazy<Family<StepMetricLabels, Gauge>> =
    Lazy::new(Family::<StepMetricLabels, Gauge>::default);

pub static LATEST_TRANSACTION_TIMESTAMP: Lazy<Family<StepMetricLabels, Gauge<f64, AtomicU64>>> =
    Lazy::new(Family::<StepMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static NUM_TRANSACTIONS_PROCESSED_COUNT: Lazy<Family<StepMetricLabels, Counter>> =
    Lazy::new(Family::<StepMetricLabels, Counter>::default);

pub static PROCESSING_DURATION_IN_SECS: Lazy<Family<StepMetricLabels, Gauge<f64, AtomicU64>>> =
    Lazy::new(Family::<StepMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static TRANSACTION_SIZE: Lazy<Family<StepMetricLabels, Gauge>> =
    Lazy::new(Family::<StepMetricLabels, Gauge>::default);

pub static PROCESSING_ERROR_COUNT: Lazy<Family<StepMetricLabels, Counter>> =
    Lazy::new(Family::<StepMetricLabels, Counter>::default);

#[derive(Builder)]
pub struct StepMetrics {
    pub labels: StepMetricLabels,
    #[builder(setter(strip_option))]
    latest_processed_version: Option<u64>,
    latest_transaction_timestamp: Option<f64>,
    #[builder(setter(strip_option))]
    num_transactions_processed_count: Option<u64>,
    #[builder(setter(strip_option))]
    processing_duration_in_secs: Option<f64>,
    #[builder(setter(strip_option))]
    transaction_size: Option<u64>,
}

impl StepMetrics {
    pub fn log_metrics(&mut self) {
        if let Some(version) = self.latest_processed_version {
            LATEST_PROCESSED_VERSION
                .get_or_create(&self.labels)
                .set(version as i64);
        }
        if let Some(timestamp) = self.latest_transaction_timestamp {
            LATEST_TRANSACTION_TIMESTAMP
                .get_or_create(&self.labels)
                .set(timestamp);
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
        if let Some(size) = self.transaction_size {
            TRANSACTION_SIZE
                .get_or_create(&self.labels)
                .set(size as i64);
        }
    }

    pub fn inc_procssing_error_count(&self) {
        PROCESSING_ERROR_COUNT.get_or_create(&self.labels).inc();
    }
}

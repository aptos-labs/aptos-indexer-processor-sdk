use derive_builder::Builder;
use once_cell::sync::Lazy;
use prometheus_client::{
    collector::Collector,
    encoding::{DescriptorEncoder, EncodeLabelSet, EncodeMetric},
    metrics::{
        counter::Counter,
        family::Family,
        gauge::{ConstGauge, Gauge},
        histogram::Histogram,
    },
    registry::Registry,
};

pub const METRICS_PREFIX: &str = "aptos_procsdk_step_";

pub async fn init_step_metrics_registry() {
    let mut registry = <Registry>::with_prefix(METRICS_PREFIX);
    registry.register(
        "latest_processed_version",
        "Latest version this step has finished processing",
        LATEST_PROCESSED_VERSION.clone(),
    );
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StepMetricLabels {
    pub processor_name: String,
    pub step_name: String,
}

pub static LATEST_PROCESSED_VERSION: Lazy<Family<StepMetricLabels, Gauge>> =
    Lazy::new(Family::<StepMetricLabels, Gauge>::default);

#[derive(Builder)]
pub struct StepMetrics {
    pub labels: StepMetricLabels,
    lastest_processed_version: Option<i64>,
}

impl Drop for StepMetrics {
    fn drop(&mut self) {
        if let Some(version) = self.lastest_processed_version {
            LATEST_PROCESSED_VERSION
                .get_or_create(&self.labels)
                .set(version);
        }
    }
}

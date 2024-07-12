use autometrics::settings::AutometricsSettings;
use derive_builder::Builder;
use once_cell::sync::Lazy;
use prometheus_client::{
    encoding::EncodeLabelSet,
    metrics::{counter::Counter, family::Family, gauge::Gauge},
    registry::Registry,
};
use std::sync::atomic::AtomicU64;

pub const METRICS_PREFIX: &str = "aptos_procsdk_channel_";

pub fn init_channel_metrics_registry() {
    let mut registry = <Registry>::with_prefix(METRICS_PREFIX);
    registry.register(
        "sent_messages_count",
        "Number of messages sent",
        SENT_MESSAGES_COUNT.clone(),
    );

    registry.register(
        "received_messages_count",
        "Number of messages received",
        RECEIVED_MESSAGES_COUNT.clone(),
    );

    registry.register(
        "send_duration",
        "Time taken to complete sending a message in seconds",
        SEND_DURATION.clone(),
    );

    registry.register(
        "receive_duration",
        "Time taken to complete receiving a message in seconds",
        RECEIVE_DURATION.clone(),
    );

    registry.register(
        "failed_sends_count",
        "Number of failed message sends",
        FAILED_SENDS_COUNT.clone(),
    );

    registry.register(
        "failed_receives_count",
        "Number of failed message receives",
        FAILED_RECEIVES_COUNT.clone(),
    );

    registry.register(
        "channel_size",
        "Number of messages in the channel",
        CHANNEL_SIZE.clone(),
    );

    AutometricsSettings::builder()
        .prometheus_client_registry(registry)
        .init();
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ChannelMetricLabels {
    pub channel_name: String,
}

pub static SENT_MESSAGES_COUNT: Lazy<Family<ChannelMetricLabels, Counter>> =
    Lazy::new(Family::<ChannelMetricLabels, Counter>::default);

pub static RECEIVED_MESSAGES_COUNT: Lazy<Family<ChannelMetricLabels, Counter>> =
    Lazy::new(Family::<ChannelMetricLabels, Counter>::default);

pub static SEND_DURATION: Lazy<Family<ChannelMetricLabels, Gauge<f64, AtomicU64>>> =
    Lazy::new(Family::<ChannelMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static RECEIVE_DURATION: Lazy<Family<ChannelMetricLabels, Gauge<f64, AtomicU64>>> =
    Lazy::new(Family::<ChannelMetricLabels, Gauge<f64, AtomicU64>>::default);

pub static FAILED_SENDS_COUNT: Lazy<Family<ChannelMetricLabels, Counter>> =
    Lazy::new(Family::<ChannelMetricLabels, Counter>::default);

pub static FAILED_RECEIVES_COUNT: Lazy<Family<ChannelMetricLabels, Counter>> =
    Lazy::new(Family::<ChannelMetricLabels, Counter>::default);

pub static CHANNEL_SIZE: Lazy<Family<ChannelMetricLabels, Gauge>> =
    Lazy::new(Family::<ChannelMetricLabels, Gauge>::default);

#[derive(Builder)]
pub struct ChannelMetrics {
    pub labels: ChannelMetricLabels,
}

impl ChannelMetrics {
    pub fn new(name: String) -> Self {
        Self {
            labels: ChannelMetricLabels { channel_name: name },
        }
    }
}

impl ChannelMetrics {
    pub fn inc_sent_messages_count(&self) -> &Self {
        SENT_MESSAGES_COUNT.get_or_create(&self.labels).inc();
        self
    }

    pub fn inc_received_messages_count(&self) -> &Self {
        RECEIVED_MESSAGES_COUNT.get_or_create(&self.labels).inc();
        self
    }

    pub fn inc_failed_sends_count(&self) -> &Self {
        FAILED_SENDS_COUNT.get_or_create(&self.labels).inc();
        self
    }

    pub fn inc_failed_receives_count(&self) -> &Self {
        FAILED_RECEIVES_COUNT.get_or_create(&self.labels).inc();
        self
    }

    pub fn log_send_duration(&self, duration: f64) -> &Self {
        SEND_DURATION.get_or_create(&self.labels).set(duration);
        self
    }

    pub fn log_receive_duration(&self, duration: f64) -> &Self {
        RECEIVE_DURATION.get_or_create(&self.labels).set(duration);
        self
    }

    pub fn log_channel_size(&self, size: u64) -> &Self {
        CHANNEL_SIZE.get_or_create(&self.labels).set(size as i64);
        self
    }
}

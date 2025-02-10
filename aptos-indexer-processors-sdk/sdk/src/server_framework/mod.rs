// Copyright © Aptos Foundation

use crate::{
    config::indexer_processor_config::{DbConfig, IndexerProcessorConfig},
    instrumented_channel::channel_metrics::init_channel_metrics_registry,
    traits::processor_trait::ProcessorTrait,
    utils::step_metrics::init_step_metrics_registry,
};
use anyhow::{Context, Result};
#[cfg(target_os = "linux")]
use aptos_system_utils::profiling::start_cpu_profiling;
use autometrics::settings::AutometricsSettings;
use axum::{http::StatusCode, response::IntoResponse, routing::get, Router};
use backtrace::Backtrace;
use clap::Parser;
use prometheus_client::registry::Registry;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
// TODO: remove deprecated lint when new clippy nightly is released
#[allow(deprecated)]
use std::{fs::File, io::Read, panic::PanicInfo, path::PathBuf, process};
use tokio::runtime::Handle;
use tracing::error;
use tracing_subscriber::EnvFilter;

/// ServerArgs bootstraps a server with all common pieces. And then triggers the run method for
/// the specific service.
#[derive(Parser)]
pub struct ServerArgs {
    #[clap(short, long, value_parser)]
    pub config_path: PathBuf,
}

// impl ServerArgs {
//     pub async fn run<C, P>(&self, processor: P, handle: Handle) -> Result<()>
//     where
//         C: RunnableConfig,
//         P: ProcessorTrait + 'static,
//     {
//         // Set up the server.
//         setup_logging();
//         setup_panic_handler();
//         let config = load::<ServerConfig>(&self.config_path)?;
//         run_server_with_config(config, processor, handle).await
//     }

//     pub fn get_processor_name(&self) -> String {
//         let config = load::<ServerConfig>(&self.config_path)?;
//         config.server_config.processor_name
//     }
// }

impl ServerArgs {
    pub async fn run<P, D>(&self, processor: P, handle: Handle) -> Result<()>
    where
        P: ProcessorTrait + 'static,
        D: DbConfig + Send + Sync + 'static,
    {
        // Set up the server.
        setup_logging();
        setup_panic_handler();
        let config = load::<ServerConfig<D>>(&self.config_path)?;
        run_server_with_config(config, processor, handle).await
    }

    pub fn get_processor_name<D>(&self) -> Result<String>
    where
        D: DbConfig,
    {
        let config = load::<ServerConfig<D>>(&self.config_path)?;
        Ok(config.processor_config.processor_name)
    }
}

/// Run a server and the necessary probes. For spawning these tasks, the user must
/// provide a handle to a runtime they already have.
pub async fn run_server_with_config<P, D>(
    config: ServerConfig<D>,
    processor: P,
    handle: Handle,
) -> Result<()>
where
    P: ProcessorTrait + 'static,
    D: DbConfig + Send + Sync + 'static,
{
    let health_port = config.health_check_port;
    let additional_labels = config.metrics_config.additional_labels.clone();
    // Start liveness and readiness probes.
    let task_handler = handle.spawn(async move {
        register_probes_and_metrics_handler(health_port, additional_labels).await;
        anyhow::Ok(())
    });
    let main_task_handler =
        handle.spawn(async move { processor.run_processor::<D>(config.processor_config).await });
    tokio::select! {
        res = task_handler => {
            res.expect("Probes and metrics handler unexpectedly exited")
        },
        res = main_task_handler => {
            res.expect("Main task handler unexpectedly exited")
        },
    }
}

#[derive(Deserialize, Debug, Serialize)]
pub struct ServerConfig<D> {
    // Shared configuration among all services.
    pub health_check_port: u16,

    #[serde(default)]
    pub metrics_config: MetricsConfig,

    // Specific configuration for the processor
    pub processor_config: IndexerProcessorConfig<D>,
}

#[derive(Clone, Deserialize, Debug, Default, Serialize)]
pub struct MetricsConfig {
    /// Additional labels to use for metrics.
    pub additional_labels: Vec<(String, String)>,
}

// #[async_trait::async_trait]
// impl RunnableConfig for ServerConfig
// where
//     T: RunnableConfig,
// {
//     async fn run(&self) -> Result<()> {
//         self.server_config.run().await
//     }

//     fn get_server_name(&self) -> String {
//         self.server_config.get_server_name()
//     }
// }

/// RunnableConfig is a trait that all services must implement for their configuration.
#[async_trait::async_trait]
pub trait RunnableConfig: DeserializeOwned + Send + Sync + 'static {
    async fn run(&self) -> Result<()>;
    fn get_server_name(&self) -> String;
}

/// Parse a yaml file into a struct.
pub fn load<T: for<'de> Deserialize<'de>>(path: &PathBuf) -> Result<T> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open the file at path: {:?}", path))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .with_context(|| format!("failed to read the file at path: {:?}", path))?;
    serde_yaml::from_str::<T>(&contents).context("Unable to parse yaml file")
}

#[derive(Debug, Serialize)]
pub struct CrashInfo {
    details: String,
    backtrace: String,
}

/// Invoke to ensure process exits on a thread panic.
///
/// Tokio's default behavior is to catch panics and ignore them.  Invoking this function will
/// ensure that all subsequent thread panics (even Tokio threads) will report the
/// details/backtrace and then exit.
pub fn setup_panic_handler() {
    // TODO: remove deprecated lint when new clippy nightly is released
    #[allow(deprecated)]
    std::panic::set_hook(Box::new(move |pi: &PanicInfo<'_>| {
        handle_panic(pi);
    }));
}

// Formats and logs panic information
// TODO: remove deprecated lint when new clippy nightly is released
#[allow(deprecated)]
fn handle_panic(panic_info: &PanicInfo<'_>) {
    // The Display formatter for a PanicInfo contains the message, payload and location.
    let details = format!("{}", panic_info);
    let backtrace = format!("{:#?}", Backtrace::new());
    let info = CrashInfo { details, backtrace };
    let crash_info = toml::to_string_pretty(&info).unwrap();
    error!("{}", crash_info);
    // TODO / HACK ALARM: Write crash info synchronously via eprintln! to ensure it is written before the process exits which error! doesn't guarantee.
    // This is a workaround until https://github.com/aptos-labs/aptos-core/issues/2038 is resolved.
    eprintln!("{}", crash_info);
    // Kill the process
    process::exit(12);
}

/// Set up logging for the server.
pub fn setup_logging() {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap();
    tracing_subscriber::fmt()
        .json()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .with_thread_names(true)
        .with_env_filter(env_filter)
        .flatten_event(true)
        .init();
}

/// Register readiness and liveness probes and set up metrics endpoint.
pub async fn register_probes_and_metrics_handler(
    port: u16,
    additional_labels: Vec<(String, String)>,
) {
    let mut registry = Registry::with_labels(
        additional_labels
            .into_iter()
            .map(|(k, v)| (k.into(), v.into())),
    );
    init_step_metrics_registry(&mut registry);
    init_channel_metrics_registry(&mut registry);
    AutometricsSettings::builder()
        .prometheus_client_registry(registry)
        .init();

    let router = Router::new()
        .route("/readiness", get(StatusCode::OK))
        .route("/metrics", get(metrics_handler));

    #[cfg(target_os = "linux")]
    let router = router.merge(Router::new().route("/profilez", get(profilez_handler)));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .expect("Failed to bind TCP listener");
    axum::serve(listener, router).await.unwrap();
}

async fn metrics_handler() -> impl IntoResponse {
    match autometrics::prometheus_exporter::encode_to_string() {
        Ok(prometheus_client_rust_metrics) => (
            StatusCode::OK,
            [("Content-Type", "text/plain; version=0.0.4")],
            prometheus_client_rust_metrics,
        )
            .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", err)).into_response(),
    }
}

#[cfg(target_os = "linux")]
async fn profilez_handler() -> impl IntoResponse {
    match start_cpu_profiling(10, 99, false).await {
        Ok(body) => (
            StatusCode::OK,
            [
                ("Content-Length", body.len().to_string()),
                ("Content-Disposition", "inline".to_string()),
                ("Content-Type", "image/svg+xml".to_string()),
            ],
            body,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Profiling failed: {e:?}."),
        )
            .into_response(),
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::io::Write;
//     use tempfile::tempdir;

//     #[derive(Clone, Debug, Deserialize, Serialize)]
//     #[serde(deny_unknown_fields)]
//     pub struct TestConfig {
//         test: u32,
//         test_name: String,
//     }

//     #[async_trait::async_trait]
//     impl RunnableConfig for TestConfig {
//         async fn run(&self) -> Result<()> {
//             assert_eq!(self.test, 123);
//             assert_eq!(self.test_name, "test");
//             Ok(())
//         }

//         fn get_server_name(&self) -> String {
//             self.test_name.clone()
//         }
//     }

//     #[test]
//     fn test_random_config_creation() {
//         let dir = tempdir().expect("tempdir failure");

//         let file_path = dir.path().join("testing_yaml.yaml");
//         let mut file = File::create(&file_path).expect("create failure");
//         let raw_yaml_content = r#"
//             health_check_port: 12345
//             server_config:
//                 test: 123
//                 test_name: "test"
//         "#;
//         writeln!(file, "{}", raw_yaml_content).expect("write_all failure");

//         let config = load::<ServerConfig<TestConfig>>(&file_path).unwrap();
//         assert_eq!(config.health_check_port, 12345);
//         assert_eq!(config.server_config.test, 123);
//         assert_eq!(config.server_config.test_name, "test");
//     }

//     #[test]
//     fn verify_tool() {
//         use clap::CommandFactory;
//         ServerArgs::command().debug_assert()
//     }
// }

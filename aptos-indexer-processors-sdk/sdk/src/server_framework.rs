// Copyright © Aptos Foundation

// Re-export health types for convenience.
pub use crate::health::{
    HealthCheck, ProgressHealthChecker, ProgressHealthConfig, ProgressStatusProvider,
    default_no_progress_threshold_secs,
};
use crate::{
    instrumented_channel::channel_metrics::init_channel_metrics_registry,
    utils::step_metrics::init_step_metrics_registry,
};
use anyhow::{Context, Result};
use autometrics::settings::AutometricsSettings;
use axum::{Router, http::StatusCode, response::IntoResponse, routing::get};
use backtrace::Backtrace;
use clap::Parser;
#[cfg(target_os = "linux")]
use once_cell::sync::Lazy;
use prometheus_client::registry::Registry;
#[cfg(target_os = "linux")]
use regex::Regex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
#[cfg(target_os = "linux")]
use std::fmt::Write as _;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::time::Duration;
// TODO: remove deprecated lint when new clippy nightly is released.
#[allow(deprecated)]
use std::{fs::File, io::Read, panic::PanicInfo, path::PathBuf, process};
use tokio::runtime::Handle;
use tracing::error;
#[cfg(target_os = "linux")]
use tracing::info;
use tracing_subscriber::EnvFilter;

/// ServerArgs bootstraps a server with all common pieces. And then triggers the run method for
/// the specific service.
#[derive(Parser)]
pub struct ServerArgs {
    #[clap(short, long, value_parser)]
    pub config_path: PathBuf,
}

impl ServerArgs {
    pub async fn run<C>(&self, handle: Handle) -> Result<()>
    where
        C: RunnableConfig,
    {
        self.run_with_health_checks::<C>(handle, vec![]).await
    }

    pub async fn run_with_health_checks<C>(
        &self,
        handle: Handle,
        health_checks: Vec<Arc<dyn HealthCheck>>,
    ) -> Result<()>
    where
        C: RunnableConfig,
    {
        setup_logging();
        setup_panic_handler();
        let config = load::<GenericConfig<C>>(&self.config_path)?;
        run_server_with_config(config, handle, health_checks).await
    }
}

/// Run a server and the necessary probes. For spawning these tasks, the user must
/// provide a handle to a runtime they already have.
pub async fn run_server_with_config<C>(
    config: GenericConfig<C>,
    handle: Handle,
    health_checks: Vec<Arc<dyn HealthCheck>>,
) -> Result<()>
where
    C: RunnableConfig,
{
    let health_port = config.health_check_port;
    let additional_labels = config.metrics_config.additional_labels.clone();
    // Start health and metrics probes.
    let task_handler = handle.spawn(async move {
        register_probes_and_metrics_handler(health_port, additional_labels, health_checks).await;
        anyhow::Ok(())
    });
    let main_task_handler = handle.spawn(async move { config.run().await });
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
pub struct GenericConfig<T> {
    // Shared configuration among all services.
    pub health_check_port: u16,

    #[serde(default)]
    pub metrics_config: MetricsConfig,

    // Specific configuration for each service.
    pub server_config: T,
}

#[derive(Clone, Deserialize, Debug, Default, Serialize)]
pub struct MetricsConfig {
    /// Additional labels to use for metrics.
    pub additional_labels: Vec<(String, String)>,
}

#[async_trait::async_trait]
impl<T> RunnableConfig for GenericConfig<T>
where
    T: RunnableConfig,
{
    async fn run(&self) -> Result<()> {
        self.server_config.run().await
    }

    fn get_server_name(&self) -> String {
        self.server_config.get_server_name()
    }
}

/// RunnableConfig is a trait that all services must implement for their configuration.
#[async_trait::async_trait]
pub trait RunnableConfig: DeserializeOwned + Send + Sync + 'static {
    async fn run(&self) -> Result<()>;
    fn get_server_name(&self) -> String;
}

/// Parse a yaml file into a struct.
pub fn load<T: for<'de> Deserialize<'de>>(path: &PathBuf) -> Result<T> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open the file at path: {path:?}",))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .with_context(|| format!("failed to read the file at path: {path:?}",))?;
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
    // TODO: remove deprecated lint when new clippy nightly is released.
    #[allow(deprecated)]
    std::panic::set_hook(Box::new(move |pi: &PanicInfo<'_>| {
        handle_panic(pi);
    }));
}

// Formats and logs panic information.
// TODO: remove deprecated lint when new clippy nightly is released.
#[allow(deprecated)]
fn handle_panic(panic_info: &PanicInfo<'_>) {
    // The Display formatter for a PanicInfo contains the message, payload and location.
    let details = format!("{panic_info}",);
    let backtrace = format!("{:#?}", Backtrace::new());
    let info = CrashInfo { details, backtrace };
    let crash_info = toml::to_string_pretty(&info).unwrap();
    error!("{}", crash_info);
    // TODO / HACK ALARM: Write crash info synchronously via eprintln! to ensure it is written
    // before the process exits which error! doesn't guarantee.
    // This is a workaround until https://github.com/aptos-labs/aptos-core/issues/2038 is resolved.
    eprintln!("{crash_info}",);
    // Kill the process.
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

/// Register health and metrics probes and set up metrics endpoint.
///
/// If `health_checks` is provided, the `/healthz` endpoint will call each check
/// and return 503 if any of them fail, with the failure reasons in the response body.
pub async fn register_probes_and_metrics_handler(
    port: u16,
    additional_labels: Vec<(String, String)>,
    health_checks: Vec<Arc<dyn HealthCheck>>,
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

    let router = if health_checks.is_empty() {
        Router::new()
            .route("/healthz", get(StatusCode::OK))
            .route("/metrics", get(metrics_handler))
    } else {
        Router::new()
            .route(
                "/healthz",
                get({
                    let checks = health_checks.clone();
                    move || health_handler(checks.clone())
                }),
            )
            .route("/metrics", get(metrics_handler))
    };

    #[cfg(target_os = "linux")]
    let router = router.merge(Router::new().route("/profilez", get(profilez_handler)));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}",))
        .await
        .expect("Failed to bind TCP listener");
    axum::serve(listener, router).await.unwrap();
}

/// Health handler that runs all registered checks.
async fn health_handler(checks: Vec<Arc<dyn HealthCheck>>) -> impl IntoResponse {
    let mut failures = Vec::new();

    for check in &checks {
        if let Err(reason) = check.is_healthy().await {
            failures.push(format!("{}: {}", check.name(), reason));
        }
    }

    if failures.is_empty() {
        StatusCode::OK.into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("Not healthy:\n{}", failures.join("\n")),
        )
            .into_response()
    }
}

async fn metrics_handler() -> impl IntoResponse {
    match autometrics::prometheus_exporter::encode_to_string() {
        Ok(prometheus_client_rust_metrics) => (
            StatusCode::OK,
            [("Content-Type", "text/plain; version=0.0.4")],
            prometheus_client_rust_metrics,
        )
            .into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, format!("{err:?}",)).into_response(),
    }
}

#[cfg(target_os = "linux")]
static CPU_PROFILE_MUTEX: Lazy<tokio::sync::Mutex<()>> = Lazy::new(|| tokio::sync::Mutex::new(()));

#[cfg(target_os = "linux")]
async fn start_cpu_profiling(seconds: u64, frequency: i32) -> Result<Vec<u8>> {
    info!(
        seconds = seconds,
        frequency = frequency,
        "Starting cpu profiling."
    );
    let _lock = CPU_PROFILE_MUTEX
        .try_lock()
        .map_err(|_| anyhow::anyhow!("A profiling task is already running."))?;

    let guard = pprof::ProfilerGuard::new(frequency)
        .map_err(|e| anyhow::anyhow!("Failed to start cpu profiling: {e:?}."))?;

    tokio::time::sleep(Duration::from_secs(seconds)).await;

    let regex = Regex::new(r"^(.*)-(\d*)$").unwrap();
    let report = guard
        .report()
        .frames_post_processor(move |frames| {
            if let Some((_, [name, _])) = regex.captures(&frames.thread_name).map(|c| c.extract()) {
                frames.thread_name = name.to_string();
            }
        })
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to generate cpu profiling report: {e:?}."))?;

    let lines: Vec<String> = report
        .data
        .iter()
        .map(|(key, value)| {
            let mut line = key.thread_name_or_id();
            line.push(';');

            for frame in key.frames.iter().rev() {
                for symbol in frame.iter().rev() {
                    write!(&mut line, "{symbol};").unwrap();
                }
            }

            line.pop().unwrap_or_default();
            write!(&mut line, " {value}").unwrap();

            line
        })
        .collect();

    let mut body = Vec::new();
    if !lines.is_empty() {
        let mut options = inferno::flamegraph::Options::default();
        inferno::flamegraph::from_lines(&mut options, lines.iter().map(String::as_str), &mut body)
            .map_err(|e| anyhow::anyhow!("Failed to generate flamegraph report: {e:?}."))?;
    }

    info!("Cpu profiling is done.");
    Ok(body)
}

#[cfg(target_os = "linux")]
async fn profilez_handler() -> impl IntoResponse {
    match start_cpu_profiling(10, 99).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[derive(Clone, Debug, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct TestConfig {
        test: u32,
        test_name: String,
    }

    #[async_trait::async_trait]
    impl RunnableConfig for TestConfig {
        async fn run(&self) -> Result<()> {
            assert_eq!(self.test, 123);
            assert_eq!(self.test_name, "test");
            Ok(())
        }

        fn get_server_name(&self) -> String {
            self.test_name.clone()
        }
    }

    #[test]
    fn test_random_config_creation() {
        let dir = tempdir().expect("tempdir failure");

        let file_path = dir.path().join("testing_yaml.yaml");
        let mut file = File::create(&file_path).expect("create failure");
        let raw_yaml_content = r#"
            health_check_port: 12345
            server_config:
                test: 123
                test_name: "test"
        "#;
        writeln!(file, "{raw_yaml_content}").expect("write_all failure");

        let config = load::<GenericConfig<TestConfig>>(&file_path).unwrap();
        assert_eq!(config.health_check_port, 12345);
        assert_eq!(config.server_config.test, 123);
        assert_eq!(config.server_config.test_name, "test");
    }

    #[test]
    fn verify_tool() {
        use clap::CommandFactory;
        ServerArgs::command().debug_assert()
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_start_cpu_profiling() {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_worker = stop.clone();
        let worker = std::thread::spawn(move || {
            let mut acc = 0u64;
            while !stop_worker.load(std::sync::atomic::Ordering::Relaxed) {
                acc = acc.wrapping_add(1);
                std::hint::black_box(acc);
            }
        });

        let svg = start_cpu_profiling(1, 99).await.expect("profiling failed");
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        worker.join().unwrap();

        let svg_str = String::from_utf8(svg).expect("flamegraph should be valid UTF-8");
        assert!(svg_str.contains("<svg"), "expected SVG flamegraph output");
    }
}
